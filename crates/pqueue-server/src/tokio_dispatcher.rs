//! Tokio execution adapter for runtime-neutral owned queue-operation tasks.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use pqueue_engine::{
    DispatchError, OwnedTaskDispatcher, OwnedTaskFactory, TaskOutcome, TaskOutcomeSender,
    task_outcome_channel,
};
use tokio::runtime::Handle;

/// A bounded Tokio-backed dispatcher for owned queue-operation tasks.
///
/// Capacity covers both tasks currently being polled and accepted tasks waiting for a running slot.
/// Once accepted, a task is detached from its caller: dropping the returned outcome never cancels work.
#[derive(Clone)]
pub struct TokioTaskDispatcher {
    inner: Arc<Inner>,
}

struct Inner {
    handle: Handle,
    max_running: usize,
    max_queued: usize,
    state: Mutex<State>,
}

struct State {
    closed: bool,
    running: usize,
    outstanding: usize,
    pending_completions: usize,
    advancing_completions: bool,
    queued: VecDeque<Box<dyn ErasedJob>>,
    drainers: Vec<TaskOutcomeSender<()>>,
}

trait ErasedJob: Send {
    fn into_future(self: Box<Self>, completion: CompletionGuard) -> pqueue_engine::OwnedTask<()>;
}

struct TypedJob<T> {
    factory: OwnedTaskFactory<T>,
    outcome: TaskOutcomeSender<T>,
}

impl<T: Send + 'static> ErasedJob for TypedJob<T> {
    fn into_future(self: Box<Self>, completion: CompletionGuard) -> pqueue_engine::OwnedTask<()> {
        let Self { factory, outcome } = *self;
        Box::pin(async move {
            // Construct the guard before invoking user-supplied work. It accounts for the accepted task
            // even when the factory or task panics, or Tokio drops this future during runtime shutdown.
            let _completion = completion;
            let value = factory().await;
            outcome.send(value);
        })
    }
}

struct CompletionGuard {
    inner: Arc<Inner>,
}

struct CompletionAdvance {
    next: Option<Box<dyn ErasedJob>>,
    drainers: Vec<TaskOutcomeSender<()>>,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        task_finished(&self.inner);
    }
}

impl TokioTaskDispatcher {
    /// Bind a dispatcher to the current Tokio runtime.
    pub fn new(max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self::with_handle(Handle::current(), max_running, max_queued)
    }

    /// Bind a dispatcher to an explicit Tokio runtime handle.
    pub fn with_handle(handle: Handle, max_running: NonZeroUsize, max_queued: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                handle,
                max_running: max_running.get(),
                max_queued,
                state: Mutex::new(State {
                    closed: false,
                    running: 0,
                    outstanding: 0,
                    pending_completions: 0,
                    advancing_completions: false,
                    queued: VecDeque::new(),
                    drainers: Vec::new(),
                }),
            }),
        }
    }
}

impl OwnedTaskDispatcher for TokioTaskDispatcher {
    fn submit<T: Send + 'static>(
        &self,
        factory: OwnedTaskFactory<T>,
    ) -> Result<TaskOutcome<T>, DispatchError> {
        let (outcome_sender, outcome) = task_outcome_channel();
        let job: Box<dyn ErasedJob> = Box::new(TypedJob {
            factory,
            outcome: outcome_sender,
        });

        let job_to_start = {
            let mut state = self.inner.state.lock().expect("Tokio dispatcher poisoned");
            if state.closed {
                return Err(DispatchError::Closed);
            }
            if state.running == self.inner.max_running
                && state.queued.len() == self.inner.max_queued
            {
                return Err(DispatchError::AtCapacity);
            }

            state.outstanding += 1;
            if state.running < self.inner.max_running {
                state.running += 1;
                Some(job)
            } else {
                state.queued.push_back(job);
                None
            }
        };

        if let Some(job) = job_to_start {
            start_job(&self.inner, job);
        }
        Ok(outcome)
    }

    fn close(&self) {
        let drainers = {
            let mut state = self.inner.state.lock().expect("Tokio dispatcher poisoned");
            state.closed = true;
            take_ready_drainers(&mut state)
        };
        resolve_drainers(drainers);
    }

    fn is_closed(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("Tokio dispatcher poisoned")
            .closed
    }

    fn drain(&self) -> TaskOutcome<()> {
        let (sender, outcome) = task_outcome_channel();
        let immediate = {
            let mut state = self.inner.state.lock().expect("Tokio dispatcher poisoned");
            if state.closed && state.outstanding == 0 {
                Some(sender)
            } else {
                state.drainers.push(sender);
                None
            }
        };
        if let Some(sender) = immediate {
            sender.send(());
        }
        outcome
    }
}

fn start_job(inner: &Arc<Inner>, job: Box<dyn ErasedJob>) {
    let completion = CompletionGuard {
        inner: Arc::clone(inner),
    };
    let task = job.into_future(completion);
    // The JoinHandle is deliberately detached. The dispatcher, not the outcome receiver, owns accepted
    // execution. If Tokio aborts the task, dropping `task` drops both its outcome sender and completion
    // guard, reporting failure to the caller and allowing drain to finish.
    drop(inner.handle.spawn(task));
}

fn task_finished(inner: &Arc<Inner>) {
    let should_advance = {
        let mut state = inner.state.lock().expect("Tokio dispatcher poisoned");
        state.pending_completions += 1;
        if state.advancing_completions {
            false
        } else {
            state.advancing_completions = true;
            true
        }
    };
    if should_advance {
        advance_completions(inner);
    }
}

/// Iteratively advances completion bookkeeping and queued work.
///
/// A runtime that is shutting down may drop a newly spawned queued task immediately. Its completion
/// guard records another pending completion above instead of recursively starting the following task.
fn advance_completions(inner: &Arc<Inner>) {
    loop {
        let Some(advance) = finish_one_pending(inner) else {
            return;
        };

        resolve_drainers(advance.drainers);
        if let Some(next) = advance.next {
            start_job(inner, next);
        }
    }
}

fn finish_one_pending(inner: &Arc<Inner>) -> Option<CompletionAdvance> {
    let mut state = inner.state.lock().expect("Tokio dispatcher poisoned");
    if state.pending_completions == 0 {
        state.advancing_completions = false;
        return None;
    }

    state.pending_completions -= 1;
    state.running = state
        .running
        .checked_sub(1)
        .expect("finished Tokio task was not running");
    state.outstanding = state
        .outstanding
        .checked_sub(1)
        .expect("finished Tokio task was not outstanding");
    let next = state.queued.pop_front();
    if next.is_some() {
        state.running += 1;
    }
    let drainers = take_ready_drainers(&mut state);
    Some(CompletionAdvance { next, drainers })
}

fn take_ready_drainers(state: &mut State) -> Vec<TaskOutcomeSender<()>> {
    if state.closed && state.outstanding == 0 {
        std::mem::take(&mut state.drainers)
    } else {
        Vec::new()
    }
}

fn resolve_drainers(drainers: Vec<TaskOutcomeSender<()>>) {
    for drainer in drainers {
        drainer.send(());
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pqueue_engine::{OwnedTaskDispatcher, TaskOutcomeError};
    use tokio::sync::oneshot;

    use super::*;

    fn dispatcher(max_running: usize, max_queued: usize) -> TokioTaskDispatcher {
        TokioTaskDispatcher::new(NonZeroUsize::new(max_running).unwrap(), max_queued)
    }

    #[tokio::test]
    async fn supports_heterogeneous_outputs() {
        let dispatcher = dispatcher(2, 0);
        let number = dispatcher
            .submit(Box::new(|| Box::pin(async { 42_u64 })))
            .unwrap();
        let text = dispatcher
            .submit(Box::new(|| Box::pin(async { String::from("done") })))
            .unwrap();

        assert_eq!(number.await, Ok(42));
        assert_eq!(text.await, Ok(String::from("done")));
        dispatcher.close();
        assert_eq!(dispatcher.drain().await, Ok(()));
    }

    #[tokio::test]
    async fn capacity_counts_running_and_queued_without_invoking_rejected_factory() {
        let dispatcher = dispatcher(1, 1);
        let (first_release, first_wait) = oneshot::channel();
        let first = dispatcher
            .submit(Box::new(|| {
                Box::pin(async move {
                    first_wait.await.unwrap();
                    1
                })
            }))
            .unwrap();
        let invocations = Arc::new(AtomicUsize::new(0));
        let queued_invocations = Arc::clone(&invocations);
        let second = dispatcher
            .submit(Box::new(move || {
                queued_invocations.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { 2 })
            }))
            .unwrap();
        let rejected_invocations = Arc::clone(&invocations);
        let rejected = dispatcher.submit(Box::new(move || {
            rejected_invocations.fetch_add(100, Ordering::SeqCst);
            Box::pin(async { 3 })
        }));

        assert!(matches!(rejected, Err(DispatchError::AtCapacity)));
        assert_eq!(invocations.load(Ordering::SeqCst), 0);
        first_release.send(()).unwrap();
        assert_eq!(first.await, Ok(1));
        assert_eq!(second.await, Ok(2));
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn close_rejects_new_work_and_drain_waits_for_every_accepted_task() {
        let dispatcher = dispatcher(1, 1);
        let (release, wait) = oneshot::channel();
        let first = dispatcher
            .submit(Box::new(|| {
                Box::pin(async move {
                    wait.await.unwrap();
                })
            }))
            .unwrap();
        let second = dispatcher.submit(Box::new(|| Box::pin(async {}))).unwrap();
        dispatcher.close();
        assert!(dispatcher.is_closed());
        assert!(matches!(
            dispatcher.submit(Box::new(|| Box::pin(async {}))),
            Err(DispatchError::Closed)
        ));

        let mut drain = Box::pin(dispatcher.drain());
        assert!(matches!(
            std::future::poll_fn(|context| std::task::Poll::Ready(drain.as_mut().poll(context)))
                .await,
            std::task::Poll::Pending
        ));
        release.send(()).unwrap();
        assert_eq!(first.await, Ok(()));
        assert_eq!(second.await, Ok(()));
        assert_eq!(drain.await, Ok(()));
    }

    #[tokio::test]
    async fn dropping_outcome_does_not_cancel_accepted_task() {
        let dispatcher = dispatcher(1, 0);
        let (completed_tx, completed_rx) = oneshot::channel();
        let outcome = dispatcher
            .submit(Box::new(|| {
                Box::pin(async move {
                    completed_tx.send(()).unwrap();
                })
            }))
            .unwrap();
        drop(outcome);
        dispatcher.close();

        completed_rx.await.unwrap();
        assert_eq!(dispatcher.drain().await, Ok(()));
    }

    #[tokio::test]
    async fn panicking_task_reports_failure_and_does_not_strand_drain() {
        let dispatcher = dispatcher(1, 0);
        let outcome = dispatcher
            .submit(Box::new(|| {
                Box::pin(async move { panic!("intentional dispatcher task panic") })
            }))
            .unwrap();
        dispatcher.close();

        assert_eq!(outcome.await, Err(TaskOutcomeError::DispatcherDropped));
        assert_eq!(dispatcher.drain().await, Ok(()));
    }

    #[test]
    fn runtime_abort_reports_failure_and_iteratively_releases_queued_jobs() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let dispatcher = TokioTaskDispatcher::with_handle(
            runtime.handle().clone(),
            NonZeroUsize::new(1).unwrap(),
            4,
        );
        let (started_tx, started_rx) = oneshot::channel();
        let first_outcome = dispatcher
            .submit(Box::new(|| {
                Box::pin(async move {
                    started_tx.send(()).unwrap();
                    pending::<()>().await;
                })
            }))
            .unwrap();
        runtime.block_on(started_rx).unwrap();
        let queued_outcomes: Vec<_> = (0..4)
            .map(|_| {
                dispatcher
                    .submit(Box::new(|| Box::pin(pending::<()>())))
                    .unwrap()
            })
            .collect();
        dispatcher.close();
        let drain = dispatcher.drain();
        runtime.shutdown_background();

        let observer = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(
            observer.block_on(first_outcome),
            Err(TaskOutcomeError::DispatcherDropped)
        );
        for outcome in queued_outcomes {
            assert_eq!(
                observer.block_on(outcome),
                Err(TaskOutcomeError::DispatcherDropped)
            );
        }
        assert_eq!(observer.block_on(drain), Ok(()));
    }
}
