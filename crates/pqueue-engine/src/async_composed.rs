//! Narrow async mutation-path scaffolding from ADR-017.
//!
//! This type intentionally does not implement the legacy [`crate::Backend`] port. It proves the
//! queue-admission, owned commit-task, and runtime-neutral dispatch boundaries without claiming
//! read-path or operation-port parity.

use crate::{
    AsyncCommitStrategy, DispatchError, DurabilityClass, KeyedQueueGate, OwnedTaskDispatcher,
    QueueGateError, RawCommitRequest, TaskOutcomeError,
};

/// Failure outside the strategy-owned commit result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncCommitSubmitError {
    Admission(QueueGateError),
    Dispatch(DispatchError),
    Outcome(TaskOutcomeError),
}

/// An ADR-017 mutation-path scaffold, not a full backend implementation.
pub struct AsyncComposedBackend<S, D> {
    strategy: S,
    dispatcher: D,
    admission: KeyedQueueGate<crate::QueueKey>,
    durability: DurabilityClass,
}

impl<S, D> AsyncComposedBackend<S, D>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher<S::Output>,
{
    pub fn new(strategy: S, dispatcher: D, max_queued_commits: usize) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy,
            dispatcher,
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
        }
    }

    pub fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// Submit one typed raw commit through queue-local admission and backend-owned execution.
    ///
    /// Before `submit` succeeds, dropping this future cancels only its admission attempt. Once
    /// submission succeeds, the dispatcher owns both the commit task and its queue permit; dropping
    /// this caller future discards only the outcome receiver.
    pub async fn submit_commit(
        &self,
        request: RawCommitRequest,
    ) -> Result<S::Output, AsyncCommitSubmitError> {
        let permit = self
            .admission
            .acquire(request.shard().clone())
            .await
            .map_err(AsyncCommitSubmitError::Admission)?;
        let commit = self.strategy.commit(request);
        let owned = Box::pin(async move {
            let _permit = permit;
            commit.await
        });
        let outcome = self
            .dispatcher
            .submit(owned)
            .map_err(AsyncCommitSubmitError::Dispatch)?;
        outcome.await.map_err(AsyncCommitSubmitError::Outcome)
    }

    /// Close queued admission and dispatcher submission. Accepted tasks remain dispatcher-owned.
    pub fn close(&self) {
        self.admission.close();
        self.dispatcher.close();
    }

    pub fn is_closed(&self) -> bool {
        self.admission.is_closed() || self.dispatcher.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use pqueue_core::{QueueId, TenantId};

    use super::*;
    use crate::{
        OwnedTask, TaskOutcome, TaskOutcomeSender, UnifiedAtomicCommit, UnifiedAtomicCommitter,
        task_outcome_channel,
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(NoopWake));
        future.poll(&mut Context::from_waker(&waker))
    }

    fn queue(name: &str) -> crate::QueueKey {
        crate::QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn request(name: &str) -> RawCommitRequest {
        RawCommitRequest::new(queue(name), Vec::new(), 1)
    }

    #[derive(Clone)]
    struct ControlledCommitter {
        committed: Arc<Mutex<Vec<crate::QueueKey>>>,
    }

    impl UnifiedAtomicCommitter for ControlledCommitter {
        type Request = RawCommitRequest;
        type Output = crate::QueueKey;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            let committed = Arc::clone(&self.committed);
            Box::pin(async move {
                let key = request.shard().clone();
                committed.lock().unwrap().push(key.clone());
                key
            })
        }
    }

    struct DispatchState<T> {
        closed: AtomicBool,
        capacity: usize,
        tasks: Mutex<VecDeque<(OwnedTask<T>, TaskOutcomeSender<T>)>>,
    }

    #[derive(Clone)]
    struct ControlledDispatcher<T> {
        state: Arc<DispatchState<T>>,
    }

    impl<T: Send + 'static> ControlledDispatcher<T> {
        fn new(capacity: usize) -> Self {
            Self {
                state: Arc::new(DispatchState {
                    closed: AtomicBool::new(false),
                    capacity,
                    tasks: Mutex::new(VecDeque::new()),
                }),
            }
        }

        fn queued(&self) -> usize {
            self.state.tasks.lock().unwrap().len()
        }

        fn run(&self, index: usize) {
            let (mut task, sender) = self.state.tasks.lock().unwrap().remove(index).unwrap();
            match poll_once(task.as_mut()) {
                Poll::Ready(value) => sender.send(value),
                Poll::Pending => panic!("controlled commit unexpectedly pending"),
            }
        }
    }

    impl<T: Send + 'static> OwnedTaskDispatcher<T> for ControlledDispatcher<T> {
        fn submit(&self, task: OwnedTask<T>) -> Result<TaskOutcome<T>, DispatchError> {
            if self.state.closed.load(Ordering::Acquire) {
                return Err(DispatchError::Closed);
            }
            let mut tasks = self.state.tasks.lock().unwrap();
            if tasks.len() >= self.state.capacity {
                return Err(DispatchError::AtCapacity);
            }
            let (sender, outcome) = task_outcome_channel();
            tasks.push_back((task, sender));
            Ok(outcome)
        }

        fn close(&self) {
            self.state.closed.store(true, Ordering::Release);
        }

        fn is_closed(&self) -> bool {
            self.state.closed.load(Ordering::Acquire)
        }
    }

    type TestBackend = AsyncComposedBackend<
        UnifiedAtomicCommit<ControlledCommitter>,
        ControlledDispatcher<crate::QueueKey>,
    >;

    fn backend(
        capacity: usize,
        max_queued: usize,
    ) -> (
        TestBackend,
        ControlledDispatcher<crate::QueueKey>,
        Arc<Mutex<Vec<crate::QueueKey>>>,
    ) {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let strategy = UnifiedAtomicCommit::for_profile(
            DurabilityClass::Atomic,
            ControlledCommitter {
                committed: Arc::clone(&committed),
            },
        )
        .unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        (
            AsyncComposedBackend::new(strategy, dispatcher.clone(), max_queued),
            dispatcher,
            committed,
        )
    }

    #[test]
    fn before_first_poll_has_no_admission_or_submission_effect() {
        let (backend, dispatcher, committed) = backend(1, 1);
        let future = Box::pin(backend.submit_commit(request("q")));
        assert_eq!(backend.admission.entry_count(), 0);
        assert_eq!(backend.admission.queued(), 0);
        assert_eq!(dispatcher.queued(), 0);
        drop(future);
        assert!(committed.lock().unwrap().is_empty());
    }

    #[test]
    fn queued_cancellation_removes_waiter_without_committing() {
        let (backend, dispatcher, committed) = backend(2, 2);
        let mut first = Box::pin(backend.submit_commit(request("q")));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        let mut cancelled = Box::pin(backend.submit_commit(request("q")));
        assert!(matches!(poll_once(cancelled.as_mut()), Poll::Pending));
        assert_eq!(backend.admission.queued(), 1);
        drop(cancelled);
        assert_eq!(backend.admission.queued(), 0);
        dispatcher.run(0);
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(committed.lock().unwrap().len(), 1);
    }

    #[test]
    fn same_queue_serializes_through_the_entire_owned_task() {
        let (backend, dispatcher, _) = backend(2, 2);
        let mut first = Box::pin(backend.submit_commit(request("q")));
        let mut second = Box::pin(backend.submit_commit(request("q")));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.queued(), 1);
        dispatcher.run(0);
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.queued(), 1);
        dispatcher.run(0);
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn unrelated_queue_can_progress_while_another_task_is_stalled() {
        let (backend, dispatcher, committed) = backend(2, 2);
        let mut stalled = Box::pin(backend.submit_commit(request("a")));
        let mut unrelated = Box::pin(backend.submit_commit(request("b")));
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.queued(), 2);
        dispatcher.run(1);
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(committed.lock().unwrap().as_slice(), &[queue("b")]);
        dispatcher.run(0);
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn dispatcher_capacity_and_close_rejections_release_queue_gate() {
        let (full_backend, _, _) = backend(0, 1);
        let mut full = Box::pin(full_backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(full.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));
        assert_eq!(full_backend.admission.entry_count(), 0);

        let (closed_backend, dispatcher, _) = backend(1, 1);
        dispatcher.close();
        let mut closed = Box::pin(closed_backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(closed.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(DispatchError::Closed)))
        ));
        assert_eq!(closed_backend.admission.entry_count(), 0);
    }

    #[test]
    fn caller_drop_after_submission_does_not_cancel_commit() {
        let (backend, dispatcher, committed) = backend(1, 1);
        let mut caller = Box::pin(backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert_eq!(dispatcher.queued(), 1);
        drop(caller);
        dispatcher.run(0);
        assert_eq!(committed.lock().unwrap().as_slice(), &[queue("q")]);
        assert_eq!(backend.admission.entry_count(), 0);
    }

    #[test]
    fn submission_future_is_send_and_close_stops_admission() {
        fn assert_send<T: Send>(_: T) {}

        let (backend, _, _) = backend(1, 1);
        assert_eq!(backend.durability_class(), DurabilityClass::Atomic);
        assert_send(backend.submit_commit(request("q")));
        backend.close();
        assert!(backend.is_closed());
        let mut rejected = Box::pin(backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(rejected.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Admission(
                QueueGateError::Closed
            )))
        ));
    }
}
