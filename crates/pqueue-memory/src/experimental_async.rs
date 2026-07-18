//! Experimental ADR-017 mutation-path wiring for the in-memory composed backend.
//!
//! This module is additive. It does not replace the public memory backend alias or claim full async
//! backend parity.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use pqueue_engine::{
    AsyncComposedBackend, Backend, DispatchError, DurabilityClass, EngineResult, OwnedTask,
    OwnedTaskDispatcher, RawCommitOutcome, RawCommitRequest, TaskOutcome, UnifiedAtomicCommit,
    UnifiedAtomicCommitter, task_outcome_channel,
};

use crate::{ComposedMemoryBackend, composed_memory_backend};

/// Atomic raw-commit capability backed by the existing composed memory unit of work.
#[derive(Clone)]
pub struct MemoryAtomicCommitter {
    backend: Arc<ComposedMemoryBackend>,
}

impl MemoryAtomicCommitter {
    pub fn new(backend: Arc<ComposedMemoryBackend>) -> Self {
        Self { backend }
    }
}

impl UnifiedAtomicCommitter for MemoryAtomicCommitter {
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let backend = Arc::clone(&self.backend);
        Box::pin(async move { Backend::commit_raw(backend.as_ref(), request).await })
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

/// Memory-only dispatcher for tasks guaranteed to complete in exactly one poll.
///
/// This is not an executor and is unsuitable for I/O or any task that can yield. A `Pending` result is
/// a hard invariant violation: the task is dropped and no successful outcome receiver is returned.
pub struct MemoryOnePollDispatcher {
    closed: AtomicBool,
}

impl MemoryOnePollDispatcher {
    pub fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
        }
    }
}

impl Default for MemoryOnePollDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> OwnedTaskDispatcher<T> for MemoryOnePollDispatcher {
    fn submit(&self, mut task: OwnedTask<T>) -> Result<TaskOutcome<T>, DispatchError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DispatchError::Closed);
        }

        let waker = Waker::from(Arc::new(NoopWake));
        let value = match Future::poll(Pin::as_mut(&mut task), &mut Context::from_waker(&waker)) {
            Poll::Ready(value) => value,
            Poll::Pending => {
                panic!("MemoryOnePollDispatcher task returned Pending; I/O tasks are unsupported")
            }
        };
        let (sender, outcome) = task_outcome_channel();
        sender.send(value);
        Ok(outcome)
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Build the experimental narrow commit scaffold and return its shared legacy backend for setup/readback.
pub fn experimental_async_memory_backend(
    max_queued_commits: usize,
) -> (
    Arc<ComposedMemoryBackend>,
    AsyncComposedBackend<UnifiedAtomicCommit<MemoryAtomicCommitter>, MemoryOnePollDispatcher>,
) {
    let backend = Arc::new(composed_memory_backend());
    let strategy = UnifiedAtomicCommit::for_profile(
        DurabilityClass::Atomic,
        MemoryAtomicCommitter::new(Arc::clone(&backend)),
    )
    .expect("memory composed backend has atomic durability");
    let commits =
        AsyncComposedBackend::new(strategy, MemoryOnePollDispatcher::new(), max_queued_commits);
    (backend, commits)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::task::Poll;

    use pqueue_conformance::{envelope, item, qdef, shard};
    use pqueue_core::QueueId;
    use pqueue_engine::{
        ControlPlaneStore, LogRead, OwnedTaskDispatcher, ProjectionRead, PushCommand, QueueCommand,
        QueueKey, RawCommitRequest,
    };

    use super::*;

    fn one_poll<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let waker = Waker::from(Arc::new(NoopWake));
        match Future::poll(future.as_mut(), &mut Context::from_waker(&waker)) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("memory test operation unexpectedly returned Pending"),
        }
    }

    fn push_request(queue: &QueueKey, id: &str, epoch: u64) -> RawCommitRequest {
        let pushed = item(id, id, 1);
        let id = pushed.item_id.clone();
        RawCommitRequest::new(
            queue.clone(),
            vec![envelope(
                QueueCommand::Push(PushCommand {
                    items: vec![pushed],
                }),
                vec![id],
            )],
            epoch,
        )
    }

    #[test]
    fn experimental_commit_is_visible_in_log_and_projection() {
        let (backend, commits) = experimental_async_memory_backend(8);
        one_poll(backend.create_queue(qdef())).unwrap();
        let epoch = one_poll(backend.current_epoch(&shard())).unwrap();

        let outcome = one_poll(commits.submit_commit(push_request(&shard(), "101", epoch)))
            .unwrap()
            .unwrap();

        assert!(outcome.projection_applied());
        assert_eq!(
            one_poll(backend.read_from(&shard(), None, 10))
                .unwrap()
                .entries
                .len(),
            1
        );
        assert_eq!(one_poll(backend.peek(&shard(), 10)).unwrap().len(), 1);
    }

    #[test]
    fn immediate_completion_makes_caller_drop_after_submit_irrelevant() {
        let (backend, commits) = experimental_async_memory_backend(1);
        one_poll(backend.create_queue(qdef())).unwrap();
        let epoch = one_poll(backend.current_epoch(&shard())).unwrap();

        let mut caller = Box::pin(commits.submit_commit(push_request(&shard(), "102", epoch)));
        let waker = Waker::from(Arc::new(NoopWake));
        let result = Future::poll(caller.as_mut(), &mut Context::from_waker(&waker));
        assert!(matches!(result, Poll::Ready(Ok(Ok(_)))));
        drop(caller);

        assert_eq!(
            one_poll(backend.read_from(&shard(), None, 10))
                .unwrap()
                .entries
                .len(),
            1
        );
        assert_eq!(one_poll(backend.peek(&shard(), 10)).unwrap().len(), 1);
    }

    #[test]
    fn same_and_different_queue_commits_complete_without_cross_queue_state() {
        let (backend, commits) = experimental_async_memory_backend(2);
        one_poll(backend.create_queue(qdef())).unwrap();
        let first_key = shard();
        let first_epoch = one_poll(backend.current_epoch(&first_key)).unwrap();
        one_poll(commits.submit_commit(push_request(&first_key, "103", first_epoch)))
            .unwrap()
            .unwrap();
        one_poll(commits.submit_commit(push_request(&first_key, "104", first_epoch)))
            .unwrap()
            .unwrap();

        let mut second_definition = qdef();
        second_definition.queue_id = QueueId::new("q2").unwrap();
        let second_key = QueueKey::new(
            second_definition.tenant_id.clone(),
            second_definition.queue_id.clone(),
        );
        one_poll(backend.create_queue(second_definition)).unwrap();
        let second_epoch = one_poll(backend.current_epoch(&second_key)).unwrap();
        one_poll(commits.submit_commit(push_request(&second_key, "105", second_epoch)))
            .unwrap()
            .unwrap();

        assert_eq!(
            one_poll(backend.read_from(&first_key, None, 10))
                .unwrap()
                .entries
                .len(),
            2
        );
        assert_eq!(
            one_poll(backend.read_from(&second_key, None, 10))
                .unwrap()
                .entries
                .len(),
            1
        );
        assert_eq!(one_poll(backend.peek(&first_key, 10)).unwrap().len(), 2);
        assert_eq!(one_poll(backend.peek(&second_key, 10)).unwrap().len(), 1);
    }

    #[test]
    fn pending_task_is_a_hard_invariant_violation_without_success_outcome() {
        let dispatcher = MemoryOnePollDispatcher::new();
        let misuse = catch_unwind(AssertUnwindSafe(|| {
            dispatcher.submit(Box::pin(std::future::pending::<usize>()))
        }));
        assert!(misuse.is_err());

        let mut outcome = dispatcher.submit(Box::pin(std::future::ready(7))).unwrap();
        let waker = Waker::from(Arc::new(NoopWake));
        assert!(matches!(
            Future::poll(Pin::new(&mut outcome), &mut Context::from_waker(&waker)),
            Poll::Ready(Ok(7))
        ));
    }
}
