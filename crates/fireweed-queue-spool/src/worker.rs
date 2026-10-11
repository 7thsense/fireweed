//! Private bounded owned blocking execution. Returned resources still belong to
//! their queue reservation; execution drain is not final-reservation drain.
use super::{QueueError, QueueResult, unknown};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot};
struct State {
    owned: usize,
    closed: bool,
    poisoned: bool,
}
struct Inner {
    state: Mutex<State>,
    max_owned: usize,
    permits: Arc<Semaphore>,
    done: Notify,
    dispatcher: Arc<dyn BlockingDispatcher>,
}
type BlockingOperation = Box<dyn FnOnce() + Send + 'static>;
trait BlockingDispatcher: Send + Sync {
    fn dispatch(&self, runtime: &tokio::runtime::Handle, operation: BlockingOperation);
}
struct TokioDispatcher;
impl BlockingDispatcher for TokioDispatcher {
    fn dispatch(&self, runtime: &tokio::runtime::Handle, operation: BlockingOperation) {
        drop(runtime.spawn_blocking(operation));
    }
}
#[derive(Clone)]
pub struct ScratchWorkers {
    inner: Arc<Inner>,
}
struct Completion {
    inner: Arc<Inner>,
}
impl Drop for Completion {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.owned -= 1;
        }
        self.inner.done.notify_waiters();
    }
}
// Fields drop in this order when a waiting owner is cancelled: operation-owned
// files/charges first, then the scheduler's completion signal.
struct OwnedWork<F> {
    operation: Option<F>,
    _completion: Completion,
}
struct DispatchWork<F, T> {
    work: OwnedWork<F>,
    permit: OwnedSemaphorePermit,
    reply: oneshot::Sender<QueueResult<T>>,
}
fn take_dispatch<F, T>(cell: &Mutex<Option<DispatchWork<F, T>>>) -> Option<DispatchWork<F, T>> {
    // No user work executes while holding this failure-path handoff mutex.
    cell.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}
impl ScratchWorkers {
    pub fn new(max_workers: usize, max_owned: usize) -> QueueResult<Self> {
        Self::with_dispatcher(max_workers, max_owned, Arc::new(TokioDispatcher))
    }
    fn with_dispatcher(
        max_workers: usize,
        max_owned: usize,
        dispatcher: Arc<dyn BlockingDispatcher>,
    ) -> QueueResult<Self> {
        if max_workers == 0 || max_owned < max_workers || max_workers > Semaphore::MAX_PERMITS {
            return Err(unknown("invalid scratch execution bounds"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    owned: 0,
                    closed: false,
                    poisoned: false,
                }),
                max_owned,
                permits: Arc::new(Semaphore::new(max_workers)),
                done: Notify::new(),
                dispatcher,
            }),
        })
    }
    fn register(&self) -> QueueResult<Completion> {
        let mut state = self.inner.state.lock().map_err(unknown)?;
        if state.closed || state.poisoned || state.owned >= self.inner.max_owned {
            return Err(unknown("scratch execution admission unavailable"));
        }
        state.owned = state
            .owned
            .checked_add(1)
            .ok_or_else(|| unknown("scratch execution count overflow"))?;
        Ok(Completion {
            inner: Arc::clone(&self.inner),
        })
    }
    /// Captured resources must own their admission. Cancellation while waiting
    /// for a worker permit drops them; after dispatch the owned operation
    /// completes independently of the awaiting caller.
    /// No unbounded channel/task queue is created by this adapter.
    pub async fn run<T, F>(&self, operation: F) -> QueueResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QueueResult<T> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::try_current().map_err(unknown)?;
        let work = OwnedWork {
            operation: Some(operation),
            _completion: self.register()?,
        };
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(unknown)?;
        if self.inner.state.lock().map_err(unknown)?.poisoned {
            return Err(unknown("scratch dispatcher poisoned"));
        }
        let (reply, response) = oneshot::channel();
        let cell = Arc::new(Mutex::new(Some(DispatchWork {
            work,
            permit,
            reply,
        })));
        let handoff = Arc::clone(&cell);
        let physical: BlockingOperation = Box::new(move || {
            let Some(DispatchWork {
                mut work,
                permit,
                reply,
            }) = take_dispatch(&handoff)
            else {
                return;
            };
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work.operation.take().expect("one owned operation")()
            })) {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error @ QueueError::OutcomeUnknown(_))) => Err(error),
                Ok(Err(_)) => Err(unknown("physical scratch operation failed")),
                Err(_) => Err(unknown("scratch worker panicked")),
            };
            drop(permit);
            drop(work);
            let _ = reply.send(result);
        });
        let dispatched = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.inner.dispatcher.dispatch(&runtime, physical)
        }));
        if dispatched.is_err() {
            {
                let mut state = self.inner.state.lock().map_err(unknown)?;
                state.closed = true;
                state.poisoned = true;
            }
            // Tokio may retain its queued closure even though dispatch panicked.
            // Atomically cancel unstarted work; a later empty stub does no I/O.
            if let Some(unstarted) = take_dispatch(&cell) {
                drop(unstarted);
            } else {
                // A worker already owns it. Retain the operation/fence until its
                // terminal result arrives; discard returned resources before error.
                drop(response.await);
            }
            return Err(unknown("scratch blocking dispatch failed"));
        }
        // The queued closure must be the only owner of unstarted dispatch work.
        // Runtime cancellation can then drop it and close the response channel.
        drop(cell);
        match response.await {
            Ok(result) => result,
            Err(_) => {
                let mut state = self.inner.state.lock().map_err(unknown)?;
                state.closed = true;
                state.poisoned = true;
                Err(unknown("scratch worker stopped before execution"))
            }
        }
    }
    /// Reject new registration but allow all already registered waiters to run.
    /// This drains physical execution, not resources returned to their callers.
    pub async fn close_and_drain(&self) -> QueueResult<()> {
        self.inner.state.lock().map_err(unknown)?.closed = true;
        loop {
            let notified = self.inner.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.state.lock().map_err(unknown)?.owned == 0 {
                return Ok(());
            }
            notified.await;
        }
    }
    #[cfg(test)]
    fn owned(&self) -> usize {
        self.inner.state.lock().unwrap().owned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueueSpool, ScratchDiskQuota, SpoolComponent};
    use fireweed_engine::queue_admission::QueueAdmission;
    use fireweed_engine::queue_planner::UniqueIdentity;
    use std::sync::atomic::{AtomicBool, Ordering};
    async fn owned_count(workers: &ScratchWorkers, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while workers.owned() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn bounds_and_registered_capacity_fail_as_unknown_before_execution() {
        assert!(ScratchWorkers::new(0, 1).is_err());
        assert!(ScratchWorkers::new(2, 1).is_err());
        assert!(ScratchWorkers::new(1, 0).is_err());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let first = workers.clone();
        let operation = tokio::spawn(async move {
            first
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                    Ok(7)
                })
                .await
        });
        held.await.unwrap();
        assert_eq!(workers.owned(), 1);
        let executed = Arc::new(AtomicBool::new(false));
        let mark = Arc::clone(&executed);
        assert!(matches!(
            workers
                .run(move || {
                    mark.store(true, Ordering::SeqCst);
                    Ok(8)
                })
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(!executed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        assert_eq!(operation.await.unwrap().unwrap(), 7);
        workers.close_and_drain().await.unwrap();
        assert!(matches!(
            workers.run(|| Ok(0)).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
    }
    #[tokio::test]
    async fn queued_cancellation_drops_resources_before_registration_completion() {
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let first = workers.clone();
        let operation = tokio::spawn(async move {
            first
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                    Ok(1)
                })
                .await
        });
        held.await.unwrap();
        let admission = QueueAdmission::new(1, 100).unwrap();
        let observed = Arc::new(Mutex::new(None));
        let callback = Arc::clone(&observed);
        let counter = workers.clone();
        let job = Arc::new(
            admission
                .reserve(10)
                .unwrap()
                .with_terminal_callback(move || {
                    *callback.lock().unwrap() = Some(counter.owned());
                })
                .unwrap(),
        );
        let resource = job.owned_buffer(40).unwrap();
        drop(job);
        let executed = Arc::new(AtomicBool::new(false));
        let mark = Arc::clone(&executed);
        let queued = workers.clone();
        let queued = tokio::spawn(async move {
            queued
                .run(move || {
                    let _resource = resource;
                    mark.store(true, Ordering::SeqCst);
                    Ok(2)
                })
                .await
        });
        owned_count(&workers, 2).await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        assert_eq!(*observed.lock().unwrap(), Some(2));
        assert_eq!(workers.owned(), 1);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert!(!executed.load(Ordering::SeqCst));
        release.send(()).unwrap();
        operation.await.unwrap().unwrap();
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn close_preserves_registered_waiters_and_cancelled_close_is_repeatable() {
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let first = workers.clone();
        let first = tokio::spawn(async move {
            first
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                    Ok(1)
                })
                .await
        });
        held.await.unwrap();
        let second = workers.clone();
        let second = tokio::spawn(async move { second.run(|| Ok(2)).await });
        owned_count(&workers, 2).await;
        {
            let close = workers.close_and_drain();
            tokio::pin!(close);
            assert!(matches!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(close.as_mut().poll(cx))).await,
                std::task::Poll::Pending
            ));
        } // Drop the actual close future and its enabled notification.
        assert!(matches!(
            workers.run(|| Ok(3)).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        release.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 1);
        assert_eq!(second.await.unwrap().unwrap(), 2);
        tokio::time::timeout(std::time::Duration::from_secs(5), workers.close_and_drain())
            .await
            .unwrap()
            .unwrap();
        workers.close_and_drain().await.unwrap();
        assert_eq!(workers.owned(), 0);
    }
    #[tokio::test]
    async fn blocked_worker_does_not_stop_another_execution_slot() {
        let workers = ScratchWorkers::new(2, 2).unwrap();
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let first = workers.clone();
        let first = tokio::spawn(async move {
            first
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                    Ok(1)
                })
                .await
        });
        held.await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), workers.run(|| Ok(9)))
                .await
                .unwrap()
                .unwrap(),
            9
        );
        assert_eq!(workers.owned(), 1);
        release.send(()).unwrap();
        first.await.unwrap().unwrap();
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn cancelled_running_spool_finishes_and_final_reservation_signals_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let terminal = Arc::new(Notify::new());
        let done = Arc::clone(&terminal);
        let job = Arc::new(
            admission
                .reserve(128)
                .unwrap()
                .with_terminal_callback(move || done.notify_waiters())
                .unwrap(),
        );
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        for value in [3, 1, 2] {
            spool
                .append(
                    &UniqueIdentity {
                        collection: None,
                        index: "u".into(),
                        full_sort_key: vec![value],
                    },
                    b"owned",
                )
                .unwrap();
        }
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let running = workers.clone();
        let caller = tokio::spawn(async move {
            running
                .run(move || {
                    let _ = ready.send(());
                    let _ = wait.recv();
                    let mut sorted = spool.seal()?;
                    while let Some(entry) = sorted.next_entry()? {
                        let mut bytes = [0; 5];
                        assert_eq!(
                            sorted.read_component(
                                &entry,
                                SpoolComponent::Payload,
                                0,
                                &mut bytes
                            )?,
                            5
                        );
                        assert_eq!(&bytes, b"owned");
                    }
                    Ok(sorted)
                })
                .await
        });
        held.await.unwrap();
        drop(job);
        admission.close().unwrap();
        caller.abort();
        assert!(caller.await.err().unwrap().is_cancelled());
        assert_eq!(workers.owned(), 1);
        assert_eq!(admission.usage().unwrap().0, 1);
        let closing = workers.close_and_drain();
        tokio::pin!(closing);
        assert!(matches!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(closing.as_mut().poll(cx))).await,
            std::task::Poll::Pending
        ));
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), closing)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = terminal.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if admission.usage().unwrap().0 == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[tokio::test]
    async fn immediate_next_job_does_not_observe_previous_completion_capacity() {
        let workers = ScratchWorkers::new(1, 1).unwrap();
        for value in 0..100 {
            assert_eq!(workers.run(move || Ok(value)).await.unwrap(), value);
            assert_eq!(workers.owned(), 0);
        }
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn close_registration_and_final_completion_race_is_linearized() {
        for iteration in 0..32 {
            let workers = ScratchWorkers::new(1, 2).unwrap();
            let (ready, held) = oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let first = workers.clone();
            let first = tokio::spawn(async move {
                first
                    .run(move || {
                        let _ = ready.send(());
                        let _ = wait.recv();
                        Ok(1)
                    })
                    .await
            });
            held.await.unwrap();
            let closing = workers.clone();
            let close = tokio::spawn(async move {
                if iteration % 2 == 0 {
                    tokio::task::yield_now().await;
                }
                closing.close_and_drain().await
            });
            let executed = Arc::new(AtomicBool::new(false));
            let mark = Arc::clone(&executed);
            let extra = workers.clone();
            let extra = tokio::spawn(async move {
                if iteration % 2 != 0 {
                    tokio::task::yield_now().await;
                }
                extra
                    .run(move || {
                        mark.store(true, Ordering::SeqCst);
                        Ok(2)
                    })
                    .await
            });
            release.send(()).unwrap();
            assert_eq!(first.await.unwrap().unwrap(), 1);
            match extra.await.unwrap() {
                Ok(value) => {
                    assert_eq!(value, 2);
                    assert!(executed.load(Ordering::SeqCst));
                }
                Err(QueueError::OutcomeUnknown(_)) => assert!(!executed.load(Ordering::SeqCst)),
                other => panic!("unexpected race outcome: {other:?}"),
            }
            tokio::time::timeout(std::time::Duration::from_secs(5), close)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(workers.owned(), 0);
            workers.close_and_drain().await.unwrap();
        }
    }
    struct EnqueueThenPanic {
        retained: Mutex<Option<BlockingOperation>>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl BlockingDispatcher for EnqueueThenPanic {
        fn dispatch(&self, _runtime: &tokio::runtime::Handle, operation: BlockingOperation) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.retained.lock().unwrap() = Some(operation);
            panic!("injected enqueue before thread-creation panic");
        }
    }
    #[tokio::test]
    async fn enqueued_dispatch_panic_cancels_unstarted_io_and_poison_blocks_more_stubs() {
        let dispatcher = Arc::new(EnqueueThenPanic {
            retained: Mutex::new(None),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let workers = ScratchWorkers::with_dispatcher(1, 2, dispatcher.clone()).unwrap();
        let admission = QueueAdmission::new(1, 100).unwrap();
        let job = Arc::new(admission.reserve(10).unwrap());
        let resource = job.owned_buffer(40).unwrap();
        drop(job);
        let executed = Arc::new(AtomicBool::new(false));
        let mark = Arc::clone(&executed);
        assert!(matches!(
            workers
                .run(move || {
                    let _resource = resource;
                    mark.store(true, Ordering::SeqCst);
                    Ok(1)
                })
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(workers.owned(), 0);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert!(!executed.load(Ordering::SeqCst));
        assert!(matches!(
            workers.run(|| Ok(2)).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(dispatcher.calls.load(Ordering::SeqCst), 1);
        // Tokio's retained stub may execute later, but no longer owns any I/O.
        dispatcher.retained.lock().unwrap().take().unwrap()();
        assert!(!executed.load(Ordering::SeqCst));
        assert_eq!(workers.owned(), 0);
        workers.close_and_drain().await.unwrap();
    }
    struct StartedThenPanic {
        started: Mutex<std::sync::mpsc::Receiver<()>>,
        worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    }
    impl BlockingDispatcher for StartedThenPanic {
        fn dispatch(&self, _runtime: &tokio::runtime::Handle, operation: BlockingOperation) {
            *self.worker.lock().unwrap() = Some(std::thread::spawn(operation));
            self.started.lock().unwrap().recv().unwrap();
            panic!("injected dispatch panic after worker ownership transfer");
        }
    }
    #[tokio::test]
    async fn dispatch_panic_after_start_waits_for_io_and_drops_result_before_error() {
        let (started, received) = std::sync::mpsc::channel();
        let dispatcher = Arc::new(StartedThenPanic {
            started: Mutex::new(received),
            worker: Mutex::new(None),
        });
        let workers = ScratchWorkers::with_dispatcher(1, 1, dispatcher.clone()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(128).unwrap());
        let quota = ScratchDiskQuota::new(4096).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        spool
            .append(
                &UniqueIdentity {
                    collection: None,
                    index: "u".into(),
                    full_sort_key: vec![1],
                },
                b"owned",
            )
            .unwrap();
        drop(job);
        let (ready, held) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let running = workers.clone();
        let caller = tokio::spawn(async move {
            running
                .run(move || {
                    started.send(()).unwrap();
                    let _ = ready.send(());
                    let _ = wait.recv();
                    spool.seal()
                })
                .await
        });
        held.await.unwrap();
        assert!(!caller.is_finished());
        assert_eq!(workers.owned(), 1);
        assert_eq!(admission.usage().unwrap().0, 1);
        release.send(()).unwrap();
        assert!(matches!(
            caller.await.unwrap(),
            Err(QueueError::OutcomeUnknown(_))
        ));
        dispatcher
            .worker
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(workers.owned(), 0);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        workers.close_and_drain().await.unwrap();
    }
    struct DropQueuedDispatch;
    impl BlockingDispatcher for DropQueuedDispatch {
        fn dispatch(&self, _runtime: &tokio::runtime::Handle, operation: BlockingOperation) {
            drop(operation);
        }
    }
    #[tokio::test]
    async fn runtime_dropped_unstarted_task_releases_cell_and_reply_ownership() {
        let workers = ScratchWorkers::with_dispatcher(1, 1, Arc::new(DropQueuedDispatch)).unwrap();
        let admission = QueueAdmission::new(1, 100).unwrap();
        let job = Arc::new(admission.reserve(10).unwrap());
        let resource = job.owned_buffer(40).unwrap();
        drop(job);
        let executed = Arc::new(AtomicBool::new(false));
        let mark = Arc::clone(&executed);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            workers.run(move || {
                let _resource = resource;
                mark.store(true, Ordering::SeqCst);
                Ok(1)
            }),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
        assert!(!executed.load(Ordering::SeqCst));
        assert_eq!(admission.usage().unwrap(), (0, 0));
        assert_eq!(workers.owned(), 0);
        workers.close_and_drain().await.unwrap();
    }
    #[tokio::test]
    async fn physical_panic_and_other_error_are_unknown_and_release_execution() {
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let panic = workers
            .run(|| -> QueueResult<()> { panic!("injected physical worker panic") })
            .await;
        assert!(matches!(panic, Err(QueueError::OutcomeUnknown(_))));
        assert_eq!(workers.owned(), 0);
        assert!(matches!(
            workers.run(|| Err::<(), _>(QueueError::TooLarge)).await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        workers.close_and_drain().await.unwrap();
    }
}
