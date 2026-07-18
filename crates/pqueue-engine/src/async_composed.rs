//! Narrow async mutation-path scaffolding from ADR-017.

use std::sync::Arc;

use crate::{
    AsyncCommitStrategy, DispatchError, DurabilityClass, KeyedQueueGate, OwnedTask,
    OwnedTaskDispatcher, QueueGateError, QueueKey, RawCommitRequest, TaskOutcomeError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncCommitSubmitError {
    Admission(QueueGateError),
    Dispatch(DispatchError),
    Outcome(TaskOutcomeError),
}

/// An ADR-017 mutation-path scaffold, not a full backend implementation.
pub struct AsyncComposedBackend<S, D> {
    strategy: Arc<S>,
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
            strategy: Arc::new(strategy),
            dispatcher,
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
        }
    }

    pub fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// Serialize and dispatch one complete queue-local operation.
    ///
    /// The factory is invoked only after dispatcher acceptance. Its owned task retains the queue permit
    /// across every phase it contains, such as validation, idempotency, claim planning, commit, and render.
    /// This remains a narrow composition primitive and does not imply operation-port parity.
    pub(crate) async fn submit_operation<F>(
        &self,
        queue: QueueKey,
        operation: F,
    ) -> Result<S::Output, AsyncCommitSubmitError>
    where
        F: FnOnce() -> OwnedTask<S::Output> + Send + 'static,
    {
        let permit = self
            .admission
            .acquire(queue)
            .await
            .map_err(AsyncCommitSubmitError::Admission)?;
        let outcome = self
            .dispatcher
            .submit(Box::new(move || {
                let operation = operation();
                Box::pin(async move {
                    let _permit = permit;
                    operation.await
                })
            }))
            .map_err(AsyncCommitSubmitError::Dispatch)?;
        outcome.await.map_err(AsyncCommitSubmitError::Outcome)
    }

    /// Typed raw-commit wrapper over [`Self::submit_operation`].
    pub async fn submit_commit(
        &self,
        request: RawCommitRequest,
    ) -> Result<S::Output, AsyncCommitSubmitError> {
        let queue = request.shard().clone();
        let strategy = Arc::clone(&self.strategy);
        self.submit_operation(queue, move || strategy.commit(request))
            .await
    }

    pub fn close(&self) {
        self.admission.close();
        self.dispatcher.close();
    }

    pub fn is_closed(&self) -> bool {
        self.admission.is_closed() || self.dispatcher.is_closed()
    }

    pub async fn drain(&self) -> Result<(), TaskOutcomeError> {
        self.dispatcher.drain().await
    }

    pub async fn close_and_drain(&self) -> Result<(), TaskOutcomeError> {
        self.close();
        self.drain().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, Wake, Waker};

    use pqueue_core::{QueueId, TenantId};

    use super::*;
    use crate::{
        OwnedTask, OwnedTaskFactory, QueueKey, TaskOutcome, TaskOutcomeSender, UnifiedAtomicCommit,
        UnifiedAtomicCommitter, task_outcome_channel,
    };

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    struct TaskSlot<T> {
        task: OwnedTask<T>,
        outcome: TaskOutcomeSender<T>,
    }

    struct DispatchState<T> {
        closed: bool,
        capacity: usize,
        next_id: u64,
        accepted: usize,
        live: HashSet<u64>,
        ready: VecDeque<u64>,
        tasks: HashMap<u64, TaskSlot<T>>,
        drainers: Vec<TaskOutcomeSender<()>>,
    }

    struct DispatchInner<T> {
        state: Mutex<DispatchState<T>>,
    }

    #[derive(Clone)]
    struct ControlledDispatcher<T> {
        inner: Arc<DispatchInner<T>>,
    }

    struct TaskWake<T> {
        id: u64,
        dispatcher: Weak<DispatchInner<T>>,
    }

    impl<T: Send + 'static> Wake for TaskWake<T> {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if let Some(dispatcher) = self.dispatcher.upgrade() {
                let mut state = dispatcher.state.lock().unwrap();
                if state.live.contains(&self.id) {
                    state.ready.push_back(self.id);
                }
            }
        }
    }

    impl<T: Send + 'static> ControlledDispatcher<T> {
        fn new(capacity: usize) -> Self {
            Self {
                inner: Arc::new(DispatchInner {
                    state: Mutex::new(DispatchState {
                        closed: false,
                        capacity,
                        next_id: 0,
                        accepted: 0,
                        live: HashSet::new(),
                        ready: VecDeque::new(),
                        tasks: HashMap::new(),
                        drainers: Vec::new(),
                    }),
                }),
            }
        }

        fn accepted(&self) -> usize {
            self.inner.state.lock().unwrap().accepted
        }

        fn drive_next(&self) -> bool {
            let (id, mut slot) = loop {
                let candidate = {
                    let mut state = self.inner.state.lock().unwrap();
                    state
                        .ready
                        .pop_front()
                        .and_then(|id| state.tasks.remove(&id).map(|slot| (id, slot)))
                };
                if let Some(candidate) = candidate {
                    break candidate;
                }
                if self.inner.state.lock().unwrap().ready.is_empty() {
                    return false;
                }
            };
            let waker = Waker::from(Arc::new(TaskWake {
                id,
                dispatcher: Arc::downgrade(&self.inner),
            }));
            match slot.task.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    self.inner.state.lock().unwrap().tasks.insert(id, slot);
                }
                Poll::Ready(value) => {
                    let drainers = {
                        let mut state = self.inner.state.lock().unwrap();
                        state.live.remove(&id);
                        state.accepted -= 1;
                        if state.closed && state.accepted == 0 {
                            std::mem::take(&mut state.drainers)
                        } else {
                            Vec::new()
                        }
                    };
                    slot.outcome.send(value);
                    for drainer in drainers {
                        drainer.send(());
                    }
                }
            }
            true
        }
    }

    impl<T: Send + 'static> OwnedTaskDispatcher<T> for ControlledDispatcher<T> {
        fn submit(&self, factory: OwnedTaskFactory<T>) -> Result<TaskOutcome<T>, DispatchError> {
            let id = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed {
                    return Err(DispatchError::Closed);
                }
                if state.accepted >= state.capacity {
                    return Err(DispatchError::AtCapacity);
                }
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.accepted += 1;
                state.live.insert(id);
                id
            };
            let task = factory();
            let (outcome_sender, outcome) = task_outcome_channel();
            let mut state = self.inner.state.lock().unwrap();
            state.tasks.insert(
                id,
                TaskSlot {
                    task,
                    outcome: outcome_sender,
                },
            );
            state.ready.push_back(id);
            Ok(outcome)
        }

        fn close(&self) {
            let drainers = {
                let mut state = self.inner.state.lock().unwrap();
                state.closed = true;
                if state.accepted == 0 {
                    std::mem::take(&mut state.drainers)
                } else {
                    Vec::new()
                }
            };
            for drainer in drainers {
                drainer.send(());
            }
        }

        fn is_closed(&self) -> bool {
            self.inner.state.lock().unwrap().closed
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome) = task_outcome_channel();
            let immediate = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed && state.accepted == 0 {
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

    struct Phase {
        started: AtomicBool,
        released: AtomicBool,
        waker: Mutex<Option<Waker>>,
    }

    impl Phase {
        fn new(released: bool) -> Arc<Self> {
            Arc::new(Self {
                started: AtomicBool::new(false),
                released: AtomicBool::new(released),
                waker: Mutex::new(None),
            })
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    #[derive(Clone)]
    struct ControlledCommitter {
        constructed: Arc<AtomicUsize>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
    }

    impl UnifiedAtomicCommitter for ControlledCommitter {
        type Request = RawCommitRequest;
        type Output = QueueKey;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            self.constructed.fetch_add(1, Ordering::AcqRel);
            let key = request.shard().clone();
            let phase = Arc::clone(self.phases.lock().unwrap().get(&key).unwrap());
            let completed = Arc::clone(&self.completed);
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    completed.lock().unwrap().push(key.clone());
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    fn queue(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn request(name: &str) -> RawCommitRequest {
        RawCommitRequest::new(queue(name), Vec::new(), 1)
    }

    type TestBackend = AsyncComposedBackend<
        UnifiedAtomicCommit<ControlledCommitter>,
        ControlledDispatcher<QueueKey>,
    >;

    struct Fixture {
        backend: TestBackend,
        dispatcher: ControlledDispatcher<QueueKey>,
        constructed: Arc<AtomicUsize>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
    }

    fn fixture(capacity: usize) -> Fixture {
        let constructed = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(Mutex::new(Vec::new()));
        let phases = Arc::new(Mutex::new(HashMap::new()));
        let committer = ControlledCommitter {
            constructed: Arc::clone(&constructed),
            phases: Arc::clone(&phases),
            completed: Arc::clone(&completed),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        Fixture {
            backend: AsyncComposedBackend::new(strategy, dispatcher.clone(), 8),
            dispatcher,
            constructed,
            completed,
            phases,
        }
    }

    fn planned_operation(
        key: QueueKey,
        phase: Arc<Phase>,
        planned: Arc<Mutex<Vec<QueueKey>>>,
        active_planners: Arc<AtomicUsize>,
        max_active_planners: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> OwnedTask<QueueKey> + Send + 'static {
        move || {
            planned.lock().unwrap().push(key.clone());
            let active = active_planners.fetch_add(1, Ordering::AcqRel) + 1;
            max_active_planners.fetch_max(active, Ordering::AcqRel);
            let mut done = false;
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    if !done {
                        done = true;
                        active_planners.fetch_sub(1, Ordering::AcqRel);
                        finished.fetch_add(1, Ordering::AcqRel);
                    }
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    #[test]
    fn same_queue_operation_planning_starts_only_after_predecessor_releases() {
        let fixture = fixture(2);
        let first_phase = Phase::new(false);
        let second_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut first = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&first_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut second = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                second_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().as_slice(), &[queue("q")]);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(first_phase.started.load(Ordering::Acquire));
        assert_eq!(planned.lock().unwrap().len(), 1);

        first_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert_eq!(max_active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn different_queue_operation_planning_progresses_while_first_is_pending() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        let ready_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut stalled = Box::pin(fixture.backend.submit_operation(
            queue("a"),
            planned_operation(
                queue("a"),
                Arc::clone(&stalled_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut unrelated = Box::pin(fixture.backend.submit_operation(
            queue("b"),
            planned_operation(
                queue("b"),
                ready_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(max_active.load(Ordering::Acquire), 2);
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(unrelated.as_mut()),
            Poll::Ready(Ok(key)) if key == queue("b")
        ));
        assert_eq!(finished.load(Ordering::Acquire), 1);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn rejected_and_closed_operation_submission_never_invokes_planner() {
        let rejected = fixture(0);
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        let mut full = Box::pin(rejected.backend.submit_operation(queue("q"), move || {
            rejected_effects.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { queue("q") })
        }));
        assert!(matches!(
            poll_once(full.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));

        let closed = fixture(1);
        closed.dispatcher.close();
        let closed_effects = Arc::clone(&effects);
        let mut closed_submission =
            Box::pin(closed.backend.submit_operation(queue("q"), move || {
                closed_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("q") })
            }));
        assert!(matches!(
            poll_once(closed_submission.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(DispatchError::Closed)))
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        assert_eq!(rejected.backend.admission.entry_count(), 0);
        assert_eq!(closed.backend.admission.entry_count(), 0);
    }

    #[test]
    fn caller_cancellation_after_operation_acceptance_does_not_cancel_work() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut caller = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&phase),
                planned,
                active,
                max_active,
                Arc::clone(&finished),
            ),
        ));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn rejected_lazy_factory_has_no_construction_effects() {
        let fixture = fixture(0);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Phase::new(true));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(caller.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));
        assert_eq!(fixture.constructed.load(Ordering::Acquire), 0);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn started_caller_cancellation_does_not_cancel_accepted_task() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("q")]);
    }

    #[test]
    fn stalled_queue_does_not_block_actual_unrelated_queue_progress() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("a"), Arc::clone(&stalled_phase));
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("b"), Phase::new(true));
        let mut stalled = Box::pin(fixture.backend.submit_commit(request("a")));
        let mut unrelated = Box::pin(fixture.backend.submit_commit(request("b")));
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Ready(Ok(key)) if key == queue("b")));
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("b")]);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn submit_close_is_linearizable_and_drain_waits_for_pending_task() {
        let fixture = fixture(2);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        fixture.backend.close();
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        assert!(matches!(
            fixture.dispatcher.submit(Box::new(move || {
                rejected_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("never") })
            })),
            Err(DispatchError::Closed)
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        let mut drain = Box::pin(fixture.backend.drain());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Pending));
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Ready(Ok(()))));
        assert_eq!(fixture.dispatcher.accepted(), 0);
    }
}
