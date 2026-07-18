//! Runtime-neutral commit strategy, owned-task dispatch, and keyed admission primitives (ADR-017).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::DurabilityClass;

/// The construction-time commit mechanism. Durability metadata never selects this implicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStrategyKind {
    UnifiedAtomic,
    SeparateReplay,
}

/// A validated commit strategy capability supplied to a future async composition.
pub trait CommitStrategy: Send + Sync + 'static {
    fn kind(&self) -> CommitStrategyKind;
    fn durability_class(&self) -> DurabilityClass;
}

/// The only commit operation exposed to composition. Both request and returned work are owned.
pub trait AsyncCommitStrategy: CommitStrategy {
    type Request: Send + 'static;
    type Output: Send + 'static;

    fn commit(&self, request: Self::Request) -> OwnedTask<Self::Output>;
}

/// Profile-specific capability for a substrate transaction spanning every atomic commit effect.
pub trait UnifiedAtomicCommitter: Send + Sync + 'static {
    type Request: Send + 'static;
    type Output: Send + 'static;

    fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output>;
}

/// Profile-specific capability for durable append followed by replayable projection repair.
pub trait SeparateReplayCommitter: Send + Sync + 'static {
    type Request: Send + 'static;
    type Output: Send + 'static;

    fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output>;
}

/// Construction failure for a strategy/profile combination that cannot uphold its durability contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCommitStrategy {
    requested: CommitStrategyKind,
    durability: DurabilityClass,
}

impl InvalidCommitStrategy {
    pub fn requested(&self) -> CommitStrategyKind {
        self.requested
    }

    pub fn durability_class(&self) -> DurabilityClass {
        self.durability
    }
}

impl fmt::Display for InvalidCommitStrategy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "commit strategy {:?} is invalid for {:?}",
            self.requested, self.durability
        )
    }
}

impl std::error::Error for InvalidCommitStrategy {}

/// Proof that one substrate transaction owns log append, projection apply, frontier, and replay outcome.
#[derive(Debug, Clone)]
pub struct UnifiedAtomicCommit<C>
where
    C: UnifiedAtomicCommitter,
{
    committer: C,
}

impl<C> UnifiedAtomicCommit<C>
where
    C: UnifiedAtomicCommitter,
{
    pub fn for_profile(
        durability: DurabilityClass,
        committer: C,
    ) -> Result<Self, InvalidCommitStrategy> {
        if durability != DurabilityClass::Atomic {
            return Err(InvalidCommitStrategy {
                requested: CommitStrategyKind::UnifiedAtomic,
                durability,
            });
        }
        Ok(Self { committer })
    }
}

impl<C> CommitStrategy for UnifiedAtomicCommit<C>
where
    C: UnifiedAtomicCommitter,
{
    fn kind(&self) -> CommitStrategyKind {
        CommitStrategyKind::UnifiedAtomic
    }

    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }
}

impl<C> AsyncCommitStrategy for UnifiedAtomicCommit<C>
where
    C: UnifiedAtomicCommitter,
{
    type Request = C::Request;
    type Output = C::Output;

    fn commit(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        self.committer.commit_atomic(request)
    }
}

/// Proof that durable append precedes replayable projection repair and the response barrier.
#[derive(Debug, Clone)]
pub struct SeparateReplayCommit<C>
where
    C: SeparateReplayCommitter,
{
    committer: C,
}

impl<C> SeparateReplayCommit<C>
where
    C: SeparateReplayCommitter,
{
    pub fn for_profile(
        durability: DurabilityClass,
        committer: C,
    ) -> Result<Self, InvalidCommitStrategy> {
        if durability != DurabilityClass::EventualApply {
            return Err(InvalidCommitStrategy {
                requested: CommitStrategyKind::SeparateReplay,
                durability,
            });
        }
        Ok(Self { committer })
    }
}

impl<C> CommitStrategy for SeparateReplayCommit<C>
where
    C: SeparateReplayCommitter,
{
    fn kind(&self) -> CommitStrategyKind {
        CommitStrategyKind::SeparateReplay
    }

    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }
}

impl<C> AsyncCommitStrategy for SeparateReplayCommit<C>
where
    C: SeparateReplayCommitter,
{
    type Request = C::Request;
    type Output = C::Output;

    fn commit(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        self.committer.commit_replayable(request)
    }
}

/// A task whose request data and commit capability are fully owned by the dispatcher after submission.
pub type OwnedTask<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Lazy construction boundary invoked only after dispatcher acceptance linearizes.
pub type OwnedTaskFactory<T> = Box<dyn FnOnce() -> OwnedTask<T> + Send + 'static>;

/// Submission rejection. Rejection happens before ownership is accepted; submitted work is never cancelled
/// by dropping its outcome receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchError {
    Closed,
    AtCapacity,
}

/// Outcome-channel failure means the dispatcher violated its ownership contract or terminated unexpectedly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcomeError {
    DispatcherDropped,
}

struct OutcomeState<T> {
    value: Option<Result<T, TaskOutcomeError>>,
    waker: Option<Waker>,
}

/// Sender retained by backend-owned execution after submission.
pub struct TaskOutcomeSender<T> {
    state: Option<Arc<Mutex<OutcomeState<T>>>>,
}

impl<T> TaskOutcomeSender<T> {
    pub fn send(mut self, value: T) {
        let Some(state) = self.state.take() else {
            return;
        };
        let waker = {
            let mut state = state.lock().expect("task outcome poisoned");
            state.value = Some(Ok(value));
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for TaskOutcomeSender<T> {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let waker = {
            let mut state = state.lock().expect("task outcome poisoned");
            if state.value.is_none() {
                state.value = Some(Err(TaskOutcomeError::DispatcherDropped));
            }
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// The caller-facing response wait. Dropping it never reaches or cancels the submitted [`OwnedTask`].
pub struct TaskOutcome<T> {
    state: Arc<Mutex<OutcomeState<T>>>,
}

impl<T> Future for TaskOutcome<T> {
    type Output = Result<T, TaskOutcomeError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.lock().expect("task outcome poisoned");
        if let Some(value) = state.value.take() {
            Poll::Ready(value)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

/// Create the result channel used by a dispatcher after it accepts ownership.
pub fn task_outcome_channel<T>() -> (TaskOutcomeSender<T>, TaskOutcome<T>) {
    let state = Arc::new(Mutex::new(OutcomeState {
        value: None,
        waker: None,
    }));
    (
        TaskOutcomeSender {
            state: Some(Arc::clone(&state)),
        },
        TaskOutcome { state },
    )
}

/// Runtime-neutral owned-task submission. Implementations supply their executor/actor below this boundary.
///
/// The generic submission method intentionally makes this a static-dispatch capability. One dispatcher
/// can accept the heterogeneous result types of typed queue operations without exposing type erasure at
/// the storage boundary.
pub trait OwnedTaskDispatcher: Send + Sync {
    /// Accept `factory` or reject it without invoking it. After acceptance the dispatcher invokes the
    /// factory exactly once and drives its task to one resolution.
    fn submit<T: Send + 'static>(
        &self,
        factory: OwnedTaskFactory<T>,
    ) -> Result<TaskOutcome<T>, DispatchError>;

    /// Stop accepting new tasks. Already submitted work remains owned and must be drained by the adapter.
    fn close(&self);

    fn is_closed(&self) -> bool;

    /// Resolve after close and after every accepted task has completed.
    fn drain(&self) -> TaskOutcome<()>;
}

/// Keyed admission failure before work is submitted to an owned-task dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGateError {
    Closed,
    QueueFull,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WaiterStatus {
    Waiting,
    Granted,
    Closed,
}

struct Waiter {
    status: WaiterStatus,
    waker: Option<Waker>,
}

struct QueueEntry {
    waiters: VecDeque<u64>,
}

struct GateState<K> {
    closed: bool,
    queued: usize,
    next_waiter_id: u64,
    entries: HashMap<K, QueueEntry>,
    waiters: HashMap<u64, Waiter>,
}

struct GateInner<K> {
    max_queued: usize,
    state: Mutex<GateState<K>>,
}

/// Cancellation-safe bounded queue-local serialization.
///
/// Entries exist only while a permit or waiter references the key, providing weak/LRU-style reclamation
/// without one permanent task, connection, or loop per queue. The central mutex protects bookkeeping only;
/// no guard is stored in a future or permit and no guard crosses an await.
pub struct KeyedQueueGate<K> {
    inner: Arc<GateInner<K>>,
}

impl<K> Clone for KeyedQueueGate<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K> KeyedQueueGate<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    pub fn new(max_queued: usize) -> Self {
        Self {
            inner: Arc::new(GateInner {
                max_queued,
                state: Mutex::new(GateState {
                    closed: false,
                    queued: 0,
                    next_waiter_id: 0,
                    entries: HashMap::new(),
                    waiters: HashMap::new(),
                }),
            }),
        }
    }

    /// Admission is registered on first poll. Dropping an unpolled future has no effect.
    pub fn acquire(&self, key: K) -> QueueGateAcquire<K> {
        QueueGateAcquire {
            inner: Arc::clone(&self.inner),
            key,
            waiter_id: None,
            acquired: false,
            completed: false,
        }
    }

    /// Close admission and cancel every queued (not yet granted) waiter. Active permits drain normally.
    pub fn close(&self) {
        let wakers = {
            let mut state = self.inner.state.lock().expect("queue gate poisoned");
            state.closed = true;
            state.queued = 0;
            let mut wakers = Vec::new();
            let waiter_ids: Vec<u64> = state
                .entries
                .values_mut()
                .flat_map(|entry| entry.waiters.drain(..))
                .collect();
            for waiter_id in waiter_ids {
                if let Some(waiter) = state.waiters.get_mut(&waiter_id) {
                    waiter.status = WaiterStatus::Closed;
                    if let Some(waker) = waiter.waker.take() {
                        wakers.push(waker);
                    }
                }
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().expect("queue gate poisoned").closed
    }

    pub fn queued(&self) -> usize {
        self.inner.state.lock().expect("queue gate poisoned").queued
    }

    pub fn entry_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("queue gate poisoned")
            .entries
            .len()
    }
}

/// Future waiting for one queue-local permit.
pub struct QueueGateAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<GateInner<K>>,
    key: K,
    waiter_id: Option<u64>,
    acquired: bool,
    completed: bool,
}

impl<K> Unpin for QueueGateAcquire<K> where K: Clone + Eq + Hash + Send + 'static {}

impl<K> Future for QueueGateAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    type Output = Result<QueueGatePermit<K>, QueueGateError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            panic!("queue gate acquire polled after completion");
        }

        if let Some(waiter_id) = self.waiter_id {
            enum WaitResult {
                Waiting,
                Granted,
                Closed,
            }
            let result = {
                let mut state = self.inner.state.lock().expect("queue gate poisoned");
                let waiter = state
                    .waiters
                    .get_mut(&waiter_id)
                    .expect("registered queue waiter missing");
                match waiter.status {
                    WaiterStatus::Waiting => {
                        waiter.waker = Some(context.waker().clone());
                        WaitResult::Waiting
                    }
                    WaiterStatus::Granted => {
                        state.waiters.remove(&waiter_id);
                        WaitResult::Granted
                    }
                    WaiterStatus::Closed => {
                        state.waiters.remove(&waiter_id);
                        WaitResult::Closed
                    }
                }
            };
            return match result {
                WaitResult::Waiting => Poll::Pending,
                WaitResult::Granted => {
                    self.acquired = true;
                    self.completed = true;
                    Poll::Ready(Ok(QueueGatePermit {
                        inner: Arc::clone(&self.inner),
                        key: self.key.clone(),
                        released: false,
                    }))
                }
                WaitResult::Closed => {
                    self.completed = true;
                    Poll::Ready(Err(QueueGateError::Closed))
                }
            };
        }

        enum Registration {
            Closed,
            Acquired,
            Full,
            Queued(u64),
        }

        let registration = {
            let mut state = self.inner.state.lock().expect("queue gate poisoned");
            if state.closed {
                Registration::Closed
            } else {
                let immediately_available = !state.entries.contains_key(&self.key);
                if immediately_available {
                    state.entries.insert(
                        self.key.clone(),
                        QueueEntry {
                            waiters: VecDeque::new(),
                        },
                    );
                    Registration::Acquired
                } else if state.queued >= self.inner.max_queued {
                    Registration::Full
                } else {
                    let id = state.next_waiter_id;
                    state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                    state.waiters.insert(
                        id,
                        Waiter {
                            status: WaiterStatus::Waiting,
                            waker: Some(context.waker().clone()),
                        },
                    );
                    state
                        .entries
                        .get_mut(&self.key)
                        .expect("held queue entry missing")
                        .waiters
                        .push_back(id);
                    state.queued += 1;
                    Registration::Queued(id)
                }
            }
        };
        match registration {
            Registration::Closed => {
                self.completed = true;
                Poll::Ready(Err(QueueGateError::Closed))
            }
            Registration::Acquired => {
                self.acquired = true;
                self.completed = true;
                Poll::Ready(Ok(QueueGatePermit {
                    inner: Arc::clone(&self.inner),
                    key: self.key.clone(),
                    released: false,
                }))
            }
            Registration::Full => {
                self.completed = true;
                Poll::Ready(Err(QueueGateError::QueueFull))
            }
            Registration::Queued(waiter_id) => {
                self.waiter_id = Some(waiter_id);
                Poll::Pending
            }
        }
    }
}

impl<K> Drop for QueueGateAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn drop(&mut self) {
        if self.acquired || self.completed && self.waiter_id.is_none() {
            return;
        }
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };
        let waker = {
            let mut state = self.inner.state.lock().expect("queue gate poisoned");
            let Some(waiter) = state.waiters.remove(&waiter_id) else {
                return;
            };
            match waiter.status {
                WaiterStatus::Waiting => {
                    if let Some(entry) = state.entries.get_mut(&self.key)
                        && let Some(index) = entry.waiters.iter().position(|id| *id == waiter_id)
                    {
                        entry.waiters.remove(index);
                        state.queued = state.queued.saturating_sub(1);
                    }
                    None
                }
                WaiterStatus::Granted => release_key_locked(&mut state, &self.key),
                WaiterStatus::Closed => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Queue-local serialization permit. Release is synchronous and never holds bookkeeping across an await.
pub struct QueueGatePermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<GateInner<K>>,
    key: K,
    released: bool,
}

impl<K> Drop for QueueGatePermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            release_key(&self.inner, &self.key);
        }
    }
}

fn release_key<K>(inner: &Arc<GateInner<K>>, key: &K)
where
    K: Clone + Eq + Hash + Send + 'static,
{
    let waker = {
        let mut state = inner.state.lock().expect("queue gate poisoned");
        release_key_locked(&mut state, key)
    };
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn release_key_locked<K>(state: &mut GateState<K>, key: &K) -> Option<Waker>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    let next_waiter = state
        .entries
        .get_mut(key)
        .and_then(|entry| entry.waiters.pop_front());
    if let Some(waiter_id) = next_waiter {
        state.queued = state.queued.saturating_sub(1);
        let waiter = state
            .waiters
            .get_mut(&waiter_id)
            .expect("queued waiter state missing");
        waiter.status = WaiterStatus::Granted;
        waiter.waker.take()
    } else {
        state.entries.remove(key);
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        future.poll_unpin(&mut Context::from_waker(Waker::noop()))
    }

    trait PollUnpin: Future + Unpin {
        fn poll_unpin(&mut self, context: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(self).poll(context)
        }
    }
    impl<F: Future + Unpin> PollUnpin for F {}

    fn assert_send<T: Send>(_: T) {}

    #[derive(Clone)]
    struct ControlledAtomicCommitter {
        calls: Arc<AtomicUsize>,
    }

    impl UnifiedAtomicCommitter for ControlledAtomicCommitter {
        type Request = usize;
        type Output = usize;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::AcqRel);
                request + 1
            })
        }
    }

    #[derive(Clone)]
    struct ControlledReplayCommitter {
        calls: Arc<AtomicUsize>,
    }

    impl SeparateReplayCommitter for ControlledReplayCommitter {
        type Request = usize;
        type Output = usize;

        fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            let calls = Arc::clone(&self.calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::AcqRel);
                request + 2
            })
        }
    }

    #[test]
    fn strategy_construction_rejects_atomic_separate_fallthrough() {
        let atomic_calls = Arc::new(AtomicUsize::new(0));
        let replay_calls = Arc::new(AtomicUsize::new(0));
        let atomic_committer = ControlledAtomicCommitter {
            calls: Arc::clone(&atomic_calls),
        };
        let replay_committer = ControlledReplayCommitter {
            calls: Arc::clone(&replay_calls),
        };
        let atomic =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, atomic_committer.clone())
                .unwrap();
        assert_eq!(atomic.kind(), CommitStrategyKind::UnifiedAtomic);
        assert_eq!(atomic.durability_class(), DurabilityClass::Atomic);
        let invalid =
            SeparateReplayCommit::for_profile(DurabilityClass::Atomic, replay_committer.clone());
        assert!(matches!(
            invalid,
            Err(InvalidCommitStrategy {
                requested: CommitStrategyKind::SeparateReplay,
                ..
            })
        ));
        let replay =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, replay_committer)
                .unwrap();
        assert_eq!(replay.kind(), CommitStrategyKind::SeparateReplay);
        assert!(
            UnifiedAtomicCommit::for_profile(DurabilityClass::EventualApply, atomic_committer)
                .is_err()
        );

        let mut atomic_work = atomic.commit(40);
        let mut replay_work = replay.commit(40);
        assert!(matches!(poll_once(&mut atomic_work), Poll::Ready(41)));
        assert!(matches!(poll_once(&mut replay_work), Poll::Ready(42)));
        assert_eq!(atomic_calls.load(Ordering::Acquire), 1);
        assert_eq!(replay_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn same_queue_serializes_and_unrelated_queue_progresses() {
        let gate = KeyedQueueGate::new(8);
        let mut first = gate.acquire("a");
        let permit_a = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first permit not ready"),
        };
        let mut second = gate.acquire("a");
        assert!(matches!(poll_once(&mut second), Poll::Pending));
        let mut unrelated = gate.acquire("b");
        let permit_b = match poll_once(&mut unrelated) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("unrelated permit not ready"),
        };
        drop(permit_b);
        drop(permit_a);
        assert!(matches!(poll_once(&mut second), Poll::Ready(Ok(_))));
    }

    #[test]
    fn queued_cancellation_and_granted_cancellation_do_not_strand_permits() {
        let gate = KeyedQueueGate::new(2);
        let mut first = gate.acquire("q");
        let permit = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first permit not ready"),
        };
        let mut cancelled = gate.acquire("q");
        assert!(matches!(poll_once(&mut cancelled), Poll::Pending));
        assert_eq!(gate.queued(), 1);
        drop(cancelled);
        assert_eq!(gate.queued(), 0);

        let mut granted_then_dropped = gate.acquire("q");
        assert!(matches!(
            poll_once(&mut granted_then_dropped),
            Poll::Pending
        ));
        drop(permit);
        drop(granted_then_dropped);
        let mut successor = gate.acquire("q");
        assert!(matches!(poll_once(&mut successor), Poll::Ready(Ok(_))));
    }

    #[test]
    fn gate_reclaims_entries_and_closes_queued_admission() {
        let gate = KeyedQueueGate::new(2);
        let mut first = gate.acquire("q");
        let permit = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first permit not ready"),
        };
        let mut waiter = gate.acquire("q");
        assert!(matches!(poll_once(&mut waiter), Poll::Pending));
        assert_send(gate.acquire("other"));
        gate.close();
        assert!(gate.is_closed());
        assert!(matches!(
            poll_once(&mut waiter),
            Poll::Ready(Err(QueueGateError::Closed))
        ));
        drop(permit);
        assert_eq!(gate.entry_count(), 0);
        let mut rejected = gate.acquire("new");
        assert!(matches!(
            poll_once(&mut rejected),
            Poll::Ready(Err(QueueGateError::Closed))
        ));
    }

    #[test]
    fn bounded_waiter_admission_rejects_without_consuming_running_permits() {
        let gate = KeyedQueueGate::new(1);
        let mut first = gate.acquire("q");
        let _permit = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first permit not ready"),
        };
        let mut queued = gate.acquire("q");
        assert!(matches!(poll_once(&mut queued), Poll::Pending));
        let mut full = gate.acquire("q");
        assert!(matches!(
            poll_once(&mut full),
            Poll::Ready(Err(QueueGateError::QueueFull))
        ));
        let mut unrelated = gate.acquire("other");
        assert!(matches!(poll_once(&mut unrelated), Poll::Ready(Ok(_))));
    }

    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn releasing_permit_actually_wakes_registered_waiter() {
        let gate = KeyedQueueGate::new(1);
        let mut first = gate.acquire("q");
        let permit = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first permit not ready"),
        };
        let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut waiter = gate.acquire("q");
        assert!(matches!(
            Pin::new(&mut waiter).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        drop(permit);
        assert_eq!(wake.0.load(Ordering::Acquire), 1);
        assert!(matches!(poll_once(&mut waiter), Poll::Ready(Ok(_))));
    }

    #[test]
    fn grant_cancel_race_never_duplicates_or_strands_queue_permit() {
        for _ in 0..250 {
            let gate = KeyedQueueGate::new(1);
            let mut first = gate.acquire("q");
            let permit = match poll_once(&mut first) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("first permit not ready"),
            };
            let mut waiter = gate.acquire("q");
            assert!(matches!(poll_once(&mut waiter), Poll::Pending));
            let barrier = Arc::new(std::sync::Barrier::new(3));
            std::thread::scope(|scope| {
                let release_barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    release_barrier.wait();
                    drop(permit);
                });
                let cancel_barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    cancel_barrier.wait();
                    drop(waiter);
                });
                barrier.wait();
            });
            let mut successor = gate.acquire("q");
            let successor = match poll_once(&mut successor) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("grant/cancel race stranded queue permit"),
            };
            drop(successor);
            assert_eq!(gate.queued(), 0);
            assert_eq!(gate.entry_count(), 0);
        }
    }
}
