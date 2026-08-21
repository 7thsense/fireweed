//! Inert, runtime-neutral coordination primitives for derived lifecycle microbatching.
//!
//! These types bound retained requests and resource predecessors without changing any production
//! serving path. Later slices attach planning, committed reads, and the selection fence to them.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::command::{
    QueueCommand, SelectionFenceDisposition, selection_fence_disposition,
    selection_fence_disposition_for_commands,
};
use crate::commit::{AppendAdmissionClass, RawCommitRequest};
use crate::{KeyedQueueGate, QueueGateAcquire, QueueGateError, QueueGatePermit};

pub const CLAIM_MAX_CALLERS: usize = 1_024;
pub const CLAIM_MAX_DRIVERS: usize = 8;
pub const CLAIM_GENERATION_MAX_REQUESTS: usize = 8;
pub const GENERATION_MAX_ITEMS: usize = 800;
pub const GENERATION_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const MUTATION_MAX_GENERATIONS_PER_QUEUE: usize = 2;
pub const MUTATION_MAX_REQUESTS_PER_QUEUE: usize = 16;
pub const CLAIM_TURN_DEFAULT_MAX_WAIT: Duration = Duration::from_secs(255);
pub const DRIVER_SLOT_DEFAULT_MAX_WAIT: Duration = Duration::from_secs(95);
pub const OUTCOME_SLOT_DEFAULT_MAX_WAIT: Duration = Duration::from_secs(10);

pub const CLAIM_COORDINATOR_WAITERS_RESOURCE: &str = "claim coordinator waiters";
pub const CLAIM_DRIVER_INGRESS_RESOURCE: &str = "claim driver ingress";
pub const CLAIM_QUEUE_TURN_RESOURCE: &str = "claim queue turn";
pub const CLAIM_DRIVER_SLOTS_RESOURCE: &str = "claim driver read slots";
pub const SHARED_DRIVER_SLOTS_RESOURCE: &str = "shared driver read slots";
pub const OUTCOME_READ_SLOTS_RESOURCE: &str = "committed outcome read slots";
pub const MUTATION_SEQUENCER_RESOURCE: &str = "mutation sequencer capacity";
pub const SELECTION_FENCE_WAITERS_RESOURCE: &str = "selection fence waiters";

/// Compatible microbatch overlays currently have exactly the two reviewed FIFO shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationGenerationKind {
    Push,
    BatchUpdate,
}

/// Whether one command joins a compatible mutation generation, owns a singleton generation, or does
/// not mutate candidate selection and therefore never joins the mutation sequencer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationGenerationDisposition {
    Compatible(MutationGenerationKind),
    Singleton,
    NotCandidateMutating,
}

/// Classify mutation-generation membership with no wildcard command arms.
pub fn mutation_generation_disposition(command: &QueueCommand) -> MutationGenerationDisposition {
    use MutationGenerationDisposition::{Compatible, NotCandidateMutating, Singleton};

    match command {
        QueueCommand::CreateQueue(_) => Singleton,
        QueueCommand::Push(_) => Compatible(MutationGenerationKind::Push),
        QueueCommand::Claim(_) => NotCandidateMutating,
        QueueCommand::CohortClaim(_) => NotCandidateMutating,
        QueueCommand::RenewLease(_) => NotCandidateMutating,
        QueueCommand::CohortRenewLease(_) => NotCandidateMutating,
        QueueCommand::ReassignLease(_) => NotCandidateMutating,
        QueueCommand::Finalize(_) => match selection_fence_disposition(command) {
            SelectionFenceDisposition::Shared => Singleton,
            SelectionFenceDisposition::Bypass => NotCandidateMutating,
            SelectionFenceDisposition::Exclusive => NotCandidateMutating,
        },
        QueueCommand::CohortFinalize(_) => match selection_fence_disposition(command) {
            SelectionFenceDisposition::Shared => Singleton,
            SelectionFenceDisposition::Bypass => NotCandidateMutating,
            SelectionFenceDisposition::Exclusive => NotCandidateMutating,
        },
        QueueCommand::ReplacePending(_) => Singleton,
        QueueCommand::UpdateFields(_) => Singleton,
        QueueCommand::UpdateFieldsBatch(_) => Compatible(MutationGenerationKind::BatchUpdate),
        QueueCommand::MutateItems(_) => Singleton,
        QueueCommand::LeaseExpired(_) => Singleton,
        QueueCommand::CohortExpired(_) => Singleton,
        QueueCommand::FenceLease(_) => Singleton,
        QueueCommand::UnfenceLease(_) => Singleton,
        QueueCommand::PauseQueue(_) => Singleton,
        QueueCommand::ResumeQueue => Singleton,
        QueueCommand::PurgeItems(_) => Singleton,
        QueueCommand::SetGates(_) => Singleton,
        QueueCommand::WriteSideRecords(_) => NotCandidateMutating,
        QueueCommand::AdvanceInstanceFence(_) => NotCandidateMutating,
    }
}

/// Validate the one-admission invariant for an inert derived append classification.
///
/// `Some(1)` is one reviewed non-bypass admission, `Some(0)` is an allowed bypass/non-serving class,
/// and `None` is a carrier/disposition mismatch. No permit or fence is acquired here.
pub fn audited_append_admission_count(
    append_admission: AppendAdmissionClass,
    disposition: SelectionFenceDisposition,
) -> Option<usize> {
    match append_admission {
        AppendAdmissionClass::NonDerived
        | AppendAdmissionClass::AtomicNative
        | AppendAdmissionClass::RecoveryOnly => Some(0),
        AppendAdmissionClass::KeyedPermitLive => Some(usize::from(
            disposition != SelectionFenceDisposition::Bypass,
        )),
        AppendAdmissionClass::SelectionRequired => match disposition {
            SelectionFenceDisposition::Shared | SelectionFenceDisposition::Exclusive => Some(1),
            SelectionFenceDisposition::Bypass => None,
        },
        AppendAdmissionClass::Bypass => match disposition {
            SelectionFenceDisposition::Bypass => Some(0),
            SelectionFenceDisposition::Shared | SelectionFenceDisposition::Exclusive => None,
        },
        AppendAdmissionClass::ClaimCoordinatorLive => match disposition {
            SelectionFenceDisposition::Exclusive => Some(1),
            SelectionFenceDisposition::Bypass | SelectionFenceDisposition::Shared => None,
        },
    }
}

/// Audit a complete sealed request against its carried append-time routing class.
pub fn audited_append_request_admission_count(request: &RawCommitRequest) -> Option<usize> {
    let disposition = selection_fence_disposition_for_commands(
        request.commands().iter().map(|envelope| &envelope.command),
    );
    audited_append_admission_count(request.append_admission(), disposition)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinationError {
    Closed { resource: &'static str },
    Capacity { resource: &'static str },
    Deadline { resource: &'static str },
}

impl CoordinationError {
    pub fn resource(self) -> &'static str {
        match self {
            Self::Closed { resource }
            | Self::Capacity { resource }
            | Self::Deadline { resource } => resource,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AdmissionWaiterStatus {
    Waiting,
    Granted,
    Closed,
}

struct AdmissionWaiter {
    status: AdmissionWaiterStatus,
    waker: Option<Waker>,
}

struct SlotAdmissionState {
    closed: bool,
    active: usize,
    queued: usize,
    next_waiter_id: u64,
    order: VecDeque<u64>,
    waiters: HashMap<u64, AdmissionWaiter>,
}

struct SlotAdmissionInner {
    resource: &'static str,
    active_limit: usize,
    queued_limit: usize,
    max_wait: Duration,
    state: Mutex<SlotAdmissionState>,
}

#[derive(Clone)]
struct SlotAdmission {
    inner: Arc<SlotAdmissionInner>,
}

impl SlotAdmission {
    fn new(
        resource: &'static str,
        active_limit: usize,
        queued_limit: usize,
        max_wait: Duration,
    ) -> Self {
        assert!(active_limit > 0, "active slot capacity must be positive");
        Self {
            inner: Arc::new(SlotAdmissionInner {
                resource,
                active_limit,
                queued_limit,
                max_wait,
                state: Mutex::new(SlotAdmissionState {
                    closed: false,
                    active: 0,
                    queued: 0,
                    next_waiter_id: 0,
                    order: VecDeque::new(),
                    waiters: HashMap::new(),
                }),
            }),
        }
    }

    fn acquire(&self) -> SlotAcquire {
        SlotAcquire {
            inner: Some(Arc::clone(&self.inner)),
            waiter_id: None,
            registered_at: None,
            completed: false,
        }
    }

    fn close(&self) {
        let wakers = {
            let mut state = self.inner.state.lock().expect("slot admission poisoned");
            state.closed = true;
            state.queued = 0;
            state.order.clear();
            let mut wakers = Vec::new();
            for waiter in state.waiters.values_mut() {
                if waiter.status == AdmissionWaiterStatus::Waiting {
                    waiter.status = AdmissionWaiterStatus::Closed;
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

    fn active(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("slot admission poisoned")
            .active
    }

    fn queued(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("slot admission poisoned")
            .queued
    }

    fn max_wait(&self) -> Duration {
        self.inner.max_wait
    }
}

pub struct SlotAcquire {
    inner: Option<Arc<SlotAdmissionInner>>,
    waiter_id: Option<u64>,
    registered_at: Option<Instant>,
    completed: bool,
}

impl Unpin for SlotAcquire {}

impl Future for SlotAcquire {
    type Output = Result<SlotPermit, CoordinationError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            panic!("slot admission polled after completion");
        }
        let inner = Arc::clone(self.inner.as_ref().expect("slot admission missing"));

        if let Some(waiter_id) = self.waiter_id {
            enum WaitResult {
                Waiting,
                Granted,
                Closed,
                Deadline,
            }
            let result = {
                let mut state = inner.state.lock().expect("slot admission poisoned");
                let timed_out = self
                    .registered_at
                    .is_some_and(|start| start.elapsed() >= inner.max_wait);
                let status = state
                    .waiters
                    .get(&waiter_id)
                    .map(|waiter| waiter.status)
                    .expect("registered slot waiter missing");
                if status == AdmissionWaiterStatus::Waiting && timed_out {
                    state.waiters.remove(&waiter_id);
                    if let Some(index) = state.order.iter().position(|id| *id == waiter_id) {
                        state.order.remove(index);
                        state.queued = state.queued.saturating_sub(1);
                    }
                    WaitResult::Deadline
                } else {
                    let waiter = state
                        .waiters
                        .get_mut(&waiter_id)
                        .expect("registered slot waiter missing");
                    match waiter.status {
                        AdmissionWaiterStatus::Waiting => {
                            waiter.waker = Some(context.waker().clone());
                            WaitResult::Waiting
                        }
                        AdmissionWaiterStatus::Granted => {
                            state.waiters.remove(&waiter_id);
                            WaitResult::Granted
                        }
                        AdmissionWaiterStatus::Closed => {
                            state.waiters.remove(&waiter_id);
                            WaitResult::Closed
                        }
                    }
                }
            };
            return match result {
                WaitResult::Waiting => Poll::Pending,
                WaitResult::Granted => {
                    self.completed = true;
                    self.inner = None;
                    Poll::Ready(Ok(SlotPermit {
                        inner,
                        released: false,
                    }))
                }
                WaitResult::Closed => {
                    self.completed = true;
                    self.inner = None;
                    Poll::Ready(Err(CoordinationError::Closed {
                        resource: inner.resource,
                    }))
                }
                WaitResult::Deadline => {
                    self.completed = true;
                    self.waiter_id = None;
                    self.inner = None;
                    Poll::Ready(Err(CoordinationError::Deadline {
                        resource: inner.resource,
                    }))
                }
            };
        }

        enum Registration {
            Closed,
            Acquired,
            Capacity,
            Queued(u64),
        }
        let registration = {
            let mut state = inner.state.lock().expect("slot admission poisoned");
            if state.closed {
                Registration::Closed
            } else if state.active < inner.active_limit {
                state.active += 1;
                Registration::Acquired
            } else if state.queued >= inner.queued_limit {
                Registration::Capacity
            } else {
                let waiter_id = state.next_waiter_id;
                state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                state.order.push_back(waiter_id);
                state.waiters.insert(
                    waiter_id,
                    AdmissionWaiter {
                        status: AdmissionWaiterStatus::Waiting,
                        waker: Some(context.waker().clone()),
                    },
                );
                state.queued += 1;
                Registration::Queued(waiter_id)
            }
        };
        match registration {
            Registration::Closed => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Err(CoordinationError::Closed {
                    resource: inner.resource,
                }))
            }
            Registration::Acquired => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Ok(SlotPermit {
                    inner,
                    released: false,
                }))
            }
            Registration::Capacity => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Err(CoordinationError::Capacity {
                    resource: inner.resource,
                }))
            }
            Registration::Queued(waiter_id) => {
                self.waiter_id = Some(waiter_id);
                self.registered_at = Some(Instant::now());
                Poll::Pending
            }
        }
    }
}

impl Drop for SlotAcquire {
    fn drop(&mut self) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };
        let wake = {
            let mut state = inner.state.lock().expect("slot admission poisoned");
            let Some(waiter) = state.waiters.remove(&waiter_id) else {
                return;
            };
            match waiter.status {
                AdmissionWaiterStatus::Waiting => {
                    if let Some(index) = state.order.iter().position(|id| *id == waiter_id) {
                        state.order.remove(index);
                        state.queued = state.queued.saturating_sub(1);
                    }
                    None
                }
                AdmissionWaiterStatus::Granted => release_slot_locked(&mut state),
                AdmissionWaiterStatus::Closed => None,
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

pub struct SlotPermit {
    inner: Arc<SlotAdmissionInner>,
    released: bool,
}

impl Drop for SlotPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let wake = {
            let mut state = self.inner.state.lock().expect("slot admission poisoned");
            release_slot_locked(&mut state)
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

fn release_slot_locked(state: &mut SlotAdmissionState) -> Option<Waker> {
    state.active = state.active.saturating_sub(1);
    let waiter_id = state.order.pop_front()?;
    state.queued = state.queued.saturating_sub(1);
    state.active += 1;
    let waiter = state
        .waiters
        .get_mut(&waiter_id)
        .expect("queued slot waiter missing");
    waiter.status = AdmissionWaiterStatus::Granted;
    waiter.waker.take()
}

macro_rules! define_slot_admission {
    ($name:ident, $active:expr, $queued:expr, $resource:expr, $default_wait:expr) => {
        #[derive(Clone)]
        pub struct $name {
            inner: SlotAdmission,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new($default_wait)
            }
        }

        impl $name {
            pub fn new(max_wait: Duration) -> Self {
                Self {
                    inner: SlotAdmission::new($resource, $active, $queued, max_wait),
                }
            }

            pub fn acquire(&self) -> SlotAcquire {
                self.inner.acquire()
            }

            pub fn close(&self) {
                self.inner.close();
            }

            pub fn active(&self) -> usize {
                self.inner.active()
            }

            pub fn queued(&self) -> usize {
                self.inner.queued()
            }

            pub fn max_wait(&self) -> Duration {
                self.inner.max_wait()
            }
        }
    };
}

define_slot_admission!(
    ClaimDriverReadAdmission,
    4,
    4,
    CLAIM_DRIVER_SLOTS_RESOURCE,
    DRIVER_SLOT_DEFAULT_MAX_WAIT
);
define_slot_admission!(
    SharedDriverReadAdmission,
    12,
    12,
    SHARED_DRIVER_SLOTS_RESOURCE,
    DRIVER_SLOT_DEFAULT_MAX_WAIT
);
define_slot_admission!(
    OutcomeReadAdmission,
    8,
    8,
    OUTCOME_READ_SLOTS_RESOURCE,
    OUTCOME_SLOT_DEFAULT_MAX_WAIT
);

#[derive(Clone)]
pub struct ClaimQueueTurn<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    gate: KeyedQueueGate<K>,
    max_wait: Duration,
}

impl<K> Default for ClaimQueueTurn<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn default() -> Self {
        Self::new(CLAIM_TURN_DEFAULT_MAX_WAIT)
    }
}

impl<K> ClaimQueueTurn<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    pub fn new(max_wait: Duration) -> Self {
        Self {
            gate: KeyedQueueGate::new_with_per_key_limit(usize::MAX, 2),
            max_wait,
        }
    }

    pub fn acquire(&self, key: K) -> ClaimQueueTurnAcquire<K> {
        ClaimQueueTurnAcquire {
            inner: Some(self.gate.acquire(key)),
            max_wait: self.max_wait,
            registered_at: None,
            completed: false,
        }
    }

    pub fn close(&self) {
        self.gate.close();
    }

    pub fn queued(&self) -> usize {
        self.gate.queued()
    }

    pub fn entry_count(&self) -> usize {
        self.gate.entry_count()
    }

    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }
}

pub struct ClaimQueueTurnAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Option<QueueGateAcquire<K>>,
    max_wait: Duration,
    registered_at: Option<Instant>,
    completed: bool,
}

impl<K> Unpin for ClaimQueueTurnAcquire<K> where K: Clone + Eq + Hash + Send + 'static {}

impl<K> Future for ClaimQueueTurnAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    type Output = Result<ClaimQueueTurnPermit<K>, CoordinationError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            panic!("claim queue turn polled after completion");
        }
        let result = Pin::new(self.inner.as_mut().expect("claim queue turn missing")).poll(context);
        match result {
            Poll::Ready(Ok(permit)) => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Ok(ClaimQueueTurnPermit { permit }))
            }
            Poll::Ready(Err(QueueGateError::Closed)) => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Err(CoordinationError::Closed {
                    resource: CLAIM_QUEUE_TURN_RESOURCE,
                }))
            }
            Poll::Ready(Err(QueueGateError::QueueFull | QueueGateError::PerKeyFull)) => {
                self.completed = true;
                self.inner = None;
                Poll::Ready(Err(CoordinationError::Capacity {
                    resource: CLAIM_QUEUE_TURN_RESOURCE,
                }))
            }
            Poll::Pending => {
                let started = *self.registered_at.get_or_insert_with(Instant::now);
                if started.elapsed() >= self.max_wait {
                    self.inner = None;
                    self.completed = true;
                    Poll::Ready(Err(CoordinationError::Deadline {
                        resource: CLAIM_QUEUE_TURN_RESOURCE,
                    }))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pub struct ClaimQueueTurnPermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    permit: QueueGatePermit<K>,
}

impl<K> ClaimQueueTurnPermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    pub fn release(self) {
        drop(self.permit);
    }
}

#[derive(Debug)]
struct ClaimEntry<R> {
    id: u64,
    request: Arc<R>,
    requested_items: usize,
    rendered_bytes: usize,
}

struct ClaimBucket<R> {
    driver_active: bool,
    pending: VecDeque<ClaimEntry<R>>,
}

struct ClaimCoordinatorState<K, R> {
    closed: bool,
    callers: usize,
    next_caller_id: u64,
    buckets: HashMap<K, ClaimBucket<R>>,
}

struct ClaimCoordinatorInner<K, R> {
    max_callers: usize,
    max_drivers: usize,
    state: Mutex<ClaimCoordinatorState<K, R>>,
}

/// Bounded compatibility buckets for item-Claim callers.
///
/// The driver budget is charged only when a new compatibility bucket is created. Additional callers
/// may attach to an existing bucket until the independent caller/channel ceiling is reached.
pub struct ClaimCoordinator<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<ClaimCoordinatorInner<K, R>>,
}

impl<K, R> Clone for ClaimCoordinator<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, R> Default for ClaimCoordinator<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new(CLAIM_MAX_CALLERS, CLAIM_MAX_DRIVERS)
    }
}

impl<K, R> ClaimCoordinator<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn new(max_callers: usize, max_drivers: usize) -> Self {
        assert!(max_callers > 0, "claim caller capacity must be positive");
        assert!(max_drivers > 0, "claim driver capacity must be positive");
        Self {
            inner: Arc::new(ClaimCoordinatorInner {
                max_callers,
                max_drivers,
                state: Mutex::new(ClaimCoordinatorState {
                    closed: false,
                    callers: 0,
                    next_caller_id: 0,
                    buckets: HashMap::new(),
                }),
            }),
        }
    }

    pub fn join(
        &self,
        key: K,
        request: Arc<R>,
        requested_items: usize,
        rendered_bytes: usize,
    ) -> Result<ClaimCaller<K, R>, CoordinationError> {
        let caller_id = {
            let mut state = self.inner.state.lock().expect("claim coordinator poisoned");
            if state.closed {
                return Err(CoordinationError::Closed {
                    resource: CLAIM_COORDINATOR_WAITERS_RESOURCE,
                });
            }
            if state.callers >= self.inner.max_callers {
                return Err(CoordinationError::Capacity {
                    resource: CLAIM_COORDINATOR_WAITERS_RESOURCE,
                });
            }
            if !state.buckets.contains_key(&key) && state.buckets.len() >= self.inner.max_drivers {
                return Err(CoordinationError::Capacity {
                    resource: CLAIM_DRIVER_INGRESS_RESOURCE,
                });
            }
            let caller_id = state.next_caller_id;
            state.next_caller_id = state.next_caller_id.wrapping_add(1);
            state.callers += 1;
            state
                .buckets
                .entry(key.clone())
                .or_insert_with(|| ClaimBucket {
                    driver_active: false,
                    pending: VecDeque::new(),
                })
                .pending
                .push_back(ClaimEntry {
                    id: caller_id,
                    request: Arc::clone(&request),
                    requested_items,
                    rendered_bytes,
                });
            caller_id
        };
        Ok(ClaimCaller {
            inner: Arc::clone(&self.inner),
            key,
            caller_id: Some(caller_id),
            request,
        })
    }

    pub fn start_driver(&self, key: &K) -> Option<ClaimDriverBatch<K, R>> {
        let entries = {
            let mut state = self.inner.state.lock().expect("claim coordinator poisoned");
            let bucket = state.buckets.get_mut(key)?;
            if bucket.driver_active || bucket.pending.is_empty() {
                return None;
            }
            bucket.driver_active = true;
            let mut entries = Vec::new();
            let mut items = 0usize;
            let mut bytes = 0usize;
            while let Some(entry) = bucket.pending.front() {
                let next_items = items.saturating_add(entry.requested_items);
                let next_bytes = bytes.saturating_add(entry.rendered_bytes);
                let fits = entries.len() < CLAIM_GENERATION_MAX_REQUESTS
                    && next_items <= GENERATION_MAX_ITEMS
                    && next_bytes <= GENERATION_MAX_RESPONSE_BYTES;
                if !fits && !entries.is_empty() {
                    break;
                }
                let entry = bucket.pending.pop_front().expect("claim entry disappeared");
                items = next_items;
                bytes = next_bytes;
                entries.push(entry);
                if entries.len() == CLAIM_GENERATION_MAX_REQUESTS {
                    break;
                }
            }
            entries
        };
        Some(ClaimDriverBatch {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            entries,
            completed: false,
        })
    }

    pub fn caller_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("claim coordinator poisoned")
            .callers
    }

    pub fn driver_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("claim coordinator poisoned")
            .buckets
            .len()
    }

    pub fn pending_for(&self, key: &K) -> usize {
        self.inner
            .state
            .lock()
            .expect("claim coordinator poisoned")
            .buckets
            .get(key)
            .map_or(0, |bucket| bucket.pending.len())
    }

    pub fn close(&self) {
        let mut state = self.inner.state.lock().expect("claim coordinator poisoned");
        state.closed = true;
        let released = state
            .buckets
            .values_mut()
            .map(|bucket| bucket.pending.drain(..).count())
            .sum::<usize>();
        state.callers = state.callers.saturating_sub(released);
        state
            .buckets
            .retain(|_, bucket| bucket.driver_active || !bucket.pending.is_empty());
    }
}

pub struct ClaimCaller<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<ClaimCoordinatorInner<K, R>>,
    key: K,
    caller_id: Option<u64>,
    request: Arc<R>,
}

impl<K, R> ClaimCaller<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn request(&self) -> &Arc<R> {
        &self.request
    }
}

impl<K, R> Drop for ClaimCaller<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let Some(caller_id) = self.caller_id.take() else {
            return;
        };
        let mut state = self.inner.state.lock().expect("claim coordinator poisoned");
        let mut removed = false;
        let mut remove_bucket = false;
        if let Some(bucket) = state.buckets.get_mut(&self.key) {
            if let Some(index) = bucket
                .pending
                .iter()
                .position(|entry| entry.id == caller_id)
            {
                bucket.pending.remove(index);
                removed = true;
            }
            remove_bucket = !bucket.driver_active && bucket.pending.is_empty();
        }
        if removed {
            state.callers = state.callers.saturating_sub(1);
        }
        if remove_bucket {
            state.buckets.remove(&self.key);
        }
    }
}

pub struct ClaimDriverBatch<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<ClaimCoordinatorInner<K, R>>,
    key: K,
    entries: Vec<ClaimEntry<R>>,
    completed: bool,
}

impl<K, R> ClaimDriverBatch<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn requested_items(&self) -> usize {
        self.entries.iter().map(|entry| entry.requested_items).sum()
    }

    pub fn rendered_bytes(&self) -> usize {
        self.entries.iter().map(|entry| entry.rendered_bytes).sum()
    }

    pub fn requests(&self) -> impl ExactSizeIterator<Item = &Arc<R>> {
        self.entries.iter().map(|entry| &entry.request)
    }

    pub fn complete(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        let mut state = self.inner.state.lock().expect("claim coordinator poisoned");
        state.callers = state.callers.saturating_sub(self.entries.len());
        let remove_bucket = if let Some(bucket) = state.buckets.get_mut(&self.key) {
            bucket.driver_active = false;
            bucket.pending.is_empty()
        } else {
            false
        };
        if remove_bucket {
            state.buckets.remove(&self.key);
        }
    }
}

impl<K, R> Drop for ClaimDriverBatch<K, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    R: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationIngress {
    Direct,
    KeyedPermitLive,
}

struct MutationEntry<R> {
    id: u64,
    request: Arc<R>,
    ingress: MutationIngress,
    items: usize,
    response_bytes: usize,
}

struct MutationGeneration<C, R> {
    id: u64,
    compatibility: C,
    active: bool,
    items: usize,
    response_bytes: usize,
    requests: VecDeque<MutationEntry<R>>,
}

struct MutationQueue<C, R> {
    generations: VecDeque<MutationGeneration<C, R>>,
}

struct MutationSequencerState<K, C, R> {
    closed: bool,
    next_request_id: u64,
    next_generation_id: u64,
    queues: HashMap<K, MutationQueue<C, R>>,
}

struct MutationSequencerInner<K, C, R> {
    state: Mutex<MutationSequencerState<K, C, R>>,
}

/// Per-queue, cross-ingress FIFO generations retaining only `Arc` request ownership.
pub struct MutationSequencer<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<MutationSequencerInner<K, C, R>>,
}

impl<K, C, R> Clone for MutationSequencer<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, C, R> Default for MutationSequencer<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, C, R> MutationSequencer<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MutationSequencerInner {
                state: Mutex::new(MutationSequencerState {
                    closed: false,
                    next_request_id: 0,
                    next_generation_id: 0,
                    queues: HashMap::new(),
                }),
            }),
        }
    }

    pub fn admit(
        &self,
        queue: K,
        compatibility: C,
        ingress: MutationIngress,
        request: Arc<R>,
        items: usize,
        response_bytes: usize,
    ) -> Result<MutationTicket<K, C, R>, CoordinationError> {
        if items > GENERATION_MAX_ITEMS || response_bytes > GENERATION_MAX_RESPONSE_BYTES {
            return Err(CoordinationError::Capacity {
                resource: MUTATION_SEQUENCER_RESOURCE,
            });
        }
        let (request_id, generation_id) = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("mutation sequencer poisoned");
            if state.closed {
                return Err(CoordinationError::Closed {
                    resource: MUTATION_SEQUENCER_RESOURCE,
                });
            }
            let request_id = state.next_request_id;
            state.next_request_id = state.next_request_id.wrapping_add(1);
            let total_requests = state.queues.get(&queue).map_or(0, |entry| {
                entry
                    .generations
                    .iter()
                    .map(|generation| generation.requests.len())
                    .sum()
            });
            if total_requests >= MUTATION_MAX_REQUESTS_PER_QUEUE {
                return Err(CoordinationError::Capacity {
                    resource: MUTATION_SEQUENCER_RESOURCE,
                });
            }

            let can_attach = state
                .queues
                .get(&queue)
                .and_then(|entry| entry.generations.back())
                .is_some_and(|generation| {
                    generation.compatibility == compatibility
                        && generation.requests.len() < CLAIM_GENERATION_MAX_REQUESTS
                        && generation.items.saturating_add(items) <= GENERATION_MAX_ITEMS
                        && generation.response_bytes.saturating_add(response_bytes)
                            <= GENERATION_MAX_RESPONSE_BYTES
                });

            let generation_id = if can_attach {
                state
                    .queues
                    .get(&queue)
                    .and_then(|entry| entry.generations.back())
                    .expect("attachable mutation generation missing")
                    .id
            } else {
                let generation_count = state
                    .queues
                    .get(&queue)
                    .map_or(0, |entry| entry.generations.len());
                if generation_count >= MUTATION_MAX_GENERATIONS_PER_QUEUE {
                    return Err(CoordinationError::Capacity {
                        resource: MUTATION_SEQUENCER_RESOURCE,
                    });
                }
                let generation_id = state.next_generation_id;
                state.next_generation_id = state.next_generation_id.wrapping_add(1);
                state
                    .queues
                    .entry(queue.clone())
                    .or_insert_with(|| MutationQueue {
                        generations: VecDeque::new(),
                    })
                    .generations
                    .push_back(MutationGeneration {
                        id: generation_id,
                        compatibility: compatibility.clone(),
                        active: false,
                        items: 0,
                        response_bytes: 0,
                        requests: VecDeque::new(),
                    });
                generation_id
            };

            let generation = state
                .queues
                .get_mut(&queue)
                .and_then(|entry| {
                    entry
                        .generations
                        .iter_mut()
                        .find(|generation| generation.id == generation_id)
                })
                .expect("mutation generation missing after admission");
            generation.items += items;
            generation.response_bytes += response_bytes;
            generation.requests.push_back(MutationEntry {
                id: request_id,
                request: Arc::clone(&request),
                ingress,
                items,
                response_bytes,
            });
            (request_id, generation_id)
        };

        Ok(MutationTicket {
            inner: Arc::clone(&self.inner),
            queue,
            request_id: Some(request_id),
            generation_id,
            request,
        })
    }

    /// Elect the front generation as owned work. Caller ticket cancellation no longer removes a
    /// generation once this method succeeds; the returned turn owns it through publication.
    pub fn start_generation(&self, queue: &K) -> Option<MutationGenerationBatch<K, C, R>> {
        let (generation_id, requests, items, response_bytes) = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("mutation sequencer poisoned");
            let generation = state.queues.get_mut(queue)?.generations.front_mut()?;
            if generation.active {
                return None;
            }
            generation.active = true;
            (
                generation.id,
                generation
                    .requests
                    .iter()
                    .map(|entry| Arc::clone(&entry.request))
                    .collect::<Vec<_>>(),
                generation.items,
                generation.response_bytes,
            )
        };
        Some(MutationGenerationBatch {
            inner: Arc::clone(&self.inner),
            queue: queue.clone(),
            generation_id,
            requests,
            items,
            response_bytes,
            completed: false,
        })
    }

    pub fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("mutation sequencer poisoned");
        state.closed = true;
        state.queues.retain(|_, queue| {
            queue.generations.retain(|generation| generation.active);
            !queue.generations.is_empty()
        });
    }

    pub fn generation_count(&self, queue: &K) -> usize {
        self.inner
            .state
            .lock()
            .expect("mutation sequencer poisoned")
            .queues
            .get(queue)
            .map_or(0, |entry| entry.generations.len())
    }

    pub fn request_count(&self, queue: &K) -> usize {
        self.inner
            .state
            .lock()
            .expect("mutation sequencer poisoned")
            .queues
            .get(queue)
            .map_or(0, |entry| {
                entry
                    .generations
                    .iter()
                    .map(|generation| generation.requests.len())
                    .sum()
            })
    }

    pub fn ingress_counts(&self, queue: &K) -> (usize, usize) {
        self.inner
            .state
            .lock()
            .expect("mutation sequencer poisoned")
            .queues
            .get(queue)
            .map_or((0, 0), |entry| {
                entry.generations.iter().flat_map(|g| &g.requests).fold(
                    (0, 0),
                    |(direct, keyed), request| match request.ingress {
                        MutationIngress::Direct => (direct + 1, keyed),
                        MutationIngress::KeyedPermitLive => (direct, keyed + 1),
                    },
                )
            })
    }

    pub fn generation_requests(&self, queue: &K, generation_id: u64) -> Vec<Arc<R>> {
        self.inner
            .state
            .lock()
            .expect("mutation sequencer poisoned")
            .queues
            .get(queue)
            .and_then(|entry| {
                entry
                    .generations
                    .iter()
                    .find(|generation| generation.id == generation_id)
            })
            .map_or_else(Vec::new, |generation| {
                generation
                    .requests
                    .iter()
                    .map(|entry| Arc::clone(&entry.request))
                    .collect()
            })
    }
}

pub struct MutationTicket<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<MutationSequencerInner<K, C, R>>,
    queue: K,
    request_id: Option<u64>,
    generation_id: u64,
    request: Arc<R>,
}

pub struct MutationGenerationBatch<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    inner: Arc<MutationSequencerInner<K, C, R>>,
    queue: K,
    generation_id: u64,
    requests: Vec<Arc<R>>,
    items: usize,
    response_bytes: usize,
    completed: bool,
}

impl<K, C, R> MutationGenerationBatch<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn requests(&self) -> &[Arc<R>] {
        &self.requests
    }

    pub fn items(&self) -> usize {
        self.items
    }

    pub fn response_bytes(&self) -> usize {
        self.response_bytes
    }

    pub fn complete(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("mutation sequencer poisoned");
        let mut remove_queue = false;
        if let Some(queue) = state.queues.get_mut(&self.queue) {
            if let Some(index) = queue
                .generations
                .iter()
                .position(|generation| generation.id == self.generation_id)
            {
                queue.generations.remove(index);
            }
            remove_queue = queue.generations.is_empty();
        }
        if remove_queue {
            state.queues.remove(&self.queue);
        }
    }
}

impl<K, C, R> Drop for MutationGenerationBatch<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.finish();
    }
}

impl<K, C, R> MutationTicket<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }

    pub fn request(&self) -> &Arc<R> {
        &self.request
    }
}

impl<K, C, R> Drop for MutationTicket<K, C, R>
where
    K: Clone + Eq + Hash + Send + 'static,
    C: Clone + Eq + Send + 'static,
    R: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let Some(request_id) = self.request_id.take() else {
            return;
        };
        let mut state = self
            .inner
            .state
            .lock()
            .expect("mutation sequencer poisoned");
        let mut remove_queue = false;
        if let Some(queue) = state.queues.get_mut(&self.queue) {
            if let Some(generation_index) = queue
                .generations
                .iter()
                .position(|generation| generation.id == self.generation_id)
            {
                let generation = queue
                    .generations
                    .get_mut(generation_index)
                    .expect("mutation generation disappeared");
                if generation.active {
                    return;
                }
                if let Some(request_index) = generation
                    .requests
                    .iter()
                    .position(|request| request.id == request_id)
                {
                    let request = generation
                        .requests
                        .remove(request_index)
                        .expect("mutation request disappeared");
                    generation.items = generation.items.saturating_sub(request.items);
                    generation.response_bytes = generation
                        .response_bytes
                        .saturating_sub(request.response_bytes);
                }
                if generation.requests.is_empty() {
                    queue.generations.remove(generation_index);
                }
            }
            remove_queue = queue.generations.is_empty();
        }
        if remove_queue {
            state.queues.remove(&self.queue);
        }
    }
}

#[derive(Clone)]
pub struct SelectionFenceAdmission {
    inner: Arc<SelectionFenceAdmissionInner>,
}

struct SelectionFenceAdmissionInner {
    max_waiters: usize,
    state: Mutex<SelectionFenceAdmissionState>,
}

struct SelectionFenceAdmissionState {
    closed: bool,
    waiters: usize,
}

impl SelectionFenceAdmission {
    pub fn new(max_waiters: usize) -> Self {
        Self {
            inner: Arc::new(SelectionFenceAdmissionInner {
                max_waiters,
                state: Mutex::new(SelectionFenceAdmissionState {
                    closed: false,
                    waiters: 0,
                }),
            }),
        }
    }

    /// Charge only a caller that will wait for the underlying keyed fence.
    pub fn admit_waiter(&self) -> Result<SelectionFenceWaiterPermit, CoordinationError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("selection fence admission poisoned");
        if state.closed {
            return Err(CoordinationError::Closed {
                resource: SELECTION_FENCE_WAITERS_RESOURCE,
            });
        }
        if state.waiters >= self.inner.max_waiters {
            return Err(CoordinationError::Capacity {
                resource: SELECTION_FENCE_WAITERS_RESOURCE,
            });
        }
        state.waiters += 1;
        drop(state);
        Ok(SelectionFenceWaiterPermit {
            inner: Arc::clone(&self.inner),
            released: false,
        })
    }

    pub fn close(&self) {
        self.inner
            .state
            .lock()
            .expect("selection fence admission poisoned")
            .closed = true;
    }

    pub fn waiter_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("selection fence admission poisoned")
            .waiters
    }
}

pub struct SelectionFenceWaiterPermit {
    inner: Arc<SelectionFenceAdmissionInner>,
    released: bool,
}

impl Drop for SelectionFenceWaiterPermit {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("selection fence admission poisoned");
        state.waiters = state.waiters.saturating_sub(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionFenceMode {
    Shared,
    Exclusive,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FenceWaiterStatus {
    Waiting,
    Granted,
    Closed,
}

struct FenceWaiter {
    mode: SelectionFenceMode,
    status: FenceWaiterStatus,
    waker: Option<Waker>,
}

struct FenceEntry {
    readers: usize,
    writer: bool,
    waiters: VecDeque<u64>,
}

struct SelectionFenceState<K> {
    closed: bool,
    next_waiter_id: u64,
    entries: HashMap<K, FenceEntry>,
    waiters: HashMap<u64, FenceWaiter>,
}

struct SelectionFenceInner<K> {
    state: Mutex<SelectionFenceState<K>>,
}

/// Fair keyed read/write fence prepared for later activation.
///
/// A queued exclusive waiter prevents later shared callers from passing it. This type is not attached
/// to a production append site in S2.
pub struct SelectionFence<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<SelectionFenceInner<K>>,
}

impl<K> Clone for SelectionFence<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K> Default for SelectionFence<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> SelectionFence<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SelectionFenceInner {
                state: Mutex::new(SelectionFenceState {
                    closed: false,
                    next_waiter_id: 0,
                    entries: HashMap::new(),
                    waiters: HashMap::new(),
                }),
            }),
        }
    }

    pub fn acquire_shared(&self, key: K) -> SelectionFenceAcquire<K> {
        self.acquire(key, SelectionFenceMode::Shared)
    }

    pub fn acquire_exclusive(&self, key: K) -> SelectionFenceAcquire<K> {
        self.acquire(key, SelectionFenceMode::Exclusive)
    }

    fn acquire(&self, key: K, mode: SelectionFenceMode) -> SelectionFenceAcquire<K> {
        SelectionFenceAcquire {
            inner: Arc::clone(&self.inner),
            key,
            mode,
            waiter_id: None,
            acquired: false,
            completed: false,
        }
    }

    pub fn close(&self) {
        let wakers = {
            let mut state = self.inner.state.lock().expect("selection fence poisoned");
            state.closed = true;
            let waiter_ids = state
                .entries
                .values_mut()
                .flat_map(|entry| entry.waiters.drain(..))
                .collect::<Vec<_>>();
            let mut wakers = Vec::new();
            for waiter_id in waiter_ids {
                if let Some(waiter) = state.waiters.get_mut(&waiter_id) {
                    waiter.status = FenceWaiterStatus::Closed;
                    if let Some(waker) = waiter.waker.take() {
                        wakers.push(waker);
                    }
                }
            }
            state
                .entries
                .retain(|_, entry| entry.writer || entry.readers > 0);
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub fn entry_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("selection fence poisoned")
            .entries
            .len()
    }
}

pub struct SelectionFenceAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<SelectionFenceInner<K>>,
    key: K,
    mode: SelectionFenceMode,
    waiter_id: Option<u64>,
    acquired: bool,
    completed: bool,
}

impl<K> Unpin for SelectionFenceAcquire<K> where K: Clone + Eq + Hash + Send + 'static {}

impl<K> Future for SelectionFenceAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    type Output = Result<SelectionFencePermit<K>, CoordinationError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            panic!("selection fence polled after completion");
        }
        if let Some(waiter_id) = self.waiter_id {
            let status = {
                let mut state = self.inner.state.lock().expect("selection fence poisoned");
                let waiter = state
                    .waiters
                    .get_mut(&waiter_id)
                    .expect("selection fence waiter missing");
                if waiter.status == FenceWaiterStatus::Waiting {
                    waiter.waker = Some(context.waker().clone());
                }
                waiter.status
            };
            return match status {
                FenceWaiterStatus::Waiting => Poll::Pending,
                FenceWaiterStatus::Granted => {
                    self.inner
                        .state
                        .lock()
                        .expect("selection fence poisoned")
                        .waiters
                        .remove(&waiter_id);
                    self.acquired = true;
                    self.completed = true;
                    Poll::Ready(Ok(SelectionFencePermit {
                        inner: Arc::clone(&self.inner),
                        key: self.key.clone(),
                        mode: self.mode,
                        released: false,
                    }))
                }
                FenceWaiterStatus::Closed => {
                    self.inner
                        .state
                        .lock()
                        .expect("selection fence poisoned")
                        .waiters
                        .remove(&waiter_id);
                    self.completed = true;
                    Poll::Ready(Err(CoordinationError::Closed {
                        resource: SELECTION_FENCE_WAITERS_RESOURCE,
                    }))
                }
            };
        }

        enum Registration {
            Closed,
            Acquired,
            Queued(u64),
        }
        let registration = {
            let mut state = self.inner.state.lock().expect("selection fence poisoned");
            if state.closed {
                Registration::Closed
            } else {
                let immediately_available = state.entries.get(&self.key).is_none_or(|entry| {
                    !entry.writer
                        && entry.waiters.is_empty()
                        && (self.mode == SelectionFenceMode::Shared || entry.readers == 0)
                });
                if immediately_available {
                    let entry = state.entries.entry(self.key.clone()).or_insert(FenceEntry {
                        readers: 0,
                        writer: false,
                        waiters: VecDeque::new(),
                    });
                    match self.mode {
                        SelectionFenceMode::Shared => entry.readers += 1,
                        SelectionFenceMode::Exclusive => entry.writer = true,
                    }
                    Registration::Acquired
                } else {
                    let waiter_id = state.next_waiter_id;
                    state.next_waiter_id = state.next_waiter_id.wrapping_add(1);
                    state.waiters.insert(
                        waiter_id,
                        FenceWaiter {
                            mode: self.mode,
                            status: FenceWaiterStatus::Waiting,
                            waker: Some(context.waker().clone()),
                        },
                    );
                    state
                        .entries
                        .get_mut(&self.key)
                        .expect("contended selection fence entry missing")
                        .waiters
                        .push_back(waiter_id);
                    Registration::Queued(waiter_id)
                }
            }
        };
        match registration {
            Registration::Closed => {
                self.completed = true;
                Poll::Ready(Err(CoordinationError::Closed {
                    resource: SELECTION_FENCE_WAITERS_RESOURCE,
                }))
            }
            Registration::Acquired => {
                self.acquired = true;
                self.completed = true;
                Poll::Ready(Ok(SelectionFencePermit {
                    inner: Arc::clone(&self.inner),
                    key: self.key.clone(),
                    mode: self.mode,
                    released: false,
                }))
            }
            Registration::Queued(waiter_id) => {
                self.waiter_id = Some(waiter_id);
                Poll::Pending
            }
        }
    }
}

impl<K> Drop for SelectionFenceAcquire<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn drop(&mut self) {
        if self.acquired {
            return;
        }
        let Some(waiter_id) = self.waiter_id.take() else {
            return;
        };
        let wakers = {
            let mut state = self.inner.state.lock().expect("selection fence poisoned");
            let Some(waiter) = state.waiters.remove(&waiter_id) else {
                return;
            };
            match waiter.status {
                FenceWaiterStatus::Waiting => {
                    if let Some(entry) = state.entries.get_mut(&self.key)
                        && let Some(index) = entry.waiters.iter().position(|id| *id == waiter_id)
                    {
                        entry.waiters.remove(index);
                    }
                    grant_fence_locked(&mut state, &self.key)
                }
                FenceWaiterStatus::Granted => {
                    release_fence_locked(&mut state, &self.key, waiter.mode)
                }
                FenceWaiterStatus::Closed => Vec::new(),
            }
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

pub struct SelectionFencePermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    inner: Arc<SelectionFenceInner<K>>,
    key: K,
    mode: SelectionFenceMode,
    released: bool,
}

impl<K> Drop for SelectionFencePermit<K>
where
    K: Clone + Eq + Hash + Send + 'static,
{
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let wakers = {
            let mut state = self.inner.state.lock().expect("selection fence poisoned");
            release_fence_locked(&mut state, &self.key, self.mode)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

fn release_fence_locked<K>(
    state: &mut SelectionFenceState<K>,
    key: &K,
    mode: SelectionFenceMode,
) -> Vec<Waker>
where
    K: Clone + Eq + Hash,
{
    if let Some(entry) = state.entries.get_mut(key) {
        match mode {
            SelectionFenceMode::Shared => {
                entry.readers = entry.readers.saturating_sub(1);
            }
            SelectionFenceMode::Exclusive => entry.writer = false,
        }
    }
    grant_fence_locked(state, key)
}

fn grant_fence_locked<K>(state: &mut SelectionFenceState<K>, key: &K) -> Vec<Waker>
where
    K: Clone + Eq + Hash,
{
    let grant_ids = {
        let Some(entry) = state.entries.get_mut(key) else {
            return Vec::new();
        };
        if entry.writer || entry.readers > 0 {
            return Vec::new();
        }
        let Some(first_id) = entry.waiters.front().copied() else {
            state.entries.remove(key);
            return Vec::new();
        };
        let first_mode = state
            .waiters
            .get(&first_id)
            .expect("queued selection fence waiter missing")
            .mode;
        match first_mode {
            SelectionFenceMode::Exclusive => {
                entry.waiters.pop_front();
                entry.writer = true;
                vec![first_id]
            }
            SelectionFenceMode::Shared => {
                let mut ids = Vec::new();
                while let Some(waiter_id) = entry.waiters.front().copied() {
                    let mode = state
                        .waiters
                        .get(&waiter_id)
                        .expect("queued selection fence waiter missing")
                        .mode;
                    if mode != SelectionFenceMode::Shared {
                        break;
                    }
                    entry.waiters.pop_front();
                    ids.push(waiter_id);
                }
                entry.readers += ids.len();
                ids
            }
        }
    };

    let mut wakers = Vec::new();
    for waiter_id in grant_ids {
        let waiter = state
            .waiters
            .get_mut(&waiter_id)
            .expect("granted selection fence waiter missing");
        waiter.status = FenceWaiterStatus::Granted;
        if let Some(waker) = waiter.waker.take() {
            wakers.push(waker);
        }
    }
    wakers
}

#[cfg(test)]
mod tests {
    use std::task::{Wake, Waker};

    use fireweed_core::{ItemId, LeaseToken, UtcTimestamp};

    use super::*;
    use crate::{
        ClaimCommand, FinalizeCommand, FinalizeKind, FinalizeOutcome, LeaseExpiredCommand,
        PushCommand, RenewLeaseCommand, UpdateFieldsBatchCommand, WriteSideRecordsCommand,
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(NoopWake));
        Pin::new(future).poll(&mut Context::from_waker(&waker))
    }

    fn assert_error<T>(result: Result<T, CoordinationError>, expected: CoordinationError) {
        match result {
            Err(actual) => assert_eq!(actual, expected),
            Ok(_) => panic!("expected coordination error"),
        }
    }

    fn assert_poll_error<T>(
        result: Poll<Result<T, CoordinationError>>,
        expected: CoordinationError,
    ) {
        match result {
            Poll::Ready(Err(actual)) => assert_eq!(actual, expected),
            Poll::Ready(Ok(_)) | Poll::Pending => panic!("expected ready coordination error"),
        }
    }

    fn finalize(kind: FinalizeKind) -> QueueCommand {
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: ItemId::from_u64(1),
                kind,
                applied_state: None,
                not_before: None,
            }],
        })
    }

    fn claim() -> QueueCommand {
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![ItemId::from_u64(1)],
            lease_token: LeaseToken::new("lease").unwrap(),
            lease_expires_at: UtcTimestamp::new(2, 0).unwrap(),
            worker_id: None,
        })
    }

    #[test]
    fn candidate_mutations_join_generations_and_pending_consumers_do_not() {
        let compatible = [
            (
                QueueCommand::Push(PushCommand { items: Vec::new() }),
                MutationGenerationKind::Push,
            ),
            (
                QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
                    updates: Vec::new(),
                }),
                MutationGenerationKind::BatchUpdate,
            ),
        ];
        for (command, kind) in compatible {
            assert_eq!(
                selection_fence_disposition(&command),
                SelectionFenceDisposition::Shared
            );
            assert_eq!(
                mutation_generation_disposition(&command),
                MutationGenerationDisposition::Compatible(kind)
            );
        }

        for command in [
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![ItemId::from_u64(1)],
            }),
            finalize(FinalizeKind::Retry),
        ] {
            assert_eq!(
                selection_fence_disposition(&command),
                SelectionFenceDisposition::Shared
            );
            assert_eq!(
                mutation_generation_disposition(&command),
                MutationGenerationDisposition::Singleton
            );
        }

        let pending_consumer = claim();
        assert_eq!(
            selection_fence_disposition(&pending_consumer),
            SelectionFenceDisposition::Exclusive
        );
        assert_eq!(
            mutation_generation_disposition(&pending_consumer),
            MutationGenerationDisposition::NotCandidateMutating
        );
    }

    #[test]
    fn leased_and_non_work_commands_bypass_generations() {
        let commands = [
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![ItemId::from_u64(1)],
                lease_expires_at: UtcTimestamp::new(3, 0).unwrap(),
            }),
            finalize(FinalizeKind::Complete),
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand::default()),
        ];
        for command in commands {
            assert_eq!(
                selection_fence_disposition(&command),
                SelectionFenceDisposition::Bypass
            );
            assert_eq!(
                mutation_generation_disposition(&command),
                MutationGenerationDisposition::NotCandidateMutating
            );
        }
    }

    #[test]
    fn non_bypass_derived_append_has_exactly_one_audited_admission() {
        for (class, disposition) in [
            (
                AppendAdmissionClass::KeyedPermitLive,
                SelectionFenceDisposition::Shared,
            ),
            (
                AppendAdmissionClass::SelectionRequired,
                SelectionFenceDisposition::Shared,
            ),
            (
                AppendAdmissionClass::SelectionRequired,
                SelectionFenceDisposition::Exclusive,
            ),
            (
                AppendAdmissionClass::ClaimCoordinatorLive,
                SelectionFenceDisposition::Exclusive,
            ),
        ] {
            assert_eq!(audited_append_admission_count(class, disposition), Some(1));
        }

        assert_eq!(
            audited_append_admission_count(
                AppendAdmissionClass::Bypass,
                SelectionFenceDisposition::Bypass,
            ),
            Some(0)
        );
        for class in [
            AppendAdmissionClass::AtomicNative,
            AppendAdmissionClass::RecoveryOnly,
            AppendAdmissionClass::NonDerived,
        ] {
            assert_eq!(
                audited_append_admission_count(class, SelectionFenceDisposition::Shared),
                Some(0)
            );
        }
        assert_eq!(
            audited_append_admission_count(
                AppendAdmissionClass::ClaimCoordinatorLive,
                SelectionFenceDisposition::Shared,
            ),
            None
        );
        assert_eq!(
            audited_append_admission_count(
                AppendAdmissionClass::Bypass,
                SelectionFenceDisposition::Exclusive,
            ),
            None
        );
    }

    #[test]
    fn claim_coordinator_rejects_1025_compatible_callers_within_eight_admitted_buckets() {
        let coordinator = ClaimCoordinator::<u8, usize>::default();
        let mut callers = Vec::new();
        for index in 0..CLAIM_MAX_CALLERS {
            callers.push(
                coordinator
                    .join((index % CLAIM_MAX_DRIVERS) as u8, Arc::new(index), 1, 8)
                    .expect("caller within capacity"),
            );
        }
        assert_eq!(coordinator.caller_count(), CLAIM_MAX_CALLERS);
        assert_eq!(coordinator.driver_count(), CLAIM_MAX_DRIVERS);
        assert_error(
            coordinator.join(0, Arc::new(1_025), 1, 8),
            CoordinationError::Capacity {
                resource: CLAIM_COORDINATOR_WAITERS_RESOURCE,
            },
        );
        drop(callers);
        assert_eq!(coordinator.caller_count(), 0);
        assert_eq!(coordinator.driver_count(), 0);
    }

    #[test]
    fn claim_coordinator_checks_driver_capacity_only_for_a_new_bucket_and_redrives_suffix() {
        let coordinator = ClaimCoordinator::<u8, usize>::default();
        let mut callers = Vec::new();
        for key in 0..CLAIM_MAX_DRIVERS as u8 {
            callers.push(
                coordinator
                    .join(key, Arc::new(key as usize), 100, 32)
                    .expect("driver bucket within capacity"),
            );
        }
        assert_error(
            coordinator.join(9, Arc::new(9), 100, 32),
            CoordinationError::Capacity {
                resource: CLAIM_DRIVER_INGRESS_RESOURCE,
            },
        );
        callers.push(
            coordinator
                .join(0, Arc::new(10), 100, 32)
                .expect("compatible caller attaches despite full driver budget"),
        );
        for value in 11..19 {
            callers.push(
                coordinator
                    .join(0, Arc::new(value), 100, 32)
                    .expect("suffix caller"),
            );
        }
        let first = coordinator.start_driver(&0).expect("first driver");
        assert_eq!(first.len(), CLAIM_GENERATION_MAX_REQUESTS);
        assert_eq!(coordinator.pending_for(&0), 2);
        assert!(coordinator.start_driver(&0).is_none());
        first.complete();
        let suffix = coordinator
            .start_driver(&0)
            .expect("suffix driver re-elected");
        assert_eq!(suffix.len(), 2);
        suffix.complete();
        drop(callers);
        assert_eq!(coordinator.caller_count(), 0);
    }

    #[test]
    fn claim_queue_turn_rejects_third_driver_and_releases_on_cancel_and_close() {
        let turns = ClaimQueueTurn::<&'static str>::default();
        assert_eq!(turns.max_wait(), CLAIM_TURN_DEFAULT_MAX_WAIT);
        let mut first = turns.acquire("q");
        let first = match poll_once(&mut first) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first claim turn not ready"),
        };
        let mut second = turns.acquire("q");
        assert!(matches!(poll_once(&mut second), Poll::Pending));
        let mut third = turns.acquire("q");
        assert_poll_error(
            poll_once(&mut third),
            CoordinationError::Capacity {
                resource: CLAIM_QUEUE_TURN_RESOURCE,
            },
        );
        assert_eq!(turns.queued(), 1);
        drop(second);
        assert_eq!(turns.queued(), 0);

        let mut queued = turns.acquire("q");
        assert!(matches!(poll_once(&mut queued), Poll::Pending));
        turns.close();
        assert_eq!(turns.queued(), 0);
        assert_poll_error(
            poll_once(&mut queued),
            CoordinationError::Closed {
                resource: CLAIM_QUEUE_TURN_RESOURCE,
            },
        );
        drop(first);
        assert_eq!(turns.entry_count(), 0);
    }

    fn assert_two_wave_capacity(
        admission: &SlotAdmission,
        active_limit: usize,
        queued_limit: usize,
        resource: &'static str,
    ) {
        let mut active = Vec::new();
        for _ in 0..active_limit {
            let mut acquire = admission.acquire();
            match poll_once(&mut acquire) {
                Poll::Ready(Ok(permit)) => active.push(permit),
                _ => panic!("active slot was not admitted"),
            }
        }
        let mut queued = Vec::new();
        for _ in 0..queued_limit {
            let mut acquire = admission.acquire();
            assert!(matches!(poll_once(&mut acquire), Poll::Pending));
            queued.push(acquire);
        }
        let mut above = admission.acquire();
        assert_poll_error(
            poll_once(&mut above),
            CoordinationError::Capacity { resource },
        );
        assert_eq!(admission.active(), active_limit);
        assert_eq!(admission.queued(), queued_limit);
        drop(active.pop());
        assert!(matches!(poll_once(&mut queued[0]), Poll::Ready(Ok(_))));
        drop(active);
        drop(queued);
        assert_eq!(admission.active(), 0);
        assert_eq!(admission.queued(), 0);
    }

    #[test]
    fn driver_and_outcome_admissions_enforce_two_waves_and_configurable_caps() {
        let claim = ClaimDriverReadAdmission::default();
        assert_eq!(claim.max_wait(), DRIVER_SLOT_DEFAULT_MAX_WAIT);
        assert_two_wave_capacity(&claim.inner, 4, 4, CLAIM_DRIVER_SLOTS_RESOURCE);

        let shared = SharedDriverReadAdmission::default();
        assert_eq!(shared.max_wait(), DRIVER_SLOT_DEFAULT_MAX_WAIT);
        assert_two_wave_capacity(&shared.inner, 12, 12, SHARED_DRIVER_SLOTS_RESOURCE);

        let outcome = OutcomeReadAdmission::default();
        assert_eq!(outcome.max_wait(), OUTCOME_SLOT_DEFAULT_MAX_WAIT);
        assert_two_wave_capacity(&outcome.inner, 8, 8, OUTCOME_READ_SLOTS_RESOURCE);

        let custom = OutcomeReadAdmission::new(Duration::from_millis(7));
        assert_eq!(custom.max_wait(), Duration::from_millis(7));
    }

    #[test]
    fn slot_close_and_cancellation_release_waiter_accounting() {
        let admission = ClaimDriverReadAdmission::default();
        let mut active = Vec::new();
        for _ in 0..4 {
            let mut acquire = admission.acquire();
            active.push(match poll_once(&mut acquire) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("active slot was not admitted"),
            });
        }
        let mut cancelled = admission.acquire();
        assert!(matches!(poll_once(&mut cancelled), Poll::Pending));
        drop(cancelled);
        assert_eq!(admission.queued(), 0);
        let mut closed = admission.acquire();
        assert!(matches!(poll_once(&mut closed), Poll::Pending));
        admission.close();
        assert_eq!(admission.queued(), 0);
        assert_poll_error(
            poll_once(&mut closed),
            CoordinationError::Closed {
                resource: CLAIM_DRIVER_SLOTS_RESOURCE,
            },
        );
        drop(active);
        assert_eq!(admission.active(), 0);
    }

    #[test]
    fn configurable_deadlines_release_turn_and_slot_waiter_counts() {
        let admission = ClaimDriverReadAdmission::new(Duration::ZERO);
        let mut active = Vec::new();
        for _ in 0..4 {
            let mut acquire = admission.acquire();
            active.push(match poll_once(&mut acquire) {
                Poll::Ready(Ok(permit)) => permit,
                _ => panic!("active slot was not admitted"),
            });
        }
        let mut deadline = admission.acquire();
        assert!(matches!(poll_once(&mut deadline), Poll::Pending));
        assert_poll_error(
            poll_once(&mut deadline),
            CoordinationError::Deadline {
                resource: CLAIM_DRIVER_SLOTS_RESOURCE,
            },
        );
        assert_eq!(admission.queued(), 0);
        drop(active);

        let turns = ClaimQueueTurn::<&'static str>::new(Duration::ZERO);
        let mut active_turn = turns.acquire("q");
        let active_turn = match poll_once(&mut active_turn) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("active claim turn was not admitted"),
        };
        let mut turn_deadline = turns.acquire("q");
        assert_poll_error(
            poll_once(&mut turn_deadline),
            CoordinationError::Deadline {
                resource: CLAIM_QUEUE_TURN_RESOURCE,
            },
        );
        assert_eq!(turns.queued(), 0);
        drop(active_turn);
    }

    #[test]
    fn mutation_generations_are_fifo_cross_ingress_bounded_and_zero_copy() {
        let sequencer = MutationSequencer::<&'static str, u8, Vec<u8>>::new();
        let payloads = (0..MUTATION_MAX_REQUESTS_PER_QUEUE)
            .map(|index| Arc::new(vec![index as u8; 1024]))
            .collect::<Vec<_>>();
        let mut tickets = Vec::new();
        for index in 0..MUTATION_MAX_REQUESTS_PER_QUEUE {
            let ticket = sequencer
                .admit(
                    "q",
                    1,
                    if index % 2 == 0 {
                        MutationIngress::Direct
                    } else {
                        MutationIngress::KeyedPermitLive
                    },
                    Arc::clone(&payloads[index]),
                    100,
                    1024,
                )
                .expect("request within two generations");
            if index < CLAIM_GENERATION_MAX_REQUESTS {
                assert_eq!(
                    ticket.generation_id(),
                    tickets
                        .first()
                        .map_or(0, |t: &MutationTicket<_, _, _>| t.generation_id())
                );
            }
            assert!(Arc::ptr_eq(ticket.request(), &payloads[index]));
            tickets.push(ticket);
        }
        assert_eq!(sequencer.generation_count(&"q"), 2);
        assert_eq!(sequencer.request_count(&"q"), 16);
        assert_eq!(sequencer.ingress_counts(&"q"), (8, 8));
        let first_generation = tickets[0].generation_id();
        let first_requests = sequencer.generation_requests(&"q", first_generation);
        assert_eq!(
            first_requests
                .iter()
                .map(|request| request[0])
                .collect::<Vec<_>>(),
            (0..CLAIM_GENERATION_MAX_REQUESTS as u8).collect::<Vec<_>>()
        );
        assert!(
            first_requests
                .iter()
                .zip(&payloads)
                .all(|(retained, original)| Arc::ptr_eq(retained, original))
        );
        let active = sequencer
            .start_generation(&"q")
            .expect("front generation elected");
        assert_eq!(active.items(), GENERATION_MAX_ITEMS);
        assert_eq!(active.response_bytes(), 8 * 1024);
        assert!(
            active
                .requests()
                .iter()
                .zip(&payloads)
                .all(|(retained, original)| Arc::ptr_eq(retained, original))
        );
        assert!(sequencer.start_generation(&"q").is_none());
        assert_error(
            sequencer.admit(
                "q",
                1,
                MutationIngress::Direct,
                Arc::clone(&payloads[0]),
                1,
                1,
            ),
            CoordinationError::Capacity {
                resource: MUTATION_SEQUENCER_RESOURCE,
            },
        );

        tickets
            .drain(..CLAIM_GENERATION_MAX_REQUESTS)
            .for_each(drop);
        assert_eq!(sequencer.generation_count(&"q"), 2);
        assert_eq!(sequencer.request_count(&"q"), 16);
        active.complete();
        assert_eq!(sequencer.generation_count(&"q"), 1);
        assert_eq!(sequencer.request_count(&"q"), 8);
        let suffix = sequencer
            .start_generation(&"q")
            .expect("suffix generation elected");
        drop(tickets);
        assert_eq!(sequencer.request_count(&"q"), 8);
        sequencer.close();
        assert_eq!(sequencer.generation_count(&"q"), 1);
        suffix.complete();
        assert_eq!(sequencer.generation_count(&"q"), 0);
    }

    #[test]
    fn mutation_third_generation_and_oversized_descriptor_reject_before_retention() {
        let sequencer = MutationSequencer::<&'static str, u8, usize>::new();
        let first = sequencer
            .admit("q", 1, MutationIngress::Direct, Arc::new(1), 1, 1)
            .unwrap();
        let second = sequencer
            .admit("q", 2, MutationIngress::KeyedPermitLive, Arc::new(2), 1, 1)
            .unwrap();
        assert_eq!(sequencer.generation_count(&"q"), 2);
        assert_error(
            sequencer.admit("q", 3, MutationIngress::Direct, Arc::new(3), 1, 1),
            CoordinationError::Capacity {
                resource: MUTATION_SEQUENCER_RESOURCE,
            },
        );
        assert_error(
            sequencer.admit(
                "other",
                1,
                MutationIngress::Direct,
                Arc::new(4),
                1,
                GENERATION_MAX_RESPONSE_BYTES + 1,
            ),
            CoordinationError::Capacity {
                resource: MUTATION_SEQUENCER_RESOURCE,
            },
        );
        drop((first, second));
        sequencer.close();
        assert_eq!(sequencer.request_count(&"q"), 0);
    }

    #[test]
    fn selection_fence_admission_counts_only_waiters_and_releases_on_close() {
        let admission = SelectionFenceAdmission::new(1_024);
        let permits = (0..1_024)
            .map(|_| admission.admit_waiter().expect("waiter within capacity"))
            .collect::<Vec<_>>();
        assert_eq!(admission.waiter_count(), 1_024);
        assert_error(
            admission.admit_waiter(),
            CoordinationError::Capacity {
                resource: SELECTION_FENCE_WAITERS_RESOURCE,
            },
        );
        drop(permits);
        assert_eq!(admission.waiter_count(), 0);
        admission.close();
        assert_error(
            admission.admit_waiter(),
            CoordinationError::Closed {
                resource: SELECTION_FENCE_WAITERS_RESOURCE,
            },
        );
    }

    #[test]
    fn inert_selection_fence_is_fifo_and_writer_preferring() {
        let fence = SelectionFence::<&'static str>::new();
        let mut first_reader = fence.acquire_shared("q");
        let first_reader = match poll_once(&mut first_reader) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("first reader not ready"),
        };
        let mut writer = fence.acquire_exclusive("q");
        assert!(matches!(poll_once(&mut writer), Poll::Pending));
        let mut later_reader = fence.acquire_shared("q");
        assert!(matches!(poll_once(&mut later_reader), Poll::Pending));
        drop(first_reader);
        let writer = match poll_once(&mut writer) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("writer did not receive the fence first"),
        };
        assert!(matches!(poll_once(&mut later_reader), Poll::Pending));
        drop(writer);
        let later_reader = match poll_once(&mut later_reader) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("reader did not advance after writer"),
        };
        drop(later_reader);
        assert_eq!(fence.entry_count(), 0);
    }
}
