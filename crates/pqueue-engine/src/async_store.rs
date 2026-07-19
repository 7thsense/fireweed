//! Runtime-neutral asynchronous storage axes.
//!
//! These traits are the additive migration surface for the first async [`crate::ComposedBackend`]
//! conversion. They intentionally cover the initial shard, append/replay, projection-apply, basic claim,
//! rich relational claim selection, and control-plane paths only. Retention, snapshots, group commit,
//! indexes, and repair operations remain on the legacy axes until the slice that migrates each behavior
//! and its tests.
//!
//! Every request value is owned. Shared receivers allow independent queue/connection work to progress
//! without requiring a process-global mutable store borrow; implementations provide their own per-queue or
//! per-connection synchronization. An implementation may borrow `self` while its future is alive, but may not
//! borrow caller-owned command buffers, identifiers, or database transactions across a suspension point.
//! The returned futures are `Send` and expose no executor type.
//!
//! # Cancellation and transaction ownership
//!
//! Dropping a future before its durable commit point must leave no effect. A backend that can suspend while
//! committing transfers the owned operation and its connection/transaction capability to backend-owned
//! execution before that suspension; dropping the caller then discards only the response. Atomic stores
//! commit append, projection apply, cursor/frontier, and replay outcome together. Eventual-apply stores may
//! make append durable first, but must preserve the response barrier and repair the projection by replay.
//!
//! Blocking implementations must offload one complete transaction below this boundary. They must not hold a
//! `std::sync::MutexGuard` or borrowed blocking transaction across an `.await`, and must not offload
//! individual statements belonging to the same transaction.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use pqueue_core::{ItemId, ItemState, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp};

use crate::{
    ClaimCompatibility, ClaimUnit, ClaimedItem, CommandEnvelope, CommandPage, CommandPosition,
    ControlPlane, CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeTarget,
    IdempotencyDecision, LogStore, ProjectionSnapshot, ProjectionStore, PushFingerprint, PushItem,
    QueueKey, RenewTarget, RichClaimSelection, SnapshotRef,
};

/// An owned blocking operation over one store instance.
///
/// This is the compatibility boundary for blocking substrates. Callers transfer one complete storage
/// operation into the adapter; they do not hand out per-statement callbacks or borrowed transactions.
pub trait BlockingStoreOperation<S>: Send + 'static {
    type Output: Send + 'static;

    fn run(self, store: &mut S) -> EngineResult<Self::Output>;
}

impl<S, O, F> BlockingStoreOperation<S> for F
where
    F: FnOnce(&mut S) -> EngineResult<O> + Send + 'static,
    O: Send + 'static,
{
    type Output = O;

    fn run(self, store: &mut S) -> EngineResult<Self::Output> {
        self(store)
    }
}

#[derive(Clone)]
struct BoundedBlockingExecutor {
    permits: Arc<BlockingPermits>,
}

impl BoundedBlockingExecutor {
    fn new(max_in_flight: usize) -> EngineResult<Self> {
        if max_in_flight == 0 {
            return Err(EngineError::Invalid(
                "blocking adapter bound must be nonzero",
            ));
        }
        Ok(Self {
            permits: Arc::new(BlockingPermits::new(max_in_flight)),
        })
    }

    fn execute<T, F>(&self, task: F) -> BlockingTaskFuture<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        BlockingTaskFuture {
            permits: self.permits.clone(),
            task: Some(Box::new(task)),
            state: Arc::new(Mutex::new(BlockingTaskState {
                started: false,
                result: None,
                waker: None,
            })),
        }
    }
}

struct BlockingPermits {
    inner: Mutex<BlockingPermitsInner>,
}

struct BlockingPermitsInner {
    available: usize,
    waiters: Vec<Waker>,
}

impl BlockingPermits {
    fn new(available: usize) -> Self {
        Self {
            inner: Mutex::new(BlockingPermitsInner {
                available,
                waiters: Vec::new(),
            }),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<BlockingPermit> {
        let mut inner = self.inner.lock().expect("blocking permit mutex poisoned");
        if inner.available == 0 {
            return None;
        }
        inner.available -= 1;
        Some(BlockingPermit {
            permits: self.clone(),
        })
    }

    fn park(&self, waker: &Waker) {
        let mut inner = self.inner.lock().expect("blocking permit mutex poisoned");
        if !inner.waiters.iter().any(|queued| queued.will_wake(waker)) {
            inner.waiters.push(waker.clone());
        }
    }

    fn release(&self) {
        let wake = {
            let mut inner = self.inner.lock().expect("blocking permit mutex poisoned");
            inner.available += 1;
            inner.waiters.pop()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

struct BlockingPermit {
    permits: Arc<BlockingPermits>,
}

impl Drop for BlockingPermit {
    fn drop(&mut self) {
        self.permits.release();
    }
}

struct BlockingTaskState<T> {
    started: bool,
    result: Option<T>,
    waker: Option<Waker>,
}

struct BlockingTaskFuture<T> {
    permits: Arc<BlockingPermits>,
    task: Option<Box<dyn FnOnce() -> T + Send + 'static>>,
    state: Arc<Mutex<BlockingTaskState<T>>>,
}

impl<T: Send + 'static> Future for BlockingTaskFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        {
            let mut state = this.state.lock().expect("blocking task mutex poisoned");
            if let Some(result) = state.result.take() {
                return Poll::Ready(result);
            }
            if state.started {
                state.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }

        let Some(permit) = this.permits.try_acquire() else {
            this.permits.park(cx.waker());
            return Poll::Pending;
        };

        let task = this
            .task
            .take()
            .expect("blocking task missing before start");
        let state = this.state.clone();
        {
            let mut state = state.lock().expect("blocking task mutex poisoned");
            state.started = true;
            state.waker = Some(cx.waker().clone());
        }
        std::thread::spawn(move || {
            let result = task();
            let wake = {
                let mut state = state.lock().expect("blocking task mutex poisoned");
                state.result = Some(result);
                state.waker.take()
            };
            drop(permit);
            if let Some(waker) = wake {
                waker.wake();
            }
        });
        Poll::Pending
    }
}

/// Immediate ready-future adapter for a synchronous log store.
pub struct ImmediateLogStore<S> {
    store: Arc<Mutex<S>>,
}

impl<S> ImmediateLogStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

/// Immediate ready-future adapter for a synchronous projection store.
pub struct ImmediateProjectionStore<S> {
    store: Arc<Mutex<S>>,
}

impl<S> ImmediateProjectionStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

/// Immediate ready-future adapter for a synchronous control plane.
pub struct ImmediateControlPlane<S> {
    store: Arc<S>,
}

impl<S> ImmediateControlPlane<S> {
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}

/// Bounded blocking adapter for a synchronous log store.
pub struct BlockingLogStore<S> {
    store: Arc<Mutex<S>>,
    executor: BoundedBlockingExecutor,
}

impl<S> BlockingLogStore<S> {
    pub fn new(store: S, max_in_flight: usize) -> EngineResult<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
        })
    }

    pub fn run_operation<O>(
        &self,
        operation: O,
    ) -> impl Future<Output = EngineResult<O::Output>> + Send
    where
        S: Send + 'static,
        O: BlockingStoreOperation<S>,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store.lock().expect("blocking log store mutex poisoned");
            operation.run(&mut *store)
        })
    }
}

/// Bounded blocking adapter for a synchronous projection store.
pub struct BlockingProjectionStore<S> {
    store: Arc<Mutex<S>>,
    executor: BoundedBlockingExecutor,
}

impl<S> BlockingProjectionStore<S> {
    pub fn new(store: S, max_in_flight: usize) -> EngineResult<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
        })
    }

    pub fn run_operation<O>(
        &self,
        operation: O,
    ) -> impl Future<Output = EngineResult<O::Output>> + Send
    where
        S: Send + 'static,
        O: BlockingStoreOperation<S>,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store
                .lock()
                .expect("blocking projection store mutex poisoned");
            operation.run(&mut *store)
        })
    }
}

/// Bounded blocking adapter for a synchronous control plane.
pub struct BlockingControlPlane<S> {
    store: Arc<Mutex<S>>,
    executor: BoundedBlockingExecutor,
}

impl<S> BlockingControlPlane<S> {
    pub fn new(store: S, max_in_flight: usize) -> EngineResult<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
        })
    }

    pub fn run_operation<O>(
        &self,
        operation: O,
    ) -> impl Future<Output = EngineResult<O::Output>> + Send
    where
        S: Send + 'static,
        O: BlockingStoreOperation<S>,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store.lock().expect("blocking control plane mutex poisoned");
            operation.run(&mut *store)
        })
    }
}

/// Projection-sealed retry inputs for one ordinary leased item.
///
/// The projection must return one member for each requested target in the same order. Relational
/// projections read both counters from the same validation row; the default implementation uses the
/// queue-level retry bound supplied by the lifecycle planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeLeaseMember {
    pub item_id: ItemId,
    pub attempt_count: u32,
    pub max_attempts: u32,
}

/// Native-async command-log, epoch-fence, replay, and high-water operations needed by initial composition.
pub trait AsyncLogStore: Send + Sync {
    /// Immutable after construction; implementations must not acquire an async lock here.
    fn durability_class(&self) -> DurabilityClass;

    fn ensure_shard(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    fn current_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    fn acquire_epoch(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send;

    /// Append one owned batch under the exact expected epoch.
    ///
    /// Returning success means the positions are durable according to [`Self::durability_class`]. If the
    /// call can suspend at its commit point, its surrounding backend-owned commit task must continue after
    /// caller cancellation and retain a replay-resolvable outcome.
    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommandPosition>>> + Send;

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send;

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        async move {
            let _ = (shard, position, snapshot);
            Err(EngineError::Unavailable)
        }
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        async move {
            let _ = shard;
            Err(EngineError::Unavailable)
        }
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        async move {
            let _ = snapshot_ref;
            Err(EngineError::Unavailable)
        }
    }

    fn snapshot_at_or_before(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        async move {
            let latest = self.latest_snapshot(shard).await?;
            Ok(match latest {
                Some(snapshot)
                    if snapshot.position.precedes(&position) || snapshot.position == position =>
                {
                    Some(snapshot)
                }
                _ => None,
            })
        }
    }

    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        std::future::ready(Ok(Vec::new()))
    }
}

/// Native-async projection operations needed by initial append/apply, recovery, and item claim paths.
pub trait AsyncProjectionStore: Send + Sync {
    /// Immutable after construction; implementations must not acquire an async lock here.
    fn supports_gates(&self) -> bool {
        false
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Fail-closed mutation admission immediately before the append/apply unit begins.
    fn admit_mutation(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Validate every projection-owned pre-append push constraint (client keys, cohorts/groups, and
    /// typed-index uniqueness) against one owned candidate batch. Unsupported projections fail closed.
    fn validate_push(
        &self,
        _shard: QueueKey,
        _items: Vec<PushItem>,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn pause_blocks_intake(
        &self,
        _shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<bool>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Resolve retained push idempotency from state updated by the same commit/apply boundary.
    fn push_idempotency(
        &self,
        _shard: QueueKey,
        _request_id: RequestId,
        _fingerprint: PushFingerprint,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Validate that every item remains an active, unfenced lease.
    fn renew_validate(
        &self,
        _shard: QueueKey,
        _targets: Vec<RenewTarget>,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<FinalizeTarget>,
        now: UtcTimestamp,
        default_max_attempts: u32,
    ) -> impl std::future::Future<Output = EngineResult<Vec<FinalizeLeaseMember>>> + Send {
        async move {
            self.renew_validate(
                shard.clone(),
                targets
                    .iter()
                    .map(|t| RenewTarget {
                        item_id: t.item_id,
                        lease_token: t.lease_token.clone(),
                    })
                    .collect(),
                now,
            )
            .await?;
            let items = self
                .render_claimed(shard, targets.iter().map(|t| t.item_id).collect())
                .await?;
            if items.len() != targets.len() {
                return Err(EngineError::StaleLease);
            }
            items
                .into_iter()
                .zip(targets)
                .map(|(item, target)| {
                    if item.item_id != target.item_id || item.item_version != target.item_version {
                        Err(EngineError::Conflict)
                    } else {
                        Ok(FinalizeLeaseMember {
                            item_id: item.item_id,
                            attempt_count: item.attempt_count,
                            max_attempts: default_max_attempts,
                        })
                    }
                })
                .collect()
        }
    }

    /// Validate one complete cohort lease and return its deterministic member footprint. Implementations
    /// must verify the shared token, live lease state, unfenced/unsuperseded members, and cohort
    /// completeness in one storage operation.
    fn cohort_lease_validate(
        &self,
        _shard: QueueKey,
        _target: crate::CohortLeaseTarget,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::CohortLeaseMember>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn purge_validate(
        &self,
        _shard: QueueKey,
        _ids: Vec<ItemId>,
        _force: bool,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Deterministically select leases expired strictly before `now`, ordered by item id and capped before
    /// returning. Selection is read-only; the resulting `LeaseExpired` command owns the transition.
    fn expired_leases(
        &self,
        _shard: QueueKey,
        _now: UtcTimestamp,
        _max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Apply a committed owned batch to the live serving image.
    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    /// Apply an owned replay batch and durably advance the projection recovery frontier with it.
    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send;

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send;

    /// Select item-level candidates while preserving the item-compatible group/metadata predicates.
    /// Stores without a filter-capable read model fail closed when either predicate is present.
    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            if compatibility.group_key.is_some() || !compatibility.metadata_equals.is_empty() {
                return Err(EngineError::Unavailable);
            }
            self.eligible_candidates(shard, now, max).await
        }
    }

    /// Select a relational rich-claim unit without durably mutating projection state. Implementations that
    /// do not materialize group/cohort state fail closed rather than degrading to item-level selection.
    fn select_rich_claim(
        &self,
        _shard: QueueKey,
        _unit: ClaimUnit,
        _compatibility: ClaimCompatibility,
        _now: UtcTimestamp,
        _max_items: usize,
    ) -> impl std::future::Future<Output = EngineResult<RichClaimSelection>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send;

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send;

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send;

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send;

    /// Enumerate definitions persisted by a durable projection during recovery-on-open.
    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send;
}

/// Native-async queue-definition control plane. Assignment epochs remain authoritative on the log axis.
pub trait AsyncControlPlane: Send + Sync {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send;

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send;

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send;
}

impl<S> AsyncLogStore for ImmediateLogStore<S>
where
    S: LogStore + Send,
{
    fn durability_class(&self) -> DurabilityClass {
        self.store
            .lock()
            .expect("immediate log store mutex poisoned")
            .durability_class()
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .ensure_shard(&shard);
        std::future::ready(result)
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .current_epoch(&shard);
        std::future::ready(result)
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .acquire_epoch(&shard);
        std::future::ready(result)
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .append(&shard, &commands, expected_epoch);
        std::future::ready(result)
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .read_from(&shard, from, limit);
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .high_water(&shard);
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .set_high_water(&shard, position);
        std::future::ready(result)
    }

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .write_snapshot(&shard, position, snapshot);
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .latest_snapshot(&shard);
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .read_snapshot(&snapshot_ref);
        std::future::ready(result)
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned")
            .recover_definitions();
        std::future::ready(result)
    }
}

impl<S> AsyncProjectionStore for ImmediateProjectionStore<S>
where
    S: ProjectionStore + Send,
{
    fn supports_gates(&self) -> bool {
        self.store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .supports_gates()
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .ensure_shard(&definition);
        std::future::ready(result)
    }

    fn admit_mutation(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .admit_mutation(&shard);
        std::future::ready(result)
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        _now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .index_validate_push(&shard, &items);
        std::future::ready(result)
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<bool>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .pause_blocks_intake(&shard);
        std::future::ready(result)
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .apply_live_owned(positions, commands);
        std::future::ready(result)
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .apply_recovery(&positions, &commands);
        std::future::ready(result)
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .eligible_candidates(&shard, now, max);
        std::future::ready(result)
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .select_item_claim(&shard, &compatibility, now, max);
        std::future::ready(result)
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: ClaimUnit,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl Future<Output = EngineResult<RichClaimSelection>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .select_rich_claim(&shard, unit, &compatibility, now, max_items);
        std::future::ready(result)
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .render_claimed(&shard, &ids);
        std::future::ready(result)
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<ItemState>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .item_state(&shard, &id);
        std::future::ready(result)
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .item_version(&shard, &id);
        std::future::ready(result)
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .recovery_high_water(&shard);
        std::future::ready(result)
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        let result = self
            .store
            .lock()
            .expect("immediate projection store mutex poisoned")
            .recover_definitions();
        std::future::ready(result)
    }
}

impl<S> AsyncControlPlane for ImmediateControlPlane<S>
where
    S: ControlPlane + Send + Sync,
{
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = self.store.create_queue(definition);
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self.store.queue_definition(&key);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result = self.store.list_queues(&tenant);
        std::future::ready(result)
    }
}

impl<S> AsyncLogStore for BlockingLogStore<S>
where
    S: LogStore + Send + 'static,
{
    fn durability_class(&self) -> DurabilityClass {
        self.store
            .lock()
            .expect("blocking log store mutex poisoned")
            .durability_class()
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.ensure_shard(&shard))
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_operation(move |store: &mut S| store.current_epoch(&shard))
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_operation(move |store: &mut S| store.acquire_epoch(&shard))
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        self.run_operation(move |store: &mut S| store.append(&shard, &commands, expected_epoch))
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        self.run_operation(move |store: &mut S| store.read_from(&shard, from, limit))
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_operation(move |store: &mut S| store.high_water(&shard))
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.set_high_water(&shard, position))
    }

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl Future<Output = EngineResult<SnapshotRef>> + Send {
        self.run_operation(move |store: &mut S| store.write_snapshot(&shard, position, snapshot))
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        self.run_operation(move |store: &mut S| store.latest_snapshot(&shard))
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        self.run_operation(move |store: &mut S| store.read_snapshot(&snapshot_ref))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_operation(move |store: &mut S| store.recover_definitions())
    }
}

impl<S> AsyncProjectionStore for BlockingProjectionStore<S>
where
    S: ProjectionStore + Send + 'static,
{
    fn supports_gates(&self) -> bool {
        self.store
            .lock()
            .expect("blocking projection store mutex poisoned")
            .supports_gates()
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.ensure_shard(&definition))
    }

    fn admit_mutation(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.admit_mutation(&shard))
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        _now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.index_validate_push(&shard, &items))
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<bool>> + Send {
        self.run_operation(move |store: &mut S| store.pause_blocks_intake(&shard))
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.apply_live_owned(positions, commands))
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_operation(move |store: &mut S| store.apply_recovery(&positions, &commands))
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_operation(move |store: &mut S| store.eligible_candidates(&shard, now, max))
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_operation(move |store: &mut S| {
            store.select_item_claim(&shard, &compatibility, now, max)
        })
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: ClaimUnit,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl Future<Output = EngineResult<RichClaimSelection>> + Send {
        self.run_operation(move |store: &mut S| {
            store.select_rich_claim(&shard, unit, &compatibility, now, max_items)
        })
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        self.run_operation(move |store: &mut S| store.render_claimed(&shard, &ids))
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<ItemState>>> + Send {
        self.run_operation(move |store: &mut S| store.item_state(&shard, &id))
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        self.run_operation(move |store: &mut S| store.item_version(&shard, &id))
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_operation(move |store: &mut S| store.recovery_high_water(&shard))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_operation(move |store: &mut S| store.recover_definitions())
    }
}

impl<S> AsyncControlPlane for BlockingControlPlane<S>
where
    S: ControlPlane + Send + 'static,
{
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        self.run_operation(move |store: &mut S| store.create_queue(definition))
    }

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        self.run_operation(move |store: &mut S| store.queue_definition(&key))
    }

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl Future<Output = EngineResult<Vec<QueueId>>> + Send {
        self.run_operation(move |store: &mut S| store.list_queues(&tenant))
    }
}

#[cfg(test)]
mod tests {
    // The concrete `Ready` return types make these compile-time assertions fail if a future ceases to be
    // `Send`; production implementations remain free to return their own opaque future types.
    #![allow(refining_impl_trait)]

    use std::future::{Ready, ready};

    use pqueue_core::{
        BodyHash, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId,
    };

    use super::*;
    use crate::{EngineError, QueueKey};

    struct ImmediateLog;

    impl AsyncLogStore for ImmediateLog {
        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::Atomic
        }

        fn ensure_shard(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn current_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(0))
        }

        fn acquire_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(1))
        }

        fn append(
            &self,
            _shard: QueueKey,
            _commands: Vec<CommandEnvelope>,
            _expected_epoch: u64,
        ) -> Ready<EngineResult<Vec<CommandPosition>>> {
            ready(Ok(Vec::new()))
        }

        fn read_from(
            &self,
            _shard: QueueKey,
            _from: Option<CommandPosition>,
            _limit: usize,
        ) -> Ready<EngineResult<CommandPage>> {
            ready(Ok(CommandPage {
                entries: Vec::new(),
                next: None,
            }))
        }

        fn high_water(&self, _shard: QueueKey) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Ok(None))
        }

        fn set_high_water(
            &self,
            _shard: QueueKey,
            _position: CommandPosition,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }
    }

    struct ImmediateProjection;

    impl AsyncProjectionStore for ImmediateProjection {
        fn ensure_shard(&self, _definition: QueueDefinition) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn admit_mutation(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn apply_live(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn apply_recovery(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }

        fn eligible_candidates(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            ready(Ok(Vec::new()))
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _ids: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<ClaimedItem>>> {
            ready(Ok(Vec::new()))
        }

        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<ItemState>>> {
            ready(Ok(None))
        }

        fn item_version(&self, _shard: QueueKey, _id: ItemId) -> Ready<EngineResult<Option<u64>>> {
            ready(Ok(None))
        }

        fn recovery_high_water(
            &self,
            _shard: QueueKey,
        ) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Ok(None))
        }

        fn recover_definitions(&self) -> Ready<EngineResult<Vec<QueueDefinition>>> {
            ready(Ok(Vec::new()))
        }
    }

    struct ImmediateControl;

    impl AsyncControlPlane for ImmediateControl {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> Ready<EngineResult<CreateQueueOutcome>> {
            ready(Err(EngineError::Unavailable))
        }

        fn queue_definition(&self, _key: QueueKey) -> Ready<EngineResult<QueueDefinition>> {
            ready(Err(EngineError::NotFound))
        }

        fn list_queues(&self, _tenant: TenantId) -> Ready<EngineResult<Vec<QueueId>>> {
            ready(Ok(Vec::new()))
        }
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn definition() -> QueueDefinition {
        QueueDefinition {
            tenant_id: shard().tenant_id,
            queue_id: shard().queue_id,
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn assert_send<T: Send>(_: T) {}

    struct OwnedWholeOperation {
        value: String,
    }

    impl BlockingStoreOperation<Vec<String>> for OwnedWholeOperation {
        type Output = usize;

        fn run(self, store: &mut Vec<String>) -> EngineResult<Self::Output> {
            store.push(self.value);
            Ok(store.len())
        }
    }

    #[test]
    fn every_log_future_is_send() {
        let log = ImmediateLog;
        assert_send(log.ensure_shard(shard()));
        assert_send(log.current_epoch(shard()));
        assert_send(log.acquire_epoch(shard()));
        assert_send(log.append(shard(), Vec::new(), 0));
        assert_send(log.read_from(shard(), None, 1));
        assert_send(log.high_water(shard()));
        assert_send(log.set_high_water(shard(), CommandPosition::new(shard(), 0, 0)));
        assert_send(log.write_snapshot(
            shard(),
            CommandPosition::new(shard(), 0, 0),
            ProjectionSnapshot {
                payload: Vec::new(),
            },
        ));
        assert_send(log.latest_snapshot(shard()));
        assert_send(log.read_snapshot(SnapshotRef {
            queue: shard(),
            position: CommandPosition::new(shard(), 0, 0),
            ref_id: "snapshot".to_string(),
        }));
        assert_send(log.snapshot_at_or_before(shard(), CommandPosition::new(shard(), 0, 0)));
        assert_send(log.recover_definitions());
    }

    #[test]
    fn every_projection_future_is_send() {
        let projection = ImmediateProjection;
        assert_send(projection.ensure_shard(definition()));
        assert_send(projection.admit_mutation(shard()));
        assert_send(projection.validate_push(
            shard(),
            Vec::new(),
            UtcTimestamp::new(0, 0).unwrap(),
        ));
        assert_send(projection.pause_blocks_intake(shard()));
        assert_send(projection.push_idempotency(
            shard(),
            RequestId::new("request").unwrap(),
            PushFingerprint {
                canonical_sha256: [1; 32],
                legacy_body_hash: BodyHash(1),
            },
            UtcTimestamp::new(0, 0).unwrap(),
        ));
        assert_send(projection.apply_live(Vec::new(), Vec::new()));
        assert_send(projection.apply_recovery(Vec::new(), Vec::new()));
        assert_send(projection.expired_leases(shard(), UtcTimestamp::new(0, 0).unwrap(), 16));
        assert_send(projection.eligible_candidates(shard(), UtcTimestamp::new(0, 0).unwrap(), 1));
        assert_send(projection.select_rich_claim(
            shard(),
            ClaimUnit::SameGroupKey,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            UtcTimestamp::new(0, 0).unwrap(),
            1,
        ));
        let id = ItemId::new("1").unwrap();
        assert_send(projection.render_claimed(shard(), vec![id]));
        assert_send(projection.item_state(shard(), ItemId::new("1").unwrap()));
        assert_send(projection.item_version(shard(), ItemId::new("1").unwrap()));
        assert_send(projection.recovery_high_water(shard()));
        assert_send(projection.recover_definitions());
    }

    #[test]
    fn default_rich_claim_selection_fails_closed() {
        let result = futures::executor::block_on(ImmediateProjection.select_rich_claim(
            shard(),
            ClaimUnit::WholeCohort,
            ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
            UtcTimestamp::new(0, 0).unwrap(),
            10,
        ));
        assert!(matches!(result, Err(EngineError::Unavailable)));
    }

    #[test]
    fn default_push_preappend_capabilities_fail_closed() {
        let projection = ImmediateProjection;
        assert!(matches!(
            futures::executor::block_on(projection.validate_push(
                shard(),
                Vec::new(),
                UtcTimestamp::new(0, 0).unwrap(),
            )),
            Err(EngineError::Unavailable)
        ));
        assert!(matches!(
            futures::executor::block_on(projection.pause_blocks_intake(shard())),
            Err(EngineError::Unavailable)
        ));
        assert!(matches!(
            futures::executor::block_on(projection.push_idempotency(
                shard(),
                RequestId::new("request").unwrap(),
                PushFingerprint {
                    canonical_sha256: [1; 32],
                    legacy_body_hash: BodyHash(1)
                },
                UtcTimestamp::new(0, 0).unwrap(),
            )),
            Err(EngineError::Unavailable)
        ));
    }

    #[test]
    fn every_control_plane_future_is_send() {
        let control = ImmediateControl;
        assert_send(control.create_queue(definition()));
        assert_send(control.queue_definition(shard()));
        assert_send(control.list_queues(TenantId::new("tenant").unwrap()));
    }

    #[test]
    fn blocking_adapter_accepts_owned_whole_operation() {
        let adapter = BlockingProjectionStore::new(Vec::<String>::new(), 1).unwrap();
        let future = adapter.run_operation(OwnedWholeOperation {
            value: "committed transaction".to_string(),
        });
        assert_send(future);

        let len = futures::executor::block_on(adapter.run_operation(OwnedWholeOperation {
            value: "committed transaction".to_string(),
        }))
        .unwrap();
        assert_eq!(len, 1);
    }
}
