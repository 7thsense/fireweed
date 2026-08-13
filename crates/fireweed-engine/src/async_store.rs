//! Runtime-neutral asynchronous storage axes.
//!
//! # Dual-stack collapse (program B)
//!
//! Sync [`crate::LogStore`] / [`crate::ProjectionStore`] remain axis traits under in-process bridges;
//! product composition is [`crate::AsyncLogReplayBackend`]. Legacy sync `ComposedBackend` was removed.
//! deletion. These async traits + [`crate::AsyncComposedBackend`] are the sole remaining composition
//! polarity. Until Gates B pass, product code may still use the sync stack; **no new sync features**.
//!
//! ## B1 parity inventory (exit: product cells expressible on async alone)
//!
//! Compared to sync `LogStore` (~38 methods), `AsyncLogStore` still needs product coverage for:
//! - `is_durable_log`
//! - `append_serialized` (admission-boundary encoding)
//! - definition persistence / `recover_definitions_page` / `persist_definition` / `create_or_read_definition`
//! - retention: `retention_floor`, `advance_retention_floor`, `max_trimmable_seq_before`,
//!   `expire_segments_through_bounded`, branch pin / `gc_orphaned_branches_bounded`
//! - maintenance: `maintenance_owner_epoch`, `supports_objectlog_maintenance`, `detached_maintenance`
//! - emission cursor: `emission_cursor`, `supports_emission_cursor`, `set_emission_cursor`, `current_position`
//! - group-commit facet: `supports_group_commit`, `gc_enqueue*`, `gc_seal`, `gc_flush_due`,
//!   `gc_advance_high_water`, `gc_max_latency_ms`
//!
//! Compared to sync `ProjectionStore` (~73 methods), `AsyncProjectionStore` still needs coverage for
//! (non-exhaustive): live reads used by product ports (`peek`/`pending`/`metrics`/… via composition),
//! `apply`/`apply_live_owned` variants already partial, index validate/query, batch update + item mutation
//! plans, recovery poison/backpressure/lineage, `install_recovery_shard`, flush_deferred, gates support
//! surface already partial, commit_transition / side records, bounded mutation, etc.
//!
//! Compared to sync `ComposedBackend` product ports, `AsyncComposedBackend` / product adapters still need
//! planners/ops for: reschedule, reassign (beyond cohort), SnapshotStore, ItemMutationPort on objectlog,
//! recover on open parity, change-record emission hooks. Objectlog products (fireweed-dd6cbcde) now wire
//! upsert, update_fields, CommitTransitionPort (Strict), BatchUpdatePort, HotProjectionQueryPort, and
//! IndexQueryPort via `fireweed_objectlog::port_surface` (parity with `AsyncLogReplayBackend`).
//!
//! Temporary bridges (`InProcessLogStore` / `BlockingLogStore` over sync `LogStore`) exist only until
//! each backend implements async axes natively; object-log must not use BlockingLogStore after program A.
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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use fireweed_core::{
    BodyHash, ItemId, ItemState, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};

use crate::{
    ClaimCompatibility, ClaimRef, ClaimUnit, ClaimedItem, CommandEnvelope, CommandPage,
    CommandPosition, CommitOutcomeEntry, ControlPlane, CreateQueueOutcome, DurabilityClass,
    EngineError, EngineResult, FinalizeTarget, IdempotencyDecision, LogStore, ProjectionSnapshot,
    ProjectionStore, PushFingerprint, PushItem, QueueCommand, QueueIdempotencyCache, QueueKey,
    RenewTarget, RichClaimSelection, SideRecordPage, SnapshotRef, request_expires_at,
};

/// An owned blocking operation over one store instance.
///
/// This is the compatibility boundary for blocking substrates. Callers transfer one complete storage
/// operation into the adapter; they do not hand out per-statement callbacks or borrowed transactions.
pub trait BlockingStoreOperation<S>: Send + 'static {
    type Output: Send + 'static;

    fn run(self, store: &mut S) -> EngineResult<Self::Output>;
}

/// Cloneable admission boundary for synchronous storage work. Waiting operations consume no OS thread;
/// at most `max_in_flight` tasks are started concurrently, and reactor threads never execute the task.
#[derive(Clone)]
pub struct BoundedBlockingExecutor {
    permits: Arc<BlockingPermits>,
}

impl BoundedBlockingExecutor {
    pub fn new(max_in_flight: usize) -> EngineResult<Self> {
        if max_in_flight == 0 {
            return Err(EngineError::Invalid(
                "blocking adapter bound must be nonzero",
            ));
        }
        Ok(Self {
            permits: Arc::new(BlockingPermits::new(max_in_flight)),
        })
    }

    pub fn execute<T, F>(&self, task: F) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        T: Send + 'static,
        F: FnOnce() -> EngineResult<T> + Send + 'static,
    {
        BlockingTaskFuture {
            permits: self.permits.clone(),
            task: Some(Box::new(move || {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)).unwrap_or_else(|_| {
                    Err(EngineError::Storage(
                        "blocking storage operation panicked".to_string(),
                    ))
                })
            })),
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
    waiters: VecDeque<Waker>,
}

impl BlockingPermits {
    fn new(available: usize) -> Self {
        Self {
            inner: Mutex::new(BlockingPermitsInner {
                available,
                waiters: VecDeque::new(),
            }),
        }
    }

    fn acquire_or_park(self: &Arc<Self>, waker: &Waker) -> Option<BlockingPermit> {
        let mut inner = self.inner.lock().expect("blocking permit mutex poisoned");
        if inner.available == 0 {
            if !inner.waiters.iter().any(|queued| queued.will_wake(waker)) {
                inner.waiters.push_back(waker.clone());
            }
            return None;
        }
        inner.available -= 1;
        Some(BlockingPermit {
            permits: self.clone(),
        })
    }

    fn release(&self) {
        let wake = {
            let mut inner = self.inner.lock().expect("blocking permit mutex poisoned");
            inner.available += 1;
            inner.waiters.pop_front()
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

        let Some(permit) = this.permits.acquire_or_park(cx.waker()) else {
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

/// Shared adapter for a synchronous log store.
///
/// By default operations complete on the polling thread (CPU-only / memory axes). When constructed
/// with [`Self::new_with_blocking_offload`], each whole store operation runs on a private
/// [`BoundedBlockingExecutor`] so rusqlite/disk work never stalls a Tokio worker (adapter-local
/// offload — not process-wide `BlockingLibBackend`).
///
/// Durable offload axes also enable **group-commit** on [`AsyncLogStore::append`]: concurrent
/// appends that share `(shard, epoch)` coalesce into one `append_serialized` / FULL fsync
/// (fireweed-2a564ff7 / 10k campaign). Memory / non-durable axes leave group-commit off.
pub struct InProcessLogStore<S> {
    store: Arc<Mutex<S>>,
    executor: Option<BoundedBlockingExecutor>,
    durability_class: DurabilityClass,
    durable_log: bool,
    /// The store keeps the pre-encoded bytes handed to `append_serialized` (see
    /// [`LogStore::retains_serialized_appends`]). Only such a store may be appended bytes-only.
    retains_serialized: bool,
    lock_phase: Arc<LockPhaseCounters>,
    /// Present only when `durable_log && retains_serialized && executor` — batches concurrent
    /// appends. The seal loop carries encoded bytes only, so an axis that cannot consume them
    /// must not group-commit (fireweed-ecf5ee96).
    group_commit: Option<Arc<Mutex<GroupCommitState>>>,
}

/// Waiter + seal accounting for durable log group-commit (fireweed-2a564ff7).
struct GroupCommitState {
    pending: Vec<GroupCommitWaiter>,
    /// A seal loop is scheduled or running on the blocking executor.
    sealer_active: bool,
    /// Completed seal transactions (each is one Immediate + FULL fsync for one shard/epoch group).
    seals_completed: u64,
    /// Individual logical appends that joined a seal (waiters completed).
    appends_completed: u64,
}

struct GroupCommitWaiter {
    shard: QueueKey,
    serialized: Vec<Vec<u8>>,
    expected_epoch: u64,
    result: Arc<Mutex<GroupCommitSlot>>,
}

struct GroupCommitSlot {
    done: Option<EngineResult<Vec<CommandPosition>>>,
    waker: Option<Waker>,
}

struct GroupCommitFuture {
    result: Arc<Mutex<GroupCommitSlot>>,
}

impl Future for GroupCommitFuture {
    type Output = EngineResult<Vec<CommandPosition>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self
            .result
            .lock()
            .expect("group-commit result mutex poisoned");
        if let Some(done) = slot.done.take() {
            return Poll::Ready(done);
        }
        slot.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// Default in-flight bound for adapter-local durable log offload (serialized by the store mutex).
pub const DEFAULT_BLOCKING_AXIS_IN_FLIGHT: usize = 32;

/// Cumulative wait/hold timing for one store-mutex axis (fireweed-77ae7a87 commit-section
/// contention decomposition). `wait` is time a caller spent blocked acquiring the store `Mutex`
/// before its operation could start; `hold` is time spent executing the operation while holding
/// it. Neither includes off-lock work a caller does before/after invoking the axis (queue
/// definition lookup, command validation, response assembly) — that is the residual between a
/// probe's end-to-end wall time and `wait + hold`.
#[derive(Debug, Clone, Copy, Default)]
pub struct LockPhaseSnapshot {
    pub calls: u64,
    pub wait: Duration,
    pub hold: Duration,
}

#[derive(Default)]
struct LockPhaseCounters {
    calls: AtomicU64,
    wait_nanos: AtomicU64,
    hold_nanos: AtomicU64,
}

impl LockPhaseCounters {
    fn record(&self, wait: Duration, hold: Duration) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.wait_nanos
            .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
        self.hold_nanos
            .fetch_add(hold.as_nanos() as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LockPhaseSnapshot {
        LockPhaseSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            wait: Duration::from_nanos(self.wait_nanos.load(Ordering::Relaxed)),
            hold: Duration::from_nanos(self.hold_nanos.load(Ordering::Relaxed)),
        }
    }

    fn reset(&self) {
        self.calls.store(0, Ordering::Relaxed);
        self.wait_nanos.store(0, Ordering::Relaxed);
        self.hold_nanos.store(0, Ordering::Relaxed);
    }
}

impl<S: LogStore> InProcessLogStore<S> {
    pub fn new(store: S) -> Self {
        let durability_class = store.durability_class();
        let durable_log = store.is_durable_log();
        let retains_serialized = store.retains_serialized_appends();
        Self {
            store: Arc::new(Mutex::new(store)),
            executor: None,
            durability_class,
            durable_log,
            retains_serialized,
            lock_phase: Arc::new(LockPhaseCounters::default()),
            group_commit: None,
        }
    }

    /// Same as [`Self::new`] but every async axis op and [`Self::run_with_store`] offloads through a
    /// private bounded blocking executor (spawned OS threads). Use for durable blocking stores
    /// (sqlite) so product ports are non-blocking-under-poll without process-wide BLB.
    ///
    /// When the store is a durable log, enables group-commit on append (fireweed-2a564ff7).
    pub fn new_with_blocking_offload(store: S, max_in_flight: usize) -> EngineResult<Self> {
        let durability_class = store.durability_class();
        let durable_log = store.is_durable_log();
        let retains_serialized = store.retains_serialized_appends();
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: Some(BoundedBlockingExecutor::new(max_in_flight)?),
            durability_class,
            durable_log,
            retains_serialized,
            lock_phase: Arc::new(LockPhaseCounters::default()),
            group_commit: (durable_log && retains_serialized).then(|| {
                Arc::new(Mutex::new(GroupCommitState {
                    pending: Vec::new(),
                    sealer_active: false,
                    seals_completed: 0,
                    appends_completed: 0,
                }))
            }),
        })
    }

    /// Cumulative time callers spent waiting for / holding this axis's store mutex across every
    /// [`Self::run_with_store`] / [`Self::run_with_store_mut`] call since construction or the last
    /// [`Self::reset_lock_phase_stats`] (fireweed-77ae7a87 commit-section contention probe).
    pub fn lock_phase_stats(&self) -> LockPhaseSnapshot {
        self.lock_phase.snapshot()
    }

    /// Zero the cumulative lock-phase counters so a probe can bracket one measurement window.
    pub fn reset_lock_phase_stats(&self) {
        self.lock_phase.reset();
    }

    /// Group-commit seal / append counters when group-commit is enabled (`None` otherwise).
    /// `(seals_completed, appends_completed)` — under load `seals < appends` proves coalescing.
    pub fn group_commit_stats(&self) -> Option<(u64, u64)> {
        self.group_commit.as_ref().map(|gc| {
            let g = gc.lock().expect("group-commit mutex poisoned");
            (g.seals_completed, g.appends_completed)
        })
    }

    /// Zero group-commit counters (tests / probes).
    pub fn reset_group_commit_stats(&self) {
        if let Some(gc) = &self.group_commit {
            let mut g = gc.lock().expect("group-commit mutex poisoned");
            g.seals_completed = 0;
            g.appends_completed = 0;
        }
    }

    /// Whether this axis may be appended pre-encoded bytes **without** the envelopes
    /// ([`LogStore::retains_serialized_appends`]). Callers that encode off-lock must branch on this:
    /// `true` → [`Self::append_encoded`], `false` → [`Self::append_owned`].
    pub fn retains_serialized_appends(&self) -> bool {
        self.retains_serialized
    }

    /// Append envelopes plus their pre-encoded bytes, handing the envelopes back so the caller can
    /// apply them by move.
    ///
    /// This is the correct form for axes that do NOT retain the encoded bytes (in-memory log,
    /// object log): they append from `commands`, so the envelopes must travel with the bytes. The
    /// move round-trip keeps the commit path clone-free (fireweed-ecf5ee96 / fireweed-9d2281f0).
    pub fn append_owned(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<(Vec<CommandPosition>, Vec<CommandEnvelope>)>> + Send + 'static
    where
        S: Send + 'static,
    {
        let this_store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        async move {
            let run = move || {
                let wait_start = Instant::now();
                let mut guard = this_store
                    .lock()
                    .expect("immediate log store mutex poisoned");
                let wait = wait_start.elapsed();
                let hold_start = Instant::now();
                let result =
                    guard.append_serialized(&shard, &commands, serialized, expected_epoch);
                lock_phase.record(wait, hold_start.elapsed());
                result.map(|positions| (positions, commands))
            };
            if let Some(executor) = executor {
                executor.execute(run).await
            } else {
                run()
            }
        }
    }

    /// Append pre-encoded JSON envelopes without consuming CommandEnvelopes so apply can
    /// take ownership of the same batch (kills double clone on atomic commit / 10k path).
    ///
    /// Only valid for axes where [`Self::retains_serialized_appends`] is true — the envelopes are
    /// never handed to the store, so any other axis would append nothing.
    pub fn append_encoded(
        &self,
        shard: QueueKey,
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send + 'static
    where
        S: Send + 'static,
    {
        let group_commit = self.group_commit.clone();
        let this_store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        let retains_serialized = self.retains_serialized;
        async move {
            if !retains_serialized {
                // Fail loudly instead of appending nothing: this axis derives its append from the
                // envelopes, which the bytes-only form does not carry (fireweed-ecf5ee96).
                return Err(EngineError::Storage(
                    "append_encoded requires a log that retains serialized appends; \
                     use append_owned"
                        .to_string(),
                ));
            }
            if let Some(gc) = group_commit {
                let executor = executor.expect("group-commit requires offload executor");
                let slot = Arc::new(Mutex::new(GroupCommitSlot {
                    done: None,
                    waker: None,
                }));
                let waiter = GroupCommitWaiter {
                    shard,
                    serialized,
                    expected_epoch,
                    result: Arc::clone(&slot),
                };
                let is_leader = {
                    let mut state = gc.lock().expect("group-commit mutex poisoned");
                    state.pending.push(waiter);
                    if !state.sealer_active {
                        state.sealer_active = true;
                        true
                    } else {
                        false
                    }
                };
                if is_leader {
                    let gc_seal = Arc::clone(&gc);
                    let store_seal = Arc::clone(&this_store);
                    let lock_phase_seal = Arc::clone(&lock_phase);
                    executor
                        .execute(move || {
                            seal_group_commit_loop::<S>(gc_seal, store_seal, lock_phase_seal);
                            Ok(())
                        })
                        .await?;
                }
                GroupCommitFuture { result: slot }.await
            } else {
                let run = move || {
                    let wait_start = Instant::now();
                    let mut guard = this_store
                        .lock()
                        .expect("immediate log store mutex poisoned");
                    let wait = wait_start.elapsed();
                    let hold_start = Instant::now();
                    let result =
                        guard.append_serialized(&shard, &[], serialized, expected_epoch);
                    lock_phase.record(wait, hold_start.elapsed());
                    result
                };
                if let Some(executor) = executor {
                    executor.execute(run).await
                } else {
                    run()
                }
            }
        }
    }

    /// Run a synchronous read against the underlying log (open/recover/tests). Blocks the caller;
    /// prefer [`Self::run_with_store`] on async product paths when offload is configured.
    pub fn with_store<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        let store = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned");
        f(&*store)
    }

    /// Run a synchronous mutation against the underlying log. Blocks the caller.
    pub fn with_store_mut<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        let mut store = self
            .store
            .lock()
            .expect("immediate log store mutex poisoned");
        f(&mut *store)
    }

    /// Async read against the underlying log. Offloads when constructed with blocking offload.
    pub fn run_with_store<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&S) -> EngineResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        async move {
            if let Some(executor) = executor {
                executor
                    .execute(move || {
                        let wait_start = Instant::now();
                        let store = store.lock().expect("immediate log store mutex poisoned");
                        let wait = wait_start.elapsed();
                        let hold_start = Instant::now();
                        let result = operation(&*store);
                        lock_phase.record(wait, hold_start.elapsed());
                        result
                    })
                    .await
            } else {
                let wait_start = Instant::now();
                let store = store.lock().expect("immediate log store mutex poisoned");
                let wait = wait_start.elapsed();
                let hold_start = Instant::now();
                let result = operation(&*store);
                lock_phase.record(wait, hold_start.elapsed());
                result
            }
        }
    }

    /// Async mutation against the underlying log. Offloads when constructed with blocking offload.
    pub fn run_with_store_mut<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        async move {
            if let Some(executor) = executor {
                executor
                    .execute(move || {
                        let wait_start = Instant::now();
                        let mut store = store.lock().expect("immediate log store mutex poisoned");
                        let wait = wait_start.elapsed();
                        let hold_start = Instant::now();
                        let result = operation(&mut *store);
                        lock_phase.record(wait, hold_start.elapsed());
                        result
                    })
                    .await
            } else {
                let wait_start = Instant::now();
                let mut store = store.lock().expect("immediate log store mutex poisoned");
                let wait = wait_start.elapsed();
                let hold_start = Instant::now();
                let result = operation(&mut *store);
                lock_phase.record(wait, hold_start.elapsed());
                result
            }
        }
    }
}

/// Drain pending group-commit waiters until empty. One Immediate+fsync per (shard, epoch) group.
fn seal_group_commit_loop<S: LogStore>(
    gc: Arc<Mutex<GroupCommitState>>,
    store: Arc<Mutex<S>>,
    lock_phase: Arc<LockPhaseCounters>,
) {
    loop {
        let batch = {
            let mut state = gc.lock().expect("group-commit mutex poisoned");
            if state.pending.is_empty() {
                state.sealer_active = false;
                return;
            }
            std::mem::take(&mut state.pending)
        };

        // Group waiters by (shard, epoch) preserving first-seen order of keys.
        let mut groups: HashMap<(QueueKey, u64), Vec<GroupCommitWaiter>> = HashMap::new();
        let mut order: Vec<(QueueKey, u64)> = Vec::new();
        for w in batch {
            let key = (w.shard.clone(), w.expected_epoch);
            if !groups.contains_key(&key) {
                order.push(key.clone());
            }
            groups.entry(key).or_default().push(w);
        }

        for key in order {
            let mut waiters = groups.remove(&key).expect("group present");
            let (shard, expected_epoch) = key;
            let waiter_count = waiters.len() as u64;
            let counts: Vec<usize> = waiters.iter().map(|w| w.serialized.len()).collect();

            // Move envelopes (avoid clone); single-waiter path is the common open_sqlite case
            // under the queue-local admit permit (only one commit in flight per queue).
            let combined: Vec<Vec<u8>> = if waiters.len() == 1 {
                std::mem::take(&mut waiters[0].serialized)
            } else {
                let mut combined = Vec::with_capacity(counts.iter().sum());
                for w in &mut waiters {
                    combined.extend(std::mem::take(&mut w.serialized));
                }
                combined
            };

            let wait_start = Instant::now();
            let mut guard = store.lock().expect("immediate log store mutex poisoned");
            let wait = wait_start.elapsed();
            let hold_start = Instant::now();
            // commands empty: SqliteLog append_serialized uses serialized only.
            let outcome = guard.append_serialized(&shard, &[], combined, expected_epoch);
            lock_phase.record(wait, hold_start.elapsed());
            drop(guard);

            {
                let mut state = gc.lock().expect("group-commit mutex poisoned");
                state.seals_completed = state.seals_completed.saturating_add(1);
                state.appends_completed = state.appends_completed.saturating_add(waiter_count);
            }

            match outcome {
                Ok(positions) => {
                    let mut offset = 0usize;
                    for (w, n) in waiters.into_iter().zip(counts) {
                        let slice = positions
                            .get(offset..offset + n)
                            .map(|s| s.to_vec())
                            .unwrap_or_default();
                        offset += n;
                        complete_group_commit_waiter(w.result, Ok(slice));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    for w in waiters {
                        complete_group_commit_waiter(
                            w.result,
                            Err(EngineError::Storage(msg.clone())),
                        );
                    }
                }
            }
        }
    }
}

fn complete_group_commit_waiter(
    slot: Arc<Mutex<GroupCommitSlot>>,
    result: EngineResult<Vec<CommandPosition>>,
) {
    let waker = {
        let mut s = slot.lock().expect("group-commit result mutex poisoned");
        s.done = Some(result);
        s.waker.take()
    };
    if let Some(w) = waker {
        w.wake();
    }
}

/// Default retention when recording push request-ids from apply envelopes without a queue definition.
const IN_PROCESS_PUSH_IDEM_RETENTION_MS: u64 = 86_400_000;

/// Type-erased lock strategy behind [`InProcessProjectionStore`] (fireweed-7b74ceac). Hidden behind
/// a trait object so the store's field type — and therefore `InProcessProjectionStore<S>` itself —
/// stays identical regardless of which variant backs it; callers (e.g.
/// `AsyncLogReplayBackend<L, P>`) never need a second generic parameter to pick a lock strategy.
///
/// [`Mutex`] implements this exclusively (works for any `S: Send`, including connection-backed axes
/// that are not `Sync`-safe). [`RwLock`] implements it concurrently (`S: Send + Sync` required by the
/// `impl`, matching `RwLock<S>`'s own `Sync` bound) so [`Self::lock_read`] callers run in parallel
/// with each other and only serialize against [`Self::lock_write`].
trait StoreLockOps<S>: Send + Sync {
    fn lock_read<'a>(&'a self) -> Box<dyn std::ops::Deref<Target = S> + 'a>;
    fn lock_write<'a>(&'a self) -> Box<dyn std::ops::DerefMut<Target = S> + 'a>;
}

struct ExclusiveReadGuard<'a, S>(std::sync::MutexGuard<'a, S>);

impl<S> std::ops::Deref for ExclusiveReadGuard<'_, S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.0
    }
}

struct ExclusiveWriteGuard<'a, S>(std::sync::MutexGuard<'a, S>);

impl<S> std::ops::Deref for ExclusiveWriteGuard<'_, S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.0
    }
}

impl<S> std::ops::DerefMut for ExclusiveWriteGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.0
    }
}

impl<S: Send + 'static> StoreLockOps<S> for Mutex<S> {
    fn lock_read<'a>(&'a self) -> Box<dyn std::ops::Deref<Target = S> + 'a> {
        Box::new(ExclusiveReadGuard(
            self.lock().expect("immediate projection store mutex poisoned"),
        ))
    }

    fn lock_write<'a>(&'a self) -> Box<dyn std::ops::DerefMut<Target = S> + 'a> {
        Box::new(ExclusiveWriteGuard(
            self.lock().expect("immediate projection store mutex poisoned"),
        ))
    }
}

struct SharedReadGuard<'a, S>(std::sync::RwLockReadGuard<'a, S>);

impl<S> std::ops::Deref for SharedReadGuard<'_, S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.0
    }
}

struct SharedWriteGuard<'a, S>(std::sync::RwLockWriteGuard<'a, S>);

impl<S> std::ops::Deref for SharedWriteGuard<'_, S> {
    type Target = S;
    fn deref(&self) -> &S {
        &self.0
    }
}

impl<S> std::ops::DerefMut for SharedWriteGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut S {
        &mut self.0
    }
}

impl<S: Send + Sync + 'static> StoreLockOps<S> for RwLock<S> {
    fn lock_read<'a>(&'a self) -> Box<dyn std::ops::Deref<Target = S> + 'a> {
        Box::new(SharedReadGuard(
            self.read().expect("immediate projection store lock poisoned"),
        ))
    }

    fn lock_write<'a>(&'a self) -> Box<dyn std::ops::DerefMut<Target = S> + 'a> {
        Box::new(SharedWriteGuard(
            self.write().expect("immediate projection store lock poisoned"),
        ))
    }
}

/// Shared adapter for a synchronous projection store.
///
/// Default construction ([`Self::new`] / [`Self::new_with_blocking_offload`]) keeps the historical
/// **funnel** behavior (fireweed-451a6b23): every axis op — reads and writes alike — funnels through
/// one exclusive `Mutex<S>`, so [`Self::run_with_store`] (shared borrow) and
/// [`Self::run_with_store_mut`] (exclusive borrow) both serialize against each other. A point read
/// (`query_index*`, `live_item*`, `item_state`, …) queues behind another worker's concurrent commit
/// (`apply_live`, `admit_mutation`, …) and vice versa. A pure-commit workload never sees this
/// (fireweed-77ae7a87's commit-section probe measured this mutex flat at ~0.14 ms/entry, w=1..8),
/// but interleaving point reads with commits — the realistic shape snorri's ladder drives — inflates
/// commit-span latency well beyond that (mixed-op funnel probe, `sqlite_mixed_op_funnel_probe.rs`).
///
/// [`Self::new_with_concurrent_reads`] (fireweed-7b74ceac) opts into a `RwLock<S>` instead: reads run
/// concurrently with each other and only serialize against an in-flight write. Gated on `S: Sync` —
/// use only for backing stores that are genuinely safe for concurrent shared access (e.g.
/// `InMemoryProjection`); connection-backed axes (`SqliteRelational`, `PostgresRelational`, …) are not
/// `Sync`-safe and must keep using the exclusive constructors.
pub struct InProcessProjectionStore<S> {
    store: Arc<dyn StoreLockOps<S>>,
    executor: Option<BoundedBlockingExecutor>,
    supports_gates: bool,
    /// Per-shard push request-id cache (parity with `AsyncLogReplayBackend` / sync composition).
    push_idempotency: Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>>,
    lock_phase: Arc<LockPhaseCounters>,
}

impl<S: ProjectionStore> InProcessProjectionStore<S> {
    pub fn new(store: S) -> Self
    where
        S: Send + 'static,
    {
        let supports_gates = store.supports_gates();
        let store: Arc<dyn StoreLockOps<S>> = Arc::new(Mutex::new(store));
        Self {
            store,
            executor: None,
            supports_gates,
            push_idempotency: Arc::new(Mutex::new(HashMap::new())),
            lock_phase: Arc::new(LockPhaseCounters::default()),
        }
    }

    /// Same as [`Self::new`] but backed by a [`RwLock`] instead of a [`Mutex`]: concurrent
    /// [`Self::run_with_store`] / [`Self::with_store`] reads no longer serialize behind each other or
    /// behind another caller's in-flight [`Self::run_with_store_mut`] wait — only writers exclude
    /// readers and each other, exactly as `std::sync::RwLock` guarantees. Requires `S: Sync`.
    pub fn new_with_concurrent_reads(store: S) -> Self
    where
        S: Send + Sync + 'static,
    {
        let supports_gates = store.supports_gates();
        let store: Arc<dyn StoreLockOps<S>> = Arc::new(RwLock::new(store));
        Self {
            store,
            executor: None,
            supports_gates,
            push_idempotency: Arc::new(Mutex::new(HashMap::new())),
            lock_phase: Arc::new(LockPhaseCounters::default()),
        }
    }

    /// Durable / blocking projection axis: adapter-local whole-op offload.
    pub fn new_with_blocking_offload(store: S, max_in_flight: usize) -> EngineResult<Self>
    where
        S: Send + 'static,
    {
        let supports_gates = store.supports_gates();
        let store: Arc<dyn StoreLockOps<S>> = Arc::new(Mutex::new(store));
        Ok(Self {
            store,
            executor: Some(BoundedBlockingExecutor::new(max_in_flight)?),
            supports_gates,
            push_idempotency: Arc::new(Mutex::new(HashMap::new())),
            lock_phase: Arc::new(LockPhaseCounters::default()),
        })
    }

    /// Cumulative time callers spent waiting for / holding this axis's store mutex (fireweed-77ae7a87).
    pub fn lock_phase_stats(&self) -> LockPhaseSnapshot {
        self.lock_phase.snapshot()
    }

    /// Zero the cumulative lock-phase counters so a probe can bracket one measurement window.
    pub fn reset_lock_phase_stats(&self) {
        self.lock_phase.reset();
    }

    /// Synchronous read (open/recover/tests). Blocks the caller.
    pub fn with_store<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        let guard = self.store.lock_read();
        f(&**guard)
    }

    /// Rebuild process-local push request-id maps from durable log envelopes.
    ///
    /// Snapshot-tail open only runs [`AsyncProjectionStore::apply_recovery`] on the tail, so
    /// historical `Push` markers never re-enter that path. Callers that keep request-id
    /// authority on a separate log must rehydrate after open so lost-response retries replay.
    pub fn rehydrate_push_idempotency(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        retention_ms: u64,
    ) -> EngineResult<()> {
        let mut cache = self
            .push_idempotency
            .lock()
            .expect("push idempotency mutex poisoned");
        for env in commands {
            let Some(request_id) = env.request_id.clone() else {
                continue;
            };
            let QueueCommand::Push(push) = &env.command else {
                continue;
            };
            let fingerprint = match env.request_fingerprint {
                Some(fp) => BodyHash(fp),
                None => crate::compose::push_item_body_hash(&push.items)?,
            };
            let expires_at = request_expires_at(env.created_at, retention_ms);
            let ids = match &env.request_outcome {
                Some(crate::RequestOutcome::Push { item_ids }) => item_ids.clone(),
                _ => env.item_ids.clone(),
            };
            cache.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids,
                expires_at,
            );
        }
        Ok(())
    }

    /// Synchronous mutation. Blocks the caller.
    pub fn with_store_mut<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        let mut guard = self.store.lock_write();
        f(&mut **guard)
    }

    /// Async read; offloads when constructed with blocking offload. Runs concurrently with other
    /// readers when constructed via [`Self::new_with_concurrent_reads`]; otherwise serializes against
    /// every other op class exactly like [`Self::run_with_store_mut`] (fireweed-451a6b23 funnel).
    pub fn run_with_store<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&S) -> EngineResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        let run = move || {
            let wait_start = Instant::now();
            let guard = store.lock_read();
            let wait = wait_start.elapsed();
            let hold_start = Instant::now();
            let result = operation(&**guard);
            lock_phase.record(wait, hold_start.elapsed());
            result
        };
        async move {
            if let Some(executor) = executor {
                executor.execute(run).await
            } else {
                run()
            }
        }
    }

    /// Async mutation; offloads when constructed with blocking offload. Always exclusive: excludes
    /// every concurrent reader and writer alike, regardless of lock strategy.
    pub fn run_with_store_mut<T, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        let run = move || {
            let wait_start = Instant::now();
            let mut guard = store.lock_write();
            let wait = wait_start.elapsed();
            let hold_start = Instant::now();
            let result = operation(&mut **guard);
            lock_phase.record(wait, hold_start.elapsed());
            result
        };
        async move {
            if let Some(executor) = executor {
                executor.execute(run).await
            } else {
                run()
            }
        }
    }
}

/// Bounded blocking adapter for a synchronous log store.
pub struct BlockingLogStore<S> {
    store: Arc<Mutex<S>>,
    executor: BoundedBlockingExecutor,
    durability_class: DurabilityClass,
    durable_log: bool,
}

impl<S: LogStore> BlockingLogStore<S> {
    pub fn new(store: S, max_in_flight: usize) -> EngineResult<Self> {
        let durability_class = store.durability_class();
        let durable_log = store.is_durable_log();
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
            durability_class,
            durable_log,
        })
    }

    /// Execute one caller-defined, owned transaction operation on the blocking store.
    ///
    /// Unlike the adapter's private single-axis compatibility calls, this public boundary accepts only a
    /// named [`BlockingStoreOperation`]. Closures deliberately do not implement that trait, preventing a
    /// caller from accidentally offloading individual statements of one transaction.
    pub fn run_owned_operation<O>(
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

    fn run_sync<T, F>(&self, operation: F) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store.lock().expect("blocking log store mutex poisoned");
            operation(&mut *store)
        })
    }
}

/// Bounded blocking adapter for a synchronous projection store.
pub struct BlockingProjectionStore<S> {
    store: Arc<Mutex<S>>,
    executor: BoundedBlockingExecutor,
    supports_gates: bool,
}

impl<S: ProjectionStore> BlockingProjectionStore<S> {
    pub fn new(store: S, max_in_flight: usize) -> EngineResult<Self> {
        let supports_gates = store.supports_gates();
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            executor: BoundedBlockingExecutor::new(max_in_flight)?,
            supports_gates,
        })
    }

    pub fn run_owned_operation<O>(
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

    fn run_sync<T, F>(&self, operation: F) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store
                .lock()
                .expect("blocking projection store mutex poisoned");
            operation(&mut *store)
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

    pub fn run_owned_operation<O>(
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

    fn run_sync<T, F>(&self, operation: F) -> impl Future<Output = EngineResult<T>> + Send + 'static
    where
        S: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut S) -> EngineResult<T> + Send + 'static,
    {
        let store = self.store.clone();
        self.executor.execute(move || {
            let mut store = store.lock().expect("blocking control plane mutex poisoned");
            operation(&mut *store)
        })
    }
}

/// Projection-sealed retry inputs for one ordinary leased item.
///
/// The projection must return one member for each requested target in the same order. Both counters
/// are item-scoped: `attempt_count` is deliveries so far, and `max_attempts` is the bound stored on
/// the item at push (which may be tighter than the queue default). Renderers populate
/// [`ClaimedItem::max_attempts`]; the default finalize_validate path prefers that value and only
/// falls back to the queue default when the renderer left it unset (`0`).
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

    /// Whether this log retains commands across process death (ADR-013 Class A).
    ///
    /// Default `true`. In-process / Class B logs return `false`.
    fn is_durable_log(&self) -> bool {
        true
    }

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

    /// Resolve a retained vectorized commit from durable projection authority.
    fn replay_durable_commit(
        &self,
        _shard: QueueKey,
        _request_id: RequestId,
        _fingerprint: u64,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Validate all vectorized-commit leases against one projection image.
    fn commit_validate(
        &self,
        _shard: QueueKey,
        _claim_refs: Vec<ClaimRef>,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn instance_fence(
        &self,
        _shard: QueueKey,
        _key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn index_validate_push(
        &self,
        _shard: QueueKey,
        _items: Vec<PushItem>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn read_durable_commit(
        &self,
        _shard: QueueKey,
        _request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }

    fn side_record(
        &self,
        _shard: QueueKey,
        _key: Vec<u8>,
    ) -> impl std::future::Future<Output = EngineResult<Option<bytes::Bytes>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }

    /// Paged, key-ascending scan of opaque side records whose key starts with `prefix` (bead
    /// fireweed-e47e9287; see [`SideRecordPage`]).
    fn side_records_by_prefix(
        &self,
        _shard: QueueKey,
        _prefix: Vec<u8>,
        _page_size: usize,
        _cursor: Option<Vec<u8>>,
    ) -> impl std::future::Future<Output = EngineResult<SideRecordPage>> + Send {
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

    /// Resolve item-id lifecycle targets while preserving projection-owned rejection precedence.
    /// Implementations with a lock or transaction should override this so validation and rendering
    /// observe one projection image.
    ///
    /// Default precedence when a rendered lease set is incomplete (mirrors
    /// projection `validate_leased`): absent → [`EngineError::NotFound`], terminal →
    /// [`EngineError::Terminal`], otherwise → [`EngineError::StaleLease`].
    fn resolve_lease_targets(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        async move {
            let items = self.render_claimed(shard.clone(), ids.clone()).await?;
            if items.len() == ids.len() {
                return Ok(items);
            }
            for id in ids {
                if items.iter().any(|item| item.item_id == id) {
                    continue;
                }
                match self.item_state(shard.clone(), id).await? {
                    None => return Err(EngineError::NotFound),
                    Some(state) if state.is_terminal() => return Err(EngineError::Terminal),
                    Some(_) => return Err(EngineError::StaleLease),
                }
            }
            Err(EngineError::StaleLease)
        }
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
                        // Prefer the item-scoped bound from the projection image. Fall back to the
                        // queue default only when a renderer has not yet populated max_attempts
                        // (legacy / incomplete ClaimedItem construction).
                        let max_attempts = if item.max_attempts > 0 {
                            item.max_attempts
                        } else {
                            default_max_attempts
                        };
                        Ok(FinalizeLeaseMember {
                            item_id: item.item_id,
                            attempt_count: item.attempt_count,
                            max_attempts,
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

impl<S> AsyncLogStore for InProcessLogStore<S>
where
    S: LogStore + Send + 'static,
{
    fn durability_class(&self) -> DurabilityClass {
        self.durability_class
    }

    fn is_durable_log(&self) -> bool {
        self.durable_log
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store_mut(move |store| store.ensure_shard(&shard))
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_with_store(move |store| store.current_epoch(&shard))
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_with_store_mut(move |store| store.acquire_epoch(&shard))
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        // Durable offload (`open_sqlite`): pre-encode native FWC1 off the exclusive writer lock,
        // then join group-commit so concurrent workers coalesce FULL fsyncs (fireweed-2a564ff7).
        // Memory / non-offload: encode stays inside the store mutex via plain `append`.
        let group_commit = self.group_commit.clone();
        let this_store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        let lock_phase = Arc::clone(&self.lock_phase);
        // Rebuild a thin handle for group-commit / direct paths without cloning the whole self.
        let gc_enabled = group_commit.is_some();
        async move {
            if gc_enabled {
                let serialized = commands
                    .iter()
                    .map(crate::command_codec::encode_command_envelope)
                    .collect::<EngineResult<Vec<_>>>()?;
                // Inline the group-commit path (same as append_via_group_commit) using captured fields.
                let gc = group_commit.expect("gc_enabled");
                let executor = executor.expect("group-commit requires offload executor");
                let slot = Arc::new(Mutex::new(GroupCommitSlot {
                    done: None,
                    waker: None,
                }));
                let waiter = GroupCommitWaiter {
                    shard,
                    serialized,
                    expected_epoch,
                    result: Arc::clone(&slot),
                };
                let is_leader = {
                    let mut state = gc.lock().expect("group-commit mutex poisoned");
                    state.pending.push(waiter);
                    if !state.sealer_active {
                        state.sealer_active = true;
                        true
                    } else {
                        false
                    }
                };
                if is_leader {
                    let gc_seal = Arc::clone(&gc);
                    let store_seal = Arc::clone(&this_store);
                    let lock_phase_seal = Arc::clone(&lock_phase);
                    executor
                        .execute(move || {
                            seal_group_commit_loop::<S>(gc_seal, store_seal, lock_phase_seal);
                            Ok(())
                        })
                        .await?;
                }
                GroupCommitFuture { result: slot }.await
            } else {
                // Non-durable / no offload: mutate under store mutex (possibly via executor).
                let store = Arc::clone(&this_store);
                let executor = executor;
                let lock_phase = Arc::clone(&lock_phase);
                async move {
                    if let Some(executor) = executor {
                        executor
                            .execute(move || {
                                let wait_start = Instant::now();
                                let mut guard =
                                    store.lock().expect("immediate log store mutex poisoned");
                                let wait = wait_start.elapsed();
                                let hold_start = Instant::now();
                                let result =
                                    guard.append(&shard, &commands, expected_epoch);
                                lock_phase.record(wait, hold_start.elapsed());
                                result
                            })
                            .await
                    } else {
                        let wait_start = Instant::now();
                        let mut guard =
                            store.lock().expect("immediate log store mutex poisoned");
                        let wait = wait_start.elapsed();
                        let hold_start = Instant::now();
                        let result = guard.append(&shard, &commands, expected_epoch);
                        lock_phase.record(wait, hold_start.elapsed());
                        result
                    }
                }
                .await
            }
        }
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        self.run_with_store(move |store| store.read_from(&shard, from, limit))
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_with_store(move |store| store.high_water(&shard))
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store_mut(move |store| store.set_high_water(&shard, position))
    }

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl Future<Output = EngineResult<SnapshotRef>> + Send {
        self.run_with_store_mut(move |store| store.write_snapshot(&shard, position, snapshot))
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        self.run_with_store(move |store| store.latest_snapshot(&shard))
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        self.run_with_store(move |store| store.read_snapshot(&snapshot_ref))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_with_store(move |store| store.recover_definitions())
    }
}

impl<S> AsyncProjectionStore for InProcessProjectionStore<S>
where
    S: ProjectionStore + Send + 'static,
{
    fn supports_gates(&self) -> bool {
        self.supports_gates
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store_mut(move |store| store.ensure_shard(&definition))
    }

    fn admit_mutation(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store_mut(move |store| store.admit_mutation(&shard))
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        _now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store(move |store| store.index_validate_push(&shard, &items))
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<bool>> + Send {
        self.run_with_store(move |store| store.pause_blocks_intake(&shard))
    }

    fn push_idempotency(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: PushFingerprint,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send {
        let result = {
            let guard = self
                .push_idempotency
                .lock()
                .expect("push idempotency mutex poisoned");
            Ok(guard
                .get(&shard)
                .map(|c| c.check(&request_id, fingerprint.legacy_body_hash, now))
                .unwrap_or(IdempotencyDecision::Proceed))
        };
        std::future::ready(result)
    }

    fn replay_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send {
        self.run_with_store_mut(move |store| {
            store.replay_durable_commit(&shard, &request_id, fingerprint, now)
        })
    }

    fn commit_validate(
        &self,
        shard: QueueKey,
        claim_refs: Vec<ClaimRef>,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store(move |store| store.commit_validate(&shard, &claim_refs, now))
    }

    fn instance_fence(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        self.run_with_store(move |store| store.instance_fence(&shard, &key))
    }

    fn index_validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_with_store(move |store| store.index_validate_push(&shard, &items))
    }

    fn read_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
    ) -> impl Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send {
        self.run_with_store(move |store| store.read_durable_commit(&shard, &request_id))
    }

    fn side_record(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl Future<Output = EngineResult<Option<bytes::Bytes>>> + Send {
        self.run_with_store(move |store| store.side_record(&shard, &key))
    }

    fn side_records_by_prefix(
        &self,
        shard: QueueKey,
        prefix: Vec<u8>,
        page_size: usize,
        cursor: Option<Vec<u8>>,
    ) -> impl Future<Output = EngineResult<SideRecordPage>> + Send {
        self.run_with_store(move |store| {
            store.side_records_by_prefix(&shard, &prefix, page_size, cursor)
        })
    }

    fn renew_validate(
        &self,
        shard: QueueKey,
        targets: Vec<RenewTarget>,
        _now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let ids = targets.iter().map(|t| t.item_id).collect::<Vec<_>>();
        self.run_with_store(move |store| store.renew_validate(&shard, &ids))
    }

    fn resolve_lease_targets(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        self.run_with_store(move |store| {
            store.renew_validate(&shard, &ids)?;
            let items = store.render_claimed(&shard, &ids)?;
            if items.len() != ids.len() {
                return Err(EngineError::Storage(
                    "validated lease targets were not renderable".into(),
                ));
            }
            Ok(items)
        })
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<FinalizeTarget>,
        _now: UtcTimestamp,
        default_max_attempts: u32,
    ) -> impl Future<Output = EngineResult<Vec<FinalizeLeaseMember>>> + Send {
        self.run_with_store(move |store| {
            let outcomes = targets
                .iter()
                .map(|t| crate::FinalizeOutcome {
                    item_id: t.item_id,
                    kind: t.kind,
                    applied_state: None,
                    not_before: t.not_before,
                })
                .collect::<Vec<_>>();
            store.finalize_validate(&shard, &outcomes)?;
            let ids = targets.iter().map(|t| t.item_id).collect::<Vec<_>>();
            let claimed = store.render_claimed(&shard, &ids)?;
            if claimed.len() != targets.len() {
                return Err(EngineError::StaleLease);
            }
            claimed
                .into_iter()
                .map(|item| {
                    // Prefer the item-scoped bound from the projection image (push can pin a
                    // per-item max_attempts below the queue default). Wrong bound here seals
                    // applied_state=Pending while SQL apply computes Failed → Conflict on
                    // composed log-replay paths (retry_beyond_max_attempts_goes_terminal).
                    let max_attempts = if item.max_attempts > 0 {
                        item.max_attempts
                    } else {
                        default_max_attempts
                    };
                    Ok(FinalizeLeaseMember {
                        item_id: item.item_id,
                        attempt_count: item.attempt_count,
                        max_attempts,
                    })
                })
                .collect()
        })
    }

    fn purge_validate(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        use crate::validate_purge_force;
        self.run_with_store(move |store| {
            let mut present = Vec::new();
            for id in &ids {
                if present.contains(id) {
                    continue;
                }
                if let Some(state) = store.item_state(&shard, id)? {
                    validate_purge_force(state == ItemState::Leased, force)?;
                    present.push(*id);
                }
            }
            Ok(present)
        })
    }

    fn expired_leases(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_with_store(move |store| {
            let mut ids = store.expired_leases(&shard, now)?;
            ids.sort();
            if max > 0 && ids.len() > max {
                ids.truncate(max);
            }
            Ok(ids)
        })
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        let push_idempotency = Arc::clone(&self.push_idempotency);
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        async move {
            let queue = positions.first().map(|p| p.queue.clone());
            // Capture push-idempotency records BEFORE moving envelopes into apply so
            // large Push payloads are not cloned (fireweed-85855781 / 10k-tps path).
            let mut push_idem_records = Vec::new();
            for env in &commands {
                let Some(request_id) = env.request_id.clone() else {
                    continue;
                };
                let QueueCommand::Push(push) = &env.command else {
                    continue;
                };
                let fingerprint = match env.request_fingerprint {
                    Some(fp) => BodyHash(fp),
                    None => crate::compose::push_item_body_hash(&push.items)?,
                };
                let expires_at =
                    request_expires_at(env.created_at, IN_PROCESS_PUSH_IDEM_RETENTION_MS);
                let ids = match &env.request_outcome {
                    Some(crate::RequestOutcome::Push { item_ids }) => item_ids.clone(),
                    _ => env.item_ids.clone(),
                };
                push_idem_records.push((request_id, fingerprint, ids, expires_at));
            }
            let apply = move || {
                let mut store = store.lock_write();
                store.apply_live_owned(positions, commands)?;
                Ok(())
            };
            if let Some(executor) = executor {
                executor.execute(apply).await?
            } else {
                apply()?
            };
            if let Some(queue) = queue {
                let mut cache = push_idempotency
                    .lock()
                    .expect("push idempotency mutex poisoned");
                let entry = cache.entry(queue).or_default();
                for (request_id, fingerprint, ids, expires_at) in push_idem_records {
                    entry.record(request_id, fingerprint, ids, expires_at);
                }
            }
            Ok(())
        }
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        // Mirror apply_live: durable recovery must rebuild process-local push request-id
        // maps so a post-reopen retry replays instead of re-executing (AC-TXN-3 mid-pipeline).
        let push_idempotency = Arc::clone(&self.push_idempotency);
        let store = Arc::clone(&self.store);
        let executor = self.executor.clone();
        async move {
            let queue = positions.first().map(|p| p.queue.clone());
            let apply = move || {
                let mut store = store.lock_write();
                store.apply_recovery(&positions, &commands)?;
                Ok(commands)
            };
            let commands = if let Some(executor) = executor {
                executor.execute(apply).await?
            } else {
                apply()?
            };
            if let Some(queue) = queue {
                let mut cache = push_idempotency
                    .lock()
                    .expect("push idempotency mutex poisoned");
                for env in &commands {
                    let Some(request_id) = env.request_id.clone() else {
                        continue;
                    };
                    let QueueCommand::Push(push) = &env.command else {
                        continue;
                    };
                    let fingerprint = match env.request_fingerprint {
                        Some(fp) => BodyHash(fp),
                        None => crate::compose::push_item_body_hash(&push.items)?,
                    };
                    let expires_at =
                        request_expires_at(env.created_at, IN_PROCESS_PUSH_IDEM_RETENTION_MS);
                    let ids = match &env.request_outcome {
                        Some(crate::RequestOutcome::Push { item_ids }) => item_ids.clone(),
                        _ => env.item_ids.clone(),
                    };
                    cache.entry(queue.clone()).or_default().record(
                        request_id,
                        fingerprint,
                        ids,
                        expires_at,
                    );
                }
            }
            Ok(())
        }
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_with_store(move |store| store.eligible_candidates(&shard, now, max))
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_with_store(move |store| store.select_item_claim(&shard, &compatibility, now, max))
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: ClaimUnit,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl Future<Output = EngineResult<RichClaimSelection>> + Send {
        self.run_with_store(move |store| {
            store.select_rich_claim(&shard, unit, &compatibility, now, max_items)
        })
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        self.run_with_store(move |store| store.render_claimed(&shard, &ids))
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<ItemState>>> + Send {
        self.run_with_store(move |store| store.item_state(&shard, &id))
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        self.run_with_store(move |store| store.item_version(&shard, &id))
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_with_store(move |store| store.recovery_high_water(&shard))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_with_store(move |store| store.recover_definitions())
    }
}

impl<S> AsyncLogStore for BlockingLogStore<S>
where
    S: LogStore + Send + 'static,
{
    fn durability_class(&self) -> DurabilityClass {
        self.durability_class
    }

    fn is_durable_log(&self) -> bool {
        self.durable_log
    }

    fn ensure_shard(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.ensure_shard(&shard))
    }

    fn current_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_sync(move |store: &mut S| store.current_epoch(&shard))
    }

    fn acquire_epoch(&self, shard: QueueKey) -> impl Future<Output = EngineResult<u64>> + Send {
        self.run_sync(move |store: &mut S| store.acquire_epoch(&shard))
    }

    fn append(
        &self,
        shard: QueueKey,
        commands: Vec<CommandEnvelope>,
        expected_epoch: u64,
    ) -> impl Future<Output = EngineResult<Vec<CommandPosition>>> + Send {
        // Encode native FWC1 **before** the exclusive offload/store lock so concurrent workers
        // pay codec cost in parallel and only serialize on the durable seal (fireweed-9d2281f0).
        // Axes that do not consume `serialized` drop it and re-derive from `commands`.
        async move {
            let serialized = commands
                .iter()
                .map(crate::command_codec::encode_command_envelope)
                .collect::<EngineResult<Vec<_>>>()?;
            self.run_sync(move |store: &mut S| {
                store.append_serialized(&shard, &commands, serialized, expected_epoch)
            })
            .await
        }
    }

    fn read_from(
        &self,
        shard: QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = EngineResult<CommandPage>> + Send {
        self.run_sync(move |store: &mut S| store.read_from(&shard, from, limit))
    }

    fn high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_sync(move |store: &mut S| store.high_water(&shard))
    }

    fn set_high_water(
        &self,
        shard: QueueKey,
        position: CommandPosition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.set_high_water(&shard, position))
    }

    fn write_snapshot(
        &self,
        shard: QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl Future<Output = EngineResult<SnapshotRef>> + Send {
        self.run_sync(move |store: &mut S| store.write_snapshot(&shard, position, snapshot))
    }

    fn latest_snapshot(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        self.run_sync(move |store: &mut S| store.latest_snapshot(&shard))
    }

    fn read_snapshot(
        &self,
        snapshot_ref: SnapshotRef,
    ) -> impl Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        self.run_sync(move |store: &mut S| store.read_snapshot(&snapshot_ref))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_sync(move |store: &mut S| store.recover_definitions())
    }
}

impl<S> AsyncProjectionStore for BlockingProjectionStore<S>
where
    S: ProjectionStore + Send + 'static,
{
    fn supports_gates(&self) -> bool {
        self.supports_gates
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.ensure_shard(&definition))
    }

    fn admit_mutation(&self, shard: QueueKey) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.admit_mutation(&shard))
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        _now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.index_validate_push(&shard, &items))
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<bool>> + Send {
        self.run_sync(move |store: &mut S| store.pause_blocks_intake(&shard))
    }

    fn replay_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send {
        self.run_sync(move |store: &mut S| {
            store.replay_durable_commit(&shard, &request_id, fingerprint, now)
        })
    }

    fn commit_validate(
        &self,
        shard: QueueKey,
        claim_refs: Vec<ClaimRef>,
        now: UtcTimestamp,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.commit_validate(&shard, &claim_refs, now))
    }

    fn instance_fence(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        self.run_sync(move |store: &mut S| store.instance_fence(&shard, &key))
    }

    fn index_validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.index_validate_push(&shard, &items))
    }

    fn read_durable_commit(
        &self,
        shard: QueueKey,
        request_id: RequestId,
    ) -> impl Future<Output = EngineResult<Option<Vec<CommitOutcomeEntry>>>> + Send {
        self.run_sync(move |store: &mut S| store.read_durable_commit(&shard, &request_id))
    }

    fn side_record(
        &self,
        shard: QueueKey,
        key: Vec<u8>,
    ) -> impl Future<Output = EngineResult<Option<bytes::Bytes>>> + Send {
        self.run_sync(move |store: &mut S| store.side_record(&shard, &key))
    }

    fn side_records_by_prefix(
        &self,
        shard: QueueKey,
        prefix: Vec<u8>,
        page_size: usize,
        cursor: Option<Vec<u8>>,
    ) -> impl Future<Output = EngineResult<SideRecordPage>> + Send {
        self.run_sync(move |store: &mut S| {
            store.side_records_by_prefix(&shard, &prefix, page_size, cursor)
        })
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.apply_live_owned(positions, commands))
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = EngineResult<()>> + Send {
        self.run_sync(move |store: &mut S| store.apply_recovery(&positions, &commands))
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_sync(move |store: &mut S| store.eligible_candidates(&shard, now, max))
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.run_sync(move |store: &mut S| {
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
        self.run_sync(move |store: &mut S| {
            store.select_rich_claim(&shard, unit, &compatibility, now, max_items)
        })
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        self.run_sync(move |store: &mut S| store.render_claimed(&shard, &ids))
    }

    fn resolve_lease_targets(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        self.run_sync(move |store: &mut S| {
            store.renew_validate(&shard, &ids)?;
            let items = store.render_claimed(&shard, &ids)?;
            if items.len() != ids.len() {
                return Err(EngineError::Storage(
                    "validated lease targets were not renderable".into(),
                ));
            }
            Ok(items)
        })
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<ItemState>>> + Send {
        self.run_sync(move |store: &mut S| store.item_state(&shard, &id))
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl Future<Output = EngineResult<Option<u64>>> + Send {
        self.run_sync(move |store: &mut S| store.item_version(&shard, &id))
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        self.run_sync(move |store: &mut S| store.recovery_high_water(&shard))
    }

    fn recover_definitions(
        &self,
    ) -> impl Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        self.run_sync(move |store: &mut S| store.recover_definitions())
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
        self.run_sync(move |store: &mut S| store.create_queue(definition))
    }

    fn queue_definition(
        &self,
        key: QueueKey,
    ) -> impl Future<Output = EngineResult<QueueDefinition>> + Send {
        self.run_sync(move |store: &mut S| store.queue_definition(&key))
    }

    fn list_queues(
        &self,
        tenant: TenantId,
    ) -> impl Future<Output = EngineResult<Vec<QueueId>>> + Send {
        self.run_sync(move |store: &mut S| store.list_queues(&tenant))
    }
}

#[cfg(test)]
mod tests {
    // The concrete `Ready` return types make these compile-time assertions fail if a future ceases to be
    // `Send`; production implementations remain free to return their own opaque future types.
    #![allow(refining_impl_trait)]

    use std::future::{Ready, ready};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::task::Wake;
    use std::time::Duration;

    use fireweed_core::{
        BodyHash, CohortId, EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection,
        PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
        RecurrencePolicy, RetryPolicy, TenantId,
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
        let now = UtcTimestamp::new(0, 0).unwrap();
        assert_send(projection.renew_validate(
            shard(),
            vec![RenewTarget {
                item_id: ItemId::new("10").unwrap(),
                lease_token: LeaseToken::new("renew-token").unwrap(),
            }],
            now,
        ));
        assert_send(projection.finalize_validate(
            shard(),
            vec![FinalizeTarget {
                item_id: ItemId::new("11").unwrap(),
                lease_token: LeaseToken::new("finalize-token").unwrap(),
                item_version: 1,
                kind: crate::FinalizeKind::Complete,
                not_before: None,
            }],
            now,
            3,
        ));
        assert_send(projection.cohort_lease_validate(
            shard(),
            crate::CohortLeaseTarget {
                cohort_id: CohortId::new("cohort").unwrap(),
                cohort_lease_token: LeaseToken::new("cohort-token").unwrap(),
            },
            now,
        ));
        assert_send(projection.purge_validate(shard(), vec![ItemId::new("12").unwrap()], true));
        assert_send(projection.apply_live(Vec::new(), Vec::new()));
        assert_send(projection.apply_recovery(Vec::new(), Vec::new()));
        assert_send(projection.expired_leases(shard(), UtcTimestamp::new(0, 0).unwrap(), 16));
        assert_send(projection.eligible_candidates(shard(), UtcTimestamp::new(0, 0).unwrap(), 1));
        assert_send(projection.select_item_claim(
            shard(),
            ClaimCompatibility::default(),
            UtcTimestamp::new(0, 0).unwrap(),
            1,
        ));
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
        let adapter = BlockingControlPlane::new(Vec::<String>::new(), 1).unwrap();
        let future = adapter.run_owned_operation(OwnedWholeOperation {
            value: "committed transaction".to_string(),
        });
        assert_send(future);

        let len = futures::executor::block_on(adapter.run_owned_operation(OwnedWholeOperation {
            value: "committed transaction".to_string(),
        }))
        .unwrap();
        assert_eq!(len, 1);
    }

    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    struct OrderedWake {
        id: usize,
        order: Arc<Mutex<Vec<usize>>>,
    }

    impl Wake for OrderedWake {
        fn wake(self: Arc<Self>) {
            self.order
                .lock()
                .expect("wake order poisoned")
                .push(self.id);
        }
    }

    #[test]
    fn blocking_permit_registration_and_acquire_are_atomic() {
        let permits = Arc::new(BlockingPermits::new(1));
        let wake_count = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker = Waker::from(wake_count.clone());

        let held = permits
            .acquire_or_park(&waker)
            .expect("first permit is available");
        assert!(permits.acquire_or_park(&waker).is_none());
        drop(held);

        assert_eq!(wake_count.0.load(AtomicOrdering::SeqCst), 1);
        assert!(permits.acquire_or_park(&waker).is_some());
    }

    #[test]
    fn blocking_permits_wake_saturated_work_in_fifo_order() {
        let permits = Arc::new(BlockingPermits::new(1));
        let held_waker = Waker::from(Arc::new(CountingWake(AtomicUsize::new(0))));
        let held = permits
            .acquire_or_park(&held_waker)
            .expect("first permit is available");
        let order = Arc::new(Mutex::new(Vec::new()));
        let first = Waker::from(Arc::new(OrderedWake {
            id: 1,
            order: order.clone(),
        }));
        let second = Waker::from(Arc::new(OrderedWake {
            id: 2,
            order: order.clone(),
        }));

        assert!(permits.acquire_or_park(&first).is_none());
        assert!(permits.acquire_or_park(&second).is_none());
        drop(held);
        assert_eq!(*order.lock().unwrap(), vec![1]);

        let first_held = permits
            .acquire_or_park(&first)
            .expect("oldest waiter receives released permit");
        drop(first_held);
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    }

    #[test]
    fn blocking_executor_wakes_saturated_work_and_contains_panics() {
        let executor = BoundedBlockingExecutor::new(1).unwrap();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = executor.execute(move || {
            release_rx.recv().expect("release first operation");
            Ok::<_, EngineError>(1usize)
        });
        let second = executor.execute(|| Ok::<_, EngineError>(2usize));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            release_tx.send(()).expect("first operation is waiting");
        });

        let (first, second) = futures::executor::block_on(async { futures::join!(first, second) });
        assert_eq!(first.unwrap(), 1);
        assert_eq!(second.unwrap(), 2);

        let panic_result = futures::executor::block_on(
            executor.execute::<(), _>(|| panic!("injected blocking operation panic")),
        );
        assert!(
            matches!(panic_result, Err(EngineError::Storage(message)) if message.contains("panicked"))
        );
    }

    #[test]
    fn dropping_unpolled_blocking_work_cancels_before_start() {
        let executor = BoundedBlockingExecutor::new(1).unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_task = ran.clone();
        let future = executor.execute(move || {
            ran_in_task.store(true, AtomicOrdering::SeqCst);
            Ok::<_, EngineError>(())
        });
        drop(future);
        std::thread::sleep(Duration::from_millis(10));
        assert!(!ran.load(AtomicOrdering::SeqCst));
    }

    /// fireweed-7b74ceac: the reader/writer split's whole point is that
    /// [`InProcessProjectionStore::new_with_concurrent_reads`]'s `RwLock`-backed
    /// [`StoreLockOps::lock_read`] lets readers overlap, while the default `Mutex`-backed path keeps
    /// funneling every read through one exclusive lock. Exercise the primitive directly (not through
    /// a full `ProjectionStore` fake) so this stays a fast, deterministic unit test.
    #[test]
    fn concurrent_reads_lock_lets_readers_overlap_while_exclusive_lock_serializes() {
        fn measure(lock: Arc<dyn StoreLockOps<u32>>) -> Duration {
            let start = Instant::now();
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let lock = Arc::clone(&lock);
                    std::thread::spawn(move || {
                        let _guard = lock.lock_read();
                        std::thread::sleep(Duration::from_millis(80));
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("reader thread panicked");
            }
            start.elapsed()
        }

        let concurrent: Arc<dyn StoreLockOps<u32>> = Arc::new(RwLock::new(0u32));
        let exclusive: Arc<dyn StoreLockOps<u32>> = Arc::new(Mutex::new(0u32));

        let concurrent_elapsed = measure(concurrent);
        let exclusive_elapsed = measure(exclusive);

        assert!(
            concurrent_elapsed < Duration::from_millis(150),
            "two RwLock-backed readers should overlap, took {concurrent_elapsed:?}"
        );
        assert!(
            exclusive_elapsed >= Duration::from_millis(150),
            "two Mutex-backed readers must serialize, took {exclusive_elapsed:?}"
        );
    }

    /// A writer must still exclude every concurrent reader on the `RwLock`-backed path — the split
    /// only lets *readers* overlap with each other, never with a write.
    #[test]
    fn concurrent_reads_lock_write_excludes_readers() {
        let lock: Arc<dyn StoreLockOps<u32>> = Arc::new(RwLock::new(0u32));
        let start = Instant::now();

        let writer_lock = Arc::clone(&lock);
        let writer = std::thread::spawn(move || {
            let mut guard = writer_lock.lock_write();
            std::thread::sleep(Duration::from_millis(80));
            **guard = 1;
        });
        // Give the writer a head start so the reader below reliably queues behind it.
        std::thread::sleep(Duration::from_millis(20));
        let reader_lock = Arc::clone(&lock);
        let reader = std::thread::spawn(move || {
            let guard = reader_lock.lock_read();
            (start.elapsed(), **guard)
        });

        writer.join().expect("writer thread panicked");
        let (reader_wait, value) = reader.join().expect("reader thread panicked");
        assert_eq!(value, 1, "reader must observe the write, not a torn state");
        assert!(
            reader_wait >= Duration::from_millis(75),
            "reader must block until the writer releases, waited {reader_wait:?}"
        );
    }
}
