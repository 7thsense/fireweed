// The port impls below return `-> impl Future` (the engine's port signature) with `async move` bodies —
// the deliberate codebase pattern, not convertible to bare `async fn` without changing the trait shape.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    Backend, BoundedBlockingExecutor, BufferedByteBudget, BufferedByteBudgetConfig,
    BufferedByteBudgetStats, ByteAdmissionError, ClaimCommand, ClaimCompatibility, ClaimPort,
    ClaimRequest, Claimed, CommandChecksum, CommandEnvelope, CommandId, CommandPosition,
    CompiledSchema, ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError,
    EngineResult, FinalizeCommand, FinalizeOutcome, FinalizePort, IdempotencyDecision, ItemView,
    LeaseView, LiveItemView, LogRead, OwnedBytePermit, PendingPage, PendingSummary, ProjectionRead,
    PurgePort, PushCommand, PushPort, PushSpec, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, QueueMetrics, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver,
    RenewLeaseCommand, RenewLeasePort, TerminalEmissionMetrics, TickReport, UpsertOutcome,
    UpsertPort, build_push_items, compile_entity_schema, require_item_level_claim, validate_entity,
    validate_gate_command, validate_gate_push,
};
use fireweed_objectlog::segmented::{
    BlobStore, FaultHook, LocalFsBlobStore, ManifestPointerStore, PointerFencedBlobStore,
    SegmentConfig, SegmentCounters, SegmentedObjectLog,
};
use fireweed_objectlog::{
    LocalObjectLog, ObjectLogByteAdmissionSnapshot, prepare_serialized_commands,
};
use fireweed_projection::ProjectionData;
use fireweed_sqlite::SqliteProjectionStore;
use tokio::sync::oneshot;

/// Per-queue recovery telemetry recorded by the snapshot-tail reopen path (bead pqueue-8a76daad). Exposed so
/// a test (and an operator-facing log line) can prove recovery resumed from the persisted high-water rather
/// than replaying the full genesis log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// The object-log sequence recovery began replaying at — the projection's persisted high-water
    /// (`relational_cursor.next_seq`). `0` means a full-genesis replay (no valid snapshot).
    pub start_seq: u64,
    /// Number of object-log tail entries replayed beyond the snapshot (`<<` total when a snapshot exists).
    pub tail_replayed: u64,
    /// Whether a durable snapshot/high-water short-circuited the genesis replay (`start_seq > 0`).
    pub snapshot_used: bool,
    /// Hard command-page limit used by production replay.
    pub replay_command_page_limit: u64,
    /// Largest command page actually materialized.
    pub peak_replay_commands_buffered: u64,
    /// Largest manifest-object page actually materialized.
    pub peak_manifest_objects_buffered: u64,
    /// Hard manifest-object page limit used by production replay.
    pub manifest_object_page_limit: u64,
    /// Recovery replay workers. Replay is deliberately synchronous per queue.
    pub replay_worker_tasks: u64,
    /// Bounded, measured replay high-water samples captured after applied pages.
    pub replay_progress_samples: Vec<u64>,
    pub recovery_index_node_visits: u64,
    pub recovery_index_entries_visited: u64,
    pub recovery_index_height: u64,
    pub recovery_index_nodes_written_last_append: u64,
    pub recovery_segment_gets: u64,
    pub recovery_segment_bytes_fetched: u64,
    pub recovery_peak_segment_bytes_buffered: u64,
    pub recovery_peak_index_node_bytes_buffered: u64,
    pub recovery_peak_cursor_bytes_buffered: u64,
    pub bounded_authority_index: bool,
}

fn record_replay_progress(samples: &mut Vec<u64>, sequence: u64) {
    const MAX_SAMPLES: usize = 64;
    if samples.last().copied() == Some(sequence) {
        return;
    }
    if samples.len() < MAX_SAMPLES {
        samples.push(sequence);
    } else if let Some(last) = samples.last_mut() {
        *last = sequence;
    }
}

/// Default recovery-window budget: the max object-log tail (commands) a normal reopen is expected to replay
/// beyond the durable projection snapshot. The materialized projection advances its high-water inside the
/// same transaction that applies each sealed batch, so the tail is normally a handful of commands (only what
/// was durably sealed but not yet projection-applied at crash time). Exceeding this budget is logged as a
/// recovery-window warning so an operator can investigate a projection that has fallen far behind the log.
///
/// This is the in-code default; the composition root may override it from typed [`Config`](crate::Config)
/// (populated by the bin from `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS`) via [`Self::with_recovery_max_tail`]. The
/// backend itself never reads the process environment.
pub const DEFAULT_RECOVERY_MAX_TAIL: u64 = 1_000_000;

// Recovery-index maintenance is deliberately independent of segment sealing. One reclaim tick touches a
// fixed queue page and fixed object pages; repeated ticks round-robin across arbitrarily many queues.
const RECOVERY_MAINTENANCE_QUEUE_PAGE: usize = 8;
const RECOVERY_MAINTENANCE_PIN_PAGE: usize = 64;
const RECOVERY_MAINTENANCE_GC_BATCH_PAGE: usize = 8;
const RECOVERY_MAINTENANCE_BLOCKING_CONCURRENCY: usize = 4;
const RECOVERY_MAINTENANCE_TASK_LIMIT: usize = RECOVERY_MAINTENANCE_QUEUE_PAGE;
const SEGMENT_FLUSH_QUEUE_PAGE: usize = 4;

/// O(1)-average keyed queue registration plus a stable insertion-order ring for bounded maintenance.
///
/// Queues are never removed from these backends, so a persistent numeric cursor can safely walk the ring
/// without rebuilding or rotating a full active-queue snapshot. Registration performs one hash lookup;
/// each caller chooses a fixed page size and clones only that page.
struct QueueRegistry<V> {
    entries: HashMap<QueueKey, V>,
    order: Vec<QueueKey>,
}

impl<V> Default for QueueRegistry<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
        }
    }
}

impl<V: Clone> QueueRegistry<V> {
    fn get_or_insert_with(&mut self, shard: &QueueKey, create: impl FnOnce() -> V) -> V {
        match self.entries.entry(shard.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.order.push(shard.clone());
                entry.insert(create()).clone()
            }
        }
    }

    fn page(&self, cursor: &AtomicUsize, limit: usize) -> Vec<(QueueKey, V)> {
        if self.order.is_empty() || limit == 0 {
            return Vec::new();
        }
        let count = self.order.len().min(limit);
        let start = cursor.fetch_add(count, Ordering::Relaxed) % self.order.len();
        (0..count)
            .map(|offset| {
                let key = self.order[(start + offset) % self.order.len()].clone();
                let value = self
                    .entries
                    .get(&key)
                    .expect("queue registry order and keyed entries diverged")
                    .clone();
                (key, value)
            })
            .collect()
    }
}

fn registered_queue_page<V: Clone>(
    registry: &Mutex<QueueRegistry<V>>,
    cursor: &AtomicUsize,
    limit: usize,
) -> Vec<(QueueKey, V)> {
    registry
        .lock()
        .expect("queue registry poisoned")
        .page(cursor, limit)
}

fn registered_maintenance_page<V>(
    registry: &Mutex<QueueRegistry<V>>,
    cursor: &AtomicUsize,
    limit: usize,
    excluded: &HashSet<QueueKey>,
) -> Vec<QueueKey> {
    let registry = registry.lock().expect("queue registry poisoned");
    if registry.order.is_empty() || limit == 0 {
        return Vec::new();
    }
    let start = cursor.load(Ordering::Relaxed) % registry.order.len();
    let mut selected = Vec::with_capacity(limit.min(registry.order.len()));
    let mut inspected = 0;
    let probe_limit = limit
        .saturating_add(excluded.len())
        .min(RECOVERY_MAINTENANCE_TASK_LIMIT)
        .min(registry.order.len());
    while inspected < probe_limit && selected.len() < limit {
        let shard = &registry.order[(start + inspected) % registry.order.len()];
        inspected += 1;
        if !excluded.contains(shard) {
            selected.push(shard.clone());
        }
    }
    cursor.fetch_add(inspected, Ordering::Relaxed);
    selected
}

struct RecoveryMaintenanceTask {
    shard: QueueKey,
    handle: tokio::task::JoinHandle<EngineResult<()>>,
}

/// Separately admits bounded per-shard maintenance work.
///
/// A tick only dispatches work; it does not wait for a provider call that may be indefinitely delayed.
/// Completed failures are both logged by the task and returned by the next tick, preserving the reclaim
/// loop's error counter. The fixed task cap bounds Tokio wrappers while [`BoundedBlockingExecutor`] bounds
/// the OS threads that may enter a synchronous object-store provider.
struct RecoveryMaintenanceDispatcher {
    executor: BoundedBlockingExecutor,
    tasks: Mutex<Vec<RecoveryMaintenanceTask>>,
    deferred: Mutex<VecDeque<QueueKey>>,
    dispatch_gate: tokio::sync::Mutex<()>,
}

impl RecoveryMaintenanceDispatcher {
    fn new() -> EngineResult<Self> {
        Ok(Self {
            executor: BoundedBlockingExecutor::new(RECOVERY_MAINTENANCE_BLOCKING_CONCURRENCY)?,
            tasks: Mutex::new(Vec::with_capacity(RECOVERY_MAINTENANCE_TASK_LIMIT)),
            deferred: Mutex::new(VecDeque::new()),
            dispatch_gate: tokio::sync::Mutex::new(()),
        })
    }

    async fn dispatch<F>(
        &self,
        log: Arc<FsSegmentedLog>,
        select_shards: F,
        now_ms: i64,
    ) -> EngineResult<TickReport>
    where
        F: FnOnce(usize, &HashSet<QueueKey>) -> Vec<QueueKey> + Send,
    {
        let _dispatch_guard = self.dispatch_gate.lock().await;
        let completed = {
            let mut tasks = self
                .tasks
                .lock()
                .expect("recovery maintenance tasks poisoned");
            let mut completed = Vec::new();
            let mut index = 0;
            while index < tasks.len() {
                if tasks[index].handle.is_finished() {
                    completed.push(tasks.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            completed
        };

        let mut first_error = None;
        for task in completed {
            let result = match task.handle.await {
                Ok(result) => result,
                Err(error) => Err(EngineError::Storage(format!(
                    "recovery maintenance task failed for tenant={} queue={}: {error}",
                    task.shard.tenant_id.as_str(),
                    task.shard.queue_id.as_str(),
                ))),
            };
            if first_error.is_none() {
                first_error = result.err();
            }
        }

        let (available, mut in_flight) = {
            let tasks = self
                .tasks
                .lock()
                .expect("recovery maintenance tasks poisoned");
            (
                RECOVERY_MAINTENANCE_TASK_LIMIT - tasks.len(),
                tasks
                    .iter()
                    .map(|task| task.shard.clone())
                    .collect::<HashSet<_>>(),
            )
        };
        let mut shards = Vec::with_capacity(available);
        {
            let mut deferred = self
                .deferred
                .lock()
                .expect("deferred recovery maintenance tasks poisoned");
            while shards.len() < available {
                let Some(shard) = deferred.pop_front() else {
                    break;
                };
                if in_flight.insert(shard.clone()) {
                    shards.push(shard);
                }
            }
        }
        if shards.len() < available {
            shards.extend(select_shards(available - shards.len(), &in_flight));
        }

        {
            let mut tasks = self
                .tasks
                .lock()
                .expect("recovery maintenance tasks poisoned");
            let mut shards = shards.into_iter();
            while let Some(shard) = shards.next() {
                debug_assert!(tasks.len() < RECOVERY_MAINTENANCE_TASK_LIMIT);
                debug_assert!(!tasks.iter().any(|task| task.shard == shard));
                let task_log = Arc::clone(&log);
                let executor = self.executor.clone();
                let task_shard = shard.clone();
                let Some(handle) = fireweed_resp::try_spawn_governed(async move {
                    let operation_shard = task_shard.clone();
                    let result = executor
                        .execute(move || {
                            task_log.reap_recovery_pins_expired_bounded(
                                &operation_shard,
                                now_ms,
                                RECOVERY_MAINTENANCE_PIN_PAGE,
                            )?;
                            task_log.reap_recovery_index_garbage_bounded(
                                &operation_shard,
                                RECOVERY_MAINTENANCE_GC_BATCH_PAGE,
                            )?;
                            Ok(())
                        })
                        .await;
                    if let Err(error) = &result {
                        eprintln!(
                            "[objectlog] recovery maintenance failed tenant={} queue={}: {error:?}",
                            task_shard.tenant_id.as_str(),
                            task_shard.queue_id.as_str(),
                        );
                    }
                    result
                }) else {
                    let mut deferred = self
                        .deferred
                        .lock()
                        .expect("deferred recovery maintenance tasks poisoned");
                    deferred.push_back(shard);
                    deferred.extend(shards);
                    break;
                };
                tasks.push(RecoveryMaintenanceTask { shard, handle });
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(TickReport::default()),
        }
    }

    #[cfg(test)]
    fn in_flight_len(&self) -> usize {
        self.tasks
            .lock()
            .expect("recovery maintenance tasks poisoned")
            .len()
    }

    #[cfg(test)]
    fn all_tasks_finished(&self) -> bool {
        self.tasks
            .lock()
            .expect("recovery maintenance tasks poisoned")
            .iter()
            .all(|task| task.handle.is_finished())
    }
}

impl Drop for RecoveryMaintenanceDispatcher {
    fn drop(&mut self) {
        let tasks = self
            .tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for task in tasks.drain(..) {
            task.handle.abort();
        }
    }
}

fn default_objectlog_byte_budget() -> BufferedByteBudget {
    BufferedByteBudget::new(
        BufferedByteBudgetConfig::new(crate::DEFAULT_OBJECTLOG_BUFFERED_BYTES_GLOBAL)
            .expect("constant object-log byte budget is valid"),
    )
}

fn map_byte_admission_error(error: ByteAdmissionError) -> EngineError {
    match error {
        ByteAdmissionError::Closed => EngineError::Unavailable,
        ByteAdmissionError::Backpressure => EngineError::Backpressure {
            resource: "buffered bytes",
        },
        ByteAdmissionError::Oversize {
            requested, limit, ..
        } => EngineError::RequestTooLarge { requested, limit },
    }
}

fn production_byte_admission_snapshot(
    budget: &BufferedByteBudget,
    queue_byte_limit: usize,
) -> ObjectLogByteAdmissionSnapshot {
    let stats = budget.stats();
    ObjectLogByteAdmissionSnapshot {
        configured_global_bytes: budget.config().global_limit(),
        configured_tenant_bytes: budget.config().tenant_limit(),
        configured_queue_waiting_bytes: queue_byte_limit,
        current_bytes: stats.charged_bytes,
        peak_bytes: stats.peak_charged_bytes,
        waiters: stats.waiting_requests,
        waits: stats.wait_count,
        rejects: stats.rejection_count,
        total_wait_nanos: stats.total_wait_nanos,
        max_wait_nanos: stats.max_wait_nanos,
    }
}

fn byte_admission_telemetry(snapshot: ObjectLogByteAdmissionSnapshot) -> String {
    format!(
        "admission_current={} admission_peak={} admission_waiters={} admission_waits={} admission_rejects={} admission_total_wait_nanos={} admission_max_wait_nanos={} admission_global_limit={} admission_tenant_limit={} admission_queue_limit={}",
        snapshot.current_bytes,
        snapshot.peak_bytes,
        snapshot.waiters,
        snapshot.waits,
        snapshot.rejects,
        snapshot.total_wait_nanos,
        snapshot.max_wait_nanos,
        snapshot.configured_global_bytes,
        snapshot
            .configured_tenant_bytes
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
        snapshot.configured_queue_waiting_bytes,
    )
}

fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

fn compile_queue_schema(definition: &QueueDefinition) -> EngineResult<Option<Arc<CompiledSchema>>> {
    definition
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()
}

fn validate_push_items(
    schema: Option<&Arc<CompiledSchema>>,
    items: &[PushSpec],
) -> EngineResult<()> {
    for item in items {
        validate_entity(schema, item.entity.as_ref())?;
    }
    Ok(())
}

fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
    let total = now.seconds as i128 * 1_000_000_000
        + now.nanoseconds as i128
        + retention_ms as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

pub struct ObjectLogSqliteBackend {
    log: LocalObjectLog,
    projection: SqliteProjectionStore,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
    schemas: Mutex<HashMap<QueueKey, Arc<CompiledSchema>>>,
    idempotency: Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    op_lock: tokio::sync::Mutex<()>,
    /// Recovery-window budget (max tail commands) before a reopen logs a recovery-window warning.
    recovery_max_tail: u64,
    /// Last per-queue snapshot-tail recovery telemetry (proof the reopen avoided a full-genesis replay).
    recovery_stats: Mutex<HashMap<QueueKey, RecoveryStats>>,
}

impl ObjectLogSqliteBackend {
    pub fn open(object_root: impl Into<PathBuf>, projection_path: &str) -> EngineResult<Self> {
        Ok(Self {
            log: LocalObjectLog::open(object_root)?,
            projection: SqliteProjectionStore::open(projection_path)?,
            queues: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            idempotency: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            op_lock: tokio::sync::Mutex::new(()),
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            recovery_stats: Mutex::new(HashMap::new()),
        })
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    /// Override the recovery-window budget (max tail commands) — the explicit form of the
    /// `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` env knob, used by tests and embedders.
    pub fn with_recovery_max_tail(mut self, max_tail: u64) -> Self {
        self.recovery_max_tail = max_tail;
        self
    }

    /// The last snapshot-tail recovery telemetry for `shard` (bead pqueue-8a76daad proof seam).
    pub fn recovery_stats(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats
            .lock()
            .expect("recovery stats poisoned")
            .get(shard)
            .cloned()
    }

    fn next_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("olsqlite-{}-{n}", self.node_id)),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    fn next_request_envelope(
        &self,
        request_id: RequestId,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("olsqlite-{}-{n}", self.node_id)),
            request_id: Some(request_id),
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    /// Snapshot-tail recovery (bead pqueue-8a76daad). Resume from the projection's durable high-water
    /// (`relational_cursor.next_seq`) instead of replaying from genesis: the persisted SQLite projection IS
    /// the snapshot, so only the object-log tail at `>= high_water` is re-applied. Counters are re-seeded
    /// from the materialized items (the snapshot prefix) plus each tail entry, so post-reopen id minting
    /// never collides. A `None` high-water (queue never projected) falls back to a full replay.
    async fn replay_queue(&self, shard: &QueueKey) -> EngineResult<()> {
        let high_water = self.projection.recovery_high_water(shard)?;
        // Seed the mint counter from the snapshot's materialized items (full-genesis observe replacement).
        self.projection
            .observe_item_counters(shard, &self.counters)?;
        // `recovery_high_water` returns the LAST-applied position (`next_seq - 1`), so the true resume point
        // (the first unapplied sequence) is `high_water.sequence + 1 == next_seq`. `read_from` is exclusive
        // (starts at `from.sequence + 1`), so passing the last-applied position resumes at exactly the first
        // unapplied entry. No snapshot → genesis.
        let snapshot_used = high_water.is_some();
        let start_seq = high_water
            .as_ref()
            .map(|position| position.sequence + 1)
            .unwrap_or(0);
        let mut from = high_water;
        let mut tail_replayed: u64 = 0;
        loop {
            let page = self.log.read_from(shard, from.clone(), 256).await?;
            for (position, envelope) in &page.entries {
                for id in &envelope.item_ids {
                    self.counters.observe(shard, *id);
                }
                // Idempotent: `apply_committed` skips any position the persisted cursor already absorbed.
                self.projection.apply_committed(position, envelope)?;
                tail_replayed += 1;
            }
            match page.next {
                Some(next) => from = Some(next),
                None => break,
            }
        }
        if tail_replayed > self.recovery_max_tail {
            eprintln!(
                "[recovery] object-log-sqlite tail for {}:{} replayed {tail_replayed} commands beyond \
                 snapshot high-water {start_seq} (budget {}); projection may have fallen behind the log",
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                self.recovery_max_tail,
            );
        }
        self.recovery_stats
            .lock()
            .expect("recovery stats poisoned")
            .insert(
                shard.clone(),
                RecoveryStats {
                    start_seq,
                    tail_replayed,
                    snapshot_used,
                    ..RecoveryStats::default()
                },
            );
        Ok(())
    }

    async fn append_apply(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let epoch = match expected_epoch {
            Some(epoch) => epoch,
            None => self.log.current_epoch(shard)?,
        };
        let positions = self
            .log
            .append(shard, std::slice::from_ref(&envelope), epoch)?;
        self.projection.apply_committed(&positions[0], &envelope)
    }

    async fn require_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        if ids.iter().copied().collect::<HashSet<_>>().len() == ids.len()
            && self.projection.all_leased(shard, ids)?
        {
            return Ok(());
        }
        let renderable = self.projection.claimed_view(shard, ids).await?;
        if renderable.len() == ids.len() {
            Ok(())
        } else {
            Err(EngineError::Invalid("item is not leased"))
        }
    }
}

impl Backend for ObjectLogSqliteBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn commit_raw(
        &self,
        _request: fireweed_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::RawCommitOutcome>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ControlPlaneStore for ObjectLogSqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            let outcome = self.log.create_queue(definition)?;
            let definition = outcome.definition.clone();
            self.projection
                .create_queue_projection(definition.clone())?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.queues
                .lock()
                .expect("object-log sqlite queues poisoned")
                .insert(key.clone(), definition);
            let compiled_schema = compile_queue_schema(&outcome.definition)?;
            if let Some(cs) = compiled_schema {
                self.schemas
                    .lock()
                    .expect("object-log sqlite schemas poisoned")
                    .insert(key.clone(), cs);
            }
            self.replay_queue(&key).await?;
            Ok(outcome)
        }
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .queues
            .lock()
            .expect("object-log sqlite queues poisoned")
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result = self
            .queues
            .lock()
            .expect("object-log sqlite queues poisoned")
            .keys()
            .filter(|key| key.tenant_id.as_str() == tenant.as_str())
            .map(|key| key.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        self.replay_queue(shard)
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(self.log.current_epoch(shard))
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(self.log.acquire_epoch(shard))
    }
}

impl PushPort for ObjectLogSqliteBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            let _guard = self.op_lock.lock().await;
            let definition = self.queue_definition(shard).await?;
            let schema = self
                .schemas
                .lock()
                .expect("object-log sqlite schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let epoch = expected_epoch.unwrap_or(self.log.current_epoch(shard)?);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) = build_push_items(
                items,
                epoch,
                self.node_id,
                counter_base,
                definition.retry_policy.max_attempts,
            );
            let envelope = self.next_envelope(
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.append_apply(shard, envelope, Some(epoch)).await?;
            Ok(ids)
        }
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            let _guard = self.op_lock.lock().await;
            let definition = self.queue_definition(shard).await?;
            let schema = self
                .schemas
                .lock()
                .expect("object-log sqlite schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let expires_at = request_expires_at(now, definition.request_id_retention_ms);
            {
                let mut idempotency = self.idempotency.lock().expect("idempotency poisoned");
                match idempotency.entry(shard.clone()).or_default().check(
                    &request_id,
                    fingerprint,
                    now,
                ) {
                    IdempotencyDecision::Replay(ids) => return Ok(ids),
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }
            let epoch = expected_epoch.unwrap_or(self.log.current_epoch(shard)?);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) = build_push_items(
                items,
                epoch,
                self.node_id,
                counter_base,
                definition.retry_policy.max_attempts,
            );
            let envelope = self.next_request_envelope(
                request_id.clone(),
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.append_apply(shard, envelope, Some(epoch)).await?;
            self.idempotency
                .lock()
                .expect("idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .record(request_id, fingerprint, ids.clone(), expires_at);
            Ok(ids)
        }
    }
}

impl ClaimPort for ObjectLogSqliteBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            if req.compatibility != ClaimCompatibility::default() {
                let definition = self.queue_definition(&req.shard).await?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, &definition)?;
            }
            let candidates = self
                .projection
                .select_eligible(&req.shard, req.eligibility_at(), req.max_items)
                .await?;
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let envelope = self.next_envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                    worker_id: Some(req.worker_id.clone()),
                }),
                candidates.clone(),
                req.now,
            );
            self.append_apply(&req.shard, envelope, req.expected_epoch)
                .await?;
            let items = self
                .projection
                .claimed_view(&req.shard, &candidates)
                .await?;
            Ok(Claimed {
                items,
                ..Default::default()
            })
        }
    }
}

/// Snorri authoritative vectorized claimed-work commit (epic pqueue-2201fd37). This composite backend has
/// no single atomic transition boundary; inherits the default impl returning `EngineError::Unavailable`.
impl fireweed_engine::CommitTransitionPort for ObjectLogSqliteBackend {}

/// Recovery/explain reads inherit the `Unavailable` default (no authoritative commit boundary on this path).
impl fireweed_engine::RecoveryReadPort for ObjectLogSqliteBackend {}

impl FinalizePort for ObjectLogSqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|outcome| outcome.item_id).collect();
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl UpsertPort for ObjectLogSqliteBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl RenewLeasePort for ObjectLogSqliteBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl ReassignLeasePort for ObjectLogSqliteBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let _guard = self.op_lock.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let envelope = self.next_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.append_apply(shard, envelope, expected_epoch).await
        }
    }
}

impl PurgePort for ObjectLogSqliteBackend {
    fn purge(
        &self,
        _shard: &QueueKey,
        _item_ids: Vec<ItemId>,
        _force: bool,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for ObjectLogSqliteBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

impl ProjectionRead for ObjectLogSqliteBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.projection.select_eligible(shard, now, limit)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        self.projection.peek(shard, limit)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.pending(shard)
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        self.projection.pending_summary(shard)
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        self.projection.pending_page(shard, start, limit)
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection
            .pending_range(shard, start, end, consumer, limit)
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.pending_by_ids(shard, ids)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send
    {
        self.projection.claimed_view(shard, ids)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        self.projection.live_items(shard, keys)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        self.projection.metrics(queue)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        self.projection
            .terminal_emission_metrics(shard, now, emit_change_records, emission_cursor)
    }
}

// ===========================================================================
// Segmented (group-commit) object-log + SQLite projection backend
// ===========================================================================
//
// The `object_log_sqlite_projection` runtime's high-throughput variant. Where `ObjectLogSqliteBackend`
// pays a per-command object-log write (a full log-directory scan + two `fs::write`s + a file lock) AND a
// per-command SQLite transaction, this backend funnels concurrent pushes through the **segmented
// group-commit substrate** (`SegmentedObjectLog<LocalFsBlobStore>`): many concurrent pushes co-buffer into
// one segment, which seals as ONE durable object + ONE manifest-CAS commit, after which the whole batch is
// applied to the SQLite projection in ONE transaction. Ack is withheld until that seal+apply completes
// (eventual-apply durability class is preserved: the durable boundary is the sealed segment + manifest
// entry, and the projection is rebuilt from `read_all` on open).
//
// ## Group-commit / ack-after-seal mechanism
// - Each mutating op builds one `CommandEnvelope` and registers `(envelope, oneshot)` on the queue's
//   `ShardCoord` under its async `state` lock, in arrival order, then `enqueue`s the envelope into the
//   segment buffer. The `pending` envelope vec mirrors the substrate's internal buffer 1:1 (both are only
//   mutated under the same `state` lock), so a seal that drains the substrate buffer drains exactly the
//   same `pending`/`waiters` prefix.
// - A push does NOT force a seal: it either rides a size-triggered seal (the substrate seals inside
//   `enqueue` when the buffer reaches `target_bytes`) or waits for the background **flusher** to seal on
//   the `max_latency_ms` latency cap. Concurrent pushes from many RESP connections naturally co-buffer.
// - Read-modify-write ops (claim/finalize/renew/reassign) take a per-queue `mutate_lock` and FORCE a seal
//   so the projection reflects the command before the op returns (and so two claims can never select the
//   same candidate — the mutate_lock serializes select→commit→apply).
// - On seal, `distribute` applies the whole batch to SQLite in one transaction (`apply_committed_batch`)
//   and then completes every waiting op's `oneshot`. An epoch-fenced/failed seal fails all current waiters
//   (the substrate discarded the buffer), keeping `pending` consistent with the substrate buffer.

/// The segmented log this backend drives (group-commit over a pluggable [`BlobStore`]). Production wires the
/// durable local-filesystem store via [`SegmentedObjectLogSqliteBackend::open`]; the live S3/MinIO TP-002 E3
/// harness injects an [`fireweed_objectlog::segmented::S3BlobStore`] via
/// [`SegmentedObjectLogSqliteBackend::open_with_blob_store`]. `Arc<dyn BlobStore>` already implements
/// `BlobStore` (the substrate's `Arc` blanket impl), so the substrate stays generic with no boxing churn.
type FsSegmentedLog = SegmentedObjectLog<Arc<dyn BlobStore>>;

/// Per-queue group-commit coordination state (guarded by an async mutex so the buffer/waiter registry is
/// mutated atomically with the substrate's `enqueue`/`seal`).
struct ShardCoord {
    state: tokio::sync::Mutex<CoordState>,
}

struct CoordState {
    /// Envelopes buffered-but-not-yet-acked, mirroring the substrate's internal buffer 1:1 (arrival order).
    pending: Vec<CommandEnvelope>,
    /// Non-cloneable resident-byte ownership, aligned 1:1 with `pending` until projection apply completes.
    permits: Vec<OwnedBytePermit>,
    /// One responder per buffered envelope; fired (Ok/Err) when the envelope's segment seals + applies.
    waiters: Vec<oneshot::Sender<EngineResult<()>>>,
}

/// Removes a queue from the flusher's in-flight set on every exit path, including unwinding from a
/// provider panic. The backend remains strongly owned by the blocking job for the guard's lifetime.
struct FlushInflightGuard<'a> {
    inflight: &'a Mutex<HashSet<QueueKey>>,
    shard: QueueKey,
}

impl Drop for FlushInflightGuard<'_> {
    fn drop(&mut self) {
        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.shard);
    }
}

/// Group-committing object-log authority (`SegmentedObjectLog<LocalFsBlobStore>`) + SQLite materialized
/// projection. Eventual-apply durability class.
pub struct SegmentedObjectLogSqliteBackend {
    log: Arc<FsSegmentedLog>,
    projection: Arc<SqliteProjectionStore>,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
    schemas: Mutex<HashMap<QueueKey, Arc<CompiledSchema>>>,
    /// Cached current `assignment_epoch` per queue (avoids a manifest read on the hot push path and feeds
    /// the flusher's seal). Authoritative epoch still lives in the manifest; this only mirrors it.
    epochs: Mutex<HashMap<QueueKey, u64>>,
    registry: Mutex<QueueRegistry<Arc<ShardCoord>>>,
    maintenance_cursor: AtomicUsize,
    maintenance_dispatcher: RecoveryMaintenanceDispatcher,
    mutate_locks: Mutex<HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    /// How often the flusher polls each queue for a latency-due seal (a fraction of `max_latency_ms`).
    flush_interval: Duration,
    flush_slots: Arc<tokio::sync::Semaphore>,
    flush_cursor: AtomicUsize,
    flush_inflight: Mutex<HashSet<QueueKey>>,
    /// Recovery-window budget (max tail commands) before a reopen logs a recovery-window warning.
    recovery_max_tail: u64,
    /// Opt-in group-commit telemetry: when set, the flusher logs the segment counters ~1x/s. Set by the
    /// composition root from typed `Config` (populated by the bin from `FIREWEED_DEBUG_SEGMENTS`); the backend
    /// never reads the process environment.
    debug_segments: bool,
    /// Last per-queue snapshot-tail recovery telemetry (proof the reopen avoided a full-genesis replay).
    recovery_stats: Mutex<HashMap<QueueKey, RecoveryStats>>,
    /// Per-queue request-id replay/conflict cache (API-001 / TD-007 §4): a retried `request_id` with the
    /// same body replays the committed ids without a second append; a different body is `RequestIdConflict`.
    idempotency: Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>,
    byte_budget: BufferedByteBudget,
    queue_byte_limit: usize,
}

fn ts_to_ms(now: UtcTimestamp) -> i64 {
    now.seconds
        .saturating_mul(1000)
        .saturating_add((now.nanoseconds / 1_000_000) as i64)
}

fn system_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl SegmentedObjectLogSqliteBackend {
    /// Bounded page of authoritative pending order, exposed for release recovery verification.
    pub fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<fireweed_engine::ItemView>> {
        self.projection.peek_page(shard, after, limit)
    }

    /// Install the deterministic object-log fault seam used by ownership/fencing conformance tests.
    pub fn set_object_log_fault_hook(&self, hook: Option<Arc<dyn FaultHook>>) {
        self.log.set_fault_hook(hook);
    }
    /// Open (or recover) a segmented object log rooted at `object_root` with `config`, plus the SQLite
    /// projection at `projection_path`. Recovery (replay of committed segments into the projection) happens
    /// per-queue in `create_queue` as the bootstrap queues are provisioned.
    pub fn open(
        object_root: impl Into<PathBuf>,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let store: Arc<dyn BlobStore> = Arc::new(LocalFsBlobStore::open(object_root)?);
        Self::open_with_blob_store(store, projection_path, config)
    }

    /// Open (or recover) over an arbitrary [`BlobStore`] (the production [`LocalFsBlobStore`] is just one
    /// such store). The live S3/MinIO TP-002 E3 evidence harness passes an
    /// [`fireweed_objectlog::segmented::S3BlobStore`] here to exercise the SAME group-commit ack-after-seal +
    /// snapshot-tail recovery pipeline against a real S3-compatible endpoint.
    pub fn open_with_blob_store(
        store: Arc<dyn BlobStore>,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let projection = Arc::new(SqliteProjectionStore::open(projection_path)?);
        Self::open_with_blob_store_and_projection(store, projection, config)
    }

    /// Assemble one pool member over a projection handle shared by the fixed server backend pool. SQLite
    /// permits one writer at a time; sharing the store makes that serialization explicit in-process instead
    /// of letting independent connections race into `SQLITE_BUSY` during concurrent multi-queue writes.
    pub(crate) fn open_with_blob_store_and_projection(
        store: Arc<dyn BlobStore>,
        projection: Arc<SqliteProjectionStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let log = Arc::new(SegmentedObjectLog::open(store, config));
        // Poll near the latency cap so a buffered-but-quiet segment seals within ~max_latency_ms.
        let flush_ms = (config.max_latency_ms / 4).max(1);
        Ok(Self {
            log,
            projection,
            queues: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            registry: Mutex::new(QueueRegistry::default()),
            maintenance_cursor: AtomicUsize::new(0),
            maintenance_dispatcher: RecoveryMaintenanceDispatcher::new()?,
            mutate_locks: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            flush_interval: Duration::from_millis(flush_ms),
            flush_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            flush_cursor: AtomicUsize::new(0),
            flush_inflight: Mutex::new(HashSet::new()),
            recovery_max_tail: DEFAULT_RECOVERY_MAX_TAIL,
            debug_segments: false,
            recovery_stats: Mutex::new(HashMap::new()),
            idempotency: Mutex::new(HashMap::new()),
            byte_budget: default_objectlog_byte_budget(),
            queue_byte_limit: crate::DEFAULT_OBJECTLOG_QUEUE_WAITING_BYTES,
        })
    }

    /// Open over a no-CAS object store with a transactional manifest-pointer authority.
    pub fn open_with_manifest_pointer(
        store: Arc<dyn BlobStore>,
        pointers: Arc<dyn ManifestPointerStore>,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Self::open_with_blob_store(
            Arc::new(PointerFencedBlobStore::new(store, pointers)),
            projection_path,
            config,
        )
    }

    /// Production no-CAS composition: immutable objects remain in the supplied store while Postgres owns
    /// the atomically versioned manifest head and assignment epoch.
    pub fn open_with_postgres_manifest_pointer(
        store: Arc<dyn BlobStore>,
        postgres_url: &str,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let pointers = Arc::new(fireweed_postgres::PostgresManifestPointer::open(
            postgres_url,
        )?);
        Self::open_with_manifest_pointer(store, pointers, projection_path, config)
    }

    /// A snapshot of the measured group-commit segment/object counters (segments sealed, objects PUT,
    /// commands committed, per-segment batch sizes) — the release-ledger object-log cost surface the
    /// TP-002 E3 harness reports per segment-size configuration.
    pub fn segment_counters(&self) -> SegmentCounters {
        self.log.counters()
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    pub fn with_worker_partition(self, _index: usize, _partitions: usize) -> Self {
        self
    }

    /// Override the recovery-window budget (max tail commands) — the explicit form of the
    /// `FIREWEED_RECOVERY_MAX_TAIL_COMMANDS` env knob, used by tests and embedders.
    pub fn with_recovery_max_tail(mut self, max_tail: u64) -> Self {
        self.recovery_max_tail = max_tail;
        self
    }

    /// Enable opt-in group-commit telemetry (the explicit form of the `FIREWEED_DEBUG_SEGMENTS` env knob):
    /// when `true`, the flusher logs segment counters ~1x/s. Set by the composition root from typed `Config`.
    pub fn with_debug_segments(mut self, debug_segments: bool) -> Self {
        self.debug_segments = debug_segments;
        self
    }

    /// Install the node-shared, validated object-log resident-byte budget selected by the composition root.
    pub fn with_byte_admission(
        mut self,
        budget: BufferedByteBudget,
        queue_byte_limit: usize,
    ) -> Self {
        self.byte_budget = budget;
        self.queue_byte_limit = queue_byte_limit;
        self
    }

    pub fn byte_admission_stats(&self) -> BufferedByteBudgetStats {
        self.byte_budget.stats()
    }

    pub fn byte_admission_snapshot(&self) -> ObjectLogByteAdmissionSnapshot {
        production_byte_admission_snapshot(&self.byte_budget, self.queue_byte_limit)
    }

    /// The last snapshot-tail recovery telemetry for `shard` (bead pqueue-8a76daad proof seam).
    pub fn recovery_stats(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats
            .lock()
            .expect("recovery stats poisoned")
            .get(shard)
            .cloned()
    }

    /// Spawn the background flusher that seals each queue's latency-due segment (the latency seal trigger).
    /// Without it, a buffer below `target_bytes` would never seal and its pushes would never ack.
    pub fn spawn_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let interval = self.flush_interval;
        let debug_segments = self.debug_segments;
        fireweed_resp::spawn_governed(async move {
            Self::flush_loop(weak, interval, debug_segments).await
        })
    }

    fn coord_for(&self, shard: &QueueKey) -> Arc<ShardCoord> {
        self.registry
            .lock()
            .expect("segmented queue registry poisoned")
            .get_or_insert_with(shard, || {
                Arc::new(ShardCoord {
                    state: tokio::sync::Mutex::new(CoordState {
                        pending: Vec::new(),
                        permits: Vec::new(),
                        waiters: Vec::new(),
                    }),
                })
            })
    }

    fn mutate_lock_for(&self, shard: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut g = self
            .mutate_locks
            .lock()
            .expect("segmented mutate-locks poisoned");
        g.entry(shard.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn cached_epoch(&self, shard: &QueueKey) -> u64 {
        self.epochs
            .lock()
            .expect("segmented epochs poisoned")
            .get(shard)
            .copied()
            .unwrap_or(0)
    }

    fn set_epoch(&self, shard: &QueueKey, epoch: u64) {
        self.epochs
            .lock()
            .expect("segmented epochs poisoned")
            .insert(shard.clone(), epoch);
    }

    fn next_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("segolsqlite-{}-{n}", self.node_id)),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    /// Same as [`Self::next_envelope`] but carries API-001's envelope-level `request_id` into the durable
    /// command (the request-id'd push path), so the committed log records the caller's request id.
    fn next_request_envelope(
        &self,
        request_id: RequestId,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("segolsqlite-{}-{n}", self.node_id)),
            request_id: Some(request_id),
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    /// Apply a freshly-sealed batch to the SQLite projection in one transaction, then complete every waiter
    /// that contributed to it. `positions` covers the WHOLE drained substrate buffer, so it pairs 1:1 with
    /// the front of `pending`/`waiters`. Called while holding the coord `state` lock.
    fn distribute(&self, state: &mut CoordState, positions: Vec<CommandPosition>) {
        let n = positions.len();
        debug_assert!(
            n <= state.pending.len() && n <= state.waiters.len(),
            "sealed batch cannot exceed buffered/waiting commands"
        );
        let envelopes: Vec<CommandEnvelope> = state.pending.drain(..n).collect();
        let permits: Vec<OwnedBytePermit> = state.permits.drain(..n).collect();
        let waiters: Vec<_> = state.waiters.drain(..n).collect();
        let result = self
            .projection
            .apply_committed_batch(&positions, &envelopes);
        drop(permits);
        for w in waiters {
            let _ = w.send(result.clone());
        }
    }

    /// A seal failed (epoch fence / storage): the substrate discarded the buffer, so fail every registered
    /// waiter and clear `pending` to stay consistent with the now-empty substrate buffer.
    fn fail_all(state: &mut CoordState, err: EngineError) {
        state.pending.clear();
        state.permits.clear();
        for w in state.waiters.drain(..) {
            let _ = w.send(Err(err.clone()));
        }
    }

    /// Register one envelope on the queue's coordinator and enqueue it into the segment buffer. With
    /// `force`, a seal is driven immediately (read-modify-write ops); otherwise the envelope co-buffers and
    /// is acked by a size-triggered seal or the flusher. Resolves once the envelope's segment is committed
    /// AND applied to the projection (ack-after-seal).
    async fn commit(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        epoch: u64,
        now: UtcTimestamp,
        force: bool,
    ) -> EngineResult<()> {
        // Pre-validate to parity with the substrate's gate so a post-registration `enqueue` cannot reject
        // and desync `pending` from the substrate buffer.
        validate_gate_command(false, &envelope.command)?;
        if matches!(envelope.command, QueueCommand::ReplacePending(_)) {
            return Err(EngineError::Unavailable);
        }
        let coord = self.coord_for(shard);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = coord.state.lock().await;
            // Same-queue callers waiting for this lock own only their request envelope, never a global byte
            // permit or an extra serialized copy. Canonical encoding, queue reservation, and non-waiting
            // global admission linearize together under the coordinator lock.
            let (serialized, charged_bytes) = prepare_serialized_commands(
                vec![envelope],
                self.byte_budget.config().global_limit(),
            )?;
            let queue_bytes: usize = state.permits.iter().map(OwnedBytePermit::bytes).sum();
            if !state.pending.is_empty()
                && queue_bytes.saturating_add(charged_bytes) > self.queue_byte_limit
            {
                return Err(EngineError::Backpressure {
                    resource: "queue buffered bytes",
                });
            }
            let permit = self
                .byte_budget
                .try_acquire(shard.tenant_id.clone(), charged_bytes)
                .map_err(map_byte_admission_error)?;
            let now_ms = ts_to_ms(now);
            let enqueued = self
                .log
                .enqueue_serialized(shard, serialized, epoch, now_ms);
            match enqueued {
                Ok((outcome, envelopes)) => {
                    state.pending.extend(envelopes);
                    state.permits.push(permit);
                    state.waiters.push(tx);
                    if !outcome.committed.is_empty() {
                        // A size-triggered seal fired inside `enqueue`; hand the batch to the apply task.
                        self.distribute(&mut state, outcome.committed);
                    } else if force {
                        match self.log.seal(shard, epoch, now_ms) {
                            Ok(positions) => self.distribute(&mut state, positions),
                            Err(e) => Self::fail_all(&mut state, e),
                        }
                    }
                    // else: buffered; the flusher (or a later size seal) commits it.
                }
                Err(e) => {
                    Self::fail_all(&mut state, e.clone());
                    let _ = tx.send(Err(e));
                }
            }
        }
        rx.await
            .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))?
    }

    /// Enqueue distinct one-item commands under one coordinator lock in caller order, then await their
    /// durability/apply barriers after releasing the lock. This preserves per-item outcomes and queue
    /// history order while allowing the segment substrate to group the commands into fewer seals.
    async fn commit_ordered_independent(
        &self,
        shard: &QueueKey,
        envelopes: Vec<CommandEnvelope>,
        epoch: u64,
        now: UtcTimestamp,
    ) -> Vec<EngineResult<()>> {
        let count = envelopes.len();
        let coord = self.coord_for(shard);
        let mut outcomes = vec![None; count];
        let mut receivers = Vec::with_capacity(count);
        {
            let mut state = coord.state.lock().await;
            for (index, envelope) in envelopes.into_iter().enumerate() {
                let (serialized, charged_bytes) = match prepare_serialized_commands(
                    vec![envelope],
                    self.byte_budget.config().global_limit(),
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
                let queue_bytes: usize = state.permits.iter().map(OwnedBytePermit::bytes).sum();
                if !state.pending.is_empty()
                    && queue_bytes.saturating_add(charged_bytes) > self.queue_byte_limit
                {
                    outcomes[index] = Some(Err(EngineError::Backpressure {
                        resource: "queue buffered bytes",
                    }));
                    continue;
                }
                let permit = match self
                    .byte_budget
                    .try_acquire(shard.tenant_id.clone(), charged_bytes)
                    .map_err(map_byte_admission_error)
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
                let (tx, rx) = oneshot::channel();
                match self
                    .log
                    .enqueue_serialized(shard, serialized, epoch, ts_to_ms(now))
                {
                    Ok((outcome, accepted)) => {
                        state.pending.extend(accepted);
                        state.permits.push(permit);
                        state.waiters.push(tx);
                        receivers.push((index, rx));
                        if !outcome.committed.is_empty() {
                            self.distribute(&mut state, outcome.committed);
                        }
                    }
                    Err(error) => {
                        Self::fail_all(&mut state, error.clone());
                        outcomes[index] = Some(Err(error.clone()));
                        for outcome in outcomes.iter_mut().skip(index + 1) {
                            *outcome = Some(Err(error.clone()));
                        }
                        break;
                    }
                }
            }
        }
        for (index, receiver) in receivers {
            outcomes[index] = Some(
                receiver
                    .await
                    .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))
                    .and_then(|result| result),
            );
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every ordered push has an outcome"))
            .collect()
    }

    async fn flush_loop(
        weak: std::sync::Weak<Self>,
        flush_interval: Duration,
        debug_segments: bool,
    ) {
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Opt-in group-commit telemetry (the typed `debug_segments` flag, set by the composition root from
        // `Config`). When set, log the segment counters ~1x/s so seal rate + mean batch size are observable
        // during a load run. The hot tick path stays allocation-free.
        let mut dbg_last = std::time::Instant::now();
        loop {
            ticker.tick().await;
            let Some(this) = weak.upgrade() else {
                break;
            };
            if debug_segments && dbg_last.elapsed() >= std::time::Duration::from_millis(1000) {
                dbg_last = std::time::Instant::now();
                let c = this.log.counters();
                let admission = byte_admission_telemetry(this.byte_admission_snapshot());
                eprintln!(
                    "[seg] sealed={} commands={} mean_batch={:.1} max_batch={} objects_put={} {}",
                    c.segments_sealed,
                    c.commands_committed,
                    c.mean_batch_size(),
                    c.max_batch_size(),
                    c.objects_put,
                    admission,
                );
            }
            let shards =
                registered_queue_page(&this.registry, &this.flush_cursor, SEGMENT_FLUSH_QUEUE_PAGE);
            let now_ms = system_now_ms();
            for (shard, coord) in shards {
                let Ok(slot) = this.flush_slots.clone().try_acquire_owned() else {
                    break;
                };
                {
                    let mut inflight = this.flush_inflight.lock().expect("flush inflight poisoned");
                    if !inflight.insert(shard.clone()) {
                        continue;
                    }
                }
                let backend = Arc::clone(&this);
                tokio::task::spawn_blocking(move || {
                    let _slot = slot;
                    let _inflight = FlushInflightGuard {
                        inflight: &backend.flush_inflight,
                        shard: shard.clone(),
                    };
                    let epoch = backend.cached_epoch(&shard);
                    let mut state = coord.state.blocking_lock();
                    if !state.pending.is_empty() {
                        match backend.log.flush_due(&shard, epoch, now_ms) {
                            Ok(positions) if !positions.is_empty() => {
                                backend.distribute(&mut state, positions)
                            }
                            Ok(_) => {}
                            Err(e) => Self::fail_all(&mut state, e),
                        }
                    }
                });
            }
        }
    }

    /// Snapshot-tail recovery (bead pqueue-8a76daad) — the production `object_log_sqlite_projection` path.
    ///
    /// The SQLite projection persists, per queue, both the materialized state AND its high-water
    /// (`relational_cursor.next_seq`, advanced inside the same transaction that applies each sealed batch).
    /// So the projection IS the snapshot: on reopen we read the high-water and replay ONLY the manifest-
    /// committed object-log tail at `>= high_water` ([`SegmentedObjectLog::read_from`]), which never fetches
    /// or decodes a segment object that lies entirely in the snapshot. A `None`/`0` high-water (fresh queue,
    /// or the E3 smoke rebuild with no persisted projection) falls back to a full-genesis replay.
    ///
    /// Crash-consistency: because the high-water is only advanced by the projection apply, it can never be
    /// ahead of what is durably materialized — a crash between a segment's manifest commit and its projection
    /// apply leaves the tail at `>= high_water`, which this path re-applies (the batch apply is idempotent,
    /// so an overlapping prefix is skipped, never double-applied).
    fn replay_queue(&self, shard: &QueueKey) -> EngineResult<()> {
        let high_water = self.projection.recovery_high_water(shard)?;
        // Seed the mint counter from the snapshot's materialized items (full-genesis observe replacement).
        self.projection
            .observe_item_counters(shard, &self.counters)?;
        // `recovery_high_water` returns the LAST-applied position (`next_seq - 1`); resume at the first
        // unapplied sequence (`next_seq`). `read_from` is inclusive of `from_seq`, so it returns exactly the
        // manifest-committed tail beyond the snapshot. No snapshot → genesis (`start_seq == 0`).
        let snapshot_used = high_water.is_some();
        let start_seq = high_water
            .as_ref()
            .map(|position| position.sequence + 1)
            .unwrap_or(0);
        let mut next_seq = start_seq;
        let mut tail_replayed = 0u64;
        let mut peak_replay_commands_buffered = 0u64;
        let mut peak_manifest_objects_buffered = 0u64;
        let mut recovery_index_node_visits = 0u64;
        let mut recovery_index_entries_visited = 0u64;
        let mut recovery_index_height = 0u64;
        let mut recovery_index_nodes_written_last_append = 0u64;
        let mut recovery_segment_gets = 0u64;
        let mut recovery_segment_bytes_fetched = 0u64;
        let mut recovery_peak_segment_bytes_buffered = 0u64;
        let mut recovery_peak_index_node_bytes_buffered = 0u64;
        let mut recovery_peak_cursor_bytes_buffered = 0u64;
        let mut bounded_authority_index = true;
        let mut replay_progress_samples = vec![start_seq];
        let mut recovery_cursor = self.log.open_recovery_cursor(shard, start_seq)?;
        loop {
            let (entries, page_stats) = self.log.read_recovery_cursor_page(&mut recovery_cursor)?;
            if entries.is_empty() {
                break;
            }
            peak_replay_commands_buffered = peak_replay_commands_buffered.max(entries.len() as u64);
            peak_manifest_objects_buffered = peak_manifest_objects_buffered
                .max(page_stats.peak_manifest_objects_buffered as u64);
            recovery_index_node_visits = recovery_index_node_visits
                .saturating_add(page_stats.recovery_index_node_visits as u64);
            recovery_index_entries_visited = recovery_index_entries_visited
                .saturating_add(page_stats.recovery_index_entries_visited as u64);
            recovery_index_height =
                recovery_index_height.max(page_stats.recovery_index_height as u64);
            recovery_index_nodes_written_last_append = recovery_index_nodes_written_last_append
                .max(page_stats.recovery_index_nodes_written_last_append as u64);
            recovery_segment_gets =
                recovery_segment_gets.saturating_add(page_stats.segment_gets as u64);
            recovery_segment_bytes_fetched = recovery_segment_bytes_fetched
                .saturating_add(page_stats.segment_bytes_fetched as u64);
            recovery_peak_segment_bytes_buffered = recovery_peak_segment_bytes_buffered
                .max(page_stats.peak_segment_bytes_buffered as u64);
            recovery_peak_index_node_bytes_buffered = recovery_peak_index_node_bytes_buffered
                .max(page_stats.peak_index_node_bytes_buffered as u64);
            recovery_peak_cursor_bytes_buffered = recovery_peak_cursor_bytes_buffered
                .max(page_stats.peak_cursor_bytes_buffered as u64);
            bounded_authority_index &= page_stats.bounded_authority_index;
            for (_pos, env) in &entries {
                for id in &env.item_ids {
                    self.counters.observe(shard, *id);
                }
            }
            let positions: Vec<CommandPosition> = entries.iter().map(|(p, _)| p.clone()).collect();
            let envelopes: Vec<CommandEnvelope> =
                entries.into_iter().map(|(_, envelope)| envelope).collect();
            self.projection
                .apply_committed_batch(&positions, &envelopes)?;
            tail_replayed = tail_replayed.saturating_add(positions.len() as u64);
            next_seq = positions
                .last()
                .map(|position| position.sequence.saturating_add(1))
                .unwrap_or(next_seq);
            record_replay_progress(&mut replay_progress_samples, next_seq);
        }
        if tail_replayed > self.recovery_max_tail {
            eprintln!(
                "[recovery] segmented object-log-sqlite tail for {}:{} replayed {tail_replayed} commands \
                 beyond snapshot high-water {start_seq} (budget {}); projection may have fallen behind",
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                self.recovery_max_tail,
            );
        }
        self.recovery_stats
            .lock()
            .expect("recovery stats poisoned")
            .insert(
                shard.clone(),
                RecoveryStats {
                    start_seq,
                    tail_replayed,
                    snapshot_used,
                    replay_command_page_limit:
                        fireweed_objectlog::segmented::RECOVERY_COMMAND_PAGE_LIMIT as u64,
                    peak_replay_commands_buffered,
                    peak_manifest_objects_buffered,
                    manifest_object_page_limit: fireweed_objectlog::segmented::S3_LIST_PAGE_MAX_KEYS
                        as u64,
                    replay_worker_tasks: 1,
                    replay_progress_samples,
                    recovery_index_node_visits,
                    recovery_index_entries_visited,
                    recovery_index_height,
                    recovery_index_nodes_written_last_append,
                    recovery_segment_gets,
                    recovery_segment_bytes_fetched,
                    recovery_peak_segment_bytes_buffered,
                    recovery_peak_index_node_bytes_buffered,
                    recovery_peak_cursor_bytes_buffered,
                    bounded_authority_index,
                },
            );
        Ok(())
    }

    async fn require_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        if ids.iter().copied().collect::<HashSet<_>>().len() == ids.len()
            && self.projection.all_leased(shard, ids)?
        {
            return Ok(());
        }
        let renderable = self.projection.claimed_view(shard, ids).await?;
        if renderable.len() == ids.len() {
            Ok(())
        } else {
            Err(EngineError::Invalid("item is not leased"))
        }
    }
}

impl Backend for SegmentedObjectLogSqliteBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn commit_raw(
        &self,
        _request: fireweed_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::RawCommitOutcome>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ControlPlaneStore for SegmentedObjectLogSqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let outcome = self.log.create_definition(&definition)?;
            let definition = outcome.definition.clone();
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.log.create_queue(&definition)?;
            self.projection
                .create_queue_projection(definition.clone())?;
            self.queues
                .lock()
                .expect("segmented queues poisoned")
                .insert(key.clone(), definition);
            let compiled_schema = compile_queue_schema(&outcome.definition)?;
            if let Some(cs) = compiled_schema {
                self.schemas
                    .lock()
                    .expect("segmented schemas poisoned")
                    .insert(key.clone(), cs);
            }
            let epoch = self.log.current_epoch(&key).unwrap_or(0);
            self.set_epoch(&key, epoch);
            let _ = self.coord_for(&key);
            self.replay_queue(&key)?;
            Ok(outcome)
        }
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .queues
            .lock()
            .expect("segmented queues poisoned")
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result = self
            .queues
            .lock()
            .expect("segmented queues poisoned")
            .keys()
            .filter(|key| key.tenant_id.as_str() == tenant.as_str())
            .map(|key| key.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        std::future::ready(self.replay_queue(shard))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.current_epoch(shard);
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.acquire_epoch(shard, system_now_ms());
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }

    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.fence_epoch(shard, target_epoch, system_now_ms());
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }
}

impl PushPort for SegmentedObjectLogSqliteBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            // Hot path: copy just `max_attempts` (a `u32`) under the lock instead of cloning the whole
            // `QueueDefinition` on every push.
            let max_attempts = {
                let g = self.queues.lock().expect("segmented queues poisoned");
                g.get(shard)
                    .map(|d| d.retry_policy.max_attempts)
                    .ok_or(EngineError::NotFound)?
            };
            let schema = self
                .schemas
                .lock()
                .expect("segmented schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let envelope = self.next_envelope(
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            // Group-commit (no force): co-buffer with concurrent pushes; ack after the seal+apply.
            self.commit(shard, envelope, epoch, now, false).await?;
            Ok(ids)
        }
    }

    fn push_ordered_independent(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Vec<EngineResult<ItemId>>> + Send {
        async move {
            let count = items.len();
            if count > fireweed_engine::MAX_ORDERED_INDEPENDENT_PUSH_ITEMS {
                return vec![
                    Err(EngineError::Invalid(
                        "ordered independent push exceeds bounded item limit",
                    ));
                    count
                ];
            }
            let max_attempts = match self
                .queues
                .lock()
                .expect("segmented queues poisoned")
                .get(shard)
                .map(|definition| definition.retry_policy.max_attempts)
            {
                Some(max_attempts) => max_attempts,
                None => return vec![Err(EngineError::NotFound); count],
            };
            let schema = self
                .schemas
                .lock()
                .expect("segmented schemas poisoned")
                .get(shard)
                .cloned();
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let mut outcomes = vec![None; count];
            let mut accepted = Vec::with_capacity(count);
            for (index, item) in items.into_iter().enumerate() {
                if let Err(error) =
                    validate_gate_push(self.supports_gates(), std::slice::from_ref(&item)).and_then(
                        |()| validate_push_items(schema.as_ref(), std::slice::from_ref(&item)),
                    )
                {
                    outcomes[index] = Some(Err(error));
                    continue;
                }
                let counter_base = self.counters.reserve(shard, epoch, 1);
                let (mut push_items, mut ids) =
                    build_push_items(vec![item], epoch, self.node_id, counter_base, max_attempts);
                let id = ids.pop().expect("one scalar push id");
                let push_item = push_items.pop().expect("one scalar push item");
                let envelope = self.next_envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![push_item],
                    }),
                    vec![id],
                    now,
                );
                accepted.push((index, id, envelope));
            }
            let commit_results = self
                .commit_ordered_independent(
                    shard,
                    accepted
                        .iter()
                        .map(|(_, _, envelope)| envelope.clone())
                        .collect(),
                    epoch,
                    now,
                )
                .await;
            for ((index, id, _), result) in accepted.into_iter().zip(commit_results) {
                outcomes[index] = Some(result.map(|()| id));
            }
            outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every ordered push has an outcome"))
                .collect()
        }
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            // Serialize the request-id'd push with claims/commits on this queue so the cache
            // check + segment commit + record is atomic (the request-id path is not the hot path).
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            let (max_attempts, retention_ms) = {
                let g = self.queues.lock().expect("segmented queues poisoned");
                let d = g.get(shard).ok_or(EngineError::NotFound)?;
                (d.retry_policy.max_attempts, d.request_id_retention_ms)
            };
            let schema = self
                .schemas
                .lock()
                .expect("segmented schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let expires_at = request_expires_at(now, retention_ms);
            {
                let mut idem = self
                    .idempotency
                    .lock()
                    .expect("segmented idempotency poisoned");
                match idem
                    .entry(shard.clone())
                    .or_default()
                    .check(&request_id, fingerprint, now)
                {
                    IdempotencyDecision::Replay(ids) => return Ok(ids),
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let envelope = self.next_request_envelope(
                request_id.clone(),
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.commit(shard, envelope, epoch, now, false).await?;
            // Record only AFTER a successful commit, so a rejected append leaves no replay entry.
            self.idempotency
                .lock()
                .expect("segmented idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .record(request_id, fingerprint, ids.clone(), expires_at);
            Ok(ids)
        }
    }
}

impl ClaimPort for SegmentedObjectLogSqliteBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move {
            // Serialize select→commit→apply per queue so two claims never select the same candidate.
            let mutate = self.mutate_lock_for(&req.shard);
            let _guard = mutate.lock().await;
            if req.compatibility != ClaimCompatibility::default() {
                let definition = self.queue_definition(&req.shard).await?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, &definition)?;
            }
            let candidates = self
                .projection
                .select_eligible(&req.shard, req.eligibility_at(), req.max_items)
                .await?;
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let epoch = req
                .expected_epoch
                .unwrap_or_else(|| self.cached_epoch(&req.shard));
            let envelope = self.next_envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                    worker_id: Some(req.worker_id.clone()),
                }),
                candidates.clone(),
                req.now,
            );
            self.commit(&req.shard, envelope, epoch, req.now, true)
                .await?;
            let items = self
                .projection
                .claimed_view(&req.shard, &candidates)
                .await?;
            Ok(Claimed {
                items,
                ..Default::default()
            })
        }
    }
}

impl fireweed_engine::CommitTransitionPort for SegmentedObjectLogSqliteBackend {}
impl fireweed_engine::RecoveryReadPort for SegmentedObjectLogSqliteBackend {}

impl FinalizePort for SegmentedObjectLogSqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|outcome| outcome.item_id).collect();
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl UpsertPort for SegmentedObjectLogSqliteBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl RenewLeasePort for SegmentedObjectLogSqliteBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl ReassignLeasePort for SegmentedObjectLogSqliteBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl PurgePort for SegmentedObjectLogSqliteBackend {
    fn purge(
        &self,
        _shard: &QueueKey,
        _item_ids: Vec<ItemId>,
        _force: bool,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for SegmentedObjectLogSqliteBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let log = Arc::clone(&self.log);
        self.maintenance_dispatcher.dispatch(
            log,
            |available, in_flight| {
                registered_maintenance_page(
                    &self.registry,
                    &self.maintenance_cursor,
                    available,
                    in_flight,
                )
            },
            ts_to_ms(now),
        )
    }
}

impl ProjectionRead for SegmentedObjectLogSqliteBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        self.projection.select_eligible(shard, now, limit)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        self.projection.peek(shard, limit)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.pending(shard)
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        self.projection.pending_summary(shard)
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        self.projection.pending_page(shard, start, limit)
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection
            .pending_range(shard, start, end, consumer, limit)
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.pending_by_ids(shard, ids)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send
    {
        self.projection.claimed_view(shard, ids)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        self.projection.live_items(shard, keys)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        self.projection.metrics(queue)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        self.projection
            .terminal_emission_metrics(shard, now, emit_change_records, emission_cursor)
    }
}

// ===========================================================================
// Segmented (group-commit) object-log + in-memory projection backend (Fix B)
// ===========================================================================
//
// Same durable authority and group-commit ack-after-seal coordination as
// `SegmentedObjectLogSqliteBackend`, but the materialized projection is the in-process
// `fireweed_projection::ProjectionData` (one per queue, behind its own `Mutex`) instead of SQLite. The
// sealed segment + manifest entry is still the durable boundary (eventual-apply class preserved; recovery
// replays `read_all` into `ProjectionData` on `create_queue`); the per-segment projection write is now a
// cheap in-memory `apply_command` per command rather than a batched SQLite transaction. This is the fast
// path selected by `FIREWEED_OBJECT_LOG_MODE=segmented` + `FIREWEED_PROJECTION_BACKEND=inmemory`.

/// Group-committing object-log authority (`SegmentedObjectLog<LocalFsBlobStore>`) + in-memory
/// `ProjectionData`. Eventual-apply durability class.
pub struct SegmentedObjectLogInMemoryBackend {
    log: Arc<FsSegmentedLog>,
    /// One in-memory projection per queue, each behind its own `Mutex` (applied on seal, read on the
    /// query ports). A derived, rebuildable view — `read_all` replay reconstructs it on open.
    projections: Mutex<HashMap<QueueKey, Arc<Mutex<ProjectionData>>>>,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
    /// Compiled entity schema per queue (ADR-011). Populated at `create_queue` time; consulted on every
    /// push path to reject invalid entity documents before counter reservation or durable append.
    schemas: Mutex<HashMap<QueueKey, Arc<CompiledSchema>>>,
    epochs: Mutex<HashMap<QueueKey, u64>>,
    registry: Mutex<QueueRegistry<Arc<ShardCoord>>>,
    maintenance_cursor: AtomicUsize,
    maintenance_dispatcher: RecoveryMaintenanceDispatcher,
    mutate_locks: Mutex<HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    flush_interval: Duration,
    flush_slots: Arc<tokio::sync::Semaphore>,
    flush_cursor: AtomicUsize,
    flush_inflight: Mutex<HashSet<QueueKey>>,
    /// Observed full-log replay telemetry for the intentionally ephemeral projection.
    recovery_stats: Mutex<HashMap<QueueKey, RecoveryStats>>,
    /// Per-queue request-id replay/conflict cache (API-001 / TD-007 §4): a retried `request_id` with the
    /// same body replays the committed ids without a second append; a different body is `RequestIdConflict`.
    idempotency: Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>,
    byte_budget: BufferedByteBudget,
    queue_byte_limit: usize,
    debug_segments: bool,
}

impl SegmentedObjectLogInMemoryBackend {
    /// Bounded page of authoritative pending order, exposed for release recovery verification.
    pub fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<fireweed_engine::ItemView>> {
        let projection = self.projection_for(shard)?;
        Ok(projection
            .lock()
            .expect("segmented inmemory projection poisoned")
            .peek_page(after, limit))
    }

    /// Install the deterministic object-log fault seam used by blocking/fencing conformance tests.
    pub fn set_object_log_fault_hook(&self, hook: Option<Arc<dyn FaultHook>>) {
        self.log.set_fault_hook(hook);
    }

    /// Open (or recover) a segmented object log rooted at `object_root` with `config`, paired with in-memory
    /// projections. Recovery replays committed segments into each queue's `ProjectionData` in `create_queue`.
    pub fn open(object_root: impl Into<PathBuf>, config: SegmentConfig) -> EngineResult<Self> {
        let store: Arc<dyn BlobStore> = Arc::new(LocalFsBlobStore::open(object_root)?);
        Self::open_with_blob_store(store, config)
    }

    /// Open over a caller-selected production blob store.
    pub fn open_with_blob_store(
        store: Arc<dyn BlobStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let log = Arc::new(SegmentedObjectLog::open(store, config));
        let flush_ms = (config.max_latency_ms / 4).max(1);
        Ok(Self {
            log,
            projections: Mutex::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            registry: Mutex::new(QueueRegistry::default()),
            maintenance_cursor: AtomicUsize::new(0),
            maintenance_dispatcher: RecoveryMaintenanceDispatcher::new()?,
            mutate_locks: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            flush_interval: Duration::from_millis(flush_ms),
            flush_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            flush_cursor: AtomicUsize::new(0),
            flush_inflight: Mutex::new(HashSet::new()),
            recovery_stats: Mutex::new(HashMap::new()),
            idempotency: Mutex::new(HashMap::new()),
            byte_budget: default_objectlog_byte_budget(),
            queue_byte_limit: crate::DEFAULT_OBJECTLOG_QUEUE_WAITING_BYTES,
            debug_segments: false,
        })
    }

    /// Open over a no-CAS object store with a transactional manifest-pointer authority.
    pub fn open_with_manifest_pointer(
        store: Arc<dyn BlobStore>,
        pointers: Arc<dyn ManifestPointerStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Self::open_with_blob_store(
            Arc::new(PointerFencedBlobStore::new(store, pointers)),
            config,
        )
    }

    /// Production no-CAS composition: immutable objects remain in the supplied store while Postgres owns
    /// the atomically versioned manifest head and assignment epoch.
    pub fn open_with_postgres_manifest_pointer(
        store: Arc<dyn BlobStore>,
        postgres_url: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let pointers = Arc::new(fireweed_postgres::PostgresManifestPointer::open(
            postgres_url,
        )?);
        Self::open_with_manifest_pointer(store, pointers, config)
    }

    /// A snapshot of the measured group-commit segment/object counters (segments sealed, objects PUT,
    /// commands committed, per-segment batch sizes) for the in-memory projection variant.
    pub fn segment_counters(&self) -> SegmentCounters {
        self.log.counters()
    }

    /// Last observed durable-log rebuild for this queue. The in-memory projection has no snapshot, so a
    /// successful reopen records `start_seq=0`, `snapshot_used=false`, and the exact number of replayed
    /// command envelopes in `tail_replayed`.
    pub fn recovery_stats(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats
            .lock()
            .expect("inmemory recovery stats poisoned")
            .get(shard)
            .cloned()
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    pub fn with_worker_partition(self, _index: usize, _partitions: usize) -> Self {
        self
    }

    pub fn with_byte_admission(
        mut self,
        budget: BufferedByteBudget,
        queue_byte_limit: usize,
    ) -> Self {
        self.byte_budget = budget;
        self.queue_byte_limit = queue_byte_limit;
        self
    }

    pub fn byte_admission_stats(&self) -> BufferedByteBudgetStats {
        self.byte_budget.stats()
    }

    pub fn byte_admission_snapshot(&self) -> ObjectLogByteAdmissionSnapshot {
        production_byte_admission_snapshot(&self.byte_budget, self.queue_byte_limit)
    }

    pub fn with_debug_segments(mut self, debug_segments: bool) -> Self {
        self.debug_segments = debug_segments;
        self
    }

    /// Spawn the background flusher that seals each queue's latency-due segment (the latency seal trigger).
    pub fn spawn_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let interval = self.flush_interval;
        let debug_segments = self.debug_segments;
        fireweed_resp::spawn_governed(async move {
            Self::flush_loop(weak, interval, debug_segments).await
        })
    }

    fn projection_for(&self, shard: &QueueKey) -> EngineResult<Arc<Mutex<ProjectionData>>> {
        self.projections
            .lock()
            .expect("segmented inmemory projections poisoned")
            .get(shard)
            .cloned()
            .ok_or(EngineError::NotFound)
    }

    fn coord_for(&self, shard: &QueueKey) -> Arc<ShardCoord> {
        self.registry
            .lock()
            .expect("segmented queue registry poisoned")
            .get_or_insert_with(shard, || {
                Arc::new(ShardCoord {
                    state: tokio::sync::Mutex::new(CoordState {
                        pending: Vec::new(),
                        permits: Vec::new(),
                        waiters: Vec::new(),
                    }),
                })
            })
    }

    fn mutate_lock_for(&self, shard: &QueueKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut g = self
            .mutate_locks
            .lock()
            .expect("segmented mutate-locks poisoned");
        g.entry(shard.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn cached_epoch(&self, shard: &QueueKey) -> u64 {
        self.epochs
            .lock()
            .expect("segmented epochs poisoned")
            .get(shard)
            .copied()
            .unwrap_or(0)
    }

    fn set_epoch(&self, shard: &QueueKey, epoch: u64) {
        self.epochs
            .lock()
            .expect("segmented epochs poisoned")
            .insert(shard.clone(), epoch);
    }

    fn next_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("seginmem-{}-{n}", self.node_id)),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    /// Same as [`Self::next_envelope`] but carries API-001's envelope-level `request_id` into the durable
    /// command (the request-id'd push path), so the committed log records the caller's request id.
    fn next_request_envelope(
        &self,
        request_id: RequestId,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        now: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.command_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("seginmem-{}-{n}", self.node_id)),
            request_id: Some(request_id),
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    /// Apply a freshly-sealed batch to the queue's in-memory projection, then complete its waiters.
    /// `positions` covers the WHOLE drained substrate buffer, pairing 1:1 with the front of `pending`.
    fn distribute(&self, state: &mut CoordState, positions: Vec<CommandPosition>) {
        let n = positions.len();
        debug_assert!(
            n <= state.pending.len() && n <= state.waiters.len(),
            "sealed batch cannot exceed buffered/waiting commands"
        );
        let envelopes: Vec<CommandEnvelope> = state.pending.drain(..n).collect();
        let permits: Vec<OwnedBytePermit> = state.permits.drain(..n).collect();
        let waiters: Vec<_> = state.waiters.drain(..n).collect();
        let result = match positions.first() {
            Some(pos) => self.apply_batch(&pos.queue, &envelopes),
            None => Ok(()),
        };
        drop(permits);
        for w in waiters {
            let _ = w.send(result.clone());
        }
    }

    /// Apply a batch of already-durable commands to the queue's in-memory projection (one lock, infallible
    /// per command because the orchestration ports pre-validate — same contract as the file backend).
    fn apply_batch(&self, shard: &QueueKey, envelopes: &[CommandEnvelope]) -> EngineResult<()> {
        let proj = self.projection_for(shard)?;
        let mut p = proj.lock().expect("segmented inmemory projection poisoned");
        for env in envelopes {
            p.apply_command(&env.command)?;
        }
        Ok(())
    }

    fn fail_all(state: &mut CoordState, err: EngineError) {
        state.pending.clear();
        state.permits.clear();
        for w in state.waiters.drain(..) {
            let _ = w.send(Err(err.clone()));
        }
    }

    /// Register one envelope on the coordinator and enqueue it into the segment buffer (ack-after-seal).
    async fn commit(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        epoch: u64,
        now: UtcTimestamp,
        force: bool,
    ) -> EngineResult<()> {
        validate_gate_command(false, &envelope.command)?;
        if matches!(envelope.command, QueueCommand::ReplacePending(_)) {
            return Err(EngineError::Unavailable);
        }
        let coord = self.coord_for(shard);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = coord.state.lock().await;
            let (serialized, charged_bytes) = prepare_serialized_commands(
                vec![envelope],
                self.byte_budget.config().global_limit(),
            )?;
            let queue_bytes: usize = state.permits.iter().map(OwnedBytePermit::bytes).sum();
            if !state.pending.is_empty()
                && queue_bytes.saturating_add(charged_bytes) > self.queue_byte_limit
            {
                return Err(EngineError::Backpressure {
                    resource: "queue buffered bytes",
                });
            }
            let permit = self
                .byte_budget
                .try_acquire(shard.tenant_id.clone(), charged_bytes)
                .map_err(map_byte_admission_error)?;
            let now_ms = ts_to_ms(now);
            let enqueued = self
                .log
                .enqueue_serialized(shard, serialized, epoch, now_ms);
            match enqueued {
                Ok((outcome, envelopes)) => {
                    state.pending.extend(envelopes);
                    state.permits.push(permit);
                    state.waiters.push(tx);
                    if !outcome.committed.is_empty() {
                        self.distribute(&mut state, outcome.committed);
                    } else if force {
                        match self.log.seal(shard, epoch, now_ms) {
                            Ok(positions) => self.distribute(&mut state, positions),
                            Err(e) => Self::fail_all(&mut state, e),
                        }
                    }
                }
                Err(e) => {
                    Self::fail_all(&mut state, e.clone());
                    let _ = tx.send(Err(e));
                }
            }
        }
        rx.await
            .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))?
    }

    async fn commit_ordered_independent(
        &self,
        shard: &QueueKey,
        envelopes: Vec<CommandEnvelope>,
        epoch: u64,
        now: UtcTimestamp,
    ) -> Vec<EngineResult<()>> {
        let count = envelopes.len();
        let coord = self.coord_for(shard);
        let mut outcomes = vec![None; count];
        let mut receivers = Vec::with_capacity(count);
        {
            let mut state = coord.state.lock().await;
            for (index, envelope) in envelopes.into_iter().enumerate() {
                let (serialized, charged_bytes) = match prepare_serialized_commands(
                    vec![envelope],
                    self.byte_budget.config().global_limit(),
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
                let queue_bytes: usize = state.permits.iter().map(OwnedBytePermit::bytes).sum();
                if !state.pending.is_empty()
                    && queue_bytes.saturating_add(charged_bytes) > self.queue_byte_limit
                {
                    outcomes[index] = Some(Err(EngineError::Backpressure {
                        resource: "queue buffered bytes",
                    }));
                    continue;
                }
                let permit = match self
                    .byte_budget
                    .try_acquire(shard.tenant_id.clone(), charged_bytes)
                    .map_err(map_byte_admission_error)
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
                let (tx, rx) = oneshot::channel();
                match self
                    .log
                    .enqueue_serialized(shard, serialized, epoch, ts_to_ms(now))
                {
                    Ok((outcome, accepted)) => {
                        state.pending.extend(accepted);
                        state.permits.push(permit);
                        state.waiters.push(tx);
                        receivers.push((index, rx));
                        if !outcome.committed.is_empty() {
                            self.distribute(&mut state, outcome.committed);
                        }
                    }
                    Err(error) => {
                        Self::fail_all(&mut state, error.clone());
                        outcomes[index] = Some(Err(error.clone()));
                        for outcome in outcomes.iter_mut().skip(index + 1) {
                            *outcome = Some(Err(error.clone()));
                        }
                        break;
                    }
                }
            }
        }
        for (index, receiver) in receivers {
            outcomes[index] = Some(
                receiver
                    .await
                    .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))
                    .and_then(|result| result),
            );
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every ordered push has an outcome"))
            .collect()
    }

    async fn flush_loop(
        weak: std::sync::Weak<Self>,
        flush_interval: Duration,
        debug_segments: bool,
    ) {
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut dbg_last = std::time::Instant::now();
        loop {
            ticker.tick().await;
            let Some(this) = weak.upgrade() else {
                break;
            };
            if debug_segments && dbg_last.elapsed() >= Duration::from_secs(1) {
                dbg_last = std::time::Instant::now();
                let counters = this.log.counters();
                let admission = byte_admission_telemetry(this.byte_admission_snapshot());
                eprintln!(
                    "[seg] profile=objectlog/inmemory sealed={} commands={} mean_batch={:.1} max_batch={} objects_put={} {}",
                    counters.segments_sealed,
                    counters.commands_committed,
                    counters.mean_batch_size(),
                    counters.max_batch_size(),
                    counters.objects_put,
                    admission,
                );
            }
            let shards =
                registered_queue_page(&this.registry, &this.flush_cursor, SEGMENT_FLUSH_QUEUE_PAGE);
            let now_ms = system_now_ms();
            for (shard, coord) in shards {
                let Ok(slot) = this.flush_slots.clone().try_acquire_owned() else {
                    break;
                };
                {
                    let mut inflight = this.flush_inflight.lock().expect("flush inflight poisoned");
                    if !inflight.insert(shard.clone()) {
                        continue;
                    }
                }
                let backend = Arc::clone(&this);
                tokio::task::spawn_blocking(move || {
                    let _slot = slot;
                    let _inflight = FlushInflightGuard {
                        inflight: &backend.flush_inflight,
                        shard: shard.clone(),
                    };
                    let epoch = backend.cached_epoch(&shard);
                    let mut state = coord.state.blocking_lock();
                    if !state.pending.is_empty() {
                        match backend.log.flush_due(&shard, epoch, now_ms) {
                            Ok(positions) if !positions.is_empty() => {
                                backend.distribute(&mut state, positions)
                            }
                            Ok(_) => {}
                            Err(e) => Self::fail_all(&mut state, e),
                        }
                    }
                });
            }
        }
    }

    /// Replay every committed segment for `shard` into its in-memory projection (recovery / open).
    fn replay_queue(
        &self,
        shard: &QueueKey,
        proj: &Arc<Mutex<ProjectionData>>,
    ) -> EngineResult<RecoveryStats> {
        let mut next_seq = 0u64;
        let mut replayed = 0u64;
        let mut peak_replay_commands_buffered = 0u64;
        let mut peak_manifest_objects_buffered = 0u64;
        let mut recovery_index_node_visits = 0u64;
        let mut recovery_index_entries_visited = 0u64;
        let mut recovery_index_height = 0u64;
        let mut recovery_index_nodes_written_last_append = 0u64;
        let mut recovery_segment_gets = 0u64;
        let mut recovery_segment_bytes_fetched = 0u64;
        let mut recovery_peak_segment_bytes_buffered = 0u64;
        let mut recovery_peak_index_node_bytes_buffered = 0u64;
        let mut recovery_peak_cursor_bytes_buffered = 0u64;
        let mut bounded_authority_index = true;
        let mut replay_progress_samples = vec![0];
        let mut recovery_cursor = self.log.open_recovery_cursor(shard, 0)?;
        loop {
            let (entries, page_stats) = self.log.read_recovery_cursor_page(&mut recovery_cursor)?;
            if entries.is_empty() {
                break;
            }
            peak_replay_commands_buffered = peak_replay_commands_buffered.max(entries.len() as u64);
            peak_manifest_objects_buffered = peak_manifest_objects_buffered
                .max(page_stats.peak_manifest_objects_buffered as u64);
            recovery_index_node_visits = recovery_index_node_visits
                .saturating_add(page_stats.recovery_index_node_visits as u64);
            recovery_index_entries_visited = recovery_index_entries_visited
                .saturating_add(page_stats.recovery_index_entries_visited as u64);
            recovery_index_height =
                recovery_index_height.max(page_stats.recovery_index_height as u64);
            recovery_index_nodes_written_last_append = recovery_index_nodes_written_last_append
                .max(page_stats.recovery_index_nodes_written_last_append as u64);
            recovery_segment_gets =
                recovery_segment_gets.saturating_add(page_stats.segment_gets as u64);
            recovery_segment_bytes_fetched = recovery_segment_bytes_fetched
                .saturating_add(page_stats.segment_bytes_fetched as u64);
            recovery_peak_segment_bytes_buffered = recovery_peak_segment_bytes_buffered
                .max(page_stats.peak_segment_bytes_buffered as u64);
            recovery_peak_index_node_bytes_buffered = recovery_peak_index_node_bytes_buffered
                .max(page_stats.peak_index_node_bytes_buffered as u64);
            recovery_peak_cursor_bytes_buffered = recovery_peak_cursor_bytes_buffered
                .max(page_stats.peak_cursor_bytes_buffered as u64);
            bounded_authority_index &= page_stats.bounded_authority_index;
            let mut p = proj.lock().expect("segmented inmemory projection poisoned");
            for (position, envelope) in entries {
                for id in &envelope.item_ids {
                    self.counters.observe(shard, *id);
                }
                p.apply_command(&envelope.command)?;
                replayed = replayed.saturating_add(1);
                next_seq = position.sequence.saturating_add(1);
            }
            drop(p);
            record_replay_progress(&mut replay_progress_samples, next_seq);
        }
        Ok(RecoveryStats {
            start_seq: 0,
            tail_replayed: replayed,
            snapshot_used: false,
            replay_command_page_limit: fireweed_objectlog::segmented::RECOVERY_COMMAND_PAGE_LIMIT
                as u64,
            peak_replay_commands_buffered,
            peak_manifest_objects_buffered,
            manifest_object_page_limit: fireweed_objectlog::segmented::S3_LIST_PAGE_MAX_KEYS as u64,
            replay_worker_tasks: 1,
            replay_progress_samples,
            recovery_index_node_visits,
            recovery_index_entries_visited,
            recovery_index_height,
            recovery_index_nodes_written_last_append,
            recovery_segment_gets,
            recovery_segment_bytes_fetched,
            recovery_peak_segment_bytes_buffered,
            recovery_peak_index_node_bytes_buffered,
            recovery_peak_cursor_bytes_buffered,
            bounded_authority_index,
        })
    }

    async fn require_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let proj = self.projection_for(shard)?;
        let p = proj.lock().expect("segmented inmemory projection poisoned");
        if p.render_claimed(ids).len() == ids.len() {
            Ok(())
        } else {
            Err(EngineError::Invalid("item is not leased"))
        }
    }
}

impl Backend for SegmentedObjectLogInMemoryBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn commit_raw(
        &self,
        _request: fireweed_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::RawCommitOutcome>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ControlPlaneStore for SegmentedObjectLogInMemoryBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let outcome = self.log.create_definition(&definition)?;
            let definition = outcome.definition.clone();
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.log.create_queue(&definition)?;
            let proj = Arc::new(Mutex::new(
                ProjectionData::new(
                    definition.priority_model,
                    definition.ordering_mode,
                    definition.max_rank_error,
                    definition.recurrence,
                    &definition.secondary_indexes,
                )
                .with_typed_indexes(&definition.typed_indexes),
            ));
            self.projections
                .lock()
                .expect("segmented inmemory projections poisoned")
                .insert(key.clone(), proj.clone());
            self.queues
                .lock()
                .expect("segmented queues poisoned")
                .insert(key.clone(), definition.clone());
            let compiled_schema = compile_queue_schema(&definition)?;
            if let Some(cs) = compiled_schema {
                self.schemas
                    .lock()
                    .expect("segmented inmemory schemas poisoned")
                    .insert(key.clone(), cs);
            }
            let epoch = self.log.current_epoch(&key).unwrap_or(0);
            self.set_epoch(&key, epoch);
            let _ = self.coord_for(&key);
            let recovery_stats = self.replay_queue(&key, &proj)?;
            self.recovery_stats
                .lock()
                .expect("inmemory recovery stats poisoned")
                .insert(key, recovery_stats);
            Ok(outcome)
        }
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .queues
            .lock()
            .expect("segmented queues poisoned")
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result = self
            .queues
            .lock()
            .expect("segmented queues poisoned")
            .keys()
            .filter(|key| key.tenant_id.as_str() == tenant.as_str())
            .map(|key| key.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let definition = self
                .queues
                .lock()
                .expect("segmented queues poisoned")
                .get(shard)
                .cloned()
                .ok_or(EngineError::NotFound)?;
            let projection = Arc::new(Mutex::new(
                ProjectionData::new(
                    definition.priority_model,
                    definition.ordering_mode,
                    definition.max_rank_error,
                    definition.recurrence,
                    &definition.secondary_indexes,
                )
                .with_typed_indexes(&definition.typed_indexes),
            ));
            let recovery_stats = self.replay_queue(shard, &projection)?;
            self.projections
                .lock()
                .expect("segmented inmemory projections poisoned")
                .insert(shard.clone(), projection);
            self.recovery_stats
                .lock()
                .expect("inmemory recovery stats poisoned")
                .insert(shard.clone(), recovery_stats);
            Ok(())
        })();
        std::future::ready(result)
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.current_epoch(shard);
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.acquire_epoch(shard, system_now_ms());
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }

    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = self.log.fence_epoch(shard, target_epoch, system_now_ms());
        if let Ok(epoch) = result {
            self.set_epoch(shard, epoch);
        }
        std::future::ready(result)
    }
}

impl PushPort for SegmentedObjectLogInMemoryBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            let max_attempts = {
                let g = self.queues.lock().expect("segmented queues poisoned");
                g.get(shard)
                    .map(|d| d.retry_policy.max_attempts)
                    .ok_or(EngineError::NotFound)?
            };
            // Pre-commit entity schema validation (ADR-011): reject before counter reservation or append.
            let schema = self
                .schemas
                .lock()
                .expect("segmented inmemory schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let envelope = self.next_envelope(
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.commit(shard, envelope, epoch, now, false).await?;
            Ok(ids)
        }
    }

    fn push_ordered_independent(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = Vec<EngineResult<ItemId>>> + Send {
        async move {
            let count = items.len();
            if count > fireweed_engine::MAX_ORDERED_INDEPENDENT_PUSH_ITEMS {
                return vec![
                    Err(EngineError::Invalid(
                        "ordered independent push exceeds bounded item limit",
                    ));
                    count
                ];
            }
            let max_attempts = match self
                .queues
                .lock()
                .expect("segmented queues poisoned")
                .get(shard)
                .map(|definition| definition.retry_policy.max_attempts)
            {
                Some(max_attempts) => max_attempts,
                None => return vec![Err(EngineError::NotFound); count],
            };
            let schema = self
                .schemas
                .lock()
                .expect("segmented inmemory schemas poisoned")
                .get(shard)
                .cloned();
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let mut outcomes = vec![None; count];
            let mut accepted = Vec::with_capacity(count);
            for (index, item) in items.into_iter().enumerate() {
                if let Err(error) =
                    validate_gate_push(self.supports_gates(), std::slice::from_ref(&item)).and_then(
                        |()| validate_push_items(schema.as_ref(), std::slice::from_ref(&item)),
                    )
                {
                    outcomes[index] = Some(Err(error));
                    continue;
                }
                let counter_base = self.counters.reserve(shard, epoch, 1);
                let (mut push_items, mut ids) =
                    build_push_items(vec![item], epoch, self.node_id, counter_base, max_attempts);
                let id = ids.pop().expect("one scalar push id");
                let push_item = push_items.pop().expect("one scalar push item");
                let envelope = self.next_envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![push_item],
                    }),
                    vec![id],
                    now,
                );
                accepted.push((index, id, envelope));
            }
            let commit_results = self
                .commit_ordered_independent(
                    shard,
                    accepted
                        .iter()
                        .map(|(_, _, envelope)| envelope.clone())
                        .collect(),
                    epoch,
                    now,
                )
                .await;
            for ((index, id, _), result) in accepted.into_iter().zip(commit_results) {
                outcomes[index] = Some(result.map(|()| id));
            }
            outcomes
                .into_iter()
                .map(|outcome| outcome.expect("every ordered push has an outcome"))
                .collect()
        }
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            validate_gate_push(self.supports_gates(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            // Serialize the request-id'd push with claims/commits on this queue so the cache
            // check + segment commit + record is atomic (the request-id path is not the hot path).
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            let (max_attempts, retention_ms) = {
                let g = self.queues.lock().expect("segmented queues poisoned");
                let d = g.get(shard).ok_or(EngineError::NotFound)?;
                (d.retry_policy.max_attempts, d.request_id_retention_ms)
            };
            // Pre-commit entity schema validation (ADR-011): reject before counter reservation.
            // A rejected append leaves no idempotency entry (record happens only on success below).
            let schema = self
                .schemas
                .lock()
                .expect("segmented inmemory schemas poisoned")
                .get(shard)
                .cloned();
            validate_push_items(schema.as_ref(), &items)?;
            let expires_at = request_expires_at(now, retention_ms);
            {
                let mut idem = self
                    .idempotency
                    .lock()
                    .expect("segmented idempotency poisoned");
                match idem
                    .entry(shard.clone())
                    .or_default()
                    .check(&request_id, fingerprint, now)
                {
                    IdempotencyDecision::Replay(ids) => return Ok(ids),
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let envelope = self.next_request_envelope(
                request_id.clone(),
                QueueCommand::Push(PushCommand { items: push_items }),
                ids.clone(),
                now,
            );
            self.commit(shard, envelope, epoch, now, false).await?;
            // Record only AFTER a successful commit, so a rejected append leaves no replay entry.
            self.idempotency
                .lock()
                .expect("segmented idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .record(request_id, fingerprint, ids.clone(), expires_at);
            Ok(ids)
        }
    }
}

impl ClaimPort for SegmentedObjectLogInMemoryBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move {
            let mutate = self.mutate_lock_for(&req.shard);
            let _guard = mutate.lock().await;
            if req.compatibility != ClaimCompatibility::default() {
                let definition = self.queue_definition(&req.shard).await?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, &definition)?;
            }
            let candidates = {
                let proj = self.projection_for(&req.shard)?;
                let p = proj.lock().expect("segmented inmemory projection poisoned");
                p.select_eligible(req.eligibility_at(), req.max_items)
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let epoch = req
                .expected_epoch
                .unwrap_or_else(|| self.cached_epoch(&req.shard));
            let envelope = self.next_envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                    worker_id: Some(req.worker_id.clone()),
                }),
                candidates.clone(),
                req.now,
            );
            self.commit(&req.shard, envelope, epoch, req.now, true)
                .await?;
            let items = {
                let proj = self.projection_for(&req.shard)?;
                let p = proj.lock().expect("segmented inmemory projection poisoned");
                p.render_claimed(&candidates)
            };
            Ok(Claimed {
                items,
                ..Default::default()
            })
        }
    }
}

impl fireweed_engine::CommitTransitionPort for SegmentedObjectLogInMemoryBackend {}
impl fireweed_engine::RecoveryReadPort for SegmentedObjectLogInMemoryBackend {}

impl FinalizePort for SegmentedObjectLogInMemoryBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|outcome| outcome.item_id).collect();
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl UpsertPort for SegmentedObjectLogInMemoryBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl RenewLeasePort for SegmentedObjectLogInMemoryBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl ReassignLeasePort for SegmentedObjectLogInMemoryBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let mutate = self.mutate_lock_for(shard);
            let _guard = mutate.lock().await;
            self.require_leased(shard, &item_ids).await?;
            let epoch = expected_epoch.unwrap_or_else(|| self.cached_epoch(shard));
            let envelope = self.next_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit(shard, envelope, epoch, now, true).await
        }
    }
}

impl PurgePort for SegmentedObjectLogInMemoryBackend {
    fn purge(
        &self,
        _shard: &QueueKey,
        _item_ids: Vec<ItemId>,
        _force: bool,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for SegmentedObjectLogInMemoryBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let log = Arc::clone(&self.log);
        self.maintenance_dispatcher.dispatch(
            log,
            |available, in_flight| {
                registered_maintenance_page(
                    &self.registry,
                    &self.maintenance_cursor,
                    available,
                    in_flight,
                )
            },
            ts_to_ms(now),
        )
    }
}

impl ProjectionRead for SegmentedObjectLogInMemoryBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.select_eligible(now, limit))
        })();
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.peek(limit))
        })();
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.pending_leases())
        })();
        std::future::ready(result)
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        let result = (|| {
            let projection = self.projection_for(shard)?;
            Ok(projection
                .lock()
                .expect("segmented inmemory projection poisoned")
                .pending_summary())
        })();
        std::future::ready(result)
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        let result = (|| {
            let projection = self.projection_for(shard)?;
            Ok(projection
                .lock()
                .expect("segmented inmemory projection poisoned")
                .pending_page(start, limit))
        })();
        std::future::ready(result)
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let projection = self.projection_for(shard)?;
            Ok(projection
                .lock()
                .expect("segmented inmemory projection poisoned")
                .pending_range(start, end, consumer, limit))
        })();
        std::future::ready(result)
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let projection = self.projection_for(shard)?;
            Ok(projection
                .lock()
                .expect("segmented inmemory projection poisoned")
                .pending_by_ids(ids))
        })();
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send
    {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.render_claimed(ids))
        })();
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.live_items_by_key(keys))
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let proj = self.projection_for(queue)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.metrics())
        })();
        std::future::ready(result)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let result = (|| {
            let proj = self.projection_for(shard)?;
            let p = proj.lock().expect("segmented inmemory projection poisoned");
            Ok(p.terminal_emission_metrics(now, emit_change_records, emission_cursor))
        })();
        std::future::ready(result)
    }
}

// ===========================================================================
// Snapshot-tail recovery tests (bead pqueue-8a76daad)
// ===========================================================================
#[cfg(test)]
#[path = "object_log_sqlite_sp06_handoff_profile_tests.rs"]
mod sp06_handoff_profile_tests;

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use fireweed_core::{
        CohortOnIncomplete, CohortPolicy, EligibilityPolicy, EntitySchemaDocument, GateKeyPolicy,
        IndexDeclaration, IndexDef, IndexSpec, IndexType, MetadataValue, OrderingMode,
        PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueIndex,
        RecurrenceMode, RecurrencePolicy, RequestId, RetryPolicy, WorkerId,
    };
    use fireweed_engine::{
        ControlPlaneStore, EngineError, ProjectionRead, PushPort, ReclaimDriver,
    };
    use fireweed_objectlog::segmented::InMemoryBlobStore;
    use serde_json::json;

    fn registry_key(index: usize) -> QueueKey {
        QueueKey::new(
            TenantId::new("registry-test").unwrap(),
            QueueId::new(format!("q-{index}")).unwrap(),
        )
    }

    #[test]
    fn queue_registry_registration_is_unique_and_keeps_original_value() {
        let mut registry = QueueRegistry::default();
        let key = registry_key(7);

        assert_eq!(registry.get_or_insert_with(&key, || 11), 11);
        assert_eq!(registry.get_or_insert_with(&key, || 99), 11);
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.order, vec![key]);
    }

    #[test]
    fn queue_registry_pages_are_stable_fair_and_bounded() {
        const QUEUES: usize = 12;
        const PAGE: usize = 4;
        let mut registry = QueueRegistry::default();
        for index in 0..QUEUES {
            let key = registry_key(index);
            registry.get_or_insert_with(&key, || index);
        }
        let cursor = AtomicUsize::new(0);
        let mut visited = Vec::new();
        for _ in 0..(QUEUES / PAGE) {
            let page = registry.page(&cursor, PAGE);
            assert_eq!(page.len(), PAGE);
            visited.extend(page.into_iter().map(|(key, _)| key));
        }
        assert_eq!(visited, registry.order);
        assert_eq!(registry.page(&cursor, PAGE)[0].0, registry.order[0]);
    }

    #[test]
    fn queue_registry_large_cardinality_still_materializes_only_one_fixed_page() {
        const QUEUES: usize = 100_000;
        let mut registry = QueueRegistry::default();
        for index in 0..QUEUES {
            let key = registry_key(index);
            registry.get_or_insert_with(&key, || index);
        }
        assert_eq!(registry.entries.len(), QUEUES);
        assert_eq!(registry.order.len(), QUEUES);

        let cursor = AtomicUsize::new(QUEUES - 2);
        let page = registry.page(&cursor, SEGMENT_FLUSH_QUEUE_PAGE);
        assert_eq!(page.len(), SEGMENT_FLUSH_QUEUE_PAGE);
        assert_eq!(page[0].1, QUEUES - 2);
        assert_eq!(page[1].1, QUEUES - 1);
        assert_eq!(page[2].1, 0);
        assert_eq!(page[3].1, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn fixed_backend_pool_shares_one_sqlite_writer_across_queues() {
        const MEMBERS: usize = 8;
        const PUSHES: usize = 50;
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
        let projection = Arc::new(SqliteProjectionStore::in_memory().unwrap());
        let mut members = Vec::new();
        for index in 0..MEMBERS {
            let backend = Arc::new(
                SegmentedObjectLogSqliteBackend::open_with_blob_store_and_projection(
                    Arc::clone(&store),
                    Arc::clone(&projection),
                    seal_each_config(),
                )
                .unwrap()
                .with_worker_partition(index, MEMBERS),
            );
            let definition = queue_def("pool", &format!("q-{index}"));
            backend.create_queue(definition).await.unwrap();
            members.push(backend);
        }

        let pushes: Vec<_> = members
            .iter()
            .enumerate()
            .map(|(index, backend)| {
                let backend = Arc::clone(backend);
                let shard = QueueKey::new(
                    TenantId::new("pool").unwrap(),
                    QueueId::new(format!("q-{index}")).unwrap(),
                );
                tokio::spawn(async move {
                    for push in 0..PUSHES {
                        backend
                            .push(
                                &shard,
                                vec![spec(&format!("pool-{index}-{push}"))],
                                ts(),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    assert_eq!(
                        backend.metrics(&shard).await.unwrap().pending,
                        PUSHES as u64
                    );
                })
            })
            .collect();
        for push in pushes {
            push.await.unwrap();
        }
    }

    /// A unique scratch directory under the system temp dir, removed on drop.
    struct TmpDir {
        path: PathBuf,
    }
    impl TmpDir {
        fn new(label: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = std::env::temp_dir().join(format!(
                "fireweed-recovery-{label}-{}-{n}-{nanos}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
        fn object_root(&self) -> PathBuf {
            self.path.join("object-log")
        }
        fn projection(&self) -> String {
            self.projection_named("projection")
        }
        fn projection_named(&self, name: &str) -> String {
            self.path
                .join(format!("{name}.db"))
                .to_str()
                .expect("utf8 temp path")
                .to_string()
        }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn queue_def(tenant: &str, queue: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new(tenant).unwrap(),
            queue_id: QueueId::new(queue).unwrap(),
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
            max_push_batch_size: 1000,
            max_claim_batch_size: 1000,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
        }
    }

    fn typed_queue_def(tenant: &str, queue: &str) -> QueueDefinition {
        let mut def = queue_def(tenant, queue);
        def.entity_schema = Some(
            serde_json::from_value::<EntitySchemaDocument>(json!({
                "entity_schema": {
                    "type": "object",
                    "required": ["name"],
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            }))
            .unwrap(),
        );
        def
    }

    fn rich_queue_def(tenant: &str, queue: &str) -> QueueDefinition {
        let mut definition = queue_def(tenant, queue);
        definition.priority_model = PriorityModel {
            kind: PriorityModelKind::Text,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::ClientItemKey,
        };
        definition.ordering_mode = OrderingMode::BoundedRelaxed;
        definition.max_rank_error = 7;
        definition.progress_bound_ms = 12_345;
        definition.eligibility_policy = EligibilityPolicy {
            metadata_blockers: BTreeMap::from([(
                "blocked".to_string(),
                vec![MetadataValue::String("yes".to_string())],
            )]),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(3),
            max_gates_per_request: Some(5),
        };
        definition.cohort_policy = Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(9_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(8),
        });
        definition.recurrence = RecurrencePolicy {
            mode: RecurrenceMode::Recurring,
            until: Some(UtcTimestamp::new(4_242, 123_000_000).unwrap()),
        };
        definition.request_id_retention_ms = 11_000;
        definition.client_item_key_retention_ms = 12_000;
        definition.terminal_retention_ms = 13_000;
        definition.max_lease_duration_ms = 14_000;
        definition.retry_policy = RetryPolicy { max_attempts: 9 };
        definition.max_push_batch_size = 17;
        definition.max_claim_batch_size = 19;
        definition.max_eligible_group_size = Some(23);
        definition.secondary_indexes = vec![IndexSpec {
            name: "by_customer".to_string(),
            fields: vec!["customer".to_string(), "region".to_string()],
            unique: true,
        }];
        definition.entity_schema = Some(
            serde_json::from_value::<EntitySchemaDocument>(json!({
                "entity_schema": {
                    "type": "object",
                    "required": ["status"],
                    "properties": {
                        "status": {"type": "string"},
                        "attempt": {"type": "integer"}
                    }
                }
            }))
            .unwrap(),
        );
        definition.typed_indexes = vec![QueueIndex {
            name: "by_status".to_string(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "status".to_string(),
                index_type: IndexType::String,
                unique: false,
            }),
        }];
        definition.emit_change_records = false;
        definition
    }

    fn assert_rich_definition(actual: &QueueDefinition, expected: &QueueDefinition) {
        assert_eq!(actual.tenant_id, expected.tenant_id);
        assert_eq!(actual.queue_id, expected.queue_id);
        assert_eq!(actual.priority_model, expected.priority_model);
        assert_eq!(actual.ordering_mode, expected.ordering_mode);
        assert_eq!(actual.max_rank_error, expected.max_rank_error);
        assert_eq!(actual.progress_bound_ms, expected.progress_bound_ms);
        assert_eq!(actual.eligibility_policy, expected.eligibility_policy);
        assert_eq!(actual.cohort_policy, expected.cohort_policy);
        assert_eq!(actual.recurrence, expected.recurrence);
        assert_eq!(
            actual.request_id_retention_ms,
            expected.request_id_retention_ms
        );
        assert_eq!(
            actual.client_item_key_retention_ms,
            expected.client_item_key_retention_ms
        );
        assert_eq!(actual.terminal_retention_ms, expected.terminal_retention_ms);
        assert_eq!(actual.max_lease_duration_ms, expected.max_lease_duration_ms);
        assert_eq!(actual.retry_policy, expected.retry_policy);
        assert_eq!(actual.max_push_batch_size, expected.max_push_batch_size);
        assert_eq!(actual.max_claim_batch_size, expected.max_claim_batch_size);
        assert_eq!(
            actual.max_eligible_group_size,
            expected.max_eligible_group_size
        );
        assert_eq!(actual.secondary_indexes, expected.secondary_indexes);
        assert_eq!(actual.entity_schema, expected.entity_schema);
        assert_eq!(actual.typed_indexes, expected.typed_indexes);
        assert_eq!(actual.emit_change_records, expected.emit_change_records);
    }

    fn claim_request(shard: &QueueKey) -> ClaimRequest {
        ClaimRequest {
            shard: shard.clone(),
            worker_id: WorkerId::new("race-worker").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new("race-lease").unwrap(),
            lease_expires_at: UtcTimestamp::new(ts().seconds + 60, 0).unwrap(),
            now: ts(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: None,
        }
    }

    #[tokio::test]
    async fn object_log_sqlite_create_returns_authoritative_rich_definition() {
        let tmp = TmpDir::new("object-log-sqlite-rich-definition");
        let definition = rich_queue_def("rich-tenant", "rich-queue");
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let backend = ObjectLogSqliteBackend::open(tmp.object_root(), &tmp.projection()).unwrap();

        let outcome = backend.create_queue(definition.clone()).await.unwrap();

        assert!(outcome.created);
        assert_rich_definition(&outcome.definition, &definition);
        assert_rich_definition(&backend.queue_definition(&key).await.unwrap(), &definition);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segmented_object_log_sqlite_compatible_create_race_uses_durable_winner() {
        let tmp = TmpDir::new("segmented-sqlite-compatible-race");
        let definition = rich_queue_def("race-tenant", "compatible");
        let backends = [
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &tmp.projection_named("first"),
                    seal_each_config(),
                )
                .unwrap(),
            ),
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &tmp.projection_named("second"),
                    seal_each_config(),
                )
                .unwrap(),
            ),
        ];
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles = backends
            .iter()
            .cloned()
            .map(|backend| {
                let barrier = Arc::clone(&barrier);
                let definition = definition.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    futures::executor::block_on(backend.create_queue(definition))
                })
            })
            .collect::<Vec<_>>();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<EngineResult<Vec<_>>>()
            .unwrap();

        assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.definition == definition)
        );
        assert_eq!(
            backends[0]
                .log
                .read_definition(&QueueKey::new(
                    definition.tenant_id.clone(),
                    definition.queue_id.clone()
                ))
                .unwrap(),
            definition
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segmented_object_log_sqlite_incompatible_create_conflicts_without_overwrite() {
        let tmp = TmpDir::new("segmented-sqlite-incompatible-race");
        let first = rich_queue_def("race-tenant", "incompatible");
        let mut second = first.clone();
        second.priority_model.direction = PriorityDirection::Ascending;
        let definitions = [first, second];
        let backends = [
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &tmp.projection_named("first"),
                    seal_each_config(),
                )
                .unwrap(),
            ),
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &tmp.projection_named("second"),
                    seal_each_config(),
                )
                .unwrap(),
            ),
        ];
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles = backends
            .iter()
            .cloned()
            .zip(definitions.iter().cloned())
            .map(|(backend, definition)| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    futures::executor::block_on(backend.create_queue(definition))
                })
            })
            .collect::<Vec<_>>();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(EngineError::QueueDefinitionConflict)))
                .count(),
            1
        );
        let winner = &outcomes
            .iter()
            .find_map(|outcome| outcome.as_ref().ok())
            .unwrap()
            .definition;
        let key = QueueKey::new(winner.tenant_id.clone(), winner.queue_id.clone());
        assert_eq!(backends[0].log.read_definition(&key).unwrap(), *winner);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segmented_object_log_sqlite_loser_can_push_claim_and_reopen() {
        let tmp = TmpDir::new("segmented-sqlite-loser-use");
        let definition = queue_def("race-tenant", "loser-use");
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let projection_paths = [
            tmp.projection_named("first"),
            tmp.projection_named("second"),
        ];
        let backends = [
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &projection_paths[0],
                    seal_each_config(),
                )
                .unwrap(),
            ),
            Arc::new(
                SegmentedObjectLogSqliteBackend::open(
                    tmp.object_root(),
                    &projection_paths[1],
                    seal_each_config(),
                )
                .unwrap(),
            ),
        ];
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles = backends
            .iter()
            .cloned()
            .map(|backend| {
                let barrier = Arc::clone(&barrier);
                let definition = definition.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    futures::executor::block_on(backend.create_queue(definition))
                })
            })
            .collect::<Vec<_>>();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<EngineResult<Vec<_>>>()
            .unwrap();
        let loser_index = outcomes
            .iter()
            .position(|outcome| !outcome.created)
            .expect("one durable create loser");

        backends[loser_index]
            .push(&key, vec![spec("loser-push")], ts(), None)
            .await
            .unwrap();
        let claimed = backends[loser_index]
            .claim(claim_request(&key))
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);
        drop(backends);

        let reopened = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &projection_paths[loser_index],
            seal_each_config(),
        )
        .unwrap();
        let reopened_outcome = reopened.create_queue(definition.clone()).await.unwrap();
        assert!(!reopened_outcome.created);
        assert_eq!(reopened.queue_definition(&key).await.unwrap(), definition);
    }

    #[tokio::test]
    async fn segmented_object_log_inmemory_create_uses_durable_outcome() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::new());
        let definition = rich_queue_def("inmemory-tenant", "durable-outcome");
        let first = SegmentedObjectLogInMemoryBackend::open_with_blob_store(
            Arc::clone(&store),
            seal_each_config(),
        )
        .unwrap();
        let second =
            SegmentedObjectLogInMemoryBackend::open_with_blob_store(store, seal_each_config())
                .unwrap();

        let created = first.create_queue(definition.clone()).await.unwrap();
        let existing = second.create_queue(definition.clone()).await.unwrap();

        assert!(created.created);
        assert!(!existing.created);
        assert_rich_definition(&created.definition, &definition);
        assert_rich_definition(&existing.definition, &definition);
    }

    fn spec(payload: &str) -> PushSpec {
        PushSpec {
            client_item_key: None,
            priority: None,
            not_before: None,
            group_key: None,
            payload: Some(Bytes::from(payload.to_string())),
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
            entity: None,
        }
    }

    fn typed_valid_spec(payload: &str) -> PushSpec {
        PushSpec {
            entity: Some(json!({"name": payload})),
            ..spec(payload)
        }
    }

    fn typed_invalid_spec(payload: &str) -> PushSpec {
        PushSpec {
            entity: Some(json!({"count": payload.len()})),
            ..spec(payload)
        }
    }

    fn ts() -> UtcTimestamp {
        UtcTimestamp::new(1_700_000_000, 0).unwrap()
    }

    fn system_ts() -> UtcTimestamp {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        UtcTimestamp::new(now.as_secs() as i64, now.subsec_nanos()).unwrap()
    }

    /// Force every push to seal its own segment synchronously (no flusher needed): a 1-byte target trips the
    /// size seal inside `enqueue`, so the projection is applied before `push` returns.
    fn seal_each_config() -> SegmentConfig {
        SegmentConfig::new(1, 1_000).unwrap()
    }

    fn object_prefix(shard: &QueueKey) -> String {
        fn hex(value: &str) -> String {
            value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        }
        format!(
            "t/{}/q/{}/",
            hex(shard.tenant_id.as_str()),
            hex(shard.queue_id.as_str())
        )
    }

    struct BlockingMaintenanceStore {
        inner: Arc<InMemoryBlobStore>,
        blocked_pin_prefix: String,
        progress_pin_prefix: String,
        block_pin_list: std::sync::atomic::AtomicBool,
        pin_list_entered: std::sync::atomic::AtomicBool,
        progress_pin_list_completed: std::sync::atomic::AtomicBool,
        release_pin_list: std::sync::atomic::AtomicBool,
        active_pin_lists: AtomicUsize,
        peak_active_pin_lists: AtomicUsize,
        observed_pin_lists: Mutex<HashSet<String>>,
    }

    impl BlockingMaintenanceStore {
        fn new(blocked: &QueueKey, progress: &QueueKey) -> Self {
            Self {
                inner: Arc::new(InMemoryBlobStore::new()),
                blocked_pin_prefix: format!("{}recovery_pins/v1/", object_prefix(blocked)),
                progress_pin_prefix: format!("{}recovery_pins/v1/", object_prefix(progress)),
                block_pin_list: std::sync::atomic::AtomicBool::new(false),
                pin_list_entered: std::sync::atomic::AtomicBool::new(false),
                progress_pin_list_completed: std::sync::atomic::AtomicBool::new(false),
                release_pin_list: std::sync::atomic::AtomicBool::new(false),
                active_pin_lists: AtomicUsize::new(0),
                peak_active_pin_lists: AtomicUsize::new(0),
                observed_pin_lists: Mutex::new(HashSet::new()),
            }
        }

        fn observed_maintenance(&self, shard: &QueueKey) -> bool {
            self.observed_pin_lists
                .lock()
                .expect("observed pin lists poisoned")
                .contains(&format!("{}recovery_pins/v1/", object_prefix(shard)))
        }
    }

    impl BlobStore for BlockingMaintenanceStore {
        fn backend_kind(&self) -> fireweed_objectlog::object_store_observability::BlobBackendKind {
            fireweed_objectlog::object_store_observability::BlobBackendKind::Memory
        }

        fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
            self.inner.put(key, body)
        }

        fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
            self.inner.put_if_absent(key, body)
        }

        fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
            self.inner.get(key)
        }

        fn delete(&self, key: &str) -> EngineResult<bool> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
            self.inner.list(prefix)
        }

        fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> EngineResult<Vec<String>> {
            let is_pin_list = prefix.ends_with("/recovery_pins/v1/");
            if is_pin_list {
                let active = self.active_pin_lists.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_active_pin_lists
                    .fetch_max(active, Ordering::SeqCst);
            }
            if prefix == self.blocked_pin_prefix
                && self.block_pin_list.swap(false, Ordering::SeqCst)
            {
                self.pin_list_entered.store(true, Ordering::SeqCst);
                while !self.release_pin_list.load(Ordering::SeqCst) {
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            } else if prefix == self.progress_pin_prefix {
                while !self.pin_list_entered.load(Ordering::SeqCst) {
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
            let result = self.inner.list_page(prefix, start_after, limit);
            if is_pin_list && result.is_ok() {
                self.observed_pin_lists
                    .lock()
                    .expect("observed pin lists poisoned")
                    .insert(prefix.to_string());
            }
            if prefix == self.progress_pin_prefix {
                self.progress_pin_list_completed
                    .store(true, Ordering::SeqCst);
            }
            if is_pin_list {
                self.active_pin_lists.fetch_sub(1, Ordering::SeqCst);
            }
            result
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_recovery_maintenance_does_not_block_another_queue() {
        let first = queue_def("maintenance", "blocked");
        let second = queue_def("maintenance", "progress");
        let first_key = QueueKey::new(first.tenant_id.clone(), first.queue_id.clone());
        let second_key = QueueKey::new(second.tenant_id.clone(), second.queue_id.clone());
        let store = Arc::new(BlockingMaintenanceStore::new(&first_key, &second_key));
        let blob_store: Arc<dyn BlobStore> = store.clone();
        let backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open_with_blob_store(blob_store, seal_each_config())
                .unwrap(),
        );
        backend.create_queue(first).await.unwrap();
        backend.create_queue(second).await.unwrap();
        let mut maintenance_keys = vec![first_key.clone(), second_key.clone()];
        for index in 0..(RECOVERY_MAINTENANCE_TASK_LIMIT * 2 - 2) {
            let definition = queue_def("maintenance-extra", &format!("q-{index}"));
            maintenance_keys.push(QueueKey::new(
                definition.tenant_id.clone(),
                definition.queue_id.clone(),
            ));
            backend.create_queue(definition).await.unwrap();
        }

        store.block_pin_list.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_millis(250), backend.tick(ts()))
            .await
            .expect("tick dispatch must not await a shard's blocking provider call")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !store.pin_list_entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("maintenance reached the blocking store seam");

        tokio::time::timeout(Duration::from_millis(250), async {
            while !store.progress_pin_list_completed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue B maintenance must complete while queue A LIST remains blocked");
        assert!(!store.release_pin_list.load(Ordering::SeqCst));
        assert_eq!(
            backend.maintenance_dispatcher.in_flight_len(),
            RECOVERY_MAINTENANCE_TASK_LIMIT,
            "one fixed task page is tracked even when one shard hangs"
        );
        assert!(
            store.peak_active_pin_lists.load(Ordering::SeqCst)
                <= RECOVERY_MAINTENANCE_BLOCKING_CONCURRENCY,
            "blocking store concurrency stays globally bounded"
        );

        // Repeated ticks reap completed wrappers and fairly admit every queue beyond the first page while A
        // remains hung. The fixed cap must hold throughout; no skipped page tail may starve forever.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !maintenance_keys[1..]
                .iter()
                .all(|shard| store.observed_maintenance(shard))
            {
                backend.tick(ts()).await.unwrap();
                assert!(
                    backend.maintenance_dispatcher.in_flight_len()
                        <= RECOVERY_MAINTENANCE_TASK_LIMIT,
                    "maintenance wrapper tasks remain bounded across ticks"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every queue after hung A eventually receives maintenance");

        store.release_pin_list.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn production_tick_fences_crash_pins_and_converges_recovery_index_gc() {
        let store = Arc::new(InMemoryBlobStore::new());
        let blob_store: Arc<dyn BlobStore> = store.clone();
        let backend = SegmentedObjectLogInMemoryBackend::open_with_blob_store(
            blob_store,
            seal_each_config().with_writer_format(fireweed_objectlog::SegmentWriterFormat::V3),
        )
        .unwrap();
        let definition = queue_def("maintenance", "recovery-index");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        ControlPlaneStore::fence_epoch(&backend, &shard, 0)
            .await
            .unwrap();

        for index in 0..24 {
            backend
                .push(&shard, vec![spec(&format!("item-{index}"))], ts(), None)
                .await
                .unwrap();
        }
        let prefix = object_prefix(&shard);
        let pin_prefix = format!("{prefix}recovery_pins/v1/");
        let gc_prefix = format!("{prefix}recovery_index_gc/v1/");
        let node_prefix = format!("{prefix}recovery_index/v1/");
        let batches_before = store.list(&gc_prefix).unwrap().len();
        let nodes_before = store.list(&node_prefix).unwrap().len();
        assert!(batches_before > 0, "seals published retirement work");
        assert!(nodes_before > 1);

        // Simulate a process crash: one cursor's durable pin is intentionally not dropped. A second reader
        // remains valid across reassignment because renewable reader leases, not assignment epochs, govern
        // recovery-node liveness.
        let crashed_cursor = backend.log.open_recovery_cursor(&shard, 0).unwrap();
        std::mem::forget(crashed_cursor);
        let mut retained_cursor = backend.log.open_recovery_cursor(&shard, 0).unwrap();
        assert_eq!(store.list(&pin_prefix).unwrap().len(), 2);
        ControlPlaneStore::acquire_epoch(&backend, &shard)
            .await
            .unwrap();
        let mut recovered = Vec::new();
        loop {
            let (page, _) = backend
                .log
                .read_recovery_cursor_page(&mut retained_cursor)
                .unwrap();
            if page.is_empty() {
                break;
            }
            recovered.extend(page.into_iter().map(|(position, _)| position.sequence));
        }
        assert_eq!(recovered, (0..24).collect::<Vec<_>>());
        drop(retained_cursor);
        assert_eq!(store.list(&pin_prefix).unwrap().len(), 1);

        // With no live cursor remaining, a far-future maintenance time expires the forgotten crash lease.
        // Repeated non-blocking ticks drive the separately dispatched fixed-size pin and GC pages.
        let far_future = UtcTimestamp::new(4_000_000_000, 0).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                backend.tick(far_future).await.unwrap();
                tokio::task::yield_now().await;
                if store.list(&pin_prefix).unwrap().is_empty()
                    && store.list(&gc_prefix).unwrap().is_empty()
                    && backend.maintenance_dispatcher.all_tasks_finished()
                {
                    break;
                }
            }
        })
        .await
        .expect("expired crash pin and bounded recovery-index GC converge");
        assert_eq!(
            store.list(&node_prefix).unwrap().len(),
            1,
            "only the current content-addressed recovery-index root remains"
        );

        // A currently live reader survives an actual-time maintenance pass. Wait for the dispatched task so
        // both the pin assertion and the later seal LIST delta observe a quiescent maintenance executor.
        let live_cursor = backend.log.open_recovery_cursor(&shard, 0).unwrap();
        assert_eq!(store.list(&pin_prefix).unwrap().len(), 1);
        backend.tick(system_ts()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !backend.maintenance_dispatcher.all_tasks_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual-time maintenance task completes");
        assert_eq!(
            store.list(&pin_prefix).unwrap().len(),
            1,
            "unexpired live recovery lease is preserved"
        );
        drop(live_cursor);

        let lists_before_seal = backend.segment_counters().list_count;
        backend
            .push(&shard, vec![spec("post-maintenance")], ts(), None)
            .await
            .unwrap();
        assert_eq!(
            backend.segment_counters().list_count,
            lists_before_seal,
            "segment seal must not LIST recovery maintenance namespaces"
        );
    }

    fn test_budget(bytes: usize) -> BufferedByteBudget {
        BufferedByteBudget::new(BufferedByteBudgetConfig::new(bytes).unwrap())
    }

    #[tokio::test]
    async fn ordered_independent_push_groups_1000_commands_without_reordering_or_failure_coupling()
    {
        let tmp = TmpDir::new("ordered-independent-push");
        let mut def = typed_queue_def("tenant", "ordered");
        def.max_push_batch_size = 100;
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let backend = Arc::new(
            SegmentedObjectLogSqliteBackend::open(
                tmp.object_root(),
                &tmp.projection(),
                SegmentConfig::new(64 * 1024 * 1024, 20).unwrap(),
            )
            .unwrap(),
        );
        backend.create_queue(def).await.unwrap();
        let flusher = backend.spawn_flusher();
        let items = (0..1_000)
            .map(|index| {
                if index == 500 {
                    typed_invalid_spec("rejected")
                } else {
                    typed_valid_spec(&format!("item-{index}"))
                }
            })
            .collect();
        let outcomes = backend
            .push_ordered_independent(&shard, items, ts(), None)
            .await;
        assert_eq!(outcomes.len(), 1_000);
        assert!(matches!(
            outcomes[500],
            Err(EngineError::EntitySchemaViolation(_))
        ));
        let ids: Vec<_> = outcomes.into_iter().filter_map(Result::ok).collect();
        assert_eq!(ids.len(), 999);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 999);
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: fireweed_core::WorkerId::new("ordered-worker").unwrap(),
                max_items: 999,
                lease_token: LeaseToken::new("ordered-lease").unwrap(),
                lease_expires_at: UtcTimestamp::new(ts().seconds + 60, 0).unwrap(),
                now: ts(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(
            claimed
                .items
                .iter()
                .map(|item| item.item_id)
                .collect::<Vec<_>>(),
            ids,
            "equal-priority claim order must match caller/wire result order"
        );
        assert!(
            backend.segment_counters().max_batch_size() > 100,
            "distinct scalar commands must share downstream segments"
        );
        flusher.abort();
    }

    #[tokio::test]
    async fn inmemory_projection_ordered_independent_push_also_groups_without_reordering() {
        let tmp = TmpDir::new("ordered-independent-inmemory");
        let mut def = queue_def("tenant", "ordered-inmemory");
        def.max_push_batch_size = 100;
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open(
                tmp.object_root(),
                SegmentConfig::new(64 * 1024 * 1024, 20).unwrap(),
            )
            .unwrap(),
        );
        backend.create_queue(def).await.unwrap();
        let flusher = backend.spawn_flusher();
        let outcomes = backend
            .push_ordered_independent(
                &shard,
                (0..1_000)
                    .map(|index| spec(&format!("item-{index}")))
                    .collect(),
                ts(),
                None,
            )
            .await;
        let ids: Vec<_> = outcomes
            .into_iter()
            .collect::<EngineResult<Vec<_>>>()
            .unwrap();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1_000);
        assert!(backend.segment_counters().max_batch_size() > 100);
        flusher.abort();
    }

    #[tokio::test]
    async fn production_inmemory_constructor_enforces_cap_without_coordinator_residue() {
        let tmp = TmpDir::new("byte-cap-constructor");
        let def = queue_def("tenant", "queue");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let backend = SegmentedObjectLogInMemoryBackend::open(
            tmp.object_root(),
            SegmentConfig::new(1, 1_000).unwrap(),
        )
        .unwrap()
        .with_byte_admission(test_budget(128), 128);
        backend.create_queue(def).await.unwrap();

        let error = backend
            .push(&shard, vec![spec(&"x".repeat(512))], ts(), None)
            .await
            .expect_err("oversize production push must be rejected");
        assert!(matches!(error, EngineError::RequestTooLarge { .. }));
        assert_eq!(backend.log.pending(&shard), 0);
        let coord = backend.coord_for(&shard);
        let state = coord.state.lock().await;
        assert!(state.pending.is_empty());
        assert!(state.permits.is_empty());
        assert!(state.waiters.is_empty());
        drop(state);
        let stats = backend.byte_admission_stats();
        assert_eq!(stats.charged_bytes, 0);
        assert_eq!(stats.rejection_count, 1);
        let telemetry = byte_admission_telemetry(backend.byte_admission_snapshot());
        assert!(telemetry.contains("admission_rejects=1"));
        assert!(telemetry.contains("admission_global_limit=128"));
    }

    #[tokio::test]
    async fn caller_drop_keeps_production_permit_until_seal_and_apply() {
        let tmp = TmpDir::new("byte-cap-caller-drop");
        let def = queue_def("tenant", "queue");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open(
                tmp.object_root(),
                SegmentConfig::new(4_096, 60_000).unwrap(),
            )
            .unwrap()
            .with_byte_admission(test_budget(8_192), 4_096),
        );
        backend.create_queue(def).await.unwrap();
        let task = {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            tokio::spawn(async move {
                backend
                    .push(&shard, vec![spec("resident")], ts(), None)
                    .await
            })
        };
        for _ in 0..100 {
            if backend.byte_admission_stats().charged_bytes > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(backend.byte_admission_stats().charged_bytes > 0);
        task.abort();
        let _ = task.await;
        assert!(
            backend.byte_admission_stats().charged_bytes > 0,
            "caller cancellation must not release coordinator-owned resident bytes"
        );

        let coord = backend.coord_for(&shard);
        let mut state = coord.state.lock().await;
        let positions = backend.log.seal(&shard, 0, system_now_ms()).unwrap();
        backend.distribute(&mut state, positions);
        drop(state);
        assert_eq!(backend.byte_admission_stats().charged_bytes, 0);
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
    }

    #[tokio::test]
    async fn same_queue_lock_waiters_do_not_capture_global_permits_before_queue_reservation() {
        let tmp = TmpDir::new("byte-cap-queue-lock");
        let def = queue_def("tenant", "queue");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let backend = Arc::new(
            SegmentedObjectLogInMemoryBackend::open(
                tmp.object_root(),
                SegmentConfig::new(4_096, 60_000).unwrap(),
            )
            .unwrap()
            .with_byte_admission(test_budget(8_192), 1_024),
        );
        backend.create_queue(def).await.unwrap();
        let coord = backend.coord_for(&shard);
        let gate = coord.state.lock().await;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            tasks.push(tokio::spawn(async move {
                backend.push(&shard, vec![spec("queued")], ts(), None).await
            }));
        }
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            backend.byte_admission_stats().charged_bytes,
            0,
            "same-queue lock waiters captured global permits before queue reservation"
        );
        drop(gate);
        for _ in 0..100 {
            if backend.byte_admission_stats().charged_bytes > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let stats = backend.byte_admission_stats();
        assert!(stats.charged_bytes > 0);
        assert!(stats.charged_bytes <= 1_024);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        let mut state = coord.state.lock().await;
        let positions = backend.log.seal(&shard, 0, system_now_ms()).unwrap();
        backend.distribute(&mut state, positions);
        drop(state);
        assert_eq!(backend.byte_admission_stats().charged_bytes, 0);
    }

    /// AC: a clean restart of the production segmented `object_log_sqlite_projection` recovers from the
    /// persisted projection snapshot + high-water, NOT a full-genesis replay. Proven via the recovery seam:
    /// replay resumed at the recorded high-water (not 0) and replayed zero tail entries (`<<` total objects).
    #[tokio::test]
    async fn segmented_clean_restart_recovers_from_snapshot_not_genesis() {
        let tmp = TmpDir::new("seg-clean");
        let def = queue_def("t", "q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        const N: usize = 50;

        let (total, pending_before) = {
            let b = SegmentedObjectLogSqliteBackend::open(
                tmp.object_root(),
                &tmp.projection(),
                seal_each_config(),
            )
            .unwrap();
            b.create_queue(def.clone()).await.unwrap();
            for i in 0..N {
                b.push(&shard, vec![spec(&format!("p{i}"))], ts(), None)
                    .await
                    .unwrap();
            }
            let total = b.log.read_all(&shard).unwrap().len();
            let pending = b.metrics(&shard).await.unwrap().pending;
            (total, pending)
        };
        assert_eq!(total, N, "every push committed one segment command");
        assert_eq!(pending_before, N as u64);

        // Reopen on the same paths: create_queue triggers snapshot-tail recovery.
        let b2 = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        b2.create_queue(def.clone()).await.unwrap();

        let stats = b2.recovery_stats(&shard).expect("recovery ran");
        assert!(stats.snapshot_used, "a durable snapshot existed");
        assert_eq!(
            stats.start_seq, N as u64,
            "replay resumed at the recorded high-water, not genesis (0)"
        );
        assert_eq!(
            stats.tail_replayed, 0,
            "a clean restart's projection was fully caught up; no tail to replay"
        );
        assert!(
            (stats.tail_replayed as usize) < total,
            "recovery did not replay the full genesis log"
        );
        // Committed state preserved across the restart.
        assert_eq!(b2.metrics(&shard).await.unwrap().pending, N as u64);
    }

    #[tokio::test]
    async fn segmented_inmemory_restart_reports_observed_full_replay() {
        let tmp = TmpDir::new("seg-inmemory-replay");
        let def = queue_def("t", "inmemory-q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        const N: usize = 300;

        {
            let backend =
                SegmentedObjectLogInMemoryBackend::open(tmp.object_root(), seal_each_config())
                    .unwrap();
            backend.create_queue(def.clone()).await.unwrap();
            for i in 0..N {
                backend
                    .push(&shard, vec![spec(&format!("p{i}"))], ts(), None)
                    .await
                    .unwrap();
            }
        }

        let reopened =
            SegmentedObjectLogInMemoryBackend::open(tmp.object_root(), seal_each_config()).unwrap();
        reopened.create_queue(def).await.unwrap();
        assert_eq!(reopened.metrics(&shard).await.unwrap().pending, N as u64);
        let stats = reopened.recovery_stats(&shard).unwrap();
        assert_eq!(
            (stats.start_seq, stats.tail_replayed, stats.snapshot_used),
            (0, N as u64, false)
        );
        assert!(stats.peak_replay_commands_buffered <= stats.replay_command_page_limit);
        assert_eq!(
            stats.peak_replay_commands_buffered,
            stats.replay_command_page_limit
        );
        assert!(stats.peak_manifest_objects_buffered <= stats.manifest_object_page_limit);
        assert!(
            stats
                .replay_progress_samples
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
        );
        assert!(stats.replay_progress_samples.len() >= 3);
        let mut ordered = Vec::new();
        let mut cursor = None;
        loop {
            let page = reopened.recovery_order_page(&shard, cursor, 64).unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 64);
            cursor = page.last().map(|item| item.item_id);
            ordered.extend(page.into_iter().map(|item| item.client_item_key));
        }
        assert_eq!(ordered.len(), N);
        assert_eq!(ordered.first().unwrap().as_str(), "0");
        assert_eq!(ordered.last().unwrap().as_str(), "299");
    }

    /// AC (crash-consistency): a reopen where the projection high-water LAGS the durable object-log head
    /// re-applies exactly the missing tail — no skip, no double-apply. We durably seal extra commands on the
    /// log only (bypassing the projection apply, simulating a crash between manifest commit and apply), then
    /// reopen and assert the tail is replayed exactly once; a second reopen replays nothing.
    #[tokio::test]
    async fn segmented_lagging_tail_replayed_exactly_once() {
        let tmp = TmpDir::new("seg-lag");
        let def = queue_def("t", "q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        const COMMITTED: usize = 30;
        const EXTRA: usize = 5;

        {
            let b = SegmentedObjectLogSqliteBackend::open(
                tmp.object_root(),
                &tmp.projection(),
                seal_each_config(),
            )
            .unwrap();
            b.create_queue(def.clone()).await.unwrap();
            for i in 0..COMMITTED {
                b.push(&shard, vec![spec(&format!("c{i}"))], ts(), None)
                    .await
                    .unwrap();
            }
            // Durably seal EXTRA commands on the LOG ONLY — the projection never sees them (crash window).
            let epoch = b.cached_epoch(&shard);
            for i in 0..EXTRA {
                let (items, ids) = build_push_items(
                    vec![spec(&format!("x{i}"))],
                    epoch,
                    0,
                    1_000 + i as u32,
                    def.retry_policy.max_attempts,
                );
                let env = b.next_envelope(QueueCommand::Push(PushCommand { items }), ids, ts());
                let outcome = b
                    .log
                    .enqueue(&shard, std::slice::from_ref(&env), epoch, system_now_ms())
                    .unwrap();
                assert!(
                    !outcome.committed.is_empty(),
                    "1-byte target seals each extra command durably"
                );
            }
            assert_eq!(b.log.read_all(&shard).unwrap().len(), COMMITTED + EXTRA);
            // The projection still only knows the first COMMITTED commands.
            assert_eq!(b.metrics(&shard).await.unwrap().pending, COMMITTED as u64);
        }

        // First reopen: the tail (EXTRA) must be replayed exactly.
        {
            let b2 = SegmentedObjectLogSqliteBackend::open(
                tmp.object_root(),
                &tmp.projection(),
                seal_each_config(),
            )
            .unwrap();
            b2.create_queue(def.clone()).await.unwrap();
            let stats = b2.recovery_stats(&shard).expect("recovery ran");
            assert_eq!(stats.start_seq, COMMITTED as u64);
            assert_eq!(
                stats.tail_replayed, EXTRA as u64,
                "exactly the missing tail beyond the lagging high-water is replayed"
            );
            assert_eq!(
                b2.metrics(&shard).await.unwrap().pending,
                (COMMITTED + EXTRA) as u64
            );
            let first_order_page = b2.recovery_order_page(&shard, None, 7).unwrap();
            let second_order_page = b2
                .recovery_order_page(&shard, first_order_page.last().map(|item| item.item_id), 7)
                .unwrap();
            assert_eq!(first_order_page.len(), 7);
            assert_eq!(second_order_page.len(), 7);
            assert_ne!(first_order_page[6].item_id, second_order_page[0].item_id);
        }

        // Second reopen: the projection is now caught up — no double-apply, nothing replayed.
        let b3 = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        b3.create_queue(def.clone()).await.unwrap();
        let stats = b3.recovery_stats(&shard).expect("recovery ran");
        assert_eq!(stats.start_seq, (COMMITTED + EXTRA) as u64);
        assert_eq!(stats.tail_replayed, 0, "no tail left; no double-apply");
        assert_eq!(
            b3.metrics(&shard).await.unwrap().pending,
            (COMMITTED + EXTRA) as u64,
            "state unchanged by the idempotent second recovery"
        );
    }

    #[tokio::test]
    async fn sqlite_snapshot_tail_recovers_interleaved_v3_then_v2_once() {
        let tmp = TmpDir::new("seg-mixed-tail");
        let def = queue_def("t", "q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        const APPLIED: usize = 3;
        let (v3_envelope, v2_envelope) = {
            let backend = SegmentedObjectLogSqliteBackend::open(
                tmp.object_root(),
                &tmp.projection(),
                seal_each_config(),
            )
            .unwrap();
            backend.create_queue(def.clone()).await.unwrap();
            for index in 0..APPLIED {
                backend
                    .push(&shard, vec![spec(&format!("applied-{index}"))], ts(), None)
                    .await
                    .unwrap();
            }
            let epoch = backend.cached_epoch(&shard);
            let envelope = |payload: &str, sequence: u32| {
                let (items, ids) = build_push_items(
                    vec![spec(payload)],
                    epoch,
                    0,
                    sequence,
                    def.retry_policy.max_attempts,
                );
                backend.next_envelope(QueueCommand::Push(PushCommand { items }), ids, ts())
            };
            (envelope("tail-v3", 10_001), envelope("tail-v2", 10_002))
        };

        let append_only = |format, envelope: &CommandEnvelope| {
            let store: Arc<dyn BlobStore> =
                Arc::new(LocalFsBlobStore::open(tmp.object_root()).unwrap());
            let log = SegmentedObjectLog::open(
                store,
                SegmentConfig::new(1, 1_000)
                    .unwrap()
                    .with_writer_format(format),
            );
            log.create_queue(&def).unwrap();
            log.enqueue(&shard, std::slice::from_ref(envelope), 0, system_now_ms())
                .unwrap();
            log.seal(&shard, 0, system_now_ms()).unwrap();
        };
        append_only(fireweed_objectlog::SegmentWriterFormat::V3, &v3_envelope);
        append_only(fireweed_objectlog::SegmentWriterFormat::V2, &v2_envelope);

        let recovered = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        recovered.create_queue(def.clone()).await.unwrap();
        let stats = recovered.recovery_stats(&shard).unwrap();
        assert_eq!(
            (stats.start_seq, stats.tail_replayed, stats.snapshot_used),
            (APPLIED as u64, 2, true)
        );
        assert!(stats.peak_replay_commands_buffered <= stats.replay_command_page_limit);
        assert!(stats.peak_manifest_objects_buffered <= stats.manifest_object_page_limit);
        assert_eq!(recovered.metrics(&shard).await.unwrap().pending, 5);
        drop(recovered);

        let clean = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        clean.create_queue(def).await.unwrap();
        assert_eq!(clean.recovery_stats(&shard).unwrap().tail_replayed, 0);
        assert_eq!(clean.metrics(&shard).await.unwrap().pending, 5);
    }

    /// AC: the file `ObjectLogSqliteBackend` reopen also resumes at the persisted high-water rather than
    /// re-applying the genesis log to the SQLite projection.
    #[tokio::test]
    async fn file_clean_restart_resumes_at_high_water() {
        let tmp = TmpDir::new("file-clean");
        let def = queue_def("t", "q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        const N: usize = 20;

        {
            let b = ObjectLogSqliteBackend::open(tmp.object_root(), &tmp.projection()).unwrap();
            b.create_queue(def.clone()).await.unwrap();
            for i in 0..N {
                b.push(&shard, vec![spec(&format!("f{i}"))], ts(), None)
                    .await
                    .unwrap();
            }
            assert_eq!(b.metrics(&shard).await.unwrap().pending, N as u64);
        }

        let b2 = ObjectLogSqliteBackend::open(tmp.object_root(), &tmp.projection()).unwrap();
        b2.create_queue(def.clone()).await.unwrap();
        let stats = b2.recovery_stats(&shard).expect("recovery ran");
        assert!(stats.snapshot_used);
        assert_eq!(
            stats.start_seq, N as u64,
            "file reopen resumed at the high-water, not genesis"
        );
        assert_eq!(stats.tail_replayed, 0);
        assert_eq!(b2.metrics(&shard).await.unwrap().pending, N as u64);
    }

    async fn schema_validation_backend<B>(backend: &B)
    where
        B: ControlPlaneStore + PushPort + ProjectionRead,
    {
        let def = typed_queue_def("t", "q");
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();

        let err = backend
            .push(&shard, vec![typed_invalid_spec("bad")], ts(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

        let rid = RequestId::new("req-1").unwrap();
        let err = backend
            .push_with_request_id(
                &shard,
                rid.clone(),
                vec![typed_invalid_spec("bad")],
                ts(),
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::EntitySchemaViolation(_)));
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 0);

        let first = backend
            .push_with_request_id(
                &shard,
                rid.clone(),
                vec![typed_valid_spec("ok")],
                ts(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);

        let replay = backend
            .push_with_request_id(&shard, rid, vec![typed_valid_spec("ok")], ts(), None)
            .await
            .unwrap();
        assert_eq!(first, replay, "valid replay must reuse the committed ids");
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
    }

    #[tokio::test]
    async fn object_log_sqlite_schema_validation_rejects_before_append_and_idempotency() {
        let tmp = TmpDir::new("schema-file");
        let backend = ObjectLogSqliteBackend::open(tmp.object_root(), &tmp.projection()).unwrap();
        schema_validation_backend(&backend).await;
    }

    #[tokio::test]
    async fn segmented_object_log_sqlite_schema_validation_rejects_before_append_and_idempotency() {
        let tmp = TmpDir::new("schema-segmented");
        let backend = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        schema_validation_backend(&backend).await;
    }

    #[tokio::test]
    async fn segmented_object_log_inmemory_schema_validation_rejects_before_append_and_idempotency()
    {
        let tmp = TmpDir::new("schema-seginmem");
        let backend =
            SegmentedObjectLogInMemoryBackend::open(tmp.object_root(), seal_each_config()).unwrap();
        schema_validation_backend(&backend).await;
    }
}
