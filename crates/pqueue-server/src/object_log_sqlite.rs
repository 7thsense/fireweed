// The port impls below return `-> impl Future` (the engine's port signature) with `async move` bodies —
// the deliberate codebase pattern, not convertible to bare `async fn` without changing the trait shape.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use pqueue_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, BufferedByteBudget, BufferedByteBudgetConfig, BufferedByteBudgetStats,
    ByteAdmissionError, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed,
    CommandChecksum, CommandEnvelope, CommandId, CommandPosition, CompiledSchema,
    ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError, EngineResult,
    FinalizeCommand, FinalizeOutcome, FinalizePort, IdempotencyDecision, ItemView, LeaseView,
    LiveItemView, LogRead, OwnedBytePermit, ProjectionRead, PurgePort, PushCommand, PushPort,
    PushSpec, QueueCommand, QueueCounters, QueueIdempotencyCache, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    TerminalEmissionMetrics, TickReport, UpsertOutcome, UpsertPort, build_push_items,
    compile_entity_schema, require_item_level_claim, validate_entity, validate_gate_command,
    validate_gate_push,
};
use pqueue_objectlog::segmented::{
    BlobStore, FaultHook, LocalFsBlobStore, SegmentConfig, SegmentCounters, SegmentedObjectLog,
};
use pqueue_objectlog::{
    LocalObjectLog, ObjectLogByteAdmissionSnapshot, prepare_serialized_commands_for_format,
};
use pqueue_projection::ProjectionData;
use pqueue_sqlite::SqliteProjectionStore;
use tokio::sync::oneshot;

/// Per-queue recovery telemetry recorded by the snapshot-tail reopen path (bead pqueue-8a76daad). Exposed so
/// a test (and an operator-facing log line) can prove recovery resumed from the persisted high-water rather
/// than replaying the full genesis log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// The object-log sequence recovery began replaying at — the projection's persisted high-water
    /// (`relational_cursor.next_seq`). `0` means a full-genesis replay (no valid snapshot).
    pub start_seq: u64,
    /// Number of object-log tail entries replayed beyond the snapshot (`<<` total when a snapshot exists).
    pub tail_replayed: u64,
    /// Whether a durable snapshot/high-water short-circuited the genesis replay (`start_seq > 0`).
    pub snapshot_used: bool,
}

/// Default recovery-window budget: the max object-log tail (commands) a normal reopen is expected to replay
/// beyond the durable projection snapshot. The materialized projection advances its high-water inside the
/// same transaction that applies each sealed batch, so the tail is normally a handful of commands (only what
/// was durably sealed but not yet projection-applied at crash time). Exceeding this budget is logged as a
/// recovery-window warning so an operator can investigate a projection that has fallen far behind the log.
///
/// This is the in-code default; the composition root may override it from typed [`Config`](crate::Config)
/// (populated by the bin from `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS`) via [`Self::with_recovery_max_tail`]. The
/// backend itself never reads the process environment.
pub const DEFAULT_RECOVERY_MAX_TAIL: u64 = 1_000_000;

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
    /// `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` env knob, used by tests and embedders.
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
            .copied()
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
        _request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
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
            let outcome = self.log.create_queue(definition.clone())?;
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
impl pqueue_engine::CommitTransitionPort for ObjectLogSqliteBackend {}

/// Recovery/explain reads inherit the `Unavailable` default (no authoritative commit boundary on this path).
impl pqueue_engine::RecoveryReadPort for ObjectLogSqliteBackend {}

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

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::ClaimedItem>>> + Send
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
/// harness injects an [`pqueue_objectlog::segmented::S3BlobStore`] via
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
    coords: Mutex<HashMap<QueueKey, Arc<ShardCoord>>>,
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
    /// composition root from typed `Config` (populated by the bin from `PQUEUE_DEBUG_SEGMENTS`); the backend
    /// never reads the process environment.
    debug_segments: bool,
    /// Last per-queue snapshot-tail recovery telemetry (proof the reopen avoided a full-genesis replay).
    recovery_stats: Mutex<HashMap<QueueKey, RecoveryStats>>,
    /// Per-queue request-id replay/conflict cache (API-001 / TD-007 §4): a retried `request_id` with the
    /// same body replays the committed ids without a second append; a different body is `RequestIdConflict`.
    idempotency: Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>,
    byte_budget: BufferedByteBudget,
    queue_byte_limit: usize,
    writer_format: pqueue_objectlog::SegmentWriterFormat,
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
    /// [`pqueue_objectlog::segmented::S3BlobStore`] here to exercise the SAME group-commit ack-after-seal +
    /// snapshot-tail recovery pipeline against a real S3-compatible endpoint.
    pub fn open_with_blob_store(
        store: Arc<dyn BlobStore>,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let log = Arc::new(SegmentedObjectLog::open(store, config));
        let projection = Arc::new(SqliteProjectionStore::open(projection_path)?);
        // Poll near the latency cap so a buffered-but-quiet segment seals within ~max_latency_ms.
        let flush_ms = (config.max_latency_ms / 4).max(1);
        Ok(Self {
            log,
            projection,
            queues: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            coords: Mutex::new(HashMap::new()),
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
            writer_format: config.writer_format(),
        })
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
    /// `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` env knob, used by tests and embedders.
    pub fn with_recovery_max_tail(mut self, max_tail: u64) -> Self {
        self.recovery_max_tail = max_tail;
        self
    }

    /// Enable opt-in group-commit telemetry (the explicit form of the `PQUEUE_DEBUG_SEGMENTS` env knob):
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
            .copied()
    }

    /// Spawn the background flusher that seals each queue's latency-due segment (the latency seal trigger).
    /// Without it, a buffer below `target_bytes` would never seal and its pushes would never ack.
    pub fn spawn_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let interval = self.flush_interval;
        let debug_segments = self.debug_segments;
        tokio::spawn(async move { Self::flush_loop(weak, interval, debug_segments).await })
    }

    fn coord_for(&self, shard: &QueueKey) -> Arc<ShardCoord> {
        let mut g = self.coords.lock().expect("segmented coords poisoned");
        g.entry(shard.clone())
            .or_insert_with(|| {
                Arc::new(ShardCoord {
                    state: tokio::sync::Mutex::new(CoordState {
                        pending: Vec::new(),
                        permits: Vec::new(),
                        waiters: Vec::new(),
                    }),
                })
            })
            .clone()
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
            let (serialized, charged_bytes) = prepare_serialized_commands_for_format(
                vec![envelope],
                self.byte_budget.config().global_limit(),
                self.writer_format,
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
            let mut shards: Vec<(QueueKey, Arc<ShardCoord>)> = {
                this.coords
                    .lock()
                    .expect("segmented coords poisoned")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            if !shards.is_empty() {
                let start = this.flush_cursor.fetch_add(1, Ordering::Relaxed) % shards.len();
                shards.rotate_left(start);
            }
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
        let entries = self.log.read_from(shard, start_seq)?;
        let tail_replayed = entries.len() as u64;
        if !entries.is_empty() {
            for (_pos, env) in &entries {
                for id in &env.item_ids {
                    self.counters.observe(shard, *id);
                }
            }
            let positions: Vec<CommandPosition> = entries.iter().map(|(p, _)| p.clone()).collect();
            let envelopes: Vec<CommandEnvelope> = entries.iter().map(|(_, e)| e.clone()).collect();
            self.projection
                .apply_committed_batch(&positions, &envelopes)?;
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
        _request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
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
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.log.create_queue(&definition)?;
            let outcome = self
                .projection
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

impl pqueue_engine::CommitTransitionPort for SegmentedObjectLogSqliteBackend {}
impl pqueue_engine::RecoveryReadPort for SegmentedObjectLogSqliteBackend {}

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
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
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

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::ClaimedItem>>> + Send
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
// `pqueue_projection::ProjectionData` (one per queue, behind its own `Mutex`) instead of SQLite. The
// sealed segment + manifest entry is still the durable boundary (eventual-apply class preserved; recovery
// replays `read_all` into `ProjectionData` on `create_queue`); the per-segment projection write is now a
// cheap in-memory `apply_command` per command rather than a batched SQLite transaction. This is the fast
// path selected by `PQUEUE_OBJECT_LOG_MODE=segmented` + `PQUEUE_PROJECTION_BACKEND=inmemory`.

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
    coords: Mutex<HashMap<QueueKey, Arc<ShardCoord>>>,
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
    writer_format: pqueue_objectlog::SegmentWriterFormat,
}

impl SegmentedObjectLogInMemoryBackend {
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
            coords: Mutex::new(HashMap::new()),
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
            writer_format: config.writer_format(),
        })
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
            .copied()
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
        tokio::spawn(async move { Self::flush_loop(weak, interval, debug_segments).await })
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
        let mut g = self.coords.lock().expect("segmented coords poisoned");
        g.entry(shard.clone())
            .or_insert_with(|| {
                Arc::new(ShardCoord {
                    state: tokio::sync::Mutex::new(CoordState {
                        pending: Vec::new(),
                        permits: Vec::new(),
                        waiters: Vec::new(),
                    }),
                })
            })
            .clone()
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
            let (serialized, charged_bytes) = prepare_serialized_commands_for_format(
                vec![envelope],
                self.byte_budget.config().global_limit(),
                self.writer_format,
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
            let mut shards: Vec<(QueueKey, Arc<ShardCoord>)> = {
                this.coords
                    .lock()
                    .expect("segmented coords poisoned")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            if !shards.is_empty() {
                let start = this.flush_cursor.fetch_add(1, Ordering::Relaxed) % shards.len();
                shards.rotate_left(start);
            }
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
    ) -> EngineResult<u64> {
        let entries = self.log.read_all(shard)?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut p = proj.lock().expect("segmented inmemory projection poisoned");
        for (_pos, env) in &entries {
            for id in &env.item_ids {
                self.counters.observe(shard, *id);
            }
            p.apply_command(&env.command)?;
        }
        Ok(entries.len() as u64)
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
        _request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
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
            let replayed = self.replay_queue(&key, &proj)?;
            self.recovery_stats
                .lock()
                .expect("inmemory recovery stats poisoned")
                .insert(
                    key,
                    RecoveryStats {
                        start_seq: 0,
                        tail_replayed: replayed,
                        snapshot_used: false,
                    },
                );
            Ok(CreateQueueOutcome {
                created: true,
                definition,
            })
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
            let replayed = self.replay_queue(shard, &projection)?;
            self.projections
                .lock()
                .expect("segmented inmemory projections poisoned")
                .insert(shard.clone(), projection);
            self.recovery_stats
                .lock()
                .expect("inmemory recovery stats poisoned")
                .insert(
                    shard.clone(),
                    RecoveryStats {
                        start_seq: 0,
                        tail_replayed: replayed,
                        snapshot_used: false,
                    },
                );
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

impl pqueue_engine::CommitTransitionPort for SegmentedObjectLogInMemoryBackend {}
impl pqueue_engine::RecoveryReadPort for SegmentedObjectLogInMemoryBackend {}

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
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
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

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<pqueue_engine::ClaimedItem>>> + Send
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
    use pqueue_core::{
        EligibilityPolicy, EntitySchemaDocument, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, RecurrencePolicy, RequestId, RetryPolicy,
    };
    use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushPort};
    use serde_json::json;

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
                "pqueue-recovery-{label}-{}-{n}-{nanos}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
        fn object_root(&self) -> PathBuf {
            self.path.join("object-log")
        }
        fn projection(&self) -> String {
            self.path
                .join("projection.db")
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

    /// Force every push to seal its own segment synchronously (no flusher needed): a 1-byte target trips the
    /// size seal inside `enqueue`, so the projection is applied before `push` returns.
    fn seal_each_config() -> SegmentConfig {
        SegmentConfig::new(1, 1_000).unwrap()
    }

    fn test_budget(bytes: usize) -> BufferedByteBudget {
        BufferedByteBudget::new(BufferedByteBudgetConfig::new(bytes).unwrap())
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
        const N: usize = 25;

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
        assert_eq!(
            reopened.recovery_stats(&shard),
            Some(RecoveryStats {
                start_seq: 0,
                tail_replayed: N as u64,
                snapshot_used: false,
            })
        );
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
        append_only(pqueue_objectlog::SegmentWriterFormat::V3, &v3_envelope);
        append_only(pqueue_objectlog::SegmentWriterFormat::V2, &v2_envelope);

        let recovered = SegmentedObjectLogSqliteBackend::open(
            tmp.object_root(),
            &tmp.projection(),
            seal_each_config(),
        )
        .unwrap();
        recovered.create_queue(def.clone()).await.unwrap();
        assert_eq!(
            recovered.recovery_stats(&shard),
            Some(RecoveryStats {
                start_seq: APPLIED as u64,
                tail_replayed: 2,
                snapshot_used: true,
            })
        );
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
