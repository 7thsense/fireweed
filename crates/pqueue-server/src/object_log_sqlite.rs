// The port impls below return `-> impl Future` (the engine's port signature) with `async move` bodies —
// the deliberate codebase pattern, not convertible to bare `async fn` without changing the trait shape.
#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition, QueueId,
    TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, CommandChecksum,
    CommandEnvelope, CommandId, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeCommand, FinalizeOutcome, FinalizePort,
    ItemView, LeaseView, LiveItemView, LogRead, LogWriter, ProjectionRead, ProjectionWriter,
    PurgePort, PushCommand, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey,
    QueueMetrics, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand,
    RenewLeasePort, TickReport, UpsertOutcome, UpsertPort, build_push_items,
    require_item_level_claim, validate_gate_command, validate_gate_push,
};
use pqueue_objectlog::LocalObjectLog;
use pqueue_objectlog::segmented::{LocalFsBlobStore, SegmentConfig, SegmentedObjectLog};
use pqueue_projection::ProjectionData;
use pqueue_sqlite::SqliteProjectionStore;
use tokio::sync::oneshot;

pub struct ObjectLogSqliteBackend {
    log: LocalObjectLog,
    projection: SqliteProjectionStore,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    op_lock: tokio::sync::Mutex<()>,
}

impl ObjectLogSqliteBackend {
    pub fn open(object_root: impl Into<PathBuf>, projection_path: &str) -> EngineResult<Self> {
        Ok(Self {
            log: LocalObjectLog::open(object_root)?,
            projection: SqliteProjectionStore::open(projection_path)?,
            queues: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            op_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
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
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: now,
        }
    }

    async fn replay_queue(&self, shard: &QueueKey) -> EngineResult<()> {
        let mut from = None;
        loop {
            let page = self.log.read_from(shard, from.clone(), 256).await?;
            for (position, envelope) in &page.entries {
                for id in &envelope.item_ids {
                    self.counters.observe(shard, *id);
                }
                self.projection.apply_committed(position, envelope)?;
            }
            match page.next {
                Some(next) => from = Some(next),
                None => return Ok(()),
            }
        }
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

    fn write<R, F>(&self, _f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
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
                .select_eligible(&req.shard, req.now, req.max_items)
                .await?;
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let envelope = self.next_envelope(
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
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

/// The concrete segmented log this backend drives (group-commit over a durable local-fs object store).
type FsSegmentedLog = SegmentedObjectLog<LocalFsBlobStore>;

/// Per-queue group-commit coordination state (guarded by an async mutex so the buffer/waiter registry is
/// mutated atomically with the substrate's `enqueue`/`seal`).
struct ShardCoord {
    state: tokio::sync::Mutex<CoordState>,
}

struct CoordState {
    /// Envelopes buffered-but-not-yet-acked, mirroring the substrate's internal buffer 1:1 (arrival order).
    pending: Vec<CommandEnvelope>,
    /// One responder per buffered envelope; fired (Ok/Err) when the envelope's segment seals + applies.
    waiters: Vec<oneshot::Sender<EngineResult<()>>>,
}

/// Group-committing object-log authority (`SegmentedObjectLog<LocalFsBlobStore>`) + SQLite materialized
/// projection. Eventual-apply durability class.
pub struct SegmentedObjectLogSqliteBackend {
    log: Arc<FsSegmentedLog>,
    projection: Arc<SqliteProjectionStore>,
    queues: Mutex<HashMap<QueueKey, QueueDefinition>>,
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
    /// Open (or recover) a segmented object log rooted at `object_root` with `config`, plus the SQLite
    /// projection at `projection_path`. Recovery (replay of committed segments into the projection) happens
    /// per-queue in `create_queue` as the bootstrap queues are provisioned.
    pub fn open(
        object_root: impl Into<PathBuf>,
        projection_path: &str,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let store = LocalFsBlobStore::open(object_root)?;
        let log = Arc::new(SegmentedObjectLog::open(store, config));
        let projection = Arc::new(SqliteProjectionStore::open(projection_path)?);
        // Poll near the latency cap so a buffered-but-quiet segment seals within ~max_latency_ms.
        let flush_ms = (config.max_latency_ms / 4).max(1);
        Ok(Self {
            log,
            projection,
            queues: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            coords: Mutex::new(HashMap::new()),
            mutate_locks: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            flush_interval: Duration::from_millis(flush_ms),
        })
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    /// Spawn the background flusher that seals each queue's latency-due segment (the latency seal trigger).
    /// Without it, a buffer below `target_bytes` would never seal and its pushes would never ack.
    pub fn spawn_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move { me.flush_loop().await })
    }

    fn coord_for(&self, shard: &QueueKey) -> Arc<ShardCoord> {
        let mut g = self.coords.lock().expect("segmented coords poisoned");
        g.entry(shard.clone())
            .or_insert_with(|| {
                Arc::new(ShardCoord {
                    state: tokio::sync::Mutex::new(CoordState {
                        pending: Vec::new(),
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
        let waiters: Vec<_> = state.waiters.drain(..n).collect();
        let result = self
            .projection
            .apply_committed_batch(&positions, &envelopes);
        for w in waiters {
            let _ = w.send(result.clone());
        }
    }

    /// A seal failed (epoch fence / storage): the substrate discarded the buffer, so fail every registered
    /// waiter and clear `pending` to stay consistent with the now-empty substrate buffer.
    fn fail_all(state: &mut CoordState, err: EngineError) {
        state.pending.clear();
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
            // Move the envelope into `pending` (it lives there until its segment applies), then enqueue it
            // into the substrate buffer by reference — no per-command envelope clone on the hot path (Fix A).
            state.pending.push(envelope);
            state.waiters.push(tx);
            let now_ms = ts_to_ms(now);
            let enqueued = self.log.enqueue(
                shard,
                std::slice::from_ref(state.pending.last().expect("just pushed")),
                epoch,
                now_ms,
            );
            match enqueued {
                Ok(outcome) => {
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
                Err(e) => Self::fail_all(&mut state, e),
            }
        }
        rx.await
            .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))?
    }

    async fn flush_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Opt-in group-commit telemetry (read once; the hot tick path stays allocation-free). When set, log
        // the segment counters ~1x/s so seal rate + mean batch size are observable during a load run.
        let debug_segments = std::env::var("PQUEUE_DEBUG_SEGMENTS").is_ok();
        let mut dbg_last = std::time::Instant::now();
        loop {
            ticker.tick().await;
            if debug_segments && dbg_last.elapsed() >= std::time::Duration::from_millis(1000) {
                dbg_last = std::time::Instant::now();
                let c = self.log.counters();
                eprintln!(
                    "[seg] sealed={} commands={} mean_batch={:.1} max_batch={} objects_put={}",
                    c.segments_sealed,
                    c.commands_committed,
                    c.mean_batch_size(),
                    c.max_batch_size(),
                    c.objects_put
                );
            }
            let shards: Vec<(QueueKey, Arc<ShardCoord>)> = {
                self.coords
                    .lock()
                    .expect("segmented coords poisoned")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let now_ms = system_now_ms();
            for (shard, coord) in shards {
                let epoch = self.cached_epoch(&shard);
                let mut state = coord.state.lock().await;
                if state.pending.is_empty() {
                    continue;
                }
                match self.log.flush_due(&shard, epoch, now_ms) {
                    Ok(positions) if !positions.is_empty() => {
                        self.distribute(&mut state, positions)
                    }
                    Ok(_) => {}
                    Err(e) => Self::fail_all(&mut state, e),
                }
            }
        }
    }

    /// Replay every committed segment for `shard` into the projection (recovery / open). Idempotent: the
    /// batch apply skips positions already absorbed by the projection cursor.
    fn replay_queue(&self, shard: &QueueKey) -> EngineResult<()> {
        let entries = self.log.read_all(shard)?;
        if entries.is_empty() {
            return Ok(());
        }
        for (_pos, env) in &entries {
            for id in &env.item_ids {
                self.counters.observe(shard, *id);
            }
        }
        let positions: Vec<CommandPosition> = entries.iter().map(|(p, _)| p.clone()).collect();
        let envelopes: Vec<CommandEnvelope> = entries.iter().map(|(_, e)| e.clone()).collect();
        self.projection
            .apply_committed_batch(&positions, &envelopes)
    }

    async fn require_leased(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        if ids.is_empty() {
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

    fn write<R, F>(&self, _f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
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
                .select_eligible(&req.shard, req.now, req.max_items)
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
    epochs: Mutex<HashMap<QueueKey, u64>>,
    coords: Mutex<HashMap<QueueKey, Arc<ShardCoord>>>,
    mutate_locks: Mutex<HashMap<QueueKey, Arc<tokio::sync::Mutex<()>>>>,
    counters: QueueCounters,
    command_seq: AtomicU64,
    node_id: u8,
    flush_interval: Duration,
}

impl SegmentedObjectLogInMemoryBackend {
    /// Open (or recover) a segmented object log rooted at `object_root` with `config`, paired with in-memory
    /// projections. Recovery replays committed segments into each queue's `ProjectionData` in `create_queue`.
    pub fn open(object_root: impl Into<PathBuf>, config: SegmentConfig) -> EngineResult<Self> {
        let store = LocalFsBlobStore::open(object_root)?;
        let log = Arc::new(SegmentedObjectLog::open(store, config));
        let flush_ms = (config.max_latency_ms / 4).max(1);
        Ok(Self {
            log,
            projections: Mutex::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            coords: Mutex::new(HashMap::new()),
            mutate_locks: Mutex::new(HashMap::new()),
            counters: QueueCounters::default(),
            command_seq: AtomicU64::new(0),
            node_id: 0,
            flush_interval: Duration::from_millis(flush_ms),
        })
    }

    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    /// Spawn the background flusher that seals each queue's latency-due segment (the latency seal trigger).
    pub fn spawn_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move { me.flush_loop().await })
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
        let waiters: Vec<_> = state.waiters.drain(..n).collect();
        let result = match positions.first() {
            Some(pos) => self.apply_batch(&pos.queue, &envelopes),
            None => Ok(()),
        };
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
            state.pending.push(envelope);
            state.waiters.push(tx);
            let now_ms = ts_to_ms(now);
            let enqueued = self.log.enqueue(
                shard,
                std::slice::from_ref(state.pending.last().expect("just pushed")),
                epoch,
                now_ms,
            );
            match enqueued {
                Ok(outcome) => {
                    if !outcome.committed.is_empty() {
                        self.distribute(&mut state, outcome.committed);
                    } else if force {
                        match self.log.seal(shard, epoch, now_ms) {
                            Ok(positions) => self.distribute(&mut state, positions),
                            Err(e) => Self::fail_all(&mut state, e),
                        }
                    }
                }
                Err(e) => Self::fail_all(&mut state, e),
            }
        }
        rx.await
            .map_err(|_| EngineError::Storage("segment commit responder dropped".into()))?
    }

    async fn flush_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let shards: Vec<(QueueKey, Arc<ShardCoord>)> = {
                self.coords
                    .lock()
                    .expect("segmented coords poisoned")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let now_ms = system_now_ms();
            for (shard, coord) in shards {
                let epoch = self.cached_epoch(&shard);
                let mut state = coord.state.lock().await;
                if state.pending.is_empty() {
                    continue;
                }
                match self.log.flush_due(&shard, epoch, now_ms) {
                    Ok(positions) if !positions.is_empty() => {
                        self.distribute(&mut state, positions)
                    }
                    Ok(_) => {}
                    Err(e) => Self::fail_all(&mut state, e),
                }
            }
        }
    }

    /// Replay every committed segment for `shard` into its in-memory projection (recovery / open).
    fn replay_queue(
        &self,
        shard: &QueueKey,
        proj: &Arc<Mutex<ProjectionData>>,
    ) -> EngineResult<()> {
        let entries = self.log.read_all(shard)?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut p = proj.lock().expect("segmented inmemory projection poisoned");
        for (_pos, env) in &entries {
            for id in &env.item_ids {
                self.counters.observe(shard, *id);
            }
            p.apply_command(&env.command)?;
        }
        Ok(())
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

    fn write<R, F>(&self, _f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
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
            let proj = Arc::new(Mutex::new(ProjectionData::new(
                definition.priority_model,
                definition.recurrence,
                &definition.secondary_indexes,
            )));
            self.projections
                .lock()
                .expect("segmented inmemory projections poisoned")
                .insert(key.clone(), proj.clone());
            self.queues
                .lock()
                .expect("segmented queues poisoned")
                .insert(key.clone(), definition.clone());
            let epoch = self.log.current_epoch(&key).unwrap_or(0);
            self.set_epoch(&key, epoch);
            let _ = self.coord_for(&key);
            self.replay_queue(&key, &proj)?;
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
                p.select_eligible(req.now, req.max_items)
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
}
