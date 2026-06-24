#![forbid(unsafe_code)]
//! # pqueue-memory
//!
//! In-memory reference backend (atomic durability class). Implements the engine's storage/projection
//! ports; the claim/upsert/reclaim orchestration ports build on this in the next chunk. This is the
//! conformance reference for every other backend.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemEvent, ItemId, ItemState, LeaseToken, PriorityModel,
    PriorityValue, QueueDefinition, QueueId, TenantId, UtcTimestamp, apply_transition,
    priority_sort,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, Claimed, ClaimedItem, Clock, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand, FinalizeKind,
    FinalizeOutcome, FinalizePort, IdGen, ItemView, LeaseExpiredCommand, LeaseView, LogRead,
    LogWriter, ProjectionRead, ProjectionSnapshot, ProjectionWriter, PushCommand, PushItem,
    QueueCommand, QueueKey, QueueMetrics, ReclaimDriver, ReplacePendingCommand, ShardId, ShardKey,
    SnapshotRef, SnapshotStore, TickReport, UpsertOutcome, UpsertPort,
};

// ---------------------------------------------------------------------------
// Projection record + eligibility key
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ItemRecord {
    item_id: ItemId,
    client_item_key: ClientItemKey,
    priority: Option<PriorityValue>,
    not_before: Option<UtcTimestamp>,
    group_key: Option<GroupKey>,
    /// Rendered into `ClaimedItem.payload` by `ClaimPort` (built in chunk 1c); stored at push.
    #[allow(dead_code)]
    payload: Option<Bytes>,
    state: ItemState,
    item_version: u64,
    attempt_count: u32,
    /// Retry bound; read when retry-exhaustion is wired (Finalize-Retry beyond this → terminal).
    #[allow(dead_code)]
    max_attempts: u32,
    created_seq: u64,
    lease_token: Option<LeaseToken>,
    lease_expires_at: Option<UtcTimestamp>,
    fenced: bool,
    superseded: bool,
}

/// Priority-ordered eligibility key. Ascending order = claim order: priced items first (tag 0,
/// then `priority_sort` bytes), unpriced last (tag 1), FIFO by `created_seq` within ties.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EligKey {
    sort: Vec<u8>,
    created_seq: u64,
    item: ItemId,
}

fn elig_key(rec: &ItemRecord, model: &PriorityModel) -> EligKey {
    let sort = match &rec.priority {
        Some(p) => {
            let mut v = vec![0u8];
            v.extend(priority_sort(p, model));
            v
        }
        None => vec![1u8],
    };
    EligKey {
        sort,
        created_seq: rec.created_seq,
        item: rec.item_id.clone(),
    }
}

// ---------------------------------------------------------------------------
// State (logs and projections are DISJOINT fields so Backend::write can hand out
// &mut LogWriter + &mut ProjectionWriter simultaneously — review M2)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct LogData {
    epoch: u64,
    entries: Vec<CommandEnvelope>,
    /// Persisted command_position high-water — a stored field, NOT recomputed from `entries.len()`,
    /// so it survives log retention/compaction and `item_version` never regresses (TD-007 §4).
    high_water: Option<CommandPosition>,
    snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
}

struct ProjectionData {
    items: HashMap<ItemId, ItemRecord>,
    by_key: HashMap<ClientItemKey, ItemId>,
    eligible: BTreeSet<EligKey>,
    next_seq: u64,
    priority_model: PriorityModel,
    paused: bool,
}

impl ProjectionData {
    fn new(priority_model: PriorityModel) -> Self {
        Self {
            items: HashMap::new(),
            by_key: HashMap::new(),
            eligible: BTreeSet::new(),
            next_seq: 0,
            priority_model,
            paused: false,
        }
    }

    fn insert_pending(&mut self, item: pqueue_engine::PushItem) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let rec = ItemRecord {
            item_id: item.item_id.clone(),
            client_item_key: item.client_item_key.clone(),
            priority: item.priority,
            not_before: item.not_before,
            group_key: item.group_key,
            payload: item.payload,
            state: ItemState::Pending,
            item_version: 1,
            attempt_count: 0,
            max_attempts: item.max_attempts,
            created_seq: seq,
            lease_token: None,
            lease_expires_at: None,
            fenced: false,
            superseded: false,
        };
        self.eligible.insert(elig_key(&rec, &self.priority_model));
        self.by_key
            .insert(rec.client_item_key.clone(), rec.item_id.clone());
        self.items.insert(rec.item_id.clone(), rec);
    }

    /// Drive the lifecycle state machine for one item, keeping the eligibility index in sync and
    /// bumping `item_version` (API-001: version bumps on every committed mutation).
    fn transition(&mut self, id: &ItemId, ev: ItemEvent) -> EngineResult<ItemState> {
        let model = self.priority_model;
        let (old_key, new_key, new_state) = {
            let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
            // A superseded id (replaced by upsert) must never re-enter eligible or mutate
            // (TD-007 §2.3): the orchestration ports map this to `-ERR pqueue superseded`.
            if rec.superseded {
                return Err(EngineError::Superseded);
            }
            let old = (rec.state == ItemState::Pending).then(|| elig_key(rec, &model));
            let new = apply_transition(rec.state, ev)
                .map_err(|_| EngineError::Invalid("illegal lifecycle transition"))?;
            rec.state = new;
            rec.item_version += 1;
            let nk = (new == ItemState::Pending).then(|| elig_key(rec, &model));
            (old, nk, new)
        };
        if let Some(k) = old_key {
            self.eligible.remove(&k);
        }
        if let Some(k) = new_key {
            self.eligible.insert(k);
        }
        Ok(new_state)
    }

    fn apply_command(&mut self, cmd: &QueueCommand) -> EngineResult<()> {
        match cmd {
            // Queue creation is handled by the control plane; idempotent no-op if replayed here.
            QueueCommand::CreateQueue(_) => Ok(()),
            QueueCommand::Push(c) => {
                for it in &c.items {
                    self.insert_pending(it.clone());
                }
                Ok(())
            }
            QueueCommand::Claim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1; // delivery count (flavor-diff 7)
                }
                Ok(())
            }
            QueueCommand::RenewLease(c) => {
                for id in &c.item_ids {
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.item_version += 1;
                }
                Ok(())
            }
            QueueCommand::Finalize(c) => {
                for o in &c.outcomes {
                    let ev = match o.kind {
                        FinalizeKind::Complete => ItemEvent::FinalizeComplete,
                        FinalizeKind::Fail => ItemEvent::FinalizeFail,
                        FinalizeKind::Retry => ItemEvent::FinalizeRetry,
                        FinalizeKind::Release => ItemEvent::FinalizeRelease,
                        FinalizeKind::Rearm => ItemEvent::FinalizeRearm,
                    };
                    self.transition(&o.item_id, ev)?;
                    let rec = self
                        .items
                        .get_mut(&o.item_id)
                        .ok_or(EngineError::NotFound)?;
                    rec.lease_token = None;
                    rec.lease_expires_at = None;
                    rec.fenced = false;
                    if matches!(o.kind, FinalizeKind::Rearm) {
                        rec.attempt_count = 0;
                    }
                }
                Ok(())
            }
            QueueCommand::ReplacePending(c) => {
                // Supersede the old pending item; the old id thereafter reads as deleted/superseded.
                let model = self.priority_model;
                if let Some(rec) = self.items.get_mut(&c.superseded_item_id) {
                    let old = (rec.state == ItemState::Pending).then(|| elig_key(rec, &model));
                    rec.superseded = true;
                    if let Some(k) = old {
                        self.eligible.remove(&k);
                    }
                }
                self.by_key.remove(&c.client_item_key);
                self.insert_pending(c.replacement.clone());
                Ok(())
            }
            QueueCommand::LeaseExpired(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::LeaseExpired)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = None;
                    rec.lease_expires_at = None;
                    rec.attempt_count += 1; // reclaim charges an attempt
                }
                Ok(())
            }
            QueueCommand::CohortExpired(c) => {
                let model = self.priority_model;
                let ids: Vec<ItemId> = self
                    .items
                    .values()
                    .filter(|r| {
                        r.group_key.as_ref() == Some(&c.group_key) && !r.state.is_terminal()
                    })
                    .map(|r| r.item_id.clone())
                    .collect();
                for id in ids {
                    if let Some(rec) = self.items.get_mut(&id) {
                        let old = (rec.state == ItemState::Pending).then(|| elig_key(rec, &model));
                        rec.state = ItemState::Failed; // forced terminal (cohort-incomplete)
                        rec.item_version += 1;
                        if let Some(k) = old {
                            self.eligible.remove(&k);
                        }
                    }
                }
                Ok(())
            }
            QueueCommand::FenceLease(c) => {
                for id in &c.item_ids {
                    if let Some(rec) = self.items.get_mut(id) {
                        rec.fenced = true;
                    }
                }
                Ok(())
            }
            QueueCommand::UnfenceLease(c) => {
                for id in &c.item_ids {
                    if let Some(rec) = self.items.get_mut(id) {
                        rec.fenced = false;
                    }
                }
                Ok(())
            }
            QueueCommand::PauseQueue => {
                self.paused = true;
                Ok(())
            }
            QueueCommand::ResumeQueue => {
                self.paused = false;
                Ok(())
            }
            QueueCommand::PurgeItems(c) => {
                let model = self.priority_model;
                for id in &c.item_ids {
                    if let Some(rec) = self.items.remove(id) {
                        self.by_key.remove(&rec.client_item_key);
                        if rec.state == ItemState::Pending {
                            self.eligible.remove(&elig_key(&rec, &model));
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UoW writer views (disjoint borrows of logs / projections)
// ---------------------------------------------------------------------------

struct LogWriterView<'a> {
    logs: &'a mut HashMap<ShardKey, LogData>,
}

impl LogWriter for LogWriterView<'_> {
    fn append(
        &mut self,
        shard: &ShardKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        let log = self.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
        let mut positions = Vec::with_capacity(commands.len());
        for cmd in commands {
            let seq = log.entries.len() as u64;
            log.entries.push(cmd.clone());
            let pos = CommandPosition::new(shard.clone(), log.epoch, seq);
            log.high_water = Some(pos.clone());
            positions.push(pos);
        }
        Ok(positions)
    }
}

struct ProjectionWriterView<'a> {
    projections: &'a mut HashMap<ShardKey, ProjectionData>,
}

impl ProjectionWriter for ProjectionWriterView<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            let proj = self
                .projections
                .get_mut(&pos.shard_key)
                .ok_or(EngineError::NotFound)?;
            proj.apply_command(&cmd.command)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The backend
// ---------------------------------------------------------------------------

#[derive(Default)]
struct State {
    logs: HashMap<ShardKey, LogData>,
    projections: HashMap<ShardKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
}

/// In-memory atomic-class backend. One `Mutex<State>`; `write` takes the lock for the whole unit of
/// work, so append + apply commit together (TD-007 §1 atomic class).
pub struct MemoryBackend {
    state: Mutex<State>,
    cmd_seq: AtomicU64,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            cmd_seq: AtomicU64::new(0),
        }
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn launch_shard(key: &QueueKey) -> ShardKey {
        ShardKey::new(key.tenant_id.clone(), key.queue_id.clone(), ShardId::ZERO)
    }

    fn make_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.cmd_seq.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            command_id: CommandId::new(format!("mem-{n}")),
            request_id: None,
            shard_id: ShardId::ZERO,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Append `env` to the shard log and apply it to the projection under the already-held lock —
    /// the atomic append+apply unit of work the claim/upsert/reclaim ports rely on.
    fn commit_locked(
        state: &mut State,
        shard: &ShardKey,
        env: CommandEnvelope,
    ) -> EngineResult<()> {
        let log = state.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
        let seq = log.entries.len() as u64;
        let pos = CommandPosition::new(shard.clone(), log.epoch, seq);
        log.entries.push(env.clone());
        log.high_water = Some(pos);
        let proj = state
            .projections
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?;
        proj.apply_command(&env.command)
    }

    fn to_claimed(rec: &ItemRecord) -> Option<ClaimedItem> {
        Some(ClaimedItem {
            item_id: rec.item_id.clone(),
            client_item_key: rec.client_item_key.clone(),
            item_version: rec.item_version,
            priority: rec.priority.clone(),
            group_key: rec.group_key.clone(),
            not_before: rec.not_before,
            lease_token: rec.lease_token.clone()?,
            lease_expires_at: rec.lease_expires_at?,
            attempt_count: rec.attempt_count,
            payload: rec.payload.clone(),
        })
    }
}

impl ClaimPort for MemoryBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Select priority-ordered eligible candidates (Invariant 1: per-item, in eligible order).
            let candidates: Vec<ItemId> = {
                let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
                if proj.paused {
                    return Ok(Claimed::default());
                }
                proj.eligible
                    .iter()
                    .filter_map(|k| proj.items.get(&k.item))
                    .filter(|r| {
                        r.state == ItemState::Pending
                            && !r.superseded
                            && r.not_before.map(|nb| nb <= req.now).unwrap_or(true)
                    })
                    .take(req.max_items)
                    .map(|r| r.item_id.clone())
                    .collect()
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            // Lease them atomically (append Claim command + apply, one lock).
            let cmd = QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
            });
            let env = self.make_envelope(cmd, candidates.clone(), req.now);
            Self::commit_locked(&mut g, &req.shard, env)?;
            // Render the now-leased records into the rich claimed-item shape.
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            let items: Vec<ClaimedItem> = candidates
                .iter()
                .filter_map(|id| proj.items.get(id))
                .filter_map(Self::to_claimed)
                .collect();
            // Every just-leased candidate must render (lease fields are Some under this lock).
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "leased candidate failed to render"
            );
            Ok(Claimed { items })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for MemoryBackend {
    fn replace_if_pending(
        &self,
        shard: &ShardKey,
        client_item_key: &ClientItemKey,
        new_item_id: ItemId,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let existing = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.by_key.get(client_item_key).cloned()
            };
            let max_attempts = g
                .queues
                .get(&shard.queue_key())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            let build_item = |item_id: ItemId| PushItem {
                client_item_key: client_item_key.clone(),
                item_id,
                priority: priority.clone(),
                not_before,
                group_key: group_key.clone(),
                max_attempts,
                payload: payload.clone(),
            };
            match existing {
                None => {
                    // No collision: plain insert.
                    let cmd = QueueCommand::Push(PushCommand {
                        items: vec![build_item(new_item_id.clone())],
                    });
                    let env = self.make_envelope(cmd, vec![new_item_id.clone()], now);
                    Self::commit_locked(&mut g, shard, env)?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some(existing_id) => {
                    let state = {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.items
                            .get(&existing_id)
                            .ok_or(EngineError::NotFound)?
                            .state
                    };
                    match state {
                        ItemState::Pending => {
                            let cmd = QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id.clone(),
                                replacement: build_item(new_item_id.clone()),
                            });
                            let env = self.make_envelope(cmd, vec![new_item_id.clone()], now);
                            Self::commit_locked(&mut g, shard, env)?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        // Collision with in-flight work — no lifecycle transition allowed.
                        ItemState::Leased => {
                            Err(EngineError::Invalid("collision with claimed item"))
                        }
                        // Terminal collision.
                        ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                    }
                }
            }
        })();
        std::future::ready(result)
    }
}

impl FinalizePort for MemoryBackend {
    fn finalize(
        &self,
        shard: &ShardKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Pre-commit fencing check: an operator-fenced lease's finalize is StaleLease, and the
            // Finalize command MUST NOT be appended if it would be rejected (no log/projection
            // divergence). Batch is all-or-nothing in this slice.
            {
                // Pre-commit validation so apply_command(Finalize) is infallible (B1: commit_locked
                // appends before applying, no rollback). Each item must be Leased and not fenced,
                // else reject WITHOUT appending.
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                for o in &outcomes {
                    match proj.items.get(&o.item_id) {
                        None => return Err(EngineError::NotFound),
                        Some(rec) if rec.fenced => return Err(EngineError::StaleLease),
                        Some(rec) if rec.state.is_terminal() => return Err(EngineError::Terminal),
                        Some(rec) if rec.state != ItemState::Leased => {
                            return Err(EngineError::Invalid("item is not leased"));
                        }
                        Some(_) => {}
                    }
                }
            }
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id.clone()).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = self.make_envelope(cmd, item_ids, now);
            Self::commit_locked(&mut g, shard, env)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReclaimDriver for MemoryBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            // Collect expired leases per shard (read), then reclaim them (write) — no client traffic
            // required, closing the orphan-on-quiet-queue gap (TD-007 §3).
            let mut expired: Vec<(ShardKey, Vec<ItemId>)> = Vec::new();
            for (shard, proj) in g.projections.iter() {
                let ids: Vec<ItemId> = proj
                    .items
                    .values()
                    .filter(|r| {
                        r.state == ItemState::Leased
                            && r.lease_expires_at.map(|exp| exp < now).unwrap_or(false)
                    })
                    .map(|r| r.item_id.clone())
                    .collect();
                if !ids.is_empty() {
                    expired.push((shard.clone(), ids));
                }
            }
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                });
                let env = self.make_envelope(cmd, ids.clone(), now);
                Self::commit_locked(&mut g, &shard, env)?;
                report.leases_reclaimed += ids.len() as u64;
            }
            // Cohort-timeout firing and progress-bound metering need cohort-deadline / eligible_since
            // state not yet modeled; they land with the cohort + observability features (plan §3,
            // TD-007 §3 D2 meter-only). Not fired here, so they are reported as zero rather than faked.
            Ok(report)
        })();
        std::future::ready(result)
    }
}

impl Backend for MemoryBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        // Whole UoW under one lock; the closure is synchronous (no await), so the !Send guard never
        // crosses an await point and the returned future is Send.
        let result = {
            let mut guard = self.state.lock().expect("memory backend poisoned");
            let State {
                logs, projections, ..
            } = &mut *guard;
            let mut lw = LogWriterView { logs };
            let mut pw = ProjectionWriterView { projections };
            f(&mut lw, &mut pw)
        };
        std::future::ready(result)
    }
}

impl ControlPlaneStore for MemoryBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(existing) = g.queues.get(&key) {
                // Idempotent create: compatible iff the placement-identity fields match (API-001).
                if existing.group_co_residency != definition.group_co_residency
                    || existing.shard_count != definition.shard_count
                {
                    return Err(EngineError::QueueDefinitionConflict);
                }
                return Ok(CreateQueueOutcome {
                    created: false,
                    definition: existing.clone(),
                });
            }
            let shard = Self::launch_shard(&key);
            g.logs.entry(shard.clone()).or_default();
            g.projections
                .entry(shard)
                .or_insert_with(|| ProjectionData::new(definition.priority_model));
            g.queues.insert(key, definition.clone());
            Ok(CreateQueueOutcome {
                created: true,
                definition,
            })
        })();
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .state
            .lock()
            .expect("poisoned")
            .queues
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result: Vec<QueueId> = self
            .state
            .lock()
            .expect("poisoned")
            .queues
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .map(|l| l.epoch)
            .unwrap_or(0));
        std::future::ready(result)
    }
}

impl LogRead for MemoryBackend {
    fn read_from(
        &self,
        shard: &ShardKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let log = g.logs.get(shard).ok_or(EngineError::NotFound)?;
            let start = match &from {
                Some(p) => p.sequence as usize + 1,
                None => 0,
            };
            let mut entries = Vec::new();
            for (i, cmd) in log.entries.iter().enumerate().skip(start).take(limit) {
                entries.push((
                    CommandPosition::new(shard.clone(), log.epoch, i as u64),
                    cmd.clone(),
                ));
            }
            let next = (start + entries.len() < log.entries.len()).then(|| {
                CommandPosition::new(shard.clone(), log.epoch, (start + entries.len()) as u64)
            });
            Ok(CommandPage { entries, next })
        })();
        std::future::ready(result)
    }
}

impl ProjectionRead for MemoryBackend {
    fn select_eligible(
        &self,
        shard: &ShardKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            if proj.paused {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            for key in proj.eligible.iter() {
                if out.len() >= limit {
                    break;
                }
                if let Some(rec) = proj.items.get(&key.item) {
                    let due = rec.not_before.as_ref().map(|nb| *nb <= now).unwrap_or(true);
                    if rec.state == ItemState::Pending && !rec.superseded && due {
                        out.push(rec.item_id.clone());
                    }
                }
            }
            Ok(out)
        })();
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &ShardKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            let mut out = Vec::new();
            for key in proj.eligible.iter() {
                if out.len() >= limit {
                    break;
                }
                if let Some(rec) = proj.items.get(&key.item)
                    && rec.state == ItemState::Pending
                    && !rec.superseded
                {
                    out.push(ItemView {
                        item_id: rec.item_id.clone(),
                        client_item_key: rec.client_item_key.clone(),
                        priority: rec.priority.clone(),
                        item_version: rec.item_version,
                    });
                }
            }
            Ok(out)
        })();
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            let out: Vec<LeaseView> = proj
                .items
                .values()
                .filter(|r| r.state == ItemState::Leased)
                .filter_map(|r| {
                    Some(LeaseView {
                        item_id: r.item_id.clone(),
                        lease_token: r.lease_token.clone()?,
                        lease_expires_at: r.lease_expires_at?,
                        attempt_count: r.attempt_count,
                    })
                })
                .collect();
            Ok(out)
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let shard = MemoryBackend::launch_shard(queue);
            let proj = g.projections.get(&shard).ok_or(EngineError::NotFound)?;
            let mut m = QueueMetrics::default();
            for r in proj.items.values() {
                if r.superseded {
                    continue;
                }
                match r.state {
                    ItemState::Pending => m.pending += 1,
                    ItemState::Leased => m.leased += 1,
                    ItemState::Complete => m.complete += 1,
                    ItemState::Failed => m.failed += 1,
                }
            }
            Ok(m)
        })();
        std::future::ready(result)
    }
}

impl SnapshotStore for MemoryBackend {
    fn write_snapshot(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let log = g.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
            let snap_ref = SnapshotRef {
                shard_key: shard.clone(),
                position,
                ref_id: format!("snap-{}", log.snapshots.len()),
            };
            log.snapshots.push((snap_ref.clone(), snapshot));
            Ok(snap_ref)
        })();
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .and_then(|l| l.snapshots.last().map(|(r, _)| r.clone())));
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = (|| {
            let g = self.state.lock().expect("poisoned");
            let log = g
                .logs
                .get(&snapshot_ref.shard_key)
                .ok_or(EngineError::NotFound)?;
            log.snapshots
                .iter()
                .find(|(r, _)| r.ref_id == snapshot_ref.ref_id)
                .map(|(_, s)| s.clone())
                .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &ShardKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = Ok(self
            .state
            .lock()
            .expect("poisoned")
            .logs
            .get(shard)
            .and_then(|l| l.high_water.clone()));
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.state.lock().expect("poisoned");
            let log = g.logs.get_mut(shard).ok_or(EngineError::NotFound)?;
            // Monotonic: reject a lower position (TD-007 §4).
            if let Some(cur) = &log.high_water
                && !cur.precedes(&position)
                && cur != &position
            {
                return Err(EngineError::Invalid("high-water regression"));
            }
            log.high_water = Some(position);
            Ok(())
        })();
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Injected utilities: a controllable clock and a sequential id generator
// ---------------------------------------------------------------------------

/// A clock you set explicitly — keeps reclaim/lease tests deterministic.
pub struct ManualClock {
    seconds: AtomicI64,
}

impl ManualClock {
    pub fn at(seconds: i64) -> Self {
        Self {
            seconds: AtomicI64::new(seconds),
        }
    }

    pub fn set(&self, seconds: i64) {
        self.seconds.store(seconds, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.seconds.load(Ordering::SeqCst), 0).expect("valid timestamp")
    }
}

/// Sequential id generation.
pub struct SeqIdGen {
    counter: AtomicU64,
}

impl Default for SeqIdGen {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        ItemId::new(format!("item-{n}")).expect("valid id")
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

#[cfg(test)]
mod tests;
