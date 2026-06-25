#![forbid(unsafe_code)]
//! # pqueue-projection
//!
//! The priority-ordered projection state machine ([`ProjectionData`]) and per-shard command log
//! ([`LogData`]), as pure in-memory types with no I/O. This is the **domain materialized view**: apply
//! rules, the eligibility index, lifecycle transitions, `item_version` bumps, lease/fence fields, and
//! the read queries the ports expose. Driven adapters (memory/sqlite/postgres) own only the
//! *persistence* of these, so every backend shares one correct projection rather than re-implementing
//! the apply/eligibility/lease logic.
//!
//! `LogData` and `ProjectionData` are kept SEPARATE (not bundled) so a backend can hold them in
//! disjoint maps and hand out `&mut dyn LogWriter` + `&mut dyn ProjectionWriter` simultaneously for the
//! two-writer unit of work. The free [`commit`] couples them for the orchestration ports. The owning
//! backend supplies the [`QueueKey`] (to stamp positions) and constructs each [`CommandEnvelope`] (so
//! each backend keeps its own command-id scheme); everything else is here.
//!
//! INVARIANT (TD-007 §1 / commit_locked): [`commit`] appends to the log BEFORE applying to the
//! projection and does NOT roll back. Callers that can reject a command (finalize fencing, upsert
//! collisions) MUST pre-validate via the provided helpers ([`ProjectionData::finalize_validate`],
//! [`ProjectionData::item_state`]) so `apply_command` is infallible for the command they commit.

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemEvent, ItemId, ItemState, LeaseToken, PriorityModel,
    PriorityValue, UtcTimestamp, apply_transition, failure_event, priority_sort,
};
use pqueue_engine::{
    ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult, FinalizeKind,
    FinalizeOutcome, ItemView, LeaseView, ProjectionSnapshot, PushItem, QueueCommand, QueueMetrics,
    QueueKey, SnapshotRef,
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
    payload: Option<Bytes>,
    state: ItemState,
    item_version: u64,
    attempt_count: u32,
    /// Retry bound (B'): a `Finalize{Retry}` once `attempt_count >= max_attempts` drives the item terminal
    /// (Failed) instead of back to pending — see the `Finalize` apply arm.
    max_attempts: u32,
    created_seq: u64,
    lease_token: Option<LeaseToken>,
    lease_expires_at: Option<UtcTimestamp>,
    fenced: bool,
    superseded: bool,
}

impl ItemRecord {
    fn to_claimed(&self) -> Option<ClaimedItem> {
        Some(ClaimedItem {
            item_id: self.item_id.clone(),
            client_item_key: self.client_item_key.clone(),
            item_version: self.item_version,
            priority: self.priority.clone(),
            group_key: self.group_key.clone(),
            not_before: self.not_before,
            lease_token: self.lease_token.clone()?,
            lease_expires_at: self.lease_expires_at?,
            attempt_count: self.attempt_count,
            payload: self.payload.clone(),
        })
    }
}

/// Priority-ordered eligibility key. Ascending order = claim order: priced items first (tag 0, then
/// `priority_sort` bytes), unpriced last (tag 1), FIFO by `created_seq` within ties.
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
// LogData: the per-shard command log + persisted high-water + snapshots
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LogData {
    epoch: u64,
    entries: Vec<CommandEnvelope>,
    /// Persisted command_position high-water — a stored field, NOT recomputed from `entries.len()`,
    /// so it survives log retention/compaction and `item_version` never regresses (TD-007 §4).
    high_water: Option<CommandPosition>,
    snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
}

impl LogData {
    /// `LogWriter::append` — append `commands` to this shard's log, advancing the persisted high-water,
    /// returning the committed positions in order.
    pub fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        let mut positions = Vec::with_capacity(commands.len());
        for cmd in commands {
            let seq = self.entries.len() as u64;
            self.entries.push(cmd.clone());
            let pos = CommandPosition::new(shard.clone(), self.epoch, seq);
            self.high_water = Some(pos.clone());
            positions.push(pos);
        }
        Ok(positions)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// `LogRead::read_from` — a page of committed commands for replay/rebuild.
    pub fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> pqueue_engine::CommandPage {
        let start = match &from {
            Some(p) => p.sequence as usize + 1,
            None => 0,
        };
        let mut entries = Vec::new();
        for (i, cmd) in self.entries.iter().enumerate().skip(start).take(limit) {
            entries.push((
                CommandPosition::new(shard.clone(), self.epoch, i as u64),
                cmd.clone(),
            ));
        }
        let next = (start + entries.len() < self.entries.len()).then(|| {
            CommandPosition::new(shard.clone(), self.epoch, (start + entries.len()) as u64)
        });
        pqueue_engine::CommandPage { entries, next }
    }

    pub fn high_water(&self) -> Option<CommandPosition> {
        self.high_water.clone()
    }

    /// Set the persisted high-water, rejecting a regression (TD-007 §4 monotonicity).
    pub fn set_high_water(&mut self, position: CommandPosition) -> EngineResult<()> {
        if let Some(cur) = &self.high_water
            && !cur.precedes(&position)
            && cur != &position
        {
            return Err(EngineError::Invalid("high-water regression"));
        }
        self.high_water = Some(position);
        Ok(())
    }

    pub fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> SnapshotRef {
        let snap_ref = SnapshotRef {
            queue: shard.clone(),
            position,
            ref_id: format!("snap-{}", self.snapshots.len()),
        };
        self.snapshots.push((snap_ref.clone(), snapshot));
        snap_ref
    }

    pub fn latest_snapshot(&self) -> Option<SnapshotRef> {
        self.snapshots.last().map(|(r, _)| r.clone())
    }

    pub fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        self.snapshots
            .iter()
            .find(|(r, _)| r.ref_id == snapshot_ref.ref_id)
            .map(|(_, s)| s.clone())
            .ok_or(EngineError::NotFound)
    }
}

/// Atomic append + apply (TD-007 §1): append `env` to `log`, then apply it to `proj`. The caller MUST
/// have pre-validated rejectable commands (module INVARIANT) so the apply is infallible. `log` and
/// `proj` are passed separately so a backend can hold them in disjoint maps for the two-writer UoW.
pub fn commit(
    log: &mut LogData,
    proj: &mut ProjectionData,
    shard: &QueueKey,
    env: CommandEnvelope,
) -> EngineResult<()> {
    log.append(shard, std::slice::from_ref(&env))?;
    proj.apply_command(&env.command)
}

// ---------------------------------------------------------------------------
// ProjectionData: items + eligibility index + pause flag
// ---------------------------------------------------------------------------

pub struct ProjectionData {
    items: HashMap<ItemId, ItemRecord>,
    by_key: HashMap<ClientItemKey, ItemId>,
    eligible: BTreeSet<EligKey>,
    next_seq: u64,
    priority_model: PriorityModel,
    paused: bool,
}

impl ProjectionData {
    pub fn new(priority_model: PriorityModel) -> Self {
        Self {
            items: HashMap::new(),
            by_key: HashMap::new(),
            eligible: BTreeSet::new(),
            next_seq: 0,
            priority_model,
            paused: false,
        }
    }

    fn insert_pending(&mut self, item: PushItem) {
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

    pub fn apply_command(&mut self, cmd: &QueueCommand) -> EngineResult<()> {
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
                    // Unlike the `transition()`-routed arms, renew bare-mutates the deadline, so it
                    // relies entirely on every caller pre-validating via `renew_validate`. Assert the
                    // pre-condition so a divergent replay is LOUD in debug/test rather than silently
                    // extending a non-leased lease (apply stays infallible in release).
                    debug_assert!(
                        rec.state == ItemState::Leased
                            && !rec.fenced
                            && !rec.superseded
                            && !rec.state.is_terminal(),
                        "RenewLease applied to a non-renewable item; renew_validate was bypassed"
                    );
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.item_version += 1;
                }
                Ok(())
            }
            QueueCommand::ReassignLease(c) => {
                for id in &c.item_ids {
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    // Like RenewLease, this bare-mutates an already-Leased item, so it relies on the
                    // caller pre-validating via `reassign_validate`. Assert the pre-condition so a
                    // divergent replay is LOUD (apply stays infallible in release).
                    debug_assert!(
                        rec.state == ItemState::Leased
                            && !rec.fenced
                            && !rec.superseded
                            && !rec.state.is_terminal(),
                        "ReassignLease applied to a non-renewable item; reassign_validate was bypassed"
                    );
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1; // a re-delivery to a new consumer is a delivery (TD-006:129)
                    rec.item_version += 1;
                }
                Ok(())
            }
            QueueCommand::Finalize(c) => {
                for o in &c.outcomes {
                    let ev = match o.kind {
                        FinalizeKind::Complete => ItemEvent::FinalizeComplete,
                        FinalizeKind::Fail => ItemEvent::FinalizeFail,
                        FinalizeKind::Retry => {
                            // Retry-exhaustion (B'): `attempt_count` = deliveries so far (Claim charges,
                            // reclaim/release do not). `failure_event` (the canonical core predicate) sends
                            // a retry that has used all `max_attempts` deliveries to TERMINAL (Failed)
                            // instead of back to pending; a retry UNDER the bound returns it to pending
                            // (claimable again, the next claim charging the next delivery). Only `Retry` is
                            // bounded — `Release` (no-fault give-back) and `Rearm` (recurrence) are not.
                            // NOTE (scope): this bounds the EXPLICIT-retry path only. The claim/reclaim path
                            // is NOT attempt-bounded — an item whose lease repeatedly EXPIRES (LeaseExpired
                            // → pending → re-Claim, +1 each) can exceed `max_attempts` deliveries without
                            // terminating; bounding that poison-loop is separate, owed policy.
                            // The decision is deterministic from the replayed projection, so apply stays
                            // infallible (both Leased→Pending and Leased→Failed are legal transitions).
                            let rec = self.items.get(&o.item_id).ok_or(EngineError::NotFound)?;
                            failure_event(rec.attempt_count, rec.max_attempts)
                        }
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
                    // INVARIANT: `attempt_count` = number of times the item was handed to a worker, so
                    // it increments ONLY in the Claim arm. A reclaim returns the item to pending (not a
                    // delivery) and does NOT charge — the subsequent redelivery (a fresh Claim) charges
                    // the one attempt. (TD-006:129 reconciliation; poison detection is preserved since
                    // every redelivery still increments.)
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
// Read / decision queries the orchestration ports build on
// ---------------------------------------------------------------------------

impl ProjectionData {
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Priority-ordered eligible candidates (pending, not superseded, due at `now`), capped at `max`.
    /// Returns empty while the queue is paused. This is the claim/select selection (Invariant 1:
    /// per-item, in eligible order).
    pub fn eligible_candidates(&self, now: UtcTimestamp, max: usize) -> Vec<ItemId> {
        if self.paused {
            return Vec::new();
        }
        self.eligible
            .iter()
            .filter_map(|k| self.items.get(&k.item))
            .filter(|r| {
                r.state == ItemState::Pending
                    && !r.superseded
                    && r.not_before.map(|nb| nb <= now).unwrap_or(true)
            })
            .take(max)
            .map(|r| r.item_id.clone())
            .collect()
    }

    /// `ProjectionRead::select_eligible`.
    pub fn select_eligible(&self, now: UtcTimestamp, limit: usize) -> Vec<ItemId> {
        self.eligible_candidates(now, limit)
    }

    /// `ProjectionRead::peek` — non-destructive eligible view (shows the pending order).
    pub fn peek(&self, limit: usize) -> Vec<ItemView> {
        let mut out = Vec::new();
        for key in self.eligible.iter() {
            if out.len() >= limit {
                break;
            }
            if let Some(rec) = self.items.get(&key.item)
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
        out
    }

    /// `ProjectionRead::pending` — the in-flight (leased) items.
    pub fn pending_leases(&self) -> Vec<LeaseView> {
        self.items
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
            .collect()
    }

    /// `ProjectionRead::metrics` — per-state counts (superseded items excluded).
    pub fn metrics(&self) -> QueueMetrics {
        let mut m = QueueMetrics::default();
        for r in self.items.values() {
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
        m
    }

    /// Render the given ids into the rich claimed-item shape (lease fields must be `Some`). Used right
    /// after a Claim commit to build the `ClaimPort` response.
    pub fn render_claimed(&self, ids: &[ItemId]) -> Vec<ClaimedItem> {
        ids.iter()
            .filter_map(|id| self.items.get(id))
            .filter_map(ItemRecord::to_claimed)
            .collect()
    }

    /// The item id currently mapped to `client_item_key`, if any (upsert collision lookup).
    pub fn lookup_by_key(&self, client_item_key: &ClientItemKey) -> Option<ItemId> {
        self.by_key.get(client_item_key).cloned()
    }

    /// The lifecycle state of `id`, if present (upsert collision classification).
    pub fn item_state(&self, id: &ItemId) -> Option<ItemState> {
        self.items.get(id).map(|r| r.state)
    }

    /// Pre-commit validation for a finalize batch (commit_locked has no rollback): every targeted item
    /// must be present, not fenced, and currently `Leased`. Returns the structured rejection otherwise,
    /// WITHOUT mutating anything.
    pub fn finalize_validate(&self, outcomes: &[FinalizeOutcome]) -> EngineResult<()> {
        self.validate_leased(outcomes.iter().map(|o| &o.item_id))
    }

    /// Pre-commit validation for a lease RENEW batch — IDENTICAL rejection semantics to
    /// [`finalize_validate`] (a renew of a fenced/superseded/terminal/non-leased item rejects with the
    /// same structured error, appending nothing), so renew and finalize never diverge.
    pub fn renew_validate(&self, ids: &[ItemId]) -> EngineResult<()> {
        self.validate_leased(ids.iter())
    }

    /// Pre-commit validation for a lease REASSIGN batch (cross-consumer `XCLAIM`) — IDENTICAL rejection
    /// semantics to [`renew_validate`]/[`finalize_validate`]: only a live, non-fenced, non-superseded,
    /// non-terminal leased item may be transferred.
    pub fn reassign_validate(&self, ids: &[ItemId]) -> EngineResult<()> {
        self.validate_leased(ids.iter())
    }

    /// Shared "every id is present + Leased + not fenced + not superseded" check used by finalize/renew.
    fn validate_leased<'a>(&self, ids: impl Iterator<Item = &'a ItemId>) -> EngineResult<()> {
        for id in ids {
            match self.items.get(id) {
                None => return Err(EngineError::NotFound),
                Some(rec) if rec.fenced => return Err(EngineError::StaleLease),
                Some(rec) if rec.state.is_terminal() => return Err(EngineError::Terminal),
                // A superseded id (replaced by an upsert) is an explicit `superseded` failure, NOT the
                // generic not-leased `Invalid` (TD-006 §3/§6.5). Check before the not-leased catch-all.
                Some(rec) if rec.superseded => return Err(EngineError::Superseded),
                Some(rec) if rec.state != ItemState::Leased => {
                    return Err(EngineError::Invalid("item is not leased"));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Ids whose lease has expired strictly before `now` (half-open: valid through `lease_expires_at`).
    /// Drives the reclaim tick.
    pub fn expired_leases(&self, now: UtcTimestamp) -> Vec<ItemId> {
        self.items
            .values()
            .filter(|r| {
                r.state == ItemState::Leased
                    && r.lease_expires_at.map(|exp| exp < now).unwrap_or(false)
            })
            .map(|r| r.item_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    //! White-box tests over the projection's private state (item_version, log compaction). Behavioral
    //! port-level conformance is exercised against the backends in `pqueue-conformance`.
    use super::*;
    use pqueue_core::{
        PriorityDirection, PriorityModelKind, PriorityTieBreaker, QueueId, TenantId,
    };
    use pqueue_engine::{
        ClaimCommand, CommandChecksum, CommandId, FinalizeCommand, FinalizeKind, FinalizeOutcome,
        PushCommand, RenewLeaseCommand, };

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("t1").unwrap(),
            QueueId::new("q1").unwrap(),
        )
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn model() -> PriorityModel {
        PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        }
    }
    fn iid(s: &str) -> ItemId {
        ItemId::new(s).unwrap()
    }
    fn push_item(id: &str, key: &str, priority: i64) -> PushItem {
        PushItem {
            client_item_key: ClientItemKey::new(key).unwrap(),
            item_id: iid(id),
            priority: Some(PriorityValue::Int64(priority)),
            not_before: None,
            group_key: None,
            max_attempts: 3,
            payload: None,
        }
    }
    fn env(command: QueueCommand) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new("c"),
            request_id: None,
            item_ids: vec![],
            command,
            checksum: CommandChecksum(0),
            created_at: ts(0),
        }
    }
    fn version_of(proj: &ProjectionData, id: &str) -> u64 {
        proj.items.get(&iid(id)).unwrap().item_version
    }

    #[test]
    fn item_version_is_monotonic_per_item() {
        let sk = shard();
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(model());

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Push(PushCommand {
                items: vec![push_item("a", "ka", 5)],
            })),
        )
        .unwrap();
        let v0 = version_of(&proj, "a"); // push -> 1

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("a")],
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: ts(500),
            })),
        )
        .unwrap();
        let v1 = version_of(&proj, "a"); // claim -> 2

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("a")],
                lease_expires_at: ts(600),
            })),
        )
        .unwrap();
        let v2 = version_of(&proj, "a"); // renew -> 3

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: iid("a"),
                    kind: FinalizeKind::Complete,
                }],
            })),
        )
        .unwrap();
        let v3 = version_of(&proj, "a"); // finalize -> 4

        assert_eq!(
            (v0, v1, v2, v3),
            (1, 2, 3, 4),
            "item_version bumps exactly once per committed mutation (API-001)"
        );
    }

    #[test]
    fn high_water_survives_log_compaction() {
        let sk = shard();
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(model());
        for p in [10_i64, 20, 30] {
            commit(
                &mut log,
                &mut proj,
                &sk,
                env(QueueCommand::Push(PushCommand {
                    items: vec![push_item(&format!("i{p}"), &format!("k{p}"), p)],
                })),
            )
            .unwrap();
        }
        let before = log.high_water().unwrap();
        // Simulate log compaction: drop the stored entries (retention). The persisted high-water is a
        // separate field, NOT recomputed from entries.len() — so it MUST be unchanged (TD-007 §4).
        log.entries.clear();
        let after = log.high_water().unwrap();
        assert_eq!(
            before, after,
            "high-water is persisted, not recomputed from a compacted log"
        );
        assert_eq!(
            after.sequence, 2,
            "3 commits -> seq 2 (would be 0 if recomputed from empty entries)"
        );
    }
}
