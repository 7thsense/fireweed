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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, IndexSpec, ItemEvent, ItemId, ItemState, LeaseToken, Metadata,
    PriorityModel, PriorityValue, UtcTimestamp, apply_transition, failure_event, priority_sort,
};
use pqueue_engine::{
    ClaimedItem, CommandEnvelope, CommandPosition, EngineError, EngineResult, FinalizeKind,
    FinalizeOutcome, IndexHit, ItemView, LeaseView, LiveItemView, PayloadUpdate,
    ProjectionSnapshot, PushItem, QueueCommand, QueueKey, QueueMetrics, SnapshotRef,
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
    fields: BTreeMap<String, Bytes>,
    metadata: Metadata,
    gate_keys: Vec<String>,
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
            item_id: self.item_id,
            client_item_key: self.client_item_key.clone(),
            item_version: self.item_version,
            priority: self.priority.clone(),
            group_key: self.group_key.clone(),
            not_before: self.not_before,
            lease_token: Some(self.lease_token.clone()?),
            lease_expires_at: self.lease_expires_at?,
            attempt_count: self.attempt_count,
            payload: self.payload.clone(),
            fields: self.fields.clone(),
            metadata: self.metadata.clone(),
            gate_keys: self.gate_keys.clone(),
        })
    }

    fn to_live(&self) -> Option<LiveItemView> {
        if self.superseded || self.state.is_terminal() {
            return None;
        }
        Some(LiveItemView {
            item_id: self.item_id,
            client_item_key: self.client_item_key.clone(),
            item_version: self.item_version,
            lifecycle_state: self.state,
            priority: self.priority.clone(),
            group_key: self.group_key.clone(),
            not_before: self.not_before,
            attempt_count: self.attempt_count,
            payload: self.payload.clone(),
            fields: self.fields.clone(),
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
        item: rec.item_id,
    }
}

// ---------------------------------------------------------------------------
// Secondary indexes (ADR-010): per-queue, name-keyed composite-key maps over configured item fields
// ---------------------------------------------------------------------------

/// One per-queue secondary index. Unique maps a composite key to exactly one item; non-unique maps a
/// key to the (id-ordered) set of items that carry it.
enum SecondaryIndex {
    Unique(BTreeMap<Vec<u8>, ItemId>),
    NonUnique(BTreeMap<Vec<u8>, BTreeSet<ItemId>>),
}

/// Composite-key encoding (ADR-010 §4.1): the length-prefixed concatenation of each field value, in the
/// spec's field order — `be_u32(len) || value_bytes` per field. Unambiguous (no separator can collide
/// with arbitrary content), order-sensitive, and total/`Ord` as a `Vec<u8>`. Raw bytes; no normalization.
fn encode_index_key(spec: &IndexSpec, fields: &BTreeMap<String, Bytes>) -> Option<Vec<u8>> {
    let mut key = Vec::new();
    for field in &spec.fields {
        // Sparse rule: a configured field missing from the item leaves the item out of THIS index.
        let value = fields.get(field)?;
        key.extend_from_slice(&(value.len() as u32).to_be_bytes());
        key.extend_from_slice(value);
    }
    Some(key)
}

/// Encode the lookup-side key values (already in field order) with the §4.1 rule — byte-identical to
/// [`encode_index_key`] for the same ordered values.
fn encode_index_lookup_key(values: &[Vec<u8>]) -> Vec<u8> {
    let mut key = Vec::new();
    for value in values {
        key.extend_from_slice(&(value.len() as u32).to_be_bytes());
        key.extend_from_slice(value);
    }
    key
}

/// Every `(index_name, composite_key)` this record's fields currently belong to (sparse skip — an index
/// missing any of its fields is omitted). A free function over `specs` so callers can compute keys while
/// holding other shared borrows of `self`.
fn index_keys(specs: &[IndexSpec], fields: &BTreeMap<String, Bytes>) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for spec in specs {
        if let Some(key) = encode_index_key(spec, fields) {
            out.push((spec.name.clone(), key));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// LogData: the per-shard command log + persisted high-water + snapshots
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LogData {
    epoch: u64,
    /// Each entry is stored with the `assignment_epoch` it was appended under (BQ-20), so a position
    /// replayed across an epoch boundary carries its true epoch — not a relabel to the current one.
    entries: Vec<(u64, CommandEnvelope)>,
    /// Persisted command_position high-water — a stored field, NOT recomputed from `entries.len()`,
    /// so it survives log retention/compaction and `item_version` never regresses (TD-007 §4).
    high_water: Option<CommandPosition>,
    snapshots: Vec<(SnapshotRef, ProjectionSnapshot)>,
}

impl LogData {
    /// `LogWriter::append` — append `commands` to this shard's log under `expected_epoch`, advancing the
    /// persisted high-water, returning the committed positions in order. TD-003 fencing rule: an
    /// `expected_epoch` that is not the log's current epoch is rejected with [`EngineError::EpochFenced`]
    /// (a stale owner), appending nothing.
    pub fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if expected_epoch != self.epoch {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for cmd in commands {
            let seq = self.entries.len() as u64;
            self.entries.push((self.epoch, cmd.clone()));
            let pos = CommandPosition::new(shard.clone(), self.epoch, seq);
            self.high_water = Some(pos.clone());
            positions.push(pos);
        }
        Ok(positions)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Advance to a new, strictly-greater `assignment_epoch` (TD-003 acquire / "durable fence before
    /// use"). Returns the new epoch. The seq counter is continuous across epochs (a new epoch fences who
    /// may extend the log; it never rewinds it — TD-003 Recovery), so positions stay monotonic by
    /// `(epoch, seq)`.
    pub fn advance_epoch(&mut self) -> u64 {
        self.epoch += 1;
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
        for (i, (entry_epoch, cmd)) in self.entries.iter().enumerate().skip(start).take(limit) {
            entries.push((
                CommandPosition::new(shard.clone(), *entry_epoch, i as u64),
                cmd.clone(),
            ));
        }
        let next = (start + entries.len() < self.entries.len()).then(|| {
            let (next_epoch, _) = &self.entries[start + entries.len()];
            CommandPosition::new(shard.clone(), *next_epoch, (start + entries.len()) as u64)
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
    expected_epoch: Option<u64>,
) -> EngineResult<()> {
    // The append is stamped with the queue's current epoch. An owner that supplies its cached acquire-time
    // epoch (`Some`) is fenced here if it has been superseded (ADR-009 / TD-003); `None` is the degenerate
    // sole-owner path (stamp current, never fence).
    let epoch = log.epoch();
    if expected_epoch.is_some_and(|e| e != epoch) {
        return Err(EngineError::EpochFenced);
    }
    log.append(shard, std::slice::from_ref(&env), epoch)?;
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
    /// Per-queue secondary indexes (ADR-010), keyed by `IndexSpec.name`. Built once from the queue's
    /// specs and maintained in the same `apply_command` arms that maintain `eligible`.
    indexes: BTreeMap<String, SecondaryIndex>,
    /// The index declarations (field lists), needed to recompute keys from a record's fields.
    index_specs: Vec<IndexSpec>,
}

impl ProjectionData {
    pub fn new(priority_model: PriorityModel, specs: &[IndexSpec]) -> Self {
        let mut indexes = BTreeMap::new();
        for spec in specs {
            let index = if spec.unique {
                SecondaryIndex::Unique(BTreeMap::new())
            } else {
                SecondaryIndex::NonUnique(BTreeMap::new())
            };
            indexes.insert(spec.name.clone(), index);
        }
        Self {
            items: HashMap::new(),
            by_key: HashMap::new(),
            eligible: BTreeSet::new(),
            next_seq: 0,
            priority_model,
            paused: false,
            indexes,
            index_specs: specs.to_vec(),
        }
    }

    /// Add `(item_id, keys)` to every covering index (Unique: set/replace the holder; NonUnique: add to
    /// the key's id set). Keys are precomputed by the caller so this can run after other borrows release.
    fn index_insert_keys(&mut self, item_id: ItemId, keys: &[(String, Vec<u8>)]) {
        for (name, key) in keys {
            match self.indexes.get_mut(name) {
                Some(SecondaryIndex::Unique(map)) => {
                    map.insert(key.clone(), item_id);
                }
                Some(SecondaryIndex::NonUnique(map)) => {
                    map.entry(key.clone()).or_default().insert(item_id);
                }
                None => {}
            }
        }
    }

    /// Remove `item_id` from every covering index for `keys` (Unique: drop the entry only if it still
    /// maps to this id; NonUnique: drop the id from the set, dropping the set when it empties).
    fn index_remove_keys(&mut self, item_id: ItemId, keys: &[(String, Vec<u8>)]) {
        for (name, key) in keys {
            match self.indexes.get_mut(name) {
                Some(SecondaryIndex::Unique(map)) => {
                    if map.get(key) == Some(&item_id) {
                        map.remove(key);
                    }
                }
                Some(SecondaryIndex::NonUnique(map)) => {
                    if let Some(set) = map.get_mut(key) {
                        set.remove(&item_id);
                        if set.is_empty() {
                            map.remove(key);
                        }
                    }
                }
                None => {}
            }
        }
    }

    fn insert_pending(&mut self, item: PushItem) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let rec = ItemRecord {
            item_id: item.item_id,
            client_item_key: item.client_item_key.clone(),
            priority: item.priority,
            not_before: item.not_before,
            group_key: item.group_key,
            payload: item.payload,
            fields: item.fields,
            metadata: item.metadata,
            gate_keys: item.gate_keys,
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
        self.by_key.insert(rec.client_item_key.clone(), rec.item_id);
        let keys = index_keys(&self.index_specs, &rec.fields);
        self.index_insert_keys(rec.item_id, &keys);
        self.items.insert(rec.item_id, rec);
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
            QueueCommand::CohortClaim(c) => {
                for id in &c.item_ids {
                    self.transition(id, ItemEvent::Claim)?;
                    let rec = self.items.get_mut(id).ok_or(EngineError::NotFound)?;
                    rec.lease_token = Some(c.lease_token.clone());
                    rec.lease_expires_at = Some(c.lease_expires_at);
                    rec.attempt_count += 1;
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
            QueueCommand::CohortRenewLease(_) => Ok(()),
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
            QueueCommand::UpdateFields(c) => {
                let rec = self
                    .items
                    .get_mut(&c.item_id)
                    .ok_or(EngineError::NotFound)?;
                // Bare field/payload merge (no lifecycle change), so it relies on `update_fields_validate`
                // having run pre-commit. Assert the pre-condition so a divergent replay is LOUD in
                // debug/test (apply stays infallible in release).
                debug_assert!(
                    !rec.state.is_terminal() && !rec.superseded && !rec.fenced,
                    "UpdateFields applied to a non-updatable item; update_fields_validate was bypassed"
                );
                // Secondary-index delta (ADR-010 §5): capture the keys this record holds BEFORE the field
                // ops, apply the ops, then recompute AFTER; keys that left are removed, keys that arrived
                // are inserted (unchanged keys are untouched). Read-after-write for the index.
                let old_keys = index_keys(&self.index_specs, &rec.fields);
                for (k, op) in &c.field_ops {
                    match op {
                        Some(v) => {
                            rec.fields.insert(k.clone(), v.clone());
                        }
                        None => {
                            rec.fields.remove(k);
                        }
                    }
                }
                match &c.payload {
                    PayloadUpdate::Keep => {}
                    PayloadUpdate::Set(p) => rec.payload = p.clone(),
                }
                rec.item_version += 1;
                let new_keys = index_keys(&self.index_specs, &rec.fields);
                let item_id = c.item_id;
                let removed: Vec<(String, Vec<u8>)> = old_keys
                    .iter()
                    .filter(|k| !new_keys.contains(k))
                    .cloned()
                    .collect();
                let added: Vec<(String, Vec<u8>)> = new_keys
                    .iter()
                    .filter(|k| !old_keys.contains(k))
                    .cloned()
                    .collect();
                self.index_remove_keys(item_id, &removed);
                self.index_insert_keys(item_id, &added);
                // Eligibility (state) is unchanged, so the eligibility index needs no update.
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
                    // Queue-native retry backoff: a Retry that returned the item to Pending (still under the
                    // attempt bound) defers its re-eligibility to `not_before`. `eligible_candidates` filters
                    // `not_before <= now`, and `elig_key` is not_before-independent, so no index update is
                    // needed. Guarded on Pending so an exhausted Retry (-> Failed) gets no backoff.
                    if matches!(o.kind, FinalizeKind::Retry)
                        && rec.state == ItemState::Pending
                        && let Some(nb) = o.not_before
                    {
                        rec.not_before = Some(nb);
                    }
                }
                Ok(())
            }
            QueueCommand::CohortFinalize(_) => Ok(()),
            QueueCommand::ReplacePending(c) => {
                // Supersede the old pending item; the old id thereafter reads as deleted/superseded.
                let model = self.priority_model;
                // Drop the superseded record's index keys (ADR-010 §5): a superseded item leaves every
                // index, then the replacement is inserted via `insert_pending`.
                let superseded_keys = self
                    .items
                    .get(&c.superseded_item_id)
                    .map(|rec| index_keys(&self.index_specs, &rec.fields));
                if let Some(rec) = self.items.get_mut(&c.superseded_item_id) {
                    let old = (rec.state == ItemState::Pending).then(|| elig_key(rec, &model));
                    rec.superseded = true;
                    if let Some(k) = old {
                        self.eligible.remove(&k);
                    }
                }
                if let Some(keys) = superseded_keys {
                    self.index_remove_keys(c.superseded_item_id, &keys);
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
                    .map(|r| r.item_id)
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
            // Gates (BQ-14d) are a relational-mode feature; the in-memory family stores no gate state and
            // no item gate keys, so a gate flip is a no-op here (the log-replay backends replay it as such).
            QueueCommand::SetGates(_) => Ok(()),
            QueueCommand::PurgeItems(c) => {
                let model = self.priority_model;
                for id in &c.item_ids {
                    if let Some(rec) = self.items.remove(id) {
                        self.by_key.remove(&rec.client_item_key);
                        if rec.state == ItemState::Pending {
                            self.eligible.remove(&elig_key(&rec, &model));
                        }
                        let keys = index_keys(&self.index_specs, &rec.fields);
                        self.index_remove_keys(rec.item_id, &keys);
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
            .map(|r| r.item_id)
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
                    item_id: rec.item_id,
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
                    item_id: r.item_id,
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

    /// Render live hot-storage items by client key, preserving input order.
    pub fn live_items_by_key(&self, keys: &[ClientItemKey]) -> Vec<Option<LiveItemView>> {
        keys.iter()
            .map(|key| {
                self.by_key
                    .get(key)
                    .and_then(|id| self.items.get(id))
                    .and_then(ItemRecord::to_live)
            })
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

    /// The current `item_version` of `id`, if present (read post-apply to return the bumped version
    /// from an `UpdateFields`).
    pub fn item_version(&self, id: &ItemId) -> Option<u64> {
        self.items.get(id).map(|r| r.item_version)
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

    /// Pre-commit validation for an in-place field/payload update (FAC-1). Legal while the item is live
    /// (Pending OR Leased) and not fenced/superseded; terminal/superseded/absent reject with the same
    /// structured errors as finalize. An `expected_item_version` mismatch rejects with `Conflict`
    /// (optimistic concurrency). Mutates nothing.
    pub fn update_fields_validate(
        &self,
        item_id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        match self.items.get(item_id) {
            None => Err(EngineError::NotFound),
            Some(rec) if rec.fenced => Err(EngineError::StaleLease),
            Some(rec) if rec.state.is_terminal() => Err(EngineError::Terminal),
            Some(rec) if rec.superseded => Err(EngineError::Superseded),
            Some(rec) => match expected_item_version {
                Some(v) if rec.item_version != v => Err(EngineError::Conflict),
                _ => Ok(()),
            },
        }
    }

    // -----------------------------------------------------------------------
    // Secondary-index pre-commit validation + reads (ADR-010 §5.1/§6)
    // -----------------------------------------------------------------------

    /// Pre-commit unique-index validation (ADR-010 §5.1; `commit` has no rollback). Returns
    /// [`EngineError::Conflict`] if inserting/keeping `item_id` with `fields` would land on a UNIQUE
    /// composite key already held by a DIFFERENT item — `exclude` (e.g. the superseded item in an upsert)
    /// is ignored. Mutates nothing.
    pub fn index_validate(
        &self,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        for (name, key) in index_keys(&self.index_specs, fields) {
            if let Some(SecondaryIndex::Unique(map)) = self.indexes.get(&name)
                && let Some(holder) = map.get(&key)
                && holder != item_id
                && Some(holder) != exclude
            {
                return Err(EngineError::Conflict);
            }
        }
        Ok(())
    }

    /// Pre-commit unique-index validation for a PUSH batch: each item is checked against the existing
    /// index AND against earlier items in the same batch (a violating batch appends nothing).
    pub fn index_validate_push(&self, items: &[PushItem]) -> EngineResult<()> {
        let mut batch: BTreeMap<(String, Vec<u8>), ItemId> = BTreeMap::new();
        for item in items {
            self.index_validate(&item.item_id, &item.fields, None)?;
            for (name, key) in index_keys(&self.index_specs, &item.fields) {
                if matches!(self.indexes.get(&name), Some(SecondaryIndex::Unique(_)))
                    && let Some(prev) = batch.insert((name, key), item.item_id)
                    && prev != item.item_id
                {
                    return Err(EngineError::Conflict);
                }
            }
        }
        Ok(())
    }

    /// Pre-commit unique-index validation for an in-place field update: the item's keys are recomputed
    /// from its CURRENT fields merged with `field_ops`, then checked (its own existing entries do not
    /// conflict). Mutates nothing.
    pub fn index_validate_update(
        &self,
        item_id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
    ) -> EngineResult<()> {
        let rec = self.items.get(item_id).ok_or(EngineError::NotFound)?;
        let mut merged = rec.fields.clone();
        for (k, op) in field_ops {
            match op {
                Some(v) => {
                    merged.insert(k.clone(), v.clone());
                }
                None => {
                    merged.remove(k);
                }
            }
        }
        self.index_validate(item_id, &merged, None)
    }

    /// Pre-commit unique-index validation for an upsert replacement: the replacement's keys are checked
    /// against every item EXCEPT the superseded one (which is removed in the same command).
    pub fn index_validate_replace(
        &self,
        superseded_item_id: &ItemId,
        replacement: &PushItem,
    ) -> EngineResult<()> {
        self.index_validate(
            &replacement.item_id,
            &replacement.fields,
            Some(superseded_item_id),
        )
    }

    /// Build the [`IndexHit`] for `id` from its current record (current `client_item_key`/`item_version`).
    fn index_hit(&self, id: &ItemId) -> Option<IndexHit> {
        self.items.get(id).map(|rec| IndexHit {
            client_item_key: rec.client_item_key.clone(),
            item_id: rec.item_id,
            item_version: rec.item_version,
        })
    }

    /// Resolve and validate a lookup against `index_name`: the index must exist and the supplied key value
    /// count must equal the spec's field count.
    fn index_spec(&self, index_name: &str, key_arity: usize) -> EngineResult<&IndexSpec> {
        let spec = self
            .index_specs
            .iter()
            .find(|s| s.name == index_name)
            .ok_or(EngineError::Invalid("unknown secondary index"))?;
        if key_arity != spec.fields.len() {
            return Err(EngineError::Invalid("secondary index key arity mismatch"));
        }
        Ok(spec)
    }

    /// Exact composite-key get on a UNIQUE index (ADR-010 §6). `Ok(None)` if no item holds the key;
    /// [`EngineError::Invalid`] if `index_name` is not a unique index on this queue or the key arity is wrong.
    pub fn index_get_unique(
        &self,
        index_name: &str,
        key_values: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        self.index_spec(index_name, key_values.len())?;
        match self.indexes.get(index_name) {
            Some(SecondaryIndex::Unique(map)) => {
                let key = encode_index_lookup_key(key_values);
                Ok(map.get(&key).and_then(|id| self.index_hit(id)))
            }
            _ => Err(EngineError::Invalid("secondary index is not unique")),
        }
    }

    /// Exact composite-key lookup on a (unique or non-unique) index (ADR-010 §6). Returns all matching
    /// items ordered by `item_id` ascending; empty if none.
    pub fn index_lookup(
        &self,
        index_name: &str,
        key_values: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.index_spec(index_name, key_values.len())?;
        let key = encode_index_lookup_key(key_values);
        let ids: Vec<ItemId> = match self.indexes.get(index_name) {
            Some(SecondaryIndex::Unique(map)) => map.get(&key).copied().into_iter().collect(),
            Some(SecondaryIndex::NonUnique(map)) => map
                .get(&key)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default(),
            None => Vec::new(),
        };
        Ok(ids.iter().filter_map(|id| self.index_hit(id)).collect())
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
            .map(|r| r.item_id)
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
        PushCommand, RenewLeaseCommand,
    };

    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
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
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            cohort_size: None,
            gate_keys: Vec::new(),
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

    /// BQ-20: an epoch advance fences future appends to the new epoch but does NOT rewind the log; a
    /// position replayed across the boundary carries its TRUE per-entry epoch (not a relabel to the
    /// current one), so `read_from` is consistent with the durably-stamped position and the high-water
    /// guard never false-regresses.
    #[test]
    fn read_from_carries_true_per_entry_epoch_across_an_advance() {
        let mut log = LogData::default();
        // Two appends at epoch 0.
        log.append(&shard(), &[env(QueueCommand::PauseQueue)], 0)
            .unwrap();
        log.append(&shard(), &[env(QueueCommand::ResumeQueue)], 0)
            .unwrap();
        // Acquire E+1 (durable fence), then one append at epoch 1.
        assert_eq!(log.advance_epoch(), 1);
        let pos = log
            .append(&shard(), &[env(QueueCommand::PauseQueue)], 1)
            .unwrap();
        // A stale epoch-0 append is now fenced (the seq counter is unchanged — no rewind).
        assert_eq!(
            log.append(&shard(), &[env(QueueCommand::ResumeQueue)], 0),
            Err(EngineError::EpochFenced)
        );

        // read_from labels each entry with the epoch it was written under, not the current epoch.
        let page = log.read_from(&shard(), None, 10);
        let epochs: Vec<u64> = page.entries.iter().map(|(p, _)| p.backend_epoch).collect();
        let seqs: Vec<u64> = page.entries.iter().map(|(p, _)| p.sequence).collect();
        assert_eq!(
            epochs,
            vec![0, 0, 1],
            "historical entries keep their true epoch"
        );
        assert_eq!(
            seqs,
            vec![0, 1, 2],
            "seq is continuous across the epoch boundary"
        );
        // The durably-returned append position matches what read_from reconstructs (epoch 1, seq 2).
        assert_eq!((pos[0].backend_epoch, pos[0].sequence), (1, 2));
        // The high-water (epoch 1, seq 2) does NOT regress against the replayed last position.
        let last = &page.entries.last().unwrap().0;
        assert_eq!(log.high_water().as_ref(), Some(last));
    }

    #[test]
    fn item_version_is_monotonic_per_item() {
        let sk = shard();
        let mut log = LogData::default();
        let mut proj = ProjectionData::new(model(), &[]);

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Push(PushCommand {
                items: vec![push_item("1", "ka", 5)],
            })),
            None,
        )
        .unwrap();
        let v0 = version_of(&proj, "1"); // push -> 1

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Claim(ClaimCommand {
                item_ids: vec![iid("1")],
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: ts(500),
            })),
            None,
        )
        .unwrap();
        let v1 = version_of(&proj, "1"); // claim -> 2

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![iid("1")],
                lease_expires_at: ts(600),
            })),
            None,
        )
        .unwrap();
        let v2 = version_of(&proj, "1"); // renew -> 3

        commit(
            &mut log,
            &mut proj,
            &sk,
            env(QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(iid("1"), FinalizeKind::Complete)],
            })),
            None,
        )
        .unwrap();
        let v3 = version_of(&proj, "1"); // finalize -> 4

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
        let mut proj = ProjectionData::new(model(), &[]);
        for p in [10_i64, 20, 30] {
            commit(
                &mut log,
                &mut proj,
                &sk,
                env(QueueCommand::Push(PushCommand {
                    items: vec![push_item(&format!("{p}"), &format!("k{p}"), p)],
                })),
                None,
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
