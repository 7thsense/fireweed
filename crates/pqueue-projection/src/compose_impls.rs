//! Axis implementations over the in-process projection state machine (ADR-012, Phase 1).
//!
//! - [`MemoryLog`] — a [`pqueue_engine::LogStore`] over a per-shard [`LogData`] map (in-process command
//!   log + epoch authority). The log substrate of the composed memory backend.
//! - [`InMemoryProjection`] — a [`pqueue_engine::ProjectionStore`] over a per-shard [`ProjectionData`]
//!   map. The shared materialized read model; reused by BOTH the composed memory backend (with
//!   [`MemoryLog`]) and the composed sqlite backend (with the durable sqlite log).
//!
//! These are extracted verbatim from the `State`/`Inner` internals of the monolithic `MemoryBackend` and
//! `SqliteBackend`, so the compositions are behaviorally identical to the monoliths (proven by running the
//! shared TD-001 conformance suite against both).

use std::collections::BTreeMap;

use rustc_hash::FxHashMap;

use bytes::Bytes;
use pqueue_core::{ClientItemKey, ItemId, ItemState, QueueDefinition, UtcTimestamp};
use pqueue_engine::{
    ClaimRef, ClaimedItem, CommandEnvelope, CommandPage, CommandPosition, EngineError,
    EngineResult, FinalizeOutcome, IndexHit, ItemView, LeaseView, LiveItemView, LogStore,
    ProjectionSnapshot, ProjectionStore, PushItem, QueueKey, QueueMetrics, SnapshotRef,
};

use crate::{LogData, ProjectionData, ProjectionImage};

// ---------------------------------------------------------------------------
// MemoryLog — the in-process command-log axis
// ---------------------------------------------------------------------------

/// The in-process command-log axis (ADR-012): one [`LogData`] per shard (command log + epoch authority +
/// snapshots + high-water). The log substrate of the composed memory backend.
#[derive(Default)]
pub struct MemoryLog {
    logs: FxHashMap<QueueKey, LogData>,
}

impl MemoryLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LogStore for MemoryLog {
    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        self.logs.entry(shard.clone()).or_default();
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.logs
            .get(shard)
            .map(|l| l.epoch())
            .ok_or(EngineError::NotFound)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        self.logs
            .get_mut(shard)
            .map(|l| l.advance_epoch())
            .ok_or(EngineError::NotFound)
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .append(shard, commands, expected_epoch)
    }

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage> {
        Ok(self
            .logs
            .get(shard)
            .ok_or(EngineError::NotFound)?
            .read_from(shard, from, limit))
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        Ok(self.logs.get(shard).and_then(|l| l.high_water()))
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        self.logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .set_high_water(position)
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        Ok(self
            .logs
            .get_mut(shard)
            .ok_or(EngineError::NotFound)?
            .write_snapshot(shard, position, snapshot))
    }

    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        Ok(self.logs.get(shard).and_then(|l| l.latest_snapshot()))
    }

    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        self.logs
            .get(&snapshot_ref.queue)
            .ok_or(EngineError::NotFound)?
            .read_snapshot(snapshot_ref)
    }
}

// ---------------------------------------------------------------------------
// InMemoryProjection — the in-memory materialized-read-model axis
// ---------------------------------------------------------------------------

/// The in-memory projection axis (ADR-012): one [`ProjectionData`] per shard. The shared read model used
/// by every log-replay composition (memory + sqlite-log), so the two backends are byte-for-byte identical
/// on the projection.
#[derive(Default)]
pub struct InMemoryProjection {
    projections: FxHashMap<QueueKey, ProjectionData>,
}

impl InMemoryProjection {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace one in-memory shard with a fully materialized projection image.
    pub fn hydrate_shard(
        &mut self,
        definition: &QueueDefinition,
        image: ProjectionImage,
    ) -> EngineResult<()> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let projection = ProjectionData::from_image(definition, image)?;
        self.projections.insert(key, projection);
        Ok(())
    }

    pub fn apply_borrowed(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
                .ok_or(EngineError::NotFound)?
                .apply_command(&cmd.command)?;
        }
        Ok(())
    }

    fn get(&self, shard: &QueueKey) -> EngineResult<&ProjectionData> {
        self.projections.get(shard).ok_or(EngineError::NotFound)
    }
}

impl ProjectionStore for InMemoryProjection {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.projections.entry(key).or_insert_with(|| {
            ProjectionData::new(
                definition.priority_model,
                definition.ordering_mode,
                definition.max_rank_error,
                definition.recurrence,
                &definition.secondary_indexes,
            )
            .with_typed_indexes(&definition.typed_indexes)
        });
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply_borrowed(positions, commands)
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.eligible_candidates(now, max))
    }

    fn eligible_candidates_after(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        after: Option<ItemId>,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.eligible_candidates_after(now, after, max))
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        Ok(self.get(shard)?.render_claimed(ids))
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        Ok(self.get(shard)?.lookup_by_key(client_item_key))
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        Ok(self.get(shard)?.item_state(id))
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        Ok(self.get(shard)?.item_version(id))
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.expired_leases(now))
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        self.projections
            .iter()
            .filter_map(|(shard, proj)| {
                let ids = proj.expired_leases(now);
                (!ids.is_empty()).then(|| (shard.clone(), ids))
            })
            .collect()
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        self.get(shard)?.finalize_validate(outcomes)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.get(shard)?.renew_validate(ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.get(shard)?.reassign_validate(ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .update_fields_validate(id, expected_item_version)
    }

    fn index_validate(
        &self,
        shard: &QueueKey,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&serde_json::Value>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .index_validate_with_entity(item_id, fields, entity, exclude)
    }

    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()> {
        self.get(shard)?.index_validate_push(items)
    }

    fn index_validate_replace(
        &self,
        shard: &QueueKey,
        existing_id: &ItemId,
        item: &PushItem,
    ) -> EngineResult<()> {
        self.get(shard)?.index_validate_replace(existing_id, item)
    }

    fn index_validate_update(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
        entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        self.get(shard)?
            .index_validate_update_with_entity(id, field_ops, entity)
    }

    // -- commit-class: the in-memory projection materializes the full Snorri commit-class read model
    //    (side records + instance fences + lease-token/version commit validation), lifted verbatim from
    //    `ProjectionData`, so the composed memory backend reaches capability parity with `MemoryBackend`.

    fn supports_commit_transition(&self) -> bool {
        true
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.get(shard)?.commit_validate(refs, now)
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        Ok(self.get(shard)?.instance_fence(key))
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        Ok(self.get(shard)?.side_record(key).cloned())
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        Ok(self.get(shard)?.select_eligible(now, limit))
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        Ok(self.get(shard)?.peek(limit))
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        Ok(self.get(shard)?.pending_leases())
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        Ok(self.get(shard)?.metrics())
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        Ok(self.get(shard)?.live_items_by_key(keys))
    }

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        self.get(shard)?.index_get_unique(index, key)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.get(shard)?.index_lookup(index, key)
    }
}
