//! In-memory reference implementation of all storage traits.
//!
//! This backend provides no durability (`DurabilityProfile::None`). It is the
//! target for the `storage_conformance` test suite and serves as the reference
//! implementation for backend contract validation.

use parking_lot::Mutex;
use pqueue_core::{
    apply_transition, evaluate_eligibility, EligibilitySnapshot, ItemEvent, ItemId, ItemState,
    Metadata, QueueDefinition, QueueEligibilityRules, QueueId, TenantId, UtcTimestamp,
};
use std::collections::HashMap;

use crate::commands::{CommandEnvelope, FinalizeKind, QueueCommand};
use crate::traits::{
    AppendBatchResult, ClaimRequest, ClaimResult, CommandPage, ControlPlaneError, ControlPlaneStore,
    CreateQueueResult, DurabilityProfile, LogStore, LogStoreError, ProjectionError, ProjectionStore,
    ProjectionSnapshot, QueueMetricsSnapshot, ShardAssignment, SnapshotError, SnapshotRef,
    SnapshotStore,
};
use crate::types::{CommandPosition, QueueKey, ShardId, ShardKey};

// ---------------------------------------------------------------------------
// MemoryLogStore
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ShardLog {
    epoch: u64,
    entries: Vec<(CommandPosition, CommandEnvelope)>,
}

pub struct MemoryLogStore {
    shards: Mutex<HashMap<ShardKey, ShardLog>>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self { shards: Mutex::new(HashMap::new()) }
    }
}

impl Default for MemoryLogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore for MemoryLogStore {
    async fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError> {
        let mut shards = self.shards.lock();
        let log = shards.entry(shard.clone()).or_default();

        if expected_epoch.is_some_and(|exp| log.epoch != exp) {
            return Err(LogStoreError::StalEpoch {
                expected: expected_epoch.unwrap(),
                current: log.epoch,
            });
        }

        let mut last_position = None;
        for cmd in commands {
            let seq = log.entries.len() as u64;
            let pos = CommandPosition {
                shard_key: shard.clone(),
                sequence: seq,
                backend_epoch: log.epoch,
            };
            last_position = Some(pos.clone());
            log.entries.push((pos, cmd));
        }

        Ok(AppendBatchResult {
            last_position: last_position.unwrap_or(CommandPosition {
                shard_key: shard.clone(),
                sequence: 0,
                backend_epoch: log.epoch,
            }),
        })
    }

    async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, LogStoreError> {
        let shards = self.shards.lock();
        let log = shards.get(shard).ok_or(LogStoreError::ShardNotFound)?;

        let start = position.map(|p| p.sequence as usize + 1).unwrap_or(0);
        let end = (start + limit).min(log.entries.len());

        let commands = log.entries[start..end].to_vec();
        let next_position = if end < log.entries.len() {
            Some(log.entries[end - 1].0.clone())
        } else {
            None
        };

        Ok(CommandPage { commands, next_position })
    }

    fn durability_profile(&self) -> DurabilityProfile {
        DurabilityProfile::None
    }
}

// ---------------------------------------------------------------------------
// MemoryProjectionStore
// ---------------------------------------------------------------------------

struct ItemRecord {
    item_id: ItemId,
    state: ItemState,
    not_before: Option<UtcTimestamp>,
    retry_backoff_until: Option<UtcTimestamp>,
    #[allow(dead_code)]
    max_attempts: u32,
    attempts: u32,
    lease_token: Option<String>,
    lease_expires_at: Option<UtcTimestamp>,
    insertion_order: usize,
}

#[derive(Default)]
struct ShardProjection {
    items: HashMap<ItemId, ItemRecord>,
    next_insertion: usize,
}

impl ShardProjection {
    fn metrics(&self) -> QueueMetricsSnapshot {
        let mut m = QueueMetricsSnapshot::default();
        for rec in self.items.values() {
            match rec.state {
                ItemState::Pending => m.pending_count += 1,
                ItemState::Leased => m.leased_count += 1,
                ItemState::Complete => m.completed_count += 1,
                ItemState::Failed => m.failed_count += 1,
            }
        }
        m
    }
}

pub struct MemoryProjectionStore {
    shards: Mutex<HashMap<ShardKey, ShardProjection>>,
}

impl MemoryProjectionStore {
    pub fn new() -> Self {
        Self { shards: Mutex::new(HashMap::new()) }
    }
}

impl Default for MemoryProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

fn finalize_kind_to_event(kind: FinalizeKind) -> ItemEvent {
    match kind {
        FinalizeKind::Complete => ItemEvent::FinalizeComplete,
        FinalizeKind::Fail => ItemEvent::FinalizeFail,
        FinalizeKind::Retry => ItemEvent::FinalizeRetry,
        FinalizeKind::Release => ItemEvent::FinalizeRelease,
        FinalizeKind::Rearm => ItemEvent::FinalizeRearm,
    }
}

impl ProjectionStore for MemoryProjectionStore {
    async fn apply_committed(
        &self,
        position: CommandPosition,
        commands: &[CommandEnvelope],
    ) -> Result<(), ProjectionError> {
        let mut shards = self.shards.lock();
        let proj = shards.entry(position.shard_key).or_default();

        for envelope in commands {
            match &envelope.command {
                QueueCommand::BatchPush(cmd) => {
                    for item in &cmd.items {
                        let order = proj.next_insertion;
                        proj.next_insertion += 1;
                        proj.items.insert(
                            item.item_id.clone(),
                            ItemRecord {
                                item_id: item.item_id.clone(),
                                state: ItemState::Pending,
                                not_before: item.not_before.clone(),
                                retry_backoff_until: None,
                                max_attempts: item.max_attempts,
                                attempts: 0,
                                lease_token: None,
                                lease_expires_at: None,
                                insertion_order: order,
                            },
                        );
                    }
                }
                QueueCommand::BatchClaim(cmd) => {
                    for id in &cmd.item_ids {
                        if let Some(rec) = proj.items.get_mut(id) {
                            if let Ok(next) = apply_transition(rec.state, ItemEvent::Claim) {
                                rec.state = next;
                                rec.attempts += 1;
                                rec.lease_token = Some(cmd.lease_token.clone());
                                rec.lease_expires_at = Some(cmd.lease_expires_at.clone());
                            }
                        }
                    }
                }
                QueueCommand::BatchFinalize(cmd) => {
                    for outcome in &cmd.outcomes {
                        if let Some(rec) = proj.items.get_mut(&outcome.item_id) {
                            let event = finalize_kind_to_event(outcome.kind);
                            if let Ok(next) = apply_transition(rec.state, event) {
                                rec.state = next;
                                rec.lease_token = None;
                                rec.lease_expires_at = None;
                            }
                        }
                    }
                }
                QueueCommand::BatchRenewLeases(cmd) => {
                    for id in &cmd.item_ids {
                        if let Some(rec) = proj.items.get_mut(id) {
                            rec.lease_expires_at = Some(cmd.lease_expires_at.clone());
                        }
                    }
                }
                QueueCommand::LeaseExpired(cmd) => {
                    for id in &cmd.item_ids {
                        if let Some(rec) = proj.items.get_mut(id) {
                            if let Ok(next) = apply_transition(rec.state, ItemEvent::LeaseExpired) {
                                rec.state = next;
                                rec.lease_token = None;
                                rec.lease_expires_at = None;
                            }
                        }
                    }
                }
                // CreateQueue, BatchUpdate, CohortExpired, PurgeItems handled at control-plane layer.
                _ => {}
            }
        }
        Ok(())
    }

    async fn batch_claim(&self, request: ClaimRequest) -> Result<ClaimResult, ProjectionError> {
        let mut shards = self.shards.lock();
        let proj = shards.get_mut(&request.shard_key).ok_or(ProjectionError::QueueNotFound)?;

        let rules = QueueEligibilityRules {
            metadata_blockers: Default::default(),
            blocked_gate_keys: Default::default(),
        };

        // Collect eligible item IDs sorted by insertion order for stable FIFO claim.
        let mut eligible: Vec<(usize, ItemId)> = proj
            .items
            .values()
            .filter_map(|rec| {
                let snapshot = EligibilitySnapshot {
                    state: rec.state,
                    not_before: rec.not_before.clone(),
                    retry_backoff_until: rec.retry_backoff_until.clone(),
                    metadata: Metadata::default(),
                    gate_keys: vec![],
                };
                evaluate_eligibility(&snapshot, &rules, &request.now).ok()?;
                Some((rec.insertion_order, rec.item_id.clone()))
            })
            .collect();
        eligible.sort_by_key(|(order, _)| *order);
        eligible.truncate(request.max_items);

        let claimed_item_ids: Vec<ItemId> = eligible.into_iter().map(|(_, id)| id).collect();

        for id in &claimed_item_ids {
            if let Some(rec) = proj.items.get_mut(id) {
                rec.state = ItemState::Leased;
                rec.attempts += 1;
                rec.lease_token = Some(request.lease_token.clone());
                rec.lease_expires_at = Some(request.lease_expires_at.clone());
            }
        }

        Ok(ClaimResult { claimed_item_ids, lease_token: request.lease_token })
    }

    async fn metrics(&self, queue: &QueueKey) -> Result<QueueMetricsSnapshot, ProjectionError> {
        let shards = self.shards.lock();
        let mut found = false;
        let mut total = QueueMetricsSnapshot::default();
        for (sk, proj) in shards.iter() {
            if sk.tenant_id == queue.tenant_id && sk.queue_id == queue.queue_id {
                found = true;
                let m = proj.metrics();
                total.pending_count += m.pending_count;
                total.leased_count += m.leased_count;
                total.completed_count += m.completed_count;
                total.failed_count += m.failed_count;
            }
        }
        if found { Ok(total) } else { Err(ProjectionError::QueueNotFound) }
    }
}

// ---------------------------------------------------------------------------
// MemorySnapshotStore
// ---------------------------------------------------------------------------

pub struct MemorySnapshotStore {
    snapshots: Mutex<HashMap<ShardKey, (SnapshotRef, ProjectionSnapshot)>>,
}

impl MemorySnapshotStore {
    pub fn new() -> Self {
        Self { snapshots: Mutex::new(HashMap::new()) }
    }
}

impl Default for MemorySnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotStore for MemorySnapshotStore {
    async fn write_snapshot(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> Result<SnapshotRef, SnapshotError> {
        let ref_id = format!(
            "{}/{}/{}/{}",
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.0,
            position.sequence
        );
        let snapshot_ref = SnapshotRef {
            shard_key: shard.clone(),
            position,
            ref_id,
        };
        self.snapshots.lock().insert(shard.clone(), (snapshot_ref.clone(), snapshot));
        Ok(snapshot_ref)
    }

    async fn latest_snapshot(
        &self,
        shard: &ShardKey,
    ) -> Result<Option<SnapshotRef>, SnapshotError> {
        Ok(self.snapshots.lock().get(shard).map(|(r, _)| r.clone()))
    }

    async fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> Result<ProjectionSnapshot, SnapshotError> {
        self.snapshots
            .lock()
            .get(&snapshot_ref.shard_key)
            .filter(|(r, _)| r == snapshot_ref)
            .map(|(_, s)| s.clone())
            .ok_or(SnapshotError::SnapshotNotFound)
    }
}

// ---------------------------------------------------------------------------
// MemoryControlPlaneStore
// ---------------------------------------------------------------------------

pub struct MemoryControlPlaneStore {
    queues: Mutex<HashMap<QueueKey, QueueEntry>>,
}

struct QueueEntry {
    definition: QueueDefinition,
    shards: Vec<ShardAssignment>,
}

impl MemoryControlPlaneStore {
    pub fn new() -> Self {
        Self { queues: Mutex::new(HashMap::new()) }
    }

    fn make_key(definition: &QueueDefinition) -> QueueKey {
        QueueKey {
            tenant_id: definition.tenant_id.clone(),
            queue_id: definition.queue_id.clone(),
        }
    }
}

impl Default for MemoryControlPlaneStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlPlaneStore for MemoryControlPlaneStore {
    async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> Result<CreateQueueResult, ControlPlaneError> {
        let key = Self::make_key(&definition);
        let mut queues = self.queues.lock();
        if queues.contains_key(&key) {
            return Err(ControlPlaneError::QueueAlreadyExists);
        }
        let shard_count = definition.shard_count;
        let shards = (0..shard_count)
            .map(|i| ShardAssignment {
                shard_key: ShardKey {
                    tenant_id: definition.tenant_id.clone(),
                    queue_id: definition.queue_id.clone(),
                    shard_id: ShardId::new(i),
                },
                epoch: 1,
                worker_id: None,
            })
            .collect();
        let result = CreateQueueResult { created: true, definition: definition.clone() };
        queues.insert(key, QueueEntry { definition, shards });
        Ok(result)
    }

    async fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> Result<QueueDefinition, ControlPlaneError> {
        self.queues
            .lock()
            .get(key)
            .map(|e| e.definition.clone())
            .ok_or(ControlPlaneError::QueueNotFound)
    }

    async fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> Result<Vec<ShardAssignment>, ControlPlaneError> {
        self.queues
            .lock()
            .get(key)
            .map(|e| e.shards.clone())
            .ok_or(ControlPlaneError::QueueNotFound)
    }

    async fn list_queues(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<QueueId>, ControlPlaneError> {
        let queues = self.queues.lock();
        let ids = queues
            .keys()
            .filter(|k| k.tenant_id == *tenant_id)
            .map(|k| k.queue_id.clone())
            .collect();
        Ok(ids)
    }
}
