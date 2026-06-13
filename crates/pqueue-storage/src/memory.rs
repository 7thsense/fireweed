//! In-memory reference implementation of all storage traits.
//!
//! This backend provides no durability (`DurabilityProfile::None`). It is the
//! target for the `storage_conformance` test suite and serves as the reference
//! implementation for backend contract validation.

use parking_lot::Mutex;
use pqueue_core::{QueueDefinition, QueueId, TenantId};
use std::collections::HashMap;

use crate::commands::CommandEnvelope;
use crate::traits::{
    AppendBatchResult, CommandPage, ControlPlaneError, ControlPlaneStore, CreateQueueResult,
    DurabilityProfile, LogStore, LogStoreError, ProjectionError, ProjectionStore,
    QueueMetricsSnapshot, ShardAssignment, SnapshotError, SnapshotRef, SnapshotStore,
    ProjectionSnapshot,
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

#[derive(Default)]
struct QueueProjection {
    metrics: QueueMetricsSnapshot,
}

pub struct MemoryProjectionStore {
    queues: Mutex<HashMap<QueueKey, QueueProjection>>,
}

impl MemoryProjectionStore {
    pub fn new() -> Self {
        Self { queues: Mutex::new(HashMap::new()) }
    }
}

impl Default for MemoryProjectionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionStore for MemoryProjectionStore {
    async fn apply_committed(
        &self,
        _position: CommandPosition,
        _commands: &[CommandEnvelope],
    ) -> Result<(), ProjectionError> {
        Ok(())
    }

    async fn metrics(&self, queue: &QueueKey) -> Result<QueueMetricsSnapshot, ProjectionError> {
        let queues = self.queues.lock();
        let proj = queues.get(queue).ok_or(ProjectionError::QueueNotFound)?;
        Ok(proj.metrics.clone())
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
