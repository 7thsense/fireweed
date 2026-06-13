use pqueue_core::{ItemId, QueueDefinition, QueueId, TenantId, UtcTimestamp};
use std::fmt;

use crate::commands::CommandEnvelope;
use crate::types::{CommandPosition, QueueKey, ShardKey};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogStoreError {
    StalEpoch { expected: u64, current: u64 },
    ShardNotFound,
    StorageFailure(String),
}

impl fmt::Display for LogStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StalEpoch { expected, current } => {
                write!(f, "stale epoch: expected {}, current {}", expected, current)
            }
            Self::ShardNotFound => write!(f, "shard not found"),
            Self::StorageFailure(msg) => write!(f, "storage failure: {}", msg),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionError {
    QueueNotFound,
    StorageFailure(String),
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueNotFound => write!(f, "queue not found"),
            Self::StorageFailure(msg) => write!(f, "storage failure: {}", msg),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    SnapshotNotFound,
    StorageFailure(String),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotNotFound => write!(f, "snapshot not found"),
            Self::StorageFailure(msg) => write!(f, "storage failure: {}", msg),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneError {
    QueueAlreadyExists,
    QueueNotFound,
    StorageFailure(String),
}

impl fmt::Display for ControlPlaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueAlreadyExists => write!(f, "queue already exists"),
            Self::QueueNotFound => write!(f, "queue not found"),
            Self::StorageFailure(msg) => write!(f, "storage failure: {}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// The durability guarantee offered by a `LogStore`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityProfile {
    /// Commands are not persisted; data is lost on restart.
    None,
    /// Commands are written to local disk; data survives a single node restart.
    LocalDisk,
    /// Commands are replicated to at least two nodes; survives single node loss.
    Replicated,
}

/// Result of a `LogStore::append_batch` call.
#[derive(Debug, Clone)]
pub struct AppendBatchResult {
    /// Position of the last command in the batch.
    pub last_position: CommandPosition,
}

/// A page of commands read from a `LogStore`.
#[derive(Debug, Clone)]
pub struct CommandPage {
    pub commands: Vec<(CommandPosition, CommandEnvelope)>,
    /// Position to use in the next `read_from` call; `None` if at the tail.
    pub next_position: Option<CommandPosition>,
}

/// An active shard assignment record from `ControlPlaneStore`.
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    pub shard_key: ShardKey,
    pub epoch: u64,
    pub worker_id: Option<String>,
}

/// Result of `ControlPlaneStore::create_queue`.
#[derive(Debug, Clone)]
pub struct CreateQueueResult {
    pub created: bool,
    pub definition: QueueDefinition,
}

/// Opaque reference to a stored projection snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotRef {
    pub shard_key: ShardKey,
    pub position: CommandPosition,
    pub ref_id: String,
}

/// Opaque serialized projection snapshot.
#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub payload: Vec<u8>,
}

/// Minimal metrics snapshot from a `ProjectionStore`.
#[derive(Debug, Clone, Default)]
pub struct QueueMetricsSnapshot {
    pub pending_count: u64,
    pub leased_count: u64,
    pub completed_count: u64,
    pub failed_count: u64,
}

/// A simple claim request for the in-memory backend (items + lease parameters).
#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub shard_key: ShardKey,
    pub max_items: usize,
    pub now: UtcTimestamp,
    pub lease_token: String,
    pub lease_expires_at: UtcTimestamp,
}

/// Result of a projection claim.
#[derive(Debug, Clone)]
pub struct ClaimResult {
    pub claimed_item_ids: Vec<ItemId>,
    pub lease_token: String,
}

// ---------------------------------------------------------------------------
// Storage traits (async fn in traits, available in Rust 1.75+)
// ---------------------------------------------------------------------------

/// Durable command log for a single shard.
///
/// The log is the ack boundary: a push/finalize/claim may only return success
/// after `append_batch` returns `Ok`. The `expected_epoch` parameter fences
/// stale workers from appending to a reassigned shard.
pub trait LogStore: Send + Sync {
    fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> impl Future<Output = Result<AppendBatchResult, LogStoreError>> + Send;

    fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> impl Future<Output = Result<CommandPage, LogStoreError>> + Send;

    fn durability_profile(&self) -> DurabilityProfile;
}

/// Query-optimized projection that supports claims and metrics.
///
/// `apply_committed` ingests commands from the log. `batch_claim` runs the
/// Eligibility Precedence algorithm and returns leased items.
pub trait ProjectionStore: Send + Sync {
    fn apply_committed(
        &self,
        position: CommandPosition,
        commands: &[CommandEnvelope],
    ) -> impl Future<Output = Result<(), ProjectionError>> + Send;

    fn batch_claim(
        &self,
        request: ClaimRequest,
    ) -> impl Future<Output = Result<ClaimResult, ProjectionError>> + Send;

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl Future<Output = Result<QueueMetricsSnapshot, ProjectionError>> + Send;
}

/// Checkpoint store for projection snapshots (used to accelerate replay).
pub trait SnapshotStore: Send + Sync {
    fn write_snapshot(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl Future<Output = Result<SnapshotRef, SnapshotError>> + Send;

    fn latest_snapshot(
        &self,
        shard: &ShardKey,
    ) -> impl Future<Output = Result<Option<SnapshotRef>, SnapshotError>> + Send;

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl Future<Output = Result<ProjectionSnapshot, SnapshotError>> + Send;
}

/// Control plane: queue definitions, shard assignments, and backend profiles.
pub trait ControlPlaneStore: Send + Sync {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl Future<Output = Result<CreateQueueResult, ControlPlaneError>> + Send;

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl Future<Output = Result<QueueDefinition, ControlPlaneError>> + Send;

    fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> impl Future<Output = Result<Vec<ShardAssignment>, ControlPlaneError>> + Send;

    fn list_queues(
        &self,
        tenant_id: &TenantId,
    ) -> impl Future<Output = Result<Vec<QueueId>, ControlPlaneError>> + Send;
}

use std::future::Future;
