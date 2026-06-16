#![forbid(unsafe_code)]

pub mod commands;
pub mod concurrency_registry;
pub mod fault_injection;
pub mod memory;
pub mod multi_shard;
pub mod traits;
pub mod types;

pub use commands::{CommandEnvelope, CommandId, QueueCommand};
pub use traits::{
    AppendBatchResult, ClaimRequest, ClaimResult, CommandPage, ControlPlaneError,
    ControlPlaneStore, DurabilityProfile, LogStore, LogStoreError, ProjectionError,
    ProjectionStore, ShardAssignment, SnapshotError, SnapshotStore,
};
pub use types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }
}
