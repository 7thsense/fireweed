#![forbid(unsafe_code)]

pub mod commands;
pub mod memory;
pub mod traits;
pub mod types;

pub use commands::{CommandEnvelope, CommandId, QueueCommand};
pub use traits::{
    AppendBatchResult, CommandPage, ControlPlaneError, ControlPlaneStore, DurabilityProfile,
    LogStore, LogStoreError, ProjectionError, ProjectionStore, ShardAssignment, SnapshotError,
    SnapshotStore,
};
pub use types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};

pub mod scaffold {
    pub fn core_name() -> &'static str {
        pqueue_core::scaffold::name()
    }
}
