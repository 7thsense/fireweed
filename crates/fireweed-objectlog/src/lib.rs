#![forbid(unsafe_code)]
//! # fireweed-objectlog
//!
//! Object-log product backends over crates.io [`object_log::LogEngine`] (program A).
//!
//! - [`ObjectLogEngineStore`] — native-async log axis
//! - [`AsyncObjectLogMemoryBackend`] / [`AsyncObjectLogSqliteBackend`] /
//!   [`AsyncObjectLogHybridBackend`] — log × projection products
//! - [`composed_objectlog_backend`] — sync-open convenience for tests/embedders

mod async_product;
mod async_product_hybrid;
mod async_product_sqlite;
pub mod commit_surface;
pub mod compose_log;
mod log_engine_store;
pub mod maintenance;
pub mod object_store_observability;
mod port_surface;
mod recovery_stats;
mod segment_config;
pub mod storage_generation;

pub use async_product::{AsyncObjectLogMemoryBackend, SeqIdGen, composed_objectlog_memory_async};
pub use async_product_hybrid::{AsyncObjectLogHybridBackend, HybridProductConfig};
pub use async_product_sqlite::AsyncObjectLogSqliteBackend;
pub use recovery_stats::{
    RECOVERY_COMMAND_PAGE_LIMIT, RECOVERY_MANIFEST_OBJECT_PAGE_LIMIT, RecoveryStats,
    RecoveryStatsMap, replay_log_into_projection,
};
pub use commit_surface::{
    CommitIdempotency, PreparedCommitTransition, durability_for_strict,
    eventual_commit_capabilities, explain_commit_if_authoritative,
    finish_prepared_commit_transition, map_submit_error, new_commit_idempotency, outcomes_of,
    prepare_commit_transition, record_commit_idempotency, side_record, strict_commit_capabilities,
};
pub use compose_log::{
    ComposedObjectLogBackend, ObjectLogTaskDispatcher, block_on_objectlog,
    block_on_objectlog_future, composed_objectlog_backend, composed_objectlog_backend_group_commit,
    open_object_log_engine_local, open_object_log_engine_local_sync, open_object_log_engine_s3,
    open_object_log_engine_s3_sync,
};
pub use log_engine_store::{ObjectLogEngineStore, flush_config_from_segment};
pub use object_log::{BlobStore, FlushConfig, S3BlobStore};
pub use segment_config::{MAX_SEGMENT_BYTES, SegmentConfig};
pub use storage_generation::{
    FWSG_SEGMENT_MAGIC, INCOMPATIBLE_OBJECT_LOG_GENERATION, MIXED_OBJECT_LOG_GENERATION,
    STORAGE_GENERATION_DOC, is_incompatible_generation_error,
};

/// Compatibility alias for pre-cutover `fireweed_objectlog::segmented::*` imports.
/// The in-tree segmented FWSG substrate was replaced by crates.io `object_log`.
pub mod segmented {
    pub use object_log::{BlobStore, S3BlobStore};
}
