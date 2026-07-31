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
pub mod compose_log;
mod log_engine_store;
pub mod maintenance;
pub mod object_store_observability;
mod segment_config;

pub use async_product::{
    AsyncObjectLogMemoryBackend, SeqIdGen, composed_objectlog_memory_async,
};
pub use async_product_hybrid::{AsyncObjectLogHybridBackend, HybridProductConfig};
pub use async_product_sqlite::AsyncObjectLogSqliteBackend;
pub use compose_log::{
    ComposedObjectLogBackend, block_on_objectlog, composed_objectlog_backend,
    composed_objectlog_backend_group_commit, open_object_log_engine_local,
    open_object_log_engine_local_sync, open_object_log_engine_s3, open_object_log_engine_s3_sync,
};
pub use log_engine_store::{ObjectLogEngineStore, flush_config_from_segment};
pub use object_log::FlushConfig;
pub use segment_config::{MAX_SEGMENT_BYTES, SegmentConfig};
