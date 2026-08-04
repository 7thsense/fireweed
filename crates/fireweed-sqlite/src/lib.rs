#![forbid(unsafe_code)]
//! # fireweed-sqlite
//!
//! Driven adapter (atomic durability class): a durable sqlite command **LOG** ([`SqliteLog`]) composed
//! with a projection axis by the one generic [`fireweed_engine::ComposedBackend`] (ADR-012). The log rows are
//! the source of truth (CQRS); the projection is a derived view that any reopen reconstructs by replaying
//! the durable log. All apply/eligibility/lease/metrics logic is shared with every other backend (no
//! re-implementation) via [`fireweed_projection`]; this crate owns only the durable sqlite persistence axes:
//! the [`SqliteLog`] command-log and the derived relational [`SqliteProjectionStore`].

mod async_log_replay;
mod async_projection;
mod compose_log;
mod relational;
pub use async_log_replay::{
    async_composed_sqlite_backend, async_composed_sqlite_backend_in_memory, from_sqlite_log,
};
pub use async_projection::{AsyncSqliteProjectionStore, DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY};
pub use compose_log::SqliteLog;
pub use fireweed_projection::InMemoryProjection;
pub use relational::{
    BackpressureLevel, CheckpointLineage, CheckpointProgress, DEFAULT_DEFERRED_FLUSH_CHUNK,
    HybridAsyncDebt, HybridAsyncMetrics, HybridAsyncMonitor, HybridAsyncThresholds,
    HybridFaultCutPoint, HybridFaultHook, HybridProjectionStore, SqliteCheckpointStore,
    SqliteProjectionStore, SqliteRelational, SqliteRelationalBackend, WalCheckpointStats,
    composed_sqlite_relational, composed_sqlite_relational_in_memory,
};

// Neutral compatibility names used while the legacy server selectors still consume the retired
// hot-memory-plus-SQLite implementation. New composition paths use `AsyncSqliteProjectionStore`.
pub type AsyncProjectionDebt = HybridAsyncDebt;
pub type AsyncProjectionMetrics = HybridAsyncMetrics;
pub type AsyncProjectionMonitor = HybridAsyncMonitor;
pub type AsyncProjectionThresholds = HybridAsyncThresholds;
pub type AsyncProjectionFaultCutPoint = HybridFaultCutPoint;
pub use HybridFaultHook as AsyncProjectionFaultHook;
pub type LegacySqliteProjectionStore = HybridProjectionStore;

use fireweed_engine::{
    AsyncLogReplayBackend, EngineResult, assemble_async_log_replay_with_axis_offload,
};

/// Assemble a composed sqlite backend over an ephemeral `:memory:` durable log.
pub fn composed_sqlite_backend_in_memory()
-> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    async_composed_sqlite_backend_in_memory()
}

/// Assemble a composed sqlite backend over a DURABLE sqlite command log at `path` — the composition root
/// wires this. Runs recovery-on-open (ADR-012 P2): a reopen rebuilds the in-memory projection by replaying
/// the durable log.
///
/// Adapter-local log offload only (no process-wide BlockingLibBackend).
pub fn composed_sqlite_backend(
    path: &str,
) -> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    async_composed_sqlite_backend(path)
}

/// Assemble and recover exactly one fixed-pool worker partition.
///
/// Worker partitioning for async composition currently opens a full recovered backend (affinity is
/// applied at the composition-root pool layer). Partitioned recovery optimization remains a follow-on.
pub fn composed_sqlite_backend_for_worker(
    path: &str,
    _index: usize,
    _partitions: usize,
) -> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    async_composed_sqlite_backend(path)
}

/// Assemble a composed sqlite-LOG + sqlite-PROJECTION backend over ephemeral `:memory:` stores.
///
/// Product ports live on [`AsyncLogReplayBackend`] (ADR-012 / async log-replay product).
/// Both axes offload rusqlite whole-ops on private bounded executors.
pub fn composed_sqlite_log_sqlite_projection_in_memory()
-> EngineResult<AsyncLogReplayBackend<SqliteLog, SqliteProjectionStore>> {
    assemble_async_log_replay_with_axis_offload(
        SqliteLog::in_memory()?,
        SqliteProjectionStore::in_memory()?,
        0,
        true,
        true,
    )
}

/// Assemble a composed sqlite-LOG + sqlite-PROJECTION backend over DURABLE stores (the log at `log_path`,
/// the derived projection at `projection_path`). Runs recovery-on-open (ADR-012 P2): a reopen replays only
/// the durable log tail beyond the persisted projection high-water (snapshot-tail recovery).
///
/// Both axes use adapter-local blocking offload (not process-wide BlockingLibBackend).
pub fn composed_sqlite_log_sqlite_projection(
    log_path: &str,
    projection_path: &str,
) -> EngineResult<AsyncLogReplayBackend<SqliteLog, SqliteProjectionStore>> {
    assemble_async_log_replay_with_axis_offload(
        SqliteLog::open(log_path)?,
        SqliteProjectionStore::open(projection_path)?,
        0,
        true,
        true,
    )?
    .recover()
}
