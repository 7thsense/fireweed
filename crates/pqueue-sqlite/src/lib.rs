#![forbid(unsafe_code)]
//! # pqueue-sqlite
//!
//! Driven adapter (atomic durability class): a durable sqlite command **LOG** ([`SqliteLog`]) composed
//! with a projection axis by the one generic [`pqueue_engine::ComposedBackend`] (ADR-012). The log rows are
//! the source of truth (CQRS); the projection is a derived view that any reopen reconstructs by replaying
//! the durable log. All apply/eligibility/lease/metrics logic is shared with every other backend (no
//! re-implementation) via [`pqueue_projection`]; this crate owns only the durable sqlite persistence axes:
//! the [`SqliteLog`] command-log and the derived relational [`SqliteProjectionStore`].

mod compose_log;
mod relational;
pub use compose_log::SqliteLog;
pub use relational::{
    BackpressureLevel, CheckpointLineage, CheckpointProgress, ComposedSqliteRelationalBackend,
    HybridAsyncDebt, HybridAsyncMetrics, HybridAsyncMonitor, HybridAsyncThresholds,
    HybridProjectionStore, SqliteCheckpointStore, SqliteProjectionStore, SqliteRelational,
    SqliteRelationalBackend, WalCheckpointStats, composed_sqlite_relational,
    composed_sqlite_relational_in_memory,
};

use pqueue_engine::{ComposedBackend, EngineResult, InProcessControlPlane};
use pqueue_projection::InMemoryProjection;

/// The composed sqlite backend (ADR-012): the durable sqlite command LOG re-expressed as the orthogonal
/// product `SqliteLog × InMemoryProjection × InProcessControlPlane`, assembled by the one generic
/// `ComposedBackend`. The in-memory log-replay family: a durable command log + an in-memory projection.
pub type ComposedSqliteBackend =
    ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>;

/// Assemble a composed sqlite backend over an ephemeral `:memory:` durable log.
pub fn composed_sqlite_backend_in_memory() -> EngineResult<ComposedSqliteBackend> {
    Ok(ComposedBackend::new(
        SqliteLog::in_memory()?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    ))
}

/// Assemble a composed sqlite backend over a DURABLE sqlite command log at `path` — the composition root
/// wires this. Runs recovery-on-open (ADR-012 P2): a reopen rebuilds the in-memory projection by replaying
/// the durable log.
pub fn composed_sqlite_backend(path: &str) -> EngineResult<ComposedSqliteBackend> {
    ComposedBackend::new(
        SqliteLog::open(path)?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    )
    .recover()
}

/// The composed sqlite-LOG + sqlite-PROJECTION backend (ADR-012 P1b-ii, Part B): a durable sqlite command
/// LOG ([`SqliteLog`]) paired with the DERIVED relational SQL projection ([`SqliteProjectionStore`]) instead
/// of the in-memory projection. Atomic durability class (the log axis), so it runs the full
/// `core_suite!(@atomic)` — the projection family that stubs secondary indexes.
pub type ComposedSqliteLogSqliteProjectionBackend =
    ComposedBackend<SqliteLog, SqliteProjectionStore, InProcessControlPlane>;

/// Assemble a composed sqlite-LOG + sqlite-PROJECTION backend over ephemeral `:memory:` stores.
pub fn composed_sqlite_log_sqlite_projection_in_memory()
-> EngineResult<ComposedSqliteLogSqliteProjectionBackend> {
    Ok(ComposedBackend::new(
        SqliteLog::in_memory()?,
        SqliteProjectionStore::in_memory()?,
        InProcessControlPlane::new(),
    ))
}

/// Assemble a composed sqlite-LOG + sqlite-PROJECTION backend over DURABLE stores (the log at `log_path`,
/// the derived projection at `projection_path`). Runs recovery-on-open (ADR-012 P2): a reopen replays only
/// the durable log tail beyond the persisted projection high-water (snapshot-tail recovery).
pub fn composed_sqlite_log_sqlite_projection(
    log_path: &str,
    projection_path: &str,
) -> EngineResult<ComposedSqliteLogSqliteProjectionBackend> {
    ComposedBackend::new(
        SqliteLog::open(log_path)?,
        SqliteProjectionStore::open(projection_path)?,
        InProcessControlPlane::new(),
    )
    .recover()
}
