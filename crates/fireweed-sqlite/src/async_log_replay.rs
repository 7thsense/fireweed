//! Sqlite family factory for the generic async log-replay product.
//!
//! Product ports live once on [`fireweed_engine::AsyncLogReplayBackend`]. Call sites
//! should type against that generic product (or port traits), not a family alias.
//!
//! Durable sqlite axes assemble with **adapter-local** blocking offload
//! ([`fireweed_engine::assemble_async_log_replay_with_axis_offload`]) so rusqlite never
//! runs on a Tokio reactor thread. That is not process-wide `BlockingLibBackend`.

use fireweed_engine::{
    AsyncLogReplayBackend, EngineResult, assemble_async_log_replay_with_concurrent_projection_reads,
};
use fireweed_projection::InMemoryProjection;

use crate::SqliteLog;

/// Assemble an ephemeral `:memory:` async sqlite log-replay backend.
///
/// Log axis offloads (rusqlite); memory projection stays in-process.
pub fn async_composed_sqlite_backend_in_memory()
-> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    from_sqlite_log(SqliteLog::in_memory()?, 0)
}

/// Assemble and recover a durable async sqlite log-replay backend at `path`.
pub fn async_composed_sqlite_backend(
    path: &str,
) -> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    from_sqlite_log(SqliteLog::open(path)?, 0)?.recover()
}

/// Assemble from an already-opened [`SqliteLog`] (caller typically runs recover for durable paths).
///
/// Offloads the sqlite log axis only; pairs with an in-memory projection. The projection axis uses
/// [`InProcessProjectionStore::new_with_concurrent_reads`](fireweed_engine::InProcessProjectionStore::new_with_concurrent_reads)
/// (fireweed-7b74ceac): `InMemoryProjection` is `Send + Sync` and safe for concurrent shared reads, so
/// point reads no longer funnel behind an in-flight commit's exclusive lock (fireweed-451a6b23's
/// mixed-op funnel probe).
pub fn from_sqlite_log(
    log: SqliteLog,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<SqliteLog, InMemoryProjection>> {
    // offload_log=true: rusqlite whole-ops on private bounded executor.
    // offload_projection is implicit "false": InMemoryProjection is CPU-only ready-future safe.
    assemble_async_log_replay_with_concurrent_projection_reads(
        log,
        InMemoryProjection::new(),
        node_id,
        true,
    )
}
