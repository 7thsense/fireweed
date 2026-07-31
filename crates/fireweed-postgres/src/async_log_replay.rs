//! Postgres family factory for the generic async log-replay product.
//!
//! Product ports live once on [`fireweed_engine::AsyncLogReplayBackend`]. Call sites
//! should type against that generic product (or port traits), not a family alias.

use fireweed_engine::{AsyncLogReplayBackend, EngineResult, assemble_async_log_replay};
use fireweed_projection::InMemoryProjection;

use crate::PostgresLog;
use crate::connect::PostgresConnectConfig;

/// Assemble and recover the composed postgres log-replay backend over `url`.
pub fn async_composed_postgres_backend(
    url: &str,
) -> EngineResult<AsyncLogReplayBackend<PostgresLog, InMemoryProjection>> {
    from_postgres_log(PostgresLog::connect(url)?, 0)?.recover()
}

/// Assemble and recover from a fully-built connect config (Lakebase-aware path).
pub fn async_composed_postgres_backend_with_config(
    config: PostgresConnectConfig,
) -> EngineResult<AsyncLogReplayBackend<PostgresLog, InMemoryProjection>> {
    from_postgres_log(PostgresLog::connect_with_config(config)?, 0)?.recover()
}

/// Assemble and recover isolated in `schema` (test/reopen path).
pub fn async_composed_postgres_backend_in_schema(
    url: &str,
    schema: &str,
) -> EngineResult<AsyncLogReplayBackend<PostgresLog, InMemoryProjection>> {
    from_postgres_log(PostgresLog::connect_in_schema(url, schema)?, 0)?.recover()
}

/// Assemble from an already-opened [`PostgresLog`] (caller typically runs recover).
pub fn from_postgres_log(
    log: PostgresLog,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<PostgresLog, InMemoryProjection>> {
    assemble_async_log_replay(log, InMemoryProjection::new(), node_id)
}
