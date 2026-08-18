//! Driver-neutral SQL executor used by the shared SQLite-family apply/query surface.

use fireweed_engine::{EngineError, EngineResult};

use crate::{RelRow, RelValue};

/// One transactional SQL session. SQLite and Turso implement this; apply and query live here once.
pub trait RelTx {
    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize>;
    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>>;
}

pub fn rel_exec(tx: &impl RelTx, sql: &str, params: impl AsRef<[RelValue]>) -> EngineResult<usize> {
    tx.execute(sql, params.as_ref())
}

pub fn rel_query(
    tx: &impl RelTx,
    sql: &str,
    params: impl AsRef<[RelValue]>,
) -> EngineResult<Vec<RelRow>> {
    tx.query(sql, params.as_ref())
}

pub fn query_row<T>(
    tx: &impl RelTx,
    sql: &str,
    params: impl AsRef<[RelValue]>,
    map: impl FnOnce(&RelRow) -> EngineResult<T>,
) -> EngineResult<T> {
    let rows = tx.query(sql, params.as_ref())?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| EngineError::Storage("query returned no rows".to_string()))?;
    map(&row)
}

pub fn query_optional<T>(
    tx: &impl RelTx,
    sql: &str,
    params: impl AsRef<[RelValue]>,
    map: impl FnOnce(&RelRow) -> EngineResult<T>,
) -> EngineResult<Option<T>> {
    Ok(match tx.query(sql, params.as_ref())?.into_iter().next() {
        Some(row) => Some(map(&row)?),
        None => None,
    })
}
