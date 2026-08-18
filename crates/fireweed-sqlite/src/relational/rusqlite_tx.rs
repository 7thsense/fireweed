//! rusqlite RelTx adapter — the SQLite driver for the shared apply/query surface.

use fireweed_engine::EngineResult;
use fireweed_relational::{RelRow, RelTx, RelValue};
use rusqlite::params_from_iter;
use rusqlite::types::Value as SqlValue;

use super::st;

pub fn to_rusqlite(value: &RelValue) -> SqlValue {
    match value {
        RelValue::Null => SqlValue::Null,
        RelValue::Integer(value) => SqlValue::Integer(*value),
        RelValue::Real(value) => SqlValue::Real(*value),
        RelValue::Text(value) => SqlValue::Text(value.clone()),
        RelValue::Blob(value) => SqlValue::Blob(value.clone()),
    }
}

pub fn from_rusqlite(value: SqlValue) -> RelValue {
    match value {
        SqlValue::Null => RelValue::Null,
        SqlValue::Integer(value) => RelValue::Integer(value),
        SqlValue::Real(value) => RelValue::Real(value),
        SqlValue::Text(value) => RelValue::Text(value),
        SqlValue::Blob(value) => RelValue::Blob(value),
    }
}

/// Newtype so RelTx can be implemented without violating the orphan rule.
pub struct SqliteRel<'a>(pub &'a rusqlite::Connection);

impl RelTx for SqliteRel<'_> {
    fn execute(&self, sql: &str, params: &[RelValue]) -> EngineResult<usize> {
        let converted: Vec<SqlValue> = params.iter().map(to_rusqlite).collect();
        let mut stmt = st(self.0.prepare_cached(sql))?;
        st(stmt.execute(params_from_iter(converted.iter())))
    }

    fn query(&self, sql: &str, params: &[RelValue]) -> EngineResult<Vec<RelRow>> {
        let converted: Vec<SqlValue> = params.iter().map(to_rusqlite).collect();
        let mut stmt = st(self.0.prepare_cached(sql))?;
        let column_count = stmt.column_count();
        let mapped = st(stmt.query_map(params_from_iter(converted.iter()), |row| {
            let mut values = Vec::with_capacity(column_count);
            for index in 0..column_count {
                values.push(from_rusqlite(row.get::<_, SqlValue>(index)?));
            }
            Ok(RelRow(values))
        }))?;
        let mut rows = Vec::new();
        for row in mapped {
            rows.push(st(row)?);
        }
        Ok(rows)
    }
}
