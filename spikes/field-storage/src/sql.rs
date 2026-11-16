//! Chunked multi-row statement helper shared by every model. All batched
//! writes (`INSERT ... VALUES`, the `UPDATE ... FROM (VALUES ...)` set-based
//! patch, and the `INSERT ... ON CONFLICT DO UPDATE` upserts for EAV and the
//! index side table) go through this so param-count chunking is identical
//! across models and cannot bias the comparison.
use turso::{Connection, Value};

/// Conservative per-statement parameter cap. SQLite's historical default
/// `SQLITE_MAX_VARIABLE_NUMBER` is 999; Turso's own limit was not found in
/// the vendored source (grepped, no hit), so this stays under the
/// lowest common value rather than assume a higher one.
pub const MAX_PARAMS: usize = 900;

fn row_placeholders(n_cols: usize) -> String {
    let mut s = String::with_capacity(n_cols * 2 + 2);
    s.push('(');
    for i in 0..n_cols {
        if i > 0 {
            s.push(',');
        }
        s.push('?');
    }
    s.push(')');
    s
}

/// Execute `{prefix}{VALUES (...),(...)}{suffix}` in chunks of at most
/// `MAX_PARAMS` bound parameters, all against `conn` (caller controls the
/// surrounding transaction). `rows` is row-major; every row must have the
/// same width.
pub async fn exec_chunked(
    conn: &Connection,
    prefix: &str,
    suffix: &str,
    rows: &[Vec<Value>],
) -> turso::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let cols = rows[0].len();
    let rows_per_stmt = (MAX_PARAMS / cols).max(1);
    let placeholder_row = row_placeholders(cols);
    for chunk in rows.chunks(rows_per_stmt) {
        let values_clause = std::iter::repeat(placeholder_row.as_str())
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("{prefix}{values_clause}{suffix}");
        let params: Vec<Value> = chunk.iter().flat_map(|r| r.iter().cloned()).collect();
        conn.execute(&sql, params).await?;
    }
    Ok(())
}

/// Batched `SELECT {select_cols} FROM {table} WHERE {key_col} IN (...)`,
/// chunked the same way. Returns raw rows (caller decodes).
pub async fn select_in_chunked(
    conn: &Connection,
    table: &str,
    key_col: &str,
    select_cols: &str,
    keys: &[i64],
) -> turso::Result<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(MAX_PARAMS) {
        let placeholders = std::iter::repeat("?")
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT {select_cols} FROM {table} WHERE {key_col} IN ({placeholders})");
        let params: Vec<Value> = chunk.iter().map(|k| Value::Integer(*k)).collect();
        let mut rows = conn.query(&sql, params).await?;
        while let Some(row) = rows.next().await? {
            out.push(row.into_values().collect());
        }
    }
    Ok(out)
}
