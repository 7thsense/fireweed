//! Chunked multi-row statement helper, reproduced from
//! `spikes/field-storage/src/sql.rs` so batched writes here use the same
//! param-count chunking (and therefore the same per-statement shape) as the
//! sibling spike.
use turso::{Connection, Value};

/// Conservative per-statement parameter cap. SQLite's historical default
/// `SQLITE_MAX_VARIABLE_NUMBER` is 999; Turso's own limit was not found in
/// the vendored source, so this stays under the lowest common value rather
/// than assume a higher one.
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

/// Same as `exec_chunked`, but returns the total rows changed across every
/// chunk (probe/debug use only — the timed harness paths don't need it).
pub async fn exec_chunked_count(
    conn: &Connection,
    prefix: &str,
    suffix: &str,
    rows: &[Vec<Value>],
) -> turso::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let cols = rows[0].len();
    let rows_per_stmt = (MAX_PARAMS / cols).max(1);
    let placeholder_row = row_placeholders(cols);
    let mut total = 0u64;
    for chunk in rows.chunks(rows_per_stmt) {
        let values_clause = std::iter::repeat(placeholder_row.as_str())
            .take(chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("{prefix}{values_clause}{suffix}");
        let params: Vec<Value> = chunk.iter().flat_map(|r| r.iter().cloned()).collect();
        total += conn.execute(&sql, params).await?;
    }
    Ok(total)
}
