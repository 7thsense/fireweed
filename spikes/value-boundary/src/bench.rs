//! The two storage variants under comparison (inline vs. off-row) plus the
//! ingest/update/read/claim-walk measurements run against each. Every op is
//! wall-clock bounded so a slow config degrades to "fewer rows/ops
//! measured", never "runs past the time budget" (pattern from
//! `spikes/field-storage/src/bench.rs`).
use std::path::Path;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::SeedableRng;
use turso::{Connection, Value as SqlValue};

use crate::db::on_disk_size;
use crate::gen::ValueTemplate;
use crate::sql::exec_chunked;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    /// One `items` row holds both `small` (always present, always the same
    /// size) and `large` (the field under test), inline in the same B-tree
    /// record.
    Inline,
    /// `items` holds only `small`; the large value lives in `values_tbl
    /// (item_no, field_no)`, off the hot row. The hot row's own `item_no`
    /// is the reference (PK equality), the same design
    /// `fireweed_item_payloads` already uses
    /// (`crates/fireweed-relational/src/schema.rs:55-57` — no separate
    /// pointer column).
    OffRow,
}

impl Variant {
    pub fn short(self) -> &'static str {
        match self {
            Variant::Inline => "inline",
            Variant::OffRow => "offrow",
        }
    }
}

/// The single off-row field's fixed `field_no`. This spike tests one large
/// field per record, so the `(item_no, field_no)` schema the task specified
/// always keys on the same `field_no = 0`.
const LARGE_FIELD_NO: i64 = 0;

pub async fn create_schema(conn: &Connection, variant: Variant) -> turso::Result<()> {
    match variant {
        Variant::Inline => {
            conn.execute(
                "CREATE TABLE items (item_no INTEGER PRIMARY KEY, small BLOB NOT NULL, large BLOB NOT NULL)",
                (),
            )
            .await?;
        }
        Variant::OffRow => {
            conn.execute(
                "CREATE TABLE items (item_no INTEGER PRIMARY KEY, small BLOB NOT NULL)",
                (),
            )
            .await?;
            conn.execute(
                "CREATE TABLE values_tbl (item_no INTEGER NOT NULL, field_no INTEGER NOT NULL, \
                 value BLOB NOT NULL, PRIMARY KEY (item_no, field_no))",
                (),
            )
            .await?;
        }
    }
    // `rank`: the owner's round-3 answer to "why not just rank?" (int64(now)
    // when unspecified gives insertion order) — a mandatory ordering field on
    // every record, not an optional secondary index, so it is populated for
    // every ingested row below, not just on request.
    conn.execute(
        "CREATE TABLE item_index (item_no INTEGER PRIMARY KEY, rank INTEGER NOT NULL)",
        (),
    )
    .await?;
    conn.execute(
        "CREATE INDEX item_index_rank_idx ON item_index (rank, item_no)",
        (),
    )
    .await?;
    Ok(())
}

/// Touches every row in `pool` once (full row: `small` + the large value),
/// untimed, before any timed op runs against that pool. Without this, the
/// *first* op measured after `run_ingest`'s closing checkpoint pays a
/// one-time cold-cache tax (pages evicted/invalidated by
/// `PRAGMA wal_checkpoint(TRUNCATE)`) that every later op against the same
/// connection does not, which would bias op-to-op comparisons by
/// measurement order rather than by storage variant. `pool` is small enough
/// (1,000 rows) that it fits well inside the 128 MiB page cache even at
/// 64 KiB values (~65 MiB), so this is a one-time cost, not a per-op one.
pub async fn warm_pool(conn: &Connection, variant: Variant, pool: &[i64]) -> turso::Result<()> {
    match variant {
        Variant::Inline => {
            let mut stmt = conn.prepare("SELECT small, large FROM items WHERE item_no = ?").await?;
            for &id in pool {
                let mut rows = stmt.query([id]).await?;
                let row = rows.next().await?.expect("row must exist");
                let _small: Vec<u8> = row.get(0)?;
                let _large: Vec<u8> = row.get(1)?;
            }
        }
        Variant::OffRow => {
            let mut row_stmt = conn.prepare("SELECT small FROM items WHERE item_no = ?").await?;
            let mut val_stmt = conn
                .prepare("SELECT value FROM values_tbl WHERE item_no = ? AND field_no = ?")
                .await?;
            for &id in pool {
                let mut rows = row_stmt.query([id]).await?;
                let row = rows.next().await?.expect("row must exist");
                let _small: Vec<u8> = row.get(0)?;
                let mut vrows = val_stmt.query((id, LARGE_FIELD_NO)).await?;
                let vrow = vrows.next().await?.expect("value row must exist");
                let _large: Vec<u8> = vrow.get(0)?;
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------- ingest

#[derive(Debug, Clone)]
pub struct IngestResult {
    pub rows: u64,
    pub elapsed: Duration,
    pub cut_off: bool,
    pub db_bytes: u64,
}

impl IngestResult {
    pub fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64()
    }
    pub fn bytes_per_row(&self) -> f64 {
        self.db_bytes as f64 / self.rows.max(1) as f64
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_ingest(
    variant: Variant,
    large_size: usize,
    small_size: usize,
    target_rows: u64,
    batch_size: u64,
    db_path: &Path,
    page_size: u32,
    seed: u64,
    cutoff: Duration,
) -> turso::Result<IngestResult> {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
    let (_db, conn) = crate::db::open_db(db_path, page_size).await?;
    create_schema(&conn, variant).await?;

    let mut rng = StdRng::seed_from_u64(seed);
    let small_tpl = ValueTemplate::new(&mut rng, small_size);
    let large_tpl = ValueTemplate::new(&mut rng, large_size);

    let start = Instant::now();
    let mut rows_done = 0u64;
    let mut cut_off = false;
    let mut next_id = 0i64;
    while rows_done < target_rows {
        let n = batch_size.min(target_rows - rows_done);
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        match variant {
            Variant::Inline => {
                let rows: Vec<Vec<SqlValue>> = (0..n)
                    .map(|i| {
                        let id = next_id + i as i64;
                        vec![
                            SqlValue::Integer(id),
                            SqlValue::Blob(small_tpl.value_for(id, 0)),
                            SqlValue::Blob(large_tpl.value_for(id, 0)),
                        ]
                    })
                    .collect();
                exec_chunked(
                    &conn,
                    "INSERT INTO items (item_no, small, large) VALUES ",
                    "",
                    &rows,
                )
                .await?;
            }
            Variant::OffRow => {
                let item_rows: Vec<Vec<SqlValue>> = (0..n)
                    .map(|i| {
                        let id = next_id + i as i64;
                        vec![SqlValue::Integer(id), SqlValue::Blob(small_tpl.value_for(id, 0))]
                    })
                    .collect();
                exec_chunked(
                    &conn,
                    "INSERT INTO items (item_no, small) VALUES ",
                    "",
                    &item_rows,
                )
                .await?;
                let value_rows: Vec<Vec<SqlValue>> = (0..n)
                    .map(|i| {
                        let id = next_id + i as i64;
                        vec![
                            SqlValue::Integer(id),
                            SqlValue::Integer(LARGE_FIELD_NO),
                            SqlValue::Blob(large_tpl.value_for(id, 0)),
                        ]
                    })
                    .collect();
                exec_chunked(
                    &conn,
                    "INSERT INTO values_tbl (item_no, field_no, value) VALUES ",
                    "",
                    &value_rows,
                )
                .await?;
            }
        }
        let index_rows: Vec<Vec<SqlValue>> = (0..n)
            .map(|i| {
                let id = next_id + i as i64;
                vec![SqlValue::Integer(id), SqlValue::Integer(id)]
            })
            .collect();
        exec_chunked(
            &conn,
            "INSERT INTO item_index (item_no, rank) VALUES ",
            "",
            &index_rows,
        )
        .await?;
        conn.execute("COMMIT", ()).await?;

        next_id += n as i64;
        rows_done += n;
        if start.elapsed() > cutoff {
            cut_off = true;
            break;
        }
    }
    let elapsed = start.elapsed();
    let db_bytes = on_disk_size(&conn, db_path).await?;
    Ok(IngestResult {
        rows: rows_done,
        elapsed,
        cut_off,
        db_bytes,
    })
}

// --------------------------------------------------------------- updates

#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub updates: u64,
    pub elapsed: Duration,
    pub cut_off: bool,
}

impl UpdateResult {
    pub fn updates_per_sec(&self) -> f64 {
        self.updates as f64 / self.elapsed.as_secs_f64()
    }
}

/// Repeatedly overwrites `small` on the same `pool` of rows, `rounds` times.
/// Never touches `large`/`values_tbl` — this isolates whether an inline
/// large value slows down updates of a *different*, small field on the same
/// hot row (SQLite/Turso rewrite one whole serialized row per UPDATE,
/// regardless of which columns are named in SET).
pub async fn run_update_small(
    conn: &Connection,
    pool: &[i64],
    small_size: usize,
    rounds: u64,
    seed: u64,
    cutoff: Duration,
) -> turso::Result<UpdateResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    let tpl = ValueTemplate::new(&mut rng, small_size);
    let start = Instant::now();
    let mut updates_done = 0u64;
    let mut cut_off = false;
    for round in 0..rounds {
        let rows: Vec<Vec<SqlValue>> = pool
            .iter()
            .map(|&id| vec![SqlValue::Integer(id), SqlValue::Blob(tpl.value_for(id, round))])
            .collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        exec_chunked(
            conn,
            "UPDATE items SET small = v.column2 FROM (VALUES ",
            ") AS v WHERE items.item_no = v.column1",
            &rows,
        )
        .await?;
        conn.execute("COMMIT", ()).await?;
        updates_done += rows.len() as u64;
        if start.elapsed() > cutoff {
            cut_off = true;
            break;
        }
    }
    Ok(UpdateResult {
        updates: updates_done,
        elapsed: start.elapsed(),
        cut_off,
    })
}

/// Repeatedly overwrites the large field itself (`items.large` inline,
/// `values_tbl.value` off-row) on the same `pool` of rows, `rounds` times.
pub async fn run_update_large(
    conn: &Connection,
    variant: Variant,
    pool: &[i64],
    large_size: usize,
    rounds: u64,
    seed: u64,
    cutoff: Duration,
) -> turso::Result<UpdateResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    let tpl = ValueTemplate::new(&mut rng, large_size);
    let start = Instant::now();
    let mut updates_done = 0u64;
    let mut cut_off = false;
    for round in 0..rounds {
        let rows: Vec<Vec<SqlValue>> = pool
            .iter()
            .map(|&id| vec![SqlValue::Integer(id), SqlValue::Blob(tpl.value_for(id, round))])
            .collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        match variant {
            Variant::Inline => {
                exec_chunked(
                    conn,
                    "UPDATE items SET large = v.column2 FROM (VALUES ",
                    ") AS v WHERE items.item_no = v.column1",
                    &rows,
                )
                .await?;
            }
            Variant::OffRow => {
                // NOT `UPDATE ... FROM (VALUES ...)` here: probed
                // (`SPIKE_UPDATE_PROBE`) and confirmed via `EXPLAIN QUERY
                // PLAN` that Turso's planner picks `SCAN values_tbl USING
                // COVERING INDEX ... / SCAN v` — an O(pool × table) nested
                // loop — for that join form against `values_tbl`, because
                // its PK is the composite `(item_no, field_no)`, not the
                // rowid (contrast `items`, an `INTEGER PRIMARY KEY` table,
                // which gets the efficient `SCAN v` / `SEARCH ... rowid=?`
                // plan for the same join shape). A plain per-row equality
                // `UPDATE ... WHERE item_no = ? AND field_no = ?` uses the
                // table's own `(item_no, field_no)` index directly
                // (confirmed: `SEARCH values_tbl USING INDEX
                // sqlite_autoindex_values_tbl_1 (item_no=? AND field_no=?)`)
                // and measured ~100x faster at 20,000 background rows
                // (243.6 vs 25,937.9 updates/s, single txn, pool=200) — see
                // the results doc's "off-row UPDATE...FROM planner gotcha"
                // finding. One `execute` per row, all inside the round's one
                // transaction, is the fair (and the one a real
                // implementation should actually use) measurement.
                for row in &rows {
                    let id = match &row[0] {
                        SqlValue::Integer(i) => *i,
                        other => panic!("expected item_no INTEGER, got {other:?}"),
                    };
                    let value = row[1].clone();
                    conn.execute(
                        "UPDATE values_tbl SET value = ? WHERE item_no = ? AND field_no = ?",
                        (value, id, LARGE_FIELD_NO),
                    )
                    .await?;
                }
            }
        }
        conn.execute("COMMIT", ()).await?;
        updates_done += rows.len() as u64;
        if start.elapsed() > cutoff {
            cut_off = true;
            break;
        }
    }
    Ok(UpdateResult {
        updates: updates_done,
        elapsed: start.elapsed(),
        cut_off,
    })
}

// ------------------------------------------------------------------ reads

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub n: u64,
    pub avg_us: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
}

fn summarize(mut samples: Vec<f64>) -> ReadResult {
    let n = samples.len() as u64;
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = samples.iter().sum::<f64>() / samples.len() as f64;
    let p50 = samples[samples.len() / 2];
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    let max = *samples.last().unwrap();
    ReadResult {
        n,
        avg_us: avg,
        p50_us: p50,
        p99_us: p99,
        max_us: max,
    }
}

/// Point read of the hot row's small field only — never selects `large`
/// (inline) or joins `values_tbl` (off-row). Same query text both variants
/// (`items` has no `large` column at all in the off-row schema); the
/// question this isolates is whether Turso's row decode pays for the large
/// value's overflow chain even when the query never asks for that column.
pub async fn run_read_without(
    conn: &Connection,
    max_item_no: i64,
    n: u64,
    seed: u64,
) -> turso::Result<ReadResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut stmt = conn.prepare("SELECT small FROM items WHERE item_no = ?").await?;
    let mut samples = Vec::with_capacity(n as usize);
    use rand::Rng;
    for _ in 0..n {
        let id = rng.gen_range(0..max_item_no);
        let t0 = Instant::now();
        let mut rows = stmt.query([id]).await?;
        let row = rows.next().await?.expect("row must exist");
        let _small: Vec<u8> = row.get(0)?;
        samples.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    Ok(summarize(samples))
}

/// Point read of the hot row's small field *and* the large value.
/// Inline: one `SELECT small, large`. Off-row: two round trips (row, then
/// value) — the realistic shape of a caller that reads the hot row and then
/// conditionally fetches the payload, not a SQL join.
pub async fn run_read_with(
    conn: &Connection,
    variant: Variant,
    max_item_no: i64,
    n: u64,
    seed: u64,
) -> turso::Result<ReadResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    use rand::Rng;
    let mut samples = Vec::with_capacity(n as usize);
    match variant {
        Variant::Inline => {
            let mut stmt = conn
                .prepare("SELECT small, large FROM items WHERE item_no = ?")
                .await?;
            for _ in 0..n {
                let id = rng.gen_range(0..max_item_no);
                let t0 = Instant::now();
                let mut rows = stmt.query([id]).await?;
                let row = rows.next().await?.expect("row must exist");
                let _small: Vec<u8> = row.get(0)?;
                let _large: Vec<u8> = row.get(1)?;
                samples.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
            }
        }
        Variant::OffRow => {
            let mut row_stmt = conn.prepare("SELECT small FROM items WHERE item_no = ?").await?;
            let mut val_stmt = conn
                .prepare("SELECT value FROM values_tbl WHERE item_no = ? AND field_no = ?")
                .await?;
            for _ in 0..n {
                let id = rng.gen_range(0..max_item_no);
                let t0 = Instant::now();
                let mut rows = row_stmt.query([id]).await?;
                let row = rows.next().await?.expect("row must exist");
                let _small: Vec<u8> = row.get(0)?;
                let mut vrows = val_stmt.query((id, LARGE_FIELD_NO)).await?;
                let vrow = vrows.next().await?.expect("value row must exist");
                let _large: Vec<u8> = vrow.get(0)?;
                samples.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
            }
        }
    }
    Ok(summarize(samples))
}

// ------------------------------------------------------------- claim walk

#[derive(Debug, Clone)]
pub struct ClaimResult {
    pub batches: u64,
    pub claims: u64,
    pub elapsed: Duration,
    pub cut_off: bool,
}

impl ClaimResult {
    pub fn claims_per_sec(&self) -> f64 {
        self.claims as f64 / self.elapsed.as_secs_f64()
    }
}

/// A claim-style op: walk the `rank`-ordered secondary index for a batch of
/// item_nos starting at a random offset, then fetch each matched row's
/// `small` field only (a claim does not want the large payload). Measures
/// index-range-scan + N row fetches together, the shape a real claim
/// implementation would pay.
pub async fn run_claim_walk(
    conn: &Connection,
    max_item_no: i64,
    batches: u64,
    batch_size: usize,
    seed: u64,
    cutoff: Duration,
) -> turso::Result<ClaimResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    use rand::Rng;
    let mut idx_stmt = conn
        .prepare("SELECT item_no FROM item_index WHERE rank >= ? ORDER BY rank LIMIT ?")
        .await?;
    let mut row_stmt = conn.prepare("SELECT small FROM items WHERE item_no = ?").await?;

    let start = Instant::now();
    let mut claims_done = 0u64;
    let mut batches_done = 0u64;
    let mut cut_off = false;
    let hi = (max_item_no - batch_size as i64).max(1);
    for _ in 0..batches {
        let start_rank = rng.gen_range(0..hi);
        let mut ids = Vec::with_capacity(batch_size);
        {
            let mut rows = idx_stmt.query((start_rank, batch_size as i64)).await?;
            while let Some(row) = rows.next().await? {
                let id: i64 = row.get(0)?;
                ids.push(id);
            }
        }
        for id in &ids {
            let mut rows = row_stmt.query([*id]).await?;
            let row = rows.next().await?.expect("row must exist");
            let _small: Vec<u8> = row.get(0)?;
        }
        claims_done += ids.len() as u64;
        batches_done += 1;
        if start.elapsed() > cutoff {
            cut_off = true;
            break;
        }
    }
    Ok(ClaimResult {
        batches: batches_done,
        claims: claims_done,
        elapsed: start.elapsed(),
        cut_off,
    })
}
