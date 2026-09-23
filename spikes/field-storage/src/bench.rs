//! Run harness: ingest, update-churn and point-read measurements, each
//! wall-clock bounded so a slow config degrades to "fewer rows measured",
//! never "runs past the time budget".
use std::path::Path;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::SeedableRng;
use turso::Connection;

use crate::db::{open_db, on_disk_size};
use crate::gen::{full_record, touched_fields, ValueTemplate};
use crate::models::{self, create_index_table, create_schema, Model};

/// Every run stops after this much wall time, whatever row count it has
/// reached, and reports the row count actually completed. Bounded well
/// under the ~2 minute per-run ceiling the task set.
pub const CUTOFF: Duration = Duration::from_secs(90);

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
        self.db_bytes as f64 / self.rows as f64
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_ingest(
    model: Model,
    fields: usize,
    value_size: usize,
    target_rows: u64,
    batch_size: u64,
    db_path: &Path,
    seed: u64,
    keep_db: bool,
) -> turso::Result<IngestResult> {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
    let (_db, conn) = open_db(db_path).await?;
    create_schema(&conn, model).await?;
    create_index_table(&conn).await?;

    let mut rng = StdRng::seed_from_u64(seed);
    let tpl = ValueTemplate::new(&mut rng, value_size);

    let start = Instant::now();
    let mut rows_done = 0u64;
    let mut cut_off = false;
    let mut next_id = 0i64;
    while rows_done < target_rows {
        let n = batch_size.min(target_rows - rows_done);
        let records: Vec<(i64, Vec<(usize, String)>)> = (0..n)
            .map(|i| {
                let id = next_id + i as i64;
                (id, full_record(&tpl, id, fields))
            })
            .collect();
        next_id += n as i64;

        conn.execute("BEGIN IMMEDIATE", ()).await?;
        models::ingest(&conn, model, &records).await?;
        conn.execute("COMMIT", ()).await?;

        rows_done += n;
        if start.elapsed() > CUTOFF {
            cut_off = true;
            break;
        }
    }
    let elapsed = start.elapsed();
    let db_bytes = on_disk_size(&conn, db_path).await?;
    if !keep_db {
        drop(conn);
        drop(_db);
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
    }
    Ok(IngestResult {
        rows: rows_done,
        elapsed,
        cut_off,
        db_bytes,
    })
}

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

/// Repeatedly updates the same `pool` of item_nos (already present in the DB
/// at `conn`), `rounds` times, touching `fields_touched` fields per record
/// each round (field 0 included iff `indexed`, in which case the index side
/// table is upserted for every touched record too). This is the churn
/// scenario: by round 2 every touched hot-row hits an UPDATE of an
/// already-populated row, and every touched index entry is a genuine rekey.
#[allow(clippy::too_many_arguments)]
pub async fn run_update(
    conn: &Connection,
    model: Model,
    pool: &[i64],
    total_fields: usize,
    fields_touched: usize,
    indexed: bool,
    value_size: usize,
    rounds: u64,
    seed: u64,
) -> turso::Result<UpdateResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    let tpl = ValueTemplate::new(&mut rng, value_size);
    let touched = touched_fields(fields_touched, total_fields, indexed);

    let start = Instant::now();
    let mut updates_done = 0u64;
    let mut cut_off = false;
    for round in 0..rounds {
        let batch: Vec<(i64, Vec<(usize, String)>)> = pool
            .iter()
            .map(|&id| {
                let fields = touched
                    .iter()
                    .map(|&f| (f, tpl.value_for(id, f, round)))
                    .collect();
                (id, fields)
            })
            .collect();

        conn.execute("BEGIN IMMEDIATE", ()).await?;
        models::update(conn, model, &batch).await?;
        if indexed {
            let idx_rows: Vec<(i64, String)> = batch
                .iter()
                .map(|(id, fields)| (*id, fields[0].1.clone()))
                .collect();
            models::upsert_index_rows(conn, &idx_rows).await?;
        }
        conn.execute("COMMIT", ()).await?;

        updates_done += batch.len() as u64;
        if start.elapsed() > CUTOFF {
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

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub n: u64,
    pub avg_us: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
}

pub async fn run_point_reads(
    conn: &Connection,
    model: Model,
    max_item_no: i64,
    n: u64,
    seed: u64,
) -> turso::Result<ReadResult> {
    let mut rng = StdRng::seed_from_u64(seed);
    let sql = models::point_read_sql(model);
    let mut stmt = conn.prepare(sql).await?;
    let mut samples = Vec::with_capacity(n as usize);
    use rand::Rng;
    for _ in 0..n {
        let id = rng.gen_range(0..max_item_no);
        let t0 = Instant::now();
        let _ = models::point_read(&mut stmt, model, id).await?;
        samples.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = samples.iter().sum::<f64>() / samples.len() as f64;
    let p50 = samples[samples.len() / 2];
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    let max = *samples.last().unwrap();
    Ok(ReadResult {
        n,
        avg_us: avg,
        p50_us: p50,
        p99_us: p99,
        max_us: max,
    })
}
