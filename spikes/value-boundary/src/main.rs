//! SPIKE-005 value-boundary runner. Prints machine-parseable, `|`-delimited
//! result lines to stdout (one section tag per line: INGEST, UPDATE_SMALL,
//! UPDATE_LARGE, READ_WITHOUT, READ_WITH, CLAIM, PAGE); progress goes to
//! stderr. See
//! docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md for
//! the write-up this feeds.
mod bench;
mod db;
mod gen;
mod sql;

use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bench::Variant;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

const SEED: u64 = 0xF17E_ED05;
const SMALL_SIZE: usize = 32;
const SIZES: [usize; 10] = [256, 512, 1024, 2048, 3072, 4096, 8192, 16384, 32768, 65536];

const INGEST_TARGET: u64 = 50_000;
const INGEST_BATCH: u64 = 1_000;
const INGEST_CUTOFF: Duration = Duration::from_secs(30);

const UPDATE_POOL: usize = 1_000;
const UPDATE_ROUNDS: u64 = 5;
const UPDATE_CUTOFF: Duration = Duration::from_secs(20);

const READ_SAMPLES: u64 = 1_000;

const CLAIM_BATCH_SIZE: usize = 100;
const CLAIM_BATCHES: u64 = 20;
const CLAIM_CUTOFF: Duration = Duration::from_secs(20);

const REPS: u32 = 3;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn db_dir() -> PathBuf {
    let dir = std::env::var("SPIKE_DB_DIR").unwrap_or_else(|_| "dbdata".to_string());
    let path = PathBuf::from(dir);
    std::fs::create_dir_all(&path).expect("create db dir");
    path
}

fn rm(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
}

fn pick_pool(rows: u64, want: usize, seed: u64) -> Vec<i64> {
    let mut rng = StdRng::seed_from_u64(seed);
    if rows <= want as u64 {
        return (0..rows as i64).collect();
    }
    let mut all: Vec<i64> = (0..rows as i64).collect();
    all.shuffle(&mut rng);
    all.truncate(want);
    all
}

/// Sizes to run the full 6-operation matrix at. Overridable via
/// `SPIKE_SIZES` (comma-separated bytes) for a fast pilot run.
fn sizes() -> Vec<usize> {
    match std::env::var("SPIKE_SIZES") {
        Ok(s) => s.split(',').map(|x| x.trim().parse().expect("bad SPIKE_SIZES entry")).collect(),
        Err(_) => SIZES.to_vec(),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var("SPIKE_PAGE_SIZE_SWEEP").is_ok() {
        return page_size_sweep().await;
    }
    if std::env::var("SPIKE_UPDATE_PROBE").is_ok() {
        return update_probe().await;
    }

    let dir = db_dir();
    let ingest_target = env_u64("SPIKE_INGEST_ROWS", INGEST_TARGET);
    let reps = env_u64("SPIKE_REPS", REPS as u64) as u32;
    println!(
        "# base config: small_size={SMALL_SIZE}B ingest_target={ingest_target} ingest_batch={INGEST_BATCH} \
         ingest_cutoff_s={} update_pool={UPDATE_POOL} update_rounds={UPDATE_ROUNDS} update_cutoff_s={} \
         read_samples={READ_SAMPLES} claim_batch={CLAIM_BATCH_SIZE} claim_batches={CLAIM_BATCHES} \
         claim_cutoff_s={} reps={reps} seed={SEED:#x}",
        INGEST_CUTOFF.as_secs(),
        UPDATE_CUTOFF.as_secs(),
        CLAIM_CUTOFF.as_secs(),
    );
    println!("INGEST|variant|value_size_b|rep|rows|elapsed_s|rows_per_sec|db_bytes|bytes_per_row|cut_off");
    println!("UPDATE_SMALL|variant|value_size_b|rep|pool|rounds|updates|elapsed_s|updates_per_sec|cut_off");
    println!("UPDATE_LARGE|variant|value_size_b|rep|pool|rounds|updates|elapsed_s|updates_per_sec|cut_off");
    println!("READ_WITHOUT|variant|value_size_b|rep|n|avg_us|p50_us|p99_us|max_us");
    println!("READ_WITH|variant|value_size_b|rep|n|avg_us|p50_us|p99_us|max_us");
    println!("CLAIM|variant|value_size_b|rep|batches|batch_size|claims|elapsed_s|claims_per_sec|cut_off");

    for &variant in &[Variant::Inline, Variant::OffRow] {
        for &size in &sizes() {
            eprintln!("=== variant={} size={}B ===", variant.short(), size);
            let base_path = dir.join(format!("base_{}_{}.db", variant.short(), size));
            let mut last_rows = 0u64;
            for rep in 0..reps {
                rm(&base_path);
                let r = bench::run_ingest(
                    variant,
                    size,
                    SMALL_SIZE,
                    ingest_target,
                    INGEST_BATCH,
                    &base_path,
                    db::DEFAULT_PAGE_SIZE,
                    SEED ^ (rep as u64),
                    INGEST_CUTOFF,
                )
                .await?;
                println!(
                    "INGEST|{}|{}|{}|{}|{:.3}|{:.1}|{}|{:.2}|{}",
                    variant.short(),
                    size,
                    rep,
                    r.rows,
                    r.elapsed.as_secs_f64(),
                    r.rows_per_sec(),
                    r.db_bytes,
                    r.bytes_per_row(),
                    r.cut_off,
                );
                eprintln!(
                    "  ingest rep{}: {} rows in {:.3}s ({:.0} rows/s), {} B/row{}",
                    rep,
                    r.rows,
                    r.elapsed.as_secs_f64(),
                    r.rows_per_sec(),
                    r.bytes_per_row() as u64,
                    if r.cut_off { " [CUT OFF]" } else { "" }
                );
                last_rows = r.rows;
            }
            // base_path now holds the last rep's ingested data (last_rows
            // rows); every op below runs against that one populated DB.
            let (_db, conn) = db::open_db(&base_path, db::DEFAULT_PAGE_SIZE).await?;
            let pool = pick_pool(last_rows, UPDATE_POOL, SEED ^ 0x1111);
            bench::warm_pool(&conn, variant, &pool).await?;
            // Untimed warm-up writes (one round each, discarded): the WAL was
            // just truncated by `run_ingest`'s closing checkpoint, and the
            // *first* write batch after a truncate pays a one-time file-growth
            // cost that later batches do not — without this, whichever timed
            // op happens to run first would be biased slow purely by
            // measurement order, not by variant or size.
            bench::run_update_small(&conn, &pool, SMALL_SIZE, 1, SEED ^ 0xFFFE, UPDATE_CUTOFF).await?;
            bench::run_update_large(&conn, variant, &pool, size, 1, SEED ^ 0xFFFD, UPDATE_CUTOFF).await?;

            for rep in 0..reps {
                let r = bench::run_update_small(
                    &conn,
                    &pool,
                    SMALL_SIZE,
                    UPDATE_ROUNDS,
                    SEED ^ (0x10 + rep as u64),
                    UPDATE_CUTOFF,
                )
                .await?;
                println!(
                    "UPDATE_SMALL|{}|{}|{}|{}|{}|{}|{:.3}|{:.1}|{}",
                    variant.short(),
                    size,
                    rep,
                    pool.len(),
                    UPDATE_ROUNDS,
                    r.updates,
                    r.elapsed.as_secs_f64(),
                    r.updates_per_sec(),
                    r.cut_off,
                );
            }
            for rep in 0..reps {
                let r = bench::run_update_large(
                    &conn,
                    variant,
                    &pool,
                    size,
                    UPDATE_ROUNDS,
                    SEED ^ (0x20 + rep as u64),
                    UPDATE_CUTOFF,
                )
                .await?;
                println!(
                    "UPDATE_LARGE|{}|{}|{}|{}|{}|{}|{:.3}|{:.1}|{}",
                    variant.short(),
                    size,
                    rep,
                    pool.len(),
                    UPDATE_ROUNDS,
                    r.updates,
                    r.elapsed.as_secs_f64(),
                    r.updates_per_sec(),
                    r.cut_off,
                );
            }
            for rep in 0..reps {
                let r =
                    bench::run_read_without(&conn, last_rows.max(1) as i64, READ_SAMPLES, SEED ^ (0x30 + rep as u64))
                        .await?;
                println!(
                    "READ_WITHOUT|{}|{}|{}|{}|{:.2}|{:.2}|{:.2}|{:.2}",
                    variant.short(),
                    size,
                    rep,
                    r.n,
                    r.avg_us,
                    r.p50_us,
                    r.p99_us,
                    r.max_us
                );
            }
            for rep in 0..reps {
                let r = bench::run_read_with(
                    &conn,
                    variant,
                    last_rows.max(1) as i64,
                    READ_SAMPLES,
                    SEED ^ (0x40 + rep as u64),
                )
                .await?;
                println!(
                    "READ_WITH|{}|{}|{}|{}|{:.2}|{:.2}|{:.2}|{:.2}",
                    variant.short(),
                    size,
                    rep,
                    r.n,
                    r.avg_us,
                    r.p50_us,
                    r.p99_us,
                    r.max_us
                );
            }
            for rep in 0..reps {
                let r = bench::run_claim_walk(
                    &conn,
                    last_rows.max(1) as i64,
                    CLAIM_BATCHES,
                    CLAIM_BATCH_SIZE,
                    SEED ^ (0x50 + rep as u64),
                    CLAIM_CUTOFF,
                )
                .await?;
                println!(
                    "CLAIM|{}|{}|{}|{}|{}|{}|{:.3}|{:.1}|{}",
                    variant.short(),
                    size,
                    rep,
                    r.batches,
                    CLAIM_BATCH_SIZE,
                    r.claims,
                    r.elapsed.as_secs_f64(),
                    r.claims_per_sec(),
                    r.cut_off,
                );
            }

            drop(conn);
            drop(_db);
            rm(&base_path);
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// One-off probe (SPIKE_UPDATE_PROBE=1): explains and directly measures the
/// off-row `UPDATE ... FROM (VALUES ...)` query plan against `values_tbl`,
/// to check whether its dramatically lower `run_update_large` throughput
/// (vs. the inline variant at the same value size) is a real per-write cost
/// or a bad query plan / harness bug.
async fn update_probe() -> Result<(), Box<dyn Error>> {
    let dir = db_dir();
    let path = dir.join("probe_offrow.db");
    rm(&path);
    let (_db, conn) = db::open_db(&path, db::DEFAULT_PAGE_SIZE).await?;
    bench::create_schema(&conn, Variant::OffRow).await?;
    let n = 20_000i64;
    let size = 4096usize;
    let mut rng = StdRng::seed_from_u64(SEED);
    let small_tpl = gen::ValueTemplate::new(&mut rng, SMALL_SIZE);
    let large_tpl = gen::ValueTemplate::new(&mut rng, size);
    conn.execute("BEGIN IMMEDIATE", ()).await?;
    for id in 0..n {
        conn.execute(
            "INSERT INTO items (item_no, small) VALUES (?, ?)",
            (id, small_tpl.value_for(id, 0)),
        )
        .await?;
        conn.execute(
            "INSERT INTO values_tbl (item_no, field_no, value) VALUES (?, 0, ?)",
            (id, large_tpl.value_for(id, 0)),
        )
        .await?;
    }
    conn.execute("COMMIT", ()).await?;

    // EXPLAIN QUERY PLAN for the exact statement shape run_update_large uses
    // (single-row form here; the batched VALUES form is size-only, not
    // plan-relevant for index selection).
    let plan_sql =
        "UPDATE values_tbl SET value = v.column2 FROM (VALUES (?,?)) AS v WHERE values_tbl.item_no = v.column1 AND values_tbl.field_no = 0";
    let mut rows = conn.query(format!("EXPLAIN QUERY PLAN {plan_sql}"), ()).await?;
    eprintln!("-- EXPLAIN QUERY PLAN (offrow update_large) --");
    while let Some(row) = rows.next().await? {
        let vals: Vec<String> = (0..row.column_count())
            .map(|i| format!("{:?}", row.get_value(i).unwrap()))
            .collect();
        eprintln!("{}", vals.join(" | "));
    }

    // Also EXPLAIN QUERY PLAN for the inline equivalent, for comparison.
    conn.execute(
        "CREATE TABLE items2 (item_no INTEGER PRIMARY KEY, small BLOB NOT NULL, large BLOB NOT NULL)",
        (),
    )
    .await?;
    let plan_sql2 =
        "UPDATE items2 SET large = v.column2 FROM (VALUES (?,?)) AS v WHERE items2.item_no = v.column1";
    let mut rows2 = conn.query(format!("EXPLAIN QUERY PLAN {plan_sql2}"), ()).await?;
    eprintln!("-- EXPLAIN QUERY PLAN (inline update_large) --");
    while let Some(row) = rows2.next().await? {
        let vals: Vec<String> = (0..row.column_count())
            .map(|i| format!("{:?}", row.get_value(i).unwrap()))
            .collect();
        eprintln!("{}", vals.join(" | "));
    }

    // EXPLAIN QUERY PLAN for a plain (non-join) single-row UPDATE against
    // values_tbl's own (item_no, field_no) equality — no VALUES-derived `v`
    // table involved.
    let plan_sql3 = "UPDATE values_tbl SET value = ? WHERE item_no = ? AND field_no = 0";
    let mut rows3 = conn.query(format!("EXPLAIN QUERY PLAN {plan_sql3}"), ()).await?;
    eprintln!("-- EXPLAIN QUERY PLAN (offrow single-row, no VALUES join) --");
    while let Some(row) = rows3.next().await? {
        let vals: Vec<String> = (0..row.column_count())
            .map(|i| format!("{:?}", row.get_value(i).unwrap()))
            .collect();
        eprintln!("{}", vals.join(" | "));
    }

    // Direct timing: 1 round, pool=200, single-statement-per-row (all inside
    // ONE explicit transaction, so per-commit overhead does not dominate)
    // vs. the batched VALUES...FROM form, both against values_tbl, to see
    // whether the batched form's SCAN/SCAN plan is the actual cost driver.
    let pool: Vec<i64> = (0..200).collect();
    let rev = 1u64;
    conn.execute("BEGIN IMMEDIATE", ()).await?;
    let t0 = std::time::Instant::now();
    for &id in &pool {
        let n_changed = conn
            .execute(
                "UPDATE values_tbl SET value = ? WHERE item_no = ? AND field_no = 0",
                (large_tpl.value_for(id, rev), id),
            )
            .await?;
        assert_eq!(n_changed, 1, "single-row update must change exactly 1 row");
    }
    let single_row_elapsed = t0.elapsed();
    conn.execute("COMMIT", ()).await?;
    eprintln!(
        "single-row UPDATE x{} (1 txn): {:.3}s ({:.1}/s)",
        pool.len(),
        single_row_elapsed.as_secs_f64(),
        pool.len() as f64 / single_row_elapsed.as_secs_f64()
    );

    let rows: Vec<Vec<turso::Value>> = pool
        .iter()
        .map(|&id| {
            vec![
                turso::Value::Integer(id),
                turso::Value::Blob(large_tpl.value_for(id, rev + 1)),
            ]
        })
        .collect();
    let t1 = std::time::Instant::now();
    let changed = sql::exec_chunked_count(
        &conn,
        "UPDATE values_tbl SET value = v.column2 FROM (VALUES ",
        ") AS v WHERE values_tbl.item_no = v.column1 AND values_tbl.field_no = 0",
        &rows,
    )
    .await?;
    eprintln!(
        "batched VALUES UPDATE x{} (changed={}): {:.3}s ({:.1}/s)",
        pool.len(),
        changed,
        t1.elapsed().as_secs_f64(),
        pool.len() as f64 / t1.elapsed().as_secs_f64()
    );

    drop(conn);
    drop(_db);
    rm(&path);
    Ok(())
}

/// `PRAGMA page_size` sweep: inline variant only, at two sizes straddling
/// the default 4 KiB page's overflow threshold (`usable_space - 35`,
/// `vendor/turso_core/storage/btree.rs:9027-9036`), across page sizes
/// {4096, 8192, 16384, 65536}. Uses a smaller ingest target so the sweep
/// stays fast: this sub-experiment is about the *shape* of the effect, not
/// an independent full-scale run.
async fn page_size_sweep() -> Result<(), Box<dyn Error>> {
    let dir = db_dir();
    let reps = env_u64("SPIKE_REPS", REPS as u64) as u32;
    let target = env_u64("SPIKE_PAGE_INGEST_ROWS", 20_000);
    let page_sizes: [u32; 4] = [4096, 8192, 16384, 65536];
    let test_sizes: [usize; 2] = [4096, 16384];

    println!("# page-size sweep: target_rows={target} reps={reps} seed={SEED:#x}");
    println!("PAGE_INGEST|page_size|value_size_b|rep|rows|elapsed_s|rows_per_sec|db_bytes|bytes_per_row|cut_off");
    println!("PAGE_UPDATE_LARGE|page_size|value_size_b|rep|pool|rounds|updates|elapsed_s|updates_per_sec|cut_off");

    for &page_size in &page_sizes {
        for &size in &test_sizes {
            eprintln!("=== page_size={page_size} size={size}B ===");
            let base_path = dir.join(format!("page_{page_size}_{size}.db"));
            let mut last_rows = 0u64;
            for rep in 0..reps {
                rm(&base_path);
                let r = bench::run_ingest(
                    Variant::Inline,
                    size,
                    SMALL_SIZE,
                    target,
                    INGEST_BATCH,
                    &base_path,
                    page_size,
                    SEED ^ (rep as u64),
                    INGEST_CUTOFF,
                )
                .await?;
                println!(
                    "PAGE_INGEST|{}|{}|{}|{}|{:.3}|{:.1}|{}|{:.2}|{}",
                    page_size,
                    size,
                    rep,
                    r.rows,
                    r.elapsed.as_secs_f64(),
                    r.rows_per_sec(),
                    r.db_bytes,
                    r.bytes_per_row(),
                    r.cut_off,
                );
                last_rows = r.rows;
            }
            let (_db, conn) = db::open_db(&base_path, page_size).await?;
            let pool = pick_pool(last_rows, UPDATE_POOL, SEED ^ 0x2222);
            bench::warm_pool(&conn, Variant::Inline, &pool).await?;
            bench::run_update_large(&conn, Variant::Inline, &pool, size, 1, SEED ^ 0xFFFC, UPDATE_CUTOFF).await?;
            for rep in 0..reps {
                let r = bench::run_update_large(
                    &conn,
                    Variant::Inline,
                    &pool,
                    size,
                    UPDATE_ROUNDS,
                    SEED ^ (0x60 + rep as u64),
                    UPDATE_CUTOFF,
                )
                .await?;
                println!(
                    "PAGE_UPDATE_LARGE|{}|{}|{}|{}|{}|{}|{:.3}|{:.1}|{}",
                    page_size,
                    size,
                    rep,
                    pool.len(),
                    UPDATE_ROUNDS,
                    r.updates,
                    r.elapsed.as_secs_f64(),
                    r.updates_per_sec(),
                    r.cut_off,
                );
            }
            drop(conn);
            drop(_db);
            rm(&base_path);
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
