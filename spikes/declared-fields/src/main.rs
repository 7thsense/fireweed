//! SPIKE-006 measurement harness: declared-field storage layout at scale on
//! the vendored Turso 0.7.2 engine, connection configured the way
//! `crates/fireweed-turso/src/local.rs` opens its log-backed projection
//! (see `db::configure_connection`). See
//! docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md
//! for the write-up. Standalone crate, not a workspace member.
//!
//! Subcommands:
//!   ingest <layout> <db> <n_queues> <rows_per_queue>
//!   bench  <layout> <db> <touch_queue_no> <trials> <claim_reps> <op_reps>
//!   per_queue_tables <db> <n_queues> <rows_per_queue>
//!   dbsize <db>

mod codec;
mod db;
mod gen;
mod layouts;

use std::env;
use std::io::Write as _;
use std::time::Instant;

use turso::{Builder, Value as SqlValue};

use layouts::{Layout, STAGE_DONE, STAGE_WORK};

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn ingest_batch_size() -> usize {
    env_usize("FW_SPIKE_BATCH_SIZE", 200)
}

fn now_micros() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64
}

fn load_line(tag: &str) {
    eprintln!("[load] {tag} loadavg_1m={:.2}", db::load_avg_1m());
}

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// ---------------------------------------------------------------------------
// ingest
// ---------------------------------------------------------------------------

async fn cmd_ingest(layout: Layout, db_path: &str, n_queues: i64, rows_per_queue: i64) -> turso::Result<()> {
    load_line("ingest:start");
    db::remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let database = Builder::new_local(db_path).build().await?;
    let mut conn = database.connect()?;
    db::configure_connection(&conn).await?;
    for stmt in layouts::shared_schema_ddl(layout) {
        conn.execute(&stmt, ()).await?;
    }

    let batch = ingest_batch_size() as i64;
    let base_ts = now_micros();
    let t0 = Instant::now();
    let mut total_rows = 0i64;
    for queue_no in 1..=n_queues {
        for stmt in layouts::per_queue_schema_ddl(layout, queue_no) {
            conn.execute(&stmt, ()).await?;
        }
        let n_done = (rows_per_queue * 95) / 100; // 95/5 split, same ratio as spikes/claim-scale
        let n_work = rows_per_queue - n_done;
        let mut item_no = 0i64;
        for (stage, count) in [(STAGE_DONE, n_done), (STAGE_WORK, n_work)] {
            let mut i = 0i64;
            while i < count {
                let end = (i + batch).min(count);
                let tx = conn
                    .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
                    .await?;
                for _ in i..end {
                    let rank = gen::rank_for(base_ts, item_no);
                    layouts::ingest_row(&tx, layout, queue_no, item_no, stage, rank).await?;
                    item_no += 1;
                }
                tx.commit().await?;
                db::maybe_passive_checkpoint(&conn, &wal_path).await?;
                i = end;
            }
        }
        total_rows += rows_per_queue;
        eprintln!("[ingest] {layout} queue={queue_no}/{n_queues} rows_so_far={total_rows}");
        let _ = std::io::stderr().flush();
    }
    let ingest_elapsed = t0.elapsed();

    let pre_main = db::file_len(db_path);
    let cp_start = Instant::now();
    db::checkpoint_truncate(&conn).await?;
    let cp_elapsed = cp_start.elapsed();
    let main_bytes = db::file_len(db_path);

    println!(
        "{{\"phase\":\"ingest\",\"layout\":\"{layout}\",\"n_queues\":{n_queues},\
         \"rows_per_queue\":{rows_per_queue},\"total_rows\":{total_rows},\
         \"elapsed_ms\":{ms:.1},\"rows_per_sec\":{rps:.1},\
         \"pre_checkpoint_main_bytes\":{pre_main},\"checkpoint_ms\":{cms:.1},\
         \"main_bytes\":{main_bytes},\"bytes_per_row\":{bpr:.2}}}",
        ms = ingest_elapsed.as_secs_f64() * 1000.0,
        rps = total_rows as f64 / ingest_elapsed.as_secs_f64(),
        cms = cp_elapsed.as_secs_f64() * 1000.0,
        bpr = main_bytes as f64 / total_rows as f64,
    );
    let _ = std::io::stdout().flush();
    load_line("ingest:end");
    drop(conn);
    Ok(())
}

/// Append one more queue to an already-ingested db (no wipe, schema already
/// exists) -- used to give the claim measurement a guaranteed-fresh
/// WORK-stage pool without re-ingesting the whole file, once repeated
/// `bench` invocations against the primary sweep's 3 queues have consumed
/// theirs.
async fn cmd_ingest_queue(layout: Layout, db_path: &str, queue_no: i64, rows: i64) -> turso::Result<()> {
    load_line("ingest_queue:start");
    let wal_path = format!("{db_path}-wal");
    let database = Builder::new_local(db_path).build().await?;
    let mut conn = database.connect()?;
    db::configure_connection(&conn).await?;
    for stmt in layouts::per_queue_schema_ddl(layout, queue_no) {
        conn.execute(&stmt, ()).await?;
    }
    let batch = ingest_batch_size() as i64;
    let base_ts = now_micros();
    let t0 = Instant::now();
    let n_done = (rows * 95) / 100;
    let n_work = rows - n_done;
    let mut item_no = 0i64;
    for (stage, count) in [(STAGE_DONE, n_done), (STAGE_WORK, n_work)] {
        let mut i = 0i64;
        while i < count {
            let end = (i + batch).min(count);
            let tx = conn
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
                .await?;
            for _ in i..end {
                let rank = gen::rank_for(base_ts, item_no);
                layouts::ingest_row(&tx, layout, queue_no, item_no, stage, rank).await?;
                item_no += 1;
            }
            tx.commit().await?;
            db::maybe_passive_checkpoint(&conn, &wal_path).await?;
            i = end;
        }
    }
    let elapsed = t0.elapsed();
    println!(
        "{{\"phase\":\"ingest_queue\",\"layout\":\"{layout}\",\"queue_no\":{queue_no},\"rows\":{rows},\
         \"elapsed_ms\":{ms:.1},\"rows_per_sec\":{rps:.1}}}",
        ms = elapsed.as_secs_f64() * 1000.0,
        rps = rows as f64 / elapsed.as_secs_f64(),
    );
    load_line("ingest_queue:end");
    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// bench: open an already-ingested db, run every operation against
// `touch_queue_no`, 3 trials each with load-avg disclosed at trial start/end.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn cmd_bench(
    layout: Layout,
    db_path: &str,
    touch_queue_no: i64,
    rows_per_queue: i64,
    trials: usize,
    claim_reps: usize,
    op_reps: usize,
) -> turso::Result<()> {
    let wal_path = format!("{db_path}-wal");
    let database = Builder::new_local(db_path).build().await?;
    let mut conn = database.connect()?;
    db::configure_connection(&conn).await?;

    // claim_id space: seeded from wall-clock nanos so repeated `bench`
    // invocations against the same already-ingested db (the cache-size
    // ablation re-runs `bench` several times against one persisted db)
    // never collide on `claims.claim_id`'s PK. Ingest never writes claims.
    let mut claim_id_counter: i64 = {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
    };

    // Untimed warm-up: the first write transaction after opening a
    // checkpointed file pays a one-off page-cache/WAL-reopen cost unrelated
    // to the layout under test (spikes/claim-scale's identical finding,
    // Method addendum: "it dominated trial-1 p99, ~200x every other rep").
    // A plain read does not pay the write side of that cost, so the warm-up
    // must be a real claim+lease+fetch cycle, same shape as the timed loop.
    {
        let _ = layouts::read_small(&conn, layout, touch_queue_no, 0).await?;
        let n = do_claim(&mut conn, &wal_path, layout, touch_queue_no, &mut claim_id_counter).await?;
        if n != 100 {
            eprintln!("[warn] warm-up claim starved ({n} < 100); WORK pool too small for a clean warm-up");
        }
    }

    // ---- claim ----
    for trial in 1..=trials {
        load_line(&format!("claim trial={trial} start"));
        let mut lat = Vec::with_capacity(claim_reps);
        let t0 = Instant::now();
        for _ in 0..claim_reps {
            let rep_t0 = Instant::now();
            let n = do_claim(&mut conn, &wal_path, layout, touch_queue_no, &mut claim_id_counter).await?;
            assert_eq!(n, 100, "claim starved the WORK-stage pool");
            lat.push(rep_t0.elapsed());
        }
        let elapsed = t0.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"claim\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{claim_reps},\
             \"batch\":100,\"elapsed_ms\":{ms:.1},\"claims_per_sec\":{cps:.2},\
             \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = elapsed.as_secs_f64() * 1000.0,
            cps = claim_reps as f64 / elapsed.as_secs_f64(),
            p50 = db::percentile_us(&lat, 0.50),
            p99 = db::percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
        load_line(&format!("claim trial={trial} end"));
    }

    // ---- update: 1-3 small unindexed fields ----
    run_update_phase(&mut conn, &wal_path, layout, "update_small", touch_queue_no, rows_per_queue, trials, op_reps, |conn, layout, qn, item_no, round| {
        Box::pin(layouts::update_small_fields(conn, layout, qn, item_no, &gen::UPDATE_SMALL_FIELDS, round))
    })
    .await?;

    // ---- update: one indexed field ----
    run_update_phase(&mut conn, &wal_path, layout, "update_indexed", touch_queue_no, rows_per_queue, trials, op_reps, |conn, layout, qn, item_no, round| {
        Box::pin(layouts::update_indexed_field(conn, layout, qn, item_no, round))
    })
    .await?;

    // ---- update: one large field (rare -- fewer reps) ----
    let large_reps = (op_reps / 10).max(5);
    run_update_phase(&mut conn, &wal_path, layout, "update_large", touch_queue_no, rows_per_queue, trials, large_reps, |conn, layout, qn, item_no, round| {
        Box::pin(layouts::update_large_field(conn, layout, qn, item_no, round))
    })
    .await?;

    // ---- point read: full record ----
    run_read_phase(&conn, layout, "read_full", touch_queue_no, rows_per_queue, trials, op_reps, |c, l, q, i| {
        Box::pin(layouts::read_full(c, l, q, i))
    })
    .await?;

    // ---- point read: small fields only ----
    run_read_phase(&conn, layout, "read_small", touch_queue_no, rows_per_queue, trials, op_reps, |c, l, q, i| {
        Box::pin(layouts::read_small(c, l, q, i))
    })
    .await?;

    // ---- aggregate: count grouped by the indexed field over a range ----
    for trial in 1..=trials {
        load_line(&format!("aggregate trial={trial} start"));
        let agg_reps = 5;
        let mut lat = Vec::with_capacity(agg_reps);
        let mut last = (0usize, 0i64);
        let t0 = Instant::now();
        for r in 0..agg_reps {
            let lo = (r as i64) * 300;
            let hi = lo + 500;
            let rep_t0 = Instant::now();
            last = layouts::aggregate_count(&conn, layout, touch_queue_no, lo, hi).await?;
            lat.push(rep_t0.elapsed());
        }
        let elapsed = t0.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"aggregate\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{agg_reps},\
             \"elapsed_ms\":{ms:.1},\"groups\":{groups},\"rows_counted\":{rows},\
             \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = elapsed.as_secs_f64() * 1000.0,
            groups = last.0,
            rows = last.1,
            p50 = db::percentile_us(&lat, 0.50),
            p99 = db::percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
        load_line(&format!("aggregate trial={trial} end"));
    }

    drop(conn);
    Ok(())
}

/// One claim: select 100 free rows, lease them, and fetch each one's small
/// fields, all inside ONE `BEGIN IMMEDIATE`/`COMMIT` transaction -- mirroring
/// `spikes/claim-scale`'s `do_claim` shape. **Two real bugs found and fixed
/// here, not anticipated up front, both via an implausibly slow first
/// result**: (1) an earlier version issued the ~201 statements of a
/// 100-record claim with no explicit transaction, so each ran as its own
/// autocommit transaction; (2) even after wrapping them in one transaction,
/// claim stayed orders of magnitude slower than every other operation --
/// traced to the lease-update and fetch-small steps each re-preparing their
/// (identical) SQL text on every one of the 100 rows instead of preparing
/// once and reusing the statement, the same prepare-once/execute-many shape
/// `spikes/claim-scale`'s `do_claim` always used. `claim_lease_all` and
/// `claim_fetch_small_all` (layouts.rs) fix this. Every claim number in
/// this document's Findings is measured after both fixes.
async fn do_claim(
    conn: &mut turso::Connection,
    wal_path: &str,
    layout: Layout,
    queue_no: i64,
    claim_id_counter: &mut i64,
) -> turso::Result<usize> {
    let tx = conn
        .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
        .await?;
    let ids = layouts::claim_select(&tx, layout, queue_no, 100).await?;
    let claim_id = *claim_id_counter;
    *claim_id_counter += 1;
    let expires_at = now_micros() + 30_000_000;
    layouts::insert_claim(&tx, claim_id, expires_at, ids.len()).await?;
    layouts::claim_lease_all(&tx, layout, queue_no, &ids, claim_id).await?;
    let small_fields_total = layouts::claim_fetch_small_all(&tx, layout, queue_no, &ids).await?;
    std::hint::black_box(small_fields_total);
    tx.commit().await?;
    db::maybe_passive_checkpoint(conn, wal_path).await?;
    Ok(ids.len())
}

type UpdateFut<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = turso::Result<()>> + 'a>>;

#[allow(clippy::too_many_arguments)]
async fn run_update_phase<F>(
    conn: &mut turso::Connection,
    wal_path: &str,
    layout: Layout,
    phase: &str,
    touch_queue_no: i64,
    rows_per_queue: i64,
    trials: usize,
    reps: usize,
    op: F,
) -> turso::Result<()>
where
    F: for<'a> Fn(&'a turso::Connection, Layout, i64, i64, u64) -> UpdateFut<'a>,
{
    for trial in 1..=trials {
        load_line(&format!("{phase} trial={trial} start"));
        let mut lat = Vec::with_capacity(reps);
        let t0 = Instant::now();
        // `phase_salt`: a phase-derived FNV-1a hash folded into the item_no
        // seed. Without this, update_small/update_indexed/update_large (all
        // three call this same function) picked the *identical* item_no
        // sequence for a given (trial, r) -- found via an implausible first
        // result (layout A's update_large measuring 85x faster than
        // update_small): whichever phase ran last got its rows
        // page-cache-warmed by the earlier phase's writes to the same rows,
        // an artifact of shared RNG state, not a real per-layout cost
        // difference. Each phase now draws an independent item_no sequence.
        let phase_salt = fnv1a(phase);
        for r in 0..reps {
            let item_no =
                (gen::mix64((trial * 1_000_003 + r) as u64 ^ phase_salt) % rows_per_queue as u64) as i64;
            let rep_t0 = Instant::now();
            // One explicit transaction per op -- matching claim-scale's
            // per-operation transaction shape (found necessary the same way
            // `do_claim`'s fix was: an unwrapped multi-statement op pays
            // per-statement autocommit overhead instead of one commit).
            let tx = conn
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
                .await?;
            op(&tx, layout, touch_queue_no, item_no, r as u64 + 1).await?;
            tx.commit().await?;
            db::maybe_passive_checkpoint(conn, wal_path).await?;
            lat.push(rep_t0.elapsed());
        }
        let elapsed = t0.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"{phase}\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{reps},\
             \"elapsed_ms\":{ms:.1},\"ops_per_sec\":{ops:.2},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = elapsed.as_secs_f64() * 1000.0,
            ops = reps as f64 / elapsed.as_secs_f64(),
            p50 = db::percentile_us(&lat, 0.50),
            p99 = db::percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
        load_line(&format!("{phase} trial={trial} end"));
    }
    Ok(())
}

type ReadFut<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = turso::Result<usize>> + 'a>>;

async fn run_read_phase<F>(
    conn: &turso::Connection,
    layout: Layout,
    phase: &str,
    touch_queue_no: i64,
    rows_per_queue: i64,
    trials: usize,
    reps: usize,
    op: F,
) -> turso::Result<()>
where
    F: for<'a> Fn(&'a turso::Connection, Layout, i64, i64) -> ReadFut<'a>,
{
    for trial in 1..=trials {
        load_line(&format!("{phase} trial={trial} start"));
        let mut lat = Vec::with_capacity(reps);
        let t0 = Instant::now();
        let mut checksum = 0usize;
        for r in 0..reps {
            let item_no = (gen::mix64((trial * 7_000_003 + r) as u64) % rows_per_queue as u64) as i64;
            let rep_t0 = Instant::now();
            checksum += op(conn, layout, touch_queue_no, item_no).await?;
            lat.push(rep_t0.elapsed());
        }
        std::hint::black_box(checksum);
        let elapsed = t0.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"{phase}\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{reps},\
             \"elapsed_ms\":{ms:.1},\"reads_per_sec\":{rps:.2},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = elapsed.as_secs_f64() * 1000.0,
            rps = reps as f64 / elapsed.as_secs_f64(),
            p50 = db::percentile_us(&lat, 0.50),
            p99 = db::percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
        load_line(&format!("{phase} trial={trial} end"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// per_queue_tables (D only): schema-creation cost, schema-only DB size, and
// open time at N queues' worth of physical tables in one file.
// ---------------------------------------------------------------------------

async fn cmd_per_queue_tables(db_path: &str, n_queues: i64, rows_per_queue: i64) -> turso::Result<()> {
    load_line("per_queue_tables:start");
    db::remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let database = Builder::new_local(db_path).build().await?;
    let mut conn = database.connect()?;
    db::configure_connection(&conn).await?;
    for stmt in layouts::shared_schema_ddl(Layout::D) {
        conn.execute(&stmt, ()).await?;
    }

    let t0 = Instant::now();
    let base_ts = now_micros();
    for queue_no in 1..=n_queues {
        for stmt in layouts::per_queue_schema_ddl(Layout::D, queue_no) {
            conn.execute(&stmt, ()).await?;
        }
        if rows_per_queue > 0 {
            let tx = conn
                .transaction_with_behavior(turso::transaction::TransactionBehavior::Immediate)
                .await?;
            for item_no in 0..rows_per_queue {
                let rank = gen::rank_for(base_ts, item_no);
                layouts::ingest_row(&tx, Layout::D, queue_no, item_no, STAGE_DONE, rank).await?;
            }
            tx.commit().await?;
        }
        db::maybe_passive_checkpoint(&conn, &wal_path).await?;
        if queue_no % 1000 == 0 {
            eprintln!("[per_queue_tables] created {queue_no}/{n_queues} queues");
            let _ = std::io::stderr().flush();
        }
    }
    let create_elapsed = t0.elapsed();

    let cp_start = Instant::now();
    db::checkpoint_truncate(&conn).await?;
    let cp_elapsed = cp_start.elapsed();
    let schema_bytes = db::file_len(db_path);
    drop(conn);
    drop(database);

    // Reopen: Turso open time at N queues' worth of schema/tables.
    let open_t0 = Instant::now();
    let database2 = Builder::new_local(db_path).build().await?;
    let conn2 = database2.connect()?;
    db::configure_connection(&conn2).await?;
    // First query forces the schema to actually load/parse.
    let mut stmt = conn2.prepare("SELECT count(*) FROM sqlite_master WHERE type='table'").await?;
    let mut rows = stmt.query(()).await?;
    let mut table_count = 0i64;
    if let Some(row) = rows.next().await? {
        if let SqlValue::Integer(c) = row.get_value(0)? {
            table_count = c;
        }
    }
    let open_elapsed = open_t0.elapsed();

    println!(
        "{{\"phase\":\"per_queue_tables\",\"n_queues\":{n_queues},\"rows_per_queue\":{rows_per_queue},\
         \"create_ms\":{cms:.1},\"checkpoint_ms\":{ckms:.1},\"schema_bytes\":{schema_bytes},\
         \"open_ms\":{oms:.3},\"table_count\":{table_count}}}",
        cms = create_elapsed.as_secs_f64() * 1000.0,
        ckms = cp_elapsed.as_secs_f64() * 1000.0,
        oms = open_elapsed.as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();
    load_line("per_queue_tables:end");
    drop(conn2);
    Ok(())
}

async fn cmd_dbsize(db_path: &str) -> turso::Result<()> {
    let main_bytes = db::file_len(db_path);
    println!("{{\"phase\":\"dbsize\",\"main_bytes\":{main_bytes}}}");
    Ok(())
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> turso::Result<()> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("ingest") => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let n_queues: i64 = args[4].parse().unwrap();
            let rows_per_queue: i64 = args[5].parse().unwrap();
            cmd_ingest(layout, db_path, n_queues, rows_per_queue).await?;
        }
        Some("ingest_queue") => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let queue_no: i64 = args[4].parse().unwrap();
            let rows: i64 = args[5].parse().unwrap();
            cmd_ingest_queue(layout, db_path, queue_no, rows).await?;
        }
        Some("bench") => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let touch_queue_no: i64 = args[4].parse().unwrap();
            let rows_per_queue: i64 = args[5].parse().unwrap();
            let trials: usize = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(3);
            let claim_reps: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(20);
            let op_reps: usize = args.get(8).map(|s| s.parse().unwrap()).unwrap_or(200);
            cmd_bench(layout, db_path, touch_queue_no, rows_per_queue, trials, claim_reps, op_reps).await?;
        }
        Some("per_queue_tables") => {
            let db_path = &args[2];
            let n_queues: i64 = args[3].parse().unwrap();
            let rows_per_queue: i64 = args[4].parse().unwrap();
            cmd_per_queue_tables(db_path, n_queues, rows_per_queue).await?;
        }
        Some("dbsize") => {
            cmd_dbsize(&args[2]).await?;
        }
        _ => {
            eprintln!(
                "usage: spike ingest <a|b|c|d> <db> <n_queues> <rows_per_queue>\n\
                 \x20      spike bench <a|b|c|d> <db> <touch_queue_no> <rows_per_queue> [trials] [claim_reps] [op_reps]\n\
                 \x20      spike per_queue_tables <db> <n_queues> <rows_per_queue>\n\
                 \x20      spike dbsize <db>"
            );
            std::process::exit(2);
        }
    }
    Ok(())
}
