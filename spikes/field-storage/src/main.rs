//! SPIKE-003 field-storage runner. Prints machine-parseable, `|`-delimited
//! result lines to stdout (one section tag per line: INGEST, READ, UPDATE,
//! PAYLOAD); progress goes to stderr. See
//! docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md for the
//! write-up this feeds.
mod bench;
mod db;
mod gen;
mod models;
mod sql;
mod value;

use std::error::Error;
use std::path::{Path, PathBuf};

use models::{Model, ALL_MODELS};

const BASE_FIELDS: usize = 50;
const BASE_VALUE_SIZE: usize = 256;
const SEED: u64 = 0xF17E_ED00;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var("SPIKE_SQL_PROBE").is_ok() {
        return sql_probe().await;
    }
    if std::env::var("SPIKE_SIZE_PROBE").is_ok() {
        size_probe();
        return Ok(());
    }
    if std::env::var("SPIKE_DB_SIZE_PROBE").is_ok() {
        return db_size_probe().await;
    }
    if std::env::var("SPIKE_PAIR_PROBE").is_ok() {
        return pair_probe().await;
    }
    if std::env::var("SPIKE_PAYLOAD_ONLY").is_ok() {
        let rounds = env_u64("SPIKE_UPDATE_ROUNDS", 20);
        return payload_sweep(&db_dir(), rounds).await;
    }
    let dir = db_dir();
    let target_rows = env_u64("SPIKE_ROWS", 100_000);
    let batch_size = env_u64("SPIKE_BATCH", 1_000);
    let pool_size = env_u64("SPIKE_POOL", 1_000) as usize;
    let update_rounds = env_u64("SPIKE_UPDATE_ROUNDS", 20);
    let read_samples = env_u64("SPIKE_READ_SAMPLES", 3_000);
    println!(
        "# base config: fields={BASE_FIELDS} value_size={BASE_VALUE_SIZE}B rows={target_rows} \
         batch={batch_size} pool={pool_size} update_rounds={update_rounds} read_samples={read_samples} \
         seed={SEED:#x}"
    );

    struct IngestCfg {
        fields: usize,
        value_size: usize,
        keep_as_base: bool,
    }
    let ingest_cfgs = [
        IngestCfg { fields: 5, value_size: BASE_VALUE_SIZE, keep_as_base: false },
        IngestCfg { fields: BASE_FIELDS, value_size: BASE_VALUE_SIZE, keep_as_base: true },
        IngestCfg { fields: 200, value_size: BASE_VALUE_SIZE, keep_as_base: false },
        IngestCfg { fields: BASE_FIELDS, value_size: 16, keep_as_base: false },
        IngestCfg { fields: BASE_FIELDS, value_size: 4_096, keep_as_base: false },
    ];

    println!(
        "INGEST|model|fields|value_size_b|rows|elapsed_s|rows_per_sec|db_bytes|bytes_per_row|cut_off"
    );
    for model in ALL_MODELS {
        for cfg in &ingest_cfgs {
            let path = dir.join(format!(
                "ingest_{}_{}f_{}b.db",
                model.short(),
                cfg.fields,
                cfg.value_size
            ));
            eprintln!(
                "[ingest] {} fields={} size={}B -> {}",
                model.name(),
                cfg.fields,
                cfg.value_size,
                path.display()
            );
            let r = bench::run_ingest(
                model,
                cfg.fields,
                cfg.value_size,
                target_rows,
                batch_size,
                &path,
                SEED,
                cfg.keep_as_base,
            )
            .await?;
            println!(
                "INGEST|{}|{}|{}|{}|{:.3}|{:.1}|{}|{:.2}|{}",
                model.short(),
                cfg.fields,
                cfg.value_size,
                r.rows,
                r.elapsed.as_secs_f64(),
                r.rows_per_sec(),
                r.db_bytes,
                r.bytes_per_row(),
                r.cut_off,
            );
            eprintln!(
                "  -> {} rows in {:.3}s ({:.0} rows/s), {} bytes on disk ({:.1} B/row){}",
                r.rows,
                r.elapsed.as_secs_f64(),
                r.rows_per_sec(),
                r.db_bytes,
                r.bytes_per_row(),
                if r.cut_off { " [CUT OFF]" } else { "" }
            );
        }
    }

    println!("READ|model|n|avg_us|p50_us|p99_us|max_us");
    println!(
        "UPDATE|model|fields_touched|indexed|pool|rounds|updates|elapsed_s|updates_per_sec|cut_off"
    );
    for model in ALL_MODELS {
        let base_path = dir.join(format!(
            "ingest_{}_{}f_{}b.db",
            model.short(),
            BASE_FIELDS,
            BASE_VALUE_SIZE
        ));

        // Point reads against the untouched base DB.
        {
            let (_db, conn) = db::open_db(&base_path).await?;
            let r = bench::run_point_reads(&conn, model, target_rows as i64, read_samples, SEED ^ 1)
                .await?;
            println!(
                "READ|{}|{}|{:.2}|{:.2}|{:.2}|{:.2}",
                model.short(),
                r.n,
                r.avg_us,
                r.p50_us,
                r.p99_us,
                r.max_us
            );
            eprintln!(
                "[read] {} avg={:.2}us p50={:.2}us p99={:.2}us max={:.2}us",
                model.name(),
                r.avg_us,
                r.p50_us,
                r.p99_us,
                r.max_us
            );
        }

        // Update churn: {1,10} fields touched x {indexed, not}, each on a
        // fresh copy of the same base state.
        let pool: Vec<i64> = (0..pool_size as i64).collect();
        for &fields_touched in &[1usize, 10] {
            for &indexed in &[false, true] {
                let tmp_path = dir.join(format!(
                    "update_{}_{}ft_{}.db",
                    model.short(),
                    fields_touched,
                    if indexed { "idx" } else { "noidx" }
                ));
                rm(&tmp_path);
                std::fs::copy(&base_path, &tmp_path)?;
                // base_path (built by run_ingest) already created item_index
                // (bench.rs run_ingest calls create_index_table unconditionally),
                // so the copy has it too — nothing to create here.
                let (_db, conn) = db::open_db(&tmp_path).await?;
                eprintln!(
                    "[update] {} touched={} indexed={}",
                    model.name(),
                    fields_touched,
                    indexed
                );
                let r = bench::run_update(
                    &conn,
                    model,
                    &pool,
                    BASE_FIELDS,
                    fields_touched,
                    indexed,
                    BASE_VALUE_SIZE,
                    update_rounds,
                    SEED ^ 2,
                )
                .await?;
                println!(
                    "UPDATE|{}|{}|{}|{}|{}|{}|{:.3}|{:.1}|{}",
                    model.short(),
                    fields_touched,
                    indexed,
                    pool.len(),
                    update_rounds,
                    r.updates,
                    r.elapsed.as_secs_f64(),
                    r.updates_per_sec(),
                    r.cut_off,
                );
                eprintln!(
                    "  -> {} updates in {:.3}s ({:.0} updates/s){}",
                    r.updates,
                    r.elapsed.as_secs_f64(),
                    r.updates_per_sec(),
                    if r.cut_off { " [CUT OFF]" } else { "" }
                );
                drop(conn);
                drop(_db);
                rm(&tmp_path);
            }
        }
        rm(&base_path);
    }

    payload_sweep(&dir, update_rounds).await?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// One-off probe (SPIKE_SIZE_PROBE=1): confirms the postcard blob actually
/// differs between String-keyed (b) and u16-keyed (d) encodings, and by how
/// much, independent of any SQLite/Turso page-rounding effects.
fn size_probe() {
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    let mut rng = StdRng::seed_from_u64(SEED);
    let tpl = gen::ValueTemplate::new(&mut rng, 256);
    for &fields in &[5usize, 50, 200] {
        let rec = gen::full_record(&tpl, 12345, fields);
        let map_s: std::collections::BTreeMap<String, models::PValue> = rec
            .iter()
            .map(|(i, v)| (value::field_name(*i), models::PValue::Str(v.clone())))
            .collect();
        let map_u: std::collections::BTreeMap<u16, models::PValue> = rec
            .iter()
            .map(|(i, v)| (*i as u16, models::PValue::Str(v.clone())))
            .collect();
        let blob_s = postcard::to_allocvec(&map_s).unwrap();
        let blob_u = postcard::to_allocvec(&map_u).unwrap();
        println!(
            "fields={fields}: String-keyed blob={} bytes, u16-keyed blob={} bytes, diff={} ({:.2} B/field)",
            blob_s.len(),
            blob_u.len(),
            blob_s.len() as i64 - blob_u.len() as i64,
            (blob_s.len() as i64 - blob_u.len() as i64) as f64 / fields as f64
        );
    }
}

/// One-off probe (SPIKE_DB_SIZE_PROBE=1): ingest a handful of records through
/// the real `models::ingest` dispatch for both (b) and (d), then read back
/// `length(fields)` per row from the DB itself, to rule out a dispatch bug
/// behind the earlier equal-file-size observation.
async fn db_size_probe() -> Result<(), Box<dyn Error>> {
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(SEED);
    let tpl = gen::ValueTemplate::new(&mut rng, 256);
    for model in [Model::Binary, Model::BinaryDeclared] {
        let path = PathBuf::from(format!("probe_{}.db", model.short()));
        rm(&path);
        let (_db, conn) = db::open_db(&path).await?;
        models::create_schema(&conn, model).await?;
        let records: Vec<(i64, Vec<(usize, String)>)> =
            (0..5).map(|id| (id, gen::full_record(&tpl, id, 5))).collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        models::ingest(&conn, model, &records).await?;
        conn.execute("COMMIT", ()).await?;
        let mut rows = conn.query("SELECT item_no, length(fields) FROM items ORDER BY item_no", ()).await?;
        print!("{}: ", model.short());
        while let Some(row) = rows.next().await? {
            let id: i64 = row.get(0)?;
            let len: i64 = row.get(1)?;
            print!("(item {id} len {len}) ");
        }
        println!();
        drop(conn);
        drop(_db);
        rm(&path);
    }
    Ok(())
}

/// One-off probe (SPIKE_PAIR_PROBE=1): binary vs declared at fields=50/256B,
/// alternated three times each in a tight loop, to check whether the large
/// gap seen in the full runs survives outside a ~10-minute window shared with
/// unrelated host load.
async fn pair_probe() -> Result<(), Box<dyn Error>> {
    for round in 0..3 {
        for model in [Model::Binary, Model::BinaryDeclared] {
            let path = PathBuf::from(format!("pair_{}_{round}.db", model.short()));
            rm(&path);
            let r = bench::run_ingest(model, 50, 256, 20_000, 1_000, &path, SEED, false).await?;
            println!(
                "round={round} {:<9} {:.3}s ({:.0} rows/s)",
                model.short(),
                r.elapsed.as_secs_f64(),
                r.rows_per_sec()
            );
        }
    }
    for round in 0..3 {
        for (label, size) in [("16B", 16usize), ("256B", 256)] {
            let path = PathBuf::from(format!("pair_eav_{label}_{round}.db"));
            rm(&path);
            let r = bench::run_ingest(Model::Eav, 50, size, 20_000, 1_000, &path, SEED, false).await?;
            println!(
                "round={round} eav/{label:<5} {:.3}s ({:.0} rows/s)",
                r.elapsed.as_secs_f64(),
                r.rows_per_sec()
            );
        }
    }
    Ok(())
}

/// One-off probe (SPIKE_SQL_PROBE=1) to find which `UPDATE ... FROM`
/// spellings this vendored Turso build actually accepts.
async fn sql_probe() -> Result<(), Box<dyn Error>> {
    let path = std::path::PathBuf::from("probe.db");
    rm(&path);
    let (_db, conn) = db::open_db(&path).await?;
    conn.execute(
        "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields TEXT NOT NULL)",
        (),
    )
    .await?;
    conn.execute("INSERT INTO items VALUES (1,'{}'),(2,'{}')", ())
        .await?;

    let attempts: Vec<(&str, String, Vec<turso::Value>)> = vec![
        (
            "values+alias-cols",
            "UPDATE items SET fields = v.patch FROM (VALUES (?,?),(?,?)) AS v(item_no, patch) \
             WHERE items.item_no = v.item_no"
                .to_string(),
            vec![
                turso::Value::Integer(1),
                turso::Value::Text("\"a\"".into()),
                turso::Value::Integer(2),
                turso::Value::Text("\"b\"".into()),
            ],
        ),
        (
            "values+no-alias-cols(v.column1)",
            "UPDATE items SET fields = v.column2 FROM (VALUES (?,?),(?,?)) AS v \
             WHERE items.item_no = v.column1"
                .to_string(),
            vec![
                turso::Value::Integer(1),
                turso::Value::Text("\"a\"".into()),
                turso::Value::Integer(2),
                turso::Value::Text("\"b\"".into()),
            ],
        ),
        (
            "select-union-all+alias-cols",
            "UPDATE items SET fields = v.patch FROM (SELECT ? AS item_no, ? AS patch \
             UNION ALL SELECT ?, ?) AS v WHERE items.item_no = v.item_no"
                .to_string(),
            vec![
                turso::Value::Integer(1),
                turso::Value::Text("\"a\"".into()),
                turso::Value::Integer(2),
                turso::Value::Text("\"b\"".into()),
            ],
        ),
        (
            "json_patch+values+alias-cols",
            "UPDATE items SET fields = json_patch(fields, v.patch) FROM (VALUES (?,?)) AS v(item_no, patch) \
             WHERE items.item_no = v.item_no"
                .to_string(),
            vec![turso::Value::Integer(1), turso::Value::Text("{\"x\":1}".into())],
        ),
        (
            "on-conflict-upsert",
            "INSERT INTO items (item_no, fields) VALUES (?,?) \
             ON CONFLICT(item_no) DO UPDATE SET fields = excluded.fields"
                .to_string(),
            vec![turso::Value::Integer(1), turso::Value::Text("\"c\"".into())],
        ),
    ];

    for (label, sql, params) in attempts {
        match conn.execute(&sql, params).await {
            Ok(n) => println!("PROBE OK   {label:<32} rows_changed={n}"),
            Err(e) => println!("PROBE FAIL {label:<32} {e}"),
        }
    }
    Ok(())
}

/// Isolates the cost of a large value colocated on the hot row (rewritten by
/// every UPDATE, whether or not the SET clause names it — SQLite/Turso store
/// one serialized record per row) versus the same value in a separate,
/// untouched off-row table. Model (b)'s binary hot row stands in for all
/// four: the effect is a generic row-rewrite cost, not a fields-encoding one.
async fn payload_sweep(dir: &Path, rounds: u64) -> Result<(), Box<dyn Error>> {
    use turso::Value as SqlValue;

    let fields = 10usize;
    let value_size = 256usize;
    let payload_size = 64 * 1024usize;
    let pool_size = 1_000i64;
    let seed = SEED ^ 0xABCD;

    println!("PAYLOAD|variant|pool|rounds|updates|elapsed_s|updates_per_sec|cut_off");

    // -- baseline: fields only, no payload column at all.
    {
        let path = dir.join("payload_baseline.db");
        rm(&path);
        let (_db, conn) = db::open_db(&path).await?;
        conn.execute(
            "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields BLOB NOT NULL)",
            (),
        )
        .await?;
        let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(seed);
        let tpl = gen::ValueTemplate::new(&mut rng, value_size);
        let records: Vec<(i64, Vec<(usize, String)>)> = (0..pool_size)
            .map(|id| (id, gen::full_record(&tpl, id, fields)))
            .collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        models::ingest(&conn, Model::Binary, &records).await?;
        conn.execute("COMMIT", ()).await?;
        // Checkpoint the ingest's WAL pages now, so the timed update loop below
        // never absorbs an auto-checkpoint flush of THIS transaction's pages
        // (which would conflate "off-row storage's update cost" with a one-time
        // WAL-flush of the initial payload write, see results doc methodology).
        db::on_disk_size(&conn, &path).await?;

        let pool: Vec<i64> = (0..pool_size).collect();
        let r = bench::run_update(
            &conn, Model::Binary, &pool, fields, 1, false, value_size, rounds, seed,
        )
        .await?;
        println!(
            "PAYLOAD|baseline_no_payload|{}|{}|{}|{:.3}|{:.1}|{}",
            pool.len(),
            rounds,
            r.updates,
            r.elapsed.as_secs_f64(),
            r.updates_per_sec(),
            r.cut_off
        );
        eprintln!(
            "[payload] baseline (no payload col): {:.0} updates/s",
            r.updates_per_sec()
        );
        drop(conn);
        drop(_db);
        rm(&path);
    }

    // -- inline: payload BLOB colocated on the hot row, never SET, never read.
    {
        let path = dir.join("payload_inline.db");
        rm(&path);
        let (_db, conn) = db::open_db(&path).await?;
        conn.execute(
            "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields BLOB NOT NULL, payload BLOB)",
            (),
        )
        .await?;
        let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(seed);
        let tpl = gen::ValueTemplate::new(&mut rng, value_size);
        use rand::Rng;
        use rand::distributions::Alphanumeric;
        let payload: Vec<u8> = rng
            .sample_iter(&Alphanumeric)
            .take(payload_size)
            .collect();
        let rows: Vec<Vec<SqlValue>> = (0..pool_size)
            .map(|id| {
                let map: std::collections::BTreeMap<String, models::PValue> =
                    gen::full_record(&tpl, id, fields)
                        .into_iter()
                        .map(|(i, v)| (value::field_name(i), models::PValue::Str(v)))
                        .collect();
                let blob = postcard::to_allocvec(&map).expect("postcard encode");
                vec![
                    SqlValue::Integer(id),
                    SqlValue::Blob(blob),
                    SqlValue::Blob(payload.clone()),
                ]
            })
            .collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        sql::exec_chunked(
            &conn,
            "INSERT INTO items (item_no, fields, payload) VALUES ",
            "",
            &rows,
        )
        .await?;
        conn.execute("COMMIT", ()).await?;
        // Checkpoint the ingest's WAL pages now, so the timed update loop below
        // never absorbs an auto-checkpoint flush of THIS transaction's pages
        // (which would conflate "off-row storage's update cost" with a one-time
        // WAL-flush of the initial payload write, see results doc methodology).
        db::on_disk_size(&conn, &path).await?;

        let pool: Vec<i64> = (0..pool_size).collect();
        let r = bench::run_update(
            &conn, Model::Binary, &pool, fields, 1, false, value_size, rounds, seed,
        )
        .await?;
        println!(
            "PAYLOAD|inline_{}kib_payload|{}|{}|{}|{:.3}|{:.1}|{}",
            payload_size / 1024,
            pool.len(),
            rounds,
            r.updates,
            r.elapsed.as_secs_f64(),
            r.updates_per_sec(),
            r.cut_off
        );
        eprintln!(
            "[payload] inline {}KiB payload column: {:.0} updates/s",
            payload_size / 1024,
            r.updates_per_sec()
        );
        drop(conn);
        drop(_db);
        rm(&path);
    }

    // -- off-row: payload lives in a separate table, untouched by field updates.
    {
        let path = dir.join("payload_offrow.db");
        rm(&path);
        let (_db, conn) = db::open_db(&path).await?;
        conn.execute(
            "CREATE TABLE items (item_no INTEGER PRIMARY KEY, fields BLOB NOT NULL)",
            (),
        )
        .await?;
        conn.execute(
            "CREATE TABLE payloads (item_no INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
            (),
        )
        .await?;
        let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(seed);
        let tpl = gen::ValueTemplate::new(&mut rng, value_size);
        use rand::Rng;
        use rand::distributions::Alphanumeric;
        let payload: Vec<u8> = rng
            .sample_iter(&Alphanumeric)
            .take(payload_size)
            .collect();
        let records: Vec<(i64, Vec<(usize, String)>)> = (0..pool_size)
            .map(|id| (id, gen::full_record(&tpl, id, fields)))
            .collect();
        let payload_rows: Vec<Vec<SqlValue>> = (0..pool_size)
            .map(|id| vec![SqlValue::Integer(id), SqlValue::Blob(payload.clone())])
            .collect();
        conn.execute("BEGIN IMMEDIATE", ()).await?;
        models::ingest(&conn, Model::Binary, &records).await?;
        sql::exec_chunked(
            &conn,
            "INSERT INTO payloads (item_no, payload) VALUES ",
            "",
            &payload_rows,
        )
        .await?;
        conn.execute("COMMIT", ()).await?;
        // Checkpoint the ingest's WAL pages now, so the timed update loop below
        // never absorbs an auto-checkpoint flush of THIS transaction's pages
        // (which would conflate "off-row storage's update cost" with a one-time
        // WAL-flush of the initial payload write, see results doc methodology).
        db::on_disk_size(&conn, &path).await?;

        let pool: Vec<i64> = (0..pool_size).collect();
        let r = bench::run_update(
            &conn, Model::Binary, &pool, fields, 1, false, value_size, rounds, seed,
        )
        .await?;
        println!(
            "PAYLOAD|offrow_{}kib_payload|{}|{}|{}|{:.3}|{:.1}|{}",
            payload_size / 1024,
            pool.len(),
            rounds,
            r.updates,
            r.elapsed.as_secs_f64(),
            r.updates_per_sec(),
            r.cut_off
        );
        eprintln!(
            "[payload] off-row {}KiB payload table: {:.0} updates/s",
            payload_size / 1024,
            r.updates_per_sec()
        );
        drop(conn);
        drop(_db);
        rm(&path);
    }

    Ok(())
}
