//! SPIKE-004 measurement harness: claim-path lease representation at scale on
//! the vendored Turso 0.7.2 engine, configured the way
//! `crates/fireweed-turso/src/local.rs` opens its projection (WAL journal,
//! synchronous=NORMAL, cache_size=-131072 KiB, temp_store=MEMORY,
//! wal_autocheckpoint=0 — the `with_log_backed_projection()` profile that
//! `crates/fireweed/src/turso_compose.rs:136` actually opens in production;
//! see `configure_connection` below, copied verbatim from
//! `spikes/key-storage/src/main.rs`, itself copied from `local.rs:3407-3444`).
//!
//! Compares two lease representations for the kernel contract's claim path
//! (draft §1 "lease", §2.3 `claim`; §7.2 claim-order index
//! `fireweed_items_pending_eligible_order_idx`):
//!
//!   L1 "lease columns": `lease_token_hash BLOB`, `lease_expires_at INTEGER`
//!   on the item row (today's shape, `fireweed-relational/src/schema.rs:37-38`).
//!   Claim-order index `(queue_no, stage, rank, seq) WHERE lease_expires_at
//!   IS NULL`. A dedicated `(queue_no, lease_expires_at) WHERE lease_expires_at
//!   IS NOT NULL` index serves the expiry sweep.
//!
//!   L2 "claims table" (the owner's round-4 proposal): a separate `claims`
//!   table `(claim_id INTEGER PK, token_hash, holder, expires_at,
//!   record_count)`, plus a nullable, indexed `claim_id INTEGER` on the item
//!   row. Claim-order index `(queue_no, stage, rank, seq) WHERE claim_id IS
//!   NULL`. `claims(expires_at)` serves the expiry sweep; `items(claim_id)
//!   WHERE claim_id IS NOT NULL` serves finish/extend-by-claim.
//!
//! Standalone crate. Not a member of the fireweed workspace. See
//! docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md for the
//! write-up.

use std::env;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use turso::{Builder, Connection, Value, transaction::TransactionBehavior};

// ---------------------------------------------------------------------------
// Config knobs (env-overridable, defaults chosen for a ~3-repeat sweep).
// ---------------------------------------------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn ingest_batch_size() -> usize { env_usize("FW_SPIKE_BATCH_SIZE", 1000) }
fn claim_batch() -> usize { env_usize("FW_SPIKE_CLAIM_BATCH", 100) }
fn claim_trials() -> usize { env_usize("FW_SPIKE_CLAIM_TRIALS", 3) }
fn claim_reps() -> usize { env_usize("FW_SPIKE_CLAIM_REPS", 100) }
fn extend_batch() -> usize { env_usize("FW_SPIKE_EXTEND_BATCH", 1000) }
fn extend_trials() -> usize { env_usize("FW_SPIKE_EXTEND_TRIALS", 3) }
fn extend_reps() -> usize { env_usize("FW_SPIKE_EXTEND_REPS", 20) }
fn sweep_size() -> usize { env_usize("FW_SPIKE_SWEEP_SIZE", 10_000) }
fn sweep_trials() -> usize { env_usize("FW_SPIKE_SWEEP_TRIALS", 3) }
fn sweep_subbatch() -> usize { env_usize("FW_SPIKE_SWEEP_SUBBATCH", 100) }
fn blind_reps() -> usize { env_usize("FW_SPIKE_BLIND_REPS", 3) }
fn lease_micros() -> i64 { env_i64("FW_SPIKE_LEASE_MICROS", 30_000_000) } // 30s

const QUEUE_NO: i64 = 1;
const STAGE_INITIAL: i64 = 0;
const STAGE_COMPLETED: i64 = 1;
const FIELDS_PAYLOAD_HEX_LEN: usize = 292; // -> ~300B wrapped as `{"d":"<hex>"}`

// ---------------------------------------------------------------------------
// Deterministic generation (SplitMix64 finalizer, a bijection over u64 so
// distinct row indices never collide — same approach as spikes/key-storage).
// ---------------------------------------------------------------------------

fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn rand_hex_string(seed: u64, len: usize) -> String {
    let mut out = String::with_capacity(len + 16);
    let mut counter: u64 = 0;
    while out.len() < len {
        let v = mix64(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(counter));
        let _ = write!(out, "{v:016x}");
        counter += 1;
    }
    out.truncate(len);
    out
}

/// Pseudo-random but reproducible selection of `n_selected` distinct queue
/// numbers out of `1..=n_queues_total` (mix64 over an index, modulo range,
/// dedup by retry). Shared by multi_claim and finding8.
fn select_queues(n_queues_total: i64, n_selected: usize) -> Vec<i64> {
    let mut selected: Vec<i64> = Vec::with_capacity(n_selected);
    let mut seen = std::collections::HashSet::new();
    let mut i: u64 = 0;
    while selected.len() < n_selected {
        let q = 1 + (mix64(0xA5A5_A5A5_A5A5_A5A5 ^ i) % (n_queues_total as u64)) as i64;
        if seen.insert(q) {
            selected.push(q);
        }
        i += 1;
    }
    selected
}

/// 16-byte BLOB key, upstream-id-shaped (e.g. a UUID), unique per row index.
fn key_blob(i: u64) -> Vec<u8> {
    let a = mix64(i ^ 0x1111_1111_1111_1111);
    let b = mix64(i ^ 0x2222_2222_2222_2222);
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&a.to_be_bytes());
    v.extend_from_slice(&b.to_be_bytes());
    v
}

/// ~300-byte `fields` TEXT: a single opaque string field wrapped as JSON,
/// matching `fireweed_items.fields TEXT` (`schema.rs:29`) shape without
/// needing the field to be semantically meaningful for this spike.
fn fields_text(i: u64) -> String {
    format!("{{\"d\":\"{}\"}}", rand_hex_string(i ^ 0x3333_3333_3333_3333, FIELDS_PAYLOAD_HEX_LEN))
}

fn now_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64
}

// ---------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    L1,
    L2,
}

impl Layout {
    fn parse(s: &str) -> Self {
        match s {
            "l1" => Layout::L1,
            "l2" => Layout::L2,
            other => panic!("unknown layout {other} (want l1|l2)"),
        }
    }
}
impl std::fmt::Display for Layout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// DDL statements for a layout, in execution order. `with_claim_index`
/// toggles only the `(queue_no, stage, rank, seq)` claim-order partial
/// index, so `size_baseline` can isolate its on-disk cost by delta against
/// an otherwise-identical schema (Turso 0.7.2 has no `dbstat` — see
/// spikes/key-storage's SPIKE-002 Finding, still true on this vendored
/// version: `grep -rli dbstat vendor/turso_core/ vendor/turso/` finds only a
/// comment referencing upstream sqlite3's dbstat.c, no registered vtab).
fn schema_ddl(layout: Layout, with_claim_index: bool) -> Vec<String> {
    let mut ddl = Vec::new();
    match layout {
        Layout::L1 => {
            ddl.push(
                "CREATE TABLE items (\n\
                    queue_no INTEGER NOT NULL,\n\
                    seq INTEGER NOT NULL,\n\
                    key BLOB NOT NULL,\n\
                    stage INTEGER NOT NULL,\n\
                    rank INTEGER NOT NULL,\n\
                    fields TEXT NOT NULL,\n\
                    version INTEGER NOT NULL,\n\
                    lease_token_hash BLOB,\n\
                    lease_expires_at INTEGER,\n\
                    PRIMARY KEY (queue_no, seq)\n\
                )"
                    .to_string(),
            );
            ddl.push("CREATE UNIQUE INDEX items_key_idx ON items (queue_no, key)".to_string());
            if with_claim_index {
                ddl.push(
                    "CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, seq) \
                     WHERE lease_expires_at IS NULL"
                        .to_string(),
                );
            }
            ddl.push(
                "CREATE INDEX items_lease_expiry_idx ON items (queue_no, lease_expires_at) \
                 WHERE lease_expires_at IS NOT NULL"
                    .to_string(),
            );
        }
        Layout::L2 => {
            ddl.push(
                "CREATE TABLE items (\n\
                    queue_no INTEGER NOT NULL,\n\
                    seq INTEGER NOT NULL,\n\
                    key BLOB NOT NULL,\n\
                    stage INTEGER NOT NULL,\n\
                    rank INTEGER NOT NULL,\n\
                    fields TEXT NOT NULL,\n\
                    version INTEGER NOT NULL,\n\
                    claim_id INTEGER,\n\
                    PRIMARY KEY (queue_no, seq)\n\
                )"
                    .to_string(),
            );
            ddl.push("CREATE UNIQUE INDEX items_key_idx ON items (queue_no, key)".to_string());
            if with_claim_index {
                ddl.push(
                    "CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, seq) \
                     WHERE claim_id IS NULL"
                        .to_string(),
                );
            }
            ddl.push(
                "CREATE INDEX items_claim_id_idx ON items (claim_id) WHERE claim_id IS NOT NULL"
                    .to_string(),
            );
            ddl.push(
                "CREATE TABLE claims (\n\
                    claim_id INTEGER PRIMARY KEY,\n\
                    token_hash BLOB NOT NULL,\n\
                    holder TEXT,\n\
                    expires_at INTEGER NOT NULL,\n\
                    record_count INTEGER NOT NULL\n\
                )"
                    .to_string(),
            );
            ddl.push("CREATE INDEX claims_expires_idx ON claims (expires_at)".to_string());
        }
    }
    ddl
}

fn free_predicate(layout: Layout) -> &'static str {
    match layout {
        Layout::L1 => "lease_expires_at IS NULL",
        Layout::L2 => "claim_id IS NULL",
    }
}

// ---------------------------------------------------------------------------
// Lean claim-path row (SPIKE-004 Finding 9, testing SPIKE-006 layout C): the
// items row carries NO field bytes at all — `fields TEXT` is dropped from the
// table entirely, leaving exactly `queue_no, seq, key, stage, rank, version,
// claim_id` (this spike's already-established field set minus `fields`; see
// SPIKE-004's own Limitations note that `attempts`/`gate`/`group` are out of
// this spike's scope, unchanged here). An optional separate `fields` table
// `(queue_no, seq) -> TEXT` holds the same ~300B payload bulk_build's fat
// layout puts inline, populated 1:1 with the item rows so the file's total
// working set is comparable in size — but the claim path (do_claim/
// do_claim_q/do_finish/do_extend/do_sweep) never references it, by
// construction (none of those statements select or update `fields` on
// either layout, fat or lean; grep confirms). Only implemented for L2, the
// Recommendation — this is not a new lease representation, it is the same
// L2 claims-table design with a narrower base row.
fn schema_ddl_lean(with_claim_index: bool) -> Vec<String> {
    let mut ddl = Vec::new();
    ddl.push(
        "CREATE TABLE items (\n\
            queue_no INTEGER NOT NULL,\n\
            seq INTEGER NOT NULL,\n\
            key BLOB NOT NULL,\n\
            stage INTEGER NOT NULL,\n\
            rank INTEGER NOT NULL,\n\
            version INTEGER NOT NULL,\n\
            claim_id INTEGER,\n\
            PRIMARY KEY (queue_no, seq)\n\
        )"
            .to_string(),
    );
    ddl.push("CREATE UNIQUE INDEX items_key_idx ON items (queue_no, key)".to_string());
    if with_claim_index {
        ddl.push(
            "CREATE INDEX items_claim_idx ON items (queue_no, stage, rank, seq) \
             WHERE claim_id IS NULL"
                .to_string(),
        );
    }
    ddl.push(
        "CREATE INDEX items_claim_id_idx ON items (claim_id) WHERE claim_id IS NOT NULL"
            .to_string(),
    );
    ddl.push(
        "CREATE TABLE claims (\n\
            claim_id INTEGER PRIMARY KEY,\n\
            token_hash BLOB NOT NULL,\n\
            holder TEXT,\n\
            expires_at INTEGER NOT NULL,\n\
            record_count INTEGER NOT NULL\n\
        )"
            .to_string(),
    );
    ddl.push("CREATE INDEX claims_expires_idx ON claims (expires_at)".to_string());
    ddl
}

/// The lean layout's optional fields table: same ~300B payload per record as
/// the fat layout's inline `fields` column (same `hex(randomblob(146))`
/// wrapped-JSON shape bulk_build's fat INSERT..SELECT already uses), in a
/// separate table keyed by (queue_no, seq). Never touched by the
/// claim/finish/extend/sweep path — present only so the file's total working
/// set (what page cache must hold) is comparable to the fat layout's,
/// matching the task's "so what the working set contains is comparable".
const FIELDS_TABLE_DDL: &str = "CREATE TABLE fields (\n\
    queue_no INTEGER NOT NULL,\n\
    seq INTEGER NOT NULL,\n\
    fields TEXT NOT NULL,\n\
    PRIMARY KEY (queue_no, seq)\n\
)";

// ---------------------------------------------------------------------------
// Connection configuration — verbatim from spikes/key-storage/src/main.rs,
// itself mirroring crates/fireweed-turso/src/local.rs:3407-3444
// `configure_connection` under the `with_log_backed_projection()` profile
// production opens (`crates/fireweed/src/turso_compose.rs:136`).
// ---------------------------------------------------------------------------

async fn configure_connection(connection: &Connection) -> turso::Result<()> {
    connection.pragma_update("page_size", "4096").await?;
    connection.pragma_update("journal_mode", "WAL").await?;
    connection.pragma_update("synchronous", "NORMAL").await?;
    // Production's default is -131072 (128 MiB). Finding 9's cache_size
    // ablation (128 MiB vs 1 GiB) overrides this via FW_SPIKE_CACHE_SIZE
    // (a raw `cache_size` pragma value, e.g. "-1048576" for 1 GiB) rather
    // than hardcoding a second code path — every other subcommand is
    // unaffected because the default here is unchanged.
    let cache_size = env::var("FW_SPIKE_CACHE_SIZE").unwrap_or_else(|_| "-131072".to_string());
    connection.pragma_update("cache_size", cache_size.as_str()).await?;
    connection.pragma_update("temp_store", "MEMORY").await?;
    connection.pragma_update("wal_autocheckpoint", "0").await?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(())
}

/// Diagnostic only (`FW_SPIKE_IOTRACE=1`): (rchar, wchar, syscr, syscw) deltas
/// from /proc/self/io, to localize which phase drives unexpected I/O volume.
fn io_snapshot() -> (u64, u64, u64, u64) {
    let text = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    let mut rchar = 0u64;
    let mut wchar = 0u64;
    let mut syscr = 0u64;
    let mut syscw = 0u64;
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("rchar: ") {
            rchar = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("wchar: ") {
            wchar = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("syscr: ") {
            syscr = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("syscw: ") {
            syscw = v.trim().parse().unwrap_or(0);
        }
    }
    (rchar, wchar, syscr, syscw)
}

fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn checkpoint(conn: &Connection) -> turso::Result<()> {
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

/// Mirrors `crates/fireweed-turso/src/projection.rs`
/// `maybe_passive_checkpoint_log_backed` / `LOG_BACKED_CHECKPOINT_TRY_BYTES`:
/// a log-backed connection (`wal_autocheckpoint=0`) is never auto-checkpointed
/// by SQLite/Turso itself, so production issues an explicit `PRAGMA
/// wal_checkpoint(PASSIVE)` after every apply batch once the WAL exceeds this
/// byte threshold (`projection.rs:127-129`, "Idle/close and a process-wide
/// Passive cap bound the WAL instead" — `local.rs:3403`). Without this, a
/// sustained loop of many small write transactions lets the WAL grow without
/// bound for the whole run — measured (not this constant, the *absence* of
/// this call) to make per-transaction cost escalate severely as cumulative
/// WAL size grows; see the results doc's methodology note. This spike is a
/// single connection with no multi-store stampede risk, so it skips
/// production's two-permit semaphore and the harder MUST-checkpoint tier —
/// only the same TRY threshold and the same PASSIVE (non-blocking) pragma.
const LOG_BACKED_CHECKPOINT_TRY_BYTES: u64 = 400 * 1024 * 1024;

async fn maybe_passive_checkpoint(conn: &Connection, wal_path: &str) -> turso::Result<()> {
    if file_len(wal_path) < LOG_BACKED_CHECKPOINT_TRY_BYTES {
        return Ok(());
    }
    let mut rows = conn.query("PRAGMA wal_checkpoint(PASSIVE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

fn remove_db_files(db_path: &str) {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

// ---------------------------------------------------------------------------
// Percentile / spread helpers
// ---------------------------------------------------------------------------

fn percentile_us(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx].as_secs_f64() * 1_000_000.0
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

async fn ingest_stage(
    conn: &mut Connection,
    layout: Layout,
    stage: i64,
    start_seq: i64,
    count: i64,
    base_ts: i64,
    wal_path: &str,
) -> turso::Result<()> {
    let batch = ingest_batch_size() as i64;
    let insert_sql = match layout {
        Layout::L1 => {
            "INSERT INTO items (queue_no,seq,key,stage,rank,fields,version,lease_token_hash,lease_expires_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,NULL,NULL)"
        }
        Layout::L2 => {
            "INSERT INTO items (queue_no,seq,key,stage,rank,fields,version,claim_id) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,NULL)"
        }
    };
    let mut i = 0i64;
    while i < count {
        let end = (i + batch).min(count);
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        {
            let mut stmt = tx.prepare(insert_sql).await?;
            for row in i..end {
                let seq = start_seq + row;
                let rank = base_ts + row; // monotonic, distinct sort keys (not all-equal rank)
                stmt.execute((
                    Value::Integer(QUEUE_NO),
                    Value::Integer(seq),
                    Value::Blob(key_blob(seq as u64)),
                    Value::Integer(stage),
                    Value::Integer(rank),
                    Value::Text(fields_text(seq as u64)),
                    Value::Integer(1),
                ))
                .await?;
            }
        }
        tx.commit().await?;
        maybe_passive_checkpoint(conn, wal_path).await?;
        i = end;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Claim (leases `n` free INITIAL rows in rank order). Returns the leased
// row seqs and (for L2) the claim_id.
// ---------------------------------------------------------------------------

struct ClaimOutcome {
    seqs: Vec<i64>,
    claim_id: Option<i64>,
}

async fn do_claim(
    conn: &mut Connection,
    layout: Layout,
    n: usize,
    expires_at: i64,
    claim_id_counter: &mut i64,
    wal_path: &str,
) -> turso::Result<ClaimOutcome> {
    let select_sql = format!(
        "SELECT seq FROM items INDEXED BY items_claim_idx \
         WHERE queue_no=?1 AND stage=?2 AND {} ORDER BY rank, seq LIMIT {}",
        free_predicate(layout),
        n
    );
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    let seqs: Vec<i64> = {
        let mut stmt = tx.prepare(&select_sql).await?;
        let mut rows = stmt.query((Value::Integer(QUEUE_NO), Value::Integer(STAGE_INITIAL))).await?;
        let mut out = Vec::with_capacity(n);
        while let Some(row) = rows.next().await? {
            if let Value::Integer(seq) = row.get_value(0)? {
                out.push(seq);
            }
        }
        out
    };
    let claim_id = match layout {
        Layout::L1 => {
            let mut stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY sqlite_autoindex_items_1 \
                     SET lease_token_hash=?1, lease_expires_at=?2 WHERE queue_no=?3 AND seq=?4",
                )
                .await?;
            let token = key_blob(expires_at as u64);
            for &seq in &seqs {
                stmt.execute((
                    Value::Blob(token.clone()),
                    Value::Integer(expires_at),
                    Value::Integer(QUEUE_NO),
                    Value::Integer(seq),
                ))
                .await?;
            }
            None
        }
        Layout::L2 => {
            let cid = *claim_id_counter;
            *claim_id_counter += 1;
            let mut ins = tx
                .prepare(
                    "INSERT INTO claims (claim_id,token_hash,holder,expires_at,record_count) \
                     VALUES (?1,?2,?3,?4,?5)",
                )
                .await?;
            ins.execute((
                Value::Integer(cid),
                Value::Blob(key_blob(cid as u64)),
                Value::Text("worker-0".to_string()),
                Value::Integer(expires_at),
                Value::Integer(seqs.len() as i64),
            ))
            .await?;
            let mut upd = tx
                .prepare("UPDATE items INDEXED BY sqlite_autoindex_items_1 SET claim_id=?1 WHERE queue_no=?2 AND seq=?3")
                .await?;
            for &seq in &seqs {
                upd.execute((Value::Integer(cid), Value::Integer(QUEUE_NO), Value::Integer(seq))).await?;
            }
            Some(cid)
        }
    };
    tx.commit().await?;
    maybe_passive_checkpoint(conn, wal_path).await?;
    Ok(ClaimOutcome { seqs, claim_id })
}

/// Finish: release the lease and move the claimed rows to STAGE_COMPLETED.
async fn do_finish(conn: &mut Connection, layout: Layout, queue_no: i64, outcome: &ClaimOutcome, wal_path: &str) -> turso::Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    match layout {
        Layout::L1 => {
            let mut stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY sqlite_autoindex_items_1 \
                     SET stage=?1, lease_token_hash=NULL, lease_expires_at=NULL, \
                     version=version+1 WHERE queue_no=?2 AND seq=?3",
                )
                .await?;
            for &seq in &outcome.seqs {
                stmt.execute((Value::Integer(STAGE_COMPLETED), Value::Integer(queue_no), Value::Integer(seq)))
                    .await?;
            }
        }
        Layout::L2 => {
            // Bulk UPDATE keyed by claim_id via items_claim_id_idx, mirroring
            // do_sweep's own clear-by-claim_id pattern below, instead of a
            // 100-row by-PK loop. All rows in one finish() share the same
            // claim_id (that is the whole point of the owner's design), so
            // this is a single indexed statement instead of `batch` separate
            // PK descents. Root-caused via `spike diag` against the
            // persisted 10M-row db: the by-PK loop measurably worsened L2's
            // page-cache pressure against the ~9.5M-entry items_claim_idx at
            // 10M-row scale (see the results doc's regression finding) —
            // this fix does not fully eliminate that residual cache
            // pressure (a real property of the claims-table design at this
            // scale under production's fixed 128 MiB cache_size), but
            // removes the self-inflicted extra cost of not using the
            // claim_id grouping key L2 already has, the same way the sweep
            // already does below.
            let mut stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY items_claim_id_idx \
                     SET stage=?1, claim_id=NULL, version=version+1 \
                     WHERE claim_id=?2 AND claim_id IS NOT NULL",
                )
                .await?;
            stmt.execute((Value::Integer(STAGE_COMPLETED), Value::Integer(outcome.claim_id.expect("L2 finish needs a claim_id"))))
                .await?;
            let mut del = tx.prepare("DELETE FROM claims WHERE claim_id=?1").await?;
            del.execute((Value::Integer(outcome.claim_id.expect("L2 finish needs a claim_id")),)).await?;
        }
    }
    tx.commit().await?;
    maybe_passive_checkpoint(conn, wal_path).await?;
    Ok(())
}

/// Extend: push the claim's expiry further into the future.
async fn do_extend(
    conn: &mut Connection,
    layout: Layout,
    queue_no: i64,
    outcome: &ClaimOutcome,
    new_expires_at: i64,
    wal_path: &str,
) -> turso::Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    match layout {
        Layout::L1 => {
            let mut stmt = tx
                .prepare("UPDATE items INDEXED BY sqlite_autoindex_items_1 SET lease_expires_at=?1 WHERE queue_no=?2 AND seq=?3")
                .await?;
            for &seq in &outcome.seqs {
                stmt.execute((Value::Integer(new_expires_at), Value::Integer(queue_no), Value::Integer(seq)))
                    .await?;
            }
        }
        Layout::L2 => {
            let mut stmt = tx.prepare("UPDATE claims SET expires_at=?1 WHERE claim_id=?2").await?;
            stmt.execute((Value::Integer(new_expires_at), Value::Integer(outcome.claim_id.expect("L2 extend needs a claim_id"))))
                .await?;
        }
    }
    tx.commit().await?;
    maybe_passive_checkpoint(conn, wal_path).await?;
    Ok(())
}

/// Expiry sweep: find and reclaim leases expired before `now`. Returns
/// (discovery_elapsed, total_elapsed, item_rows_reclaimed, claims_touched —
/// `None` for L1, which has no separate claims table to scan).
async fn do_sweep(
    conn: &mut Connection,
    layout: Layout,
    queue_no: i64,
    now: i64,
    wal_path: &str,
) -> turso::Result<(Duration, Duration, usize, Option<usize>)> {
    let sweep_start = Instant::now();
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    match layout {
        Layout::L1 => {
            let discovery_start = Instant::now();
            let seqs: Vec<i64> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT seq FROM items INDEXED BY items_lease_expiry_idx \
                         WHERE queue_no=?1 AND lease_expires_at IS NOT NULL AND lease_expires_at < ?2",
                    )
                    .await?;
                let mut rows = stmt.query((Value::Integer(queue_no), Value::Integer(now))).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    if let Value::Integer(seq) = row.get_value(0)? {
                        out.push(seq);
                    }
                }
                out
            };
            let discovery_elapsed = discovery_start.elapsed();
            let n = seqs.len();
            let mut stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY sqlite_autoindex_items_1 \
                     SET lease_token_hash=NULL, lease_expires_at=NULL WHERE queue_no=?1 AND seq=?2",
                )
                .await?;
            for seq in seqs {
                stmt.execute((Value::Integer(queue_no), Value::Integer(seq))).await?;
            }
            tx.commit().await?;
            maybe_passive_checkpoint(conn, wal_path).await?;
            Ok((discovery_elapsed, sweep_start.elapsed(), n, None))
        }
        Layout::L2 => {
            let discovery_start = Instant::now();
            let claims: Vec<(i64, i64)> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT claim_id, record_count FROM claims INDEXED BY claims_expires_idx \
                         WHERE expires_at < ?1",
                    )
                    .await?;
                let mut rows = stmt.query((Value::Integer(now),)).await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    if let (Value::Integer(cid), Value::Integer(rc)) = (row.get_value(0)?, row.get_value(1)?) {
                        out.push((cid, rc));
                    }
                }
                out
            };
            let discovery_elapsed = discovery_start.elapsed();
            let mut clear_stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY items_claim_id_idx SET claim_id=NULL \
                     WHERE claim_id=?1 AND claim_id IS NOT NULL",
                )
                .await?;
            let mut del_stmt = tx.prepare("DELETE FROM claims WHERE claim_id=?1").await?;
            let mut item_rows_reclaimed = 0usize;
            for &(cid, rc) in &claims {
                clear_stmt.execute((Value::Integer(cid),)).await?;
                del_stmt.execute((Value::Integer(cid),)).await?;
                item_rows_reclaimed += rc as usize;
            }
            let claims_touched = claims.len();
            tx.commit().await?;
            maybe_passive_checkpoint(conn, wal_path).await?;
            Ok((discovery_elapsed, sweep_start.elapsed(), item_rows_reclaimed, Some(claims_touched)))
        }
    }
}

// ---------------------------------------------------------------------------
// EXPLAIN QUERY PLAN diagnostics
// ---------------------------------------------------------------------------

async fn explain(conn: &Connection, label: &str, sql: &str, params: Vec<Value>) -> turso::Result<()> {
    let plan_sql = format!("EXPLAIN QUERY PLAN {sql}");
    let mut stmt = conn.prepare(&plan_sql).await?;
    let mut rows = stmt.query(params).await?;
    eprintln!("-- {label}: {sql}");
    while let Some(row) = rows.next().await? {
        eprintln!("   {:?}", row.get_value(row.column_count() - 1)?);
    }
    Ok(())
}

async fn run_explain(layout: Layout, db_path: &str) -> turso::Result<()> {
    remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;
    for stmt in schema_ddl(layout, true) {
        conn.execute(&stmt, ()).await?;
    }
    let base_ts = now_micros();
    ingest_stage(&mut conn, layout, STAGE_COMPLETED, 0, 1900, base_ts, &wal_path).await?;
    ingest_stage(&mut conn, layout, STAGE_INITIAL, 1900, 100, base_ts, &wal_path).await?;

    let bound_sql = format!(
        "SELECT seq FROM items INDEXED BY items_claim_idx WHERE queue_no=?1 AND stage=?2 AND {} \
         ORDER BY rank, seq LIMIT 100",
        free_predicate(layout)
    );
    explain(&conn, "stage-BOUND claim select", &bound_sql, vec![Value::Integer(QUEUE_NO), Value::Integer(STAGE_INITIAL)]).await?;

    match layout {
        Layout::L1 => {
            explain(
                &conn,
                "L1 claim lease-set UPDATE (by PK)",
                "UPDATE items SET lease_token_hash=?1, lease_expires_at=?2 WHERE queue_no=?3 AND seq=?4",
                vec![Value::Blob(vec![0u8; 16]), Value::Integer(0), Value::Integer(QUEUE_NO), Value::Integer(0)],
            )
            .await?;
            explain(
                &conn,
                "L1 finish UPDATE (by PK)",
                "UPDATE items SET stage=?1, lease_token_hash=NULL, lease_expires_at=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
                vec![Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(0)],
            )
            .await?;
            explain(
                &conn,
                "L1 finish UPDATE, INDEXED BY sqlite_autoindex_items_1 forced",
                "UPDATE items INDEXED BY sqlite_autoindex_items_1 SET stage=?1, lease_token_hash=NULL, lease_expires_at=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
                vec![Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(0)],
            )
            .await?;
        }
        Layout::L2 => {
            explain(
                &conn,
                "L2 claim claim_id-set UPDATE (by PK)",
                "UPDATE items SET claim_id=?1 WHERE queue_no=?2 AND seq=?3",
                vec![Value::Integer(1), Value::Integer(QUEUE_NO), Value::Integer(0)],
            )
            .await?;
            explain(
                &conn,
                "L2 finish UPDATE (by PK, unhinted) -- historical: Finding 0's original wrong-plan defect, no longer shipped",
                "UPDATE items SET stage=?1, claim_id=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
                vec![Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(0)],
            )
            .await?;
            explain(
                &conn,
                "L2 finish UPDATE, AS SHIPPED: bulk by claim_id via items_claim_id_idx (fixes the 10M-scale finish regression, mirrors do_sweep's own pattern)",
                "UPDATE items INDEXED BY items_claim_id_idx SET stage=?1, claim_id=NULL, version=version+1 \
                 WHERE claim_id=?2 AND claim_id IS NOT NULL",
                vec![Value::Integer(STAGE_COMPLETED), Value::Integer(1)],
            )
            .await?;
            explain(
                &conn,
                "L2 sweep clear-by-claim_id UPDATE",
                "UPDATE items SET claim_id=NULL WHERE claim_id=?1",
                vec![Value::Integer(1)],
            )
            .await?;
            explain(
                &conn,
                "L2 sweep clear-by-claim_id UPDATE, IS NOT NULL made explicit for the partial index",
                "UPDATE items INDEXED BY items_claim_id_idx SET claim_id=NULL \
                 WHERE claim_id=?1 AND claim_id IS NOT NULL",
                vec![Value::Integer(1)],
            )
            .await?;
        }
    }

    let blind_sql = format!(
        "SELECT seq FROM items WHERE queue_no=?1 AND {} ORDER BY rank, seq LIMIT 100",
        free_predicate(layout)
    );
    explain(&conn, "stage-BLIND claim select (no stage predicate)", &blind_sql, vec![Value::Integer(QUEUE_NO)]).await?;

    let blind_hinted_sql = format!(
        "SELECT seq FROM items INDEXED BY items_claim_idx WHERE queue_no=?1 AND {} \
         ORDER BY rank, seq LIMIT 100",
        free_predicate(layout)
    );
    explain(&conn, "stage-BLIND claim select, INDEXED BY forced", &blind_hinted_sql, vec![Value::Integer(QUEUE_NO)]).await?;

    match layout {
        Layout::L1 => {
            explain(
                &conn,
                "L1 sweep discovery",
                "SELECT seq FROM items INDEXED BY items_lease_expiry_idx WHERE queue_no=?1 AND lease_expires_at IS NOT NULL AND lease_expires_at < ?2",
                vec![Value::Integer(QUEUE_NO), Value::Integer(now_micros())],
            )
            .await?;
        }
        Layout::L2 => {
            explain(
                &conn,
                "L2 sweep discovery",
                "SELECT claim_id FROM claims INDEXED BY claims_expires_idx WHERE expires_at < ?1",
                vec![Value::Integer(now_micros())],
            )
            .await?;
        }
    }
    drop(conn);
    remove_db_files(db_path);
    Ok(())
}

// ---------------------------------------------------------------------------
// size_baseline: ingest only, with/without the claim-order index, for the
// index-only-cost delta (Turso has no dbstat — see schema_ddl doc comment).
// ---------------------------------------------------------------------------

async fn run_size_baseline(layout: Layout, db_path: &str, rows_completed: i64, rows_initial: i64) -> turso::Result<()> {
    remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;
    for stmt in schema_ddl(layout, false) {
        conn.execute(&stmt, ()).await?;
    }
    let base_ts = now_micros();
    let ingest_start = Instant::now();
    ingest_stage(&mut conn, layout, STAGE_COMPLETED, 0, rows_completed, base_ts, &wal_path).await?;
    ingest_stage(&mut conn, layout, STAGE_INITIAL, rows_completed, rows_initial, base_ts, &wal_path).await?;
    let ingest_elapsed = ingest_start.elapsed();
    checkpoint(&conn).await?;
    let main_bytes = file_len(db_path);
    println!(
        "{{\"phase\":\"size_baseline\",\"layout\":\"{layout}\",\"with_claim_index\":false,\
         \"rows_completed\":{rows_completed},\"rows_initial\":{rows_initial},\
         \"ingest_ms\":{ingest_ms:.1},\"main_bytes\":{main_bytes}}}",
        ingest_ms = ingest_elapsed.as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();
    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// diag: opens an EXISTING db (already ingested) and runs a handful of
// instrumented claim+finish reps, breaking each finish down into its
// component statements and reporting WAL file size + whether
// maybe_passive_checkpoint actually issued a checkpoint and how long that
// pragma alone took. Root-causes the L2-finish-at-10M regression without
// re-ingesting. Diagnostic only, not part of the measured findings.
// ---------------------------------------------------------------------------
async fn run_diag(layout: Layout, db_path: &str, n_reps: usize) -> turso::Result<()> {
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;
    if let Ok(cs) = env::var("FW_SPIKE_DIAG_CACHE_SIZE") {
        eprintln!("[diag] overriding cache_size={cs} (diagnostic ablation only)");
        conn.pragma_update("cache_size", cs.as_str()).await?;
    }

    if layout == Layout::L2 {
        let mut stmt = conn.prepare("SELECT count(*) FROM claims").await?;
        let mut rows = stmt.query(()).await?;
        if let Some(row) = rows.next().await? {
            eprintln!("[diag] claims row count = {:?}", row.get_value(0)?);
        }
    }
    eprintln!("[diag] wal_path={wal_path} initial wal_bytes={}", file_len(&wal_path));

    if layout == Layout::L2 {
        explain(&conn, "L2 finish claims DELETE (by PK), AT SCALE, unhinted", "DELETE FROM claims WHERE claim_id=?1", vec![Value::Integer(1)]).await?;
        explain(&conn, "L2 finish items UPDATE, AT SCALE, INDEXED BY forced (as shipped)",
            "UPDATE items INDEXED BY sqlite_autoindex_items_1 SET stage=?1, claim_id=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
            vec![Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(0)]).await?;
    }

    let mut claim_id_counter: i64 = 10_000_000;
    for rep in 0..n_reps {
        let wal_before = file_len(&wal_path);
        let expires_at = now_micros() + lease_micros();
        let t_claim = Instant::now();
        let outcome = do_claim(&mut conn, layout, claim_batch(), expires_at, &mut claim_id_counter, &wal_path).await?;
        let claim_elapsed = t_claim.elapsed();
        let wal_after_claim = file_len(&wal_path);

        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        let t_items = Instant::now();
        match layout {
            Layout::L1 => {
                let mut stmt = tx.prepare(
                    "UPDATE items INDEXED BY sqlite_autoindex_items_1 \
                     SET stage=?1, lease_token_hash=NULL, lease_expires_at=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
                ).await?;
                for &seq in &outcome.seqs {
                    stmt.execute((Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(seq))).await?;
                }
            }
            Layout::L2 => {
                if env::var("FW_SPIKE_DIAG_BULK_FINISH").is_ok() {
                    // Ablation: ONE bulk UPDATE keyed by claim_id (via
                    // items_claim_id_idx) instead of 100 per-row PK updates —
                    // possible only because L2's claim_id groups the whole
                    // claim, a grouping L1 has no equivalent of.
                    let mut stmt = tx.prepare(
                        "UPDATE items INDEXED BY items_claim_id_idx \
                         SET stage=?1, claim_id=NULL, version=version+1 \
                         WHERE claim_id=?2 AND claim_id IS NOT NULL",
                    ).await?;
                    stmt.execute((Value::Integer(STAGE_COMPLETED), Value::Integer(outcome.claim_id.expect("claim_id")))).await?;
                } else {
                    let mut stmt = tx.prepare(
                        "UPDATE items INDEXED BY sqlite_autoindex_items_1 \
                         SET stage=?1, claim_id=NULL, version=version+1 WHERE queue_no=?2 AND seq=?3",
                    ).await?;
                    for &seq in &outcome.seqs {
                        stmt.execute((Value::Integer(STAGE_COMPLETED), Value::Integer(QUEUE_NO), Value::Integer(seq))).await?;
                    }
                }
            }
        }
        let items_elapsed = t_items.elapsed();
        let t_claims_del = Instant::now();
        if layout == Layout::L2 {
            let mut del = tx.prepare("DELETE FROM claims WHERE claim_id=?1").await?;
            del.execute((Value::Integer(outcome.claim_id.expect("claim_id")),)).await?;
        }
        let claims_del_elapsed = t_claims_del.elapsed();
        let t_commit = Instant::now();
        tx.commit().await?;
        let commit_elapsed = t_commit.elapsed();

        let wal_before_cp = file_len(&wal_path);
        let will_checkpoint = wal_before_cp >= LOG_BACKED_CHECKPOINT_TRY_BYTES;
        let t_cp = Instant::now();
        maybe_passive_checkpoint(&conn, &wal_path).await?;
        let cp_elapsed = t_cp.elapsed();
        let wal_after_cp = file_len(&wal_path);

        eprintln!(
            "[diag] rep={rep} wal_before_claim={wal_before} wal_after_claim={wal_after_claim} \
             claim_us={} items_update_us={} claims_del_us={} commit_us={} \
             wal_before_cp={wal_before_cp} will_checkpoint={will_checkpoint} cp_us={} wal_after_cp={wal_after_cp}",
            claim_elapsed.as_micros(),
            items_elapsed.as_micros(),
            claims_del_elapsed.as_micros(),
            commit_elapsed.as_micros(),
            cp_elapsed.as_micros(),
        );
    }
    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// run: the full per-layout, per-scale measurement sequence.
// ---------------------------------------------------------------------------

async fn run(layout: Layout, db_path: &str, rows_completed: i64, rows_initial: i64) -> turso::Result<()> {
    let explain_on = env::var("FW_SPIKE_EXPLAIN").is_ok();
    remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;
    for stmt in schema_ddl(layout, true) {
        conn.execute(&stmt, ()).await?;
    }

    eprintln!("[{layout}] ingesting {rows_completed} COMPLETED + {rows_initial} INITIAL rows...");
    let base_ts = now_micros();
    let ingest_start = Instant::now();
    ingest_stage(&mut conn, layout, STAGE_COMPLETED, 0, rows_completed, base_ts, &wal_path).await?;
    ingest_stage(&mut conn, layout, STAGE_INITIAL, rows_completed, rows_initial, base_ts, &wal_path).await?;
    let ingest_elapsed = ingest_start.elapsed();
    let total_rows = rows_completed + rows_initial;
    println!(
        "{{\"phase\":\"ingest\",\"layout\":\"{layout}\",\"rows_completed\":{rows_completed},\
         \"rows_initial\":{rows_initial},\"elapsed_ms\":{ms:.1},\"rows_per_sec\":{rps:.0}}}",
        ms = ingest_elapsed.as_secs_f64() * 1000.0,
        rps = total_rows as f64 / ingest_elapsed.as_secs_f64(),
    );
    let _ = std::io::stdout().flush();

    eprintln!("[{layout}] checkpointing...");
    let pre_main = file_len(db_path);
    let pre_wal = file_len(&format!("{db_path}-wal"));
    let cp_start = Instant::now();
    checkpoint(&conn).await?;
    let cp_elapsed = cp_start.elapsed();
    let post_main = file_len(db_path);
    println!(
        "{{\"phase\":\"checkpoint\",\"layout\":\"{layout}\",\"pre_main_bytes\":{pre_main},\
         \"pre_wal_bytes\":{pre_wal},\"post_main_bytes\":{post_main},\"checkpoint_ms\":{ms:.1}}}",
        ms = cp_elapsed.as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();

    let mut claim_id_counter: i64 = 1;

    // One untimed warm-up claim+finish: the first write after the ingest
    // checkpoint pays a one-off page-cache/WAL-reopen cost (measured: it
    // dominated trial-1 p99 in a smoke run, ~200x every other latency in
    // that trial) that has nothing to do with the layout under test.
    {
        let warm_expires_at = now_micros() + lease_micros();
        let warm = do_claim(&mut conn, layout, claim_batch(), warm_expires_at, &mut claim_id_counter, &wal_path).await?;
        do_finish(&mut conn, layout, QUEUE_NO, &warm, &wal_path).await?;
    }

    // --- claim / finish trials ---------------------------------------------
    let cbatch = claim_batch();
    let ctrials = claim_trials();
    let creps = claim_reps();
    let iotrace = env::var("FW_SPIKE_IOTRACE").is_ok();
    for trial in 1..=ctrials {
        eprintln!("[{layout}] claim trial {trial}/{ctrials} ({creps} x {cbatch})...");
        let mut claim_lat = Vec::with_capacity(creps);
        let mut outcomes = Vec::with_capacity(creps);
        let trial_start = Instant::now();
        for rep in 0..creps {
            let expires_at = now_micros() + lease_micros();
            let io_before = if iotrace { Some(io_snapshot()) } else { None };
            let t0 = Instant::now();
            let outcome = do_claim(&mut conn, layout, cbatch, expires_at, &mut claim_id_counter, &wal_path).await?;
            let elapsed = t0.elapsed();
            if let Some((r0, w0, sr0, sw0)) = io_before {
                let (r1, w1, sr1, sw1) = io_snapshot();
                eprintln!(
                    "[iotrace] trial={trial} rep={rep} claim_us={} d_rchar={} d_wchar={} d_syscr={} d_syscw={}",
                    elapsed.as_micros(),
                    r1 - r0,
                    w1 - w0,
                    sr1 - sr0,
                    sw1 - sw0
                );
            }
            claim_lat.push(elapsed);
            assert_eq!(outcome.seqs.len(), cbatch, "claim starved the INITIAL pool");
            outcomes.push(outcome);
        }
        let trial_elapsed = trial_start.elapsed();
        claim_lat.sort();
        println!(
            "{{\"phase\":\"claim\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{creps},\
             \"batch\":{cbatch},\"elapsed_ms\":{ms:.1},\"claims_per_sec\":{cps:.1},\
             \"records_per_sec\":{rps:.0},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            cps = creps as f64 / trial_elapsed.as_secs_f64(),
            rps = (creps * cbatch) as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&claim_lat, 0.50),
            p99 = percentile_us(&claim_lat, 0.99),
        );
        let _ = std::io::stdout().flush();

        eprintln!("[{layout}] finish trial {trial}/{ctrials}...");
        let mut finish_lat = Vec::with_capacity(creps);
        let trial_start = Instant::now();
        for outcome in &outcomes {
            let t0 = Instant::now();
            do_finish(&mut conn, layout, QUEUE_NO, outcome, &wal_path).await?;
            finish_lat.push(t0.elapsed());
        }
        let trial_elapsed = trial_start.elapsed();
        finish_lat.sort();
        println!(
            "{{\"phase\":\"finish\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{creps},\
             \"batch\":{cbatch},\"elapsed_ms\":{ms:.1},\"finishes_per_sec\":{fps:.1},\
             \"records_per_sec\":{rps:.0},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            fps = creps as f64 / trial_elapsed.as_secs_f64(),
            rps = (creps * cbatch) as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&finish_lat, 0.50),
            p99 = percentile_us(&finish_lat, 0.99),
        );
        let _ = std::io::stdout().flush();
    }

    // --- extend trials (one 1000-record claim, extended repeatedly) -------
    let ebatch = extend_batch();
    let etrials = extend_trials();
    let ereps = extend_reps();
    eprintln!("[{layout}] claiming the {ebatch}-record extend target...");
    let extend_expires_at = now_micros() + lease_micros();
    let extend_outcome = do_claim(&mut conn, layout, ebatch, extend_expires_at, &mut claim_id_counter, &wal_path).await?;
    assert_eq!(extend_outcome.seqs.len(), ebatch, "extend claim starved the INITIAL pool");
    for trial in 1..=etrials {
        eprintln!("[{layout}] extend trial {trial}/{etrials} ({ereps} x {ebatch})...");
        let mut lat = Vec::with_capacity(ereps);
        let trial_start = Instant::now();
        for r in 0..ereps {
            let new_expires_at = now_micros() + lease_micros() + r as i64;
            let t0 = Instant::now();
            do_extend(&mut conn, layout, QUEUE_NO, &extend_outcome, new_expires_at, &wal_path).await?;
            lat.push(t0.elapsed());
        }
        let trial_elapsed = trial_start.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"extend\",\"layout\":\"{layout}\",\"trial\":{trial},\"reps\":{ereps},\
             \"batch\":{ebatch},\"elapsed_ms\":{ms:.1},\"extends_per_sec\":{eps:.1},\
             \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            eps = ereps as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&lat, 0.50),
            p99 = percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
    }
    // Tidy up: finish the extend target so it doesn't linger Leased.
    do_finish(&mut conn, layout, QUEUE_NO, &extend_outcome, &wal_path).await?;

    // --- sweep trials (claim+backdate sweep_size, then reclaim) -----------
    let ssize = sweep_size();
    let strials = sweep_trials();
    let ssub = sweep_subbatch();
    for trial in 1..=strials {
        eprintln!("[{layout}] sweep trial {trial}/{strials}: staging {ssize} expired leases...");
        let past_expiry = now_micros() - 1_000_000; // 1s in the past: already expired
        let mut staged = 0usize;
        while staged < ssize {
            let n = ssub.min(ssize - staged);
            let outcome = do_claim(&mut conn, layout, n, past_expiry, &mut claim_id_counter, &wal_path).await?;
            assert_eq!(outcome.seqs.len(), n, "sweep staging starved the INITIAL pool");
            staged += n;
        }
        eprintln!("[{layout}] sweep trial {trial}/{strials}: sweeping...");
        let (discovery, total, item_rows_reclaimed, claims_touched) = do_sweep(&mut conn, layout, QUEUE_NO, now_micros(), &wal_path).await?;
        let claims_touched_json = match claims_touched {
            Some(n) => n.to_string(),
            None => "null".to_string(),
        };
        println!(
            "{{\"phase\":\"sweep\",\"layout\":\"{layout}\",\"trial\":{trial},\"sweep_size\":{ssize},\
             \"sub_batch\":{ssub},\"discovery_ms\":{dms:.2},\"total_ms\":{tms:.2},\
             \"item_rows_reclaimed\":{item_rows_reclaimed},\"claims_touched\":{claims_touched_json}}}",
            dms = discovery.as_secs_f64() * 1000.0,
            tms = total.as_secs_f64() * 1000.0,
        );
        let _ = std::io::stdout().flush();
    }

    // --- stage-blind vs stage-bound claim select ---------------------------
    let breps = blind_reps();
    let bound_sql = format!(
        "SELECT seq FROM items INDEXED BY items_claim_idx WHERE queue_no=?1 AND stage=?2 AND {} \
         ORDER BY rank, seq LIMIT 100",
        free_predicate(layout)
    );
    let mut bound_lat = Vec::with_capacity(breps);
    for _ in 0..breps {
        let t0 = Instant::now();
        let mut stmt = conn.prepare(&bound_sql).await?;
        let mut rows = stmt.query((Value::Integer(QUEUE_NO), Value::Integer(STAGE_INITIAL))).await?;
        let mut n = 0;
        while rows.next().await?.is_some() {
            n += 1;
        }
        bound_lat.push(t0.elapsed());
        let _ = n;
    }
    bound_lat.sort();
    println!(
        "{{\"phase\":\"stage_blind\",\"layout\":\"{layout}\",\"mode\":\"bound\",\"reps\":{breps},\
         \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
        p50 = percentile_us(&bound_lat, 0.50),
        p99 = percentile_us(&bound_lat, 0.99),
    );
    let _ = std::io::stdout().flush();

    if explain_on {
        explain(&conn, "stage-BOUND claim select (full scale)", &bound_sql, vec![Value::Integer(QUEUE_NO), Value::Integer(STAGE_INITIAL)]).await?;
    }

    let blind_sql = format!(
        "SELECT seq FROM items WHERE queue_no=?1 AND {} ORDER BY rank, seq LIMIT 100",
        free_predicate(layout)
    );
    if explain_on {
        explain(&conn, "stage-BLIND claim select (full scale, no stage predicate)", &blind_sql, vec![Value::Integer(QUEUE_NO)]).await?;
    }
    eprintln!("[{layout}] running the stage-blind query ({breps} reps)...");
    let mut blind_lat = Vec::with_capacity(breps);
    for _ in 0..breps {
        let t0 = Instant::now();
        let mut stmt = conn.prepare(&blind_sql).await?;
        let mut rows = stmt.query((Value::Integer(QUEUE_NO),)).await?;
        let mut n = 0;
        while rows.next().await?.is_some() {
            n += 1;
        }
        blind_lat.push(t0.elapsed());
        let _ = n;
    }
    blind_lat.sort();
    println!(
        "{{\"phase\":\"stage_blind\",\"layout\":\"{layout}\",\"mode\":\"blind\",\"reps\":{breps},\
         \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
        p50 = percentile_us(&blind_lat, 0.50),
        p99 = percentile_us(&blind_lat, 0.99),
    );
    let _ = std::io::stdout().flush();

    // --- final size ----------------------------------------------------------
    eprintln!("[{layout}] final checkpoint...");
    checkpoint(&conn).await?;
    let final_main = file_len(db_path);
    println!("{{\"phase\":\"final_size\",\"layout\":\"{layout}\",\"main_bytes\":{final_main}}}");
    let _ = std::io::stdout().flush();

    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// bulk_build: fastest-possible multi-queue dataset build for the owner's
// target shape (Finding 8): N queues sharing ONE Turso file, ~rows_per_queue
// records each, most resting COMPLETED. NOT an ingest-rate measurement (that
// is already covered by Finding 6 at 1M/10M scale on the production-faithful
// per-batch-checkpoint path) — this is "build it however is fastest" per the
// task: large per-queue transactions, generate_series-driven INSERT..SELECT
// (no per-row Rust-side value marshaling).
//
// `lean` (Finding 9, L2 only): builds the lean claim-path row instead of the
// fat one (schema_ddl_lean instead of schema_ddl) — `fields` dropped from
// the items row entirely, with an optional separate `fields` table carrying
// the same ~300B payload per record. Per the task's instruction to avoid
// Finding 8's 84.65-minute post-load `CREATE UNIQUE INDEX items_key_idx`
// where practical, the lean path builds `items_key_idx` (and the `fields`
// table's own PK) BEFORE the load, maintained incrementally as part of each
// per-queue INSERT..SELECT, instead of as a bulk step after — the one
// non-unique-index-shaped structure (items_claim_idx) and the rest
// (items_claim_id_idx, claims, claims_expires_idx) still build AFTER the
// load, matching Finding 8's already-established fast pattern for those
// (6.78 + 0.50 + ~0 min combined at 100M rows). Fat layout keeps the
// original all-indexes-after-load shape unchanged (Finding 8, untouched).
// ---------------------------------------------------------------------------
async fn run_bulk_build(
    layout: Layout,
    db_path: &str,
    n_queues: i64,
    rows_completed: i64,
    rows_initial: i64,
    lean: bool,
) -> turso::Result<()> {
    if lean && layout != Layout::L2 {
        panic!("lean row layout is only implemented for L2 (Finding 9 tests the Recommendation's layout with fields dropped)");
    }
    remove_db_files(db_path);
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;

    let row_layout = if lean { "lean" } else { "fat" };

    let base_ts = now_micros();

    if lean {
        // items table + its UNIQUE key index + the fields table, all created
        // BEFORE the load so items_key_idx is maintained incrementally
        // during ingest rather than bulk-built after (task instruction).
        let ddl = schema_ddl_lean(true);
        conn.execute(&ddl[0], ()).await?; // CREATE TABLE items (lean, no fields column)
        let key_idx_start = Instant::now();
        conn.execute(&ddl[1], ()).await?; // CREATE UNIQUE INDEX items_key_idx (empty table, near-instant)
        eprintln!("[bulk_build:{layout}:lean] items_key_idx created before load in {:.3}s (empty table)", key_idx_start.elapsed().as_secs_f64());
        conn.execute(FIELDS_TABLE_DDL, ()).await?;

        let insert_items_sql = "INSERT INTO items (queue_no,seq,key,stage,rank,version) \
             SELECT ?1, ?2 + (value-1), randomblob(16), ?3, ?4 + (value-1), 1 \
             FROM generate_series(1, ?5)";
        let insert_fields_sql = "INSERT INTO fields (queue_no,seq,fields) \
             SELECT ?1, ?2 + (value-1), \
             char(123)||char(34)||char(100)||char(34)||char(58)||char(34)||hex(randomblob(146))||char(34)||char(125) \
             FROM generate_series(1, ?3)";

        eprintln!(
            "[bulk_build:{layout}:lean] loading {n_queues} queues x ({rows_completed} COMPLETED + {rows_initial} INITIAL), \
             items_key_idx maintained during ingest..."
        );
        let load_start = Instant::now();
        for q in 1..=n_queues {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
            {
                let mut items_stmt = tx.prepare(insert_items_sql).await?;
                items_stmt
                    .execute((Value::Integer(q), Value::Integer(0), Value::Integer(STAGE_COMPLETED), Value::Integer(base_ts), Value::Integer(rows_completed)))
                    .await?;
                items_stmt
                    .execute((Value::Integer(q), Value::Integer(rows_completed), Value::Integer(STAGE_INITIAL), Value::Integer(base_ts), Value::Integer(rows_initial)))
                    .await?;
                let mut fields_stmt = tx.prepare(insert_fields_sql).await?;
                fields_stmt.execute((Value::Integer(q), Value::Integer(0), Value::Integer(rows_completed))).await?;
                fields_stmt.execute((Value::Integer(q), Value::Integer(rows_completed), Value::Integer(rows_initial))).await?;
            }
            tx.commit().await?;
            if q % 5 == 0 {
                maybe_passive_checkpoint(&conn, &wal_path).await?;
            }
            // Finer-grained than the fat path's every-10: items_key_idx is
            // maintained during ingest here (task instruction), so per-queue
            // cost may grow as the shared unique index outgrows cache —
            // every-queue visibility lets a real run be monitored for that
            // without waiting for a 10-queue milestone.
            if q % 2 == 0 || q == n_queues {
                eprintln!(
                    "[bulk_build:{layout}:lean] {q}/{n_queues} queues loaded, elapsed={:.1}s, wal_bytes={}",
                    load_start.elapsed().as_secs_f64(),
                    file_len(&wal_path),
                );
            }
        }
        let load_elapsed = load_start.elapsed();
        let total_rows = n_queues * (rows_completed + rows_initial);
        println!(
            "{{\"phase\":\"bulk_load\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"n_queues\":{n_queues},\
             \"rows_completed_per_queue\":{rows_completed},\"rows_initial_per_queue\":{rows_initial},\
             \"total_rows\":{total_rows},\"elapsed_ms\":{ms:.1},\"rows_per_sec\":{rps:.0},\
             \"items_key_idx_maintained_during_ingest\":true}}",
            ms = load_elapsed.as_secs_f64() * 1000.0,
            rps = total_rows as f64 / load_elapsed.as_secs_f64(),
        );
        let _ = std::io::stdout().flush();

        let remaining = &ddl[2..]; // items_claim_idx, items_claim_id_idx, claims, claims_expires_idx
        eprintln!("[bulk_build:{layout}:lean] building {} remaining secondary indexes/tables...", remaining.len());
        let index_start = Instant::now();
        for stmt in remaining {
            eprintln!("[bulk_build:{layout}:lean]   {stmt}");
            let t0 = Instant::now();
            conn.execute(stmt, ()).await?;
            eprintln!("[bulk_build:{layout}:lean]   -> {:.1}s", t0.elapsed().as_secs_f64());
        }
        let index_elapsed = index_start.elapsed();
        println!(
            "{{\"phase\":\"bulk_index_build\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"elapsed_ms\":{ms:.1}}}",
            ms = index_elapsed.as_secs_f64() * 1000.0,
        );
        let _ = std::io::stdout().flush();

        eprintln!("[bulk_build:{layout}:lean] final checkpoint...");
        let cp_start = Instant::now();
        checkpoint(&conn).await?;
        let cp_elapsed = cp_start.elapsed();
        let main_bytes = file_len(db_path);
        println!(
            "{{\"phase\":\"bulk_build_final\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"main_bytes\":{main_bytes},\
             \"checkpoint_ms\":{ms:.1},\"total_build_ms\":{total_ms:.1}}}",
            ms = cp_elapsed.as_secs_f64() * 1000.0,
            total_ms = (load_elapsed + index_elapsed + cp_elapsed).as_secs_f64() * 1000.0,
        );
        let _ = std::io::stdout().flush();

        drop(conn);
        return Ok(());
    }

    let ddl = schema_ddl(layout, true);
    // ddl[0] is always the base CREATE TABLE items; everything after is
    // secondary indexes (and, for L2, the claims table + its index) — built
    // AFTER the bulk load, per the task's "indexes created after the load".
    conn.execute(&ddl[0], ()).await?;

    let insert_sql = "INSERT INTO items (queue_no,seq,key,stage,rank,fields,version)          SELECT ?1, ?2 + (value-1), randomblob(16), ?3, ?4 + (value-1),          char(123)||char(34)||char(100)||char(34)||char(58)||char(34)||hex(randomblob(146))||char(34)||char(125), 1          FROM generate_series(1, ?5)";

    eprintln!(
        "[bulk_build:{layout}] loading {n_queues} queues x ({rows_completed} COMPLETED + {rows_initial} INITIAL)..."
    );
    let load_start = Instant::now();
    for q in 1..=n_queues {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
        {
            let mut stmt = tx.prepare(insert_sql).await?;
            // COMPLETED chunk: seq 0..rows_completed-1, rank base_ts..base_ts+rows_completed-1.
            stmt.execute((
                Value::Integer(q),
                Value::Integer(0),
                Value::Integer(STAGE_COMPLETED),
                Value::Integer(base_ts),
                Value::Integer(rows_completed),
            ))
            .await?;
            // INITIAL chunk: seq rows_completed..rows_completed+rows_initial-1,
            // rank base_ts..base_ts+rows_initial-1 (resets, matching
            // ingest_stage's own per-stage row-numbered rank scheme exactly).
            stmt.execute((
                Value::Integer(q),
                Value::Integer(rows_completed),
                Value::Integer(STAGE_INITIAL),
                Value::Integer(base_ts),
                Value::Integer(rows_initial),
            ))
            .await?;
        }
        tx.commit().await?;
        if q % 5 == 0 {
            maybe_passive_checkpoint(&conn, &wal_path).await?;
        }
        if q % 10 == 0 || q == n_queues {
            eprintln!(
                "[bulk_build:{layout}] {q}/{n_queues} queues loaded, elapsed={:.1}s, wal_bytes={}",
                load_start.elapsed().as_secs_f64(),
                file_len(&wal_path),
            );
        }
    }
    let load_elapsed = load_start.elapsed();
    let total_rows = n_queues * (rows_completed + rows_initial);
    println!(
        "{{\"phase\":\"bulk_load\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"n_queues\":{n_queues},\
         \"rows_completed_per_queue\":{rows_completed},\"rows_initial_per_queue\":{rows_initial},\
         \"total_rows\":{total_rows},\"elapsed_ms\":{ms:.1},\"rows_per_sec\":{rps:.0}}}",
        ms = load_elapsed.as_secs_f64() * 1000.0,
        rps = total_rows as f64 / load_elapsed.as_secs_f64(),
    );
    let _ = std::io::stdout().flush();

    eprintln!("[bulk_build:{layout}] building {} secondary indexes/tables...", ddl.len() - 1);
    let index_start = Instant::now();
    for stmt in &ddl[1..] {
        eprintln!("[bulk_build:{layout}]   {stmt}");
        let t0 = Instant::now();
        conn.execute(stmt, ()).await?;
        eprintln!("[bulk_build:{layout}]   -> {:.1}s", t0.elapsed().as_secs_f64());
    }
    let index_elapsed = index_start.elapsed();
    println!(
        "{{\"phase\":\"bulk_index_build\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"elapsed_ms\":{ms:.1}}}",
        ms = index_elapsed.as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();

    eprintln!("[bulk_build:{layout}] final checkpoint...");
    let cp_start = Instant::now();
    checkpoint(&conn).await?;
    let cp_elapsed = cp_start.elapsed();
    let main_bytes = file_len(db_path);
    println!(
        "{{\"phase\":\"bulk_build_final\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"main_bytes\":{main_bytes},\
         \"checkpoint_ms\":{ms:.1},\"total_build_ms\":{total_ms:.1}}}",
        ms = cp_elapsed.as_secs_f64() * 1000.0,
        total_ms = (load_elapsed + index_elapsed + cp_elapsed).as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();

    drop(conn);
    Ok(())
}

// ---------------------------------------------------------------------------
// multi_claim: Finding 8's concurrency test. Opens an already-built
// multi-queue db (bulk_build) and drives claim+finish round-robin across a
// fixed set of `n_selected` queues (picked pseudo-randomly out of
// `n_queues_total`) from `n_workers` concurrent tasks, in one of two modes:
//   "shared"   — ALL workers share ONE Connection behind a tokio Mutex,
//                mirroring production's own writer: Mutex<Connection>
//                pattern (projection.rs run_passive_checkpoint's `writer`
//                parameter is exactly this shape).
//   "separate" — each worker opens its OWN Connection (Database::connect())
//                to the same file, relying on Turso's own file-level
//                locking + busy_timeout to serialize writers, with no
//                application-level coordination.
// Reports aggregate claims/s and per-claim p50/p99 latency, so the
// shared-vs-separate, N=1-vs-N=many comparisons show directly how writes
// serialize under each approach.
// ---------------------------------------------------------------------------
async fn run_multi_claim(
    layout: Layout,
    db_path: &str,
    n_queues_total: i64,
    n_selected: usize,
    n_workers: usize,
    ops_per_worker: usize,
    mode: &str,
) -> turso::Result<()> {
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;

    let selected = select_queues(n_queues_total, n_selected);
    eprintln!("[multi_claim:{layout}:{mode}] n_workers={n_workers} ops_per_worker={ops_per_worker} selected_queues={selected:?}");

    let op_counter = Arc::new(AtomicUsize::new(0));
    let claim_id_base = Arc::new(AtomicI64::new(1_000_000_000)); // disjoint from bulk_build/run's own ids
    let selected = Arc::new(selected);
    let load_avg_start = read_loadavg();
    let overall_start = Instant::now();

    let shared_conn: Option<Arc<tokio::sync::Mutex<Connection>>> = if mode == "shared" {
        let c = db.connect()?;
        configure_connection(&c).await?;
        Some(Arc::new(tokio::sync::Mutex::new(c)))
    } else {
        None
    };

    let mut handles = Vec::with_capacity(n_workers);
    for worker_id in 0..n_workers {
        let db = db.clone();
        let selected = Arc::clone(&selected);
        let op_counter = Arc::clone(&op_counter);
        let claim_id_base = Arc::clone(&claim_id_base);
        let wal_path = wal_path.clone();
        let shared_conn = shared_conn.clone();
        let mode = mode.to_string();
        handles.push(tokio::spawn(async move {
            let mut own_conn = if mode == "separate" {
                let c = db.connect().expect("connect");
                Some(c)
            } else {
                None
            };
            if let Some(c) = &own_conn {
                configure_connection(c).await.expect("configure");
            }
            let mut lat = Vec::with_capacity(ops_per_worker);
            let mut local_claim_id = claim_id_base.fetch_add(1_000_000, Ordering::Relaxed);
            for _ in 0..ops_per_worker {
                let op_idx = op_counter.fetch_add(1, Ordering::Relaxed);
                let queue_no = selected[op_idx % selected.len()];
                let expires_at = now_micros() + lease_micros();
                let t0 = Instant::now();
                let outcome = if let Some(shared) = &shared_conn {
                    let mut guard = shared.lock().await;
                    let o = do_claim_q(&mut guard, layout, queue_no, claim_batch(), expires_at, &mut local_claim_id, &wal_path)
                        .await
                        .expect("claim");
                    do_finish(&mut guard, layout, queue_no, &o, &wal_path).await.expect("finish");
                    o
                } else {
                    let conn = own_conn.as_mut().expect("own conn");
                    let o = do_claim_q(conn, layout, queue_no, claim_batch(), expires_at, &mut local_claim_id, &wal_path)
                        .await
                        .expect("claim");
                    do_finish(conn, layout, queue_no, &o, &wal_path).await.expect("finish");
                    o
                };
                lat.push(t0.elapsed());
                assert_eq!(outcome.seqs.len(), claim_batch(), "multi_claim starved queue {queue_no}");
            }
            (worker_id, lat)
        }));
    }

    let mut all_lat: Vec<Duration> = Vec::new();
    for h in handles {
        let (_wid, lat) = h.await.expect("worker join");
        all_lat.extend(lat);
    }
    let overall_elapsed = overall_start.elapsed();
    let load_avg_end = read_loadavg();
    all_lat.sort();
    let total_ops = n_workers * ops_per_worker;
    println!(
        "{{\"phase\":\"multi_claim\",\"layout\":\"{layout}\",\"mode\":\"{mode}\",\
         \"n_workers\":{n_workers},\"ops_per_worker\":{ops_per_worker},\"total_ops\":{total_ops},\
         \"elapsed_ms\":{ms:.1},\"claims_per_sec\":{cps:.1},\
         \"p50_us\":{p50:.1},\"p99_us\":{p99:.1},\
         \"loadavg_start\":\"{load_avg_start}\",\"loadavg_end\":\"{load_avg_end}\"}}",
        ms = overall_elapsed.as_secs_f64() * 1000.0,
        cps = total_ops as f64 / overall_elapsed.as_secs_f64(),
        p50 = percentile_us(&all_lat, 0.50),
        p99 = percentile_us(&all_lat, 0.99),
    );
    let _ = std::io::stdout().flush();
    Ok(())
}

fn read_loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .take(3)
        .collect::<Vec<_>>()
        .join(" ")
}

/// Like do_claim, but for an explicit queue_no (do_claim hardcodes QUEUE_NO
/// for the single-queue 1M/10M trials; multi_claim needs to address any of
/// the multi-queue db's 100 queues).
async fn do_claim_q(
    conn: &mut Connection,
    layout: Layout,
    queue_no: i64,
    n: usize,
    expires_at: i64,
    claim_id_counter: &mut i64,
    wal_path: &str,
) -> turso::Result<ClaimOutcome> {
    let select_sql = format!(
        "SELECT seq FROM items INDEXED BY items_claim_idx          WHERE queue_no=?1 AND stage=?2 AND {} ORDER BY rank, seq LIMIT {}",
        free_predicate(layout),
        n
    );
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).await?;
    let seqs: Vec<i64> = {
        let mut stmt = tx.prepare(&select_sql).await?;
        let mut rows = stmt.query((Value::Integer(queue_no), Value::Integer(STAGE_INITIAL))).await?;
        let mut out = Vec::with_capacity(n);
        while let Some(row) = rows.next().await? {
            if let Value::Integer(seq) = row.get_value(0)? {
                out.push(seq);
            }
        }
        out
    };
    let claim_id = match layout {
        Layout::L1 => {
            let mut stmt = tx
                .prepare(
                    "UPDATE items INDEXED BY sqlite_autoindex_items_1                      SET lease_token_hash=?1, lease_expires_at=?2 WHERE queue_no=?3 AND seq=?4",
                )
                .await?;
            let token = key_blob(expires_at as u64);
            for &seq in &seqs {
                stmt.execute((Value::Blob(token.clone()), Value::Integer(expires_at), Value::Integer(queue_no), Value::Integer(seq)))
                    .await?;
            }
            None
        }
        Layout::L2 => {
            let cid = *claim_id_counter;
            *claim_id_counter += 1;
            let mut ins = tx
                .prepare(
                    "INSERT INTO claims (claim_id,token_hash,holder,expires_at,record_count)                      VALUES (?1,?2,?3,?4,?5)",
                )
                .await?;
            ins.execute((
                Value::Integer(cid),
                Value::Blob(key_blob(cid as u64)),
                Value::Text("worker".to_string()),
                Value::Integer(expires_at),
                Value::Integer(seqs.len() as i64),
            ))
            .await?;
            let mut upd = tx
                .prepare("UPDATE items INDEXED BY sqlite_autoindex_items_1 SET claim_id=?1 WHERE queue_no=?2 AND seq=?3")
                .await?;
            for &seq in &seqs {
                upd.execute((Value::Integer(cid), Value::Integer(queue_no), Value::Integer(seq))).await?;
            }
            Some(cid)
        }
    };
    tx.commit().await?;
    maybe_passive_checkpoint(conn, wal_path).await?;
    Ok(ClaimOutcome { seqs, claim_id })
}

// ---------------------------------------------------------------------------
// finding8: single-writer claim/finish/extend/sweep round-robin across
// n_selected queues of a multi-queue db (bulk_build's output), same trial
// shape as run()'s single-queue methodology (Finding 1-6) so the numbers
// are directly comparable. Also demonstrates the sweep-scoping difference
// forced by each layout's design in a multi-queue file: L1's sweep is
// queue-scoped (its expiry index is keyed (queue_no, lease_expires_at)), so
// covering N queues costs N separate sweep calls; L2's sweep is GLOBAL (the
// claims table and claims_expires_idx carry no queue_no at all), so one call
// covers every queue's expired claims regardless of how many are involved.
//
// `row_layout` ("fat" or "lean", Finding 9) is a label only, embedded in
// every JSON output line for traceability — it does not change any SQL
// here. do_claim_q/do_finish/do_extend/do_sweep never reference `fields` on
// either the fat or the lean items row, so the claim/finish/extend/sweep
// path this function drives is byte-for-byte identical code regardless of
// which db (fat- or lean-built) it is pointed at; only the db_path and this
// label differ between a fat and a lean finding8 run.
// ---------------------------------------------------------------------------
async fn run_finding8(layout: Layout, db_path: &str, n_queues_total: i64, n_selected: usize, row_layout: &str) -> turso::Result<()> {
    let wal_path = format!("{db_path}-wal");
    let db = Builder::new_local(db_path).build().await?;
    let mut conn = db.connect()?;
    configure_connection(&conn).await?;

    let selected = select_queues(n_queues_total, n_selected);
    eprintln!("[finding8:{layout}:{row_layout}] selected_queues={selected:?}");

    let start_main = file_len(db_path);
    let start_wal = file_len(&wal_path);
    println!(
        "{{\"phase\":\"finding8_start_size\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"main_bytes\":{start_main},\"wal_bytes\":{start_wal},\"selected_queues\":{selected:?}}}"
    );
    let _ = std::io::stdout().flush();

    let mut claim_id_counter: i64 = 5_000_000_000;

    // Untimed warm-up (same rationale as run()'s: first write after a fresh
    // connection/checkpoint pays a one-off cost unrelated to the layout).
    {
        let q = selected[0];
        let expires_at = now_micros() + lease_micros();
        let warm = do_claim_q(&mut conn, layout, q, claim_batch(), expires_at, &mut claim_id_counter, &wal_path).await?;
        do_finish(&mut conn, layout, q, &warm, &wal_path).await?;
    }

    // --- claim / finish round-robin, single writer, same trial shape as run() ---
    let cbatch = claim_batch();
    let ctrials = claim_trials();
    let creps = claim_reps();
    let mut op_idx: usize = 0;
    for trial in 1..=ctrials {
        eprintln!("[finding8:{layout}] claim trial {trial}/{ctrials} ({creps} x {cbatch}, round-robin over {n_selected} queues)...");
        let mut claim_lat = Vec::with_capacity(creps);
        let mut outcomes: Vec<(i64, ClaimOutcome)> = Vec::with_capacity(creps);
        let trial_start = Instant::now();
        for _ in 0..creps {
            let q = selected[op_idx % selected.len()];
            op_idx += 1;
            let expires_at = now_micros() + lease_micros();
            let t0 = Instant::now();
            let outcome = do_claim_q(&mut conn, layout, q, cbatch, expires_at, &mut claim_id_counter, &wal_path).await?;
            claim_lat.push(t0.elapsed());
            assert_eq!(outcome.seqs.len(), cbatch, "finding8 claim starved queue {q}");
            outcomes.push((q, outcome));
        }
        let trial_elapsed = trial_start.elapsed();
        claim_lat.sort();
        println!(
            "{{\"phase\":\"finding8_claim\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"trial\":{trial},\"reps\":{creps},\
             \"batch\":{cbatch},\"n_selected_queues\":{n_selected},\"elapsed_ms\":{ms:.1},\
             \"claims_per_sec\":{cps:.1},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            cps = creps as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&claim_lat, 0.50),
            p99 = percentile_us(&claim_lat, 0.99),
        );
        let _ = std::io::stdout().flush();

        eprintln!("[finding8:{layout}] finish trial {trial}/{ctrials}...");
        let mut finish_lat = Vec::with_capacity(creps);
        let trial_start = Instant::now();
        for (q, outcome) in &outcomes {
            let t0 = Instant::now();
            do_finish(&mut conn, layout, *q, outcome, &wal_path).await?;
            finish_lat.push(t0.elapsed());
        }
        let trial_elapsed = trial_start.elapsed();
        finish_lat.sort();
        println!(
            "{{\"phase\":\"finding8_finish\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"trial\":{trial},\"reps\":{creps},\
             \"batch\":{cbatch},\"n_selected_queues\":{n_selected},\"elapsed_ms\":{ms:.1},\
             \"finishes_per_sec\":{fps:.1},\"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            fps = creps as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&finish_lat, 0.50),
            p99 = percentile_us(&finish_lat, 0.99),
        );
        let _ = std::io::stdout().flush();
    }

    // --- extend: one 1000-record claim per trial, rotated across selected queues ---
    let ebatch = extend_batch();
    let etrials = extend_trials();
    let ereps = extend_reps();
    for trial in 1..=etrials {
        let q = selected[(trial as usize - 1) % selected.len()];
        eprintln!("[finding8:{layout}] extend trial {trial}/{etrials} ({ereps} x {ebatch}) on queue {q}...");
        let expires_at = now_micros() + lease_micros();
        let extend_outcome = do_claim_q(&mut conn, layout, q, ebatch, expires_at, &mut claim_id_counter, &wal_path).await?;
        assert_eq!(extend_outcome.seqs.len(), ebatch, "finding8 extend claim starved queue {q}");
        let mut lat = Vec::with_capacity(ereps);
        let trial_start = Instant::now();
        for r in 0..ereps {
            let new_expires_at = now_micros() + lease_micros() + r as i64;
            let t0 = Instant::now();
            do_extend(&mut conn, layout, q, &extend_outcome, new_expires_at, &wal_path).await?;
            lat.push(t0.elapsed());
        }
        let trial_elapsed = trial_start.elapsed();
        lat.sort();
        println!(
            "{{\"phase\":\"finding8_extend\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"trial\":{trial},\"reps\":{ereps},\
             \"batch\":{ebatch},\"queue_no\":{q},\"elapsed_ms\":{ms:.1},\"extends_per_sec\":{eps:.1},\
             \"p50_us\":{p50:.1},\"p99_us\":{p99:.1}}}",
            ms = trial_elapsed.as_secs_f64() * 1000.0,
            eps = ereps as f64 / trial_elapsed.as_secs_f64(),
            p50 = percentile_us(&lat, 0.50),
            p99 = percentile_us(&lat, 0.99),
        );
        let _ = std::io::stdout().flush();
        do_finish(&mut conn, layout, q, &extend_outcome, &wal_path).await?;
    }

    // --- sweep: stage expired leases SPREAD across the selected queues, then reclaim ---
    let ssize = sweep_size();
    let strials = sweep_trials();
    let ssub = sweep_subbatch();
    for trial in 1..=strials {
        eprintln!("[finding8:{layout}] sweep trial {trial}/{strials}: staging {ssize} expired leases across {n_selected} queues...");
        let past_expiry = now_micros() - 1_000_000;
        let mut staged: Vec<i64> = Vec::new(); // queue_no per staged claim
        let mut staged_count = 0usize;
        let mut rr = 0usize;
        while staged_count < ssize {
            let n = ssub.min(ssize - staged_count);
            let q = selected[rr % selected.len()];
            rr += 1;
            let outcome = do_claim_q(&mut conn, layout, q, n, past_expiry, &mut claim_id_counter, &wal_path).await?;
            assert_eq!(outcome.seqs.len(), n, "finding8 sweep staging starved queue {q}");
            staged_count += n;
            staged.push(q);
        }
        let mut distinct_queues: Vec<i64> = staged.clone();
        distinct_queues.sort();
        distinct_queues.dedup();

        eprintln!("[finding8:{layout}] sweep trial {trial}/{strials}: sweeping ({} distinct queues touched)...", distinct_queues.len());
        match layout {
            Layout::L1 => {
                // Queue-scoped design: one sweep call per distinct queue, summed.
                let mut total_discovery = Duration::ZERO;
                let mut total_total = Duration::ZERO;
                let mut total_reclaimed = 0usize;
                let now = now_micros();
                for &q in &distinct_queues {
                    let (d, t, n, _) = do_sweep(&mut conn, layout, q, now, &wal_path).await?;
                    total_discovery += d;
                    total_total += t;
                    total_reclaimed += n;
                }
                println!(
                    "{{\"phase\":\"finding8_sweep\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"trial\":{trial},\"sweep_size\":{ssize},\
                     \"n_sweep_calls\":{ncalls},\"distinct_queues_touched\":{ndq},\
                     \"discovery_ms\":{dms:.2},\"total_ms\":{tms:.2},\"item_rows_reclaimed\":{total_reclaimed}}}",
                    ncalls = distinct_queues.len(),
                    ndq = distinct_queues.len(),
                    dms = total_discovery.as_secs_f64() * 1000.0,
                    tms = total_total.as_secs_f64() * 1000.0,
                );
            }
            Layout::L2 => {
                // Global design: ONE sweep call covers every queue's expired claims.
                let now = now_micros();
                let (d, t, n, claims_touched) = do_sweep(&mut conn, layout, 0, now, &wal_path).await?;
                let ct = claims_touched.unwrap_or(0);
                println!(
                    "{{\"phase\":\"finding8_sweep\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"trial\":{trial},\"sweep_size\":{ssize},\
                     \"n_sweep_calls\":1,\"distinct_queues_touched\":{ndq},\"claims_touched\":{ct},\
                     \"discovery_ms\":{dms:.2},\"total_ms\":{tms:.2},\"item_rows_reclaimed\":{n}}}",
                    ndq = distinct_queues.len(),
                    dms = d.as_secs_f64() * 1000.0,
                    tms = t.as_secs_f64() * 1000.0,
                );
            }
        }
        let _ = std::io::stdout().flush();
    }

    eprintln!("[finding8:{layout}] final checkpoint...");
    let cp_start = Instant::now();
    checkpoint(&conn).await?;
    let cp_elapsed = cp_start.elapsed();
    let final_main = file_len(db_path);
    println!(
        "{{\"phase\":\"finding8_final_size\",\"layout\":\"{layout}\",\"row_layout\":\"{row_layout}\",\"main_bytes\":{final_main},\"checkpoint_ms\":{ms:.1}}}",
        ms = cp_elapsed.as_secs_f64() * 1000.0,
    );
    let _ = std::io::stdout().flush();

    drop(conn);
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build tokio runtime");

    if args.len() < 2 {
        eprintln!(
            "usage:\n  spike run <l1|l2> <db_path> <rows_completed> <rows_initial>\n  \
             spike size_baseline <l1|l2> <db_path> <rows_completed> <rows_initial>\n  \
             spike explain <l1|l2> <db_path>\n  \
             spike diag <l1|l2> <db_path> [n_reps]\n  \
             spike bulk_build <l1|l2> <db_path> <n_queues> <rows_completed_per_queue> <rows_initial_per_queue> [fat|lean]\n  \
             spike finding8 <l1|l2> <db_path> <n_queues_total> <n_selected> [fat|lean]\n  \
             spike multi_claim <l1|l2> <db_path> <n_queues_total> <n_selected> <n_workers> <ops_per_worker> [shared|separate]\n  \
             spike gs_test"
        );
        std::process::exit(2);
    }
    match args[1].as_str() {
        "run" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let rows_completed: i64 = args[4].parse().expect("rows_completed");
            let rows_initial: i64 = args[5].parse().expect("rows_initial");
            ensure_parent(db_path);
            rt.block_on(run(layout, db_path, rows_completed, rows_initial)).expect("run failed");
        }
        "size_baseline" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let rows_completed: i64 = args[4].parse().expect("rows_completed");
            let rows_initial: i64 = args[5].parse().expect("rows_initial");
            ensure_parent(db_path);
            rt.block_on(run_size_baseline(layout, db_path, rows_completed, rows_initial)).expect("size_baseline failed");
        }
        "explain" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            ensure_parent(db_path);
            rt.block_on(run_explain(layout, db_path)).expect("explain failed");
        }
        "finding8" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let n_queues_total: i64 = args[4].parse().expect("n_queues_total");
            let n_selected: usize = args[5].parse().expect("n_selected");
            let row_layout = args.get(6).map(|s| s.as_str()).unwrap_or("fat");
            rt.block_on(run_finding8(layout, db_path, n_queues_total, n_selected, row_layout)).expect("finding8 failed");
        }
        "multi_claim" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let n_queues_total: i64 = args[4].parse().expect("n_queues_total");
            let n_selected: usize = args[5].parse().expect("n_selected");
            let n_workers: usize = args[6].parse().expect("n_workers");
            let ops_per_worker: usize = args[7].parse().expect("ops_per_worker");
            let mode = args.get(8).map(|s| s.as_str()).unwrap_or("shared");
            let rt_mt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("build mt runtime");
            rt_mt
                .block_on(run_multi_claim(layout, db_path, n_queues_total, n_selected, n_workers, ops_per_worker, mode))
                .expect("multi_claim failed");
        }
        "bulk_build" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let n_queues: i64 = args[4].parse().expect("n_queues");
            let rows_completed: i64 = args[5].parse().expect("rows_completed");
            let rows_initial: i64 = args[6].parse().expect("rows_initial");
            let lean = args.get(7).map(|s| s.as_str()) == Some("lean");
            ensure_parent(db_path);
            rt.block_on(run_bulk_build(layout, db_path, n_queues, rows_completed, rows_initial, lean)).expect("bulk_build failed");
        }
        "gs_test" => {
            rt.block_on(async {
                let db = Builder::new_local(":memory:").build().await.expect("open");
                let conn = db.connect().expect("connect");
                let t0 = Instant::now();
                let mut stmt = conn.prepare("SELECT count(*), sum(value) FROM generate_series(1, 1000000)").await.expect("prepare gs");
                let mut rows = stmt.query(()).await.expect("query gs");
                if let Some(row) = rows.next().await.expect("row") {
                    eprintln!("generate_series(1,1000000): count={:?} sum={:?} elapsed={:?}", row.get_value(0), row.get_value(1), t0.elapsed());
                }
                conn.execute("CREATE TABLE probe (a INTEGER, b BLOB, c TEXT)", ()).await.expect("ddl");
                let t1 = Instant::now();
                let insert_sql = "INSERT INTO probe (a,b,c) SELECT value, randomblob(16), char(123) || char(34) || char(100) || char(34) || char(58) || char(34) || hex(randomblob(146)) || char(34) || char(125) FROM generate_series(1, 1000000)";
                let n: u64 = conn.execute(insert_sql, ()).await.expect("bulk insert");
                eprintln!("bulk INSERT..SELECT generate_series 1,000,000 rows: n={n} elapsed={:?}", t1.elapsed());
            });
        }
        "diag" => {
            let layout = Layout::parse(&args[2]);
            let db_path = &args[3];
            let n_reps: usize = args.get(4).map(|s| s.parse().expect("n_reps")).unwrap_or(20);
            rt.block_on(run_diag(layout, db_path, n_reps)).expect("diag failed");
        }
        other => {
            eprintln!("unknown subcommand {other}");
            std::process::exit(2);
        }
    }
}

fn ensure_parent(db_path: &str) {
    if let Some(parent) = Path::new(db_path).parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent).expect("mkdir db dir");
        }
    }
}
