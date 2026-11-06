//! Connection configuration and checkpoint policy, mirrored verbatim from
//! `crates/fireweed-turso/src/local.rs` `configure_connection`
//! (local.rs:107-141) under the log-backed profile production actually opens
//! (`TursoConfig::local(path).with_log_backed_projection()`,
//! `crates/fireweed/src/turso_compose.rs:136`), and from
//! `crates/fireweed-turso/src/projection.rs:127-176`
//! (`maybe_passive_checkpoint_log_backed`, `LOG_BACKED_CHECKPOINT_TRY_BYTES`)
//! for the Passive-checkpoint cap. Copied from `spikes/claim-scale/src/main.rs`
//! `configure_connection`/`maybe_passive_checkpoint`, which already carries
//! this same provenance note.

use std::env;
use std::time::Duration;

use turso::Connection;

pub async fn configure_connection(connection: &Connection) -> turso::Result<()> {
    configure_connection_with_cache(connection, cache_size_kib()).await
}

/// `cache_kib`: negative KiB per SQLite's `PRAGMA cache_size` convention
/// (e.g. -131072 = 128 MiB). Parameterized so the cache-size ablation can
/// reuse the exact same connection setup with only that one pragma changed.
pub async fn configure_connection_with_cache(
    connection: &Connection,
    cache_kib: i64,
) -> turso::Result<()> {
    connection.pragma_update("page_size", "4096").await?;
    connection.pragma_update("journal_mode", "WAL").await?;
    connection.pragma_update("synchronous", "NORMAL").await?;
    connection
        .pragma_update("cache_size", cache_kib.to_string())
        .await?;
    connection.pragma_update("temp_store", "MEMORY").await?;
    connection.pragma_update("wal_autocheckpoint", "0").await?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(())
}

/// Production's default: 128 MiB cache cap (`local.rs` `cache_size =
/// -131072`). Overridable via `FW_SPIKE_CACHE_KIB` for the cache-size
/// ablation the task asks for.
pub fn cache_size_kib() -> i64 {
    env::var("FW_SPIKE_CACHE_KIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(-131072)
}

pub fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

pub async fn checkpoint_truncate(conn: &Connection) -> turso::Result<()> {
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

/// Mirrors `projection.rs:127-176` `maybe_passive_checkpoint_log_backed`:
/// a log-backed connection (`wal_autocheckpoint=0`) is never
/// auto-checkpointed, so production issues `PRAGMA wal_checkpoint(PASSIVE)`
/// after every apply batch once the WAL exceeds this threshold. Single
/// connection here (task's single-writer requirement), so this spike skips
/// production's two-permit semaphore / MUST-checkpoint tier, same
/// simplification `spikes/claim-scale` already used and justified (SPIKE-004
/// Finding 0).
const LOG_BACKED_CHECKPOINT_TRY_BYTES: u64 = 400 * 1024 * 1024;

pub async fn maybe_passive_checkpoint(conn: &Connection, wal_path: &str) -> turso::Result<()> {
    if file_len(wal_path) < LOG_BACKED_CHECKPOINT_TRY_BYTES {
        return Ok(());
    }
    let mut rows = conn.query("PRAGMA wal_checkpoint(PASSIVE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

pub fn remove_db_files(db_path: &str) {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{db_path}-wal"));
    let _ = std::fs::remove_file(format!("{db_path}-shm"));
}

// --------------------------------------------------------------- stats

/// Median and (max-min)/avg spread over 3 (or more) trial totals -- the
/// variance convention SPIKE-002/004/005 already use throughout.
pub fn median(vals: &[f64]) -> f64 {
    let mut v = vals.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

pub fn spread_pct(vals: &[f64]) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }
    let max = vals.iter().cloned().fold(f64::MIN, f64::max);
    let min = vals.iter().cloned().fold(f64::MAX, f64::min);
    let avg = vals.iter().sum::<f64>() / vals.len() as f64;
    if avg == 0.0 {
        0.0
    } else {
        (max - min) / avg * 100.0
    }
}

pub fn percentile_us(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx].as_secs_f64() * 1_000_000.0
}

/// Read `/proc/loadavg`'s 1-minute average, for the measurement-hygiene
/// load-at-start/load-at-end disclosure the task requires on every cell.
pub fn load_avg_1m() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(|s| s.to_string()))
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1.0)
}
