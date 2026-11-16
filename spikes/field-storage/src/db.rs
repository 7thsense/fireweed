//! Connection setup mirrored from `crates/fireweed-turso/src/local.rs`
//! `configure_connection` (local.rs:3407-3445): page_size 4096 (set before
//! the schema is created on a fresh file, local.rs:3408-3411), WAL journal
//! mode (local.rs:3412-3416), `cache_size -131072` (128 MiB cap,
//! local.rs:3430-3431), a 5s busy timeout (`DEFAULT_BUSY_TIMEOUT`,
//! local.rs:59,100,3443).
//!
//! `synchronous`: the adapter uses NORMAL only when `rebuildable_io` is set,
//! i.e. when a custom object-log-backed IO impl replaces physical fsync
//! (local.rs:3417-3429, "The log-backed I/O adapter omits physical sync; the
//! object-log is durable."). This spike does not build that IO adapter, so it
//! takes the adapter's own non-rebuildable branch: `synchronous = OFF`. That
//! is the same choice `TursoConfig::local()` defaults to (local.rs:96-104,
//! `rebuildable_io: false`) and is what every non-log-backed fireweed-turso
//! caller (all in-repo tests) actually runs. `wal_autocheckpoint` follows the
//! same branch: 1000 frames, not the log-backed path's 0
//! (`checkpoint_frames`, local.rs:3396-3405).
use std::path::Path;
use std::time::Duration;
use turso::{Builder, Connection, Database};

pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn open_db(path: &Path) -> turso::Result<(Database, Connection)> {
    let db = Builder::new_local(path.to_str().expect("utf8 path"))
        .build()
        .await?;
    let conn = db.connect()?;
    configure(&conn).await?;
    Ok((db, conn))
}

async fn configure(conn: &Connection) -> turso::Result<()> {
    conn.pragma_update("page_size", "4096").await?;
    conn.pragma_update("journal_mode", "WAL").await?;
    conn.pragma_update("synchronous", "OFF").await?;
    conn.pragma_update("cache_size", "-131072").await?;
    conn.pragma_update("wal_autocheckpoint", "1000").await?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    Ok(())
}

/// Checkpoint the WAL back into the main file and return the main file's
/// on-disk size: the steady-state footprint a consumer would actually see,
/// not a snapshot mid-WAL.
pub async fn on_disk_size(conn: &Connection, path: &Path) -> turso::Result<u64> {
    // PRAGMA wal_checkpoint returns a row (busy, log, checkpointed), so it
    // must go through the row-aware `query` API, not `execute` (which
    // errors on an unexpected row — see the journal_mode comment this
    // module's header borrows from, local.rs:3412-3413).
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while rows.next().await?.is_some() {}
    Ok(std::fs::metadata(path).map(|m| m.len()).unwrap_or(0))
}
