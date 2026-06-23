//! Durable `LogStore` over a SQLite command-log table (TD-005).
//!
//! This is the durable command-log authority for the standalone `sqlite`
//! backend: the command log lives in a `pqueue_command_log` table in the same
//! SQLite database that holds the projection. The durable ack boundary is a
//! committed SQLite transaction under WAL with `synchronous=FULL`, which fsyncs
//! the WAL on every commit — so `append_batch` returns only after the appended
//! commands are on stable storage (survives process crash *and* power loss).
//! (`synchronous=NORMAL`, which only fsyncs at checkpoint, would leave a
//! power-loss window where an acked append can be lost; it is intentionally not
//! the default for a backend whose log is the ack boundary.) Single-writer: one
//! host process owns the file, so there is no cross-writer CAS; `expected_epoch`
//! fences stale appends after a restart.

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use pqueue_storage::codec::{decode_envelope, encode_envelope};
use pqueue_storage::commands::CommandEnvelope;
use pqueue_storage::traits::{
    AppendBatchResult, CommandPage, DurabilityProfile, LogStore, LogStoreError,
};
use pqueue_storage::types::{CommandPosition, ShardKey};
use rusqlite::{Connection, OptionalExtension, params};

/// A SQLite-backed durable command log.
pub struct SqliteLogStore {
    conn: Mutex<Connection>,
    durability: DurabilityProfile,
}

impl SqliteLogStore {
    /// Open (or create) a file-backed durable log. WAL + `synchronous=FULL`
    /// fsyncs on every commit, so a returned `append_batch` is on stable storage.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        initialize_log_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            durability: DurabilityProfile::LocalDisk,
        })
    }

    /// Open an in-memory log (tests/dev only — not durable).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        initialize_log_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            durability: DurabilityProfile::None,
        })
    }

    /// Current epoch recorded for a shard, or `None` if the shard has no log yet.
    pub fn shard_epoch(&self, shard: &ShardKey) -> Result<Option<u64>, LogStoreError> {
        let conn = self.conn.lock().expect("log mutex");
        read_shard_epoch(&conn, shard)
    }

    /// Set a shard's fencing epoch (control-plane / restart use). Creates the
    /// shard row if absent.
    pub fn set_shard_epoch(&self, shard: &ShardKey, epoch: u64) -> Result<(), LogStoreError> {
        let conn = self.conn.lock().expect("log mutex");
        conn.execute(
            "INSERT INTO pqueue_log_shard (tenant_id, queue_id, shard_id, epoch)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(tenant_id, queue_id, shard_id) DO UPDATE SET epoch = excluded.epoch",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                shard.shard_id.as_u32() as i64,
                epoch as i64
            ],
        )
        .map_err(storage_failure)?;
        Ok(())
    }
}

impl LogStore for SqliteLogStore {
    async fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError> {
        // The whole append runs synchronously under one connection lock: this is
        // the single-writer serialization point. The returned future stays `Send`
        // (the trait requires it) ONLY because there is no `.await` while the
        // `!Send` MutexGuard is held — do not introduce one inside this method.
        let mut conn = self.conn.lock().expect("log mutex");
        let tx = conn.transaction().map_err(storage_failure)?;
        // Durable ack boundary: commit fsyncs the WAL (synchronous=FULL), so on
        // return the appended commands have reached stable storage.
        let result = append_into_tx(&tx, shard, expected_epoch, &commands)?;
        tx.commit().map_err(storage_failure)?;
        Ok(result)
    }

    async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, LogStoreError> {
        let conn = self.conn.lock().expect("log mutex");
        read_page(&conn, shard, position, limit)
    }

    fn durability_profile(&self) -> DurabilityProfile {
        self.durability
    }
}

/// Append a batch of commands within an existing transaction (the shared
/// append core). Composing this with the projection apply core in ONE
/// transaction is the standalone `SqliteBackend`'s atomic append+apply
/// (TD-005); the standalone `SqliteLogStore` wraps it in its own transaction.
/// Returns the position of the last appended command. No `.await` here, so the
/// caller's future stays `Send` while it holds the `!Send` connection guard.
pub(crate) fn append_into_tx(
    tx: &Connection,
    shard: &ShardKey,
    expected_epoch: Option<u64>,
    commands: &[CommandEnvelope],
) -> Result<AppendBatchResult, LogStoreError> {
    let current_epoch = read_shard_epoch(tx, shard)?.unwrap_or(0);
    if let Some(expected) = expected_epoch
        && expected != current_epoch
    {
        return Err(LogStoreError::StalEpoch {
            expected,
            current: current_epoch,
        });
    }

    // Register the shard (idempotent) so an empty append still creates it,
    // matching the in-memory backend.
    tx.execute(
        "INSERT OR IGNORE INTO pqueue_log_shard (tenant_id, queue_id, shard_id, epoch)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64,
            current_epoch as i64
        ],
    )
    .map_err(storage_failure)?;

    let mut sequence: u64 = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM pqueue_command_log
             WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                shard.shard_id.as_u32() as i64
            ],
            |row| row.get::<_, i64>(0),
        )
        .map_err(storage_failure)? as u64;

    let mut last_position = CommandPosition {
        shard_key: shard.clone(),
        sequence: 0,
        backend_epoch: current_epoch,
    };
    for envelope in commands {
        let payload = encode_envelope(envelope)
            .map_err(|err| LogStoreError::StorageFailure(format!("encode command: {err}")))?;
        tx.execute(
            "INSERT INTO pqueue_command_log
             (tenant_id, queue_id, shard_id, sequence, backend_epoch, checksum, payload, created_at_seconds)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                shard.shard_id.as_u32() as i64,
                sequence as i64,
                current_epoch as i64,
                envelope.checksum.0 as i64,
                payload,
                envelope.created_at.seconds
            ],
        )
        .map_err(storage_failure)?;
        last_position = CommandPosition {
            shard_key: shard.clone(),
            sequence,
            backend_epoch: current_epoch,
        };
        sequence += 1;
    }

    Ok(AppendBatchResult { last_position })
}

/// Read an indexed page of committed commands (the shared read core). Used by
/// both the standalone `SqliteLogStore` and `SqliteBackend`.
pub(crate) fn read_page(
    conn: &Connection,
    shard: &ShardKey,
    position: Option<CommandPosition>,
    limit: usize,
) -> Result<CommandPage, LogStoreError> {
    if read_shard_epoch(conn, shard)?.is_none() {
        return Err(LogStoreError::ShardNotFound);
    }
    if limit == 0 {
        return Ok(CommandPage {
            commands: Vec::new(),
            next_position: None,
        });
    }

    let start = position.map(|pos| pos.sequence + 1).unwrap_or(0);
    // Fetch one extra row to detect a further page. Saturate so `usize::MAX`
    // ("read everything") cannot overflow or wrap to a negative SQLite LIMIT.
    let fetch_limit = limit.saturating_add(1).min(i64::MAX as usize) as i64;
    let mut stmt = conn
        .prepare(
            "SELECT sequence, backend_epoch, payload FROM pqueue_command_log
             WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND sequence >= ?4
             ORDER BY sequence LIMIT ?5",
        )
        .map_err(storage_failure)?;
    let rows: Vec<(u64, u64, Vec<u8>)> = stmt
        .query_map(
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                shard.shard_id.as_u32() as i64,
                start as i64,
                fetch_limit
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .map_err(storage_failure)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(storage_failure)?;

    let has_more = rows.len() > limit;
    let mut commands = Vec::with_capacity(rows.len().min(limit));
    for (sequence, backend_epoch, payload) in rows.into_iter().take(limit) {
        let envelope = decode_envelope(&payload)
            .map_err(|err| LogStoreError::StorageFailure(format!("decode command: {err}")))?;
        commands.push((
            CommandPosition {
                shard_key: shard.clone(),
                sequence,
                backend_epoch,
            },
            envelope,
        ));
    }
    let next_position = if has_more {
        commands.last().map(|(position, _)| position.clone())
    } else {
        None
    };
    Ok(CommandPage {
        commands,
        next_position,
    })
}

pub(crate) fn read_shard_epoch(
    conn: &Connection,
    shard: &ShardKey,
) -> Result<Option<u64>, LogStoreError> {
    conn.query_row(
        "SELECT epoch FROM pqueue_log_shard
         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64
        ],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|opt| opt.map(|epoch| epoch as u64))
    .map_err(storage_failure)
}

pub(crate) fn storage_failure(err: rusqlite::Error) -> LogStoreError {
    LogStoreError::StorageFailure(err.to_string())
}

pub(crate) fn initialize_log_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS pqueue_log_shard (
            tenant_id TEXT NOT NULL,
            queue_id  TEXT NOT NULL,
            shard_id  INTEGER NOT NULL,
            epoch     INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, queue_id, shard_id)
        );
        CREATE TABLE IF NOT EXISTS pqueue_command_log (
            tenant_id         TEXT NOT NULL,
            queue_id          TEXT NOT NULL,
            shard_id          INTEGER NOT NULL,
            sequence          INTEGER NOT NULL,
            backend_epoch     INTEGER NOT NULL,
            checksum          INTEGER NOT NULL,
            payload           BLOB NOT NULL,
            created_at_seconds INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, queue_id, shard_id, sequence)
        );",
    )
}
