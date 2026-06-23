//! Unified single-transaction `SqliteBackend` (TD-005) — the wired-up standalone
//! durable backend.
//!
//! The headline property: the durable command log AND the queryable projection
//! live in ONE SQLite database on ONE connection, and a command is appended to
//! the log and applied to the projection in the SAME transaction. On commit
//! (WAL + `synchronous=FULL`) both are durable together with one fsync, so a
//! returned ack means the projection already reflects the command (strict
//! read-after-write). This removes the eventual-apply / reservation machinery
//! the object-log backend needs, and means there is no committed-but-unapplied
//! window — so reopening the file reads the persisted projection directly with
//! NO log replay required.
//!
//! Single-writer: one host process owns the file. `open` acquires an exclusive
//! lock and rejects a second opener (WAL alone would merely serialize writers).
//! `backend_epoch` is recorded per shard and bumped on open for restart-fencing
//! / observability; there is no cross-writer CAS.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pqueue_core::{ItemId, QueueDefinition, QueueId, TenantId, UtcTimestamp};
use pqueue_storage::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, BatchRenewLeasesCommand,
    CommandEnvelope, CommandId, FinalizeOutcome, LeaseExpiredCommand, PushItem, QueueCommand,
};
use pqueue_storage::traits::{
    AppendBatchResult, CommandPage, ControlPlaneError, LogStoreError, ProjectionError,
    QueueMetricsSnapshot, ShardAssignment,
};
use pqueue_storage::types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};

use crate::control_plane::{control_failure, initialize_control_plane_schema};
use crate::log::{append_into_tx, initialize_log_schema, read_page, storage_failure};
use crate::projection::{
    apply_into_tx, ids_to_item_ids, initialize_projection_schema, metrics_query, select_eligible,
    shard_exists, ts_to_nanos,
};

/// WAL `synchronous` strictness for the durable ack boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteSynchronous {
    /// fsync the WAL on every commit (survives process crash AND power loss).
    /// The default for a backend whose log is the ack boundary (TD-005).
    Full,
    /// fsync only at checkpoint — higher throughput, but an acked append can be
    /// lost on power loss. An explicit opt-in, never the default.
    Normal,
}

impl SqliteSynchronous {
    fn pragma(self) -> &'static str {
        match self {
            Self::Full => "FULL",
            Self::Normal => "NORMAL",
        }
    }
}

/// Error returned by `SqliteBackend` operations.
#[derive(Debug)]
pub enum SqliteBackendError {
    /// The database file is already owned by another live `SqliteBackend`
    /// (single-writer enforcement).
    AlreadyOpen,
    Log(LogStoreError),
    Projection(ProjectionError),
    ControlPlane(ControlPlaneError),
    Storage(String),
}

impl std::fmt::Display for SqliteBackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyOpen => write!(
                f,
                "sqlite backend file is already owned by another process (single-writer)"
            ),
            Self::Log(err) => write!(f, "{err}"),
            Self::Projection(err) => write!(f, "{err}"),
            Self::ControlPlane(err) => write!(f, "{err}"),
            Self::Storage(msg) => write!(f, "storage failure: {msg}"),
        }
    }
}

impl std::error::Error for SqliteBackendError {}

impl From<LogStoreError> for SqliteBackendError {
    fn from(err: LogStoreError) -> Self {
        Self::Log(err)
    }
}
impl From<ProjectionError> for SqliteBackendError {
    fn from(err: ProjectionError) -> Self {
        Self::Projection(err)
    }
}
impl From<ControlPlaneError> for SqliteBackendError {
    fn from(err: ControlPlaneError) -> Self {
        Self::ControlPlane(err)
    }
}

/// A single-file durable backend composing the command log, projection, and
/// control plane on ONE connection (TD-005).
pub struct SqliteBackend {
    conn: Mutex<Connection>,
    next_command_id: AtomicU64,
}

impl SqliteBackend {
    /// Open (or create) a file-backed durable backend. WAL + `synchronous`
    /// fsync the WAL on commit (the durable ack boundary). Acquires an exclusive
    /// single-writer lock — a second opener of the same file returns
    /// [`SqliteBackendError::AlreadyOpen`].
    pub fn open(
        path: impl AsRef<Path>,
        synchronous: SqliteSynchronous,
    ) -> Result<Self, SqliteBackendError> {
        let conn =
            Connection::open(path).map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        // Fail fast while probing single-writer ownership rather than blocking.
        conn.busy_timeout(Duration::from_millis(0))
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        conn.execute_batch(&format!(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous={}; PRAGMA locking_mode=EXCLUSIVE;",
            synchronous.pragma()
        ))
        .map_err(map_busy_as_already_open)?;
        // Acquire (and, under EXCLUSIVE locking mode, retain) the write lock now.
        // A second process holding it makes this fail with BUSY/LOCKED.
        conn.execute_batch("BEGIN IMMEDIATE; COMMIT;")
            .map_err(map_busy_as_already_open)?;
        init_schema(&conn).map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        bump_epochs_on_open(&conn)?;
        // Restore the in-process concurrency timeout for normal operation.
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
            next_command_id: AtomicU64::new(0),
        })
    }

    /// Open a file-backed backend with the default `synchronous=FULL` ack
    /// boundary (TD-005).
    pub fn open_durable(path: impl AsRef<Path>) -> Result<Self, SqliteBackendError> {
        Self::open(path, SqliteSynchronous::Full)
    }

    /// Open an in-memory backend (tests/dev only — not durable, no ownership
    /// lock, single connection so process-private).
    pub fn open_in_memory() -> Result<Self, SqliteBackendError> {
        let conn = Connection::open_in_memory()
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        init_schema(&conn).map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
            next_command_id: AtomicU64::new(0),
        })
    }

    /// Create a queue: persist the validated definition + shard assignments AND
    /// bootstrap each log shard's epoch to match the control-plane assignment
    /// (epoch 1) in ONE transaction — so a subsequent `append_and_apply` fenced
    /// with the assignment epoch does not hit a stale-epoch (the log defaults a
    /// fresh shard to epoch 0 otherwise). Returns whether the queue was created.
    pub async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> Result<bool, SqliteBackendError> {
        let tenant = definition.tenant_id.as_str().to_string();
        let queue = definition.queue_id.as_str().to_string();
        let json = serde_json::to_string(&definition)
            .map_err(|err| SqliteBackendError::Storage(format!("encode queue: {err}")))?;

        let mut conn = self.conn.lock().expect("backend mutex");
        let tx = conn.transaction().map_err(control_failure)?;
        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM pqueue_queue WHERE tenant_id = ?1 AND queue_id = ?2",
                params![tenant, queue],
                |_| Ok(()),
            )
            .optional()
            .map_err(control_failure)?
            .is_some();
        if exists {
            return Err(ControlPlaneError::QueueAlreadyExists.into());
        }
        tx.execute(
            "INSERT INTO pqueue_queue (tenant_id, queue_id, definition_json) VALUES (?1, ?2, ?3)",
            params![tenant, queue, json],
        )
        .map_err(control_failure)?;
        for shard_id in 0..definition.shard_count {
            // Control-plane assignment starts at epoch 1 (matches the in-memory
            // reference backend).
            tx.execute(
                "INSERT INTO pqueue_shard_assignment
                   (tenant_id, queue_id, shard_id, epoch, worker_id)
                 VALUES (?1, ?2, ?3, 1, NULL)",
                params![tenant, queue, shard_id as i64],
            )
            .map_err(control_failure)?;
            // Epoch bootstrap: keep the log shard's epoch in lockstep with the
            // control-plane assignment so the two never disagree.
            tx.execute(
                "INSERT INTO pqueue_log_shard (tenant_id, queue_id, shard_id, epoch)
                 VALUES (?1, ?2, ?3, 1)
                 ON CONFLICT(tenant_id, queue_id, shard_id) DO UPDATE SET epoch = 1",
                params![tenant, queue, shard_id as i64],
            )
            .map_err(control_failure)?;
        }
        tx.commit().map_err(control_failure)?;
        Ok(true)
    }

    /// **Headline**: append the commands to the log AND apply them to the
    /// projection in ONE transaction (one WAL fsync). On return the projection
    /// reflects the commands (strict read-after-write). `expected_epoch` fences
    /// stale appends after a restart (`None` = no fence, matching the embedder).
    pub async fn append_and_apply(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, SqliteBackendError> {
        // No `.await` while the `!Send` connection guard is held — keeps the
        // returned future `Send`.
        let mut conn = self.conn.lock().expect("backend mutex");
        let tx = conn
            .transaction()
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        let result = append_into_tx(&tx, shard, expected_epoch, &commands)?;
        apply_into_tx(&tx, shard, &commands)?;
        tx.commit()
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        Ok(result)
    }

    /// Atomically claim eligible pending items: select the FIFO-eligible ids
    /// (read-only), then append a `BatchClaim` command AND apply it in ONE
    /// transaction. The lease (and the single `attempts` increment) happen
    /// exactly once via the apply path — this is why the backend deliberately
    /// does NOT expose the standalone `batch_claim` (which also leases): mixing
    /// the two would double-increment `attempts`.
    pub async fn claim(
        &self,
        shard: &ShardKey,
        max_items: usize,
        lease_token: impl Into<String>,
        now: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> Result<Vec<ItemId>, SqliteBackendError> {
        let lease_token = lease_token.into();
        let mut conn = self.conn.lock().expect("backend mutex");
        if !shard_exists(&conn, shard)? {
            return Err(ProjectionError::QueueNotFound.into());
        }
        let now_nanos = ts_to_nanos(&now);
        let tx = conn
            .transaction()
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        let ids = select_eligible(&tx, shard, now_nanos, max_items)?;
        if ids.is_empty() {
            tx.commit()
                .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
            return Ok(Vec::new());
        }
        let item_ids = ids_to_item_ids(ids)?;
        let envelope = self.build_envelope(
            shard,
            item_ids.clone(),
            QueueCommand::BatchClaim(BatchClaimCommand {
                item_ids: item_ids.clone(),
                lease_token,
                lease_expires_at,
            }),
        );
        let commands = std::slice::from_ref(&envelope);
        append_into_tx(&tx, shard, None, commands)?;
        apply_into_tx(&tx, shard, commands)?;
        tx.commit()
            .map_err(|err| SqliteBackendError::Storage(err.to_string()))?;
        Ok(item_ids)
    }

    /// Push items (durably append+apply a `BatchPush`).
    pub async fn push(
        &self,
        shard: &ShardKey,
        items: Vec<PushItem>,
    ) -> Result<AppendBatchResult, SqliteBackendError> {
        let item_ids = items.iter().map(|item| item.item_id.clone()).collect();
        let envelope = self.build_envelope(
            shard,
            item_ids,
            QueueCommand::BatchPush(BatchPushCommand { items }),
        );
        self.append_and_apply(shard, None, vec![envelope]).await
    }

    /// Finalize claimed items (complete/fail/retry/release/rearm).
    pub async fn finalize(
        &self,
        shard: &ShardKey,
        outcomes: Vec<FinalizeOutcome>,
    ) -> Result<AppendBatchResult, SqliteBackendError> {
        let item_ids = outcomes
            .iter()
            .map(|outcome| outcome.item_id.clone())
            .collect();
        let envelope = self.build_envelope(
            shard,
            item_ids,
            QueueCommand::BatchFinalize(BatchFinalizeCommand { outcomes }),
        );
        self.append_and_apply(shard, None, vec![envelope]).await
    }

    /// Renew the leases on the given items.
    pub async fn renew_leases(
        &self,
        shard: &ShardKey,
        item_ids: Vec<ItemId>,
        lease_expires_at: UtcTimestamp,
    ) -> Result<AppendBatchResult, SqliteBackendError> {
        let envelope = self.build_envelope(
            shard,
            item_ids.clone(),
            QueueCommand::BatchRenewLeases(BatchRenewLeasesCommand {
                item_ids,
                lease_expires_at,
            }),
        );
        self.append_and_apply(shard, None, vec![envelope]).await
    }

    /// Return expired-lease items to pending.
    pub async fn expire_leases(
        &self,
        shard: &ShardKey,
        item_ids: Vec<ItemId>,
    ) -> Result<AppendBatchResult, SqliteBackendError> {
        let envelope = self.build_envelope(
            shard,
            item_ids.clone(),
            QueueCommand::LeaseExpired(LeaseExpiredCommand { item_ids }),
        );
        self.append_and_apply(shard, None, vec![envelope]).await
    }

    /// Lifecycle-state counts for a queue.
    pub async fn metrics(
        &self,
        queue: &QueueKey,
    ) -> Result<QueueMetricsSnapshot, SqliteBackendError> {
        let conn = self.conn.lock().expect("backend mutex");
        Ok(metrics_query(&conn, queue)?)
    }

    /// Read an indexed page of committed commands from the log.
    pub async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, SqliteBackendError> {
        let conn = self.conn.lock().expect("backend mutex");
        Ok(read_page(&conn, shard, position, limit)?)
    }

    /// The persisted (validated) queue definition.
    pub async fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> Result<QueueDefinition, SqliteBackendError> {
        let conn = self.conn.lock().expect("backend mutex");
        let json: Option<String> = conn
            .query_row(
                "SELECT definition_json FROM pqueue_queue WHERE tenant_id = ?1 AND queue_id = ?2",
                params![key.tenant_id.as_str(), key.queue_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(control_failure)?;
        let json = json.ok_or(ControlPlaneError::QueueNotFound)?;
        serde_json::from_str(&json)
            .map_err(|err| SqliteBackendError::Storage(format!("decode queue: {err}")))
    }

    /// Shard assignments (shard → epoch) for a queue.
    pub async fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> Result<Vec<ShardAssignment>, SqliteBackendError> {
        let conn = self.conn.lock().expect("backend mutex");
        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM pqueue_queue WHERE tenant_id = ?1 AND queue_id = ?2",
                params![key.tenant_id.as_str(), key.queue_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(control_failure)?
            .is_some();
        if !exists {
            return Err(ControlPlaneError::QueueNotFound.into());
        }
        let mut stmt = conn
            .prepare(
                "SELECT shard_id, epoch, worker_id FROM pqueue_shard_assignment
                 WHERE tenant_id = ?1 AND queue_id = ?2 ORDER BY shard_id",
            )
            .map_err(control_failure)?;
        let assignments = stmt
            .query_map(
                params![key.tenant_id.as_str(), key.queue_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u32,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .map_err(control_failure)?
            .map(|row| {
                row.map(|(shard_id, epoch, worker_id)| ShardAssignment {
                    shard_key: ShardKey {
                        tenant_id: key.tenant_id.clone(),
                        queue_id: key.queue_id.clone(),
                        shard_id: ShardId::new(shard_id),
                    },
                    epoch,
                    worker_id,
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(control_failure)?;
        Ok(assignments)
    }

    /// List queues for a tenant.
    pub async fn list_queues(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<QueueId>, SqliteBackendError> {
        let conn = self.conn.lock().expect("backend mutex");
        let mut stmt = conn
            .prepare("SELECT queue_id FROM pqueue_queue WHERE tenant_id = ?1 ORDER BY queue_id")
            .map_err(control_failure)?;
        let ids = stmt
            .query_map(params![tenant_id.as_str()], |row| row.get::<_, String>(0))
            .map_err(control_failure)?
            .map(|row| {
                row.map_err(control_failure).and_then(|s| {
                    QueueId::new(s)
                        .map_err(|err| ControlPlaneError::StorageFailure(err.to_string()))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ids)
    }

    fn build_envelope(
        &self,
        shard: &ShardKey,
        item_ids: Vec<ItemId>,
        command: QueueCommand,
    ) -> CommandEnvelope {
        let n = self.next_command_id.fetch_add(1, Ordering::Relaxed);
        CommandEnvelope {
            command_id: CommandId::new(format!("sqlite-backend-{n}")),
            request_id: None,
            tenant_id: shard.tenant_id.clone(),
            queue_id: shard.queue_id.clone(),
            shard_id: shard.shard_id.clone(),
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(0, 0).expect("zero timestamp is valid"),
        }
    }
}

/// Initialize the union schema (log + projection + control plane) on one
/// connection.
fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    initialize_log_schema(conn)?;
    initialize_projection_schema(conn)?;
    initialize_control_plane_schema(conn)?;
    Ok(())
}

/// Bump every shard's epoch in lockstep across the log and control-plane tables
/// (restart fencing / observability). A no-op on a fresh database. Single-writer
/// means this is not a CAS — it records that a new owner has taken the file.
fn bump_epochs_on_open(conn: &Connection) -> Result<(), SqliteBackendError> {
    conn.execute("UPDATE pqueue_log_shard SET epoch = epoch + 1", [])
        .map_err(storage_failure)?;
    conn.execute("UPDATE pqueue_shard_assignment SET epoch = epoch + 1", [])
        .map_err(storage_failure)?;
    Ok(())
}

/// Map a SQLite BUSY/LOCKED error (another process owns the file) to the
/// single-writer `AlreadyOpen` error; everything else is a storage failure.
fn map_busy_as_already_open(err: rusqlite::Error) -> SqliteBackendError {
    if let rusqlite::Error::SqliteFailure(failure, _) = &err
        && matches!(
            failure.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        )
    {
        return SqliteBackendError::AlreadyOpen;
    }
    SqliteBackendError::Storage(err.to_string())
}
