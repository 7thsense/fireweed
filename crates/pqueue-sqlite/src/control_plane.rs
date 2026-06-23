//! `ControlPlaneStore` over SQLite (TD-005): durable queue definitions and
//! shard assignments in the same database family as the log and projection.
//!
//! `QueueDefinition` is persisted as JSON (the pqueue-core config types are
//! serde, and id newtypes validate on deserialize, so a stored definition can
//! never be reconstituted invalid).

use std::sync::Mutex;

use pqueue_core::{QueueDefinition, QueueId, TenantId};
use pqueue_storage::traits::{
    ControlPlaneError, ControlPlaneStore, CreateQueueResult, ShardAssignment,
};
use pqueue_storage::types::{QueueKey, ShardId, ShardKey};
use rusqlite::{Connection, OptionalExtension, params};

/// A SQLite-backed control plane (queue definitions + shard assignments).
pub struct SqliteControlPlaneStore {
    conn: Mutex<Connection>,
}

impl SqliteControlPlaneStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        initialize_control_plane_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        initialize_control_plane_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl ControlPlaneStore for SqliteControlPlaneStore {
    async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> Result<CreateQueueResult, ControlPlaneError> {
        let tenant = definition.tenant_id.as_str().to_string();
        let queue = definition.queue_id.as_str().to_string();
        let json = serde_json::to_string(&definition)
            .map_err(|err| ControlPlaneError::StorageFailure(format!("encode queue: {err}")))?;

        let mut conn = self.conn.lock().expect("control-plane mutex");
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
            return Err(ControlPlaneError::QueueAlreadyExists);
        }
        tx.execute(
            "INSERT INTO pqueue_queue (tenant_id, queue_id, definition_json)
             VALUES (?1, ?2, ?3)",
            params![tenant, queue, json],
        )
        .map_err(control_failure)?;
        // Shards start at epoch 1, matching the in-memory reference backend.
        for shard_id in 0..definition.shard_count {
            tx.execute(
                "INSERT INTO pqueue_shard_assignment
                   (tenant_id, queue_id, shard_id, epoch, worker_id)
                 VALUES (?1, ?2, ?3, 1, NULL)",
                params![tenant, queue, shard_id as i64],
            )
            .map_err(control_failure)?;
        }
        tx.commit().map_err(control_failure)?;
        Ok(CreateQueueResult {
            created: true,
            definition,
        })
    }

    async fn queue_definition(&self, key: &QueueKey) -> Result<QueueDefinition, ControlPlaneError> {
        let conn = self.conn.lock().expect("control-plane mutex");
        let json: Option<String> = conn
            .query_row(
                "SELECT definition_json FROM pqueue_queue
                 WHERE tenant_id = ?1 AND queue_id = ?2",
                params![key.tenant_id.as_str(), key.queue_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(control_failure)?;
        let json = json.ok_or(ControlPlaneError::QueueNotFound)?;
        serde_json::from_str(&json)
            .map_err(|err| ControlPlaneError::StorageFailure(format!("decode queue: {err}")))
    }

    async fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> Result<Vec<ShardAssignment>, ControlPlaneError> {
        let conn = self.conn.lock().expect("control-plane mutex");
        // Distinguish "queue absent" (QueueNotFound) from "queue with zero shards".
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
            return Err(ControlPlaneError::QueueNotFound);
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

    async fn list_queues(&self, tenant_id: &TenantId) -> Result<Vec<QueueId>, ControlPlaneError> {
        let conn = self.conn.lock().expect("control-plane mutex");
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
}

pub(crate) fn control_failure(err: rusqlite::Error) -> ControlPlaneError {
    ControlPlaneError::StorageFailure(err.to_string())
}

pub(crate) fn initialize_control_plane_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS pqueue_queue (
            tenant_id       TEXT NOT NULL,
            queue_id        TEXT NOT NULL,
            definition_json TEXT NOT NULL,
            PRIMARY KEY (tenant_id, queue_id)
        );
        CREATE TABLE IF NOT EXISTS pqueue_shard_assignment (
            tenant_id TEXT NOT NULL,
            queue_id  TEXT NOT NULL,
            shard_id  INTEGER NOT NULL,
            epoch     INTEGER NOT NULL,
            worker_id TEXT,
            PRIMARY KEY (tenant_id, queue_id, shard_id)
        );",
    )
}
