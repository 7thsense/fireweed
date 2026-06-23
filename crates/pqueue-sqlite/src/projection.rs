//! Full `ProjectionStore` over SQLite (TD-005).
//!
//! This is the durable, queryable item-lifecycle projection used by BOTH the
//! standalone `sqlite` backend and the `object_log_sqlite_projection` backend.
//! It mirrors the reference `MemoryProjectionStore` semantics exactly so it
//! passes the shared storage-conformance suite:
//!
//! - `apply_committed` ingests committed commands and drives the item lifecycle
//!   state machine (`pqueue_core::apply_transition`).
//! - `batch_claim` selects eligible pending items (state + `not_before` +
//!   retry-backoff), ordered by insertion order, and atomically leases them — the
//!   single-active-lease serialization point, here a SQLite transaction.
//! - `metrics` counts items by lifecycle state for a queue.

use std::sync::Mutex;

use pqueue_core::{ItemEvent, ItemState, UtcTimestamp, apply_transition};
use pqueue_storage::commands::{CommandEnvelope, FinalizeKind, QueueCommand};
use pqueue_storage::traits::{
    ClaimRequest, ClaimResult, ProjectionError, ProjectionStore, QueueMetricsSnapshot,
};
use pqueue_storage::types::{CommandPosition, QueueKey, ShardKey};
use rusqlite::{Connection, OptionalExtension, params};

/// A SQLite-backed projection implementing the full `ProjectionStore` contract.
pub struct SqliteProjectionStore {
    conn: Mutex<Connection>,
}

impl SqliteProjectionStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        initialize_projection_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        initialize_projection_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl ProjectionStore for SqliteProjectionStore {
    async fn apply_committed(
        &self,
        position: CommandPosition,
        commands: &[CommandEnvelope],
    ) -> Result<(), ProjectionError> {
        // No `.await` while the `!Send` guard is held — keeps the future `Send`.
        let mut conn = self.conn.lock().expect("projection mutex");
        let tx = conn.transaction().map_err(projection_failure)?;
        apply_into_tx(&tx, &position.shard_key, commands)?;
        tx.commit().map_err(projection_failure)?;
        Ok(())
    }

    async fn batch_claim(&self, request: ClaimRequest) -> Result<ClaimResult, ProjectionError> {
        let mut conn = self.conn.lock().expect("projection mutex");
        if !shard_exists(&conn, &request.shard_key)? {
            return Err(ProjectionError::QueueNotFound);
        }
        let now = ts_to_nanos(&request.now);
        let lease_expires = ts_to_nanos(&request.lease_expires_at);
        let tx = conn.transaction().map_err(projection_failure)?;
        let shard = &request.shard_key;

        let ids = select_eligible(&tx, shard, now, request.max_items)?;
        for id in &ids {
            tx.execute(
                "UPDATE pqueue_proj_items
                 SET state = 'leased', attempts = attempts + 1,
                     lease_token = ?4, lease_expires_nanos = ?5
                 WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?6",
                params![
                    shard.tenant_id.as_str(),
                    shard.queue_id.as_str(),
                    shard.shard_id.as_u32() as i64,
                    request.lease_token,
                    lease_expires,
                    id
                ],
            )
            .map_err(projection_failure)?;
        }
        tx.commit().map_err(projection_failure)?;

        let claimed_item_ids = ids_to_item_ids(ids)?;
        Ok(ClaimResult {
            claimed_item_ids,
            lease_token: request.lease_token,
        })
    }

    async fn metrics(&self, queue: &QueueKey) -> Result<QueueMetricsSnapshot, ProjectionError> {
        let conn = self.conn.lock().expect("projection mutex");
        metrics_query(&conn, queue)
    }
}

/// Apply committed commands within an existing transaction (the shared apply
/// core). `SqliteBackend` composes this with the log append core in ONE
/// transaction (TD-005 atomic append+apply); the standalone
/// `SqliteProjectionStore` wraps it in its own transaction. No `.await` here,
/// so the caller's future stays `Send` while it holds the connection guard.
pub(crate) fn apply_into_tx(
    tx: &Connection,
    shard: &ShardKey,
    commands: &[CommandEnvelope],
) -> Result<(), ProjectionError> {
    register_shard(tx, shard)?;
    for envelope in commands {
        apply_one(tx, shard, envelope)?;
    }
    Ok(())
}

/// Select eligible pending item ids (the FIFO eligibility read), WITHOUT
/// leasing. The standalone `batch_claim` leases the result; `SqliteBackend`'s
/// atomic `claim` instead appends a `BatchClaim` command and applies it, so the
/// lease (and the single `attempts` increment) happen exactly once via the apply
/// path. `now_nanos` gates `not_before` / retry-backoff.
pub(crate) fn select_eligible(
    conn: &Connection,
    shard: &ShardKey,
    now_nanos: i64,
    max_items: usize,
) -> Result<Vec<String>, ProjectionError> {
    // Eligible = pending AND not_before<=now AND retry_backoff<=now,
    // ordered by insertion order (the FIFO tie-break), capped at max_items.
    let mut stmt = conn
        .prepare(
            "SELECT item_id FROM pqueue_proj_items
             WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3
               AND state = 'pending'
               AND (not_before_nanos IS NULL OR not_before_nanos <= ?4)
               AND (retry_backoff_nanos IS NULL OR retry_backoff_nanos <= ?4)
             ORDER BY insertion_order LIMIT ?5",
        )
        .map_err(projection_failure)?;
    let max = if max_items > i64::MAX as usize {
        i64::MAX
    } else {
        max_items as i64
    };
    stmt.query_map(
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64,
            now_nanos,
            max
        ],
        |row| row.get::<_, String>(0),
    )
    .map_err(projection_failure)?
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(projection_failure)
}

pub(crate) fn ids_to_item_ids(
    ids: Vec<String>,
) -> Result<Vec<pqueue_core::ItemId>, ProjectionError> {
    ids.into_iter()
        .map(|id| {
            pqueue_core::ItemId::new(id)
                .map_err(|err| ProjectionError::StorageFailure(err.to_string()))
        })
        .collect()
}

/// Count items by lifecycle state for a queue (the shared metrics read). Shared
/// by the standalone `SqliteProjectionStore` and `SqliteBackend`.
pub(crate) fn metrics_query(
    conn: &Connection,
    queue: &QueueKey,
) -> Result<QueueMetricsSnapshot, ProjectionError> {
    // A queue "exists" once any of its shards has been touched (matches the
    // in-memory backend's shard-map presence).
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM pqueue_proj_shard WHERE tenant_id = ?1 AND queue_id = ?2 LIMIT 1",
            params![queue.tenant_id.as_str(), queue.queue_id.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(projection_failure)?
        .is_some();
    if !exists {
        return Err(ProjectionError::QueueNotFound);
    }

    let mut snapshot = QueueMetricsSnapshot::default();
    let mut stmt = conn
        .prepare(
            "SELECT state, COUNT(*) FROM pqueue_proj_items
             WHERE tenant_id = ?1 AND queue_id = ?2 GROUP BY state",
        )
        .map_err(projection_failure)?;
    let rows = stmt
        .query_map(
            params![queue.tenant_id.as_str(), queue.queue_id.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64)),
        )
        .map_err(projection_failure)?;
    for row in rows {
        let (state, count) = row.map_err(projection_failure)?;
        match state.as_str() {
            "pending" => snapshot.pending_count += count,
            "leased" => snapshot.leased_count += count,
            "complete" => snapshot.completed_count += count,
            "failed" => snapshot.failed_count += count,
            _ => {}
        }
    }
    Ok(snapshot)
}

fn apply_one(
    tx: &Connection,
    shard: &ShardKey,
    envelope: &CommandEnvelope,
) -> Result<(), ProjectionError> {
    match &envelope.command {
        QueueCommand::BatchPush(cmd) => {
            let mut order = next_insertion_order(tx, shard)?;
            for item in &cmd.items {
                // Re-push overwrites (idempotent by item_id), matching memory.
                tx.execute(
                    "INSERT INTO pqueue_proj_items
                       (tenant_id, queue_id, shard_id, item_id, state,
                        not_before_nanos, retry_backoff_nanos, max_attempts, attempts,
                        lease_token, lease_expires_nanos, insertion_order)
                     VALUES (?1, ?2, ?3, ?4, 'pending', ?5, NULL, ?6, 0, NULL, NULL, ?7)
                     ON CONFLICT(tenant_id, queue_id, shard_id, item_id) DO UPDATE SET
                        state = 'pending', not_before_nanos = excluded.not_before_nanos,
                        retry_backoff_nanos = NULL, max_attempts = excluded.max_attempts,
                        attempts = 0, lease_token = NULL, lease_expires_nanos = NULL,
                        insertion_order = excluded.insertion_order",
                    params![
                        shard.tenant_id.as_str(),
                        shard.queue_id.as_str(),
                        shard.shard_id.as_u32() as i64,
                        item.item_id.as_str(),
                        item.not_before.as_ref().map(ts_to_nanos),
                        item.max_attempts as i64,
                        order
                    ],
                )
                .map_err(projection_failure)?;
                order += 1;
            }
        }
        QueueCommand::BatchClaim(cmd) => {
            for id in &cmd.item_ids {
                if transition(tx, shard, id.as_str(), ItemEvent::Claim)? {
                    tx.execute(
                        "UPDATE pqueue_proj_items
                         SET attempts = attempts + 1, lease_token = ?4, lease_expires_nanos = ?5
                         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?6",
                        params![
                            shard.tenant_id.as_str(),
                            shard.queue_id.as_str(),
                            shard.shard_id.as_u32() as i64,
                            cmd.lease_token,
                            ts_to_nanos(&cmd.lease_expires_at),
                            id.as_str()
                        ],
                    )
                    .map_err(projection_failure)?;
                }
            }
        }
        QueueCommand::BatchFinalize(cmd) => {
            for outcome in &cmd.outcomes {
                let event = finalize_event(outcome.kind);
                if transition(tx, shard, outcome.item_id.as_str(), event)? {
                    clear_lease(tx, shard, outcome.item_id.as_str())?;
                }
            }
        }
        QueueCommand::BatchRenewLeases(cmd) => {
            for id in &cmd.item_ids {
                tx.execute(
                    "UPDATE pqueue_proj_items SET lease_expires_nanos = ?4
                     WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?5",
                    params![
                        shard.tenant_id.as_str(),
                        shard.queue_id.as_str(),
                        shard.shard_id.as_u32() as i64,
                        ts_to_nanos(&cmd.lease_expires_at),
                        id.as_str()
                    ],
                )
                .map_err(projection_failure)?;
            }
        }
        QueueCommand::LeaseExpired(cmd) => {
            for id in &cmd.item_ids {
                if transition(tx, shard, id.as_str(), ItemEvent::LeaseExpired)? {
                    clear_lease(tx, shard, id.as_str())?;
                }
            }
        }
        // CreateQueue, BatchUpdate, CohortExpired, PurgeItems handled elsewhere.
        _ => {}
    }
    Ok(())
}

/// Apply a lifecycle event to one item if the transition is legal; returns
/// whether the state changed.
fn transition(
    tx: &Connection,
    shard: &ShardKey,
    item_id: &str,
    event: ItemEvent,
) -> Result<bool, ProjectionError> {
    let current: Option<String> = tx
        .query_row(
            "SELECT state FROM pqueue_proj_items
             WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?4",
            params![
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                shard.shard_id.as_u32() as i64,
                item_id
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(projection_failure)?;
    let Some(current) = current else {
        return Ok(false);
    };
    let Some(state) = parse_state(&current) else {
        return Ok(false);
    };
    let Ok(next) = apply_transition(state, event) else {
        return Ok(false);
    };
    tx.execute(
        "UPDATE pqueue_proj_items SET state = ?5
         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?4",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64,
            item_id,
            state_str(next)
        ],
    )
    .map_err(projection_failure)?;
    Ok(true)
}

fn clear_lease(tx: &Connection, shard: &ShardKey, item_id: &str) -> Result<(), ProjectionError> {
    tx.execute(
        "UPDATE pqueue_proj_items SET lease_token = NULL, lease_expires_nanos = NULL
         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3 AND item_id = ?4",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64,
            item_id
        ],
    )
    .map_err(projection_failure)?;
    Ok(())
}

fn next_insertion_order(tx: &Connection, shard: &ShardKey) -> Result<i64, ProjectionError> {
    tx.query_row(
        "SELECT COALESCE(MAX(insertion_order), -1) + 1 FROM pqueue_proj_items
         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64
        ],
        |row| row.get::<_, i64>(0),
    )
    .map_err(projection_failure)
}

pub(crate) fn register_shard(tx: &Connection, shard: &ShardKey) -> Result<(), ProjectionError> {
    tx.execute(
        "INSERT OR IGNORE INTO pqueue_proj_shard (tenant_id, queue_id, shard_id)
         VALUES (?1, ?2, ?3)",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64
        ],
    )
    .map_err(projection_failure)?;
    Ok(())
}

pub(crate) fn shard_exists(conn: &Connection, shard: &ShardKey) -> Result<bool, ProjectionError> {
    conn.query_row(
        "SELECT 1 FROM pqueue_proj_shard
         WHERE tenant_id = ?1 AND queue_id = ?2 AND shard_id = ?3",
        params![
            shard.tenant_id.as_str(),
            shard.queue_id.as_str(),
            shard.shard_id.as_u32() as i64
        ],
        |_| Ok(()),
    )
    .optional()
    .map(|opt| opt.is_some())
    .map_err(projection_failure)
}

fn finalize_event(kind: FinalizeKind) -> ItemEvent {
    match kind {
        FinalizeKind::Complete => ItemEvent::FinalizeComplete,
        FinalizeKind::Fail => ItemEvent::FinalizeFail,
        FinalizeKind::Retry => ItemEvent::FinalizeRetry,
        FinalizeKind::Release => ItemEvent::FinalizeRelease,
        FinalizeKind::Rearm => ItemEvent::FinalizeRearm,
    }
}

fn parse_state(s: &str) -> Option<ItemState> {
    match s {
        "pending" => Some(ItemState::Pending),
        "leased" => Some(ItemState::Leased),
        "complete" => Some(ItemState::Complete),
        "failed" => Some(ItemState::Failed),
        _ => None,
    }
}

fn state_str(state: ItemState) -> &'static str {
    match state {
        ItemState::Pending => "pending",
        ItemState::Leased => "leased",
        ItemState::Complete => "complete",
        ItemState::Failed => "failed",
    }
}

pub(crate) fn ts_to_nanos(ts: &UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanoseconds as i64)
}

pub(crate) fn projection_failure(err: rusqlite::Error) -> ProjectionError {
    ProjectionError::StorageFailure(err.to_string())
}

pub(crate) fn initialize_projection_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS pqueue_proj_shard (
            tenant_id TEXT NOT NULL,
            queue_id  TEXT NOT NULL,
            shard_id  INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, queue_id, shard_id)
        );
        CREATE TABLE IF NOT EXISTS pqueue_proj_items (
            tenant_id           TEXT NOT NULL,
            queue_id            TEXT NOT NULL,
            shard_id            INTEGER NOT NULL,
            item_id             TEXT NOT NULL,
            state               TEXT NOT NULL,
            not_before_nanos    INTEGER,
            retry_backoff_nanos INTEGER,
            max_attempts        INTEGER NOT NULL,
            attempts            INTEGER NOT NULL,
            lease_token         TEXT,
            lease_expires_nanos INTEGER,
            insertion_order     INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, queue_id, shard_id, item_id)
        );
        CREATE INDEX IF NOT EXISTS idx_proj_items_claim
          ON pqueue_proj_items (tenant_id, queue_id, shard_id, state, insertion_order);",
    )
}
