//! The durable sqlite command-log axis (ADR-012, Phase 1).
//!
//! [`SqliteLog`] is a [`pqueue_engine::LogStore`] whose command log + epoch + high-water + snapshots are
//! durable rows in sqlite. Extracted from the monolithic [`crate::SqliteBackend`]'s `Inner` so the
//! composed sqlite backend (`ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>`) is
//! behaviorally identical to the monolith on the shared TD-001 suite.
//!
//! Unlike the monolith, the epoch lives in this LOG axis (a `log_epochs` table), not in a `queues` table —
//! ADR-012 co-locates the epoch/fence authority with the log and leaves queue DEFINITIONS to the separate
//! control-plane axis. There is therefore no `queues` table here: the control plane owns definitions.

use pqueue_core::QueueDefinition;
use pqueue_engine::{
    CommandEnvelope, CommandPage, CommandPosition, EngineError, EngineResult, LogStore,
    ProjectionSnapshot, QueueKey, SnapshotRef,
};
use rusqlite::{Connection, OptionalExtension, params};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS log_epochs (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    assignment_epoch INTEGER NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS log_entries (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
    envelope TEXT NOT NULL,
    PRIMARY KEY (tenant, queue, epoch, seq)
);
CREATE TABLE IF NOT EXISTS high_water (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS snapshots (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, ref_id TEXT NOT NULL,
    epoch INTEGER NOT NULL, seq INTEGER NOT NULL, payload BLOB NOT NULL,
    PRIMARY KEY (tenant, queue, ref_id)
);
-- Durable queue-definition catalog (ADR-012 P2 recovery-on-open). The composition's in-process control
-- plane is not durable, so the log persists definitions here; a reopened composition enumerates them to
-- rebuild the in-memory projection WITHOUT a re-create_queue. The epoch/fence stays in `log_epochs`.
CREATE TABLE IF NOT EXISTS queue_defs (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
"#;

fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

fn opt<T>(r: rusqlite::Result<T>) -> EngineResult<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

/// The durable sqlite command-log axis.
pub struct SqliteLog {
    conn: Connection,
}

impl SqliteLog {
    /// Open (or create) a durable sqlite log at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` durable log (a real command log within the process).
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        st(conn.execute_batch(SCHEMA))?;
        Ok(Self { conn })
    }

    /// Read a shard's durable `assignment_epoch`. Missing shard → `NotFound`.
    fn read_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let (t, q) = parts(shard);
        let epoch: Option<i64> = st(self
            .conn
            .query_row(
                "SELECT assignment_epoch FROM log_epochs WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?;
        Ok(epoch.ok_or(EngineError::NotFound)? as u64)
    }
}

impl LogStore for SqliteLog {
    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        let (t, q) = parts(shard);
        st(self.conn.execute(
            "INSERT INTO log_epochs(tenant,queue,assignment_epoch) VALUES(?1,?2,0) \
             ON CONFLICT(tenant,queue) DO NOTHING",
            params![t, q],
        ))?;
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.read_epoch(shard)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        let (t, q) = parts(shard);
        // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
        let new_epoch: Option<i64> = st(self
            .conn
            .query_row(
                "UPDATE log_epochs SET assignment_epoch = assignment_epoch + 1 \
                 WHERE tenant=?1 AND queue=?2 RETURNING assignment_epoch",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?;
        new_epoch.ok_or(EngineError::NotFound).map(|e| e as u64)
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        let tx = st(self.conn.transaction())?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        let epoch: i64 = st(tx
            .query_row(
                "SELECT assignment_epoch FROM log_epochs WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        for env in commands {
            let json = to_json(env)?;
            // MAX(seq)+1, NOT COUNT(*): must survive compaction/retention so a position never collides or
            // regresses (TD-007 §4). Empty log → -1+1 = 0.
            let seq: i64 = st(tx.query_row(
                "SELECT COALESCE(MAX(seq), -1) + 1 FROM log_entries WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            ))?;
            st(tx.execute(
                "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES(?1,?2,?3,?4,?5)",
                params![t, q, epoch, seq, json],
            ))?;
            st(tx.execute(
                "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES(?1,?2,?3,?4) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
                params![t, q, epoch, seq],
            ))?;
            positions.push(CommandPosition::new(
                shard.clone(),
                epoch as u64,
                seq as u64,
            ));
        }
        st(tx.commit())?;
        Ok(positions)
    }

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage> {
        let (t, q) = parts(shard);
        let start = match &from {
            Some(p) => p.sequence + 1,
            None => 0,
        };
        let total: i64 = st(self.conn.query_row(
            "SELECT COUNT(*) FROM log_entries WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        ))?;
        let mut stmt = st(self.conn.prepare(
            "SELECT seq, epoch, envelope FROM log_entries \
             WHERE tenant=?1 AND queue=?2 AND seq>=?3 ORDER BY seq LIMIT ?4",
        ))?;
        let mapped = st(
            stmt.query_map(params![t, q, start as i64, limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            }),
        )?;
        let mut entries = Vec::new();
        for r in mapped {
            let (seq, epoch, json) = st(r)?;
            let env: CommandEnvelope =
                serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?;
            entries.push((
                CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
                env,
            ));
        }
        let consumed = start + entries.len() as u64;
        let cursor_epoch = entries.last().map(|(p, _)| p.backend_epoch).unwrap_or(0);
        let next = (consumed < total as u64)
            .then(|| CommandPosition::new(shard.clone(), cursor_epoch, consumed));
        Ok(CommandPage { entries, next })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let (t, q) = parts(shard);
        let row = opt(self.conn.query_row(
            "SELECT epoch, seq FROM high_water WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        ))?;
        Ok(row.map(|(epoch, seq)| CommandPosition::new(shard.clone(), epoch as u64, seq as u64)))
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        let (t, q) = parts(shard);
        // Monotonic: reject a position that does not advance the stored one (TD-007 §4).
        let current = opt(self.conn.query_row(
            "SELECT epoch, seq FROM high_water WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        ))?;
        if let Some((epoch, seq)) = current {
            let cur = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
            if !cur.precedes(&position) && cur != position {
                return Err(EngineError::Invalid("high-water regression"));
            }
        }
        st(self.conn.execute(
            "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES(?1,?2,?3,?4) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
            params![
                t,
                q,
                position.backend_epoch as i64,
                position.sequence as i64
            ],
        ))?;
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        let (t, q) = parts(shard);
        let n: i64 = st(self.conn.query_row(
            "SELECT COUNT(*) FROM snapshots WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        ))?;
        let ref_id = format!("snap-{n}");
        st(self.conn.execute(
            "INSERT INTO snapshots(tenant,queue,ref_id,epoch,seq,payload) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                t,
                q,
                ref_id,
                position.backend_epoch as i64,
                position.sequence as i64,
                snapshot.payload
            ],
        ))?;
        Ok(SnapshotRef {
            queue: shard.clone(),
            position,
            ref_id,
        })
    }

    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        let (t, q) = parts(shard);
        let row = opt(self.conn.query_row(
            "SELECT ref_id, epoch, seq FROM snapshots \
             WHERE tenant=?1 AND queue=?2 ORDER BY rowid DESC LIMIT 1",
            params![t, q],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        ))?;
        Ok(row.map(|(ref_id, epoch, seq)| SnapshotRef {
            queue: shard.clone(),
            position: CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
            ref_id,
        }))
    }

    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        let (t, q) = parts(&snapshot_ref.queue);
        let payload: Option<Vec<u8>> = opt(self.conn.query_row(
            "SELECT payload FROM snapshots WHERE tenant=?1 AND queue=?2 AND ref_id=?3",
            params![t, q, snapshot_ref.ref_id],
            |row| row.get(0),
        ))?;
        payload
            .map(|payload| ProjectionSnapshot { payload })
            .ok_or(EngineError::NotFound)
    }

    fn persist_definition(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let (t, q) = parts(&QueueKey::new(
            definition.tenant_id.clone(),
            definition.queue_id.clone(),
        ));
        st(self.conn.execute(
            "INSERT INTO queue_defs(tenant,queue,definition) VALUES(?1,?2,?3) \
             ON CONFLICT(tenant,queue) DO UPDATE SET definition=excluded.definition",
            params![t, q, to_json(definition)?],
        ))?;
        Ok(())
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        let mut stmt = st(self.conn.prepare("SELECT definition FROM queue_defs"))?;
        let mapped = st(stmt.query_map([], |row| row.get::<_, String>(0)))?;
        let mut out = Vec::new();
        for r in mapped {
            let json = st(r)?;
            out.push(serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
        Ok(out)
    }
}
