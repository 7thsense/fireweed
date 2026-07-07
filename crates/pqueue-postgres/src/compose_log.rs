//! The durable postgres command-LOG axis (ADR-012 P2).
//!
//! [`PostgresLog`] is a [`pqueue_engine::LogStore`] whose command log + epoch + high-water + snapshots +
//! durable queue catalog are rows in postgres, driven over the SYNC `postgres` client. Extracted from the
//! monolithic [`crate::PostgresBackend`]'s `Inner` (same SQL, same fence/sequence rules) so the composed
//! postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection, InProcessControlPlane>`) is
//! behaviorally identical to the monolith on the shared TD-001 suite — but with the orthogonal orchestration
//! living ONCE in [`pqueue_engine::ComposedBackend`].
//!
//! Like [`pqueue_sqlite::SqliteLog`], the epoch lives in THIS log axis (a `log_epochs` table), not in a
//! `queues` definition table — ADR-012 co-locates the epoch/fence authority with the log and leaves queue
//! DEFINITIONS to the separate control-plane axis. The durable `queue_defs` catalog here exists only so a
//! reopened composition can enumerate its queues for recovery-on-open WITHOUT a re-`create_queue` (the
//! in-process control plane is not itself durable).
//!
//! ## Interior mutability
//!
//! The sync `postgres::Client` requires `&mut` even for queries, but [`LogStore`]'s read methods take
//! `&self`. The client lives behind a [`RefCell`] so the `&self` reads can borrow it mutably. This is sound:
//! the composition serializes EVERY axis call behind its unit-of-work `Mutex`, so there is never a
//! concurrent or re-entrant borrow (a `borrow_mut` here cannot conflict). `PostgresLog` is therefore `Send`
//! (the bound `LogStore` requires) but not `Sync` — which is fine, because the composition only needs its
//! log axis to be `Send` (it is held inside `Mutex<Inner>`).
//!
//! ## Blocking caveat (same as the monolith)
//!
//! Every method makes a **blocking** postgres network call. The sync `postgres` client drives its own
//! internal tokio runtime per call, so the composition MUST be driven off the reactor (the `pqueue-server`
//! blocking wrapper delegates every port call to `spawn_blocking`). The composition's own port methods are
//! sync-but-`ready`, so this LogStore's calls run on whatever thread drives the composition — in production,
//! a Tokio blocking-pool thread, never a reactor worker.

use std::cell::RefCell;

use postgres::Client;
use pqueue_core::QueueDefinition;
use pqueue_engine::{
    CommandEnvelope, CommandPage, CommandPosition, EngineError, EngineResult, LogStore,
    ProjectionSnapshot, QueueKey, SnapshotRef,
};

use crate::connect::{PostgresConnectConfig, connect};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS log_epochs (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    assignment_epoch BIGINT NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS log_entries (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch BIGINT NOT NULL, seq BIGINT NOT NULL,
    envelope TEXT NOT NULL,
    PRIMARY KEY (tenant, queue, epoch, seq)
);
CREATE TABLE IF NOT EXISTS log_counters (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, next_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS high_water (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch BIGINT NOT NULL, seq BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS snapshots (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, ref_id TEXT NOT NULL,
    ord BIGSERIAL, epoch BIGINT NOT NULL, seq BIGINT NOT NULL, payload BYTEA NOT NULL,
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

fn st<T>(r: Result<T, postgres::Error>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
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

/// The durable postgres command-log axis (ADR-012). The composition serializes access behind its
/// unit-of-work `Mutex`, so the single connection in the [`RefCell`] is never used concurrently.
pub struct PostgresLog {
    client: RefCell<Client>,
}

impl PostgresLog {
    /// Connect to `url` (default `search_path`) and ensure the log schema.
    pub fn connect(url: &str) -> EngineResult<Self> {
        Self::connect_with_config(PostgresConnectConfig::new(url))
    }

    /// Connect using a fully-built [`PostgresConnectConfig`] (the credential-provider-aware path for
    /// Lakebase) and ensure the log schema.
    pub fn connect_with_config(config: PostgresConnectConfig) -> EngineResult<Self> {
        Self::from_client(connect(config)?)
    }

    /// Connect and isolate this log in a dedicated `schema` (`CREATE SCHEMA IF NOT EXISTS` + `SET
    /// search_path`). Reconnecting with the SAME `schema` reopens the same durable log — used by the
    /// conformance/recovery suites (a fresh schema per scenario).
    pub fn connect_in_schema(url: &str, schema: &str) -> EngineResult<Self> {
        if !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = connect(PostgresConnectConfig::new(url))?;
        st(client.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; SET search_path TO {schema};"
        )))?;
        Self::from_client(client)
    }

    fn from_client(mut client: Client) -> EngineResult<Self> {
        st(client.batch_execute(SCHEMA))?;
        Ok(Self {
            client: RefCell::new(client),
        })
    }
}

impl LogStore for PostgresLog {
    fn supports_emission_cursor(&self) -> bool {
        true
    }

    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        let (t, q) = parts(shard);
        st(self.client.get_mut().execute(
            "INSERT INTO log_epochs(tenant,queue,assignment_epoch) VALUES($1,$2,0) \
             ON CONFLICT(tenant,queue) DO NOTHING",
            &[&t, &q],
        ))?;
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let (t, q) = parts(shard);
        let epoch: i64 = st(self.client.borrow_mut().query_opt(
            "SELECT assignment_epoch FROM log_epochs WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?
        .get(0);
        Ok(epoch as u64)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        let (t, q) = parts(shard);
        // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
        let epoch: i64 = st(self.client.get_mut().query_opt(
            "UPDATE log_epochs SET assignment_epoch = assignment_epoch + 1 \
             WHERE tenant=$1 AND queue=$2 RETURNING assignment_epoch",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?
        .get(0);
        Ok(epoch as u64)
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        let client = self.client.get_mut();
        let mut tx = st(client.transaction())?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        let epoch: i64 = st(tx.query_opt(
            "SELECT assignment_epoch FROM log_epochs WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?
        .get(0);
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        for env in commands {
            let json = to_json(env)?;
            // Atomically allocate the next per-queue sequence number so concurrent connections cannot
            // read the same value.
            let seq: i64 = st(tx.query_one(
                "INSERT INTO log_counters(tenant,queue,next_seq) VALUES($1,$2,1) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET next_seq = log_counters.next_seq + 1 \
                 RETURNING next_seq - 1",
                &[&t, &q],
            ))?
            .get(0);
            st(tx.execute(
                "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES($1,$2,$3,$4,$5)",
                &[&t, &q, &epoch, &seq, &json],
            ))?;
            st(tx.execute(
                "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq \
                 WHERE (high_water.epoch, high_water.seq) <= (EXCLUDED.epoch, EXCLUDED.seq)",
                &[&t, &q, &epoch, &seq],
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
        let mut client = self.client.borrow_mut();
        let total: i64 = st(client.query_one(
            "SELECT COUNT(*) FROM log_entries WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .get(0);
        let rows = st(client.query(
            "SELECT seq, epoch, envelope FROM log_entries \
             WHERE tenant=$1 AND queue=$2 AND seq>=$3 ORDER BY seq LIMIT $4",
            &[&t, &q, &(start as i64), &(limit as i64)],
        ))?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let seq: i64 = row.get(0);
            let epoch: i64 = row.get(1);
            let json: String = row.get(2);
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
        let row = st(self.client.borrow_mut().query_opt(
            "SELECT epoch, seq FROM high_water WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        Ok(row.map(|row| {
            let epoch: i64 = row.get(0);
            let seq: i64 = row.get(1);
            CommandPosition::new(shard.clone(), epoch as u64, seq as u64)
        }))
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        let (t, q) = parts(shard);
        let client = self.client.get_mut();
        // Fold the monotonic guard into the write so concurrent connections cannot regress it.
        let updated = st(client.query_opt(
            "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq \
             WHERE (high_water.epoch, high_water.seq) <= (EXCLUDED.epoch, EXCLUDED.seq) \
             RETURNING epoch, seq",
            &[
                &t,
                &q,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
            ],
        ))?;
        if updated.is_none() {
            return Err(EngineError::Invalid("high-water regression"));
        }
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        let (t, q) = parts(shard);
        let client = self.client.get_mut();
        let n: i64 = st(client.query_one(
            "SELECT COUNT(*) FROM snapshots WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .get(0);
        let ref_id = format!("snap-{n}");
        st(client.execute(
            "INSERT INTO snapshots(tenant,queue,ref_id,epoch,seq,payload) VALUES($1,$2,$3,$4,$5,$6)",
            &[
                &t,
                &q,
                &ref_id,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
                &snapshot.payload,
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
        let row = st(self.client.borrow_mut().query_opt(
            "SELECT ref_id, epoch, seq FROM snapshots \
             WHERE tenant=$1 AND queue=$2 ORDER BY ord DESC LIMIT 1",
            &[&t, &q],
        ))?;
        Ok(row.map(|row| {
            let ref_id: String = row.get(0);
            let epoch: i64 = row.get(1);
            let seq: i64 = row.get(2);
            SnapshotRef {
                queue: shard.clone(),
                position: CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
                ref_id,
            }
        }))
    }

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<SnapshotRef>> {
        let (t, q) = parts(shard);
        let row = st(self.client.borrow_mut().query_opt(
            "SELECT ref_id, epoch, seq FROM snapshots \
             WHERE tenant=$1 AND queue=$2 AND (epoch, seq) <= ($3, $4) \
             ORDER BY epoch DESC, seq DESC LIMIT 1",
            &[
                &t,
                &q,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
            ],
        ))?;
        Ok(row.map(|row| {
            let ref_id: String = row.get(0);
            let epoch: i64 = row.get(1);
            let seq: i64 = row.get(2);
            SnapshotRef {
                queue: shard.clone(),
                position: CommandPosition::new(shard.clone(), epoch as u64, seq as u64),
                ref_id,
            }
        }))
    }

    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        let (t, q) = parts(&snapshot_ref.queue);
        let row = st(self.client.borrow_mut().query_opt(
            "SELECT payload FROM snapshots WHERE tenant=$1 AND queue=$2 AND ref_id=$3",
            &[&t, &q, &snapshot_ref.ref_id],
        ))?;
        row.map(|row| ProjectionSnapshot {
            payload: row.get::<_, Vec<u8>>(0),
        })
        .ok_or(EngineError::NotFound)
    }

    fn persist_definition(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let (t, q) = parts(&QueueKey::new(
            definition.tenant_id.clone(),
            definition.queue_id.clone(),
        ));
        st(self.client.get_mut().execute(
            "INSERT INTO queue_defs(tenant,queue,definition) VALUES($1,$2,$3) \
             ON CONFLICT(tenant,queue) DO UPDATE SET definition=EXCLUDED.definition",
            &[&t, &q, &to_json(definition)?],
        ))?;
        Ok(())
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        let rows = st(self
            .client
            .borrow_mut()
            .query("SELECT definition FROM queue_defs", &[]))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let json: String = row.get(0);
            out.push(serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
        Ok(out)
    }
}
