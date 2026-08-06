//! The durable postgres command-LOG axis (ADR-012 P2).
//!
//! [`PostgresLog`] is a [`fireweed_engine::LogStore`] whose command log + epoch + high-water + snapshots +
//! durable queue catalog are rows in postgres, driven over the SYNC `postgres` client. Extracted from the
//! monolithic [`crate::PostgresBackend`]'s `Inner` (same SQL, same fence/sequence rules) so the composed
//! postgres backend (`ComposedBackend<PostgresLog, InMemoryProjection, InProcessControlPlane>`) is
//! behaviorally identical to the monolith on the shared TD-001 suite — but with the orthogonal orchestration
//! living ONCE in [`fireweed_engine::ComposedBackend`].
//!
//! Like [`fireweed_sqlite::SqliteLog`], the epoch lives in THIS log axis (a `log_epochs` table), not in a
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
//! internal tokio runtime per call, so the composition MUST be driven off the reactor (the `fireweed-server`
//! blocking wrapper delegates every port call to `spawn_blocking`). The composition's own port methods are
//! sync-but-`ready`, so this LogStore's calls run on whatever thread drives the composition — in production,
//! a Tokio blocking-pool thread, never a reactor worker.

use std::cell::RefCell;

use fireweed_core::QueueDefinition;
use fireweed_engine::{
    CommandEnvelope, CommandPage, CommandPosition, DefinitionCursor, DefinitionPage, EngineError,
    EngineResult, LogStore, ProjectionSnapshot, QueueKey, SnapshotRef,
    definition_page_from_storage_rows,
};
use postgres::Client;

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
-- `read_from` spans ownership epochs while sequence is queue-global. The primary key cannot serve its
-- `(tenant, queue, seq)` order because `epoch` precedes `seq`, so give bounded page reads their own index.
CREATE INDEX IF NOT EXISTS log_entries_read_idx ON log_entries(tenant, queue, seq);
CREATE TABLE IF NOT EXISTS log_counters (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, next_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS high_water (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch BIGINT NOT NULL, seq BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS emission_cursor (
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

// The row lock is part of append's fencing protocol, not an optimization. It prevents a concurrent
// connection from advancing the authoritative epoch after append validates it but before append commits.
const LOCK_CURRENT_EPOCH_SQL: &str =
    "SELECT assignment_epoch FROM log_epochs WHERE tenant=$1 AND queue=$2 FOR UPDATE";

const ALLOCATE_SEQUENCE_RANGE_SQL: &str = "INSERT INTO log_counters(tenant,queue,next_seq) VALUES($1,$2,$3) \
     ON CONFLICT(tenant,queue) DO UPDATE \
     SET next_seq = log_counters.next_seq + EXCLUDED.next_seq \
     RETURNING next_seq - $3";

const INSERT_LOG_ENTRY_BATCH_SQL: &str = "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) \
     SELECT $1,$2,$3,batch.seq,batch.envelope \
     FROM UNNEST($4::bigint[], $5::text[]) AS batch(seq,envelope)";

const ADVANCE_HIGH_WATER_SQL: &str = "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
     ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq \
     WHERE (high_water.epoch, high_water.seq) <= (EXCLUDED.epoch, EXCLUDED.seq)";

const READ_PAGE_SQL: &str = "SELECT seq, epoch, envelope FROM log_entries \
     WHERE tenant=$1 AND queue=$2 AND seq>=$3 ORDER BY seq LIMIT $4";

// Array parameters keep each insert set-based. Chunking bounds a single postgres message when envelopes
// are large without returning to one network round-trip per command.
const APPEND_INSERT_CHUNK_SIZE: usize = 1024;
#[cfg(test)]
const APPEND_FIXED_QUERY_COUNT: usize = 3; // epoch lock + range allocation + high-water advance
#[cfg(test)]
const READ_PAGE_QUERY_COUNT: usize = 1;

#[cfg(test)]
fn append_query_count(batch_len: usize) -> usize {
    if batch_len == 0 {
        1 // epoch lock only
    } else {
        APPEND_FIXED_QUERY_COUNT + batch_len.div_ceil(APPEND_INSERT_CHUNK_SIZE)
    }
}

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

fn next_page_cursor(
    has_more: bool,
    last_returned: Option<&CommandPosition>,
) -> Option<CommandPosition> {
    has_more.then(|| last_returned.cloned()).flatten()
}

/// The durable postgres command-log axis (ADR-012). The composition serializes access behind its
/// unit-of-work `Mutex`, so the single connection in the [`RefCell`] is never used concurrently.
pub struct PostgresLog {
    /// `Option` so [`Drop`] can move the client to a bare OS thread.
    ///
    /// `postgres::Client::drop` drives an internal `block_on`. That panics with
    /// "Cannot start a runtime from within a runtime" when a Tokio handle is
    /// already present (e.g. Turso `block_on_turso` current-thread open). Drop
    /// therefore always closes the client on a thread with no Tokio handle.
    client: RefCell<Option<Client>>,
}

impl Drop for PostgresLog {
    fn drop(&mut self) {
        if let Some(client) = self.client.get_mut().take() {
            // Always offload close: cheap when no handle is present, required when one is.
            let _ = std::thread::spawn(move || drop(client)).join();
        }
    }
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
            client: RefCell::new(Some(client)),
        })
    }

    fn client_mut(&self) -> std::cell::RefMut<'_, Client> {
        std::cell::RefMut::map(self.client.borrow_mut(), |slot| {
            slot.as_mut().expect("postgres log client closed")
        })
    }

    fn client_get_mut(&mut self) -> &mut Client {
        self.client
            .get_mut()
            .as_mut()
            .expect("postgres log client closed")
    }
}

impl LogStore for PostgresLog {
    fn supports_emission_cursor(&self) -> bool {
        true
    }

    fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let (t, q) = parts(shard);
        let row = st(self.client_mut().query_opt(
            "SELECT epoch, seq FROM emission_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        Ok(row.map(|row| {
            let epoch: i64 = row.get(0);
            let seq: i64 = row.get(1);
            CommandPosition::new(shard.clone(), epoch as u64, seq as u64)
        }))
    }

    fn set_emission_cursor(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        let (t, q) = parts(shard);
        let client = self.client_get_mut();
        let current = st(client.query_opt(
            "SELECT epoch, seq FROM emission_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        if let Some(row) = current {
            let epoch: i64 = row.get(0);
            let seq: i64 = row.get(1);
            let cur = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
            if !cur.precedes(&position) && cur != position {
                return Err(EngineError::Invalid("emission cursor regression"));
            }
        }
        st(client.execute(
            "INSERT INTO emission_cursor(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
            &[
                &t,
                &q,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
            ],
        ))?;
        Ok(())
    }

    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        let (t, q) = parts(shard);
        st(self.client_get_mut().execute(
            "INSERT INTO log_epochs(tenant,queue,assignment_epoch) VALUES($1,$2,0) \
             ON CONFLICT(tenant,queue) DO NOTHING",
            &[&t, &q],
        ))?;
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let (t, q) = parts(shard);
        let epoch: i64 = st(self.client_mut().query_opt(
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
        let epoch: i64 = st(self.client_get_mut().query_opt(
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
        let envelopes = commands
            .iter()
            .map(to_json)
            .collect::<EngineResult<Vec<_>>>()?;
        let client = self.client_get_mut();
        let mut tx = st(client.transaction())?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        let epoch: i64 = st(tx.query_opt(LOCK_CURRENT_EPOCH_SQL, &[&t, &q]))?
            .ok_or(EngineError::NotFound)?
            .get(0);
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }

        if envelopes.is_empty() {
            st(tx.commit())?;
            return Ok(Vec::new());
        }

        let batch_len = i64::try_from(envelopes.len())
            .map_err(|_| EngineError::Invalid("append batch is too large"))?;
        // Allocate the whole batch in one atomic counter update. Concurrent appenders therefore receive
        // disjoint contiguous ranges while the epoch lock keeps the allocation under the same fence.
        let first_seq: i64 =
            st(tx.query_one(ALLOCATE_SEQUENCE_RANGE_SQL, &[&t, &q, &batch_len]))?.get(0);

        for (chunk_index, envelope_chunk) in envelopes.chunks(APPEND_INSERT_CHUNK_SIZE).enumerate()
        {
            let chunk_offset = chunk_index
                .checked_mul(APPEND_INSERT_CHUNK_SIZE)
                .ok_or(EngineError::Invalid("append batch is too large"))?;
            let chunk_first = first_seq
                .checked_add(
                    i64::try_from(chunk_offset)
                        .map_err(|_| EngineError::Invalid("append batch is too large"))?,
                )
                .ok_or(EngineError::Invalid("log sequence exhausted"))?;
            let sequences = (0..envelope_chunk.len())
                .map(|offset| {
                    chunk_first
                        .checked_add(offset as i64)
                        .ok_or(EngineError::Invalid("log sequence exhausted"))
                })
                .collect::<EngineResult<Vec<_>>>()?;
            st(tx.execute(
                INSERT_LOG_ENTRY_BATCH_SQL,
                &[&t, &q, &epoch, &sequences, &envelope_chunk],
            ))?;
        }

        let last_seq = first_seq
            .checked_add(batch_len - 1)
            .ok_or(EngineError::Invalid("log sequence exhausted"))?;
        st(tx.execute(ADVANCE_HIGH_WATER_SQL, &[&t, &q, &epoch, &last_seq]))?;
        st(tx.commit())?;
        Ok((first_seq..=last_seq)
            .map(|seq| CommandPosition::new(shard.clone(), epoch as u64, seq as u64))
            .collect())
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
        if limit == 0 {
            return Ok(CommandPage {
                entries: Vec::new(),
                next: None,
            });
        }
        let start = i64::try_from(start)
            .map_err(|_| EngineError::Invalid("log cursor exceeds postgres sequence range"))?;
        let fetch_limit = i64::try_from(limit)
            .unwrap_or(i64::MAX - 1)
            .saturating_add(1);
        // Fetching one lookahead row proves continuation without a history-wide COUNT(*). This is one
        // bounded indexed query per page regardless of total shard history.
        let mut rows = st(self
            .client_mut()
            .query(READ_PAGE_SQL, &[&t, &q, &start, &fetch_limit]))?;
        let has_more = rows.len() > limit;
        if has_more {
            rows.pop();
        }
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
        // `from` is the last consumed position and the next read adds one. Carry the last position this
        // page actually returned; carrying `consumed` here would skip one command at every page boundary.
        let next = next_page_cursor(has_more, entries.last().map(|(position, _)| position));
        Ok(CommandPage { entries, next })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let (t, q) = parts(shard);
        let row = st(self.client_mut().query_opt(
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
        let client = self.client_get_mut();
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
        let client = self.client_get_mut();
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
        let row = st(self.client_mut().query_opt(
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
        let row = st(self.client_mut().query_opt(
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
        let row = st(self.client_mut().query_opt(
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
        st(self.client_get_mut().execute(
            "INSERT INTO queue_defs(tenant,queue,definition) VALUES($1,$2,$3) \
             ON CONFLICT(tenant,queue) DO UPDATE SET definition=EXCLUDED.definition",
            &[&t, &q, &to_json(definition)?],
        ))?;
        Ok(())
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        let rows = st(self
            .client_mut()
            .query("SELECT definition FROM queue_defs", &[]))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let json: String = row.get(0);
            out.push(serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
        Ok(out)
    }

    fn recover_definitions_page(
        &self,
        cursor: Option<&DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<DefinitionPage> {
        if limit == 0 {
            return Err(EngineError::Invalid(
                "definition page limit must be nonzero",
            ));
        }
        let (tenant, queue) = cursor
            .map(DefinitionCursor::queue_parts)
            .transpose()?
            .unwrap_or_default();
        let rows = st(self.client_mut().query(
            "SELECT definition FROM queue_defs \
             WHERE ($1 = '' OR tenant > $1 OR (tenant = $1 AND queue > $2)) \
             ORDER BY tenant, queue LIMIT $3",
            &[&tenant, &queue, &(limit.saturating_add(1) as i64)],
        ))?;
        let has_more = rows.len() > limit;
        let mut definitions = Vec::with_capacity(limit);
        for row in rows.into_iter().take(limit) {
            let json: String = row.get(0);
            definitions.push(
                serde_json::from_str(&json)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
            );
        }
        Ok(definition_page_from_storage_rows(
            definitions,
            has_more,
            worker_partition,
        ))
    }
}

#[cfg(test)]
mod safety_shape_tests {
    use fireweed_core::{QueueId, TenantId};
    use fireweed_engine::{CommandPosition, QueueKey};

    use super::{
        ADVANCE_HIGH_WATER_SQL, ALLOCATE_SEQUENCE_RANGE_SQL, APPEND_INSERT_CHUNK_SIZE,
        INSERT_LOG_ENTRY_BATCH_SQL, LOCK_CURRENT_EPOCH_SQL, READ_PAGE_QUERY_COUNT, READ_PAGE_SQL,
        SCHEMA, append_query_count, next_page_cursor,
    };

    #[test]
    fn append_locks_epoch_authority_until_its_transaction_finishes() {
        let normalized = LOCK_CURRENT_EPOCH_SQL.to_ascii_uppercase();
        assert!(normalized.contains("FROM LOG_EPOCHS"));
        assert!(normalized.ends_with("FOR UPDATE"));
    }

    #[test]
    fn pagination_cursor_is_last_returned_not_next_sequence() {
        let shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        );
        let last_returned = CommandPosition::new(shard, 4, 7);
        let cursor = next_page_cursor(true, Some(&last_returned)).unwrap();
        assert_eq!(cursor, last_returned);
        assert_eq!(cursor.sequence, 7, "the reader adds one when resuming");
        assert!(next_page_cursor(false, Some(&cursor)).is_none());
    }

    #[test]
    fn append_query_shape_is_fixed_plus_set_based_chunks() {
        let allocation = ALLOCATE_SEQUENCE_RANGE_SQL.to_ascii_uppercase();
        assert!(allocation.contains("NEXT_SEQ = LOG_COUNTERS.NEXT_SEQ + EXCLUDED.NEXT_SEQ"));
        assert!(allocation.contains("RETURNING NEXT_SEQ - $3"));

        let insert = INSERT_LOG_ENTRY_BATCH_SQL.to_ascii_uppercase();
        assert!(insert.contains("FROM UNNEST($4::BIGINT[], $5::TEXT[])"));
        assert!(!insert.contains("VALUES($1,$2,$3,$4,$5)"));

        let high_water = ADVANCE_HIGH_WATER_SQL.to_ascii_uppercase();
        assert_eq!(high_water.matches("INSERT INTO HIGH_WATER").count(), 1);
        assert_eq!(append_query_count(1), 4);
        assert_eq!(append_query_count(APPEND_INSERT_CHUNK_SIZE), 4);
        assert_eq!(append_query_count(APPEND_INSERT_CHUNK_SIZE + 1), 5);
    }

    #[test]
    fn read_page_is_one_indexed_limit_plus_one_query_without_history_count() {
        let query = READ_PAGE_SQL.to_ascii_uppercase();
        assert_eq!(READ_PAGE_QUERY_COUNT, 1);
        assert!(!query.contains("COUNT("));
        assert!(query.contains("SEQ>=$3 ORDER BY SEQ LIMIT $4"));

        let schema = SCHEMA.to_ascii_uppercase();
        assert!(schema.contains(
            "CREATE INDEX IF NOT EXISTS LOG_ENTRIES_READ_IDX ON LOG_ENTRIES(TENANT, QUEUE, SEQ)"
        ));
    }
}
