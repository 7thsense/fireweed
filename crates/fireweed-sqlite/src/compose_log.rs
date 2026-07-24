//! The durable sqlite command-log axis (ADR-012, Phase 1).
//!
//! [`SqliteLog`] is a [`fireweed_engine::LogStore`] whose command log + epoch + high-water + snapshots are
//! durable rows in sqlite. It backs the composed sqlite backend
//! ([`crate::ComposedSqliteBackend`] = `ComposedBackend<SqliteLog, InMemoryProjection,
//! InProcessControlPlane>`), which runs the shared TD-001 suite.
//!
//! Unlike the monolith, the epoch lives in this LOG axis (a `log_epochs` table), not in a `queues` table —
//! ADR-012 co-locates the epoch/fence authority with the log and leaves queue DEFINITIONS to the separate
//! control-plane axis. There is therefore no `queues` table here: the control plane owns definitions.

use fireweed_core::QueueDefinition;
use fireweed_engine::{
    CommandEnvelope, CommandPage, CommandPosition, DefinitionCursor, DefinitionPage, EngineError,
    EngineResult, LogStore, ProjectionSnapshot, QueueKey, SnapshotRef,
    definition_page_from_storage_rows,
};
use std::fmt::Write as _;

use rusqlite::{
    Connection, OptionalExtension, TransactionBehavior, params, params_from_iter, types::Value,
};

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
-- Queue-global sequence ordering crosses ownership epochs, so the primary key cannot serve `read_from`
-- efficiently (`epoch` precedes `seq` there). This index makes each limit+1 page an indexed bounded read.
CREATE INDEX IF NOT EXISTS log_entries_read_idx ON log_entries(tenant, queue, seq);
CREATE TABLE IF NOT EXISTS log_counters (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, next_seq INTEGER NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS high_water (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS emission_cursor (
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

const ALLOCATE_SEQUENCE_RANGE_SQL: &str = "INSERT INTO log_counters(tenant,queue,next_seq) \
     VALUES(?1,?2,(SELECT COALESCE(MAX(seq),-1)+1+?3 FROM log_entries \
                    WHERE tenant=?1 AND queue=?2)) \
     ON CONFLICT(tenant,queue) DO UPDATE SET next_seq=log_counters.next_seq+?3 \
     RETURNING next_seq-?3";

const ADVANCE_HIGH_WATER_SQL: &str = "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES(?1,?2,?3,?4) \
     ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq";

const READ_PAGE_SQL: &str = "SELECT seq, epoch, envelope FROM log_entries \
     WHERE tenant=?1 AND queue=?2 AND seq>=?3 ORDER BY seq LIMIT ?4";

// Five bind parameters per row. 128 rows stays below SQLite's historical 999-variable limit while
// keeping accepted command batches set-based and bounding statement size on every host.
const APPEND_INSERT_CHUNK_SIZE: usize = 128;

fn insert_batch_sql(rows: usize) -> String {
    debug_assert!(rows > 0 && rows <= APPEND_INSERT_CHUNK_SIZE);
    let mut sql = String::from("INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES ");
    for row in 0..rows {
        if row > 0 {
            sql.push(',');
        }
        let first = row * 5 + 1;
        write!(
            sql,
            "(?{first},?{},?{},?{},?{})",
            first + 1,
            first + 2,
            first + 3,
            first + 4
        )
        .expect("writing to String cannot fail");
    }
    sql
}

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

fn next_page_cursor(
    has_more: bool,
    last_returned: Option<&CommandPosition>,
) -> Option<CommandPosition> {
    has_more.then(|| last_returned.cloned()).flatten()
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
    fn supports_emission_cursor(&self) -> bool {
        true
    }

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
        let envelopes = commands
            .iter()
            .map(to_json)
            .collect::<EngineResult<Vec<_>>>()?;
        // Acquire the SQLite writer slot before the epoch read. A deferred read transaction can lose an
        // upgrade race and fail an otherwise valid concurrent append with SQLITE_BUSY.
        let tx = st(self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate))?;
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

        if envelopes.is_empty() {
            st(tx.commit())?;
            return Ok(Vec::new());
        }

        let batch_len = i64::try_from(envelopes.len())
            .map_err(|_| EngineError::Invalid("append batch is too large"))?;
        // Allocate a single contiguous sequence range. The transaction serializes this counter update
        // with every insert chunk and the one final high-water advance.
        let first_seq: i64 = st(tx.query_row(
            ALLOCATE_SEQUENCE_RANGE_SQL,
            params![t, q, batch_len],
            |row| row.get(0),
        ))?;

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
            let mut values = Vec::with_capacity(envelope_chunk.len() * 5);
            for (offset, envelope) in envelope_chunk.iter().enumerate() {
                let seq = chunk_first
                    .checked_add(offset as i64)
                    .ok_or(EngineError::Invalid("log sequence exhausted"))?;
                values.extend([
                    Value::Text(t.clone()),
                    Value::Text(q.clone()),
                    Value::Integer(epoch),
                    Value::Integer(seq),
                    Value::Text(envelope.clone()),
                ]);
            }
            st(tx.execute(
                &insert_batch_sql(envelope_chunk.len()),
                params_from_iter(values.iter()),
            ))?;
        }

        let last_seq = first_seq
            .checked_add(batch_len - 1)
            .ok_or(EngineError::Invalid("log sequence exhausted"))?;
        st(tx.execute(ADVANCE_HIGH_WATER_SQL, params![t, q, epoch, last_seq]))?;
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
            Some(p) => p
                .sequence
                .checked_add(1)
                .ok_or(EngineError::Invalid("log cursor sequence exhausted"))?,
            None => 0,
        };
        if limit == 0 {
            return Ok(CommandPage {
                entries: Vec::new(),
                next: None,
            });
        }
        let start = i64::try_from(start)
            .map_err(|_| EngineError::Invalid("log cursor exceeds sqlite sequence range"))?;
        let fetch_limit = i64::try_from(limit)
            .unwrap_or(i64::MAX - 1)
            .saturating_add(1);
        let mut stmt = st(self.conn.prepare(READ_PAGE_SQL))?;
        let mapped = st(stmt.query_map(params![t, q, start, fetch_limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        }))?;
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
        let has_more = entries.len() > limit;
        if has_more {
            entries.pop();
        }
        let next = next_page_cursor(has_more, entries.last().map(|(position, _)| position));
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

    fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let (t, q) = parts(shard);
        let row = opt(self.conn.query_row(
            "SELECT epoch, seq FROM emission_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        ))?;
        Ok(row.map(|(epoch, seq)| CommandPosition::new(shard.clone(), epoch as u64, seq as u64)))
    }

    fn set_emission_cursor(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        let (t, q) = parts(shard);
        let current = opt(self.conn.query_row(
            "SELECT epoch, seq FROM emission_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        ))?;
        if let Some((epoch, seq)) = current {
            let cur = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
            if !cur.precedes(&position) && cur != position {
                return Err(EngineError::Invalid("emission cursor regression"));
            }
        }
        st(self.conn.execute(
            "INSERT INTO emission_cursor(tenant,queue,epoch,seq) VALUES(?1,?2,?3,?4) \
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

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<SnapshotRef>> {
        let (t, q) = parts(shard);
        let row = opt(self.conn.query_row(
            "SELECT ref_id, epoch, seq FROM snapshots \
             WHERE tenant=?1 AND queue=?2 AND (epoch, seq) <= (?3, ?4) \
             ORDER BY epoch DESC, seq DESC LIMIT 1",
            params![
                t,
                q,
                position.backend_epoch as i64,
                position.sequence as i64
            ],
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
        let mut stmt = st(self.conn.prepare(
            "SELECT definition FROM queue_defs \
             WHERE (?1 = '' OR tenant > ?1 OR (tenant = ?1 AND queue > ?2)) \
             ORDER BY tenant, queue LIMIT ?3",
        ))?;
        let mapped = st(stmt.query_map(
            params![tenant, queue, limit.saturating_add(1) as i64],
            |row| row.get::<_, String>(0),
        ))?;
        let mut rows = Vec::with_capacity(limit.saturating_add(1));
        for row in mapped {
            rows.push(
                serde_json::from_str(&st(row)?)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
            );
        }
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        Ok(definition_page_from_storage_rows(
            rows,
            has_more,
            worker_partition,
        ))
    }
}

#[cfg(test)]
mod batching_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use fireweed_conformance::{envelope, item, qkey};
    use fireweed_engine::{LogStore, PushCommand, QueueCommand};

    use super::*;

    static TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);
    static PAGE_TRACE_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn count_statement(_: &str) {
        TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn count_page_statement(_: &str) {
        PAGE_TRACE_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    fn commands(start: usize, count: usize) -> Vec<CommandEnvelope> {
        (start..start + count)
            .map(|ordinal| {
                let id = (ordinal + 1_000_000).to_string();
                let key = format!("compose-log-{ordinal}");
                envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item(&id, &key, ordinal as i64)],
                    }),
                    vec![],
                )
            })
            .collect()
    }

    fn ready_log() -> (SqliteLog, QueueKey, u64) {
        let mut log = SqliteLog::in_memory().unwrap();
        let shard = qkey();
        log.ensure_shard(&shard).unwrap();
        let epoch = log.acquire_epoch(&shard).unwrap();
        (log, shard, epoch)
    }

    fn append_statement_count(rows: usize) -> usize {
        let (mut log, shard, epoch) = ready_log();
        let batch = commands(0, rows);
        TRACE_COUNT.store(0, Ordering::Relaxed);
        log.conn.trace(Some(count_statement));
        log.append(&shard, &batch, epoch).unwrap();
        log.conn.trace(None);
        TRACE_COUNT.load(Ordering::Relaxed)
    }

    #[test]
    fn append_statement_count_is_fixed_plus_bounded_insert_chunks() {
        let one = append_statement_count(1);
        let full_chunk = append_statement_count(APPEND_INSERT_CHUNK_SIZE);
        let two_chunks = append_statement_count(APPEND_INSERT_CHUNK_SIZE + 1);
        assert_eq!(one, full_chunk, "rows within one chunk add no statements");
        assert_eq!(
            two_chunks,
            one + 1,
            "only another set-based insert is added"
        );
        assert_eq!(
            one, 6,
            "BEGIN + fence + allocate + insert + high-water + COMMIT"
        );
    }

    #[test]
    fn append_and_page_queries_have_set_based_bounded_shapes() {
        let insert = insert_batch_sql(APPEND_INSERT_CHUNK_SIZE);
        assert_eq!(insert.matches("),(").count() + 1, APPEND_INSERT_CHUNK_SIZE);
        assert_eq!(insert.matches('?').count(), APPEND_INSERT_CHUNK_SIZE * 5);
        assert!(!insert.contains("SELECT MAX"));

        let allocation = ALLOCATE_SEQUENCE_RANGE_SQL.to_ascii_uppercase();
        assert!(allocation.contains("RETURNING NEXT_SEQ-?3"));
        assert!(allocation.contains("NEXT_SEQ=LOG_COUNTERS.NEXT_SEQ+?3"));
        let page = READ_PAGE_SQL.to_ascii_uppercase();
        assert!(!page.contains("COUNT("));
        assert!(page.contains("SEQ>=?3 ORDER BY SEQ LIMIT ?4"));
        assert!(SCHEMA.to_ascii_uppercase().contains(
            "CREATE INDEX IF NOT EXISTS LOG_ENTRIES_READ_IDX ON LOG_ENTRIES(TENANT, QUEUE, SEQ)"
        ));
    }

    #[test]
    fn chunked_append_preserves_order_and_limit_plus_one_pages() {
        let (mut log, shard, epoch) = ready_log();
        let batch = commands(0, APPEND_INSERT_CHUNK_SIZE * 2 + 1);
        let positions = log.append(&shard, &batch, epoch).unwrap();
        for (expected, position) in positions.iter().enumerate() {
            assert_eq!(position.sequence, expected as u64);
            assert_eq!(position.backend_epoch, epoch);
        }
        assert_eq!(log.high_water(&shard).unwrap().unwrap().sequence, 256);

        PAGE_TRACE_COUNT.store(0, Ordering::Relaxed);
        log.conn.trace(Some(count_page_statement));
        let first = log
            .read_from(&shard, None, APPEND_INSERT_CHUNK_SIZE)
            .unwrap();
        log.conn.trace(None);
        assert_eq!(PAGE_TRACE_COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(first.entries.len(), APPEND_INSERT_CHUNK_SIZE);
        assert_eq!(first.next.as_ref().unwrap().sequence, 127);

        let second = log
            .read_from(&shard, first.next, APPEND_INSERT_CHUNK_SIZE)
            .unwrap();
        assert_eq!(second.entries[0].0.sequence, 128);
        assert_eq!(second.entries.last().unwrap().0.sequence, 255);
        assert_eq!(second.next.as_ref().unwrap().sequence, 255);
        let final_page = log
            .read_from(&shard, second.next, APPEND_INSERT_CHUNK_SIZE)
            .unwrap();
        assert_eq!(final_page.entries.len(), 1);
        assert_eq!(final_page.entries[0].0.sequence, 256);
        assert!(final_page.next.is_none());
    }

    #[test]
    fn failure_in_second_insert_chunk_rolls_back_range_rows_and_high_water() {
        let (mut log, shard, epoch) = ready_log();
        st(log.conn.execute_batch(
            "CREATE TRIGGER reject_second_chunk BEFORE INSERT ON log_entries
             WHEN NEW.seq=128 BEGIN SELECT RAISE(ABORT, 'forced second chunk failure'); END;",
        ))
        .unwrap();
        let batch = commands(0, APPEND_INSERT_CHUNK_SIZE + 1);
        assert!(log.append(&shard, &batch, epoch).is_err());
        assert!(log.read_from(&shard, None, 1).unwrap().entries.is_empty());
        assert!(log.high_water(&shard).unwrap().is_none());
        let counter_rows: i64 = log
            .conn
            .query_row("SELECT COUNT(*) FROM log_counters", [], |row| row.get(0))
            .unwrap();
        assert_eq!(counter_rows, 0);

        log.conn
            .execute_batch("DROP TRIGGER reject_second_chunk")
            .unwrap();
        let retry = log.append(&shard, &batch[..1], epoch).unwrap();
        assert_eq!(retry[0].sequence, 0);
    }

    #[test]
    fn chunked_log_reopens_with_exact_order_and_counter_continuity() {
        let path = std::env::temp_dir().join(format!(
            "fireweed-sqlite-compose-log-batch-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let path = path.to_str().unwrap();
        let shard = qkey();
        let epoch;
        {
            let mut log = SqliteLog::open(path).unwrap();
            log.ensure_shard(&shard).unwrap();
            epoch = log.acquire_epoch(&shard).unwrap();
            log.append(&shard, &commands(0, APPEND_INSERT_CHUNK_SIZE + 1), epoch)
                .unwrap();
        }
        {
            let mut reopened = SqliteLog::open(path).unwrap();
            let page = reopened.read_from(&shard, None, 256).unwrap();
            assert_eq!(page.entries.len(), APPEND_INSERT_CHUNK_SIZE + 1);
            for (expected, (position, _)) in page.entries.iter().enumerate() {
                assert_eq!(position.sequence, expected as u64);
            }
            assert!(page.next.is_none());
            let tail = reopened
                .append(&shard, &commands(10_000, 1), epoch)
                .unwrap();
            assert_eq!(tail[0].sequence, (APPEND_INSERT_CHUNK_SIZE + 1) as u64);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn first_counter_allocation_continues_an_existing_pre_counter_log() {
        let (mut log, shard, epoch) = ready_log();
        let (tenant, queue) = parts(&shard);
        let legacy = commands(0, 1);
        log.conn
            .execute(
                "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES(?1,?2,?3,41,?4)",
                params![tenant, queue, epoch as i64, to_json(&legacy[0]).unwrap()],
            )
            .unwrap();
        let appended = log.append(&shard, &commands(1, 2), epoch).unwrap();
        assert_eq!(
            appended
                .iter()
                .map(|position| position.sequence)
                .collect::<Vec<_>>(),
            vec![42, 43]
        );
    }

    #[test]
    fn concurrent_chunked_appenders_receive_disjoint_contiguous_ranges() {
        let path = std::env::temp_dir().join(format!(
            "fireweed-sqlite-compose-log-concurrent-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let path = path.to_str().unwrap().to_string();
        let shard = qkey();
        let epoch = {
            let mut log = SqliteLog::open(&path).unwrap();
            log.ensure_shard(&shard).unwrap();
            log.acquire_epoch(&shard).unwrap()
        };
        let barrier = Arc::new(Barrier::new(2));
        let handles = [0, 10_000].map(|start| {
            let barrier = barrier.clone();
            let path = path.clone();
            let shard = shard.clone();
            std::thread::spawn(move || {
                let mut log = SqliteLog::open(&path).unwrap();
                let batch = commands(start, APPEND_INSERT_CHUNK_SIZE + 1);
                barrier.wait();
                log.append(&shard, &batch, epoch).unwrap()
            })
        });
        let mut positions = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .map(|position| position.sequence)
            .collect::<Vec<_>>();
        positions.sort_unstable();
        assert_eq!(
            positions,
            (0..(APPEND_INSERT_CHUNK_SIZE as u64 + 1) * 2).collect::<Vec<_>>()
        );

        let reopened = SqliteLog::open(&path).unwrap();
        assert_eq!(
            reopened.read_from(&shard, None, 512).unwrap().entries.len(),
            (APPEND_INSERT_CHUNK_SIZE + 1) * 2
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}
