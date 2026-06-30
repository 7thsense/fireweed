#![forbid(unsafe_code)]
//! # pqueue-sqlite
//!
//! Driven adapter (atomic durability class): the command **LOG is durable in sqlite**, and the
//! priority-ordered **projection is the shared [`pqueue_projection::ProjectionData`] materialization,
//! rebuilt from the log**. The log rows are the source of truth (CQRS); the in-memory projection is a
//! derived view that any restart reconstructs via [`pqueue_projection::ProjectionData::apply_command`].
//!
//! All apply/eligibility/lease/metrics logic is shared with every other backend (no re-implementation);
//! this crate owns only persistence: serialize each [`CommandEnvelope`] to a `log_entries` row, keep a
//! persisted `high_water`, and replay the log to (re)build the projection.
//!
//! INVARIANT (commit has no rollback): every orchestration port pre-validates (via the projection's
//! decision helpers) BEFORE the durable write, so the in-memory `apply_command` that follows a committed
//! log row is infallible — the log and projection cannot diverge. Write ordering is **durable-first**:
//! the sqlite transaction (log row + high_water) commits first; only then is the projection updated.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, ClaimedItem,
    CommandChecksum, CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand,
    FinalizeOutcome, FinalizePort, IdempotencyDecision, IndexHit, IndexQueryPort, ItemView,
    LeaseExpiredCommand, LeaseView, LiveItemView, LogRead, LogWriter, PayloadUpdate,
    ProjectionRead, ProjectionSnapshot, ProjectionWriter, PurgeItemsCommand, PurgePort,
    PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, QueueMetrics, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort,
    RenewLeaseCommand, RenewLeasePort, ReplacePendingCommand, SnapshotRef, SnapshotStore,
    TickReport, UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort, build_push_items,
    require_item_level_claim, validate_gate_command, validate_gate_push, validate_purge_force,
};
use pqueue_projection::ProjectionData;
use rusqlite::{Connection, OptionalExtension, params};

mod compose_log;
mod relational;
pub use compose_log::SqliteLog;
pub use relational::{
    ComposedSqliteRelationalBackend, SqliteProjectionStore, SqliteRelational,
    SqliteRelationalBackend, composed_sqlite_relational_in_memory,
};

use pqueue_engine::{ComposedBackend, InProcessControlPlane};
use pqueue_projection::InMemoryProjection;

/// The composed sqlite backend (ADR-012, Phase 1): the durable sqlite command LOG re-expressed as the
/// orthogonal product `SqliteLog × InMemoryProjection × InProcessControlPlane`, assembled by the one
/// generic `ComposedBackend`. Added ALONGSIDE the monolithic [`SqliteBackend`]; the shared TD-001
/// conformance suite runs against BOTH, proving the composition is faithful before the monolith is removed
/// (Phase 2). Like the monolith it is the in-memory log-replay family: a durable command log + an
/// in-memory projection.
pub type ComposedSqliteBackend =
    ComposedBackend<SqliteLog, InMemoryProjection, InProcessControlPlane>;

/// Assemble a composed sqlite backend over an ephemeral `:memory:` durable log.
pub fn composed_sqlite_backend_in_memory() -> EngineResult<ComposedSqliteBackend> {
    Ok(ComposedBackend::new(
        SqliteLog::in_memory()?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    ))
}

/// Assemble a composed sqlite backend over a DURABLE sqlite command log at `path` — the composed
/// replacement for the monolithic `SqliteBackend::open(path)` (the composition root wires this).
pub fn composed_sqlite_backend(path: &str) -> EngineResult<ComposedSqliteBackend> {
    Ok(ComposedBackend::new(
        SqliteLog::open(path)?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    ))
}

/// The composed sqlite-LOG + sqlite-PROJECTION backend (ADR-012 P1b-ii, Part B): a durable sqlite command
/// LOG ([`SqliteLog`]) paired with the DERIVED relational SQL projection ([`SqliteProjectionStore`]) instead
/// of the in-memory projection. Atomic durability class (the log axis), so it runs the full
/// `core_suite!(@atomic)` — the projection family that stubs secondary indexes.
pub type ComposedSqliteLogSqliteProjectionBackend =
    ComposedBackend<SqliteLog, SqliteProjectionStore, InProcessControlPlane>;

/// Assemble a composed sqlite-LOG + sqlite-PROJECTION backend over ephemeral `:memory:` stores.
pub fn composed_sqlite_log_sqlite_projection_in_memory()
-> EngineResult<ComposedSqliteLogSqliteProjectionBackend> {
    Ok(ComposedBackend::new(
        SqliteLog::in_memory()?,
        SqliteProjectionStore::in_memory()?,
        InProcessControlPlane::new(),
    ))
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
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
"#;

/// Map a rusqlite error to the engine's adapter-level storage error.
fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

/// A single-row query that may legitimately return no rows: `Ok(None)` for "row absent", a real
/// `Storage` error otherwise (so corruption/I/O is never silently swallowed as "absent").
fn opt<T>(r: rusqlite::Result<T>) -> EngineResult<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

/// Serialize an envelope/definition to JSON, mapping a (practically impossible) failure to a structured
/// storage error rather than panicking inside the durable write path.
fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
    let total = now.seconds as i128 * 1_000_000_000
        + now.nanoseconds as i128
        + retention_ms as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

/// Read a queue's durable `assignment_epoch` (TD-003 fence authority). Missing queue → `NotFound`.
fn read_epoch(conn: &Connection, shard: &QueueKey) -> EngineResult<u64> {
    let (t, q) = parts(shard);
    let epoch: Option<i64> = st(conn
        .query_row(
            "SELECT assignment_epoch FROM queues WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    Ok(epoch.ok_or(EngineError::NotFound)? as u64)
}

struct Inner {
    conn: Connection,
    projections: HashMap<QueueKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
    idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
    cmd_seq: u64,
}

impl Inner {
    fn make_envelope(
        &mut self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.cmd_seq;
        self.cmd_seq += 1;
        CommandEnvelope {
            command_id: CommandId::new(format!("sql-{n}")),
            request_id: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Durably append `env` to the shard's log + advance the persisted high-water in ONE transaction.
    /// Returns the committed position. Does NOT touch the projection (caller applies after, infallibly).
    ///
    /// ADR-009 / TD-003 In-Process Library Owner-Runtime: the data-plane fast path stamps the queue's
    /// current durable epoch, and — when the owner supplies its cached acquire-time epoch (`Some`) — fences
    /// against it (a superseded owner whose cached epoch is not current is rejected `EpochFenced` and NOTHING
    /// is written). `None` is the degenerate sole-owner path (stamp current, never fence). This brings the
    /// fast path to parity with the [`SqlLogWriter::append`] seam.
    fn append_durable(
        &mut self,
        shard: &QueueKey,
        env: &CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<CommandPosition> {
        validate_gate_command(false, &env.command)?;
        let (t, q) = parts(shard);
        let json = to_json(env)?;
        let tx = st(self.conn.transaction())?;
        // Stamp the queue's current durable epoch (TD-003); fence against the owner's cached epoch if given.
        let epoch: i64 = st(tx
            .query_row(
                "SELECT assignment_epoch FROM queues WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
        // Next sequence is MAX(seq)+1, NOT COUNT(*): it must survive log compaction/retention so a
        // persisted position never collides or regresses (TD-007 §4). Empty log → -1+1 = 0.
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
        st(tx.commit())?;
        Ok(CommandPosition::new(
            shard.clone(),
            epoch as u64,
            seq as u64,
        ))
    }

    /// Durable append + in-memory apply (the atomic unit the orchestration ports rely on). The caller
    /// MUST have pre-validated so `apply_command` is infallible (commit has no rollback).
    ///
    /// `append_durable` can fail cleanly (the sqlite txn rolls back — nothing committed). But ONCE the
    /// log row is durably committed, the in-memory apply MUST succeed: the caller pre-validated it. If
    /// it doesn't, the durable log has advanced past the live projection — a silent in-process
    /// divergence. We refuse to return that as an ordinary `Err` (indistinguishable from a clean
    /// pre-commit rejection); we panic, which is the correct "rebuild the projection" signal (B2).
    fn commit_locked(
        &mut self,
        shard: &QueueKey,
        env: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        self.append_durable(shard, &env, expected_epoch)?;
        self.projections
            .get_mut(shard)
            .expect("projection exists for a shard that just accepted a durable commit")
            .apply_command(&env.command)
            .expect(
                "post-commit apply must be infallible after a durable append (caller pre-validates); \
                 a failure here means the durable log advanced past the in-memory projection",
            );
        Ok(())
    }

    /// Reconstruct every queue's projection from durable state (queues + their replayed logs). Proves
    /// the log is the source of truth: a restart loses no committed state (TD-007 §4 replay).
    fn rebuild_all(&mut self, counters: &QueueCounters) -> EngineResult<()> {
        let rows: Vec<(String, String, String)> = {
            let mut stmt = st(self
                .conn
                .prepare("SELECT tenant, queue, definition FROM queues"))?;
            let mapped = st(stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))))?;
            let mut out = Vec::new();
            for r in mapped {
                out.push(st(r)?);
            }
            out
        };
        // Track the highest `sql-N` command id already in the durable log so the regenerated counter
        // does not re-mint an id that already exists after restart (B1: command_id must stay unique).
        let mut max_cmd_seq: Option<u64> = None;
        for (t, q, def_json) in rows {
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let shard = key.clone();
            let mut proj = ProjectionData::new(
                definition.priority_model,
                definition.ordering_mode,
                definition.max_rank_error,
                definition.recurrence,
                &definition.secondary_indexes,
            );
            for env in self.read_log_envelopes(&t, &q)? {
                // Command-id is `sql-{node}-{n}` (or legacy `sql-{n}`); the trailing component is the seq.
                if let Some(n) = env
                    .command_id
                    .0
                    .rsplit('-')
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_cmd_seq = Some(max_cmd_seq.map_or(n, |m| m.max(n)));
                }
                // Restart-safety: resume the per-queue item counter past every id already in the log so a
                // push after reopen never re-mints an existing id (ADR-009 / `QueueCounters::observe`).
                for id in &env.item_ids {
                    counters.observe(&shard, *id);
                }
                proj.apply_command(&env.command)?;
            }
            self.projections.insert(shard, proj);
            self.queues.insert(key, definition);
        }
        if let Some(m) = max_cmd_seq {
            self.cmd_seq = m + 1;
        }
        Ok(())
    }

    /// Every log envelope for a shard, ordered by sequence (replay order).
    fn read_log_envelopes(&self, tenant: &str, queue: &str) -> EngineResult<Vec<CommandEnvelope>> {
        let mut stmt = st(self.conn.prepare(
            "SELECT envelope FROM log_entries WHERE tenant=?1 AND queue=?2 ORDER BY epoch, seq",
        ))?;
        let mapped = st(stmt.query_map(params![tenant, queue], |row| row.get::<_, String>(0)))?;
        let mut out = Vec::new();
        for r in mapped {
            let json = st(r)?;
            out.push(serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
        Ok(out)
    }
}

/// Sqlite-backed atomic-class backend.
pub struct SqliteBackend {
    inner: Mutex<Inner>,
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009). `0` single-instance.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see `QueueCounters`.
    counters: QueueCounters,
}

impl SqliteBackend {
    /// Open (or create) a sqlite database at `path`, ensure the schema, and rebuild the in-memory
    /// projection of every known queue by replaying its durable log.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` backend (still a real durable log within the process).
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`]
    /// so distinct nodes competing for one queue never mint a colliding id (ADR-009).
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        st(conn.execute_batch(SCHEMA))?;
        let mut inner = Inner {
            conn,
            projections: HashMap::new(),
            queues: HashMap::new(),
            idempotency: HashMap::new(),
            cmd_seq: 0,
        };
        let counters = QueueCounters::default();
        inner.rebuild_all(&counters)?;
        Ok(Self {
            inner: Mutex::new(inner),
            node_id: 0,
            counters,
        })
    }
}

// ---------------------------------------------------------------------------
// UoW writer views (Backend::write) — disjoint borrows of conn / projections
// ---------------------------------------------------------------------------

struct SqlLogWriter<'a> {
    conn: &'a mut Connection,
}

impl LogWriter for SqlLogWriter<'_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        for env in commands {
            validate_gate_command(false, &env.command)?;
        }
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        let tx = st(self.conn.transaction())?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        let epoch: i64 = st(tx
            .query_row(
                "SELECT assignment_epoch FROM queues WHERE tenant=?1 AND queue=?2",
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
}

struct SqlProjectionWriter<'a> {
    projections: &'a mut HashMap<QueueKey, ProjectionData>,
}

impl ProjectionWriter for SqlProjectionWriter<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
                .ok_or(EngineError::NotFound)?
                .apply_command(&cmd.command)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

impl Backend for SqliteBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = {
            let mut guard = self.inner.lock().expect("sqlite backend poisoned");
            let Inner {
                conn, projections, ..
            } = &mut *guard;
            let mut lw = SqlLogWriter { conn };
            let mut pw = SqlProjectionWriter { projections };
            f(&mut lw, &mut pw)
        };
        std::future::ready(result)
    }
}

impl ClaimPort for SqliteBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // BQ-14a: gate non-item compatibility (selection lands in BQ-14b/c); item-level path unchanged.
            if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, def)?;
            }
            let candidates: Vec<ItemId> = {
                let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
                proj.eligible_candidates(req.now, req.max_items)
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let cmd = QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
            });
            let env = g.make_envelope(cmd, candidates.clone(), req.now);
            g.commit_locked(&req.shard, env, req.expected_epoch)?;
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            Ok(Claimed {
                items: proj.render_claimed(&candidates),
                ..Default::default()
            })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for SqliteBackend {
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let existing = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.lookup_by_key(client_item_key)
            };
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            // The command id stays a backend-local sequence; the item id is minted from
            // (epoch, node, per-queue counter) so it never collides across writers (ADR-009).
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
            };
            let mk = |command: QueueCommand| CommandEnvelope {
                command_id: CommandId::new(format!("sql-{}-{n}", self.node_id)),
                request_id: None,
                item_ids: vec![new_item_id],
                command,
                checksum: CommandChecksum(0),
                created_at: now,
            };
            match existing {
                None => {
                    // Pre-commit unique-index validation (ADR-010 §5.1): a violating insert appends nothing.
                    {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.index_validate(&item.item_id, &item.fields, None)?;
                    }
                    let env = mk(QueueCommand::Push(PushCommand { items: vec![item] }));
                    g.commit_locked(shard, env, expected_epoch)?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some(existing_id) => {
                    let state = {
                        let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                        proj.item_state(&existing_id).ok_or(EngineError::NotFound)?
                    };
                    match state {
                        ItemState::Pending => {
                            // Superseded item is removed in the same command, so it does not conflict.
                            {
                                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                                proj.index_validate_replace(&existing_id, &item)?;
                            }
                            let env = mk(QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id,
                                replacement: item,
                            }));
                            g.commit_locked(shard, env, expected_epoch)?;
                            Ok(UpsertOutcome::Replaced {
                                new_item_id,
                                superseded_item_id: existing_id,
                            })
                        }
                        ItemState::Leased => {
                            Err(EngineError::Invalid("collision with claimed item"))
                        }
                        ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                    }
                }
            }
        })();
        std::future::ready(result)
    }
}

impl PushPort for SqliteBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-validate the shard exists BEFORE any durable write (commit_locked expects it), so a
            // Push never leaves a durable log row without a projection apply (divergence-safe).
            if !g.projections.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            // Pre-commit unique-index validation (ADR-010 §5.1): a violating push appends nothing.
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.index_validate_push(&push_items)?;
            }
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("sql-{}-{n}", self.node_id)),
                request_id: None,
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let fingerprint = push_body_hash(&items)?;
            let mut g = self.inner.lock().expect("poisoned");
            if !g.projections.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
            let def = g.queues.get(shard).ok_or(EngineError::NotFound)?;
            let max_attempts = def.retry_policy.max_attempts;
            let expires_at = request_expires_at(now, def.request_id_retention_ms);
            match g.idempotency.entry(shard.clone()).or_default().check(
                &request_id,
                fingerprint,
                now,
            ) {
                IdempotencyDecision::Replay(ids) => return Ok(ids),
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.index_validate_push(&push_items)?;
            }
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("sql-{}-{n}", self.node_id)),
                request_id: Some(request_id.clone()),
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            g.commit_locked(shard, env, expected_epoch)?;
            g.idempotency.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids.clone(),
                expires_at,
            );
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

/// Snorri authoritative vectorized claimed-work commit (epic pqueue-2201fd37). The durable relational
/// parity slice is deferred (C9); this backend inherits the default impl, which returns
/// [`pqueue_engine::EngineError::Unavailable`] so a caller rejects it before activation.
impl pqueue_engine::CommitTransitionPort for SqliteBackend {}

// Gates are a relational-mode feature; the log-replay sqlite family rejects SetGates with the default
// `Unavailable` (consistent with `validate_gate_command`). The gate-capable backend is SqliteRelationalBackend.
impl pqueue_engine::SetGatesPort for SqliteBackend {}

// Reschedule is wired in the in-memory log-replay reference (MemoryBackend); the durable log-replay
// sqlite backend has not wired it yet, so it refuses with the default `Unavailable`.
impl pqueue_engine::ReschedulePort for SqliteBackend {}

// Active-scope discovery is a relational-class feature (per-group summary); the log-replay sqlite family refuses it.
impl pqueue_engine::DiscoveryPort for SqliteBackend {}

/// Recovery/explain reads inherit the `Unavailable` default; the authoritative commit boundary lives on
/// `SqliteRelationalBackend`, not this log-replay backend.
impl pqueue_engine::RecoveryReadPort for SqliteBackend {}

impl FinalizePort for SqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.finalize_validate(&outcomes)?;
            }
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl RenewLeasePort for SqliteBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.renew_validate(&item_ids)?;
            }
            let cmd = QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: item_ids.clone(),
                lease_expires_at: new_lease_expires_at,
            });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReassignLeasePort for SqliteBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.reassign_validate(&item_ids)?;
            }
            let cmd = QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: item_ids.clone(),
                lease_token: new_lease_token,
                lease_expires_at: new_lease_expires_at,
            });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl PurgePort for SqliteBackend {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let present: Vec<ItemId> = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                let mut present = Vec::new();
                for id in &item_ids {
                    // De-dup: a repeated id removes once and counts once (Redis XDEL semantics; the
                    // apply arm's second `remove` would be a no-op but `present.len()` would over-count).
                    if present.contains(id) {
                        continue;
                    }
                    if let Some(state) = proj.item_state(id) {
                        validate_purge_force(state == ItemState::Leased, force)?;
                        present.push(*id);
                    }
                }
                present
            };
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            let cmd = QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: present.clone(),
                force,
            });
            let env = g.make_envelope(cmd, present, now);
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(count)
        })();
        std::future::ready(result)
    }
}

impl UpdateFieldsPort for SqliteBackend {
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.update_fields_validate(&item_id, expected_item_version)?;
                // Pre-commit unique-index validation (ADR-010 §5.1): a violating update appends nothing.
                proj.index_validate_update(&item_id, &field_ops)?;
            }
            let cmd = QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id,
                field_ops,
                payload,
                set_priority: Default::default(),
                set_not_before: Default::default(),
            });
            let env = g.make_envelope(cmd, vec![item_id], now);
            g.commit_locked(shard, env, expected_epoch)?;
            // Read the bumped version back from the just-applied projection.
            g.projections
                .get(shard)
                .and_then(|p| p.item_version(&item_id))
                .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for SqliteBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let mut ids = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.expired_leases(now)
            };
            if let Some(limit) = limit {
                ids.truncate(limit);
            }
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED (unlike the global ReclaimDriver::tick, which passes None).
            let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: ids.clone(),
            });
            let env = g.make_envelope(cmd, ids.clone(), now);
            g.commit_locked(shard, env, expected_epoch)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl ReclaimDriver for SqliteBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let expired: Vec<(QueueKey, Vec<ItemId>)> = g
                .projections
                .iter()
                .filter_map(|(shard, proj)| {
                    let ids = proj.expired_leases(now);
                    (!ids.is_empty()).then(|| (shard.clone(), ids))
                })
                .collect();
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                });
                let env = g.make_envelope(cmd, ids.clone(), now);
                g.commit_locked(&shard, env, None)?;
                report.leases_reclaimed += ids.len() as u64;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

impl ControlPlaneStore for SqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(existing) = g.queues.get(&key) {
                if existing.ordering_mode != definition.ordering_mode
                    || existing.priority_model != definition.priority_model
                {
                    return Err(EngineError::QueueDefinitionConflict);
                }
                return Ok(CreateQueueOutcome {
                    created: false,
                    definition: existing.clone(),
                });
            }
            let (t, q) = (key.tenant_id.as_str(), key.queue_id.as_str());
            let def_json = to_json(&definition)?;
            st(g.conn.execute(
                "INSERT INTO queues(tenant,queue,definition) VALUES(?1,?2,?3)",
                params![t, q, def_json],
            ))?;
            let shard = key.clone();
            g.projections.insert(
                shard,
                ProjectionData::new(
                    definition.priority_model,
                    definition.ordering_mode,
                    definition.max_rank_error,
                    definition.recurrence,
                    &definition.secondary_indexes,
                ),
            );
            g.queues.insert(key, definition.clone());
            Ok(CreateQueueOutcome {
                created: true,
                definition,
            })
        })();
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result: Vec<QueueId> = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            read_epoch(&g.conn, shard)
        };
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let g = self.inner.lock().expect("poisoned");
            // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
            let new_epoch: Option<i64> = st(g
                .conn
                .query_row(
                    "UPDATE queues SET assignment_epoch = assignment_epoch + 1 \
                     WHERE tenant=?1 AND queue=?2 RETURNING assignment_epoch",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?;
            new_epoch.ok_or(EngineError::NotFound).map(|e| e as u64)
        })();
        std::future::ready(result)
    }
}

impl LogRead for SqliteBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let start = match &from {
                Some(p) => p.sequence + 1,
                None => 0,
            };
            let g = self.inner.lock().expect("poisoned");
            let total: i64 = st(g.conn.query_row(
                "SELECT COUNT(*) FROM log_entries WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            ))?;
            let mut stmt = st(g.conn.prepare(
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
            // BQ-20: carry each entry's stored epoch (not a hardcoded 0) so a position replayed across an
            // epoch boundary keeps its true `(epoch, seq)` and the high-water guard never false-regresses.
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
            // The continuation cursor needs only a `seq` (read_from keys off `sequence`); its epoch is not
            // load-bearing, so reuse the last returned entry's epoch (or 0 when the page is empty).
            let cursor_epoch = entries.last().map(|(p, _)| p.backend_epoch).unwrap_or(0);
            let next = (consumed < total as u64)
                .then(|| CommandPosition::new(shard.clone(), cursor_epoch, consumed));
            Ok(CommandPage { entries, next })
        })();
        std::future::ready(result)
    }
}

impl ProjectionRead for SqliteBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.select_eligible(now, limit))
        })();
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.peek(limit))
        })();
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.pending_leases())
        })();
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.render_claimed(ids))
        })();
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.live_items_by_key(keys))
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let shard = queue.clone();
            let proj = g.projections.get(&shard).ok_or(EngineError::NotFound)?;
            Ok(proj.metrics())
        })();
        std::future::ready(result)
    }
}

impl IndexQueryPort for SqliteBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_get_unique(index, key)
        })();
        std::future::ready(result)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_lookup(index, key)
        })();
        std::future::ready(result)
    }
}

impl SnapshotStore for SqliteBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let g = self.inner.lock().expect("poisoned");
            let n: i64 = st(g.conn.query_row(
                "SELECT COUNT(*) FROM snapshots WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            ))?;
            let ref_id = format!("snap-{n}");
            st(g.conn.execute(
                "INSERT INTO snapshots(tenant,queue,ref_id,epoch,seq,payload) \
                 VALUES(?1,?2,?3,?4,?5,?6)",
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
        })();
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let g = self.inner.lock().expect("poisoned");
            let row = opt(g.conn.query_row(
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
        })();
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = (|| {
            let (t, q) = parts(&snapshot_ref.queue);
            let g = self.inner.lock().expect("poisoned");
            let payload: Option<Vec<u8>> = opt(g.conn.query_row(
                "SELECT payload FROM snapshots WHERE tenant=?1 AND queue=?2 AND ref_id=?3",
                params![t, q, snapshot_ref.ref_id],
                |row| row.get(0),
            ))?;
            payload
                .map(|payload| ProjectionSnapshot { payload })
                .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let g = self.inner.lock().expect("poisoned");
            let row = opt(g.conn.query_row(
                "SELECT epoch, seq FROM high_water WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            ))?;
            Ok(row
                .map(|(epoch, seq)| CommandPosition::new(shard.clone(), epoch as u64, seq as u64)))
        })();
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let g = self.inner.lock().expect("poisoned");
            // Monotonic: reject a position that does not advance the stored one (TD-007 §4).
            let current = opt(g.conn.query_row(
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
            st(g.conn.execute(
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
        })();
        std::future::ready(result)
    }
}
