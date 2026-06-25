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

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityValue, QueueDefinition,
    QueueId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, Claimed, ClaimedItem, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand,
    FinalizeOutcome, FinalizePort, ItemView, LeaseExpiredCommand, LeaseView, LogRead, LogWriter,
    ProjectionRead, ProjectionSnapshot, ProjectionWriter, PurgeItemsCommand, PurgePort,
    PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    ReplacePendingCommand, SnapshotRef, SnapshotStore, TickReport, UpsertOutcome, UpsertPort,
    build_push_items, validate_purge_force,
};
use pqueue_projection::ProjectionData;
use rusqlite::{Connection, params};

mod relational;
pub use relational::SqliteRelationalBackend;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
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

fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

struct Inner {
    conn: Connection,
    projections: HashMap<QueueKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
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
    fn append_durable(
        &mut self,
        shard: &QueueKey,
        env: &CommandEnvelope,
    ) -> EngineResult<CommandPosition> {
        let (t, q) = parts(shard);
        let json = to_json(env)?;
        let tx = st(self.conn.transaction())?;
        // Next sequence is MAX(seq)+1, NOT COUNT(*): it must survive log compaction/retention so a
        // persisted position never collides or regresses (TD-007 §4). Empty log → -1+1 = 0.
        let seq: i64 = st(tx.query_row(
            "SELECT COALESCE(MAX(seq), -1) + 1 FROM log_entries WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        ))?;
        st(tx.execute(
            "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES(?1,?2,0,?3,?4)",
            params![t, q, seq, json],
        ))?;
        st(tx.execute(
            "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES(?1,?2,0,?3) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
            params![t, q, seq],
        ))?;
        st(tx.commit())?;
        Ok(CommandPosition::new(shard.clone(), 0, seq as u64))
    }

    /// Durable append + in-memory apply (the atomic unit the orchestration ports rely on). The caller
    /// MUST have pre-validated so `apply_command` is infallible (commit has no rollback).
    ///
    /// `append_durable` can fail cleanly (the sqlite txn rolls back — nothing committed). But ONCE the
    /// log row is durably committed, the in-memory apply MUST succeed: the caller pre-validated it. If
    /// it doesn't, the durable log has advanced past the live projection — a silent in-process
    /// divergence. We refuse to return that as an ordinary `Err` (indistinguishable from a clean
    /// pre-commit rejection); we panic, which is the correct "rebuild the projection" signal (B2).
    fn commit_locked(&mut self, shard: &QueueKey, env: CommandEnvelope) -> EngineResult<()> {
        self.append_durable(shard, &env)?;
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
    fn rebuild_all(&mut self) -> EngineResult<()> {
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
            let mut proj = ProjectionData::new(definition.priority_model);
            for env in self.read_log_envelopes(&t, &q)? {
                if let Some(n) = env
                    .command_id
                    .0
                    .strip_prefix("sql-")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_cmd_seq = Some(max_cmd_seq.map_or(n, |m| m.max(n)));
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

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        st(conn.execute_batch(SCHEMA))?;
        let mut inner = Inner {
            conn,
            projections: HashMap::new(),
            queues: HashMap::new(),
            cmd_seq: 0,
        };
        inner.rebuild_all()?;
        Ok(Self {
            inner: Mutex::new(inner),
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
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        let tx = st(self.conn.transaction())?;
        for env in commands {
            let json = to_json(env)?;
            let seq: i64 = st(tx.query_row(
                "SELECT COALESCE(MAX(seq), -1) + 1 FROM log_entries WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            ))?;
            st(tx.execute(
                "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES(?1,?2,0,?3,?4)",
                params![t, q, seq, json],
            ))?;
            st(tx.execute(
                "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES(?1,?2,0,?3) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=excluded.epoch, seq=excluded.seq",
                params![t, q, seq],
            ))?;
            positions.push(CommandPosition::new(shard.clone(), 0, seq as u64));
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
            g.commit_locked(&req.shard, env)?;
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            Ok(Claimed {
                items: proj.render_claimed(&candidates),
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
        now: UtcTimestamp,
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
            // ONE command-sequence number stamps both the command id and the assigned item id (the
            // cmd_seq is restored past the max on rebuild, so no collision across restart).
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let new_item_id = ItemId::new(format!("sql-{n}-0")).expect("id");
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id.clone(),
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
            };
            let mk = |command: QueueCommand| CommandEnvelope {
                command_id: CommandId::new(format!("sql-{n}")),
                request_id: None,
                item_ids: vec![new_item_id.clone()],
                command,
                checksum: CommandChecksum(0),
                created_at: now,
            };
            match existing {
                None => {
                    let env = mk(QueueCommand::Push(PushCommand { items: vec![item] }));
                    g.commit_locked(shard, env)?;
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
                            let env = mk(QueueCommand::ReplacePending(ReplacePendingCommand {
                                client_item_key: client_item_key.clone(),
                                superseded_item_id: existing_id.clone(),
                                replacement: item,
                            }));
                            g.commit_locked(shard, env)?;
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
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
            let (push_items, ids) = build_push_items(items, n, "sql", max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("sql-{n}")),
                request_id: None,
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            g.commit_locked(shard, env)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl FinalizePort for SqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.finalize_validate(&outcomes)?;
            }
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id.clone()).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env)?;
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
            g.commit_locked(shard, env)?;
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
            g.commit_locked(shard, env)?;
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
                        present.push(id.clone());
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
            g.commit_locked(shard, env)?;
            Ok(count)
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
                g.commit_locked(&shard, env)?;
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
            g.projections
                .insert(shard, ProjectionData::new(definition.priority_model));
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
        _shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        // Single-node, single-epoch for launch (plan §2.5); epoch fencing is post-launch.
        std::future::ready(Ok(0))
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
                "SELECT seq, envelope FROM log_entries \
                 WHERE tenant=?1 AND queue=?2 AND seq>=?3 ORDER BY seq LIMIT ?4",
            ))?;
            let mapped = st(
                stmt.query_map(params![t, q, start as i64, limit as i64], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                }),
            )?;
            let mut entries = Vec::new();
            for r in mapped {
                let (seq, json) = st(r)?;
                let env: CommandEnvelope =
                    serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?;
                entries.push((CommandPosition::new(shard.clone(), 0, seq as u64), env));
            }
            let consumed = start + entries.len() as u64;
            let next =
                (consumed < total as u64).then(|| CommandPosition::new(shard.clone(), 0, consumed));
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
