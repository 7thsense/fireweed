#![forbid(unsafe_code)]
//! # pqueue-postgres
//!
//! Driven adapter (atomic durability class): the command **LOG is durable in postgres**, and the
//! priority-ordered **projection is the shared [`pqueue_projection::ProjectionData`] materialization,
//! rebuilt from the log**. The log rows are the source of truth (CQRS); the in-memory projection is a
//! derived view that any reopen reconstructs via [`pqueue_projection::ProjectionData::apply_command`].
//! This is the [`pqueue_sqlite`]-established durable-adapter template over the SYNC `postgres` client —
//! same apply/eligibility/lease/metrics logic (no re-implementation), same invariants, same guarantees.
//!
//! INVARIANT (commit has no rollback): every orchestration port pre-validates (via the projection's
//! decision helpers) BEFORE the durable write, so the in-memory `apply_command` that follows a committed
//! log row is infallible — the log and projection cannot diverge. Write ordering is **durable-first**:
//! the postgres transaction (log row + high_water) commits first; only then is the projection updated.
//!
//! ## Blocking-executor caveat (post-launch refinement, recorded per OWED-resolution Chunk 4 / I1)
//!
//! The port bodies make **blocking** postgres network calls inside `std::future::ready` (mirroring
//! sqlite). The sync `postgres` client drives its own internal tokio runtime per call, so it must NOT be
//! invoked from within an ambient tokio runtime — doing so PANICS ("cannot start a runtime from within a
//! runtime"), it does not merely starve. The conformance/durability tests therefore drive the backend
//! with a NON-tokio executor (`futures::executor::block_on`), each scenario on its own connection. For a
//! PRODUCTION `pqueue-server` (whose RESP loop + reclaim driver run under tokio), this backend must be
//! driven off the runtime: every call wrapped in `spawn_blocking` (+ a connection pool), or replaced by
//! the relational-projection `FOR UPDATE SKIP LOCKED` multi-node mode. That is a recorded POST-LAUNCH
//! refinement; `pqueue-server` does NOT yet wire postgres into its backend selector, so no tokio-driven
//! path reaches this client today. The launch posture is a single-node durable-log + in-memory projection
//! with guarantees identical to sqlite. This is NOT a silent sqlite copy — the caveat is the reason it is
//! single-node and not yet server-wired.
//!
//! ## Serialization caveat for the future pooling work (recorded from the Chunk-4 fresh-eyes review)
//!
//! Three write paths are serialized today ONLY by the process-wide `self.inner` Mutex (one connection per
//! backend instance), NOT by the database: the `MAX(seq)+1` log-sequence read-then-insert
//! ([`Inner::append_durable`]), and the read-check-then-write high-water guard
//! ([`SnapshotStore::set_high_water`]). Sqlite serializes these implicitly via its whole-db write lock;
//! postgres under default `READ COMMITTED` does NOT. While single-connection this is correct (the Mutex
//! is the lock); the worst case even under a race is a clean PK-conflict rollback BEFORE the projection is
//! touched, so the durability invariant cannot be violated and the log cannot corrupt. BUT the high-water
//! guard is a genuine TOCTOU: under a connection pool two callers could both pass the `precedes` check and
//! regress the stored high-water. So the pooling/`spawn_blocking` refinement above MUST also add
//! row-level locking (`SELECT … FOR UPDATE` / a `SERIALIZABLE` txn, or fold the monotonic check into a
//! single conditional `UPDATE`) before introducing a second concurrent connection.

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use postgres::{Client, NoTls};
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityValue, QueueDefinition,
    QueueId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, ClaimedItem,
    CommandChecksum, CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand,
    FinalizeOutcome, FinalizePort, ItemView, LeaseExpiredCommand, LeaseView, LogRead, LogWriter,
    ProjectionRead, ProjectionSnapshot, ProjectionWriter, PurgeItemsCommand, PurgePort,
    PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    ReplacePendingCommand, SnapshotRef, SnapshotStore, TickReport, UpsertOutcome, UpsertPort,
    build_push_items, require_item_level_claim, validate_purge_force,
};
use pqueue_projection::ProjectionData;

mod relational;
pub use relational::PostgresRelationalBackend;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS log_entries (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, epoch BIGINT NOT NULL, seq BIGINT NOT NULL,
    envelope TEXT NOT NULL,
    PRIMARY KEY (tenant, queue, epoch, seq)
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
"#;

/// Map a postgres error to the engine's adapter-level storage error.
fn st<T>(r: Result<T, postgres::Error>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
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
    client: Client,
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
            command_id: CommandId::new(format!("pg-{n}")),
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
        let mut tx = st(self.client.transaction())?;
        // Next sequence is MAX(seq)+1, NOT COUNT(*): it must survive log compaction/retention so a
        // persisted position never collides or regresses (TD-007 §4). Empty log → -1+1 = 0.
        let seq: i64 = st(tx.query_one(
            "SELECT COALESCE(MAX(seq), -1) + 1 FROM log_entries WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .get(0);
        st(tx.execute(
            "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES($1,$2,0,$3,$4)",
            &[&t, &q, &seq, &json],
        ))?;
        st(tx.execute(
            "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,0,$3) \
             ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
            &[&t, &q, &seq],
        ))?;
        st(tx.commit())?;
        Ok(CommandPosition::new(shard.clone(), 0, seq as u64))
    }

    /// Durable append + in-memory apply (the atomic unit the orchestration ports rely on). The caller
    /// MUST have pre-validated so `apply_command` is infallible (commit has no rollback).
    ///
    /// `append_durable` can fail cleanly (the postgres txn rolls back — nothing committed). But ONCE the
    /// log row is durably committed, the in-memory apply MUST succeed: the caller pre-validated it. If it
    /// doesn't, the durable log has advanced past the live projection — a silent in-process divergence. We
    /// refuse to return that as an ordinary `Err` (indistinguishable from a clean pre-commit rejection);
    /// we panic, which is the correct "rebuild the projection" signal.
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

    /// Reconstruct every queue's projection from durable state (queues + their replayed logs). Proves the
    /// log is the source of truth: a reopen loses no committed state (TD-007 §4 replay).
    fn rebuild_all(&mut self) -> EngineResult<()> {
        let rows = st(self
            .client
            .query("SELECT tenant, queue, definition FROM queues", &[]))?;
        // Track the highest `pg-N` command id already in the durable log so the regenerated counter does
        // not re-mint an id that already exists after a reopen (command_id must stay unique).
        let mut max_cmd_seq: Option<u64> = None;
        for row in rows {
            let t: String = row.get(0);
            let q: String = row.get(1);
            let def_json: String = row.get(2);
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let shard = key.clone();
            let mut proj = ProjectionData::new(definition.priority_model);
            for env in self.read_log_envelopes(&t, &q)? {
                if let Some(n) = env
                    .command_id
                    .0
                    .strip_prefix("pg-")
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
    fn read_log_envelopes(
        &mut self,
        tenant: &str,
        queue: &str,
    ) -> EngineResult<Vec<CommandEnvelope>> {
        let rows = st(self.client.query(
            "SELECT envelope FROM log_entries WHERE tenant=$1 AND queue=$2 ORDER BY epoch, seq",
            &[&tenant, &queue],
        ))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let json: String = row.get(0);
            out.push(serde_json::from_str(&json).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
        Ok(out)
    }
}

/// Postgres-backed atomic-class backend.
pub struct PostgresBackend {
    inner: Mutex<Inner>,
}

impl PostgresBackend {
    /// Connect to `url` (using the connection's default `search_path`), ensure the schema, and rebuild the
    /// in-memory projection of every known queue by replaying its durable log.
    pub fn connect(url: &str) -> EngineResult<Self> {
        let client = st(Client::connect(url, NoTls))?;
        Self::from_client(client)
    }

    /// Connect to `url`, isolate this backend in a dedicated `schema` (`CREATE SCHEMA IF NOT EXISTS` +
    /// `SET search_path`), ensure the schema's tables, and rebuild from the log. Reconnecting with the
    /// SAME `schema` reopens the same durable log (the postgres analogue of reopening a sqlite file) —
    /// used by the conformance suite (a fresh schema per scenario) and the durability reopen test.
    pub fn connect_in_schema(url: &str, schema: &str) -> EngineResult<Self> {
        if !schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = st(Client::connect(url, NoTls))?;
        st(client.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; SET search_path TO {schema};"
        )))?;
        Self::from_client(client)
    }

    fn from_client(mut client: Client) -> EngineResult<Self> {
        st(client.batch_execute(SCHEMA))?;
        let mut inner = Inner {
            client,
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
// UoW writer views (Backend::write) — disjoint borrows of client / projections
// ---------------------------------------------------------------------------

struct PgLogWriter<'a> {
    client: &'a mut Client,
}

impl LogWriter for PgLogWriter<'_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        let mut tx = st(self.client.transaction())?;
        for env in commands {
            let json = to_json(env)?;
            let seq: i64 = st(tx.query_one(
                "SELECT COALESCE(MAX(seq), -1) + 1 FROM log_entries WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .get(0);
            st(tx.execute(
                "INSERT INTO log_entries(tenant,queue,epoch,seq,envelope) VALUES($1,$2,0,$3,$4)",
                &[&t, &q, &seq, &json],
            ))?;
            st(tx.execute(
                "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,0,$3) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
                &[&t, &q, &seq],
            ))?;
            positions.push(CommandPosition::new(shard.clone(), 0, seq as u64));
        }
        st(tx.commit())?;
        Ok(positions)
    }
}

struct PgProjectionWriter<'a> {
    projections: &'a mut HashMap<QueueKey, ProjectionData>,
}

impl ProjectionWriter for PgProjectionWriter<'_> {
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

impl Backend for PostgresBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = {
            let mut guard = self.inner.lock().expect("postgres backend poisoned");
            let Inner {
                client,
                projections,
                ..
            } = &mut *guard;
            let mut lw = PgLogWriter { client };
            let mut pw = PgProjectionWriter { projections };
            f(&mut lw, &mut pw)
        };
        std::future::ready(result)
    }
}

impl ClaimPort for PostgresBackend {
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
            g.commit_locked(&req.shard, env)?;
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            Ok(Claimed {
                items: proj.render_claimed(&candidates),
            })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for PostgresBackend {
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
            // cmd_seq is restored past the max on rebuild, so no collision across a reopen).
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let new_item_id = ItemId::new(format!("pg-{n}-0")).expect("id");
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id.clone(),
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                cohort_size: None,
                gate_keys: Vec::new(),
            };
            let mk = |command: QueueCommand| CommandEnvelope {
                command_id: CommandId::new(format!("pg-{n}")),
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

impl PushPort for PostgresBackend {
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
            let (push_items, ids) = build_push_items(items, n, "pg", max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("pg-{n}")),
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

impl FinalizePort for PostgresBackend {
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

impl RenewLeasePort for PostgresBackend {
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

impl ReassignLeasePort for PostgresBackend {
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

impl PurgePort for PostgresBackend {
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

impl ReclaimDriver for PostgresBackend {
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

impl ControlPlaneStore for PostgresBackend {
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
            let (t, q) = (
                key.tenant_id.as_str().to_string(),
                key.queue_id.as_str().to_string(),
            );
            let def_json = to_json(&definition)?;
            st(g.client.execute(
                "INSERT INTO queues(tenant,queue,definition) VALUES($1,$2,$3)",
                &[&t, &q, &def_json],
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

impl LogRead for PostgresBackend {
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
            let mut g = self.inner.lock().expect("poisoned");
            let total: i64 = st(g.client.query_one(
                "SELECT COUNT(*) FROM log_entries WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .get(0);
            let rows = st(g.client.query(
                "SELECT seq, envelope FROM log_entries \
                 WHERE tenant=$1 AND queue=$2 AND seq>=$3 ORDER BY seq LIMIT $4",
                &[&t, &q, &(start as i64), &(limit as i64)],
            ))?;
            let mut entries = Vec::with_capacity(rows.len());
            for row in rows {
                let seq: i64 = row.get(0);
                let json: String = row.get(1);
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

impl ProjectionRead for PostgresBackend {
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

impl SnapshotStore for PostgresBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let mut g = self.inner.lock().expect("poisoned");
            let n: i64 = st(g.client.query_one(
                "SELECT COUNT(*) FROM snapshots WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .get(0);
            let ref_id = format!("snap-{n}");
            st(g.client.execute(
                "INSERT INTO snapshots(tenant,queue,ref_id,epoch,seq,payload) \
                 VALUES($1,$2,$3,$4,$5,$6)",
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
        })();
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let mut g = self.inner.lock().expect("poisoned");
            let row = st(g.client.query_opt(
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
        })();
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = (|| {
            let (t, q) = parts(&snapshot_ref.queue);
            let mut g = self.inner.lock().expect("poisoned");
            let row = st(g.client.query_opt(
                "SELECT payload FROM snapshots WHERE tenant=$1 AND queue=$2 AND ref_id=$3",
                &[&t, &q, &snapshot_ref.ref_id],
            ))?;
            row.map(|row| ProjectionSnapshot {
                payload: row.get::<_, Vec<u8>>(0),
            })
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
            let mut g = self.inner.lock().expect("poisoned");
            let row = st(g.client.query_opt(
                "SELECT epoch, seq FROM high_water WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            Ok(row.map(|row| {
                let epoch: i64 = row.get(0);
                let seq: i64 = row.get(1);
                CommandPosition::new(shard.clone(), epoch as u64, seq as u64)
            }))
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
            let mut g = self.inner.lock().expect("poisoned");
            // Monotonic: reject a position that does not advance the stored one (TD-007 §4).
            let current = st(g.client.query_opt(
                "SELECT epoch, seq FROM high_water WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            if let Some(row) = current {
                let epoch: i64 = row.get(0);
                let seq: i64 = row.get(1);
                let cur = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
                if !cur.precedes(&position) && cur != position {
                    return Err(EngineError::Invalid("high-water regression"));
                }
            }
            st(g.client.execute(
                "INSERT INTO high_water(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
                &[
                    &t,
                    &q,
                    &(position.backend_epoch as i64),
                    &(position.sequence as i64),
                ],
            ))?;
            Ok(())
        })();
        std::future::ready(result)
    }
}
