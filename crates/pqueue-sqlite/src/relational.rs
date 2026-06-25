//! # Relational projection family (sqlite) — BQ-11a
//!
//! A SECOND, **DB-authoritative** projection family for sqlite (ADR-008 / TD-001 relational class),
//! distinct from the log-replay [`crate::SqliteBackend`]. Here the `pqueue_items` SQL table **is** the
//! projection (TD-002 columns): every lifecycle command is applied as SQL INSERT/UPDATE/DELETE against
//! `pqueue_items` inside the unit of work, and reads (eligibility, peek, pending, metrics) are SQL
//! queries over it. There is **no** shared in-memory [`pqueue_projection::ProjectionData`] and **no**
//! command log — a reopen recovers committed state from the table itself (the relational-reconnect class,
//! proven in BQ-11d), not by replaying a log.
//!
//! Scope (plan §2): BQ-11a = the schema + the 14-arm apply-UoW. BQ-11b = the serialized claim CTE
//! (candidate-select + lease in one transaction) + Eligibility Precedence in SQL, wiring the full
//! `core_suite!(@atomic)` at parity with the in-memory reference. Still ahead: `pqueue_group_summary` +
//! dup-push idempotency/tombstone (BQ-11c), the relational-reconnect suite (BQ-11d), and group/cohort/gate
//! selection (BQ-14). [`UpsertPort`] is the basic insert/replace-pending form (its idempotency-replay +
//! `client_item_key` tombstone hardening is BQ-11c); `progress_guard_sort` bounded-relaxed promotion is a
//! cross-family enhancement deferred so the two projection families never diverge on the core class.
//!
//! ## Lease tokens (TD-004 §security / TD-002 parity)
//! The durable projection stores only the lease token **hash** (`lease_token_hash`, never the cleartext
//! token). The cleartext token lives in an ephemeral in-process map ([`Inner::live_tokens`]) so
//! `pending()` / `claimed_view()` return the real token at parity with the in-memory family. The at-rest
//! hash is currently inert (lease validation is by `(state, fenced, superseded)`, exactly like the
//! in-memory family — see [`validate_leased`] — never by presented-token comparison); it is persisted so
//! the column is populated for the production posture where an owner validates a presented token's hash.
//!
//! INTENTIONAL DIVERGENCE (flagged for BQ-11d reconnect): a crash/reopen drops the live tokens (only the
//! hash survives) while item *state* persists in `pqueue_items`. So a still-`Leased` item is present in
//! `pqueue_items` after reopen but is **omitted** from `pending()`/`claimed_view()` (its cleartext token
//! is gone) — unlike the log-replay family, which reconstructs the token by replaying the `Claim`
//! command. This is the relational family's by-design recovery semantics (the token is a worker
//! capability, not durable server state; a tokenless in-flight lease is reclaimed by the epoch owner),
//! which is why the relational-reconnect conformance scenario asserts only pending-item state. BQ-11d
//! must keep its reconnect assertions within this contract (no post-reopen token claims).

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityModel, PriorityValue,
    QueueDefinition, QueueId, TenantId, UtcTimestamp, is_retry_exhausted, priority_sort,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, Claimed, ClaimedItem, CommandEnvelope,
    CommandPosition, ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError,
    EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort, ItemView,
    LeaseExpiredCommand, LeaseView, LogWriter, ProjectionRead, ProjectionWriter, PurgeItemsCommand,
    PurgePort, PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    ReplacePendingCommand, TickReport, UpsertOutcome, UpsertPort, build_push_items,
    validate_purge_force,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};

/// The relational schema. `pqueue_items` is TD-002's item projection (sqlite-typed); `fenced`,
/// `superseded`, and `max_attempts` are reference-projection columns mirroring the `FenceLease`/
/// `UnfenceLease`, `ReplacePending`, and retry-exhaustion apply arms (the production postgres mode
/// realizes fence via epoch and supersede via the `client_item_key` tombstone — see TD-002 note). The
/// partial unique index enforces one ACTIVE item per `client_item_key`, letting a superseded predecessor
/// and its replacement coexist (ReplacePending). `relational_cursor` is the per-queue command sequence
/// (the `last_command_sequence` source), persisted so positions resume monotonically across a reopen.
const RELATIONAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS pqueue_items (
    tenant_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    client_item_key TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    priority TEXT,
    priority_sort BLOB NOT NULL,
    not_before INTEGER,
    eligible_since INTEGER,
    group_key TEXT,
    cohort_size INTEGER,
    recurrence_until INTEGER,
    payload BLOB,
    metadata TEXT NOT NULL DEFAULT '{}',
    retry_count INTEGER NOT NULL DEFAULT 0,
    item_version INTEGER NOT NULL,
    lease_token_hash BLOB,
    lease_expires_at INTEGER,
    worker_id TEXT,
    last_command_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER,
    fenced INTEGER NOT NULL DEFAULT 0,
    superseded INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    -- Stable per-queue insertion order: the relational analogue of the in-memory `created_seq` FIFO
    -- tiebreaker. Assigned once at insert, NEVER updated, so a released/reclaimed item keeps its original
    -- eligibility position (unlike `last_command_sequence`, which advances on every mutation). An explicit
    -- column rather than the implicit `rowid`, which VACUUM may renumber.
    created_seq INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS pqueue_items_active_key
    ON pqueue_items (tenant_id, queue_id, client_item_key) WHERE superseded = 0;
CREATE TABLE IF NOT EXISTS relational_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    next_seq INTEGER NOT NULL,        -- command-position sequence (last_command_sequence source)
    next_item_seq INTEGER NOT NULL,   -- monotonic per-queue item insertion counter (created_seq source)
    PRIMARY KEY (tenant, queue)
);
"#;

// ---------------------------------------------------------------------------
// small conversions / error mapping
// ---------------------------------------------------------------------------

fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
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

/// Pack a timestamp as nanoseconds-since-epoch (comparable in SQL for `not_before`/expiry ordering).
/// Saturating so a far-future timestamp (> ~year 2262) clamps rather than overflow-panics; realistic
/// queue timestamps are far inside the i64-nanos range.
fn ts_nanos(ts: UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanoseconds as i64)
}

fn ts_nanos_opt(ts: Option<UtcTimestamp>) -> Option<i64> {
    ts.map(ts_nanos)
}

fn nanos_ts(v: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        v.div_euclid(1_000_000_000),
        v.rem_euclid(1_000_000_000) as u32,
    )
    .expect("nanoseconds bounded by rem_euclid")
}

fn state_str(s: ItemState) -> &'static str {
    match s {
        ItemState::Pending => "Pending",
        ItemState::Leased => "Leased",
        ItemState::Complete => "Complete",
        ItemState::Failed => "Failed",
    }
}

fn parse_state(s: &str) -> EngineResult<ItemState> {
    match s {
        "Pending" => Ok(ItemState::Pending),
        "Leased" => Ok(ItemState::Leased),
        "Complete" => Ok(ItemState::Complete),
        "Failed" => Ok(ItemState::Failed),
        other => Err(EngineError::Storage(format!(
            "unknown lifecycle_state {other}"
        ))),
    }
}

/// Tagged priority-sort encoding, byte-identical to the in-memory `elig_key` (priced items tag 0 then
/// the model's `priority_sort` bytes; unpriced tag 1) — so `ORDER BY priority_sort` matches the
/// in-memory eligibility order exactly.
fn elig_sort(priority: &Option<PriorityValue>, model: &PriorityModel) -> Vec<u8> {
    match priority {
        Some(p) => {
            let mut v = vec![0u8];
            v.extend(priority_sort(p, model));
            v
        }
        None => vec![1u8],
    }
}

fn lease_hash(token: &LeaseToken) -> Vec<u8> {
    Sha256::digest(token.as_str().as_bytes()).to_vec()
}

fn parse_priority(raw: Option<String>) -> EngineResult<Option<PriorityValue>> {
    raw.map(|s| serde_json::from_str(&s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
}

// ---------------------------------------------------------------------------
// Inner: the durable connection + the queue-definition cache + the live-token map
// ---------------------------------------------------------------------------

struct Inner {
    conn: Connection,
    /// Definitions cache (priority model for `priority_sort`, retry bound). Rebuilt from `queues` on open.
    queues: HashMap<QueueKey, QueueDefinition>,
    /// Ephemeral live lease tokens (cleartext is never persisted; only the hash is). Lost on reopen.
    live_tokens: HashMap<ItemId, LeaseToken>,
    /// Monotonic counter for command ids only (positions come from `relational_cursor`).
    cmd_seq: u64,
}

impl Inner {
    /// Rebuild the in-RAM definition cache from the durable `queues` table. The item projection itself is
    /// already durable in `pqueue_items` (DB-authoritative) — nothing to replay.
    fn reload(&mut self) -> EngineResult<()> {
        let rows: Vec<String> = {
            let mut stmt = st(self.conn.prepare("SELECT definition FROM queues"))?;
            let mapped = st(stmt.query_map([], |row| row.get::<_, String>(0)))?;
            let mut out = Vec::new();
            for r in mapped {
                out.push(st(r)?);
            }
            out
        };
        for def_json in rows {
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.queues.insert(key, definition);
        }
        // Restore the command counter past every server-assigned id (`rel-{n}-{i}`) already in the durable
        // projection, so a push after reopen never re-mints an existing item id (PushPort restart-safety;
        // mirrors the log backend's counter restore). There is no log to scan — the items table is the
        // authority — so we derive it from the live ids directly.
        let mut max_n: Option<u64> = None;
        {
            let mut stmt = st(self.conn.prepare("SELECT item_id FROM pqueue_items"))?;
            let mapped = st(stmt.query_map([], |row| row.get::<_, String>(0)))?;
            for r in mapped {
                let id = st(r)?;
                if let Some(n) = id
                    .strip_prefix("rel-")
                    .and_then(|s| s.split('-').next())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_n = Some(max_n.map_or(n, |m| m.max(n)));
                }
            }
        }
        if let Some(m) = max_n {
            self.cmd_seq = m + 1;
        }
        Ok(())
    }

    /// Assign the next command sequence for `shard`, apply `command` to `pqueue_items`, and advance the
    /// cursor — all in one transaction (the atomic append+apply UoW the async ports rely on).
    fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let Inner {
            conn,
            queues,
            live_tokens,
            ..
        } = self;
        let (t, q) = parts(shard);
        let tx = st(conn.transaction())?;
        let seq: i64 = st(tx
            .query_row(
                "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        let mut token_ops = Vec::new();
        apply_command_sql(
            &tx,
            queues,
            &mut token_ops,
            shard,
            seq as u64,
            now,
            &command,
        )?;
        st(tx.execute(
            "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
            params![t, q, seq + 1],
        ))?;
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// apply: the 14-arm command -> SQL projection write (the BQ-11a headline)
// ---------------------------------------------------------------------------

/// Insert one new pending item (Push / ReplacePending replacement).
fn insert_item(
    tx: &Transaction<'_>,
    model: &PriorityModel,
    shard: &QueueKey,
    item: &PushItem,
    seq: u64,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let sort = elig_sort(&item.priority, model);
    let priority_json = item.priority.as_ref().map(to_json).transpose()?;
    let not_before = ts_nanos_opt(item.not_before);
    let eligible_since = not_before.unwrap_or_else(|| ts_nanos(now));
    let payload = item.payload.as_ref().map(|b| b.to_vec());
    // Allocate the stable FIFO position (monotonic per queue, never reused — matches in-memory next_seq).
    let created_seq: i64 = st(tx.query_row(
        "SELECT next_item_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;
    st(tx.execute(
        "UPDATE relational_cursor SET next_item_seq=?3 WHERE tenant=?1 AND queue=?2",
        params![t, q, created_seq + 1],
    ))?;
    st(tx.execute(
        "INSERT INTO pqueue_items \
         (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
          not_before,eligible_since,group_key,payload,metadata,retry_count,item_version,\
          lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,updated_at,\
          terminal_at,fenced,superseded,max_attempts,created_seq) \
         VALUES (?1,?2,?3,?4,'Pending',?5,?6,?7,?8,?9,?10,'{}',0,1,NULL,NULL,NULL,?11,?12,?12,NULL,0,0,?13,?14)",
        params![
            t,
            q,
            item.item_id.as_str(),
            item.client_item_key.as_str(),
            priority_json,
            sort,
            not_before,
            eligible_since,
            item.group_key.as_ref().map(|g| g.as_str()),
            payload,
            seq as i64,
            ts_nanos(now),
            item.max_attempts as i64,
            created_seq,
        ],
    ))?;
    Ok(())
}

/// A deferred mutation of the in-RAM live-token map, collected during apply and replayed onto the map
/// ONLY after the transaction commits — so a commit failure can never leave the RAM tokens ahead of the
/// durable `pqueue_items` state (F4).
enum TokenOp {
    Set(ItemId, LeaseToken),
    Clear(ItemId),
}

fn apply_token_ops(live_tokens: &mut HashMap<ItemId, LeaseToken>, ops: Vec<TokenOp>) {
    for op in ops {
        match op {
            TokenOp::Set(id, token) => {
                live_tokens.insert(id, token);
            }
            TokenOp::Clear(id) => {
                live_tokens.remove(&id);
            }
        }
    }
}

/// Apply one command to `pqueue_items` as SQL. Mirrors `ProjectionData::apply_command` arm-for-arm. The
/// caller must have pre-validated rejectable commands (commit has no rollback past this point), so the
/// only errors here are storage/`NotFound` faults, never behavioral rejections. Live-token mutations are
/// appended to `token_ops` (applied post-commit by the caller), never mutated in place.
fn apply_command_sql(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    seq: u64,
    now: UtcTimestamp,
    command: &QueueCommand,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    match command {
        // Queue creation is a control-plane concern; idempotent no-op if it reaches the apply path.
        QueueCommand::CreateQueue(_) => Ok(()),
        QueueCommand::Push(c) => {
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            for it in &c.items {
                insert_item(tx, &model, shard, it, seq, now)?;
            }
            Ok(())
        }
        QueueCommand::Claim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?4, \
                     lease_expires_at=?5, retry_count=retry_count+1, item_version=item_version+1, \
                     updated_at=?6, last_command_sequence=?7 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str(), hash, exp, now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Set(id.clone(), c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lease_expires_at=?4, item_version=item_version+1, \
                     updated_at=?5, last_command_sequence=?6 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str(), exp, now_n, seq as i64],
                ))?;
            }
            Ok(())
        }
        QueueCommand::ReassignLease(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lease_token_hash=?4, lease_expires_at=?5, \
                     retry_count=retry_count+1, item_version=item_version+1, updated_at=?6, \
                     last_command_sequence=?7 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str(), hash, exp, now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Set(id.clone(), c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::Finalize(c) => {
            for o in &c.outcomes {
                let new_state = match o.kind {
                    FinalizeKind::Complete => ItemState::Complete,
                    FinalizeKind::Fail => ItemState::Failed,
                    FinalizeKind::Retry => {
                        // Retry-exhaustion (B'): a retry that has used all `max_attempts` deliveries goes
                        // terminal (Failed); under the bound it returns to pending (claimable again).
                        let (rc, ma): (i64, i64) = st(tx.query_row(
                            "SELECT retry_count, max_attempts FROM pqueue_items \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                            params![t, q, o.item_id.as_str()],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        ))?;
                        if is_retry_exhausted(rc as u32, ma as u32) {
                            ItemState::Failed
                        } else {
                            ItemState::Pending
                        }
                    }
                    FinalizeKind::Release => ItemState::Pending,
                    FinalizeKind::Rearm => ItemState::Pending,
                };
                let terminal_at = new_state.is_terminal().then_some(now_n);
                // Rearm resets the delivery count (recurrence); other dispositions keep it.
                let reset_attempts = matches!(o.kind, FinalizeKind::Rearm);
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state=?4, lease_token_hash=NULL, \
                     lease_expires_at=NULL, fenced=0, item_version=item_version+1, \
                     retry_count=CASE WHEN ?5 THEN 0 ELSE retry_count END, \
                     terminal_at=?6, updated_at=?7, last_command_sequence=?8 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![
                        t,
                        q,
                        o.item_id.as_str(),
                        state_str(new_state),
                        reset_attempts,
                        terminal_at,
                        now_n,
                        seq as i64,
                    ],
                ))?;
                token_ops.push(TokenOp::Clear(o.item_id.clone()));
            }
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            // Supersede the old pending item (drops it from the active partial-unique index + eligibility),
            // then insert the replacement under the same client_item_key.
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![t, q, c.superseded_item_id.as_str(), now_n, seq as i64],
            ))?;
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_item(tx, &model, shard, &c.replacement, seq, now)?;
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                     lease_expires_at=NULL, item_version=item_version+1, updated_at=?4, \
                     last_command_sequence=?5 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str(), now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            Ok(())
        }
        QueueCommand::CohortExpired(c) => {
            // Force every non-terminal member of the cohort to Failed (cohort-incomplete).
            let ids: Vec<ItemId> = {
                let mut stmt = st(tx.prepare(
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                     AND group_key=?3 AND lifecycle_state NOT IN ('Complete','Failed')",
                ))?;
                let mapped = st(stmt.query_map(params![t, q, c.group_key.as_str()], |row| {
                    row.get::<_, String>(0)
                }))?;
                let mut out = Vec::new();
                for r in mapped {
                    out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
                }
                out
            };
            for id in &ids {
                // Force terminal only (parity with the in-memory arm, which leaves the lease fields as-is
                // on the now-terminal row); the live token is dropped from the RAM map post-commit.
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Failed', item_version=item_version+1, \
                     terminal_at=?4, updated_at=?4, last_command_sequence=?5 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str(), now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            for id in &c.item_ids {
                // Operator fence: no item_version bump (parity with the in-memory arm).
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=1 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str()],
                ))?;
            }
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=0 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str()],
                ))?;
            }
            Ok(())
        }
        QueueCommand::PauseQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=1 WHERE tenant=?1 AND queue=?2",
                params![t, q],
            ))?;
            Ok(())
        }
        QueueCommand::ResumeQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=0 WHERE tenant=?1 AND queue=?2",
                params![t, q],
            ))?;
            Ok(())
        }
        QueueCommand::PurgeItems(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "DELETE FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.as_str()],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// read queries (SQL over pqueue_items)
// ---------------------------------------------------------------------------

fn queue_paused(conn: &Connection, shard: &QueueKey) -> EngineResult<bool> {
    let (t, q) = parts(shard);
    let paused: i64 = st(conn
        .query_row(
            "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    Ok(paused != 0)
}

/// Priority-ordered eligible candidates (pending, not superseded, due at `now`), capped at `limit`. Empty
/// while paused. `created_seq` is the stable FIFO tiebreaker (the relational analogue of the in-memory
/// `created_seq`; BQ-11b adds Eligibility-Precedence progress-guard ordering).
fn select_eligible_sql(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    // The TD-002 `BatchClaim` candidate predicate (owner-local, no shard filter): pending, due, eligible,
    // ordered by the strict-claim key. `eligible_since IS NOT NULL` matches the CTE; `progress_guard_sort`
    // is omitted — under `ordering_mode=strict` (TD-002:649 sanctions strict ordering as the valid first
    // implementation) it reduces to this strict order, which is also exact parity with the in-memory
    // reference (`eligible_candidates` has no at-risk promotion). `created_seq` is the stable analogue of
    // the CTE's `created_at, item_id` FIFO tiebreak.
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND lifecycle_state='Pending' AND superseded=0 \
         AND (not_before IS NULL OR not_before<=?3) \
         AND eligible_since IS NOT NULL \
         ORDER BY priority_sort, created_seq LIMIT ?4",
    ))?;
    let mapped = st(
        stmt.query_map(params![t, q, ts_nanos(now), limit as i64], |row| {
            row.get::<_, String>(0)
        }),
    )?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// Non-destructive eligible view (every pending non-superseded item in priority order; ignores
/// `not_before`/pause exactly like the in-memory `peek`).
fn peek_sql(conn: &Connection, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id, client_item_key, priority, item_version FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Pending' AND superseded=0 \
         ORDER BY priority_sort, created_seq LIMIT ?3",
    ))?;
    let rows = st(stmt.query_map(params![t, q, limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
        ))
    }))?;
    let mut out = Vec::new();
    for r in rows {
        let (id, key, priority, version) = st(r)?;
        out.push(ItemView {
            item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
            client_item_key: ClientItemKey::new(key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            priority: parse_priority(priority)?,
            item_version: version as u64,
        });
    }
    Ok(out)
}

/// In-flight (leased) items. The lease token comes from the ephemeral live-token map (the durable table
/// keeps only the hash); a leased item whose token was lost to a reopen is omitted.
fn pending_sql(
    conn: &Connection,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
) -> EngineResult<Vec<LeaseView>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id, lease_expires_at, retry_count FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND lifecycle_state='Leased'",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, i64>(2)?,
        ))
    }))?;
    let mut out = Vec::new();
    for r in rows {
        let (id, exp, retry) = st(r)?;
        let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
        let (Some(token), Some(exp)) = (live_tokens.get(&item_id), exp) else {
            continue;
        };
        out.push(LeaseView {
            item_id,
            lease_token: token.clone(),
            lease_expires_at: nanos_ts(exp),
            attempt_count: retry as u32,
        });
    }
    Ok(out)
}

/// Render the rich claimed-item shape for specific leased `ids` (the claim/XCLAIM reply). The lease token
/// for each id is supplied by `resolve` — the just-claimed token when rendering inside the claim txn, or
/// the live-token map for the `claimed_view` read port. Ids absent / not leased / with no resolvable token
/// are omitted (the caller knows the set it just acted on).
fn render_claimed(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
    resolve: impl Fn(&ItemId) -> Option<LeaseToken>,
) -> EngineResult<Vec<ClaimedItem>> {
    let (t, q) = parts(shard);
    let mut out = Vec::new();
    for id in ids {
        let Some(token) = resolve(id) else {
            continue;
        };
        let row = st(conn
            .query_row(
                "SELECT client_item_key, item_version, priority, group_key, not_before, \
                 lease_expires_at, retry_count, payload FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 AND lifecycle_state='Leased'",
                params![t, q, id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<Vec<u8>>>(7)?,
                    ))
                },
            )
            .optional())?;
        let Some((key, version, priority, group, not_before, exp, retry, payload)) = row else {
            continue;
        };
        let Some(exp) = exp else { continue };
        out.push(ClaimedItem {
            item_id: id.clone(),
            client_item_key: ClientItemKey::new(key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            item_version: version as u64,
            priority: parse_priority(priority)?,
            group_key: group
                .map(GroupKey::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            not_before: not_before.map(nanos_ts),
            lease_token: token,
            lease_expires_at: nanos_ts(exp),
            attempt_count: retry as u32,
            payload: payload.map(Bytes::from),
        });
    }
    Ok(out)
}

fn metrics_sql(conn: &Connection, shard: &QueueKey) -> EngineResult<QueueMetrics> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT lifecycle_state, COUNT(*) FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND superseded=0 GROUP BY lifecycle_state",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }))?;
    let mut m = QueueMetrics::default();
    for r in rows {
        let (state, count) = st(r)?;
        let count = count as u64;
        match parse_state(&state)? {
            ItemState::Pending => m.pending = count,
            ItemState::Leased => m.leased = count,
            ItemState::Complete => m.complete = count,
            ItemState::Failed => m.failed = count,
        }
    }
    Ok(m)
}

/// Lifecycle state + flags for one item (validation lookups). `None` if absent.
fn item_flags(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<(ItemState, bool, bool)>> {
    let (t, q) = parts(shard);
    let row = st(conn
        .query_row(
            "SELECT lifecycle_state, fenced, superseded FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional())?;
    row.map(|(s, fenced, superseded)| Ok((parse_state(&s)?, fenced != 0, superseded != 0)))
        .transpose()
}

/// Shared "present + Leased + not fenced + not superseded + not terminal" check — identical error
/// precedence to `ProjectionData::validate_leased` (finalize/renew/reassign pre-commit).
fn validate_leased(conn: &Connection, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
    for id in ids {
        match item_flags(conn, shard, id)? {
            None => return Err(EngineError::NotFound),
            Some((_, true, _)) => return Err(EngineError::StaleLease),
            Some((s, _, _)) if s.is_terminal() => return Err(EngineError::Terminal),
            Some((_, _, true)) => return Err(EngineError::Superseded),
            Some((s, _, _)) if s != ItemState::Leased => {
                return Err(EngineError::Invalid("item is not leased"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SqliteRelationalBackend
// ---------------------------------------------------------------------------

/// Sqlite-backed **relational** projection family: `pqueue_items` is the DB-authoritative projection
/// (ADR-008 / TD-001 relational class). Atomic durability class.
pub struct SqliteRelationalBackend {
    inner: Mutex<Inner>,
}

impl SqliteRelationalBackend {
    /// Open (or create) the relational store at `path` and load the queue-definition cache. The item
    /// projection is already durable in `pqueue_items`; there is no log to replay.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` relational store.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        st(conn.execute_batch(RELATIONAL_SCHEMA))?;
        let mut inner = Inner {
            conn,
            queues: HashMap::new(),
            live_tokens: HashMap::new(),
            cmd_seq: 0,
        };
        inner.reload()?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }
}

// --- Backend::write unit of work (disjoint borrows: tx over conn, &mut live-token map, &queues) -------

struct RelLogWriter<'a> {
    tx: &'a Transaction<'a>,
}

impl LogWriter for RelLogWriter<'_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut next: i64 = st(self
            .tx
            .query_row(
                "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        let mut positions = Vec::with_capacity(commands.len());
        for _ in commands {
            positions.push(CommandPosition::new(shard.clone(), 0, next as u64));
            next += 1;
        }
        st(self.tx.execute(
            "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
            params![t, q, next],
        ))?;
        Ok(positions)
    }
}

struct RelProjectionWriter<'a> {
    tx: &'a Transaction<'a>,
    queues: &'a HashMap<QueueKey, QueueDefinition>,
    /// Token mutations accumulate here and are replayed onto the live map by `write` AFTER commit (F4).
    token_ops: &'a mut Vec<TokenOp>,
}

impl ProjectionWriter for RelProjectionWriter<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, env) in positions.iter().zip(commands) {
            apply_command_sql(
                self.tx,
                self.queues,
                self.token_ops,
                &pos.queue,
                pos.sequence,
                env.created_at,
                &env.command,
            )?;
        }
        Ok(())
    }
}

impl Backend for SqliteRelationalBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = (|| {
            let mut guard = self.inner.lock().expect("relational backend poisoned");
            let Inner {
                conn,
                queues,
                live_tokens,
                ..
            } = &mut *guard;
            let tx = st(conn.transaction())?;
            let mut token_ops = Vec::new();
            let r = {
                let mut lw = RelLogWriter { tx: &tx };
                let mut pw = RelProjectionWriter {
                    tx: &tx,
                    queues,
                    token_ops: &mut token_ops,
                };
                f(&mut lw, &mut pw)?
            };
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
            Ok(r)
        })();
        std::future::ready(result)
    }
}

impl ControlPlaneStore for SqliteRelationalBackend {
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
                "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)",
                params![t, q, def_json],
            ))?;
            st(g.conn.execute(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES(?1,?2,0,0)",
                params![t, q],
            ))?;
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
        // Single-node, single-epoch for launch (plan §2.5); epoch fencing is BQ-20.
        std::future::ready(Ok(0))
    }
}

impl ProjectionRead for SqliteRelationalBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            select_eligible_sql(&g.conn, shard, now, limit)
        };
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            peek_sql(&g.conn, shard, limit)
        };
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            pending_sql(&g.conn, &g.live_tokens, shard)
        };
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            render_claimed(&g.conn, shard, ids, |id| g.live_tokens.get(id).cloned())
        };
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            metrics_sql(&g.conn, queue)
        };
        std::future::ready(result)
    }
}

impl PushPort for SqliteRelationalBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let (push_items, ids) = build_push_items(items, n, "rel", max_attempts);
            g.commit_command(
                shard,
                QueueCommand::Push(PushCommand { items: push_items }),
                now,
            )?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl ClaimPort for SqliteRelationalBackend {
    /// BQ-11b: the TD-002 serialized claim CTE — candidate selection and the lease land in **one**
    /// transaction (`with candidates as (select … order by … limit … for update skip locked) update …
    /// returning`), so there is no select-then-lease TOCTOU (unlike the BQ-11a two-transaction form).
    ///
    /// CONCURRENCY NOTE: the serialization that makes the in-one-transaction select+lease safe here comes
    /// from the whole-backend `Mutex<Inner>` (one writer at a time), NOT from the sqlite transaction — a
    /// deferred transaction takes no row lock at SELECT time. The transaction provides failure-atomicity
    /// (rollback on error/crash). BQ-12 (postgres_native) has no such Mutex and MUST use a real `FOR UPDATE
    /// SKIP LOCKED` candidate lock; it cannot inherit this pattern unchanged.
    ///
    /// Eligibility ordering is the strict-claim key (`priority_sort, created_seq`), exact parity with the
    /// in-memory reference; `progress_guard_sort` bounded-relaxed promotion is a cross-family enhancement
    /// deferred so the two families never diverge on the conformance core class (TD-002:649;
    /// group/`same_group_key` selection is BQ-14).
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let Inner {
                conn,
                queues,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(&req.shard);
            let tx = st(conn.transaction())?;
            // Candidate selection inside the claim transaction (the CTE's locked candidate set).
            let candidates = select_eligible_sql(&tx, &req.shard, req.now, req.max_items)?;
            if candidates.is_empty() {
                return Ok(Claimed::default()); // tx dropped (rolled back) — nothing leased
            }
            // Lease the selected candidates in the SAME transaction (the CTE's `update … returning`).
            let seq: i64 = st(tx
                .query_row(
                    "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            let mut token_ops = Vec::new();
            apply_command_sql(
                &tx,
                queues,
                &mut token_ops,
                &req.shard,
                seq as u64,
                req.now,
                &QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                }),
            )?;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq + 1],
            ))?;
            // Render the reply from the just-leased rows + the token we just minted (the CTE's RETURNING).
            let items = render_claimed(&tx, &req.shard, &candidates, |_| {
                Some(req.lease_token.clone())
            })?;
            // Every selected candidate was just leased in this txn, so it must render (parity guard the
            // in-memory backend also carries) — a miss means an apply/render divergence, not a no-op.
            debug_assert_eq!(
                items.len(),
                candidates.len(),
                "every claimed candidate must render"
            );
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
            Ok(Claimed { items })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for SqliteRelationalBackend {
    /// BQ-11a: basic insert / replace-pending / reject-claimed / reject-terminal (mirrors the log-replay
    /// backend). Dup-push idempotency-replay + the `client_item_key` tombstone are BQ-11c.
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
            let (t, q) = parts(shard);
            let mut g = self.inner.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            // Active item under this key (superseded predecessors excluded by the partial index).
            let existing: Option<(String, String)> = st(g
                .conn
                .query_row(
                    "SELECT item_id, lifecycle_state FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 AND superseded=0",
                    params![t, q, client_item_key.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional())?;
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let new_item_id = ItemId::new(format!("rel-{n}-0")).expect("id");
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id.clone(),
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
            };
            match existing {
                None => {
                    g.commit_command(
                        shard,
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        now,
                    )?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some((existing_id, state)) => {
                    let existing_id = ItemId::new(existing_id)
                        .map_err(|e| EngineError::Storage(e.to_string()))?;
                    match parse_state(&state)? {
                        ItemState::Pending => {
                            g.commit_command(
                                shard,
                                QueueCommand::ReplacePending(ReplacePendingCommand {
                                    client_item_key: client_item_key.clone(),
                                    superseded_item_id: existing_id.clone(),
                                    replacement: item,
                                }),
                                now,
                            )?;
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

impl FinalizePort for SqliteRelationalBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id.clone()).collect();
            validate_leased(&g.conn, shard, &ids)?;
            g.commit_command(
                shard,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                now,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl RenewLeasePort for SqliteRelationalBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_leased(&g.conn, shard, &item_ids)?;
            g.commit_command(
                shard,
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReassignLeasePort for SqliteRelationalBackend {
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
            validate_leased(&g.conn, shard, &item_ids)?;
            g.commit_command(
                shard,
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids,
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl PurgePort for SqliteRelationalBackend {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let mut present: Vec<ItemId> = Vec::new();
            for id in &item_ids {
                if present.contains(id) {
                    continue; // de-dup: remove + count once (XDEL semantics)
                }
                if let Some((state, _, _)) = item_flags(&g.conn, shard, id)? {
                    validate_purge_force(state == ItemState::Leased, force)?;
                    present.push(id.clone());
                }
            }
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            g.commit_command(
                shard,
                QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: present,
                    force,
                }),
                now,
            )?;
            Ok(count)
        })();
        std::future::ready(result)
    }
}

impl ReclaimDriver for SqliteRelationalBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // Expired (half-open: valid through lease_expires_at) leased items, per queue.
            let now_n = ts_nanos(now);
            let expired: Vec<(QueueKey, Vec<ItemId>)> = {
                let mut stmt = st(g.conn.prepare(
                    "SELECT tenant_id, queue_id, item_id FROM pqueue_items \
                     WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                     AND lease_expires_at<?1 ORDER BY tenant_id, queue_id",
                ))?;
                let rows = st(stmt.query_map(params![now_n], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                }))?;
                let mut by_queue: Vec<(QueueKey, Vec<ItemId>)> = Vec::new();
                for r in rows {
                    let (t, q, id) = st(r)?;
                    let key = QueueKey::new(
                        TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                        QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
                    );
                    let id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
                    match by_queue.last_mut() {
                        Some((k, ids)) if *k == key => ids.push(id),
                        _ => by_queue.push((key, vec![id])),
                    }
                }
                by_queue
            };
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                report.leases_reclaimed += ids.len() as u64;
                g.commit_command(
                    &shard,
                    QueueCommand::LeaseExpired(LeaseExpiredCommand { item_ids: ids }),
                    now,
                )?;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}
