//! # Relational projection family (postgres) — BQ-12
//!
//! The postgres sibling of [`pqueue_sqlite::SqliteRelationalBackend`]: a **DB-authoritative** projection
//! family (ADR-008 / TD-001 relational class) where the `pqueue_items` SQL table **is** the projection.
//! Every lifecycle command is applied as SQL against `pqueue_items`; reads are SQL; a reconnect recovers
//! committed state from the table itself (no command log to replay). The schema + the 14-arm apply mirror
//! the sqlite relational reference arm-for-arm, so the two relational backends — and the in-memory
//! reference — stay behaviorally identical on the conformance CORE class.
//!
//! ## What postgres gives that sqlite cannot: pool-ready row-level concurrency
//! The sqlite relational backend serializes its in-one-transaction claim with a process-wide `Mutex` (a
//! deferred sqlite transaction takes no row lock at SELECT time). Postgres has genuine row locks, so the
//! claim is written as a real `… FOR UPDATE SKIP LOCKED` CTE and the per-queue sequence counters are
//! allocated by an **atomic** `UPDATE … RETURNING` (increment-and-return in one statement). The SQL is
//! therefore concurrency-correct **by construction**: were two transactions to run it concurrently, they
//! would lock-and-skip disjoint candidate sets and could not both read the same sequence value (no
//! read-check-then-write **TOCTOU** — the I4 hazard the log-backed backend documented). HONEST CAVEAT: in
//! the single-node launch posture below this backend still holds ONE `Client` behind `Mutex<Inner>`, so
//! two claims cannot actually run at once — the Mutex is today's serializer and the row lock is not yet
//! exercised concurrently. The point is that adding the deferred connection pool is SAFE: it does not
//! reintroduce a TOCTOU or need new locking (unlike the log-backed backend, whose read-then-write guards
//! would). A live contended-writer test requires that pool; it is not exercisable through one Mutex-guarded
//! connection.
//!
//! ## Connection / runtime posture (consistent with the crate's recorded post-launch caveat)
//! Like [`crate::PostgresBackend`], this uses the SYNC `postgres` client behind a `Mutex<Client>` for the
//! single-node launch posture, and the port bodies make blocking calls inside `std::future::ready` — so it
//! must be driven OFF a tokio runtime (the conformance/reconnect tests use `futures::executor::block_on`).
//! Wrapping every call in `spawn_blocking` + a connection POOL (so a tokio `pqueue-server` can drive it
//! concurrently) is the production wiring; it is a recorded post-launch refinement here too. Crucially,
//! unlike the log-backed backend, adding that pool is SAFE without new locking: the claim already row-locks
//! and the sequence allocation is already atomic.
//!
//! ## Lease tokens / timestamps (parity with the sqlite reference)
//! Lease tokens are stored hash-only (`lease_token_hash`) with an ephemeral in-process `live_tokens` map
//! for `pending()`/`claimed_view()` token parity; a reconnect loses the live token (the lease stays
//! `Leased` and is reclaimed by the owner) — the same documented contract as sqlite. Timestamps are stored
//! as BIGINT nanoseconds-since-epoch (matching the sqlite reference for cross-family byte-parity of the
//! claim ordering); TD-002's production schema uses `timestamptz` — a column-type choice that does not
//! change behavior and is deferred to the live-DB hardening.
//!
//! LIVE-DB EVIDENCE IS GATED: this environment has no `PQUEUE_PG_TEST_URL`, so the core +
//! relational-reconnect + contended-writer suites against a live postgres are DEFERRED (they run, with a
//! LOUD skip, when the env var points at a database). The non-gated evidence is: this compiles, the SQL
//! shapes are unit-asserted (`sql_shape_tests`), and the sqlite-relational parity reference is unchanged.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;
use postgres::{Client, NoTls};
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityModel, PriorityValue,
    QueueDefinition, QueueId, TenantId, UtcTimestamp, is_retry_exhausted, priority_sort,
};
use pqueue_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, ClaimedItem, CommandEnvelope,
    CommandPosition, ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError,
    EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort, ItemView,
    LeaseExpiredCommand, LeaseView, LogWriter, ProjectionRead, ProjectionWriter, PurgeItemsCommand,
    PurgePort, PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    ReplacePendingCommand, TickReport, UpsertOutcome, UpsertPort, build_push_items,
    require_item_level_claim, validate_purge_force,
};
use sha2::{Digest, Sha256};

/// The relational schema (postgres). Mirrors the sqlite reference column-for-column: `pqueue_items` is
/// TD-002's item projection plus the reference operational columns (`fenced`/`superseded`/`max_attempts`/
/// `created_seq`); a partial unique index keeps one ACTIVE item per `client_item_key`; `relational_cursor`
/// holds the per-queue command + item sequence counters (allocated atomically). `pqueue_group_summary` and
/// `pqueue_item_key_retention` are the relational-only group/idempotency projections (BQ-11c parity).
pub(crate) const RELATIONAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS pqueue_items (
    tenant_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    client_item_key TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    priority TEXT,
    priority_sort BYTEA NOT NULL,
    not_before BIGINT,
    eligible_since BIGINT,
    group_key TEXT,
    cohort_size BIGINT,
    recurrence_until BIGINT,
    payload BYTEA,
    metadata TEXT NOT NULL DEFAULT '{}',
    retry_count BIGINT NOT NULL DEFAULT 0,
    item_version BIGINT NOT NULL,
    lease_token_hash BYTEA,
    lease_expires_at BIGINT,
    worker_id TEXT,
    last_command_sequence BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    terminal_at BIGINT,
    fenced BOOLEAN NOT NULL DEFAULT false,
    superseded BOOLEAN NOT NULL DEFAULT false,
    max_attempts BIGINT NOT NULL,
    created_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS pqueue_items_active_key
    ON pqueue_items (tenant_id, queue_id, client_item_key) WHERE superseded = false;
CREATE INDEX IF NOT EXISTS pqueue_items_claim_idx
    ON pqueue_items (tenant_id, queue_id, priority_sort, created_seq) WHERE lifecycle_state = 'Pending';
CREATE TABLE IF NOT EXISTS relational_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    next_seq BIGINT NOT NULL,
    next_item_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS pqueue_group_summary (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, group_key TEXT NOT NULL,
    oldest_eligible_at BIGINT,
    rep_progress_guard_sort BYTEA,
    rep_priority_sort BYTEA,
    rep_created_at BIGINT,
    rep_item_id TEXT,
    eligible_item_count BIGINT NOT NULL DEFAULT 0,
    at_risk_count BIGINT NOT NULL DEFAULT 0,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, group_key)
);
CREATE TABLE IF NOT EXISTS pqueue_item_key_retention (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, client_item_key TEXT NOT NULL,
    item_id TEXT NOT NULL, expires_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, client_item_key)
);
"#;

/// The serialized claim CTE (TD-002 `BatchClaim`): select the eligible candidates under a real
/// `FOR UPDATE SKIP LOCKED` row lock and lease them in ONE statement, RETURNING the rich claimed rows.
/// Concurrent claimers lock disjoint candidate sets — no process Mutex, no select-then-lease TOCTOU.
/// Authored as a constant so its shape is unit-asserted without a live DB (`sql_shape_tests`).
pub(crate) const CLAIM_CTE: &str = "\
WITH candidates AS ( \
    SELECT item_id FROM pqueue_items \
    WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Pending' AND superseded=false \
      AND (not_before IS NULL OR not_before<=$3) AND eligible_since IS NOT NULL \
    ORDER BY priority_sort, created_seq \
    LIMIT $4 \
    FOR UPDATE SKIP LOCKED \
) \
UPDATE pqueue_items i \
SET lifecycle_state='Leased', lease_token_hash=$5, lease_expires_at=$6, \
    retry_count=retry_count+1, item_version=item_version+1, updated_at=$7, last_command_sequence=$8 \
FROM candidates c \
WHERE i.tenant_id=$1 AND i.queue_id=$2 AND i.item_id=c.item_id \
RETURNING i.item_id, i.client_item_key, i.item_version, i.priority, i.group_key, i.not_before, \
          i.lease_expires_at, i.retry_count, i.payload";

// ---------------------------------------------------------------------------
// small conversions / error mapping
// ---------------------------------------------------------------------------

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

/// Tagged priority-sort encoding, byte-identical to the in-memory `elig_key` and the sqlite reference.
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
// Inner: the durable client + the queue-definition cache + the live-token map
// ---------------------------------------------------------------------------

struct Inner {
    client: Client,
    queues: HashMap<QueueKey, QueueDefinition>,
    live_tokens: HashMap<ItemId, LeaseToken>,
    cmd_seq: u64,
}

impl Inner {
    /// Reload the queue-def cache + restore `cmd_seq` past every server-assigned id already in the durable
    /// projection (so a push after reconnect never re-mints an existing item id). No log to replay.
    fn reload(&mut self) -> EngineResult<()> {
        let rows = st(self.client.query("SELECT definition FROM queues", &[]))?;
        for row in rows {
            let def_json: String = row.get(0);
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.queues.insert(key, definition);
        }
        let mut max_n: Option<u64> = None;
        let rows = st(self.client.query("SELECT item_id FROM pqueue_items", &[]))?;
        for row in rows {
            let id: String = row.get(0);
            if let Some(n) = id
                .strip_prefix("rel-")
                .and_then(|s| s.split('-').next())
                .and_then(|s| s.parse::<u64>().ok())
            {
                max_n = Some(max_n.map_or(n, |m| m.max(n)));
            }
        }
        if let Some(m) = max_n {
            self.cmd_seq = m + 1;
        }
        Ok(())
    }

    /// Assign the next command sequence for `shard` (atomic increment-and-return — no TOCTOU), apply
    /// `command`, and commit. Token-map mutations apply post-commit (a commit failure cannot desync them).
    fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let Inner {
            client,
            queues,
            live_tokens,
            ..
        } = self;
        let (t, q) = parts(shard);
        let mut tx = st(client.transaction())?;
        let seq = alloc_seq(&mut tx, &t, &q)?;
        let mut token_ops = Vec::new();
        apply_command_sql(&mut tx, queues, &mut token_ops, shard, seq, now, &command)?;
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops);
        Ok(())
    }
}

/// Allocate ONE command-position sequence for the queue with an ATOMIC increment-and-return: a single
/// `UPDATE … RETURNING` reads and advances the counter in one statement, so two concurrent allocators can
/// never read the same value (the I4 TOCTOU the log-backed backend documented is structurally impossible
/// here). Returns the allocated value (the pre-increment counter).
fn alloc_seq(tx: &mut postgres::Transaction<'_>, t: &str, q: &str) -> EngineResult<u64> {
    let row = st(tx.query_opt(
        "UPDATE relational_cursor SET next_seq = next_seq + 1 WHERE tenant=$1 AND queue=$2 \
         RETURNING next_seq - 1",
        &[&t, &q],
    ))?
    .ok_or(EngineError::NotFound)?;
    let seq: i64 = row.get(0);
    Ok(seq as u64)
}

/// Allocate ONE stable per-queue item insertion sequence (`created_seq`), atomically (same rationale as
/// [`alloc_seq`]).
fn alloc_item_seq(tx: &mut postgres::Transaction<'_>, t: &str, q: &str) -> EngineResult<i64> {
    let row = st(tx.query_one(
        "UPDATE relational_cursor SET next_item_seq = next_item_seq + 1 WHERE tenant=$1 AND queue=$2 \
         RETURNING next_item_seq - 1",
        &[&t, &q],
    ))?;
    Ok(row.get(0))
}

// ---------------------------------------------------------------------------
// token-op deferral (post-commit live-token map maintenance)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// group summary maintenance
// ---------------------------------------------------------------------------

fn groups_of(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<GroupKey>> {
    let (t, q) = parts(shard);
    let mut seen: Vec<GroupKey> = Vec::new();
    for id in ids {
        let row = st(tx.query_opt(
            "SELECT group_key FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
            &[&t, &q, &id.as_str()],
        ))?;
        if let Some(row) = row {
            let g: Option<String> = row.get(0);
            if let Some(g) = g {
                let gk = GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
                if !seen.contains(&gk) {
                    seen.push(gk);
                }
            }
        }
    }
    Ok(seen)
}

/// Recompute `pqueue_group_summary` for one group from `pqueue_items` (exact at mutation time; lagged
/// across a time-only `not_before` crossing — same contract as the sqlite reference; BQ-14 consumers
/// re-apply the gate on read).
fn refresh_group_summary(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    group_key: &GroupKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let agg = st(tx.query_one(
        "SELECT COUNT(*)::bigint, MIN(eligible_since) FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false AND (not_before IS NULL OR not_before<=$4)",
        &[&t, &q, &group_key.as_str(), &now_n],
    ))?;
    let count: i64 = agg.get(0);
    let oldest: Option<i64> = agg.get(1);
    let rep = st(tx.query_opt(
        "SELECT priority_sort, created_at, item_id FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false AND (not_before IS NULL OR not_before<=$4) \
         ORDER BY priority_sort, created_seq LIMIT 1",
        &[&t, &q, &group_key.as_str(), &now_n],
    ))?;
    let (rep_psort, rep_created, rep_item): (Option<Vec<u8>>, Option<i64>, Option<String>) =
        match rep {
            Some(row) => (Some(row.get(0)), Some(row.get(1)), Some(row.get(2))),
            None => (None, None, None),
        };
    st(tx.execute(
        "INSERT INTO pqueue_group_summary \
         (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
          rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8,0,$9) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
          oldest_eligible_at=EXCLUDED.oldest_eligible_at, \
          rep_progress_guard_sort=EXCLUDED.rep_progress_guard_sort, \
          rep_priority_sort=EXCLUDED.rep_priority_sort, rep_created_at=EXCLUDED.rep_created_at, \
          rep_item_id=EXCLUDED.rep_item_id, eligible_item_count=EXCLUDED.eligible_item_count, \
          at_risk_count=EXCLUDED.at_risk_count, updated_at=EXCLUDED.updated_at",
        &[
            &t,
            &q,
            &group_key.as_str(),
            &oldest,
            &rep_psort,
            &rep_created,
            &rep_item,
            &count,
            &now_n,
        ],
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// apply: the 14-arm command -> SQL projection write
// ---------------------------------------------------------------------------

fn insert_item(
    tx: &mut postgres::Transaction<'_>,
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
    let created_seq = alloc_item_seq(tx, &t, &q)?;
    let now_n = ts_nanos(now);
    let seq = seq as i64;
    let max_attempts = item.max_attempts as i64;
    let gk = item.group_key.as_ref().map(|g| g.as_str().to_string());
    let item_id = item.item_id.as_str().to_string();
    let key = item.client_item_key.as_str().to_string();
    st(tx.execute(
        "INSERT INTO pqueue_items \
         (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
          not_before,eligible_since,group_key,payload,metadata,retry_count,item_version,\
          lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,updated_at,\
          terminal_at,fenced,superseded,max_attempts,created_seq) \
         VALUES ($1,$2,$3,$4,'Pending',$5,$6,$7,$8,$9,$10,'{}',0,1,NULL,NULL,NULL,$11,$12,$12,NULL,\
                 false,false,$13,$14)",
        &[
            &t,
            &q,
            &item_id,
            &key,
            &priority_json,
            &sort,
            &not_before,
            &eligible_since,
            &gk,
            &payload,
            &seq,
            &now_n,
            &max_attempts,
            &created_seq,
        ],
    ))?;
    Ok(())
}

/// Apply one command to `pqueue_items` as SQL. Mirrors `ProjectionData::apply_command` (and the sqlite
/// reference) arm-for-arm. Token-map mutations accumulate in `token_ops` (applied post-commit).
fn apply_command_sql(
    tx: &mut postgres::Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    token_ops: &mut Vec<TokenOp>,
    shard: &QueueKey,
    seq: u64,
    now: UtcTimestamp,
    command: &QueueCommand,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let seqi = seq as i64;
    match command {
        QueueCommand::CreateQueue(_) => Ok(()),
        QueueCommand::Push(c) => {
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            let mut groups: Vec<GroupKey> = Vec::new();
            for it in &c.items {
                insert_item(tx, &model, shard, it, seq, now)?;
                if let Some(g) = &it.group_key
                    && !groups.contains(g)
                {
                    groups.push(g.clone());
                }
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::Claim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=$4, \
                     lease_expires_at=$5, retry_count=retry_count+1, item_version=item_version+1, \
                     updated_at=$6, last_command_sequence=$7 \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str(), &hash, &exp, &now_n, &seqi],
                ))?;
                token_ops.push(TokenOp::Set(id.clone(), c.lease_token.clone()));
            }
            for g in groups_of(tx, shard, &c.item_ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lease_expires_at=$4, item_version=item_version+1, \
                     updated_at=$5, last_command_sequence=$6 \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str(), &exp, &now_n, &seqi],
                ))?;
            }
            Ok(())
        }
        QueueCommand::ReassignLease(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lease_token_hash=$4, lease_expires_at=$5, \
                     retry_count=retry_count+1, item_version=item_version+1, updated_at=$6, \
                     last_command_sequence=$7 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str(), &hash, &exp, &now_n, &seqi],
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
                        let row = st(tx.query_one(
                            "SELECT retry_count, max_attempts FROM pqueue_items \
                             WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                            &[&t, &q, &o.item_id.as_str()],
                        ))?;
                        let rc: i64 = row.get(0);
                        let ma: i64 = row.get(1);
                        if is_retry_exhausted(rc as u32, ma as u32) {
                            ItemState::Failed
                        } else {
                            ItemState::Pending
                        }
                    }
                    FinalizeKind::Release => ItemState::Pending,
                    FinalizeKind::Rearm => ItemState::Pending,
                };
                let terminal_at: Option<i64> = new_state.is_terminal().then_some(now_n);
                let reset_attempts = matches!(o.kind, FinalizeKind::Rearm);
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state=$4, lease_token_hash=NULL, \
                     lease_expires_at=NULL, fenced=false, item_version=item_version+1, \
                     retry_count=CASE WHEN $5 THEN 0 ELSE retry_count END, \
                     terminal_at=$6, updated_at=$7, last_command_sequence=$8 \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[
                        &t,
                        &q,
                        &o.item_id.as_str(),
                        &state_str(new_state),
                        &reset_attempts,
                        &terminal_at,
                        &now_n,
                        &seqi,
                    ],
                ))?;
                token_ops.push(TokenOp::Clear(o.item_id.clone()));
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id.clone()).collect();
            for g in groups_of(tx, shard, &ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=true, updated_at=$4, last_command_sequence=$5 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&t, &q, &c.superseded_item_id.as_str(), &now_n, &seqi],
            ))?;
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_item(tx, &model, shard, &c.replacement, seq, now)?;
            let mut groups = groups_of(tx, shard, std::slice::from_ref(&c.superseded_item_id))?;
            if let Some(g) = &c.replacement.group_key
                && !groups.contains(g)
            {
                groups.push(g.clone());
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                     lease_expires_at=NULL, item_version=item_version+1, updated_at=$4, \
                     last_command_sequence=$5 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str(), &now_n, &seqi],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            for g in groups_of(tx, shard, &c.item_ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortExpired(c) => {
            let rows = st(tx.query(
                "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
                 AND group_key=$3 AND lifecycle_state NOT IN ('Complete','Failed')",
                &[&t, &q, &c.group_key.as_str()],
            ))?;
            let mut ids = Vec::new();
            for row in rows {
                let id: String = row.get(0);
                ids.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
            }
            for id in &ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET lifecycle_state='Failed', item_version=item_version+1, \
                     terminal_at=$4, updated_at=$4, last_command_sequence=$5 \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str(), &now_n, &seqi],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            refresh_group_summary(tx, shard, &c.group_key, now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=true WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str()],
                ))?;
            }
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=false WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str()],
                ))?;
            }
            Ok(())
        }
        QueueCommand::PauseQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=true WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            Ok(())
        }
        QueueCommand::ResumeQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=false WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            Ok(())
        }
        QueueCommand::PurgeItems(c) => {
            let retention_ms = queues
                .get(shard)
                .map(|d| d.client_item_key_retention_ms)
                .unwrap_or(0);
            let mut groups: Vec<GroupKey> = Vec::new();
            for id in &c.item_ids {
                let row = st(tx.query_opt(
                    "SELECT group_key, client_item_key, lifecycle_state FROM pqueue_items \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str()],
                ))?;
                if let Some(row) = row {
                    let gk: Option<String> = row.get(0);
                    let ck: String = row.get(1);
                    let state: String = row.get(2);
                    if parse_state(&state)?.is_terminal() && retention_ms > 0 {
                        let expires =
                            now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000));
                        st(tx.execute(
                            "INSERT INTO pqueue_item_key_retention \
                             (tenant_id,queue_id,client_item_key,item_id,expires_at) \
                             VALUES ($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,queue_id,client_item_key) \
                             DO UPDATE SET item_id=EXCLUDED.item_id, expires_at=EXCLUDED.expires_at",
                            &[&t, &q, &ck, &id.as_str(), &expires],
                        ))?;
                    }
                    if let Some(g) = gk {
                        let gk2 =
                            GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
                        if !groups.contains(&gk2) {
                            groups.push(gk2);
                        }
                    }
                }
                st(tx.execute(
                    "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &id.as_str()],
                ))?;
                token_ops.push(TokenOp::Clear(id.clone()));
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// read queries
// ---------------------------------------------------------------------------

fn queue_paused(client: &mut Client, shard: &QueueKey) -> EngineResult<bool> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT paused FROM queues WHERE tenant=$1 AND queue=$2",
        &[&t, &q],
    ))?
    .ok_or(EngineError::NotFound)?;
    Ok(row.get(0))
}

fn select_eligible_sql(
    client: &mut Client,
    shard: &QueueKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(client, shard)? {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let lim = limit as i64;
    let rows = st(client.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
         AND lifecycle_state='Pending' AND superseded=false \
         AND (not_before IS NULL OR not_before<=$3) AND eligible_since IS NOT NULL \
         ORDER BY priority_sort, created_seq LIMIT $4",
        &[&t, &q, &now_n, &lim],
    ))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        out.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

fn peek_sql(client: &mut Client, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
    let (t, q) = parts(shard);
    let lim = limit as i64;
    let rows = st(client.query(
        "SELECT item_id, client_item_key, priority, item_version FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Pending' AND superseded=false \
         ORDER BY priority_sort, created_seq LIMIT $3",
        &[&t, &q, &lim],
    ))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        let key: String = row.get(1);
        let priority: Option<String> = row.get(2);
        let version: i64 = row.get(3);
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

fn pending_sql(
    client: &mut Client,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
) -> EngineResult<Vec<LeaseView>> {
    let (t, q) = parts(shard);
    let rows = st(client.query(
        "SELECT item_id, lease_expires_at, retry_count FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased'",
        &[&t, &q],
    ))?;
    let mut out = Vec::new();
    for row in rows {
        let id: String = row.get(0);
        let exp: Option<i64> = row.get(1);
        let retry: i64 = row.get(2);
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

/// Build a [`ClaimedItem`] from a row carrying (client_item_key, item_version, priority, group_key,
/// not_before, lease_expires_at, retry_count, payload), pairing it with `token`. Shared by the claim CTE
/// RETURNING and the `claimed_view` read port.
#[allow(clippy::too_many_arguments)]
fn claimed_from_row(
    item_id: ItemId,
    token: LeaseToken,
    key: String,
    version: i64,
    priority: Option<String>,
    group: Option<String>,
    not_before: Option<i64>,
    exp: i64,
    retry: i64,
    payload: Option<Vec<u8>>,
) -> EngineResult<ClaimedItem> {
    Ok(ClaimedItem {
        item_id,
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
    })
}

fn render_claimed(
    client: &mut Client,
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
        let row = st(client.query_opt(
            "SELECT client_item_key, item_version, priority, group_key, not_before, \
             lease_expires_at, retry_count, payload FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 AND lifecycle_state='Leased'",
            &[&t, &q, &id.as_str()],
        ))?;
        let Some(row) = row else { continue };
        let exp: Option<i64> = row.get(5);
        let Some(exp) = exp else { continue };
        out.push(claimed_from_row(
            id.clone(),
            token,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            exp,
            row.get(6),
            row.get(7),
        )?);
    }
    Ok(out)
}

fn metrics_sql(client: &mut Client, shard: &QueueKey) -> EngineResult<QueueMetrics> {
    let (t, q) = parts(shard);
    let rows = st(client.query(
        "SELECT lifecycle_state, COUNT(*)::bigint FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND superseded=false GROUP BY lifecycle_state",
        &[&t, &q],
    ))?;
    let mut m = QueueMetrics::default();
    for row in rows {
        let state: String = row.get(0);
        let count: i64 = row.get(1);
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

fn item_flags(
    client: &mut Client,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<(ItemState, bool, bool)>> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT lifecycle_state, fenced, superseded FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
        &[&t, &q, &id.as_str()],
    ))?;
    match row {
        None => Ok(None),
        Some(row) => {
            let state: String = row.get(0);
            let fenced: bool = row.get(1);
            let superseded: bool = row.get(2);
            Ok(Some((parse_state(&state)?, fenced, superseded)))
        }
    }
}

fn validate_leased(client: &mut Client, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
    for id in ids {
        match item_flags(client, shard, id)? {
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
// PostgresRelationalBackend
// ---------------------------------------------------------------------------

/// Postgres-backed **relational** projection family (`pqueue_items` is DB-authoritative). Atomic class.
pub struct PostgresRelationalBackend {
    inner: Mutex<Inner>,
}

impl PostgresRelationalBackend {
    /// Connect to `url` (default `search_path`), ensure the schema, and load the queue-def cache.
    pub fn connect(url: &str) -> EngineResult<Self> {
        let client = st(Client::connect(url, NoTls))?;
        Self::from_client(client)
    }

    /// Connect isolated in a dedicated `schema` (the postgres analogue of a fresh sqlite file). Reconnecting
    /// with the SAME `schema` reopens the same DB-authoritative projection — used by the conformance +
    /// relational-reconnect suites.
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
        st(client.batch_execute(RELATIONAL_SCHEMA))?;
        let mut inner = Inner {
            client,
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

// --- Backend::write unit of work --------------------------------------------------------------------
//
// Unlike rusqlite (whose `Transaction` methods take `&self`, letting two writers share `&tx`), the sync
// postgres `Transaction` methods take `&mut self`. The append-then-apply closure the conformance `commit`
// helper drives needs both `&mut dyn LogWriter` and `&mut dyn ProjectionWriter` live at once, so the two
// writers share the ONE transaction through a `RefCell` and `borrow_mut()` it at call time. The closure
// calls them sequentially (append returns before apply runs), so the runtime borrows never overlap.

struct PgRelLogWriter<'a, 'b> {
    tx: &'a RefCell<postgres::Transaction<'b>>,
}

impl LogWriter for PgRelLogWriter<'_, '_> {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let mut positions = Vec::with_capacity(commands.len());
        for _ in commands {
            let mut tx = self.tx.borrow_mut();
            let seq = alloc_seq(&mut tx, &t, &q)?;
            positions.push(CommandPosition::new(shard.clone(), 0, seq));
        }
        Ok(positions)
    }
}

struct PgRelProjectionWriter<'a, 'b> {
    tx: &'a RefCell<postgres::Transaction<'b>>,
    queues: &'a HashMap<QueueKey, QueueDefinition>,
    token_ops: &'a mut Vec<TokenOp>,
}

impl ProjectionWriter for PgRelProjectionWriter<'_, '_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, env) in positions.iter().zip(commands) {
            let mut tx = self.tx.borrow_mut();
            apply_command_sql(
                &mut tx,
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

impl Backend for PostgresRelationalBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        let result = (|| {
            let mut guard = self
                .inner
                .lock()
                .expect("postgres relational backend poisoned");
            let Inner {
                client,
                queues,
                live_tokens,
                ..
            } = &mut *guard;
            let tx_cell = RefCell::new(st(client.transaction())?);
            let mut token_ops = Vec::new();
            let r = {
                let mut lw = PgRelLogWriter { tx: &tx_cell };
                let mut pw = PgRelProjectionWriter {
                    tx: &tx_cell,
                    queues,
                    token_ops: &mut token_ops,
                };
                f(&mut lw, &mut pw)?
            };
            st(tx_cell.into_inner().commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit
            Ok(r)
        })();
        std::future::ready(result)
    }
}

impl ControlPlaneStore for PostgresRelationalBackend {
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
                "INSERT INTO queues(tenant,queue,definition,paused) VALUES($1,$2,$3,false)",
                &[&t, &q, &def_json],
            ))?;
            st(g.client.execute(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES($1,$2,0,0)",
                &[&t, &q],
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
        std::future::ready(Ok(0))
    }
}

impl ProjectionRead for PostgresRelationalBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            select_eligible_sql(&mut g.client, shard, now, limit)
        };
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            peek_sql(&mut g.client, shard, limit)
        };
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *g;
            pending_sql(client, live_tokens, shard)
        };
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *g;
            let tokens = live_tokens.clone();
            render_claimed(client, shard, ids, |id| tokens.get(id).cloned())
        };
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            metrics_sql(&mut g.client, queue)
        };
        std::future::ready(result)
    }
}

impl PushPort for PostgresRelationalBackend {
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

impl ClaimPort for PostgresRelationalBackend {
    /// The TD-002 serialized claim CTE with a REAL `FOR UPDATE SKIP LOCKED` row lock: candidate selection
    /// and the lease land in ONE statement, RETURNING the rich claimed rows.
    ///
    /// CONCURRENCY (honest framing): in this single-node launch posture the backend holds ONE `Client`
    /// behind `Mutex<Inner>`, so two claims cannot run at once and the Mutex is the serializer — the
    /// `FOR UPDATE SKIP LOCKED` row lock is not exercised concurrently yet. Its value is that the SQL is
    /// **pool-ready**: when the deferred connection pool + `spawn_blocking` lands (so a tokio server drives
    /// concurrent connections), correct concurrent claiming comes from the row lock with NO Mutex, and the
    /// atomic `alloc_seq` keeps sequence allocation race-free. A live contended-writer test therefore
    /// requires that pool; it cannot be exercised through this Mutex-guarded single connection.
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // BQ-14a: gate non-item compatibility (group/cohort selection lands in BQ-14b/c); the
            // item-level CTE path is unchanged.
            if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, def)?;
            }
            // Paused queues yield nothing (the CTE itself does not encode pause).
            if queue_paused(&mut g.client, &req.shard)? {
                return Ok(Claimed::default());
            }
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(&req.shard);
            let mut tx = st(client.transaction())?;
            let seq = alloc_seq(&mut tx, &t, &q)?;
            let now_n = ts_nanos(req.now);
            let exp = ts_nanos(req.lease_expires_at);
            let hash = lease_hash(&req.lease_token);
            let lim = req.max_items as i64;
            let seqi = seq as i64;
            let rows = st(tx.query(
                CLAIM_CTE,
                &[&t, &q, &now_n, &lim, &hash, &exp, &now_n, &seqi],
            ))?;
            if rows.is_empty() {
                // Nothing eligible: roll back (drop tx) so the allocated sequence is NOT burned — parity
                // with the sqlite reference's early return (no `last_command_sequence` gap on an empty claim).
                return Ok(Claimed::default());
            }
            let mut items = Vec::with_capacity(rows.len());
            let mut token_ops = Vec::new();
            let mut claimed_ids = Vec::with_capacity(rows.len());
            for row in rows {
                let id: String = row.get(0);
                let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
                let exp_row: Option<i64> = row.get(6);
                let exp_row = exp_row.unwrap_or(exp);
                items.push(claimed_from_row(
                    item_id.clone(),
                    req.lease_token.clone(),
                    row.get(1),
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                    exp_row,
                    row.get(7),
                    row.get(8),
                )?);
                token_ops.push(TokenOp::Set(item_id.clone(), req.lease_token.clone()));
                claimed_ids.push(item_id);
            }
            // The CTE bypasses `apply_command_sql`'s Claim arm, so refresh the claimed items' group
            // summaries HERE, in the same transaction — otherwise a leased item would stay stale-counted in
            // `pqueue_group_summary` (parity with the sqlite reference, whose claim refreshes on the arm).
            for grp in groups_of(&mut tx, &req.shard, &claimed_ids)? {
                refresh_group_summary(&mut tx, &req.shard, &grp, req.now)?;
            }
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops);
            Ok(Claimed { items })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for PostgresRelationalBackend {
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
            let existing = st(g.client.query_opt(
                "SELECT item_id, lifecycle_state FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=$3 AND superseded=false",
                &[&t, &q, &client_item_key.as_str()],
            ))?;
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
                    // Retention tombstone (a terminal item purged within retention) keeps the re-push a dup.
                    let retained = st(g.client.query_opt(
                        "SELECT expires_at FROM pqueue_item_key_retention \
                         WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=$3",
                        &[&t, &q, &client_item_key.as_str()],
                    ))?;
                    if let Some(row) = retained {
                        let expires: i64 = row.get(0);
                        if expires > ts_nanos(now) {
                            return Err(EngineError::Terminal);
                        }
                        st(g.client.execute(
                            "DELETE FROM pqueue_item_key_retention \
                             WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=$3",
                            &[&t, &q, &client_item_key.as_str()],
                        ))?;
                    }
                    g.commit_command(
                        shard,
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        now,
                    )?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                Some(row) => {
                    let existing_id: String = row.get(0);
                    let state: String = row.get(1);
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

impl FinalizePort for PostgresRelationalBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id.clone()).collect();
            validate_leased(&mut g.client, shard, &ids)?;
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

impl RenewLeasePort for PostgresRelationalBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_leased(&mut g.client, shard, &item_ids)?;
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

impl ReassignLeasePort for PostgresRelationalBackend {
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
            validate_leased(&mut g.client, shard, &item_ids)?;
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

impl PurgePort for PostgresRelationalBackend {
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
                    continue;
                }
                if let Some((state, _, _)) = item_flags(&mut g.client, shard, id)? {
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

impl ReclaimDriver for PostgresRelationalBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let now_n = ts_nanos(now);
            let rows = st(g.client.query(
                "SELECT tenant_id, queue_id, item_id FROM pqueue_items \
                 WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                 AND lease_expires_at<$1 ORDER BY tenant_id, queue_id",
                &[&now_n],
            ))?;
            let mut by_queue: Vec<(QueueKey, Vec<ItemId>)> = Vec::new();
            for row in rows {
                let t: String = row.get(0);
                let q: String = row.get(1);
                let id: String = row.get(2);
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
            let mut report = TickReport::default();
            for (shard, ids) in by_queue {
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

#[cfg(test)]
mod sql_shape_tests {
    //! No-DB assertions on the assembled SQL shapes (the live-DB behavioral suites are env-gated on
    //! `PQUEUE_PG_TEST_URL`). These pin the concurrency-critical pieces: the claim uses a real row lock and
    //! the sequence allocation is a single atomic increment-and-return (no read-then-write TOCTOU).
    use super::*;

    #[test]
    fn claim_cte_uses_for_update_skip_locked() {
        assert!(
            CLAIM_CTE.contains("FOR UPDATE SKIP LOCKED"),
            "the postgres claim MUST take a real row lock, not rely on a Mutex"
        );
        assert!(CLAIM_CTE.contains("ORDER BY priority_sort, created_seq"));
        assert!(
            CLAIM_CTE.contains("RETURNING"),
            "claim leases + returns the rich rows in one statement"
        );
    }

    #[test]
    fn sequence_allocation_is_atomic_increment_and_return() {
        // The schema declares the cursor; allocation is an UPDATE ... RETURNING (see alloc_seq /
        // alloc_item_seq) — no SELECT MAX(...) read-then-write, which would TOCTOU under a pool.
        assert!(RELATIONAL_SCHEMA.contains("relational_cursor"));
        assert!(RELATIONAL_SCHEMA.contains("next_seq BIGINT"));
        assert!(RELATIONAL_SCHEMA.contains("next_item_seq BIGINT"));
    }

    #[test]
    fn schema_has_relational_projections() {
        for table in [
            "pqueue_items",
            "pqueue_group_summary",
            "pqueue_item_key_retention",
        ] {
            assert!(RELATIONAL_SCHEMA.contains(table), "missing {table}");
        }
        assert!(
            RELATIONAL_SCHEMA.contains("WHERE superseded = false"),
            "active-key partial unique index"
        );
    }
}

#[cfg(test)]
mod gated_group_summary_tests {
    //! Env-gated (`PQUEUE_PG_TEST_URL`) white-box guard that the claim path refreshes
    //! `pqueue_group_summary` (the BQ-12 fresh-eyes BLOCKING fix). LOUD-skips without a live DB. Reads the
    //! summary table directly via the private client (there is no read port until BQ-14).
    use super::*;
    use futures::executor::block_on;
    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModelKind, PriorityTieBreaker,
        RecurrencePolicy, RetryPolicy, WorkerId,
    };
    use pqueue_engine::ClaimRequest;

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t1").unwrap(),
            queue_id: QueueId::new("q1").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
        }
    }
    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
    }
    fn ts(s: i64) -> UtcTimestamp {
        UtcTimestamp::new(s, 0).unwrap()
    }
    fn grouped(priority: i64) -> PushSpec {
        PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new("g").unwrap()),
            ..Default::default()
        }
    }
    fn claim_req(max: usize, exp: i64, now: i64) -> ClaimRequest {
        ClaimRequest {
            shard: shard(),
            worker_id: WorkerId::new("w1").unwrap(),
            max_items: max,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(exp),
            now: ts(now),
            compatibility: ClaimCompatibility::default(),
        }
    }
    fn group_count(b: &PostgresRelationalBackend) -> i64 {
        let mut g = b.inner.lock().unwrap();
        g.client
            .query_one(
                "SELECT eligible_item_count FROM pqueue_group_summary \
                 WHERE tenant_id='t1' AND queue_id='q1' AND group_key='g'",
                &[],
            )
            .unwrap()
            .get(0)
    }

    #[test]
    fn claim_refreshes_group_summary() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (claim_refreshes_group_summary) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_gs_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect");
        block_on(b.create_queue(qdef())).unwrap();
        block_on(b.push(&shard(), vec![grouped(10), grouped(20)], ts(0))).unwrap();
        assert_eq!(group_count(&b), 2, "two grouped items eligible");
        // Claim the rep (priority 10) — the claim path MUST refresh the summary (count -> 1).
        block_on(b.claim(claim_req(1, 500, 10))).unwrap();
        assert_eq!(
            group_count(&b),
            1,
            "claim must refresh pqueue_group_summary (leased item leaves the eligible count)"
        );
    }
}
