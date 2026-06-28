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
//! `core_suite!(@atomic)` at parity with the in-memory reference. BQ-11c = `pqueue_group_summary`
//! (maintained in-transaction with every grouped-item mutation; consumer is BQ-14 g1/g4) + the
//! `client_item_key` retention tombstone (`pqueue_item_key_retention`) for duplicate-push convergence
//! across a purge. Still ahead: the relational-reconnect suite (BQ-11d) and group/cohort/gate selection
//! (BQ-14). `progress_guard_sort` bounded-relaxed promotion is a cross-family enhancement deferred so the
//! two projection families never diverge on the core class.
//!
//! RELATIONAL-ONLY (deliberately OUT of the shared core class): the retention tombstone makes
//! push→complete→purge→re-push(same key) return `Terminal` here, whereas the log-replay/in-memory family
//! (no retention) would `Insert` a fresh item. No core conformance scenario exercises that sequence, so
//! the "two families identical on core" invariant holds; BQ-13 must keep retention (and `group_summary`)
//! a relational-class concern, NOT add it to the shared core suite — else the families would diverge.
//!
//! DEFERRED — data-plane request-id idempotency (`pqueue_request_idempotency`, TD-002 §Idempotency): no
//! orchestration port carries a `request_id` today (every `CommandEnvelope` is built with `request_id:
//! None`; the facade passes none; `QueueIdempotencyCache` is deliberately operator-repair-only, see
//! `pqueue_engine::operator`). Building the table now would be unreachable dead code; end-to-end
//! request-id replay needs a request-id-carrying port (a separate cross-cutting bead), so it is not
//! implemented here rather than faked. Tracked as a follow-up.
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

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityModel, PriorityValue,
    QueueDefinition, QueueId, TenantId, UtcTimestamp, is_retry_exhausted, priority_sort,
};
use pqueue_engine::ClaimUnit;
use pqueue_engine::{
    ActiveScope, Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed,
    ClaimedItem, CommandEnvelope, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DiscoveryGranularity, DiscoveryPort, DurabilityClass, EngineError, EngineResult,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort, ItemView, LeaseExpiredCommand,
    LeaseView, LiveItemView, LogWriter, ProjectionRead, ProjectionWriter, PurgeItemsCommand,
    PurgePort, PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeaseCommand, RenewLeasePort,
    QueueCounters, ReplacePendingCommand, TickReport, UpsertOutcome, UpsertPort, build_push_items,
    project_scopes, validate_claim_compatibility, validate_purge_force,
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
    fields TEXT NOT NULL DEFAULT '{}',
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
    assignment_epoch INTEGER NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
-- BQ-11c: the single per-group summary projection (TD-002 §Per-Group Summary Projection), maintained
-- in the SAME transaction as every grouped-item mutation (recompute-from-items; exact at mutation time,
-- lagged across a time-only not_before crossing — see refresh_group_summary). Consumer: BQ-14 g1
-- whole-group selection + g4 discovery + per-group observability. `rep_progress_guard_sort` is NULL while
-- the progress-guard derivation is deferred (parity with the strict claim ordering); pause is not modeled
-- (the summary counts intrinsic eligibility, ignoring the queue-global pause gate — BQ-14 applies pause).
CREATE TABLE IF NOT EXISTS pqueue_group_summary (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, group_key TEXT NOT NULL,
    oldest_eligible_at INTEGER,          -- NULL = no currently-eligible item
    rep_progress_guard_sort BLOB,
    rep_priority_sort BLOB,
    rep_created_at INTEGER,
    rep_item_id TEXT,
    eligible_item_count INTEGER NOT NULL DEFAULT 0,
    at_risk_count INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, group_key)
);
-- BQ-11c: duplicate-push convergence across a purge (TD-002 §Idempotency `pqueue_item_key_retention`):
-- when a TERMINAL item is purged, its `client_item_key` is retained until `client_item_key_retention_ms`
-- elapses, so a re-push of the same key is still rejected as a duplicate (Terminal) rather than
-- resurrecting the work. (A pending purge records no tombstone — its key is freely reusable, matching the
-- log-replay family.)
CREATE TABLE IF NOT EXISTS pqueue_item_key_retention (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, client_item_key TEXT NOT NULL,
    item_id TEXT NOT NULL, expires_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, client_item_key)
);
-- BQ-14c: the cohort projection (TD-002 §cohort). The cohort key IS the group_key; `cohort_size` is the
-- declared total member count (set by the first cohort member's push). Completeness + per-member
-- eligibility are evaluated LIVE from pqueue_items at whole_cohort claim time (this row is the
-- authoritative size + a discovery anchor). The richer lifecycle (cohort lease token, forming/leased/
-- terminal state, retention, divergent-size conflict) is deferred — see pqueue-a162438c.
CREATE TABLE IF NOT EXISTS pqueue_cohorts (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, group_key TEXT NOT NULL,
    cohort_size INTEGER NOT NULL, created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, group_key)
);
-- BQ-14d: gates (TD-002 §gate / API-001 g2). `pqueue_item_gates` is the item↔gate-key membership
-- (inserted on Push); `pqueue_gate_state` is the queue's BLOCKED gate keys (one row per blocked key,
-- maintained by SetGates). An item is gate-blocked (ineligible) iff any of its gate keys is in
-- pqueue_gate_state — the eligibility predicate anti-joins these (exact-on-read, O(blocked keys)).
CREATE TABLE IF NOT EXISTS pqueue_item_gates (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL, gate_key TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id, gate_key)
);
CREATE TABLE IF NOT EXISTS pqueue_gate_state (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, gate_key TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, gate_key)
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

fn fields_to_json(fields: &BTreeMap<String, Bytes>) -> EngineResult<String> {
    let raw: BTreeMap<&str, Vec<u8>> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.to_vec()))
        .collect();
    to_json(&raw)
}

fn fields_from_json(raw: String) -> EngineResult<BTreeMap<String, Bytes>> {
    let decoded: BTreeMap<String, Vec<u8>> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(decoded
        .into_iter()
        .map(|(k, v)| (k, Bytes::from(v)))
        .collect())
}

fn ensure_item_fields_column(conn: &Connection) -> EngineResult<()> {
    match conn.execute(
        "ALTER TABLE pqueue_items ADD COLUMN fields TEXT NOT NULL DEFAULT '{}'",
        [],
    ) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
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
        // NOTE: item-id restart-safety is handled by `restore_counters` (it seeds `QueueCounters` past the
        // highest durable id, decoding `(epoch, counter)` straight from the packed id — ADR-009).
        Ok(())
    }

    /// Assign the next command sequence for `shard`, apply `command` to `pqueue_items`, and advance the
    /// cursor — all in one transaction (the atomic append+apply UoW the async ports rely on).
    ///
    /// BQ-20 NOTE: this is the data-plane fast path (every claim/push/finalize port routes here). It is the
    /// in-process owner, so it is NOT epoch-fenced — the TD-003 `assignment_epoch` fence lives at the
    /// [`RelLogWriter::append`] seam (`LogWriter::append`). Fencing a STALE owner's claim end-to-end needs
    /// the owner to cache + pass its `expected_epoch` on every write, which arrives with the ownership/lease
    /// identity layer (BQ-21); until then no second owner exists in-process, so the gap is theoretical.
    fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let Inner {
            conn,
            queues,
            live_tokens,
            ..
        } = self;
        let (t, q) = parts(shard);
        let tx = st(conn.transaction())?;
        // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence against the owner's
        // cached acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
        // `None` is the degenerate sole-owner path (no fence). Brings this data-plane path to parity with the
        // `RelLogWriter::append` seam.
        let (seq, epoch): (i64, i64) = st(tx
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
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
    let fields = fields_to_json(&item.fields)?;
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
          not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,retry_count,item_version,\
          lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,updated_at,\
          terminal_at,fenced,superseded,max_attempts,created_seq) \
         VALUES (?1,?2,?3,?4,'Pending',?5,?6,?7,?8,?9,?16,?10,?15,'{}',0,1,NULL,NULL,NULL,?11,?12,?12,NULL,0,0,?13,?14)",
        params![
            t,
            q,
            item.item_id.to_string(),
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
            fields,
            item.cohort_size.map(|s| s as i64),
        ],
    ))?;
    // BQ-14c: a cohort member (group_key + cohort_size) forms/updates its pqueue_cohorts row.
    if let (Some(group), Some(size)) = (&item.group_key, item.cohort_size) {
        upsert_cohort(tx, shard, group, size, now)?;
    }
    // BQ-14d: record this item's gate-key membership (the anti-join source).
    for gk in &item.gate_keys {
        st(tx.execute(
            "INSERT INTO pqueue_item_gates (tenant_id,queue_id,item_id,gate_key) VALUES (?1,?2,?3,?4) \
             ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING",
            params![t, q, item.item_id.to_string(), gk.as_str()],
        ))?;
    }
    Ok(())
}

/// Maintain the `pqueue_cohorts` projection for a cohort member's push (BQ-14c). The cohort key IS the
/// `group_key`; `cohort_size` is the declared total. First declaration sets the size; a later DIVERGENT
/// `cohort_size` for the same key is recorded but does NOT overwrite (the first declaration is
/// authoritative — divergent-push CONFLICT rejection is a documented follow-up, pqueue-a162438c).
/// Completeness (`member_count == cohort_size`) is evaluated LIVE from `pqueue_items` at claim time (the
/// row is the authoritative size + a discovery anchor, like `pqueue_group_summary`).
fn upsert_cohort(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    group: &GroupKey,
    cohort_size: u64,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO pqueue_cohorts (tenant_id,queue_id,group_key,cohort_size,created_at) \
         VALUES (?1,?2,?3,?4,?5) ON CONFLICT(tenant_id,queue_id,group_key) DO NOTHING",
        params![t, q, group.as_str(), cohort_size as i64, ts_nanos(now)],
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

/// The distinct non-null `group_key`s of the given item ids (for summary refresh). For arms that DELETE
/// (purge), call this BEFORE the delete so the groups are still discoverable.
fn groups_of(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<GroupKey>> {
    let (t, q) = parts(shard);
    let mut seen: Vec<GroupKey> = Vec::new();
    for id in ids {
        let g: Option<String> = st(tx
            .query_row(
                "SELECT group_key FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![t, q, id.to_string()],
                |row| row.get(0),
            )
            .optional())?
        .flatten();
        if let Some(g) = g {
            let gk = GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
            if !seen.contains(&gk) {
                seen.push(gk);
            }
        }
    }
    Ok(seen)
}

/// Recompute `pqueue_group_summary` for one group from `pqueue_items` (exact aggregate over the group's
/// currently-eligible items, in the SAME transaction as the mutation that touched it). The representative
/// is the would-be-first-claimed eligible item (strict-claim key `priority_sort, created_seq`), matching
/// the claim selection; `rep_progress_guard_sort`/`at_risk_count` stay NULL/0 while the progress-guard
/// derivation is deferred (parity with the strict claim ordering, BQ-14).
///
/// EXACT AT MUTATION TIME, lagged across a time-only `not_before` crossing: the aggregate filters
/// `not_before<=now`, so a deferred item that becomes due WITHOUT a subsequent mutation is not reflected
/// in `oldest_eligible_at`/`rep_*`/`eligible_item_count` until the next mutation refreshes its group. The
/// per-item `select_eligible` path re-evaluates `not_before` on read and is unaffected. BQ-14 g1/g4
/// consumers MUST re-apply the `not_before` gate on read (or a due-sweep must refresh) rather than trust
/// the stored value as live across time alone.
fn refresh_group_summary(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    group_key: &GroupKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    // Eligible aggregate: pending, not superseded, due at `now`.
    let (count, oldest): (i64, Option<i64>) = st(tx.query_row(
        "SELECT COUNT(*), MIN(eligible_since) FROM pqueue_items \
         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4)",
        params![t, q, group_key.as_str(), now_n],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ))?;
    // Representative = first-claimable eligible item of the group.
    let rep: Option<(Vec<u8>, i64, String)> = st(tx
        .query_row(
            "SELECT priority_sort, created_at, item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
             ORDER BY priority_sort, created_seq LIMIT 1",
            params![t, q, group_key.as_str(), now_n],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let (rep_psort, rep_created, rep_item): (Option<Vec<u8>>, Option<i64>, Option<String>) =
        match rep {
            Some((p, c, i)) => (Some(p), Some(c), Some(i)),
            None => (None, None, None),
        };
    st(tx.execute(
        "INSERT INTO pqueue_group_summary \
         (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort,\
          rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         VALUES (?1,?2,?3,?4,NULL,?5,?6,?7,?8,0,?9) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
          oldest_eligible_at=excluded.oldest_eligible_at, \
          rep_progress_guard_sort=excluded.rep_progress_guard_sort, \
          rep_priority_sort=excluded.rep_priority_sort, rep_created_at=excluded.rep_created_at, \
          rep_item_id=excluded.rep_item_id, eligible_item_count=excluded.eligible_item_count, \
          at_risk_count=excluded.at_risk_count, updated_at=excluded.updated_at",
        params![
            t,
            q,
            group_key.as_str(),
            oldest,
            rep_psort,
            rep_created,
            rep_item,
            count,
            now_n,
        ],
    ))?;
    Ok(())
}

/// Apply one command to `pqueue_items` as SQL. Mirrors `ProjectionData::apply_command` arm-for-arm. The
/// caller must have pre-validated rejectable commands (commit has no rollback past this point), so the
/// only errors here are storage/`NotFound` faults, never behavioral rejections. Live-token mutations are
/// appended to `token_ops` (applied post-commit by the caller), never mutated in place. Grouped-item
/// mutations also refresh `pqueue_group_summary` for the affected group(s) in this same transaction.
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
                    "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?4, \
                     lease_expires_at=?5, retry_count=retry_count+1, item_version=item_version+1, \
                     updated_at=?6, last_command_sequence=?7 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string(), hash, exp, now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
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
                    "UPDATE pqueue_items SET lease_expires_at=?4, item_version=item_version+1, \
                     updated_at=?5, last_command_sequence=?6 \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string(), exp, now_n, seq as i64],
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
                    params![t, q, id.to_string(), hash, exp, now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
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
                            params![t, q, o.item_id.to_string()],
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
                        o.item_id.to_string(),
                        state_str(new_state),
                        reset_attempts,
                        terminal_at,
                        now_n,
                        seq as i64,
                    ],
                ))?;
                token_ops.push(TokenOp::Clear(o.item_id));
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id).collect();
            for g in groups_of(tx, shard, &ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            // Supersede the old pending item (drops it from the active partial-unique index + eligibility),
            // then insert the replacement under the same client_item_key.
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![t, q, c.superseded_item_id.to_string(), now_n, seq as i64],
            ))?;
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_item(tx, &model, shard, &c.replacement, seq, now)?;
            // Refresh both the superseded item's group and the replacement's (often the same).
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
                     lease_expires_at=NULL, item_version=item_version+1, updated_at=?4, \
                     last_command_sequence=?5 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string(), now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Clear(*id));
            }
            for g in groups_of(tx, shard, &c.item_ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
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
                    params![t, q, id.to_string(), now_n, seq as i64],
                ))?;
                token_ops.push(TokenOp::Clear(*id));
            }
            // The whole cohort (group) is now terminal — refresh its summary to empty.
            refresh_group_summary(tx, shard, &c.group_key, now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            for id in &c.item_ids {
                // Operator fence: no item_version bump (parity with the in-memory arm).
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=1 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string()],
                ))?;
            }
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            for id in &c.item_ids {
                st(tx.execute(
                    "UPDATE pqueue_items SET fenced=0 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string()],
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
            let retention_ms = queues
                .get(shard)
                .map(|d| d.client_item_key_retention_ms)
                .unwrap_or(0);
            let mut groups: Vec<GroupKey> = Vec::new();
            for id in &c.item_ids {
                // Read group + key + state BEFORE the delete (for summary refresh + terminal-key retention).
                let row: Option<(Option<String>, String, String)> = st(tx
                    .query_row(
                        "SELECT group_key, client_item_key, lifecycle_state FROM pqueue_items \
                         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        params![t, q, id.to_string()],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional())?;
                if let Some((gk, ck, state)) = row {
                    // TD-002 retention tombstone: purging a TERMINAL item keeps its client_item_key a
                    // duplicate (re-push rejected) until `client_item_key_retention_ms` elapses. A pending
                    // purge records nothing (its key is freely reusable, matching the log-replay family).
                    if parse_state(&state)?.is_terminal() && retention_ms > 0 {
                        let expires =
                            now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000));
                        st(tx.execute(
                            "INSERT INTO pqueue_item_key_retention \
                             (tenant_id,queue_id,client_item_key,item_id,expires_at) \
                             VALUES (?1,?2,?3,?4,?5) ON CONFLICT(tenant_id,queue_id,client_item_key) \
                             DO UPDATE SET item_id=excluded.item_id, expires_at=excluded.expires_at",
                            params![t, q, ck, id.to_string(), expires],
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
                    "DELETE FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string()],
                ))?;
                // BQ-14d: drop the purged item's gate membership (the anti-join source).
                st(tx.execute(
                    "DELETE FROM pqueue_item_gates WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, id.to_string()],
                ))?;
                token_ops.push(TokenOp::Clear(*id));
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::SetGates(c) => {
            // BQ-14d (TD-002 §gate): set/clear queue gate-key block state. A blocked gate key makes every
            // item carrying it ineligible (enforced by the eligibility anti-join). This is exact-on-read:
            // toggling a gate flips eligibility on the next claim with no per-item rewrite.
            if c.blocked {
                for gk in &c.gate_keys {
                    st(tx.execute(
                        "INSERT INTO pqueue_gate_state (tenant_id,queue_id,gate_key) VALUES (?1,?2,?3) \
                         ON CONFLICT(tenant_id,queue_id,gate_key) DO NOTHING",
                        params![t, q, gk.as_str()],
                    ))?;
                }
            } else {
                for gk in &c.gate_keys {
                    st(tx.execute(
                        "DELETE FROM pqueue_gate_state WHERE tenant_id=?1 AND queue_id=?2 AND gate_key=?3",
                        params![t, q, gk.as_str()],
                    ))?;
                }
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
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
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

// ---------------------------------------------------------------------------
// BQ-14b: group-aware claim selection (group_batching / same_group_key), owner-local, consuming
// `pqueue_group_summary`. The queue has one owner, so every group is owner-local (ADR-008); the sqlite
// relational backend serializes the whole claim under `Mutex<Inner>`, so two claims cannot split a group
// (the postgres backend takes a real `FOR UPDATE SKIP LOCKED` group-summary lock for the same guarantee).
// ---------------------------------------------------------------------------

/// Candidate groups for the queue, ordered by each group's representative claim key (TD-002 g1:
/// `rep_progress_guard_sort` NULL today → `rep_priority_sort, rep_created_at, rep_item_id`). Only groups
/// with a current representative (`oldest_eligible_at IS NOT NULL`) are candidates; the live eligibility is
/// re-read per group at claim time (the summary is the ordering hint; the items are the authority).
/// KNOWN LIMITATION (tracked, pqueue-64351bdd): `oldest_eligible_at` is the BQ-11c mutation-time value
/// (lagged across a `not_before` crossing), so a group made eligible ONLY by time passing — with no
/// subsequent mutation — is not discovered by a group claim until its next mutation. Item-level claims read
/// `pqueue_items` live and are unaffected.
fn candidate_groups(conn: &Connection, shard: &QueueKey) -> EngineResult<Vec<GroupKey>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT group_key FROM pqueue_group_summary \
         WHERE tenant_id=?1 AND queue_id=?2 AND oldest_eligible_at IS NOT NULL \
         ORDER BY rep_priority_sort, rep_created_at, rep_item_id",
    ))?;
    let mapped = st(stmt.query_map(params![t, q], |row| row.get::<_, String>(0)))?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(GroupKey::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// The live currently-eligible items of one group (pending, not superseded, due at `now`), in claim order,
/// capped at `limit`.
fn group_eligible_items(
    conn: &Connection,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 \
         AND (not_before IS NULL OR not_before<=?4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT ?5",
    ))?;
    let mapped = st(stmt.query_map(
        params![t, q, group.as_str(), ts_nanos(now), limit as i64],
        |row| row.get::<_, String>(0),
    ))?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// `group_batching` selection (API-001 whole-eligible-group, `max_groups=N`): accumulate the oldest-N
/// candidate groups' WHOLE eligible sets, in rep order, stopping when adding the next group would exceed
/// `max_items`. A group is fetched with one extra item (`max_items+1`) so an oversized group is detected:
/// a single group that alone exceeds `max_items` cannot be delivered whole → `BatchTooLarge` (TD-002:711;
/// `max_eligible_group_size` is only a config knob, NOT a hard cap on actual group size, so this guard is
/// load-bearing). Empty groups (no live-eligible item) are skipped. Paused → empty.
fn select_group_batching(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    max_groups: u32,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new()); // a paused queue claims nothing (parity with item-level select_eligible)
    }
    let mut acc = Vec::new();
    let mut used = 0u32;
    for group in candidate_groups(conn, shard)? {
        if used >= max_groups {
            break;
        }
        // Fetch max_items+1 to distinguish "group of exactly max_items" from "group larger than max_items".
        let elig = group_eligible_items(conn, shard, &group, now, max_items + 1)?;
        if elig.is_empty() {
            continue; // discard a group with no live-eligible item
        }
        if elig.len() > max_items {
            // This single whole group alone exceeds the batch ceiling — a whole-group claim cannot deliver
            // it. Roll back, lease nothing (TD-002 batch-too-large).
            return Err(EngineError::BatchTooLarge);
        }
        if acc.len() + elig.len() > max_items {
            break; // adding this whole group would exceed the ceiling — stop, keep the whole groups that fit
        }
        acc.extend(elig);
        used += 1;
    }
    Ok(acc)
}

/// `same_group_key` selection (API-001): the server picks the single oldest eligible group and leases its
/// eligible items capped at `max_items` (a partial group is allowed — no batch-too-large). Paused → empty.
fn select_same_group(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    for group in candidate_groups(conn, shard)? {
        let elig = group_eligible_items(conn, shard, &group, now, max_items)?;
        if !elig.is_empty() {
            return Ok(elig);
        }
    }
    Ok(Vec::new())
}

/// `whole_cohort` selection (API-001 G6, all-or-nothing): the oldest COMPLETE cohort whose members are ALL
/// currently eligible. A cohort (group_key with a declared `cohort_size`) is complete when its live
/// non-superseded member count equals `cohort_size`; it is claimable only when every member is also
/// pending+due (no member leased/terminal). The whole cohort leases together, or the cohort is skipped.
/// `BatchTooLarge` if the selected cohort exceeds `max_items`. Paused → empty.
fn select_whole_cohort(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
) -> EngineResult<Vec<ItemId>> {
    if queue_paused(conn, shard)? {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    // Declared cohorts, oldest-first.
    let cohorts: Vec<(String, i64)> = {
        let mut stmt = st(conn.prepare(
            "SELECT group_key, cohort_size FROM pqueue_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 ORDER BY created_at, group_key",
        ))?;
        let rows = st(stmt.query_map(params![t, q], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        }))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(st(r)?);
        }
        out
    };
    for (gk, size) in cohorts {
        let size = size as usize;
        let group = GroupKey::new(gk).map_err(|e| EngineError::Storage(e.to_string()))?;
        // COHORT members are the items that DECLARED cohort membership (`cohort_size IS NOT NULL`), NOT all
        // items sharing the group_key — so a plain (non-cohort) push to the same key neither inflates the
        // count nor strands the cohort (fresh-eyes F1). Live non-superseded member count (any state).
        let members: i64 = st(conn.query_row(
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND group_key=?3 AND superseded=0 AND cohort_size IS NOT NULL",
            params![t, q, group.as_str()],
            |row| row.get(0),
        ))?;
        if members as usize != size {
            continue; // incomplete cohort (not all declared members present)
        }
        // All members must be currently eligible (pending+due). Fetch size+1 to detect any extra.
        let elig = cohort_eligible_items(conn, shard, &group, now, size + 1)?;
        if elig.len() != size {
            continue; // some member is leased / terminal / not-due — the cohort is not claimable now
        }
        if size > max_items {
            return Err(EngineError::BatchTooLarge); // the selected complete cohort exceeds the ceiling
        }
        return Ok(elig); // lease the whole cohort
    }
    Ok(Vec::new())
}

/// The live currently-eligible COHORT members of one group (`cohort_size IS NOT NULL`), in claim order,
/// capped at `limit`. Like [`group_eligible_items`] but restricted to cohort-declared members (F1).
fn cohort_eligible_items(
    conn: &Connection,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NOT NULL \
         AND (not_before IS NULL OR not_before<=?4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT ?5",
    ))?;
    let mapped = st(stmt.query_map(
        params![t, q, group.as_str(), ts_nanos(now), limit as i64],
        |row| row.get::<_, String>(0),
    ))?;
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

/// BQ-14e active-scope discovery: roll up `pqueue_group_summary` into ranked [`ActiveScope`]s. Each group
/// that currently holds eligible work (`oldest_eligible_at IS NOT NULL`) becomes one source scope, ordered
/// owner-local oldest-first (smallest `oldest_eligible_at` = most-aged group, group-key tiebreak for
/// determinism); `eligible_item_count` carries through as the eligible signal. [`project_scopes`] then
/// collapses to the requested granularity (Group = per-group detail in the oldest-first order; Queue = a
/// single rollup row for the queue — see [`project_scopes`] arithmetic).
///
/// `progress_bound_risk_count` is reported as `None` ("no signal"), NOT `Some(0)`: the summary's
/// `at_risk_count` is a hardcoded `0` placeholder while the progress-guard/at-risk derivation is deferred
/// (see `refresh_group_summary`), and the [`ActiveScope`] contract reserves `None` for an uncomputed
/// signal vs `Some(0)` for a measured zero. When at-risk becomes live, map it to `Some` here.
///
/// PAUSE (intentional divergence from the claim path): discovery reports a group's INTRINSIC eligibility
/// and does NOT short-circuit on `queue_paused` (unlike `select_eligible_sql`/group selection). An operator
/// hunting starvation wants to see work that has built up *because* a queue is paused; the summary itself
/// is pause-agnostic, so discovery mirrors it. (A read of a queue that does not exist yields an empty list,
/// not `NotFound` — a discovery read of an unknown queue simply has no active scopes.)
///
/// KNOWN LIMITATION (shared with the group-claim path, tracked pqueue-64351bdd): `oldest_eligible_at` is
/// the BQ-11c mutation-time value (lagged across a pure `not_before` crossing). A group made eligible ONLY
/// by time passing — with no subsequent mutation — keeps `oldest_eligible_at = NULL` and is NOT discovered
/// until its next mutation or a due-sweep refresh. So discovery can UNDER-report time-triggered starvation;
/// it never over-reports (an item present in the summary as eligible truly was at its last mutation).
fn discover_active_scopes_sql(
    conn: &Connection,
    shard: &QueueKey,
    granularity: DiscoveryGranularity,
    now: UtcTimestamp,
) -> EngineResult<Vec<ActiveScope>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT group_key, oldest_eligible_at, eligible_item_count \
         FROM pqueue_group_summary \
         WHERE tenant_id=?1 AND queue_id=?2 AND oldest_eligible_at IS NOT NULL \
         ORDER BY oldest_eligible_at ASC, group_key ASC",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    }))?;
    let mut source = Vec::new();
    for r in rows {
        let (group_key, oldest_eligible_at, eligible) = st(r)?;
        // Age from `now`; a summary timestamp in the future (clock skew) clamps to 0, never underflows.
        let age_ms = now_n.saturating_sub(oldest_eligible_at).max(0) as u64 / 1_000_000;
        source.push(ActiveScope {
            queue_id: q.clone(),
            group_key: Some(group_key),
            oldest_eligible_age_ms: age_ms,
            eligible_count: Some(eligible as u64),
            // Deferred at-risk derivation → no signal (not a measured zero). See the doc above.
            progress_bound_risk_count: None,
        });
    }
    Ok(project_scopes(source, granularity))
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
                 lease_expires_at, retry_count, payload, fields FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 AND lifecycle_state='Leased'",
                params![t, q, id.to_string()],
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
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional())?;
        let Some((key, version, priority, group, not_before, exp, retry, payload, fields)) = row
        else {
            continue;
        };
        let Some(exp) = exp else { continue };
        out.push(ClaimedItem {
            item_id: *id,
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
            fields: fields_from_json(fields)?,
        });
    }
    Ok(out)
}

fn live_items_sql(
    conn: &Connection,
    shard: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<Vec<Option<LiveItemView>>> {
    let (t, q) = parts(shard);
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let row = st(conn
            .query_row(
                "SELECT item_id, item_version, lifecycle_state, priority, group_key, not_before, \
                 retry_count, payload, fields FROM pqueue_items \
                 WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 \
                   AND superseded=0 AND lifecycle_state IN ('Pending','Leased')",
                params![t, q, key.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<Vec<u8>>>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional())?;
        out.push(match row {
            Some((id, version, state, priority, group, not_before, retry, payload, fields)) => {
                Some(LiveItemView {
                    item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
                    client_item_key: key.clone(),
                    item_version: version as u64,
                    lifecycle_state: parse_state(&state)?,
                    priority: parse_priority(priority)?,
                    group_key: group
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    not_before: not_before.map(nanos_ts),
                    attempt_count: retry as u32,
                    payload: payload.map(Bytes::from),
                    fields: fields_from_json(fields)?,
                })
            }
            None => None,
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
            params![t, q, id.to_string()],
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
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009). `0` single-instance.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see `QueueCounters`.
    counters: QueueCounters,
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

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`]
    /// so distinct nodes competing for one queue never mint a colliding id (ADR-009).
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        st(conn.execute_batch(RELATIONAL_SCHEMA))?;
        ensure_item_fields_column(&conn)?;
        let mut inner = Inner {
            conn,
            queues: HashMap::new(),
            live_tokens: HashMap::new(),
        };
        inner.reload()?;
        let backend = Self {
            inner: Mutex::new(inner),
            node_id: 0,
            counters: QueueCounters::default(),
        };
        backend.restore_counters()?;
        Ok(backend)
    }

    /// Restart recovery: seed the per-queue mint counter past every id already in `pqueue_items`, so a push
    /// after reopen never re-mints an existing item id (the durable items table is the authority — there is
    /// no log to replay). `observe` decodes `(epoch, counter)` from each packed id and only advances.
    fn restore_counters(&self) -> EngineResult<()> {
        let g = self.inner.lock().expect("poisoned");
        let mut stmt =
            st(g.conn.prepare("SELECT tenant_id, queue_id, item_id FROM pqueue_items"))?;
        let rows = st(stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        }))?;
        for r in rows {
            let (t, q, id) = st(r)?;
            let key = QueueKey::new(
                TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
            );
            let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
            self.counters.observe(&key, item_id);
        }
        Ok(())
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
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        let (mut next, epoch): (i64, i64) = st(self
            .tx
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for _ in commands {
            positions.push(CommandPosition::new(
                shard.clone(),
                epoch as u64,
                next as u64,
            ));
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
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let (t, q) = parts(shard);
        let result = {
            let g = self.inner.lock().expect("poisoned");
            st(g.conn
                .query_row(
                    "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get::<_, i64>(0),
                )
                .optional())
            .and_then(|opt| opt.ok_or(EngineError::NotFound).map(|e| e as u64))
        };
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let (t, q) = parts(shard);
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
            let new_epoch: Option<i64> = st(g
                .conn
                .query_row(
                    "UPDATE relational_cursor SET assignment_epoch = assignment_epoch + 1 \
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

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            live_items_sql(&g.conn, shard, keys)
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

impl DiscoveryPort for SqliteRelationalBackend {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ActiveScope>>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            discover_active_scopes_sql(&g.conn, shard, granularity, now)
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
        // Fence threading for this backend family is deferred (B1b continuation); accepted for the port
        // contract so the owner fence is uniform once the relational/object write paths thread it.
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            g.commit_command(
                shard,
                QueueCommand::Push(PushCommand { items: push_items }),
                now, expected_epoch
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
            // BQ-14a/b: resolve the claim unit from the compatibility options. Item-level (the default) is
            // byte-identical; WholeGroup / SameGroupKey select group-aware from `pqueue_group_summary`;
            // WholeCohort is gated to `Unavailable` until BQ-14c. An invalid combo propagates the
            // structured validation error.
            let unit = if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                validate_claim_compatibility(&req.compatibility, req.max_items as u64, def)?
            } else {
                ClaimUnit::Item
            };
            let Inner {
                conn,
                queues,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(&req.shard);
            let tx = st(conn.transaction())?;
            // ADR-009 / TD-003 fence: a superseded owner (cached `expected_epoch` != the durable
            // assignment_epoch) is rejected BEFORE selecting/leasing — nothing is claimed. `None` = sole-owner.
            let claim_epoch: i64 = st(tx
                .query_row(
                    "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| row.get(0),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if req.expected_epoch.is_some_and(|e| e != claim_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            // Candidate selection inside the claim transaction (serialized under the backend Mutex). The
            // item-level path is the strict-claim order; the group/cohort paths consume their projections.
            let candidates = match unit {
                ClaimUnit::Item => select_eligible_sql(&tx, &req.shard, req.now, req.max_items)?,
                ClaimUnit::WholeGroup => {
                    let max_groups = req
                        .compatibility
                        .group_batching
                        .as_ref()
                        .map(|gb| gb.max_groups)
                        .unwrap_or(0);
                    select_group_batching(&tx, &req.shard, req.now, req.max_items, max_groups)?
                }
                ClaimUnit::SameGroupKey => {
                    select_same_group(&tx, &req.shard, req.now, req.max_items)?
                }
                ClaimUnit::WholeCohort => {
                    select_whole_cohort(&tx, &req.shard, req.now, req.max_items)?
                }
            };
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
    /// Insert / replace-pending / reject-claimed / reject-terminal. BQ-11c adds the `client_item_key`
    /// retention tombstone: when no active item exists but a non-expired retention record does (a TERMINAL
    /// item under this key was purged within `client_item_key_retention_ms`), the re-push is still rejected
    /// as a duplicate (`Terminal`) rather than resurrecting the work — duplicate-push convergence across a
    /// purge (TD-002 §Idempotency). Data-plane request-id replay is a separate concern (no port carries a
    /// `request_id` yet — see the module note).
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
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
                cohort_size: None,
                gate_keys: Vec::new(),
            };
            match existing {
                None => {
                    // No active item — but a non-expired retention tombstone (a terminal item under this
                    // key was purged within retention) keeps the re-push a duplicate (TD-002).
                    let retained: Option<i64> = st(g
                        .conn
                        .query_row(
                            "SELECT expires_at FROM pqueue_item_key_retention \
                             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                            params![t, q, client_item_key.as_str()],
                            |row| row.get(0),
                        )
                        .optional())?;
                    if let Some(expires) = retained {
                        if expires > ts_nanos(now) {
                            return Err(EngineError::Terminal);
                        }
                        // Expired: the key is reusable again — clear the stale tombstone, then insert.
                        st(g.conn.execute(
                            "DELETE FROM pqueue_item_key_retention \
                             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3",
                            params![t, q, client_item_key.as_str()],
                        ))?;
                    }
                    g.commit_command(
                        shard,
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        now, expected_epoch
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
                                    superseded_item_id: existing_id,
                                    replacement: item,
                                }),
                                now, expected_epoch
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
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            validate_leased(&g.conn, shard, &ids)?;
            g.commit_command(
                shard,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                now, expected_epoch
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
        expected_epoch: Option<u64>,
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
                now, expected_epoch
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
        expected_epoch: Option<u64>,
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
                now, expected_epoch
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
        expected_epoch: Option<u64>,
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
                    present.push(*id);
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
                now, expected_epoch
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
                    now, None
                )?;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

#[cfg(test)]
mod group_summary_tests {
    //! White-box tests for `pqueue_group_summary` maintenance — they read the summary table directly
    //! (it has no read port yet; BQ-14 consumes it), driving state through the public ports.
    use super::*;
    use pqueue_core::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModelKind, PriorityTieBreaker,
        QueueId, RecurrencePolicy, RetryPolicy, TenantId, WorkerId,
    };
    use pqueue_engine::{ClaimRequest, CommandChecksum, CommandId};

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
    fn grouped(priority: i64, group: &str) -> PushSpec {
        PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new(group).unwrap()),
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
            expected_epoch: None,
        }
    }

    /// (oldest_eligible_at, eligible_item_count, rep_item_id) for the group, or None if no row exists.
    fn summary(
        b: &SqliteRelationalBackend,
        group: &str,
    ) -> Option<(Option<i64>, i64, Option<String>)> {
        let g = b.inner.lock().unwrap();
        g.conn
            .query_row(
                "SELECT oldest_eligible_at, eligible_item_count, rep_item_id \
                 FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' AND group_key=?1",
                params![group],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .unwrap()
    }

    #[tokio::test]
    async fn group_summary_tracks_eligibility_through_the_lifecycle() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();

        // Push two grouped items (priorities 10, 20) — rep is the priority-10 item, count 2.
        let ids = b
            .push(&shard(), vec![grouped(10, "g"), grouped(20, "g")], ts(0), None)
            .await
            .unwrap();
        let (oldest, count, rep) = summary(&b, "g").expect("summary row created on grouped push");
        assert_eq!(count, 2);
        assert!(
            oldest.is_some(),
            "oldest_eligible_at set while items eligible"
        );
        assert_eq!(
            rep,
            Some(ids[0].to_string()),
            "rep is the first-claimable item"
        );

        // Claim the rep (priority 10) — it leaves eligibility; count 1, rep advances to the priority-20 item.
        b.claim(claim_req(1, 500, 10)).await.unwrap();
        let (_, count, rep) = summary(&b, "g").unwrap();
        assert_eq!(count, 1, "leased item leaves the eligible count");
        assert_eq!(
            rep,
            Some(ids[1].to_string()),
            "rep advances to the next eligible item"
        );

        // Purge the remaining pending grouped item — group drains to empty.
        b.purge(&shard(), vec![ids[1].clone()], false, ts(20), None)
            .await
            .unwrap();
        let (oldest, count, rep) = summary(&b, "g").unwrap();
        assert_eq!(count, 0, "empty group has zero eligible");
        assert!(
            oldest.is_none() && rep.is_none(),
            "no representative when empty"
        );
    }

    #[tokio::test]
    async fn lease_expiry_returns_item_to_the_group_summary() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(&shard(), vec![grouped(5, "g")], ts(0), None)
            .await
            .unwrap();
        b.claim(claim_req(1, 100, 10)).await.unwrap();
        assert_eq!(summary(&b, "g").unwrap().1, 0, "leased -> not eligible");

        // Reclaim the expired lease (tick) -> the item is pending again and back in the group's count.
        b.tick(ts(101)).await.unwrap();
        let (_, count, rep) = summary(&b, "g").unwrap();
        assert_eq!(count, 1, "reclaimed item is eligible again");
        assert_eq!(rep, Some(ids[0].to_string()));
    }

    #[tokio::test]
    async fn finalize_release_returns_item_to_the_group_summary() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(&shard(), vec![grouped(5, "g")], ts(0), None)
            .await
            .unwrap();
        b.claim(claim_req(1, 500, 10)).await.unwrap();
        assert_eq!(summary(&b, "g").unwrap().1, 0, "leased -> not eligible");

        // Release (no-fault give-back) returns the item to pending -> back in the group's eligible count.
        b.finalize(
            &shard(),
            vec![FinalizeOutcome {
                item_id: ids[0].clone(),
                kind: FinalizeKind::Release,
            }],
            ts(20), None
        )
        .await
        .unwrap();
        let (_, count, rep) = summary(&b, "g").unwrap();
        assert_eq!(count, 1, "released item is eligible again");
        assert_eq!(rep, Some(ids[0].to_string()));
    }

    #[tokio::test]
    async fn cohort_expired_drains_the_group_summary() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        b.push(&shard(), vec![grouped(5, "g"), grouped(6, "g")], ts(0), None)
            .await
            .unwrap();
        assert_eq!(summary(&b, "g").unwrap().1, 2);

        // Force the whole cohort terminal -> the group's eligible summary drains to empty.
        commit_cohort_expired(&b, "g", ts(20)).await;
        let (oldest, count, rep) = summary(&b, "g").unwrap();
        assert_eq!(
            count, 0,
            "cohort-expired members are terminal -> not eligible"
        );
        assert!(oldest.is_none() && rep.is_none());
    }

    #[tokio::test]
    async fn pending_purge_records_no_retention_tombstone() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let key = ClientItemKey::new("pk").unwrap();
        let id = match b
            .replace_if_pending(
                &shard(),
                &key,
                Some(PriorityValue::Int64(5)),
                None,
                None,
                None,
                BTreeMap::new(),
                ts(0), None
            )
            .await
            .unwrap()
        {
            UpsertOutcome::Inserted { item_id } => item_id,
            _ => panic!("insert"),
        };
        // Purge a PENDING item (not terminal) -> no retention tombstone, so the key is freely reusable.
        b.purge(&shard(), vec![id], false, ts(1), None).await.unwrap();
        assert!(
            matches!(
                b.replace_if_pending(
                    &shard(),
                    &key,
                    None,
                    None,
                    None,
                    None,
                    BTreeMap::new(),
                    ts(2),
                None)
                .await
                .unwrap(),
                UpsertOutcome::Inserted { .. }
            ),
            "a pending purge leaves no tombstone (parity with the log-replay family)"
        );
    }

    /// Apply a `CohortExpired` command through the write UoW (no dedicated port).
    async fn commit_cohort_expired(b: &SqliteRelationalBackend, group: &str, now: UtcTimestamp) {
        let env = CommandEnvelope {
            command_id: CommandId::new("ce"),
            request_id: None,
            item_ids: vec![],
            command: QueueCommand::CohortExpired(pqueue_engine::CohortExpiredCommand {
                group_key: GroupKey::new(group).unwrap(),
            }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        let epoch = b.current_epoch(&shard()).await.unwrap();
        b.write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env), epoch)?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .unwrap();
    }

    /// BQ-11d: `pqueue_group_summary` is durable — it survives a reopen with the recovered representative,
    /// because it is a DB table maintained in-transaction, not in-process state.
    #[tokio::test]
    async fn group_summary_survives_reopen() {
        let path = std::env::temp_dir()
            .join(format!("pqueue-rel-gs-reopen-{}.db", std::process::id()))
            .to_str()
            .unwrap()
            .to_string();
        let _ = std::fs::remove_file(&path);

        let rep_before;
        {
            let a = SqliteRelationalBackend::open(&path).unwrap();
            a.create_queue(qdef()).await.unwrap();
            let ids = a
                .push(&shard(), vec![grouped(10, "g"), grouped(20, "g")], ts(0), None)
                .await
                .unwrap();
            let (_, count, rep) = summary(&a, "g").unwrap();
            assert_eq!(count, 2);
            assert_eq!(rep, Some(ids[0].to_string()));
            rep_before = rep;
        } // crash

        let b = SqliteRelationalBackend::open(&path).unwrap();
        let (_, count, rep) = summary(&b, "g").expect("group_summary row survives reopen");
        assert_eq!(
            count, 2,
            "eligible count recovered from the durable summary"
        );
        assert_eq!(rep, rep_before, "representative recovered unchanged");
        let _ = std::fs::remove_file(&path);
    }
}
