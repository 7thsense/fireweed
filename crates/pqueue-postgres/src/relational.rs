//! # Relational projection family (postgres) — BQ-12
//!
//! The postgres sibling of [`pqueue_sqlite::SqliteRelationalBackend`]: a rebuildable relational
//! projection family (ADR-008 / TD-001 relational class) where the `pqueue_items` SQL table holds the
//! durable projection cache. Every lifecycle command is applied as SQL against `pqueue_items`; reads are
//! SQL; a reconnect recovers committed state from the table itself (no command log to replay). The
//! schema + the 14-arm apply mirror the sqlite relational reference arm-for-arm, so the two relational
//! backends - and the in-memory reference - stay behaviorally identical on the conformance CORE class.
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
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axon_esf::CompiledSchema;
use bytes::Bytes;
use postgres::error::SqlState;
use postgres::types::ToSql;
use postgres::{Client, GenericClient};
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken,
    Metadata, PriorityModel, PriorityValue, QueueDefinition, QueueId, QueueIndex, RequestId,
    TenantId, UtcTimestamp, is_retry_exhausted, priority_sort,
};
use pqueue_engine::{
    ActiveScope, AdvanceInstanceFenceCommand, Backend, ClaimCommand, ClaimCompatibility, ClaimPort,
    ClaimRequest, ClaimUnit, Claimed, ClaimedItem, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortFinalizePort, CohortLeaseTarget, CohortRenewLeaseCommand,
    CohortRenewLeasePort, CommandEnvelope, CommandPosition, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome, DiscoveryGranularity,
    DiscoveryPort, DurabilityClass, EngineError, EngineResult, EntryRecovery, FinalizeCommand,
    FinalizeKind, FinalizeOutcome, FinalizePort, HistoricalProjectionRead, IndexHit,
    IndexQueryPort, ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, LogWriter,
    PayloadUpdate, ProjectionRead, ProjectionWriter, PurgeItemsCommand, PurgePort, PushCommand,
    PushItem, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort,
    RenewLeaseCommand, RenewLeasePort, ReplacePendingCommand, SetGatesCommand, SetGatesPort,
    TerminalEmissionMetrics, TickReport, UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome,
    UpsertPort, WriteSideRecordsCommand, build_push_items, compile_entity_schema, project_scopes,
    validate_claim_compatibility, validate_entity, validate_gate_push, validate_instance_fence,
    validate_purge_force,
};
use pqueue_engine::{
    AsOfProjectionStore, CommandPage, ComposedBackend, InProcessControlPlane, LogStore,
    ProjectionSnapshot, ProjectionStore, SnapshotRef,
};
use sha2::{Digest, Sha256};

use crate::{PostgresConnectConfig, connect};

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
    fields TEXT NOT NULL DEFAULT '{}',
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
    assignment_epoch BIGINT NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS relational_emission_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    seq BIGINT NOT NULL,
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
-- TD-002 §cohort lifecycle projection.
CREATE TABLE IF NOT EXISTS pqueue_cohorts (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, group_key TEXT NOT NULL,
    cohort_id TEXT NOT NULL,
    cohort_size BIGINT NOT NULL,
    member_count BIGINT NOT NULL,
    state TEXT NOT NULL,
    cohort_created_at BIGINT NOT NULL,
    first_eligible_at BIGINT,
    expire_command_pos BIGINT,
    cohort_lease_token_hash BYTEA,
    retention_until BIGINT,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, group_key)
);
CREATE INDEX IF NOT EXISTS pqueue_cohorts_claim_idx
    ON pqueue_cohorts (tenant_id, queue_id, state)
    WHERE state='complete';
CREATE INDEX IF NOT EXISTS pqueue_cohorts_expiry_idx
    ON pqueue_cohorts (tenant_id, queue_id, cohort_created_at)
    WHERE state IN ('forming','complete');
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
CREATE TABLE IF NOT EXISTS pqueue_request_idempotency (
    tenant_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_fingerprint BYTEA NOT NULL,
    response_payload TEXT NOT NULL,
    expires_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, operation, request_id)
);
CREATE INDEX IF NOT EXISTS pqueue_request_idempotency_expiry_idx
    ON pqueue_request_idempotency (expires_at);
-- ADR-011 (pqueue-f4ffd679): typed secondary index rows. PK is (tenant, queue, index_name, item_id)
-- because each item has at most one canonical key per named index. Unique typed indexes are also protected
-- by a partial unique index over `(tenant, queue, index_name, index_key) WHERE is_unique`, so cross-instance
-- writers cannot race past the application-level pre-check. Rows are inserted on Push/ReplacePending/
-- UpdateFields and deleted only on PurgeItems — terminal items keep their index rows so they are still
-- findable (parity with in-memory projection).
CREATE TABLE IF NOT EXISTS pqueue_item_index (
    tenant_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_key BYTEA NOT NULL,
    item_id TEXT NOT NULL,
    is_unique BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id, queue_id, index_name, item_id)
);
CREATE INDEX IF NOT EXISTS pqueue_item_index_key_idx
    ON pqueue_item_index (tenant_id, queue_id, index_name, index_key);
CREATE UNIQUE INDEX IF NOT EXISTS pqueue_item_index_unique_key_idx
    ON pqueue_item_index (tenant_id, queue_id, index_name, index_key)
    WHERE is_unique = true;
-- C9 (epic pqueue-2201fd37): opaque NON-WORK side records written by the authoritative vectorized
-- claimed-work commit (Snorri StateStore boundary). Deliberately SEPARATE from `pqueue_items`: a side
-- record carries no lifecycle/lease/priority/eligibility, so it is never claimable, eligible, peekable, or
-- counted as work. `key`/`payload` are opaque bytes pqueue stores verbatim; the apply arm upserts by key.
-- Mirrors `pqueue-sqlite`'s `pqueue_side_records` (`crates/pqueue-sqlite/src/relational.rs:234-237`).
CREATE TABLE IF NOT EXISTS pqueue_side_records (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, key BYTEA NOT NULL, payload BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, key)
);
-- C6 (epic pqueue-2201fd37): caller-supplied opaque instance/state fences advanced by the authoritative
-- vectorized claimed-work commit (Snorri StateStore boundary). SEPARATE from `pqueue_items`: a fence carries
-- no lifecycle/lease and is never claimable/eligible/peekable. `instance_key` is opaque bytes; an absent key
-- reads as fence 0 (the unset convention). The commit upserts the row to `next` only after validation.
-- Mirrors `pqueue-sqlite`'s `pqueue_instance_fences` (`crates/pqueue-sqlite/src/relational.rs:242-245`).
CREATE TABLE IF NOT EXISTS pqueue_instance_fences (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, instance_key BYTEA NOT NULL, fence BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, instance_key)
);
"#;

const COHORT_EXPIRY_SWEEP_LIMIT: i64 = 128;
const IDEMPOTENCY_OPERATION_PUSH: &str = "push";

/// The serialized claim CTE (TD-002 `BatchClaim`): select the eligible candidates under a real
/// `FOR UPDATE SKIP LOCKED` row lock and lease them in ONE statement, RETURNING the rich claimed rows.
/// Concurrent claimers lock disjoint candidate sets — no process Mutex, no select-then-lease TOCTOU.
/// Authored as a constant so its shape is unit-asserted without a live DB (`sql_shape_tests`).
pub(crate) const CLAIM_CTE: &str = "\
WITH candidates AS ( \
    SELECT item_id FROM pqueue_items \
    WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Pending' AND superseded=false AND cohort_size IS NULL \
      AND (not_before IS NULL OR not_before<=$3) AND eligible_since IS NOT NULL \
      AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
          ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
          WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
          AND ig.item_id=pqueue_items.item_id) \
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
		          i.lease_expires_at, i.retry_count, i.payload, i.fields, i.metadata";

pub(crate) const ITEM_GATE_KEYS_BATCH_SQL: &str = "\
SELECT item_id, gate_key FROM pqueue_item_gates \
WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3) \
ORDER BY item_id, gate_key";

// ---------------------------------------------------------------------------
// small conversions / error mapping
// ---------------------------------------------------------------------------

fn st<T>(r: Result<T, postgres::Error>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

fn push_request_fingerprint(items: &[PushSpec]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

fn request_expires_at(
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<i64> {
    let retention_ms = queues
        .get(shard)
        .map(|d| d.request_id_retention_ms)
        .ok_or(EngineError::NotFound)?;
    Ok(ts_nanos(now).saturating_add((retention_ms as i64).saturating_mul(1_000_000)))
}

fn item_ids_to_json(ids: &[ItemId]) -> EngineResult<String> {
    let raw: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    to_json(&raw)
}

fn item_ids_from_json(raw: String) -> EngineResult<Vec<ItemId>> {
    let decoded: Vec<String> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    decoded
        .into_iter()
        .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
        .collect()
}

fn check_request_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<ItemId>>> {
    let (t, q) = parts(shard);
    let prior = st(tx.query_opt(
        "SELECT request_fingerprint, response_payload, expires_at \
         FROM pqueue_request_idempotency \
         WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
        &[&t, &q, &operation, &request_id.as_str()],
    ))?;
    let Some(row) = prior else {
        return Ok(None);
    };
    let prior_fingerprint: Vec<u8> = row.get(0);
    let response_payload: String = row.get(1);
    let expires_at: i64 = row.get(2);
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM pqueue_request_idempotency \
             WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
            &[&t, &q, &operation, &request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(item_ids_from_json(response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

#[allow(clippy::too_many_arguments)]
fn record_request_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    response_ids: &[ItemId],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let response_payload = item_ids_to_json(response_ids)?;
    st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,expires_at,created_at) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=EXCLUDED.request_fingerprint, \
          response_payload=EXCLUDED.response_payload, \
          expires_at=EXCLUDED.expires_at",
        &[
            &t,
            &q,
            &operation,
            &request_id.as_str(),
            &fingerprint,
            &response_payload,
            &expires_at,
            &ts_nanos(now),
        ],
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// C9: authoritative vectorized claimed-work commit — idempotency + validation helpers
// ---------------------------------------------------------------------------

const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";

fn commit_request_fingerprint(entries: &[CommitTransitionEntry]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredEntryRecovery {
    consumed_input_id: String,
    #[serde(default)]
    instance: Option<(Vec<u8>, u64)>,
    #[serde(default)]
    side_record_keys: Vec<Vec<u8>>,
    #[serde(default)]
    lifecycle_item_ids: Vec<String>,
    #[serde(default)]
    rejected: Option<(String, Option<String>)>,
}

fn encode_engine_error(e: &EngineError) -> (&'static str, Option<String>) {
    match e {
        EngineError::NotFound => ("not_found", None),
        EngineError::QueueDefinitionConflict => ("queue_definition_conflict", None),
        EngineError::Invalid(why) => ("invalid", Some((*why).to_string())),
        EngineError::Terminal => ("terminal", None),
        EngineError::StaleLease => ("stale_lease", None),
        EngineError::Superseded => ("superseded", None),
        EngineError::Unavailable => ("unavailable", None),
        EngineError::Conflict => ("conflict", None),
        EngineError::BatchTooLarge => ("batch_too_large", None),
        EngineError::RequestIdConflict => ("request_id_conflict", None),
        EngineError::RequestExpired => ("request_expired", None),
        EngineError::EpochFenced => ("epoch_fenced", None),
        EngineError::Paused { drain_intake } => (
            "paused",
            Some((if *drain_intake { "true" } else { "false" }).to_string()),
        ),
        EngineError::Forbidden(why) => ("forbidden", Some((*why).to_string())),
        EngineError::Storage(msg) => ("storage", Some(msg.clone())),
        EngineError::EntitySchemaViolation(msg) => ("entity_schema_violation", Some(msg.clone())),
    }
}

fn decode_engine_error(code: &str, detail: Option<String>) -> EngineError {
    match code {
        "not_found" => EngineError::NotFound,
        "queue_definition_conflict" => EngineError::QueueDefinitionConflict,
        "invalid" => EngineError::Invalid(match detail.as_deref() {
            Some("item is not leased") => "item is not leased",
            _ => "invalid",
        }),
        "terminal" => EngineError::Terminal,
        "stale_lease" => EngineError::StaleLease,
        "superseded" => EngineError::Superseded,
        "unavailable" => EngineError::Unavailable,
        "conflict" => EngineError::Conflict,
        "batch_too_large" => EngineError::BatchTooLarge,
        "request_id_conflict" => EngineError::RequestIdConflict,
        "request_expired" => EngineError::RequestExpired,
        "epoch_fenced" => EngineError::EpochFenced,
        "forbidden" => EngineError::Forbidden("forbidden"),
        _ => EngineError::Storage(detail.unwrap_or_else(|| code.to_string())),
    }
}

fn recovery_to_outcomes(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
    recovery
        .iter()
        .map(|r| match &r.status {
            CommitEntryStatus::Committed => CommitEntryOutcome::Committed {
                lifecycle_item_ids: r.lifecycle_item_ids.clone(),
            },
            CommitEntryStatus::Rejected(e) => CommitEntryOutcome::Rejected(e.clone()),
        })
        .collect()
}

fn encode_commit_recovery(recovery: &[EntryRecovery]) -> EngineResult<String> {
    let stored: Vec<StoredEntryRecovery> = recovery
        .iter()
        .map(|r| StoredEntryRecovery {
            consumed_input_id: r.consumed_input_id.to_string(),
            instance: r.instance.clone(),
            side_record_keys: r.side_record_keys.clone(),
            lifecycle_item_ids: r
                .lifecycle_item_ids
                .iter()
                .map(|id| id.to_string())
                .collect(),
            rejected: match &r.status {
                CommitEntryStatus::Committed => None,
                CommitEntryStatus::Rejected(e) => {
                    let (code, detail) = encode_engine_error(e);
                    Some((code.to_string(), detail))
                }
            },
        })
        .collect();
    to_json(&stored)
}

fn decode_commit_recovery(raw: &str) -> EngineResult<Vec<EntryRecovery>> {
    let stored: Vec<StoredEntryRecovery> =
        serde_json::from_str(raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    stored
        .into_iter()
        .map(|s| {
            let lifecycle_item_ids = s
                .lifecycle_item_ids
                .into_iter()
                .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
            let status = match s.rejected {
                None => CommitEntryStatus::Committed,
                Some((code, detail)) => {
                    CommitEntryStatus::Rejected(decode_engine_error(&code, detail))
                }
            };
            Ok(EntryRecovery {
                consumed_input_id: ItemId::new(s.consumed_input_id)
                    .map_err(|e| EngineError::Storage(e.to_string()))?,
                instance: s.instance,
                side_record_keys: s.side_record_keys,
                lifecycle_item_ids,
                status,
            })
        })
        .collect()
}

fn check_commit_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let prior = st(tx.query_opt(
        "SELECT request_fingerprint, response_payload, expires_at \
         FROM pqueue_request_idempotency \
         WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
        &[&t, &q, &IDEMPOTENCY_OPERATION_COMMIT, &request_id.as_str()],
    ))?;
    let Some(row) = prior else {
        return Ok(None);
    };
    let prior_fingerprint: Vec<u8> = row.get(0);
    let response_payload: String = row.get(1);
    let expires_at: i64 = row.get(2);
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM pqueue_request_idempotency \
             WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
            &[&t, &q, &IDEMPOTENCY_OPERATION_COMMIT, &request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(decode_commit_recovery(&response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

fn read_commit_recovery(
    client: &mut Client,
    shard: &QueueKey,
    request_id: &RequestId,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let payload: Option<String> = st(client.query_opt(
        "SELECT response_payload FROM pqueue_request_idempotency \
         WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
        &[&t, &q, &IDEMPOTENCY_OPERATION_COMMIT, &request_id.as_str()],
    ))?
    .map(|row| row.get(0));
    match payload {
        Some(raw) => Ok(Some(decode_commit_recovery(&raw)?)),
        None => Ok(None),
    }
}

fn record_commit_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    recovery: &[EntryRecovery],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,expires_at,created_at) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=EXCLUDED.request_fingerprint, \
          response_payload=EXCLUDED.response_payload, \
          expires_at=EXCLUDED.expires_at",
        &[
            &t,
            &q,
            &IDEMPOTENCY_OPERATION_COMMIT,
            &request_id.as_str(),
            &fingerprint,
            &encode_commit_recovery(recovery)?,
            &expires_at,
            &ts_nanos(now),
        ],
    ))?;
    Ok(())
}

fn commit_validate_sql(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    claim_ref: &pqueue_engine::ClaimRef,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row = st(tx.query_opt(
        "SELECT lifecycle_state, fenced, superseded, lease_token_hash, lease_expires_at, item_version \
         FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
        &[&t, &q, &claim_ref.item_id.to_string()],
    ))?;
    let Some(row) = row else {
        return Err(EngineError::NotFound);
    };
    let state: String = row.get(0);
    let fenced: bool = row.get(1);
    let superseded: bool = row.get(2);
    let lease_token_hash: Option<Vec<u8>> = row.get(3);
    let lease_expires_at: Option<i64> = row.get(4);
    let item_version: i64 = row.get(5);
    let state = parse_state(&state)?;
    if fenced {
        return Err(EngineError::StaleLease);
    }
    if state.is_terminal() {
        return Err(EngineError::Terminal);
    }
    if superseded {
        return Err(EngineError::Superseded);
    }
    if state != ItemState::Leased {
        return Err(EngineError::Invalid("item is not leased"));
    }
    if lease_token_hash.as_deref() != Some(lease_hash(&claim_ref.lease_token).as_slice()) {
        return Err(EngineError::StaleLease);
    }
    if lease_expires_at.is_some_and(|exp| exp < ts_nanos(now)) {
        return Err(EngineError::StaleLease);
    }
    if item_version as u64 != claim_ref.item_version {
        return Err(EngineError::Conflict);
    }
    Ok(())
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

fn metadata_to_json(metadata: &Metadata) -> EngineResult<String> {
    to_json(&metadata.clone().into_inner())
}

fn metadata_from_json(raw: String) -> EngineResult<Metadata> {
    let entries = serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Metadata::from_entries(entries))
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
// ADR-011 typed secondary index helpers (port of the sqlite relational helpers)
// ---------------------------------------------------------------------------

/// Decode a caller-supplied raw lookup byte slice into a `serde_json::Value` for re-encoding via
/// `IndexDef::index_key` / `CompoundIndexDef::index_key`. Mirrors `decode_typed_lookup_value_rel`
/// in `pqueue_sqlite::relational` — the two must stay identical so lookup keys byte-match stored keys.
fn decode_typed_lookup_value_rel(
    index_type: &IndexType,
    bytes: &[u8],
) -> EngineResult<serde_json::Value> {
    match index_type {
        IndexType::String => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(serde_json::Value::String(s.to_owned()))
        }
        IndexType::Datetime => {
            if let Ok(value @ serde_json::Value::Number(_)) =
                serde_json::from_slice::<serde_json::Value>(bytes)
            {
                return Ok(value);
            }
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(serde_json::Value::String(s.to_owned()))
        }
        IndexType::Integer | IndexType::Float => serde_json::from_slice::<serde_json::Value>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number")),
        IndexType::Boolean => serde_json::from_slice::<serde_json::Value>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON boolean")),
    }
}

/// Compute the canonical `index_key` bytes for a lookup against a named index.
fn typed_lookup_canonical_key(qi: &QueueIndex, key_values: &[Vec<u8>]) -> EngineResult<Vec<u8>> {
    match &qi.declaration {
        IndexDeclaration::Single(def) => {
            let val = decode_typed_lookup_value_rel(&def.index_type, &key_values[0])?;
            let mut record = serde_json::Map::new();
            record.insert(def.field.clone(), val);
            def.index_key(&serde_json::Value::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
        IndexDeclaration::Compound(def) => {
            let mut record = serde_json::Map::new();
            for (field, bytes) in def.fields.iter().zip(key_values.iter()) {
                let val = decode_typed_lookup_value_rel(&field.index_type, bytes)?;
                record.insert(field.field.clone(), val);
            }
            def.index_key(&serde_json::Value::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
    }
}

/// Compute `(index_name, canonical_key_bytes)` pairs for an item's `entity_document`.
/// Returns empty when `typed_indexes` is empty or `entity` is `None` (schema-less queues).
fn typed_index_keys_for_entity(
    typed_indexes: &[QueueIndex],
    entity: Option<&serde_json::Value>,
) -> EngineResult<Vec<(String, Vec<u8>)>> {
    let Some(entity) = entity else {
        return Ok(vec![]);
    };
    let mut out = Vec::with_capacity(typed_indexes.len());
    for qi in typed_indexes {
        let key = match &qi.declaration {
            IndexDeclaration::Single(def) => def.index_key(entity),
            IndexDeclaration::Compound(def) => def.index_key(entity),
        };
        if let Some(k) = key.map_err(|e| EngineError::Storage(e.to_string()))? {
            out.push((qi.name.clone(), k));
        }
    }
    Ok(out)
}

type TypedIndexRows = Vec<(String, Vec<(String, Vec<u8>)>)>;

fn index_is_unique(qi: &QueueIndex) -> bool {
    match &qi.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

fn is_unique_violation(err: &postgres::Error) -> bool {
    err.code() == Some(&SqlState::UNIQUE_VIOLATION)
}

/// Check unique-index constraints for `keys` against existing DB rows. Returns `Conflict` if any
/// unique index already maps the same key to a *different* item.
fn check_typed_unique_conflicts(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    keys: &[(String, Vec<u8>)],
    exclude_item_id: Option<&str>,
) -> EngineResult<()> {
    for (name, key) in keys {
        let unique = typed_indexes
            .iter()
            .find(|qi| &qi.name == name)
            .map(index_is_unique)
            .unwrap_or(false);
        if !unique {
            continue;
        }
        let holder: Option<String> = match exclude_item_id {
            Some(excl) => st(tx.query_opt(
                "SELECT item_id FROM pqueue_item_index \
                 WHERE tenant_id=$1 AND queue_id=$2 AND index_name=$3 AND index_key=$4 \
                 AND item_id<>$5 LIMIT 1",
                &[&t, &q, name, &key.as_slice(), &excl],
            ))?
            .map(|row| row.get(0)),
            None => st(tx.query_opt(
                "SELECT item_id FROM pqueue_item_index \
                 WHERE tenant_id=$1 AND queue_id=$2 AND index_name=$3 AND index_key=$4 LIMIT 1",
                &[&t, &q, name, &key.as_slice()],
            ))?
            .map(|row| row.get(0)),
        };
        if holder.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

/// Insert `pqueue_item_index` rows for one item's `(name, key)` pairs (upsert so a retry is safe).
fn insert_typed_index_rows(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    item_id: &str,
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    for (name, key) in keys {
        let is_unique = typed_indexes
            .iter()
            .find(|qi| &qi.name == name)
            .map(index_is_unique)
            .unwrap_or(false);
        tx.execute(
            "INSERT INTO pqueue_item_index \
             (tenant_id, queue_id, index_name, index_key, item_id, is_unique) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT(tenant_id,queue_id,index_name,item_id) DO UPDATE SET \
             index_key=EXCLUDED.index_key, is_unique=EXCLUDED.is_unique",
            &[&t, &q, name, &key.as_slice(), &item_id, &is_unique],
        )
        .map_err(|err| {
            if is_unique_violation(&err) {
                EngineError::Conflict
            } else {
                EngineError::Storage(err.to_string())
            }
        })?;
    }
    Ok(())
}

/// Delete all `pqueue_item_index` rows for the given item IDs.
fn delete_typed_index_rows(
    tx: &mut impl GenericClient,
    t: &str,
    q: &str,
    item_ids: &[String],
) -> EngineResult<()> {
    if item_ids.is_empty() {
        return Ok(());
    }
    st(tx.execute(
        "DELETE FROM pqueue_item_index \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
        &[&t, &q, &item_ids],
    ))?;
    Ok(())
}

/// Enforce unique constraints and insert index rows for all `items` in a push batch.
fn maintain_typed_indexes_on_insert(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    items: &[PushItem],
) -> EngineResult<()> {
    if typed_indexes.is_empty() {
        return Ok(());
    }
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    let mut item_keys: TypedIndexRows = Vec::with_capacity(items.len());
    for item in items {
        let keys = typed_index_keys_for_entity(typed_indexes, item.entity_document.as_ref())?;
        check_typed_unique_conflicts(tx, t, q, typed_indexes, &keys, None)?;
        for (name, key) in &keys {
            if typed_indexes
                .iter()
                .find(|qi| &qi.name == name)
                .map(index_is_unique)
                .unwrap_or(false)
            {
                let bk = (name.clone(), key.clone());
                let id_str = item.item_id.to_string();
                if let Some(prev) = batch_unique.get(&bk) {
                    if prev != &id_str {
                        return Err(EngineError::Conflict);
                    }
                } else {
                    batch_unique.insert(bk, id_str);
                }
            }
        }
        item_keys.push((item.item_id.to_string(), keys));
    }
    for (item_id, keys) in &item_keys {
        insert_typed_index_rows(tx, t, q, typed_indexes, item_id, keys)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Inner: the durable client + the queue-definition cache + the live-token map
// ---------------------------------------------------------------------------

struct Inner {
    client: Client,
    queues: HashMap<QueueKey, QueueDefinition>,
    schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    live_tokens: HashMap<ItemId, LeaseToken>,
}

impl Inner {
    /// Reload the queue-def cache from the durable `queues` table. The item projection itself is already
    /// durable in `pqueue_items` as a rebuildable cache - nothing to replay.
    ///
    /// NOTE: item-id restart-safety is handled by `restore_counters` (it seeds `QueueCounters` past the
    /// highest durable id, decoding `(epoch, counter)` straight from the packed id — ADR-009).
    fn reload(&mut self) -> EngineResult<()> {
        let rows = st(self.client.query("SELECT definition FROM queues", &[]))?;
        for row in rows {
            let def_json: String = row.get(0);
            let definition: QueueDefinition =
                serde_json::from_str(&def_json).map_err(|e| EngineError::Storage(e.to_string()))?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(cs) = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?
            {
                self.schemas.insert(key.clone(), cs);
            }
            self.queues.insert(key, definition);
        }
        Ok(())
    }

    /// Assign the next command sequence for `shard` (atomic increment-and-return — no TOCTOU), apply
    /// `command`, and commit. Token-map mutations apply post-commit (a commit failure cannot desync them).
    ///
    /// BQ-20 NOTE: the data-plane fast path (every port routes here) is the in-process owner and is NOT
    /// epoch-fenced — the TD-003 `assignment_epoch` fence lives at the [`PgRelLogWriter::append`] seam.
    /// Caching + stamping the owner's `expected_epoch` on the hot path (so a stale owner's claim is fenced
    /// end-to-end) arrives with the ownership/lease identity layer (BQ-21).
    fn commit_command(
        &mut self,
        shard: &QueueKey,
        command: QueueCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        let Inner {
            client,
            queues,
            live_tokens,
            ..
        } = self;
        let (t, q) = parts(shard);
        let mut tx = st(client.transaction())?;
        // ADR-009 / TD-003: fence a superseded owner (cached `expected_epoch` != durable assignment_epoch)
        // before applying — nothing is written. `None` is the degenerate sole-owner path (no fence). BQ-23
        // makes this `assignment_epoch` the same single durable value the control-plane acquire advances.
        let epoch: i64 = st(tx.query_one(
            "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .get(0);
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
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

/// Bulk-allocate `n` consecutive stable per-queue item insertion sequences (`created_seq`) in ONE atomic
/// `UPDATE … RETURNING` (same rationale as [`alloc_seq`]); the i-th batched item takes `base + i`, preserving
/// the per-item FIFO order the former one-at-a-time allocator produced — in a single round-trip.
fn alloc_item_seqs(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    n: i64,
) -> EngineResult<i64> {
    let row = st(tx.query_one(
        "UPDATE relational_cursor SET next_item_seq = next_item_seq + $3 WHERE tenant=$1 AND queue=$2 \
         RETURNING next_item_seq - $3",
        &[&t, &q, &n],
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    // One set-based round-trip (was one SELECT per item): the distinct non-null group keys of these ids.
    let rows = st(tx.query(
        "SELECT DISTINCT group_key FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
         AND item_id = ANY($3) AND group_key IS NOT NULL",
        &[&t, &q, &id_strs],
    ))?;
    let mut seen: Vec<GroupKey> = Vec::with_capacity(rows.len());
    for row in rows {
        let g: String = row.get(0);
        let gk = GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
        if !seen.contains(&gk) {
            seen.push(gk);
        }
    }
    Ok(seen)
}

fn cohort_group_for_id(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<GroupKey> {
    let (t, q) = parts(shard);
    let row = st(tx.query_one(
        "SELECT group_key FROM pqueue_cohorts WHERE tenant_id=$1 AND queue_id=$2 AND cohort_id=$3",
        &[&t, &q, &cohort_id.as_str()],
    ))?;
    let group: String = row.get(0);
    GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))
}

fn cohort_item_ids(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let group = cohort_group_for_id(tx, shard, cohort_id)?;
    let rows = st(tx.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND superseded=false AND cohort_size IS NOT NULL AND lifecycle_state NOT IN ('Complete','Failed') \
         ORDER BY priority_sort, created_seq",
        &[&t, &q, &group.as_str()],
    ))?;
    rows.into_iter()
        .map(|row| {
            let id: String = row.get(0);
            ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))
        })
        .collect()
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

/// One materialized row for the batched `pqueue_items` insert. Owns its values so the param slice can
/// borrow them across the multi-row statement build.
struct ItemRow {
    item_id: String,
    key: String,
    priority_json: Option<String>,
    sort: Vec<u8>,
    not_before: Option<i64>,
    eligible_since: i64,
    group_key: Option<String>,
    cohort_size: Option<i64>,
    payload: Option<Vec<u8>>,
    fields: String,
    metadata: String,
    max_attempts: i64,
    created_seq: i64,
}

/// Max `pqueue_items` rows per INSERT statement: 13 bound params/row + 4 shared; 1000 rows ≈ 13k params,
/// well under postgres' 65535 bound-parameter ceiling.
const PG_INSERT_CHUNK: usize = 1000;

/// Batch-insert all `items` of a Push (or the single ReplacePending replacement) as set-based statements:
/// one (chunked) multi-row INSERT into `pqueue_items`, one multi-row INSERT into `pqueue_item_gates`, and
/// one multi-row upsert into `pqueue_cohorts` — replacing the former per-item `insert_item` (N+ round-trips
/// → a handful). Column values, the `fields` TEXT-JSON encoding, and the `eligible_since`/`not_before`
/// pairing are identical to the per-item path; `created_seq` is bulk-allocated (`base + i`) so FIFO order is
/// preserved.
fn insert_items(
    tx: &mut postgres::Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    model: &PriorityModel,
    shard: &QueueKey,
    items: &[PushItem],
    seq: u64,
    now: UtcTimestamp,
) -> EngineResult<()> {
    if items.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let seqi = seq as i64;
    let base_seq = alloc_item_seqs(tx, &t, &q, items.len() as i64)?;
    let mut rows: Vec<ItemRow> = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let not_before = ts_nanos_opt(item.not_before);
        rows.push(ItemRow {
            item_id: item.item_id.to_string(),
            key: item.client_item_key.as_str().to_string(),
            priority_json: item.priority.as_ref().map(to_json).transpose()?,
            sort: elig_sort(&item.priority, model),
            not_before,
            eligible_since: not_before.unwrap_or(now_n),
            group_key: item.group_key.as_ref().map(|g| g.as_str().to_string()),
            cohort_size: item.cohort_size.map(|s| s as i64),
            payload: item.payload.as_ref().map(|b| b.to_vec()),
            fields: fields_to_json(&item.fields)?,
            metadata: metadata_to_json(&item.metadata)?,
            max_attempts: item.max_attempts as i64,
            created_seq: base_seq + i as i64,
        });
    }
    for chunk in rows.chunks(PG_INSERT_CHUNK) {
        let mut sql = String::from(
            "INSERT INTO pqueue_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,retry_count,\
              item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,fenced,superseded,max_attempts,created_seq) VALUES ",
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&t, &q, &seqi, &now_n];
        for (r, row) in chunk.iter().enumerate() {
            let b = 5 + r * 13;
            if r > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "($1,$2,${},${},'Pending',${},${},${},${},${},${},${},${},${},0,1,NULL,NULL,NULL,\
                 $3,$4,$4,NULL,false,false,${},${})",
                b,
                b + 1,
                b + 2,
                b + 3,
                b + 4,
                b + 5,
                b + 6,
                b + 7,
                b + 8,
                b + 9,
                b + 10,
                b + 11,
                b + 12,
            ));
            params.push(&row.item_id);
            params.push(&row.key);
            params.push(&row.priority_json);
            params.push(&row.sort);
            params.push(&row.not_before);
            params.push(&row.eligible_since);
            params.push(&row.group_key);
            params.push(&row.cohort_size);
            params.push(&row.payload);
            params.push(&row.fields);
            params.push(&row.metadata);
            params.push(&row.max_attempts);
            params.push(&row.created_seq);
        }
        st(tx.execute(sql.as_str(), &params))?;
    }
    insert_gates(tx, &t, &q, items)?;
    upsert_cohorts(tx, queues, shard, &t, &q, items, now_n)?;
    // ADR-011: typed secondary index maintenance.
    let typed_indexes = queues
        .get(shard)
        .map(|d| d.typed_indexes.as_slice())
        .unwrap_or(&[]);
    maintain_typed_indexes_on_insert(tx, &t, &q, typed_indexes, items)?;
    Ok(())
}

/// Batch the per-item gate-membership inserts (BQ-14d) into chunked multi-row INSERTs. Pairs are deduped so
/// a single statement never proposes the same `(item_id, gate_key)` twice (the per-item path relied on
/// `ON CONFLICT DO NOTHING` for that).
fn insert_gates(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    items: &[PushItem],
) -> EngineResult<()> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for item in items {
        let id = item.item_id.to_string();
        for gate_key in &item.gate_keys {
            let g = gate_key.as_str().to_string();
            if !pairs.iter().any(|(a, b)| a == &id && b == &g) {
                pairs.push((id.clone(), g));
            }
        }
    }
    if pairs.is_empty() {
        return Ok(());
    }
    for chunk in pairs.chunks(5000) {
        let mut sql = String::from(
            "INSERT INTO pqueue_item_gates (tenant_id,queue_id,item_id,gate_key) VALUES ",
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&t, &q];
        for (r, (id, g)) in chunk.iter().enumerate() {
            let b = 3 + r * 2;
            if r > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("($1,$2,${},${})", b, b + 1));
            params.push(id);
            params.push(g);
        }
        sql.push_str(" ON CONFLICT (tenant_id,queue_id,item_id,gate_key) DO NOTHING");
        st(tx.execute(sql.as_str(), &params))?;
    }
    Ok(())
}

fn cohort_id_for(group_key: &str, now_n: i64) -> String {
    format!("coh:{group_key}:{now_n}")
}

fn cohort_retention_until(
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    now_n: i64,
) -> EngineResult<i64> {
    let retention_ms = queues
        .get(shard)
        .map(|d| d.terminal_retention_ms)
        .ok_or(EngineError::NotFound)?;
    Ok(now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000)))
}

fn cohort_expiry_deadline(
    definition: &QueueDefinition,
    cohort_created_at: i64,
    first_eligible_at: Option<i64>,
) -> Option<i64> {
    let bound_ms = definition.cohort_policy.as_ref()?.completion_bound_ms?;
    let start = first_eligible_at
        .map(|first| cohort_created_at.min(first))
        .unwrap_or(cohort_created_at);
    Some(start.saturating_add((bound_ms as i64).saturating_mul(1_000_000)))
}

fn cohort_member_count_state(count: i64, size: i64) -> &'static str {
    if count >= size { "complete" } else { "forming" }
}

/// Maintain TD-002 cohort lifecycle projection for newly accepted cohort members.
fn upsert_cohorts(
    tx: &mut postgres::Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    t: &str,
    q: &str,
    items: &[PushItem],
    now_n: i64,
) -> EngineResult<()> {
    let mut cohorts: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for item in items {
        if let (Some(group), Some(size)) = (&item.group_key, item.cohort_size) {
            let gk = group.as_str().to_string();
            let size = size as i64;
            let entry = cohorts.entry(gk).or_insert((size, 0));
            if entry.0 != size {
                return Err(EngineError::Conflict);
            }
            entry.1 += 1;
        }
    }
    if cohorts.is_empty() {
        return Ok(());
    }
    let _ = cohort_retention_until(queues, shard, now_n)?;
    for (gk, (size, added)) in cohorts {
        let existing = st(tx.query_opt(
            "SELECT cohort_size, member_count, state, retention_until FROM pqueue_cohorts \
             WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 FOR UPDATE",
            &[&t, &q, &gk],
        ))?;
        match existing {
            None => {
                if added > size {
                    return Err(EngineError::Conflict);
                }
                let state = cohort_member_count_state(added, size);
                let first_eligible_at = if state == "complete" {
                    Some(now_n)
                } else {
                    None
                };
                st(tx.execute(
                    "INSERT INTO pqueue_cohorts \
                     (tenant_id,queue_id,group_key,cohort_id,cohort_size,member_count,state,\
                      cohort_created_at,first_eligible_at,created_at) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$8)",
                    &[
                        &t,
                        &q,
                        &gk,
                        &cohort_id_for(&gk, now_n),
                        &size,
                        &added,
                        &state,
                        &now_n,
                        &first_eligible_at,
                    ],
                ))?;
            }
            Some(row) => {
                let existing_size: i64 = row.get(0);
                let member_count: i64 = row.get(1);
                let state: String = row.get(2);
                let retention_until: Option<i64> = row.get(3);
                if state == "terminal" {
                    if retention_until.is_some_and(|until| until > now_n) {
                        return Err(EngineError::Conflict);
                    }
                    if added > size {
                        return Err(EngineError::Conflict);
                    }
                    let next_state = cohort_member_count_state(added, size);
                    let first_eligible_at = if next_state == "complete" {
                        Some(now_n)
                    } else {
                        None
                    };
                    st(tx.execute(
                        "UPDATE pqueue_cohorts SET cohort_id=$4, cohort_size=$5, member_count=$6, \
                         state=$7, cohort_created_at=$8, first_eligible_at=$9, expire_command_pos=NULL, \
                         cohort_lease_token_hash=NULL, retention_until=NULL, created_at=$8 \
                         WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3",
                        &[
                            &t,
                            &q,
                            &gk,
                            &cohort_id_for(&gk, now_n),
                            &size,
                            &added,
                            &next_state,
                            &now_n,
                            &first_eligible_at,
                        ],
                    ))?;
                    continue;
                }
                if existing_size != size {
                    return Err(EngineError::Conflict);
                }
                if member_count + added > existing_size {
                    return Err(EngineError::Conflict);
                }
                let next_count = member_count + added;
                let next_state = if state == "leased" {
                    state.as_str()
                } else {
                    cohort_member_count_state(next_count, existing_size)
                };
                let set_first = next_state == "complete";
                st(tx.execute(
                    "UPDATE pqueue_cohorts SET member_count=$4, state=$5, \
                     first_eligible_at=CASE WHEN $6 AND first_eligible_at IS NULL THEN $7 ELSE first_eligible_at END \
                     WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3",
                    &[&t, &q, &gk, &next_count, &next_state, &set_first, &now_n],
                ))?;
            }
        }
    }
    Ok(())
}

/// Apply the shared Finalize SET to a whole bucket of item ids in ONE statement (skips an empty bucket).
/// `state`/`reset`/`terminal_at` are the bucket-invariant disposition values.
#[allow(clippy::too_many_arguments)]
fn finalize_update(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    state: &str,
    reset: bool,
    terminal_at: Option<i64>,
    ids: &[String],
    now_n: i64,
    seqi: i64,
) -> EngineResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    st(tx.execute(
        "UPDATE pqueue_items SET lifecycle_state=$4, lease_token_hash=NULL, lease_expires_at=NULL, \
         fenced=false, item_version=item_version+1, \
         retry_count=CASE WHEN $5 THEN 0 ELSE retry_count END, \
         terminal_at=$6, updated_at=$7, last_command_sequence=$8 \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
        &[&t, &q, &ids, &state, &reset, &terminal_at, &now_n, &seqi],
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
            insert_items(tx, queues, &model, shard, &c.items, seq, now)?;
            let mut groups: Vec<GroupKey> = Vec::new();
            for it in &c.items {
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
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=$4, \
                 lease_expires_at=$5, retry_count=retry_count+1, item_version=item_version+1, \
                 updated_at=$6, last_command_sequence=$7 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &hash, &exp, &now_n, &seqi],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            for g in groups_of(tx, shard, &c.item_ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortClaim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=$4, \
                 lease_expires_at=$5, retry_count=retry_count+1, item_version=item_version+1, \
                 updated_at=$6, last_command_sequence=$7 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &hash, &exp, &now_n, &seqi],
            ))?;
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='leased', cohort_lease_token_hash=$4 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND cohort_id=$3",
                &[&t, &q, &c.cohort_id.as_str(), &hash],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            for g in groups_of(tx, shard, &c.item_ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET lease_expires_at=$4, item_version=item_version+1, \
                 updated_at=$5, last_command_sequence=$6 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &exp, &now_n, &seqi],
            ))?;
            Ok(())
        }
        QueueCommand::CohortRenewLease(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
            let exp = ts_nanos(c.lease_expires_at);
            st(tx.execute(
                "UPDATE pqueue_items SET lease_expires_at=$4, item_version=item_version+1, \
                 updated_at=$5, last_command_sequence=$6 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &id_strs, &exp, &now_n, &seqi],
            ))?;
            Ok(())
        }
        QueueCommand::UpdateFields(c) => {
            // Read-merge-write the hot-storage fields/payload of a LIVE item (Pending|Leased, not
            // superseded/fenced). `fields` is the SAME TEXT-JSON representation insert_item / live_items_sql
            // use (`fields_from_json`/`fields_to_json` over a BTreeMap<String, Vec<u8>>). Pre-validated by the
            // UpdateFieldsPort, so a missing/ineligible row here is a no-op (commit has no rollback).
            let item_id = c.item_id.to_string();
            let row = st(tx.query_opt(
                "SELECT fields FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=false AND fenced=false",
                &[&t, &q, &item_id],
            ))?;
            let Some(row) = row else { return Ok(()) };
            let mut fields = fields_from_json(row.get::<_, String>(0))?;
            for (k, op) in &c.field_ops {
                match op {
                    Some(v) => {
                        fields.insert(k.clone(), v.clone());
                    }
                    None => {
                        fields.remove(k);
                    }
                }
            }
            let fields_json = fields_to_json(&fields)?;
            match &c.payload {
                PayloadUpdate::Keep => {
                    st(tx.execute(
                        "UPDATE pqueue_items SET fields=$4, item_version=item_version+1, \
                         updated_at=$5, last_command_sequence=$6 \
                         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                         AND lifecycle_state IN ('Pending','Leased') AND superseded=false AND fenced=false",
                        &[&t, &q, &item_id, &fields_json, &now_n, &seqi],
                    ))?;
                }
                PayloadUpdate::Set(p) => {
                    let payload: Option<Vec<u8>> = p.as_ref().map(|b| b.to_vec());
                    st(tx.execute(
                        "UPDATE pqueue_items SET fields=$4, payload=$5, item_version=item_version+1, \
                         updated_at=$6, last_command_sequence=$7 \
                         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                         AND lifecycle_state IN ('Pending','Leased') AND superseded=false AND fenced=false",
                        &[&t, &q, &item_id, &fields_json, &payload, &now_n, &seqi],
                    ))?;
                }
            }
            // ADR-011: if a new entity document was supplied, re-index this item. Delete the
            // old rows first so the unique slot is freed before the conflict check fires.
            if let Some(ref doc) = c.set_entity_document {
                let typed_indexes = queues
                    .get(shard)
                    .map(|d| d.typed_indexes.as_slice())
                    .unwrap_or(&[]);
                if !typed_indexes.is_empty() {
                    delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&item_id))?;
                    let new_keys = typed_index_keys_for_entity(typed_indexes, Some(doc))?;
                    check_typed_unique_conflicts(tx, &t, &q, typed_indexes, &new_keys, None)?;
                    insert_typed_index_rows(tx, &t, &q, typed_indexes, &item_id, &new_keys)?;
                }
            }
            Ok(())
        }
        QueueCommand::ReassignLease(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET lease_token_hash=$4, lease_expires_at=$5, \
                 retry_count=retry_count+1, item_version=item_version+1, updated_at=$6, \
                 last_command_sequence=$7 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &hash, &exp, &now_n, &seqi],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::Finalize(c) => {
            // Resolve Retry-exhaustion for all Retry outcomes in ONE read (was one SELECT per outcome).
            let retry_ids: Vec<String> = c
                .outcomes
                .iter()
                .filter(|o| matches!(o.kind, FinalizeKind::Retry))
                .map(|o| o.item_id.to_string())
                .collect();
            let mut retry_info: HashMap<String, (i64, i64)> = HashMap::new();
            if !retry_ids.is_empty() {
                let rows = st(tx.query(
                    "SELECT item_id, retry_count, max_attempts FROM pqueue_items \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                    &[&t, &q, &retry_ids],
                ))?;
                for row in rows {
                    let id: String = row.get(0);
                    retry_info.insert(id, (row.get(1), row.get(2)));
                }
            }
            // Bucket outcomes by the target SET, then issue ONE UPDATE per bucket. The disposition fully
            // determines (new_state, terminal_at, reset_attempts), so there are at most four buckets.
            let mut to_complete: Vec<String> = Vec::new();
            let mut to_failed: Vec<String> = Vec::new();
            let mut to_pending: Vec<String> = Vec::new();
            let mut to_pending_rearm: Vec<String> = Vec::new();
            let mut backoff: BTreeMap<i64, Vec<String>> = BTreeMap::new();
            for o in &c.outcomes {
                let id = o.item_id.to_string();
                let new_state = match o.kind {
                    FinalizeKind::Complete => ItemState::Complete,
                    FinalizeKind::Fail => ItemState::Failed,
                    FinalizeKind::Retry => {
                        let (rc, ma) = retry_info.get(&id).copied().ok_or(EngineError::NotFound)?;
                        if is_retry_exhausted(rc as u32, ma as u32) {
                            ItemState::Failed
                        } else {
                            ItemState::Pending
                        }
                    }
                    FinalizeKind::Release => ItemState::Pending,
                    FinalizeKind::Rearm => ItemState::Pending,
                };
                match new_state {
                    ItemState::Complete => to_complete.push(id.clone()),
                    ItemState::Failed => to_failed.push(id.clone()),
                    ItemState::Pending if matches!(o.kind, FinalizeKind::Rearm) => {
                        to_pending_rearm.push(id.clone())
                    }
                    ItemState::Pending => to_pending.push(id.clone()),
                    ItemState::Leased => unreachable!("Finalize never targets Leased"),
                }
                // Queue-native retry backoff: a Retry that returned the item to Pending (still under the
                // attempt bound) defers its re-eligibility to `not_before`. Mirror insert_item's pairing of
                // not_before + eligible_since. Grouped by identical not_before so each value is one UPDATE.
                if matches!(o.kind, FinalizeKind::Retry)
                    && new_state == ItemState::Pending
                    && let Some(nb) = o.not_before
                {
                    backoff.entry(ts_nanos(nb)).or_default().push(id.clone());
                }
                token_ops.push(TokenOp::Clear(o.item_id));
            }
            let complete = state_str(ItemState::Complete);
            let failed = state_str(ItemState::Failed);
            let pending = state_str(ItemState::Pending);
            finalize_update(
                tx,
                &t,
                &q,
                complete,
                false,
                Some(now_n),
                &to_complete,
                now_n,
                seqi,
            )?;
            finalize_update(
                tx,
                &t,
                &q,
                failed,
                false,
                Some(now_n),
                &to_failed,
                now_n,
                seqi,
            )?;
            finalize_update(tx, &t, &q, pending, false, None, &to_pending, now_n, seqi)?;
            finalize_update(
                tx,
                &t,
                &q,
                pending,
                true,
                None,
                &to_pending_rearm,
                now_n,
                seqi,
            )?;
            for (nb_n, ids) in &backoff {
                st(tx.execute(
                    "UPDATE pqueue_items SET not_before=$4, eligible_since=$4 \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                    &[&t, &q, ids, nb_n],
                ))?;
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id).collect();
            for g in groups_of(tx, shard, &ids)? {
                refresh_group_summary(tx, shard, &g, now)?;
            }
            Ok(())
        }
        QueueCommand::CohortFinalize(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            if ids.is_empty() {
                return Err(EngineError::NotFound);
            }
            let outcomes: Vec<FinalizeOutcome> = ids
                .iter()
                .map(|item_id| FinalizeOutcome {
                    item_id: *item_id,
                    kind: c.kind,
                    not_before: c.not_before,
                })
                .collect();
            apply_command_sql(
                tx,
                queues,
                token_ops,
                shard,
                seq,
                now,
                &QueueCommand::Finalize(FinalizeCommand { outcomes }),
            )?;
            let next_state = match c.kind {
                FinalizeKind::Complete | FinalizeKind::Fail => "terminal",
                FinalizeKind::Retry | FinalizeKind::Release => "complete",
                FinalizeKind::Rearm => return Err(EngineError::Invalid("cohort rearm is invalid")),
            };
            let retention_until = if next_state == "terminal" {
                Some(cohort_retention_until(queues, shard, now_n)?)
            } else {
                None
            };
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state=$4, cohort_lease_token_hash=NULL, retention_until=$5 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND cohort_id=$3",
                &[&t, &q, &c.cohort_id.as_str(), &next_state, &retention_until],
            ))?;
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            // ADR-011: delete the superseded item's index rows first so the replacement can claim
            // the same unique key without a spurious Conflict.
            let superseded_str = c.superseded_item_id.to_string();
            delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&superseded_str))?;
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=true, updated_at=$4, last_command_sequence=$5 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&t, &q, &c.superseded_item_id.to_string(), &now_n, &seqi],
            ))?;
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            insert_items(
                tx,
                queues,
                &model,
                shard,
                std::slice::from_ref(&c.replacement),
                seq,
                now,
            )?;
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
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                 lease_expires_at=NULL, item_version=item_version+1, updated_at=$4, \
                 last_command_sequence=$5 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &now_n, &seqi],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(*id));
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
            let mut id_strs: Vec<String> = Vec::new();
            for row in rows {
                let id: String = row.get(0);
                id_strs.push(id.clone());
                ids.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
            }
            st(tx.execute(
                "UPDATE pqueue_items SET lifecycle_state='Failed', item_version=item_version+1, \
                 terminal_at=$4, updated_at=$4, last_command_sequence=$5 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &id_strs, &now_n, &seqi],
            ))?;
            for id in &ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='terminal', expire_command_pos=$4, \
                 cohort_lease_token_hash=NULL, retention_until=$5 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3",
                &[
                    &t,
                    &q,
                    &c.group_key.as_str(),
                    &seqi,
                    &cohort_retention_until(queues, shard, now_n)?,
                ],
            ))?;
            refresh_group_summary(tx, shard, &c.group_key, now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET fenced=true WHERE tenant_id=$1 AND queue_id=$2 \
                 AND item_id = ANY($3)",
                &[&t, &q, &ids],
            ))?;
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            st(tx.execute(
                "UPDATE pqueue_items SET fenced=false WHERE tenant_id=$1 AND queue_id=$2 \
                 AND item_id = ANY($3)",
                &[&t, &q, &ids],
            ))?;
            Ok(())
        }
        QueueCommand::PauseQueue(_) => {
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
            let id_strs: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            // One set-based read of every purged item (was one SELECT per item).
            let rows = st(tx.query(
                "SELECT item_id, group_key, client_item_key, lifecycle_state FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &id_strs],
            ))?;
            let mut groups: Vec<GroupKey> = Vec::new();
            // (client_item_key, item_id) tombstones for terminal items, deduped LAST-wins on key so the
            // batched upsert never touches the same conflict target twice (DO UPDATE cardinality).
            let mut retention: Vec<(String, String)> = Vec::new();
            for row in rows {
                let item_id: String = row.get(0);
                let gk: Option<String> = row.get(1);
                let ck: String = row.get(2);
                let state: String = row.get(3);
                if parse_state(&state)?.is_terminal() && retention_ms > 0 {
                    retention.retain(|(k, _)| k != &ck);
                    retention.push((ck, item_id));
                }
                if let Some(g) = gk {
                    let gk2 = GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
                    if !groups.contains(&gk2) {
                        groups.push(gk2);
                    }
                }
            }
            if !retention.is_empty() {
                let expires = now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000));
                let mut sql = String::from(
                    "INSERT INTO pqueue_item_key_retention \
                     (tenant_id,queue_id,client_item_key,item_id,expires_at) VALUES ",
                );
                let mut params: Vec<&(dyn ToSql + Sync)> = vec![&t, &q, &expires];
                for (r, (ck, item_id)) in retention.iter().enumerate() {
                    let b = 4 + r * 2;
                    if r > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!("($1,$2,${},${},$3)", b, b + 1));
                    params.push(ck);
                    params.push(item_id);
                }
                sql.push_str(
                    " ON CONFLICT(tenant_id,queue_id,client_item_key) \
                     DO UPDATE SET item_id=EXCLUDED.item_id, expires_at=EXCLUDED.expires_at",
                );
                st(tx.execute(sql.as_str(), &params))?;
            }
            // Set-based deletes (item rows + their gate membership) — one round-trip each.
            st(tx.execute(
                "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &id_strs],
            ))?;
            st(tx.execute(
                "DELETE FROM pqueue_item_gates WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &id_strs],
            ))?;
            // ADR-011: drop the purged items' typed secondary index rows.
            delete_typed_index_rows(tx, &t, &q, &id_strs)?;
            for id in &c.item_ids {
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
                for gate_key in &c.gate_keys {
                    let gk_str = gate_key.as_str().to_string();
                    st(tx.execute(
                        "INSERT INTO pqueue_gate_state (tenant_id,queue_id,gate_key) VALUES ($1,$2,$3) \
                         ON CONFLICT (tenant_id,queue_id,gate_key) DO NOTHING",
                        &[&t, &q, &gk_str],
                    ))?;
                }
            } else {
                for gate_key in &c.gate_keys {
                    let gk_str = gate_key.as_str().to_string();
                    st(tx.execute(
                        "DELETE FROM pqueue_gate_state WHERE tenant_id=$1 AND queue_id=$2 AND gate_key=$3",
                        &[&t, &q, &gk_str],
                    ))?;
                }
            }
            Ok(())
        }
        // C9 (epic pqueue-2201fd37): opaque NON-WORK side records (Snorri authoritative-commit boundary).
        // Upsert each (key,payload) into `pqueue_side_records` — a table disjoint from `pqueue_items`, so a
        // side record is never claimable/eligible/peekable nor counted as work. Apply is infallible
        // (insert-or-overwrite by key), mirroring `pqueue-sqlite`'s arm. `CommitTransitionPort` itself is not
        // yet wired on this backend (a separate bead) — this arm only makes the storage ready for it.
        QueueCommand::WriteSideRecords(c) => {
            for rec in &c.records {
                st(tx.execute(
                    "INSERT INTO pqueue_side_records (tenant_id,queue_id,key,payload) \
                     VALUES ($1,$2,$3,$4) \
                     ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET payload=EXCLUDED.payload",
                    &[&t, &q, &rec.key, &rec.payload.as_ref()],
                ))?;
            }
            Ok(())
        }
        // C6 (epic pqueue-2201fd37): advance a caller-supplied opaque instance/state fence. Validated
        // pre-commit (stored==expected, next>expected), so the upsert is infallible. Disjoint from
        // `pqueue_items` — a fence is never claimable/peekable work. `CommitTransitionPort` itself is not
        // yet wired on this backend (a separate bead) — this arm only makes the storage ready for it.
        QueueCommand::AdvanceInstanceFence(c) => {
            st(tx.execute(
                "INSERT INTO pqueue_instance_fences (tenant_id,queue_id,instance_key,fence) \
                 VALUES ($1,$2,$3,$4) \
                 ON CONFLICT(tenant_id,queue_id,instance_key) DO UPDATE SET fence=EXCLUDED.fence",
                &[&t, &q, &c.instance_key, &(c.next as i64)],
            ))?;
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
         AND lifecycle_state='Pending' AND superseded=false AND cohort_size IS NULL \
         AND (not_before IS NULL OR not_before<=$3) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
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

// ---------------------------------------------------------------------------
// BQ-14b: group-aware claim selection (group_batching / same_group_key), owner-local, consuming
// `pqueue_group_summary`. The candidate groups are locked with `FOR UPDATE SKIP LOCKED` on their summary
// rows — TD-002's per-group lock that guarantees two concurrent claims never split a group (the real
// row-lock the sqlite backend approximates with its process Mutex). Runs inside the claim transaction.
// ---------------------------------------------------------------------------

/// Candidate groups for the queue, ordered by representative claim key (`rep_priority_sort,
/// rep_created_at, rep_item_id`; `rep_progress_guard_sort` deferred/NULL), LOCKED with `FOR UPDATE SKIP
/// LOCKED` so a group held by a concurrent claim is skipped (contended), not split. Same lagged-summary
/// caveat as the sqlite backend (BQ-11c: a `not_before` crossing without a mutation is not yet reflected).
fn candidate_groups(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
) -> EngineResult<Vec<GroupKey>> {
    let (t, q) = parts(shard);
    let rows = st(tx.query(
        "SELECT group_key FROM pqueue_group_summary \
         WHERE tenant_id=$1 AND queue_id=$2 AND oldest_eligible_at IS NOT NULL \
         ORDER BY rep_priority_sort, rep_created_at, rep_item_id \
         FOR UPDATE SKIP LOCKED",
        &[&t, &q],
    ))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let g: String = row.get(0);
        out.push(GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// The live currently-eligible items of one group (re-read under the live predicate, FOR UPDATE so the
/// whole locked group leases together — no SKIP LOCKED inside a locked group), capped at `limit`.
fn group_eligible_items(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let lim = limit as i64;
    let rows = st(tx.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false \
         AND (not_before IS NULL OR not_before<=$4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT $5 FOR UPDATE",
        &[&t, &q, &group.as_str(), &now_n, &lim],
    ))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        out.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// `group_batching` selection (API-001 whole-eligible-group, `max_groups=N`): accumulate the oldest-N
/// candidate groups' whole eligible sets in rep order, stopping when adding the next would exceed
/// `max_items`. Fetches `max_items+1` per group so an oversized group (alone > `max_items`) is detected →
/// `BatchTooLarge` (TD-002:711; `max_eligible_group_size` is a config knob, not a hard size cap). Pause is
/// gated in `claim` before this is reached.
fn select_group_batching(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    max_groups: u32,
) -> EngineResult<Vec<ItemId>> {
    let groups = candidate_groups(tx, shard)?;
    let mut acc = Vec::new();
    let mut used = 0u32;
    for group in groups {
        if used >= max_groups {
            break;
        }
        let elig = group_eligible_items(tx, shard, &group, now, max_items + 1)?;
        if elig.is_empty() {
            continue;
        }
        if elig.len() > max_items {
            return Err(EngineError::BatchTooLarge); // a single group alone exceeds the ceiling
        }
        if acc.len() + elig.len() > max_items {
            break;
        }
        acc.extend(elig);
        used += 1;
    }
    Ok(acc)
}

/// `same_group_key` selection: the single oldest eligible group, capped at `max_items` (partial allowed).
fn select_same_group(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
) -> EngineResult<Vec<ItemId>> {
    for group in candidate_groups(tx, shard)? {
        let elig = group_eligible_items(tx, shard, &group, now, max_items)?;
        if !elig.is_empty() {
            return Ok(elig);
        }
    }
    Ok(Vec::new())
}

/// `whole_cohort` selection (API-001 G6, all-or-nothing): the oldest COMPLETE cohort whose members are ALL
/// currently eligible. The cohort summary row is locked `FOR UPDATE SKIP LOCKED`; completeness
/// (`member_count == cohort_size`) + per-member eligibility are re-read live. `BatchTooLarge` if the
/// selected cohort exceeds `max_items`. Pause is gated in `claim` before this.
#[derive(Debug, Clone)]
struct SelectedCohort {
    cohort_id: CohortId,
    item_ids: Vec<ItemId>,
}

fn select_whole_cohort(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
) -> EngineResult<Option<SelectedCohort>> {
    let (t, q) = parts(shard);
    let cohorts: Vec<(String, String, i64)> = {
        let rows = st(tx.query(
            "SELECT group_key, cohort_id, cohort_size FROM pqueue_cohorts \
             WHERE tenant_id=$1 AND queue_id=$2 AND state='complete' \
             ORDER BY cohort_created_at, group_key FOR UPDATE SKIP LOCKED",
            &[&t, &q],
        ))?;
        rows.into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect()
    };
    for (gk, cohort_id, size) in cohorts {
        let size = size as usize;
        let group = GroupKey::new(gk).map_err(|e| EngineError::Storage(e.to_string()))?;
        let members: i64 = st(tx.query_one(
            "SELECT COUNT(*)::bigint FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
             AND group_key=$3 AND superseded=false AND cohort_size IS NOT NULL \
             AND lifecycle_state NOT IN ('Complete','Failed')",
            &[&t, &q, &group.as_str()],
        ))?
        .get(0);
        if members as usize != size {
            continue; // incomplete cohort
        }
        let elig = cohort_eligible_items(tx, shard, &group, now, size + 1)?;
        if elig.len() != size {
            continue; // a member is leased / terminal / not-due
        }
        if size > max_items {
            return Err(EngineError::BatchTooLarge);
        }
        return Ok(Some(SelectedCohort {
            cohort_id: CohortId::new(cohort_id).map_err(|e| EngineError::Storage(e.to_string()))?,
            item_ids: elig,
        }));
    }
    Ok(None)
}

/// The live currently-eligible COHORT members of one group (`cohort_size IS NOT NULL`), capped at `limit`,
/// `FOR UPDATE` (the whole locked cohort leases together). Restricted to cohort-declared members (F1).
fn cohort_eligible_items(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let lim = limit as i64;
    let rows = st(tx.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false AND cohort_size IS NOT NULL \
         AND (not_before IS NULL OR not_before<=$4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
         ORDER BY priority_sort, created_seq LIMIT $5 FOR UPDATE",
        &[&t, &q, &group.as_str(), &now_n, &lim],
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

/// BQ-14e active-scope discovery: roll up `pqueue_group_summary` into ranked [`ActiveScope`]s (mirror of
/// the sqlite backend — see that helper for the full contract). Each group holding eligible work
/// (`oldest_eligible_at IS NOT NULL`) becomes one source scope, owner-local oldest-first (group-key
/// tiebreak); [`project_scopes`] collapses to the requested granularity.
///
/// `progress_bound_risk_count` is `None` ("no signal"), not `Some(0)`: the summary's `at_risk_count` is a
/// deferred `0` placeholder (see `refresh_group_summary`), and the [`ActiveScope`] contract reserves `None`
/// for an uncomputed signal. Discovery does NOT short-circuit on `queue_paused` (reports intrinsic
/// eligibility — an operator wants to see pause-induced buildup; a read of an unknown queue → empty list).
/// KNOWN LIMITATION (shared with group-claim, tracked pqueue-64351bdd): `oldest_eligible_at` is the
/// mutation-time value, lagged across a pure `not_before` crossing — discovery can UNDER-report
/// time-triggered starvation until the group's next mutation / a due-sweep refresh.
fn discover_active_scopes_sql(
    client: &mut Client,
    shard: &QueueKey,
    granularity: DiscoveryGranularity,
    now: UtcTimestamp,
) -> EngineResult<Vec<ActiveScope>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let rows = st(client.query(
        "SELECT group_key, oldest_eligible_at, eligible_item_count \
         FROM pqueue_group_summary \
         WHERE tenant_id=$1 AND queue_id=$2 AND oldest_eligible_at IS NOT NULL \
         ORDER BY oldest_eligible_at ASC, group_key ASC",
        &[&t, &q],
    ))?;
    let mut source = Vec::with_capacity(rows.len());
    for row in rows {
        let group_key: String = row.get(0);
        let oldest_eligible_at: i64 = row.get(1);
        let eligible: i64 = row.get(2);
        // Age from `now`; a future summary timestamp (clock skew) clamps to 0.
        let age_ms = now_n.saturating_sub(oldest_eligible_at).max(0) as u64 / 1_000_000;
        source.push(ActiveScope {
            queue_id: q.clone(),
            group_key: Some(group_key),
            oldest_eligible_age_ms: age_ms,
            eligible_count: Some(eligible as u64),
            // Deferred at-risk derivation → no signal (not a measured zero).
            progress_bound_risk_count: None,
        });
    }
    Ok(project_scopes(source, granularity))
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
/// not_before, lease_expires_at, retry_count, payload, fields), pairing it with `token`. Shared by the
/// claim CTE RETURNING and the `claimed_view` read port.
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
    fields: String,
    metadata: String,
    gate_keys: Vec<String>,
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
        lease_token: Some(token),
        lease_expires_at: nanos_ts(exp),
        attempt_count: retry as u32,
        payload: payload.map(Bytes::from),
        fields: fields_from_json(fields)?,
        metadata: metadata_from_json(metadata)?,
        gate_keys,
    })
}

fn item_gate_keys_by_id<C: GenericClient>(
    client: &mut C,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<HashMap<String, Vec<String>>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let (t, q) = parts(shard);
    let id_strings: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let rows = st(client.query(ITEM_GATE_KEYS_BATCH_SQL, &[&t, &q, &id_strings]))?;
    let mut by_id: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        by_id
            .entry(row.get::<_, String>(0))
            .or_default()
            .push(row.get(1));
    }
    Ok(by_id)
}

fn apply_whole_cohort_response_shape(items: &mut [ClaimedItem]) -> Option<GroupKey> {
    let cohort_id = items.first().and_then(|item| item.group_key.clone());
    for item in items {
        item.lease_token = None;
    }
    cohort_id
}

fn render_claimed(
    client: &mut Client,
    shard: &QueueKey,
    ids: &[ItemId],
    resolve: impl Fn(&ItemId) -> Option<LeaseToken>,
) -> EngineResult<Vec<ClaimedItem>> {
    let (t, q) = parts(shard);
    let mut out = Vec::new();
    let mut gate_keys_by_id = item_gate_keys_by_id(client, shard, ids)?;
    for id in ids {
        let Some(token) = resolve(id) else {
            continue;
        };
        let row = st(client.query_opt(
            "SELECT client_item_key, item_version, priority, group_key, not_before, \
             lease_expires_at, retry_count, payload, fields, metadata FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 AND lifecycle_state='Leased'",
            &[&t, &q, &id.to_string()],
        ))?;
        let Some(row) = row else { continue };
        let exp: Option<i64> = row.get(5);
        let Some(exp) = exp else { continue };
        let gate_keys = gate_keys_by_id.remove(&id.to_string()).unwrap_or_default();
        out.push(claimed_from_row(
            *id,
            token,
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            exp,
            row.get(6),
            row.get(7),
            row.get(8),
            row.get(9),
            gate_keys,
        )?);
    }
    Ok(out)
}

fn live_items_sql(
    client: &mut Client,
    shard: &QueueKey,
    keys: &[ClientItemKey],
) -> EngineResult<Vec<Option<LiveItemView>>> {
    let (t, q) = parts(shard);
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let row = st(client.query_opt(
            "SELECT item_id, item_version, lifecycle_state, priority, group_key, not_before, \
             retry_count, payload, fields FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=$3 \
               AND superseded=false AND lifecycle_state IN ('Pending','Leased')",
            &[&t, &q, &key.as_str()],
        ))?;
        out.push(match row {
            Some(row) => {
                let id: String = row.get(0);
                let state: String = row.get(2);
                let group: Option<String> = row.get(4);
                let not_before: Option<i64> = row.get(5);
                let payload: Option<Vec<u8>> = row.get(7);
                let fields: String = row.get(8);
                Some(LiveItemView {
                    item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
                    client_item_key: key.clone(),
                    item_version: row.get::<_, i64>(1) as u64,
                    lifecycle_state: parse_state(&state)?,
                    priority: parse_priority(row.get(3))?,
                    group_key: group
                        .map(GroupKey::new)
                        .transpose()
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    not_before: not_before.map(nanos_ts),
                    attempt_count: row.get::<_, i64>(6) as u32,
                    payload: payload.map(Bytes::from),
                    fields: fields_from_json(fields)?,
                })
            }
            None => None,
        });
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
    m.resident_terminal_count = m.complete + m.failed;
    Ok(m)
}

/// Lifecycle state + flags for a BATCH of items in ONE round-trip (was one SELECT per id), keyed by
/// `item_id` string. Absent ids are simply missing from the map (the per-id classifier treats a miss as
/// `NotFound`). Replaces the former per-item `item_flags` helper.
fn item_flags_map(
    client: &mut Client,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<HashMap<String, (ItemState, bool, bool, bool)>> {
    let mut map = HashMap::with_capacity(ids.len());
    if ids.is_empty() {
        return Ok(map);
    }
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let rows = st(client.query(
        "SELECT item_id, lifecycle_state, fenced, superseded, cohort_size IS NOT NULL FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
        &[&t, &q, &id_strs],
    ))?;
    for row in rows {
        let id: String = row.get(0);
        let state = parse_state(&row.get::<_, String>(1))?;
        let fenced: bool = row.get(2);
        let superseded: bool = row.get(3);
        let cohort_member: bool = row.get(4);
        map.insert(id, (state, fenced, superseded, cohort_member));
    }
    Ok(map)
}

/// Shared "present + Leased + not fenced + not superseded + not terminal" check — identical error
/// precedence to `ProjectionData::validate_leased` (finalize/renew/reassign pre-commit). Classifies every
/// id from ONE batched read; precedence is still evaluated per id in request order (first failing id wins),
/// byte-for-byte as the former per-id SELECT loop did.
fn validate_leased(client: &mut Client, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
    let flags = item_flags_map(client, shard, ids)?;
    for id in ids {
        match flags.get(&id.to_string()) {
            None => return Err(EngineError::NotFound),
            Some((_, true, _, _)) => return Err(EngineError::StaleLease),
            Some((s, _, _, _)) if s.is_terminal() => return Err(EngineError::Terminal),
            Some((_, _, true, _)) => return Err(EngineError::Superseded),
            Some((_, _, _, true)) => {
                return Err(EngineError::Invalid("cohort member requires cohort lease"));
            }
            Some((s, _, _, _)) if *s != ItemState::Leased => {
                return Err(EngineError::Invalid("item is not leased"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn validate_cohort_lease(
    client: &mut Client,
    shard: &QueueKey,
    target: &CohortLeaseTarget,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT state, cohort_lease_token_hash FROM pqueue_cohorts \
         WHERE tenant_id=$1 AND queue_id=$2 AND cohort_id=$3",
        &[&t, &q, &target.cohort_id.as_str()],
    ))?;
    let Some(row) = row else {
        return Err(EngineError::NotFound);
    };
    let state: String = row.get(0);
    let hash: Option<Vec<u8>> = row.get(1);
    if state == "terminal" {
        return Err(EngineError::Terminal);
    }
    if state != "leased" {
        return Err(EngineError::Invalid("cohort is not leased"));
    }
    if hash.as_deref() != Some(lease_hash(&target.cohort_lease_token).as_slice()) {
        return Err(EngineError::StaleLease);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PostgresRelationalBackend
// ---------------------------------------------------------------------------

/// Postgres-backed **relational** projection family (`pqueue_items` is a rebuildable projection cache).
/// Atomic class.
pub struct PostgresRelationalBackend {
    inner: Mutex<Inner>,
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009). `0` single-instance.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see [`QueueCounters`].
    counters: QueueCounters,
}

impl PostgresRelationalBackend {
    /// Connect to `url` (default `search_path`), ensure the schema, and load the queue-def cache.
    pub fn connect(url: &str) -> EngineResult<Self> {
        let client = connect(PostgresConnectConfig::new(url))?;
        Self::from_client(client)
    }

    /// Connect isolated in a dedicated `schema` (the postgres analogue of a fresh sqlite file).
    /// Reconnecting with the SAME `schema` reopens the same rebuildable projection cache — used by the
    /// conformance + relational-reconnect suites.
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
        st(client.batch_execute(RELATIONAL_SCHEMA))?;
        st(client.batch_execute(
            "ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS fields TEXT NOT NULL DEFAULT '{}';",
        ))?;
        st(client.batch_execute(
            "ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS metadata TEXT NOT NULL DEFAULT '{}';",
        ))?;
        st(client.batch_execute(
            "ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS cohort_id TEXT;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS member_count BIGINT NOT NULL DEFAULT 0;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'forming';\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS cohort_created_at BIGINT;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS first_eligible_at BIGINT;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS expire_command_pos BIGINT;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS cohort_lease_token_hash BYTEA;\
             ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS retention_until BIGINT;\
             UPDATE pqueue_cohorts SET cohort_id=group_key WHERE cohort_id IS NULL;\
             UPDATE pqueue_cohorts SET cohort_created_at=created_at WHERE cohort_created_at IS NULL;\
             UPDATE pqueue_cohorts c SET member_count=(SELECT COUNT(*) FROM pqueue_items i \
               WHERE i.tenant_id=c.tenant_id AND i.queue_id=c.queue_id AND i.group_key=c.group_key \
               AND i.superseded=false AND i.cohort_size IS NOT NULL \
               AND i.lifecycle_state NOT IN ('Complete','Failed'));\
             UPDATE pqueue_cohorts SET state=CASE WHEN member_count >= cohort_size THEN 'complete' ELSE 'forming' END \
               WHERE state IS NULL OR state='forming' OR state='complete';\
             CREATE INDEX IF NOT EXISTS pqueue_cohorts_claim_idx \
               ON pqueue_cohorts (tenant_id, queue_id, state) WHERE state='complete';\
             CREATE INDEX IF NOT EXISTS pqueue_cohorts_expiry_idx \
               ON pqueue_cohorts (tenant_id, queue_id, cohort_created_at) \
               WHERE state IN ('forming','complete');",
        ))?;
        let mut inner = Inner {
            client,
            queues: HashMap::new(),
            schemas: HashMap::new(),
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
    /// after reconnect never re-mints an existing item id (the durable items table is the authority — there
    /// is no log to replay). `observe` decodes `(epoch, counter)` from each packed id and only advances.
    fn restore_counters(&self) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("poisoned");
        let rows = st(g
            .client
            .query("SELECT tenant_id, queue_id, item_id FROM pqueue_items", &[]))?;
        for row in rows {
            let t: String = row.get(0);
            let q: String = row.get(1);
            let id: String = row.get(2);
            let key = QueueKey::new(
                TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
            );
            let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
            self.counters.observe(&key, item_id);
        }
        Ok(())
    }

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`]
    /// so distinct nodes competing for one queue never mint a colliding id (ADR-009).
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
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
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let (t, q) = parts(shard);
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        let epoch = {
            let mut tx = self.tx.borrow_mut();
            let epoch: i64 = st(tx.query_opt(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?
            .get(0);
            epoch as u64
        };
        if expected_epoch != epoch {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for _ in commands {
            let mut tx = self.tx.borrow_mut();
            let seq = alloc_seq(&mut tx, &t, &q)?;
            positions.push(CommandPosition::new(shard.clone(), epoch, seq));
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

    fn supports_gates(&self) -> bool {
        true
    }

    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: false,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic postgres transaction over the relational projection",
        }
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
            // Compile the entity schema once at create time (ADR-011).
            let compiled_schema = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
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
            if let Some(cs) = compiled_schema {
                g.schemas.insert(key.clone(), cs);
            }
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
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let epoch: i64 = st(g.client.query_opt(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?
            .get(0);
            Ok(epoch as u64)
        })();
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let (t, q) = parts(shard);
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // TD-003 acquire: strictly-greater epoch, durably recorded (the fence authority advances).
            let epoch: i64 = st(g.client.query_opt(
                "UPDATE relational_cursor SET assignment_epoch = assignment_epoch + 1 \
                 WHERE tenant=$1 AND queue=$2 RETURNING assignment_epoch",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?
            .get(0);
            Ok(epoch as u64)
        })();
        std::future::ready(result)
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

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            live_items_sql(&mut g.client, shard, keys)
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

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            metrics_sql(&mut g.client, shard).map(|metrics| TerminalEmissionMetrics {
                resident_terminal_count: metrics.resident_terminal_count,
                emission_lag_commands: 0,
                emission_oldest_unemitted_age_ms: 0,
            })
        };
        std::future::ready(result)
    }
}

/// ADR-011 (pqueue-f4ffd679): typed secondary index queries backed by `pqueue_item_index`.
impl IndexQueryPort for PostgresRelationalBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?
                .clone();
            if !index_is_unique(&qi) {
                return Err(EngineError::Invalid("secondary index is not unique"));
            }
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(&qi, key)?;
            let (t, q) = parts(shard);
            let row = st(g.client.query_opt(
                "SELECT i.item_id, i.client_item_key, i.item_version \
                 FROM pqueue_item_index idx \
                 JOIN pqueue_items i \
                   ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                  AND i.item_id=idx.item_id \
                 WHERE idx.tenant_id=$1 AND idx.queue_id=$2 \
                   AND idx.index_name=$3 AND idx.index_key=$4 \
                 LIMIT 1",
                &[&t, &q, &index, &canonical.as_slice()],
            ))?;
            Ok(row.map(|row| {
                let id_str: String = row.get(0);
                let ck_str: String = row.get(1);
                let ver: i64 = row.get(2);
                IndexHit {
                    item_id: ItemId::new(id_str).expect("valid stored item_id"),
                    client_item_key: ClientItemKey::new(ck_str)
                        .expect("valid stored client_item_key"),
                    item_version: ver as u64,
                }
            }))
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
            let mut g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?
                .clone();
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(&qi, key)?;
            let (t, q) = parts(shard);
            let rows = st(g.client.query(
                "SELECT i.item_id, i.client_item_key, i.item_version \
                 FROM pqueue_item_index idx \
                 JOIN pqueue_items i \
                   ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                  AND i.item_id=idx.item_id \
                 WHERE idx.tenant_id=$1 AND idx.queue_id=$2 \
                   AND idx.index_name=$3 AND idx.index_key=$4 \
                 ORDER BY i.item_id",
                &[&t, &q, &index, &canonical.as_slice()],
            ))?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let id_str: String = row.get(0);
                let ck_str: String = row.get(1);
                let ver: i64 = row.get(2);
                out.push(IndexHit {
                    item_id: ItemId::new(id_str)
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    client_item_key: ClientItemKey::new(ck_str)
                        .map_err(|e| EngineError::Storage(e.to_string()))?,
                    item_version: ver as u64,
                });
            }
            Ok(out)
        })();
        std::future::ready(result)
    }
}

impl DiscoveryPort for PostgresRelationalBackend {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ActiveScope>>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            discover_active_scopes_sql(&mut g.client, shard, granularity, now)
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
        // Fence threading for this backend family is deferred (B1b continuation); accepted for the port
        // contract so the owner fence is uniform once the relational/object write paths thread it.
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
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
                now,
                expected_epoch,
            )?;
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
            let mut g = self.inner.lock().expect("poisoned");
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
            let fingerprint = push_request_fingerprint(&items)?;
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let expires_at = request_expires_at(&g.queues, shard, now)?;
            let Inner {
                client,
                queues,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let mut tx = st(client.transaction())?;
            let cursor_epoch: i64 = st(tx.query_opt(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?
            .get(0);
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            if let Some(ids) = check_request_idempotency(
                &mut tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                ts_nanos(now),
            )? {
                return Ok(ids);
            }
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let seq = alloc_seq(&mut tx, &t, &q)?;
            let mut token_ops = Vec::new();
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                shard,
                seq,
                now,
                &QueueCommand::Push(PushCommand { items: push_items }),
            )?;
            record_request_idempotency(
                &mut tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                &ids,
                now,
                expires_at,
            )?;
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops);
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl pqueue_engine::ReschedulePort for PostgresRelationalBackend {}

impl SetGatesPort for PostgresRelationalBackend {
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            g.commit_command(shard, QueueCommand::SetGates(command), now, expected_epoch)
        };
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
            // BQ-14a/b: resolve the claim unit. Item-level (default) keeps the CTE path; WholeGroup /
            // SameGroupKey select group-aware from pqueue_group_summary; WholeCohort → Unavailable (BQ-14c).
            let unit = if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                validate_claim_compatibility(&req.compatibility, req.max_items as u64, def)?
            } else {
                ClaimUnit::Item
            };
            // Paused queues yield nothing (neither the CTE nor the group/cohort selection encodes pause).
            if queue_paused(&mut g.client, &req.shard)? {
                return Ok(Claimed::default());
            }
            let Inner {
                client,
                queues,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(&req.shard);
            let mut tx = st(client.transaction())?;
            // ADR-009 / TD-003 fence: a superseded owner is rejected BEFORE selecting/leasing — nothing is
            // claimed. `None` = sole-owner (no fence). The assignment_epoch is the BQ-23 single durable value.
            let claim_epoch: i64 = st(tx.query_one(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .get(0);
            if req.expected_epoch.is_some_and(|e| e != claim_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            let seq = alloc_seq(&mut tx, &t, &q)?;
            let now_n = ts_nanos(req.now);
            let exp = ts_nanos(req.lease_expires_at);
            let hash = lease_hash(&req.lease_token);
            let seqi = seq as i64;

            if matches!(unit, ClaimUnit::Item) {
                // Item-level: the serialized FOR UPDATE SKIP LOCKED CTE (select + lease + RETURNING).
                let lim = req.max_items as i64;
                let rows = st(tx.query(
                    CLAIM_CTE,
                    &[&t, &q, &now_n, &lim, &hash, &exp, &now_n, &seqi],
                ))?;
                if rows.is_empty() {
                    return Ok(Claimed::default()); // roll back — no sequence burned (sqlite parity)
                }
                let mut claimed_ids = Vec::with_capacity(rows.len());
                for row in &rows {
                    let id: String = row.get(0);
                    claimed_ids
                        .push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
                }
                let mut gate_keys_by_id = item_gate_keys_by_id(&mut tx, &req.shard, &claimed_ids)?;
                let mut items = Vec::with_capacity(rows.len());
                let mut token_ops = Vec::new();
                for (row, item_id) in rows.into_iter().zip(claimed_ids.iter().copied()) {
                    let exp_row: Option<i64> = row.get(6);
                    let exp_row = exp_row.unwrap_or(exp);
                    let gate_keys = gate_keys_by_id
                        .remove(&item_id.to_string())
                        .unwrap_or_default();
                    items.push(claimed_from_row(
                        item_id,
                        req.lease_token.clone(),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                        row.get(5),
                        exp_row,
                        row.get(7),
                        row.get(8),
                        row.get(9),
                        row.get(10),
                        gate_keys,
                    )?);
                    token_ops.push(TokenOp::Set(item_id, req.lease_token.clone()));
                }
                // The CTE bypasses apply_command_sql's Claim arm, so refresh the claimed groups here.
                for grp in groups_of(&mut tx, &req.shard, &claimed_ids)? {
                    refresh_group_summary(&mut tx, &req.shard, &grp, req.now)?;
                }
                st(tx.commit())?;
                apply_token_ops(live_tokens, token_ops);
                return Ok(Claimed {
                    items,
                    ..Default::default()
                });
            }

            // Group-aware: gather the candidate items under the per-group FOR UPDATE SKIP LOCKED lock, then
            // lease them via apply_command_sql's Claim arm (which UPDATEs + refreshes the affected groups).
            let mut selected_cohort: Option<CohortId> = None;
            let candidates = match unit {
                ClaimUnit::WholeGroup => {
                    let max_groups = req
                        .compatibility
                        .group_batching
                        .as_ref()
                        .map(|gb| gb.max_groups)
                        .unwrap_or(0);
                    select_group_batching(&mut tx, &req.shard, req.now, req.max_items, max_groups)?
                }
                ClaimUnit::SameGroupKey => {
                    select_same_group(&mut tx, &req.shard, req.now, req.max_items)?
                }
                ClaimUnit::WholeCohort => {
                    match select_whole_cohort(&mut tx, &req.shard, req.now, req.max_items)? {
                        Some(selected) => {
                            selected_cohort = Some(selected.cohort_id);
                            selected.item_ids
                        }
                        None => Vec::new(),
                    }
                }
                ClaimUnit::Item => unreachable!("Item handled by the CTE path above"),
            };
            if candidates.is_empty() {
                return Ok(Claimed::default()); // roll back — no sequence burned
            }
            let mut token_ops = Vec::new();
            let claim_command = if let Some(cohort_id) = selected_cohort.clone() {
                QueueCommand::CohortClaim(CohortClaimCommand {
                    cohort_id,
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                })
            } else {
                QueueCommand::Claim(ClaimCommand {
                    item_ids: candidates.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                })
            };
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                &req.shard,
                seq,
                req.now,
                &claim_command,
            )?;
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops); // tokens live only after the durable commit
            // Render from the now-committed leased rows (the tx released the client on commit); the live
            // tokens we just applied resolve each id's token.
            let items = render_claimed(client, &req.shard, &candidates, |id| {
                live_tokens.get(id).cloned()
            })?;
            let mut claimed = Claimed {
                items,
                ..Default::default()
            };
            if matches!(unit, ClaimUnit::WholeCohort) {
                claimed.cohort_lease_token = Some(req.lease_token.clone());
                let _ = apply_whole_cohort_response_shape(&mut claimed.items);
                claimed.cohort_id = selected_cohort;
            }
            Ok(claimed)
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
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-commit entity schema validation (ADR-011): reject before any mutation.
            validate_entity(g.schemas.get(shard), entity.as_ref())?;
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
                entity_document: entity,
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
                        expected_epoch,
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
                                    superseded_item_id: existing_id,
                                    replacement: item,
                                }),
                                now,
                                expected_epoch,
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

impl CommitTransitionPort for PostgresRelationalBackend {
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<CommitEntryOutcome>>> + Send {
        let result = (|| {
            let CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_request_fingerprint(&entries)?;
            let mut g = self.inner.lock().expect("poisoned");
            let max_attempts = g
                .queues
                .get(shard)
                .map(|d| d.retry_policy.max_attempts)
                .ok_or(EngineError::NotFound)?;
            let expires_at = request_expires_at(&g.queues, shard, now)?;
            let epoch = expected_epoch.unwrap_or(0);
            let schema = g.schemas.get(shard).cloned();
            let Inner {
                client,
                queues,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let mut tx = st(client.transaction())?;
            let row = st(tx.query_opt(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?;
            let seq0: i64 = row.get(0);
            let cursor_epoch: i64 = row.get(1);
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            if let Some(rid) = &request_id
                && let Some(stored) =
                    check_commit_idempotency(&mut tx, shard, rid, &fingerprint, ts_nanos(now))?
            {
                return Ok(recovery_to_outcomes(&stored));
            }

            let mut seq = seq0 as u64;
            let mut token_ops = Vec::new();

            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            for entry in entries {
                let consumed_input_id = entry.claim_ref.item_id;
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };
                if let Err(e) = commit_validate_sql(&mut tx, shard, &entry.claim_ref, now) {
                    recovery.push(reject(e));
                    continue;
                }
                if let Some(fence) = &entry.instance_fence {
                    let (it, iq) = parts(shard);
                    let row = st(tx.query_opt(
                        "SELECT fence FROM pqueue_instance_fences \
                             WHERE tenant_id=$1 AND queue_id=$2 AND instance_key=$3",
                        &[&it, &iq, &fence.instance_key],
                    ))?;
                    let stored: i64 = row.map(|row| row.get(0)).unwrap_or(0);
                    if let Err(e) = validate_instance_fence(stored as u64, fence) {
                        recovery.push(reject(e));
                        continue;
                    }
                }
                if !entry.lifecycle_items.is_empty()
                    && let Some(e) = entry.lifecycle_items.iter().find_map(|item| {
                        validate_entity(schema.as_ref(), item.entity.as_ref()).err()
                    })
                {
                    recovery.push(reject(e));
                    continue;
                }

                let side_record_keys: Vec<Vec<u8>> =
                    entry.side_records.iter().map(|r| r.key.clone()).collect();
                let instance = entry
                    .instance_fence
                    .as_ref()
                    .map(|f| (f.instance_key.clone(), f.next));

                if !entry.side_records.is_empty() {
                    apply_command_sql(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        shard,
                        seq,
                        now,
                        &QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: entry.side_records,
                        }),
                    )?;
                    seq += 1;
                }
                if let Some(fence) = entry.instance_fence {
                    apply_command_sql(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        shard,
                        seq,
                        now,
                        &QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                    )?;
                    seq += 1;
                }
                let mut lifecycle_item_ids = Vec::new();
                if !entry.lifecycle_items.is_empty() {
                    let counter_base =
                        self.counters
                            .reserve(shard, epoch, entry.lifecycle_items.len() as u32);
                    let (push_items, ids) = build_push_items(
                        entry.lifecycle_items,
                        epoch,
                        self.node_id,
                        counter_base,
                        max_attempts,
                    );
                    lifecycle_item_ids = ids;
                    apply_command_sql(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        shard,
                        seq,
                        now,
                        &QueueCommand::Push(PushCommand { items: push_items }),
                    )?;
                    seq += 1;
                }
                apply_command_sql(
                    &mut tx,
                    queues,
                    &mut token_ops,
                    shard,
                    seq,
                    now,
                    &QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(
                            entry.claim_ref.item_id,
                            entry.finalize,
                        )],
                    }),
                )?;
                seq += 1;
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }
            let outcomes = recovery_to_outcomes(&recovery);

            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2",
                &[&t, &q, &(seq as i64)],
            ))?;
            if let Some(rid) = &request_id {
                record_commit_idempotency(
                    &mut tx,
                    shard,
                    rid,
                    &fingerprint,
                    &recovery,
                    now,
                    expires_at,
                )?;
            }
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops);
            Ok(outcomes)
        })();
        std::future::ready(result)
    }
}

impl RecoveryReadPort for PostgresRelationalBackend {
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let entries = read_commit_recovery(&mut g.client, shard, &request_id)?;
            Ok(entries.map(|entries| CommitRecovery {
                request_id,
                entries,
            }))
        })();
        std::future::ready(result)
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let payload: Option<Vec<u8>> = st(g.client.query_opt(
                "SELECT payload FROM pqueue_side_records \
                 WHERE tenant_id=$1 AND queue_id=$2 AND key=$3",
                &[&t, &q, &key],
            ))?
            .map(|row| row.get(0));
            Ok(payload.map(Bytes::from))
        })();
        std::future::ready(result)
    }
}

/// Hot projection query substrate (API-004) is not implemented for any backend in epic pqueue-45e13e4d;
/// the postgres-relational family inherits the all-`Unavailable` default.
impl pqueue_engine::HotProjectionQueryPort for PostgresRelationalBackend {}

impl FinalizePort for PostgresRelationalBackend {
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
            validate_leased(&mut g.client, shard, &ids)?;
            g.commit_command(
                shard,
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortFinalizePort for PostgresRelationalBackend {
    fn finalize_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            if matches!(kind, FinalizeKind::Rearm) {
                return Err(EngineError::Invalid("cohort rearm is invalid"));
            }
            let mut g = self.inner.lock().expect("poisoned");
            validate_cohort_lease(&mut g.client, shard, &target)?;
            g.commit_command(
                shard,
                QueueCommand::CohortFinalize(CohortFinalizeCommand {
                    cohort_id: target.cohort_id,
                    kind,
                    not_before,
                }),
                now,
                expected_epoch,
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
        expected_epoch: Option<u64>,
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
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortRenewLeasePort for PostgresRelationalBackend {
    fn renew_cohort(
        &self,
        shard: &QueueKey,
        target: CohortLeaseTarget,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            validate_cohort_lease(&mut g.client, shard, &target)?;
            g.commit_command(
                shard,
                QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
                    cohort_id: target.cohort_id,
                    lease_expires_at: new_lease_expires_at,
                }),
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl UpdateFieldsPort for PostgresRelationalBackend {
    #[allow(clippy::too_many_arguments)]
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let id_str = item_id.to_string();
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-commit entity schema validation (ADR-011): reject before any mutation.
            validate_entity(g.schemas.get(shard), entity.as_ref())?;
            // Pre-validate exactly like the in-memory `update_fields_validate`: absent=NotFound,
            // fenced=StaleLease, terminal=Terminal, superseded=Superseded, version-mismatch=Conflict.
            // Nothing is appended on rejection (commit has no rollback).
            let row = st(g.client.query_opt(
                "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&t, &q, &id_str],
            ))?
            .ok_or(EngineError::NotFound)?;
            let state = parse_state(&row.get::<_, String>(0))?;
            let superseded: bool = row.get(1);
            let fenced: bool = row.get(2);
            let version: i64 = row.get(3);
            if fenced {
                return Err(EngineError::StaleLease);
            }
            if state.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded {
                return Err(EngineError::Superseded);
            }
            if let Some(v) = expected_item_version
                && version as u64 != v
            {
                return Err(EngineError::Conflict);
            }
            g.commit_command(
                shard,
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops,
                    payload,
                    set_priority: Default::default(),
                    set_not_before: Default::default(),
                    set_entity_document: entity,
                }),
                now,
                expected_epoch,
            )?;
            // Re-read the bumped version from the now-committed projection.
            let new_version: i64 = st(g.client.query_one(
                "SELECT item_version FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&t, &q, &id_str],
            ))?
            .get(0);
            Ok(new_version as u64)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for PostgresRelationalBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let (t, q) = parts(shard);
            let now_n = ts_nanos(now);
            let mut g = self.inner.lock().expect("poisoned");
            // This queue's leases that expired strictly before `now` (FAC-2); LIMIT caps the batch.
            let rows = match limit {
                Some(lim) => st(g.client.query(
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
                     AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                     AND lease_expires_at<$3 ORDER BY item_id LIMIT $4",
                    &[&t, &q, &now_n, &(lim as i64)],
                ))?,
                None => st(g.client.query(
                    "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
                     AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                     AND lease_expires_at<$3 ORDER BY item_id",
                    &[&t, &q, &now_n],
                ))?,
            };
            let mut ids = Vec::with_capacity(rows.len());
            for row in rows {
                let id: String = row.get(0);
                ids.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
            }
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED by `expected_epoch` (unlike the global ReclaimDriver::tick, which is None).
            g.commit_command(
                shard,
                QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                }),
                now,
                expected_epoch,
            )?;
            Ok(ids)
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
        expected_epoch: Option<u64>,
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
                expected_epoch,
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
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // Classify every candidate from ONE batched read (was one SELECT per id), then preserve the
            // exact in-order, deduped, force-gated `present` set the per-item loop produced.
            let flags = item_flags_map(&mut g.client, shard, &item_ids)?;
            let mut present: Vec<ItemId> = Vec::new();
            for id in &item_ids {
                if present.contains(id) {
                    continue;
                }
                if let Some((state, _, _, _)) = flags.get(&id.to_string()) {
                    validate_purge_force(*state == ItemState::Leased, force)?;
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
                now,
                expected_epoch,
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
                    None,
                )?;
            }
            let rows = st(g.client.query(
                "SELECT c.tenant_id, c.queue_id, c.group_key, c.cohort_created_at, \
                 c.first_eligible_at, r.assignment_epoch \
                 FROM pqueue_cohorts c \
                 JOIN relational_cursor r ON r.tenant=c.tenant_id AND r.queue=c.queue_id \
                 WHERE c.state IN ('forming','complete') \
                 ORDER BY c.tenant_id, c.queue_id, c.group_key \
                 LIMIT $1",
                &[&COHORT_EXPIRY_SWEEP_LIMIT],
            ))?;
            let mut due_cohorts: Vec<(QueueKey, GroupKey, u64)> = Vec::new();
            for row in rows {
                let t: String = row.get(0);
                let q: String = row.get(1);
                let group: String = row.get(2);
                let cohort_created_at: i64 = row.get(3);
                let first_eligible_at: Option<i64> = row.get(4);
                let epoch: i64 = row.get(5);
                let shard = QueueKey::new(
                    TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                    QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
                );
                let Some(definition) = g.queues.get(&shard) else {
                    continue;
                };
                let Some(deadline) =
                    cohort_expiry_deadline(definition, cohort_created_at, first_eligible_at)
                else {
                    continue;
                };
                if deadline <= now_n {
                    due_cohorts.push((
                        shard,
                        GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))?,
                        epoch as u64,
                    ));
                }
            }
            for (shard, group_key, epoch) in due_cohorts {
                g.commit_command(
                    &shard,
                    QueueCommand::CohortExpired(CohortExpiredCommand { group_key }),
                    now,
                    Some(epoch),
                )?;
                report.cohorts_expired += 1;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

// ===========================================================================
// ADR-012 P1b-ii: the UNIFIED postgres-relational store as `LogStore + ProjectionStore`
// ===========================================================================
//
// The postgres half of the keystone "same robustness as flat postgres" composition: ONE store value
// implements BOTH axes, so the generic [`ComposedBackend::commit_locked`] drives append+apply into ONE
// durable postgres transaction with NO phantom log row (ADR-012 §"The atomic write seam"). Same mechanism
// as the sqlite unified store:
//   * [`LogStore::append`] STAGES — read `relational_cursor` (next_seq + assignment_epoch), TD-003 fence,
//     MINT positions. No durable write, no cursor advance.
//   * [`ProjectionStore::apply`] COMMITS — one postgres transaction: the 14-arm `apply_command_sql` at the
//     minted positions + the cursor `next_seq` advance, then post-commit live-token maintenance.
// The unified relational store still only covers the log/projection seam; the Snorri commit boundary is
// provided by the `PostgresRelationalBackend` facade below, which opts in separately.

/// Active (non-superseded) item id under `client_item_key`, or `None` (the generic upsert look-then-replace).
fn lookup_active_by_key(
    client: &mut Client,
    shard: &QueueKey,
    client_item_key: &ClientItemKey,
) -> EngineResult<Option<ItemId>> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT item_id FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=$3 AND superseded=false",
        &[&t, &q, &client_item_key.as_str()],
    ))?;
    row.map(|row| {
        ItemId::new(row.get::<_, String>(0)).map_err(|e| EngineError::Storage(e.to_string()))
    })
    .transpose()
}

/// Lifecycle state of `id`, or `None` if absent.
fn item_state_sql(
    client: &mut Client,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<ItemState>> {
    Ok(item_flags_map(client, shard, std::slice::from_ref(id))?
        .get(&id.to_string())
        .map(|(s, _, _, _)| *s))
}

/// Committed `item_version` of `id`, or `None` if absent.
fn item_version_sql(
    client: &mut Client,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT item_version FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
        &[&t, &q, &id.to_string()],
    ))?;
    Ok(row.map(|row| row.get::<_, i64>(0) as u64))
}

/// This queue's leases expired strictly before `now` (half-open), ordered by item id (the generic
/// `reclaim_expired` truncates to its `limit`).
fn expired_leases_sql(
    client: &mut Client,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let rows = st(client.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
         AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND lease_expires_at<$3 ORDER BY item_id",
        &[&t, &q, &now_n],
    ))?;
    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        ids.push(
            ItemId::new(row.get::<_, String>(0))
                .map_err(|e| EngineError::Storage(e.to_string()))?,
        );
    }
    Ok(ids)
}

/// Every queue's expired leases at `now` (the global tick sweep), grouped per queue.
fn all_expired_leases_sql(
    client: &mut Client,
    now: UtcTimestamp,
) -> EngineResult<Vec<(QueueKey, Vec<ItemId>)>> {
    let now_n = ts_nanos(now);
    let rows = st(client.query(
        "SELECT tenant_id, queue_id, item_id FROM pqueue_items \
         WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND lease_expires_at<$1 ORDER BY tenant_id, queue_id",
        &[&now_n],
    ))?;
    let mut by_queue: Vec<(QueueKey, Vec<ItemId>)> = Vec::new();
    for row in rows {
        let key = QueueKey::new(
            TenantId::new(row.get::<_, String>(0))
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            QueueId::new(row.get::<_, String>(1))
                .map_err(|e| EngineError::Storage(e.to_string()))?,
        );
        let id = ItemId::new(row.get::<_, String>(2))
            .map_err(|e| EngineError::Storage(e.to_string()))?;
        match by_queue.last_mut() {
            Some((k, ids)) if *k == key => ids.push(id),
            _ => by_queue.push((key, vec![id])),
        }
    }
    Ok(by_queue)
}

/// Terminal records that are past both the retention window and, for emit-enabled queues, the durable
/// emission frontier. `emit_change_records=false` keeps the retention-only opt-out path.
fn reap_terminal_items_sql(
    client: &mut impl GenericClient,
    shard: &QueueKey,
    now: UtcTimestamp,
    terminal_retention_ms: u64,
    emit_change_records: bool,
    emission_cursor: Option<&CommandPosition>,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let cutoff = now_n.saturating_sub((terminal_retention_ms as i64).saturating_mul(1_000_000));
    let rows = if emit_change_records {
        let Some(cursor) = emission_cursor else {
            return Ok(Vec::new());
        };
        let cursor_seq = cursor.sequence as i64;
        st(client.query(
            "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
             AND superseded=false AND lifecycle_state IN ('Complete','Failed') \
             AND terminal_at IS NOT NULL AND terminal_at<=$3 \
             AND last_command_sequence<=$4 ORDER BY item_id",
            &[&t, &q, &cutoff, &cursor_seq],
        ))?
    } else {
        st(client.query(
            "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
             AND superseded=false AND lifecycle_state IN ('Complete','Failed') \
             AND terminal_at IS NOT NULL AND terminal_at<=$3 ORDER BY item_id",
            &[&t, &q, &cutoff],
        ))?
    };
    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        ids.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let id_strs: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    st(client.execute(
        "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
        &[&t, &q, &id_strs],
    ))?;
    st(client.execute(
        "DELETE FROM pqueue_item_gates WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
        &[&t, &q, &id_strs],
    ))?;
    delete_typed_index_rows(client, &t, &q, &id_strs)?;
    Ok(ids)
}

/// In-place field/payload update pre-commit validation (absent → `NotFound`, fenced → `StaleLease`,
/// terminal → `Terminal`, superseded → `Superseded`, version mismatch → `Conflict`). Mutates nothing.
fn update_fields_validate_sql(
    client: &mut Client,
    shard: &QueueKey,
    id: &ItemId,
    expected_item_version: Option<u64>,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
        &[&t, &q, &id.to_string()],
    ))?
    .ok_or(EngineError::NotFound)?;
    let state = parse_state(&row.get::<_, String>(0))?;
    let superseded: bool = row.get(1);
    let fenced: bool = row.get(2);
    let version: i64 = row.get(3);
    if fenced {
        return Err(EngineError::StaleLease);
    }
    if state.is_terminal() {
        return Err(EngineError::Terminal);
    }
    if superseded {
        return Err(EngineError::Superseded);
    }
    if expected_item_version.is_some_and(|v| v != version as u64) {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

/// The unified postgres-relational store: ONE value, shared behind `Arc<Mutex<Inner>>`, that implements BOTH
/// the [`LogStore`] and [`ProjectionStore`] axes of [`ComposedBackend`]. Two clones (one per axis field)
/// point at the same `Inner` (one `Client`), so `commit_locked`'s append→apply is one transactional unit.
#[derive(Clone)]
pub struct PostgresRelational {
    inner: Arc<Mutex<Inner>>,
}

impl PostgresRelational {
    /// Connect to `url`, ensure the schema, and load the queue-def cache.
    pub fn connect(url: &str) -> EngineResult<Self> {
        let client = connect(PostgresConnectConfig::new(url))?;
        Self::from_client(client)
    }

    /// Connect isolated in a dedicated `schema` (the postgres analogue of a fresh sqlite file). Reconnecting
    /// with the SAME `schema` reopens the same rebuildable projection cache.
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
        st(client.batch_execute(RELATIONAL_SCHEMA))?;
        st(client.batch_execute(
            "ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS fields TEXT NOT NULL DEFAULT '{}';\
             ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS metadata TEXT NOT NULL DEFAULT '{}';",
        ))?;
        let mut inner = Inner {
            client,
            queues: HashMap::new(),
            schemas: HashMap::new(),
            live_tokens: HashMap::new(),
        };
        inner.reload()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("postgres relational store poisoned")
    }
}

impl LogStore for PostgresRelational {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn ensure_shard(&mut self, _shard: &QueueKey) -> EngineResult<()> {
        // The durable cursor/queue rows are created by the projection axis' `ensure_shard`.
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let epoch: i64 = st(g.client.query_opt(
            "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?
        .get(0);
        Ok(epoch as u64)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let epoch: i64 = st(g.client.query_opt(
            "UPDATE relational_cursor SET assignment_epoch = assignment_epoch + 1 \
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
        // STAGE only: read cursor + assignment_epoch, fence, MINT positions. No durable write — the apply
        // axis advances the cursor inside its own transaction (no phantom log row).
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row = st(g.client.query_opt(
            "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?;
        let next: i64 = row.get(0);
        let epoch: i64 = row.get(1);
        if expected_epoch != epoch as u64 {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for (i, _) in commands.iter().enumerate() {
            positions.push(CommandPosition::new(
                shard.clone(),
                epoch as u64,
                (next as u64) + i as u64,
            ));
        }
        Ok(positions)
    }

    fn read_from(
        &self,
        _shard: &QueueKey,
        _from: Option<CommandPosition>,
        _limit: usize,
    ) -> EngineResult<CommandPage> {
        // DB-authoritative: no replayable command log (the CORE class never reads it).
        Ok(CommandPage {
            entries: Vec::new(),
            next: None,
        })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row = st(g.client.query_opt(
            "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        Ok(row.and_then(|row| {
            let next: i64 = row.get(0);
            let epoch: i64 = row.get(1);
            (next > 0).then(|| CommandPosition::new(shard.clone(), epoch as u64, (next as u64) - 1))
        }))
    }

    fn emission_cursor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row: Option<postgres::Row> = st(g.client.query_opt(
            "SELECT epoch, seq FROM relational_emission_cursor WHERE tenant=$1 AND queue=$2",
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
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row: Option<postgres::Row> = st(g.client.query_opt(
            "SELECT epoch, seq FROM relational_emission_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        if let Some(row) = row {
            let epoch: i64 = row.get(0);
            let seq: i64 = row.get(1);
            let current = CommandPosition::new(shard.clone(), epoch as u64, seq as u64);
            if !current.precedes(&position) && current != position {
                return Err(EngineError::Invalid("emission cursor regression"));
            }
        }
        st(g.client.execute(
            "INSERT INTO relational_emission_cursor(tenant,queue,epoch,seq) VALUES($1,$2,$3,$4) \
             ON CONFLICT (tenant,queue) DO UPDATE SET epoch=EXCLUDED.epoch, seq=EXCLUDED.seq",
            &[
                &t,
                &q,
                &(position.backend_epoch as i64),
                &(position.sequence as i64),
            ],
        ))?;
        Ok(())
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let next = position
            .sequence
            .checked_add(1)
            .ok_or(EngineError::Invalid("high-water overflow"))?;
        let row: Option<postgres::Row> = st(g.client.query_opt(
            "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?;
        let Some(row) = row else {
            return Err(EngineError::NotFound);
        };
        let next_seq: i64 = row.get(0);
        let epoch: i64 = row.get(1);
        if epoch as u64 != position.backend_epoch {
            return Err(EngineError::EpochFenced);
        }
        let next = next as i64;
        if next_seq > next {
            return Err(EngineError::Invalid("high-water regression"));
        }
        if next_seq < next {
            st(g.client.execute(
                "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2 AND next_seq<$3",
                &[&t, &q, &next],
            ))?;
        }
        Ok(())
    }

    fn write_snapshot(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
        _snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        Err(EngineError::Unavailable)
    }

    fn latest_snapshot(&self, _shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        Ok(None)
    }

    fn read_snapshot(&self, _snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        Err(EngineError::Unavailable)
    }
}

impl HistoricalProjectionRead for PostgresRelationalBackend {
    type AsOfProjection = PostgresRelational;

    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<CommandPosition>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let row: Option<postgres::Row> = st(g.client.query_opt(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            row.and_then(|row| {
                let next: i64 = row.get(0);
                let epoch: i64 = row.get(1);
                (next > 0)
                    .then(|| CommandPosition::new(shard.clone(), epoch as u64, (next as u64) - 1))
            })
            .ok_or(EngineError::NotFound)
        })();
        std::future::ready(result)
    }

    fn read_as_of<T, F>(
        &self,
        _shard: &QueueKey,
        _position: CommandPosition,
        _query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send,
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ProjectionStore for PostgresRelational {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let mut g = self.lock();
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        if g.queues.contains_key(&key) {
            return Ok(());
        }
        let (t, q) = parts(&key);
        let def_json = to_json(definition)?;
        st(g.client.execute(
            "INSERT INTO queues(tenant,queue,definition,paused) VALUES($1,$2,$3,false) \
             ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q, &def_json],
        ))?;
        st(g.client.execute(
            "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES($1,$2,0,0) \
             ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q],
        ))?;
        g.queues.insert(key, definition.clone());
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // COMMIT: one postgres transaction — apply each command at its minted position, then advance each
        // touched queue's cursor to max(seq)+1. `append` left the cursor untouched, so a crash here leaves
        // the cursor behind the (un-applied) work, never ahead of it.
        if positions.is_empty() {
            return Ok(());
        }
        let mut g = self.lock();
        let Inner {
            client,
            queues,
            live_tokens,
            ..
        } = &mut *g;
        let mut tx = st(client.transaction())?;
        let mut token_ops = Vec::new();
        let mut max_seq: HashMap<QueueKey, u64> = HashMap::new();
        for (pos, env) in positions.iter().zip(commands) {
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                &pos.queue,
                pos.sequence,
                env.created_at,
                &env.command,
            )?;
            let slot = max_seq.entry(pos.queue.clone()).or_insert(pos.sequence);
            if pos.sequence > *slot {
                *slot = pos.sequence;
            }
        }
        for (queue, &seq) in &max_seq {
            let (t, q) = parts(queue);
            let next = (seq + 1) as i64;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=$3 WHERE tenant=$1 AND queue=$2 AND next_seq<$3",
                &[&t, &q, &next],
            ))?;
        }
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops);
        Ok(())
    }

    // -- recovery-on-open (ADR-012 P2): the DB-authoritative store persists the applied cursor in
    //    `relational_cursor`, so recovery can resume from that durable high-water and only replay the
    //    retained log tail. Recovery also repopulates the in-process control plane and re-seeds the
    //    id-mint counters from `pqueue_items`.

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        LogStore::high_water(self, shard)
    }

    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let rows = st(g.client.query(
            "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2",
            &[&t, &q],
        ))?;
        for row in rows {
            let id: String = row.get(0);
            let item_id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
            counters.observe(shard, item_id);
        }
        Ok(())
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&mut self.lock().client, shard, now, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        let mut g = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *g;
        let tokens = live_tokens.clone();
        render_claimed(client, shard, ids, |id| tokens.get(id).cloned())
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        lookup_active_by_key(&mut self.lock().client, shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        item_state_sql(&mut self.lock().client, shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        item_version_sql(&mut self.lock().client, shard, id)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        expired_leases_sql(&mut self.lock().client, shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        all_expired_leases_sql(&mut self.lock().client, now).unwrap_or_default()
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
        validate_leased(&mut self.lock().client, shard, &ids)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&mut self.lock().client, shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&mut self.lock().client, shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        update_fields_validate_sql(&mut self.lock().client, shard, id, expected_item_version)
    }

    // Secondary indexes are deferred (the family stubs them): validation is a no-op, queries `Unavailable`.
    fn index_validate(
        &self,
        _shard: &QueueKey,
        _item_id: &ItemId,
        _fields: &BTreeMap<String, Bytes>,
        _entity: Option<&serde_json::Value>,
        _exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        Ok(())
    }

    fn index_validate_push(&self, _shard: &QueueKey, _items: &[PushItem]) -> EngineResult<()> {
        Ok(())
    }

    fn index_validate_replace(
        &self,
        _shard: &QueueKey,
        _existing_id: &ItemId,
        _item: &PushItem,
    ) -> EngineResult<()> {
        Ok(())
    }

    fn index_validate_update(
        &self,
        _shard: &QueueKey,
        _id: &ItemId,
        _field_ops: &BTreeMap<String, Option<Bytes>>,
        _entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        Ok(())
    }

    // The store wrapper itself still delegates the commit-class boundary to the backend facade below; its
    // own trait surface intentionally remains the default read-only shape.

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&mut self.lock().client, shard, now, limit)
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        peek_sql(&mut self.lock().client, shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let mut g = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *g;
        pending_sql(client, live_tokens, shard)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&mut self.lock().client, shard)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<TerminalEmissionMetrics> {
        let metrics = metrics_sql(&mut self.lock().client, shard)?;
        Ok(TerminalEmissionMetrics {
            resident_terminal_count: metrics.resident_terminal_count,
            emission_lag_commands: 0,
            emission_oldest_unemitted_age_ms: 0,
        })
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        live_items_sql(&mut self.lock().client, shard, keys)
    }

    fn reap_terminal_items(
        &mut self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<Vec<ItemId>> {
        let mut g = self.lock();
        let mut tx = st(g.client.transaction())?;
        let reaped = reap_terminal_items_sql(
            &mut tx,
            shard,
            now,
            terminal_retention_ms,
            emit_change_records,
            emission_cursor,
        )?;
        st(tx.commit())?;
        Ok(reaped)
    }

    fn index_get_unique(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        Err(EngineError::Unavailable)
    }

    fn index_lookup(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        Err(EngineError::Unavailable)
    }
}

impl AsOfProjectionStore for PostgresRelational {
    type AsOfProjection = PostgresRelational;

    fn reconstruct_as_of(
        &self,
        _definition: &QueueDefinition,
        _snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        Err(EngineError::Unavailable)
    }
}

/// The composed unified postgres-relational backend (ADR-012 P1b-ii):
/// `ComposedBackend<PostgresRelational, PostgresRelational, InProcessControlPlane>` — one relational store
/// on both axes, so append+apply commit as one postgres transaction. Capability-equivalent to the
/// monolithic [`PostgresRelationalBackend`] on the CORE conformance class.
pub type ComposedPostgresRelationalBackend =
    ComposedBackend<PostgresRelational, PostgresRelational, InProcessControlPlane>;

/// Assemble a unified postgres-relational composition isolated in `schema`. Both axes are clones of the SAME
/// store (shared `Client`), so the orthogonal `commit_locked` drives one durable transaction. Runs
/// recovery-on-open (ADR-012 P2): a reconnect to the same schema repopulates the in-process control plane
/// from the durable `queues` catalog and re-seeds the id-mint counters (the DB projection needs no replay).
pub fn composed_postgres_relational_in_schema(
    url: &str,
    schema: &str,
) -> EngineResult<ComposedPostgresRelationalBackend> {
    let store = PostgresRelational::connect_in_schema(url, schema)?;
    ComposedBackend::new(store.clone(), store, InProcessControlPlane::new()).recover()
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
        assert!(
            CLAIM_CTE.contains("pqueue_item_gates") && CLAIM_CTE.contains("pqueue_gate_state"),
            "BQ-14d: item-level claim MUST anti-join blocked gates (a blocked gate hides its items)"
        );
    }

    #[test]
    fn claimed_gate_key_lookup_is_batched_by_item_id_array() {
        assert!(
            ITEM_GATE_KEYS_BATCH_SQL.contains("item_id = ANY($3)"),
            "claimed response gate keys must be loaded for the whole claimed batch"
        );
        assert!(
            ITEM_GATE_KEYS_BATCH_SQL.contains("ORDER BY item_id, gate_key"),
            "batched gate lookup must preserve per-item gate-key ordering"
        );
    }

    #[test]
    fn sequence_allocation_is_atomic_increment_and_return() {
        // The schema declares the cursor; allocation is an UPDATE ... RETURNING (see alloc_seq /
        // alloc_item_seq) — no SELECT MAX(...) read-then-write, which would TOCTOU under a pool.
        assert!(RELATIONAL_SCHEMA.contains("relational_cursor"));
        assert!(RELATIONAL_SCHEMA.contains("next_seq BIGINT"));
        assert!(RELATIONAL_SCHEMA.contains("next_item_seq BIGINT"));
        // BQ-20: the durable ownership epoch (TD-003 fence authority) lives on the per-queue cursor row.
        assert!(RELATIONAL_SCHEMA.contains("assignment_epoch BIGINT"));
    }

    #[test]
    fn schema_has_relational_projections() {
        for table in [
            "pqueue_items",
            "pqueue_group_summary",
            "pqueue_item_key_retention",
            "pqueue_cohorts",
            "relational_emission_cursor",
            "pqueue_item_gates",
            "pqueue_gate_state",
            "pqueue_item_index",
        ] {
            assert!(RELATIONAL_SCHEMA.contains(table), "missing {table}");
        }
        assert!(
            RELATIONAL_SCHEMA.contains("WHERE superseded = false"),
            "active-key partial unique index"
        );
        assert!(
            RELATIONAL_SCHEMA.contains("is_unique BOOLEAN NOT NULL DEFAULT false"),
            "typed side index rows must carry uniqueness metadata"
        );
        assert!(
            RELATIONAL_SCHEMA
                .contains("CREATE UNIQUE INDEX IF NOT EXISTS pqueue_item_index_unique_key_idx")
                && RELATIONAL_SCHEMA.contains("WHERE is_unique = true"),
            "unique typed indexes must be protected by a database-level partial unique index"
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
    use postgres::NoTls;
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
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: vec![],
            entity_schema: None,
            typed_indexes: vec![],
            emit_change_records: true,
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
            expected_epoch: None,
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
        block_on(b.push(&shard(), vec![grouped(10), grouped(20)], ts(0), None)).unwrap();
        assert_eq!(group_count(&b), 2, "two grouped items eligible");
        // Claim the rep (priority 10) — the claim path MUST refresh the summary (count -> 1).
        block_on(b.claim(claim_req(1, 500, 10))).unwrap();
        assert_eq!(
            group_count(&b),
            1,
            "claim must refresh pqueue_group_summary (leased item leaves the eligible count)"
        );
    }

    /// BQ-14b: group_batching leases whole groups oldest-first (env-gated; LOUD-skips without a DB).
    #[test]
    fn group_batching_leases_whole_groups() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (group_batching_leases_whole_groups) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_gb_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let def = QueueDefinition {
            max_eligible_group_size: Some(5),
            secondary_indexes: vec![],
            ..qdef()
        };
        let g2 = |priority: i64, group: &str| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new(group).unwrap()),
            ..Default::default()
        };
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect");
        block_on(b.create_queue(def)).unwrap();
        block_on(b.push(
            &shard(),
            vec![
                g2(11, "g1"),
                g2(10, "g1"),
                g2(21, "g2"),
                g2(20, "g2"),
                g2(30, "g3"),
            ],
            ts(0),
            None,
        ))
        .unwrap();
        // group_batching max_groups=2 → the two oldest groups (g1, g2) leased whole (4 items); g3 stays.
        let req = ClaimRequest {
            compatibility: ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 2 }),
                ..Default::default()
            },
            ..claim_req(10, 500, 100)
        };
        let claimed = block_on(b.claim(req)).unwrap();
        assert_eq!(claimed.items.len(), 4, "g1 + g2 leased whole");
        assert_eq!(
            block_on(b.metrics(&shard())).unwrap().pending,
            1,
            "g3 (1 item) untouched"
        );
    }

    /// BQ-14c: whole_cohort leases a complete, all-eligible cohort (env-gated; LOUD-skips without a DB).
    #[test]
    fn whole_cohort_leases_complete_cohort() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (whole_cohort_leases_complete_cohort) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_wc_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let def = QueueDefinition {
            cohort_policy: Some(pqueue_core::CohortPolicy {
                enabled: true,
                completion_bound_ms: Some(30_000),
                on_incomplete: None,
                max_cohort_size: Some(10),
            }),
            ..qdef()
        };
        let cm = |priority: i64, size: u64| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new("c1").unwrap()),
            cohort_size: Some(size),
            ..Default::default()
        };
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect");
        block_on(b.create_queue(def)).unwrap();
        block_on(b.push(&shard(), vec![cm(10, 3), cm(11, 3), cm(12, 3)], ts(0), None)).unwrap();
        let req = ClaimRequest {
            compatibility: ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
            ..claim_req(10, 500, 100)
        };
        let claimed = block_on(b.claim(req)).unwrap();
        assert_eq!(
            claimed.cohort_lease_token,
            Some(LeaseToken::new("lease-1").unwrap()),
            "whole-cohort claims carry the shared lease token at the response top level"
        );
        assert_eq!(
            claimed.cohort_id.as_ref().map(|id| id.as_str()),
            Some("coh:c1:0"),
            "whole-cohort claims identify the stored cohort generation at the response top level"
        );
        assert!(
            claimed.items.iter().all(|item| item.lease_token.is_none()),
            "whole-cohort item rows omit per-item lease tokens"
        );
        assert_eq!(
            claimed.items.len(),
            3,
            "the whole complete cohort leases together"
        );
        assert_eq!(block_on(b.metrics(&shard())).unwrap().leased, 3);
    }

    /// BQ-14e: discover_active_scopes rolls up pqueue_group_summary, ranks oldest-first, reports deferred
    /// at-risk as None, and drops fully-leased groups (env-gated; LOUD-skips without a DB).
    #[test]
    fn discover_active_scopes_rolls_up_group_summary() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (discover_active_scopes_rolls_up_group_summary) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_ds_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let def = QueueDefinition {
            max_eligible_group_size: Some(5),
            secondary_indexes: vec![],
            ..qdef()
        };
        let g2 = |priority: i64, group: &str| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new(group).unwrap()),
            ..Default::default()
        };
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect");
        block_on(b.create_queue(def)).unwrap();
        // g1 eligible since t=10 (2 items), g2 since t=20 (1 item).
        block_on(b.push(&shard(), vec![g2(10, "g1"), g2(11, "g1")], ts(10), None)).unwrap();
        block_on(b.push(&shard(), vec![g2(20, "g2")], ts(20), None)).unwrap();

        // Group granularity: oldest-first (g1 then g2), per-group eligible counts, at-risk None.
        let scopes =
            block_on(b.discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(1000)))
                .unwrap();
        let order: Vec<&str> = scopes
            .iter()
            .map(|s| s.group_key.as_deref().unwrap())
            .collect();
        assert_eq!(order, vec!["g1", "g2"], "ranked most-aged first");
        assert_eq!(scopes[0].oldest_eligible_age_ms, 990_000);
        assert_eq!(scopes[0].eligible_count, Some(2));
        assert_eq!(scopes[1].eligible_count, Some(1));
        assert!(
            scopes.iter().all(|s| s.progress_bound_risk_count.is_none()),
            "deferred at-risk is None"
        );

        // Queue granularity: one rollup (max age, summed counts).
        let rolled =
            block_on(b.discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000)))
                .unwrap();
        assert_eq!(rolled.len(), 1);
        assert_eq!(rolled[0].group_key, None);
        assert_eq!(rolled[0].oldest_eligible_age_ms, 990_000);
        assert_eq!(rolled[0].eligible_count, Some(3));

        // Leasing g1's whole group drops it from discovery (no eligible work left).
        let req = ClaimRequest {
            compatibility: ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            ..claim_req(10, 500, 100)
        };
        block_on(b.claim(req)).unwrap();
        let after =
            block_on(b.discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(1000)))
                .unwrap();
        let after_order: Vec<&str> = after
            .iter()
            .map(|s| s.group_key.as_deref().unwrap())
            .collect();
        assert_eq!(
            after_order,
            vec!["g2"],
            "fully-leased g1 is no longer active"
        );
    }
}

#[cfg(test)]
mod commit_transition_tests {
    use super::*;
    use futures::executor::block_on;
    use pqueue_conformance::{qdef, shard, ts};
    use pqueue_core::{LeaseToken, PriorityValue, RequestId, WorkerId};
    use pqueue_engine::{
        ClaimPort, ClaimRef, CommitEntryOutcome, CommitTransition, CommitTransitionEntry,
        CommitTransitionPort, ControlPlaneStore, EngineError, FinalizeKind, ProjectionRead,
        PushPort, RecoveryReadPort, RenewLeasePort, SideRecord,
    };

    fn unique_schema(tag: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!(
            "pq_rel_commit_{}_{}_{}",
            tag,
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        )
    }

    fn item(priority: i64) -> PushSpec {
        PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            ..Default::default()
        }
    }

    fn side(key: &str, payload: &str) -> SideRecord {
        SideRecord {
            key: key.as_bytes().to_vec(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        }
    }

    fn claim_req(max: usize, exp: i64, now: i64) -> pqueue_engine::ClaimRequest {
        pqueue_engine::ClaimRequest {
            shard: shard(),
            worker_id: WorkerId::new("w1").unwrap(),
            max_items: max,
            lease_token: LeaseToken::new("lease-1").unwrap(),
            lease_expires_at: ts(exp),
            now: ts(now),
            compatibility: Default::default(),
            expected_epoch: None,
        }
    }

    async fn push_and_claim(
        backend: &PostgresRelationalBackend,
        now: i64,
        priority: i64,
    ) -> ClaimRef {
        backend
            .push(&shard(), vec![item(priority)], ts(now), None)
            .await
            .unwrap();
        let claimed = backend.claim(claim_req(1, now + 600, now)).await.unwrap();
        let c = &claimed.items[0];
        ClaimRef {
            item_id: c.item_id,
            lease_token: c.lease_token.clone().expect("claimed item carries a token"),
            lease_expires_at: c.lease_expires_at,
            item_version: c.item_version,
        }
    }

    fn count_side_records(backend: &PostgresRelationalBackend) -> i64 {
        let mut g = backend.inner.lock().unwrap();
        g.client
            .query_one("SELECT COUNT(*) FROM pqueue_side_records", &[])
            .unwrap()
            .get(0)
    }

    fn read_side_record(backend: &PostgresRelationalBackend, key: &str) -> Option<Vec<u8>> {
        let mut g = backend.inner.lock().unwrap();
        let q = shard();
        let tenant = q.tenant_id.as_str().to_string();
        let queue = q.queue_id.as_str().to_string();
        g.client
            .query_opt(
                "SELECT payload FROM pqueue_side_records WHERE tenant_id=$1 AND queue_id=$2 AND key=$3",
                &[&tenant, &queue, &key.as_bytes()],
            )
            .unwrap()
            .map(|row| row.get(0))
    }

    async fn backend_for_schema(url: &str, schema: &str) -> PostgresRelationalBackend {
        let backend = PostgresRelationalBackend::connect_in_schema(url, schema).unwrap();
        backend.create_queue(qdef()).await.unwrap();
        backend
    }

    #[test]
    fn commit_transition_rejects_bad_token_bad_version_and_writes_nothing() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_rejects_bad_token_bad_version_and_writes_nothing) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = unique_schema("rejects");
        let backend = block_on(backend_for_schema(&url, &schema));

        let mut bad_token = block_on(push_and_claim(&backend, 0, 10));
        bad_token.lease_token = LeaseToken::new("wrong").unwrap();
        let outcomes = block_on(backend.commit_transition(
            &shard(),
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref: bad_token,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/bad-token", "x")],
                    lifecycle_items: vec![item(20)],
                    instance_fence: None,
                }],
            },
            ts(1),
            None,
        ))
        .unwrap();
        assert_eq!(
            outcomes,
            vec![CommitEntryOutcome::Rejected(EngineError::StaleLease)]
        );
        assert_eq!(count_side_records(&backend), 0);

        let mut bad_version = block_on(push_and_claim(&backend, 2, 11));
        bad_version.item_version += 99;
        let outcomes = block_on(backend.commit_transition(
            &shard(),
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref: bad_version,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![side("state/bad-version", "y")],
                    lifecycle_items: vec![item(30)],
                    instance_fence: None,
                }],
            },
            ts(3),
            None,
        ))
        .unwrap();
        assert_eq!(
            outcomes,
            vec![CommitEntryOutcome::Rejected(EngineError::Conflict)]
        );
        assert_eq!(count_side_records(&backend), 0);
    }

    #[test]
    fn commit_transition_request_id_replays_without_double_write() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_request_id_replays_without_double_write) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = unique_schema("replay");
        let backend = block_on(backend_for_schema(&url, &schema));
        let claim_ref = block_on(push_and_claim(&backend, 0, 10));
        let rid = RequestId::new("txn-replay-1").unwrap();
        let body = |cr: ClaimRef| CommitTransition {
            request_id: Some(rid.clone()),
            entries: vec![CommitTransitionEntry {
                claim_ref: cr,
                finalize: FinalizeKind::Complete,
                side_records: vec![side("state/replay", "v1")],
                lifecycle_items: vec![item(20)],
                instance_fence: None,
            }],
        };

        let first =
            block_on(backend.commit_transition(&shard(), body(claim_ref.clone()), ts(1), None))
                .unwrap();
        let lifecycle_id = match &first[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
            other => panic!("expected Committed, got {other:?}"),
        };
        assert_eq!(count_side_records(&backend), 1);

        let replay =
            block_on(backend.commit_transition(&shard(), body(claim_ref), ts(1), None)).unwrap();
        assert_eq!(first, replay);
        assert_eq!(count_side_records(&backend), 1);
        assert_eq!(
            block_on(backend.side_record(&shard(), b"state/replay"))
                .unwrap()
                .as_deref(),
            Some(&b"v1"[..])
        );
        assert_eq!(block_on(backend.metrics(&shard())).unwrap().pending, 1);
        let recovery = block_on(backend.explain_commit(&shard(), rid))
            .unwrap()
            .expect("replay record retained");
        assert_eq!(recovery.entries.len(), 1);
        assert_eq!(recovery.entries[0].lifecycle_item_ids, vec![lifecycle_id]);
    }

    #[test]
    fn commit_transition_conflict_is_per_entry_during_race() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_conflict_is_per_entry_during_race) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = unique_schema("race");
        let b1 = block_on(backend_for_schema(&url, &schema));
        let b2 = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();

        let stale = block_on(push_and_claim(&b1, 0, 10));
        let live = block_on(push_and_claim(&b1, 2, 11));
        block_on(b2.renew(&shard(), vec![stale.item_id], ts(500), ts(3), None)).unwrap();

        let outcomes = block_on(b1.commit_transition(
            &shard(),
            CommitTransition {
                request_id: None,
                entries: vec![
                    CommitTransitionEntry {
                        claim_ref: stale,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/stale", "x")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
                    },
                    CommitTransitionEntry {
                        claim_ref: live.clone(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/live", "y")],
                        lifecycle_items: vec![item(30)],
                        instance_fence: None,
                    },
                ],
            },
            ts(4),
            None,
        ))
        .unwrap();

        assert!(matches!(
            outcomes[0],
            CommitEntryOutcome::Rejected(EngineError::Conflict)
        ));
        assert!(matches!(outcomes[1], CommitEntryOutcome::Committed { .. }));
        assert!(read_side_record(&b1, "state/stale").is_none());
        assert!(read_side_record(&b1, "state/live").is_some());
    }
}
