use super::*;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use pqueue_core::{
    FilterOp, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken, Metadata, OrderField,
    PriorityModel, PriorityValue, QueryFilter, QueueDefinition, QueueIndex, RangeScanRow,
    RequestId, SortDirection, TypedValue, UtcTimestamp, priority_sort,
};
use pqueue_engine::{
    ClaimRef, CommandPosition, CommitEntryOutcome, CommitEntryStatus, CommitTransitionEntry,
    EngineError, EngineResult, EntryRecovery, PushItem, PushSpec, QueueKey,
};
use pqueue_projection::{InMemoryProjection, ProjectionImage};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

/// The relational schema. `pqueue_items` is TD-002's item projection (sqlite-typed); `fenced`,
/// `superseded`, and `max_attempts` are reference-projection columns mirroring the `FenceLease`/
/// `UnfenceLease`, `ReplacePending`, and retry-exhaustion apply arms (the production postgres mode
/// realizes fence via epoch and supersede via the `client_item_key` tombstone — see TD-002 note). The
/// partial unique index enforces one ACTIVE item per `client_item_key`, letting a superseded predecessor
/// and its replacement coexist (ReplacePending). `relational_cursor` is the per-queue command sequence
/// (the `last_command_sequence` source), persisted so positions resume monotonically across a reopen.
pub(crate) const RELATIONAL_SCHEMA: &str = r#"
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
    entity_document TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    item_version INTEGER NOT NULL,
    lease_token_hash BLOB,
    lease_expires_at INTEGER,
    worker_id TEXT,
    last_command_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER,
    terminal_command_epoch INTEGER,
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
CREATE INDEX IF NOT EXISTS pqueue_items_group_due_idx
    ON pqueue_items (tenant_id, queue_id, lifecycle_state, group_key, not_before, priority_sort, created_seq)
    WHERE group_key IS NOT NULL AND superseded = 0;
CREATE TABLE IF NOT EXISTS relational_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    next_seq INTEGER NOT NULL,        -- command-position sequence (last_command_sequence source)
    next_item_seq INTEGER NOT NULL,   -- monotonic per-queue item insertion counter (created_seq source)
    assignment_epoch INTEGER NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
-- Durable item-id high-water (ADR-009 mint-counter recovery floor). Terminal-item retention reaping now
-- DELETES item rows (objectlog/hybrid-async), so the surviving `pqueue_items` rows are no longer the complete
-- minted set — a reopen that seeded `QueueCounters` only from survivors could re-mint a reaped id. Every reap
-- advances this MONOTONIC per-queue high-water past the greatest id it deletes, and recovery observes it, so a
-- push after reaping ALL rows still mints strictly past every previously-minted id. Stored as the raw
-- `ItemId` (it encodes `(epoch, counter)`); recovery decodes + `QueueCounters::observe`s it, which is
-- epoch-aware and only ever advances — a stale lower-epoch floor never lowers a fresh tenure.
CREATE TABLE IF NOT EXISTS pqueue_id_high_water (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    item_id TEXT NOT NULL,
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS relational_emission_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    epoch INTEGER NOT NULL, seq INTEGER NOT NULL,
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
-- BQ-11e: API-001 request-id replay for request-id-carrying relational operations. The first wired
-- operation is BatchPush: same `(tenant,queue,operation,request_id)` + same fingerprint replays the stored
-- response ids; a different fingerprint is `request-id-conflict`.
CREATE TABLE IF NOT EXISTS pqueue_request_idempotency (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, operation TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_fingerprint BLOB NOT NULL,
    response_payload TEXT NOT NULL,
    command_positions TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, operation, request_id)
);
CREATE INDEX IF NOT EXISTS pqueue_request_idempotency_expiry_idx
    ON pqueue_request_idempotency (expires_at);
-- TD-002 §cohort lifecycle projection. The group_key is the logical cohort key; cohort_id is the stable
-- generation identity returned to callers and changes only after terminal retention permits group reuse.
CREATE TABLE IF NOT EXISTS pqueue_cohorts (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, group_key TEXT NOT NULL,
    cohort_id TEXT NOT NULL,
    cohort_size INTEGER NOT NULL,
    member_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    cohort_created_at INTEGER NOT NULL,
    first_eligible_at INTEGER,
    expire_command_pos INTEGER,
    cohort_lease_token_hash BLOB,
    retention_until INTEGER,
    created_at INTEGER NOT NULL,
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
-- C9 (epic pqueue-2201fd37): opaque NON-WORK side records written by the authoritative vectorized
-- claimed-work commit (Snorri StateStore boundary). Deliberately SEPARATE from `pqueue_items`: a side
-- record carries no lifecycle/lease/priority/eligibility, so it is never claimable, eligible, peekable, or
-- counted as work. `key`/`payload` are opaque bytes pqueue stores verbatim; the apply arm upserts by key.
CREATE TABLE IF NOT EXISTS pqueue_side_records (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, key BLOB NOT NULL, payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, key)
);
-- C6 (epic pqueue-2201fd37): caller-supplied opaque instance/state fences advanced by the authoritative
-- vectorized claimed-work commit (Snorri StateStore boundary). SEPARATE from `pqueue_items`: a fence carries
-- no lifecycle/lease and is never claimable/eligible/peekable. `instance_key` is opaque bytes; an absent key
-- reads as fence 0 (the unset convention). The commit upserts the row to `next` only after validation.
CREATE TABLE IF NOT EXISTS pqueue_instance_fences (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, instance_key BLOB NOT NULL, fence INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, instance_key)
);
-- ADR-011 (pqueue-f4ffd679): typed secondary index rows. PK is (tenant, queue, index_name, item_id)
-- because each item has at most one canonical key per named index. Uniqueness is enforced in application
-- logic before INSERT (SQL cannot express a per-name unique constraint on a single row). Rows are inserted
-- on Push/ReplacePending/UpdateFields and deleted only on PurgeItems — terminal items keep their index
-- rows so they are still findable (parity with in-memory projection).
CREATE TABLE IF NOT EXISTS pqueue_item_index (
    tenant_id TEXT NOT NULL,
    queue_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_key BLOB NOT NULL,
    item_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, index_name, item_id)
);
CREATE INDEX IF NOT EXISTS pqueue_item_index_key_idx
    ON pqueue_item_index (tenant_id, queue_id, index_name, index_key);
-- objectlog/hybrid-async logical checkpoint lineage (bead pqueue-16b85e28, plan §Snapshot Authority).
-- The async SQLite checkpoint worker records, per queue, the object-log lineage the durable SQLite
-- projection was last advanced from: the LOGICAL high-water it reached (relational_cursor.next_seq at
-- checkpoint time), the object-log assignment epoch, and an opaque object-log segment/manifest reference
-- (stored verbatim — pqueue-sqlite does not depend on pqueue-objectlog types). This is LOGICAL high-water
-- lineage, deliberately distinct from the PHYSICAL SQLite WAL checkpoint (PRAGMA wal_checkpoint), which is
-- a storage-file concern that reclaims WAL frames and never advances the command cursor. The row is
-- upserted in the SAME transaction that advances the logical high-water, so recorded lineage can never be
-- ahead of durably materialized projection state.
CREATE TABLE IF NOT EXISTS pqueue_checkpoint_lineage (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    logical_high_water INTEGER NOT NULL,   -- relational_cursor.next_seq reached by this checkpoint
    source_epoch INTEGER NOT NULL,         -- object-log assignment epoch the batch was committed under
    source_segment TEXT NOT NULL,          -- opaque object-log segment/manifest reference
    applied_commands INTEGER NOT NULL,     -- cumulative commands absorbed into this checkpoint lineage
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant, queue)
);
"#;

// ---------------------------------------------------------------------------
// small conversions / error mapping
// ---------------------------------------------------------------------------

pub(crate) fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

pub(crate) const IDEMPOTENCY_OPERATION_PUSH: &str = "push";

pub(crate) fn push_request_fingerprint(items: &[PushSpec]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(items).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

pub(crate) fn request_expires_at(
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

pub(crate) fn item_ids_to_json(ids: &[ItemId]) -> EngineResult<String> {
    let raw: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    to_json(&raw)
}

pub(crate) fn item_ids_from_json(raw: String) -> EngineResult<Vec<ItemId>> {
    let decoded: Vec<String> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    decoded
        .into_iter()
        .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
        .collect()
}

pub(crate) fn positions_to_json(positions: &[CommandPosition]) -> EngineResult<String> {
    let raw: Vec<(u64, u64)> = positions
        .iter()
        .map(|pos| (pos.backend_epoch, pos.sequence))
        .collect();
    to_json(&raw)
}

pub(crate) fn check_request_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<ItemId>>> {
    let (t, q) = parts(shard);
    let prior: Option<(Vec<u8>, String, i64)> = st(tx
        .query_row(
            "SELECT request_fingerprint, response_payload, expires_at \
             FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, operation, request_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let Some((prior_fingerprint, response_payload, expires_at)) = prior else {
        return Ok(None);
    };
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, operation, request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(item_ids_from_json(response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_request_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    operation: &str,
    request_id: &RequestId,
    fingerprint: &[u8],
    response_ids: &[ItemId],
    positions: &[CommandPosition],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
          command_positions,expires_at,created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=excluded.request_fingerprint, \
          response_payload=excluded.response_payload, \
          command_positions=excluded.command_positions, \
          expires_at=excluded.expires_at",
        params![
            t,
            q,
            operation,
            request_id.as_str(),
            fingerprint,
            item_ids_to_json(response_ids)?,
            positions_to_json(positions)?,
            expires_at,
            ts_nanos(now),
        ],
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// C9: authoritative vectorized claimed-work commit — idempotency + validation helpers
// (epic pqueue-2201fd37)
// ---------------------------------------------------------------------------

/// The retained-request-id operation key for the vectorized commit path, distinct from the push key so the
/// two operations never collide on a shared `request_id` in `pqueue_request_idempotency`.
pub(crate) const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";

/// Stable body fingerprint for the commit path: SHA-256 over the serialized entries (the `request_id` is the
/// cache KEY, not part of the body — same shape as [`push_request_fingerprint`]). A different body under the
/// same request id is a `RequestIdConflict`; an equal body replays the stored per-entry outcomes.
pub(crate) fn commit_request_fingerprint(
    entries: &[CommitTransitionEntry],
) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

/// Durable, replay-faithful mirror of an [`EntryRecovery`] (which carries non-`Serialize` types — an
/// [`EngineError`] in its rejected arm and an [`ItemId`]). Projected to this shape for the
/// `pqueue_request_idempotency.response_payload` column and reconstructed verbatim on replay AND for the
/// recovery/explain read (epic pqueue-2201fd37 acceptance #5). A `None` `rejected` means the entry committed.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StoredEntryRecovery {
    consumed_input_id: String,
    #[serde(default)]
    instance: Option<(Vec<u8>, u64)>,
    #[serde(default)]
    side_record_keys: Vec<Vec<u8>>,
    #[serde(default)]
    lifecycle_item_ids: Vec<String>,
    /// `None` = committed; `Some((code, detail))` = the structured rejection.
    #[serde(default)]
    rejected: Option<(String, Option<String>)>,
}

/// Stable `(code, detail)` projection of an [`EngineError`] for durable replay. `Invalid`/`Forbidden` carry
/// their `&'static str` reason in `detail` so the exact variant round-trips for the reasons this path emits.
pub(crate) fn encode_engine_error(e: &EngineError) -> (&'static str, Option<String>) {
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

/// Reconstruct an [`EngineError`] from its durable `(code, detail)` projection. `Invalid` reasons this path
/// emits ("item is not leased") round-trip to the same `&'static str`; any other reason falls back to a
/// stable static so the variant (and its `PartialEq`) is preserved.
pub(crate) fn decode_engine_error(code: &str, detail: Option<String>) -> EngineError {
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

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value), mirroring the in-memory `outcomes_from_recovery`.
pub(crate) fn recovery_to_outcomes(recovery: &[EntryRecovery]) -> Vec<CommitEntryOutcome> {
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

pub(crate) fn encode_commit_recovery(recovery: &[EntryRecovery]) -> EngineResult<String> {
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

pub(crate) fn decode_commit_recovery(raw: &str) -> EngineResult<Vec<EntryRecovery>> {
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

/// Commit-path twin of [`check_request_idempotency`]: same retained-request-id table + replay / conflict /
/// expired classification, but the stored `response_payload` is the rich per-entry outcome vector (encoded
/// via [`encode_commit_outcomes`]) rather than a flat id list. A live record with an equal fingerprint
/// REPLAYS the prior outcomes; a different fingerprint is `RequestIdConflict`; an expired/absent record
/// returns `None` (proceed fresh, deleting the stale row).
pub(crate) fn check_commit_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let prior: Option<(Vec<u8>, String, i64)> = st(tx
        .query_row(
            "SELECT request_fingerprint, response_payload, expires_at \
             FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let Some((prior_fingerprint, response_payload, expires_at)) = prior else {
        return Ok(None);
    };
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint == fingerprint {
        return Ok(Some(decode_commit_recovery(&response_payload)?));
    }
    Err(EngineError::RequestIdConflict)
}

/// Recovery/explain read of the retained commit record by `request_id`, IGNORING the body fingerprint (the
/// reader has only the id). Returns the durable recovery while the record is retained; `None` once it has
/// elapsed/been deleted. Read-only — does not delete an expired row (that is the commit path's job).
pub(crate) fn read_commit_recovery(
    conn: &Connection,
    shard: &QueueKey,
    request_id: &RequestId,
) -> EngineResult<Option<Vec<EntryRecovery>>> {
    let (t, q) = parts(shard);
    let payload: Option<String> = st(conn
        .query_row(
            "SELECT response_payload FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_COMMIT, request_id.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    match payload {
        Some(raw) => Ok(Some(decode_commit_recovery(&raw)?)),
        None => Ok(None),
    }
}

/// Commit-path twin of [`record_request_idempotency`]: persist the whole-body outcome under the `commit`
/// operation so a later replay returns it verbatim with no second write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_commit_idempotency(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    recovery: &[EntryRecovery],
    positions: &[CommandPosition],
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,\
          command_positions,expires_at,created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=excluded.request_fingerprint, \
          response_payload=excluded.response_payload, \
          command_positions=excluded.command_positions, \
          expires_at=excluded.expires_at",
        params![
            t,
            q,
            IDEMPOTENCY_OPERATION_COMMIT,
            request_id.as_str(),
            fingerprint,
            encode_commit_recovery(recovery)?,
            positions_to_json(positions)?,
            expires_at,
            ts_nanos(now),
        ],
    ))?;
    Ok(())
}

/// Pre-commit validation of one [`ClaimRef`] against the durable `pqueue_items` row, with rejection
/// precedence IDENTICAL to the in-memory [`pqueue_projection::ProjectionData::commit_validate`]: absent ->
/// `NotFound`, fenced -> `StaleLease`, terminal -> `Terminal`, superseded -> `Superseded`, non-leased ->
/// `Invalid`, presented-token mismatch -> `StaleLease`, expired lease (half-open: `lease_expires_at < now`)
/// -> `StaleLease`, version-fence mismatch -> `Conflict`. Nothing is mutated.
///
/// LEASE-TOKEN NOTE (flagged): the relational projection persists only the lease token **hash**
/// (`lease_token_hash`), so token authority is checked by hashing the presented token and comparing hashes —
/// whereas the in-memory family compares cleartext tokens. The accept/reject decision (and its precedence)
/// is identical; only the stored representation differs.
pub(crate) fn commit_validate_sql(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    claim_ref: &ClaimRef,
    now: UtcTimestamp,
) -> EngineResult<()> {
    /// `(lifecycle_state, fenced, superseded, lease_token_hash, lease_expires_at, item_version)`.
    type CommitRow = (String, i64, i64, Option<Vec<u8>>, Option<i64>, i64);
    let (t, q) = parts(shard);
    let row: Option<CommitRow> = st(tx
        .query_row(
            "SELECT lifecycle_state, fenced, superseded, lease_token_hash, lease_expires_at, item_version \
             FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, claim_ref.item_id.to_string()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional())?;
    let Some((state, fenced, superseded, lease_token_hash, lease_expires_at, item_version)) = row
    else {
        return Err(EngineError::NotFound);
    };
    let state = parse_state(&state)?;
    if fenced != 0 {
        return Err(EngineError::StaleLease);
    }
    if state.is_terminal() {
        return Err(EngineError::Terminal);
    }
    if superseded != 0 {
        return Err(EngineError::Superseded);
    }
    if state != ItemState::Leased {
        return Err(EngineError::Invalid("item is not leased"));
    }
    // Claim authority: the presented token's hash must equal the stored hash (a forged/stale token differs).
    if lease_token_hash.as_deref() != Some(lease_hash(&claim_ref.lease_token).as_slice()) {
        return Err(EngineError::StaleLease);
    }
    // The lease must be unexpired (half-open, identical to `expired_leases`: expired iff strictly before now).
    if lease_expires_at.is_some_and(|exp| exp < ts_nanos(now)) {
        return Err(EngineError::StaleLease);
    }
    // Optimistic state fence: the caller's observed version must equal the committed version.
    if item_version as u64 != claim_ref.item_version {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

pub(crate) fn fields_to_json(fields: &BTreeMap<String, Bytes>) -> EngineResult<String> {
    let raw: BTreeMap<&str, Vec<u8>> = fields
        .iter()
        .map(|(k, v)| (k.as_str(), v.to_vec()))
        .collect();
    to_json(&raw)
}

pub(crate) fn fields_from_json(raw: String) -> EngineResult<BTreeMap<String, Bytes>> {
    let decoded: BTreeMap<String, Vec<u8>> =
        serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(decoded
        .into_iter()
        .map(|(k, v)| (k, Bytes::from(v)))
        .collect())
}

pub(crate) fn metadata_to_json(metadata: &Metadata) -> EngineResult<String> {
    to_json(&metadata.clone().into_inner())
}

pub(crate) fn metadata_from_json(raw: String) -> EngineResult<Metadata> {
    let entries = serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Metadata::from_entries(entries))
}

pub(crate) fn ensure_item_text_column(
    conn: &Connection,
    column: &str,
    default_json: &str,
) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!(
        "ALTER TABLE pqueue_items ADD COLUMN {column} TEXT NOT NULL DEFAULT '{default_json}'"
    );
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_fields_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "fields", "{}")
}

pub(crate) fn ensure_item_metadata_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "metadata", "{}")
}

pub(crate) fn ensure_item_entity_document_column(conn: &Connection) -> EngineResult<()> {
    match conn.execute(
        "ALTER TABLE pqueue_items ADD COLUMN entity_document TEXT",
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

pub(crate) fn ensure_item_integer_column(conn: &Connection, column: &str) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!("ALTER TABLE pqueue_items ADD COLUMN {column} INTEGER");
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_item_terminal_command_epoch_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_integer_column(conn, "terminal_command_epoch")
}

pub(crate) fn ensure_cohort_column(
    conn: &Connection,
    column: &str,
    definition: &str,
) -> EngineResult<()> {
    if !column
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(EngineError::Invalid("column name must be [A-Za-z0-9_]"));
    }
    let sql = format!("ALTER TABLE pqueue_cohorts ADD COLUMN {column} {definition}");
    match conn.execute(&sql, []) {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(EngineError::Storage(e.to_string())),
    }
}

pub(crate) fn ensure_cohort_lifecycle_columns(conn: &Connection) -> EngineResult<()> {
    ensure_cohort_column(conn, "cohort_id", "TEXT")?;
    ensure_cohort_column(conn, "member_count", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_cohort_column(conn, "state", "TEXT NOT NULL DEFAULT 'forming'")?;
    ensure_cohort_column(conn, "cohort_created_at", "INTEGER")?;
    ensure_cohort_column(conn, "first_eligible_at", "INTEGER")?;
    ensure_cohort_column(conn, "expire_command_pos", "INTEGER")?;
    ensure_cohort_column(conn, "cohort_lease_token_hash", "BLOB")?;
    ensure_cohort_column(conn, "retention_until", "INTEGER")?;
    st(conn.execute(
        "UPDATE pqueue_cohorts SET cohort_id=group_key WHERE cohort_id IS NULL",
        [],
    ))?;
    st(conn.execute(
        "UPDATE pqueue_cohorts SET cohort_created_at=created_at WHERE cohort_created_at IS NULL",
        [],
    ))?;
    st(conn.execute(
        "UPDATE pqueue_cohorts SET member_count=(SELECT COUNT(*) FROM pqueue_items i \
         WHERE i.tenant_id=pqueue_cohorts.tenant_id AND i.queue_id=pqueue_cohorts.queue_id \
         AND i.group_key=pqueue_cohorts.group_key AND i.superseded=0 AND i.cohort_size IS NOT NULL \
         AND i.lifecycle_state NOT IN ('Complete','Failed'))",
        [],
    ))?;
    st(conn.execute(
        "UPDATE pqueue_cohorts SET state=CASE \
         WHEN member_count >= cohort_size THEN 'complete' ELSE 'forming' END \
         WHERE state IS NULL OR state='forming' OR state='complete'",
        [],
    ))?;
    st(conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS pqueue_cohorts_claim_idx \
             ON pqueue_cohorts (tenant_id, queue_id, state) WHERE state='complete';\
         CREATE INDEX IF NOT EXISTS pqueue_cohorts_expiry_idx \
             ON pqueue_cohorts (tenant_id, queue_id, cohort_created_at) \
             WHERE state IN ('forming','complete');",
    ))?;
    Ok(())
}

pub(crate) fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

pub(crate) fn query_projection_image<R>(
    definition: &QueueDefinition,
    image: ProjectionImage,
    f: impl FnOnce(&InMemoryProjection, &QueueKey) -> EngineResult<R>,
) -> EngineResult<R> {
    let mut projection = InMemoryProjection::new();
    projection.hydrate_shard(definition, image)?;
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    f(&projection, &shard)
}

// ---------------------------------------------------------------------------
// ADR-011 typed secondary index helpers
// ---------------------------------------------------------------------------

/// Decode a caller-supplied raw lookup byte slice into a `serde_json::Value` for re-encoding via
/// `IndexDef::index_key` / `CompoundIndexDef::index_key`. Mirrors `decode_typed_lookup_value` in
/// `pqueue_projection` — the two must stay identical so lookup keys byte-match stored keys.
pub(crate) fn decode_typed_lookup_value_rel(
    index_type: &IndexType,
    bytes: &[u8],
) -> EngineResult<JsonValue> {
    match index_type {
        IndexType::String => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(JsonValue::String(s.to_owned()))
        }
        IndexType::Datetime => {
            if let Ok(value @ JsonValue::Number(_)) = serde_json::from_slice::<JsonValue>(bytes) {
                return Ok(value);
            }
            let s = std::str::from_utf8(bytes)
                .map_err(|_| EngineError::Invalid("lookup key is not valid UTF-8"))?;
            Ok(JsonValue::String(s.to_owned()))
        }
        IndexType::Integer | IndexType::Float => serde_json::from_slice::<JsonValue>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON number")),
        IndexType::Boolean => serde_json::from_slice::<JsonValue>(bytes)
            .map_err(|_| EngineError::Invalid("lookup key is not a valid JSON boolean")),
    }
}

/// Compute the canonical `index_key` bytes for a lookup against a named index. Roundtrips the
/// caller's raw byte slices through their declared types so the result is byte-identical to stored
/// keys regardless of how the caller encoded the lookup value.
pub(crate) fn typed_lookup_canonical_key(
    qi: &QueueIndex,
    key_values: &[Vec<u8>],
) -> EngineResult<Vec<u8>> {
    match &qi.declaration {
        IndexDeclaration::Single(def) => {
            let val = decode_typed_lookup_value_rel(&def.index_type, &key_values[0])?;
            let mut record = serde_json::Map::new();
            record.insert(def.field.clone(), val);
            def.index_key(&JsonValue::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
        IndexDeclaration::Compound(def) => {
            let mut record = serde_json::Map::new();
            for (field, bytes) in def.fields.iter().zip(key_values.iter()) {
                let val = decode_typed_lookup_value_rel(&field.index_type, bytes)?;
                record.insert(field.field.clone(), val);
            }
            def.index_key(&JsonValue::Object(record))
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .ok_or_else(|| EngineError::Storage("missing lookup key".to_string()))
        }
    }
}

/// Compute `(index_name, canonical_key_bytes)` pairs for an item's `entity_document`.
/// Returns empty when `typed_indexes` is empty or `entity` is `None` (schema-less queues).
pub(crate) fn typed_index_keys_for_entity(
    typed_indexes: &[QueueIndex],
    entity: Option<&JsonValue>,
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RangeScanCursorState {
    pub(crate) index: String,
    pub(crate) filters: Vec<QueryFilter>,
    pub(crate) order_by: Vec<OrderField>,
    pub(crate) anchor_item_id: ItemId,
    pub(crate) anchor_values: Vec<TypedValue>,
}

pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub(crate) fn parse_utc_timestamp(value: &str) -> EngineResult<UtcTimestamp> {
    let Some(value) = value
        .strip_suffix('Z')
        .or_else(|| value.strip_suffix("+00:00"))
    else {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    };
    let (date, time) = value.split_once('T').ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let month: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let day: i64 = date_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    if date_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }

    let mut time_parts = time.split(':');
    let hour: i64 = time_parts
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ))?;
    let minute: i64 =
        time_parts
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or(EngineError::Invalid(
                "typed index value is not a valid datetime",
            ))?;
    let sec_part = time_parts.next().ok_or(EngineError::Invalid(
        "typed index value is not a valid datetime",
    ))?;
    if time_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "typed index value is not a valid datetime",
        ));
    }
    let (second, nanos) = match sec_part.split_once('.') {
        Some((whole, frac)) => {
            let second: i64 = whole
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            if frac.is_empty() || frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
                return Err(EngineError::Invalid(
                    "typed index value is not a valid datetime",
                ));
            }
            let mut digits = frac.to_string();
            while digits.len() < 9 {
                digits.push('0');
            }
            let nanos: u32 = digits
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?;
            (second, nanos)
        }
        None => (
            sec_part
                .parse()
                .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))?,
            0,
        ),
    };
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    UtcTimestamp::new(seconds, nanos)
        .map_err(|_| EngineError::Invalid("typed index value is not a valid datetime"))
}

pub(crate) fn typed_value_for_json(
    value: &JsonValue,
    index_type: &IndexType,
) -> EngineResult<Option<TypedValue>> {
    if value.is_null() {
        return Ok(None);
    }
    let typed = match index_type {
        IndexType::String => value
            .as_str()
            .map(|s| TypedValue::String(s.to_string()))
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Integer => {
            value
                .as_i64()
                .map(TypedValue::Integer)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?
        }
        IndexType::Float => value
            .as_f64()
            .map(TypedValue::Float)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Boolean => value
            .as_bool()
            .map(TypedValue::Bool)
            .ok_or(EngineError::Invalid(
                "typed index value is not valid for declared type",
            ))?,
        IndexType::Datetime => match value {
            JsonValue::String(s) => TypedValue::DateTime(parse_utc_timestamp(s)?),
            JsonValue::Number(n) => {
                let seconds = n.as_i64().ok_or({
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?;
                TypedValue::DateTime(UtcTimestamp::new(seconds, 0).map_err(|_| {
                    EngineError::Invalid("typed index value is not valid for declared type")
                })?)
            }
            _ => {
                return Err(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ));
            }
        },
    };
    Ok(Some(typed))
}

pub(crate) fn typed_value_from_filter_value(
    value: &TypedValue,
    index_type: &IndexType,
) -> EngineResult<TypedValue> {
    match (value, index_type) {
        (TypedValue::String(v), IndexType::String) => Ok(TypedValue::String(v.clone())),
        (TypedValue::Integer(v), IndexType::Integer) => Ok(TypedValue::Integer(*v)),
        (TypedValue::Float(v), IndexType::Float) => Ok(TypedValue::Float(*v)),
        (TypedValue::Bool(v), IndexType::Boolean) => Ok(TypedValue::Bool(*v)),
        (TypedValue::DateTime(v), IndexType::Datetime) => Ok(TypedValue::DateTime(*v)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

pub(crate) fn typed_value_matches_query(value: &TypedValue, filter: &TypedValue) -> bool {
    match (value, filter) {
        (TypedValue::String(a), TypedValue::String(b)) => a == b,
        (TypedValue::Integer(a), TypedValue::Integer(b)) => a == b,
        (TypedValue::Float(a), TypedValue::Float(b)) => a == b,
        (TypedValue::Bool(a), TypedValue::Bool(b)) => a == b,
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => a == b,
        _ => false,
    }
}

pub(crate) fn typed_value_compare(a: &TypedValue, b: &TypedValue) -> EngineResult<Ordering> {
    match (a, b) {
        (TypedValue::String(a), TypedValue::String(b)) => Ok(a.cmp(b)),
        (TypedValue::Integer(a), TypedValue::Integer(b)) => Ok(a.cmp(b)),
        (TypedValue::Float(a), TypedValue::Float(b)) => a.partial_cmp(b).ok_or(
            EngineError::Invalid("typed index value comparison is undefined"),
        ),
        (TypedValue::Bool(a), TypedValue::Bool(b)) => Ok(a.cmp(b)),
        (TypedValue::DateTime(a), TypedValue::DateTime(b)) => Ok(a.cmp(b)),
        _ => Err(EngineError::Invalid(
            "typed index value is not valid for declared type",
        )),
    }
}

pub(crate) fn merge_entity_document(
    entity: Option<&JsonValue>,
    set_fields: &BTreeMap<String, TypedValue>,
) -> EngineResult<JsonValue> {
    let mut object = match entity {
        Some(JsonValue::Object(map)) => map.clone(),
        Some(_) => {
            return Err(EngineError::Invalid("typed index entity is not an object"));
        }
        None => serde_json::Map::new(),
    };
    for (field, value) in set_fields {
        object.insert(
            field.clone(),
            match value {
                TypedValue::String(v) => JsonValue::String(v.clone()),
                TypedValue::Integer(v) => JsonValue::Number((*v).into()),
                TypedValue::Float(v) => {
                    JsonValue::Number(serde_json::Number::from_f64(*v).ok_or({
                        EngineError::Invalid("typed index value is not valid for declared type")
                    })?)
                }
                TypedValue::Bool(v) => JsonValue::Bool(*v),
                TypedValue::DateTime(v) => JsonValue::Number(v.seconds.into()),
            },
        );
    }
    Ok(JsonValue::Object(object))
}

pub(crate) fn typed_index_row_from_entity(
    spec: &QueueIndex,
    item_id: ItemId,
    entity: &JsonValue,
) -> EngineResult<Option<RangeScanRow>> {
    let mut fields = BTreeMap::new();
    match &spec.declaration {
        IndexDeclaration::Single(def) => {
            let Some(value) = typed_value_for_json(
                entity.get(&def.field).unwrap_or(&JsonValue::Null),
                &def.index_type,
            )?
            else {
                return Ok(None);
            };
            fields.insert(def.field.clone(), value);
        }
        IndexDeclaration::Compound(def) => {
            for field in &def.fields {
                let Some(value) = typed_value_for_json(
                    entity.get(&field.field).unwrap_or(&JsonValue::Null),
                    &field.index_type,
                )?
                else {
                    return Ok(None);
                };
                fields.insert(field.field.clone(), value);
            }
        }
    }
    Ok(Some(RangeScanRow { item_id, fields }))
}

pub(crate) fn typed_index_row_matches(
    spec: &QueueIndex,
    filters: &[QueryFilter],
    row: &RangeScanRow,
) -> EngineResult<bool> {
    let fields: Vec<(&str, &IndexType)> = match &spec.declaration {
        IndexDeclaration::Single(def) => vec![(def.field.as_str(), &def.index_type)],
        IndexDeclaration::Compound(def) => def
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    };
    let mut filter_map: BTreeMap<&str, &QueryFilter> = BTreeMap::new();
    for filter in filters {
        filter_map.insert(filter.field.as_str(), filter);
    }
    let mut prefix_len = 0usize;
    for (field_name, index_type) in &fields {
        let Some(filter) = filter_map.get(field_name).copied() else {
            break;
        };
        let typed = typed_value_from_filter_value(&filter.value, index_type)?;
        let Some(value) = row.fields.get(*field_name) else {
            return Ok(false);
        };
        if filter.op != FilterOp::Eq || !typed_value_matches_query(value, &typed) {
            break;
        }
        prefix_len += 1;
    }
    for filter in filters {
        let Some((idx, (_, index_type))) = fields
            .iter()
            .enumerate()
            .find(|(_, (field_name, _))| *field_name == filter.field.as_str())
        else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        if idx < prefix_len {
            continue;
        }
        let Some(value) = row.fields.get(filter.field.as_str()) else {
            return Ok(false);
        };
        let typed = typed_value_from_filter_value(&filter.value, index_type)?;
        let ord = typed_value_compare(value, &typed)?;
        let ok = match filter.op {
            FilterOp::Eq => ord.is_eq(),
            FilterOp::Gte => ord.is_ge(),
            FilterOp::Gt => ord.is_gt(),
            FilterOp::Lte => ord.is_le(),
            FilterOp::Lt => ord.is_lt(),
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn compare_rows(
    lhs: &RangeScanRow,
    rhs: &RangeScanRow,
    order_by: &[OrderField],
) -> EngineResult<Ordering> {
    for field in order_by {
        let left = lhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let right = rhs
            .fields
            .get(&field.field)
            .ok_or(EngineError::Invalid("unindexed-field"))?;
        let ord = typed_value_compare(left, right)?;
        let ord = match field.direction {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        };
        if !ord.is_eq() {
            return Ok(ord);
        }
    }
    Ok(lhs.item_id.cmp(&rhs.item_id))
}

pub(crate) type TypedIndexRows = Vec<(String, Vec<(String, Vec<u8>)>)>;

pub(crate) fn index_is_unique(qi: &QueueIndex) -> bool {
    match &qi.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

/// Check unique-index constraints for `keys` against existing DB rows. Returns `Conflict` if any
/// unique index already maps the same key to a *different* item. Pass `exclude_item_id = Some(id)`
/// when the item whose old rows were just deleted might still appear in DB (i.e. for UpdateFields).
pub(crate) fn check_typed_unique_conflicts(
    tx: &Transaction<'_>,
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
            Some(excl) => st(tx
                .query_row(
                    "SELECT item_id FROM pqueue_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     AND item_id!=?5 LIMIT 1",
                    params![t, q, name, key.as_slice(), excl],
                    |row| row.get(0),
                )
                .optional())?,
            None => st(tx
                .query_row(
                    "SELECT item_id FROM pqueue_item_index \
                     WHERE tenant_id=?1 AND queue_id=?2 AND index_name=?3 AND index_key=?4 \
                     LIMIT 1",
                    params![t, q, name, key.as_slice()],
                    |row| row.get(0),
                )
                .optional())?,
        };
        if holder.is_some() {
            return Err(EngineError::Conflict);
        }
    }
    Ok(())
}

/// Insert `pqueue_item_index` rows for one item's `(name, key)` pairs (upsert so a retry is safe).
pub(crate) fn insert_typed_index_rows(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    item_id: &str,
    keys: &[(String, Vec<u8>)],
) -> EngineResult<()> {
    for (name, key) in keys {
        st(tx.execute(
            "INSERT INTO pqueue_item_index \
             (tenant_id, queue_id, index_name, index_key, item_id) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(tenant_id,queue_id,index_name,item_id) DO UPDATE SET index_key=excluded.index_key",
            params![t, q, name, key.as_slice(), item_id],
        ))?;
    }
    Ok(())
}

/// Delete all `pqueue_item_index` rows for the given item IDs.
pub(crate) fn delete_typed_index_rows(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    item_ids: &[String],
) -> EngineResult<()> {
    for chunk in item_ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "DELETE FROM pqueue_item_index \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.to_string()), Value::Text(q.to_string())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
    }
    Ok(())
}

/// Pack and check unique conflicts, then insert index rows for all `items` in a push batch.
/// `typed_indexes` must already be resolved from the queue definition.
pub(crate) fn maintain_typed_indexes_on_insert(
    tx: &Transaction<'_>,
    t: &str,
    q: &str,
    typed_indexes: &[QueueIndex],
    items: &[PushItem],
) -> EngineResult<()> {
    if typed_indexes.is_empty() {
        return Ok(());
    }
    // Collect (item_id, keys) and enforce within-batch uniqueness in a single pass.
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    let mut item_keys: TypedIndexRows = Vec::with_capacity(items.len());
    for item in items {
        let keys = typed_index_keys_for_entity(typed_indexes, item.entity_document.as_ref())?;
        // DB-level unique check (no exclusion: new items have no prior rows).
        check_typed_unique_conflicts(tx, t, q, typed_indexes, &keys, None)?;
        // Within-batch: detect two items in the same push sharing a unique key.
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
        insert_typed_index_rows(tx, t, q, item_id, keys)?;
    }
    Ok(())
}

/// Pre-commit (ADR-010 §5.1; `commit` has no rollback) UNIQUE typed-index validation for a push batch on
/// the relational projection: reject with [`EngineError::Conflict`] when inserting `items` would collide on
/// a UNIQUE typed index — against existing durable rows AND against another item earlier in the same batch —
/// mirroring the apply-time enforcement in [`maintain_typed_indexes_on_insert`] (identical key derivation
/// and uniqueness rule). The composed commit path stages every committed entry's pushes into ONE candidate
/// batch and validates here BEFORE the durable log append, so a within-commit duplicate unique key is caught
/// at VALIDATION and the appended batch is always appliable (no recovery poison). Mutates nothing; non-unique
/// indexes and disjoint keys pass; schema-less queues (no typed indexes) short-circuit.
pub(crate) fn validate_typed_unique_push(
    conn: &Connection,
    shard: &QueueKey,
    typed_indexes: &[QueueIndex],
    items: &[PushItem],
) -> EngineResult<()> {
    if typed_indexes.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    // Read-only unchecked transaction (dropped/rolled back — no writes): a stable snapshot for the DB-level
    // unique lookups, exactly the queries `maintain_typed_indexes_on_insert` runs at apply time.
    let tx = st(conn.unchecked_transaction())?;
    let mut batch_unique: std::collections::HashMap<(String, Vec<u8>), String> =
        std::collections::HashMap::new();
    for item in items {
        let keys = typed_index_keys_for_entity(typed_indexes, item.entity_document.as_ref())?;
        // (b) DB-level unique check (no exclusion: pushed items are new and hold no prior rows).
        check_typed_unique_conflicts(&tx, &t, &q, typed_indexes, &keys, None)?;
        // (a) Within-batch: two items in the SAME candidate batch (possibly from different commit entries)
        // sharing a unique key collide — this is the cross-entry duplicate apply enforces only at insert time.
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
    }
    Ok(())
}

/// Pack a timestamp as nanoseconds-since-epoch (comparable in SQL for `not_before`/expiry ordering).
/// Saturating so a far-future timestamp (> ~year 2262) clamps rather than overflow-panics; realistic
/// queue timestamps are far inside the i64-nanos range.
pub(crate) fn ts_nanos(ts: UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.nanoseconds as i64)
}

pub(crate) fn ts_nanos_opt(ts: Option<UtcTimestamp>) -> Option<i64> {
    ts.map(ts_nanos)
}

pub(crate) fn nanos_ts(v: i64) -> UtcTimestamp {
    UtcTimestamp::new(
        v.div_euclid(1_000_000_000),
        v.rem_euclid(1_000_000_000) as u32,
    )
    .expect("nanoseconds bounded by rem_euclid")
}

pub(crate) fn state_str(s: ItemState) -> &'static str {
    match s {
        ItemState::Pending => "Pending",
        ItemState::Leased => "Leased",
        ItemState::Complete => "Complete",
        ItemState::Failed => "Failed",
    }
}

pub(crate) fn parse_state(s: &str) -> EngineResult<ItemState> {
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
pub(crate) fn elig_sort(priority: &Option<PriorityValue>, model: &PriorityModel) -> Vec<u8> {
    match priority {
        Some(p) => {
            let mut v = vec![0u8];
            v.extend(priority_sort(p, model));
            v
        }
        None => vec![1u8],
    }
}

pub(crate) fn lease_hash(token: &LeaseToken) -> Vec<u8> {
    Sha256::digest(token.as_str().as_bytes()).to_vec()
}

pub(crate) fn parse_priority(raw: Option<String>) -> EngineResult<Option<PriorityValue>> {
    raw.map(|s| serde_json::from_str(&s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
}

pub(crate) fn is_fifo_claim_scan_item(item: &PushItem) -> bool {
    item.priority.is_none()
        && item.not_before.is_none()
        && item.group_key.is_none()
        && item.cohort_size.is_none()
        && item.gate_keys.is_empty()
}

pub(crate) fn reset_claim_scan_hint(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
) {
    claim_scan_hints.remove(shard);
    claim_scan_default_fifo.insert(shard.clone(), false);
}

pub(crate) fn observe_push_for_claim_scan(
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    items: &[PushItem],
) {
    if items.iter().all(is_fifo_claim_scan_item) {
        claim_scan_default_fifo.entry(shard.clone()).or_insert(true);
    } else {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
    }
}

pub(crate) fn advance_claim_scan_hint_for_ids(
    tx: &Transaction<'_>,
    claim_scan_hints: &mut HashMap<QueueKey, i64>,
    claim_scan_default_fifo: &mut HashMap<QueueKey, bool>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    if item_ids.is_empty() || !claim_scan_default_fifo.get(shard).copied().unwrap_or(false) {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    let ids: Vec<String> = item_ids.iter().map(|id| id.to_string()).collect();
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT COUNT(*), MAX(rowid), \
             COALESCE(SUM(CASE WHEN priority IS NOT NULL OR not_before IS NOT NULL \
             OR group_key IS NOT NULL OR cohort_size IS NOT NULL THEN 1 ELSE 0 END), 0) \
             FROM pqueue_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let (count, chunk_max_rowid, chunk_rich): (i64, Option<i64>, i64) =
            st(tx.query_row(&sql, params_from_iter(p.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            }))?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_max_rowid {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != item_ids.len() as i64 || rich_rows > 0 {
        reset_claim_scan_hint(claim_scan_hints, claim_scan_default_fifo, shard);
        return Ok(());
    }
    if let Some(rowid) = max_rowid {
        let next = rowid
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("claim scan hint overflow".into()))?;
        let slot = claim_scan_hints.entry(shard.clone()).or_insert(0);
        if next > *slot {
            *slot = next;
        }
    }
    Ok(())
}

pub(crate) fn fifo_rowid_range_for_id_strings(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[String],
    expected_state: Option<&str>,
) -> EngineResult<Option<(i64, i64)>> {
    if ids.is_empty() {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let mut seen = 0_i64;
    let mut min_rowid: Option<i64> = None;
    let mut max_rowid: Option<i64> = None;
    let mut rich_rows = 0_i64;
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT COUNT(*), MIN(rowid), MAX(rowid), \
             COALESCE(SUM(CASE WHEN priority IS NOT NULL OR not_before IS NOT NULL \
             OR group_key IS NOT NULL OR cohort_size IS NOT NULL THEN 1 ELSE 0 END), 0) \
             FROM pqueue_items WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let (count, chunk_min, chunk_max, chunk_rich): (i64, Option<i64>, Option<i64>, i64) =
            st(conn.query_row(&sql, params_from_iter(p.iter()), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            }))?;
        seen += count;
        rich_rows += chunk_rich;
        if let Some(rowid) = chunk_min {
            min_rowid = Some(min_rowid.map_or(rowid, |current| current.min(rowid)));
        }
        if let Some(rowid) = chunk_max {
            max_rowid = Some(max_rowid.map_or(rowid, |current| current.max(rowid)));
        }
    }
    if seen != ids.len() as i64 || rich_rows > 0 {
        return Ok(None);
    }
    let (Some(min_rowid), Some(max_rowid)) = (min_rowid, max_rowid) else {
        return Ok(None);
    };
    let range_count: i64 = if let Some(state) = expected_state {
        st(conn.query_row(
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4 AND lifecycle_state=?5",
            params![t, q, min_rowid, max_rowid, state],
            |row| row.get(0),
        ))?
    } else {
        st(conn.query_row(
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND rowid BETWEEN ?3 AND ?4",
            params![t, q, min_rowid, max_rowid],
            |row| row.get(0),
        ))?
    };
    if range_count == ids.len() as i64 {
        Ok(Some((min_rowid, max_rowid)))
    } else {
        Ok(None)
    }
}
