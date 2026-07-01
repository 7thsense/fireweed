//! # Relational projection family (sqlite) — BQ-11a
//!
//! A SECOND, **DB-authoritative** projection family for sqlite (ADR-008 / TD-001 relational class),
//! distinct from the log-replay [`crate::ComposedSqliteBackend`]. Here the `pqueue_items` SQL table **is** the
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
//! REQUEST-ID IDEMPOTENCY (BQ-11e slice): `pqueue_request_idempotency` is wired for the first
//! request-id-carrying data-plane path, BatchPush. That proves the TD-002 relational table/replay flow
//! without claiming full API-001 coverage for every mutating operation. Claim replay and finalize/update
//! replay remain later request-id-carrying port work.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axon_esf::CompiledSchema;
use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, CohortId, GroupKey, IndexDeclaration, IndexType, ItemId, ItemState, LeaseToken,
    Metadata, PriorityModel, PriorityValue, QueueDefinition, QueueId, QueueIndex, RequestId,
    TenantId, UtcTimestamp, is_retry_exhausted, priority_sort,
};
use pqueue_engine::ClaimUnit;
use pqueue_engine::{
    ActiveScope, AdvanceInstanceFenceCommand, Backend, ClaimCommand, ClaimCompatibility, ClaimPort,
    ClaimRef, ClaimRequest, Claimed, ClaimedItem, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortFinalizePort, CohortLeaseTarget, CohortRenewLeaseCommand,
    CohortRenewLeasePort, CommandEnvelope, CommandPosition, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionEntry, ControlPlaneStore,
    CreateQueueOutcome, DiscoveryGranularity, DiscoveryPort, DurabilityClass, EngineError,
    EngineResult, EntryRecovery, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort,
    IndexHit, IndexQueryPort, ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, LogWriter,
    PayloadUpdate, ProjectionRead, ProjectionWriter, PurgeItemsCommand, PurgePort, PushCommand,
    PushItem, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort,
    RenewLeaseCommand, RenewLeasePort, ReplacePendingCommand, RequestOutcome, SetGatesCommand,
    SetGatesPort, TickReport, UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort,
    WriteSideRecordsCommand, build_push_items, compile_entity_schema, project_scopes,
    validate_claim_compatibility, validate_entity, validate_gate_push, validate_instance_fence,
    validate_purge_force,
};
use pqueue_engine::{
    CommandPage, ComposedBackend, InProcessControlPlane, LogStore, ProjectionSnapshot,
    ProjectionStore, SnapshotRef,
};
use pqueue_projection::{InMemoryProjection, ProjectionImage, ProjectionImageItem};
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

fn st<T>(r: rusqlite::Result<T>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(|e| EngineError::Storage(e.to_string()))
}

const IDEMPOTENCY_OPERATION_PUSH: &str = "push";

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

fn positions_to_json(positions: &[CommandPosition]) -> EngineResult<String> {
    let raw: Vec<(u64, u64)> = positions
        .iter()
        .map(|pos| (pos.backend_epoch, pos.sequence))
        .collect();
    to_json(&raw)
}

fn check_request_idempotency(
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
fn record_request_idempotency(
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
const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";

/// Stable body fingerprint for the commit path: SHA-256 over the serialized entries (the `request_id` is the
/// cache KEY, not part of the body — same shape as [`push_request_fingerprint`]). A different body under the
/// same request id is a `RequestIdConflict`; an equal body replays the stored per-entry outcomes.
fn commit_request_fingerprint(entries: &[CommitTransitionEntry]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes).to_vec())
}

/// Durable, replay-faithful mirror of an [`EntryRecovery`] (which carries non-`Serialize` types — an
/// [`EngineError`] in its rejected arm and an [`ItemId`]). Projected to this shape for the
/// `pqueue_request_idempotency.response_payload` column and reconstructed verbatim on replay AND for the
/// recovery/explain read (epic pqueue-2201fd37 acceptance #5). A `None` `rejected` means the entry committed.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredEntryRecovery {
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
        EngineError::Forbidden(why) => ("forbidden", Some((*why).to_string())),
        EngineError::Storage(msg) => ("storage", Some(msg.clone())),
        EngineError::EntitySchemaViolation(msg) => ("entity_schema_violation", Some(msg.clone())),
    }
}

/// Reconstruct an [`EngineError`] from its durable `(code, detail)` projection. `Invalid` reasons this path
/// emits ("item is not leased") round-trip to the same `&'static str`; any other reason falls back to a
/// stable static so the variant (and its `PartialEq`) is preserved.
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

/// Project the retained per-entry recovery records into the public per-entry outcomes (the commit return /
/// replay value), mirroring the in-memory `outcomes_from_recovery`.
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

/// Commit-path twin of [`check_request_idempotency`]: same retained-request-id table + replay / conflict /
/// expired classification, but the stored `response_payload` is the rich per-entry outcome vector (encoded
/// via [`encode_commit_outcomes`]) rather than a flat id list. A live record with an equal fingerprint
/// REPLAYS the prior outcomes; a different fingerprint is `RequestIdConflict`; an expired/absent record
/// returns `None` (proceed fresh, deleting the stale row).
fn check_commit_idempotency(
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
fn read_commit_recovery(
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
fn record_commit_idempotency(
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
fn commit_validate_sql(
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

fn ensure_item_text_column(
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

fn ensure_item_fields_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "fields", "{}")
}

fn ensure_item_metadata_column(conn: &Connection) -> EngineResult<()> {
    ensure_item_text_column(conn, "metadata", "{}")
}

fn ensure_item_entity_document_column(conn: &Connection) -> EngineResult<()> {
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

fn ensure_cohort_column(conn: &Connection, column: &str, definition: &str) -> EngineResult<()> {
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

fn ensure_cohort_lifecycle_columns(conn: &Connection) -> EngineResult<()> {
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

fn parts(shard: &QueueKey) -> (String, String) {
    (
        shard.tenant_id.as_str().to_string(),
        shard.queue_id.as_str().to_string(),
    )
}

// ---------------------------------------------------------------------------
// ADR-011 typed secondary index helpers
// ---------------------------------------------------------------------------

/// Decode a caller-supplied raw lookup byte slice into a `serde_json::Value` for re-encoding via
/// `IndexDef::index_key` / `CompoundIndexDef::index_key`. Mirrors `decode_typed_lookup_value` in
/// `pqueue_projection` — the two must stay identical so lookup keys byte-match stored keys.
fn decode_typed_lookup_value_rel(index_type: &IndexType, bytes: &[u8]) -> EngineResult<JsonValue> {
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
fn typed_lookup_canonical_key(qi: &QueueIndex, key_values: &[Vec<u8>]) -> EngineResult<Vec<u8>> {
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
fn typed_index_keys_for_entity(
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

type TypedIndexRows = Vec<(String, Vec<(String, Vec<u8>)>)>;

fn index_is_unique(qi: &QueueIndex) -> bool {
    match &qi.declaration {
        IndexDeclaration::Single(def) => def.unique,
        IndexDeclaration::Compound(def) => def.unique,
    }
}

/// Check unique-index constraints for `keys` against existing DB rows. Returns `Conflict` if any
/// unique index already maps the same key to a *different* item. Pass `exclude_item_id = Some(id)`
/// when the item whose old rows were just deleted might still appear in DB (i.e. for UpdateFields).
fn check_typed_unique_conflicts(
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
fn insert_typed_index_rows(
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
fn delete_typed_index_rows(
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
fn maintain_typed_indexes_on_insert(
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
    /// Compiled entity schemas (ADR-011). Rebuilt from `queues` on open; keyed by queue.
    schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    grouped_shards: HashSet<QueueKey>,
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
        self.grouped_shards.clear();
        let mut stmt = st(self.conn.prepare(
            "SELECT DISTINCT tenant_id, queue_id FROM pqueue_items WHERE group_key IS NOT NULL",
        ))?;
        let mapped = st(stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        }))?;
        for r in mapped {
            let (tenant, queue) = st(r)?;
            self.grouped_shards.insert(QueueKey::new(
                TenantId::new(tenant).map_err(|e| EngineError::Storage(e.to_string()))?,
                QueueId::new(queue).map_err(|e| EngineError::Storage(e.to_string()))?,
            ));
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
            grouped_shards,
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
            grouped_shards,
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

/// Max rows per dynamically-built multi-row / `IN (...)` statement. Each `pqueue_items` row binds 17
/// params; 256 rows ≈ 4.4k params, well under sqlite's 32766 bound-variable ceiling (bundled SQLite).
const SQLITE_BATCH: usize = 256;
const COHORT_EXPIRY_SWEEP_LIMIT: usize = 128;
const GROUP_DUE_REFRESH_LIMIT: i64 = 128;

fn opt_text(v: Option<String>) -> Value {
    v.map_or(Value::Null, Value::Text)
}
fn opt_int(v: Option<i64>) -> Value {
    v.map_or(Value::Null, Value::Integer)
}
fn opt_blob(v: Option<Vec<u8>>) -> Value {
    v.map_or(Value::Null, Value::Blob)
}

/// Batch-insert all `items` of a Push (or the single ReplacePending replacement) as set-based statements:
/// chunked multi-row INSERTs into `pqueue_items`, `pqueue_item_gates`, and `pqueue_cohorts` — replacing the
/// former per-item `insert_item` (N+ round-trips → a handful, chunked to the bound-variable limit). Column
/// values, the `fields` TEXT-JSON encoding, and the `eligible_since`/`not_before` pairing are identical to
/// the per-item path; `created_seq` is bulk-allocated (`base + i`) so the FIFO order is preserved.
fn insert_items(
    tx: &Transaction<'_>,
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
    // Bulk-allocate the stable FIFO positions in one read+advance (was a read+UPDATE per item).
    let base_seq: i64 = st(tx.query_row(
        "SELECT next_item_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;
    st(tx.execute(
        "UPDATE relational_cursor SET next_item_seq=?3 WHERE tenant=?1 AND queue=?2",
        params![t, q, base_seq + items.len() as i64],
    ))?;
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let not_before = ts_nanos_opt(item.not_before);
        rows.push(vec![
            Value::Text(t.clone()),
            Value::Text(q.clone()),
            Value::Text(item.item_id.to_string()),
            Value::Text(item.client_item_key.as_str().to_string()),
            opt_text(item.priority.as_ref().map(to_json).transpose()?),
            Value::Blob(elig_sort(&item.priority, model)),
            opt_int(not_before),
            Value::Integer(not_before.unwrap_or(now_n)),
            opt_text(item.group_key.as_ref().map(|g| g.as_str().to_string())),
            opt_int(item.cohort_size.map(|s| s as i64)),
            opt_blob(item.payload.as_ref().map(|b| b.to_vec())),
            Value::Text(fields_to_json(&item.fields)?),
            Value::Text(metadata_to_json(&item.metadata)?),
            opt_text(item.entity_document.as_ref().map(to_json).transpose()?),
            Value::Integer(seqi),
            Value::Integer(now_n),
            Value::Integer(now_n),
            Value::Integer(item.max_attempts as i64),
            Value::Integer(base_seq + i as i64),
        ]);
    }
    const ROW_PH: &str =
        "(?,?,?,?,'Pending',?,?,?,?,?,?,?,?,?,?,0,1,NULL,NULL,NULL,?,?,?,NULL,0,0,?,?)";
    for chunk in rows.chunks(SQLITE_BATCH) {
        let values = vec![ROW_PH; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO pqueue_items \
             (tenant_id,queue_id,item_id,client_item_key,lifecycle_state,priority,priority_sort,\
              not_before,eligible_since,group_key,cohort_size,payload,fields,metadata,entity_document,retry_count,\
              item_version,lease_token_hash,lease_expires_at,worker_id,last_command_sequence,created_at,\
              updated_at,terminal_at,fenced,superseded,max_attempts,created_seq) VALUES {values}"
        );
        let flat = chunk.iter().flatten();
        st(tx.execute(&sql, params_from_iter(flat)))?;
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
/// one statement never proposes the same `(item_id, gate_key)` twice.
fn insert_gates(tx: &Transaction<'_>, t: &str, q: &str, items: &[PushItem]) -> EngineResult<()> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for item in items {
        let id = item.item_id.to_string();
        for gk in &item.gate_keys {
            let g = gk.as_str().to_string();
            if !pairs.iter().any(|(a, b)| a == &id && b == &g) {
                pairs.push((id.clone(), g));
            }
        }
    }
    if pairs.is_empty() {
        return Ok(());
    }
    for chunk in pairs.chunks(SQLITE_BATCH) {
        let values = vec!["(?,?,?,?)"; chunk.len()].join(",");
        let sql = format!(
            "INSERT INTO pqueue_item_gates (tenant_id,queue_id,item_id,gate_key) VALUES {values} \
             ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING"
        );
        let mut p: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
        for (id, g) in chunk {
            p.push(Value::Text(t.to_string()));
            p.push(Value::Text(q.to_string()));
            p.push(Value::Text(id.clone()));
            p.push(Value::Text(g.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
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
    tx: &Transaction<'_>,
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
        let existing: Option<(i64, i64, String, Option<i64>)> = st(tx
            .query_row(
                "SELECT cohort_size, member_count, state, retention_until FROM pqueue_cohorts \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                params![t, q, gk],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional())?;
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
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?8)",
                    params![
                        t,
                        q,
                        gk,
                        cohort_id_for(&gk, now_n),
                        size,
                        added,
                        state,
                        now_n,
                        first_eligible_at,
                    ],
                ))?;
            }
            Some((existing_size, member_count, state, retention_until)) => {
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
                        "UPDATE pqueue_cohorts SET cohort_id=?4, cohort_size=?5, member_count=?6, \
                         state=?7, cohort_created_at=?8, first_eligible_at=?9, expire_command_pos=NULL, \
                         cohort_lease_token_hash=NULL, retention_until=NULL, created_at=?8 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                        params![
                            t,
                            q,
                            gk,
                            cohort_id_for(&gk, now_n),
                            size,
                            added,
                            next_state,
                            now_n,
                            first_eligible_at,
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
                    "UPDATE pqueue_cohorts SET member_count=?4, state=?5, \
                     first_eligible_at=CASE WHEN ?6 AND first_eligible_at IS NULL THEN ?7 ELSE first_eligible_at END \
                     WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                    params![t, q, gk, next_count, next_state, set_first, now_n],
                ))?;
            }
        }
    }
    Ok(())
}

/// Run `{prefix} (chunk)` (e.g. an `UPDATE … item_id IN` or `DELETE … item_id IN`) once per ≤256-id chunk.
/// `lead` are the bound values for the prefix's leading placeholders (the SET clause, if any); the prefix's
/// trailing `tenant_id=? AND queue_id=?` then bind `t`,`q`, followed by the chunk ids. Chunking keeps the
/// bound-variable count under sqlite's limit.
fn exec_items_in(
    tx: &Transaction<'_>,
    prefix: &str,
    lead: &[Value],
    t: &str,
    q: &str,
    ids: &[String],
) -> EngineResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    for chunk in ids.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!("{prefix} ({ph})");
        let mut p: Vec<Value> = lead.to_vec();
        p.push(Value::Text(t.to_string()));
        p.push(Value::Text(q.to_string()));
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        st(tx.execute(&sql, params_from_iter(p.iter())))?;
    }
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let mut seen: Vec<GroupKey> = Vec::new();
    // One set-based round-trip per chunk (was one SELECT per item): distinct non-null group keys.
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT DISTINCT group_key FROM pqueue_items WHERE tenant_id=? AND queue_id=? \
             AND item_id IN ({ph}) AND group_key IS NOT NULL"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(tx.prepare(&sql))?;
        let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| row.get::<_, String>(0)))?;
        for r in mapped {
            let gk = GroupKey::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?;
            if !seen.contains(&gk) {
                seen.push(gk);
            }
        }
    }
    Ok(seen)
}

fn cohort_group_for_id(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<GroupKey> {
    let (t, q) = parts(shard);
    let group: String = st(tx.query_row(
        "SELECT group_key FROM pqueue_cohorts WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
        params![t, q, cohort_id.as_str()],
        |row| row.get(0),
    ))?;
    GroupKey::new(group).map_err(|e| EngineError::Storage(e.to_string()))
}

fn cohort_item_ids(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    cohort_id: &CohortId,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let group = cohort_group_for_id(tx, shard, cohort_id)?;
    let mut stmt = st(tx.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
         AND superseded=0 AND cohort_size IS NOT NULL AND lifecycle_state NOT IN ('Complete','Failed') \
         ORDER BY priority_sort, created_seq",
    ))?;
    let mapped = st(stmt.query_map(params![t, q, group.as_str()], |row| row.get::<_, String>(0)))?;
    let mut out = Vec::new();
    for r in mapped {
        out.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// Recompute `pqueue_group_summary` for one group from `pqueue_items` (exact aggregate over the group's
/// currently-eligible items, in the SAME transaction as the mutation that touched it). The representative
/// is the would-be-first-claimed eligible item (strict-claim key `priority_sort, created_seq`), matching
/// the claim selection, including live gate state; `rep_progress_guard_sort`/`at_risk_count` stay NULL/0
/// while the progress-guard derivation is deferred (parity with the strict claim ordering, BQ-14).
///
/// EXACT AT MUTATION TIME, lagged across a time-only `not_before` crossing: the aggregate filters
/// `not_before<=now`, so a deferred item that becomes due WITHOUT a subsequent mutation is not reflected
/// in `oldest_eligible_at`/`rep_*`/`eligible_item_count` until the next mutation refreshes its group. The
/// per-item `select_eligible` path re-evaluates `not_before` on read and is unaffected. BQ-14 g1/g4
/// consumers refresh due groups before mutation-backed group claims; read-only discovery still cannot
/// mutate and therefore may under-report until a due sweep or later mutation refreshes the group.
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
         AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id)",
        params![t, q, group_key.as_str(), now_n],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ))?;
    // Representative = first-claimable eligible item of the group.
    let rep: Option<(Vec<u8>, i64, String)> = st(tx
        .query_row(
            "SELECT priority_sort, created_at, item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3 \
             AND lifecycle_state='Pending' AND superseded=0 AND (not_before IS NULL OR not_before<=?4) \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                 ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
                 AND ig.item_id=pqueue_items.item_id) \
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
#[allow(clippy::too_many_arguments)]
fn apply_command_sql(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    grouped_shards: &mut HashSet<QueueKey>,
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
            insert_items(tx, queues, &model, shard, &c.items, seq, now)?;
            let mut groups: Vec<GroupKey> = Vec::new();
            for it in &c.items {
                if let Some(g) = &it.group_key
                    && !groups.contains(g)
                {
                    groups.push(g.clone());
                }
            }
            if !groups.is_empty() {
                grouped_shards.insert(shard.clone());
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
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?, \
                 lease_expires_at=?, retry_count=retry_count+1, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Blob(hash),
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::CohortClaim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=?, \
                 lease_expires_at=?, retry_count=retry_count+1, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Blob(hash.clone()),
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='leased', cohort_lease_token_hash=?4 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![t, q, c.cohort_id.as_str(), hash],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
            }
            Ok(())
        }
        QueueCommand::RenewLease(c) => {
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_expires_at=?, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            Ok(())
        }
        QueueCommand::CohortRenewLease(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
            let exp = ts_nanos(c.lease_expires_at);
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_expires_at=?, item_version=item_version+1, \
                 updated_at=?, last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &id_strs,
            )?;
            Ok(())
        }
        QueueCommand::ReassignLease(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lease_token_hash=?, lease_expires_at=?, \
                 retry_count=retry_count+1, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Blob(hash),
                    Value::Integer(exp),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            Ok(())
        }
        QueueCommand::UpdateFields(c) => {
            // FAC-1 in-place merge of a LIVE item's fields/payload (no lifecycle change). Read-merge-write
            // the `fields` JSON map in the same representation as insert/read (`fields_to_json`/
            // `fields_from_json`), apply the per-key delta, then UPDATE within this transaction. The caller
            // pre-validated, so the row is live (Pending/Leased, not superseded/fenced); if it is gone here
            // (a divergence) we apply nothing rather than fault, mirroring the in-memory `debug_assert`.
            let current: Option<String> = st(tx
                .query_row(
                    "SELECT fields FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                     AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                    params![t, q, c.item_id.to_string()],
                    |row| row.get(0),
                )
                .optional())?;
            if let Some(raw) = current {
                let mut fields = fields_from_json(raw)?;
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
                    // Keep: leave `payload` untouched (fields-only update).
                    PayloadUpdate::Keep => {
                        st(tx.execute(
                            "UPDATE pqueue_items SET fields=?4, item_version=item_version+1, \
                             updated_at=?5, last_command_sequence=?6 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                             AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                            params![t, q, c.item_id.to_string(), fields_json, now_n, seq as i64],
                        ))?;
                    }
                    // Set(Some)=replace BLOB, Set(None)=NULL.
                    PayloadUpdate::Set(p) => {
                        let payload = p.as_ref().map(|b| b.to_vec());
                        st(tx.execute(
                            "UPDATE pqueue_items SET fields=?4, payload=?5, item_version=item_version+1, \
                             updated_at=?6, last_command_sequence=?7 \
                             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
                             AND lifecycle_state IN ('Pending','Leased') AND superseded=0 AND fenced=0",
                            params![
                                t,
                                q,
                                c.item_id.to_string(),
                                fields_json,
                                payload,
                                now_n,
                                seq as i64,
                            ],
                        ))?;
                    }
                }
                if let Some(ref doc) = c.set_entity_document {
                    st(tx.execute(
                        "UPDATE pqueue_items SET entity_document=?4 \
                         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                        params![t, q, c.item_id.to_string(), to_json(doc)?],
                    ))?;
                }
                // ADR-011: if a new entity document was supplied, re-index this item. Delete the
                // old rows first so the unique slot is freed before the conflict check fires.
                if let Some(ref doc) = c.set_entity_document {
                    let typed_indexes = queues
                        .get(shard)
                        .map(|d| d.typed_indexes.as_slice())
                        .unwrap_or(&[]);
                    if !typed_indexes.is_empty() {
                        let item_id_str = c.item_id.to_string();
                        delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&item_id_str))?;
                        let new_keys = typed_index_keys_for_entity(typed_indexes, Some(doc))?;
                        check_typed_unique_conflicts(tx, &t, &q, typed_indexes, &new_keys, None)?;
                        insert_typed_index_rows(tx, &t, &q, &item_id_str, &new_keys)?;
                    }
                }
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
            for chunk in retry_ids.chunks(SQLITE_BATCH) {
                if chunk.is_empty() {
                    break;
                }
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT item_id, retry_count, max_attempts FROM pqueue_items \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
                );
                let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
                for id in chunk {
                    p.push(Value::Text(id.clone()));
                }
                let mut stmt = st(tx.prepare(&sql))?;
                let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }))?;
                for r in mapped {
                    let (id, rc, ma) = st(r)?;
                    retry_info.insert(id, (rc, ma));
                }
            }
            // Bucket outcomes by the target SET (disposition fully determines new_state/terminal/reset → at
            // most four buckets), then issue ONE UPDATE per bucket.
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
                        // Retry-exhaustion (B'): a retry that has used all `max_attempts` deliveries goes
                        // terminal (Failed); under the bound it returns to pending (claimable again).
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
            const FINALIZE_SET: &str = "UPDATE pqueue_items SET lifecycle_state=?, lease_token_hash=NULL, \
                 lease_expires_at=NULL, fenced=0, item_version=item_version+1, \
                 retry_count=CASE WHEN ? THEN 0 ELSE retry_count END, terminal_at=?, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN";
            let buckets = [
                (
                    state_str(ItemState::Complete),
                    false,
                    Value::Integer(now_n),
                    &to_complete,
                ),
                (
                    state_str(ItemState::Failed),
                    false,
                    Value::Integer(now_n),
                    &to_failed,
                ),
                (
                    state_str(ItemState::Pending),
                    false,
                    Value::Null,
                    &to_pending,
                ),
                (
                    state_str(ItemState::Pending),
                    true,
                    Value::Null,
                    &to_pending_rearm,
                ),
            ];
            for (state, reset, terminal_at, ids) in buckets {
                exec_items_in(
                    tx,
                    FINALIZE_SET,
                    &[
                        Value::Text(state.to_string()),
                        Value::Integer(reset as i64),
                        terminal_at,
                        Value::Integer(now_n),
                        Value::Integer(seq as i64),
                    ],
                    &t,
                    &q,
                    ids,
                )?;
            }
            for (nb_n, ids) in &backoff {
                exec_items_in(
                    tx,
                    "UPDATE pqueue_items SET not_before=?, eligible_since=? \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN",
                    &[Value::Integer(*nb_n), Value::Integer(*nb_n)],
                    &t,
                    &q,
                    ids,
                )?;
            }
            let ids: Vec<ItemId> = c.outcomes.iter().map(|o| o.item_id).collect();
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
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
                grouped_shards,
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
                "UPDATE pqueue_cohorts SET state=?4, cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
                params![t, q, c.cohort_id.as_str(), next_state, retention_until],
            ))?;
            Ok(())
        }
        QueueCommand::ReplacePending(c) => {
            // Supersede the old pending item (drops it from the active partial-unique index + eligibility),
            // then insert the replacement under the same client_item_key.
            // ADR-011: delete the superseded item's index rows first so the replacement can claim
            // the same unique key without a spurious Conflict.
            let superseded_str = c.superseded_item_id.to_string();
            delete_typed_index_rows(tx, &t, &q, std::slice::from_ref(&superseded_str))?;
            st(tx.execute(
                "UPDATE pqueue_items SET superseded=1, updated_at=?4, last_command_sequence=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                params![t, q, c.superseded_item_id.to_string(), now_n, seq as i64],
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
            // Refresh both the superseded item's group and the replacement's (often the same).
            let mut groups = if grouped_shards.contains(shard) {
                groups_of(tx, shard, std::slice::from_ref(&c.superseded_item_id))?
            } else {
                Vec::new()
            };
            if let Some(g) = &c.replacement.group_key
                && !groups.contains(g)
            {
                grouped_shards.insert(shard.clone());
                groups.push(g.clone());
            }
            for g in &groups {
                refresh_group_summary(tx, shard, g, now)?;
            }
            Ok(())
        }
        QueueCommand::LeaseExpired(c) => {
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Pending', lease_token_hash=NULL, \
                 lease_expires_at=NULL, item_version=item_version+1, updated_at=?, \
                 last_command_sequence=? WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[Value::Integer(now_n), Value::Integer(seq as i64)],
                &t,
                &q,
                &ids,
            )?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            if grouped_shards.contains(shard) {
                for g in groups_of(tx, shard, &c.item_ids)? {
                    refresh_group_summary(tx, shard, &g, now)?;
                }
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
            let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
            // Force terminal only (parity with the in-memory arm, which leaves the lease fields as-is on the
            // now-terminal row); the live token is dropped from the RAM map post-commit.
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET lifecycle_state='Failed', item_version=item_version+1, \
                 terminal_at=?, updated_at=?, last_command_sequence=? \
                 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[
                    Value::Integer(now_n),
                    Value::Integer(now_n),
                    Value::Integer(seq as i64),
                ],
                &t,
                &q,
                &id_strs,
            )?;
            for id in &ids {
                token_ops.push(TokenOp::Clear(*id));
            }
            st(tx.execute(
                "UPDATE pqueue_cohorts SET state='terminal', expire_command_pos=?4, \
                 cohort_lease_token_hash=NULL, retention_until=?5 \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key=?3",
                params![
                    t,
                    q,
                    c.group_key.as_str(),
                    seq as i64,
                    cohort_retention_until(queues, shard, now_n)?,
                ],
            ))?;
            // The whole cohort (group) is now terminal — refresh its summary to empty.
            refresh_group_summary(tx, shard, &c.group_key, now)?;
            Ok(())
        }
        QueueCommand::FenceLease(c) => {
            // Operator fence: no item_version bump (parity with the in-memory arm).
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET fenced=1 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &ids,
            )?;
            Ok(())
        }
        QueueCommand::UnfenceLease(c) => {
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            exec_items_in(
                tx,
                "UPDATE pqueue_items SET fenced=0 WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &ids,
            )?;
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
            let id_strs: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            let mut groups: Vec<GroupKey> = Vec::new();
            // (client_item_key, item_id) tombstones for terminal items, deduped LAST-wins on key so the
            // batched upsert never touches the same conflict target twice (DO UPDATE cardinality).
            let mut retention: Vec<(String, String)> = Vec::new();
            // One set-based read of every purged item (was one SELECT per item).
            for chunk in id_strs.chunks(SQLITE_BATCH) {
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT item_id, group_key, client_item_key, lifecycle_state FROM pqueue_items \
                     WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
                );
                let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
                for id in chunk {
                    p.push(Value::Text(id.clone()));
                }
                let mut stmt = st(tx.prepare(&sql))?;
                let mapped = st(stmt.query_map(params_from_iter(p.iter()), |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<String>>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                }))?;
                for r in mapped {
                    let (item_id, gk, ck, state) = st(r)?;
                    // TD-002 retention tombstone: purging a TERMINAL item keeps its client_item_key a
                    // duplicate (re-push rejected) until `client_item_key_retention_ms` elapses. A pending
                    // purge records nothing (its key is freely reusable, matching the log-replay family).
                    if parse_state(&state)?.is_terminal() && retention_ms > 0 {
                        retention.retain(|(k, _)| k != &ck);
                        retention.push((ck, item_id));
                    }
                    if let Some(g) = gk {
                        let gk2 =
                            GroupKey::new(g).map_err(|e| EngineError::Storage(e.to_string()))?;
                        if !groups.contains(&gk2) {
                            groups.push(gk2);
                        }
                    }
                }
            }
            if !retention.is_empty() {
                let expires = now_n.saturating_add((retention_ms as i64).saturating_mul(1_000_000));
                for chunk in retention.chunks(SQLITE_BATCH) {
                    let values = vec!["(?,?,?,?,?)"; chunk.len()].join(",");
                    let sql = format!(
                        "INSERT INTO pqueue_item_key_retention \
                         (tenant_id,queue_id,client_item_key,item_id,expires_at) VALUES {values} \
                         ON CONFLICT(tenant_id,queue_id,client_item_key) \
                         DO UPDATE SET item_id=excluded.item_id, expires_at=excluded.expires_at"
                    );
                    let mut p: Vec<Value> = Vec::with_capacity(chunk.len() * 5);
                    for (ck, item_id) in chunk {
                        p.push(Value::Text(t.clone()));
                        p.push(Value::Text(q.clone()));
                        p.push(Value::Text(ck.clone()));
                        p.push(Value::Text(item_id.clone()));
                        p.push(Value::Integer(expires));
                    }
                    st(tx.execute(&sql, params_from_iter(p.iter())))?;
                }
            }
            // Set-based deletes (item rows + their gate membership) — one round-trip per chunk each.
            exec_items_in(
                tx,
                "DELETE FROM pqueue_items WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
            // BQ-14d: drop the purged items' gate membership (the anti-join source).
            exec_items_in(
                tx,
                "DELETE FROM pqueue_item_gates WHERE tenant_id=? AND queue_id=? AND item_id IN",
                &[],
                &t,
                &q,
                &id_strs,
            )?;
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
        // C9 (epic pqueue-2201fd37): opaque NON-WORK side records (Snorri authoritative-commit boundary).
        // Upsert each (key,payload) into `pqueue_side_records` — a table disjoint from `pqueue_items`, so a
        // side record is never claimable/eligible/peekable nor counted as work. Apply is infallible
        // (insert-or-overwrite by key), exactly like the in-memory `side_records` map.
        QueueCommand::WriteSideRecords(c) => {
            for rec in &c.records {
                st(tx.execute(
                    "INSERT INTO pqueue_side_records (tenant_id,queue_id,key,payload) \
                     VALUES (?1,?2,?3,?4) \
                     ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET payload=excluded.payload",
                    params![t, q, rec.key, rec.payload.as_ref()],
                ))?;
            }
            Ok(())
        }
        // C6 (epic pqueue-2201fd37): advance a caller-supplied opaque instance/state fence. Validated
        // pre-commit (stored==expected, next>expected), so the upsert is infallible. Disjoint from
        // `pqueue_items` — a fence is never claimable/peekable work.
        QueueCommand::AdvanceInstanceFence(c) => {
            st(tx.execute(
                "INSERT INTO pqueue_instance_fences (tenant_id,queue_id,instance_key,fence) \
                 VALUES (?1,?2,?3,?4) \
                 ON CONFLICT(tenant_id,queue_id,instance_key) DO UPDATE SET fence=excluded.fence",
                params![t, q, c.instance_key, c.next as i64],
            ))?;
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
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
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
/// re-read per group at claim time (the summary is the ordering hint; the items are the authority). Before
/// group-aware claims call this, they refresh a bounded set of groups that became due by time alone.
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

/// Refresh a bounded set of groups that became eligible by time alone (`not_before <= now`) since their
/// last mutation-time summary refresh. Runs only inside mutating group-aware claims; discovery stays
/// read-only and may still under-report until a mutation/tick refreshes the row.
fn refresh_due_group_summaries(
    tx: &Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(tx.prepare(
        "SELECT DISTINCT i.group_key \
         FROM pqueue_items i \
         LEFT JOIN pqueue_group_summary gs \
           ON gs.tenant_id=i.tenant_id AND gs.queue_id=i.queue_id AND gs.group_key=i.group_key \
         WHERE i.tenant_id=?1 AND i.queue_id=?2 \
           AND i.lifecycle_state='Pending' AND i.superseded=0 AND i.group_key IS NOT NULL \
           AND i.eligible_since IS NOT NULL AND (i.not_before IS NULL OR i.not_before<=?3) \
           AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gstate \
             ON gstate.tenant_id=ig.tenant_id AND gstate.queue_id=ig.queue_id AND gstate.gate_key=ig.gate_key \
             WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
           AND (gs.group_key IS NULL OR gs.oldest_eligible_at IS NULL OR gs.rep_item_id IS NULL) \
         ORDER BY i.group_key LIMIT ?4",
    ))?;
    let mapped = st(
        stmt.query_map(params![t, q, now_n, GROUP_DUE_REFRESH_LIMIT], |row| {
            row.get::<_, String>(0)
        }),
    )?;
    let mut groups = Vec::new();
    for r in mapped {
        groups.push(GroupKey::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    drop(stmt);
    for group in groups {
        refresh_group_summary(tx, shard, &group, now)?;
    }
    Ok(())
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
         AND lifecycle_state='Pending' AND superseded=0 AND cohort_size IS NULL \
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
#[derive(Debug, Clone)]
struct SelectedCohort {
    cohort_id: CohortId,
    item_ids: Vec<ItemId>,
}

fn select_whole_cohort(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
) -> EngineResult<Option<SelectedCohort>> {
    if queue_paused(conn, shard)? {
        return Ok(None);
    }
    let (t, q) = parts(shard);
    let cohorts: Vec<(String, String, i64)> = {
        let mut stmt = st(conn.prepare(
            "SELECT group_key, cohort_id, cohort_size FROM pqueue_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 AND state='complete' ORDER BY cohort_created_at, group_key",
        ))?;
        let rows = st(stmt.query_map(params![t, q], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        }))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(st(r)?);
        }
        out
    };
    for (gk, cohort_id, size) in cohorts {
        let size = size as usize;
        let group = GroupKey::new(gk).map_err(|e| EngineError::Storage(e.to_string()))?;
        let members: i64 = st(conn.query_row(
            "SELECT COUNT(*) FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
             AND group_key=?3 AND superseded=0 AND cohort_size IS NOT NULL \
             AND lifecycle_state NOT IN ('Complete','Failed')",
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
        return Ok(Some(SelectedCohort {
            cohort_id: CohortId::new(cohort_id).map_err(|e| EngineError::Storage(e.to_string()))?,
            item_ids: elig,
        }));
    }
    Ok(None)
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
/// KNOWN LIMITATION: read-only discovery does not run the mutating due-refresh used by group-aware claims.
/// A group made eligible ONLY by time passing can keep `oldest_eligible_at = NULL` until its next mutation
/// or a background due-sweep refresh, so discovery can UNDER-report time-triggered starvation.
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
                 lease_expires_at, retry_count, payload, fields, metadata FROM pqueue_items \
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
                        row.get::<_, String>(9)?,
                    ))
                },
            )
            .optional())?;
        let Some((
            key,
            version,
            priority,
            group,
            not_before,
            exp,
            retry,
            payload,
            fields,
            metadata,
        )) = row
        else {
            continue;
        };
        let Some(exp) = exp else { continue };
        let gate_keys = item_gate_keys(conn, shard, id)?;
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
            lease_token: Some(token),
            lease_expires_at: nanos_ts(exp),
            attempt_count: retry as u32,
            payload: payload.map(Bytes::from),
            fields: fields_from_json(fields)?,
            metadata: metadata_from_json(metadata)?,
            gate_keys,
        });
    }
    Ok(out)
}

fn item_gate_keys(conn: &Connection, shard: &QueueKey, id: &ItemId) -> EngineResult<Vec<String>> {
    let (t, q) = parts(shard);
    let mut stmt = st(conn.prepare(
        "SELECT gate_key FROM pqueue_item_gates \
         WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3 \
         ORDER BY gate_key",
    ))?;
    let rows = st(stmt.query_map(params![t, q, id.to_string()], |row| row.get::<_, String>(0)))?;
    let mut keys = Vec::new();
    for row in rows {
        keys.push(st(row)?);
    }
    Ok(keys)
}

fn apply_whole_cohort_response_shape(items: &mut [ClaimedItem]) -> Option<GroupKey> {
    let cohort_id = items.first().and_then(|item| item.group_key.clone());
    for item in items {
        item.lease_token = None;
    }
    cohort_id
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

/// Lifecycle state + flags for a BATCH of items in ONE round-trip per ≤256-id chunk (was one SELECT per
/// id), keyed by `item_id` string. Absent ids are simply missing from the map (the per-id classifier
/// treats a miss as `NotFound`). Replaces the former per-item `item_flags` helper.
fn item_flags_map(
    conn: &Connection,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<HashMap<String, (ItemState, bool, bool, bool)>> {
    let (t, q) = parts(shard);
    let id_strs: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let mut map = HashMap::with_capacity(ids.len());
    for chunk in id_strs.chunks(SQLITE_BATCH) {
        let ph = vec!["?"; chunk.len()].join(",");
        let sql = format!(
            "SELECT item_id, lifecycle_state, fenced, superseded, cohort_size IS NOT NULL FROM pqueue_items \
             WHERE tenant_id=? AND queue_id=? AND item_id IN ({ph})"
        );
        let mut p: Vec<Value> = vec![Value::Text(t.clone()), Value::Text(q.clone())];
        for id in chunk {
            p.push(Value::Text(id.clone()));
        }
        let mut stmt = st(conn.prepare(&sql))?;
        let mapped = st(stmt.query_map(params_from_iter(p.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        }))?;
        for r in mapped {
            let (id, s, fenced, superseded, cohort_member) = st(r)?;
            map.insert(
                id,
                (
                    parse_state(&s)?,
                    fenced != 0,
                    superseded != 0,
                    cohort_member != 0,
                ),
            );
        }
    }
    Ok(map)
}

/// Shared "present + Leased + not fenced + not superseded + not terminal" check — identical error
/// precedence to `ProjectionData::validate_leased` (finalize/renew/reassign pre-commit). Classifies every
/// id from ONE batched read; precedence is still evaluated per id in request order (first failing id wins),
/// byte-for-byte as the former per-id SELECT loop did.
fn validate_leased(conn: &Connection, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
    let flags = item_flags_map(conn, shard, ids)?;
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
    conn: &Connection,
    shard: &QueueKey,
    target: &CohortLeaseTarget,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row: Option<(String, Option<Vec<u8>>)> = st(conn
        .query_row(
            "SELECT state, cohort_lease_token_hash FROM pqueue_cohorts \
             WHERE tenant_id=?1 AND queue_id=?2 AND cohort_id=?3",
            params![t, q, target.cohort_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional())?;
    let Some((state, hash)) = row else {
        return Err(EngineError::NotFound);
    };
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

/// SQLite materialized projection fed by an external command-log authority.
///
/// This is intentionally not a full backend: it does not mint ids, append log entries, or expose
/// data-plane mutation ports. It reuses the relational SQL apply path so an object-log composite can
/// rebuild/read from SQLite without duplicating the 14-arm command projection.
pub struct SqliteProjectionStore {
    inner: Mutex<Inner>,
}

/// Durable SQLite projection plus hot in-memory serving projection.
///
/// `HybridProjectionStore` is the object-log/hybrid projection axis: every committed batch is durably
/// absorbed by [`SqliteProjectionStore`] first, then applied to [`InMemoryProjection`]. Reads and
/// pre-commit validation use only the in-memory projection after `ensure_shard` has hydrated it from
/// SQLite's exported image. If SQLite advances but memory rejects the same batch, the store is poisoned so
/// the current process cannot serve or mutate from a memory image that is behind the durable cursor.
pub struct HybridProjectionStore {
    sqlite: SqliteProjectionStore,
    memory: InMemoryProjection,
    hydrated: HashSet<QueueKey>,
    memory_next_seq: HashMap<QueueKey, u64>,
    poisoned: Option<String>,
}

impl HybridProjectionStore {
    pub fn open(path: &str) -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::open(path)?))
    }

    pub fn in_memory() -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::in_memory()?))
    }

    pub fn new(sqlite: SqliteProjectionStore) -> Self {
        Self {
            sqlite,
            memory: InMemoryProjection::new(),
            hydrated: HashSet::new(),
            memory_next_seq: HashMap::new(),
            poisoned: None,
        }
    }

    /// Support constructor for recovery and fail-closed tests that need explicit parts.
    pub fn from_parts(sqlite: SqliteProjectionStore, memory: InMemoryProjection) -> Self {
        Self {
            sqlite,
            memory,
            hydrated: HashSet::new(),
            memory_next_seq: HashMap::new(),
            poisoned: None,
        }
    }

    pub fn sqlite(&self) -> &SqliteProjectionStore {
        &self.sqlite
    }

    fn shard_for(definition: &QueueDefinition) -> QueueKey {
        QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone())
    }

    fn poison_error(reason: &str) -> EngineError {
        EngineError::Storage(format!("hybrid projection poisoned: {reason}"))
    }

    fn check_healthy(&self) -> EngineResult<()> {
        match &self.poisoned {
            Some(reason) => Err(Self::poison_error(reason)),
            None => Ok(()),
        }
    }

    fn poison<T>(&mut self, reason: String) -> EngineResult<T> {
        self.poisoned = Some(reason.clone());
        Err(Self::poison_error(&reason))
    }

    fn require_hydrated(&self, shard: &QueueKey) -> EngineResult<()> {
        self.check_healthy()?;
        if self.hydrated.contains(shard) {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "hybrid projection shard {}/{} is not hydrated",
                shard.tenant_id, shard.queue_id
            )))
        }
    }

    fn hydrate_from_sqlite(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let shard = Self::shard_for(definition);
        let image = self.sqlite.export_projection_image(&shard)?;
        let expected_metrics = image.metrics.clone();
        let expected_high_water = image.high_water.clone();
        self.memory.hydrate_shard(definition, image)?;
        let actual_metrics = ProjectionStore::metrics(&self.memory, &shard)?;
        if actual_metrics != expected_metrics {
            return Err(EngineError::Storage(format!(
                "hybrid projection hydration parity failed for {}/{}: sqlite metrics {:?}, memory metrics {:?}",
                shard.tenant_id, shard.queue_id, expected_metrics, actual_metrics
            )));
        }
        let sqlite_high_water = self.sqlite.recovery_high_water(&shard)?;
        let sqlite_high_water = sqlite_high_water
            .and_then(|n| (n > 0).then(|| CommandPosition::new(shard.clone(), 0, n - 1)));
        let high_water_matches = match (&sqlite_high_water, &expected_high_water) {
            (Some(cursor), Some(image)) => {
                cursor.queue == image.queue && cursor.sequence == image.sequence
            }
            (None, None) => true,
            _ => false,
        };
        if !high_water_matches {
            return Err(EngineError::Storage(format!(
                "hybrid projection hydration high-water mismatch for {}/{}: cursor {:?}, image {:?}",
                shard.tenant_id, shard.queue_id, sqlite_high_water, expected_high_water
            )));
        }
        self.memory_next_seq.insert(
            shard.clone(),
            expected_high_water.map_or(0, |pos| pos.sequence.saturating_add(1)),
        );
        self.hydrated.insert(shard);
        Ok(())
    }
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
        let inner = open_inner(conn)?;
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
        let mut stmt = st(g
            .conn
            .prepare("SELECT tenant_id, queue_id, item_id FROM pqueue_items"))?;
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

impl SqliteProjectionStore {
    /// Open (or create) a SQLite projection database at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` projection store for tests.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        Ok(Self {
            inner: Mutex::new(open_inner(conn)?),
        })
    }

    /// Create or validate queue projection metadata.
    pub fn create_queue_projection(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        let mut g = self.inner.lock().expect("projection store poisoned");
        create_queue_sql(&mut g, definition)
    }

    /// Apply one already-durable command at its externally assigned log position.
    pub fn apply_committed(
        &self,
        position: &CommandPosition,
        envelope: &CommandEnvelope,
    ) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("projection store poisoned");
        apply_committed_sql(&mut g, position, envelope)
    }

    /// Apply a whole sealed segment's worth of already-durable commands in **one** SQLite transaction.
    ///
    /// This is the group-commit batch apply for the segmented object-log backend: instead of paying a
    /// BEGIN/COMMIT (and rollback-journal create/delete) per command, the entire batch commits once. Each
    /// `positions[i]` is the externally assigned log position of `envelopes[i]`; positions for a given
    /// queue MUST be contiguous and start at that queue's `next_seq` (already-applied prefixes are skipped
    /// idempotently, so a recovery replay that overlaps prior state is a no-op). A gap is a hard error.
    pub fn apply_committed_batch(
        &self,
        positions: &[CommandPosition],
        envelopes: &[CommandEnvelope],
    ) -> EngineResult<()> {
        if positions.len() != envelopes.len() {
            return Err(EngineError::Storage(
                "apply_committed_batch: positions/envelopes length mismatch".into(),
            ));
        }
        if positions.is_empty() {
            return Ok(());
        }
        let mut g = self.inner.lock().expect("projection store poisoned");
        apply_committed_batch_sql(&mut g, positions, envelopes)
    }

    /// Snapshot recovery seam (bead pqueue-8a76daad): the per-queue **high-water** durably recorded by the
    /// materialized projection — the next command sequence the projection expects (`relational_cursor.
    /// next_seq`). The last sequence already absorbed is therefore `high_water - 1`, so a reopen need only
    /// replay the object-log tail at `>= high_water` rather than from genesis. `None` if the queue has no
    /// projection row yet (a never-created queue → caller falls back to a full replay).
    ///
    /// Because every committed batch advances this cursor INSIDE the same SQLite transaction that applies
    /// the batch, the persisted high-water can never be ahead of what is durably materialized: a crash
    /// between the object-log commit and the projection apply leaves the cursor behind the log head, so the
    /// uncommitted tail is replayed (never skipped) on recovery.
    pub fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        let g = self.inner.lock().expect("projection store poisoned");
        let (t, q) = parts(shard);
        let next_seq: Option<i64> = st(g
            .conn
            .query_row(
                "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get(0),
            )
            .optional())?;
        Ok(next_seq.map(|n| n as u64))
    }

    /// Export the durable SQLite projection rows for `shard` as a typed in-memory projection image.
    pub fn export_projection_image(&self, shard: &QueueKey) -> EngineResult<ProjectionImage> {
        let g = self.inner.lock().expect("projection store poisoned");
        export_projection_image_sql(&g.conn, shard)
    }

    /// Restart recovery for the object-log backends' item-id mint counter: seed `counters` past every item
    /// id already materialized in the snapshot (`pqueue_items`), so a push after a snapshot-tail reopen never
    /// re-mints an id that the full-genesis replay would have observed. Safe because the object_log_sqlite
    /// backends never delete item rows (purge / replace-pending are `Unavailable` on the eventual-apply
    /// class), so the persisted items are the complete minted set up to the high-water; the bounded tail
    /// then observes any ids minted beyond it.
    pub fn observe_item_counters(
        &self,
        shard: &QueueKey,
        counters: &QueueCounters,
    ) -> EngineResult<()> {
        let g = self.inner.lock().expect("projection store poisoned");
        let (t, q) = parts(shard);
        let mut stmt = st(g
            .conn
            .prepare("SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2"))?;
        let rows = st(stmt.query_map(params![t, q], |row| row.get::<_, String>(0)))?;
        for r in rows {
            let id = ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?;
            counters.observe(shard, id);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Async SQLite logical checkpoint store (bead pqueue-16b85e28, backend:objectlog-hybrid-async)
// ---------------------------------------------------------------------------

/// Object-log lineage for one async checkpoint: which committed object-log segment/manifest the durable
/// SQLite logical high-water was advanced from. `source_segment` is an opaque object-log reference (a
/// segment object name or manifest id) stored verbatim — pqueue-sqlite deliberately does NOT depend on
/// pqueue-objectlog types, so lineage crosses the crate boundary as opaque metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointLineage {
    pub source_epoch: u64,
    pub source_segment: String,
}

/// Durable progress recorded by the async SQLite checkpoint worker: the LOGICAL high-water (the next
/// command sequence the projection expects, `relational_cursor.next_seq`) plus the cumulative object-log
/// lineage it was derived from. This is distinct from the PHYSICAL SQLite WAL checkpoint (see
/// [`SqliteCheckpointStore::wal_checkpoint`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointProgress {
    /// `relational_cursor.next_seq`: the next command sequence the projection expects. `None` until the
    /// queue's projection row exists.
    pub logical_high_water: Option<u64>,
    /// Cumulative object-log commands absorbed into the recorded lineage.
    pub applied_commands: u64,
    /// The object-log lineage recorded for the last checkpoint, if any.
    pub lineage: Option<CheckpointLineage>,
}

/// Physical SQLite WAL checkpoint result (`PRAGMA wal_checkpoint`). Deliberately SEPARATE from the logical
/// high-water: this reclaims WAL frames into the main database file; it does NOT advance the logical
/// command cursor or mutate the projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCheckpointStats {
    /// `1` if the checkpoint could not run to completion because another connection held a lock.
    pub busy: i64,
    /// Total frames in the WAL before the checkpoint.
    pub wal_frames: i64,
    /// Frames successfully moved into the database file.
    pub checkpointed_frames: i64,
}

/// The async SQLite **logical checkpoint** store for the `objectlog/hybrid-async` profile.
///
/// The object log is the durability authority; this store is the owner-local restart accelerator. Off the
/// hot request path, the checkpoint worker consumes committed object-log entries IN ORDER and, for each
/// batch, in ONE SQLite transaction: applies every command to the durable projection, persists request-id
/// idempotency/outcome rows so a committed-but-unreturned push converges after restart, records the
/// object-log lineage, and advances the LOGICAL high-water LAST. Because the high-water advances inside the
/// same transaction, a crash mid-checkpoint leaves the cursor behind the object-log head, so the
/// uncommitted tail is replayed (never skipped) on recovery.
///
/// The LOGICAL high-water (which command sequence is durably materialized) is distinct from the PHYSICAL
/// SQLite WAL checkpoint (which reclaims WAL frames): [`Self::wal_checkpoint`] is a storage-file concern
/// that never advances the command cursor.
pub struct SqliteCheckpointStore {
    store: SqliteProjectionStore,
}

impl SqliteCheckpointStore {
    /// Open (or create) a durable checkpoint store at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::open(path)?))
    }

    /// An ephemeral `:memory:` checkpoint store for tests.
    pub fn in_memory() -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::in_memory()?))
    }

    pub fn new(store: SqliteProjectionStore) -> Self {
        Self { store }
    }

    /// The wrapped durable projection store (hot reads / image export for hydration).
    pub fn projection(&self) -> &SqliteProjectionStore {
        &self.store
    }

    /// Create or validate the queue projection metadata this worker checkpoints into.
    pub fn create_queue_projection(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        self.store.create_queue_projection(definition)
    }

    /// Consume a batch of committed object-log entries for `shard` IN ORDER and checkpoint them durably.
    ///
    /// In ONE SQLite transaction: apply each command to the projection, persist request-id
    /// idempotency/outcome rows, upsert the object-log `lineage`, and advance the LOGICAL high-water LAST.
    /// `positions[i]` is the object-log position of `envelopes[i]`; positions MUST be contiguous from the
    /// queue's current logical high-water (an already-applied prefix is skipped idempotently, an
    /// out-of-order position is a hard gap error). Every position MUST belong to `shard`.
    pub async fn checkpoint(
        &self,
        shard: &QueueKey,
        positions: &[CommandPosition],
        envelopes: &[CommandEnvelope],
        lineage: &CheckpointLineage,
    ) -> EngineResult<CheckpointProgress> {
        if positions.len() != envelopes.len() {
            return Err(EngineError::Storage(
                "checkpoint: positions/envelopes length mismatch".into(),
            ));
        }
        let mut g = self.store.inner.lock().expect("projection store poisoned");
        checkpoint_batch_sql(&mut g, shard, positions, envelopes, lineage)
    }

    /// The durable LOGICAL high-water for `shard`: the next command sequence the projection expects
    /// (`relational_cursor.next_seq`). `None` if the queue has no projection row yet. This is the cursor a
    /// restart resumes the object-log tail from — NOT the physical WAL checkpoint.
    pub fn logical_high_water(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        recovery_high_water_sql(&g.conn, shard)
    }

    /// The recorded checkpoint progress (logical high-water + cumulative object-log lineage) for `shard`.
    pub fn progress(&self, shard: &QueueKey) -> EngineResult<CheckpointProgress> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        let logical_high_water = recovery_high_water_sql(&g.conn, shard)?;
        let lineage = read_checkpoint_lineage_sql(&g.conn, shard)?;
        Ok(CheckpointProgress {
            logical_high_water,
            applied_commands: lineage.as_ref().map(|(_, n)| *n).unwrap_or(0),
            lineage: lineage.map(|(l, _)| l),
        })
    }

    /// Replay the durably persisted push response ids for `(shard, request_id)`, or `None` if no
    /// idempotency row survives (unknown request id or expired). This is the restart-convergence seam: a
    /// same-body retry after a crash returns the original ids without re-appending to the object log.
    pub fn replay_push(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
    ) -> EngineResult<Option<Vec<ItemId>>> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        read_push_replay_sql(&g.conn, shard, request_id)
    }

    /// Run a PHYSICAL SQLite WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`): reclaim WAL frames into
    /// the main database file. This is a storage-file concern, DELIBERATELY distinct from advancing the
    /// logical high-water — it never changes the command cursor or the projection. A no-op on `:memory:` /
    /// non-WAL databases (reports zero frames).
    pub async fn wal_checkpoint(&self) -> EngineResult<WalCheckpointStats> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        let (busy, wal_frames, checkpointed_frames) =
            st(g.conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }))?;
        Ok(WalCheckpointStats {
            busy,
            wal_frames,
            checkpointed_frames,
        })
    }
}

/// The persisted LOGICAL high-water for `shard`: `relational_cursor.next_seq`, or `None` if the queue has
/// no projection row yet.
fn recovery_high_water_sql(conn: &Connection, shard: &QueueKey) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let next_seq: Option<i64> = st(conn
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?;
    Ok(next_seq.map(|n| n as u64))
}

/// Read the recorded object-log lineage (+ cumulative applied-command count) for `shard`, if any.
fn read_checkpoint_lineage_sql(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<Option<(CheckpointLineage, u64)>> {
    let (t, q) = parts(shard);
    let row: Option<(i64, String, i64)> = st(conn
        .query_row(
            "SELECT source_epoch, source_segment, applied_commands \
             FROM pqueue_checkpoint_lineage WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    Ok(row.map(|(epoch, segment, applied)| {
        (
            CheckpointLineage {
                source_epoch: epoch as u64,
                source_segment: segment,
            },
            applied as u64,
        )
    }))
}

/// Read the durably persisted push response ids for `(shard, request_id)`, or `None` if absent.
fn read_push_replay_sql(
    conn: &Connection,
    shard: &QueueKey,
    request_id: &RequestId,
) -> EngineResult<Option<Vec<ItemId>>> {
    let (t, q) = parts(shard);
    let payload: Option<String> = st(conn
        .query_row(
            "SELECT response_payload FROM pqueue_request_idempotency \
             WHERE tenant_id=?1 AND queue_id=?2 AND operation=?3 AND request_id=?4",
            params![t, q, IDEMPOTENCY_OPERATION_PUSH, request_id.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    payload.map(item_ids_from_json).transpose()
}

/// Persist the durable request-id idempotency/outcome row for a committed request-id-bearing command, so a
/// committed-but-unreturned retry converges after restart (plan §Request-Id Replay). Only push commands
/// currently carry replayable outcomes; a command with no `request_id`, or one missing replay metadata, is
/// skipped (nothing durable to replay). Written inside the caller's checkpoint transaction.
fn persist_request_outcome_sql(
    tx: &Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    shard: &QueueKey,
    env: &CommandEnvelope,
    pos: &CommandPosition,
) -> EngineResult<()> {
    let Some(request_id) = env.request_id.as_ref() else {
        return Ok(());
    };
    let (Some(fingerprint), Some(RequestOutcome::Push { item_ids })) =
        (env.request_fingerprint, env.request_outcome.as_ref())
    else {
        return Ok(());
    };
    let expires_at = request_expires_at(queues, shard, env.created_at)?;
    record_request_idempotency(
        tx,
        shard,
        IDEMPOTENCY_OPERATION_PUSH,
        request_id,
        &fingerprint.to_be_bytes(),
        item_ids,
        std::slice::from_ref(pos),
        env.created_at,
        expires_at,
    )
}

/// Single-shard checkpoint apply (bead pqueue-16b85e28): apply an ordered batch of already-committed
/// object-log commands, persist request-id idempotency rows, record object-log lineage, and advance the
/// LOGICAL high-water LAST — all in ONE transaction. Mirrors [`apply_committed_batch_sql`] but is bound to
/// a single shard so it can additionally stamp that queue's checkpoint-lineage row.
fn checkpoint_batch_sql(
    g: &mut Inner,
    shard: &QueueKey,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
    lineage: &CheckpointLineage,
) -> EngineResult<CheckpointProgress> {
    if !g.queues.contains_key(shard) {
        return Err(EngineError::NotFound);
    }
    for pos in positions {
        if &pos.queue != shard {
            return Err(EngineError::Storage(
                "checkpoint: position does not belong to the checkpointed shard".into(),
            ));
        }
    }
    if positions.is_empty() {
        // Nothing to checkpoint; report current durable progress without opening a transaction.
        let logical_high_water = recovery_high_water_sql(&g.conn, shard)?;
        let lineage_row = read_checkpoint_lineage_sql(&g.conn, shard)?;
        return Ok(CheckpointProgress {
            logical_high_water,
            applied_commands: lineage_row.as_ref().map(|(_, n)| *n).unwrap_or(0),
            lineage: lineage_row.map(|(l, _)| l),
        });
    }
    let Inner {
        conn,
        queues,
        grouped_shards,
        live_tokens,
        ..
    } = g;
    let (t, q) = parts(shard);
    let tx = st(conn.transaction())?;
    let mut cursor: i64 = st(tx
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    let mut max_epoch = lineage.source_epoch as i64;
    let mut token_ops = Vec::new();
    let mut applied_this_batch: u64 = 0;
    for (pos, env) in positions.iter().zip(envelopes) {
        let incoming = pos.sequence as i64;
        if incoming < cursor {
            // Idempotent replay of an already-absorbed prefix.
            continue;
        }
        if incoming > cursor {
            return Err(EngineError::Storage(format!(
                "sqlite checkpoint replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                shard.tenant_id.as_str(),
                shard.queue_id.as_str()
            )));
        }
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            &mut token_ops,
            shard,
            pos.sequence,
            env.created_at,
            &env.command,
        )?;
        // Persist request-id idempotency/outcome BEFORE the cursor advance, so the row lands under the
        // same high-water it belongs to.
        persist_request_outcome_sql(&tx, queues, shard, env, pos)?;
        cursor = incoming
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
        let e = pos.backend_epoch as i64;
        if e > max_epoch {
            max_epoch = e;
        }
        applied_this_batch += 1;
    }
    // Object-log lineage: cumulative applied-command count + the segment/manifest this high-water derives
    // from. Upserted in the SAME transaction, BEFORE the high-water write.
    let prior_applied: i64 = st(tx
        .query_row(
            "SELECT applied_commands FROM pqueue_checkpoint_lineage WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .unwrap_or(0);
    let total_applied = prior_applied + applied_this_batch as i64;
    let updated_at = ts_nanos(envelopes[envelopes.len() - 1].created_at);
    st(tx.execute(
        "INSERT INTO pqueue_checkpoint_lineage \
         (tenant,queue,logical_high_water,source_epoch,source_segment,applied_commands,updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(tenant,queue) DO UPDATE SET \
          logical_high_water=excluded.logical_high_water, \
          source_epoch=excluded.source_epoch, \
          source_segment=excluded.source_segment, \
          applied_commands=excluded.applied_commands, \
          updated_at=excluded.updated_at",
        params![
            t,
            q,
            cursor,
            lineage.source_epoch as i64,
            lineage.source_segment,
            total_applied,
            updated_at,
        ],
    ))?;
    // LOGICAL high-water LAST: advance the command cursor (and the durable ownership epoch) as the final
    // write before commit, so the cursor can never be ahead of the applied projection + persisted lineage.
    st(tx.execute(
        "UPDATE relational_cursor SET \
         next_seq=?3, \
         assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
         WHERE tenant=?1 AND queue=?2",
        params![t, q, cursor, max_epoch],
    ))?;
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(CheckpointProgress {
        logical_high_water: Some(cursor as u64),
        applied_commands: total_applied as u64,
        lineage: Some(lineage.clone()),
    })
}

fn open_inner(conn: Connection) -> EngineResult<Inner> {
    // WAL + synchronous=NORMAL: the group-commit projection seals one batched transaction per segment and
    // wants commits cheap. Default DELETE journaling pays a rollback-journal create/delete (and an extra
    // directory fsync) per COMMIT; WAL appends and checkpoints lazily, and NORMAL drops the per-commit
    // fsync (durable at checkpoint). The projection is rebuildable from the durable object log, so this
    // trades nothing the object-log authority does not already guarantee. `busy_timeout` keeps a
    // concurrent checkpoint/reader from turning into a spurious SQLITE_BUSY. (No-ops on `:memory:`.)
    st(conn.execute_batch(
        "PRAGMA journal_mode=WAL;\
         PRAGMA synchronous=NORMAL;\
         PRAGMA busy_timeout=5000;",
    ))?;
    st(conn.execute_batch(RELATIONAL_SCHEMA))?;
    ensure_item_fields_column(&conn)?;
    ensure_item_metadata_column(&conn)?;
    ensure_item_entity_document_column(&conn)?;
    ensure_cohort_lifecycle_columns(&conn)?;
    let mut inner = Inner {
        conn,
        queues: HashMap::new(),
        schemas: HashMap::new(),
        grouped_shards: HashSet::new(),
        live_tokens: HashMap::new(),
    };
    inner.reload()?;
    Ok(inner)
}

fn create_queue_sql(
    g: &mut Inner,
    definition: QueueDefinition,
) -> EngineResult<CreateQueueOutcome> {
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
    let (t, q) = parts(&key);
    let def_json = to_json(&definition)?;
    st(g.conn.execute(
        "INSERT INTO queues(tenant,queue,definition,paused) VALUES(?1,?2,?3,0)",
        params![t, q, def_json],
    ))?;
    st(g.conn.execute(
        "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES(?1,?2,0,0)",
        params![t, q],
    ))?;
    if let Some(cs) = compiled_schema {
        g.schemas.insert(key.clone(), cs);
    }
    g.queues.insert(key, definition.clone());
    Ok(CreateQueueOutcome {
        created: true,
        definition,
    })
}

fn export_projection_image_sql(
    conn: &Connection,
    shard: &QueueKey,
) -> EngineResult<ProjectionImage> {
    let (t, q) = parts(shard);
    let cursor: Option<(i64, i64, i64)> = st(conn
        .query_row(
            "SELECT next_seq,next_item_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional())?;
    let (next_seq, next_item_seq, assignment_epoch) = cursor.ok_or(EngineError::NotFound)?;
    let paused: i64 = st(conn.query_row(
        "SELECT paused FROM queues WHERE tenant=?1 AND queue=?2",
        params![t, q],
        |row| row.get(0),
    ))?;

    let mut stmt = st(conn.prepare(
        "SELECT item_id,client_item_key,lifecycle_state,priority,not_before,group_key,payload,\
         fields,metadata,entity_document,retry_count,item_version,lease_expires_at,fenced,\
         superseded,max_attempts,created_seq \
         FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 ORDER BY created_seq,item_id",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<Vec<u8>>>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, Option<i64>>(12)?,
            row.get::<_, i64>(13)?,
            row.get::<_, i64>(14)?,
            row.get::<_, i64>(15)?,
            row.get::<_, i64>(16)?,
        ))
    }))?;
    let mut items = Vec::new();
    for row in rows {
        let (
            item_id,
            client_item_key,
            lifecycle_state,
            priority,
            not_before,
            group_key,
            payload,
            fields,
            metadata,
            entity_document,
            retry_count,
            item_version,
            lease_expires_at,
            fenced,
            superseded,
            max_attempts,
            created_seq,
        ) = st(row)?;
        let item_id = ItemId::new(item_id).map_err(|e| EngineError::Storage(e.to_string()))?;
        let entity_document = entity_document
            .map(|raw| serde_json::from_str(&raw).map_err(|e| EngineError::Storage(e.to_string())))
            .transpose()?;
        items.push(ProjectionImageItem {
            item_id,
            client_item_key: ClientItemKey::new(client_item_key)
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            priority: parse_priority(priority)?,
            not_before: not_before.map(nanos_ts),
            group_key: group_key
                .map(GroupKey::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            payload: payload.map(Bytes::from),
            fields: fields_from_json(fields)?,
            metadata: metadata_from_json(metadata)?,
            gate_keys: item_gate_keys(conn, shard, &item_id)?,
            entity_document,
            state: parse_state(&lifecycle_state)?,
            item_version: item_version as u64,
            attempt_count: retry_count as u32,
            max_attempts: max_attempts as u32,
            created_seq: created_seq as u64,
            lease_token: None,
            lease_expires_at: lease_expires_at.map(nanos_ts),
            fenced: fenced != 0,
            superseded: superseded != 0,
        });
    }

    let mut side_records = BTreeMap::new();
    let mut stmt = st(conn.prepare(
        "SELECT key,payload FROM pqueue_side_records \
         WHERE tenant_id=?1 AND queue_id=?2 ORDER BY key",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
    }))?;
    for row in rows {
        let (key, payload) = st(row)?;
        side_records.insert(key, Bytes::from(payload));
    }

    let mut instance_fences = BTreeMap::new();
    let mut stmt = st(conn.prepare(
        "SELECT instance_key,fence FROM pqueue_instance_fences \
         WHERE tenant_id=?1 AND queue_id=?2 ORDER BY instance_key",
    ))?;
    let rows = st(stmt.query_map(params![t, q], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
    }))?;
    for row in rows {
        let (key, fence) = st(row)?;
        instance_fences.insert(key, fence as u64);
    }

    let high_water = (next_seq > 0)
        .then(|| CommandPosition::new(shard.clone(), assignment_epoch as u64, next_seq as u64 - 1));
    Ok(ProjectionImage {
        high_water,
        paused: paused != 0,
        next_seq: next_item_seq as u64,
        items,
        side_records,
        instance_fences,
        metrics: metrics_sql(conn, shard)?,
    })
}

fn apply_committed_sql(
    g: &mut Inner,
    position: &CommandPosition,
    envelope: &CommandEnvelope,
) -> EngineResult<()> {
    if !g.queues.contains_key(&position.queue) {
        return Err(EngineError::NotFound);
    }
    let Inner {
        conn,
        queues,
        grouped_shards,
        live_tokens,
        ..
    } = g;
    let (t, q) = parts(&position.queue);
    let tx = st(conn.transaction())?;
    let next_seq: i64 = st(tx
        .query_row(
            "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
            params![t, q],
            |row| row.get(0),
        )
        .optional())?
    .ok_or(EngineError::NotFound)?;
    let incoming_seq = position.sequence as i64;
    if incoming_seq < next_seq {
        return Ok(());
    }
    if incoming_seq > next_seq {
        return Err(EngineError::Storage(format!(
            "sqlite projection replay gap for {}:{}: expected sequence {next_seq}, got {incoming_seq}",
            position.queue.tenant_id.as_str(),
            position.queue.queue_id.as_str()
        )));
    }
    let mut token_ops = Vec::new();
    apply_command_sql(
        &tx,
        queues,
        grouped_shards,
        &mut token_ops,
        &position.queue,
        position.sequence,
        envelope.created_at,
        &envelope.command,
    )?;
    let new_next_seq = position
        .sequence
        .checked_add(1)
        .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
    st(tx.execute(
        "UPDATE relational_cursor SET \
         next_seq=?3, \
         assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
         WHERE tenant=?1 AND queue=?2",
        params![t, q, new_next_seq as i64, position.backend_epoch as i64],
    ))?;
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(())
}

/// Batched analogue of [`apply_committed_sql`]: apply many already-durable commands in ONE transaction,
/// reading each queue's cursor once and advancing it once at the end (group-commit apply). Already-applied
/// positions (`sequence < next_seq`) are skipped idempotently; an out-of-order position is a hard gap error.
fn apply_committed_batch_sql(
    g: &mut Inner,
    positions: &[CommandPosition],
    envelopes: &[CommandEnvelope],
) -> EngineResult<()> {
    let Inner {
        conn,
        queues,
        grouped_shards,
        live_tokens,
        ..
    } = g;
    for pos in positions {
        if !queues.contains_key(&pos.queue) {
            return Err(EngineError::NotFound);
        }
    }
    let tx = st(conn.transaction())?;
    let mut token_ops = Vec::new();
    // Per-queue running cursor (next expected sequence) + the highest epoch observed, so the cursor row is
    // read once and written once per queue across the whole batch.
    let mut next_seq: HashMap<QueueKey, i64> = HashMap::new();
    let mut max_epoch: HashMap<QueueKey, i64> = HashMap::new();
    for (pos, env) in positions.iter().zip(envelopes) {
        let (t, q) = parts(&pos.queue);
        let cursor = match next_seq.get(&pos.queue) {
            Some(&n) => n,
            None => {
                let n: i64 = st(tx
                    .query_row(
                        "SELECT next_seq FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                        params![t, q],
                        |row| row.get(0),
                    )
                    .optional())?
                .ok_or(EngineError::NotFound)?;
                next_seq.insert(pos.queue.clone(), n);
                n
            }
        };
        let incoming = pos.sequence as i64;
        if incoming < cursor {
            // Already applied (idempotent replay of a prefix the projection has already absorbed).
            continue;
        }
        if incoming > cursor {
            return Err(EngineError::Storage(format!(
                "sqlite projection replay gap for {}:{}: expected sequence {cursor}, got {incoming}",
                pos.queue.tenant_id.as_str(),
                pos.queue.queue_id.as_str()
            )));
        }
        apply_command_sql(
            &tx,
            queues,
            grouped_shards,
            &mut token_ops,
            &pos.queue,
            pos.sequence,
            env.created_at,
            &env.command,
        )?;
        let new_next = incoming
            .checked_add(1)
            .ok_or_else(|| EngineError::Storage("command sequence overflow".into()))?;
        next_seq.insert(pos.queue.clone(), new_next);
        let e = pos.backend_epoch as i64;
        let slot = max_epoch.entry(pos.queue.clone()).or_insert(e);
        if e > *slot {
            *slot = e;
        }
    }
    for (queue, &next) in &next_seq {
        let (t, q) = parts(queue);
        let epoch = max_epoch.get(queue).copied().unwrap_or(0);
        st(tx.execute(
            "UPDATE relational_cursor SET \
             next_seq=?3, \
             assignment_epoch=CASE WHEN assignment_epoch<?4 THEN ?4 ELSE assignment_epoch END \
             WHERE tenant=?1 AND queue=?2",
            params![t, q, next, epoch],
        ))?;
    }
    st(tx.commit())?;
    apply_token_ops(live_tokens, token_ops);
    Ok(())
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
    grouped_shards: &'a mut HashSet<QueueKey>,
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
                self.grouped_shards,
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

    fn supports_gates(&self) -> bool {
        true
    }

    /// Authoritative-commit capabilities (epic pqueue-2201fd37). The DB-authoritative relational backend
    /// implements the full vectorized claimed-work commit boundary in one sqlite transaction: atomic per-entry
    /// transition, vectorized commit, lease-token (hash) + version + lease-expiry validation, retained
    /// whole-body request-id idempotency (`pqueue_request_idempotency`), opaque non-work side records
    /// (`pqueue_side_records`), and authoritative recovery/explain reads. Delayed/timer lifecycle work is
    /// supported (`not_before`). The boundary is `Atomic` (single-transaction durability).
    fn commit_capabilities(&self) -> CommitCapabilities {
        CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic single-transaction commit on sqlite",
        }
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
                grouped_shards,
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
                    grouped_shards,
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
        let result = {
            let mut g = self.inner.lock().expect("poisoned");
            create_queue_sql(&mut g, definition)
        };
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

impl ProjectionRead for SqliteProjectionStore {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = {
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
            peek_sql(&g.conn, shard, limit)
        };
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
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
            let g = self.inner.lock().expect("projection store poisoned");
            live_items_sql(&g.conn, shard, keys)
        };
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = {
            let g = self.inner.lock().expect("projection store poisoned");
            metrics_sql(&g.conn, queue)
        };
        std::future::ready(result)
    }
}

/// ADR-011 (pqueue-f4ffd679): typed secondary index queries backed by `pqueue_item_index`.
impl IndexQueryPort for SqliteRelationalBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?;
            if !index_is_unique(qi) {
                return Err(EngineError::Invalid("secondary index is not unique"));
            }
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(qi, key)?;
            let (t, q) = parts(shard);
            let row: Option<(String, String, i64)> = st(g
                .conn
                .query_row(
                    "SELECT i.item_id, i.client_item_key, i.item_version \
                     FROM pqueue_item_index idx \
                     JOIN pqueue_items i \
                       ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                      AND i.item_id=idx.item_id \
                     WHERE idx.tenant_id=?1 AND idx.queue_id=?2 \
                       AND idx.index_name=?3 AND idx.index_key=?4 \
                     LIMIT 1",
                    params![t, q, index, canonical.as_slice()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional())?;
            Ok(row.map(|(id_str, ck_str, ver)| IndexHit {
                item_id: ItemId::new(id_str).expect("valid stored item_id"),
                client_item_key: ClientItemKey::new(ck_str).expect("valid stored client_item_key"),
                item_version: ver as u64,
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
            let g = self.inner.lock().expect("projection store poisoned");
            let qi = g
                .queues
                .get(shard)
                .and_then(|d| d.typed_indexes.iter().find(|qi| qi.name == index))
                .ok_or(EngineError::Invalid("unknown secondary index"))?;
            let expected_arity = match &qi.declaration {
                IndexDeclaration::Single(_) => 1,
                IndexDeclaration::Compound(def) => def.fields.len(),
            };
            if key.len() != expected_arity {
                return Err(EngineError::Invalid("secondary index key arity mismatch"));
            }
            let canonical = typed_lookup_canonical_key(qi, key)?;
            let (t, q) = parts(shard);
            let mut stmt = st(g.conn.prepare(
                "SELECT i.item_id, i.client_item_key, i.item_version \
                 FROM pqueue_item_index idx \
                 JOIN pqueue_items i \
                   ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id \
                  AND i.item_id=idx.item_id \
                 WHERE idx.tenant_id=?1 AND idx.queue_id=?2 \
                   AND idx.index_name=?3 AND idx.index_key=?4 \
                 ORDER BY i.item_id",
            ))?;
            let rows = st(
                stmt.query_map(params![t, q, index, canonical.as_slice()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }),
            )?;
            let mut out = Vec::new();
            for r in rows {
                let (id_str, ck_str, ver) = st(r)?;
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
            let epoch = expected_epoch.unwrap_or(0);
            let Inner {
                conn,
                queues,
                grouped_shards,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let tx = st(conn.transaction())?;
            let (seq, cursor_epoch): (i64, i64) = st(tx
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            if let Some(ids) = check_request_idempotency(
                &tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                ts_nanos(now),
            )? {
                return Ok(ids);
            }

            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let mut token_ops = Vec::new();
            apply_command_sql(
                &tx,
                queues,
                grouped_shards,
                &mut token_ops,
                shard,
                seq as u64,
                now,
                &QueueCommand::Push(PushCommand { items: push_items }),
            )?;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq + 1],
            ))?;
            let positions = [CommandPosition::new(
                shard.clone(),
                cursor_epoch as u64,
                seq as u64,
            )];
            record_request_idempotency(
                &tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                &request_id,
                &fingerprint,
                &ids,
                &positions,
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

impl pqueue_engine::ReschedulePort for SqliteRelationalBackend {}

impl SetGatesPort for SqliteRelationalBackend {
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
                grouped_shards,
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
            if matches!(unit, ClaimUnit::WholeGroup | ClaimUnit::SameGroupKey) {
                refresh_due_group_summaries(&tx, &req.shard, req.now)?;
            }
            // Candidate selection inside the claim transaction (serialized under the backend Mutex). The
            // item-level path is the strict-claim order; the group/cohort paths consume their projections.
            let mut selected_cohort: Option<CohortId> = None;
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
                    match select_whole_cohort(&tx, &req.shard, req.now, req.max_items)? {
                        Some(selected) => {
                            selected_cohort = Some(selected.cohort_id);
                            selected.item_ids
                        }
                        None => Vec::new(),
                    }
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
                &tx,
                queues,
                grouped_shards,
                &mut token_ops,
                &req.shard,
                seq as u64,
                req.now,
                &claim_command,
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
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: entity,
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
                        now,
                        expected_epoch,
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

/// Snorri authoritative vectorized claimed-work commit on the DB-authoritative relational family (C9, epic
/// pqueue-2201fd37) — "at least one durable backend" parity for the commit boundary. The WHOLE request body
/// runs in ONE sqlite transaction so request-id check + per-entry validate + side-record/lifecycle/finalize
/// writes + outcome record commit atomically (or roll back together on a storage fault).
impl pqueue_engine::CommitTransitionPort for SqliteRelationalBackend {
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
                conn,
                queues,
                grouped_shards,
                live_tokens,
                ..
            } = &mut *g;
            let (t, q) = parts(shard);
            let tx = st(conn.transaction())?;
            // ADR-009 / TD-003: read the durable assignment_epoch with the cursor and fence the owner's cached
            // acquire-time epoch (`Some`) — a superseded owner is rejected `EpochFenced`, nothing applied.
            let (seq0, cursor_epoch): (i64, i64) = st(tx
                .query_row(
                    "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                    params![t, q],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional())?
            .ok_or(EngineError::NotFound)?;
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }

            // (1) Request-id idempotency over the WHOLE commit body (same retained-request-id table/path as
            //     the relational push). A retained body+id REPLAYS the prior per-entry outcomes (no re-write);
            //     a different body under that id is `RequestIdConflict`; an expired/absent record proceeds.
            if let Some(rid) = &request_id
                && let Some(stored) =
                    check_commit_idempotency(&tx, shard, rid, &fingerprint, ts_nanos(now))?
            {
                return Ok(recovery_to_outcomes(&stored));
            }

            // (2) Per entry: validate the lease-token + version-fenced claim_ref, then apply the entry's
            //     side-records + lifecycle push + input finalize in this same transaction. A rejected entry
            //     applies nothing (its outcome is captured; later entries still proceed). The caller's
            //     `request_id` is recorded with the whole-body outcome (no `request_id: None` on this path).
            let mut token_ops = Vec::new();
            let mut seq = seq0 as u64;
            let mut positions: Vec<CommandPosition> = Vec::new();
            let mut apply =
                |command: &QueueCommand, token_ops: &mut Vec<TokenOp>| -> EngineResult<()> {
                    apply_command_sql(
                        &tx,
                        queues,
                        grouped_shards,
                        token_ops,
                        shard,
                        seq,
                        now,
                        command,
                    )?;
                    positions.push(CommandPosition::new(
                        shard.clone(),
                        cursor_epoch as u64,
                        seq,
                    ));
                    seq += 1;
                    Ok(())
                };

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
                if let Err(e) = commit_validate_sql(&tx, shard, &entry.claim_ref, now) {
                    recovery.push(reject(e));
                    continue;
                }
                // C6: validate the caller-supplied instance fence against the durable fence (absent == 0).
                // A stale `expected` -> Conflict, a non-monotonic `next` -> Invalid; NOTHING is applied.
                if let Some(fence) = &entry.instance_fence {
                    let (it, iq) = parts(shard);
                    let stored: i64 = st(tx
                        .query_row(
                            "SELECT fence FROM pqueue_instance_fences \
                             WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
                            params![it, iq, fence.instance_key],
                            |row| row.get(0),
                        )
                        .optional())?
                    .unwrap_or(0);
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
                    apply(
                        &QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: entry.side_records,
                        }),
                        &mut token_ops,
                    )?;
                }
                if let Some(fence) = entry.instance_fence {
                    apply(
                        &QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                        &mut token_ops,
                    )?;
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
                    apply(
                        &QueueCommand::Push(PushCommand { items: push_items }),
                        &mut token_ops,
                    )?;
                }
                apply(
                    &QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(
                            entry.claim_ref.item_id,
                            entry.finalize,
                        )],
                    }),
                    &mut token_ops,
                )?;
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }
            let outcomes = recovery_to_outcomes(&recovery);

            // Advance the durable command sequence past every command this body applied.
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=?3 WHERE tenant=?1 AND queue=?2",
                params![t, q, seq as i64],
            ))?;

            // (3) Record the whole-body outcome (only when a request_id was supplied) BEFORE commit, so a
            //     later replay returns it verbatim with no second write.
            if let Some(rid) = &request_id {
                record_commit_idempotency(
                    &tx,
                    shard,
                    rid,
                    &fingerprint,
                    &recovery,
                    &positions,
                    now,
                    expires_at,
                )?;
            }
            st(tx.commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit (F4)
            Ok(outcomes)
        })();
        std::future::ready(result)
    }
}

impl RecoveryReadPort for SqliteRelationalBackend {
    /// Reconstruct a committed transition from the retained `pqueue_request_idempotency` record (epic
    /// pqueue-2201fd37 acceptance #5). The durable `response_payload` already holds every per-entry recovery
    /// field; we only re-attach the `request_id`. `Ok(None)` when nothing is retained under that id. Survives
    /// a reopen (the record is a durable table row).
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommitRecovery>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let entries = read_commit_recovery(&g.conn, shard, &request_id)?;
            Ok(entries.map(|entries| CommitRecovery {
                request_id,
                entries,
            }))
        })();
        std::future::ready(result)
    }

    /// Read an opaque non-work side record by key from `pqueue_side_records` (recovery/audit read). Disjoint
    /// from `pqueue_items`, so it never reflects claimable work and survives input finalization + reopen.
    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let payload: Option<Vec<u8>> = st(g
                .conn
                .query_row(
                    "SELECT payload FROM pqueue_side_records \
                     WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
                    params![t, q, key],
                    |row| row.get(0),
                )
                .optional())?;
            Ok(payload.map(Bytes::from))
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
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortFinalizePort for SqliteRelationalBackend {
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
            validate_cohort_lease(&g.conn, shard, &target)?;
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
                now,
                expected_epoch,
            )?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl CohortRenewLeasePort for SqliteRelationalBackend {
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
            validate_cohort_lease(&g.conn, shard, &target)?;
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
                now,
                expected_epoch,
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
            // Classify every candidate from ONE batched read (was one SELECT per id), then preserve the
            // exact in-order, deduped, force-gated `present` set the per-item loop produced.
            let flags = item_flags_map(&g.conn, shard, &item_ids)?;
            let mut present: Vec<ItemId> = Vec::new();
            for id in &item_ids {
                if present.contains(id) {
                    continue; // de-dup: remove + count once (XDEL semantics)
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

impl UpdateFieldsPort for SqliteRelationalBackend {
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
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-commit entity schema validation (ADR-011): reject before any mutation.
            validate_entity(g.schemas.get(shard), entity.as_ref())?;
            // Pre-validate with the SAME error precedence as `ProjectionData::update_fields_validate`
            // (commit has no rollback): absent => NotFound, fenced => StaleLease, terminal => Terminal,
            // superseded => Superseded, version mismatch => Conflict.
            let (t, q) = parts(shard);
            let row: Option<(String, i64, i64, i64)> = st(g
                .conn
                .query_row(
                    "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
                     WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
                    params![t, q, item_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional())?;
            let (state, superseded, fenced, version) = row.ok_or(EngineError::NotFound)?;
            if fenced != 0 {
                return Err(EngineError::StaleLease);
            }
            if parse_state(&state)?.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded != 0 {
                return Err(EngineError::Superseded);
            }
            if expected_item_version.is_some_and(|v| v != version as u64) {
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
            // The apply bumped item_version by one (the row was validated live above).
            Ok(version as u64 + 1)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for SqliteRelationalBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let (t, q) = parts(shard);
            let now_n = ts_nanos(now);
            // This queue's leases expired strictly before `now` (half-open, like the tick), optionally capped.
            let base = "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
                        AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
                        AND lease_expires_at<?3 ORDER BY item_id";
            let id_strs: Vec<String> = {
                let mut out = Vec::new();
                if let Some(lim) = limit {
                    let sql = format!("{base} LIMIT ?4");
                    let mut stmt = st(g.conn.prepare(&sql))?;
                    let rows = st(stmt.query_map(params![t, q, now_n, lim as i64], |row| {
                        row.get::<_, String>(0)
                    }))?;
                    for r in rows {
                        out.push(st(r)?);
                    }
                } else {
                    let mut stmt = st(g.conn.prepare(base))?;
                    let rows =
                        st(stmt.query_map(params![t, q, now_n], |row| row.get::<_, String>(0)))?;
                    for r in rows {
                        out.push(st(r)?);
                    }
                }
                out
            };
            let ids: Vec<ItemId> = id_strs
                .into_iter()
                .map(|s| ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED (unlike the global ReclaimDriver::tick, which passes None).
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
                    None,
                )?;
            }
            let due_cohorts: Vec<(QueueKey, GroupKey, u64)> = {
                let mut stmt = st(g.conn.prepare(
                    "SELECT c.tenant_id, c.queue_id, c.group_key, c.cohort_created_at, \
                     c.first_eligible_at, r.assignment_epoch \
                     FROM pqueue_cohorts c \
                     JOIN relational_cursor r ON r.tenant=c.tenant_id AND r.queue=c.queue_id \
                     WHERE c.state IN ('forming','complete') \
                     ORDER BY c.tenant_id, c.queue_id, c.group_key \
                     LIMIT ?1",
                ))?;
                let rows = st(
                    stmt.query_map(params![COHORT_EXPIRY_SWEEP_LIMIT as i64], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    }),
                )?;
                let mut out = Vec::new();
                for r in rows {
                    let (t, q, group, cohort_created_at, first_eligible_at, epoch) = st(r)?;
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
                        out.push((
                            shard,
                            GroupKey::new(group)
                                .map_err(|e| EngineError::Storage(e.to_string()))?,
                            epoch as u64,
                        ));
                    }
                }
                out
            };
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
// ADR-012 P1b-ii: the UNIFIED relational store as `LogStore + ProjectionStore`
// ===========================================================================
//
// The keystone "same robustness as flat postgres" composition: ONE store value implements BOTH the log
// axis and the projection axis, so the generic [`ComposedBackend::commit_locked`] drives append+apply into
// ONE durable relational transaction with NO phantom log row. The mechanism (ADR-012 §"The atomic write
// seam", unified-transactional path):
//
//   * [`LogStore::append`] STAGES — it reads the durable `relational_cursor` (next_seq + assignment_epoch),
//     applies the TD-003 fence (`expected_epoch` must equal the recorded epoch), and MINTS the
//     `CommandPosition`s in memory. It performs NO durable write and does NOT advance the cursor. There is
//     therefore no log row that can exist without its projection apply.
//   * [`ProjectionStore::apply`] COMMITS — it runs the single durable relational transaction (the projection
//     rows via the 14-arm `apply_command_sql`, the request-id/idempotency rows where applicable, and the
//     cursor `next_seq` advance), exactly what the monolith's `commit_command` / `apply_committed_batch_sql`
//     do. The cursor advance lands in the SAME transaction as the projection write, so a crash leaves the
//     cursor behind the (un-applied) work — never ahead of it.
//
// Because `commit_locked` holds the composed unit-of-work lock across append→apply and the two axes share
// ONE `Inner` (one connection) behind an `Arc<Mutex<_>>`, the mint and the durable apply are consistent:
// `append` mints at the cursor, `apply` applies at that position and advances the cursor by one.
//
// This reaches capability parity with the monolithic [`SqliteRelationalBackend`] for the CORE conformance
// class: the orthogonal orchestration (already proven against `InMemoryProjection`) gets correct answers
// from the relational SQL projection, so the composition passes `core_suite!(@atomic)` identically.

/// Active (non-superseded) item id under `client_item_key`, or `None`. The generic upsert's look-then-replace
/// read; the partial unique index keeps this single-valued.
fn lookup_active_by_key(
    conn: &Connection,
    shard: &QueueKey,
    client_item_key: &ClientItemKey,
) -> EngineResult<Option<ItemId>> {
    let (t, q) = parts(shard);
    let id: Option<String> = st(conn
        .query_row(
            "SELECT item_id FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND client_item_key=?3 AND superseded=0",
            params![t, q, client_item_key.as_str()],
            |row| row.get(0),
        )
        .optional())?;
    id.map(|s| ItemId::new(s).map_err(|e| EngineError::Storage(e.to_string())))
        .transpose()
}

/// Lifecycle state of `id` (any superseded/terminal flavor), or `None` if absent.
fn item_state_sql(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
) -> EngineResult<Option<ItemState>> {
    Ok(item_flags_map(conn, shard, std::slice::from_ref(id))?
        .get(&id.to_string())
        .map(|(s, _, _, _)| *s))
}

/// Committed `item_version` of `id`, or `None` if absent.
fn item_version_sql(conn: &Connection, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let v: Option<i64> = st(conn
        .query_row(
            "SELECT item_version FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, id.to_string()],
            |row| row.get(0),
        )
        .optional())?;
    Ok(v.map(|v| v as u64))
}

/// This queue's leases expired strictly before `now` (half-open), ordered by item id (the generic
/// `reclaim_expired` truncates to its `limit`). Mirrors the monolith's per-queue reclaim selection.
fn expired_leases_sql(
    conn: &Connection,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<Vec<ItemId>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2 \
         AND lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND lease_expires_at<?3 ORDER BY item_id",
    ))?;
    let rows = st(stmt.query_map(params![t, q, now_n], |row| row.get::<_, String>(0)))?;
    let mut ids = Vec::new();
    for r in rows {
        ids.push(ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(ids)
}

/// Every queue's expired leases at `now` (the global tick sweep), grouped per queue. Mirrors the monolith's
/// `ReclaimDriver::tick` selection (queues with none are omitted).
fn all_expired_leases_sql(
    conn: &Connection,
    now: UtcTimestamp,
) -> EngineResult<Vec<(QueueKey, Vec<ItemId>)>> {
    let now_n = ts_nanos(now);
    let mut stmt = st(conn.prepare(
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
    Ok(by_queue)
}

/// In-place field/payload update pre-commit validation, with the exact error precedence the monolith's
/// `UpdateFieldsPort` enforces: absent → `NotFound`, fenced → `StaleLease`, terminal → `Terminal`,
/// superseded → `Superseded`, version mismatch → `Conflict`. Mutates nothing.
fn update_fields_validate_sql(
    conn: &Connection,
    shard: &QueueKey,
    id: &ItemId,
    expected_item_version: Option<u64>,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let row: Option<(String, i64, i64, i64)> = st(conn
        .query_row(
            "SELECT lifecycle_state, superseded, fenced, item_version FROM pqueue_items \
             WHERE tenant_id=?1 AND queue_id=?2 AND item_id=?3",
            params![t, q, id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional())?;
    let (state, superseded, fenced, version) = row.ok_or(EngineError::NotFound)?;
    if fenced != 0 {
        return Err(EngineError::StaleLease);
    }
    if parse_state(&state)?.is_terminal() {
        return Err(EngineError::Terminal);
    }
    if superseded != 0 {
        return Err(EngineError::Superseded);
    }
    if expected_item_version.is_some_and(|v| v != version as u64) {
        return Err(EngineError::Conflict);
    }
    Ok(())
}

/// Durable instance/state fence for `key` (absent → `None`, read by the caller as the unset value `0`).
fn instance_fence_sql(
    conn: &Connection,
    shard: &QueueKey,
    key: &[u8],
) -> EngineResult<Option<u64>> {
    let (t, q) = parts(shard);
    let v: Option<i64> = st(conn
        .query_row(
            "SELECT fence FROM pqueue_instance_fences \
             WHERE tenant_id=?1 AND queue_id=?2 AND instance_key=?3",
            params![t, q, key],
            |row| row.get(0),
        )
        .optional())?;
    Ok(v.map(|v| v as u64))
}

/// Opaque non-work side record by key, or `None`.
fn side_record_sql(conn: &Connection, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
    let (t, q) = parts(shard);
    let payload: Option<Vec<u8>> = st(conn
        .query_row(
            "SELECT payload FROM pqueue_side_records \
             WHERE tenant_id=?1 AND queue_id=?2 AND key=?3",
            params![t, q, key],
            |row| row.get(0),
        )
        .optional())?;
    Ok(payload.map(Bytes::from))
}

/// The unified sqlite-relational store: ONE value, shared behind `Arc<Mutex<Inner>>`, that implements BOTH
/// the [`LogStore`] (epoch/fence + position mint) and [`ProjectionStore`] (durable apply + the full read /
/// validate / commit-class surface) axes of [`ComposedBackend`]. Two clones (one per axis field) point at
/// the same `Inner`, so `commit_locked`'s append→apply is one transactional unit (ADR-012 P1b-ii).
#[derive(Clone)]
pub struct SqliteRelational {
    inner: Arc<Mutex<Inner>>,
}

impl SqliteRelational {
    /// Open (or create) the unified relational store at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Self::from_conn(st(Connection::open(path))?)
    }

    /// An ephemeral `:memory:` unified relational store.
    pub fn in_memory() -> EngineResult<Self> {
        Self::from_conn(st(Connection::open_in_memory())?)
    }

    fn from_conn(conn: Connection) -> EngineResult<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(open_inner(conn)?)),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("sqlite relational store poisoned")
    }
}

impl LogStore for SqliteRelational {
    fn durability_class(&self) -> DurabilityClass {
        // append+apply land in ONE relational transaction (apply commits both projection + cursor advance).
        DurabilityClass::Atomic
    }

    fn ensure_shard(&mut self, _shard: &QueueKey) -> EngineResult<()> {
        // The durable cursor/queue rows are created by the projection axis' `ensure_shard` (which has the
        // full `QueueDefinition`); the log axis shares the same `Inner`, so there is nothing extra to do.
        Ok(())
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let g = self.lock();
        let (t, q) = parts(shard);
        st(g.conn
            .query_row(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| row.get::<_, i64>(0),
            )
            .optional())?
        .ok_or(EngineError::NotFound)
        .map(|e| e as u64)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        let g = self.lock();
        let (t, q) = parts(shard);
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
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // STAGE only: read the cursor, fence, and MINT positions. No durable write — the apply axis advances
        // the cursor inside its own transaction, so no log row can outlive a failed projection apply.
        let g = self.lock();
        let (t, q) = parts(shard);
        let (next, epoch): (i64, i64) = st(g
            .conn
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        .ok_or(EngineError::NotFound)?;
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
        // The relational family is DB-authoritative: there is no replayable command log (the projection is
        // the source of truth). The conformance CORE class never reads the log; surface an empty page.
        Ok(CommandPage {
            entries: Vec::new(),
            next: None,
        })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        // The durable projection cursor is the high-water analogue: the next sequence is `next_seq`, so the
        // last absorbed position is `next_seq - 1`. `None` before any command is applied.
        let g = self.lock();
        let (t, q) = parts(shard);
        let (next, epoch): (i64, i64) = match st(g
            .conn
            .query_row(
                "SELECT next_seq, assignment_epoch FROM relational_cursor WHERE tenant=?1 AND queue=?2",
                params![t, q],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional())?
        {
            Some(v) => v,
            None => return Ok(None),
        };
        Ok(
            (next > 0)
                .then(|| CommandPosition::new(shard.clone(), epoch as u64, (next as u64) - 1)),
        )
    }

    fn set_high_water(
        &mut self,
        _shard: &QueueKey,
        _position: CommandPosition,
    ) -> EngineResult<()> {
        // The cursor advances transactionally inside `apply`; an external high-water set is a no-op for the
        // DB-authoritative family (there is no detached log tail to acknowledge).
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

impl ProjectionStore for SqliteRelational {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let mut g = self.lock();
        create_queue_sql(&mut g, definition.clone())?;
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // COMMIT: the single durable relational transaction (projection rows + cursor advance), at the
        // positions the log axis just minted. Reuses the group-commit apply verbatim.
        let mut g = self.lock();
        apply_committed_batch_sql(&mut g, positions, commands)
    }

    // -- recovery-on-open (ADR-012 P2): the DB-authoritative store already holds the full projection +
    //    definitions, so there is nothing to replay (the default `recovery_high_water` → `None` makes the
    //    composition's `read_from` return an empty page). Recovery only repopulates the in-process control
    //    plane (via `recover_definitions`) and re-seeds the id-mint counters from `pqueue_items`.

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        let g = self.lock();
        let (t, q) = parts(shard);
        let mut stmt = st(g
            .conn
            .prepare("SELECT item_id FROM pqueue_items WHERE tenant_id=?1 AND queue_id=?2"))?;
        let rows = st(stmt.query_map(params![t, q], |row| row.get::<_, String>(0)))?;
        for r in rows {
            let id = ItemId::new(st(r)?).map_err(|e| EngineError::Storage(e.to_string()))?;
            counters.observe(shard, id);
        }
        Ok(())
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&self.lock().conn, shard, now, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        let g = self.lock();
        render_claimed(&g.conn, shard, ids, |id| g.live_tokens.get(id).cloned())
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        lookup_active_by_key(&self.lock().conn, shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        item_state_sql(&self.lock().conn, shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        item_version_sql(&self.lock().conn, shard, id)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        expired_leases_sql(&self.lock().conn, shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        all_expired_leases_sql(&self.lock().conn, now).unwrap_or_default()
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
        validate_leased(&self.lock().conn, shard, &ids)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        update_fields_validate_sql(&self.lock().conn, shard, id, expected_item_version)
    }

    // Secondary indexes are a deferred relational feature (the family stubs them) — validation is a no-op
    // and queries report `Unavailable`, exactly like the monolithic `SqliteRelationalBackend`.
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

    // -- commit-class (Snorri vectorized commit boundary): the relational projection materializes the full
    //    read model, so it opts in and answers the pre-commit reads from its own SQL.
    fn supports_commit_transition(&self) -> bool {
        true
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let g = self.lock();
        let tx = st(g.conn.unchecked_transaction())?;
        for claim_ref in refs {
            commit_validate_sql(&tx, shard, claim_ref, now)?;
        }
        Ok(())
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        instance_fence_sql(&self.lock().conn, shard, key)
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        side_record_sql(&self.lock().conn, shard, key)
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&self.lock().conn, shard, now, limit)
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        peek_sql(&self.lock().conn, shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let g = self.lock();
        pending_sql(&g.conn, &g.live_tokens, shard)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&self.lock().conn, shard)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        live_items_sql(&self.lock().conn, shard, keys)
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

// ---------------------------------------------------------------------------
// ADR-012 P1b-ii (Part B): the DERIVED sqlite projection as a `ProjectionStore`
// ---------------------------------------------------------------------------
//
// `SqliteProjectionStore` is the relational SQL projection fed by an EXTERNAL command-log authority (the
// object log, or a sqlite/postgres LOG axis). Wrapping it as a [`ProjectionStore`] lets the generic
// [`ComposedBackend`] pair it with any [`LogStore`] — `ComposedBackend<SqliteLog, SqliteProjectionStore>`
// (atomic) and `ComposedBackend<ObjectLog, SqliteProjectionStore>` (eventual-apply) — instead of the
// hand-written `ObjectLogSqliteBackend` monolith. `apply` is the same group-commit `apply_committed_batch`
// the monolith uses (idempotent prefix-skip, gap error), so a committed log position is materialized once.

impl SqliteProjectionStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("projection store poisoned")
    }
}

impl ProjectionStore for HybridProjectionStore {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        self.check_healthy()?;
        self.sqlite.create_queue_projection(definition.clone())?;
        self.hydrate_from_sqlite(definition)
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.check_healthy()?;
        self.sqlite.apply_committed_batch(positions, commands)?;
        let mut advanced: HashMap<QueueKey, u64> = HashMap::new();
        let apply_result: EngineResult<()> = (|| {
            for (pos, env) in positions.iter().zip(commands.iter()) {
                let next_seq = self.memory_next_seq.get(&pos.queue).copied().unwrap_or(0);
                if pos.sequence >= next_seq {
                    self.memory
                        .apply_borrowed(std::slice::from_ref(pos), std::slice::from_ref(env))?;
                    let candidate = pos.sequence.saturating_add(1);
                    advanced
                        .entry(pos.queue.clone())
                        .and_modify(|next| *next = (*next).max(candidate))
                        .or_insert(candidate);
                }
            }
            Ok(())
        })();
        match apply_result {
            Ok(()) => {
                self.memory_next_seq.extend(advanced);
                Ok(())
            }
            Err(err) => self.poison(format!("memory apply failed after sqlite commit: {err}")),
        }
    }

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        self.require_hydrated(shard)?;
        let next = self.sqlite.recovery_high_water(shard)?;
        Ok(next.and_then(|n| (n > 0).then(|| CommandPosition::new(shard.clone(), 0, n - 1))))
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        self.check_healthy()?;
        Ok(self.sqlite.lock().queues.values().cloned().collect())
    }

    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.sqlite.observe_item_counters(shard, counters)
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.eligible_candidates(shard, now, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        self.require_hydrated(shard)?;
        self.memory.render_claimed(shard, ids)
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.lookup_by_key(shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        self.require_hydrated(shard)?;
        self.memory.item_state(shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        self.require_hydrated(shard)?;
        self.memory.item_version(shard, id)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.expired_leases(shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        if self.poisoned.is_some() {
            return Vec::new();
        }
        self.memory.all_expired_leases(now)
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.finalize_validate(shard, outcomes)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.renew_validate(shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.reassign_validate(shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .update_fields_validate(shard, id, expected_item_version)
    }

    fn index_validate(
        &self,
        shard: &QueueKey,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&serde_json::Value>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .index_validate(shard, item_id, fields, entity, exclude)
    }

    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.index_validate_push(shard, items)
    }

    fn index_validate_replace(
        &self,
        shard: &QueueKey,
        existing_id: &ItemId,
        item: &PushItem,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.index_validate_replace(shard, existing_id, item)
    }

    fn index_validate_update(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
        entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .index_validate_update(shard, id, field_ops, entity)
    }

    fn supports_commit_transition(&self) -> bool {
        self.poisoned.is_none() && self.memory.supports_commit_transition()
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.commit_validate(shard, refs, now)
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        self.require_hydrated(shard)?;
        self.memory.instance_fence(shard, key)
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        self.require_hydrated(shard)?;
        self.memory.side_record(shard, key)
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.select_eligible(shard, now, limit)
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.require_hydrated(shard)?;
        self.memory.peek(shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        self.require_hydrated(shard)?;
        self.memory.pending(shard)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        self.require_hydrated(shard)?;
        self.memory.metrics(shard)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        self.require_hydrated(shard)?;
        self.memory.live_items(shard, keys)
    }

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        self.require_hydrated(shard)?;
        self.memory.index_get_unique(shard, index, key)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.require_hydrated(shard)?;
        self.memory.index_lookup(shard, index, key)
    }
}

impl ProjectionStore for SqliteProjectionStore {
    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let mut g = self.lock();
        create_queue_sql(&mut g, definition.clone())?;
        Ok(())
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // Idempotent group-commit apply at the externally assigned log positions (one sqlite transaction).
        self.apply_committed_batch(positions, commands)
    }

    // -- recovery-on-open (ADR-012 P2): this derived sqlite projection persists its high-water + definitions,
    //    so a reopened composition replays only the object-/sqlite-log tail beyond the snapshot.

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        // The inherent method returns `next_seq`; the last position already absorbed is `next_seq - 1`
        // (the log's `read_from` resumes at `sequence + 1`, so the first replayed command is `next_seq`).
        let next = SqliteProjectionStore::recovery_high_water(self, shard)?;
        Ok(next.and_then(|n| (n > 0).then(|| CommandPosition::new(shard.clone(), 0, n - 1))))
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn restore_counters(&self, shard: &QueueKey, counters: &QueueCounters) -> EngineResult<()> {
        self.observe_item_counters(shard, counters)
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&self.lock().conn, shard, now, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        let g = self.lock();
        render_claimed(&g.conn, shard, ids, |id| g.live_tokens.get(id).cloned())
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        lookup_active_by_key(&self.lock().conn, shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        item_state_sql(&self.lock().conn, shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        item_version_sql(&self.lock().conn, shard, id)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        expired_leases_sql(&self.lock().conn, shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        all_expired_leases_sql(&self.lock().conn, now).unwrap_or_default()
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
        validate_leased(&self.lock().conn, shard, &ids)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        validate_leased(&self.lock().conn, shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        update_fields_validate_sql(&self.lock().conn, shard, id, expected_item_version)
    }

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

    fn supports_commit_transition(&self) -> bool {
        true
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let g = self.lock();
        let tx = st(g.conn.unchecked_transaction())?;
        for claim_ref in refs {
            commit_validate_sql(&tx, shard, claim_ref, now)?;
        }
        Ok(())
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        instance_fence_sql(&self.lock().conn, shard, key)
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        side_record_sql(&self.lock().conn, shard, key)
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&self.lock().conn, shard, now, limit)
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        peek_sql(&self.lock().conn, shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let g = self.lock();
        pending_sql(&g.conn, &g.live_tokens, shard)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&self.lock().conn, shard)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        live_items_sql(&self.lock().conn, shard, keys)
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

/// The composed unified sqlite-relational backend (ADR-012 P1b-ii):
/// `ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>` — one relational store on
/// both the log and projection axes, so append+apply commit as one transaction. Capability-equivalent to
/// the monolithic [`SqliteRelationalBackend`] on the CORE conformance class.
pub type ComposedSqliteRelationalBackend =
    ComposedBackend<SqliteRelational, SqliteRelational, InProcessControlPlane>;

/// Assemble a unified sqlite-relational composition over an ephemeral `:memory:` store. Both axes are clones
/// of the SAME store (shared connection), so the orthogonal `commit_locked` drives one durable transaction.
pub fn composed_sqlite_relational_in_memory() -> EngineResult<ComposedSqliteRelationalBackend> {
    let store = SqliteRelational::in_memory()?;
    Ok(ComposedBackend::new(
        store.clone(),
        store,
        InProcessControlPlane::new(),
    ))
}

/// Assemble a unified sqlite-relational composition over a DURABLE store at `path`. Runs recovery-on-open
/// (ADR-012 P2): the DB-authoritative projection needs no log replay, so recovery only repopulates the
/// in-process control plane from the durable `queues` catalog and re-seeds the id-mint counters.
pub fn composed_sqlite_relational(path: &str) -> EngineResult<ComposedSqliteRelationalBackend> {
    let store = SqliteRelational::open(path)?;
    ComposedBackend::new(store.clone(), store, InProcessControlPlane::new()).recover()
}

#[cfg(test)]
mod group_summary_tests {
    //! White-box tests for `pqueue_group_summary` maintenance — they read the summary table directly
    //! (it has no read port yet; BQ-14 consumes it), driving state through the public ports.
    use super::*;
    use pqueue_core::{
        EligibilityPolicy, GateKeyPolicy, OrderingMode, PriorityDirection, PriorityModelKind,
        PriorityTieBreaker, QueueId, RecurrencePolicy, RetryPolicy, TenantId, WorkerId,
    };
    use pqueue_engine::{
        ClaimRequest, CommandChecksum, CommandId, GroupBatching, SetGatesCommand, SetGatesPort,
    };

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
        }
    }
    fn qdef_gates() -> QueueDefinition {
        QueueDefinition {
            eligibility_policy: EligibilityPolicy {
                gate_keys: GateKeyPolicy::Dynamic,
                max_gate_keys_per_item: Some(8),
                max_gates_per_request: Some(8),
                ..EligibilityPolicy::default()
            },
            ..qdef()
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
    fn grouped_not_before(priority: i64, group: &str, not_before: i64) -> PushSpec {
        PushSpec {
            not_before: Some(ts(not_before)),
            ..grouped(priority, group)
        }
    }
    fn gated_grouped_not_before(
        priority: i64,
        group: &str,
        not_before: i64,
        gate: &str,
    ) -> PushSpec {
        PushSpec {
            gate_keys: vec![gate.to_string()],
            ..grouped_not_before(priority, group, not_before)
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
    fn claim_req_compat(
        max: usize,
        exp: i64,
        now: i64,
        compatibility: ClaimCompatibility,
    ) -> ClaimRequest {
        ClaimRequest {
            compatibility,
            ..claim_req(max, exp, now)
        }
    }

    async fn set_gate(b: &SqliteRelationalBackend, gate_key: &str, blocked: bool, now: i64) {
        b.set_gates(
            &shard(),
            SetGatesCommand {
                gate_keys: vec![gate_key.to_string()],
                blocked,
            },
            ts(now),
            None,
        )
        .await
        .unwrap();
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

    fn next_seq(b: &SqliteRelationalBackend) -> i64 {
        let g = b.inner.lock().unwrap();
        g.conn
            .query_row(
                "SELECT next_seq FROM relational_cursor WHERE tenant='t1' AND queue='q1'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn request_id_push_replays_prior_ids_without_second_append() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let request_id = RequestId::new("push-req-1").unwrap();
        let body = vec![PushSpec::default(), grouped(20, "g")];

        let first = b
            .push_with_request_id(&shard(), request_id.clone(), body.clone(), ts(0), None)
            .await
            .unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(next_seq(&b), 1);

        let replay = b
            .push_with_request_id(&shard(), request_id, body, ts(1), None)
            .await
            .unwrap();
        assert_eq!(replay, first, "same request body replays the prior ids");
        assert_eq!(next_seq(&b), 1, "replay did not append a second command");
    }

    #[tokio::test]
    async fn request_id_push_conflicts_on_different_body() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let request_id = RequestId::new("push-req-conflict").unwrap();

        b.push_with_request_id(
            &shard(),
            request_id.clone(),
            vec![PushSpec::default()],
            ts(0),
            None,
        )
        .await
        .unwrap();

        let err = b
            .push_with_request_id(
                &shard(),
                request_id,
                vec![grouped(99, "other")],
                ts(1),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err, EngineError::RequestIdConflict);
        assert_eq!(next_seq(&b), 1, "conflict did not append");
    }

    #[tokio::test]
    async fn push_without_request_id_still_appends_each_call() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();

        let first = b
            .push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let second = b
            .push(&shard(), vec![PushSpec::default()], ts(1), None)
            .await
            .unwrap();

        assert_ne!(second, first);
        assert_eq!(next_seq(&b), 2);
    }

    #[tokio::test]
    async fn same_group_claim_discovers_group_that_becomes_due_by_time() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();
        let ids = b
            .push(
                &shard(),
                vec![grouped_not_before(10, "deferred", 10)],
                ts(0),
                None,
            )
            .await
            .unwrap();

        let early = b
            .claim(claim_req_compat(
                10,
                500,
                9,
                ClaimCompatibility {
                    same_group_key: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert!(early.items.is_empty(), "not_before is half-open before due");

        let due = b
            .claim(claim_req_compat(
                10,
                500,
                10,
                ClaimCompatibility {
                    same_group_key: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            due.items
                .iter()
                .map(|item| item.item_id)
                .collect::<Vec<_>>(),
            ids,
            "same_group_key sees the group exactly at not_before with no intervening mutation"
        );
    }

    #[tokio::test]
    async fn group_batching_discovers_group_that_becomes_due_by_time() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(QueueDefinition {
            max_eligible_group_size: Some(5),
            ..qdef()
        })
        .await
        .unwrap();
        let ids = b
            .push(
                &shard(),
                vec![
                    grouped_not_before(10, "deferred", 10),
                    grouped_not_before(11, "deferred", 10),
                ],
                ts(0),
                None,
            )
            .await
            .unwrap();

        let claimed = b
            .claim(claim_req_compat(
                10,
                500,
                10,
                ClaimCompatibility {
                    group_batching: Some(GroupBatching { max_groups: 1 }),
                    ..Default::default()
                },
            ))
            .await
            .unwrap();

        assert_eq!(
            claimed
                .items
                .iter()
                .map(|item| item.item_id)
                .collect::<Vec<_>>(),
            ids,
            "group_batching refreshes and leases the whole due group"
        );
    }

    #[tokio::test]
    async fn due_refresh_keeps_gate_blocked_groups_unclaimable() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef_gates()).await.unwrap();
        let ids = b
            .push(
                &shard(),
                vec![gated_grouped_not_before(10, "deferred", 10, "hold")],
                ts(0),
                None,
            )
            .await
            .unwrap();
        set_gate(&b, "hold", true, 1).await;

        let blocked = b
            .claim(claim_req_compat(
                10,
                500,
                10,
                ClaimCompatibility {
                    same_group_key: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert!(
            blocked.items.is_empty(),
            "due refresh must not make a gate-blocked group claimable"
        );

        set_gate(&b, "hold", false, 11).await;
        let unblocked = b
            .claim(claim_req_compat(
                10,
                500,
                12,
                ClaimCompatibility {
                    same_group_key: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            unblocked
                .items
                .iter()
                .map(|item| item.item_id)
                .collect::<Vec<_>>(),
            ids,
            "clearing the gate lets the due group refresh and claim"
        );
    }

    #[tokio::test]
    async fn group_summary_tracks_eligibility_through_the_lifecycle() {
        let b = SqliteRelationalBackend::in_memory().unwrap();
        b.create_queue(qdef()).await.unwrap();

        // Push two grouped items (priorities 10, 20) — rep is the priority-10 item, count 2.
        let ids = b
            .push(
                &shard(),
                vec![grouped(10, "g"), grouped(20, "g")],
                ts(0),
                None,
            )
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
        b.purge(&shard(), vec![ids[1]], false, ts(20), None)
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
            vec![FinalizeOutcome::new(ids[0], FinalizeKind::Release)],
            ts(20),
            None,
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
        b.push(
            &shard(),
            vec![grouped(5, "g"), grouped(6, "g")],
            ts(0),
            None,
        )
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
                Default::default(),
                None,
                ts(0),
                None,
            )
            .await
            .unwrap()
        {
            UpsertOutcome::Inserted { item_id } => item_id,
            _ => panic!("insert"),
        };
        // Purge a PENDING item (not terminal) -> no retention tombstone, so the key is freely reusable.
        b.purge(&shard(), vec![id], false, ts(1), None)
            .await
            .unwrap();
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
                    Default::default(),
                    None,
                    ts(2),
                    None
                )
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
            request_fingerprint: None,
            request_outcome: None,
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
                .push(
                    &shard(),
                    vec![grouped(10, "g"), grouped(20, "g")],
                    ts(0),
                    None,
                )
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
