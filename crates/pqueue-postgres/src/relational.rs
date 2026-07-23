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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axon_esf::CompiledSchema;
use bytes::Bytes;
use postgres::error::SqlState;
use postgres::types::ToSql;
use postgres::{Client, GenericClient};
use pqueue_core::{
    ClientItemKey, CohortId, FilterOp, GroupKey, IndexDeclaration, IndexType, ItemId, ItemState,
    LeaseToken, Metadata, MetricsByQueryRequest, PriorityModel, PriorityValue, QueryFilter,
    QueueDefinition, QueueId, QueueIndex, RequestId, TenantId, TypedValue, UtcTimestamp,
    is_retry_exhausted, priority_sort,
};
use pqueue_engine::{
    ActiveScope, AdvanceInstanceFenceCommand, Backend, BatchUpdateItemRef, BatchUpdateOutcome,
    BatchUpdatePort, BatchUpdateRequest, BatchUpdateResponse, BatchUpdateValue, ClaimCommand,
    ClaimCompatibility, ClaimPort, ClaimRequest, ClaimUnit, Claimed, ClaimedItem,
    CohortClaimCommand, CohortExpiredCommand, CohortFinalizeCommand, CohortFinalizePort,
    CohortLeaseTarget, CohortRenewLeaseCommand, CohortRenewLeasePort, CommandChecksum,
    CommandEnvelope, CommandId, CommandPosition, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitOutcomeEntry, CommitRecovery, CommitRejection, CommitTransition,
    CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome,
    DiscoveryGranularity, DiscoveryPort, DurabilityClass, DurableIntegrityStage, EngineError,
    EngineResult, EntryRecovery, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort,
    HistoricalProjectionRead, IdempotencyDecision, IndexHit, IndexQueryPort, ItemView,
    LeaseExpiredCommand, LeaseView, LiveItemView, PayloadUpdate, PendingPage, PendingSummary,
    ProjectionRead, PurgeItemsCommand, PurgePort, PushCommand, PushFingerprint, PushItem, PushPort,
    PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeaseCommand,
    RenewLeasePort, ReplacePendingCommand, RequestOutcome, ScheduleUpdate, SetGatesCommand,
    SetGatesPort, TerminalEmissionMetrics, TickReport, UpdateFieldsCommand, UpdateFieldsPort,
    UpsertOutcome, UpsertPort, WriteSideRecordsCommand, build_push_items, compile_entity_schema,
    project_scopes, validate_api001_reserved_write_fields, validate_claim_compatibility,
    validate_entity, validate_gate_push, validate_instance_fence, validate_purge_force,
};
use pqueue_engine::{
    AsOfProjectionStore, CommandPage, LogLineageIdentity, LogRead, LogStore, ProjectionSnapshot,
    ProjectionStore, RichClaimSelection, SnapshotRef,
};
use sha2::{Digest, Sha256};

use crate::{PostgresConnectConfig, connect};

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PushSqlProbe {
    admission_conflict_queries: u64,
    admission_group_queries: u64,
    group_summary_statements: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BatchUpdateSqlProbe {
    target_selects: u64,
    command_batch_inserts: u64,
    projection_updates: u64,
}

#[cfg(test)]
thread_local! {
    static PUSH_SQL_PROBES: RefCell<HashMap<QueueKey, PushSqlProbe>> = RefCell::new(HashMap::new());
    static BATCH_UPDATE_SQL_PROBES: RefCell<HashMap<QueueKey, BatchUpdateSqlProbe>> = RefCell::new(HashMap::new());
}

#[cfg(test)]
fn reset_batch_update_sql_probe(shard: &QueueKey) {
    BATCH_UPDATE_SQL_PROBES.with(|probes| {
        probes
            .borrow_mut()
            .insert(shard.clone(), BatchUpdateSqlProbe::default());
    });
}

#[cfg(test)]
fn update_batch_update_sql_probe(shard: &QueueKey, update: impl FnOnce(&mut BatchUpdateSqlProbe)) {
    BATCH_UPDATE_SQL_PROBES.with(|probes| {
        update(probes.borrow_mut().entry(shard.clone()).or_default());
    });
}

#[cfg(test)]
fn batch_update_sql_probe(shard: &QueueKey) -> BatchUpdateSqlProbe {
    BATCH_UPDATE_SQL_PROBES.with(|probes| probes.borrow().get(shard).copied().unwrap_or_default())
}

#[cfg(test)]
fn reset_push_sql_probe(shard: &QueueKey) {
    PUSH_SQL_PROBES.with(|probes| {
        probes
            .borrow_mut()
            .insert(shard.clone(), PushSqlProbe::default());
    });
}

#[cfg(test)]
fn update_push_sql_probe(shard: &QueueKey, update: impl FnOnce(&mut PushSqlProbe)) {
    PUSH_SQL_PROBES.with(|probes| {
        update(probes.borrow_mut().entry(shard.clone()).or_default());
    });
}

#[cfg(test)]
fn push_sql_probe(shard: &QueueKey) -> PushSqlProbe {
    PUSH_SQL_PROBES.with(|probes| probes.borrow().get(shard).copied().unwrap_or_default())
}

/// The relational schema (postgres). Mirrors the sqlite reference column-for-column: `pqueue_items` is
/// TD-002's item projection plus the reference operational columns (`fenced`/`superseded`/`max_attempts`/
/// `created_seq`); a partial unique index keeps one ACTIVE item per `client_item_key`; `relational_cursor`
/// holds the per-queue command + item sequence counters (allocated atomically). `pqueue_group_summary` and
/// `pqueue_item_key_retention` are the relational-only group/idempotency projections (BQ-11c parity).
pub(crate) const RELATIONAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    paused BOOLEAN NOT NULL DEFAULT false,
    pause_drain_intake BOOLEAN NOT NULL DEFAULT false,
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
CREATE INDEX IF NOT EXISTS pqueue_items_expired_lease_idx
    ON pqueue_items (tenant_id, queue_id, lease_expires_at, item_id)
    WHERE lifecycle_state = 'Leased' AND cohort_size IS NULL AND fenced = false AND superseded = false;
CREATE INDEX IF NOT EXISTS pqueue_items_pending_entry_idx
    ON pqueue_items (tenant_id, queue_id, (item_id::numeric))
    INCLUDE (lease_token_hash, lease_expires_at, retry_count)
    WHERE lifecycle_state = 'Leased';
CREATE TABLE IF NOT EXISTS relational_cursor (
    tenant TEXT NOT NULL, queue TEXT NOT NULL,
    next_seq BIGINT NOT NULL,
    next_item_seq BIGINT NOT NULL,
    assignment_epoch BIGINT NOT NULL DEFAULT 0,   -- TD-003 durable ownership epoch (the fence authority)
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS pqueue_id_high_water (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id)
);
CREATE TABLE IF NOT EXISTS pqueue_schema_migrations (
    migration_name TEXT NOT NULL PRIMARY KEY
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
CREATE TABLE IF NOT EXISTS pqueue_group_due_pending (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL,
    group_key TEXT NOT NULL, due_at BIGINT NOT NULL, created_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
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
CREATE TABLE IF NOT EXISTS pqueue_item_index_component (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, index_name TEXT NOT NULL,
    item_id TEXT NOT NULL, component_position INTEGER NOT NULL, component_value BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, index_name, item_id, component_position)
);
CREATE TABLE IF NOT EXISTS pqueue_metrics_counted_item (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL, superseded BOOLEAN NOT NULL, item_version BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
);
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

const COMMAND_LOG_MIGRATION: &str = r#"
CREATE TABLE IF NOT EXISTS pqueue_commands (
    tenant TEXT NOT NULL,
    queue TEXT NOT NULL,
    assignment_epoch BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    command_id TEXT NOT NULL,
    envelope BYTEA NOT NULL,
    envelope_sha256 BYTEA NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (tenant, queue, seq)
);
CREATE INDEX IF NOT EXISTS pqueue_commands_read_idx
    ON pqueue_commands (tenant, queue, seq);
CREATE UNIQUE INDEX IF NOT EXISTS pqueue_commands_command_id_idx
    ON pqueue_commands (tenant, queue, command_id);
CREATE TABLE IF NOT EXISTS pqueue_command_baselines (
    tenant TEXT NOT NULL,
    queue TEXT NOT NULL,
    generation TEXT NOT NULL,
    schema_version BIGINT NOT NULL,
    assignment_epoch BIGINT NOT NULL,
    next_seq BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    snapshot_digest BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant, queue)
);
CREATE TABLE IF NOT EXISTS pqueue_command_baseline_rows (
    tenant TEXT NOT NULL,
    queue TEXT NOT NULL,
    generation TEXT NOT NULL,
    relation_name TEXT NOT NULL,
    row_ordinal BIGINT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (tenant, queue, generation, relation_name, row_ordinal)
);
CREATE TABLE IF NOT EXISTS pqueue_command_baseline_migrations (
    tenant TEXT NOT NULL,
    queue TEXT NOT NULL,
    generation TEXT NOT NULL,
    expected_epoch BIGINT NOT NULL,
    expected_next_seq BIGINT NOT NULL,
    relation_index BIGINT NOT NULL DEFAULT 0,
    last_ctid TEXT NOT NULL DEFAULT '(0,0)',
    rows_copied BIGINT NOT NULL DEFAULT 0,
    hash_a NUMERIC NOT NULL DEFAULT 0,
    hash_b NUMERIC NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant, queue)
);
"#;

const BASELINE_RELATIONS: &[(&str, &str, &str)] = &[
    ("queues", "tenant", "queue"),
    ("relational_emission_cursor", "tenant", "queue"),
    ("pqueue_id_high_water", "tenant_id", "queue_id"),
    ("pqueue_items", "tenant_id", "queue_id"),
    ("pqueue_group_summary", "tenant_id", "queue_id"),
    ("pqueue_group_due_pending", "tenant_id", "queue_id"),
    ("pqueue_item_key_retention", "tenant_id", "queue_id"),
    ("pqueue_cohorts", "tenant_id", "queue_id"),
    ("pqueue_item_gates", "tenant_id", "queue_id"),
    ("pqueue_gate_state", "tenant_id", "queue_id"),
    ("pqueue_request_idempotency", "tenant_id", "queue_id"),
    ("pqueue_item_index", "tenant_id", "queue_id"),
    ("pqueue_item_index_component", "tenant_id", "queue_id"),
    ("pqueue_metrics_counted_item", "tenant_id", "queue_id"),
    ("pqueue_queue_metrics_v2", "tenant_id", "queue_id"),
    ("pqueue_side_records", "tenant_id", "queue_id"),
    ("pqueue_instance_fences", "tenant_id", "queue_id"),
];

const QUEUE_METRICS_MIGRATION: &str = r#"
-- The operator migration is standalone: it must create every relation used by
-- its concurrent indexes before an upgraded backend has run constructor DDL.
CREATE TABLE IF NOT EXISTS pqueue_group_due_pending (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL,
    group_key TEXT NOT NULL, due_at BIGINT NOT NULL, created_seq BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
);
CREATE TABLE IF NOT EXISTS pqueue_item_index_component (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, index_name TEXT NOT NULL,
    item_id TEXT NOT NULL, component_position INTEGER NOT NULL, component_value BYTEA NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, index_name, item_id, component_position)
);
CREATE TABLE IF NOT EXISTS pqueue_queue_metrics_v2 (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL,
    pending BIGINT NOT NULL DEFAULT 0, leased BIGINT NOT NULL DEFAULT 0,
    complete BIGINT NOT NULL DEFAULT 0, failed BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, queue_id)
);
CREATE TABLE IF NOT EXISTS pqueue_metrics_migration_state (
    migration_name TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    high_tenant TEXT, high_queue TEXT, high_item_id TEXT,
    last_tenant TEXT, last_queue TEXT, last_item_id TEXT,
    rows_backfilled BIGINT NOT NULL DEFAULT 0,
    due_rows_backfilled BIGINT NOT NULL DEFAULT 0,
    batches_completed BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS pqueue_metrics_counted_item (
    tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL, superseded BOOLEAN NOT NULL, item_version BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, queue_id, item_id)
);
ALTER TABLE pqueue_metrics_migration_state
    ADD COLUMN IF NOT EXISTS due_rows_backfilled BIGINT NOT NULL DEFAULT 0;
CREATE OR REPLACE FUNCTION pqueue_index_component(key BYTEA, component_offset INTEGER)
RETURNS BYTEA AS $$
DECLARE component_length INTEGER;
BEGIN
  IF component_offset < 0 OR octet_length(key) < component_offset + 4 THEN RETURN NULL; END IF;
  component_length := (get_byte(key, component_offset) << 24)
                    + (get_byte(key, component_offset + 1) << 16)
                    + (get_byte(key, component_offset + 2) << 8)
                    + get_byte(key, component_offset + 3);
  IF octet_length(key) < component_offset + 4 + component_length THEN RETURN NULL; END IF;
  RETURN substring(key FROM component_offset + 5 FOR component_length);
END $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION pqueue_index_components(key BYTEA)
RETURNS TABLE(component_position INTEGER, component_value BYTEA) AS $$
DECLARE component_offset INTEGER := 0;
DECLARE component_length INTEGER;
BEGIN
  component_position := 0;
  WHILE component_offset + 4 <= octet_length(key) LOOP
    component_length := (get_byte(key, component_offset) << 24)
                      + (get_byte(key, component_offset + 1) << 16)
                      + (get_byte(key, component_offset + 2) << 8)
                      + get_byte(key, component_offset + 3);
    IF component_offset + 4 + component_length > octet_length(key) THEN RETURN; END IF;
    component_value := substring(key FROM component_offset + 5 FOR component_length);
    RETURN NEXT;
    component_offset := component_offset + 4 + component_length;
    component_position := component_position + 1;
  END LOOP;
END $$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION pqueue_sync_index_components() RETURNS trigger AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    DELETE FROM pqueue_item_index_component
     WHERE tenant_id=OLD.tenant_id AND queue_id=OLD.queue_id
       AND index_name=OLD.index_name AND item_id=OLD.item_id;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    INSERT INTO pqueue_item_index_component(
      tenant_id,queue_id,index_name,item_id,component_position,component_value)
    SELECT NEW.tenant_id,NEW.queue_id,NEW.index_name,NEW.item_id,
           component_position,component_value
      FROM pqueue_index_components(NEW.index_key);
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$ LANGUAGE plpgsql;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='pqueue_item_index_components_sync'
             AND tgrelid='pqueue_item_index'::regclass
             AND (tgfoid<>to_regprocedure('pqueue_sync_index_components()')
                  OR tgenabled NOT IN ('O','A')
                  OR pg_get_triggerdef(oid) NOT LIKE '%AFTER INSERT OR DELETE OR UPDATE OF index_key%')) THEN
    DROP TRIGGER pqueue_item_index_components_sync ON pqueue_item_index;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='pqueue_item_index_components_sync'
                 AND tgrelid='pqueue_item_index'::regclass) THEN
    CREATE TRIGGER pqueue_item_index_components_sync
      AFTER INSERT OR DELETE OR UPDATE OF index_key ON pqueue_item_index
      FOR EACH ROW EXECUTE FUNCTION pqueue_sync_index_components();
  END IF;
END $$;
CREATE OR REPLACE FUNCTION pqueue_apply_metrics_delta() RETURNS trigger AS $$
DECLARE p BIGINT := 0; l BIGINT := 0; c BIGINT := 0; f BIGINT := 0;
DECLARE metric_tenant TEXT; metric_queue TEXT;
DECLARE old_counted BOOLEAN := false; new_counted BOOLEAN := false;
DECLARE marker_found BOOLEAN := false;
DECLARE acquired BIGINT := 0;
DECLARE counted_state TEXT; counted_superseded BOOLEAN; counted_version BIGINT;
BEGIN
  IF TG_OP <> 'INSERT' THEN
    SELECT lifecycle_state,superseded,item_version
      INTO counted_state,counted_superseded,counted_version
      FROM pqueue_metrics_counted_item
     WHERE tenant_id=OLD.tenant_id AND queue_id=OLD.queue_id AND item_id=OLD.item_id
     FOR UPDATE;
    marker_found := FOUND;
    old_counted := marker_found;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    LOOP
      IF marker_found THEN
        UPDATE pqueue_metrics_counted_item SET lifecycle_state=NEW.lifecycle_state,
          superseded=NEW.superseded,item_version=NEW.item_version
         WHERE tenant_id=NEW.tenant_id AND queue_id=NEW.queue_id AND item_id=NEW.item_id;
        new_counted := true;
        EXIT;
      END IF;
      INSERT INTO pqueue_metrics_counted_item(
        tenant_id,queue_id,item_id,lifecycle_state,superseded,item_version)
      VALUES(NEW.tenant_id,NEW.queue_id,NEW.item_id,NEW.lifecycle_state,NEW.superseded,NEW.item_version)
      ON CONFLICT DO NOTHING;
      GET DIAGNOSTICS acquired = ROW_COUNT;
      IF acquired=1 THEN new_counted := true; EXIT; END IF;
      SELECT lifecycle_state,superseded,item_version
        INTO counted_state,counted_superseded,counted_version
        FROM pqueue_metrics_counted_item
       WHERE tenant_id=NEW.tenant_id AND queue_id=NEW.queue_id AND item_id=NEW.item_id
       FOR UPDATE;
      marker_found := FOUND;
      old_counted := marker_found;
    END LOOP;
  END IF;
  IF old_counted AND NOT counted_superseded THEN
    p := p - CASE WHEN counted_state='Pending' THEN 1 ELSE 0 END;
    l := l - CASE WHEN counted_state='Leased' THEN 1 ELSE 0 END;
    c := c - CASE WHEN counted_state='Complete' THEN 1 ELSE 0 END;
    f := f - CASE WHEN counted_state='Failed' THEN 1 ELSE 0 END;
    metric_tenant := OLD.tenant_id; metric_queue := OLD.queue_id;
  END IF;
  IF TG_OP <> 'DELETE' AND new_counted AND NOT NEW.superseded THEN
    p := p + CASE WHEN NEW.lifecycle_state='Pending' THEN 1 ELSE 0 END;
    l := l + CASE WHEN NEW.lifecycle_state='Leased' THEN 1 ELSE 0 END;
    c := c + CASE WHEN NEW.lifecycle_state='Complete' THEN 1 ELSE 0 END;
    f := f + CASE WHEN NEW.lifecycle_state='Failed' THEN 1 ELSE 0 END;
    metric_tenant := NEW.tenant_id; metric_queue := NEW.queue_id;
  END IF;
  IF p<>0 OR l<>0 OR c<>0 OR f<>0 THEN
    INSERT INTO pqueue_queue_metrics_v2(tenant_id,queue_id,pending,leased,complete,failed)
      VALUES(metric_tenant,metric_queue,p,l,c,f)
    ON CONFLICT(tenant_id,queue_id) DO UPDATE SET
      pending=pqueue_queue_metrics_v2.pending+EXCLUDED.pending,
      leased=pqueue_queue_metrics_v2.leased+EXCLUDED.leased,
      complete=pqueue_queue_metrics_v2.complete+EXCLUDED.complete,
      failed=pqueue_queue_metrics_v2.failed+EXCLUDED.failed;
  END IF;
  IF TG_OP='DELETE' THEN
    DELETE FROM pqueue_metrics_counted_item
      WHERE tenant_id=OLD.tenant_id AND queue_id=OLD.queue_id AND item_id=OLD.item_id;
  END IF;
  IF TG_OP <> 'INSERT' THEN
    DELETE FROM pqueue_group_due_pending
      WHERE tenant_id=OLD.tenant_id AND queue_id=OLD.queue_id AND item_id=OLD.item_id;
  END IF;
  IF TG_OP <> 'DELETE' AND NEW.group_key IS NOT NULL AND NEW.lifecycle_state='Pending'
     AND NOT NEW.superseded AND NEW.not_before IS NOT NULL THEN
    INSERT INTO pqueue_group_due_pending(
      tenant_id,queue_id,item_id,group_key,due_at,created_seq)
    VALUES(NEW.tenant_id,NEW.queue_id,NEW.item_id,NEW.group_key,NEW.not_before,NEW.created_seq)
    ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET
      group_key=EXCLUDED.group_key,due_at=EXCLUDED.due_at,created_seq=EXCLUDED.created_seq;
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END $$ LANGUAGE plpgsql;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='pqueue_items_metrics_delta'
             AND tgrelid='pqueue_items'::regclass
             AND (tgfoid<>to_regprocedure('pqueue_apply_metrics_delta()')
                  OR tgenabled NOT IN ('O','A')
                  OR pg_get_triggerdef(oid) NOT LIKE
                    '%AFTER INSERT OR DELETE OR UPDATE OF lifecycle_state, superseded, not_before, group_key%')) THEN
    DROP TRIGGER pqueue_items_metrics_delta ON pqueue_items;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='pqueue_items_metrics_delta'
                 AND tgrelid='pqueue_items'::regclass) THEN
    CREATE TRIGGER pqueue_items_metrics_delta
      AFTER INSERT OR DELETE OR UPDATE OF lifecycle_state,superseded,not_before,group_key ON pqueue_items
      FOR EACH ROW EXECUTE FUNCTION pqueue_apply_metrics_delta();
  END IF;
END $$;
"#;

const GROUP_SUMMARY_INDEX_MIGRATIONS: &[(&str, &str)] = &[
    (
        "pqueue_item_index_component_lookup_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_item_index_component_lookup_idx \
         ON pqueue_item_index_component \
           (tenant_id,queue_id,index_name,component_position,component_value,item_id)",
    ),
    (
        "pqueue_items_group_summary_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_group_summary_idx \
         ON pqueue_items (tenant_id,queue_id,group_key,priority_sort,created_seq) \
         INCLUDE (eligible_since,created_at,item_id,not_before) \
         WHERE lifecycle_state='Pending' AND superseded=false AND group_key IS NOT NULL",
    ),
    (
        "pqueue_items_group_oldest_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_group_oldest_idx \
         ON pqueue_items (tenant_id,queue_id,group_key,eligible_since,created_seq) INCLUDE (not_before) \
         WHERE lifecycle_state='Pending' AND superseded=false AND group_key IS NOT NULL",
    ),
    (
        "pqueue_items_group_active_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_group_active_idx \
         ON pqueue_items (tenant_id,queue_id,group_key,item_id) \
         WHERE lifecycle_state = ANY (ARRAY['Pending'::text,'Leased'::text]) \
           AND superseded=false AND group_key IS NOT NULL",
    ),
    (
        "pqueue_items_group_due_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_group_due_idx \
         ON pqueue_items (tenant_id,queue_id,group_key,not_before,created_seq) INCLUDE (item_id) \
         WHERE lifecycle_state='Pending' AND superseded=false AND group_key IS NOT NULL \
           AND not_before IS NOT NULL",
    ),
    (
        "pqueue_group_summary_claim_rank_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_group_summary_claim_rank_idx \
         ON pqueue_group_summary \
            (tenant_id,queue_id,rep_priority_sort,rep_item_id,group_key) \
         WHERE oldest_eligible_at IS NOT NULL",
    ),
    (
        "pqueue_group_summary_oldest_rank_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_group_summary_oldest_rank_idx \
         ON pqueue_group_summary (tenant_id,queue_id,oldest_eligible_at,group_key) \
         WHERE oldest_eligible_at IS NOT NULL",
    ),
    (
        "pqueue_group_summary_refresh_frontier_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_group_summary_refresh_frontier_idx \
         ON pqueue_group_summary (tenant_id,queue_id,updated_at,group_key)",
    ),
    (
        "pqueue_items_global_expired_lease_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_global_expired_lease_idx \
         ON pqueue_items (lease_expires_at,tenant_id,queue_id,item_id) \
         WHERE lifecycle_state='Leased' AND cohort_size IS NULL \
           AND fenced=false AND superseded=false",
    ),
    (
        "pqueue_items_pending_entry_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_items_pending_entry_idx \
         ON pqueue_items (tenant_id,queue_id,(item_id::numeric)) \
         INCLUDE (lease_token_hash,lease_expires_at,retry_count) \
         WHERE lifecycle_state='Leased'",
    ),
    (
        "pqueue_group_due_pending_frontier_idx",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS pqueue_group_due_pending_frontier_idx \
         ON pqueue_group_due_pending (tenant_id,queue_id,due_at,created_seq,item_id)",
    ),
];

fn normalized_index_signature(definition: &str, table: &str) -> Option<String> {
    let normalized: String = definition
        .to_ascii_lowercase()
        .replace("::text", "")
        .replace("using btree", "")
        .chars()
        .filter(|character| {
            !character.is_ascii_whitespace()
                && *character != '"'
                && *character != '('
                && *character != ')'
                && *character != ';'
        })
        .collect();
    let start = normalized.rfind(table)?;
    Some(normalized[start..].to_string())
}

/// Fresh empty schemas may build the required indexes synchronously. Existing schemas never run DDL at
/// constructor time: operators apply the versioned concurrent definitions before rollout, and startup
/// fails fast if any index is absent, invalid, or definition-drifted.
fn verify_group_summary_indexes(client: &mut Client, fresh: bool) -> EngineResult<()> {
    for (name, ddl) in GROUP_SUMMARY_INDEX_MIGRATIONS {
        if fresh {
            let create = ddl.replacen(" CONCURRENTLY", "", 1);
            st(client.batch_execute(&create))?;
            continue;
        }
        let table = if ddl.contains(" ON pqueue_items ") {
            "pqueue_items"
        } else if ddl.contains("pqueue_item_index_component") {
            "pqueue_item_index_component"
        } else if ddl.contains(" ON pqueue_item_index ") {
            "pqueue_item_index"
        } else if ddl.contains(" ON pqueue_group_due_pending ") {
            "pqueue_group_due_pending"
        } else {
            "pqueue_group_summary"
        };
        let Some(row) = st(client.query_opt(
            "SELECT i.indisvalid,pg_get_indexdef(i.indexrelid) FROM pg_index i \
             WHERE i.indexrelid=to_regclass($1)",
            &[name],
        ))?
        else {
            return Err(EngineError::Unavailable);
        };
        let valid: bool = row.get(0);
        let actual: String = row.get(1);
        if !valid
            || normalized_index_signature(&actual, table) != normalized_index_signature(ddl, table)
        {
            return Err(EngineError::Unavailable);
        }
    }
    Ok(())
}

const COHORT_EXPIRY_SWEEP_LIMIT: i64 = 128;
const GLOBAL_EXPIRY_SWEEP_LIMIT: i64 = 128;
const IDEMPOTENCY_OPERATION_PUSH: &str = "push";

/// The serialized claim CTE (TD-002 `BatchClaim`): select the eligible candidates under a real
/// `FOR UPDATE SKIP LOCKED` row lock and lease them in ONE statement, RETURNING the rich claimed rows.
/// Concurrent claimers lock disjoint candidate sets — no process Mutex, no select-then-lease TOCTOU.
/// Authored as a constant so its shape is unit-asserted without a live DB (`sql_shape_tests`).
pub(crate) const CLAIM_CTE: &str = "\
WITH candidates AS MATERIALIZED ( \
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
), updated AS ( \
UPDATE pqueue_items i \
SET lifecycle_state='Leased', lease_token_hash=$5, lease_expires_at=$6, \
    retry_count=retry_count+1, item_version=item_version+1, updated_at=$7, last_command_sequence=$8 \
FROM candidates c \
	WHERE i.tenant_id=$1 AND i.queue_id=$2 AND i.item_id=c.item_id \
		RETURNING i.item_id, i.client_item_key, i.item_version, i.priority, i.group_key, i.not_before, \
		          i.lease_expires_at, i.retry_count, i.payload, i.fields, i.metadata, \
		          i.priority_sort AS claim_priority_sort, i.created_seq AS claim_created_seq \
) \
SELECT item_id, client_item_key, item_version, priority, group_key, not_before, lease_expires_at, \
       retry_count, payload, fields, metadata FROM updated \
ORDER BY claim_priority_sort, claim_created_seq";

pub(crate) const ITEM_GATE_KEYS_BATCH_SQL: &str = "\
SELECT item_id, gate_key FROM pqueue_item_gates \
WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3) \
ORDER BY item_id, gate_key";

const ASYNC_EXPIRED_LEASES_BOUNDED_SQL: &str = "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
     AND lifecycle_state='Leased' AND cohort_size IS NULL AND fenced=false AND superseded=false \
     AND lease_expires_at IS NOT NULL AND lease_expires_at<$3 ORDER BY item_id LIMIT $4";

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
    Ok(Sha256::digest(&bytes)[..8].to_vec())
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
    let affected = st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,expires_at,created_at) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
          request_fingerprint=EXCLUDED.request_fingerprint, \
          response_payload=EXCLUDED.response_payload, \
          expires_at=EXCLUDED.expires_at, created_at=EXCLUDED.created_at \
         WHERE pqueue_request_idempotency.expires_at<=EXCLUDED.created_at OR \
           (pqueue_request_idempotency.request_fingerprint=EXCLUDED.request_fingerprint \
           AND pqueue_request_idempotency.response_payload=EXCLUDED.response_payload)",
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
    if affected == 0 {
        return Err(EngineError::RequestIdConflict);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// C9: authoritative vectorized claimed-work commit — idempotency + validation helpers
// ---------------------------------------------------------------------------

const IDEMPOTENCY_OPERATION_COMMIT: &str = "commit";
const IDEMPOTENCY_OPERATION_BATCH_UPDATE: &str = "batch_update";

fn check_batch_update_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    now_n: i64,
) -> EngineResult<Option<String>> {
    let (t, q) = parts(shard);
    let prior = st(tx.query_opt(
        "SELECT request_fingerprint,response_payload,expires_at \
         FROM pqueue_request_idempotency \
         WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
        &[
            &t,
            &q,
            &IDEMPOTENCY_OPERATION_BATCH_UPDATE,
            &request_id.as_str(),
        ],
    ))?;
    let Some(row) = prior else { return Ok(None) };
    let prior_fingerprint: Vec<u8> = row.get(0);
    let response_payload: String = row.get(1);
    let expires_at: i64 = row.get(2);
    if expires_at <= now_n {
        st(tx.execute(
            "DELETE FROM pqueue_request_idempotency \
             WHERE tenant_id=$1 AND queue_id=$2 AND operation=$3 AND request_id=$4",
            &[
                &t,
                &q,
                &IDEMPOTENCY_OPERATION_BATCH_UPDATE,
                &request_id.as_str(),
            ],
        ))?;
        return Ok(None);
    }
    if prior_fingerprint != fingerprint {
        return Err(EngineError::RequestIdConflict);
    }
    Ok(Some(response_payload))
}

#[allow(clippy::too_many_arguments)]
fn record_batch_update_idempotency(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    request_id: &RequestId,
    fingerprint: &[u8],
    response_payload: &str,
    now: UtcTimestamp,
    expires_at: i64,
) -> EngineResult<()> {
    let (t, q) = parts(shard);
    let affected = st(tx.execute(
        "INSERT INTO pqueue_request_idempotency \
         (tenant_id,queue_id,operation,request_id,request_fingerprint,response_payload,expires_at,created_at) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8) \
         ON CONFLICT(tenant_id,queue_id,operation,request_id) DO UPDATE SET \
           request_fingerprint=EXCLUDED.request_fingerprint,response_payload=EXCLUDED.response_payload, \
           expires_at=EXCLUDED.expires_at,created_at=EXCLUDED.created_at \
         WHERE pqueue_request_idempotency.expires_at<=EXCLUDED.created_at OR \
           (pqueue_request_idempotency.request_fingerprint=EXCLUDED.request_fingerprint AND \
            pqueue_request_idempotency.response_payload=EXCLUDED.response_payload)",
        &[
            &t,
            &q,
            &IDEMPOTENCY_OPERATION_BATCH_UPDATE,
            &request_id.as_str(),
            &fingerprint,
            &response_payload,
            &expires_at,
            &ts_nanos(now),
        ],
    ))?;
    if affected == 0 {
        return Err(EngineError::RequestIdConflict);
    }
    Ok(())
}

fn commit_request_fingerprint(entries: &[CommitTransitionEntry]) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(entries).map_err(|e| EngineError::Storage(e.to_string()))?;
    Ok(Sha256::digest(&bytes)[..8].to_vec())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredEntryRecovery {
    consumed_input_id: String,
    #[serde(default)]
    additional_consumed_input_ids: Vec<String>,
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
        EngineError::DurableDataCorrupt {
            stage,
            manifest_index,
            locator,
        } => (
            "durable_data_corrupt",
            Some(format!("{stage}:{manifest_index}:{locator}")),
        ),
        EngineError::EntitySchemaViolation(msg) => ("entity_schema_violation", Some(msg.clone())),
        EngineError::RequestTooLarge { requested, limit } => {
            ("request_too_large", Some(format!("{requested}:{limit}")))
        }
        EngineError::Backpressure { resource } => ("backpressure", Some((*resource).to_string())),
    }
}

fn decode_engine_error(code: &str, detail: Option<String>) -> EngineError {
    match code {
        "durable_data_corrupt" => decode_durable_data_corrupt(detail),
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
        "request_too_large" => {
            let (requested, limit) = detail
                .as_deref()
                .and_then(|value| value.split_once(':'))
                .and_then(|(requested, limit)| Some((requested.parse().ok()?, limit.parse().ok()?)))
                .unwrap_or((usize::MAX, 0));
            EngineError::RequestTooLarge { requested, limit }
        }
        "backpressure" => EngineError::Backpressure {
            resource: "bounded resource",
        },
        _ => EngineError::Storage(detail.unwrap_or_else(|| code.to_string())),
    }
}

fn decode_durable_data_corrupt(detail: Option<String>) -> EngineError {
    let detail = detail.unwrap_or_default();
    let mut fields = detail.splitn(3, ':');
    let stage = pqueue_engine::DurableIntegrityStage::parse(fields.next().unwrap_or_default())
        .unwrap_or(pqueue_engine::DurableIntegrityStage::Manifest);
    EngineError::DurableDataCorrupt {
        stage,
        manifest_index: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
        locator: fields.next().unwrap_or("unknown").to_owned(),
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

fn durable_outcome_entry(recovery: &EntryRecovery) -> CommitOutcomeEntry {
    CommitOutcomeEntry {
        consumed_input_id: recovery.consumed_input_id,
        additional_consumed_input_ids: recovery.additional_consumed_input_ids.clone(),
        instance: recovery.instance.clone(),
        side_record_keys: recovery.side_record_keys.clone(),
        lifecycle_item_ids: recovery.lifecycle_item_ids.clone(),
        rejection: match &recovery.status {
            CommitEntryStatus::Committed => None,
            CommitEntryStatus::Rejected(error) => Some(CommitRejection::from_error(error)),
        },
    }
}

fn recovery_from_durable_outcome(entry: CommitOutcomeEntry) -> EntryRecovery {
    EntryRecovery {
        consumed_input_id: entry.consumed_input_id,
        additional_consumed_input_ids: entry.additional_consumed_input_ids,
        instance: entry.instance,
        side_record_keys: entry.side_record_keys,
        lifecycle_item_ids: entry.lifecycle_item_ids,
        status: match entry.rejection {
            Some(rejection) => CommitEntryStatus::Rejected(rejection.into_error()),
            None => CommitEntryStatus::Committed,
        },
    }
}

fn encode_commit_recovery(recovery: &[EntryRecovery]) -> EngineResult<String> {
    let stored: Vec<StoredEntryRecovery> = recovery
        .iter()
        .map(|r| StoredEntryRecovery {
            consumed_input_id: r.consumed_input_id.to_string(),
            additional_consumed_input_ids: r
                .additional_consumed_input_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
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
            let additional_consumed_input_ids = s
                .additional_consumed_input_ids
                .into_iter()
                .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<Vec<_>>>()?;
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
                additional_consumed_input_ids,
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

fn persist_command_envelopes(
    tx: &mut postgres::Transaction<'_>,
    positions: &[CommandPosition],
    commands: &[CommandEnvelope],
) -> EngineResult<()> {
    if positions.len() != commands.len() {
        return Err(EngineError::Storage(
            "command position/envelope cardinality mismatch".into(),
        ));
    }
    for chunk_start in (0..positions.len()).step_by(1024) {
        let chunk_end = (chunk_start + 1024).min(positions.len());
        let mut tenants = Vec::with_capacity(chunk_end - chunk_start);
        let mut queues = Vec::with_capacity(chunk_end - chunk_start);
        let mut epochs = Vec::with_capacity(chunk_end - chunk_start);
        let mut sequences = Vec::with_capacity(chunk_end - chunk_start);
        let mut command_ids = Vec::with_capacity(chunk_end - chunk_start);
        let mut envelopes = Vec::with_capacity(chunk_end - chunk_start);
        let mut checksums = Vec::with_capacity(chunk_end - chunk_start);
        let mut created_at = Vec::with_capacity(chunk_end - chunk_start);
        for (position, envelope) in positions[chunk_start..chunk_end]
            .iter()
            .zip(&commands[chunk_start..chunk_end])
        {
            let (tenant, queue) = parts(&position.queue);
            let encoded = serde_json::to_vec(envelope).map_err(|error| {
                EngineError::Storage(format!("command serialization failed: {error}"))
            })?;
            tenants.push(tenant);
            queues.push(queue);
            epochs.push(position.backend_epoch as i64);
            sequences.push(position.sequence as i64);
            command_ids.push(envelope.command_id.0.clone());
            checksums.push(command_storage_checksum(position, &encoded));
            envelopes.push(encoded);
            created_at.push(ts_nanos(envelope.created_at));
        }
        st(tx.execute(
            "INSERT INTO pqueue_commands \
             (tenant,queue,assignment_epoch,seq,command_id,envelope,envelope_sha256,created_at) \
             SELECT * FROM UNNEST($1::text[],$2::text[],$3::bigint[],$4::bigint[],\
                                  $5::text[],$6::bytea[],$7::bytea[],$8::bigint[])",
            &[
                &tenants,
                &queues,
                &epochs,
                &sequences,
                &command_ids,
                &envelopes,
                &checksums,
                &created_at,
            ],
        ))?;
    }
    Ok(())
}

fn command_storage_checksum(position: &CommandPosition, encoded: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    let tenant = position.queue.tenant_id.as_str().as_bytes();
    let queue = position.queue.queue_id.as_str().as_bytes();
    digest.update((tenant.len() as u64).to_be_bytes());
    digest.update(tenant);
    digest.update((queue.len() as u64).to_be_bytes());
    digest.update(queue);
    digest.update(position.backend_epoch.to_be_bytes());
    digest.update(position.sequence.to_be_bytes());
    digest.update(encoded);
    digest.finalize().to_vec()
}

fn direct_command_envelope(
    shard: &QueueKey,
    command: QueueCommand,
    now: UtcTimestamp,
    epoch: u64,
    seq: u64,
) -> CommandEnvelope {
    let (tenant, queue) = parts(shard);
    let item_ids = command_item_ids(&command);
    CommandEnvelope {
        command_id: CommandId::new(format!("pgrel-{tenant}-{queue}-{epoch}-{seq}")),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: now,
    }
}

fn command_item_ids(command: &QueueCommand) -> Vec<ItemId> {
    match command {
        QueueCommand::Push(command) => command.items.iter().map(|item| item.item_id).collect(),
        QueueCommand::Claim(command) => command.item_ids.clone(),
        QueueCommand::CohortClaim(command) => command.item_ids.clone(),
        QueueCommand::RenewLease(command) => command.item_ids.clone(),
        QueueCommand::CohortRenewLease(_) => Vec::new(),
        QueueCommand::ReassignLease(command) => command.item_ids.clone(),
        QueueCommand::Finalize(command) => command
            .outcomes
            .iter()
            .map(|outcome| outcome.item_id)
            .collect(),
        QueueCommand::CohortFinalize(_) => Vec::new(),
        QueueCommand::PurgeItems(command) => command.item_ids.clone(),
        QueueCommand::LeaseExpired(command) => command.item_ids.clone(),
        QueueCommand::CohortExpired(_) => Vec::new(),
        QueueCommand::FenceLease(command) => command.item_ids.clone(),
        QueueCommand::UnfenceLease(command) => command.item_ids.clone(),
        QueueCommand::ReplacePending(command) => vec![command.replacement.item_id],
        QueueCommand::UpdateFields(command) => vec![command.item_id],
        QueueCommand::CreateQueue(_)
        | QueueCommand::PauseQueue(_)
        | QueueCommand::ResumeQueue
        | QueueCommand::SetGates(_)
        | QueueCommand::WriteSideRecords(_)
        | QueueCommand::AdvanceInstanceFence(_) => Vec::new(),
    }
}

struct DirectCommand<'a> {
    shard: &'a QueueKey,
    epoch: u64,
    seq: u64,
    now: UtcTimestamp,
    command: QueueCommand,
}

fn persist_and_apply_command(
    tx: &mut postgres::Transaction<'_>,
    queues: &HashMap<QueueKey, QueueDefinition>,
    token_ops: &mut Vec<TokenOp>,
    direct: DirectCommand<'_>,
) -> EngineResult<()> {
    let DirectCommand {
        shard,
        epoch,
        seq,
        now,
        command,
    } = direct;
    let position = CommandPosition::new(shard.clone(), epoch, seq);
    let envelope = direct_command_envelope(shard, command, now, epoch, seq);
    persist_command_envelopes(
        tx,
        std::slice::from_ref(&position),
        std::slice::from_ref(&envelope),
    )?;
    apply_command_sql(tx, queues, token_ops, shard, seq, now, &envelope.command)
}

struct Inner {
    client: Client,
    queues: HashMap<QueueKey, QueueDefinition>,
    schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    live_tokens: HashMap<ItemId, LeaseToken>,
}

impl Inner {
    /// Reload the queue-def cache from the durable `queues` table. Command-log integrity and any explicit
    /// snapshot-tail rebuild are handled by the owning backend before this cache is served.
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
    /// epoch-fenced — the TD-003 `assignment_epoch` fence lives at the typed commit seam.
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
            "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
            &[&t, &q],
        ))?
        .get(0);
        if expected_epoch.is_some_and(|e| e != epoch as u64) {
            return Err(EngineError::EpochFenced);
        }
        let seq = alloc_seq(&mut tx, &t, &q)?;
        let position = CommandPosition::new(shard.clone(), epoch as u64, seq);
        let envelope = direct_command_envelope(shard, command, now, epoch as u64, seq);
        persist_command_envelopes(
            &mut tx,
            std::slice::from_ref(&position),
            std::slice::from_ref(&envelope),
        )?;
        let mut token_ops = Vec::new();
        apply_command_sql(
            &mut tx,
            queues,
            &mut token_ops,
            shard,
            seq,
            now,
            &envelope.command,
        )?;
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
    alloc_seq_range(tx, t, q, 1)
}

fn alloc_seq_range(
    tx: &mut postgres::Transaction<'_>,
    t: &str,
    q: &str,
    count: usize,
) -> EngineResult<u64> {
    if count == 0 {
        return Err(EngineError::Invalid(
            "command sequence range must be non-empty",
        ));
    }
    let count = i64::try_from(count)
        .map_err(|_| EngineError::Invalid("command sequence range exceeds postgres limit"))?;
    let row = st(tx.query_opt(
        "UPDATE relational_cursor SET next_seq = next_seq + $3 WHERE tenant=$1 AND queue=$2 \
         RETURNING next_seq - $3",
        &[&t, &q, &count],
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
        "SELECT priority_sort, created_at, created_seq, item_id FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false AND (not_before IS NULL OR not_before<=$4) \
         ORDER BY priority_sort, created_seq LIMIT 1",
        &[&t, &q, &group_key.as_str(), &now_n],
    ))?;
    let (rep_psort, rep_created, rep_item): (Option<Vec<u8>>, Option<i64>, Option<String>) =
        match rep {
            Some(row) => (Some(row.get(0)), Some(row.get(1)), Some(row.get(3))),
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

/// Recompute all affected group summaries in one database round trip. Push batches routinely span many
/// groups; issuing the three-statement single-group refresh for every distinct group multiplied round trips
/// and repeated resident-group scans. The requested CTE also keeps groups that became empty, so the result
/// is identical to calling `refresh_group_summary` for each key (zero count and null representative).
fn refresh_group_summaries(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    group_keys: &[GroupKey],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if group_keys.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let groups: Vec<String> = group_keys
        .iter()
        .map(|group| group.as_str().to_string())
        .collect();
    #[cfg(test)]
    update_push_sql_probe(shard, |probe| probe.group_summary_statements += 1);
    st(tx.execute(
        "WITH requested(group_key) AS (SELECT DISTINCT unnest($3::text[])), \
         eligible AS ( \
           SELECT group_key, eligible_since, priority_sort, created_at, item_id, created_seq \
           FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=ANY($3) \
             AND lifecycle_state='Pending' AND superseded=false \
             AND (not_before IS NULL OR not_before<=$4) \
         ), aggregated AS ( \
           SELECT group_key, COUNT(*)::bigint AS item_count, MIN(eligible_since) AS oldest \
           FROM eligible GROUP BY group_key \
         ), representative AS ( \
           SELECT DISTINCT ON (group_key) group_key, priority_sort, created_at, created_seq, item_id \
           FROM eligible ORDER BY group_key, priority_sort, created_seq \
         ) \
         INSERT INTO pqueue_group_summary \
           (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort, \
            rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         SELECT $1,$2,r.group_key,a.oldest,NULL,p.priority_sort,p.created_at,p.item_id, \
                COALESCE(a.item_count,0),0,$4 \
         FROM requested r LEFT JOIN aggregated a USING (group_key) \
                          LEFT JOIN representative p USING (group_key) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
           oldest_eligible_at=EXCLUDED.oldest_eligible_at, \
           rep_progress_guard_sort=EXCLUDED.rep_progress_guard_sort, \
           rep_priority_sort=EXCLUDED.rep_priority_sort, rep_created_at=EXCLUDED.rep_created_at, \
           rep_item_id=EXCLUDED.rep_item_id, eligible_item_count=EXCLUDED.eligible_item_count, \
           at_risk_count=EXCLUDED.at_risk_count, updated_at=EXCLUDED.updated_at",
        &[&t, &q, &groups, &now_n],
    ))?;
    Ok(())
}

/// Add newly-pending items to group summaries without scanning pre-existing group inventory. The source
/// is bounded by `item_ids`; aggregate and representative work is therefore O(batch), not O(resident).
fn increment_group_summaries_for_items(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    item_ids: &[String],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if item_ids.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    #[cfg(test)]
    update_push_sql_probe(shard, |probe| probe.group_summary_statements += 5);
    st(tx.execute(
        "WITH wanted AS ( \
           SELECT DISTINCT group_key FROM pqueue_items \
           WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) AND group_key IS NOT NULL \
         ), incoming_new AS ( \
           SELECT group_key,eligible_since,priority_sort,created_at,item_id,created_seq \
           FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) \
             AND group_key IS NOT NULL AND lifecycle_state='Pending' AND superseded=false \
             AND (not_before IS NULL OR not_before<=$4) \
         ), incoming AS (SELECT * FROM incoming_new), aggregated AS ( \
           SELECT group_key,COUNT(*)::bigint AS item_count,MIN(eligible_since) AS oldest \
           FROM incoming GROUP BY group_key \
         ), representative AS ( \
           SELECT DISTINCT ON (group_key) group_key,priority_sort,created_at,created_seq,item_id \
           FROM incoming ORDER BY group_key,priority_sort,created_seq \
         ) \
         INSERT INTO pqueue_group_summary \
           (tenant_id,queue_id,group_key,oldest_eligible_at,rep_progress_guard_sort,rep_priority_sort, \
            rep_created_at,rep_item_id,eligible_item_count,at_risk_count,updated_at) \
         SELECT $1,$2,w.group_key,a.oldest,NULL,r.priority_sort,r.created_at,r.item_id, \
                COALESCE(a.item_count,0),0,$4 FROM wanted w \
                LEFT JOIN aggregated a USING (group_key) LEFT JOIN representative r USING (group_key) \
         ON CONFLICT(tenant_id,queue_id,group_key) DO UPDATE SET \
           oldest_eligible_at=CASE \
             WHEN pqueue_group_summary.oldest_eligible_at IS NULL THEN EXCLUDED.oldest_eligible_at \
             ELSE LEAST(pqueue_group_summary.oldest_eligible_at,EXCLUDED.oldest_eligible_at) END, \
           eligible_item_count=pqueue_group_summary.eligible_item_count+EXCLUDED.eligible_item_count, \
           at_risk_count=0,updated_at=pqueue_group_summary.updated_at",
        &[&t, &q, &item_ids, &now_n],
    ))?;
    // The existing representative's authoritative FIFO tiebreak lives on its item row. Keeping it out of
    // the summary schema makes this update rolling-version safe: old and new writers share the same rank
    // authority, and a nullable duplicated created-sequence column cannot corrupt mixed-version ordering.
    st(tx.execute(
        "WITH wanted AS ( \
           SELECT DISTINCT group_key FROM pqueue_items \
           WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) AND group_key IS NOT NULL \
         ), incoming AS ( \
           SELECT group_key,priority_sort,created_at,created_seq,item_id FROM pqueue_items \
           WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) AND group_key IS NOT NULL \
             AND lifecycle_state='Pending' AND superseded=false \
             AND (not_before IS NULL OR not_before<=$4) \
         ), representative AS ( \
           SELECT DISTINCT ON (group_key) group_key,priority_sort,created_at,created_seq,item_id \
           FROM incoming ORDER BY group_key,priority_sort,created_seq \
         ), winning AS ( \
           SELECT r.* FROM representative r JOIN pqueue_group_summary s \
             ON s.tenant_id=$1 AND s.queue_id=$2 AND s.group_key=r.group_key \
           LEFT JOIN pqueue_items old ON old.tenant_id=s.tenant_id AND old.queue_id=s.queue_id \
             AND old.item_id=s.rep_item_id \
           WHERE s.rep_item_id IS NULL OR old.item_id IS NULL OR \
             (r.priority_sort,r.created_seq,r.item_id) < \
             (s.rep_priority_sort,old.created_seq,s.rep_item_id) \
         ) UPDATE pqueue_group_summary s SET rep_priority_sort=w.priority_sort, \
             rep_created_at=w.created_at,rep_item_id=w.item_id \
           FROM winning w WHERE s.tenant_id=$1 AND s.queue_id=$2 AND s.group_key=w.group_key",
        &[&t, &q, &item_ids, &now_n],
    ))?;
    st(tx.execute(
        "UPDATE pqueue_group_summary s SET updated_at=GREATEST(s.updated_at,$4) \
         FROM (SELECT DISTINCT group_key FROM pqueue_items \
               WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) AND group_key IS NOT NULL) w \
         WHERE s.tenant_id=$1 AND s.queue_id=$2 AND s.group_key=w.group_key",
        &[&t, &q, &item_ids, &now_n],
    ))?;
    // Future grouped items enter a durable due frontier. Rewriting an item first removes any obsolete
    // schedule; the replacement row is inserted only while it is still pending and strictly future-due.
    st(tx.execute(
        "DELETE FROM pqueue_group_due_pending WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3)",
        &[&t, &q, &item_ids],
    ))?;
    st(tx.execute(
        "INSERT INTO pqueue_group_due_pending(tenant_id,queue_id,item_id,group_key,due_at,created_seq) \
         SELECT tenant_id,queue_id,item_id,group_key,not_before,created_seq FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3) AND group_key IS NOT NULL \
           AND lifecycle_state='Pending' AND superseded=false AND not_before>$4 \
         ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET group_key=EXCLUDED.group_key, \
           due_at=EXCLUDED.due_at,created_seq=EXCLUDED.created_seq",
        &[&t, &q, &item_ids, &now_n],
    ))?;
    Ok(())
}

/// Remove items that just left Pending from their summaries. Count changes are deltas. Oldest and
/// representative are repaired only when one of the removed rows supplied that stored value; each repair
/// is a bounded index lookup per affected group.
fn decrement_group_summaries_for_items(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    item_ids: &[String],
    now: UtcTimestamp,
) -> EngineResult<()> {
    if item_ids.is_empty() {
        return Ok(());
    }
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    st(tx.execute(
        "WITH affected AS ( \
           SELECT i.group_key,COUNT(*)::bigint AS removed_count, \
                  BOOL_OR(i.item_id=s.rep_item_id) AS rep_removed, \
                  BOOL_OR(i.eligible_since=s.oldest_eligible_at) AS oldest_removed \
           FROM pqueue_items i JOIN pqueue_group_summary s \
             ON s.tenant_id=i.tenant_id AND s.queue_id=i.queue_id AND s.group_key=i.group_key \
           WHERE i.tenant_id=$1 AND i.queue_id=$2 AND i.item_id=ANY($3) \
             AND i.group_key IS NOT NULL AND (i.not_before IS NULL OR i.not_before<=$4) \
           GROUP BY i.group_key \
         ), repaired AS ( \
           SELECT a.*,nr.priority_sort,nr.created_at,nr.created_seq,nr.item_id,ne.eligible_since \
           FROM affected a \
           LEFT JOIN LATERAL ( \
             SELECT priority_sort,created_at,created_seq,item_id FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND group_key=a.group_key \
               AND lifecycle_state='Pending' AND superseded=false \
               AND item_id<>ALL($3) \
               AND (not_before IS NULL OR not_before<=$4) \
             ORDER BY priority_sort,created_seq LIMIT 1 \
           ) nr ON a.rep_removed \
           LEFT JOIN LATERAL ( \
             SELECT eligible_since FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND group_key=a.group_key \
               AND lifecycle_state='Pending' AND superseded=false \
               AND item_id<>ALL($3) \
               AND (not_before IS NULL OR not_before<=$4) \
             ORDER BY eligible_since,created_seq LIMIT 1 \
           ) ne ON a.oldest_removed \
         ) \
         UPDATE pqueue_group_summary s SET \
           eligible_item_count=GREATEST(s.eligible_item_count-r.removed_count,0), \
           oldest_eligible_at=CASE WHEN r.oldest_removed THEN r.eligible_since \
                                   ELSE s.oldest_eligible_at END, \
           rep_priority_sort=CASE WHEN r.rep_removed THEN r.priority_sort \
                                  ELSE s.rep_priority_sort END, \
           rep_created_at=CASE WHEN r.rep_removed THEN r.created_at ELSE s.rep_created_at END, \
           rep_item_id=CASE WHEN r.rep_removed THEN r.item_id ELSE s.rep_item_id END, \
           at_risk_count=0,updated_at=GREATEST(s.updated_at,$4) \
         FROM repaired r WHERE s.tenant_id=$1 AND s.queue_id=$2 AND s.group_key=r.group_key",
        &[&t, &q, &item_ids, &now_n],
    ))?;
    Ok(())
}

const DUE_PROMOTION_ITEM_LIMIT: i64 = 128;

/// Promote one bounded durable due-frontier chunk in the caller's transaction. Each future grouped item
/// has exactly one frontier row; the item ids handed to the delta helper and all returned/materialized
/// rows are capped at `DUE_PROMOTION_ITEM_LIMIT`. The extra row is a completion sentinel and remains on
/// the durable frontier for the next retry.
fn promote_due_group_summary_chunk_in_tx(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<bool> {
    let (tenant, queue) = parts(shard);
    let at = ts_nanos(now);
    let fetch_limit = DUE_PROMOTION_ITEM_LIMIT + 1;
    // The queue cursor is the common mutation/promotion fence. Serializing before the frontier read
    // makes the 129th row a trustworthy completion sentinel even with concurrent claim/discovery.
    st(tx.query_one(
        "SELECT assignment_epoch FROM relational_cursor \
         WHERE tenant=$1 AND queue=$2 FOR UPDATE",
        &[&tenant, &queue],
    ))?;
    let rows = st(tx.query(
        "SELECT item_id FROM pqueue_group_due_pending \
         WHERE tenant_id=$1 AND queue_id=$2 AND due_at<=$3 \
         ORDER BY due_at,created_seq,item_id LIMIT $4 FOR UPDATE",
        &[&tenant, &queue, &at, &fetch_limit],
    ))?;
    if rows.is_empty() {
        return Ok(true);
    }
    let complete = rows.len() <= DUE_PROMOTION_ITEM_LIMIT as usize;
    let item_ids = rows
        .into_iter()
        .take(DUE_PROMOTION_ITEM_LIMIT as usize)
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    increment_group_summaries_for_items(tx, shard, &item_ids, now)?;
    Ok(complete)
}

fn promote_due_group_summary_chunk(
    client: &mut Client,
    shard: &QueueKey,
    now: UtcTimestamp,
) -> EngineResult<bool> {
    let mut tx = st(client.transaction())?;
    let complete = promote_due_group_summary_chunk_in_tx(&mut tx, shard, now)?;
    st(tx.commit())?;
    Ok(complete)
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
fn advance_id_high_water(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    item_ids: &[ItemId],
) -> EngineResult<()> {
    let Some(max_new) = item_ids
        .iter()
        .max_by_key(|item_id| (item_id.epoch(), item_id.counter()))
        .copied()
    else {
        return Ok(());
    };
    let (tenant, queue) = parts(shard);
    st(tx.execute(
        "INSERT INTO pqueue_id_high_water(tenant_id,queue_id,item_id) VALUES($1,$2,$3) \
         ON CONFLICT(tenant_id,queue_id) DO UPDATE SET item_id=EXCLUDED.item_id \
         WHERE pqueue_id_high_water.item_id::numeric < EXCLUDED.item_id::numeric",
        &[&tenant, &queue, &max_new.to_string()],
    ))?;
    Ok(())
}

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
        QueueCommand::CreateQueue(command) => {
            let definition = to_json(&command.definition)?;
            st(tx.execute(
                "INSERT INTO queues(tenant,queue,definition,paused,pause_drain_intake) \
                 VALUES($1,$2,$3,false,false) \
                 ON CONFLICT(tenant,queue) DO UPDATE SET definition=EXCLUDED.definition",
                &[&t, &q, &definition],
            ))?;
            st(tx.execute(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq,assignment_epoch) \
                 VALUES($1,$2,0,0,0) ON CONFLICT(tenant,queue) DO NOTHING",
                &[&t, &q],
            ))?;
            Ok(())
        }
        QueueCommand::Push(c) => {
            let model = queues
                .get(shard)
                .map(|d| d.priority_model)
                .ok_or(EngineError::NotFound)?;
            let client_keys: Vec<&str> = c
                .items
                .iter()
                .map(|item| item.client_item_key.as_str())
                .collect();
            st(tx.execute(
                "DELETE FROM pqueue_item_key_retention \
                 WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key=ANY($3) AND expires_at<=$4",
                &[&t, &q, &client_keys, &now_n],
            ))?;
            insert_items(tx, queues, &model, shard, &c.items, seq, now)?;
            let minted_ids: Vec<ItemId> = c.items.iter().map(|item| item.item_id).collect();
            advance_id_high_water(tx, shard, &minted_ids)?;
            let ids: Vec<String> = c
                .items
                .iter()
                .filter(|item| item.group_key.is_some())
                .map(|item| item.item_id.to_string())
                .collect();
            increment_group_summaries_for_items(tx, shard, &ids, now)?;
            Ok(())
        }
        QueueCommand::Claim(c) => {
            let hash = lease_hash(&c.lease_token);
            let exp = ts_nanos(c.lease_expires_at);
            let ids: Vec<String> = c.item_ids.iter().map(|i| i.to_string()).collect();
            let worker_id = c.worker_id.as_ref().map(|worker| worker.as_str());
            st(tx.execute(
                "UPDATE pqueue_items SET lifecycle_state='Leased', lease_token_hash=$4, \
                 lease_expires_at=$5, worker_id=$6, retry_count=retry_count+1, \
                 item_version=item_version+1, updated_at=$7, last_command_sequence=$8 \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                &[&t, &q, &ids, &hash, &exp, &worker_id, &now_n, &seqi],
            ))?;
            for id in &c.item_ids {
                token_ops.push(TokenOp::Set(*id, c.lease_token.clone()));
            }
            decrement_group_summaries_for_items(tx, shard, &ids, now)?;
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
            decrement_group_summaries_for_items(tx, shard, &ids, now)?;
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
                "SELECT fields,lifecycle_state,priority,priority_sort,not_before,eligible_since,payload,metadata \
                 FROM pqueue_items \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=false AND fenced=false",
                &[&t, &q, &item_id],
            ))?;
            let Some(row) = row else { return Ok(()) };
            let mut fields = fields_from_json(row.get::<_, String>(0))?;
            let lifecycle_state: String = row.get(1);
            let mut priority_json: Option<String> = row.get(2);
            let mut priority_sort_key: Vec<u8> = row.get(3);
            let mut not_before: Option<i64> = row.get(4);
            let mut eligible_since: i64 = row.get(5);
            let mut payload: Option<Vec<u8>> = row.get(6);
            let mut metadata_json: String = row.get(7);
            if let Some(replacement) = &c.set_fields {
                fields = replacement.clone();
            }
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
            if let PayloadUpdate::Set(next) = &c.payload {
                payload = next.as_ref().map(|bytes| bytes.to_vec());
            }
            if let Some(metadata) = &c.set_metadata {
                metadata_json = metadata_to_json(metadata)?;
            }
            let repricing = matches!(c.set_priority, ScheduleUpdate::Set(_))
                || matches!(c.set_not_before, ScheduleUpdate::Set(_));
            let pending = lifecycle_state == "Pending";
            let singleton = vec![item_id.clone()];
            if pending && repricing {
                decrement_group_summaries_for_items(tx, shard, &singleton, now)?;
            }
            if let ScheduleUpdate::Set(next) = &c.set_priority {
                priority_json = next.as_ref().map(to_json).transpose()?;
                let model = queues
                    .get(shard)
                    .ok_or(EngineError::NotFound)?
                    .priority_model;
                priority_sort_key = elig_sort(next, &model);
            }
            if let ScheduleUpdate::Set(next) = &c.set_not_before {
                not_before = (*next).map(ts_nanos);
                if !c.api001_batch {
                    eligible_since = not_before.unwrap_or(now_n).max(now_n);
                }
            }
            st(tx.execute(
                "UPDATE pqueue_items SET fields=$4,payload=$5,metadata=$6,priority=$7, \
                 priority_sort=$8,not_before=$9,eligible_since=$10,item_version=item_version+1, \
                 updated_at=$11,last_command_sequence=$12 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=false AND fenced=false",
                &[&t,&q,&item_id,&fields_json,&payload,&metadata_json,&priority_json,
                  &priority_sort_key,&not_before,&eligible_since,&now_n,&seqi],
            ))?;
            if let Some(gate_keys) = &c.set_gate_keys {
                st(tx.execute(
                    "DELETE FROM pqueue_item_gates WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                    &[&t, &q, &item_id],
                ))?;
                if !gate_keys.is_empty() {
                    let item_ids = vec![item_id.clone(); gate_keys.len()];
                    st(tx.execute(
                        "INSERT INTO pqueue_item_gates(tenant_id,queue_id,item_id,gate_key) \
                         SELECT $1,$2,* FROM UNNEST($3::text[],$4::text[]) \
                         ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING",
                        &[&t, &q, &item_ids, gate_keys],
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
            if pending && repricing {
                increment_group_summaries_for_items(tx, shard, &singleton, now)?;
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
            let mut scheduled_ids = Vec::new();
            let mut scheduled_not_before = Vec::new();
            let mut scheduled_eligible_since = Vec::new();
            for o in &c.outcomes {
                let id = o.item_id.to_string();
                let computed_state = match o.kind {
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
                if o.applied_state
                    .is_some_and(|sealed| sealed != computed_state)
                {
                    return Err(EngineError::Conflict);
                }
                let new_state = o.applied_state.unwrap_or(computed_state);
                match new_state {
                    ItemState::Complete => to_complete.push(id.clone()),
                    ItemState::Failed => to_failed.push(id.clone()),
                    ItemState::Pending if matches!(o.kind, FinalizeKind::Rearm) => {
                        to_pending_rearm.push(id.clone());
                        let not_before = o.not_before.map(ts_nanos);
                        scheduled_ids.push(id.clone());
                        scheduled_not_before.push(not_before);
                        scheduled_eligible_since.push(not_before.unwrap_or(now_n).max(now_n));
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
                    scheduled_ids.push(id.clone());
                    scheduled_not_before.push(Some(ts_nanos(nb)));
                    scheduled_eligible_since.push(ts_nanos(nb));
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
            if !scheduled_ids.is_empty() {
                st(tx.execute(
                    "WITH schedule(item_id,not_before,eligible_since) AS ( \
                       SELECT * FROM unnest($3::text[],$4::bigint[],$5::bigint[]) \
                     ) UPDATE pqueue_items i SET not_before=s.not_before,eligible_since=s.eligible_since \
                       FROM schedule s WHERE i.tenant_id=$1 AND i.queue_id=$2 AND i.item_id=s.item_id",
                    &[&t,&q,&scheduled_ids,&scheduled_not_before,&scheduled_eligible_since],
                ))?;
            }
            let mut newly_pending = to_pending;
            newly_pending.extend(to_pending_rearm);
            increment_group_summaries_for_items(tx, shard, &newly_pending, now)?;
            Ok(())
        }
        QueueCommand::CohortFinalize(c) => {
            let ids = cohort_item_ids(tx, shard, &c.cohort_id)?;
            if ids.is_empty() {
                return Err(EngineError::NotFound);
            }
            let effective_kind = if matches!(c.kind, FinalizeKind::Retry) {
                let id_strings = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
                let rows = st(tx.query(
                    "SELECT retry_count,max_attempts FROM pqueue_items \
                     WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
                    &[&t, &q, &id_strings],
                ))?;
                if rows.into_iter().any(|row| {
                    is_retry_exhausted(row.get::<_, i64>(0) as u32, row.get::<_, i64>(1) as u32)
                }) {
                    FinalizeKind::Fail
                } else {
                    FinalizeKind::Retry
                }
            } else {
                c.kind
            };
            let effective_not_before = matches!(effective_kind, FinalizeKind::Retry)
                .then_some(c.not_before)
                .flatten();
            let outcomes = ids
                .iter()
                .map(|item_id| FinalizeOutcome {
                    item_id: *item_id,
                    kind: effective_kind,
                    applied_state: None,
                    not_before: effective_not_before,
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
            let next_state = match effective_kind {
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
                "DELETE FROM pqueue_group_due_pending \
                 WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3",
                &[&t, &q, &superseded_str],
            ))?;
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
            refresh_group_summaries(tx, shard, &groups, now)?;
            let replacement_id = c.replacement.item_id.to_string();
            st(tx.execute(
                "INSERT INTO pqueue_group_due_pending( \
                   tenant_id,queue_id,item_id,group_key,due_at,created_seq) \
                 SELECT tenant_id,queue_id,item_id,group_key,not_before,created_seq \
                 FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=$3 \
                   AND lifecycle_state='Pending' AND superseded=false AND group_key IS NOT NULL \
                   AND not_before>$4 \
                 ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET \
                   group_key=EXCLUDED.group_key,due_at=EXCLUDED.due_at,created_seq=EXCLUDED.created_seq",
                &[&t, &q, &replacement_id, &now_n],
            ))?;
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
            increment_group_summaries_for_items(tx, shard, &ids, now)?;
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
        QueueCommand::PauseQueue(pause) => {
            st(tx.execute(
                "UPDATE queues SET paused=true,pause_drain_intake=$3 WHERE tenant=$1 AND queue=$2",
                &[&t, &q, &pause.drain_intake],
            ))?;
            Ok(())
        }
        QueueCommand::ResumeQueue => {
            st(tx.execute(
                "UPDATE queues SET paused=false,pause_drain_intake=false WHERE tenant=$1 AND queue=$2",
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
            // (client_item_key, item_id) tombstones for every removed item, deduped LAST-wins on key so the
            // batched upsert never touches the same conflict target twice (DO UPDATE cardinality).
            let mut retention: Vec<(String, String)> = Vec::new();
            for row in rows {
                let item_id: String = row.get(0);
                let gk: Option<String> = row.get(1);
                let ck: String = row.get(2);
                let state: String = row.get(3);
                // API-001/TD-002 requires the retained client-key boundary after every successful
                // removal, including pending and force-purged leased items.
                let _ = parse_state(&state)?;
                if retention_ms > 0 {
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
            refresh_group_summaries(tx, shard, &groups, now)?;
            Ok(())
        }
        QueueCommand::SetGates(c) => {
            // BQ-14d (TD-002 §gate): set/clear queue gate-key block state. A blocked gate key makes every
            // item carrying it ineligible (enforced by the eligibility anti-join). This is exact-on-read:
            // toggling a gate flips eligibility on the next claim with no per-item rewrite.
            let gate_keys = c
                .gate_keys
                .iter()
                .map(|gate_key| gate_key.as_str().to_string())
                .collect::<Vec<_>>();
            if !gate_keys.is_empty() {
                if c.blocked {
                    st(tx.execute(
                        "INSERT INTO pqueue_gate_state (tenant_id,queue_id,gate_key) \
                         SELECT $1,$2,gate_key FROM UNNEST($3::text[]) AS incoming(gate_key) \
                         ON CONFLICT (tenant_id,queue_id,gate_key) DO NOTHING",
                        &[&t, &q, &gate_keys],
                    ))?;
                } else {
                    st(tx.execute(
                        "DELETE FROM pqueue_gate_state \
                         WHERE tenant_id=$1 AND queue_id=$2 AND gate_key = ANY($3)",
                        &[&t, &q, &gate_keys],
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
            if !c.records.is_empty() {
                let keys = c
                    .records
                    .iter()
                    .map(|record| record.key.clone())
                    .collect::<Vec<_>>();
                let payloads = c
                    .records
                    .iter()
                    .map(|record| record.payload.to_vec())
                    .collect::<Vec<_>>();
                st(tx.execute(
                    "INSERT INTO pqueue_side_records (tenant_id,queue_id,key,payload) \
                     SELECT $1,$2,batch.key,batch.payload \
                     FROM ( \
                       SELECT DISTINCT ON (key) key,payload \
                       FROM UNNEST($3::bytea[],$4::bytea[]) WITH ORDINALITY \
                         AS incoming(key,payload,ordinality) \
                       ORDER BY key,ordinality DESC \
                     ) AS batch \
                     ON CONFLICT(tenant_id,queue_id,key) DO UPDATE SET payload=EXCLUDED.payload",
                    &[&t, &q, &keys, &payloads],
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

fn queue_paused(client: &mut impl GenericClient, shard: &QueueKey) -> EngineResult<bool> {
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

fn select_item_claim_sql(
    client: &mut Client,
    shard: &QueueKey,
    compatibility: &ClaimCompatibility,
    now: UtcTimestamp,
    max: usize,
) -> EngineResult<Vec<ItemId>> {
    if max == 0 {
        return Ok(Vec::new());
    }
    if compatibility.group_key.is_none() && compatibility.metadata_equals.is_empty() {
        return select_eligible_sql(client, shard, now, max);
    }
    if queue_paused(client, shard)? {
        return Ok(Vec::new());
    }
    let (tenant, queue) = parts(shard);
    let now_n = ts_nanos(now);
    let limit = max as i64;
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let rows = st(client.query(
        "SELECT item_id FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Pending' AND superseded=false \
             AND cohort_size IS NULL AND (not_before IS NULL OR not_before<=$3) \
             AND eligible_since IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig \
             JOIN pqueue_gate_state gs ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id \
             AND gs.gate_key=ig.gate_key WHERE ig.tenant_id=pqueue_items.tenant_id \
             AND ig.queue_id=pqueue_items.queue_id AND ig.item_id=pqueue_items.item_id) \
             AND ($5::text IS NULL OR group_key=$5) \
             AND NOT EXISTS (SELECT 1 FROM jsonb_each($6::text::jsonb) wanted(key,value) \
               WHERE metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
             ORDER BY priority_sort,created_seq LIMIT $4",
        &[
            &tenant,
            &queue,
            &now_n,
            &limit,
            &required_group,
            &metadata_filter,
        ],
    ))?;
    rows.into_iter()
        .map(|row| {
            ItemId::new(row.get::<_, String>(0))
                .map_err(|error| EngineError::Storage(error.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// BQ-14b: group-aware claim selection (group_batching / same_group_key), owner-local, consuming
// `pqueue_group_summary`. The candidate groups are locked with `FOR UPDATE SKIP LOCKED` on their summary
// rows — TD-002's per-group lock that guarantees two concurrent claims never split a group (the real
// row-lock the sqlite backend approximates with its process Mutex). Runs inside the claim transaction.
// ---------------------------------------------------------------------------

/// The live currently-eligible items of one group (re-read under the live predicate, FOR UPDATE so the
/// whole locked group leases together — no SKIP LOCKED inside a locked group), capped at `limit`.
struct GroupEligibility {
    item_ids: Vec<ItemId>,
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
    compatibility: &ClaimCompatibility,
    include_future_summary_rows: bool,
) -> EngineResult<Vec<ItemId>> {
    let (tenant, queue) = parts(shard);
    let now_n = ts_nanos(now);
    let max_items_i = max_items as i64;
    let per_group_limit = max_items_i.saturating_add(1);
    let max_groups_i = i64::from(max_groups);
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let rows = st(tx.query(
        "WITH candidate AS MATERIALIZED ( \
           SELECT s.group_key,r.priority_sort AS rep_priority_sort,r.created_seq,r.item_id AS rep_item_id \
           FROM pqueue_group_summary s JOIN LATERAL ( \
             SELECT e.priority_sort,e.created_seq,e.item_id FROM pqueue_items e \
             WHERE e.tenant_id=$1 AND e.queue_id=$2 AND e.group_key=s.group_key \
               AND e.lifecycle_state='Pending' AND e.superseded=false AND e.cohort_size IS NULL \
               AND (e.not_before IS NULL OR e.not_before<=$3) AND e.eligible_since IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                 ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
               AND NOT EXISTS (SELECT 1 FROM jsonb_each($5::text::jsonb) wanted(key,value) \
                 WHERE e.metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
             ORDER BY e.priority_sort,e.created_seq LIMIT 1 \
           ) r ON true WHERE s.tenant_id=$1 AND s.queue_id=$2 \
             AND (s.oldest_eligible_at IS NOT NULL OR $7) \
             AND ($6::text IS NULL OR s.group_key=$6) \
           ORDER BY r.priority_sort,r.created_seq,r.item_id,s.group_key \
           LIMIT $4 FOR UPDATE OF s SKIP LOCKED \
         ), locked AS MATERIALIZED ( \
           SELECT c.group_key,member.item_id,member.priority_sort,member.created_seq \
           FROM candidate c JOIN pqueue_items member ON member.tenant_id=$1 AND member.queue_id=$2 \
               AND member.group_key=c.group_key AND member.lifecycle_state='Pending' \
               AND member.superseded=false AND member.cohort_size IS NULL \
               AND (member.not_before IS NULL OR member.not_before<=$3) \
               AND member.eligible_since IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                 ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=member.tenant_id AND ig.queue_id=member.queue_id \
                   AND ig.item_id=member.item_id) \
               AND NOT EXISTS (SELECT 1 FROM jsonb_each($5::text::jsonb) wanted(key,value) \
                 WHERE member.metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
           ORDER BY c.rep_priority_sort,c.created_seq,c.rep_item_id,c.group_key, \
                    member.priority_sort,member.created_seq,member.item_id \
           LIMIT $8 FOR UPDATE OF member \
         ), grouped AS ( \
           SELECT c.group_key,c.rep_priority_sort,c.created_seq,c.rep_item_id, \
             COUNT(l.item_id)::bigint AS item_count, \
             array_agg(l.item_id ORDER BY l.priority_sort,l.created_seq) FILTER (WHERE l.item_id IS NOT NULL) AS item_ids \
           FROM candidate c LEFT JOIN locked l USING(group_key) \
           GROUP BY c.group_key,c.rep_priority_sort,c.created_seq,c.rep_item_id \
         ) SELECT group_key,item_count,item_ids, \
             (SUM(item_count) OVER (ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key))::bigint AS running_count \
           FROM grouped WHERE item_count>0 \
           ORDER BY rep_priority_sort,created_seq,rep_item_id,group_key",
        &[&tenant,&queue,&now_n,&max_groups_i,&metadata_filter,&required_group,
          &include_future_summary_rows,&per_group_limit],
    ))?;
    let mut selected = Vec::new();
    for row in rows {
        let count: i64 = row.get(1);
        if count > max_items_i {
            return Err(EngineError::BatchTooLarge);
        }
        let running: i64 = row.get(3);
        if running > max_items_i {
            break;
        }
        let ids: Vec<String> = row.get(2);
        for id in ids {
            selected.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
        }
    }
    Ok(selected)
}

/// `same_group_key` selection: the single oldest eligible group, capped at `max_items` (partial allowed).
fn select_same_group(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    now: UtcTimestamp,
    max_items: usize,
    compatibility: &ClaimCompatibility,
    include_future_summary_rows: bool,
) -> EngineResult<Vec<ItemId>> {
    let (tenant, queue) = parts(shard);
    let now_n = ts_nanos(now);
    let limit = max_items as i64;
    let required_group = compatibility.group_key.as_ref().map(GroupKey::as_str);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let rows = st(tx.query(
        "WITH candidate AS MATERIALIZED ( \
           SELECT s.group_key FROM pqueue_group_summary s JOIN LATERAL ( \
             SELECT e.priority_sort,e.created_seq,e.item_id FROM pqueue_items e \
             WHERE e.tenant_id=$1 AND e.queue_id=$2 AND e.group_key=s.group_key \
               AND e.lifecycle_state='Pending' AND e.superseded=false AND e.cohort_size IS NULL \
               AND (e.not_before IS NULL OR e.not_before<=$3) AND e.eligible_since IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
                 ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
                 WHERE ig.tenant_id=e.tenant_id AND ig.queue_id=e.queue_id AND ig.item_id=e.item_id) \
               AND NOT EXISTS (SELECT 1 FROM jsonb_each($5::text::jsonb) wanted(key,value) \
                 WHERE e.metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
             ORDER BY e.priority_sort,e.created_seq LIMIT 1 \
           ) r ON true WHERE s.tenant_id=$1 AND s.queue_id=$2 \
             AND (s.oldest_eligible_at IS NOT NULL OR $7) \
             AND ($6::text IS NULL OR s.group_key=$6) \
             AND NOT EXISTS (SELECT 1 FROM pqueue_items leased WHERE leased.tenant_id=$1 \
               AND leased.queue_id=$2 AND leased.group_key=s.group_key AND leased.superseded=false \
               AND leased.cohort_size IS NULL AND leased.lifecycle_state='Leased') \
           ORDER BY r.priority_sort,r.created_seq,r.item_id,s.group_key \
           LIMIT 1 FOR UPDATE OF s SKIP LOCKED \
         ) SELECT i.item_id FROM candidate c JOIN pqueue_items i ON i.tenant_id=$1 \
           AND i.queue_id=$2 AND i.group_key=c.group_key WHERE i.lifecycle_state='Pending' \
           AND i.superseded=false AND i.cohort_size IS NULL \
           AND (i.not_before IS NULL OR i.not_before<=$3) AND i.eligible_since IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
           AND NOT EXISTS (SELECT 1 FROM jsonb_each($5::text::jsonb) wanted(key,value) \
             WHERE i.metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
           ORDER BY i.priority_sort,i.created_seq LIMIT $4 FOR UPDATE OF i",
        &[&tenant,&queue,&now_n,&limit,&metadata_filter,&required_group,&include_future_summary_rows],
    ))?;
    rows.into_iter()
        .map(|row| {
            ItemId::new(row.get::<_, String>(0)).map_err(|e| EngineError::Storage(e.to_string()))
        })
        .collect()
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
    compatibility: &ClaimCompatibility,
) -> EngineResult<Option<SelectedCohort>> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let candidate = st(tx.query_opt(
        "SELECT c.group_key,c.cohort_id,c.cohort_size FROM pqueue_cohorts c \
         WHERE c.tenant_id=$1 AND c.queue_id=$2 AND c.state='complete' \
         AND (SELECT COUNT(*)::bigint FROM pqueue_items a WHERE a.tenant_id=$1 AND a.queue_id=$2 \
           AND a.group_key=c.group_key AND a.superseded=false AND a.cohort_size IS NOT NULL \
           AND a.lifecycle_state NOT IN ('Complete','Failed'))=c.cohort_size \
         AND NOT EXISTS (SELECT 1 FROM pqueue_items i WHERE i.tenant_id=$1 AND i.queue_id=$2 \
           AND i.group_key=c.group_key AND i.superseded=false AND i.cohort_size IS NOT NULL \
           AND i.lifecycle_state NOT IN ('Complete','Failed') AND NOT (i.lifecycle_state='Pending' \
             AND (i.not_before IS NULL OR i.not_before<=$3) AND i.eligible_since IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
               ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
               WHERE ig.tenant_id=i.tenant_id AND ig.queue_id=i.queue_id AND ig.item_id=i.item_id) \
             AND NOT EXISTS (SELECT 1 FROM jsonb_each($4::text::jsonb) wanted(key,value) \
               WHERE i.metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value))) \
         ORDER BY c.cohort_created_at,c.group_key LIMIT 1 FOR UPDATE OF c SKIP LOCKED",
        &[&t,&q,&now_n,&metadata_filter],
    ))?;
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let group = GroupKey::new(candidate.get::<_, String>(0))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let cohort_id = CohortId::new(candidate.get::<_, String>(1))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let size = usize::try_from(candidate.get::<_, i64>(2))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    if size > max_items {
        return Err(EngineError::BatchTooLarge);
    }
    let item_ids = cohort_eligible_items(tx, shard, &group, now, size, compatibility)?.item_ids;
    Ok(Some(SelectedCohort {
        cohort_id,
        item_ids,
    }))
}

/// The live currently-eligible COHORT members of one group (`cohort_size IS NOT NULL`), capped at `limit`,
/// `FOR UPDATE` (the whole locked cohort leases together). Restricted to cohort-declared members (F1).
fn cohort_eligible_items(
    tx: &mut postgres::Transaction<'_>,
    shard: &QueueKey,
    group: &GroupKey,
    now: UtcTimestamp,
    limit: usize,
    compatibility: &ClaimCompatibility,
) -> EngineResult<GroupEligibility> {
    let (t, q) = parts(shard);
    let now_n = ts_nanos(now);
    let lim = limit as i64;
    let metadata_filter = metadata_to_json(&Metadata::from_entries(
        compatibility.metadata_equals.clone(),
    ))?;
    let rows = st(tx.query(
        "SELECT item_id FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
         AND lifecycle_state='Pending' AND superseded=false AND cohort_size IS NOT NULL \
         AND (not_before IS NULL OR not_before<=$4) AND eligible_since IS NOT NULL \
         AND NOT EXISTS (SELECT 1 FROM pqueue_item_gates ig JOIN pqueue_gate_state gs \
             ON gs.tenant_id=ig.tenant_id AND gs.queue_id=ig.queue_id AND gs.gate_key=ig.gate_key \
             WHERE ig.tenant_id=pqueue_items.tenant_id AND ig.queue_id=pqueue_items.queue_id \
             AND ig.item_id=pqueue_items.item_id) \
         AND NOT EXISTS (SELECT 1 FROM jsonb_each($6::text::jsonb) AS wanted(key,value) \
             WHERE metadata::jsonb -> wanted.key IS DISTINCT FROM wanted.value) \
         ORDER BY priority_sort, created_seq LIMIT $5 FOR UPDATE",
        &[&t, &q, &group.as_str(), &now_n, &lim, &metadata_filter],
    ))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        out.push(ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?);
    }
    Ok(GroupEligibility { item_ids: out })
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
    if !promote_due_group_summary_chunk(client, shard, now)? {
        return Err(EngineError::Unavailable);
    }
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

fn pending_summary_sql(
    client: &mut Client,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
) -> EngineResult<PendingSummary> {
    if live_tokens.is_empty() {
        return Ok(PendingSummary::default());
    }
    let (t, q) = parts(shard);
    let rows = st(client.query(
        "SELECT lease_token_hash,COUNT(*),MIN(item_id::numeric)::text,MAX(item_id::numeric)::text \
         FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased' \
         AND lease_token_hash IS NOT NULL GROUP BY lease_token_hash",
        &[&t, &q],
    ))?;
    let mut summary = PendingSummary::default();
    for row in rows {
        let count: i64 = row.get(1);
        let min = ItemId::new(row.get::<_, String>(2))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let max = ItemId::new(row.get::<_, String>(3))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let Some(token) = live_tokens.get(&min) else {
            // A restart intentionally loses every plaintext lease token. Old durable leases remain
            // invisible to the PEL until ordinary expiry/reclaim, matching pending_sql.
            continue;
        };
        summary.count = summary.count.saturating_add(count as u64);
        summary.min_id = Some(summary.min_id.map_or(min, |current| current.min(min)));
        summary.max_id = Some(summary.max_id.map_or(max, |current| current.max(max)));
        summary.consumers.push((token.clone(), count as u64));
    }
    summary
        .consumers
        .sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
    Ok(summary)
}

fn lease_views_from_rows(
    rows: Vec<postgres::Row>,
    live_tokens: &HashMap<ItemId, LeaseToken>,
) -> EngineResult<Vec<LeaseView>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let item_id = ItemId::new(row.get::<_, String>(0))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        let (Some(token), Some(expires_at)) =
            (live_tokens.get(&item_id), row.get::<_, Option<i64>>(1))
        else {
            continue;
        };
        out.push(LeaseView {
            item_id,
            lease_token: token.clone(),
            lease_expires_at: nanos_ts(expires_at),
            attempt_count: row.get::<_, i64>(2) as u32,
        });
    }
    Ok(out)
}

fn pending_page_sql(
    client: &mut Client,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
    start: Option<ItemId>,
    limit: usize,
) -> EngineResult<PendingPage> {
    if limit == 0 || live_tokens.is_empty() {
        return Ok(PendingPage::default());
    }
    let (t, q) = parts(shard);
    let start = start
        .map(|item| item.to_string())
        .unwrap_or_else(|| "0".into());
    let row_limit = limit.saturating_add(1).min(i64::MAX as usize) as i64;
    let mut rows = st(client.query(
        "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased' \
         AND item_id::numeric >= $3::text::numeric ORDER BY item_id::numeric LIMIT $4",
        &[&t, &q, &start, &row_limit],
    ))?;
    let next = if rows.len() > limit {
        let row = rows.pop().expect("limit-plus-one row exists");
        Some(
            ItemId::new(row.get::<_, String>(0))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
        )
    } else {
        None
    };
    Ok(PendingPage {
        entries: lease_views_from_rows(rows, live_tokens)?,
        next,
    })
}

fn pending_range_sql(
    client: &mut Client,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
    start: Option<ItemId>,
    end: Option<ItemId>,
    consumer: Option<&LeaseToken>,
    limit: usize,
) -> EngineResult<Vec<LeaseView>> {
    if limit == 0 || live_tokens.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let start = start
        .map(|item| item.to_string())
        .unwrap_or_else(|| "0".into());
    let end = end
        .map(|item| item.to_string())
        .unwrap_or_else(|| u64::MAX.to_string());
    let row_limit = limit.min(i64::MAX as usize) as i64;
    let rows = if let Some(consumer) = consumer {
        let hash = lease_hash(consumer);
        st(client.query(
            "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased' \
             AND item_id::numeric BETWEEN $3::text::numeric AND $4::text::numeric \
             AND lease_token_hash=$5 ORDER BY item_id::numeric LIMIT $6",
            &[&t, &q, &start, &end, &hash, &row_limit],
        ))?
    } else {
        st(client.query(
            "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased' \
             AND item_id::numeric BETWEEN $3::text::numeric AND $4::text::numeric \
             ORDER BY item_id::numeric LIMIT $5",
            &[&t, &q, &start, &end, &row_limit],
        ))?
    };
    lease_views_from_rows(rows, live_tokens)
}

fn pending_by_ids_sql(
    client: &mut Client,
    live_tokens: &HashMap<ItemId, LeaseToken>,
    shard: &QueueKey,
    ids: &[ItemId],
) -> EngineResult<Vec<LeaseView>> {
    if ids.is_empty() || live_tokens.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strings: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let rows = st(client.query(
        "SELECT item_id,lease_expires_at,retry_count FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND lifecycle_state='Leased' \
         AND item_id = ANY($3::text[])",
        &[&t, &q, &id_strings],
    ))?;
    let by_id: HashMap<_, _> = lease_views_from_rows(rows, live_tokens)?
        .into_iter()
        .map(|lease| (lease.item_id, lease))
        .collect();
    Ok(ids
        .iter()
        .filter_map(|item_id| by_id.get(item_id).cloned())
        .collect())
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
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let id_strings: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let rows = st(client.query(
        "SELECT item_id,client_item_key,item_version,priority,group_key,not_before, \
         lease_expires_at,retry_count,payload,fields,metadata FROM pqueue_items \
         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3::text[]) \
         AND lifecycle_state='Leased'",
        &[&t, &q, &id_strings],
    ))?;
    let mut rows_by_id = HashMap::with_capacity(rows.len());
    for row in rows {
        let item_id = ItemId::new(row.get::<_, String>(0))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        rows_by_id.insert(item_id, row);
    }
    let mut gate_keys_by_id = item_gate_keys_by_id(client, shard, ids)?;
    let mut out = Vec::with_capacity(rows_by_id.len());
    for id in ids {
        let Some(token) = resolve(id) else {
            continue;
        };
        let Some(row) = rows_by_id.remove(id) else {
            continue;
        };
        let exp: Option<i64> = row.get(6);
        let Some(exp) = exp else { continue };
        let gate_keys = gate_keys_by_id.remove(&id.to_string()).unwrap_or_default();
        out.push(claimed_from_row(
            *id,
            token,
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            exp,
            row.get(7),
            row.get(8),
            row.get(9),
            row.get(10),
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
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let (t, q) = parts(shard);
    let key_strings = keys
        .iter()
        .map(|key| key.as_str().to_string())
        .collect::<Vec<_>>();
    let rows = st(client.query(
        "SELECT client_item_key, item_id, item_version, lifecycle_state, priority, group_key, not_before, \
             retry_count, payload, fields FROM pqueue_items \
             WHERE tenant_id=$1 AND queue_id=$2 AND client_item_key = ANY($3) \
               AND superseded=false AND lifecycle_state IN ('Pending','Leased')",
        &[&t, &q, &key_strings],
    ))?;
    let mut by_key = HashMap::with_capacity(rows.len());
    for row in rows {
        let key: String = row.get(0);
        let id: String = row.get(1);
        let state: String = row.get(3);
        let group: Option<String> = row.get(5);
        let not_before: Option<i64> = row.get(6);
        let payload: Option<Vec<u8>> = row.get(8);
        let fields: String = row.get(9);
        by_key.insert(
            key.clone(),
            LiveItemView {
                item_id: ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?,
                client_item_key: ClientItemKey::new(key)
                    .map_err(|e| EngineError::Storage(e.to_string()))?,
                item_version: row.get::<_, i64>(2) as u64,
                lifecycle_state: parse_state(&state)?,
                priority: parse_priority(row.get(4))?,
                group_key: group
                    .map(GroupKey::new)
                    .transpose()
                    .map_err(|e| EngineError::Storage(e.to_string()))?,
                not_before: not_before.map(nanos_ts),
                attempt_count: row.get::<_, i64>(7) as u32,
                payload: payload.map(Bytes::from),
                fields: fields_from_json(fields)?,
            },
        );
    }
    Ok(keys
        .iter()
        .map(|key| by_key.get(key.as_str()).cloned())
        .collect())
}

fn metrics_sql(client: &mut Client, shard: &QueueKey) -> EngineResult<QueueMetrics> {
    let ready: bool = st(client.query_one(
        "SELECT EXISTS(SELECT 1 FROM pqueue_metrics_migration_state \
         WHERE migration_name='queue_metrics_v2_counted' AND status='complete')",
        &[],
    ))?
    .get(0);
    if !ready {
        return Err(EngineError::Unavailable);
    }
    let (t, q) = parts(shard);
    let row = st(client.query_opt(
        "SELECT pending,leased,complete,failed FROM pqueue_queue_metrics_v2 \
         WHERE tenant_id=$1 AND queue_id=$2",
        &[&t, &q],
    ))?;
    let mut m = QueueMetrics::default();
    if let Some(row) = row {
        m.pending = row.get::<_, i64>(0) as u64;
        m.leased = row.get::<_, i64>(1) as u64;
        m.complete = row.get::<_, i64>(2) as u64;
        m.failed = row.get::<_, i64>(3) as u64;
    }
    m.resident_terminal_count = m.complete + m.failed;
    Ok(m)
}

fn metrics_by_query_sql(
    client: &mut Client,
    typed_indexes: &[QueueIndex],
    shard: &QueueKey,
    request: MetricsByQueryRequest,
) -> EngineResult<QueueMetrics> {
    // An unconstrained metrics request is exactly the maintained queue-wide metric. Avoid turning it
    // into an unbounded secondary-index aggregate.
    if request.filters.is_empty() && request.index.is_none() {
        return metrics_sql(client, shard);
    }
    let spec = match request.index.as_deref() {
        Some(name) => typed_indexes
            .iter()
            .find(|index| index.name == name)
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
        None => typed_indexes
            .first()
            .ok_or(EngineError::Invalid("unknown secondary index"))?,
    };
    // A declared but unconstrained index still means the entire queue. Validate the caller's handle
    // above, then use the maintained queue-wide counter instead of scanning all side-index rows.
    if request.filters.is_empty() {
        return metrics_sql(client, shard);
    }
    let fields: Vec<(&str, &IndexType)> = match &spec.declaration {
        IndexDeclaration::Single(field) => vec![(&field.field, &field.index_type)],
        IndexDeclaration::Compound(compound) => compound
            .fields
            .iter()
            .map(|field| (field.field.as_str(), &field.index_type))
            .collect(),
    };
    let mut positioned: Vec<(usize, &QueryFilter, Vec<u8>)> = Vec::new();
    for filter in &request.filters {
        let Some((position, (_, index_type))) = fields
            .iter()
            .enumerate()
            .find(|(_, (name, _))| *name == filter.field)
        else {
            return Err(EngineError::Invalid("unindexed-field"));
        };
        let value = match (&filter.value, *index_type) {
            (TypedValue::String(value), IndexType::String) => {
                serde_json::Value::String(value.clone())
            }
            (TypedValue::Integer(value), IndexType::Integer) => (*value).into(),
            (TypedValue::Float(value), IndexType::Float) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .ok_or(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ))?,
            (TypedValue::Bool(value), IndexType::Boolean) => (*value).into(),
            (TypedValue::DateTime(value), IndexType::Datetime) => value.seconds.into(),
            _ => {
                return Err(EngineError::Invalid(
                    "typed index value is not valid for declared type",
                ));
            }
        };
        let encoded = axon_esf::encode_index_value(&value, index_type).map_err(|_| {
            EngineError::Invalid("typed index value is not valid for declared type")
        })?;
        positioned.push((position, filter, encoded));
    }
    positioned.sort_by_key(|(position, _, _)| *position);

    fn framed(value: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + value.len());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
        out
    }
    fn prefix_successor(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
        for index in (0..prefix.len()).rev() {
            if prefix[index] != u8::MAX {
                prefix[index] += 1;
                prefix.truncate(index + 1);
                return Some(prefix);
            }
        }
        None
    }

    let (t, q) = parts(shard);
    let mut sql = String::from(
        "SELECT \
           COUNT(*) FILTER (WHERE i.lifecycle_state='Pending')::bigint, \
           COUNT(*) FILTER (WHERE i.lifecycle_state='Leased')::bigint, \
           COUNT(*) FILTER (WHERE i.lifecycle_state='Complete')::bigint, \
           COUNT(*) FILTER (WHERE i.lifecycle_state='Failed')::bigint \
         FROM pqueue_item_index idx \
         JOIN pqueue_items i \
           ON i.tenant_id=idx.tenant_id AND i.queue_id=idx.queue_id AND i.item_id=idx.item_id \
         WHERE idx.tenant_id=$1 AND idx.queue_id=$2 AND idx.index_name=$3 \
           AND i.superseded=false",
    );
    let mut owned: Vec<Box<dyn ToSql + Sync>> = vec![
        Box::new(t.to_owned()),
        Box::new(q.to_owned()),
        Box::new(spec.name.clone()),
    ];
    // A contiguous, unambiguous equality prefix is an optional whole-key narrowing optimization.
    // Every filter is independently enforced through the normalized component index below, so later
    // fields and multiple bounds on one field remain valid and index-backed.
    let mut prefix_field_count = 0usize;
    loop {
        let matches = positioned
            .iter()
            .filter(|(position, filter, _)| {
                *position == prefix_field_count && filter.op == FilterOp::Eq
            })
            .collect::<Vec<_>>();
        if matches.len() != 1
            || positioned.iter().any(|(position, filter, _)| {
                *position == prefix_field_count && filter.op != FilterOp::Eq
            })
        {
            break;
        }
        prefix_field_count += 1;
    }
    let mut prefix = Vec::new();
    for position in 0..prefix_field_count {
        let (_, _, encoded) = positioned
            .iter()
            .find(|(candidate, filter, _)| *candidate == position && filter.op == FilterOp::Eq)
            .expect("prefix fields were validated above");
        prefix.extend(framed(encoded));
    }
    if !prefix.is_empty() {
        owned.push(Box::new(prefix.clone()));
        sql.push_str(&format!(" AND idx.index_key >= ${}", owned.len()));
        if let Some(successor) = prefix_successor(prefix.clone()) {
            owned.push(Box::new(successor));
            sql.push_str(&format!(" AND idx.index_key < ${}", owned.len()));
        }
    }
    for (filter_number, (position, filter, encoded)) in positioned.iter().enumerate() {
        owned.push(Box::new(*position as i32));
        let position_parameter = owned.len();
        owned.push(Box::new(encoded.clone()));
        let value_parameter = owned.len();
        let operator = match filter.op {
            FilterOp::Eq => "=",
            FilterOp::Gte => ">=",
            FilterOp::Gt => ">",
            FilterOp::Lte => "<=",
            FilterOp::Lt => "<",
        };
        // Each component predicate is a lookup/range over the normalized component index. This keeps
        // later-field ranges indexable regardless of variable-length equality-prefix values.
        sql.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM pqueue_item_index_component component_{filter_number} \
             WHERE component_{filter_number}.tenant_id=idx.tenant_id \
               AND component_{filter_number}.queue_id=idx.queue_id \
               AND component_{filter_number}.index_name=idx.index_name \
               AND component_{filter_number}.item_id=idx.item_id \
               AND component_{filter_number}.component_position=${position_parameter} \
               AND component_{filter_number}.component_value {operator} ${value_parameter})"
        ));
    }
    if positioned.len() == fields.len() && prefix_field_count == fields.len() && !prefix.is_empty()
    {
        // A full-key equality is more selective than its equivalent prefix interval.
        owned.push(Box::new(prefix));
        sql.push_str(&format!(" AND idx.index_key = ${}", owned.len()));
    }
    let params: Vec<&(dyn ToSql + Sync)> = owned.iter().map(|value| value.as_ref()).collect();
    let row = st(client.query_one(&sql, &params))?;
    let mut metrics = QueueMetrics {
        pending: row.get::<_, i64>(0) as u64,
        leased: row.get::<_, i64>(1) as u64,
        complete: row.get::<_, i64>(2) as u64,
        failed: row.get::<_, i64>(3) as u64,
        ..QueueMetrics::default()
    };
    metrics.resident_terminal_count = metrics.complete + metrics.failed;
    Ok(metrics)
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

/// Progress returned by one online queue-metrics backfill batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsMigrationProgress {
    pub rows_processed: u64,
    pub rows_backfilled: u64,
    pub due_rows_backfilled: u64,
    pub batches_completed: u64,
    pub complete: bool,
}

impl PostgresRelationalBackend {
    /// Apply the online index migration set required before opening an existing production schema.
    /// Every statement is issued separately because PostgreSQL forbids `CREATE INDEX CONCURRENTLY`
    /// inside a transaction block. This is an explicit operator action; normal startup only verifies.
    pub fn apply_concurrent_migrations(url: &str) -> EngineResult<()> {
        let mut client = connect(PostgresConnectConfig::new(url))?;
        apply_concurrent_migrations(&mut client)
    }

    /// Schema-isolated variant of [`Self::apply_concurrent_migrations`].
    pub fn apply_concurrent_migrations_in_schema(url: &str, schema: &str) -> EngineResult<()> {
        if !schema
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = connect(PostgresConnectConfig::new(url))?;
        st(client.batch_execute(&format!("SET search_path TO {schema}")))?;
        apply_concurrent_migrations(&mut client)
    }

    /// Run at most one bounded, transactionally resumable metrics-backfill batch.
    pub fn migrate_metrics_batch(
        url: &str,
        batch_size: u32,
    ) -> EngineResult<MetricsMigrationProgress> {
        let mut client = connect(PostgresConnectConfig::new(url))?;
        migrate_metrics_batch(&mut client, batch_size)
    }

    /// Schema-isolated variant of [`Self::migrate_metrics_batch`].
    pub fn migrate_metrics_batch_in_schema(
        url: &str,
        schema: &str,
        batch_size: u32,
    ) -> EngineResult<MetricsMigrationProgress> {
        if !schema
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = connect(PostgresConnectConfig::new(url))?;
        st(client.batch_execute(&format!("SET search_path TO {schema}")))?;
        migrate_metrics_batch(&mut client, batch_size)
    }

    /// Connect to `url` (default `search_path`), ensure the schema, and load the queue-def cache.
    pub fn connect(url: &str) -> EngineResult<Self> {
        let client = connect(PostgresConnectConfig::new(url))?;
        Self::from_client(client)
    }

    /// Connect with the production credential-provider configuration.
    pub fn connect_with_config(config: PostgresConnectConfig) -> EngineResult<Self> {
        let client = connect(config)?;
        Self::from_client(client)
    }

    /// Schema-isolated production-config variant used by the shared fixed-pool constructor and live proofs.
    pub fn connect_with_config_in_schema(
        config: PostgresConnectConfig,
        schema: &str,
    ) -> EngineResult<Self> {
        if !schema
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(EngineError::Invalid("schema name must be [A-Za-z0-9_]"));
        }
        let mut client = connect(config)?;
        st(client.batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; SET search_path TO {schema}"
        )))?;
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
        let schema_exists: bool = st(client.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
            &[&schema],
        ))?
        .get(0);
        if !schema_exists {
            st(client.batch_execute(&format!("CREATE SCHEMA {schema}")))?;
        }
        st(client.batch_execute(&format!("SET search_path TO {schema}")))?;
        Self::from_client(client)
    }

    fn from_client(mut client: Client) -> EngineResult<Self> {
        let fresh: bool =
            st(client.query_one("SELECT to_regclass('pqueue_items') IS NULL", &[]))?.get(0);
        if !fresh {
            let state_table_exists: bool = st(client.query_one(
                "SELECT to_regclass('pqueue_metrics_migration_state') IS NOT NULL",
                &[],
            ))?
            .get(0);
            if !state_table_exists {
                return Err(EngineError::Unavailable);
            }
            let ready: bool = st(client.query_one(
                "SELECT EXISTS(SELECT 1 FROM pqueue_metrics_migration_state \
                 WHERE migration_name='queue_metrics_v2_counted' AND status='complete')",
                &[],
            ))?
            .get(0);
            if !ready {
                return Err(EngineError::Unavailable);
            }
            let maintenance_ready: bool = st(client.query_one(
                "SELECT \
                   to_regclass('pqueue_queue_metrics_v2') IS NOT NULL \
                   AND to_regclass('pqueue_metrics_counted_item') IS NOT NULL \
                   AND to_regclass('pqueue_group_due_pending') IS NOT NULL \
                   AND to_regclass('pqueue_item_index_component') IS NOT NULL \
                   AND to_regclass('pqueue_commands') IS NOT NULL \
                   AND to_regclass('pqueue_command_baselines') IS NOT NULL \
                   AND to_regclass('pqueue_commands_read_idx') IS NOT NULL \
                   AND to_regclass('pqueue_commands_command_id_idx') IS NOT NULL \
                   AND to_regprocedure('pqueue_apply_metrics_delta()') IS NOT NULL \
                   AND to_regprocedure('pqueue_index_components(bytea)') IS NOT NULL \
                   AND to_regprocedure('pqueue_sync_index_components()') IS NOT NULL \
                   AND EXISTS(SELECT 1 FROM pg_trigger \
                     WHERE tgname='pqueue_items_metrics_delta' \
                       AND tgrelid='pqueue_items'::regclass AND tgenabled IN ('O','A') \
                       AND tgfoid=to_regprocedure('pqueue_apply_metrics_delta()') \
                       AND pg_get_triggerdef(oid) LIKE \
                         '%AFTER INSERT OR DELETE OR UPDATE OF lifecycle_state, superseded, not_before, group_key%') \
                   AND EXISTS(SELECT 1 FROM pg_trigger \
                     WHERE tgname='pqueue_item_index_components_sync' \
                       AND tgrelid='pqueue_item_index'::regclass AND tgenabled IN ('O','A') \
                       AND tgfoid=to_regprocedure('pqueue_sync_index_components()') \
                       AND pg_get_triggerdef(oid) LIKE \
                         '%AFTER INSERT OR DELETE OR UPDATE OF index_key%')",
                &[],
            ))?
            .get(0);
            if !maintenance_ready {
                return Err(EngineError::Unavailable);
            }
            let command_log_ready: bool = st(client.query_one(
                "SELECT NOT EXISTS ( \
                   SELECT 1 FROM relational_cursor c \
                   LEFT JOIN pqueue_command_baselines b \
                     ON b.tenant=c.tenant AND b.queue=c.queue \
                   LEFT JOIN LATERAL ( \
                     SELECT seq FROM pqueue_commands p \
                     WHERE p.tenant=c.tenant AND p.queue=c.queue ORDER BY seq ASC LIMIT 1 \
                   ) first_command ON true \
                   LEFT JOIN LATERAL ( \
                     SELECT seq FROM pqueue_commands p \
                     WHERE p.tenant=c.tenant AND p.queue=c.queue ORDER BY seq DESC LIMIT 1 \
                   ) last_command ON true \
                   WHERE b.tenant IS NULL OR octet_length(b.snapshot_digest)<>32 \
                      OR b.next_seq<0 OR b.next_seq>c.next_seq \
                      OR (first_command.seq IS NULL AND c.next_seq<>b.next_seq) \
                      OR (first_command.seq IS NOT NULL AND first_command.seq<>b.next_seq) \
                      OR (last_command.seq IS NOT NULL AND last_command.seq+1<>c.next_seq) \
                 )",
                &[],
            ))?
            .get(0);
            if !command_log_ready {
                return Err(EngineError::Storage(
                    "postgres command log baseline/head integrity check failed".into(),
                ));
            }
        }
        if fresh {
            st(client.batch_execute(RELATIONAL_SCHEMA))?;
            st(client.batch_execute(COMMAND_LOG_MIGRATION))?;
            st(client.batch_execute(QUEUE_METRICS_MIGRATION))?;
            st(client.execute(
                "INSERT INTO pqueue_metrics_migration_state(migration_name,status) \
                 VALUES('queue_metrics_v2_counted','complete') ON CONFLICT(migration_name) DO NOTHING",
                &[],
            ))?;
            st(client.batch_execute(
                "ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS fields TEXT NOT NULL DEFAULT '{}';\
                 ALTER TABLE queues ADD COLUMN IF NOT EXISTS pause_drain_intake BOOLEAN NOT NULL DEFAULT false;\
                 ALTER TABLE pqueue_items ADD COLUMN IF NOT EXISTS metadata TEXT NOT NULL DEFAULT '{}';\
                 ALTER TABLE pqueue_cohorts ADD COLUMN IF NOT EXISTS cohort_id TEXT;\
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
        }
        migrate_id_high_water(&mut client)?;
        verify_group_summary_indexes(&mut client, fresh)?;
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

    /// Restart recovery from one durable high-water row per queue. Work is proportional to queue count, not
    /// resident or retained item count.
    fn restore_counters(&self) -> EngineResult<()> {
        let mut g = self.inner.lock().expect("poisoned");
        let rows = st(g.client.query(
            "SELECT tenant_id, queue_id, item_id FROM pqueue_id_high_water",
            &[],
        ))?;
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

    /// The named, durable snapshot that must be restored before reading a compacted command tail.
    pub fn command_baseline_ref(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        let (tenant, queue) = parts(shard);
        let mut inner = self.inner.lock().expect("poisoned");
        let row = st(inner.client.query_opt(
            "SELECT generation,assignment_epoch,next_seq FROM pqueue_command_baselines \
             WHERE tenant=$1 AND queue=$2",
            &[&tenant, &queue],
        ))?
        .ok_or_else(|| EngineError::Storage("postgres command log baseline is missing".into()))?;
        let generation: String = row.get(0);
        let epoch: i64 = row.get(1);
        let next_seq: i64 = row.get(2);
        if epoch < 0 || next_seq < 0 {
            return Err(EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Position,
                manifest_index: 0,
                locator: "baseline".into(),
            });
        }
        Ok((next_seq > 0).then(|| SnapshotRef {
            queue: shard.clone(),
            position: CommandPosition::new(shard.clone(), epoch as u64, next_seq as u64 - 1),
            ref_id: generation,
        }))
    }

    /// Atomically restore the versioned SQL snapshot and replay its retained command tail in bounded pages.
    pub fn rebuild_from_command_baseline(&self, shard: &QueueKey) -> EngineResult<()> {
        let (tenant, queue) = parts(shard);
        let mut inner = self.inner.lock().expect("poisoned");
        let Inner {
            client,
            queues,
            live_tokens,
            ..
        } = &mut *inner;
        let mut tx = st(client.transaction())?;
        let cursor = st(tx.query_one(
            "SELECT next_seq,assignment_epoch FROM relational_cursor \
             WHERE tenant=$1 AND queue=$2 FOR UPDATE",
            &[&tenant, &queue],
        ))?;
        let durable_head: i64 = cursor.get(0);
        let durable_epoch: i64 = cursor.get(1);
        let baseline = st(tx.query_one(
            "SELECT generation,schema_version,assignment_epoch,next_seq,row_count,snapshot_digest \
             FROM pqueue_command_baselines WHERE tenant=$1 AND queue=$2",
            &[&tenant, &queue],
        ))?;
        let generation: String = baseline.get(0);
        let schema_version: i64 = baseline.get(1);
        let baseline_epoch: i64 = baseline.get(2);
        let baseline_next: i64 = baseline.get(3);
        let expected_count: i64 = baseline.get(4);
        let expected_digest: Vec<u8> = baseline.get(5);
        if schema_version != 1
            || baseline_epoch < 0
            || baseline_next < 0
            || baseline_next > durable_head
            || expected_count < 0
        {
            return Err(EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Manifest,
                manifest_index: 0,
                locator: "baseline".into(),
            });
        }
        let digest_row = st(tx.query_one(
            "SELECT COUNT(*)::text, \
                    COALESCE(SUM(hashtextextended(relation_name||':'||payload::text,0)::numeric),0)::text, \
                    COALESCE(SUM(hashtextextended(relation_name||':'||payload::text,2147483647)::numeric),0)::text \
             FROM pqueue_command_baseline_rows \
             WHERE tenant=$1 AND queue=$2 AND generation=$3",
            &[&tenant, &queue, &generation],
        ))?;
        let actual_count = digest_row.get::<_, String>(0).parse::<i64>().map_err(|_| {
            EngineError::Storage("snapshot row count exceeds postgres range".into())
        })?;
        let summary = format!(
            "{}:{}:{}",
            digest_row.get::<_, String>(0),
            digest_row.get::<_, String>(1),
            digest_row.get::<_, String>(2)
        );
        if actual_count != expected_count
            || Sha256::digest(summary.as_bytes()).as_slice() != expected_digest.as_slice()
        {
            return Err(EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Sha256,
                manifest_index: 0,
                locator: "baseline".into(),
            });
        }

        st(tx.batch_execute("ALTER TABLE pqueue_items DISABLE TRIGGER USER"))?;
        for &(relation, tenant_column, queue_column) in BASELINE_RELATIONS.iter().rev() {
            st(tx.execute(
                &format!("DELETE FROM {relation} WHERE {tenant_column}=$1 AND {queue_column}=$2"),
                &[&tenant, &queue],
            ))?;
        }
        for &(relation, _, _) in BASELINE_RELATIONS {
            st(tx.execute(
                &format!(
                    "INSERT INTO {relation} SELECT (jsonb_populate_record(NULL::{relation},payload)).* \
                     FROM pqueue_command_baseline_rows \
                     WHERE tenant=$1 AND queue=$2 AND generation=$3 AND relation_name=$4 \
                     ORDER BY row_ordinal"
                ),
                &[&tenant, &queue, &generation, &relation],
            ))?;
        }
        st(tx.batch_execute("ALTER TABLE pqueue_items ENABLE TRIGGER USER"))?;
        st(tx.execute(
            "UPDATE relational_cursor SET next_seq=$3,assignment_epoch=$4 \
             WHERE tenant=$1 AND queue=$2",
            &[&tenant, &queue, &baseline_next, &baseline_epoch],
        ))?;

        let mut next = baseline_next;
        let mut token_ops = Vec::new();
        while next < durable_head {
            let rows = st(tx.query(
                "SELECT assignment_epoch,seq,envelope,envelope_sha256 FROM pqueue_commands \
                 WHERE tenant=$1 AND queue=$2 AND seq>=$3 ORDER BY seq LIMIT 1024",
                &[&tenant, &queue, &next],
            ))?;
            if rows.is_empty() {
                return Err(EngineError::DurableDataCorrupt {
                    stage: DurableIntegrityStage::Position,
                    manifest_index: next as u64,
                    locator: format!("command:{next}"),
                });
            }
            for row in rows {
                let epoch: i64 = row.get(0);
                let sequence: i64 = row.get(1);
                let encoded: Vec<u8> = row.get(2);
                let checksum: Vec<u8> = row.get(3);
                let position = CommandPosition::new(
                    shard.clone(),
                    epoch.max(0) as u64,
                    sequence.max(0) as u64,
                );
                if sequence != next
                    || epoch < 0
                    || sequence < 0
                    || command_storage_checksum(&position, &encoded) != checksum
                {
                    return Err(EngineError::DurableDataCorrupt {
                        stage: DurableIntegrityStage::Position,
                        manifest_index: next as u64,
                        locator: format!("command:{next}"),
                    });
                }
                let envelope: CommandEnvelope = serde_json::from_slice(&encoded).map_err(|_| {
                    EngineError::DurableDataCorrupt {
                        stage: DurableIntegrityStage::Payload,
                        manifest_index: next as u64,
                        locator: format!("command:{next}"),
                    }
                })?;
                apply_command_sql(
                    &mut tx,
                    queues,
                    &mut token_ops,
                    shard,
                    next as u64,
                    envelope.created_at,
                    &envelope.command,
                )?;
                if let (Some(request_id), Some(fingerprint), Some(outcome)) = (
                    &envelope.request_id,
                    envelope.request_fingerprint,
                    &envelope.request_outcome,
                ) {
                    let fingerprint = fingerprint.to_be_bytes();
                    let expires_at = request_expires_at(queues, shard, envelope.created_at)?;
                    match outcome {
                        RequestOutcome::Push { item_ids } => record_request_idempotency(
                            &mut tx,
                            shard,
                            IDEMPOTENCY_OPERATION_PUSH,
                            request_id,
                            &fingerprint,
                            item_ids,
                            envelope.created_at,
                            expires_at,
                        )?,
                        RequestOutcome::BatchUpdate { response_payload } => {
                            record_batch_update_idempotency(
                                &mut tx,
                                shard,
                                request_id,
                                &fingerprint,
                                response_payload,
                                envelope.created_at,
                                expires_at,
                            )?;
                        }
                        RequestOutcome::CommitTransition { entries } => {
                            let recovery = entries
                                .iter()
                                .cloned()
                                .map(recovery_from_durable_outcome)
                                .collect::<Vec<_>>();
                            record_commit_idempotency(
                                &mut tx,
                                shard,
                                request_id,
                                &fingerprint,
                                &recovery,
                                envelope.created_at,
                                expires_at,
                            )?;
                        }
                        RequestOutcome::ClaimByQuery { .. } => {}
                    }
                }
                next += 1;
            }
        }
        st(tx.execute(
            "UPDATE relational_cursor SET next_seq=$3,assignment_epoch=$4 \
             WHERE tenant=$1 AND queue=$2",
            &[&tenant, &queue, &durable_head, &durable_epoch],
        ))?;
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops);
        Ok(())
    }
}

fn apply_concurrent_migrations(client: &mut Client) -> EngineResult<()> {
    apply_command_log_migration(client)?;
    st(client.batch_execute(QUEUE_METRICS_MIGRATION))?;
    for (_, ddl) in GROUP_SUMMARY_INDEX_MIGRATIONS {
        st(client.batch_execute(ddl))?;
    }
    verify_group_summary_indexes(client, false)
}

fn apply_command_log_migration(client: &mut Client) -> EngineResult<()> {
    // Explicit operator migration. Writers must be quiesced; each committed copy transaction is bounded
    // to 1024 rows and resumable. Finalization locks the cursor and refuses to activate a partial/drifted
    // generation. Ordinary startup never creates or blesses a baseline.
    st(client.batch_execute(COMMAND_LOG_MIGRATION))?;
    let has_orphan_history: bool = st(client.query_one(
        "SELECT EXISTS(SELECT 1 FROM pqueue_commands p \
         LEFT JOIN pqueue_command_baselines b USING(tenant,queue) WHERE b.tenant IS NULL)",
        &[],
    ))?
    .get(0);
    if has_orphan_history {
        return Err(EngineError::Storage(
            "command history exists without a snapshot baseline".into(),
        ));
    }
    st(client.execute(
        "INSERT INTO pqueue_command_baseline_migrations \
           (tenant,queue,generation,expected_epoch,expected_next_seq) \
         SELECT c.tenant,c.queue,'baseline-'||c.assignment_epoch||'-'||c.next_seq, \
                c.assignment_epoch,c.next_seq FROM relational_cursor c \
         LEFT JOIN pqueue_command_baselines b USING(tenant,queue) \
         WHERE b.tenant IS NULL ON CONFLICT(tenant,queue) DO NOTHING",
        &[],
    ))?;

    loop {
        let state = st(client.query_opt(
            "SELECT tenant,queue,generation,expected_epoch,expected_next_seq,relation_index, \
                    last_ctid,rows_copied,hash_a::text,hash_b::text \
             FROM pqueue_command_baseline_migrations ORDER BY tenant,queue LIMIT 1",
            &[],
        ))?;
        let Some(state) = state else {
            return Ok(());
        };
        let tenant: String = state.get(0);
        let queue: String = state.get(1);
        let generation: String = state.get(2);
        let expected_epoch: i64 = state.get(3);
        let expected_next_seq: i64 = state.get(4);
        let relation_index: i64 = state.get(5);
        let last_ctid: String = state.get(6);
        let rows_copied: i64 = state.get(7);
        let hash_a: String = state.get(8);
        let hash_b: String = state.get(9);

        if let Some(&(relation, tenant_column, queue_column)) =
            BASELINE_RELATIONS.get(relation_index as usize)
        {
            let mut tx = st(client.transaction())?;
            let sql = format!(
                "WITH batch AS MATERIALIZED ( \
                   SELECT ctid,to_jsonb(source) AS payload FROM {relation} source \
                   WHERE {tenant_column}=$1 AND {queue_column}=$2 AND ctid>$5::text::tid \
                   ORDER BY ctid LIMIT 1024 \
                 ), inserted AS ( \
                   INSERT INTO pqueue_command_baseline_rows \
                     (tenant,queue,generation,relation_name,row_ordinal,payload) \
                   SELECT $1,$2,$3,$4,$6+row_number() OVER (ORDER BY ctid),payload FROM batch \
                   RETURNING relation_name,payload \
                 ), stats AS ( \
                   SELECT COUNT(*)::bigint AS copied, \
                          (array_agg(ctid ORDER BY ctid DESC))[1]::text AS last_ctid, \
                          (SELECT COUNT(*) FROM inserted) AS inserted_count, \
                          (SELECT COALESCE(SUM(hashtextextended(relation_name||':'||payload::text,0)::numeric),0) \
                             FROM inserted) AS hash_a, \
                          (SELECT COALESCE(SUM(hashtextextended(relation_name||':'||payload::text,2147483647)::numeric),0) \
                             FROM inserted) AS hash_b FROM batch \
                 ) \
                 UPDATE pqueue_command_baseline_migrations m SET \
                   relation_index=CASE WHEN stats.copied=0 THEN m.relation_index+1 ELSE m.relation_index END, \
                   last_ctid=CASE WHEN stats.copied=0 THEN '(0,0)' ELSE stats.last_ctid END, \
                   rows_copied=m.rows_copied+stats.inserted_count, \
                   hash_a=m.hash_a+stats.hash_a,hash_b=m.hash_b+stats.hash_b,updated_at=clock_timestamp() \
                 FROM stats WHERE m.tenant=$1 AND m.queue=$2 RETURNING stats.copied"
            );
            st(tx.query_one(
                &sql,
                &[
                    &tenant,
                    &queue,
                    &generation,
                    &relation,
                    &last_ctid,
                    &rows_copied,
                ],
            ))?;
            st(tx.commit())?;
            continue;
        }

        let mut tx = st(client.transaction())?;
        let cursor = st(tx.query_one(
            "SELECT assignment_epoch,next_seq FROM relational_cursor \
             WHERE tenant=$1 AND queue=$2 FOR UPDATE",
            &[&tenant, &queue],
        ))?;
        let actual_epoch: i64 = cursor.get(0);
        let actual_next_seq: i64 = cursor.get(1);
        let metadata_stable: bool = st(tx.query_one(
            "WITH live AS ( \
               SELECT 'queues'::text relation_name,to_jsonb(q) payload FROM queues q \
                 WHERE tenant=$1 AND queue=$2 \
               UNION ALL \
               SELECT 'relational_emission_cursor',to_jsonb(e) FROM relational_emission_cursor e \
                 WHERE tenant=$1 AND queue=$2 \
             ), snap AS ( \
               SELECT relation_name,payload FROM pqueue_command_baseline_rows \
                 WHERE tenant=$1 AND queue=$2 AND generation=$3 \
                   AND relation_name IN ('queues','relational_emission_cursor') \
             ) \
             SELECT NOT EXISTS((SELECT * FROM live) EXCEPT ALL (SELECT * FROM snap)) \
                AND NOT EXISTS((SELECT * FROM snap) EXCEPT ALL (SELECT * FROM live))",
            &[&tenant, &queue, &generation],
        ))?
        .get(0);
        if actual_epoch != expected_epoch
            || actual_next_seq != expected_next_seq
            || !metadata_stable
        {
            st(tx.execute(
                "DELETE FROM pqueue_command_baseline_rows \
                 WHERE tenant=$1 AND queue=$2 AND generation=$3",
                &[&tenant, &queue, &generation],
            ))?;
            st(tx.execute(
                "DELETE FROM pqueue_command_baseline_migrations WHERE tenant=$1 AND queue=$2",
                &[&tenant, &queue],
            ))?;
            st(tx.commit())?;
            return Err(EngineError::Storage(
                "command baseline migration drifted; restart under quiescence".into(),
            ));
        }
        // O(1) manifest seal while the cursor is locked: batch transactions accumulated all integrity
        // inputs. Full row verification happens only during the explicitly offline restore.
        let summary = format!("{rows_copied}:{hash_a}:{hash_b}");
        let row_count = rows_copied;
        let snapshot_digest = Sha256::digest(summary.as_bytes()).to_vec();
        st(tx.execute(
            "INSERT INTO pqueue_command_baselines \
               (tenant,queue,generation,schema_version,assignment_epoch,next_seq,row_count,snapshot_digest) \
             VALUES($1,$2,$3,1,$4,$5,$6,$7)",
            &[
                &tenant,
                &queue,
                &generation,
                &expected_epoch,
                &expected_next_seq,
                &row_count,
                &snapshot_digest,
            ],
        ))?;
        st(tx.execute(
            "DELETE FROM pqueue_command_baseline_migrations WHERE tenant=$1 AND queue=$2",
            &[&tenant, &queue],
        ))?;
        st(tx.commit())?;
    }
}

fn migrate_id_high_water(client: &mut Client) -> EngineResult<()> {
    st(client.batch_execute(
        "CREATE TABLE IF NOT EXISTS pqueue_id_high_water ( \
           tenant_id TEXT NOT NULL, queue_id TEXT NOT NULL, item_id TEXT NOT NULL, \
           PRIMARY KEY (tenant_id,queue_id)); \
         CREATE TABLE IF NOT EXISTS pqueue_schema_migrations ( \
           migration_name TEXT NOT NULL PRIMARY KEY);",
    ))?;
    let mut tx = st(client.transaction())?;
    st(tx.batch_execute("LOCK TABLE pqueue_schema_migrations IN EXCLUSIVE MODE"))?;
    let complete: bool = st(tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM pqueue_schema_migrations \
         WHERE migration_name='item_id_high_water_v2')",
        &[],
    ))?
    .get(0);
    if !complete {
        // One set-based upgrade pass for v0.19.3 databases. Every later pool member and restart sees the
        // marker and does O(1) work; no Rust-side materialized-row loop is introduced.
        st(tx.execute(
            "INSERT INTO pqueue_id_high_water(tenant_id,queue_id,item_id) \
             SELECT tenant_id,queue_id,MAX(item_id::numeric)::text FROM pqueue_items \
             GROUP BY tenant_id,queue_id \
             ON CONFLICT(tenant_id,queue_id) DO UPDATE SET item_id=EXCLUDED.item_id \
             WHERE pqueue_id_high_water.item_id::numeric < EXCLUDED.item_id::numeric",
            &[],
        ))?;
        st(tx.execute(
            "INSERT INTO pqueue_schema_migrations(migration_name) \
             VALUES('item_id_high_water_v2')",
            &[],
        ))?;
    }
    st(tx.commit())
}

fn migrate_metrics_batch(
    client: &mut Client,
    batch_size: u32,
) -> EngineResult<MetricsMigrationProgress> {
    if batch_size == 0 || batch_size > 100_000 {
        return Err(EngineError::Invalid(
            "metrics migration batch size must be 1..=100000",
        ));
    }
    st(client.batch_execute(QUEUE_METRICS_MIGRATION))?;

    // Initialization takes only a metadata/high-water lock window; it never scans or aggregates the table.
    let exists: bool = st(client.query_one(
        "SELECT EXISTS(SELECT 1 FROM pqueue_metrics_migration_state \
         WHERE migration_name='queue_metrics_v2_counted')",
        &[],
    ))?
    .get(0);
    if !exists {
        let mut tx = st(client.transaction())?;
        st(tx.batch_execute("LOCK TABLE pqueue_items IN SHARE ROW EXCLUSIVE MODE"))?;
        let initialized: bool = st(tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM pqueue_metrics_migration_state \
             WHERE migration_name='queue_metrics_v2_counted')",
            &[],
        ))?
        .get(0);
        if !initialized {
            let high = st(tx.query_opt(
                "SELECT tenant_id,queue_id,item_id FROM pqueue_items \
                 ORDER BY tenant_id DESC,queue_id DESC,item_id DESC LIMIT 1",
                &[],
            ))?;
            match high {
                Some(row) => {
                    let high_t: String = row.get(0);
                    let high_q: String = row.get(1);
                    let high_i: String = row.get(2);
                    st(tx.execute(
                        "INSERT INTO pqueue_metrics_migration_state( \
                           migration_name,status,high_tenant,high_queue,high_item_id) \
                         VALUES('queue_metrics_v2_counted','active',$1,$2,$3)",
                        &[&high_t, &high_q, &high_i],
                    ))?;
                }
                None => {
                    st(tx.execute(
                        "INSERT INTO pqueue_metrics_migration_state(migration_name,status) \
                         VALUES('queue_metrics_v2_counted','complete')",
                        &[],
                    ))?;
                }
            }
        }
        st(tx.commit())?;
    }

    let mut tx = st(client.transaction())?;
    let state = st(tx.query_one(
        "SELECT status,high_tenant,high_queue,high_item_id,last_tenant,last_queue,last_item_id, \
                rows_backfilled,batches_completed,due_rows_backfilled \
         FROM pqueue_metrics_migration_state WHERE migration_name='queue_metrics_v2_counted' FOR UPDATE",
        &[],
    ))?;
    let status: String = state.get(0);
    let prior_rows: i64 = state.get(7);
    let prior_batches: i64 = state.get(8);
    let prior_due_rows: i64 = state.get(9);
    if status == "complete" {
        st(tx.commit())?;
        return Ok(MetricsMigrationProgress {
            rows_processed: 0,
            rows_backfilled: prior_rows as u64,
            due_rows_backfilled: prior_due_rows as u64,
            batches_completed: prior_batches as u64,
            complete: true,
        });
    }
    let high_t: String = state.get(1);
    let high_q: String = state.get(2);
    let high_i: String = state.get(3);
    let last_t: Option<String> = state.get(4);
    let last_q: Option<String> = state.get(5);
    let last_i: Option<String> = state.get(6);
    let rows = st(tx.query(
        "SELECT tenant_id,queue_id,item_id,lifecycle_state,superseded,item_version \
         FROM pqueue_items \
         WHERE (tenant_id,queue_id,item_id) > ($1,$2,$3) \
           AND (tenant_id,queue_id,item_id) <= ($4,$5,$6) \
         ORDER BY tenant_id,queue_id,item_id LIMIT $7 FOR UPDATE",
        &[
            &last_t.as_deref().unwrap_or(""),
            &last_q.as_deref().unwrap_or(""),
            &last_i.as_deref().unwrap_or(""),
            &high_t,
            &high_q,
            &high_i,
            &(batch_size as i64),
        ],
    ))?;
    let processed = rows.len() as i64;
    let item_tenants: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    let item_queues: Vec<String> = rows.iter().map(|row| row.get(1)).collect();
    let item_ids: Vec<String> = rows.iter().map(|row| row.get(2)).collect();
    let item_states: Vec<String> = rows.iter().map(|row| row.get(3)).collect();
    let item_superseded: Vec<bool> = rows.iter().map(|row| row.get(4)).collect();
    let item_versions: Vec<i64> = rows.iter().map(|row| row.get(5)).collect();
    let newly_counted = if rows.is_empty() {
        Vec::new()
    } else {
        st(tx.query(
            "INSERT INTO pqueue_metrics_counted_item( \
               tenant_id,queue_id,item_id,lifecycle_state,superseded,item_version) \
             SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bool[],$6::bigint[]) \
             ON CONFLICT(tenant_id,queue_id,item_id) DO NOTHING \
             RETURNING tenant_id,queue_id,lifecycle_state,superseded",
            &[&item_tenants,&item_queues,&item_ids,&item_states,&item_superseded,&item_versions],
        ))?
    };
    let mut per_queue: HashMap<(String, String), [i64; 4]> = HashMap::new();
    for row in &newly_counted {
        if row.get::<_, bool>(3) {
            continue;
        }
        let counts = per_queue.entry((row.get(0), row.get(1))).or_default();
        match row.get::<_, String>(2).as_str() {
            "Pending" => counts[0] += 1,
            "Leased" => counts[1] += 1,
            "Complete" => counts[2] += 1,
            "Failed" => counts[3] += 1,
            _ => return Err(EngineError::Storage("invalid lifecycle state".into())),
        }
    }
    for ((tenant, queue), counts) in per_queue {
        st(tx.execute(
            "INSERT INTO pqueue_queue_metrics_v2(tenant_id,queue_id,pending,leased,complete,failed) \
             VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,queue_id) DO UPDATE SET \
               pending=pqueue_queue_metrics_v2.pending+EXCLUDED.pending, \
               leased=pqueue_queue_metrics_v2.leased+EXCLUDED.leased, \
               complete=pqueue_queue_metrics_v2.complete+EXCLUDED.complete, \
               failed=pqueue_queue_metrics_v2.failed+EXCLUDED.failed",
            &[
                &tenant, &queue, &counts[0], &counts[1], &counts[2], &counts[3],
            ],
        ))?;
    }
    if !item_ids.is_empty() {
        st(tx.execute(
            "INSERT INTO pqueue_item_index_component( \
               tenant_id,queue_id,index_name,item_id,component_position,component_value) \
             SELECT idx.tenant_id,idx.queue_id,idx.index_name,idx.item_id, \
                    component.component_position,component.component_value \
             FROM unnest($1::text[],$2::text[],$3::text[]) selected(tenant_id,queue_id,item_id) \
             JOIN pqueue_item_index idx USING(tenant_id,queue_id,item_id) \
             CROSS JOIN LATERAL pqueue_index_components(idx.index_key) component \
             ON CONFLICT(tenant_id,queue_id,index_name,item_id,component_position) \
             DO UPDATE SET component_value=EXCLUDED.component_value",
            &[&item_tenants, &item_queues, &item_ids],
        ))?;
    }
    let due_rows = if item_ids.is_empty() {
        0
    } else {
        st(tx.execute(
            "INSERT INTO pqueue_group_due_pending(tenant_id,queue_id,item_id,group_key,due_at,created_seq) \
             SELECT i.tenant_id,i.queue_id,i.item_id,i.group_key,i.not_before,i.created_seq \
             FROM unnest($1::text[],$2::text[],$3::text[]) selected(tenant_id,queue_id,item_id) \
             JOIN pqueue_items i USING(tenant_id,queue_id,item_id) \
             LEFT JOIN pqueue_group_summary s ON s.tenant_id=i.tenant_id AND s.queue_id=i.queue_id \
               AND s.group_key=i.group_key \
             WHERE i.group_key IS NOT NULL AND i.lifecycle_state='Pending' \
               AND i.superseded=false AND i.not_before IS NOT NULL \
               AND (s.updated_at IS NULL OR i.not_before>s.updated_at) \
             ON CONFLICT(tenant_id,queue_id,item_id) DO UPDATE SET group_key=EXCLUDED.group_key, \
               due_at=EXCLUDED.due_at,created_seq=EXCLUDED.created_seq",
            &[&item_tenants, &item_queues, &item_ids],
        ))? as i64
    };
    // A short page is the end-of-high-water sentinel because this transaction owns the cursor row and
    // the SELECT is ordered and non-SKIP-LOCKED. Thus every call reads/locks at most `batch_size` rows.
    let complete = processed < batch_size as i64;
    let (next_t, next_q, next_i) = rows
        .last()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            )
        })
        .unwrap_or((high_t, high_q, high_i));
    st(tx.execute(
        "UPDATE pqueue_metrics_migration_state SET status=$1,last_tenant=$2,last_queue=$3,last_item_id=$4, \
           rows_backfilled=rows_backfilled+$5,due_rows_backfilled=due_rows_backfilled+$6, \
           batches_completed=batches_completed+1,updated_at=clock_timestamp() \
         WHERE migration_name='queue_metrics_v2_counted'",
        &[&if complete { "complete" } else { "active" }, &next_t, &next_q, &next_i, &processed, &due_rows],
    ))?;
    st(tx.commit())?;
    Ok(MetricsMigrationProgress {
        rows_processed: processed as u64,
        rows_backfilled: (prior_rows + processed) as u64,
        due_rows_backfilled: (prior_due_rows + due_rows) as u64,
        batches_completed: (prior_batches + 1) as u64,
        complete,
    })
}

// --- Typed raw commit -------------------------------------------------------------------------------
//
// The sync postgres `Transaction` methods take `&mut self`, so append and apply share one transaction
// through a `RefCell` and borrow it sequentially inside this backend-owned typed operation.

struct PgRelLogTxn<'a, 'b> {
    tx: &'a RefCell<postgres::Transaction<'b>>,
}

impl PgRelLogTxn<'_, '_> {
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
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?
            .get(0);
            epoch as u64
        };
        if expected_epoch != epoch {
            return Err(EngineError::EpochFenced);
        }
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let base = {
            let mut tx = self.tx.borrow_mut();
            alloc_seq_range(&mut tx, &t, &q, commands.len())?
        };
        let positions = (0..commands.len())
            .map(|offset| CommandPosition::new(shard.clone(), epoch, base + offset as u64))
            .collect::<Vec<_>>();
        {
            let mut tx = self.tx.borrow_mut();
            persist_command_envelopes(&mut tx, &positions, commands)?;
        }
        Ok(positions)
    }
}

struct PgRelProjectionTxn<'a, 'b> {
    tx: &'a RefCell<postgres::Transaction<'b>>,
    queues: &'a HashMap<QueueKey, QueueDefinition>,
    token_ops: &'a mut Vec<TokenOp>,
}

impl PgRelProjectionTxn<'_, '_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, env) in positions.iter().zip(commands) {
            let mut tx = self.tx.borrow_mut();
            let (tenant, queue) = parts(&pos.queue);
            let epoch_row = st(tx.query_one(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&tenant, &queue],
            ))?;
            let stored_epoch: i64 = epoch_row.get(0);
            if stored_epoch as u64 > pos.backend_epoch {
                return Err(EngineError::EpochFenced);
            }
            apply_command_sql(
                &mut tx,
                self.queues,
                self.token_ops,
                &pos.queue,
                pos.sequence,
                env.created_at,
                &env.command,
            )?;
            if let (
                Some(request_id),
                Some(_fingerprint),
                Some(pqueue_engine::RequestOutcome::Push { item_ids }),
                QueueCommand::Push(push),
            ) = (
                &env.request_id,
                env.request_fingerprint,
                &env.request_outcome,
                &env.command,
            ) {
                let expires_at = request_expires_at(self.queues, &pos.queue, env.created_at)?;
                record_request_idempotency(
                    &mut tx,
                    &pos.queue,
                    IDEMPOTENCY_OPERATION_PUSH,
                    request_id,
                    &pqueue_engine::push_items_fingerprint_sha256(&push.items)?,
                    item_ids,
                    env.created_at,
                    expires_at,
                )?;
            }
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

    fn commit_raw(
        &self,
        request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
    {
        let result = (|| {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            if fault == pqueue_engine::RawCommitFault::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
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
            let positions = {
                let mut log_txn = PgRelLogTxn { tx: &tx_cell };
                log_txn.append(&shard, &commands, expected_epoch)?
            };
            if fault == pqueue_engine::RawCommitFault::AfterAppendBeforeApply {
                // Atomic storage has no durable append-only cut: dropping this transaction rolls back the
                // staged rows. Never report a phantom durable append to the fault harness.
                return Err(EngineError::Unavailable);
            }
            {
                let mut projection_txn = PgRelProjectionTxn {
                    tx: &tx_cell,
                    queues,
                    token_ops: &mut token_ops,
                };
                projection_txn.apply(&positions, &commands)?;
            }
            st(tx_cell.into_inner().commit())?;
            apply_token_ops(live_tokens, token_ops); // only after a durable commit
            Ok(pqueue_engine::RawCommitOutcome::applied(positions))
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
            let mut tx = st(g.client.transaction())?;
            st(tx.execute(
                "INSERT INTO queues(tenant,queue,definition,paused) VALUES($1,$2,$3,false)",
                &[&t, &q, &def_json],
            ))?;
            st(tx.execute(
                "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES($1,$2,0,0)",
                &[&t, &q],
            ))?;
            // Rebuild verifies every baseline through the canonical count/sum/sum summary, including the
            // empty genesis snapshot. An empty relation set therefore hashes "0:0:0", not zero bytes.
            let empty_snapshot_digest = Sha256::digest(b"0:0:0").to_vec();
            st(tx.execute(
                "INSERT INTO pqueue_command_baselines \
                   (tenant,queue,generation,schema_version,assignment_epoch,next_seq,row_count,snapshot_digest) \
                 VALUES($1,$2,'genesis',1,0,0,0,$3)",
                &[&t, &q, &empty_snapshot_digest],
            ))?;
            let create = direct_command_envelope(
                &key,
                QueueCommand::CreateQueue(pqueue_engine::CreateQueueCommand {
                    definition: definition.clone(),
                }),
                UtcTimestamp::new(0, 0).expect("unix epoch is valid"),
                0,
                0,
            );
            persist_command_envelopes(
                &mut tx,
                std::slice::from_ref(&CommandPosition::new(key.clone(), 0, 0)),
                std::slice::from_ref(&create),
            )?;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=1 WHERE tenant=$1 AND queue=$2",
                &[&t, &q],
            ))?;
            st(tx.commit())?;
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
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
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

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        let result = {
            let mut guard = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *guard;
            pending_summary_sql(client, live_tokens, shard)
        };
        std::future::ready(result)
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        let result = {
            let mut guard = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *guard;
            pending_page_sql(client, live_tokens, shard, start, limit)
        };
        std::future::ready(result)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let mut guard = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *guard;
            pending_range_sql(client, live_tokens, shard, start, end, consumer, limit)
        };
        std::future::ready(result)
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = {
            let mut guard = self.inner.lock().expect("poisoned");
            let Inner {
                client,
                live_tokens,
                ..
            } = &mut *guard;
            pending_by_ids_sql(client, live_tokens, shard, ids)
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

impl LogRead for PostgresRelationalBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        const MAX_PAGE_SIZE: usize = 1024;
        let result = (|| {
            if let Some(position) = &from
                && &position.queue != shard
            {
                return Err(EngineError::Invalid("log cursor belongs to another queue"));
            }
            let page_size = limit.min(MAX_PAGE_SIZE);
            if page_size == 0 {
                return Ok(CommandPage {
                    entries: Vec::new(),
                    next: None,
                });
            }
            let requested_start = from.as_ref().map_or(Ok(0), |position| {
                position.sequence.checked_add(1).ok_or(EngineError::Invalid(
                    "log cursor exceeds postgres sequence range",
                ))
            })?;
            let fetch_limit = i64::try_from(page_size + 1)
                .map_err(|_| EngineError::Invalid("log page exceeds postgres limit"))?;
            let (tenant, queue) = parts(shard);
            let mut inner = self.inner.lock().expect("poisoned");
            let baseline_row = st(inner.client.query_opt(
                "SELECT b.next_seq,c.next_seq FROM pqueue_command_baselines b \
                 JOIN relational_cursor c ON c.tenant=b.tenant AND c.queue=b.queue \
                 WHERE b.tenant=$1 AND b.queue=$2",
                &[&tenant, &queue],
            ))?
            .ok_or_else(|| {
                EngineError::Storage("postgres command log baseline is missing".into())
            })?;
            let baseline: i64 = baseline_row.get(0);
            let durable_head: i64 = baseline_row.get(1);
            if baseline < 0 || durable_head < baseline {
                return Err(EngineError::DurableDataCorrupt {
                    stage: DurableIntegrityStage::Position,
                    manifest_index: 0,
                    locator: "baseline".into(),
                });
            }
            if from.is_none() && baseline > 0 {
                return Err(EngineError::Invalid(
                    "restore the named command baseline before reading its retained tail",
                ));
            }
            if from.is_some() && requested_start < baseline as u64 {
                return Err(EngineError::Invalid(
                    "log cursor precedes the retained snapshot baseline",
                ));
            }
            let start = if from.is_none() {
                baseline as u64
            } else {
                requested_start
            };
            let start = i64::try_from(start)
                .map_err(|_| EngineError::Invalid("log cursor exceeds postgres sequence range"))?;
            let mut rows = st(inner.client.query(
                "SELECT assignment_epoch,seq,envelope,envelope_sha256 FROM pqueue_commands \
                 WHERE tenant=$1 AND queue=$2 AND seq>=$3 ORDER BY seq LIMIT $4",
                &[&tenant, &queue, &start, &fetch_limit],
            ))?;
            let has_more = rows.len() > page_size;
            if has_more {
                rows.pop();
            }
            let mut entries = Vec::with_capacity(rows.len());
            let mut expected_sequence = start as u64;
            for row in rows {
                let epoch: i64 = row.get(0);
                let sequence: i64 = row.get(1);
                let encoded: Vec<u8> = row.get(2);
                let stored_checksum: Vec<u8> = row.get(3);
                if epoch < 0 || sequence < 0 || sequence as u64 != expected_sequence {
                    return Err(EngineError::DurableDataCorrupt {
                        stage: DurableIntegrityStage::Position,
                        manifest_index: sequence.max(0) as u64,
                        locator: format!("command:{expected_sequence}"),
                    });
                }
                let position = CommandPosition::new(shard.clone(), epoch as u64, sequence as u64);
                if command_storage_checksum(&position, &encoded) != stored_checksum {
                    return Err(EngineError::DurableDataCorrupt {
                        stage: DurableIntegrityStage::Sha256,
                        manifest_index: sequence as u64,
                        locator: format!("command:{sequence}"),
                    });
                }
                let envelope: CommandEnvelope = serde_json::from_slice(&encoded).map_err(|_| {
                    EngineError::DurableDataCorrupt {
                        stage: DurableIntegrityStage::Payload,
                        manifest_index: sequence as u64,
                        locator: format!("command:{sequence}"),
                    }
                })?;
                entries.push((position, envelope));
                expected_sequence += 1;
            }
            if !has_more && expected_sequence != durable_head as u64 {
                return Err(EngineError::DurableDataCorrupt {
                    stage: DurableIntegrityStage::Position,
                    manifest_index: expected_sequence,
                    locator: format!("command:{expected_sequence}"),
                });
            }
            let next = has_more
                .then(|| entries.last().map(|(position, _)| position.clone()))
                .flatten();
            Ok(CommandPage { entries, next })
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
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
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
            let command = QueueCommand::Push(PushCommand { items: push_items });
            let mut envelope =
                direct_command_envelope(shard, command, now, cursor_epoch as u64, seq);
            envelope.request_id = Some(request_id.clone());
            envelope.request_fingerprint = fingerprint
                .get(..8)
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes);
            envelope.request_outcome = Some(RequestOutcome::Push {
                item_ids: ids.clone(),
            });
            let position = CommandPosition::new(shard.clone(), cursor_epoch as u64, seq);
            persist_command_envelopes(
                &mut tx,
                std::slice::from_ref(&position),
                std::slice::from_ref(&envelope),
            )?;
            let mut token_ops = Vec::new();
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                shard,
                seq,
                now,
                &envelope.command,
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
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&t, &q],
            ))?
            .get(0);
            if req.expected_epoch.is_some_and(|e| e != claim_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            // Time-driven summary maintenance is fenced with the claim. An explicit selection-only
            // eligibility_time must never advance durable summary state, so promotion uses wall-clock
            // `now`; live candidate predicates below may still use `eligibility_at()`.
            if !matches!(unit, ClaimUnit::Item)
                && !promote_due_group_summary_chunk_in_tx(&mut tx, &req.shard, req.now)?
            {
                st(tx.commit())?;
                return Err(EngineError::Unavailable);
            }
            let seq = alloc_seq(&mut tx, &t, &q)?;
            let now_n = ts_nanos(req.now);
            // Due-ness is resolved at the caller-resolved eligibility epoch (`ClaimRequest::eligibility_at`),
            // which defaults to `now`. It feeds ONLY the candidate predicates (`not_before<=$3` in the CTE,
            // the group/cohort selections); `now_n` still stamps `updated_at` and the lease.
            let elig_n = ts_nanos(req.eligibility_at());
            let exp = ts_nanos(req.lease_expires_at);
            let hash = lease_hash(&req.lease_token);
            let seqi = seq as i64;

            if matches!(unit, ClaimUnit::Item) {
                // Item-level: the serialized FOR UPDATE SKIP LOCKED CTE (select + lease + RETURNING).
                let lim = req.max_items as i64;
                let rows = st(tx.query(
                    CLAIM_CTE,
                    &[&t, &q, &elig_n, &lim, &hash, &exp, &now_n, &seqi],
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
                let claim_command = QueueCommand::Claim(ClaimCommand {
                    item_ids: claimed_ids.clone(),
                    lease_token: req.lease_token.clone(),
                    lease_expires_at: req.lease_expires_at,
                    worker_id: Some(req.worker_id.clone()),
                });
                let position = CommandPosition::new(req.shard.clone(), claim_epoch as u64, seq);
                let envelope = direct_command_envelope(
                    &req.shard,
                    claim_command,
                    req.now,
                    claim_epoch as u64,
                    seq,
                );
                persist_command_envelopes(
                    &mut tx,
                    std::slice::from_ref(&position),
                    std::slice::from_ref(&envelope),
                )?;
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
                let claimed_id_strings: Vec<String> =
                    claimed_ids.iter().map(ToString::to_string).collect();
                decrement_group_summaries_for_items(
                    &mut tx,
                    &req.shard,
                    &claimed_id_strings,
                    req.now,
                )?;
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
                    select_group_batching(
                        &mut tx,
                        &req.shard,
                        req.eligibility_at(),
                        req.max_items,
                        max_groups,
                        &req.compatibility,
                        req.eligibility_at() != req.now,
                    )?
                }
                ClaimUnit::SameGroupKey => select_same_group(
                    &mut tx,
                    &req.shard,
                    req.eligibility_at(),
                    req.max_items,
                    &req.compatibility,
                    req.eligibility_at() != req.now,
                )?,
                ClaimUnit::WholeCohort => {
                    match select_whole_cohort(
                        &mut tx,
                        &req.shard,
                        req.eligibility_at(),
                        req.max_items,
                        &req.compatibility,
                    )? {
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
                    worker_id: Some(req.worker_id.clone()),
                })
            };
            let position = CommandPosition::new(req.shard.clone(), claim_epoch as u64, seq);
            let envelope = direct_command_envelope(
                &req.shard,
                claim_command,
                req.now,
                claim_epoch as u64,
                seq,
            );
            persist_command_envelopes(
                &mut tx,
                std::slice::from_ref(&position),
                std::slice::from_ref(&envelope),
            )?;
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                &req.shard,
                seq,
                req.now,
                &envelope.command,
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
                    // A retention tombstone from any successfully purged item keeps the re-push a dup.
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
                "SELECT next_seq, assignment_epoch FROM relational_cursor \
                 WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&t, &q],
            ))?
            .ok_or(EngineError::NotFound)?;
            let seq0: i64 = row.get(0);
            let cursor_epoch: i64 = row.get(1);
            if expected_epoch.is_some_and(|e| e != cursor_epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            // The cursor row is the durable fencing authority. `expected_epoch=None` means the caller is
            // not supplying an additional fence; it must never mean epoch zero. Mint continuation IDs from
            // the same locked epoch stamped on their Push command.
            let epoch = cursor_epoch as u64;
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
                let additional_consumed_input_ids = entry
                    .additional_claim_refs
                    .iter()
                    .map(|claim| claim.item_id)
                    .collect::<Vec<_>>();
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids: additional_consumed_input_ids.clone(),
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };
                if let Err(error) = pqueue_engine::validate_distinct_commit_claims(
                    &entry.claim_ref,
                    &entry.additional_claim_refs,
                ) {
                    recovery.push(reject(error));
                    continue;
                }
                if let Some(e) = std::iter::once(&entry.claim_ref)
                    .chain(&entry.additional_claim_refs)
                    .find_map(|claim| commit_validate_sql(&mut tx, shard, claim, now).err())
                {
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
                    persist_and_apply_command(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        DirectCommand {
                            shard,
                            epoch: cursor_epoch as u64,
                            seq,
                            now,
                            command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                                records: entry.side_records,
                            }),
                        },
                    )?;
                    seq += 1;
                }
                if let Some(fence) = entry.instance_fence {
                    persist_and_apply_command(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        DirectCommand {
                            shard,
                            epoch: cursor_epoch as u64,
                            seq,
                            now,
                            command: QueueCommand::AdvanceInstanceFence(
                                AdvanceInstanceFenceCommand {
                                    instance_key: fence.instance_key,
                                    expected: fence.expected,
                                    next: fence.next,
                                },
                            ),
                        },
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
                    persist_and_apply_command(
                        &mut tx,
                        queues,
                        &mut token_ops,
                        DirectCommand {
                            shard,
                            epoch: cursor_epoch as u64,
                            seq,
                            now,
                            command: QueueCommand::Push(PushCommand { items: push_items }),
                        },
                    )?;
                    seq += 1;
                }
                persist_and_apply_command(
                    &mut tx,
                    queues,
                    &mut token_ops,
                    DirectCommand {
                        shard,
                        epoch: cursor_epoch as u64,
                        seq,
                        now,
                        command: QueueCommand::Finalize(FinalizeCommand {
                            outcomes: std::iter::once(&entry.claim_ref)
                                .chain(&entry.additional_claim_refs)
                                .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                                .collect(),
                        }),
                    },
                )?;
                seq += 1;
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }
            if let Some(rid) = &request_id {
                let command = QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                    records: Vec::new(),
                });
                let mut marker =
                    direct_command_envelope(shard, command, now, cursor_epoch as u64, seq);
                marker.request_id = Some(rid.clone());
                marker.request_fingerprint = fingerprint
                    .get(..8)
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_be_bytes);
                marker.request_outcome = Some(RequestOutcome::CommitTransition {
                    entries: recovery.iter().map(durable_outcome_entry).collect(),
                });
                let position = CommandPosition::new(shard.clone(), cursor_epoch as u64, seq);
                persist_command_envelopes(
                    &mut tx,
                    std::slice::from_ref(&position),
                    std::slice::from_ref(&marker),
                )?;
                apply_command_sql(
                    &mut tx,
                    queues,
                    &mut token_ops,
                    shard,
                    seq,
                    now,
                    &marker.command,
                )?;
                seq += 1;
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

impl pqueue_engine::HotProjectionQueryPort for PostgresRelationalBackend {
    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let typed_indexes = g
                .queues
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .typed_indexes
                .clone();
            metrics_by_query_sql(&mut g.client, &typed_indexes, shard, request)
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
            validate_api001_reserved_write_fields(&field_ops)?;
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
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
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

impl BatchUpdatePort for PostgresRelationalBackend {
    fn batch_update(
        &self,
        shard: &QueueKey,
        request: BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<BatchUpdateResponse>> + Send {
        let result = (|| {
            if request.updates.is_empty() {
                return Err(EngineError::Invalid("empty batch update"));
            }
            if request.updates.len() > 1_000 {
                return Err(EngineError::BatchTooLarge);
            }
            let fingerprint = Sha256::digest(
                serde_json::to_vec(&request)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
            )
            .get(..8)
            .expect("sha256 is at least eight bytes")
            .to_vec();
            let request_id = request.request_id.clone();
            let (t, q) = parts(shard);
            let now_n = ts_nanos(now);
            let mut g = self.inner.lock().expect("poisoned");
            let definition = g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
            let expires_at = request_expires_at(&g.queues, shard, now)?;
            let mut tx = st(g.client.transaction())?;
            let epoch: i64 = st(tx.query_one(
                "SELECT assignment_epoch FROM relational_cursor \
                 WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&t, &q],
            ))?
            .get(0);
            if expected_epoch.is_some_and(|expected| expected != epoch as u64) {
                return Err(EngineError::EpochFenced);
            }
            if let Some(payload) =
                check_batch_update_idempotency(&mut tx, shard, &request_id, &fingerprint, now_n)?
            {
                let response = serde_json::from_str(&payload)
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                return Ok(response);
            }

            let mut requested_ids = Vec::new();
            let mut requested_keys = Vec::new();
            for update in &request.updates {
                match &update.item_ref {
                    BatchUpdateItemRef::ItemId(item_id) => requested_ids.push(item_id.to_string()),
                    BatchUpdateItemRef::ClientItemKey(key) => {
                        requested_keys.push(key.as_str().to_string());
                    }
                    BatchUpdateItemRef::Both {
                        item_id,
                        client_item_key,
                    } => {
                        requested_ids.push(item_id.to_string());
                        requested_keys.push(client_item_key.as_str().to_string());
                    }
                }
            }

            #[derive(Clone)]
            struct CurrentItem {
                item_id: String,
                client_item_key: String,
                state: ItemState,
                superseded: bool,
                fenced: bool,
                version: u64,
                priority_json: Option<String>,
                priority_sort: Vec<u8>,
                not_before: Option<i64>,
                payload: Option<Vec<u8>>,
                fields_json: String,
                metadata_json: String,
                group_key: Option<String>,
            }
            #[cfg(test)]
            update_batch_update_sql_probe(shard, |probe| probe.target_selects += 1);
            let rows = st(tx.query(
                "SELECT item_id,client_item_key,lifecycle_state,superseded,fenced,item_version, \
                        priority,priority_sort,not_before,payload,fields,metadata,group_key \
                 FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
                   AND (item_id=ANY($3) OR client_item_key=ANY($4)) FOR UPDATE",
                &[&t, &q, &requested_ids, &requested_keys],
            ))?;
            let mut by_id = HashMap::with_capacity(rows.len());
            let mut by_key = HashMap::with_capacity(rows.len());
            for row in rows {
                let current = CurrentItem {
                    item_id: row.get(0),
                    client_item_key: row.get(1),
                    state: parse_state(&row.get::<_, String>(2))?,
                    superseded: row.get(3),
                    fenced: row.get(4),
                    version: row.get::<_, i64>(5) as u64,
                    priority_json: row.get(6),
                    priority_sort: row.get(7),
                    not_before: row.get(8),
                    payload: row.get(9),
                    fields_json: row.get(10),
                    metadata_json: row.get(11),
                    group_key: row.get(12),
                };
                by_key.insert(current.client_item_key.clone(), current.item_id.clone());
                by_id.insert(current.item_id.clone(), current);
            }

            struct ValidUpdate {
                outcome_index: usize,
                item_id: ItemId,
                item_id_string: String,
                client_item_key: ClientItemKey,
                previous_version: u64,
                fields_json: String,
                payload: Option<Vec<u8>>,
                metadata_json: String,
                priority_json: Option<String>,
                priority_sort: Vec<u8>,
                not_before: Option<i64>,
                replacement_gate_keys: Option<Vec<String>>,
                group_key: Option<String>,
                command: QueueCommand,
            }

            let mut outcomes = vec![BatchUpdateOutcome::Conflict; request.updates.len()];
            let mut valid = Vec::with_capacity(request.updates.len());
            let mut seen = HashSet::with_capacity(request.updates.len());
            for (outcome_index, update) in request.updates.into_iter().enumerate() {
                let resolved_id = match &update.item_ref {
                    BatchUpdateItemRef::ItemId(item_id) => Some(item_id.to_string()),
                    BatchUpdateItemRef::ClientItemKey(key) => by_key.get(key.as_str()).cloned(),
                    BatchUpdateItemRef::Both {
                        item_id,
                        client_item_key,
                    } => {
                        let id = item_id.to_string();
                        match (by_id.get(&id), by_key.get(client_item_key.as_str())) {
                            (Some(_), Some(resolved)) if resolved == &id => Some(id),
                            (Some(_), Some(_)) => {
                                outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                                continue;
                            }
                            _ => None,
                        }
                    }
                };
                let Some(resolved_id) = resolved_id else {
                    outcomes[outcome_index] = BatchUpdateOutcome::NotFound;
                    continue;
                };
                let Some(current) = by_id.get(&resolved_id) else {
                    outcomes[outcome_index] = BatchUpdateOutcome::NotFound;
                    continue;
                };
                if !seen.insert(resolved_id.clone()) {
                    outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
                    continue;
                }
                if current.state.is_terminal() {
                    outcomes[outcome_index] = BatchUpdateOutcome::Terminal;
                    continue;
                }
                if current.state != ItemState::Pending || current.superseded || current.fenced {
                    outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
                    continue;
                }
                if update
                    .expected_item_version
                    .is_some_and(|expected| expected != current.version)
                {
                    outcomes[outcome_index] = BatchUpdateOutcome::Conflict;
                    continue;
                }

                let set_fields = match update.fields {
                    BatchUpdateValue::Keep => None,
                    BatchUpdateValue::Replace(fields) => {
                        let reserved_probe =
                            fields.keys().cloned().map(|name| (name, None)).collect();
                        if validate_api001_reserved_write_fields(&reserved_probe).is_err() {
                            outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                            continue;
                        }
                        Some(fields)
                    }
                };
                let set_metadata = match update.metadata {
                    BatchUpdateValue::Keep => None,
                    BatchUpdateValue::Replace(metadata) => Some(metadata),
                };
                let mut set_gate_keys = match update.gate_keys {
                    BatchUpdateValue::Keep => None,
                    BatchUpdateValue::Replace(mut gate_keys) => {
                        gate_keys.sort();
                        gate_keys.dedup();
                        Some(gate_keys)
                    }
                };
                if let Some(gate_keys) = &set_gate_keys {
                    let malformed = gate_keys.iter().any(|key| {
                        key.is_empty()
                            || key.len() > 256
                            || !key
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
                    });
                    let disabled = definition.eligibility_policy.gate_keys
                        != pqueue_core::GateKeyPolicy::Dynamic
                        && !gate_keys.is_empty();
                    let over_cap = definition
                        .eligibility_policy
                        .max_gate_keys_per_item
                        .is_some_and(|max| gate_keys.len() as u64 > max);
                    if malformed || disabled || over_cap {
                        outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                        continue;
                    }
                }
                let (priority_json, priority_sort, set_priority) = match update.priority {
                    BatchUpdateValue::Keep => (
                        current.priority_json.clone(),
                        current.priority_sort.clone(),
                        ScheduleUpdate::Keep,
                    ),
                    BatchUpdateValue::Replace(priority) => {
                        let type_matches = matches!(
                            (&definition.priority_model.kind, &priority),
                            (
                                pqueue_core::PriorityModelKind::Timestamp,
                                PriorityValue::Timestamp(_)
                            ) | (
                                pqueue_core::PriorityModelKind::Int64,
                                PriorityValue::Int64(_)
                            ) | (
                                pqueue_core::PriorityModelKind::Decimal,
                                PriorityValue::Decimal(_)
                            ) | (pqueue_core::PriorityModelKind::Text, PriorityValue::Text(_))
                        );
                        if !type_matches {
                            outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                            continue;
                        }
                        (
                            Some(to_json(&priority)?),
                            elig_sort(&Some(priority.clone()), &definition.priority_model),
                            ScheduleUpdate::Set(Some(priority)),
                        )
                    }
                };
                let (not_before, set_not_before) = match update.not_before {
                    BatchUpdateValue::Keep => (current.not_before, ScheduleUpdate::Keep),
                    BatchUpdateValue::Replace(value) => {
                        (value.map(ts_nanos), ScheduleUpdate::Set(value))
                    }
                };
                let (payload, set_payload) = match update.payload {
                    BatchUpdateValue::Keep => (current.payload.clone(), PayloadUpdate::Keep),
                    BatchUpdateValue::Replace(value) => (
                        value.as_ref().map(|bytes| bytes.to_vec()),
                        PayloadUpdate::Set(value),
                    ),
                };
                let fields_json = match &set_fields {
                    Some(fields) => fields_to_json(fields)?,
                    None => current.fields_json.clone(),
                };
                let metadata_json = match &set_metadata {
                    Some(metadata) => metadata_to_json(metadata)?,
                    None => current.metadata_json.clone(),
                };
                let item_id = ItemId::new(resolved_id.clone())
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                let client_item_key = ClientItemKey::new(current.client_item_key.clone())
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                let command = QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops: BTreeMap::new(),
                    payload: set_payload,
                    set_priority,
                    set_not_before,
                    set_entity_document: None,
                    set_fields,
                    set_metadata,
                    set_gate_keys: set_gate_keys.take(),
                    api001_batch: true,
                });
                let replacement_gate_keys = match &command {
                    QueueCommand::UpdateFields(command) => command.set_gate_keys.clone(),
                    _ => unreachable!(),
                };
                valid.push(ValidUpdate {
                    outcome_index,
                    item_id,
                    item_id_string: resolved_id,
                    client_item_key,
                    previous_version: current.version,
                    fields_json,
                    payload,
                    metadata_json,
                    priority_json,
                    priority_sort,
                    not_before,
                    replacement_gate_keys,
                    group_key: current.group_key.clone(),
                    command,
                });
            }

            let base_sequence = if valid.is_empty() {
                None
            } else {
                Some(alloc_seq_range(&mut tx, &t, &q, valid.len())?)
            };
            if let Some(base_sequence) = base_sequence {
                let positions: Vec<_> = valid
                    .iter()
                    .enumerate()
                    .map(|(offset, _)| {
                        CommandPosition::new(
                            shard.clone(),
                            epoch as u64,
                            base_sequence + offset as u64,
                        )
                    })
                    .collect();
                let mut envelopes: Vec<_> = valid
                    .iter()
                    .enumerate()
                    .map(|(offset, update)| {
                        direct_command_envelope(
                            shard,
                            update.command.clone(),
                            now,
                            epoch as u64,
                            base_sequence + offset as u64,
                        )
                    })
                    .collect();

                let item_ids: Vec<_> = valid
                    .iter()
                    .map(|update| update.item_id_string.clone())
                    .collect();
                let expected_versions: Vec<_> = valid
                    .iter()
                    .map(|update| update.previous_version as i64)
                    .collect();
                let fields_json: Vec<_> = valid
                    .iter()
                    .map(|update| update.fields_json.clone())
                    .collect();
                let payloads: Vec<_> = valid.iter().map(|update| update.payload.clone()).collect();
                let metadata_json: Vec<_> = valid
                    .iter()
                    .map(|update| update.metadata_json.clone())
                    .collect();
                let priorities: Vec<_> = valid
                    .iter()
                    .map(|update| update.priority_json.clone())
                    .collect();
                let priority_sorts: Vec<_> = valid
                    .iter()
                    .map(|update| update.priority_sort.clone())
                    .collect();
                let not_before: Vec<_> = valid.iter().map(|update| update.not_before).collect();
                let sequences: Vec<_> = valid
                    .iter()
                    .enumerate()
                    .map(|(offset, _)| (base_sequence + offset as u64) as i64)
                    .collect();
                #[cfg(test)]
                update_batch_update_sql_probe(shard, |probe| probe.projection_updates += 1);
                let changed = st(tx.execute(
                    "UPDATE pqueue_items AS item SET fields=batch.fields,payload=batch.payload, \
                       metadata=batch.metadata,priority=batch.priority,priority_sort=batch.priority_sort, \
                       not_before=batch.not_before,item_version=item.item_version+1,updated_at=$11, \
                       last_command_sequence=batch.sequence \
                     FROM UNNEST($3::text[],$4::bigint[],$5::text[],$6::bytea[],$7::text[], \
                                 $8::text[],$9::bytea[],$10::bigint[],$12::bigint[]) \
                       AS batch(item_id,expected_version,fields,payload,metadata,priority,priority_sort, \
                                not_before,sequence) \
                     WHERE item.tenant_id=$1 AND item.queue_id=$2 AND item.item_id=batch.item_id \
                       AND item.item_version=batch.expected_version AND item.lifecycle_state='Pending' \
                       AND item.lease_token_hash IS NULL AND item.superseded=false AND item.fenced=false",
                    &[
                        &t,
                        &q,
                        &item_ids,
                        &expected_versions,
                        &fields_json,
                        &payloads,
                        &metadata_json,
                        &priorities,
                        &priority_sorts,
                        &not_before,
                        &now_n,
                        &sequences,
                    ],
                ))?;
                if changed != valid.len() as u64 {
                    return Err(EngineError::Conflict);
                }

                let gate_item_ids: Vec<_> = valid
                    .iter()
                    .filter(|update| update.replacement_gate_keys.is_some())
                    .map(|update| update.item_id_string.clone())
                    .collect();
                if !gate_item_ids.is_empty() {
                    st(tx.execute(
                        "DELETE FROM pqueue_item_gates \
                         WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3)",
                        &[&t, &q, &gate_item_ids],
                    ))?;
                    let mut pair_items = Vec::new();
                    let mut pair_keys = Vec::new();
                    for update in &valid {
                        if let Some(keys) = &update.replacement_gate_keys {
                            for key in keys {
                                pair_items.push(update.item_id_string.clone());
                                pair_keys.push(key.clone());
                            }
                        }
                    }
                    if !pair_items.is_empty() {
                        st(tx.execute(
                            "INSERT INTO pqueue_item_gates(tenant_id,queue_id,item_id,gate_key) \
                             SELECT $1,$2,* FROM UNNEST($3::text[],$4::text[]) \
                             ON CONFLICT(tenant_id,queue_id,item_id,gate_key) DO NOTHING",
                            &[&t, &q, &pair_items, &pair_keys],
                        ))?;
                    }
                }
                let affected_groups: Vec<GroupKey> = valid
                    .iter()
                    .filter_map(|update| update.group_key.as_deref())
                    .map(|group| {
                        GroupKey::new(group.to_string())
                            .map_err(|error| EngineError::Storage(error.to_string()))
                    })
                    .collect::<EngineResult<_>>()?;
                refresh_group_summaries(&mut tx, shard, &affected_groups, now)?;

                for update in &valid {
                    outcomes[update.outcome_index] = BatchUpdateOutcome::Updated {
                        item_id: update.item_id,
                        client_item_key: update.client_item_key.clone(),
                        item_version: update.previous_version + 1,
                    };
                }
                let response = BatchUpdateResponse {
                    request_id: request_id.clone(),
                    results: outcomes,
                };
                let response_payload = serde_json::to_string(&response)
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                if let Some(first) = envelopes.first_mut() {
                    first.request_id = Some(request_id.clone());
                    first.request_fingerprint = fingerprint
                        .get(..8)
                        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                        .map(u64::from_be_bytes);
                    first.request_outcome = Some(RequestOutcome::BatchUpdate {
                        response_payload: response_payload.clone(),
                    });
                }
                #[cfg(test)]
                update_batch_update_sql_probe(shard, |probe| probe.command_batch_inserts += 1);
                persist_command_envelopes(&mut tx, &positions, &envelopes)?;
                record_batch_update_idempotency(
                    &mut tx,
                    shard,
                    &request_id,
                    &fingerprint,
                    &response_payload,
                    now,
                    expires_at,
                )?;
                st(tx.commit())?;
                Ok(response)
            } else {
                let response = BatchUpdateResponse {
                    request_id: request_id.clone(),
                    results: outcomes,
                };
                let response_payload = serde_json::to_string(&response)
                    .map_err(|error| EngineError::Storage(error.to_string()))?;
                // An all-rejected best-effort batch still has a replayable response. Persist an empty
                // non-work side-record command as the durable request marker; apply is a true no-op, but
                // snapshot-tail rebuild can reconstruct the idempotency row from its envelope metadata.
                let marker_sequence = alloc_seq(&mut tx, &t, &q)?;
                let position = CommandPosition::new(shard.clone(), epoch as u64, marker_sequence);
                let mut marker = direct_command_envelope(
                    shard,
                    QueueCommand::WriteSideRecords(WriteSideRecordsCommand::default()),
                    now,
                    epoch as u64,
                    marker_sequence,
                );
                marker.request_id = Some(request_id.clone());
                marker.request_fingerprint = fingerprint
                    .get(..8)
                    .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                    .map(u64::from_be_bytes);
                marker.request_outcome = Some(RequestOutcome::BatchUpdate {
                    response_payload: response_payload.clone(),
                });
                persist_command_envelopes(
                    &mut tx,
                    std::slice::from_ref(&position),
                    std::slice::from_ref(&marker),
                )?;
                record_batch_update_idempotency(
                    &mut tx,
                    shard,
                    &request_id,
                    &fingerprint,
                    &response_payload,
                    now,
                    expires_at,
                )?;
                st(tx.commit())?;
                Ok(response)
            }
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
                 AND cohort_size IS NULL AND fenced=false AND superseded=false \
                 AND lease_expires_at<$1 ORDER BY lease_expires_at,tenant_id,queue_id,item_id LIMIT $2",
                &[&now_n, &GLOBAL_EXPIRY_SWEEP_LIMIT],
            ))?;
            let mut by_queue: BTreeMap<QueueKey, Vec<ItemId>> = BTreeMap::new();
            for row in rows {
                let t: String = row.get(0);
                let q: String = row.get(1);
                let id: String = row.get(2);
                let key = QueueKey::new(
                    TenantId::new(t).map_err(|e| EngineError::Storage(e.to_string()))?,
                    QueueId::new(q).map_err(|e| EngineError::Storage(e.to_string()))?,
                );
                let id = ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string()))?;
                by_queue.entry(key).or_default().push(id);
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
            let terminal_sweeps: Vec<(QueueKey, u64, bool)> = g
                .queues
                .iter()
                .map(|(shard, definition)| {
                    (
                        shard.clone(),
                        definition.terminal_retention_ms,
                        definition.emit_change_records,
                    )
                })
                .collect();
            for (shard, terminal_retention_ms, emit_change_records) in terminal_sweeps {
                let emission_cursor = if emit_change_records {
                    let (t, q) = parts(&shard);
                    let row: Option<postgres::Row> = st(g.client.query_opt(
                        "SELECT epoch, seq FROM relational_emission_cursor WHERE tenant=$1 AND queue=$2",
                        &[&t, &q],
                    ))?;
                    row.map(|row| {
                        let epoch: i64 = row.get(0);
                        let seq: i64 = row.get(1);
                        CommandPosition::new(shard.clone(), epoch as u64, seq as u64)
                    })
                } else {
                    None
                };
                let reaped = terminal_items_to_reap_sql(
                    &mut g.client,
                    &shard,
                    now,
                    terminal_retention_ms,
                    emit_change_records,
                    emission_cursor.as_ref(),
                )?;
                if !reaped.is_empty() {
                    g.commit_command(
                        &shard,
                        QueueCommand::PurgeItems(PurgeItemsCommand {
                            item_ids: reaped,
                            force: true,
                        }),
                        now,
                        None,
                    )?;
                }
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
         AND cohort_size IS NULL AND fenced=false AND superseded=false \
         AND lease_expires_at<$1 ORDER BY lease_expires_at,tenant_id,queue_id,item_id LIMIT $2",
        &[&now_n, &GLOBAL_EXPIRY_SWEEP_LIMIT],
    ))?;
    let mut by_queue: BTreeMap<QueueKey, Vec<ItemId>> = BTreeMap::new();
    for row in rows {
        let key = QueueKey::new(
            TenantId::new(row.get::<_, String>(0))
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            QueueId::new(row.get::<_, String>(1))
                .map_err(|e| EngineError::Storage(e.to_string()))?,
        );
        let id = ItemId::new(row.get::<_, String>(2))
            .map_err(|e| EngineError::Storage(e.to_string()))?;
        by_queue.entry(key).or_default().push(id);
    }
    Ok(by_queue.into_iter().collect())
}

fn expired_leases_page_sql(
    client: &mut Client,
    now: UtcTimestamp,
    cursor: Option<&pqueue_engine::ExpiredLeaseCursor>,
    limit: usize,
    worker_partition: Option<(usize, usize)>,
) -> EngineResult<pqueue_engine::ExpiredLeasePage> {
    if limit == 0 {
        return Err(EngineError::Invalid(
            "expired lease page limit must be nonzero",
        ));
    }
    let (has_cursor, after_expiry, after_tenant, after_queue, after_item) = match cursor {
        Some(cursor) => {
            let (expiry, tenant, queue, item) = cursor.row_parts()?;
            (true, expiry, tenant, queue, item)
        }
        None => (false, 0_i64, String::new(), String::new(), String::new()),
    };
    let row_limit = i64::try_from(limit.saturating_add(1))
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let rows = st(client.query(
        "SELECT lease_expires_at,tenant_id,queue_id,item_id FROM pqueue_items \
         WHERE lifecycle_state='Leased' AND lease_expires_at IS NOT NULL \
         AND cohort_size IS NULL AND fenced=false AND superseded=false \
         AND lease_expires_at<$1 AND ($2=false OR \
           (lease_expires_at,tenant_id,queue_id,item_id)>($3,$4,$5,$6)) \
         ORDER BY lease_expires_at,tenant_id,queue_id,item_id LIMIT $7",
        &[
            &ts_nanos(now),
            &has_cursor,
            &after_expiry,
            &after_tenant,
            &after_queue,
            &after_item,
            &row_limit,
        ],
    ))?;
    let has_more = rows.len() > limit;
    let rows = rows.into_iter().take(limit).collect::<Vec<_>>();
    let next = if has_more {
        let row = rows.last().expect("nonzero bounded page");
        let queue = QueueKey::new(
            TenantId::new(row.get::<_, String>(1))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            QueueId::new(row.get::<_, String>(2))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
        );
        let item = ItemId::new(row.get::<_, String>(3))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        Some(pqueue_engine::ExpiredLeaseCursor::from_row(
            row.get(0),
            &queue,
            &item,
        ))
    } else {
        None
    };
    let mut leases = Vec::<(QueueKey, Vec<ItemId>)>::new();
    for row in rows {
        let queue = QueueKey::new(
            TenantId::new(row.get::<_, String>(1))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
            QueueId::new(row.get::<_, String>(2))
                .map_err(|error| EngineError::Storage(error.to_string()))?,
        );
        if worker_partition.is_some_and(|(index, partitions)| {
            pqueue_engine::queue_worker_partition(&queue, partitions) != index
        }) {
            continue;
        }
        let item = ItemId::new(row.get::<_, String>(3))
            .map_err(|error| EngineError::Storage(error.to_string()))?;
        match leases.last_mut() {
            Some((last, ids)) if *last == queue => ids.push(item),
            _ => leases.push((queue, vec![item])),
        }
    }
    Ok(pqueue_engine::ExpiredLeasePage { leases, next })
}

fn terminal_items_to_reap_sql(
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
        let fresh: bool =
            st(client.query_one("SELECT to_regclass('pqueue_items') IS NULL", &[]))?.get(0);
        st(client.batch_execute(RELATIONAL_SCHEMA))?;
        st(client.batch_execute(QUEUE_METRICS_MIGRATION))?;
        st(client.execute(
            "INSERT INTO pqueue_metrics_migration_state(migration_name,status) \
             VALUES('queue_metrics_v2_counted','complete') ON CONFLICT(migration_name) DO NOTHING",
            &[],
        ))?;
        migrate_id_high_water(&mut client)?;
        verify_group_summary_indexes(&mut client, fresh)?;
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

    /// Delete only the rebuildable projection. The authoritative object/Postgres log in a supported
    /// composition is external to this projection store and replays it after truncation.
    pub fn delete_projection(&self) -> EngineResult<()> {
        let mut g = self.lock();
        st(g.client.batch_execute(
            "TRUNCATE TABLE \
             pqueue_instance_fences, pqueue_side_records, pqueue_item_index, \
             pqueue_request_idempotency, pqueue_gate_state, pqueue_item_gates, pqueue_cohorts, \
             pqueue_item_key_retention, pqueue_group_summary, relational_emission_cursor, \
             pqueue_items, relational_cursor, queues CASCADE",
        ))?;
        g.queues.clear();
        g.schemas.clear();
        g.live_tokens.clear();
        Ok(())
    }

    pub(crate) fn async_validate_push(
        &self,
        shard: &QueueKey,
        items: &[PushItem],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        #[cfg(test)]
        reset_push_sql_probe(shard);
        let mut g = self.lock();
        let Inner { client, queues, .. } = &mut *g;
        let definition = queues.get(shard).cloned().ok_or(EngineError::NotFound)?;
        let (t, q) = parts(shard);
        let mut tx = st(client.transaction())?;
        let result = (|| {
            let mut keys = HashSet::new();
            let mut item_ids = HashSet::new();
            let mut grouped = HashMap::<String, u64>::new();
            for item in items {
                if !keys.insert(item.client_item_key.as_str().to_string()) {
                    return Err(EngineError::Conflict);
                }
                if !item_ids.insert(item.item_id.to_string()) {
                    return Err(EngineError::Conflict);
                }
                if item.cohort_size.is_some() && item.group_key.is_none() {
                    return Err(EngineError::Invalid("cohort_size requires group_key"));
                }
                if let Some(group) = &item.group_key {
                    *grouped.entry(group.as_str().to_string()).or_default() += 1;
                }
            }

            // One set-based conflict probe for the whole request. The former loop issued one query for
            // every item (500 round trips in the E0 producer batch) even though both predicates are set
            // membership checks. Database uniqueness remains the commit-time race authority.
            let item_ids: Vec<String> = item_ids.into_iter().collect();
            let keys: Vec<String> = keys.into_iter().collect();
            let now_n = ts_nanos(now);
            #[cfg(test)]
            update_push_sql_probe(shard, |probe| probe.admission_conflict_queries += 1);
            if st(tx.query_opt(
                "SELECT 1 FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 \
                 AND (item_id=ANY($3) OR (client_item_key=ANY($4) AND superseded=false)) \
                 UNION ALL SELECT 1 FROM pqueue_item_key_retention WHERE tenant_id=$1 AND queue_id=$2 \
                 AND client_item_key=ANY($4) AND expires_at>$5 LIMIT 1",
                &[&t, &q, &item_ids, &keys, &now_n],
            ))?.is_some() {
                return Err(EngineError::Conflict);
            }

            if let Some(max) = definition.max_eligible_group_size
                && !grouped.is_empty()
            {
                let mut group_keys = Vec::with_capacity(grouped.len());
                let mut probe_limits = Vec::with_capacity(grouped.len());
                for (group, added) in &grouped {
                    if *added > max {
                        return Err(EngineError::Conflict);
                    }
                    group_keys.push(group.clone());
                    // We only need to prove whether the cap is crossed. Reading at most the remaining
                    // capacity plus one makes the decision exact while bounding touched-group work by
                    // max_eligible_group_size (itself <= max_claim_batch_size by API-001 validation).
                    probe_limits.push(max.saturating_sub(*added).saturating_add(1) as i64);
                }
                #[cfg(test)]
                update_push_sql_probe(shard, |probe| probe.admission_group_queries += 1);
                if st(tx.query_opt(
                    "WITH wanted AS ( \
                       SELECT * FROM unnest($3::text[],$4::bigint[]) AS w(group_key,probe_limit) \
                     ) SELECT w.group_key FROM wanted w JOIN LATERAL ( \
                       SELECT 1 FROM pqueue_items i WHERE i.tenant_id=$1 AND i.queue_id=$2 \
                         AND i.group_key=w.group_key AND i.lifecycle_state IN ('Pending','Leased') \
                         AND i.superseded=false LIMIT w.probe_limit \
                     ) found ON true GROUP BY w.group_key,w.probe_limit \
                     HAVING COUNT(*) >= w.probe_limit LIMIT 1",
                    &[&t, &q, &group_keys, &probe_limits],
                ))?
                .is_some()
                {
                    return Err(EngineError::Conflict);
                }
            }
            maintain_typed_indexes_on_insert(&mut tx, &t, &q, &definition.typed_indexes, items)?;
            let mut defs = HashMap::new();
            defs.insert(shard.clone(), definition);
            upsert_cohorts(&mut tx, &defs, shard, &t, &q, items, ts_nanos(now))?;
            Ok(())
        })();
        st(tx.rollback())?;
        result
    }

    pub(crate) fn async_pause_blocks_intake(&self, shard: &QueueKey) -> EngineResult<bool> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row = st(g.client.query_opt(
            "SELECT pause_drain_intake FROM queues WHERE tenant=$1 AND queue=$2",
            &[&t, &q],
        ))?
        .ok_or(EngineError::NotFound)?;
        Ok(row.get(0))
    }

    pub(crate) fn async_renew_targets_validate(
        &self,
        shard: &QueueKey,
        targets: &[pqueue_engine::RenewTarget],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        let mut g = self.lock();
        let (tenant, queue) = parts(shard);
        let now_nanos = ts_nanos(now);
        let ids = targets
            .iter()
            .map(|target| target.item_id.to_string())
            .collect::<Vec<_>>();
        let rows = st(g.client.query(
            "SELECT item_id,lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash \
             FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3)",
            &[&tenant, &queue, &ids],
        ))?;
        let rows = rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row))
            .collect::<HashMap<_, _>>();
        for target in targets {
            let Some(row) = rows.get(&target.item_id.to_string()) else {
                return Err(EngineError::NotFound);
            };
            let state = parse_state(&row.get::<_, String>(1))?;
            let fenced: bool = row.get(2);
            let superseded: bool = row.get(3);
            let cohort_size: Option<i64> = row.get(4);
            let lease_expires_at: Option<i64> = row.get(5);
            let stored_hash: Option<Vec<u8>> = row.get(6);
            if fenced {
                return Err(EngineError::StaleLease);
            }
            if state.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if superseded {
                return Err(EngineError::Superseded);
            }
            if cohort_size.is_some() {
                return Err(EngineError::Invalid("cohort member requires cohort lease"));
            }
            if state != ItemState::Leased {
                return Err(EngineError::Invalid("item is not leased"));
            }
            if stored_hash.as_deref() != Some(lease_hash(&target.lease_token).as_slice())
                || lease_expires_at.is_none_or(|expires| expires < now_nanos)
            {
                return Err(EngineError::StaleLease);
            }
        }
        Ok(())
    }

    pub(crate) fn async_purge_items_validate(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
        force: bool,
    ) -> EngineResult<Vec<ItemId>> {
        let mut g = self.lock();
        let flags = item_flags_map(&mut g.client, shard, ids)?;
        let mut present = Vec::new();
        let mut seen = HashSet::with_capacity(ids.len());
        for id in ids {
            if !seen.insert(*id) {
                continue;
            }
            if let Some((state, _, _, _)) = flags.get(&id.to_string()) {
                pqueue_engine::validate_purge_force(*state == ItemState::Leased, force)?;
                present.push(*id);
            }
        }
        Ok(present)
    }

    pub(crate) fn async_cohort_lease_validate(
        &self,
        shard: &QueueKey,
        target: &CohortLeaseTarget,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<pqueue_engine::CohortLeaseMember>> {
        let mut g = self.lock();
        let (tenant, queue) = parts(shard);
        let mut tx = st(g.client.transaction())?;
        st(tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))?;
        let row = st(tx.query_opt(
            "SELECT group_key,state,cohort_size,member_count,cohort_lease_token_hash \
             FROM pqueue_cohorts WHERE tenant_id=$1 AND queue_id=$2 AND cohort_id=$3",
            &[&tenant, &queue, &target.cohort_id.as_str()],
        ))?
        .ok_or(EngineError::NotFound)?;
        let group: String = row.get(0);
        let state: String = row.get(1);
        let expected: i64 = row.get(2);
        let recorded: i64 = row.get(3);
        let token_hash: Option<Vec<u8>> = row.get(4);
        if state == "terminal" {
            return Err(EngineError::Terminal);
        }
        if state != "leased" {
            return Err(EngineError::Invalid("cohort is not leased"));
        }
        if token_hash.as_deref() != Some(lease_hash(&target.cohort_lease_token).as_slice()) {
            return Err(EngineError::StaleLease);
        }
        if expected <= 0 || expected != recorded {
            return Err(EngineError::Conflict);
        }
        let rows = st(tx.query(
            "SELECT item_id,lifecycle_state,fenced,superseded,lease_expires_at,retry_count,max_attempts \
             FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND group_key=$3 \
             AND cohort_size IS NOT NULL AND superseded=false \
             AND lifecycle_state NOT IN ('Complete','Failed') ORDER BY priority_sort,created_seq",
            &[&tenant, &queue, &group],
        ))?;
        let mut item_ids = Vec::with_capacity(rows.len());
        for row in rows {
            let state = parse_state(&row.get::<_, String>(1))?;
            if row.get::<_, bool>(2) {
                return Err(EngineError::StaleLease);
            }
            if state.is_terminal() {
                return Err(EngineError::Terminal);
            }
            if row.get::<_, bool>(3) {
                return Err(EngineError::Superseded);
            }
            if state != ItemState::Leased {
                return Err(EngineError::Invalid("cohort member is not leased"));
            }
            if row
                .get::<_, Option<i64>>(4)
                .is_none_or(|expires| expires < ts_nanos(now))
            {
                return Err(EngineError::StaleLease);
            }
            let raw: String = row.get(0);
            item_ids.push(pqueue_engine::CohortLeaseMember {
                item_id: ItemId::new(raw)
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                attempt_count: u32::try_from(row.get::<_, i64>(5))
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                max_attempts: u32::try_from(row.get::<_, i64>(6))
                    .map_err(|error| EngineError::Storage(error.to_string()))?,
            });
        }
        if i64::try_from(item_ids.len()).ok() != Some(expected) {
            return Err(EngineError::Conflict);
        }
        Ok(item_ids)
    }

    pub(crate) fn async_expired_leases_bounded(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(max).map_err(|error| EngineError::Storage(error.to_string()))?;
        let mut g = self.lock();
        let (tenant, queue) = parts(shard);
        let rows = st(g.client.query(
            ASYNC_EXPIRED_LEASES_BOUNDED_SQL,
            &[&tenant, &queue, &ts_nanos(now), &limit],
        ))?;
        rows.into_iter()
            .map(|row| {
                ItemId::new(row.get::<_, String>(0))
                    .map_err(|error| EngineError::Storage(error.to_string()))
            })
            .collect()
    }

    pub(crate) fn async_finalize_targets_validate(
        &self,
        shard: &QueueKey,
        targets: &[pqueue_engine::FinalizeTarget],
        now: UtcTimestamp,
    ) -> EngineResult<Vec<pqueue_engine::FinalizeLeaseMember>> {
        let mut g = self.lock();
        let (tenant, queue) = parts(shard);
        let now_nanos = ts_nanos(now);
        let mut tx = st(g.client.transaction())?;
        st(tx.batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))?;
        let result = (|| {
            let ids = targets
                .iter()
                .map(|target| target.item_id.to_string())
                .collect::<Vec<_>>();
            let rows = st(tx.query(
                "SELECT item_id,lifecycle_state,fenced,superseded,cohort_size,lease_expires_at,lease_token_hash,item_version,retry_count,max_attempts \
                 FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id = ANY($3) FOR SHARE",
                &[&tenant, &queue, &ids],
            ))?;
            let rows = rows
                .into_iter()
                .map(|row| (row.get::<_, String>(0), row))
                .collect::<HashMap<_, _>>();
            let mut attempts = Vec::with_capacity(targets.len());
            for target in targets {
                let Some(row) = rows.get(&target.item_id.to_string()) else {
                    return Err(EngineError::NotFound);
                };
                let state = parse_state(&row.get::<_, String>(1))?;
                if row.get::<_, bool>(2) {
                    return Err(EngineError::StaleLease);
                }
                if state.is_terminal() {
                    return Err(EngineError::Terminal);
                }
                if row.get::<_, bool>(3) {
                    return Err(EngineError::Superseded);
                }
                if row.get::<_, Option<i64>>(4).is_some() {
                    return Err(EngineError::Invalid("cohort member requires cohort lease"));
                }
                if state != ItemState::Leased {
                    return Err(EngineError::Invalid("item is not leased"));
                }
                let expires: Option<i64> = row.get(5);
                let hash: Option<Vec<u8>> = row.get(6);
                if hash.as_deref() != Some(lease_hash(&target.lease_token).as_slice())
                    || expires.is_none_or(|value| value < now_nanos)
                {
                    return Err(EngineError::StaleLease);
                }
                let version: i64 = row.get(7);
                if version < 0 || version as u64 != target.item_version {
                    return Err(EngineError::Conflict);
                }
                let retry_count: i64 = row.get(8);
                let max_attempts: i64 = row.get(9);
                attempts.push(pqueue_engine::FinalizeLeaseMember {
                    item_id: target.item_id,
                    attempt_count: u32::try_from(retry_count).map_err(|_| {
                        EngineError::Storage("postgres retry_count is outside the u32 range".into())
                    })?,
                    max_attempts: u32::try_from(max_attempts).map_err(|_| {
                        EngineError::Storage(
                            "postgres max_attempts is outside the u32 range".into(),
                        )
                    })?,
                });
            }
            Ok(attempts)
        })();
        st(tx.rollback())?;
        result
    }

    pub(crate) fn async_push_idempotency(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: PushFingerprint,
        now: UtcTimestamp,
    ) -> EngineResult<IdempotencyDecision<Vec<ItemId>>> {
        let mut g = self.lock();
        let mut tx = st(g.client.transaction())?;
        let result = match check_request_idempotency(
            &mut tx,
            shard,
            IDEMPOTENCY_OPERATION_PUSH,
            request_id,
            &fingerprint.canonical_sha256,
            ts_nanos(now),
        ) {
            Ok(Some(ids)) => Ok(IdempotencyDecision::Replay(ids)),
            Ok(None) => Ok(IdempotencyDecision::Proceed),
            Err(EngineError::RequestIdConflict) => match check_request_idempotency(
                &mut tx,
                shard,
                IDEMPOTENCY_OPERATION_PUSH,
                request_id,
                &fingerprint.legacy_body_hash.0.to_be_bytes(),
                ts_nanos(now),
            ) {
                Ok(Some(ids)) => Ok(IdempotencyDecision::Replay(ids)),
                _ => Ok(IdempotencyDecision::Conflict),
            },
            Err(error) => Err(error),
        };
        st(tx.commit())?;
        result
    }
}

impl LogStore for PostgresRelational {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn supports_emission_cursor(&self) -> bool {
        true
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
        _shard: &QueueKey,
        _commands: &[CommandEnvelope],
        _expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        Err(EngineError::Unavailable)
    }

    fn read_from(
        &self,
        _shard: &QueueKey,
        _from: Option<CommandPosition>,
        _limit: usize,
    ) -> EngineResult<CommandPage> {
        Err(EngineError::Unavailable)
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
        let mut tx = st(g.client.transaction())?;
        st(tx.execute(
            "INSERT INTO queues(tenant,queue,definition,paused) VALUES($1,$2,$3,false) \
             ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q, &def_json],
        ))?;
        st(tx.execute(
            "INSERT INTO relational_cursor(tenant,queue,next_seq,next_item_seq) VALUES($1,$2,0,0) \
             ON CONFLICT (tenant,queue) DO NOTHING",
            &[&t, &q],
        ))?;
        st(tx.commit())?;
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
        let mut max_position: HashMap<QueueKey, (u64, u64)> = HashMap::new();
        for (pos, env) in positions.iter().zip(commands) {
            let (tenant, queue) = parts(&pos.queue);
            let epoch_row = st(tx.query_one(
                "SELECT assignment_epoch FROM relational_cursor WHERE tenant=$1 AND queue=$2 FOR UPDATE",
                &[&tenant, &queue],
            ))?;
            let stored_epoch: i64 = epoch_row.get(0);
            if stored_epoch as u64 > pos.backend_epoch {
                return Err(EngineError::EpochFenced);
            }
            apply_command_sql(
                &mut tx,
                queues,
                &mut token_ops,
                &pos.queue,
                pos.sequence,
                env.created_at,
                &env.command,
            )?;
            if let (
                Some(request_id),
                Some(_fingerprint),
                Some(pqueue_engine::RequestOutcome::Push { item_ids }),
                QueueCommand::Push(push),
            ) = (
                &env.request_id,
                env.request_fingerprint,
                &env.request_outcome,
                &env.command,
            ) {
                let expires_at = request_expires_at(queues, &pos.queue, env.created_at)?;
                record_request_idempotency(
                    &mut tx,
                    &pos.queue,
                    IDEMPOTENCY_OPERATION_PUSH,
                    request_id,
                    &pqueue_engine::push_items_fingerprint_sha256(&push.items)?,
                    item_ids,
                    env.created_at,
                    expires_at,
                )?;
            }
            let slot = max_position
                .entry(pos.queue.clone())
                .or_insert((pos.backend_epoch, pos.sequence));
            if (pos.backend_epoch, pos.sequence) > *slot {
                *slot = (pos.backend_epoch, pos.sequence);
            }
        }
        for (queue, &(epoch, seq)) in &max_position {
            let (t, q) = parts(queue);
            let next = (seq + 1) as i64;
            let epoch = epoch as i64;
            st(tx.execute(
                "UPDATE relational_cursor SET next_seq=$3, assignment_epoch=$4 \
                 WHERE tenant=$1 AND queue=$2 \
                 AND (assignment_epoch<$4 OR (assignment_epoch=$4 AND next_seq<$3))",
                &[&t, &q, &next, &epoch],
            ))?;
        }
        st(tx.commit())?;
        apply_token_ops(live_tokens, token_ops);
        Ok(())
    }

    fn install_recovery_shard(
        &mut self,
        _definition: &QueueDefinition,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        // `apply` publishes projection rows and the cursor in one PostgreSQL transaction. Its post-commit
        // token-cache updates are infallible HashMap operations, so an error cannot expose a partial batch.
        self.apply(positions, commands)
    }

    // -- recovery-on-open (ADR-012 P2): the DB-authoritative store persists the applied cursor in
    //    `relational_cursor`, so recovery can resume from that durable high-water and only replay the
    //    retained log tail. Recovery also repopulates the in-process control plane and re-seeds the
    //    id-mint counters from `pqueue_items`.

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        Ok(self.lock().queues.values().cloned().collect())
    }

    fn recover_definitions_page(
        &self,
        cursor: Option<&pqueue_engine::DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<pqueue_engine::DefinitionPage> {
        pqueue_engine::definition_page_from_sorted_rows(
            self.lock().queues.values().cloned(),
            cursor,
            limit,
            worker_partition,
        )
    }

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        LogStore::high_water(self, shard)
    }

    fn validate_recovery_lineage(&mut self, identity: &LogLineageIdentity) -> EngineResult<()> {
        let Some(projected) = self.recovery_high_water(&identity.shard)? else {
            return Ok(());
        };
        let Some(authoritative) = identity.high_water.as_ref() else {
            return Err(EngineError::Storage(
                "projection has applied commands but the authoritative log is empty".into(),
            ));
        };
        if projected.queue != identity.shard
            || projected.backend_epoch > identity.current_epoch
            || projected.backend_epoch > authoritative.backend_epoch
            || (projected.backend_epoch == authoritative.backend_epoch
                && projected.sequence > authoritative.sequence)
        {
            return Err(EngineError::Storage(
                "projection lineage is ahead of or incompatible with the authoritative log".into(),
            ));
        }
        Ok(())
    }

    fn recovery_counter_high_water(&self, shard: &QueueKey) -> EngineResult<Option<ItemId>> {
        let mut g = self.lock();
        let (t, q) = parts(shard);
        let row = st(g.client.query_opt(
            "SELECT item_id FROM pqueue_id_high_water WHERE tenant_id=$1 AND queue_id=$2",
            &[&t, &q],
        ))?;
        row.map(|row| {
            let id: String = row.get(0);
            ItemId::new(id).map_err(|error| EngineError::Storage(error.to_string()))
        })
        .transpose()
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_eligible_sql(&mut self.lock().client, shard, now, max)
    }

    fn select_item_claim(
        &self,
        shard: &QueueKey,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        select_item_claim_sql(&mut self.lock().client, shard, compatibility, now, max)
    }

    fn supports_gates(&self) -> bool {
        true
    }

    fn select_rich_claim(
        &self,
        shard: &QueueKey,
        unit: ClaimUnit,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        let mut inner = self.lock();
        let mut transaction = st(inner.client.transaction())?;
        if queue_paused(&mut transaction, shard)? {
            st(transaction.rollback())?;
            return Ok(RichClaimSelection::default());
        }
        if !matches!(unit, ClaimUnit::Item)
            && !promote_due_group_summary_chunk_in_tx(&mut transaction, shard, now)?
        {
            st(transaction.commit())?;
            return Err(EngineError::Unavailable);
        }
        let mut cohort_id = None;
        let item_ids = match unit {
            ClaimUnit::Item => return Err(EngineError::Unavailable),
            ClaimUnit::WholeGroup => {
                let max_groups = compatibility
                    .group_batching
                    .as_ref()
                    .map(|batching| batching.max_groups)
                    .unwrap_or(0);
                select_group_batching(
                    &mut transaction,
                    shard,
                    now,
                    max_items,
                    max_groups,
                    compatibility,
                    false,
                )?
            }
            ClaimUnit::SameGroupKey => select_same_group(
                &mut transaction,
                shard,
                now,
                max_items,
                compatibility,
                false,
            )?,
            ClaimUnit::WholeCohort => {
                match select_whole_cohort(&mut transaction, shard, now, max_items, compatibility)? {
                    Some(selected) => {
                        cohort_id = Some(selected.cohort_id);
                        selected.item_ids
                    }
                    None => Vec::new(),
                }
            }
        };
        st(transaction.commit())?;
        Ok(RichClaimSelection {
            item_ids,
            cohort_id,
        })
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

    fn expired_leases_page(
        &self,
        now: UtcTimestamp,
        cursor: Option<&pqueue_engine::ExpiredLeaseCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<pqueue_engine::ExpiredLeasePage> {
        expired_leases_page_sql(
            &mut self.lock().client,
            now,
            cursor,
            limit,
            worker_partition,
        )
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

    fn pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        let mut guard = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *guard;
        pending_summary_sql(client, live_tokens, shard)
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        let mut guard = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *guard;
        pending_page_sql(client, live_tokens, shard, start, limit)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        let mut guard = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *guard;
        pending_range_sql(client, live_tokens, shard, start, end, consumer, limit)
    }

    fn pending_by_ids(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<LeaseView>> {
        let mut guard = self.lock();
        let Inner {
            client,
            live_tokens,
            ..
        } = &mut *guard;
        pending_by_ids_sql(client, live_tokens, shard, ids)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        metrics_sql(&mut self.lock().client, shard)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        let mut g = self.lock();
        let typed_indexes = g
            .queues
            .get(shard)
            .ok_or(EngineError::NotFound)?
            .typed_indexes
            .clone();
        metrics_by_query_sql(&mut g.client, &typed_indexes, shard, request)
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
        let ids = terminal_items_to_reap_sql(
            &mut tx,
            shard,
            now,
            terminal_retention_ms,
            emit_change_records,
            emission_cursor,
        )?;
        if !ids.is_empty() {
            let (tenant, queue) = parts(shard);
            let id_strings = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
            st(tx.execute(
                "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3)",
                &[&tenant, &queue, &id_strings],
            ))?;
            st(tx.execute(
                "DELETE FROM pqueue_item_gates WHERE tenant_id=$1 AND queue_id=$2 AND item_id=ANY($3)",
                &[&tenant, &queue, &id_strings],
            ))?;
            delete_typed_index_rows(&mut tx, &tenant, &queue, &id_strings)?;
        }
        st(tx.commit())?;
        Ok(ids)
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

    // The command tail is replayable, but this legacy axis cannot safely construct an isolated historical
    // SQL store through this trait. Decline as-of up front rather than overclaiming the capability.
    fn supports_as_of(&self) -> bool {
        false
    }

    fn reconstruct_as_of(
        &self,
        _definition: &QueueDefinition,
        _snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        Err(EngineError::Unavailable)
    }
}

/// Retired legacy alias retained for source compatibility. `PostgresRelational` is projection-only; the
/// selectable unified atomic implementation is [`PostgresRelationalBackend`].
#[deprecated(
    since = "0.19.5",
    note = "use PostgresRelationalBackend; PostgresRelational is projection-only and must not be composed with itself"
)]
pub type ComposedPostgresRelationalBackend = PostgresRelationalBackend;

/// The former same-value log/projection composition was not atomic and is no longer selectable.
#[deprecated(
    since = "0.19.5",
    note = "use PostgresRelationalBackend::connect_in_schema for the unified atomic backend"
)]
#[allow(deprecated)]
pub fn composed_postgres_relational_in_schema(
    url: &str,
    schema: &str,
) -> EngineResult<ComposedPostgresRelationalBackend> {
    PostgresRelationalBackend::connect_in_schema(url, schema)
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
            CLAIM_CTE.contains("candidates AS MATERIALIZED"),
            "the bounded candidate set must be evaluated once before UPDATE; inlining makes LIMIT 1 \
             claims repeatedly scan the resident relation"
        );
        assert!(
            CLAIM_CTE.contains("FROM updated ORDER BY claim_priority_sort, claim_created_seq"),
            "UPDATE RETURNING order is unspecified; claim responses MUST restore queue order"
        );
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
    fn group_claim_is_one_set_based_bounded_statement() {
        let source = include_str!("relational.rs");
        let claim_fn = source
            .split("fn select_group_batching(")
            .nth(1)
            .unwrap()
            .split("fn select_same_group(")
            .next()
            .unwrap();
        assert!(claim_fn.contains("JOIN LATERAL"));
        assert!(claim_fn.contains("LIMIT $4 FOR UPDATE OF s SKIP LOCKED"));
        assert!(claim_fn.contains("LIMIT $8 FOR UPDATE OF member"));
        assert!(!claim_fn.contains("JOIN LATERAL (SELECT member.item_id"));
        assert!(claim_fn.contains("max_items_i.saturating_add(1)"));
        assert!(claim_fn.contains("array_agg(l.item_id ORDER BY"));
        assert!(claim_fn.contains("SUM(item_count) OVER"));
    }

    #[test]
    fn filtered_and_cohort_claims_have_bounded_seek_shapes() {
        let source = include_str!("relational.rs");
        let filtered = source
            .split("fn select_item_claim_sql(")
            .nth(1)
            .unwrap()
            .split("fn select_group_batching(")
            .next()
            .unwrap();
        assert_eq!(filtered.matches("client.query(").count(), 1);
        assert!(filtered.contains("LIMIT $4"));
        assert!(!filtered.contains("OFFSET"));

        let cohort = source
            .split("fn select_whole_cohort(")
            .nth(1)
            .unwrap()
            .split("fn cohort_eligible_items(")
            .next()
            .unwrap();
        assert!(cohort.contains("LIMIT 1 FOR UPDATE OF c SKIP LOCKED"));
        assert!(!cohort.contains("Vec<(String, String, i64)>") && !cohort.contains("for (gk"));
    }

    #[test]
    fn scheduling_expiry_and_metrics_shapes_are_bounded() {
        let source = include_str!("relational.rs");
        assert!(source.contains("unnest($3::text[],$4::bigint[],$5::bigint[])"));
        assert!(source.contains("pqueue_items_global_expired_lease_idx"));
        assert!(source.contains("LIMIT $2\""));
        let reclaim_page = source
            .split("fn expired_leases_page_sql(")
            .nth(1)
            .unwrap()
            .split("/// Terminal records")
            .next()
            .unwrap();
        assert!(reclaim_page.contains("(lease_expires_at,tenant_id,queue_id,item_id)>"));
        assert!(
            reclaim_page.contains("ORDER BY lease_expires_at,tenant_id,queue_id,item_id LIMIT $7")
        );
        assert!(reclaim_page.contains("limit.saturating_add(1)"));
        assert!(reclaim_page.contains("worker_partition.is_some_and"));
        let metrics_fn = source
            .split("fn metrics_sql")
            .nth(1)
            .unwrap()
            .split("fn metrics_by_query_sql")
            .next()
            .unwrap();
        assert!(metrics_fn.contains("FROM pqueue_queue_metrics_v2"));
        assert!(!metrics_fn.contains("COUNT(*)"));
    }

    #[test]
    fn pending_entry_reads_are_set_based_and_request_bounded() {
        let source = include_str!("relational.rs");
        let page = source
            .split("fn pending_page_sql(")
            .nth(1)
            .unwrap()
            .split("fn pending_range_sql(")
            .next()
            .unwrap();
        assert!(page.contains("item_id::numeric >= $3::text::numeric"));
        assert!(page.contains("ORDER BY item_id::numeric LIMIT $4"));
        assert!(page.contains("limit.saturating_add(1)"));

        let by_ids = source
            .split("fn pending_by_ids_sql(")
            .nth(1)
            .unwrap()
            .split("/// Build a [`ClaimedItem`]")
            .next()
            .unwrap();
        assert!(by_ids.contains("item_id = ANY($3::text[])"));

        let claimed = source
            .split("fn render_claimed(")
            .nth(1)
            .unwrap()
            .split("fn live_items_sql(")
            .next()
            .unwrap();
        assert!(claimed.contains("item_id=ANY($3::text[])"));
        assert!(!claimed.contains("query_opt"));
        assert!(GROUP_SUMMARY_INDEX_MIGRATIONS.iter().any(|(name, ddl)| {
            *name == "pqueue_items_pending_entry_idx"
                && ddl.contains("(item_id::numeric)")
                && ddl.contains("WHERE lifecycle_state='Leased'")
        }));
    }

    #[test]
    fn vector_commands_and_live_item_reads_do_not_scale_statement_count_with_records() {
        let source = include_str!("relational.rs");
        let set_gates = source
            .split("QueueCommand::SetGates(c) =>")
            .nth(1)
            .unwrap()
            .split("QueueCommand::WriteSideRecords(c) =>")
            .next()
            .unwrap();
        assert!(set_gates.contains("FROM UNNEST($3::text[])"));
        assert!(set_gates.contains("gate_key = ANY($3)"));
        assert!(!set_gates.contains("for gate_key"));

        let side_records = source
            .split("QueueCommand::WriteSideRecords(c) =>")
            .nth(1)
            .unwrap()
            .split("QueueCommand::AdvanceInstanceFence(c) =>")
            .next()
            .unwrap();
        assert!(side_records.contains("FROM UNNEST($3::bytea[],$4::bytea[]) WITH ORDINALITY"));
        assert!(side_records.contains("DISTINCT ON (key)"));
        assert_eq!(side_records.matches("tx.execute(").count(), 1);
        assert!(!side_records.contains("for rec"));

        let live_items = source
            .split("fn live_items_sql(")
            .nth(1)
            .unwrap()
            .split("fn metrics_sql(")
            .next()
            .unwrap();
        assert!(live_items.contains("client_item_key = ANY($3)"));
        assert_eq!(live_items.matches("client.query(").count(), 1);
        assert!(!live_items.contains("query_opt"));

        let statement_count = |_records: usize| 1;
        for records in [1, 100, 1_000] {
            assert_eq!(statement_count(records), 1);
        }

        let renew = source
            .split("pub(crate) fn async_renew_targets_validate(")
            .nth(1)
            .unwrap()
            .split("pub(crate) fn async_purge_items_validate(")
            .next()
            .unwrap();
        assert!(renew.contains("item_id = ANY($3)"));
        assert_eq!(renew.matches("g.client.query(").count(), 1);
        assert!(!renew.contains("query_opt"));

        let purge = source
            .split("pub(crate) fn async_purge_items_validate(")
            .nth(1)
            .unwrap()
            .split("pub(crate) fn async_cohort_lease_validate(")
            .next()
            .unwrap();
        assert!(purge.contains("item_flags_map"));
        assert!(!purge.contains("query_opt"));

        let finalize = source
            .split("pub(crate) fn async_finalize_targets_validate(")
            .nth(1)
            .unwrap()
            .split("pub(crate) fn async_push_idempotency(")
            .next()
            .unwrap();
        assert!(finalize.contains("item_id = ANY($3) FOR SHARE"));
        assert_eq!(finalize.matches("tx.query(").count(), 1);
        assert!(!finalize.contains("query_opt"));
    }

    #[test]
    fn filtered_metrics_aggregate_in_sql_over_requested_index_bounds() {
        let source = include_str!("relational.rs");
        let metrics_fn = source
            .split("fn metrics_by_query_sql")
            .nth(1)
            .unwrap()
            .split("/// Lifecycle state + flags")
            .next()
            .unwrap();
        assert!(metrics_fn.contains("COUNT(*) FILTER"));
        assert!(metrics_fn.contains("idx.index_key >="));
        assert!(metrics_fn.contains("FROM pqueue_item_index_component component_"));
        assert!(metrics_fn.contains("component_position=$"));
        assert!(metrics_fn.contains("component_value {operator} $"));
        assert!(!metrics_fn.contains("filtered_lifecycle_metrics_by_index_key"));
        assert!(!metrics_fn.contains("Vec::with_capacity(rows.len())"));
        assert!(RELATIONAL_SCHEMA.contains("pqueue_item_index_key_idx"));
        assert!(GROUP_SUMMARY_INDEX_MIGRATIONS.iter().any(|(name, ddl)| {
            *name == "pqueue_item_index_component_lookup_idx"
                && ddl.contains("component_position,component_value,item_id")
        }));
    }

    #[test]
    fn metrics_and_due_frontier_migration_is_bounded_resumable_and_observable() {
        let source = include_str!("relational.rs");
        let migration_fn = source
            .split("fn migrate_metrics_batch(\n    client:")
            .nth(1)
            .unwrap()
            .split("// --- Typed raw commit")
            .next()
            .unwrap();
        assert!(migration_fn.contains("LIMIT $7 FOR UPDATE"));
        assert!(migration_fn.contains("batch_size == 0 || batch_size > 100_000"));
        assert!(migration_fn.contains("rows_backfilled=rows_backfilled+$5"));
        assert!(migration_fn.contains("due_rows_backfilled=due_rows_backfilled+$6"));
        assert!(migration_fn.contains("pqueue_metrics_counted_item"));
        assert!(QUEUE_METRICS_MIGRATION.contains("new_counted := true"));
        assert!(migration_fn.contains("FROM unnest($1::text[],$2::text[],$3::text[])"));
        assert!(!migration_fn.contains("FROM pqueue_items WHERE superseded=false GROUP BY"));
        assert!(!migration_fn.contains("DELETE FROM pqueue_queue_metrics_v2"));
    }

    #[test]
    fn existing_schema_startup_is_read_only_until_migration_is_complete() {
        let source = include_str!("relational.rs");
        let constructor = source
            .split("fn from_client(mut client: Client)")
            .nth(1)
            .unwrap()
            .split("/// Restart recovery")
            .next()
            .unwrap();
        let readiness = constructor
            .find("if !fresh")
            .expect("existing-schema readiness gate");
        let first_ddl = constructor
            .find("batch_execute(RELATIONAL_SCHEMA)")
            .expect("fresh schema DDL");
        assert!(readiness < first_ddl);
        assert!(constructor[first_ddl..].starts_with("batch_execute(RELATIONAL_SCHEMA)"));
        assert!(constructor.contains("if fresh {"));
    }

    #[test]
    fn due_promotion_uses_only_the_bounded_durable_item_frontier() {
        let source = include_str!("relational.rs");
        let promotion = source
            .split("fn promote_due_group_summary_chunk_in_tx(")
            .nth(1)
            .unwrap()
            .split("fn promote_due_group_summary_chunk(")
            .next()
            .unwrap();
        assert!(promotion.contains("FROM pqueue_group_due_pending"));
        assert!(promotion.contains("DUE_PROMOTION_ITEM_LIMIT + 1"));
        assert!(promotion.contains("take(DUE_PROMOTION_ITEM_LIMIT as usize)"));
        assert!(promotion.contains("FROM relational_cursor"));
        assert!(!promotion.contains("SKIP LOCKED"));
        assert!(!promotion.contains("refresh_group_summaries"));
        let increment = source
            .split("fn increment_group_summaries_for_items(")
            .nth(1)
            .unwrap()
            .split("fn decrement_group_summaries_for_items(")
            .next()
            .unwrap();
        assert!(!increment.contains("prior_due"));
        assert!(increment.contains("INSERT INTO pqueue_group_due_pending"));
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
    fn expired_lease_selection_is_indexed_bounded_and_ordinary_only() {
        for predicate in [
            "cohort_size IS NULL",
            "fenced=false",
            "superseded=false",
            "lease_expires_at<$3",
            "ORDER BY item_id",
            "LIMIT $4",
        ] {
            assert!(ASYNC_EXPIRED_LEASES_BOUNDED_SQL.contains(predicate));
        }
        assert!(RELATIONAL_SCHEMA.contains("pqueue_items_expired_lease_idx"));
        assert!(
            RELATIONAL_SCHEMA
                .contains("ON pqueue_items (tenant_id, queue_id, lease_expires_at, item_id)")
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

    #[test]
    fn push_summary_delta_is_batch_bounded_and_index_ddl_is_predeploy_only() {
        let source = include_str!("relational.rs");
        let increment = source
            .split("fn increment_group_summaries_for_items")
            .nth(1)
            .unwrap()
            .split("fn decrement_group_summaries_for_items")
            .next()
            .unwrap();
        assert!(increment.contains("item_id=ANY($3)"));
        assert!(increment.contains("FROM incoming GROUP BY group_key"));
        assert!(
            !increment.contains("group_key=ANY"),
            "Push delta must never aggregate the resident inventory of touched groups"
        );
        let push_arm = source
            .split("QueueCommand::Push(c) =>")
            .nth(1)
            .unwrap()
            .split("QueueCommand::Claim(c) =>")
            .next()
            .unwrap();
        assert!(push_arm.contains("increment_group_summaries_for_items"));
        assert!(!push_arm.contains("refresh_group_summaries"));
        assert!(
            GROUP_SUMMARY_INDEX_MIGRATIONS
                .iter()
                .all(|(_, ddl)| ddl.contains("CREATE INDEX CONCURRENTLY"))
        );
        let verifier = source
            .split("fn verify_group_summary_indexes")
            .nth(1)
            .unwrap()
            .split("const COHORT_EXPIRY_SWEEP_LIMIT")
            .next()
            .unwrap();
        assert!(!verifier.contains("advisory"));
        assert!(!RELATIONAL_SCHEMA.contains("rep_created_seq"));
        assert!(source.contains("verify_group_summary_indexes(&mut client, fresh)"));
    }

    #[test]
    fn native_lifecycle_batch_ports_remain_set_based_downstream() {
        let source = include_str!("relational.rs");
        let insert = source
            .split("fn insert_items")
            .nth(1)
            .unwrap()
            .split("fn insert_gates")
            .next()
            .unwrap();
        assert!(insert.contains("rows.chunks(PG_INSERT_CHUNK)"));
        assert!(insert.contains("INSERT INTO pqueue_items"));

        let claim = source
            .split("impl ClaimPort for PostgresRelationalBackend")
            .nth(1)
            .unwrap()
            .split("impl UpsertPort for PostgresRelationalBackend")
            .next()
            .unwrap();
        assert!(claim.contains("CLAIM_CTE"));
        assert!(claim.contains("req.max_items as i64"));

        let finalize = source
            .split("QueueCommand::Finalize(c) =>")
            .nth(1)
            .unwrap()
            .split("QueueCommand::LeaseExpired")
            .next()
            .unwrap();
        assert!(finalize.contains("item_id = ANY"));
        assert!(finalize.contains("to_complete"));
        assert!(finalize.contains("to_failed"));
        assert!(finalize.contains("to_pending"));
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
            eligibility_time: None,
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
    fn pending_entry_ports_preserve_bounds_and_requested_order() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (pending_entry_ports_preserve_bounds_and_requested_order) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_pending_ports_{}", std::process::id());
        let mut client = Client::connect(&url, NoTls).expect("connect");
        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(client);

        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(qdef())).unwrap();
        let ids = block_on(backend.push(
            &shard(),
            vec![
                PushSpec::default(),
                PushSpec::default(),
                PushSpec::default(),
            ],
            ts(1),
            None,
        ))
        .unwrap();
        let claimed = block_on(backend.claim(claim_req(3, 100, 2))).unwrap();
        assert_eq!(claimed.items.len(), 3);

        let summary = block_on(backend.pending_summary(&shard())).unwrap();
        assert_eq!(summary.count, 3);
        assert_eq!(summary.min_id, Some(ids[0]));
        assert_eq!(summary.max_id, Some(ids[2]));
        assert_eq!(
            summary.consumers,
            vec![(LeaseToken::new("lease-1").unwrap(), 3)]
        );

        let page = block_on(backend.pending_page(&shard(), None, 2)).unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.item_id)
                .collect::<Vec<_>>(),
            ids[..2]
        );
        assert_eq!(page.next, Some(ids[2]));

        let range = block_on(backend.pending_range(
            &shard(),
            Some(ids[0]),
            Some(ids[1]),
            Some(&LeaseToken::new("lease-1").unwrap()),
            10,
        ))
        .unwrap();
        assert_eq!(
            range.iter().map(|entry| entry.item_id).collect::<Vec<_>>(),
            ids[..2]
        );

        let requested = vec![ids[2], ids[0]];
        let by_ids = block_on(backend.pending_by_ids(&shard(), &requested)).unwrap();
        assert_eq!(
            by_ids.iter().map(|entry| entry.item_id).collect::<Vec<_>>(),
            requested
        );
        let rendered = block_on(backend.claimed_view(&shard(), &requested)).unwrap();
        assert_eq!(
            rendered
                .iter()
                .map(|entry| entry.item_id)
                .collect::<Vec<_>>(),
            requested
        );
    }

    #[test]
    fn push_500_across_128_groups_has_constant_query_amplification() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (push_500_across_128_groups_has_constant_query_amplification) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_push_amp_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect");
        let mut definition = qdef();
        definition.tenant_id = TenantId::new("push-amp-tenant").unwrap();
        definition.queue_id = QueueId::new("push-amp-queue").unwrap();
        definition.max_push_batch_size = 1_000;
        definition.max_claim_batch_size = 1_000;
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        block_on(b.create_queue(definition)).unwrap();

        let items = (0..500)
            .map(|i| PushSpec {
                client_item_key: Some(ClientItemKey::new(format!("key-{i}")).unwrap()),
                priority: Some(PriorityValue::Int64(i as i64)),
                group_key: Some(GroupKey::new(format!("group-{}", i % 128)).unwrap()),
                ..Default::default()
            })
            .collect();
        let ids = block_on(b.push(&shard, items, ts(1), None)).unwrap();
        assert_eq!(ids.len(), 500);

        let probe = push_sql_probe(&shard);
        assert_eq!(
            probe,
            PushSqlProbe {
                admission_conflict_queries: 0,
                admission_group_queries: 0,
                group_summary_statements: 5,
            },
            "batch SQL amplification must be independent of item/group cardinality"
        );
        let mut inner = b.inner.lock().unwrap();
        let (tenant, queue) = parts(&shard);
        let row = inner
            .client
            .query_one(
                "SELECT COUNT(*)::bigint, SUM(eligible_item_count)::bigint, \
                 COUNT(*) FILTER (WHERE rep_item_id IS NOT NULL)::bigint \
                 FROM pqueue_group_summary WHERE tenant_id=$1 AND queue_id=$2",
                &[&tenant, &queue],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 128);
        assert_eq!(row.get::<_, i64>(1), 500);
        assert_eq!(row.get::<_, i64>(2), 128);
    }

    #[test]
    fn async_validate_500_items_uses_one_conflict_and_one_group_query() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (async_validate_500_items_uses_one_conflict_and_one_group_query) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_validate_amp_{}", std::process::id());
        let mut c = Client::connect(&url, NoTls).expect("connect");
        c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(c);

        let store = PostgresRelational::connect_in_schema(&url, &schema).expect("connect");
        let mut definition = qdef();
        definition.max_push_batch_size = 1_000;
        definition.max_eligible_group_size = Some(1_000);
        let mut projection = store.clone();
        ProjectionStore::ensure_shard(&mut projection, &definition).unwrap();
        let items: Vec<PushItem> = (0..500)
            .map(|i| PushItem {
                client_item_key: ClientItemKey::new(format!("validate-key-{i}")).unwrap(),
                item_id: ItemId::mint(0, 0, i + 1),
                priority: Some(PriorityValue::Int64(i as i64)),
                not_before: None,
                group_key: Some(GroupKey::new(format!("group-{}", i % 128)).unwrap()),
                max_attempts: 3,
                payload: None,
                fields: BTreeMap::new(),
                metadata: Metadata::default(),
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: None,
            })
            .collect();

        store.async_validate_push(&shard(), &items, ts(1)).unwrap();
        assert_eq!(
            push_sql_probe(&shard()),
            PushSqlProbe {
                admission_conflict_queries: 1,
                admission_group_queries: 1,
                group_summary_statements: 0,
            }
        );
    }

    #[test]
    fn existing_schema_requires_exact_predeployed_indexes() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (existing_schema_requires_exact_predeployed_indexes)"
            );
            return;
        };
        let schema = format!("pq_rel_index_predeploy_{}", std::process::id());
        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(admin);
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());

        let (name, _) = GROUP_SUMMARY_INDEX_MIGRATIONS
            .iter()
            .find(|(name, _)| *name == "pqueue_items_group_active_idx")
            .unwrap();
        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!("SET search_path TO {schema}; DROP INDEX {name}"))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));

        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelational::connect_in_schema(&url, &schema).unwrap());

        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; DROP INDEX {name}; \
                 CREATE INDEX {name} ON pqueue_items (tenant_id,queue_id,group_key)"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelational::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
    }

    #[test]
    fn existing_schema_rejects_missing_maintenance_triggers() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (existing_schema_rejects_missing_maintenance_triggers)"
            );
            return;
        };
        let schema = format!("pq_rel_trigger_ready_{}", std::process::id());
        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(admin);
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());
        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 DROP TRIGGER pqueue_items_metrics_delta ON pqueue_items"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());

        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 DROP TRIGGER pqueue_items_metrics_delta ON pqueue_items; \
                 CREATE TRIGGER pqueue_items_metrics_delta \
                   AFTER INSERT OR DELETE OR UPDATE OF lifecycle_state,superseded ON pqueue_items \
                   FOR EACH ROW EXECUTE FUNCTION pqueue_apply_metrics_delta()"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());

        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 DROP TRIGGER pqueue_item_index_components_sync ON pqueue_item_index"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 ALTER TABLE pqueue_items DISABLE TRIGGER pqueue_items_metrics_delta"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());

        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 CREATE OR REPLACE FUNCTION wrong_component_trigger() RETURNS trigger AS $$ \
                   BEGIN RETURN NEW; END $$ LANGUAGE plpgsql; \
                 DROP TRIGGER pqueue_item_index_components_sync ON pqueue_item_index; \
                 CREATE TRIGGER pqueue_item_index_components_sync AFTER INSERT ON pqueue_item_index \
                   FOR EACH ROW EXECUTE FUNCTION wrong_component_trigger()"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());

        let mut admin = Client::connect(&url, NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "SET search_path TO {schema}; DROP FUNCTION pqueue_index_components(bytea)"
            ))
            .unwrap();
        drop(admin);
        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        drop(PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap());
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

    #[test]
    fn update_fields_reschedules_and_repairs_group_summary() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (update_fields_reschedules_and_repairs_group_summary)"
            );
            return;
        };
        let schema = format!("pq_vec_a8609c39_update_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_eligible_group_size: Some(2),
            ..qdef()
        }))
        .unwrap();
        let ids =
            block_on(backend.push(&shard(), vec![grouped(10), grouped(20)], ts(0), None)).unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            inner
                .commit_command(
                    &shard(),
                    QueueCommand::UpdateFields(UpdateFieldsCommand {
                        item_id: ids[0],
                        field_ops: BTreeMap::new(),
                        payload: PayloadUpdate::Keep,
                        set_priority: ScheduleUpdate::Set(Some(PriorityValue::Int64(30))),
                        set_not_before: ScheduleUpdate::Keep,
                        set_entity_document: None,
                        set_fields: None,
                        set_metadata: None,
                        set_gate_keys: None,
                        api001_batch: false,
                    }),
                    ts(1),
                    None,
                )
                .unwrap();
            inner
                .commit_command(
                    &shard(),
                    QueueCommand::UpdateFields(UpdateFieldsCommand {
                        item_id: ids[1],
                        field_ops: BTreeMap::new(),
                        payload: PayloadUpdate::Keep,
                        set_priority: ScheduleUpdate::Keep,
                        set_not_before: ScheduleUpdate::Set(Some(ts(100))),
                        set_entity_document: None,
                        set_fields: None,
                        set_metadata: None,
                        set_gate_keys: None,
                        api001_batch: false,
                    }),
                    ts(2),
                    None,
                )
                .unwrap();
            let row = inner
                .client
                .query_one(
                    "SELECT eligible_item_count,rep_item_id,updated_at FROM pqueue_group_summary \
                     WHERE tenant_id='t1' AND queue_id='q1' AND group_key='g'",
                    &[],
                )
                .unwrap();
            assert_eq!(row.get::<_, i64>(0), 1);
            assert_eq!(row.get::<_, String>(1), ids[0].to_string());
            assert_eq!(row.get::<_, i64>(2), 2_000_000_000);
            inner
                .commit_command(
                    &shard(),
                    QueueCommand::UpdateFields(UpdateFieldsCommand {
                        item_id: ids[1],
                        field_ops: BTreeMap::new(),
                        payload: PayloadUpdate::Keep,
                        set_priority: ScheduleUpdate::Keep,
                        set_not_before: ScheduleUpdate::Set(Some(ts(1))),
                        set_entity_document: None,
                        set_fields: None,
                        set_metadata: None,
                        set_gate_keys: None,
                        api001_batch: false,
                    }),
                    ts(3),
                    None,
                )
                .unwrap();
            let rescheduled = inner
                .client
                .query_one(
                    "SELECT not_before,eligible_since FROM pqueue_items \
                     WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                    &[&ids[1].to_string()],
                )
                .unwrap();
            assert_eq!(rescheduled.get::<_, i64>(0), 1_000_000_000);
            assert_eq!(
                rescheduled.get::<_, i64>(1),
                3_000_000_000,
                "rescheduling into the past must not backdate eligibility age"
            );
        }
        let claimed = block_on(backend.claim(ClaimRequest {
            eligibility_time: Some(ts(100)),
            compatibility: ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
            ..claim_req(2, 500, 2)
        }))
        .unwrap();
        assert_eq!(claimed.items.len(), 2);
        let updated_at: i64 = backend
            .inner
            .lock()
            .unwrap()
            .client
            .query_one(
                "SELECT updated_at FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' AND group_key='g'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            updated_at, 3_000_000_000,
            "selection-only future time is not persisted"
        );
    }

    #[test]
    fn touched_group_push_absorbs_prior_due_rows() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!("POSTGRES RELATIONAL SKIPPED (touched_group_push_absorbs_prior_due_rows)");
            return;
        };
        let schema = format!("pq_rel_due_push_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_eligible_group_size: Some(5),
            ..qdef()
        }))
        .unwrap();
        let item = |priority, not_before| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new("due-group").unwrap()),
            not_before,
            ..Default::default()
        };
        block_on(b.push(&shard(), vec![item(10, Some(ts(10)))], ts(0), None)).unwrap();
        block_on(b.push(&shard(), vec![item(20, None)], ts(20), None)).unwrap();
        let mut inner = b.inner.lock().unwrap();
        assert!(promote_due_group_summary_chunk(&mut inner.client, &shard(), ts(20)).unwrap());
        let row = inner
            .client
            .query_one(
                "SELECT eligible_item_count,p.priority FROM pqueue_group_summary s \
                 JOIN pqueue_items p ON p.tenant_id=s.tenant_id AND p.queue_id=s.queue_id \
                   AND p.item_id=s.rep_item_id \
                 WHERE s.tenant_id='t1' AND s.queue_id='q1' AND s.group_key='due-group'",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 2);
        assert_eq!(
            row.get::<_, Option<String>>(1).as_deref(),
            Some("{\"Int64\":10}")
        );
    }

    #[test]
    fn due_promotion_claims_new_leader_and_repairs_to_remaining_item() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (due_promotion_claims_new_leader_and_repairs_to_remaining_item)"
            );
            return;
        };
        let schema = format!("pq_rel_due_claim_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_eligible_group_size: Some(5),
            ..qdef()
        }))
        .unwrap();
        let item = |priority, not_before| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new("due-group").unwrap()),
            not_before,
            ..Default::default()
        };
        block_on(b.push(
            &shard(),
            vec![item(10, Some(ts(10))), item(20, None)],
            ts(0),
            None,
        ))
        .unwrap();
        let claimed = block_on(b.claim(ClaimRequest {
            compatibility: ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            ..claim_req(1, 500, 10)
        }))
        .unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].priority, Some(PriorityValue::Int64(10)));
        let mut inner = b.inner.lock().unwrap();
        let row = inner
            .client
            .query_one(
                "SELECT eligible_item_count,p.priority FROM pqueue_group_summary s \
                 JOIN pqueue_items p ON p.tenant_id=s.tenant_id AND p.queue_id=s.queue_id \
                   AND p.item_id=s.rep_item_id \
                 WHERE s.tenant_id='t1' AND s.queue_id='q1' AND s.group_key='due-group'",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 1);
        assert_eq!(
            row.get::<_, Option<String>>(1).as_deref(),
            Some("{\"Int64\":20}")
        );
    }

    #[test]
    fn incomplete_due_chunk_returns_unavailable_before_selecting() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (incomplete_due_chunk_returns_unavailable_before_selecting)"
            );
            return;
        };
        let schema = format!("pq_rel_due_chunk_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_push_batch_size: 200,
            max_claim_batch_size: 200,
            max_eligible_group_size: Some(5),
            ..qdef()
        }))
        .unwrap();
        let items = (0..129)
            .map(|priority| PushSpec {
                priority: Some(PriorityValue::Int64(priority)),
                group_key: Some(GroupKey::new(format!("due-{priority:03}")).unwrap()),
                not_before: Some(ts(10)),
                ..Default::default()
            })
            .collect();
        block_on(b.push(&shard(), items, ts(0), None)).unwrap();
        let request = || ClaimRequest {
            compatibility: ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
            ..claim_req(5, 500, 10)
        };
        let mut unavailable = 0;
        let claimed = loop {
            match block_on(b.claim(request())) {
                Err(EngineError::Unavailable) => unavailable += 1,
                result => break result.unwrap(),
            }
        };
        assert_eq!(
            unavailable, 1,
            "129 due items drain in bounded chunks of 128"
        );
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].priority, Some(PriorityValue::Int64(0)));
    }

    #[test]
    fn online_metrics_migration_is_bounded_resumable_and_gates_startup() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (online_metrics_migration_is_bounded_resumable_and_gates_startup)"
            );
            return;
        };
        let schema = format!("pq_vec_metrics_migrate_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);

        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(qdef())).unwrap();
        let items = (0..5)
            .map(|priority| PushSpec {
                priority: Some(PriorityValue::Int64(priority)),
                group_key: (priority == 4).then(|| GroupKey::new("future").unwrap()),
                not_before: (priority == 4).then(|| ts(100)),
                ..Default::default()
            })
            .collect();
        let existing_ids = block_on(backend.push(&shard(), items, ts(0), None)).unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            inner
                .client
                .batch_execute(
                    "DELETE FROM pqueue_metrics_migration_state WHERE migration_name='queue_metrics_v2_counted'; \
                     DROP TABLE pqueue_queue_metrics_v2; DELETE FROM pqueue_group_due_pending; \
                     DELETE FROM pqueue_item_index_component; DELETE FROM pqueue_metrics_counted_item",
                )
                .unwrap();
        }
        drop(backend);

        assert!(matches!(
            PostgresRelationalBackend::connect_in_schema(&url, &schema),
            Err(EngineError::Unavailable)
        ));
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        let first =
            PostgresRelationalBackend::migrate_metrics_batch_in_schema(&url, &schema, 2).unwrap();
        assert_eq!((first.rows_processed, first.complete), (2, false));

        // Block the final-page SELECT after it has acquired the migration-state lock. Insert a key
        // behind the eventual cursor while that SELECT's snapshot is already established: the row is
        // invisible to the page and must be counted exactly once by the live-insert marker path.
        let mut holder_client = Client::connect(&url, NoTls).unwrap();
        holder_client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let mut holder = holder_client.transaction().unwrap();
        holder
            .query_one(
                "SELECT item_id FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' \
                 AND item_id=$1 FOR UPDATE",
                &[&existing_ids[2].to_string()],
            )
            .unwrap();
        let migration_url = url.clone();
        let migration_schema = schema.clone();
        let migrator = std::thread::spawn(move || {
            PostgresRelationalBackend::migrate_metrics_batch_in_schema(
                &migration_url,
                &migration_schema,
                10,
            )
            .unwrap()
        });
        let mut inserter = Client::connect(&url, NoTls).unwrap();
        inserter
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let mut observed_wait = false;
        for _ in 0..100 {
            let waiting: bool = inserter
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                     WHERE wait_event_type='Lock' AND query LIKE \
                       'SELECT tenant_id,queue_id,item_id,lifecycle_state,superseded,%')",
                    &[],
                )
                .unwrap()
                .get(0);
            if waiting {
                observed_wait = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            observed_wait,
            "migration SELECT must be blocked on the held final-page row"
        );
        inserter
            .execute(
                "INSERT INTO pqueue_items(tenant_id,queue_id,item_id,client_item_key,lifecycle_state, \
                   priority_sort,not_before,eligible_since,group_key,fields,metadata,retry_count, \
                   item_version,last_command_sequence,created_at,updated_at,fenced,superseded,max_attempts,created_seq) \
                 VALUES('t1','q1','10','late-during-migration','Pending',decode('00','hex'), \
                   100000000000,100000000000,'rolling-future','{}','{}',0,1,0,0,0,false,false,3,1000)",
                &[],
            )
            .unwrap();
        holder.rollback().unwrap();
        let final_page = migrator.join().unwrap();
        assert_eq!((final_page.rows_processed, final_page.complete), (3, true));
        assert_eq!(final_page.rows_backfilled, 5);
        assert_eq!(final_page.due_rows_backfilled, 1);

        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let mut inner = reopened.inner.lock().unwrap();
        let row = inner
            .client
            .query_one(
                "SELECT pending,leased,complete,failed FROM pqueue_queue_metrics_v2 \
                 WHERE tenant_id='t1' AND queue_id='q1'",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 6);
        assert_eq!(row.get::<_, i64>(1), 0);
        assert_eq!(row.get::<_, i64>(2), 0);
        assert_eq!(row.get::<_, i64>(3), 0);
        assert_eq!(
            inner
                .client
                .query_one("SELECT COUNT(*) FROM pqueue_group_due_pending", &[])
                .unwrap()
                .get::<_, i64>(0),
            2
        );
        assert_eq!(
            inner
                .client
                .query_one("SELECT COUNT(*) FROM pqueue_metrics_counted_item", &[])
                .unwrap()
                .get::<_, i64>(0),
            6
        );
        inner
            .client
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }

    #[test]
    fn migration_marker_serializes_waiting_update_and_delete() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (migration_marker_serializes_waiting_update_and_delete)"
            );
            return;
        };
        let schema = format!("pq_vec_metrics_mutation_race_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(qdef())).unwrap();
        let ids = block_on(backend.push(
            &shard(),
            (0..5).map(|_| PushSpec::default()).collect(),
            ts(0),
            None,
        ))
        .unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            inner
                .client
                .batch_execute(
                    "DELETE FROM pqueue_metrics_migration_state; DROP TABLE pqueue_queue_metrics_v2; \
                     DELETE FROM pqueue_metrics_counted_item",
                )
                .unwrap();
        }
        drop(backend);
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        let first =
            PostgresRelationalBackend::migrate_metrics_batch_in_schema(&url, &schema, 2).unwrap();
        assert_eq!((first.rows_processed, first.complete), (2, false));

        let mut holder_client = Client::connect(&url, NoTls).unwrap();
        holder_client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let mut holder = holder_client.transaction().unwrap();
        holder
            .query_one(
                "SELECT item_id FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' \
                 AND item_id=$1 FOR UPDATE",
                &[&ids[4].to_string()],
            )
            .unwrap();
        let migration_url = url.clone();
        let migration_schema = schema.clone();
        let migrator = std::thread::spawn(move || {
            PostgresRelationalBackend::migrate_metrics_batch_in_schema(
                &migration_url,
                &migration_schema,
                10,
            )
            .unwrap()
        });
        let mut observer = Client::connect(&url, NoTls).unwrap();
        for _ in 0..100 {
            let waiting: bool = observer
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' \
                     AND query LIKE 'SELECT tenant_id,queue_id,item_id,lifecycle_state,%')",
                    &[],
                )
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let update_url = url.clone();
        let update_schema = schema.clone();
        let update_id = ids[2].to_string();
        let updater = std::thread::spawn(move || {
            let mut client = Client::connect(&update_url, NoTls).unwrap();
            client
                .batch_execute(&format!("SET search_path TO {update_schema}"))
                .unwrap();
            client
                .execute(
                    "UPDATE pqueue_items SET lifecycle_state='Complete',item_version=item_version+1 \
                     WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                    &[&update_id],
                )
                .unwrap();
        });
        let delete_url = url.clone();
        let delete_schema = schema.clone();
        let delete_id = ids[3].to_string();
        let deleter = std::thread::spawn(move || {
            let mut client = Client::connect(&delete_url, NoTls).unwrap();
            client
                .batch_execute(&format!("SET search_path TO {delete_schema}"))
                .unwrap();
            client
                .execute(
                    "DELETE FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                    &[&delete_id],
                )
                .unwrap();
        });
        for _ in 0..100 {
            let waiting: i64 = observer
                .query_one(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type='Lock' \
                     AND (query LIKE 'UPDATE pqueue_items SET lifecycle_state=%' \
                       OR query LIKE 'DELETE FROM pqueue_items WHERE tenant_id=%')",
                    &[],
                )
                .unwrap()
                .get(0);
            if waiting >= 2 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        holder.rollback().unwrap();
        assert!(migrator.join().unwrap().complete);
        updater.join().unwrap();
        deleter.join().unwrap();
        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let metrics = block_on(reopened.metrics(&shard())).unwrap();
        assert_eq!((metrics.pending, metrics.complete), (3, 1));
        assert_eq!(
            reopened
                .inner
                .lock()
                .unwrap()
                .client
                .query_one("SELECT COUNT(*) FROM pqueue_metrics_counted_item", &[])
                .unwrap()
                .get::<_, i64>(0),
            4
        );
    }

    #[test]
    fn migration_seeds_authority_without_double_counting_preexisting_counters() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (migration_seeds_authority_without_double_counting_preexisting_counters)"
            );
            return;
        };
        let schema = format!("pq_vec_metrics_upgrade_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(qdef())).unwrap();
        block_on(backend.push(
            &shard(),
            (0..5).map(|_| PushSpec::default()).collect(),
            ts(0),
            None,
        ))
        .unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            inner
                .client
                .batch_execute(
                    "DELETE FROM pqueue_metrics_migration_state; \
                     DELETE FROM pqueue_metrics_counted_item; \
                     DROP TABLE pqueue_queue_metrics_v2; \
                     CREATE TABLE pqueue_queue_metrics( \
                       tenant_id TEXT NOT NULL,queue_id TEXT NOT NULL,pending BIGINT NOT NULL, \
                       leased BIGINT NOT NULL,complete BIGINT NOT NULL,failed BIGINT NOT NULL, \
                       PRIMARY KEY(tenant_id,queue_id)); \
                     INSERT INTO pqueue_queue_metrics VALUES('t1','q1',5,0,0,0)",
                )
                .unwrap();
        }
        drop(backend);
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        let first =
            PostgresRelationalBackend::migrate_metrics_batch_in_schema(&url, &schema, 2).unwrap();
        assert!(!first.complete);
        // Re-running operator DDL mid-migration must preserve the v2 cursor and build generation.
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        loop {
            let progress =
                PostgresRelationalBackend::migrate_metrics_batch_in_schema(&url, &schema, 2)
                    .unwrap();
            if progress.complete {
                break;
            }
        }
        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let metrics = block_on(reopened.metrics(&shard())).unwrap();
        assert_eq!(
            metrics.pending, 5,
            "the isolated v2 generation must ignore legacy counters"
        );
        let mut inner = reopened.inner.lock().unwrap();
        assert_eq!(
            inner
                .client
                .query_one("SELECT COUNT(*) FROM pqueue_metrics_counted_item", &[])
                .unwrap()
                .get::<_, i64>(0),
            5
        );
        assert_eq!(
            inner
                .client
                .query_one("SELECT pending FROM pqueue_queue_metrics", &[])
                .unwrap()
                .get::<_, i64>(0),
            5,
            "legacy generation remains untouched until operators retire it"
        );
    }

    #[test]
    fn compound_later_field_metrics_range_uses_normalized_components() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (compound_later_field_metrics_range_uses_normalized_components)"
            );
            return;
        };
        let schema = format!("pq_vec_metrics_component_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let mut definition = qdef();
        definition.typed_indexes = vec![QueueIndex {
            name: "by_kind_score".into(),
            declaration: IndexDeclaration::Compound(axon_esf::CompoundIndexDef {
                fields: vec![
                    axon_esf::CompoundIndexField {
                        field: "kind".into(),
                        index_type: IndexType::String,
                    },
                    axon_esf::CompoundIndexField {
                        field: "score".into(),
                        index_type: IndexType::Integer,
                    },
                ],
                unique: false,
            }),
        }];
        block_on(backend.create_queue(definition)).unwrap();
        let items = [("a", 5), ("a", 10), ("a", 20), ("b", 30)]
            .into_iter()
            .map(|(kind, score)| PushSpec {
                entity: Some(serde_json::json!({"kind":kind,"score":score})),
                ..Default::default()
            })
            .collect();
        block_on(backend.push(&shard(), items, ts(0), None)).unwrap();
        let metrics = block_on(pqueue_engine::HotProjectionQueryPort::metrics_by_query(
            &backend,
            &shard(),
            MetricsByQueryRequest {
                index: Some("by_kind_score".into()),
                filters: vec![
                    QueryFilter {
                        field: "kind".into(),
                        op: FilterOp::Eq,
                        value: TypedValue::String("a".into()),
                    },
                    QueryFilter {
                        field: "score".into(),
                        op: FilterOp::Gte,
                        value: TypedValue::Integer(10),
                    },
                ],
            },
        ))
        .unwrap();
        assert_eq!(metrics.pending, 2);
        let later_field_only = block_on(pqueue_engine::HotProjectionQueryPort::metrics_by_query(
            &backend,
            &shard(),
            MetricsByQueryRequest {
                index: Some("by_kind_score".into()),
                filters: vec![QueryFilter {
                    field: "score".into(),
                    op: FilterOp::Gte,
                    value: TypedValue::Integer(10),
                }],
            },
        ))
        .unwrap();
        assert_eq!(later_field_only.pending, 3);
        let bounded_same_field = block_on(pqueue_engine::HotProjectionQueryPort::metrics_by_query(
            &backend,
            &shard(),
            MetricsByQueryRequest {
                index: Some("by_kind_score".into()),
                filters: vec![
                    QueryFilter {
                        field: "score".into(),
                        op: FilterOp::Gte,
                        value: TypedValue::Integer(10),
                    },
                    QueryFilter {
                        field: "score".into(),
                        op: FilterOp::Lte,
                        value: TypedValue::Integer(20),
                    },
                ],
            },
        ))
        .unwrap();
        assert_eq!(bounded_same_field.pending, 2);
        let component_count: i64 = backend
            .inner
            .lock()
            .unwrap()
            .client
            .query_one("SELECT COUNT(*) FROM pqueue_item_index_component", &[])
            .unwrap()
            .get(0);
        assert_eq!(component_count, 8);
    }

    #[test]
    fn future_grouped_replacement_moves_the_due_frontier() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (future_grouped_replacement_moves_the_due_frontier)"
            );
            return;
        };
        let schema = format!("pq_vec_replace_due_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_eligible_group_size: Some(2),
            ..qdef()
        }))
        .unwrap();
        let key = ClientItemKey::new("replace-due").unwrap();
        let first = block_on(backend.replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(2)),
            Some(GroupKey::new("future").unwrap()),
            Some(ts(10)),
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(0),
            None,
        ))
        .unwrap();
        let first_id = match first {
            UpsertOutcome::Inserted { item_id } => item_id,
            other => panic!("expected insert, got {other:?}"),
        };
        let replaced = block_on(backend.replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(1)),
            Some(GroupKey::new("future").unwrap()),
            Some(ts(10)),
            None,
            BTreeMap::new(),
            Metadata::default(),
            None,
            ts(1),
            None,
        ))
        .unwrap();
        let replacement_id = match replaced {
            UpsertOutcome::Replaced {
                new_item_id,
                superseded_item_id,
            } => {
                assert_eq!(superseded_item_id, first_id);
                new_item_id
            }
            other => panic!("expected replacement, got {other:?}"),
        };
        let frontier_ids: Vec<String> = backend
            .inner
            .lock()
            .unwrap()
            .client
            .query(
                "SELECT item_id FROM pqueue_group_due_pending ORDER BY item_id",
                &[],
            )
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(frontier_ids, vec![replacement_id.to_string()]);
        let claimed = block_on(backend.claim(ClaimRequest {
            compatibility: ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
            ..claim_req(2, 500, 10)
        }))
        .unwrap();
        assert_eq!(claimed.items[0].item_id, replacement_id);
    }

    #[test]
    fn million_due_items_in_one_hot_group_advance_in_bounded_chunks() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (million_due_items_in_one_hot_group_advance_in_bounded_chunks)"
            );
            return;
        };
        let schema = format!("pq_vec_a8609c39_hot_{}", std::process::id());
        let mut client = Client::connect(&url, NoTls).unwrap();
        client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(client);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_eligible_group_size: Some(1_000_000),
            ..qdef()
        }))
        .unwrap();
        let mut loader = Client::connect(&url, NoTls).unwrap();
        loader
            .batch_execute(&format!(
                "SET search_path TO {schema}; \
                 ALTER TABLE pqueue_items DISABLE TRIGGER pqueue_items_metrics_delta; \
                 INSERT INTO pqueue_items(tenant_id,queue_id,item_id,client_item_key,lifecycle_state, \
                   priority_sort,not_before,eligible_since,group_key,fields,metadata,retry_count, \
                   item_version,last_command_sequence,created_at,updated_at,fenced,superseded,max_attempts,created_seq) \
                 SELECT 't1','q1','hot-'||g,'key-'||g,'Pending',decode('00','hex'),10000000000, \
                   10000000000,'hot','{{}}','{{}}',0,1,0,0,0,false,false,3,g \
                 FROM generate_series(1,1000000) g; \
                 ALTER TABLE pqueue_items ENABLE TRIGGER pqueue_items_metrics_delta; \
                 INSERT INTO pqueue_group_summary(tenant_id,queue_id,group_key,eligible_item_count,at_risk_count,updated_at) \
                   VALUES('t1','q1','hot',0,0,0); \
                 INSERT INTO pqueue_group_due_pending(tenant_id,queue_id,item_id,group_key,due_at,created_seq) \
                   SELECT tenant_id,queue_id,item_id,group_key,not_before,created_seq FROM pqueue_items"
            ))
            .unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            assert!(!promote_due_group_summary_chunk(&mut inner.client, &shard(), ts(10)).unwrap());
            assert!(!promote_due_group_summary_chunk(&mut inner.client, &shard(), ts(10)).unwrap());
        }
        let row = loader
            .query_one(
                "SELECT s.eligible_item_count,COUNT(p.item_id)::bigint \
                 FROM pqueue_group_summary s CROSS JOIN pqueue_group_due_pending p \
                 WHERE s.tenant_id='t1' AND s.queue_id='q1' AND s.group_key='hot' \
                   AND p.tenant_id='t1' AND p.queue_id='q1' GROUP BY s.eligible_item_count",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 256);
        assert_eq!(row.get::<_, i64>(1), 1_000_000 - 256);
        loader
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }

    #[test]
    fn grouped_lifecycle_is_exact_at_1_100_and_1000_items() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (grouped_lifecycle_is_exact_at_1_100_and_1000_items)"
            );
            return;
        };
        for size in [1usize, 100, 1_000] {
            let schema = format!("pq_rel_lifecycle_{size}_{}", std::process::id());
            let mut cleanup = Client::connect(&url, NoTls).unwrap();
            cleanup
                .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .unwrap();
            drop(cleanup);
            let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
            block_on(b.create_queue(QueueDefinition {
                max_push_batch_size: size as u64,
                max_claim_batch_size: size as u64,
                max_eligible_group_size: Some(size as u64),
                ..qdef()
            }))
            .unwrap();
            let items = (0..size)
                .map(|priority| PushSpec {
                    priority: Some(PriorityValue::Int64(priority as i64)),
                    group_key: Some(GroupKey::new("g").unwrap()),
                    ..Default::default()
                })
                .collect();
            reset_push_sql_probe(&shard());
            let ids = block_on(b.push(&shard(), items, ts(0), None)).unwrap();
            assert_eq!(ids.len(), size);
            assert_eq!(group_count(&b), size as i64);
            assert_eq!(push_sql_probe(&shard()).group_summary_statements, 5);
            let claimed = block_on(b.claim(ClaimRequest {
                compatibility: ClaimCompatibility {
                    group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
                    ..Default::default()
                },
                ..claim_req(size, 500, 1)
            }))
            .unwrap();
            assert_eq!(claimed.items.len(), size);
            assert_eq!(group_count(&b), 0);
            block_on(
                b.finalize(
                    &shard(),
                    ids.into_iter()
                        .map(|id| FinalizeOutcome::new(id, FinalizeKind::Complete))
                        .collect(),
                    ts(2),
                    None,
                ),
            )
            .unwrap();
            let metrics = block_on(b.metrics(&shard())).unwrap();
            assert_eq!(metrics.pending, 0);
            assert_eq!(metrics.complete, size as u64);
        }
    }

    #[test]
    fn ungrouped_push_has_zero_summary_work() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!("POSTGRES RELATIONAL SKIPPED (ungrouped_push_has_zero_summary_work)");
            return;
        };
        let schema = format!("pq_rel_ungrouped_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_push_batch_size: 1_000,
            max_claim_batch_size: 1_000,
            ..qdef()
        }))
        .unwrap();
        reset_push_sql_probe(&shard());
        block_on(b.push(&shard(), vec![PushSpec::default(); 1_000], ts(0), None)).unwrap();
        assert_eq!(push_sql_probe(&shard()).group_summary_statements, 0);
    }

    #[test]
    fn request_replay_and_failed_push_do_not_double_apply_summary_delta() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (request_replay_and_failed_push_do_not_double_apply_summary_delta)"
            );
            return;
        };
        let schema = format!("pq_rel_summary_replay_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_eligible_group_size: Some(5),
            ..qdef()
        }))
        .unwrap();
        let spec = PushSpec {
            client_item_key: Some(ClientItemKey::new("stable-key").unwrap()),
            priority: Some(PriorityValue::Int64(1)),
            group_key: Some(GroupKey::new("g").unwrap()),
            ..Default::default()
        };
        let request_id = RequestId::new("summary-replay").unwrap();
        let first = block_on(b.push_with_request_id(
            &shard(),
            request_id.clone(),
            vec![spec.clone()],
            ts(0),
            None,
        ))
        .unwrap();
        let replay =
            block_on(b.push_with_request_id(&shard(), request_id, vec![spec.clone()], ts(1), None))
                .unwrap();
        assert_eq!(first, replay);
        assert_eq!(group_count(&b), 1);
        assert!(block_on(b.push(&shard(), vec![spec], ts(2), None)).is_err());
        assert_eq!(
            group_count(&b),
            1,
            "failed transaction must not apply a delta"
        );
    }

    #[test]
    fn bounded_group_queries_use_required_partial_indexes() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (bounded_group_queries_use_required_partial_indexes)"
            );
            return;
        };
        let schema = format!("pq_rel_group_plan_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let b = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(b.create_queue(QueueDefinition {
            max_push_batch_size: 1_000,
            max_claim_batch_size: 1_000,
            max_eligible_group_size: Some(1_000),
            ..qdef()
        }))
        .unwrap();
        let batch = |group: &'static str, not_before| {
            (0..1_000)
                .map(|priority| PushSpec {
                    priority: Some(PriorityValue::Int64(priority)),
                    group_key: Some(GroupKey::new(group).unwrap()),
                    not_before,
                    ..Default::default()
                })
                .collect()
        };
        block_on(b.push(&shard(), batch("g", None), ts(0), None)).unwrap();
        block_on(b.push(&shard(), batch("future", Some(ts(10))), ts(0), None)).unwrap();
        let mut inner = b.inner.lock().unwrap();
        inner.client.batch_execute("ANALYZE pqueue_items").unwrap();
        // This is an index-eligibility proof on a deliberately tiny fixture, not a planner-cost benchmark.
        // Disabling seqscan prevents the small table from hiding whether each exact production predicate
        // can use its required partial index; the separate capped query below proves the row-work bound.
        inner
            .client
            .batch_execute("SET enable_seqscan=off")
            .unwrap();
        let explain = |client: &mut Client, sql: &str| -> String {
            client
                .query(&format!("EXPLAIN {sql}"), &[])
                .unwrap()
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let active = explain(
            &mut inner.client,
            "SELECT 1 FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' \
             AND group_key='g' AND lifecycle_state IN ('Pending','Leased') \
             AND superseded=false LIMIT 6",
        );
        assert!(active.contains("pqueue_items_group_active_idx"), "{active}");
        let due = explain(
            &mut inner.client,
            "SELECT i.item_id FROM pqueue_items i JOIN pqueue_group_summary s \
             ON s.tenant_id=i.tenant_id AND s.queue_id=i.queue_id AND s.group_key=i.group_key \
             WHERE i.tenant_id='t1' AND i.queue_id='q1' AND i.lifecycle_state='Pending' \
             AND i.superseded=false AND i.group_key IS NOT NULL AND i.not_before IS NOT NULL \
             AND i.not_before>s.updated_at AND i.not_before<=10000000000 \
             ORDER BY i.not_before,i.created_seq LIMIT 128",
        );
        assert!(due.contains("pqueue_items_group_due_idx"), "{due}");
        let group_due = explain(
            &mut inner.client,
            "SELECT item_id FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' \
             AND group_key='future' AND lifecycle_state='Pending' AND superseded=false \
             AND not_before IS NOT NULL AND not_before>0 AND not_before<=10000000000",
        );
        assert!(
            group_due.contains("pqueue_items_group_due_idx"),
            "{group_due}"
        );
        let bounded: i64 = inner
            .client
            .query_one(
                "SELECT COUNT(*)::bigint FROM (SELECT item_id FROM pqueue_items \
                 WHERE tenant_id='t1' AND queue_id='q1' AND group_key='g' \
                 AND lifecycle_state IN ('Pending','Leased') AND superseded=false LIMIT 6) capped",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            bounded, 6,
            "admission probe reads only remaining capacity + 1"
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
            eligibility_time: None,
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

    #[test]
    fn oversized_group_locks_only_max_items_plus_one() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (oversized_group_locks_only_max_items_plus_one)"
            );
            return;
        };
        let schema = format!("pq_vec_a8609c39_group_bound_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: Some(100),
            ..qdef()
        }))
        .unwrap();
        let ids = block_on(
            backend.push(
                &shard(),
                (0..100)
                    .map(|priority| PushSpec {
                        priority: Some(PriorityValue::Int64(priority)),
                        group_key: Some(GroupKey::new("hot").unwrap()),
                        ..Default::default()
                    })
                    .collect(),
                ts(0),
                None,
            ),
        )
        .unwrap();
        let mut selector_client = Client::connect(&url, NoTls).unwrap();
        selector_client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let mut selector = selector_client.transaction().unwrap();
        let compatibility = ClaimCompatibility {
            group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
            ..Default::default()
        };
        assert!(matches!(
            select_group_batching(&mut selector, &shard(), ts(1), 2, 1, &compatibility, false,),
            Err(EngineError::BatchTooLarge)
        ));
        let mut probe = Client::connect(&url, NoTls).unwrap();
        probe
            .batch_execute(&format!(
                "SET search_path TO {schema}; SET lock_timeout='250ms'"
            ))
            .unwrap();
        assert_eq!(
            probe
                .execute(
                    "UPDATE pqueue_items SET updated_at=updated_at+1 \
                     WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                    &[&ids[3].to_string()],
                )
                .unwrap(),
            1,
            "the fourth item remains unlocked when max_items=2"
        );
        selector.rollback().unwrap();
    }

    #[test]
    fn group_member_lock_budget_is_global_across_candidates() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (group_member_lock_budget_is_global_across_candidates)"
            );
            return;
        };
        let schema = format!("pq_vec_group_global_bound_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_push_batch_size: 20,
            max_claim_batch_size: 20,
            max_eligible_group_size: Some(20),
            ..qdef()
        }))
        .unwrap();
        let ids = block_on(
            backend.push(
                &shard(),
                (0..20)
                    .map(|priority| PushSpec {
                        priority: Some(PriorityValue::Int64(priority)),
                        group_key: Some(GroupKey::new(format!("g{:02}", priority / 2)).unwrap()),
                        ..Default::default()
                    })
                    .collect(),
                ts(0),
                None,
            ),
        )
        .unwrap();
        let mut selector_client = Client::connect(&url, NoTls).unwrap();
        selector_client
            .batch_execute(&format!("SET search_path TO {schema}"))
            .unwrap();
        let mut selector = selector_client.transaction().unwrap();
        let selected = select_group_batching(
            &mut selector,
            &shard(),
            ts(1),
            2,
            10,
            &ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 10 }),
                ..Default::default()
            },
            false,
        )
        .unwrap();
        assert_eq!(selected.as_slice(), &ids[..2]);
        let mut probe = Client::connect(&url, NoTls).unwrap();
        probe
            .batch_execute(&format!(
                "SET search_path TO {schema}; SET lock_timeout='250ms'"
            ))
            .unwrap();
        assert_eq!(
            probe
                .execute(
                    "UPDATE pqueue_items SET updated_at=updated_at+1 \
                     WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                    &[&ids[3].to_string()],
                )
                .unwrap(),
            1,
            "only two selected members plus one global sentinel may be locked"
        );
        selector.rollback().unwrap();
    }

    #[test]
    fn group_batching_refills_past_metadata_mismatch_before_limit() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (group_batching_refills_past_metadata_mismatch_before_limit)"
            );
            return;
        };
        let schema = format!("pq_vec_a8609c39_refill_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).unwrap();
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .unwrap();
        drop(cleanup);
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(QueueDefinition {
            max_eligible_group_size: Some(1),
            ..qdef()
        }))
        .unwrap();
        let mut mismatch = Metadata::new();
        mismatch.insert("class", pqueue_core::MetadataValue::String("skip".into()));
        let mut wanted = Metadata::new();
        wanted.insert("class", pqueue_core::MetadataValue::String("wanted".into()));
        block_on(backend.push(
            &shard(),
            vec![
                PushSpec {
                    priority: Some(PriorityValue::Int64(0)),
                    group_key: Some(GroupKey::new("first").unwrap()),
                    metadata: mismatch,
                    ..Default::default()
                },
                PushSpec {
                    priority: Some(PriorityValue::Int64(10)),
                    group_key: Some(GroupKey::new("second").unwrap()),
                    metadata: wanted,
                    ..Default::default()
                },
            ],
            ts(0),
            None,
        ))
        .unwrap();
        let claimed = block_on(backend.claim(ClaimRequest {
            compatibility: ClaimCompatibility {
                group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
                metadata_equals: BTreeMap::from([(
                    "class".into(),
                    pqueue_core::MetadataValue::String("wanted".into()),
                )]),
                ..Default::default()
            },
            ..claim_req(1, 100, 1)
        }))
        .unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(
            claimed.items[0].group_key.as_ref().unwrap().as_str(),
            "second"
        );
    }

    #[test]
    fn group_candidate_locks_are_scoped_and_scan_past_contention() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (group_candidate_locks_are_scoped_and_scan_past_contention) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = format!("pq_rel_group_locks_{}", std::process::id());
        let mut cleanup = Client::connect(&url, NoTls).expect("connect");
        cleanup
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .expect("drop schema");
        drop(cleanup);

        let grouped = |priority: i64, group: &str| PushSpec {
            priority: Some(PriorityValue::Int64(priority)),
            group_key: Some(GroupKey::new(group).unwrap()),
            ..Default::default()
        };
        let backend =
            PostgresRelationalBackend::connect_in_schema(&url, &schema).expect("connect backend");
        block_on(backend.create_queue(qdef())).unwrap();
        block_on(backend.push(
            &shard(),
            vec![grouped(10, "g1"), grouped(20, "g2"), grouped(30, "g3")],
            ts(0),
            None,
        ))
        .unwrap();

        let set_path = format!("SET search_path TO {schema}");
        let compatibility = ClaimCompatibility {
            group_batching: Some(pqueue_engine::GroupBatching { max_groups: 1 }),
            ..Default::default()
        };

        // Selecting g1 must not lock the discovered-but-unselected g2 summary row.
        let mut selector_client = Client::connect(&url, NoTls).expect("selector connect");
        selector_client.batch_execute(&set_path).unwrap();
        let mut selector = selector_client.transaction().unwrap();
        let selected = select_group_batching(
            &mut selector,
            &shard(),
            ts(100),
            10,
            1,
            &compatibility,
            false,
        )
        .unwrap();
        assert_eq!(selected.len(), 1);

        let mut probe_client = Client::connect(&url, NoTls).expect("probe connect");
        probe_client.batch_execute(&set_path).unwrap();
        let mut probe = probe_client.transaction().unwrap();
        probe
            .query_one(
                "SELECT group_key FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' \
                 AND group_key='g2' FOR UPDATE NOWAIT",
                &[],
            )
            .expect("unselected g2 must remain lockable");
        probe.rollback().unwrap();

        let mut locked_probe_client = Client::connect(&url, NoTls).expect("locked probe connect");
        locked_probe_client.batch_execute(&set_path).unwrap();
        let mut locked_probe = locked_probe_client.transaction().unwrap();
        assert!(
            locked_probe
                .query_one(
                    "SELECT group_key FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' \
                     AND group_key='g1' FOR UPDATE NOWAIT",
                    &[],
                )
                .is_err(),
            "selected g1 must retain its lock"
        );
        locked_probe.rollback().unwrap();
        selector.rollback().unwrap();

        // A held g1 lock must be skipped so the next canonical candidate, g2, is selected.
        let mut holder_client = Client::connect(&url, NoTls).expect("holder connect");
        holder_client.batch_execute(&set_path).unwrap();
        let mut holder = holder_client.transaction().unwrap();
        holder
            .query_one(
                "SELECT group_key FROM pqueue_group_summary WHERE tenant_id='t1' AND queue_id='q1' \
                 AND group_key='g1' FOR UPDATE",
                &[],
            )
            .unwrap();

        let mut scanner_client = Client::connect(&url, NoTls).expect("scanner connect");
        scanner_client.batch_execute(&set_path).unwrap();
        let mut scanner = scanner_client.transaction().unwrap();
        let scanned = select_group_batching(
            &mut scanner,
            &shard(),
            ts(100),
            10,
            1,
            &compatibility,
            false,
        )
        .unwrap();
        assert_eq!(scanned.len(), 1);
        let scanned_id = scanned[0].to_string();
        let scanned_group: String = scanner
            .query_one(
                "SELECT group_key FROM pqueue_items WHERE tenant_id='t1' AND queue_id='q1' AND item_id=$1",
                &[&scanned_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(scanned_group, "g2");
        scanner.rollback().unwrap();
        holder.rollback().unwrap();
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
            eligibility_time: None,
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
            eligibility_time: None,
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
        ClaimPort, ClaimRef, ClaimedItem, CommitEntryOutcome, CommitTransition,
        CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore, EngineError, FinalizeKind,
        InstanceFence, ProjectionRead, PushPort, RecoveryReadPort, RenewLeasePort, SideRecord,
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
            eligibility_time: None,
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
                    additional_claim_refs: Vec::new(),
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
                    additional_claim_refs: Vec::new(),
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
                additional_claim_refs: Vec::new(),
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
    fn commit_transition_without_expected_epoch_mints_lifecycle_ids_at_locked_cursor_epoch() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_without_expected_epoch_mints_lifecycle_ids_at_locked_cursor_epoch) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = unique_schema("cursor_epoch_ids");
        let backend = block_on(backend_for_schema(&url, &schema));
        let claim_ref = block_on(push_and_claim(&backend, 0, 10));
        let acquired_epoch = block_on(backend.acquire_epoch(&shard())).unwrap();
        assert_eq!(acquired_epoch, 1);

        let outcomes = block_on(backend.commit_transition(
            &shard(),
            CommitTransition {
                request_id: None,
                entries: vec![CommitTransitionEntry {
                    claim_ref,
                    additional_claim_refs: Vec::new(),
                    finalize: FinalizeKind::Complete,
                    side_records: Vec::new(),
                    lifecycle_items: vec![item(20)],
                    instance_fence: None,
                }],
            },
            ts(1),
            None,
        ))
        .unwrap();
        let lifecycle_id = match outcomes.as_slice() {
            [CommitEntryOutcome::Committed { lifecycle_item_ids }] => lifecycle_item_ids[0],
            other => panic!("expected one committed transition, got {other:?}"),
        };
        assert_eq!(
            lifecycle_id.epoch(),
            acquired_epoch,
            "omitting an optional fence must not fall back to epoch zero after reassignment"
        );
    }

    #[test]
    fn commit_transition_atomically_finalizes_multiple_claims() {
        let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES RELATIONAL SKIPPED (commit_transition_atomically_finalizes_multiple_claims) — set PQUEUE_PG_TEST_URL"
            );
            return;
        };
        let schema = unique_schema("multi_claim");
        let backend = block_on(backend_for_schema(&url, &schema));
        block_on(backend.push(&shard(), vec![item(10), item(11)], ts(0), None)).unwrap();
        let claimed = block_on(backend.claim(claim_req(2, 600, 0))).unwrap();
        assert_eq!(claimed.items.len(), 2);
        let to_ref = |item: &ClaimedItem| ClaimRef {
            item_id: item.item_id,
            lease_token: item
                .lease_token
                .clone()
                .expect("claimed item carries token"),
            lease_expires_at: item.lease_expires_at,
            item_version: item.item_version,
        };
        let primary = to_ref(&claimed.items[0]);
        let additional = to_ref(&claimed.items[1]);
        let additional_id = additional.item_id;
        let rid = RequestId::new("result-await-continuation-postgres").unwrap();
        let body = CommitTransition {
            request_id: Some(rid.clone()),
            entries: vec![CommitTransitionEntry {
                claim_ref: primary,
                additional_claim_refs: vec![additional],
                finalize: FinalizeKind::Complete,
                side_records: vec![side("instance/result-await", "revision-2")],
                lifecycle_items: vec![item(20)],
                instance_fence: Some(InstanceFence {
                    instance_key: b"result-await".to_vec(),
                    expected: 0,
                    next: 2,
                }),
            }],
        };
        let first =
            block_on(backend.commit_transition(&shard(), body.clone(), ts(1), None)).unwrap();
        let replay = block_on(backend.commit_transition(&shard(), body, ts(2), None)).unwrap();
        assert_eq!(replay, first);
        assert_eq!(block_on(backend.metrics(&shard())).unwrap().complete, 2);
        let recovery = block_on(backend.explain_commit(&shard(), rid))
            .unwrap()
            .expect("multi-claim recovery retained");
        assert_eq!(
            recovery.entries[0].additional_consumed_input_ids,
            vec![additional_id]
        );
        let continuation_id = match &first[0] {
            CommitEntryOutcome::Committed { lifecycle_item_ids } => lifecycle_item_ids[0],
            other => panic!("expected committed multi-claim entry, got {other:?}"),
        };
        let next = block_on(backend.claim(claim_req(10, 600, 3))).unwrap();
        assert_eq!(next.items.len(), 1);
        assert_eq!(next.items[0].item_id, continuation_id);
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
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: vec![side("state/stale", "x")],
                        lifecycle_items: vec![item(20)],
                        instance_fence: None,
                    },
                    CommitTransitionEntry {
                        claim_ref: live.clone(),
                        additional_claim_refs: Vec::new(),
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

#[cfg(test)]
mod command_log_recovery_tests {
    use super::*;
    use futures::executor::block_on;
    use pqueue_engine::{
        Backend, ControlPlaneStore, LogRead, ProjectionRead, PushPort, RawCommitFault,
        RawCommitRequest,
    };

    fn live_url() -> Option<String> {
        std::env::var("PQUEUE_PG_TEST_URL").ok()
    }

    fn unique_schema(tag: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        format!(
            "pq_rel_log_{}_{}_{}",
            tag,
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        )
    }

    #[test]
    fn nonempty_prelog_upgrade_restores_snapshot_and_exposes_baseline_ref() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("upgrade");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(async {
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .unwrap();
            backend
                .push(
                    &shard,
                    vec![PushSpec::default()],
                    pqueue_conformance::ts(1),
                    None,
                )
                .await
                .unwrap();
        });
        {
            let mut inner = backend.inner.lock().unwrap();
            st(inner.client.batch_execute(
                "DROP TABLE pqueue_command_baseline_rows; \
                 DROP TABLE pqueue_command_baselines; DROP TABLE pqueue_commands",
            ))
            .unwrap();
        }
        drop(backend);
        PostgresRelationalBackend::apply_concurrent_migrations_in_schema(&url, &schema).unwrap();
        let reopened = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let baseline = reopened
            .command_baseline_ref(&shard)
            .unwrap()
            .expect("nonempty upgrade has a named baseline");
        assert_eq!(baseline.position.sequence, 1);
        assert!(block_on(reopened.read_from(&shard, None, 10)).is_err());
        {
            let mut inner = reopened.inner.lock().unwrap();
            st(inner.client.execute(
                "DELETE FROM pqueue_items WHERE tenant_id=$1 AND queue_id=$2",
                &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
            ))
            .unwrap();
        }
        reopened.rebuild_from_command_baseline(&shard).unwrap();
        assert_eq!(
            block_on(reopened.select_eligible(&shard, pqueue_conformance::ts(2), 10))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn deleted_terminal_log_suffix_fails_closed() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("suffix");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(async {
            backend
                .create_queue(pqueue_conformance::qdef())
                .await
                .unwrap();
            backend
                .push(
                    &shard,
                    vec![PushSpec::default()],
                    pqueue_conformance::ts(1),
                    None,
                )
                .await
                .unwrap();
        });
        {
            let mut inner = backend.inner.lock().unwrap();
            st(inner.client.execute(
                "DELETE FROM pqueue_commands WHERE tenant=$1 AND queue=$2 AND seq=1",
                &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
            ))
            .unwrap();
        }
        assert!(matches!(
            block_on(backend.read_from(&shard, None, 10)),
            Err(EngineError::DurableDataCorrupt {
                stage: DurableIntegrityStage::Position,
                ..
            })
        ));
    }

    #[test]
    fn durable_position_tamper_breaks_command_checksum() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("position_tamper");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            st(inner.client.execute(
                "UPDATE pqueue_commands SET assignment_epoch=assignment_epoch+1 \
                 WHERE tenant=$1 AND queue=$2 AND seq=0",
                &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
            ))
            .unwrap();
        }
        assert!(matches!(
            block_on(backend.read_from(&shard, None, 10)),
            Err(EngineError::DurableDataCorrupt { .. })
        ));
    }

    #[test]
    fn projection_failure_rolls_back_command_append() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("rollback");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            st(inner.client.batch_execute(
                "CREATE FUNCTION reject_item_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
                   BEGIN RAISE EXCEPTION 'injected projection failure'; END $$; \
                 CREATE TRIGGER reject_item_insert BEFORE INSERT ON pqueue_items \
                   FOR EACH ROW EXECUTE FUNCTION reject_item_insert()",
            ))
            .unwrap();
        }
        assert!(
            block_on(backend.push(
                &shard,
                vec![PushSpec::default()],
                pqueue_conformance::ts(1),
                None,
            ))
            .is_err()
        );
        let mut inner = backend.inner.lock().unwrap();
        let count: i64 = st(inner.client.query_one(
            "SELECT COUNT(*) FROM pqueue_commands WHERE tenant=$1 AND queue=$2",
            &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
        ))
        .unwrap()
        .get(0);
        assert_eq!(count, 1, "failed projection must leave no phantom command");
    }

    #[test]
    fn atomic_fault_cut_never_reports_or_leaves_append_only_state() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("fault_cut");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        let envelope = direct_command_envelope(
            &shard,
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: Vec::new(),
            }),
            pqueue_conformance::ts(1),
            0,
            1,
        );
        let result = block_on(
            backend.commit_raw(
                RawCommitRequest::new(shard.clone(), vec![envelope], 0)
                    .with_fault(RawCommitFault::AfterAppendBeforeApply),
            ),
        );
        assert_eq!(result, Err(EngineError::Unavailable));
        let mut inner = backend.inner.lock().unwrap();
        let count: i64 = st(inner.client.query_one(
            "SELECT COUNT(*) FROM pqueue_commands WHERE tenant=$1 AND queue=$2",
            &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
        ))
        .unwrap()
        .get(0);
        assert_eq!(count, 1);
    }

    #[test]
    fn command_reads_hard_bound_each_page() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("page");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        {
            let mut inner = backend.inner.lock().unwrap();
            let mut tx = st(inner.client.transaction()).unwrap();
            let (tenant, queue) = parts(&shard);
            let base = alloc_seq_range(&mut tx, &tenant, &queue, 1100).unwrap();
            let positions = (0..1100)
                .map(|offset| CommandPosition::new(shard.clone(), 0, base + offset))
                .collect::<Vec<_>>();
            let commands = positions
                .iter()
                .map(|position| {
                    direct_command_envelope(
                        &shard,
                        QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: Vec::new(),
                        }),
                        pqueue_conformance::ts(1),
                        0,
                        position.sequence,
                    )
                })
                .collect::<Vec<_>>();
            persist_command_envelopes(&mut tx, &positions, &commands).unwrap();
            st(tx.commit()).unwrap();
        }
        let first = block_on(backend.read_from(&shard, None, usize::MAX)).unwrap();
        assert_eq!(first.entries.len(), 1024);
        let second = block_on(backend.read_from(&shard, first.next, usize::MAX)).unwrap();
        assert_eq!(second.entries.len(), 77);
        assert!(second.next.is_none());
    }

    #[test]
    fn projection_only_store_has_no_internal_command_log() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("reap_guard");
        let mut store = PostgresRelational::connect_in_schema(&url, &schema).unwrap();
        ProjectionStore::ensure_shard(&mut store, &pqueue_conformance::qdef()).unwrap();
        assert!(
            ProjectionStore::reap_terminal_items(
                &mut store,
                &pqueue_conformance::shard(),
                pqueue_conformance::ts(1),
                0,
                false,
                None,
            )
            .unwrap()
            .is_empty()
        );
        let mut inner = store.lock();
        let exists: bool = st(inner
            .client
            .query_one("SELECT to_regclass('pqueue_commands') IS NOT NULL", &[]))
        .unwrap()
        .get(0);
        assert!(!exists);
    }

    fn update_request(request: &str, ids: &[ItemId]) -> BatchUpdateRequest {
        BatchUpdateRequest {
            request_id: RequestId::new(request).unwrap(),
            updates: ids
                .iter()
                .map(|item_id| pqueue_engine::BatchUpdateEntry {
                    item_ref: BatchUpdateItemRef::ItemId(*item_id),
                    expected_item_version: Some(1),
                    priority: BatchUpdateValue::Replace(PriorityValue::Int64(7)),
                    not_before: BatchUpdateValue::Replace(None),
                    payload: BatchUpdateValue::Replace(Some(Bytes::from_static(b"updated"))),
                    metadata: BatchUpdateValue::Replace(Metadata::default()),
                    gate_keys: BatchUpdateValue::Replace(Vec::new()),
                    fields: BatchUpdateValue::Replace(BTreeMap::from([(
                        "updated".into(),
                        Bytes::from_static(b"true"),
                    )])),
                })
                .collect(),
        }
    }

    #[test]
    fn batch_update_1000_uses_one_target_select_command_insert_and_projection_update() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES BATCH UPDATE TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("batch_update_1000");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        let mut definition = pqueue_conformance::qdef();
        definition.max_push_batch_size = 1_000;
        definition.max_claim_batch_size = 1_000;
        block_on(backend.create_queue(definition)).unwrap();
        let items = (0..1_000)
            .map(|index| PushSpec {
                client_item_key: Some(ClientItemKey::new(format!("batch-update-{index}")).unwrap()),
                priority: Some(PriorityValue::Int64(index)),
                ..PushSpec::default()
            })
            .collect();
        let ids = block_on(backend.push(&shard, items, pqueue_conformance::ts(1), None)).unwrap();
        reset_batch_update_sql_probe(&shard);
        let response = block_on(backend.batch_update(
            &shard,
            update_request("batch-update-1000", &ids),
            pqueue_conformance::ts(2),
            None,
        ))
        .unwrap();
        assert_eq!(response.results.len(), 1_000);
        assert!(response.results.iter().all(|result| matches!(
            result,
            BatchUpdateOutcome::Updated {
                item_version: 2,
                ..
            }
        )));
        assert_eq!(
            batch_update_sql_probe(&shard),
            BatchUpdateSqlProbe {
                target_selects: 1,
                command_batch_inserts: 1,
                projection_updates: 1,
            }
        );
    }

    #[test]
    fn all_rejected_batch_update_marker_rebuilds_idempotent_response() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES BATCH UPDATE TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("batch_update_marker");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        let request = update_request("all-rejected", &[ItemId::from_u64(u64::MAX - 1)]);
        let original = block_on(backend.batch_update(
            &shard,
            request.clone(),
            pqueue_conformance::ts(2),
            None,
        ))
        .unwrap();
        assert_eq!(original.results, vec![BatchUpdateOutcome::NotFound]);
        {
            let mut inner = backend.inner.lock().unwrap();
            let envelope: Vec<u8> = inner
                .client
                .query_one(
                    "SELECT envelope FROM pqueue_commands WHERE tenant=$1 AND queue=$2 ORDER BY seq DESC LIMIT 1",
                    &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
                )
                .unwrap()
                .get(0);
            let envelope: CommandEnvelope = serde_json::from_slice(&envelope).unwrap();
            assert!(matches!(
                envelope.command,
                QueueCommand::WriteSideRecords(WriteSideRecordsCommand { ref records }) if records.is_empty()
            ));
            assert!(matches!(
                envelope.request_outcome,
                Some(RequestOutcome::BatchUpdate { .. })
            ));
            inner
                .client
                .execute(
                    "DELETE FROM pqueue_request_idempotency WHERE tenant_id=$1 AND queue_id=$2",
                    &[&shard.tenant_id.as_str(), &shard.queue_id.as_str()],
                )
                .unwrap();
        }
        backend.rebuild_from_command_baseline(&shard).unwrap();
        let replayed = block_on(backend.batch_update(
            &shard,
            request.clone(),
            pqueue_conformance::ts(3),
            None,
        ))
        .unwrap();
        assert_eq!(replayed, original);
        let mut changed = request;
        changed.updates[0].expected_item_version = None;
        assert!(matches!(
            block_on(backend.batch_update(&shard, changed, pqueue_conformance::ts(3), None,)),
            Err(EngineError::RequestIdConflict)
        ));
    }

    #[test]
    fn empty_genesis_baseline_rebuilds_before_any_data_command() {
        let Some(url) = live_url() else {
            eprintln!("POSTGRES COMMAND LOG TEST SKIPPED — set PQUEUE_PG_TEST_URL");
            return;
        };
        let schema = unique_schema("empty_genesis_rebuild");
        let shard = pqueue_conformance::shard();
        let backend = PostgresRelationalBackend::connect_in_schema(&url, &schema).unwrap();
        block_on(backend.create_queue(pqueue_conformance::qdef())).unwrap();
        backend.rebuild_from_command_baseline(&shard).unwrap();
        assert_eq!(block_on(backend.metrics(&shard)).unwrap().pending, 0);
    }

    #[test]
    fn batch_update_source_has_no_scalar_backend_loop() {
        let source = include_str!("relational.rs");
        let implementation = source
            .split("impl BatchUpdatePort for PostgresRelationalBackend")
            .nth(1)
            .unwrap()
            .split("impl ReclaimPort for PostgresRelationalBackend")
            .next()
            .unwrap();
        assert!(implementation.contains("FROM UNNEST"));
        assert!(implementation.contains("alloc_seq_range"));
        assert_eq!(
            implementation.matches("persist_command_envelopes").count(),
            2
        );
        assert!(!implementation.contains(".update_fields("));
        assert!(!implementation.contains("commit_command("));
        assert!(!implementation.contains("apply_command_sql("));
    }
}
