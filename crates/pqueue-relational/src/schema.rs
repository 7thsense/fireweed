/// The relational schema. `pqueue_items` is TD-002's item projection (sqlite-typed); `fenced`,
/// `superseded`, and `max_attempts` are reference-projection columns mirroring the `FenceLease`/
/// `UnfenceLease`, `ReplacePending`, and retry-exhaustion apply arms (the production postgres mode
/// realizes fence via epoch and supersede via the `client_item_key` tombstone — see TD-002 note). The
/// partial unique index enforces one ACTIVE item per `client_item_key`, letting a superseded predecessor
/// and its replacement coexist (ReplacePending). `relational_cursor` is the per-queue command sequence
/// (the `last_command_sequence` source), persisted so positions resume monotonically across a reopen.
pub const RELATIONAL_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS queues (
    tenant TEXT NOT NULL, queue TEXT NOT NULL, definition TEXT NOT NULL,
    paused INTEGER NOT NULL DEFAULT 0,
    pause_drain_intake INTEGER NOT NULL DEFAULT 0,
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
CREATE INDEX IF NOT EXISTS pqueue_items_expired_lease_idx
    ON pqueue_items (tenant_id, queue_id, lease_expires_at, item_id)
    WHERE lifecycle_state = 'Leased' AND cohort_size IS NULL AND fenced = 0 AND superseded = 0;
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
CREATE TABLE IF NOT EXISTS pqueue_schema_migrations (
    migration_name TEXT NOT NULL PRIMARY KEY
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
-- API-004 hot scans use `(index_key,item_id)` as their stable keyset.  Keep both
-- physical directions because reversing an ASC index also reverses `item_id`, while
-- the public cursor contract always uses item id ascending as its final tiebreaker.
CREATE INDEX IF NOT EXISTS pqueue_item_index_key_item_asc_idx
    ON pqueue_item_index (tenant_id, queue_id, index_name, index_key ASC, item_id ASC);
CREATE INDEX IF NOT EXISTS pqueue_item_index_key_item_desc_idx
    ON pqueue_item_index (tenant_id, queue_id, index_name, index_key DESC, item_id ASC);
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

/// Application tables owned by a disposable relational projection, in dependency-safe drop order.
pub const OWNED_PROJECTION_TABLES: &[&str] = &[
    "pqueue_checkpoint_lineage",
    "pqueue_item_index",
    "pqueue_instance_fences",
    "pqueue_side_records",
    "pqueue_gate_state",
    "pqueue_item_gates",
    "pqueue_cohorts",
    "pqueue_request_idempotency",
    "pqueue_item_key_retention",
    "pqueue_group_summary",
    "relational_emission_cursor",
    "pqueue_id_high_water",
    "pqueue_items",
    "relational_cursor",
    "queues",
];
