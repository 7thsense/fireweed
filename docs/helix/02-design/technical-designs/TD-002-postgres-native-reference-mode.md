---
ddx:
  id: td-postgres-native-reference-mode
  depends_on:
    - td-storage-architecture-backend-contracts
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - adr-rust-workspace-and-toolchain-policy
    - prd
    - concerns
  review:
    self_hash: 443e433bb2fa0ac55f95cb9ad02d35f8486e5e015967fb69807a3a50b97474c3
    deps:
      adr-auth-tenancy-and-storage-isolation: 032d34fcd4b1f8f9635686537cf579808d339f92494ecdfa56ca18462d338ad9
      adr-cqrs-log-projection-storage-model: 709f701130b5bd00666a1abeef4fb104555a623d39b9fec1fdb9b3167789de10
      adr-granularity-mapping-and-claim-domain: ba2d4c26c9fcaa4470ea65b61eff20cf382b6bba9e261cbd453f13122bfbc7c8
      adr-rust-workspace-and-toolchain-policy: 1f0c7eb647424e5ff2875cf5726f5de88b88276fabd7f203424ace231c1f6ab2
      api-native-client-interface: 6b76e5c4c37c91d40e8d5229d9eeae516f71385aa06e856fb41a4a19ee5856e8
      concerns: 122b700fbf6049b7fa177b99efa27c5fce011775767d682458a0e2872981fb54
      prd: 382115039de93226b051a09e719c7e1c50f12563d96c1ba85ef142c0ae5d0ce0
      td-storage-architecture-backend-contracts: 5980a5612e178fc0828f567f21efaafd9d49cf7e62b2d8655bf7b9ef32e97d8d
    reviewed_at: "2026-06-20T19:01:18Z"
---

# Technical Design: TD-002 Postgres-Native Reference Mode

> **Implementation status (hexagonal migration, Phase 6):** the original `pqueue-postgres` crate (which
> implemented the now-removed storage traits) has been **deleted**. The postgres adapter is **deferred**
> and will be (re)built **fresh to the engine ports** following the durable-adapter template proven by
> sqlite (durable command log + projection rebuilt-from-log) when a database is provisioned. The
> data-model intent below (postgres as an atomic-class backend) still holds; only the trait surface it
> targets has changed (engine `Backend`/`ClaimPort`/`PushPort`/… instead of the old storage traits). See
> [ADR-007](../adr/ADR-007-hexagonal-architecture-and-two-interfaces.md) and
> [`hexagonal-migration-plan.md`](../../04-build/hexagonal-migration-plan.md).


**Contract**: API-001 | **ADR**: ADR-001, ADR-004 | **Scope**: Postgres-native backend

## Scope

This technical design defines the first concrete storage backend for pqueue:
`postgres_native`. In this mode, Postgres provides the control plane, durable
command log, operational projection, idempotency state, leases, and queue
metrics.

In scope:

- Postgres logical schema for queue definitions, shard assignments, command
  log, queue items, leases, idempotency, and metrics.
- Transaction boundaries for API-001 mutating operations.
- Claim query shape for strict and bounded-relaxed ordering.
- Shard epoch fencing without pqueue-owned node discovery or cluster consensus;
  the shard-owner lease lifecycle that allocates and advances `assignment_epoch`
  (and durably fences it into the shard row at acquire time) is specified in
  TD-003 (`td-sharding-and-shard-ownership`) and stored in `pqueue_shards`.
- Retention and compaction rules needed for bounded storage growth.
- Indexing and partitioning assumptions for 10M-item hot queues.

Out of scope:

- Cross-backend object-log manifests and SQLite projections.
- P1 operator APIs for redrive, repair, and migration. (Active-scope discovery is
  in-contract, served from `pqueue_group_summary`; targeted recurring teardown
  `PurgeItems` is in-band native scope. Broad operator purge/redrive/repair remains
  a separate P1 operator contract.)
- Exact SQL migration filenames and generated Rust structs.
- Physical deployment sizing for a specific managed Postgres provider.

## Technical Approach

`postgres_native` is the reference correctness backend. It uses ordinary
Postgres transactions to commit the durable command record and the operational
projection together. This keeps the first implementation simple, gives
low-latency small-batch commits, and creates executable semantics for later
backends to match through conformance tests.

The backend still follows TD-001's capability boundaries:

- `ControlPlaneStore`: queue definitions, shard assignments, backend profile,
  and assignment epochs.
- `LogStore`: append-only command records with monotonic per-shard positions.
- `ProjectionStore`: queue item state, claim planning, lease state,
  idempotency, and metrics.
- `SnapshotStore`: optional in v1 for Postgres-native mode; logical dump or
  checkpoint support may be added after the first backend passes conformance.

## Data Model

The schema is logical. Implementation may split or rename tables, but must
preserve these records and indexes.

### Control Plane

```sql
create table pqueue_queues (
  tenant_id text not null,
  queue_id text not null,
  priority_model jsonb not null,
  ordering_mode text not null,
  group_co_residency boolean not null default false,
  recurring boolean not null default false,
  progress_bound_ms bigint not null,
  eligibility_policy jsonb not null default '{}'::jsonb,
  request_id_retention_ms bigint not null,
  client_item_key_retention_ms bigint not null,
  terminal_retention_ms bigint not null,
  max_lease_duration_ms bigint not null,
  retry_policy jsonb not null,
  max_push_batch_size integer not null,
  max_claim_batch_size integer not null,
  backend_profile text not null default 'postgres_native',
  shard_count integer not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  primary key (tenant_id, queue_id)
);

create table pqueue_shards (
  tenant_id text not null,
  queue_id text not null,
  shard_id integer not null,
  assignment_epoch bigint not null,
  placement jsonb not null default '{}'::jsonb,
  state text not null,
  active_owner_id text,
  target_owner_id text,
  lease_expires_at timestamptz,
  updated_at timestamptz not null,
  primary key (tenant_id, queue_id, shard_id),
  foreign key (tenant_id, queue_id)
    references pqueue_queues (tenant_id, queue_id)
);
```

`placement` stores control-plane routing metadata. It is not a node-discovery
protocol. Service nodes consume assignments from Postgres and pass
`assignment_epoch` into data-plane mutations as a fencing token.

`state` values are `unassigned`, `assigned`, and `draining`. The shard-owner
lease operations `acquire_shard_lease`, `renew_shard_lease`, `begin_drain`, and
`release_shard_lease` (TD-003) operate on the `active_owner_id`, `target_owner_id`,
`lease_expires_at`, and `assignment_epoch` columns transactionally.
`acquire_shard_lease` allocates a strictly greater `assignment_epoch` and updates
the row in the same transaction — this IS the durable epoch fence for
`postgres_native`; `renew_shard_lease` preserves the epoch. The data-plane append
transaction (Transaction Flows step 3) validates `expected_epoch == current
assignment_epoch`, which is the safety fence. `target_owner_id` MAY differ from
`active_owner_id` transiently during reassignment; safety depends on the epoch,
not on the two agreeing.

`group_co_residency` is immutable after creation and is part of the queue's
configuration identity for idempotent `CreateQueue` (a differing value is a
definition conflict), exactly as for `ordering_mode`, `priority_model`, and
`shard_count`. When `group_co_residency=true`, item placement MUST use
`shard_id = hash(group_key) mod shard_count` (mirroring the validation workload's
hash-on-job-key), so a `group_key`'s items are co-resident on one shard; this is
what makes whole-group (`compatibility.group_batching`) and whole-cohort
(`compatibility.whole_cohort`) claims shard-local and atomic and lets a
single-`group_key` claim return exact per-group order. The `group_key` topology
(cohort-keyed by `callback_id` versus job-keyed by `job_id`) is per ADR-004
(`adr-granularity-mapping-and-claim-domain`). For queues with
`group_co_residency=false`, shard routing MAY use any stable scheme; `group_key`
then does not constrain placement and does not promise per-group total order
across shards. `group_co_residency` is a placement capability only; it is NOT a
`claim_scope` and carries no progress meaning (progress remains queue-global).

`recurring` is an immutable per-queue flag. On a `recurring` queue, a `rearm`
finalize returns an item to `pending` without a terminal transition (see
`BatchFinalize`); one-shot and recurring items are never mixed in one queue.

### Durable Command Log

```sql
create table pqueue_commands (
  tenant_id text not null,
  queue_id text not null,
  shard_id integer not null,
  sequence bigint not null,
  assignment_epoch bigint not null,
  command_id text not null,
  request_id text,
  request_fingerprint bytea,
  command_type text not null,
  item_ids text[] not null default '{}',
  command_payload jsonb not null,
  checksum bytea not null,
  created_at timestamptz not null,
  primary key (tenant_id, queue_id, shard_id, sequence),
  unique (tenant_id, queue_id, shard_id, command_id)
);
```

`sequence` is allocated under the same transaction that validates the shard
epoch. Implementations may use a shard sequence table, advisory locks scoped to
`tenant_id/queue_id/shard_id`, or row locking on `pqueue_shards`. The result must
be a monotonic command position per shard.

### Projection

```sql
create table pqueue_items (
  tenant_id text not null,
  queue_id text not null,
  shard_id integer not null,
  item_id text not null,
  client_item_key text not null,
  lifecycle_state text not null,
  priority jsonb not null,
  priority_sort bytea not null,
  not_before timestamptz,
  eligible_since timestamptz,
  group_key text,
  cohort_size integer,
  recurrence_until timestamptz,
  payload jsonb,
  metadata jsonb not null default '{}'::jsonb,
  retry_count integer not null default 0,
  retry_metadata jsonb not null default '{}'::jsonb,
  failure_code text,
  item_version bigint not null,
  lease_token_hash bytea,
  lease_expires_at timestamptz,
  worker_id text,
  last_command_sequence bigint not null,
  created_at timestamptz not null,
  updated_at timestamptz not null,
  terminal_at timestamptz,
  primary key (tenant_id, queue_id, item_id),
  unique (tenant_id, queue_id, client_item_key)
);
```

`priority_sort` is a backend-owned canonical encoding of the declared priority
model and direction. Claim queries sort by `priority_sort`, not by ad hoc JSON
comparison.

### Per-Group Summary Projection

There is exactly ONE per-group summary projection, `pqueue_group_summary`, keyed
by `(tenant_id, queue_id, shard_id, group_key)`. It is the single source of truth
for (a) `group_batching` oldest-group selection (g1), (b) `DiscoverActiveScopes`
group-granularity ranking (g4), and (c) per-group observability. There is no
separate `pqueue_active_scope_summary` table. This projection is authored once
here and is consumed by g1 selection/locking and g4 discovery alike.

```sql
-- Single canonical per-group summary projection.
-- Consumers: (a) g1 whole-group selection + per-group lock anchor;
--            (b) g4 DiscoverActiveScopes ranking; (c) per-group observability.
create table pqueue_group_summary (
  tenant_id text not null,
  queue_id  text not null,
  shard_id  integer not null,
  group_key text not null,
  -- authoritative oldest-eligible age source (exact-on-read under the gate predicate):
  oldest_eligible_at      timestamptz,    -- null = no currently-eligible item
  -- exact selection-oriented representative claim key (g1 oldest-group ranking;
  -- maintained with item mutations, exact-on-read under the gate predicate):
  rep_progress_guard_sort bytea,          -- queue progress-guard encoding of the representative item
  rep_priority_sort       bytea,
  rep_created_at          timestamptz,
  rep_item_id             text,
  -- routing/observability hint (MAY be lagged/approximate):
  eligible_item_count     bigint not null default 0,
  at_risk_count           bigint not null default 0,
  updated_at              timestamptz not null default now(),  -- per-row watermark -> discovery as_of
  primary key (tenant_id, queue_id, shard_id, group_key)
);

-- g1 selection: oldest-N groups by representative claim key.
create index pqueue_group_summary_select_idx
  on pqueue_group_summary (
    tenant_id, queue_id, shard_id,
    rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id
  )
  where oldest_eligible_at is not null;

-- g4 discovery: rank groups by oldest-eligible age (queue-global after cross-shard merge).
create index pqueue_group_summary_rank_idx
  on pqueue_group_summary (tenant_id, queue_id, oldest_eligible_at)
  where oldest_eligible_at is not null;
```

A group is co-resident on one shard when `group_co_residency=true` (D2), so this
shard-scoped grain has exactly one row per active group while remaining coherent
for the local-SQLite backend (TD-004) and for multi-shard aggregation. Cross-shard
queue-global aggregation merges these rows by `(tenant_id, queue_id, group_key)`;
under co-residency the merge has no collisions (TD-003 Cross-Shard Progress).

Consistency model (binding):

1. **`oldest_eligible_at` and the `rep_*` representative key are authoritative and
   exact-on-read.** They are maintained in the SAME transaction as item mutations
   that change a group's eligible set (push, update, claim, finalize, retry,
   lease-expiry materialization, rearm) using the same Eligibility Precedence
   predicate (API-001, authored by g2). g1 group selection and g4 discovery
   ranking MUST rely on these, never on the counts.
2. **Gate flips are NOT applied synchronously to every affected group's summary
   row.** A queue-scoped `SetGates` flip (g2) is O(shards × keys), not O(groups or
   items), and changes no item's `eligible_since`. The summary keeps the
   authoritative fields correct WITHOUT a per-group rewrite via exact-on-read: the
   read path (g1 selection, g4 discovery, metrics) joins the candidate group's
   gate keys to current gate state (`pqueue_gate_state`), and if the stored
   representative item is gate-blocked at read time it advances to the group's
   next item open under all gate keys; a group all of whose items are gate-blocked
   is excluded entirely. This distinguishes "whole group blocked" (group excluded)
   from "oldest item blocked" (advance to the next open item). Because a group is
   gate-blocked as a unit only when a shared gate key it carries is blocked, the
   join is O(blocked keys), not O(items).
3. **`eligible_item_count` / `at_risk_count` MAY be lagged or approximate** and
   MUST be documented as such when served. They are routing/observability hints,
   never selection or completeness inputs (whole-group/whole-cohort completeness
   re-derives membership under the group lock). After a gate flip the counts MAY
   momentarily over-count gate-blocked items until a bounded, asynchronous
   recompute (scoped to the groups sharing the flipped key on that shard) corrects
   them.
4. A group row is deleted (or `oldest_eligible_at` / `rep_*` set null) when its
   last eligible item becomes ineligible. Recompute on push/finalize/expiry is
   bounded per affected group, never O(items) per read.
5. Cross-shard aggregation: the queue-global oldest-eligible age is the cross-shard
   maximum of per-shard gate-filtered oldest ages = `now() - min(oldest_eligible_at)`
   (TD-003 / D4); discovery merge-then-limit takes the queue-global top-N by
   `oldest_eligible_at` across shards before applying the result limit. Counts sum
   the (possibly lagged) per-shard counts.

### Cohort Projection

```sql
create table pqueue_cohorts (
  tenant_id          text    not null,
  queue_id           text    not null,
  group_key          text    not null,   -- cohort key (logical identity)
  shard_id           integer not null,   -- derived: hash(group_key) mod shard_count (co-residency)
  cohort_id          text    not null,   -- stable cohort identity (new on group_key reuse)
  cohort_size        integer not null,
  member_count       integer not null,   -- non-terminal members persisted
  state              text    not null,   -- forming | complete | leased | terminal
  cohort_created_at  timestamptz not null,  -- first member; incompleteness-deadline clock start
  first_eligible_at  timestamptz,        -- first complete-and-claim-eligible instant; never moved back
  expire_command_pos bigint,             -- log position of CohortExpired, if any
  cohort_lease_token_hash bytea,         -- set while leased (hash only)
  retention_until    timestamptz,        -- terminal-cohort retention; group_key reusable after this
  primary key (tenant_id, queue_id, group_key)
);

create index pqueue_cohorts_claim_idx
  on pqueue_cohorts (tenant_id, queue_id, shard_id, state)
  where state = 'complete';

create index pqueue_cohorts_expiry_idx
  on pqueue_cohorts (tenant_id, queue_id, shard_id, cohort_created_at)
  where state in ('forming', 'complete');
```

`pqueue_cohorts` is a projection of the command log (ADR-001 log-is-truth),
maintained transactionally with member mutations. `member_count` and `state` MUST
be updated in the same transaction as member inserts, AFTER `client_item_key`
idempotency convergence, so duplicate pushes do not increment `member_count` or
overfill. A new distinct member insert that would exceed `cohort_size` MUST be
rejected (`conflict`). `state=complete` is set only when
`member_count == cohort_size`. `pqueue_cohorts` records cohort completion state
ONLY; `oldest_eligible_at` and eligible counts for cohort members come from the
single `pqueue_group_summary` projection (which excludes not-yet-claim-eligible
members). `first_eligible_at` is set the first time the row is `complete` AND every
member satisfies Eligibility Precedence conditions 1-5, and is never moved
backward. `retention_until` is set on terminal transition; while non-null and in
the future the `group_key` MUST NOT start a new cohort (`conflict`); after it
elapses the `group_key` MAY be reused with a fresh `cohort_id`.

`eligible_since` is null while the item is not eligible. When an item becomes
eligible again after lease expiry, `eligible_since` must preserve the earlier
eligible time unless the item became ineligible because of a caller-controlled
delay such as `not_before` or retry backoff. This supports PRD progress-bound
semantics.

`cohort_size` is set only on cohort members (queues with `cohort_policy.enabled`,
g6); the cohort key is the existing `group_key` and placement is the group
co-residency capability, so no separate cohort-to-shard derivation exists.
Cohort completion state lives in `pqueue_cohorts` (below), not on the item row.

`recurrence_until` is the optional per-item recurrence drain instant on a
`recurring` queue. A `rearm` past `recurrence_until` MUST terminate the item
instead of re-arming it.

### Idempotency

```sql
create table pqueue_request_idempotency (
  tenant_id text not null,
  queue_id text not null,
  operation text not null,
  request_id text not null,
  request_fingerprint bytea not null,
  response_payload jsonb,
  command_positions jsonb not null,
  expires_at timestamptz not null,
  created_at timestamptz not null,
  primary key (tenant_id, queue_id, operation, request_id)
);

create table pqueue_item_key_retention (
  tenant_id text not null,
  queue_id text not null,
  client_item_key text not null,
  item_id text not null,
  expires_at timestamptz not null,
  primary key (tenant_id, queue_id, client_item_key)
);
```

Request idempotency records store the envelope fingerprint and response payload
when the operation can be replayed directly. If response storage fails after a
command commit, retry may reconstruct the response from committed command and
projection state, then backfill the idempotency response.

`pqueue_item_key_retention` keeps duplicate push convergence after terminal item
records are purged, until `client_item_key_retention_ms` expires.

## Required Indexes

The first implementation must include indexes equivalent to:

```sql
create index pqueue_items_claim_strict_idx
  on pqueue_items (
    tenant_id,
    queue_id,
    shard_id,
    lifecycle_state,
    priority_sort,
    created_at,
    item_id
  )
  where lifecycle_state = 'pending';

create index pqueue_items_eligible_age_idx
  on pqueue_items (
    tenant_id,
    queue_id,
    shard_id,
    lifecycle_state,
    eligible_since
  )
  where lifecycle_state = 'pending';

create index pqueue_items_lease_expiry_idx
  on pqueue_items (
    tenant_id,
    queue_id,
    shard_id,
    lifecycle_state,
    lease_expires_at
  )
  where lifecycle_state = 'leased';

create index pqueue_items_group_claim_idx
  on pqueue_items (
    tenant_id,
    queue_id,
    shard_id,
    group_key,
    priority_sort,
    created_at,
    item_id
  )
  where lifecycle_state = 'pending';

create index pqueue_commands_replay_idx
  on pqueue_commands (tenant_id, queue_id, shard_id, sequence);

create index pqueue_request_idempotency_expiry_idx
  on pqueue_request_idempotency (expires_at);
```

Implementations may use partial indexes per lifecycle state and table
partitioning by tenant, queue, shard, or hash. A 10M-item hot queue must not rely
on a full-table scan for claim, lease expiry, idempotency, or progress metrics.

Discovery is served from `pqueue_group_summary` and its rank index
(`pqueue_group_summary_rank_idx`, joined to `pqueue_gate_state` at read time), not
from ad hoc aggregates on `pqueue_items`. The item indexes ordered by
`priority_sort`/`eligible_since` are insufficient for
`GROUP BY group_key ORDER BY min(oldest_eligible_at)` at 10M-item scale; the
per-group summary exists specifically to avoid that aggregate scan.

## Active-Scope Discovery Reads

`DiscoverActiveScopes` (g4) serves from the maintained per-group summary projection
`pqueue_group_summary`, keyed `(tenant_id, queue_id, shard_id, group_key)`. No
separate `pqueue_active_scope_summary` table exists. Discovery never scans
`pqueue_items`.

Per-shard group-granularity read (one shard; the service layer merges across the
queue's shards and joins live gate state):

```sql
select gs.group_key,
       gs.oldest_eligible_at,
       gs.eligible_item_count,
       gs.updated_at
from pqueue_group_summary gs
where gs.tenant_id = $1 and gs.queue_id = $2 and gs.shard_id = any($3)
  and gs.oldest_eligible_at is not null
-- gate-current derivation (advance past a gate-blocked representative) is applied
-- by the read path joining pqueue_gate_state; see the consistency model.
order by gs.oldest_eligible_at asc
limit $4;   -- per-shard prefetch bound >= max_results; final top-N after merge
```

Queue-granularity discovery derives a per-queue oldest-eligible from the SAME
table as `min(oldest_eligible_at)` over the queue's group rows across shards,
exposed as a `granularity=queue` descriptor (no `group_key` field, no materialized
queue rollup). It MUST NOT be stored or exposed as a `group_key = null` row; the
projection's `group_key` column is `not null`.

**Discovery consistency model.** `oldest_eligible_at` is authoritative and exact
(maintained transactionally with item mutations), so `oldest_eligible_age_ms` is
exact as of `as_of`. `eligible_item_count` MAY be lagged/approximate and MUST be
documented as such. A g2 `SetGates` flip is queue-scoped, O(1), and writes no item
or summary rows; discovery applies the gate predicate at read time by joining
`pqueue_gate_state`, advancing past a gate-blocked representative to the next
Eligibility-Precedence-eligible item, and omits a group only when no item in it is
currently eligible.

**Cross-shard `as_of` is an observed projection frontier.** `response.as_of` MUST
be the minimum per-row `updated_at` watermark across EVERY shard read for the
result, including shards that returned no eligible rows, shards stale or unowned at
read time (using their last-known watermark; a shard with no live owner for longer
than `progress_bound_ms` is a progress-bound violation surfaced by TD-003), and the
watermark of any candidate row skipped during gate revalidation. The service layer
merges by `(queue_id, group_key)` taking the minimum `oldest_eligible_at` (== max
age) and summed counts BEFORE applying `max_results`; the returned top-N is the
true cross-shard top-N, never a per-shard top-N union. This merge is correct for
both placement modes: under `group_co_residency=true` each `group_key` lives on one
shard (a union of disjoint groups); under `group_co_residency=false` a `group_key`
MAY appear on several shards and the merge takes the cross-shard minimum timestamp.
A queue's shard set, lease validity, and epoch fencing are owned by TD-003.

## Transaction Flows

Every mutating operation follows this order inside one transaction unless
otherwise stated:

1. Authorize tenant and queue access before opening data-plane mutation state.
2. Load queue definition and target shard row.
3. Validate `assignment_epoch`; stale epochs fail before append.
4. Check request idempotency.
5. Validate item-level conditions.
6. Allocate command sequence and insert command log row.
7. Mutate projection rows.
8. Insert or update idempotency response.
9. Commit.
10. Return response derived from committed rows.

### `BatchPush`

- Reject an empty batch or a batch above configured limits.
- For each item, validate priority model, not-before, metadata gate shape, and
  payload size.
- For existing `client_item_key`, return `duplicate` without mutating the item
  or incrementing `item_version`.
- For new items, insert `pending` item with `item_version=1`, `eligible_since`
  set only when the item is eligible at commit time, and lifecycle counters
  updated.
- Insert item-key retention record for duplicate convergence.
- On a `group_co_residency=true` queue, an item pushed without `group_key` MUST
  fail per item with `invalid`; placement is `shard_id = hash(group_key) mod
  shard_count`.
- On a cohort-enabled queue (`cohort_policy.enabled`), validate
  `cohort_size <= max_cohort_size` and that `cohort_size` matches the existing
  `pqueue_cohorts` row (else `conflict`). After `client_item_key` idempotency
  convergence (duplicate is a no-op with no count change), for a new distinct
  member upsert `pqueue_cohorts` incrementing `member_count` (reject overfill with
  `conflict`), set `cohort_id` and `cohort_created_at` on the first member, and
  recompute `state`; if newly complete-and-eligible, set `first_eligible_at`. New
  members are `pending` but are not claim-eligible to non-cohort claims while any
  sibling is non-terminal.

### `BatchUpdate`

- Resolve `item_id` or `client_item_key`.
- Update only `pending` items with no active lease and no terminal state.
- Enforce `expected_item_version` when present.
- Replace fields using API-001 full-replacement semantics.
- Recompute `priority_sort`, eligibility, and `eligible_since`.
- Increment `item_version` for each successful update.
- Return per-item `conflict`, `not_found`, `terminal`, or `updated`.

### `BatchClaim`

`BatchClaim` must atomically select and lease items using row-level locks:

```sql
-- Conceptual query shape. The metadata and compatibility predicates are
-- generated from queue policy and the API-001 claim request.
with candidates as (
  select item_id
  from pqueue_items
  where tenant_id = $1
    and queue_id = $2
    and shard_id = $3
    and lifecycle_state = 'pending'
    and (not_before is null or not_before <= now())
    and eligible_since is not null
    -- and queue eligibility policy matches metadata
    -- and compatibility predicates match group_key/metadata
  order by progress_guard_sort, priority_sort, created_at, item_id
  limit $4
  for update skip locked
)
update pqueue_items i
set lifecycle_state = 'leased',
    lease_token_hash = $5,
    lease_expires_at = $6,
    worker_id = $7,
    item_version = item_version + 1,
    updated_at = now()
from candidates c
where i.item_id = c.item_id
returning i.*;
```

`progress_guard_sort` is conceptual. Implementations must ensure items near
`progress_bound_ms` cannot be bypassed indefinitely by relaxed ordering or
group-aware selection. A valid first implementation may use strict ordering for
`ordering_mode=strict` and a two-window bounded-relaxed strategy for
`ordering_mode=bounded_relaxed`:

- reserve a progress-protection window for items whose
  `eligible_since + progress_bound_ms` is near violation.
- otherwise select from a bounded candidate set ordered by priority and grouped
  by compatibility.

For `same_group_key=true`, the server chooses one group from eligible candidates
and leases only that group for the request. `same_group_key` is an item-level
domain filter: it constrains a single claim to one server-selected `group_key` and
MAY return a partial group (capped by `max_items`); it is NOT a whole-group atomic
unit. Server-selected group choice must include fairness under skew and must not
starve other groups beyond the queue's progress bound.

The claim CTE performs no downstream-rate admission; there is no rate-admission
stage in the claim pipeline, and pacing is caller-driven (see TD-001 Key Decisions
and the API-001 caller-driven downstream pacing paragraph).

When the request pins a single `group_key` on a `group_co_residency=true` queue
(or the queue's claim mode resolves to one group), the claim is routed to the
single shard owning that group, the candidate CTE filters to that group, and the
existing `order by progress_guard_sort, priority_sort, created_at, item_id` IS the
exact per-group order — no new query is required for deterministic-in-domain
ordering (ADR-004). On a `group_co_residency=false` queue a `group_key` filter is
applied as a candidate restriction within each shard's CTE; results are merged per
the cross-shard merge rule (TD-003) but are NOT a per-group total order. `shard_id`
never appears in the result ordering. The single queue-global progress guard
(`progress_guard_sort`) is unchanged and is NOT evaluated per group; progress is
queue-global (FR-12).

#### `group_batching` (whole-eligible-group claim)

For `group_batching` (`group_completeness=whole_eligible`, `max_groups=N`), claim
runs inside one serialized critical section (one Postgres transaction) with
group-level locking, and is valid only on queues created with group co-residency
(`group_co_residency=true`, ADR-004 / D2), where
`shard_id = hash(group_key) mod shard_count`; selection is therefore shard-local.
The target shard is resolved server-side from the cross-shard oldest-eligible
aggregate (TD-003), never from the client request.

1. **Overfetch candidates.** Open a cursor over `pqueue_group_summary` for the
   resolved shard, ordered by each group's representative claim key
   (`rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id`),
   honoring the queue-global progress-protection window first under `ordering_mode`.
   Fetch more than N candidates (`N + k`, refill on demand). Group eligibility is
   defined solely by the Eligibility Precedence subsection; the candidate
   `eligible_item_count` is a hint only.
2. **Lock + revalidate, in canonical order.** Sort the candidate set by group lock
   identity (`hash(tenant_id, queue_id, shard_id, group_key)`) ascending and, in
   that order, try-acquire an exclusive group lock per candidate
   (`pg_try_advisory_xact_lock(...)`, OR `select ... for update skip locked` on the
   group's summary row). A lock not immediately available means the group is
   contended: skip it (do not block, do not count it toward N). This canonical-order,
   all-modes lock discipline (generic, `same_group_key`, `group_batching`, and g6
   `whole_cohort` all use the same identity and order) prevents lock-order deadlock.
   After locking, re-read the group's currently-eligible items under the live
   eligibility predicate (including gate state and `metadata_equals` if present). If
   the group has zero eligible items at lock time, or any active lease held by
   another claim, discard it and do not count it.
3. **Lease whole + refill.** Lease ALL currently-eligible items of each valid locked
   group (`FOR UPDATE`, no `SKIP LOCKED` skipping within a locked group). Accumulate
   whole groups until adding the next group would exceed `max_items`; then stop. If
   discards exhaust the candidate set before N valid groups are collected, advance
   the cursor and fetch the next page (refill) until N valid whole groups, the
   `max_items` ceiling, or the shard's candidate groups are exhausted — so the claim
   returns the true next N groups even if early candidates were invalidated by a
   mid-claim gate flip or concurrent lease. If the next group in order alone exceeds
   `max_items`, roll back and return envelope `batch-too-large`, leasing nothing.

There is NO rate-admission step in this flow (D3). Group-level locking (not
item-level `FOR UPDATE SKIP LOCKED` over an unlocked group selection) is what
guarantees two concurrent claims never split a group.

```sql
-- group_batching (N groups): overfetch candidates, then lock-per-group (canonical
-- order, try-lock-skip), revalidate, lease each valid locked group whole, refill.
-- Run inside one transaction; queue must have group co-residency (shard=hash(group_key)).
with candidate_groups as (
  select group_key
  from pqueue_group_summary           -- single per-group summary (also backs discovery)
  where tenant_id = $1 and queue_id = $2 and shard_id = $3
    and oldest_eligible_at is not null            -- has a current eligible representative
    -- and metadata_equals predicate satisfiable for the group (if present)
  order by rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id
  limit $overfetch                                -- N + k; cursor refills on demand
)
-- application logic, candidates sorted by lock identity ascending (deadlock-free):
--   for each candidate in canonical lock order:
--     if not pg_try_advisory_xact_lock(hash(tenant,queue,shard,group_key)): skip (contended)
--     re-read eligible items of group_key under live predicate (gate state + metadata_equals);
--       if zero eligible OR any active lease held by another claim: discard (do not count)
--     else lease all eligible items (FOR UPDATE, no SKIP LOCKED inside group)
--   stop when next group would exceed max_items; if next group alone exceeds max_items
--     -> rollback, return batch-too-large; refill cursor if candidates exhausted before N.
```

#### `whole_cohort` (complete-cohort claim)

For `compatibility.whole_cohort` (g6) the claim is one transaction, shard-local, and
locks the cohort first:

1. Candidate select: most-urgent `pqueue_cohorts` row with `state='complete'` (via
   `pqueue_cohorts_claim_idx`), ordered by the cohort's representative
   `priority_sort`, limit 1.
2. Lock the cohort row `FOR UPDATE` (no `SKIP LOCKED` for the chosen cohort), then
   lock all members `FOR UPDATE` in deterministic claim order. Recheck
   `state='complete'` AND every member's Eligibility Precedence conditions 1-5 under
   the lock. If the row cannot be locked immediately or the recheck fails, skip to
   the next complete cohort; if none, return empty. NEVER block, NEVER partially
   lease.
3. Transition every member to `leased` under one `cohort_lease_token`, set
   `pqueue_cohorts.state='leased'` and `cohort_lease_token_hash`, increment
   `item_version` per member, append one claim command covering the whole cohort.
4. If the selected cohort's `cohort_size > max_items`, fail the envelope with
   `batch-too-large` (cannot occur when `max_items >= max_cohort_size`).

Item and `group_batching` claims on a cohort-enabled queue MUST take the same
`pqueue_cohorts` row lock and exclude every member of any non-terminal cohort. The
cohort lock uses the same canonical lock identity and ordering as `group_batching`,
so g1 and g6 share one lock regime.

### `BatchRenewLeases`

- Match by `item_id` and hashed `lease_token`.
- Require `lifecycle_state='leased'` and non-expired active lease.
- Update `lease_expires_at`, increment `item_version`, append renew command, and
  return per-item `renewed` or `stale_lease`.

### `BatchFinalize`

- Match by `item_id` and hashed `lease_token`.
- Require active lease for `complete`, `fail`, `retry`, and `release`.
- `complete`: set terminal `complete`, clear lease, set `terminal_at`.
- `fail`: set terminal `failed`, store failure code/metadata, clear lease, set
  `terminal_at`.
- `retry`: increment retry count; if retry policy is exhausted, set terminal
  `failed`; otherwise return to `pending` with retry metadata, `not_before`, and
  recomputed `eligible_since`.
- `release`: return to `pending`, clear lease, preserve progress-bound clock
  where required by PRD FR-11.
- `rearm` (recurring queues only): a single in-transaction update that releases
  lease state, sets the caller-supplied `not_before`, sets
  `eligible_since = max(now(), not_before)` (the deterministic effective eligible
  instant, which satisfies the claim CTE's `eligible_since is not null` guard and
  the `not_before <= now()` guard), resets `retry_count` to 0, and bumps
  `item_version`, WITHOUT marking terminal — mirroring the `retry` path's
  return-to-pending. A `rearm` past `recurrence_until` MUST terminate the item
  instead. The same transaction recomputes only the rearmed item's
  `pqueue_group_summary` row (recompute `oldest_eligible_at` from the scope's
  remaining eligible items; `oldest_eligible_at` stays authoritative/exact,
  `eligible_item_count` MAY be served lagged). Recurring observability counters
  (`recurring_pending` / `recurring_leased`) are served from the metrics
  projection, NOT from `pqueue_group_summary`.

## Lease Expiry

Lease expiry may be materialized lazily during claim or by a background task.
Either approach must append a `LeaseExpired` command before making expired items
claimable again, unless TD-002 is amended to prove that lease expiry can be a
pure projection rule without breaking replay or audit.

The first implementation should prefer lazy expiry in the claim path plus a
bounded background sweeper for metrics freshness. The sweeper must be shard and
epoch fenced, batch-limited, and safe to run concurrently.

## Cohort Completion and Expiry

A shard- and epoch-fenced sweeper MUST, for any `pqueue_cohorts` row in
`forming`/`complete` whose expiry deadline
`min(cohort_created_at, first_eligible_at) + completion_bound_ms` has passed: take
the cohort row lock, recheck the deadline and state under the lock, append
`CohortExpired` (recording `expire_command_pos`) BEFORE setting members terminal
`failed`/`cohort-incomplete`, set `state='terminal'` and `retention_until` — all in
one transaction. The sweeper and any claim contend on the same row lock,
linearizing claim-vs-expiry (leased XOR expired). Because `CreateQueue` enforces
`completion_bound_ms <= progress_bound_ms`, expiry always linearizes before a
withheld eligible member can breach the queue-global progress bound (FR-12).
Whole-cohort finalize/release/retry update `pqueue_items` and `pqueue_cohorts.state`
in one transaction.

## Recurring Item Teardown (`PurgeItems`)

Targeted recurring teardown (`PurgeItems`) is in-band native scope (P0); broad
operator purge/redrive/retention remains a separate P1 operator contract, and the
two MUST NOT be conflated. A `PurgeItems` removal deletes the `pqueue_items` row
(and, with `force`, the lease), MUST be recorded as a durable `PurgeItemsCommand`,
and MUST write a tombstone keyed by `(tenant_id, queue_id, client_item_key)`
retained for at least `client_item_key_retention_ms`. The existing
`unique (tenant_id, queue_id, client_item_key)` constraint enforces the
single-live-recurring-item-per-key invariant; once purged, the tombstone (not a
retained item row) carries replay/audit, and a re-push after the tombstone window
inserts a fresh item. The same transaction recomputes the affected
`pqueue_group_summary` row. Retention/GC MUST exclude live recurring rows and MUST
retain purge command positions per the replay/audit window rule below.

## Retention and Compaction

Postgres-native mode must enforce bounded storage growth:

- Request idempotency records expire after `request_id_retention_ms`.
- Item-key convergence records expire after `client_item_key_retention_ms`.
- Terminal item records expire after `terminal_retention_ms` unless a later
  operator contract marks them retained for inspection or archival.
- Command log rows are retained at least until all associated idempotency,
  terminal retention, replay, and audit windows have expired.

Deleting terminal item records does not delete command log rows still required
for replay or audit. Targeted recurring teardown (`PurgeItems`) is in-band native
scope (P0; see Recurring Item Teardown) and writes a tombstone plus a durable
`PurgeItemsCommand`; broad operator purge/redrive/repair APIs remain P1 and require
a separate operator contract before implementation. Live recurring item rows are
excluded from terminal retention/GC.

## Security and Tenancy

- Every table primary key or leading index starts with `tenant_id` and
  `queue_id`.
- Service mode must authorize the principal against `tenant_id` and queue
  permission before running storage queries.
- Postgres roles, schemas, or row-level security may strengthen isolation, but
  the first implementation must at minimum enforce tenant predicates in every
  query and test negative cross-tenant access.
- Lease tokens are stored only as hashes.
- Payload and metadata are caller data and must pass configured size limits.

## Performance

These targets define the single-deployment (Tier-1) envelope. `postgres_native`
mirrors a proven single-Postgres `SKIP LOCKED` design and inherits its
single-deployment scale ceiling; per ADR-001 "Scale Claim Scoping" it delivers
Tier-1 and MUST NOT be cited as evidence for the horizontal envelope (TP-002 E1
vs the per-queue throughput floor E0: >=10M items/hr per queue). The claim path here is single-shard (scoped by
`shard_id`). The horizontal envelope is delivered by multi-shard claim with
cross-shard progress (TD-001), sharding & shard ownership (TD-003), and the
`object_log_sqlite_projection` backend (TD-004, `td-s3-object-log-sqlite-projection-mode`),
validated by TP-002 E2/E3. A multi-shard `postgres_native` queue is the per-shard
building block those designs compose; each shard remains a single-shard
`SKIP LOCKED` claim.

Targets inherit PRD success metrics: 10M items in a hot queue, the per-queue
throughput floor (TP-002 E0: >=10M items/hr per queue, preserved for every queue
at any scale), at least 1000 concurrently active queues per node (queue density,
TP-002 E2), and sub-second p95/p99 for core batch operations under representative
load.

Design constraints:

- Mutating operations are batch-first and use one transaction per request.
- Claim queries must use partial indexes and `FOR UPDATE SKIP LOCKED`.
- Hot queues must be shardable by `shard_id`; no implementation may assume a
  single Postgres table partition is sufficient forever.
- Metrics may be approximate when documented, but progress-bound risk must be
  trustworthy enough to trigger operational action.
- Telemetry overhead must be included in performance tests.

### Queue density (>=1000 active queues per node)

A node MUST sustain at least 1000 concurrently active queues without per-queue
resource blow-up. The Postgres-native reference backend therefore:

- uses ONE shared, bounded connection pool per node across all queues/shards
  (sized to the node, not per queue); it MUST NOT open a pool or a long-lived
  connection per queue or per `(queue, shard)`;
- runs ONE shared lease-expiry sweeper per node that scans due leases across many
  `(tenant, queue, shard)` partitions per pass with a bounded batch and bounded
  cadence (using `pqueue_items_lease_expiry_idx`), instead of one sweeper task
  per queue/shard;
- runs idempotency/terminal-retention GC and `pqueue_group_summary` reconcile as
  bounded shared batch jobs across queues/shards, not per-queue loops;
- relies on shared, partial, partition-pruned indexes so 1000+ active queues do
  not each require dedicated hot index memory beyond what their resident set
  justifies.

Aggregate single-node throughput is bounded by the node; the density requirement
is that the 1000th active queue costs only bounded incremental resource and still
meets its progress bound, not that one node sustains 1000x the per-queue floor.
This is validated by `queue_density_single_node_tests` (TP-002 E2).

## Testing

TD-002 implementation is not complete until these scenarios pass against a real
Postgres instance:

- Create queue and shard definitions transactionally.
- Reject stale `assignment_epoch` appends.
- Replay command log by shard position.
- Push duplicate `client_item_key` without mutation.
- Update pending item priority and verify claim order changes.
- Reject update of leased and terminal items.
- Claim concurrently with `SKIP LOCKED` and prove no duplicate active lease.
- Retry claim with same `request_id` and active leases; return same lease set.
- Renew active lease and reject stale token.
- Finalize complete, fail, retry, release, and retry exhaustion.
- Materialize lease expiry and preserve progress-bound eligible age.
- Expire request-id and item-key retention records.
- Reject cross-tenant reads and writes.
- Benchmark strict and group-aware claims on a 10M-item hot queue fixture.
- `group_batching` claim returns all eligible items for exactly the N
  highest-claim-order wholly-available groups and never a partial group; the next
  selected whole group exceeding `max_items` fails with `batch-too-large` and
  leases nothing.
- Concurrent `group_batching` claims never select overlapping groups into active
  leases; many concurrent claims over overlapping candidate sets never deadlock
  (canonical ascending lock-identity order; try-lock-skip for batch claims).
- Mixed-mode: a generic / `same_group_key` claim that leased a subset of group A
  makes A a contended non-candidate for a subsequent `group_batching` claim (skipped,
  not split); a `group_batching`-locked group cannot be split by a concurrent claim.
- Exact-on-read: the first overfetched candidates are invalidated by a mid-claim
  gate flip; the claim still returns the true next N eligible whole groups via
  revalidate/refill, and a group whose items become gate-blocked is excluded via the
  read-time gate predicate with no per-group summary rewrite.
- Atomic cohort lease is never split or double-leased under concurrency or across
  writer restart; `CohortExpired` appears in the log before any claimability change
  and survives replay; duplicate-push no-op survives replay; `group_key` reuse
  yields a new `cohort_id`; cohort benchmark uses `pqueue_cohorts_claim_idx` /
  `pqueue_cohorts_expiry_idx`, not a full scan.
- Discovery on a 10M-item hot queue reads `pqueue_group_summary` via index (no
  `pqueue_items` scan in the captured plan); a gate flip blocking only the oldest
  item of a group still reports the group at the next eligible item's age; the
  cross-shard merge reports the true cross-shard min age, never a per-shard top-N
  union; `as_of` is the minimum watermark across all shards read including a
  stale/unowned shard.
- High-frequency immediate `rearm` sustains target throughput without
  version-monotonicity or projection corruption; idle recurring inventory does not
  inflate discovery or `oldest_eligible_age_ms`; `PurgeItems` (targeted, and `force`
  while leased) leaves a consistent tombstone and recomputes the group summary.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| One Postgres deployment becomes a hidden central data plane and is mistaken for the horizontal-scale story | M | H | Horizontal scale is delivered by multi-shard claim (TD-001), sharding & shard ownership (TD-003), and the `object_log_sqlite_projection` backend (TD-004), each with a benchmark gate (TP-002 E2/E3). Keep shard keys, partitioning, and backend-profile boundaries in schema and tests so a single-shard deployment is never mistaken for the scaled envelope. |
| Per-group summary grain diverges across shards/backends, breaking cross-shard aggregation | M | M | Single `pqueue_group_summary` keyed `(tenant_id, queue_id, shard_id, group_key)`; one row per active group under co-residency; cross-shard merge by `(tenant_id, queue_id, group_key)` (TD-003). |
| JSON priority or metadata predicates cause slow scans | M | H | Use canonical `priority_sort`, narrow v1 predicates, and partial indexes. |
| `SKIP LOCKED` hides starvation under contention | M | H | Add progress-bound tests with skew and explicit oldest-eligible metrics. |
| Idempotency response reconstruction is inconsistent | M | M | Prefer transactional response persistence in Postgres-native mode. |
| Retention deletes state needed for replay | M | H | Gate deletion on command log, idempotency, terminal, and audit windows. |

## Review Checklist

- [x] TD-001 storage traits map to concrete Postgres tables.
- [x] API-001 operations map to transaction flows.
- [x] Durable append and projection commit are in one transaction.
- [x] Shard epoch fencing is explicit.
- [x] Claim concurrency uses row locks and duplicate-lease tests.
- [x] Progress bounds and group-aware claims are represented.
- [x] Terminal retention is bounded without adding P1 operator APIs.
- [x] Performance and conformance evidence is required before implementation is
  accepted.
