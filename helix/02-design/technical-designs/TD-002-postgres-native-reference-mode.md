---
ddx:
  id: td-postgres-native-reference-mode
  depends_on:
    - td-storage-architecture-backend-contracts
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - adr-rust-workspace-and-toolchain-policy
    - prd
    - concerns
  review:
    self_hash: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      concerns: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T00:01:26Z"
---

# Technical Design: TD-002 Postgres-Native Reference Mode

> **Implementation status (hexagonal migration, Phase 6):** the original `fireweed-postgres` crate (which
> implemented the now-removed storage traits) has been **deleted**. The postgres adapter is **deferred**
> and will be (re)built **fresh to the engine ports** following the durable-adapter template proven by
> sqlite (durable command log + projection rebuilt-from-log) when a database is provisioned. The
> data-model intent below (postgres as an atomic-class backend) still holds; only the trait surface it
> targets has changed (engine `Backend`/`ClaimPort`/`PushPort`/… instead of the old storage traits). See
> [ADR-007](../adr/ADR-007-hexagonal-architecture-and-two-interfaces.md) and
> [`hexagonal-migration-plan.md`](../../04-build/hexagonal-migration-plan.md).


**Contract**: API-001 | **ADR**: ADR-001, ADR-004, ADR-008 | **Scope**: Postgres-native backend

## Scope

This technical design defines the first concrete storage backend for fireweed:
`postgres_native`. In this mode, Postgres provides the control plane, durable
command log, operational projection (the **relational projection family**),
idempotency state, leases, and queue metrics. Per ADR-008 the queue is the unit
of sharding: a whole queue is owned by exactly one node, so `postgres_native`'s
log and projection are per-queue and its claim is a single owner-local
`FOR UPDATE SKIP LOCKED` statement.

In scope:

- Postgres logical schema for queue definitions, queue-owner leases, command
  log, queue items, leases, idempotency, and metrics.
- Transaction boundaries for API-001 mutating operations.
- Claim query shape for strict and bounded-relaxed ordering.
- The Postgres binding of TD-003's queue-epoch fence (the `assignment_epoch`
  column + the append-transaction stale-epoch reject) without fireweed-owned node
  discovery or cluster consensus; the queue-owner lease lifecycle that allocates
  and advances `assignment_epoch` (and durably fences it into the queue-owner row
  at acquire time) is specified in TD-003 (`td-sharding-and-shard-ownership`) and
  stored in `fireweed_queue_owners`.
- Retention and compaction rules needed for bounded storage growth.
- Indexing and internal table-partitioning assumptions for 10M-item hot queues.

Out of scope:

- Cross-backend object-log manifests and SQLite projections.
- The pluggable control plane's non-Postgres implementation. `ControlPlaneStore`
  is a pluggable seam (ADR-008); Postgres is the default and the one this TD
  specifies. The deferred object-store control plane is spike-gated elsewhere.
- P1 operator APIs for redrive, repair, and migration. (Active-scope discovery is
  in-contract, served from `fireweed_group_summary`; targeted recurring teardown
  `PurgeItems` is in-band native scope. Broad operator purge/redrive/repair remains
  a separate P1 operator contract.)
- Exact SQL migration filenames and generated Rust structs.
- Physical deployment sizing for a specific managed Postgres provider.

## Technical Approach

`postgres_native` is the reference correctness backend and the reference member
of the **relational projection family** (ADR-008, as amended by ADR-013):
`fireweed_items` is a materialized cache with a persisted applied-high-water and
claim is an SQL `FOR UPDATE SKIP LOCKED` statement. It uses ordinary
Postgres transactions to commit the durable command record and the operational
projection together — the command log row is fully durable in the same
transaction that mutates the projection, and success is returned only after
both commit (ADR-013's universal ordering: log durable → projection applied →
client acknowledged). This keeps the first implementation simple, gives
low-latency small-batch commits, and creates executable semantics for later
backends to match through conformance tests. The DB-resident projection runs
the **relational reconnect-after-crash** conformance class (TD-001) in addition
to the ADR-013 rebuild-from-log obligation: `fireweed_items` and its peers MUST be
reconstructable by replaying `fireweed_commands` from genesis or from a snapshot,
and the log is never optional.

The backend still follows TD-001's capability boundaries:

- `ControlPlaneStore` (the default Postgres implementation of the pluggable seam):
  queue definitions, queue-owner assignment, backend profile, and assignment
  epochs.
- `LogStore`: append-only command records with monotonic per-queue positions.
- `ProjectionStore`: queue item state, claim planning, lease state,
  idempotency, and metrics (relational family).
- `SnapshotStore`: optional in v1 for Postgres-native mode; logical dump or
  checkpoint support may be added after the first backend passes conformance.

## Data Model

The schema is logical. Implementation may split or rename tables, but must
preserve these records and indexes.

### Control Plane

```sql
create table fireweed_queues (
  tenant_id text not null,
  queue_id text not null,
  priority_model jsonb not null,
  ordering_mode text not null,
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
  created_at timestamptz not null,
  updated_at timestamptz not null,
  primary key (tenant_id, queue_id)
);

create table fireweed_queue_owners (
  tenant_id text not null,
  queue_id text not null,
  assignment_epoch bigint not null,
  state text not null,
  active_owner_id text,
  target_owner_id text,
  lease_expires_at timestamptz,
  updated_at timestamptz not null,
  primary key (tenant_id, queue_id),
  foreign key (tenant_id, queue_id)
    references fireweed_queues (tenant_id, queue_id)
);
```

There is no `shard_count` column: the queue is the unit of sharding (ADR-008), so
a queue maps to exactly one owner. `fireweed_queue_owners` holds at most one active
owner lease per `(tenant_id, queue_id)`. Service nodes consume assignments from
Postgres and pass `assignment_epoch` into data-plane mutations as a fencing token.

`state` values are `unassigned`, `assigned`, and `draining`. The queue-owner
lease operations `acquire_queue_lease`, `renew_queue_lease`, `begin_drain`, and
`release_queue_lease` (TD-003) operate on the `active_owner_id`, `target_owner_id`,
`lease_expires_at`, and `assignment_epoch` columns transactionally.
`acquire_queue_lease` allocates a strictly greater `assignment_epoch` and updates
the row in the same transaction — this IS the durable epoch fence for
`postgres_native`; `renew_queue_lease` preserves the epoch. The data-plane append
transaction (Transaction Flows step 3) validates `expected_epoch == current
assignment_epoch`, which is the safety fence. `target_owner_id` MAY differ from
`active_owner_id` transiently during reassignment; safety depends on the epoch,
not on the two agreeing. The full ownership/lease/drain/recovery mechanism is
TD-003's; this table is its Postgres binding.

`group_key` is an **ordering/compatibility** concern only (ADR-004 D2 / ADR-008),
never a placement key, and there is no `group_co_residency` flag (removed from the
contract and the config-identity hash). Because the whole queue lives on one
owner, every item of a `group_key` is co-resident **by construction**; this is
what makes whole-group (`compatibility.group_batching`) and whole-cohort
(`compatibility.whole_cohort`) claims owner-local and atomic and lets a
single-`group_key` claim return exact per-group order. The `group_key` topology
(cohort-keyed by `callback_id` versus job-keyed by `job_id`) is per ADR-004
(`adr-granularity-mapping-and-claim-domain`). `group_key` carries no progress
meaning (progress remains queue-global and is computed locally on the owner).

`recurring` is an immutable per-queue flag. On a `recurring` queue, a `rearm`
finalize returns an item to `pending` without a terminal transition (see
`BatchFinalize`); one-shot and recurring items are never mixed in one queue.

> **Internal storage partitioning (non-normative for ownership).** A
> `postgres_native` deployment MAY *physically* partition the large item/command
> tables — e.g. Postgres declarative `partition by hash (tenant_id, queue_id)`,
> `N` partitions (default 16, power-of-2) — purely for vacuum and index-size
> isolation on 10M-item hot queues. This `% N` partition is an **internal storage
> detail**: it is client-invisible and is **not** an ownership, routing, or
> progress unit (ADR-008). It does not bound how many nodes the queue population
> spreads across, and it never appears in a result ordering or a client field.

### Durable Command Log

```sql
create table fireweed_commands (
  tenant_id text not null,
  queue_id text not null,
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
  primary key (tenant_id, queue_id, sequence),
  unique (tenant_id, queue_id, command_id)
);
```

`sequence` is allocated under the same transaction that validates the queue
epoch. Implementations may use a per-queue sequence table, advisory locks scoped
to `tenant_id/queue_id`, or row locking on `fireweed_queue_owners`. The result must
be a monotonic command position per queue.

### Projection

```sql
create table fireweed_items (
  tenant_id text not null,
  queue_id text not null,
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

A backend MAY carry additional per-item operational columns to realize the
lifecycle command arms (`FenceLease`/`UnfenceLease`, `ReplacePending`, retry
exhaustion) directly. The sqlite reference projection carries `fenced` (operator
lease fence), `superseded` (the pending item replaced by an upsert, excluded from
the active `client_item_key` partial-unique index and from eligibility), and
`max_attempts` (the per-item retry bound, denormalized from the queue's
`retry_policy`). In the production Postgres mode these are realized without
dedicated columns where cheaper — fence via the `assignment_epoch` fence record,
supersede via the `client_item_key` tombstone/retention table, and the retry
bound read from the queue definition — but the *behavior* of every arm is
identical across both projection families (the conformance core class).

### Per-Group Summary Projection

There is exactly ONE per-group summary projection, `fireweed_group_summary`, keyed
by `(tenant_id, queue_id, group_key)`. It is the single source of truth
for (a) `group_batching` oldest-group selection (g1), (b) `DiscoverActiveScopes`
group-granularity ranking (g4), and (c) per-group observability. There is no
separate `fireweed_active_scope_summary` table. This projection is authored once
here and is consumed by g1 selection/locking and g4 discovery alike.

```sql
-- Single canonical per-group summary projection.
-- Consumers: (a) g1 whole-group selection + per-group lock anchor;
--            (b) g4 DiscoverActiveScopes ranking; (c) per-group observability.
create table fireweed_group_summary (
  tenant_id text not null,
  queue_id  text not null,
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
  primary key (tenant_id, queue_id, group_key)
);

-- g1 selection: oldest-N groups by representative claim key.
create index fireweed_group_summary_select_idx
  on fireweed_group_summary (
    tenant_id, queue_id,
    rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id
  )
  where oldest_eligible_at is not null;

-- g4 discovery: rank the queue's groups by oldest-eligible age (local to the owner).
create index fireweed_group_summary_rank_idx
  on fireweed_group_summary (tenant_id, queue_id, oldest_eligible_at)
  where oldest_eligible_at is not null;
```

Because the whole queue lives on one owner, a `group_key` has exactly one summary
row; this grain has exactly one row per active group and stays coherent for the
local-SQLite backend (TD-004). There is no cross-shard aggregation: the queue's
oldest-eligible age is a local `now() - min(oldest_eligible_at)` over the queue's
rows on the owner.

Consistency model (binding):

1. **`oldest_eligible_at` and the `rep_*` representative key are authoritative and
   exact-on-read.** They are maintained in the SAME transaction as item mutations
   that change a group's eligible set (push, update, claim, finalize, retry,
   lease-expiry materialization, rearm) using the same Eligibility Precedence
   predicate (API-001, authored by g2). g1 group selection and g4 discovery
   ranking MUST rely on these, never on the counts.
2. **Gate flips are NOT applied synchronously to every affected group's summary
   row.** A queue-scoped `SetGates` flip (g2) is O(keys), not O(groups or items),
   and changes no item's `eligible_since`. The summary keeps the authoritative
   fields correct WITHOUT a per-group rewrite via exact-on-read: the read path (g1
   selection, g4 discovery, metrics) joins the candidate group's gate keys to
   current gate state (`fireweed_gate_state`), and if the stored representative item
   is gate-blocked at read time it advances to the group's next item open under
   all gate keys; a group all of whose items are gate-blocked is excluded
   entirely. This distinguishes "whole group blocked" (group excluded) from
   "oldest item blocked" (advance to the next open item). Because a group is
   gate-blocked as a unit only when a shared gate key it carries is blocked, the
   join is O(blocked keys), not O(items). (`fireweed_gate_state` is the queue's
   gate-state projection maintained by `SetGates` (g2): one row per gate key
   carrying its current open/blocked generation; its authoritative shape is owned
   by the g2 gate model in API-001. TD-002 consumes it read-only at gate-revalidation
   time.)
3. **`eligible_item_count` / `at_risk_count` MAY be lagged or approximate** and
   MUST be documented as such when served. They are routing/observability hints,
   never selection or completeness inputs (whole-group/whole-cohort completeness
   re-derives membership under the group lock). After a gate flip the counts MAY
   momentarily over-count gate-blocked items until a bounded, asynchronous
   recompute (scoped to the groups sharing the flipped key) corrects them.
4. A group row is deleted (or `oldest_eligible_at` / `rep_*` set null) when its
   last eligible item becomes ineligible. Recompute on push/finalize/expiry is
   bounded per affected group, never O(items) per read.
5. The queue-global oldest-eligible age is the local
   `now() - min(oldest_eligible_at)` over the queue's gate-filtered group rows on
   the owner (TD-003 per-queue progress); discovery ranking is a local top-N by
   `oldest_eligible_at`. Counts are the (possibly lagged) per-row counts.

### Cohort Projection

```sql
create table fireweed_cohorts (
  tenant_id          text    not null,
  queue_id           text    not null,
  group_key          text    not null,   -- cohort key (logical identity)
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

create index fireweed_cohorts_claim_idx
  on fireweed_cohorts (tenant_id, queue_id, state)
  where state = 'complete';

create index fireweed_cohorts_expiry_idx
  on fireweed_cohorts (tenant_id, queue_id, cohort_created_at)
  where state in ('forming', 'complete');
```

`fireweed_cohorts` is a projection of the command log (ADR-001 log-is-truth),
maintained transactionally with member mutations. `member_count` and `state` MUST
be updated in the same transaction as member inserts, AFTER `client_item_key`
idempotency convergence, so duplicate pushes do not increment `member_count` or
overfill. A new distinct member insert that would exceed `cohort_size` MUST be
rejected (`conflict`). `state=complete` is set only when
`member_count == cohort_size`. `fireweed_cohorts` records cohort completion state
ONLY; `oldest_eligible_at` and eligible counts for cohort members come from the
single `fireweed_group_summary` projection (which excludes not-yet-claim-eligible
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
g6); the cohort key is the existing `group_key`, co-resident on the queue's owner
by construction, so no cohort-to-shard derivation exists. Cohort completion state
lives in `fireweed_cohorts` (below), not on the item row. `fireweed_cohorts.cohort_size`
is authoritative; the `fireweed_items.cohort_size` copy is denormalized and MUST
match (a divergent push value is a `conflict`).

`recurrence_until` is the optional per-item recurrence drain instant on a
`recurring` queue. A `rearm` past `recurrence_until` MUST terminate the item
instead of re-arming it.

### Idempotency

```sql
create table fireweed_request_idempotency (
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

create table fireweed_item_key_retention (
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

`fireweed_item_key_retention` keeps duplicate push convergence after terminal item
records are purged, until `client_item_key_retention_ms` expires.

## Required Indexes

The first implementation must include indexes equivalent to:

```sql
create index fireweed_items_claim_strict_idx
  on fireweed_items (
    tenant_id,
    queue_id,
    lifecycle_state,
    priority_sort,
    created_at,
    item_id
  )
  where lifecycle_state = 'pending';

create index fireweed_items_eligible_age_idx
  on fireweed_items (
    tenant_id,
    queue_id,
    lifecycle_state,
    eligible_since
  )
  where lifecycle_state = 'pending';

create index fireweed_items_lease_expiry_idx
  on fireweed_items (
    tenant_id,
    queue_id,
    lifecycle_state,
    lease_expires_at
  )
  where lifecycle_state = 'leased';

create index fireweed_items_group_claim_idx
  on fireweed_items (
    tenant_id,
    queue_id,
    group_key,
    priority_sort,
    created_at,
    item_id
  )
  where lifecycle_state = 'pending';

create index fireweed_commands_replay_idx
  on fireweed_commands (tenant_id, queue_id, sequence);

create index fireweed_request_idempotency_expiry_idx
  on fireweed_request_idempotency (expires_at);
```

Implementations may use partial indexes per lifecycle state and internal
declarative table partitioning by `hash(tenant_id, queue_id)` (the storage detail
above) for vacuum/index-size isolation. A 10M-item hot queue must not rely on a
full-table scan for claim, lease expiry, idempotency, or progress metrics.

Discovery is served from `fireweed_group_summary` and its rank index
(`fireweed_group_summary_rank_idx`, joined to `fireweed_gate_state` at read time), not
from ad hoc aggregates on `fireweed_items`. The item indexes ordered by
`priority_sort`/`eligible_since` are insufficient for
`GROUP BY group_key ORDER BY min(oldest_eligible_at)` at 10M-item scale; the
per-group summary exists specifically to avoid that aggregate scan.

## Active-Scope Discovery Reads

`DiscoverActiveScopes` (g4) serves from the maintained per-group summary projection
`fireweed_group_summary`, keyed `(tenant_id, queue_id, group_key)`. No separate
`fireweed_active_scope_summary` table exists. Discovery never scans `fireweed_items`,
and because the queue has one owner there is no cross-owner merge.

Group-granularity read (local to the owner; joins live gate state):

```sql
select gs.group_key,
       gs.oldest_eligible_at,
       gs.eligible_item_count,
       gs.updated_at
from fireweed_group_summary gs
where gs.tenant_id = $1 and gs.queue_id = $2
  and gs.oldest_eligible_at is not null
-- gate-current derivation (advance past a gate-blocked representative) is applied
-- by the read path joining fireweed_gate_state; see the consistency model.
order by gs.oldest_eligible_at asc
limit $3;   -- top-N by oldest-eligible age
```

Queue-granularity discovery derives a per-queue oldest-eligible from the SAME
table as `min(oldest_eligible_at)` over the queue's group rows, exposed as a
`granularity=queue` descriptor (no `group_key` field, no materialized queue
rollup). It MUST NOT be stored or exposed as a `group_key = null` row; the
projection's `group_key` column is `not null`.

**Discovery consistency model.** `oldest_eligible_at` is authoritative and exact
(maintained transactionally with item mutations), so `oldest_eligible_age_ms` is
exact as of `as_of`. `eligible_item_count` MAY be lagged/approximate and MUST be
documented as such. A g2 `SetGates` flip is queue-scoped, O(1), and writes no item
or summary rows; discovery applies the gate predicate at read time by joining
`fireweed_gate_state`, advancing past a gate-blocked representative to the next
Eligibility-Precedence-eligible item, and omits a group only when no item in it is
currently eligible.

**`as_of` is the owner's observed projection frontier.** `response.as_of` MUST be
the minimum per-row `updated_at` watermark across the rows read for the result,
including the watermark of any candidate row skipped during gate revalidation,
so callers can reason about summary lag. The returned top-N is the true top-N over
the queue's groups on its owner. The queue's owner, lease validity, and epoch
fencing are owned by TD-003; a queue with no live owner for longer than
`progress_bound_ms` is a progress-bound violation surfaced by TD-003.

## Transaction Flows

Every mutating operation follows this order inside one transaction unless
otherwise stated:

1. Authorize tenant and queue access before opening data-plane mutation state.
2. Load queue definition and the queue-owner row.
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
- On a cohort-enabled queue (`cohort_policy.enabled`), validate
  `cohort_size <= max_cohort_size` and that `cohort_size` matches the existing
  `fireweed_cohorts` row (else `conflict`). After `client_item_key` idempotency
  convergence (duplicate is a no-op with no count change), for a new distinct
  member upsert `fireweed_cohorts` incrementing `member_count` (reject overfill with
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

`BatchClaim` must atomically select and lease items using row-level locks. The
claim is owner-local: the queue has one owner, so there is no shard filter and no
cross-owner fan-out.

```sql
-- Conceptual query shape. The metadata and compatibility predicates are
-- generated from queue policy and the API-001 claim request.
with candidates as (
  select item_id
  from fireweed_items
  where tenant_id = $1
    and queue_id = $2
    and lifecycle_state = 'pending'
    and (not_before is null or not_before <= now())
    and eligible_since is not null
    -- and queue eligibility policy matches metadata
    -- and compatibility predicates match group_key/metadata
  order by progress_guard_sort, priority_sort, created_at, item_id
  limit $3
  for update skip locked
)
update fireweed_items i
set lifecycle_state = 'leased',
    lease_token_hash = $4,
    lease_expires_at = $5,
    worker_id = $6,
    item_version = item_version + 1,
    updated_at = now()
from candidates c
where i.item_id = c.item_id
returning i.*;
```

`progress_guard_sort` is conceptual: it is a **query-time derivation** (over
`eligible_since` against `progress_bound_ms`, ahead of `priority_sort`), NOT a
stored `fireweed_items` column. The strict-claim index orders by
`priority_sort, created_at, item_id` (the covering subset for the normal window);
the progress-protection window is the derivation prepended at claim time.
Implementations must ensure items near `progress_bound_ms` cannot be bypassed
indefinitely by relaxed ordering or group-aware selection. A valid first implementation may use strict ordering for
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

When the request pins a single `group_key` (or the queue's claim mode resolves to
one group), the candidate CTE filters to that group, and the existing
`order by progress_guard_sort, priority_sort, created_at, item_id` IS the exact
per-group order — no new query is required for deterministic-in-domain ordering
(ADR-004), and no routing is needed because the group is already owner-local. The
single queue-global progress guard (`progress_guard_sort`) is unchanged and is NOT
evaluated per group; progress is queue-global and local to the owner (FR-12).

#### `group_batching` (whole-eligible-group claim)

For `group_batching` (`group_completeness=whole_eligible`, `max_groups=N`), claim
runs inside one serialized critical section (one Postgres transaction) with
group-level locking. Because the queue has one owner, every group is owner-local
by construction (ADR-008); selection is over the queue's groups with no shard
resolution.

1. **Overfetch candidates.** Open a cursor over `fireweed_group_summary` for the
   queue, ordered by each group's representative claim key
   (`rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id`),
   honoring the queue-global progress-protection window first under `ordering_mode`.
   Fetch more than N candidates (`N + k`, refill on demand). Group eligibility is
   defined solely by the Eligibility Precedence subsection; the candidate
   `eligible_item_count` is a hint only.
2. **Lock + revalidate, in canonical order.** Sort the candidate set by group lock
   identity (`hash(tenant_id, queue_id, group_key)`) ascending and, in
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
   `max_items` ceiling, or the queue's candidate groups are exhausted — so the claim
   returns the true next N groups even if early candidates were invalidated by a
   mid-claim gate flip or concurrent lease. If the next group in order alone exceeds
   `max_items`, roll back and return envelope `batch-too-large`, leasing nothing.

There is NO rate-admission step in this flow (D3). Group-level locking (not
item-level `FOR UPDATE SKIP LOCKED` over an unlocked group selection) is what
guarantees two concurrent claims never split a group.

```sql
-- group_batching (N groups): overfetch candidates, then lock-per-group (canonical
-- order, try-lock-skip), revalidate, lease each valid locked group whole, refill.
-- Run inside one transaction on the queue's owner; every group is owner-local.
with candidate_groups as (
  select group_key
  from fireweed_group_summary           -- single per-group summary (also backs discovery)
  where tenant_id = $1 and queue_id = $2
    and oldest_eligible_at is not null            -- has a current eligible representative
    -- and metadata_equals predicate satisfiable for the group (if present)
  order by rep_progress_guard_sort, rep_priority_sort, rep_created_at, rep_item_id
  limit $overfetch                                -- N + k; cursor refills on demand
)
-- application logic, candidates sorted by lock identity ascending (deadlock-free):
--   for each candidate in canonical lock order:
--     if not pg_try_advisory_xact_lock(hash(tenant,queue,group_key)): skip (contended)
--     re-read eligible items of group_key under live predicate (gate state + metadata_equals);
--       if zero eligible OR any active lease held by another claim: discard (do not count)
--     else lease all eligible items (FOR UPDATE, no SKIP LOCKED inside group)
--   stop when next group would exceed max_items; if next group alone exceeds max_items
--     -> rollback, return batch-too-large; refill cursor if candidates exhausted before N.
```

#### `whole_cohort` (complete-cohort claim)

For `compatibility.whole_cohort` (g6) the claim is one transaction, owner-local, and
locks the cohort first:

1. Candidate select: most-urgent `fireweed_cohorts` row with `state='complete'` (via
   `fireweed_cohorts_claim_idx`), ordered by the cohort's representative
   `priority_sort`, limit 1.
2. Lock the cohort row `FOR UPDATE` (no `SKIP LOCKED` for the chosen cohort), then
   lock all members `FOR UPDATE` in deterministic claim order. Recheck
   `state='complete'` AND every member's Eligibility Precedence conditions 1-5 under
   the lock. If the row cannot be locked immediately or the recheck fails, skip to
   the next complete cohort; if none, return empty. NEVER block, NEVER partially
   lease.
3. Transition every member to `leased` under one `cohort_lease_token`, set
   `fireweed_cohorts.state='leased'` and `cohort_lease_token_hash`, increment
   `item_version` per member, append one claim command covering the whole cohort.
4. If the selected cohort's `cohort_size > max_items`, fail the envelope with
   `batch-too-large` (cannot occur when `max_items >= max_cohort_size`).

Item and `group_batching` claims on a cohort-enabled queue MUST take the same
`fireweed_cohorts` row lock and exclude every member of any non-terminal cohort. The
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
  `fireweed_group_summary` row (recompute `oldest_eligible_at` from the scope's
  remaining eligible items; `oldest_eligible_at` stays authoritative/exact,
  `eligible_item_count` MAY be served lagged). Recurring observability counters
  (`recurring_pending` / `recurring_leased`) are served from the metrics
  projection, NOT from `fireweed_group_summary`. (The metrics projection is the
  maintained per-queue metrics record of TD-001's Logical Projection Records —
  lifecycle counts, active leases, recurring counters, `oldest_eligible_age_ms`,
  `progress_bound_risk_count` — keyed `(tenant_id, queue_id)` on the owner; its
  exact column list is an implementation detail bounded by that record.)

## Lease Expiry

Lease expiry may be materialized lazily during claim or by a background task.
Either approach must append a `LeaseExpired` command before making expired items
claimable again, unless TD-002 is amended to prove that lease expiry can be a
pure projection rule without breaking replay or audit.

The first implementation should prefer lazy expiry in the claim path plus a
bounded background sweeper for metrics freshness. The sweeper must be queue and
epoch fenced, batch-limited, and safe to run concurrently.

## Cohort Completion and Expiry

A queue- and epoch-fenced sweeper MUST, for any `fireweed_cohorts` row in
`forming`/`complete` whose expiry deadline
`min(cohort_created_at, first_eligible_at) + completion_bound_ms` has passed: take
the cohort row lock, recheck the deadline and state under the lock, append
`CohortExpired` (recording `expire_command_pos`) BEFORE setting members terminal
`failed`/`cohort-incomplete`, set `state='terminal'` and `retention_until` — all in
one transaction. The sweeper and any claim contend on the same row lock,
linearizing claim-vs-expiry (leased XOR expired). Because `CreateQueue` enforces
`completion_bound_ms <= progress_bound_ms`, expiry always linearizes before a
withheld eligible member can breach the queue-global progress bound (FR-12).
Whole-cohort finalize/release/retry update `fireweed_items` and `fireweed_cohorts.state`
in one transaction.

## Recurring Item Teardown (`PurgeItems`)

Targeted recurring teardown (`PurgeItems`) is in-band native scope (P0); broad
operator purge/redrive/retention remains a separate P1 operator contract, and the
two MUST NOT be conflated. A `PurgeItems` removal deletes the `fireweed_items` row
(and, with `force`, the lease), MUST be recorded as a durable `PurgeItemsCommand`,
and MUST write a tombstone keyed by `(tenant_id, queue_id, client_item_key)`
retained for at least `client_item_key_retention_ms`. A purge that targets an item
with an active lease MUST return `conflict` unless `force=true`. The existing
`unique (tenant_id, queue_id, client_item_key)` constraint enforces the
single-live-recurring-item-per-key invariant; once purged, the tombstone (not a
retained item row) carries replay/audit, and a re-push after the tombstone window
inserts a fresh item. The same transaction recomputes the affected
`fireweed_group_summary` row. Retention/GC MUST exclude live recurring rows and MUST
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
against E0's portable correctness, progress, and bounded-resource contract). The claim path
here is a single owner-local `SKIP LOCKED` statement. The horizontal envelope is
delivered by **cross-queue scale-out** (ADR-008) — distributing queues across
owners — with per-queue ownership (TD-003) and the `object_log_sqlite_projection`
backend (TD-004, `td-s3-object-log-sqlite-projection-mode`), validated by TP-002
E2/E3. A `postgres_native` queue is one owner-local `SKIP LOCKED` claim; scale is
achieved by running many such queues across many owners, not by sharding within a
queue.

Targets inherit PRD success metrics: 10M items in a hot queue; the portable E0
contract for exact outcomes, monotonic queue-global progress, and bounded shared
resources; and the canonical density shape of at least 1,000 cold queues plus
one hot queue per node (TP-002 E2). Core-operation throughput and p50/p95/p99
are reported for the declared topology, while pass/fail uses exact results,
structural query bounds, declared resource ceilings, and interleaved same-run
comparisons.

Design constraints:

- Mutating operations are batch-first and use one transaction per request.
- Claim queries must use partial indexes and `FOR UPDATE SKIP LOCKED`.
- Hot queues must be internally partitionable by `hash(tenant_id, queue_id)` (the
  storage detail above); no implementation may assume a single Postgres table
  partition is sufficient forever. This partitioning is for vacuum/index-size
  isolation only, never an ownership or routing unit.
- Metrics may be approximate when documented, but progress-bound risk must be
  trustworthy enough to trigger operational action.
- Telemetry overhead must be included in performance tests.

### Queue density (>=1000 active queues per node)

A node MUST sustain at least 1000 concurrently active queues without per-queue
resource blow-up. The Postgres-native reference backend therefore:

- uses ONE shared, bounded connection pool per node across all queues (sized to
  the node, not per queue); it MUST NOT open a pool or a long-lived connection per
  queue;
- runs ONE shared lease-expiry sweeper per node that scans due leases across many
  `(tenant, queue)` partitions per pass with a bounded batch and bounded cadence
  (using `fireweed_items_lease_expiry_idx`), instead of one sweeper task per queue;
- runs idempotency/terminal-retention GC and `fireweed_group_summary` reconcile as
  bounded shared batch jobs across queues, not per-queue loops;
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

- Create queue and queue-owner definitions transactionally.
- Reject stale `assignment_epoch` appends.
- Replay command log by per-queue position.
- Push duplicate `client_item_key` without mutation.
- Update pending item priority and verify claim order changes.
- Reject update of leased and terminal items.
- Claim concurrently with `SKIP LOCKED` and prove no duplicate active lease.
- Retry claim with same `request_id` and active leases; return same lease set.
- Renew active lease and reject stale token.
- Finalize complete, fail, retry, release, and retry exhaustion.
- Materialize lease expiry and preserve progress-bound eligible age.
- Reconnect-after-crash durability: after a process kill the DB-resident
  projection still returns acknowledged state on reconnect (the relational
  durability conformance class, TD-001).
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
  yields a new `cohort_id`; cohort benchmark uses `fireweed_cohorts_claim_idx` /
  `fireweed_cohorts_expiry_idx`, not a full scan.
- Discovery on a 10M-item hot queue reads `fireweed_group_summary` via index (no
  `fireweed_items` scan in the captured plan); a gate flip blocking only the oldest
  item of a group still reports the group at the next eligible item's age; the
  ranking is the true owner-local top-N by oldest-eligible age; `as_of` is the
  minimum watermark across the rows read.
- High-frequency immediate `rearm` sustains target throughput without
  version-monotonicity or projection corruption; idle recurring inventory does not
  inflate discovery or `oldest_eligible_age_ms`; `PurgeItems` (targeted, and `force`
  while leased) leaves a consistent tombstone and recomputes the group summary.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| One Postgres deployment becomes a hidden central data plane and is mistaken for the horizontal-scale story | M | H | Horizontal scale is delivered by cross-queue scale-out (ADR-008) — per-queue ownership (TD-003) and the `object_log_sqlite_projection` backend (TD-004), each with a benchmark gate (TP-002 E2/E3). Keep the per-queue ownership boundary and backend-profile boundaries in schema and tests so a single-queue deployment is never mistaken for the scaled envelope. |
| Per-group summary grain diverges across backends | M | M | Single `fireweed_group_summary` keyed `(tenant_id, queue_id, group_key)`; one row per active group; same grain in the SQLite local projection (TD-004). |
| JSON priority or metadata predicates cause slow scans | M | H | Use canonical `priority_sort`, narrow v1 predicates, and partial indexes. |
| `SKIP LOCKED` hides starvation under contention | M | H | Add progress-bound tests with skew and explicit oldest-eligible metrics. |
| Idempotency response reconstruction is inconsistent | M | M | Prefer transactional response persistence in Postgres-native mode. |
| Retention deletes state needed for replay | M | H | Gate deletion on command log, idempotency, terminal, and audit windows. |

## Review Checklist

- [x] TD-001 storage traits map to concrete Postgres tables (relational projection
      family).
- [x] API-001 operations map to transaction flows.
- [x] Durable append and projection commit are in one transaction.
- [x] Queue-epoch fencing is explicit (Postgres binding of TD-003's fence: the
      `assignment_epoch` column + append-tx validation).
- [x] Claim concurrency uses row locks and duplicate-lease tests; claim is a single
      owner-local `SKIP LOCKED` statement (no shard filter, no fan-out).
- [x] Progress bounds and group-aware claims are represented; progress is queue-global
      and computed locally on the owner.
- [x] Queue is the unit of sharding (ADR-008): no `shard_count`, no `shard_id`
      columns, no `group_co_residency`; `hash(tenant,queue)%N` is internal table
      partitioning only.
- [x] Terminal retention is bounded without adding P1 operator APIs.
- [x] Performance and conformance evidence is required before implementation is
      accepted; horizontal envelope is cross-queue scale-out.
