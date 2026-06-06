---
ddx:
  id: td-postgres-native-reference-mode
  depends_on:
    - td-storage-architecture-backend-contracts
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-rust-workspace-and-toolchain-policy
    - prd
    - concerns
---

# Technical Design: TD-002 Postgres-Native Reference Mode

**Contract**: API-001 | **ADR**: ADR-001 | **Scope**: Postgres-native backend

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
- Shard epoch fencing without pqueue-owned node discovery or cluster consensus.
- Retention and compaction rules needed for bounded storage growth.
- Indexing and partitioning assumptions for 10M-item hot queues.

Out of scope:

- Cross-backend object-log manifests and SQLite projections.
- P1 operator APIs for redrive, purge, repair, migration, and active-queue
  discovery.
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
  updated_at timestamptz not null,
  primary key (tenant_id, queue_id, shard_id),
  foreign key (tenant_id, queue_id)
    references pqueue_queues (tenant_id, queue_id)
);
```

`placement` stores control-plane routing metadata. It is not a node-discovery
protocol. Service nodes consume assignments from Postgres and pass
`assignment_epoch` into data-plane mutations as a fencing token.

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

`eligible_since` is null while the item is not eligible. When an item becomes
eligible again after lease expiry, `eligible_since` must preserve the earlier
eligible time unless the item became ineligible because of a caller-controlled
delay such as `not_before` or retry backoff. This supports PRD progress-bound
semantics.

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
and leases only that group for the request. Server-selected group choice must
include fairness under skew and must not starve other groups beyond the queue's
progress bound.

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

## Lease Expiry

Lease expiry may be materialized lazily during claim or by a background task.
Either approach must append a `LeaseExpired` command before making expired items
claimable again, unless TD-002 is amended to prove that lease expiry can be a
pure projection rule without breaking replay or audit.

The first implementation should prefer lazy expiry in the claim path plus a
bounded background sweeper for metrics freshness. The sweeper must be shard and
epoch fenced, batch-limited, and safe to run concurrently.

## Retention and Compaction

Postgres-native mode must enforce bounded storage growth:

- Request idempotency records expire after `request_id_retention_ms`.
- Item-key convergence records expire after `client_item_key_retention_ms`.
- Terminal item records expire after `terminal_retention_ms` unless a later
  operator contract marks them retained for inspection or archival.
- Command log rows are retained at least until all associated idempotency,
  terminal retention, replay, and audit windows have expired.

Deleting terminal item records does not delete command log rows still required
for replay or audit. Purge/redrive APIs remain P1 and require a separate
operator contract before implementation.

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

Targets inherit PRD success metrics: 10M items in a hot queue, millions of
writes per hour per deployment, and sub-second p95/p99 for core batch
operations under representative load.

Design constraints:

- Mutating operations are batch-first and use one transaction per request.
- Claim queries must use partial indexes and `FOR UPDATE SKIP LOCKED`.
- Hot queues must be shardable by `shard_id`; no implementation may assume a
  single Postgres table partition is sufficient forever.
- Metrics may be approximate when documented, but progress-bound risk must be
  trustworthy enough to trigger operational action.
- Telemetry overhead must be included in performance tests.

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

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| One Postgres deployment becomes a hidden central data plane | M | H | Keep shard keys, partitioning, and backend profile boundaries in schema and tests. |
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
