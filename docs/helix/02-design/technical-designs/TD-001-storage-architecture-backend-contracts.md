---
ddx:
  id: td-storage-architecture-backend-contracts
  depends_on:
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - concerns
    - prd
---

# Technical Design: TD-001 Storage Architecture and Backend Contracts

**Contract**: API-001 | **ADR**: ADR-001, ADR-004 | **Scope**: storage architecture

## Scope

This technical design defines the storage component boundaries that must satisfy
API-001. It is intentionally system-level: later story-level designs and beads
inherit these contracts when implementing the Rust workspace, Postgres-native
mode, S3/object-log mode, and conformance tests.

In scope:

- Backend capability traits for `LogStore`, `ProjectionStore`,
  `SnapshotStore`, and `ControlPlaneStore`.
- Durable command record schema and command positions.
- Item lifecycle, idempotency, lease, and `item_version` persistence rules.
- Shard identity and storage partitioning shape.
- Shard assignment, execution epochs, and fencing requirements.
- Backend profiles and conformance requirements.

Out of scope:

- Exact Postgres DDL, indexes, and query plans. TD-002 owns Postgres-native
  reference mode.
- Exact S3 object byte-framing and physical deployment sizing. TD-004 owns S3
  object layout, manifest semantics, manifest-commit fencing against the current
  control-plane epoch, group-commit thresholds, in-flight claim reservation,
  snapshot/expiry rules, cross-shard command binding, and object-log latency/cost
  validation for the `object_log_sqlite_projection` profile.
- HTTP route implementation and SDK packaging. API-001 owns client semantics.
- Broad operator repair, purge, redrive, and backend migration APIs. Targeted
  in-band recurring teardown (`PurgeItems`, per-key/`item_id`) is in native scope
  (P0); broad operator purge/redrive/retention remains a separate P1 operator
  contract. The two MUST NOT be conflated.

## Technical Approach

**Strategy**: implement pqueue as a command-log-backed queue engine with a
backend capability layer. The native API commits durable command records first,
then applies those records to a query-optimized projection used for priority
claim, lease renewal, finalization, and metrics. Postgres is the preferred
control-plane store across all modes; Postgres-native mode may also combine log
and projection in one transactional backend.

**Key Decisions**:

- **Capability traits over a flat storage adapter**: backends differ too much in
  commit latency, replay, conditional writes, and query semantics for a single
  generic store interface.
- **Command log is the ack boundary**: mutating API calls may return success
  only after their commands reach the configured durable boundary.
- **Projection is rebuildable unless the backend is transactional-authoritative**:
  SQLite or local projection state may accelerate claims, but the command log
  plus snapshots must recover acknowledged state after node loss.
- **Postgres control plane is preferred across modes**: queue definitions,
  shard assignment, backend profile, and epochs live in Postgres unless a later
  ADR justifies a different control-plane store.
- **Shard epochs fence execution**: pqueue does not run node discovery or
  cluster consensus. A control-plane assignment gives a worker authority for a
  tenant/queue/shard epoch; stale workers must be fenced before they can append
  new commands.
- **No downstream-rate stage in the claim pipeline**: the claim path evaluates
  eligibility per the single Eligibility Precedence definition (API-001) and
  selects within the queue-global progress bound (FR-9/FR-12); it contains **no**
  downstream-rate admission stage and **no** token-bucket gate. `ClaimPlan`/
  `ClaimPlanResult` carry no rate fields. Downstream API pacing is caller-driven
  via the API-001 pacing knobs (`max_items`, claim cadence, `not_before`, group
  selection). Any future deployment-level capacity control (P1) is an
  envelope-level admission concern outside the per-item eligibility/claim pipeline,
  and protects the pqueue deployment — never a caller's downstream API.
- **Conformance tests define backend eligibility**: no backend implementation is
  usable until it passes the same durability, idempotency, lease, replay, and
  progress-bound scenarios.

**Trade-offs**:

- We gain backend flexibility and a clean correctness boundary, but every
  backend must implement non-trivial conformance behavior.
- We gain low-cost S3/object-log viability for batched workloads, but accept
  higher acknowledgement latency in that profile.
- We gain a simple Postgres-native path, but must avoid letting one Postgres
  deployment become the unexamined long-term data-plane bottleneck.

## Component Changes

### New: `pqueue-core`

- **Purpose**: queue semantics, command validation, state transitions, ordering
  helpers, idempotency rules, and error types shared by all backends.
- **Interfaces**: receives API-001 operation structs; emits durable commands,
  projection mutations, per-item results, and metrics snapshots.
- **Files**: `crates/pqueue-core/src/**`

### New: `pqueue-storage`

- **Purpose**: backend capability traits, command envelopes, command positions,
  snapshot contracts, and conformance test harness.
- **Interfaces**: `LogStore`, `ProjectionStore`, `SnapshotStore`,
  `ControlPlaneStore`.
- **Files**: `crates/pqueue-storage/src/**`

### New: `pqueue-postgres`

- **Purpose**: Postgres `ControlPlaneStore`; later TD-002 expands this to
  Postgres-native `LogStore` and `ProjectionStore`.
- **Interfaces**: Postgres connection pool in; trait implementations out.
- **Files**: `crates/pqueue-postgres/src/**`

### New: `pqueue-service`

- **Purpose**: stateless HTTP service binding API-001 to core commands and
  configured backend profiles.
- **Interfaces**: HTTP/JSON in; `pqueue-core` operation results out.
- **Files**: `crates/pqueue-service/src/**`

### New: Backend Profiles

| Profile | LogStore | ProjectionStore | SnapshotStore | ControlPlaneStore |
|---------|----------|-----------------|---------------|-------------------|
| `postgres_native` | Postgres | Postgres | Optional Postgres/object storage | Postgres |
| `object_log_sqlite_projection` | S3-compatible object log | SQLite local/rebuildable | S3-compatible object storage | Postgres |
| `kafka_log_sqlite_projection` | Kafka/Redpanda partition log | SQLite local/rebuildable | Object storage or Postgres checkpoint | Postgres |
| `dynamodb_authority` | DynamoDB transaction/log table | DynamoDB query tables or local projection | DynamoDB/object storage | Postgres |

`postgres_native` (TD-002) is the reference correctness backend and is
implemented first; it delivers the single-deployment envelope.
`object_log_sqlite_projection` is the second backend committed for v1 to
substantiate horizontal-scale and cost claims; it is specified by TD-004 and
delivers the horizontal envelope's cost/scale profile. The remaining profiles
(`kafka_log_sqlite_projection`, `dynamodb_authority`) define design targets and
conformance expectations only. Every profile, including the two committed ones,
becomes usable for a queue only after it passes the shared backend conformance
suite defined in this document.

## API/Interface Design

The Rust trait shapes below are normative for design intent, not exact final
syntax. Implementations may refine lifetimes and associated types, but must keep
the same capabilities.

```rust
pub struct QueueKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
}

pub struct ShardKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub shard_id: ShardId,
}

pub struct CommandPosition {
    pub shard: ShardKey,
    pub sequence: u64,
    pub backend_epoch: u64,
}

pub enum QueueCommand {
    CreateQueue(CreateQueueCommand),
    BatchPush(BatchPushCommand),
    BatchUpdate(BatchUpdateCommand),
    BatchClaim(BatchClaimCommand),
    BatchRenewLeases(BatchRenewLeasesCommand),
    BatchFinalize(BatchFinalizeCommand),
    LeaseExpired(LeaseExpiredCommand),
    CohortExpired(CohortExpiredCommand),
    PurgeItems(PurgeItemsCommand),
}

pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub request_id: Option<RequestId>,
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
    pub shard_id: ShardId,
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: Timestamp,
}

#[async_trait]
pub trait LogStore {
    async fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError>;

    async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, LogStoreError>;

    fn durability_profile(&self) -> DurabilityProfile;
}

#[async_trait]
pub trait ProjectionStore {
    async fn apply_committed(
        &self,
        position: CommandPosition,
        commands: &[CommandEnvelope],
    ) -> Result<(), ProjectionError>;

    async fn batch_claim(
        &self,
        request: ClaimPlan,
    ) -> Result<ClaimPlanResult, ProjectionError>;

    async fn metrics(
        &self,
        queue: &QueueKey,
    ) -> Result<QueueMetricsSnapshot, ProjectionError>;
}

#[async_trait]
pub trait SnapshotStore {
    async fn write_snapshot(
        &self,
        shard: &ShardKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> Result<SnapshotRef, SnapshotError>;

    async fn latest_snapshot(
        &self,
        shard: &ShardKey,
    ) -> Result<Option<SnapshotRef>, SnapshotError>;

    async fn read_snapshot(
        &self,
        snapshot: &SnapshotRef,
    ) -> Result<ProjectionSnapshot, SnapshotError>;
}

#[async_trait]
pub trait ControlPlaneStore {
    async fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> Result<CreateQueueResult, ControlPlaneError>;

    async fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> Result<QueueDefinition, ControlPlaneError>;

    async fn shard_assignments(
        &self,
        key: &QueueKey,
    ) -> Result<Vec<ShardAssignment>, ControlPlaneError>;

    async fn backend_profile(
        &self,
        key: &QueueKey,
    ) -> Result<BackendProfileConfig, ControlPlaneError>;
}
```

`CohortExpired` is the single cohort-liveness command emitted when a cohort's
`completion_bound_ms` elapses (G6; `CohortDegraded` is not in v1). `PurgeItems`
is the targeted in-band recurring-teardown command (G5); a `rearm` outcome rides
inside `BatchFinalizeCommand` and adds no new variant.

### Operation Flow

| API-001 Operation | Storage Flow |
|-------------------|--------------|
| `CreateQueue` | Validate definition; commit queue metadata in `ControlPlaneStore`; initialize shard records. |
| `BatchPush` | Validate envelope idempotency; resolve shard; append command; apply projection; return per-item results. |
| `BatchUpdate` | Validate request idempotency and item refs; append command for valid pending items; apply projection conflicts per item. |
| `BatchClaim` | Plan claim against projection; append claim command for selected items; apply projection; return leases. |
| `BatchRenewLeases` | Validate active lease tokens; append renew command; apply projection; return per-item outcomes. |
| `BatchFinalize` | Validate active lease tokens and retry policy; append finalize command; apply projection; return per-item outcomes. |
| `GetQueueMetrics` | Read projection metrics; no log append. |

In transactional-authority backends such as Postgres-native mode, append and
projection mutation may occur in one database transaction. The implementation
must still expose equivalent command positions for replay, audit, and
conformance tests.

#### Unified ClaimPlan

`BatchClaim` plans every claim through the single `ProjectionStore.batch_claim`
entry point. `ClaimPlan` carries `claim_unit ∈ {item, whole_group, whole_cohort}`
under one shared ordering / lock / idempotency / no-fit contract:

- `item` is the default per-item claim unit.
- `whole_group` is reachable ONLY via `compatibility.group_batching` (G1); it
  leases the whole batched group atomically inside one shard's claim transaction.
- `whole_cohort` is reachable ONLY via `cohort_policy`/`whole_cohort` (G6); it is
  all-or-nothing under a shared cohort lease, locks the cohort row first, and is
  shard-local because the cohort's `group_key` is co-resident on one shard.

`same_group_key` is an item-level domain filter that constrains a claim to one
server-selected `group_key`; it leases the returned items atomically per API-001
but MAY return a partial group. It is NOT a whole-group unit and carries no
completeness/atomicity guarantee. `ClaimPlan`/`ClaimPlanResult` carry no rate
fields (see Key Decisions).

#### Re-arm and purge flows

A `rearm` outcome of `BatchFinalize` MUST release the lease, set the
caller-supplied `not_before`, record the effective
`eligible_since = max(commit_time, not_before)`, set the optional `priority`,
reset the per-cycle retry counter, and MUST NOT increment the attempt counter or
transition to a terminal state. The appended finalize command MUST record the
effective `not_before`, recorded `eligible_since`, effective `priority`, reset
counter, released lease state, and resulting `item_version` so the response is
reconstructable per Durable Ack and Response Replay. The same transaction MUST
maintain the single per-group summary projection `pqueue_group_summary` for the
item's `(tenant_id, queue_id, shard_id, group_key)` row: re-arm sets the item
ineligible until its new `not_before`, so the row's `oldest_eligible_at` MUST be
recomputed from the remaining eligible items of that scope.

`PurgeItems` is a shard-keyed mutation. Because mutations are shard-keyed and a
recurring singleton on a `group_co_residency=true` queue is single-shard by
construction, each `(client_item_key|item_id)` resolves to exactly one shard. A
`PurgeItems` request that targets items across multiple shards MUST be split into
per-shard `PurgeItemsCommand`s, each committed independently and shard-fenced.
Per-item outcomes are best-effort across shards: a partial commit (some shards
succeed, some fail/unavailable) MUST return per-item `purged`/`conflict`/
`unavailable`/`not_found` reflecting each shard's actual result, and the request
as a whole MUST NOT roll back already-committed shards. Request-id replay after a
partial commit MUST be per-shard: each shard records the `request_id` and its
committed per-item results; replaying the same `request_id` re-drives only the
not-yet-committed shards and returns recorded results for the committed ones,
yielding the same merged response on convergence. A purge MUST write a tombstone
and delete the item row in the same per-shard transaction.

### Durable Ack and Response Replay

For mutating operations, durable append is the minimum success boundary. A
successful response must be derived from committed command state, not from
pre-commit memory.

If a command batch is appended but projection application, response persistence,
or client response delivery fails, the operation is in a committed-but-unreturned
state. Retrying the same `request_id` must converge by reading the committed
command and returning the recorded or reconstructed response. Retrying the same
`request_id` with a different request fingerprint must fail with
`request-id-conflict`.

Backends may choose one of two valid response models:

- **Transactional response**: append, projection mutation, idempotency
  fingerprint, and response record commit atomically.
- **Replay response**: append commits first; projection and response records
  catch up from the log before returning, or are reconstructed on retry.

Object-log and Kafka-style profiles are expected to use replay response
semantics. Postgres-native mode should use transactional response semantics
unless TD-002 proves that a split model is needed for scale.

`rearm` and `purge` are covered by these rules: replay returns the recorded
effective values (`not_before`, `eligible_since`, `priority`, `item_version`)
and MUST NOT recompute them; purge replay is per-shard, re-driving only the
not-yet-committed shards.

### Shard Execution and Fencing

Hot data-plane operations execute against a resolved `ShardKey`. The
`ControlPlaneStore` owns shard assignment metadata and monotonically increasing
assignment epochs. pqueue service nodes consume those assignments; they do not
discover each other, elect leaders, or coordinate ownership directly.

Every `LogStore.append_batch` receives the worker's expected assignment epoch.
The backend must reject appends from stale epochs. Reassignment is a
control-plane event: once a new epoch is visible, old workers may finish
non-mutating cleanup but must not append further commands. Recovery starts by
reading the latest snapshot and log tail for the shard epoch that is now
assigned.

This keeps the hot path bounded to tenant/queue/shard routing plus backend
fencing. It does not require pqueue to maintain a cluster membership protocol,
but it does require each backend profile to document how epoch fencing is
enforced.

The full ownership lifecycle — deterministic shard-to-owner assignment (target
vs active owner) over a live worker set, storage-backed shard leases in the
`ControlPlaneStore`, monotonic epoch allocation durably fenced into the log
before a new lease is usable, rebalance (reassignment vs resharding), graceful
drain, recovery from snapshot + log tail, and cross-shard queue-global progress
aggregation — is specified in TD-003 (`td-sharding-and-shard-ownership`). TD-001
defines the fencing token (`expected_epoch` on `append_batch`), and
`append_batch` MUST reject any `expected_epoch` that is not the shard's current
recorded epoch; TD-003 defines how that epoch advances, who allocates it, and
when it becomes binding on the log.

### Multi-shard claim and cross-shard progress (v1, normative)

A queue with `shard_count > 1` distributes items across shards. Group
co-residency (ADR-004 / TD-002 placement capability) places all items of a
`group_key` on exactly one shard via `shard = hash(group_key) mod shard_count`.
The multi-shard claim contract is defined against the single shared **Eligibility
Precedence** subsection in API-001; this section adds only the cross-shard
composition rules and MUST NOT redefine "eligible". Per-shard claim is the
single-shard `ProjectionStore.batch_claim(ClaimPlan)` already defined for each
backend (TD-002 / TD-004); each per-shard claim is atomic per API-001
("`BatchClaim` MUST atomically create each returned lease").

#### Atomicity scope

| Construct | Atomicity guarantee |
|-----------|---------------------|
| `whole_group` (via `compatibility.group_batching`) / `whole_cohort` (via `cohort_policy`/`whole_cohort`) | Routes to the single owning shard (co-residency, D2) and is whole-group/whole-cohort atomic: either the whole batched group/cohort is leased or none of it is, evaluated inside one shard's claim transaction. |
| `same_group_key` / explicit `group_key` | On a `group_co_residency=true` queue, routes to the single owning shard and leases the returned items atomically per API-001 (each returned lease created atomically), but MAY return a partial group subject to `max_items` and eligibility. On a non-co-resident queue (`group_co_residency=false`), `same_group_key`/`group_key` are item-level domain filters applied across the fan-out (cross-shard merge, weaker per-group ordering); they never route to a single shard. In both cases `same_group_key`/`group_key` are item-level domain filters, NOT a whole-group unit, and do not guarantee the whole group is claimed. |
| Non-group claim | Fans out across shards; per-shard portions are each atomic; the cross-shard envelope MAY be partial (see Cross-shard claim atomicity below). |

| Rule | Requirement |
|------|-------------|
| Whole-group/cohort routing | `whole_group` and `whole_cohort` claims (which are valid only on `group_co_residency=true` queues; rejected otherwise) MUST be routed to the single shard owning that group via `hash(group_key) mod shard_count` and MUST execute as a single-shard atomic claim per the rows above. They MUST NOT fan out. |
| `group_key`/`same_group_key` routing | On a `group_co_residency=true` queue, a claim carrying `group_key` (or a server-selected group under `same_group_key`) MUST be routed to the single owning shard via `hash(group_key) mod shard_count`. On a `group_co_residency=false` queue, the group's items are not co-resident: `group_key`/`same_group_key` act as item-level filters over the cross-shard fan-out (next row) and MUST NOT be routed to a single shard. |
| Non-group fan-out | A claim with no group constraint fans out across shards and merges results, honoring `max_items` as a global upper bound across the fan-out. |
| Shard selection for fan-out | A strict (non-relaxed) queue MUST inspect/merge all relevant shards to prove global top-N ordering (FR-7/FR-14). A bounded-relaxed queue MAY sample a subset of shards ONLY within the queue's declared relaxation bound; sampling MUST NOT be used for strict queues. "Relevant shards" are the queue's shards that hold eligible items (a shard known-empty from its summary projection MAY be skipped without violating strictness). |
| Ordering | Merged cross-shard results MUST be returned in the queue's deterministic result order. The merge MUST be a deterministic k-way merge on `(priority_sort, tie_breaker)`. Bounded-relaxed queues MAY relax cross-shard ordering only within the queue's relaxation bound. |
| Single active lease | FR-25 holds per item; because each item lives on exactly one shard and leases are shard-local, no cross-shard lock is needed for lease uniqueness. |

#### Cross-shard claim atomicity, partial failure, and replay

A fan-out `BatchClaim` is a composition of independent per-shard atomic claims.
There is NO cross-shard distributed transaction. The envelope semantics are:

| Rule | Requirement |
|------|-------------|
| Claim-intent record | Before fanning out, the coordinator MUST durably record a claim-intent keyed by `request_id`/request-fingerprint at queue scope, capturing the fan-out plan: the participating shard set, the per-shard `max_items` allocation, and the gate generation observed. This is the queue-global anchor that makes replay converge. |
| Per-shard commit | Each participating shard performs its own atomic `BatchClaim` and records the leases it created under the same `request_id` in its shard-local idempotency store (per-shard transactional/replay-response commit, see Durable Ack and Response Replay). A shard's leases are committed where that shard's log/projection lives — there is no shared commit point. |
| Partial failure | If shard A commits leases and shard B fails (unavailable, fenced, timed out), the envelope MUST return the committed partial set from the shards that succeeded, with the failed shards surfaced as a retryable condition. Leases already created on shard A remain valid (FR-25). The envelope MUST NOT discard or roll back shard A's leases. `max_items` is an upper bound, so a partial set is a valid claim outcome. |
| Replay convergence | A replayed `request_id` MUST re-read the claim-intent record and, for each participating shard, return that shard's already-committed leases for this `request_id` (NOT a new claim). Shards that failed on the first attempt are re-attempted under the same plan/allocation so the result converges to a single stable lease set across retries while those leases are active. Re-attempt MUST use the recorded plan, not a fresh fan-out. |
| request-expired evaluation | `request-expired` is evaluated at envelope scope over the union of all leases recorded under the claim-intent: while ANY lease created under this `request_id` (on any shard) is still active, replay returns the same union set; once ALL such leases across ALL shards are finalized/released/expired, replay MUST fail with `request-expired`. A new claim requires a new `request_id`. |
| commit-timeout | If the coordinator cannot durably record the claim-intent, or no shard can durably commit before the configured deadline, the envelope MUST return `commit-timeout`; the caller retries with the same `request_id`, and the claim-intent makes the retry converge rather than double-claim. |
| Partial-set disclosure | The claim response envelope MAY indicate that the returned set is partial so callers can choose to retry the same `request_id` to pick up the rest; this is a non-normative response hint, not a contract change to API-001. |

#### Cross-shard queue-global progress

| Rule | Requirement |
|------|-------------|
| One bound | The queue has ONE queue-global progress bound (D1; FR-9/FR-12). There is NO per-group/per-shard progress invariant. |
| Aggregation (gate-aware) | `oldest_eligible_age_ms` for the queue MUST be the maximum per-shard effective oldest-eligible age across all shards, where "effective" means after applying the gate-aware read model — i.e., the oldest-eligible value computed against the current gate generation at read time, NOT the raw stored summary row. Eligible counts MAY be lagged/approximate; the effective oldest age MUST be authoritative. |
| Enforcement (state vs owner) | TD-003 supplies per-shard oldest-eligible state and a global owner-liveness guard. TD-001's claim planner owns the decision of how claim capacity is directed across shards so that a near-violation item on any shard is claimed in time: for a multi-shard queue the queue-global bound is enforced by the conjunction of (i) each shard's planner claiming any item near `progress_bound_ms` before the bound, and (ii) every shard having a live owner. The planner MUST be able to prioritize a shard whose oldest-eligible age is closest to the bound, using the cross-shard aggregation (TD-003 §Cross-Shard Progress). |
| Guarantee | Claim planning MUST guarantee that the shard holding the queue-global oldest-eligible item is claimed from before that item violates `progress_bound_ms`, even when another shard is hot. The detailed guard and the stalled/draining-shard rules are specified in TD-003 §Cross-Shard Progress. |
| Worker routing / fairness | Per-group fairness is achieved by routing workers via `DiscoverActiveScopes` (g4), NOT by an engine invariant (D1). `DiscoverActiveScopes` MUST report active scopes across all shards. |

The detailed ownership, fencing, rebalance, drain, recovery, stalled-shard
handling, and the algorithm that guarantees the cross-shard progress bound are
specified in TD-003 (`td-sharding-and-shard-ownership`). This section is the
storage-contract surface; TD-003 is the ownership/coordination mechanism. The
multi-shard claim path (independent single-writer claim/append per shard with a
fixed `shard_count > 1`) is a committed v1 mechanism, not a deferred concern;
only the magnitude claim (throughput/queue scale beyond a single database)
remains evidence-gated against the per-queue throughput floor (TP-002 E0:
>=10M items/hr per queue, preserved for every queue at any scale; E1-E3).

## Data Model Changes

TD-001 defines logical records. Backend-specific DDL belongs in implementation
TDs.

### Logical Control Plane Records

```text
QueueDefinition {
  tenant_id,
  queue_id,
  priority_model,
  ordering_mode,
  progress_bound_ms,
  eligibility_policy,
  request_id_retention_ms,
  client_item_key_retention_ms,
  max_lease_duration_ms,
  retry_policy,
  max_push_batch_size,
  max_claim_batch_size,
  backend_profile,
  shard_count,
  recurrence,
  created_at,
  updated_at
}

ShardAssignment {
  tenant_id,
  queue_id,
  shard_id,
  backend_profile,
  assignment_epoch,
  placement,
  state
}
```

`shard_count` is set from the client `CreateQueue.shard_count` field (API-001),
bounded by deployment policy, defaulting to `1`, immutable after create. It is
the authoritative N for `hash(...) mod shard_count` placement (ADR-004) and for
the TD-003 multi-shard mechanism.

`recurrence` carries the per-queue recurrence mode and `until` bound (G5). It is
a per-queue immutable flag; there is no `backoff` sub-object in v1 and no mixed
one-shot/recurring queues.

`QueueDefinition` carries the group co-residency placement capability (e.g.
`group_co_residency`, defined per D2 in ADR-004 and TD-002). When set, item
placement uses `shard = hash(group_key) mod shard_count`. This is a placement
capability, NOT a `claim_scope`/progress field (D1/D2): it makes group/cohort
claims shard-local and does not carry progress meaning.

The `ShardAssignment` record is extended by TD-003 with shard-owner lease fields
(`active_owner_id`, `target_owner_id`, `lease_expires_at`); `assignment_epoch` is
the same token threaded through `CommandPosition.backend_epoch`, and TD-003
specifies how it advances and when it becomes binding on the log.

### Logical Command Records

Every command record must include:

- `command_id`
- `request_id` when the API operation is mutating
- `tenant_id`, `queue_id`, `shard_id`
- command type and payload
- affected `item_id`s where known
- command timestamp
- checksum
- backend position after append

### Logical Projection Records

Projection stores must represent:

- item identity: `item_id`, `client_item_key`, `tenant_id`, `queue_id`,
  `shard_id`
- item state: lifecycle, priority, `not_before`, retry metadata, metadata,
  payload reference or payload value, a **recurring flag** and
  **recurrence-until** (G5; NO engine backoff/inactivity state)
- lease state: active `lease_token`, `lease_expires_at`, `worker_id`
- version state: `item_version`, last command position
- idempotency state: request fingerprints and responses for
  `request_id_retention_ms`; item-key convergence records for
  `client_item_key_retention_ms`
- tombstone state: a tombstone record keyed by
  `(tenant_id, queue_id, client_item_key)` for purged keys (G5)
- metrics state: lifecycle counts, retry backlog, active leases,
  `oldest_eligible_age_ms`, `progress_bound_risk_count`

There is exactly one per-group summary projection, `pqueue_group_summary`, keyed
`(tenant_id, queue_id, shard_id, group_key)` (shard-scoped): exactly one row per
`(shard_id, group_key)`. On a `group_co_residency=true` queue a group lives on
exactly one shard, so there is at most one shard row per group; on a
`group_co_residency=false` queue the same `group_key` MAY appear in several
shards' rows, which the cross-shard aggregation merges by `(queue_id, group_key)`
before applying any limit. The grain stays coherent for multi-shard cross-shard
aggregation and for the local-SQLite backend (TD-004). `oldest_eligible_at` per row is authoritative and
exact; eligible counts MAY lag/be approximate. It is the sole source for
`DiscoverActiveScopes` (G4) and for cross-shard queue-global progress (TD-003).
A queue-level gate flip MUST NOT synchronously rewrite every group's summary row;
`oldest_eligible_age_ms` stays authoritative (computed against the current gate
generation at read) while counts MAY lag.

Cohort queues add a `pqueue_cohorts` projection for cohort identity (logical key
`(tenant_id, queue_id, group_key)`, with `shard_id` derived; size, member count,
state, `cohort_created_at`, first-eligible time, expire command position, cohort
lease token hash, `retention_until`) (G6). Cohort eligible-age and counts are NOT
duplicated here; they come from the single `pqueue_group_summary`.

## Integration Points

| From | To | Method | Data |
|------|----|--------|------|
| `pqueue-service` | `pqueue-core` | Direct Rust call | API-001 operation structs |
| `pqueue-core` | `ControlPlaneStore` | Trait | queue definitions, shard assignments, backend profile |
| `pqueue-core` | `LogStore` | Trait | durable command envelopes |
| `pqueue-core` | `ProjectionStore` | Trait | committed commands, claim plans, metrics reads |
| `pqueue-core` | `SnapshotStore` | Trait | projection snapshots and recovery checkpoints |
| Backend conformance tests | Backend crates | Trait test harness | deterministic scenarios and crash/replay fixtures |

### External Dependencies

- **Postgres**: preferred `ControlPlaneStore`; fallback is no service-mode queue
  creation or shard routing until Postgres is restored.
- **Log backend**: authoritative durable commit boundary; fallback is to reject
  mutating operations with retryable commit errors.
- **Object storage**: required for object-log command segments and snapshots;
  fallback is to stop acknowledging object-log commands until durable commit
  resumes.

## Security

- **Authentication**: service mode resolves a principal before any HTTP route.
  Provider choice remains outside TD-001.
- **Authorization**: every operation authorizes the principal against
  `tenant_id` and `queue_id` before reading or mutating control-plane,
  log, projection, or snapshot state.
- **Tenant isolation**: all storage keys include `tenant_id`; backend schemas
  must make cross-tenant reads and writes explicit and testable.
- **Data protection**: payload and metadata are caller data. Backends must
  support encryption in transit and at rest through their storage provider.
- **Threats**:
  - tenant spoofing through route/path IDs: mitigate with principal-to-tenant
    authorization before storage access.
  - stale lease finalization: mitigate by validating `lease_token` and current
    active lease state in projection/transaction.
  - replay or duplicate mutation: mitigate with request fingerprint storage and
    `request-id-conflict`.
  - cross-shard corruption: mitigate by embedding shard keys and checksums in
    every command envelope.

## Performance

- **Expected Load**: the per-queue throughput floor (TP-002 E0: >=10M items/hr
  per queue, preserved for every queue at any scale); at least 10M items in a hot
  queue; at least 1000 concurrently active queues per node (queue density,
  TP-002 E2); large batches for cost-optimized object-log profiles.
- **Queue density (>=1000 active queues per node)**: backend implementations of
  the capability traits MUST NOT allocate unbounded per-queue or per-`(queue,
  shard)` resources. Background work — lease-expiry sweeps, cross-shard progress
  aggregation, `pqueue_group_summary` recompute, recurring rearm, and
  idempotency/retention GC — MUST be multiplexed onto bounded shared per-node
  pools (a batched sweeper that scans many shards per pass, a shared connection
  pool, a bounded/LRU set of open per-shard projection handles), never one task,
  loop, or connection per queue or per shard. A node MUST sustain >=1000
  concurrently active queues with each meeting its progress bound and any one
  able to reach the per-queue floor; aggregate single-node throughput is bounded
  by the node, and multi-node deployment provides aggregate headroom.
- **Response Target**: API-001 core batch operations target sub-second p95/p99
  under representative workloads, except object-log profiles where configured
  batch windows may intentionally trade acknowledgement latency for cost.
- **Delivered envelopes**: these figures define two delivered v1 envelopes. The
  single-deployment envelope is delivered by `postgres_native` and validated
  against the per-queue throughput floor (TP-002 E0: >=10M items/hr per queue;
  E1). The horizontal envelope spreads
  write/claim load across independent shards and is delivered by multi-shard
  claim with cross-shard progress aggregation, sharding & shard ownership
  (TD-003), and the `object_log_sqlite_projection` backend (TD-004); it is
  validated by TP-002 E2/E3. `shard_count`/`ShardKey` are the data-plane
  partitioning primitives that deliver this, not merely forward-compatibility
  metadata.
- **Optimizations**:
  - partition by `tenant_id / queue_id / shard_id`
  - keep claim indexes in `ProjectionStore`
  - use batch append/apply paths for every mutating operation
  - bound request-id and item-key retention windows
  - snapshot projections to bound replay for log-backed local projections
  - expose progress-bound risk via eligible age metrics

## Testing

- **Unit**: command validation, request fingerprinting, item-key
  convergence, `item_version` transitions, retry exhaustion, metadata blockers.
- **Integration**: `LogStore` append/read replay, projection rebuild from
  log, snapshot restore, Postgres control-plane create/read assignments.
- **API**: API-001 acceptance sketches, including request-id conflict,
  optimistic update conflict, leased update conflict, claim retry idempotency,
  tenant spoofing rejection, and SQS adapter limitation.
- **Security**: tenant isolation negative tests for control plane, log,
  projection, and snapshot backends.
- **Concurrency**: duplicate claim prevention, stale lease finalization,
  lease expiry redelivery, group-aware claim progress under skew.
- **Performance**: 10M-item projection benchmark, batch push/update/claim/
  finalize throughput, telemetry-on latency, object-log group commit latency.
- **Conformance**: shared backend test suite that every `LogStore`,
  `ProjectionStore`, `SnapshotStore`, and `ControlPlaneStore` implementation
  must pass before use.

### Backend Conformance Scenarios

| Scenario | Required Evidence |
|----------|-------------------|
| Durable append before ack | Kill process after ack; replay shows command. |
| Commit timeout retry | Retrying same `request_id` converges or returns recorded response. |
| Request-id conflict | Same `request_id` with different body fails. |
| Duplicate push | Same `client_item_key` returns existing item without mutation. |
| Mutable schedule | Pending item priority and `not_before` update changes claim order. |
| Leased update conflict | `BatchUpdate` against active lease returns per-item `conflict`. |
| Single active lease | Concurrent claims never return same item with active leases. |
| Stale lease finalization | Old token fails after renew/expiry/reclaim. |
| Claim replay | Same claim `request_id` returns same active lease set. |
| Snapshot recovery | Restore snapshot plus log tail reproduces projection state. |
| Progress-bound risk | Eligible age metrics identify near-violation items. |
| Tenant isolation | Tenant A cannot read or mutate tenant B state. |
| Stale-epoch reject | Append under a superseded epoch fails without mutating state (TD-003). |
| Stale writer after epoch advance, before new data segment | An epoch-E writer is rejected immediately once E+1 is fenced, before any E+1 data segment exists (TD-003). |
| Reassignment recovery | New owner with a greater epoch recovers shard state from snapshot + log tail (TD-003). |
| Multi-shard group routing | On a `group_co_residency=true` queue, items of one `group_key` land on one shard; a `whole_group` claim (via `compatibility.group_batching`) and a `whole_cohort` claim (via `cohort_policy`/`compatibility.whole_cohort`) are whole-group/whole-cohort atomic and shard-local; `same_group_key` (item-filter only) is shard-local and per-item atomic but MAY return a partial group. On a `group_co_residency=false` queue, `same_group_key`/`group_key` are item filters over the cross-shard fan-out, and `whole_group`/`whole_cohort` are rejected at `CreateQueue`. |
| Multi-shard fan-out + order | Non-group claim across shards returns a deterministic k-way-merged ordered batch within the global `max_items`; a strict queue inspects all relevant shards; a bounded-relaxed queue may sample only within its relaxation bound. |
| Cross-shard progress bound | With one hot shard and one cold shard holding the queue-global oldest-eligible item, that item is claimed before `progress_bound_ms` (queue-global, D1). |
| Multi-shard claim replay | Replayed fan-out `request_id` converges to the same lease set across shards while leases are active; a partial-failure first attempt re-attempts under the recorded plan; `request-expired` once all leases across all shards are inactive. |
| Group-commit ack boundary | For batched-log profiles, no command is acked before its durable segment/manifest commit; kill after segment write, before manifest commit, shows command not acked and re-drivable by `request_id`. |
| Current-epoch manifest fencing | A writer whose shard was reassigned (control-plane epoch advanced) before the new owner wrote any data manifest entry MUST fail its commit; manifest-recorded-epoch-only validation is insufficient and MUST NOT pass. New epoch holder reproduces acknowledged state. |
| In-flight claim reservation safety | Concurrent claims cannot both reserve the same candidate while a segment is pending; CAS failure / timeout / fence / writer crash rolls back reservations with no durable lease; retry converges. |
| Snapshot + log-tail recovery | Restore latest snapshot, replay segments after the snapshot position, validate checksums, reproduce projection state. |
| Multi-shard command convergence | A queue-scoped command (`SetGates`) applies to all occupied shards before ack, all-or-nothing; a partial-shard failure does not ack or expose partial visibility; retry by `request_id` converges with no double-apply. |
| Safe log-segment expiry | A log segment is deletable only after a covering committed snapshot plus recovery window; no expired segment is required for an in-window recovery. |
| Reject one-object-per-command | A production configuration that seals one command per durable object is rejected; only an explicit dev/test flag permits it. |
| Atomic complete-cohort claim | A complete cohort is leased all-or-nothing under one shared lease, never split or double-leased; a member is never individually claimable; `CohortExpired` precedes any claimability change; survives writer restart (G6). |
| Cohort duplicate push / reuse | Duplicate push of a cohort member is a no-op; `group_key` reuse after `retention_until` yields a new `cohort_id`; whole-cohort lease replay returns the same members/`cohort_id`/lease (G6). |
| Perpetual re-arm never terminal | Re-arm more times than `max_attempts`; never terminal; a fresh cycle gets a full per-cycle retry budget (G5). |
| Rearm replay determinism | Duplicate `request_id` returns recorded `not_before`/`eligible_since`/priority/version; no recompute; `eligible_since = max(commit_time, not_before)` (G5). |
| Recurring co-residency / progress parity | A recurring singleton's shard is `hash(group_key) mod shard_count`, unchanged across re-arms; a re-armed eligible item is claimed before the queue-global `progress_bound_ms` with no per-group bound (D1/D2, G5). |
| Recurring eligibility parity | An idle (future `not_before`) recurring item and a gate-blocked re-armed item are NOT returned by `BatchClaim` and do NOT contribute to any `DiscoverActiveScopes` descriptor; an eligible ungated re-armed item is returned and contributes (G2/G4, G5). |
| Purge replay + multi-shard split | `PurgeItems force=true` removes a leased recurring item and invalidates the lease; duplicate purge `request_id` is idempotent; a purge spanning shards where one shard is unavailable returns per-item results reflecting each shard and replay re-drives only the uncommitted shard and converges; a finalize for a purged item returns `not_found`; a re-push after the tombstone window creates a fresh item (G5). |

## Migration & Rollback

- **Backward Compatibility**: storage contracts start at v1 and are internal;
  API-001 remains the external compatibility boundary.
- **Data Migration**: first implementation starts empty. Seventh Sense migration
  requires a later migration design mapping existing queue tables into
  `BatchPush`/`BatchUpdate` commands.
- **Feature Toggle**: backend profile is queue configuration. New profiles can
  be enabled per queue after conformance tests pass.
- **Rollback**: disable a backend profile for new queues; keep existing queues
  on their last known-good backend until a migration/repair design exists.

## Implementation Sequence

1. Define `pqueue-core` domain types and API-001 operation structs.
   Files: `crates/pqueue-core/src/**`.
   Tests: unit tests for validation, lifecycle, idempotency, and version rules.
2. Define `pqueue-storage` traits and conformance harness.
   Files: `crates/pqueue-storage/src/**`.
   Tests: backend-agnostic conformance fixtures.
3. Implement Postgres `ControlPlaneStore`.
   Files: `crates/pqueue-postgres/src/control_plane/**`.
   Tests: tenant-scoped queue create/read, shard assignment, backend profile.
4. Implement Postgres-native `LogStore` and `ProjectionStore` per TD-002
   (single-deployment envelope).
5. Implement sharding & shard ownership and cross-shard claim/progress per
   TD-003.
6. Implement the `object_log_sqlite_projection` backend per TD-004: S3 `LogStore`
   (group-commit segments + manifest with current-epoch fencing), SQLite
   `ProjectionStore` (with in-flight claim reservations), S3 `SnapshotStore`,
   bounded replay, and the cross-shard command binding to TD-003 (horizontal
   envelope cost/scale).
   Files: `crates/pqueue-objectlog/src/**`, `crates/pqueue-sqlite/src/**`.
   Tests: shared conformance suite (including the object-log rows) plus the
   TD-004 scale/cost evidence record (TP-002 E3 vs E0).
7. Implement `pqueue-service` HTTP binding after core structs and first backend
   compile.

**Prerequisites**: API-001 complete; ADR-001, ADR-002, ADR-003, and ADR-004
accepted; TD-002, TD-003, and TD-004 accepted; TP-002 available for test
traceability; Rust workspace setup bead filed from ADR-003.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Trait abstraction hides backend-specific correctness requirements | M | H | Capability-specific conformance tests and durability profile metadata. |
| Postgres-native mode becomes the de facto only architecture | M | M | Keep backend profile boundaries and command positions in the first implementation. |
| Object-log profile cannot meet acceptable ack latency | M | M | Spike group commit latency before implementation; document latency/cost profile. |
| Local projections diverge from durable log | M | H | Apply only committed commands; test replay and snapshot recovery. |
| Idempotency storage grows without bound | M | M | Enforce separate request and item-key retention windows. |
| Claim compatibility causes hidden starvation | M | H | Test server-selected group fairness and document caller-filtered domain limits. |
| Incomplete/gated cohort starves queue-global progress bound | M | H | Hard `completion_bound_ms <= progress_bound_ms` check at `CreateQueue`; linearized `CohortExpired`. |

## Review Checklist

- [x] Governing API-001 operations map to storage flows.
- [x] ADR-001 CQRS/log-projection decision is preserved.
- [x] Postgres control plane preference is preserved.
- [x] Backend capability interfaces are explicit.
- [x] Durable ack boundary is explicit.
- [x] Idempotency, leases, item versions, and replay are explicit.
- [x] Security covers tenant authorization and data isolation.
- [x] Performance targets reference PRD scale requirements.
- [x] Tests include conformance, API edge cases, security, and performance.
- [x] Multi-shard claim + cross-shard queue-global progress are normative v1
      semantics; ownership/fencing detail delegated to TD-003.
- [x] Unified `ClaimPlan {item, whole_group, whole_cohort}` with claim-unit
      reachability rules; `same_group_key` is an item-level filter only.
- [x] Single shard-scoped `pqueue_group_summary` projection; recurrence, cohort,
      and tombstone records added; no rate stage in the claim pipeline.
