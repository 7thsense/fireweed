---
ddx:
  id: td-storage-architecture-backend-contracts
  depends_on:
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - concerns
    - prd
  review:
    self_hash: a0053226d680acddfc3b606ec106c47ffb09167374940dc8282607e46b8df96e
    deps:
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 9a9570ebe2718bf637c73564018e3702bc4473bcbf5a6499b52b7e1937bd0b83
      adr-granularity-mapping-and-claim-domain: f84d9bd6d3a8ab886c14f84afa45d189923e0cb7db32f57b700a9a0d8b1655b4
      adr-queue-as-shard-unit-and-projection-families: 77d1e2feb6a27e0a093564e3f07247cd8cc2c6fba6c3d20b5eeade568ba25964
      api-native-client-interface: a97e014a176aa9e37a93fbab151c31ffb47aa8428c62e802c98fa3be0413426b
      concerns: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
      prd: a910dd5fb95102767b4ddf81115569d39d85c7e082a40c62ce424dea73ca8533
    reviewed_at: "2026-06-25T04:21:18Z"
---

# Technical Design: TD-001 Storage Architecture and Backend Contracts

**Contract**: API-001 | **ADR**: ADR-001, ADR-004, ADR-008 | **Scope**: storage architecture

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
- Queue identity and internal storage-partitioning shape.
- Queue-to-owner assignment, execution epochs, and fencing requirements
  (per-queue; the mechanism is TD-003).
- The two projection families and conformance as the behavior contract.
- Backend profiles and conformance requirements.

Out of scope:

- Exact Postgres DDL, indexes, and query plans. TD-002 owns Postgres-native
  reference mode.
- Exact S3 object byte-framing and physical deployment sizing. TD-004 owns S3
  object layout, manifest semantics, manifest-commit fencing against the current
  control-plane epoch, group-commit thresholds, in-flight claim reservation,
  snapshot/expiry rules, and object-log latency/cost validation for the
  `object_log_sqlite_projection` profile.
- HTTP route implementation and SDK packaging. API-001 owns client semantics.
- Queue-to-owner assignment, leases, epoch allocation, drain, reassignment, and
  recovery *mechanism*. TD-003 owns ownership and fencing; TD-001 defines only
  the fencing token (`expected_epoch` on `append_batch`).
- The deferred no-Postgres / object-store `ControlPlaneStore` implementation
  (ADR-008; spike-gated).
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
  only after their commands reach the configured durable boundary and accepted
  effects are externally visible through the serving projection or equivalent
  committed response state.
- **External transaction contract is invariant**: backend profiles may differ in
  latency, cost, capacity, and recovery time, but every supported combination
  must preserve API-001 success, structured rejection, unknown-outcome
  `request_id` replay, read-after-success visibility, and single-active-lease
  guarantees.
- **Projection is rebuildable unless the backend is transactional-authoritative**:
  SQLite or local projection state may accelerate claims, but the command log
  plus snapshots must recover acknowledged state after node loss.
- **The projection is a family, held by conformance (ADR-008)**: pqueue supports
  two projection families — an **in-memory log-replay** projection
  (embedded/object-log) and a **relational/DB-resident** projection (`pqueue_items`
  + SQL `FOR UPDATE SKIP LOCKED` claim, sqlite/postgres). They share **behavior,
  not code**; the conformance suite is the contract that holds them identical (see
  "Projection Families and Conformance as Contract").
- **The queue is the unit of sharding (ADR-008)**: a whole queue is owned by
  exactly one node at a time; there is no intra-queue sharding, no cross-owner
  claim fan-out, and no cross-owner progress aggregation. Horizontal scale is
  cross-queue. A relational backend MAY internally hash-partition its item table
  for vacuum/index-size isolation (TD-002), but that partition is a client-invisible
  storage detail, never an ownership or routing unit.
- **Control plane is pluggable; Postgres is the default**: queue definitions,
  queue-to-owner assignment, backend profile, and epochs live in the
  `ControlPlaneStore`. Postgres is the preferred and only v1-settled
  implementation; a backend-specific control plane (e.g. an object-store
  implementation) may be supported later but MUST justify against ADR-001's bar
  (ADR-008; the object-store impl is deferred behind an S3-CAS spike).
- **Queue epochs fence execution**: pqueue does not run node discovery or
  cluster consensus. A control-plane assignment gives a worker authority for a
  `(tenant_id, queue_id)` epoch; stale workers must be fenced before they can
  append new commands (TD-003 Single Authoritative Fencing Rule).
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
- **Commit-latency bound is a profile knob, not a correctness knob**:
  durable-log profiles expose a group-commit latency bound that trades mutation
  latency against log/object-store request cost and batch density. The knob must
  be covered by scale evidence and must never weaken transaction integrity.

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
| `postgres_native` | Postgres | Postgres (relational family) | Optional Postgres/object storage | Postgres |
| `object_log_inmemory_projection` | S3-compatible object log | In-memory local/rebuildable (log-replay family) | S3-compatible object storage | Postgres |
| `object_log_sqlite_projection` | S3-compatible object log | SQLite local/rebuildable (relational family) | S3-compatible object storage | Postgres |
| `kafka_log_sqlite_projection` | Kafka/Redpanda partition log | SQLite local/rebuildable | Object storage or Postgres checkpoint | Postgres |
| `dynamodb_authority` | DynamoDB transaction/log table | DynamoDB query tables or local projection | DynamoDB/object storage | Postgres |

`postgres_native` (TD-002) is the reference correctness backend and is
implemented first; it delivers the single-deployment envelope.
The object-log profiles are the committed high-scale path for v1. The in-memory
projection variant is the low-latency serving reference for object-log replay;
the SQLite projection variant is the durable local-index profile for larger hot
sets and process restarts. TD-004 owns their shared object-log semantics and
projection-specific recovery requirements. The remaining profiles
(`kafka_log_sqlite_projection`, `dynamodb_authority`) define design targets and
conformance expectations only. Every profile, including committed ones, becomes
usable for a queue only after it passes the shared backend conformance suite
defined in this document.

### Projection Families and Conformance as Contract

Per ADR-008 the projection is a **family**, not a single shared implementation.
Two families exist, and the **conformance suite is the contract** that holds them
behaviorally identical:

- **In-memory log-replay** projection: the projection is rebuilt by replaying the
  durable log (embedded and `object_log_sqlite_projection` use this for recovery).
- **Relational / DB-resident** projection: `pqueue_items` is authoritative
  in-place and claim is an SQL `FOR UPDATE SKIP LOCKED` statement
  (`postgres_native`, and the SQLite local projection).

The conformance suite partitions into capability classes:

| Suite | What it asserts | Who runs it |
|-------|-----------------|-------------|
| **core** | Observable queue behavior independent of durability substrate: ordering, eligibility (API-001 Eligibility Precedence), claim atomicity, single-active-lease, idempotency (`request_id` + `client_item_key`), lease renewal/expiry/reclaim, epoch fencing, and the per-queue progress bound. | **Every** projection family / backend. |
| **transaction contract** | Success is durable and visible; structured envelope rejection has no committed effect; per-item rejection has no effect for that item; unknown outcomes resolve exactly once by `request_id`; crashes at every append/apply/response boundary preserve the same visible history. | **Every** supported implementation combination. |
| **log** | Replay-from-log, snapshot + log-tail recovery, segment/manifest group-commit fencing, orphan-segment handling, and commit-latency-bound behavior. | Log-bearing backends only (`object_log_inmemory_projection`, `object_log_sqlite_projection`, kafka). |
| **relational durability** | Reconnect-after-crash durability — the relational substitute for replay-from-log: after process loss the DB-resident projection still holds acknowledged state. | Relational-family backends that are transactional-authoritative (`postgres_native`). |

A backend is admissible for a queue only after it passes **core**,
**transaction contract**, and whichever of **log** / **relational durability**
matches its durability class. Durability class follows the durability
**substrate, not the projection family**: a
relational-family projection that is rebuilt from a log (the SQLite local
projection under `object_log_sqlite_projection`) discharges its durability
obligation via **log** (replay/snapshot+tail), not **relational durability**;
only a transactional-authoritative relational projection (`postgres_native`) runs
the reconnect-after-crash class. The fencing and ownership scenarios (stale-epoch
reject, reassignment recovery) are part of **core** and bind every backend; their
*mechanism* is TD-003.

## API/Interface Design

The Rust trait shapes below are normative for design intent, not exact final
syntax. Implementations may refine lifetimes and associated types, but must keep
the same capabilities. The owned/routed unit is the whole queue — `QueueKey`;
there is no `ShardKey` in the contract surface (ADR-008).

```rust
pub struct QueueKey {
    pub tenant_id: TenantId,
    pub queue_id: QueueId,
}

pub struct CommandPosition {
    pub queue: QueueKey,
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
    pub item_ids: Vec<ItemId>,
    pub command: QueueCommand,
    pub checksum: CommandChecksum,
    pub created_at: Timestamp,
}

#[async_trait]
pub trait LogStore {
    async fn append_batch(
        &self,
        queue: &QueueKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError>;

    async fn read_from(
        &self,
        queue: &QueueKey,
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
        queue: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> Result<SnapshotRef, SnapshotError>;

    async fn latest_snapshot(
        &self,
        queue: &QueueKey,
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

    async fn queue_assignment(
        &self,
        key: &QueueKey,
    ) -> Result<QueueAssignment, ControlPlaneError>;

    async fn backend_profile(
        &self,
        key: &QueueKey,
    ) -> Result<BackendProfileConfig, ControlPlaneError>;
}
```

`ControlPlaneStore` is a **pluggable capability** (ADR-008): Postgres is the
default and only v1-settled implementation. TD-003 adds the queue-ownership
operations (`register_owner`, `resolve_queue_owner`, `acquire_queue_lease`,
`renew_queue_lease`, `begin_drain`, `release_queue_lease`) to this trait and owns
their semantics; TD-001 specifies only the base definition/assignment/profile
reads above.

`CohortExpired` is the single cohort-liveness command emitted when a cohort's
`completion_bound_ms` elapses (G6; `CohortDegraded` is not in v1). `PurgeItems`
is the targeted in-band recurring-teardown command (G5); a `rearm` outcome rides
inside `BatchFinalizeCommand` and adds no new variant.

### Operation Flow

| API-001 Operation | Storage Flow |
|-------------------|--------------|
| `CreateQueue` | Validate definition; commit queue metadata in `ControlPlaneStore`; initialize the queue-owner record (TD-003). |
| `BatchPush` | Validate envelope idempotency; append command on the queue's log; apply projection; return per-item results. |
| `BatchUpdate` | Validate request idempotency and item refs; append command for valid pending items; apply projection conflicts per item. |
| `BatchClaim` | Plan claim against the owner's projection; append claim command for selected items; apply projection; return leases. |
| `BatchRenewLeases` | Validate active lease tokens; append renew command; apply projection; return per-item outcomes. |
| `BatchFinalize` | Validate active lease tokens and retry policy; append finalize command; apply projection; return per-item outcomes. |
| `GetQueueMetrics` | Read projection metrics; no log append. |

In transactional-authority backends such as Postgres-native mode, append and
projection mutation may occur in one database transaction. The implementation
must still expose equivalent command positions for replay, audit, and
conformance tests.

#### Unified ClaimPlan

`BatchClaim` plans every claim through the single `ProjectionStore.batch_claim`
entry point on the queue's one owner. `ClaimPlan` carries
`claim_unit ∈ {item, whole_group, whole_cohort}` under one shared ordering / lock
/ idempotency / no-fit contract:

- `item` is the default per-item claim unit.
- `whole_group` is reachable ONLY via `compatibility.group_batching` (G1); it
  leases the whole batched group atomically inside the owner's claim transaction.
- `whole_cohort` is reachable ONLY via `cohort_policy`/`whole_cohort` (G6); it is
  all-or-nothing under a shared cohort lease, locks the cohort row first, and is
  owner-local because every member of the cohort's `group_key` is co-resident on
  the queue's single owner by construction (ADR-008).

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
item's `(tenant_id, queue_id, group_key)` row: re-arm sets the item ineligible
until its new `not_before`, so the row's `oldest_eligible_at` MUST be recomputed
from the remaining eligible items of that scope.

`PurgeItems` is a queue-local mutation: a queue has one owner, so every
`(client_item_key|item_id)` it targets resolves to that single owner and the
purge commits in one queue-local transaction — there is no cross-owner split.
Per-item outcomes reflect each item's actual result (`purged`/`conflict`/
`not_found`). A purge that targets an item with an **active lease** MUST return
`conflict` unless `force=true`; `force=true` invalidates the active lease and
purges the item in the same transaction. Request-id replay returns the recorded
per-item results for the committed purge. A purge MUST write a tombstone and
delete the item row in the same transaction.

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
and MUST NOT recompute them; purge replay returns the recorded queue-local
per-item results.

### Queue Execution and Fencing

Hot data-plane operations execute against a resolved `QueueKey` on the queue's
one owner. The `ControlPlaneStore` owns queue-to-owner assignment metadata and
monotonically increasing assignment epochs. pqueue service nodes consume those
assignments; they do not discover each other, elect leaders, or coordinate
ownership directly.

Every `LogStore.append_batch` receives the worker's expected assignment epoch.
The backend must reject appends from stale epochs. Reassignment is a
control-plane event: once a new epoch is visible, old workers may finish
non-mutating cleanup but must not append further commands. Recovery starts by
reading the latest snapshot and log tail for the queue epoch that is now
assigned.

This keeps the hot path bounded to tenant/queue routing plus backend fencing. It
does not require pqueue to maintain a cluster membership protocol, but it does
require each backend profile to document how epoch fencing is enforced.

The full ownership lifecycle — deterministic queue-to-owner assignment (target
vs active owner) over a live worker set via HRW/rendezvous hashing, storage-backed
queue leases in the pluggable `ControlPlaneStore`, monotonic epoch allocation
durably fenced into the log before a new lease is usable, reassignment, graceful
drain, and recovery from snapshot + log tail — is specified in TD-003
(`td-sharding-and-shard-ownership`). TD-001 defines the fencing token
(`expected_epoch` on `append_batch`), and `append_batch` MUST reject any
`expected_epoch` that is not the queue's current recorded epoch; TD-003 defines
how that epoch advances, who allocates it, and when it becomes binding on the log.

### Single-owner claim and per-queue progress (v1, normative)

Because the queue is the unit of sharding (ADR-008), every claim is served by the
queue's single owner against its own projection — there is **no** cross-owner
fan-out, k-way merge, claim-intent coordination, or distributed claim
transaction. The claim contract is defined against the single shared
**Eligibility Precedence** subsection in API-001; this section MUST NOT redefine
"eligible". A claim is the owner-local `ProjectionStore.batch_claim(ClaimPlan)`
defined for each backend (TD-002 / TD-004), and is atomic per API-001
("`BatchClaim` MUST atomically create each returned lease").

#### Atomicity scope

| Construct | Atomicity guarantee |
|-----------|---------------------|
| `whole_group` (via `compatibility.group_batching`) / `whole_cohort` (via `cohort_policy`/`whole_cohort`) | Whole-group/whole-cohort atomic: either the whole batched group/cohort is leased or none of it is, evaluated inside the owner's single claim transaction. Co-residency holds by construction (ADR-008), so no routing is needed. |
| `same_group_key` / explicit `group_key` | Leases the returned items atomically per API-001 (each returned lease created atomically) but MAY return a partial group subject to `max_items` and eligibility. `same_group_key`/`group_key` are item-level domain filters, NOT a whole-group unit, and do not guarantee the whole group is claimed. |
| Non-group claim | A single owner-local atomic claim over the queue's eligible items, honoring `max_items`. |

| Rule | Requirement |
|------|-------------|
| Owner-local execution | Every claim — grouped or not — executes as a single owner-local `batch_claim` transaction. There is no fan-out and no cross-owner merge. |
| Ordering | Results MUST be returned in the queue's deterministic result order (`(priority_sort, tie_breaker)`). A bounded-relaxed queue MAY relax ordering only within the queue's declared relaxation bound. |
| Single active lease | FR-25 holds per item; leases are owner-local, so no cross-owner lock is needed for lease uniqueness. |
| Idempotency / replay | A replayed `request_id` returns the queue's already-committed leases for that `request_id` (NOT a new claim), per Durable Ack and Response Replay. `request-expired` is evaluated over the leases recorded under that `request_id`: while any is active, replay returns the same set; once all are finalized/released/expired, replay MUST fail with `request-expired`. A new claim requires a new `request_id`. |

#### Per-queue progress

| Rule | Requirement |
|------|-------------|
| One local bound | The queue has ONE queue-global progress bound (D1; FR-9/FR-12), computed **locally** on its one owner from its own projection. There is NO per-group/per-owner aggregation, k-way merge, or sum. There is NO per-group/per-shard progress invariant. |
| Source | `oldest_eligible_age_ms` MUST equal `now() - min(oldest_eligible_at)` over the queue's `pqueue_group_summary` rows on its owner (gate-aware: the read MUST exclude rows whose eligibility is voided by the current gate generation, regardless of the stored `oldest_eligible_at`; the gate-generation mechanism is TD-002/TD-004's, G2). Eligible counts MAY be lagged/approximate; the effective oldest age MUST be authoritative. |
| Enforcement (state vs owner) | TD-001 owns clause (i): the owner's claim planner MUST claim any item near `progress_bound_ms` before the bound (TD-002 claim shape). Clause (ii) — that the queue has a live owner so the planner can run at all — is TD-003's owner-liveness guard; see TD-003 for the guard and its stalled/draining-queue rules. Queue-global compliance is the conjunction of the two. |
| Worker routing / fairness | Per-group fairness is achieved by routing workers via `DiscoverActiveScopes` (G4), NOT by an engine invariant (D1). Because the queue has one owner, `DiscoverActiveScopes` ranks the queue's scopes from the owner's summary index with no cross-owner merge. |

The detailed ownership, fencing, reassignment, drain, recovery, stalled-queue
handling, and the owner-liveness guard that protects the progress bound are
specified in TD-003 (`td-sharding-and-shard-ownership`). This section is the
storage-contract surface; TD-003 is the ownership/coordination mechanism.
Per-queue ownership (one single-writer owner per queue with epoch fencing) is a
committed v1 mechanism; only the *magnitude* claim — aggregate throughput/queue
scale beyond a single deployment — remains evidence-gated, and it is expressed as
**cross-queue scale-out** (TP-002 E2) over the per-queue throughput floor (E0:
>=10M items/hr per queue, preserved for every queue at any scale; E1–E3).

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
  recurrence,
  created_at,
  updated_at
}

QueueAssignment {
  tenant_id,
  queue_id,
  backend_profile,
  assignment_epoch,          // monotonic per queue; durably fenced into the log on acquire (TD-003)
  active_owner_id,           // current lease holder; null when unassigned
  target_owner_id,           // deterministic assignment-function target
  state                      // unassigned | assigned | draining
}
```

There is no `shard_count`: the queue is the unit of sharding (ADR-008), so a
queue maps to exactly one owner. A producer needing more than one owner's
throughput partitions its stream across multiple queues at the application layer.

`recurrence` carries the per-queue recurrence mode and `until` bound (G5). It is
a per-queue immutable flag; there is no `backoff` sub-object in v1 and no mixed
one-shot/recurring queues.

`group_key` is an **ordering/compatibility** concern only (ADR-004 D2 / ADR-008),
never a placement key. Group/cohort claims are owner-local and atomic by
construction; there is no `group_co_residency` flag (it is removed from the
contract and the config-identity hash — co-residency holds because the whole
queue lives on one owner).

TD-003 owns the authoritative `QueueAssignment` lease shape (it adds
`lease_expires_at` and the ownership operations); the fields shown here are the
control-plane reads TD-001 depends on. The three epoch surfaces are **one and the
same `u64` queue epoch**: `QueueAssignment.assignment_epoch` (allocated by the
control plane), `CommandPosition.backend_epoch` (recorded on each appended
command), and the `expected_epoch` argument to `LogStore.append_batch` (the
fence). TD-003 specifies how the epoch advances and when it becomes binding on
the log.

### Logical Command Records

Every command record must include:

- `command_id`
- `request_id` when the API operation is mutating
- `tenant_id`, `queue_id`
- command type and payload
- affected `item_id`s where known
- command timestamp
- checksum
- backend position after append

### Logical Projection Records

Projection stores must represent:

- item identity: `item_id`, `client_item_key`, `tenant_id`, `queue_id`
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
`(tenant_id, queue_id, group_key)`: exactly one row per `(queue, group_key)`.
Because the whole queue lives on one owner, a group has exactly one summary row;
the grain stays coherent for the relational projection and for the local-SQLite
backend (TD-004). `oldest_eligible_at` per row is authoritative and exact;
eligible counts MAY lag/be approximate. It is the sole source for
`DiscoverActiveScopes` (G4) and for the queue's local progress bound (TD-003).
A queue-level gate flip MUST NOT synchronously rewrite every group's summary row;
`oldest_eligible_age_ms` stays authoritative (computed against the current gate
generation at read) while counts MAY lag.

Cohort queues add a `pqueue_cohorts` projection for cohort identity (logical key
`(tenant_id, queue_id, group_key)`; size, member count, state,
`cohort_created_at`, first-eligible time, expire command position, cohort lease
token hash, `retention_until`) (G6). Cohort eligible-age and counts are NOT
duplicated here; they come from the single `pqueue_group_summary`.

## Integration Points

| From | To | Method | Data |
|------|----|--------|------|
| `pqueue-service` | `pqueue-core` | Direct Rust call | API-001 operation structs |
| `pqueue-core` | `ControlPlaneStore` | Trait | queue definitions, queue assignment, backend profile |
| `pqueue-core` | `LogStore` | Trait | durable command envelopes |
| `pqueue-core` | `ProjectionStore` | Trait | committed commands, claim plans, metrics reads |
| `pqueue-core` | `SnapshotStore` | Trait | projection snapshots and recovery checkpoints |
| Backend conformance tests | Backend crates | Trait test harness | deterministic scenarios and crash/replay fixtures |

### External Dependencies

- **Postgres**: preferred `ControlPlaneStore`; fallback is no service-mode queue
  creation or queue routing until Postgres is restored.
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
  - cross-tenant/cross-queue corruption: mitigate by embedding the queue key and
    checksums in every command envelope.

## Performance

- **Expected Load**: the per-queue throughput floor (TP-002 E0: >=10M items/hr
  per queue, preserved for every queue at any scale); at least 10M items in a hot
  queue; at least 1000 concurrently active queues per node (queue density,
  TP-002 E2); large batches for cost-optimized object-log profiles.
- **Queue density (>=1000 active queues per node)**: backend implementations of
  the capability traits MUST NOT allocate unbounded per-queue resources.
  Background work — lease-expiry sweeps, progress-bound aggregation,
  `pqueue_group_summary` recompute, recurring rearm, and idempotency/retention GC
  — MUST be multiplexed onto bounded shared per-node pools (a batched sweeper that
  scans many queues per pass, a shared connection pool, a bounded/LRU set of open
  per-queue projection handles), never one task, loop, or connection per queue. A
  node MUST sustain >=1000 concurrently active queues with each meeting its
  progress bound and any one able to reach the per-queue floor; aggregate
  single-node throughput is bounded by the node, and multi-node deployment
  provides aggregate headroom.
- **Response Target**: API-001 core batch operations target sub-second p95/p99
  under representative workloads, except object-log profiles where configured
  batch windows may intentionally trade acknowledgement latency for cost.
- **Delivered envelopes**: these figures define two delivered v1 envelopes. The
  single-deployment envelope is delivered by `postgres_native` and validated
  against the per-queue throughput floor (TP-002 E0: >=10M items/hr per queue;
  E1). The horizontal envelope spreads write/claim load **across queues**
  distributed over independent owners (cross-queue scale-out, ADR-008) and is
  delivered by per-queue ownership (TD-003) and the `object_log_sqlite_projection`
  backend (TD-004); it is validated by TP-002 E2/E3. Per-queue ownership and the
  `QueueKey` routing primitive deliver this, not intra-queue sharding.
- **Optimizations**:
  - route by `tenant_id / queue_id` to the queue's owner
  - keep claim indexes in `ProjectionStore`
  - use batch append/apply paths for every mutating operation
  - bound request-id and item-key retention windows
  - snapshot projections to bound replay for log-backed local projections
  - expose progress-bound risk via eligible age metrics

## Testing

- **Unit**: command validation, request fingerprinting, item-key
  convergence, `item_version` transitions, retry exhaustion, metadata blockers.
- **Integration**: `LogStore` append/read replay, projection rebuild from
  log, snapshot restore, Postgres control-plane create/read assignment.
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
  must pass before use (core + log / relational-durability per the capability
  classes above).

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
| Relational reconnect durability | After process loss, a transactional-authoritative relational projection still returns acknowledged state on reconnect (the relational substitute for replay-from-log). |
| Progress-bound risk | Eligible age metrics identify near-violation items. |
| Tenant isolation | Tenant A cannot read or mutate tenant B state. |
| Stale-epoch reject | Append under a superseded epoch fails without mutating state (TD-003). |
| Stale writer after epoch advance, before new data segment | An epoch-E writer is rejected immediately once E+1 is fenced, before any E+1 data segment exists (TD-003). |
| Reassignment recovery | New owner with a greater epoch recovers queue state from snapshot + log tail (TD-003). |
| Group routing by construction | On a queue, items of one `group_key` are owned by the queue's single owner; a `whole_group` claim (via `compatibility.group_batching`) and a `whole_cohort` claim (via `cohort_policy`/`compatibility.whole_cohort`) are whole-group/whole-cohort atomic and owner-local; `same_group_key` (item-filter only) is owner-local and per-item atomic but MAY return a partial group. No co-residency flag exists. |
| Owner-local claim + order | A non-group claim returns a deterministic ordered batch within `max_items` from the owner's projection; no cross-owner merge. |
| Per-queue progress bound | The queue's oldest-eligible item is claimed before `progress_bound_ms` (queue-global, computed locally on the owner, D1). |
| Claim replay convergence | A replayed claim `request_id` returns the same active lease set; `request-expired` once all leases under that `request_id` are inactive. |
| Group-commit ack boundary | For batched-log profiles, no command is acked before its durable segment/manifest commit; kill after segment write, before manifest commit, shows command not acked and re-drivable by `request_id`. |
| Current-epoch manifest fencing | A writer whose queue was reassigned (control-plane epoch advanced) before the new owner wrote any data manifest entry MUST fail its commit; manifest-recorded-epoch-only validation is insufficient and MUST NOT pass. New epoch holder reproduces acknowledged state. |
| In-flight claim reservation safety | Concurrent claims cannot both reserve the same candidate while a segment is pending; CAS failure / timeout / fence / writer crash rolls back reservations with no durable lease; retry converges. |
| Snapshot + log-tail recovery | Restore latest snapshot, replay segments after the snapshot position, validate checksums, reproduce projection state. |
| Queue-scoped command convergence | A queue-scoped command (`SetGates`) applies to the queue's owner before ack, all-or-nothing; retry by `request_id` converges with no double-apply. |
| Safe log-segment expiry | A log segment is deletable only after a covering committed snapshot plus recovery window; no expired segment is required for an in-window recovery. |
| Reject one-object-per-command | A production configuration that seals one command per durable object is rejected; only an explicit dev/test flag permits it. |
| Atomic complete-cohort claim | A complete cohort is leased all-or-nothing under one shared lease, never split or double-leased; a member is never individually claimable; `CohortExpired` precedes any claimability change; survives writer restart (G6). |
| Cohort duplicate push / reuse | Duplicate push of a cohort member is a no-op; `group_key` reuse after `retention_until` yields a new `cohort_id`; whole-cohort lease replay returns the same members/`cohort_id`/lease (G6). |
| Perpetual re-arm never terminal | Re-arm more times than `max_attempts`; never terminal; a fresh cycle gets a full per-cycle retry budget (G5). |
| Rearm replay determinism | Duplicate `request_id` returns recorded `not_before`/`eligible_since`/priority/version; no recompute; `eligible_since = max(commit_time, not_before)` (G5). |
| Recurring progress parity | A recurring singleton stays owned by its queue's owner across re-arms; a re-armed eligible item is claimed before the queue-global `progress_bound_ms` with no per-group bound (D1, G5). |
| Recurring eligibility parity | An idle (future `not_before`) recurring item and a gate-blocked re-armed item are NOT returned by `BatchClaim` and do NOT contribute to any `DiscoverActiveScopes` descriptor; an eligible ungated re-armed item is returned and contributes (G2/G4, G5). |
| Purge replay | `PurgeItems force=true` removes a leased recurring item and invalidates the lease; duplicate purge `request_id` is idempotent and queue-local; a finalize for a purged item returns `not_found`; a re-push after the tombstone window creates a fresh item (G5). |

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
   Tests: backend-agnostic conformance fixtures (core / log / relational-durability).
3. Implement Postgres `ControlPlaneStore`.
   Files: `crates/pqueue-postgres/src/control_plane/**`.
   Tests: tenant-scoped queue create/read, queue assignment, backend profile.
4. Implement Postgres-native `LogStore` and `ProjectionStore` per TD-002
   (single-deployment envelope; relational projection family).
5. Implement per-queue ownership and fencing per TD-003 (queue lease, HRW owner
   assignment, epoch fence, drain, reassignment, recovery).
6. Implement the `object_log_sqlite_projection` backend per TD-004: S3 `LogStore`
   (group-commit segments + manifest with current-epoch fencing), SQLite
   `ProjectionStore` (with in-flight claim reservations), S3 `SnapshotStore`,
   bounded replay, and the per-queue epoch binding to TD-003 (horizontal envelope
   cost/scale).
   Files: `crates/pqueue-objectlog/src/**`, `crates/pqueue-sqlite/src/**`.
   Tests: shared conformance suite (including the object-log rows) plus the
   TD-004 scale/cost evidence record (TP-002 E3 vs E0).
7. Implement `pqueue-service` HTTP binding after core structs and first backend
   compile.

**Prerequisites**: API-001 complete; ADR-001, ADR-002, ADR-003, ADR-004, and
ADR-008 accepted; TD-002, TD-003, and TD-004 accepted; TP-002 available for test
traceability; Rust workspace setup bead filed from ADR-003.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Trait abstraction hides backend-specific correctness requirements | M | H | Capability-specific conformance tests and durability profile metadata. |
| Postgres-native mode becomes the de facto only architecture | M | M | Keep backend profile boundaries and command positions in the first implementation; the second committed backend (object-log) is held to the same conformance contract. |
| Object-log profile cannot meet acceptable ack latency | M | M | Spike group commit latency before implementation; document latency/cost profile. |
| Local projections diverge from durable log | M | H | Apply only committed commands; test replay and snapshot recovery. |
| Idempotency storage grows without bound | M | M | Enforce separate request and item-key retention windows. |
| Claim compatibility causes hidden starvation | M | H | Test server-selected group fairness and document caller-filtered domain limits. |
| Incomplete/gated cohort starves queue-global progress bound | M | H | Hard `completion_bound_ms <= progress_bound_ms` check at `CreateQueue`; linearized `CohortExpired`. |

## Review Checklist

- [x] Governing API-001 operations map to storage flows.
- [x] ADR-001 CQRS/log-projection decision is preserved.
- [x] Control plane is a pluggable capability; Postgres preference preserved
      (ADR-008; object-store impl deferred).
- [x] Backend capability interfaces are explicit; the owned/routed unit is the
      whole queue (`QueueKey`), no `ShardKey` in the contract surface (ADR-008).
- [x] Durable ack boundary is explicit.
- [x] Idempotency, leases, item versions, and replay are explicit.
- [x] Security covers tenant authorization and data isolation.
- [x] Performance targets reference PRD scale requirements; horizontal envelope is
      cross-queue scale-out (ADR-008), not intra-queue sharding.
- [x] Tests include conformance, API edge cases, security, and performance.
- [x] Claim is single-owner-local; per-queue progress bound is computed locally;
      ownership/fencing detail delegated to TD-003.
- [x] Two projection families held by conformance-as-contract (core / log /
      relational-durability capability classes).
- [x] Unified `ClaimPlan {item, whole_group, whole_cohort}` with claim-unit
      reachability rules; `same_group_key` is an item-level filter only.
- [x] Single `pqueue_group_summary` projection keyed `(tenant, queue, group_key)`;
      recurrence, cohort, and tombstone records added; no rate stage in the claim
      pipeline; no `shard_count` / `group_co_residency`.
