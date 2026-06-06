---
ddx:
  id: td-storage-architecture-backend-contracts
  depends_on:
    - api-native-client-interface
    - adr-cqrs-log-projection-storage-model
    - concerns
    - prd
---

# Technical Design: TD-001 Storage Architecture and Backend Contracts

**Contract**: API-001 | **ADR**: ADR-001 | **Scope**: storage architecture

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
- Exact S3 object naming, manifest format, and batching thresholds. A tech spike
  owns object-log latency and cost validation.
- HTTP route implementation and SDK packaging. API-001 owns client semantics.
- Operator repair, purge, redrive, and backend migration APIs.

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

Only `postgres_native` is expected to be implemented first. Other profiles
define design targets and conformance expectations.

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
  payload reference or payload value
- lease state: active `lease_token`, `lease_expires_at`, `worker_id`
- version state: `item_version`, last command position
- idempotency state: request fingerprints and responses for
  `request_id_retention_ms`; item-key convergence records for
  `client_item_key_retention_ms`
- metrics state: lifecycle counts, retry backlog, active leases,
  `oldest_eligible_age_ms`, `progress_bound_risk_count`

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

- **Expected Load**: millions of commands per hour per deployment; at least 10M
  items in a hot queue; large batches for cost-optimized object-log profiles.
- **Response Target**: API-001 core batch operations target sub-second p95/p99
  under representative workloads, except object-log profiles where configured
  batch windows may intentionally trade acknowledgement latency for cost.
- **Optimizations**:
  - partition by `tenant_id / queue_id / shard_id`
  - keep claim indexes in `ProjectionStore`
  - use batch append/apply paths for every mutating operation
  - bound request-id and item-key retention windows
  - snapshot projections to bound replay for log-backed local projections
  - expose progress-bound risk via eligible age metrics

## Testing

- [ ] **Unit**: command validation, request fingerprinting, item-key
  convergence, `item_version` transitions, retry exhaustion, metadata blockers.
- [ ] **Integration**: `LogStore` append/read replay, projection rebuild from
  log, snapshot restore, Postgres control-plane create/read assignments.
- [ ] **API**: API-001 acceptance sketches, including request-id conflict,
  optimistic update conflict, leased update conflict, claim retry idempotency,
  tenant spoofing rejection, and SQS adapter limitation.
- [ ] **Security**: tenant isolation negative tests for control plane, log,
  projection, and snapshot backends.
- [ ] **Concurrency**: duplicate claim prevention, stale lease finalization,
  lease expiry redelivery, group-aware claim progress under skew.
- [ ] **Performance**: 10M-item projection benchmark, batch push/update/claim/
  finalize throughput, telemetry-on latency, object-log group commit latency.
- [ ] **Conformance**: shared backend test suite that every `LogStore`,
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
4. Draft TD-002 for Postgres-native log/projection DDL and query paths before
   implementing `LogStore`/`ProjectionStore`.
5. Implement `pqueue-service` HTTP binding after core structs and first backend
   compile.

**Prerequisites**: API-001 complete; ADR-001 accepted; Rust workspace design or
initial Cargo workspace setup bead.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Trait abstraction hides backend-specific correctness requirements | M | H | Capability-specific conformance tests and durability profile metadata. |
| Postgres-native mode becomes the de facto only architecture | M | M | Keep backend profile boundaries and command positions in the first implementation. |
| Object-log profile cannot meet acceptable ack latency | M | M | Spike group commit latency before implementation; document latency/cost profile. |
| Local projections diverge from durable log | M | H | Apply only committed commands; test replay and snapshot recovery. |
| Idempotency storage grows without bound | M | M | Enforce separate request and item-key retention windows. |
| Claim compatibility causes hidden starvation | M | H | Test server-selected group fairness and document caller-filtered domain limits. |

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
