---
ddx:
  id: api-fireweed-rust-facade
  depends_on:
    - api-native-client-interface
    - api-hot-projection-query-surface
    - adr-concrete-fireweed-facade-and-optional-controls
  status: accepted
---

# API-005: Fireweed Rust facade

**Type**: Rust binding contract
**Version**: v0.20
**Status**: accepted
**Related**: API-001, API-004, ADR-009, ADR-020, ADR-022, TP-004

## Purpose

This contract binds API-001's native queue model to the public `fireweed`
crate. It defines the exact ownership and naming shape needed by downstream
embedders, with Snorri as the release acceptance client. It does not redefine
queue semantics.

## Scope and boundaries

- In scope: the supported Rust root type, constructors, configuration names,
  inherent queue methods, runtime construction facts, projection maintenance,
  export closure, compatibility, and structured errors.
- Out of scope: storage-port object safety, backend algorithms, RESP bindings,
  new queue semantics, and the backend-neutral replacement for `read_as_of`.
- Owning crate: `fireweed`.

## Normative surface

### Supported root

```rust
pub struct Fireweed { /* private */ }

impl Fireweed {
    pub fn projection_control(&self) -> Option<ProjectionControl<'_>>;
}
```

`Fireweed` has no public type parameter. Its fields, backend dispatch, and
lifecycle ownership are private. It is `Send + Sync`; callers may place it in
an `Arc`. Clone semantics are not required for v0.20.

The crate root must not require a downstream user to name `Pqueue`,
`EmbeddedPqueue`, `LibBackend`, a concrete backend, or an internal workspace
crate. `Pqueue`, `EmbeddedPqueue`, `LibBackend`, `EmbeddedHandle`, and
`Pqueue::new` are Rust implementation names, not ADR-020 package/name aliases;
they MUST be unavailable to an ordinary external crate when v0.20 ships.
First-party tests use a crate-private construction seam.

Storage authority, projection implementation, response-barrier choice, and
coordination topology are construction inputs only. `Fireweed` exposes no
post-construction backend/projection identity, discriminator, downcast, or
configuration getter. Callers make behavioral decisions through the existing
queue-scoped capability methods and through `projection_control()` when
maintenance authority exists.

### Construction

The release-critical constructors preserve clock injection and return the same
concrete type:

```rust
pub fn open_memory(clock: Arc<dyn Clock>) -> Fireweed;
pub fn open_sqlite(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
pub fn open_sqlite_relational(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
pub fn open_objectlog(root: impl Into<PathBuf>, clock: Arc<dyn Clock>)
    -> EngineResult<Fireweed>;
pub fn open_postgres(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
pub fn open_postgres_coordinated(
    url: &str,
    clock: Arc<dyn Clock>,
    instance_id: OwnerId,
    control_plane_config: ControlPlaneConfig,
) -> EngineResult<Fireweed>;
pub fn open_postgres_runtime(
    config: PostgresRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
pub fn open_objectlog_postgres(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
pub async fn open_objectlog_postgres_async(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
pub fn open_objectlog_sqlite(
    config: ObjectLogRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
```

Advanced PostgreSQL deployments may select their storage shape, schema, node
identity, and coordination topology at the composition root:

```rust
pub enum PostgresMode { LogReplay, Relational }

pub struct PostgresCoordinationConfig {
    pub instance_id: OwnerId,
    pub control_plane: ControlPlaneConfig,
}

pub struct PostgresRuntimeConfig {
    pub url: ConfigSecret,
    pub schema: Option<String>,
    pub mode: PostgresMode,
    pub node_id: Option<u8>,
    pub coordination: Option<PostgresCoordinationConfig>,
}
```

These fields are construction inputs only. The resulting `Fireweed` does not
expose the selected mode, schema, node identity, coordination topology, or
backend objects. `open_postgres` and `open_postgres_coordinated` remain the
convenience constructors for their common configurations.

The composed object-log configuration is:

```rust
pub struct ObjectLogRuntimeConfig {
    pub object_log: ObjectLogStorage,
    pub projection: ProjectionConfig,
    pub response_barrier: ResponseBarrier,
    pub segments: SegmentConfig,
    pub namespace: String,
    pub recovery: RecoveryPolicy,
}

pub enum ObjectLogStorage {
    Local { root: PathBuf },
    S3Compatible {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: ConfigSecret,
        secret_access_key: ConfigSecret,
        allow_insecure_http: bool,
    },
}

pub enum ProjectionConfig {
    Sqlite { path: PathBuf },
    Postgres { url: ConfigSecret },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConfigSecret(/* private String */);
pub enum ResponseBarrier { Strict, AsyncProjection }
pub struct SegmentConfig { pub target_bytes: usize, pub max_latency_ms: u64 }
pub enum RecoveryAction { FailClosed, RebuildProjection }
pub struct RecoveryPolicy {
    pub incompatible_projection: RecoveryAction,
    pub verify_checksums: bool,
    pub max_tail_commands: u64,
}
```

`ConfigSecret::new`, `SegmentConfig::new`, `RecoveryPolicy::default`, and
`ObjectLogRuntimeConfig::validate` preserve the corresponding current
validation behavior. `ConfigSecret` exposes no plaintext accessor and its
`Debug` implementation always redacts the contained value.
`open_objectlog_sqlite` requires
`ProjectionConfig::Sqlite`; the Postgres constructors require
`ProjectionConfig::Postgres`; a mismatched variant returns
`EngineError::Unavailable` before opening either store. The Postgres projection
constructors additionally require `ResponseBarrier::Strict`; they return
`EngineError::Unavailable` for `AsyncProjection`.

`EmbeddedSecret`, `EmbeddedObjectLogConfig`, `EmbeddedProjectionConfig`,
`EmbeddedResponseBarrier`, `EmbeddedSegmentConfig`,
`EmbeddedRecoveryAction`, `EmbeddedRecoveryPolicy`,
`EmbeddedDurabilityConfig`, and the `open_embedded*` functions are not ADR-020
compatibility aliases: ADR-020 covers package and product-name migration, not a
second Rust facade. These Rust names MUST be replaced for v0.20 rather than
promoted as supported aliases. Every constructor returns `Fireweed`; no
profile-specific wrapper type is returned.

### Queue operations

The following operation families are inherent `Fireweed` methods and preserve
their current input, output, error, and async behavior. This is a facade
compatibility inventory; it does not assert that every convenience method is a
complete binding of API-001's request-id-bearing batch operation:

| Family | Methods |
| --- | --- |
| Queue and ownership | `ownership`, `renew_owned`, `create_queue`, `queue_definition`, `ensure_queue` |
| Append and replace | `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id`, `upsert` |
| Claim | `claim`, `claim_with`, `claim_response_with`, `claim_at`, `claim_response_at`, `claim_across_queues`, `claim_by_query`, `claim_by_query_at` |
| Finalize and commit | `ack`, `complete`, `nack`, `retry`, `release`, `nack_retry_after`, `retry_after`, `commit`, `commit_multi_claim`, `commit_capabilities`, `explain_commit`, `side_record`, `fail` |
| Read and discovery | `peek`, `current_position`, `discover_active_scopes`, `discover_active_scopes_stamped`, `discover`, `live_item`, `live_items`, `query_index_unique`, `query_index`, `query_index_unique_typed`, `query_index_typed`, `claimed` |
| Metrics and projection query | `metrics`, `metrics_by_query`, `hot_projection_capabilities`, `range_scan`, `grouped_aggregate`, `declared_bucket_segment` |
| Mutation and maintenance | `renew`, `reassign`, `update_fields`, `batch_update`, `update`, `set_gates`, `reclaim_expired`, `reclaim_expired_at`, `rearm`, `rearm_at`, `rearm_after`, `purge`, `bounded_mutation` |

Iterator-taking compatibility methods may collect into `Vec<ItemId>` at the
erased boundary. This does not change their observable contract.

`push_with_request_id`, `push_batch_with_request_id`, `commit`, and
`commit_multi_claim` are the explicit retained-request-id mutation bindings in
this facade. Convenience methods without a `RequestId` parameter—such as
`push`, `push_batch`, `ack`, `complete`, `nack`, `retry`, `release`, `fail`,
`renew`, `reassign`, `update`, and `purge`—preserve their current library
behavior but MUST NOT be cited as proof of API-001 unknown-outcome replay.
Where API-001 requires a request id and per-item outcomes not represented by a
convenience signature, API-001 remains the desired transport-neutral contract
and the convenience helper is classified as a narrower Rust binding.

The free active-scope selector and its value types remain supported:
`select_active_scope_from_prefix`, `ActiveScopeDiscovery`,
`OldestFirstScopePrefix`, and `ActiveScopeSelection`.

`read_as_of<T, F>` is excluded from v0.20's supported facade because its
callback exposes a backend-associated projection type. Its replacement must be
an owned, backend-neutral request/response contract under a separately reviewed
history component. Removing this generic escape hatch does not remove
`current_position` or ordinary recovery reads.

### Projection control

```rust
pub struct ProjectionControl<'a> { /* borrowed from Fireweed */ }

impl ProjectionControl<'_> {
    pub fn capabilities(&self) -> ProjectionControlCapabilities;
    pub async fn verify(&self) -> EngineResult<ProjectionVerification>;
    pub async fn delete(&self) -> EngineResult<()>;
    pub async fn rebuild(&self) -> EngineResult<ProjectionRebuild>;
}
```

`rebuild` is the supported verb; `rehydrate` is retired from the external Rust
surface. The result and capability field use `rebuild` consistently:

```rust
pub struct ProjectionControlCapabilities {
    pub verify: bool,
    pub delete: bool,
    pub rebuild: bool,
}

pub struct ProjectionVerification {
    pub compatible: bool,
    pub projection_sequence: u64,
    pub authoritative_sequence: u64,
}

pub struct ProjectionRebuild {
    pub snapshot_used: bool,
    pub tail_commands_replayed: u64,
    pub projection_sequence: u64,
}
```

`projection_control()` returns `Some` only when the active runtime owns a
disposable projection and supports at least one maintenance operation. It is
not a test for whether reads are projection-backed. The control exposes no
queue append, claim, update, query, or commit method.

The capability value is derived from the active lifecycle implementation. The
operation still returns a structured error if capability or runtime state
changes between inspection and invocation.

The control is a borrowed view. The supported contract does not include
`Clone`, an owned lifecycle handle, or a way to retain maintenance authority
after dropping the parent `Fireweed`. Borrowing through `Arc<Fireweed>` across
an `.await` is supported.

### Export closure

The `fireweed` crate re-exports every named public input and output type used by
the methods and constructors above. This includes `ConfigSecret`,
`ControlPlaneConfig`, `ObjectLogRuntimeConfig`, `ObjectLogStorage`, `OwnerId`,
`ProjectionConfig`, `ProjectionControl`, `ProjectionControlCapabilities`,
`ProjectionRebuild`, `ProjectionVerification`, `RecoveryAction`,
`RecoveryPolicy`, `ResponseBarrier`, and `SegmentConfig`. For the Snorri
migration it also includes all current facade DTOs plus
`CompoundIndexDef`, `CompoundIndexField`, `IndexDeclaration`, `IndexType`,
`QueueIndex`, and `WorkerId`. A compile fixture depending only on `fireweed` is
the enforcement mechanism.

The Snorri named-type closure is:

`AggregateGroup`, `BucketRule`, `Bytes`, `ClaimAt`, `ClaimByQueryAt`,
`ClaimByQueryRequest`, `ClaimCompatibility`, `ClaimRef`, `Claimed`,
`ClaimedItem`, `ClientItemKey`, `Clock`, `CommitCapabilities`, `CommitEntry`,
`CommitRecovery`, `CommitRequest`, `CompoundIndexDef`, `CompoundIndexField`,
`CreateQueueOutcome`,
`DeclaredBucketSegmentRequest`, `DeclaredBucketSegmentResponse`,
`EligibilityPolicy`, `EngineError`, `EngineResult`, `EntryOutcome`, `FilterOp`,
`FinalizeKind`, `GroupByField`, `GroupedAggregateRequest`,
`GroupedAggregateResponse`, `IndexDeclaration`, `IndexHit`, `IndexType`,
`InstanceFence`, `ItemId`, `LeaseToken`, `LiveItemView`, `Metadata`,
`MetadataValue`, `MetricsByQueryRequest`, `Nack`, `NewItem`, `OrderField`,
`OrderingMode`, `PriorityModel`, `PriorityValue`, `QueryCapabilityFlags`,
`QueryCursor`, `QueryFilter`, `QueueDefinition`, `QueueId`, `QueueIndex`,
`QueueKey`, `QueueMetrics`, `RangeScanRequest`, `RangeScanResponse`,
`RecurrencePolicy`, `RequestId`, `RetryPolicy`, `ScheduleUpdate`, `SideRecord`,
`SortDirection`, `TenantId`, `TimeBucket`, `TypedValue`, `UpsertOutcome`,
`UtcTimestamp`, and `WorkerId`.

### Snorri acceptance slice

Before the release candidate is usable, Snorri must compile against one
non-generic `Fireweed` using these operations:

`create_queue`, `push`, `push_with_request_id`,
`push_batch_with_request_id`, `upsert`, `claim_with`, `claim_by_query`,
`claim_by_query_at`, `ack`, `nack`, `commit`, `commit_capabilities`,
`explain_commit`, `side_record`, `live_item`, `query_index_unique_typed`,
`update`, `purge`, `claimed`, `metrics`, `metrics_by_query`,
`hot_projection_capabilities`, `range_scan`, `grouped_aggregate`, and
`declared_bucket_segment`.

Object-log profiles additionally consume `projection_control` and its four
operations. No Snorri public or private type may retain a `LibBackend` bound.

The Snorri-critical method signatures MUST remain type-equivalent in parameter
ownership and result shape to the v0.19.6 facade, with `Self` changed from
`Pqueue<B>` to `Fireweed`. In particular:

```rust
pub async fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome>;
pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId>;
pub async fn push_with_request_id(&self, queue: &QueueKey, request_id: RequestId, item: NewItem) -> EngineResult<ItemId>;
pub async fn push_batch_with_request_id(&self, queue: &QueueKey, request_id: RequestId, items: Vec<NewItem>) -> EngineResult<Vec<ItemId>>;
pub async fn upsert(&self, queue: &QueueKey, key: ClientItemKey, item: NewItem) -> EngineResult<UpsertOutcome>;
pub async fn claim_with(&self, queue: &QueueKey, max: usize, lease_ms: u64, compatibility: ClaimCompatibility) -> EngineResult<Vec<ClaimedItem>>;
pub async fn claim_by_query(&self, queue: &QueueKey, request: ClaimByQueryRequest) -> EngineResult<Claimed>;
pub async fn claim_by_query_at(&self, queue: &QueueKey, request: ClaimByQueryRequest, at: ClaimByQueryAt) -> EngineResult<Claimed>;
pub async fn ack(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>) -> EngineResult<()>;
pub async fn nack(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>, how: Nack) -> EngineResult<()>;
pub async fn commit(&self, queue: &QueueKey, request: CommitRequest) -> EngineResult<Vec<EntryOutcome>>;
pub fn commit_capabilities(&self, queue: &QueueKey) -> EngineResult<CommitCapabilities>;
pub async fn explain_commit(&self, queue: &QueueKey, request_id: RequestId) -> EngineResult<Option<CommitRecovery>>;
pub async fn side_record(&self, queue: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>>;
pub async fn live_item(&self, queue: &QueueKey, key: ClientItemKey) -> EngineResult<Option<LiveItemView>>;
pub async fn query_index_unique_typed(&self, queue: &QueueKey, index: &str, values: &[serde_json::Value]) -> EngineResult<Option<IndexHit>>;
pub async fn update(&self, queue: &QueueKey, item_id: ItemId, priority: ScheduleUpdate<PriorityValue>, not_before: ScheduleUpdate<UtcTimestamp>, expected_item_version: Option<u64>) -> EngineResult<u64>;
pub async fn purge(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>, force: bool) -> EngineResult<u64>;
pub async fn claimed(&self, queue: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>>;
pub async fn metrics(&self, queue: &QueueKey) -> EngineResult<QueueMetrics>;
pub async fn metrics_by_query(&self, queue: &QueueKey, request: MetricsByQueryRequest) -> EngineResult<QueueMetrics>;
pub fn hot_projection_capabilities(&self, queue: &QueueKey) -> QueryCapabilityFlags;
pub async fn range_scan(&self, queue: &QueueKey, request: RangeScanRequest) -> EngineResult<RangeScanResponse>;
pub async fn grouped_aggregate(&self, queue: &QueueKey, request: GroupedAggregateRequest) -> EngineResult<GroupedAggregateResponse>;
pub async fn declared_bucket_segment(&self, queue: &QueueKey, request: DeclaredBucketSegmentRequest) -> EngineResult<DeclaredBucketSegmentResponse>;
```

## Precedence and compatibility

- API-001 and API-004 govern queue semantics. API-005 governs Rust ownership,
  names, signatures, construction, and export closure.
- Returning `Fireweed` is a deliberate source break from inferred
  `Pqueue<impl LibBackend>` return types. Migration guidance MUST show removal
  of downstream backend parameters.
- The package aliases allowed by ADR-020 do not authorize the legacy Rust
  facade types listed under Supported root.
- Adding a `Fireweed` method or capability bit is compatible. Removing or
  changing a supported method or DTO requires a breaking pre-1.0 minor and
  migration guidance.

## Error semantics

| Condition | Error / outcome | Retry | Recovery expectation |
| --- | --- | --- | --- |
| Profile does not support an operation | Existing structured `EngineError`, normally `Unavailable` | Only after selecting a supporting profile | No backend downcast or internal-port call is permitted |
| Queue-scoped capability is absent | Capability value is false or the operation returns its existing structured error | Per API-001 | Branch on the queue-scoped capability; backend identity is not observable |
| Projection maintenance is not owned | `projection_control()` returns `None` | No | Queue operations and hot-query capability checks remain independent |
| Projection is offline or maintenance fails | Structured `EngineError` from the control operation | Per the existing recovery contract | Re-inspect or rebuild through the same borrowed control |
| Synchronous object-log/Postgres open occurs inside Tokio | `EngineError::Invalid` directing the caller to `open_objectlog_postgres_async` | Yes | No partially opened `Fireweed` escapes |

## Examples

```rust
use std::sync::Arc;
use fireweed::{EngineError, Fireweed, QueueKey};

async fn activate(fireweed: Arc<Fireweed>, queue: QueueKey) -> Result<(), EngineError> {
    if !fireweed.commit_capabilities(&queue)?.atomic_transition_commit {
        return Err(EngineError::Unavailable);
    }
    if let Some(control) = fireweed.projection_control() {
        let verification = control.verify().await?;
        if !verification.compatible {
            control.rebuild().await?;
        }
    }
    Ok(())
}
```

## Non-normative notes

A private object-safe dispatch trait over high-level facade operations is one
implementation strategy. It is not part of this contract.

## Validation checklist

- [ ] `Fireweed` is concrete, `Send + Sync`, and sufficient behind `Arc`.
- [ ] Every existing supported facade family is forwarded or explicitly
      excluded by this contract.
- [ ] Snorri's exact method and named-type closure compiles from `fireweed`
      alone.
- [ ] Backend and projection identity are selectable only during construction
      and are not observable from a live `Fireweed`.
- [ ] Projection maintenance is borrowed, uses `rebuild` consistently, and
      exposes no queue operation.
- [ ] Legacy Rust facade/config names are unavailable to external code.
- [ ] Errors preserve existing structured semantics and require no backend
      downcast for recovery.
