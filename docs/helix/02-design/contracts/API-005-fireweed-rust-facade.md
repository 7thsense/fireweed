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
**Version**: v0.24
**Status**: accepted
**Related**: API-001, API-004, ADR-009, ADR-012, ADR-015, ADR-017, ADR-022,
ADR-023, TP-004, orthogonal-storage-matrix-brief

## Purpose

This contract binds API-001's native queue model to the public `fireweed`
crate. It defines the exact ownership and naming shape needed by downstream
embedders, with Snorri as the release acceptance client. It does not redefine
queue semantics.

## Scope and boundaries

- In scope: the supported Rust root type, constructors, configuration names
  (including full-matrix `StorageConfig`), inherent queue methods, runtime
  construction facts, projection maintenance, export closure, compatibility,
  structured errors, and the product execution / concurrency model for that
  facade (native-async composition path).
- Out of scope: storage-port object safety, backend algorithms, RESP bindings,
  new queue semantics, the backend-neutral historical query component, dual
  public facade types, and re-exporting `fireweed-engine` async modules as
  the embedder surface.
- Owning crate: `fireweed`.
- Non-goals: a second public root type (sync vs async facades); exposing
  engine composition modules (`AsyncComposedBackend`, async store traits,
  dispatch internals) as the supported embedder API.

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
an `Arc`. Clone semantics are not required for v0.21.

**One concrete public type.** The supported embedder surface is a single
`Fireweed` whose queue and maintenance operations are async methods (see the
Snorri acceptance signatures and the method inventory below). There is no
second public handle for "native async" versus "bridged" execution; product
work removes internal bridges without splitting the type graph.

The crate root requires downstream users to name only `Fireweed` and public
Fireweed DTOs. It must not require `LibBackend`, `EmbeddedHandle`, a concrete
backend, a composition-specific wrapper, or an internal workspace crate.
Generic facade forms, retired root/configuration types, raw backend
constructors, and the generic raw constructor MUST be unavailable to an
ordinary external crate when v0.21 ships. First-party tests use a crate-private
construction seam.

Storage authority, projection implementation, response-barrier choice, and
coordination topology are construction inputs only. `Fireweed` exposes no
post-construction backend/projection identity, discriminator, downcast, or
configuration getter. Queue-scoped capability values describe execution
characteristics of functionality that remains available on every supported
composition; callers use `projection_control()` only when maintenance
authority exists.

### Product execution model (native async)

The product execution **end-state** is **native async composition**: storage
and engine paths await drivers under ADR-015 / ADR-017 so the public
`Fireweed` methods compose on the host runtime without a process-wide
blocking bridge as architecture.

| Concern | Normative position |
| --- | --- |
| Public root | One concrete `Fireweed` with async methods |
| Product composition | Native-async / async-only composition (v0.24 product paths) |
| `BlockingLibBackend` | Residual facade bridge only — **not** the product concurrency model or end-state architecture |
| Process-wide blocking worker pool | **Not** the product concurrency model; may exist only as a temporary offload for non-native adapters |

**v0.24 reality vs end-state.** As of v0.24, product **composition** is
async-only (sync dual-stack product backends removed; log × projection cells
assemble on async products). The public facade **still bridges today** where
constructors wrap composed backends through `BlockingLibBackend` (or
equivalent process-wide blocking dispatch) so inherent async methods can call
legacy / not-yet-runtime-safe seams. That bridge is transitional. Residual
work is **removing the facade blocking bridge** after adapters are
runtime-safe under ADR-015 (no reactor stall; whole-transaction offload only
where the store is inherently blocking) and ADR-017 (owned-task dispatch,
queue-local gates). Embedders MUST NOT treat `BlockingLibBackend` as a
supported public type or as the long-term execution architecture.

This contract does **not** re-export `fireweed-engine` async modules as the
embedder surface. Engine composition types remain internal implementation
detail behind `Fireweed` / `StorageConfig` construction.

### Concurrency semantics

Correctness and progress requirements for the facade execution path:

1. **Per-queue serialization (required for correctness).** Mutations that
   share a queue remain ordered through a queue-local gate (or equivalent)
   across validation, planning, commit, projection visibility, and
   replay-outcome recording. Concurrent calls on the same queue must not
   interleave claim planning or commit application in ways that violate
   API-001 atomicity, fencing, or idempotency. See ADR-017.

2. **Cross-queue progress (required).** Unrelated queues MUST be able to
   make progress while one queue is busy or waiting on I/O. The product
   concurrency model is **not** a process-wide blocking worker pool that
   serializes all facade operations, and it is **not** a process-global
   storage lock held across awaited I/O (ADR-015, ADR-017).

3. **Adapter-local offload vs product model.** A blocking store adapter may
   still execute one complete begin/apply/commit unit on a bounded blocking
   executor or storage actor *below* the async port (ADR-015). That offload
   is an adapter concern for stores without native async drivers. It does
   not redefine the public concurrency model as process-wide blocking
   dispatch, and it is not a substitute for native-async composition where
   drivers can be awaited directly.

4. **Bridge removal criterion.** The facade may drop `BlockingLibBackend`
   (and any process-wide pool used only to paper over sync seams) once every
   supported composition path is runtime-safe: native-async await inside
   owned commit work, or whole-transaction offload confined to the adapter,
   with per-queue serialization preserved and cross-queue progress
   demonstrated under the existing conformance / heartbeat gates.
   Per-cell residual inventory (which matrix cells still wrap
   `BlockingLibBackend`, whether inner product poll would block a Tokio
   worker, and exit criteria): 
   [async-runtime-blocking-matrix-inventory](../../04-build/async-runtime-blocking-matrix-inventory.md).

### Construction

#### Normative full-matrix surface: `StorageConfig`

The product storage model is the orthogonal product of log and projection
stores (see `orthogonal-storage-matrix-brief`). **Typed `StorageConfig` is the
normative facade construction surface** for the full 5×3 matrix. Embedders
assemble log × projection (+ optional control-plane, segment, recovery, and
authority fields) and open one concrete `Fireweed`.

```rust
/// Normative composition root for log × projection (+ related axes).
pub struct StorageConfig {
    pub log: LogConfig,
    pub projection: ProjectionStoreConfig,
    pub control_plane: Option<ControlPlaneConfig>,
    /// Required for object-log peers; invalid for non-object logs.
    pub authority: Option<ObjectLogAuthority>,
    pub response_barrier: ResponseBarrier,
    pub segments: SegmentConfig,
    pub namespace: String,
    pub recovery: RecoveryPolicy,
}

/// Public log axis (five first-class values).
pub enum LogConfig {
    Memory,
    Sqlite { path: PathBuf },
    Postgres {
        url: ConfigSecret,
        schema: Option<String>,
        mode: PostgresMode,
        node_id: Option<u8>,
        coordination: Option<PostgresCoordinationConfig>,
    },
    /// Local directory tree / NAS path object log (same protocol as S3).
    Filesystem { root: PathBuf },
    /// S3-compatible object log.
    S3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: ConfigSecret,
        secret_access_key: ConfigSecret,
        allow_insecure_http: bool,
    },
}

/// Public projection axis (three first-class values).
pub enum ProjectionStoreConfig {
    Memory,
    Sqlite {
        path: PathBuf,
        /// SQLite apply batching; independent of response-barrier policy.
        sqlite_projection_deferred_flush_chunk: Option<NonZeroUsize>,
    },
    Postgres { url: ConfigSecret },
}

/// Provider-neutral bounds for returning before the selected projection has
/// applied through the committed log position.
pub struct AsyncProjectionSpec {
    pub apply_lag_max_commands: u64,
    pub apply_debt_max_bytes: u64,
    pub apply_queue_depth_max: usize,
    pub oldest_unapplied_max_ms: u64,
    pub apply_poison_retry_threshold: u32,
}

pub enum ResponseBarrier {
    Strict,
    AsyncProjection(AsyncProjectionSpec),
}

pub fn open(
    config: StorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;

pub async fn open_async(
    config: StorageConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
```

Every cell of the matrix is a valid selection:

| Log \ Projection | `memory` | `sqlite` | `postgres` |
| --- | --- | --- | --- |
| `memory` | yes | yes | yes |
| `sqlite` | yes | yes | yes |
| `postgres` | yes | yes | yes |
| `filesystem` | yes | yes | yes |
| `s3` | yes | yes | yes |

`Filesystem` and `S3` are first-class log backends that share the object-log
protocol (segments, manifest, conditional write / authority, retention). They
are not test-only substitutes for each other. There is **no** public profile
SKU product type; pair strings may appear only in test IDs and historical
evidence filenames.

##### Durability classes

Semantics across matrix cells differ by **durability class**, not by a second
architecture:

| Class | Logs | Client contract (summary) |
| --- | --- | --- |
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Success ⇒ durable on the log and visible in the serving projection; recovery via high-water + tail replay when the log remains |
| **B — Memory log** | `memory` | Success ⇒ visible in the projection; durable **iff** the projection is durable (`sqlite` / `postgres`); after process death only the projection remains—no log rebuild, branch, or read-as-of from the log |

Class B is a weaker persistence envelope, not “no LogStore.” Callers that need
Class A guarantees MUST NOT select `LogConfig::Memory`.

##### Environment variables

Environment variables are **not** the facade construction surface. The library
API accepts typed `StorageConfig` (or the convenience constructors below that
map onto it). Container / process env injection, if used by a server or Helm
chart, is an **adapter** that must deserialize into `StorageConfig` before
composition. This contract does not define `FIREWEED_*` names as the embedder
API.

`StorageConfig` fields are construction inputs only. The resulting `Fireweed`
does not expose the selected log, projection, authority, barrier, or backend
objects.

Validation is pure, fail-closed, and complete before any network connection,
file open, directory creation, schema migration, or other storage I/O. When
more than one defect exists, `StorageConfig` validation reports the first one
in this fixed precedence:

1. endpoint and location syntax;
2. response-barrier shape and positive `AsyncProjectionSpec` bounds;
3. tuple coherence (field applicability and canonical log/projection/control
   selectors);
4. compiled feature availability; then
5. durability and provider-capability requirements.

`LogConfig::Filesystem` and `LogConfig::S3` require
`ObjectLogAuthority::NativeConditionalWrite`. The configured log provider MUST
actually supply atomic conditional create/update publication; a provider that
cannot do so is rejected. PostgreSQL is not an object-log manifest-publication
fallback, and projection selection never supplies publication authority.
`authority` on a non-object log is a tuple-coherence error rather than an
ignored field.

`Strict` and `AsyncProjection(AsyncProjectionSpec)` are provider-neutral
response policies, not projection variants or public product profiles. All
five limits in `AsyncProjectionSpec` MUST be positive. The separate
`sqlite_projection_deferred_flush_chunk` is an optional positive SQLite apply
batching capability; it is valid with either `Strict` or `AsyncProjection` and
is a tuple-coherence error for non-SQLite projections. A barrier/cell whose
durability or runtime capabilities cannot satisfy the external transaction
contract is rejected at the final durability/capability step; constructors do
not silently substitute a projection, authority, or barrier.

Unsupported or mismatched configurations return a structured
`EngineError::Invalid` or `EngineError::Unavailable` before storage I/O; no
partially opened `Fireweed` escapes. These are startup-only outcomes. Although
the engine's exhaustive `CommitRejection` conversion may mirror configuration
error variants such as `ChangeRecordsRequireDurableLog`, validation proves that
they cannot originate after a `Fireweed` is constructed and MUST NOT escape a
queue method's commit path.

`ConfigSecret::new`, `SegmentConfig::new`, and `RecoveryPolicy::default`
preserve the corresponding current validation behavior. `ConfigSecret` exposes
no plaintext accessor and its `Debug` implementation always redacts the
contained value.

#### Convenience constructors (map onto `StorageConfig`)

The release-critical convenience constructors preserve clock injection and
return the same concrete type. Each is a **subset constructor** over
`StorageConfig` for a common cell or object-log pairing; they are not a
separate product model.

```rust
/// Class B: memory log × memory projection.
pub fn open_memory(clock: Arc<dyn Clock>) -> Fireweed;
/// Class A: sqlite log × sqlite projection (shared path).
pub fn open_sqlite(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
pub fn open_sqlite_relational(path: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
/// Class A: filesystem object log with a default local composition.
pub fn open_objectlog(root: impl Into<PathBuf>, clock: Arc<dyn Clock>)
    -> EngineResult<Fireweed>;
/// Class A: postgres log × postgres projection (common defaults).
pub fn open_postgres(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
pub async fn open_postgres_async(url: &str, clock: Arc<dyn Clock>) -> EngineResult<Fireweed>;
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
pub async fn open_postgres_runtime_async(
    config: PostgresRuntimeConfig,
    clock: Arc<dyn Clock>,
) -> EngineResult<Fireweed>;
/// Object-log conveniences: map `ObjectLogRuntimeConfig` → `StorageConfig`
/// (`Local` → `LogConfig::Filesystem`, `S3Compatible` → `LogConfig::S3`).
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
identity, and coordination topology at the composition root. Prefer
`LogConfig::Postgres { … }` on `StorageConfig` for new code; the types below
remain the convenience shape used by `open_postgres_runtime*`:

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

`ObjectLogRuntimeConfig` remains the structured convenience for object-log
compositions that already name storage, authority, projection, barrier,
segments, namespace, and recovery together. It MUST be describable as a
mapping into `StorageConfig` (filesystem/S3 log + projection store + shared
fields). New full-matrix work SHOULD use `StorageConfig` directly.

```rust
pub struct ObjectLogRuntimeConfig {
    pub object_log: ObjectLogStorage,
    pub authority: ObjectLogAuthority,
    pub projection: ProjectionConfig,
    pub response_barrier: ResponseBarrier,
    pub segments: SegmentConfig,
    pub namespace: String,
    pub recovery: RecoveryPolicy,
}

pub enum ObjectLogAuthority {
    NativeConditionalWrite,
}

/// Convenience object-log storage; maps to `LogConfig::Filesystem` / `S3`.
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

/// Convenience projection subset used by object-log constructors.
/// Full matrix projection selection is `ProjectionStoreConfig` (includes
/// `Memory`).
pub enum ProjectionConfig {
    Sqlite { path: PathBuf },
    Postgres { url: ConfigSecret },
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConfigSecret(/* private String */);
pub struct SegmentConfig { pub target_bytes: usize, pub max_latency_ms: u64 }
pub enum RecoveryAction { FailClosed, RebuildProjection }
pub struct RecoveryPolicy {
    pub incompatible_projection: RecoveryAction,
    pub verify_checksums: bool,
    pub max_tail_commands: u64,
}
```

Object storage, publication authority, and projection storage are independent
construction axes. Local filesystem object logs require
`NativeConditionalWrite`. S3-compatible stores require the same selector and
are supported only when the configured provider implements atomic conditional
publication. No PostgreSQL authority selector or fallback is public. The
selected authority remains private after construction.

`ObjectLogRuntimeConfig::validate` preserves the corresponding current
validation behavior. `open_objectlog_sqlite` requires
`ProjectionConfig::Sqlite`; the Postgres constructors require
`ProjectionConfig::Postgres`; a mismatched variant returns
`EngineError::Unavailable` before opening either store. Convenience
constructors use the same validation precedence and barrier semantics as
`StorageConfig`; they do not impose a provider-specific `Strict` rule or bypass
tuple, feature, and durability checks.

`EmbeddedSecret`, `EmbeddedObjectLogConfig`, `EmbeddedProjectionConfig`,
`EmbeddedResponseBarrier`, `EmbeddedSegmentConfig`,
`EmbeddedRecoveryAction`, `EmbeddedRecoveryPolicy`,
`EmbeddedDurabilityConfig`, and the `open_embedded*` functions are not supported
facade names. ADR-023 requires a hard pre-release cutover with no deprecated
alias layer. These Rust names MUST be replaced for v0.21. Every constructor
returns `Fireweed`; no composition-specific wrapper type is returned.

### Queue operations

The following operation families are inherent `Fireweed` methods and preserve
their input, output, error, and async behavior. Every supported constructor
MUST implement every method in this inventory. Construction-time storage
composition MUST NOT turn an inherent method into
`EngineError::Unavailable`; a composition that cannot implement the complete
surface is unsupported and MUST NOT be exposed by API-005. The only optional
handle is `projection_control()`, because it represents a concern absent from
compositions without a disposable projection. Capability inspection may
describe execution characteristics, but MUST NOT excuse missing core
functionality.

| Family | Methods |
| --- | --- |
| Queue and ownership | `ownership`, `renew_owned`, `create_queue`, `queue_definition`, `ensure_queue` |
| Append and replace | `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id`, `upsert` |
| Claim | `claim`, `claim_with`, `claim_response_with`, `claim_at`, `claim_response_at`, `claim_across_queues`, `claim_by_query`, `claim_by_query_at`, `claim_by_item_ids` (API-001 `BatchClaimByItemIds`; external-trigger / pre-resolved id set; ordinary leases) |
| Finalize and commit | `ack`, `complete`, `nack`, `retry`, `release`, `nack_retry_after`, `retry_after`, `commit`, `commit_multi_claim`, `commit_capabilities`, `explain_commit`, `side_record`, `fail` |
| Read and discovery | `peek`, `current_position`, `discover_active_scopes`, `discover_active_scopes_stamped`, `discover`, `live_item`, `live_items`, `query_index_unique`, `query_index`, `query_index_unique_typed`, `query_index_typed`, `claimed` |
| Metrics and projection query | `metrics`, `metrics_by_query`, `hot_projection_capabilities`, `range_scan`, `grouped_aggregate`, `declared_bucket_segment` |
| Mutation and maintenance | `renew`, `reassign`, `update_fields`, `batch_update`, `mutate_items`, `update`, `set_gates`, `reclaim_expired`, `reclaim_expired_at`, `rearm`, `rearm_at`, `rearm_after`, `purge`, `bounded_mutation` |

Per-constructor parity is a release invariant. One shared conformance suite
MUST invoke every method family against every supported constructor, including
`batch_update` and `live_items` in the same lifecycle. A representative call on
one backend, compile-only forwarding, or an expected `Unavailable` result is
not parity evidence.

Iterator-taking convenience methods may collect into `Vec<ItemId>` at the
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

### Historical query component

The backend-neutral historical query component is a separately reviewed owned
contract. It is intentionally outside the v0.21 supported facade closure until
its request, response, capability, and retention semantics are governed.

Its availability is runtime- and queue-scoped. A runtime may answer historical
queries for some queues and decline others, and queue-scoped capability values
remain authoritative for that decision. The caller must consult those
capabilities for the target queue before issuing a historical request.

Ownership stays with the runtime that already owns the queue state and any
retained snapshots or segments needed to answer the query. Callers do not take
detached ownership of backend objects, and the component does not expose a
backend-associated callback surface.

If the runtime cannot satisfy the requested queue/position, or the retained
state needed to reconstruct the answer has expired or been discarded, the
component fails closed with a structured unavailable result and returns no
partial data.

`read_as_of<T, F>` is retired from the supported facade. Its replacement is
the historical query component above, and no implementation or conformance
bead may be derived from it until the dedicated history contract is reviewed.

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
`ControlPlaneConfig`, `LogConfig`, `ObjectLogAuthority`,
`ObjectLogRuntimeConfig`, `ObjectLogStorage`, `OwnerId`, `ProjectionConfig`,
`ProjectionStoreConfig`, `ProjectionControl`, `ProjectionControlCapabilities`,
`ProjectionRebuild`, `ProjectionVerification`, `RecoveryAction`,
`RecoveryPolicy`, `ResponseBarrier`, `SegmentConfig`, and `StorageConfig`. For
the Snorri migration it also includes all current facade DTOs plus
`CompoundIndexDef`, `CompoundIndexField`, `IndexDeclaration`, `IndexType`,
`QueueIndex`, and `WorkerId`. A compile fixture depending only on `fireweed` is
the enforcement mechanism.

The Snorri named-type closure is:

`AggregateGroup`, `BatchUpdateEntry`, `BatchUpdateItemRef`,
`BatchUpdateOutcome`, `BatchUpdateRequest`, `BatchUpdateResponse`,
`BatchUpdateValue`, `AddressedMutation`, `BucketRule`, `Bytes`, `ClaimAt`, `ClaimByQueryAt`,
`ClaimByQueryRequest`, `ClaimCompatibility`, `ClaimRef`, `Claimed`,
`ClaimedItem`, `ClientItemKey`, `Clock`, `CommitCapabilities`, `CommitEntry`,
`CommitRecovery`, `CommitRequest`, `CompoundIndexDef`, `CompoundIndexField`,
`CreateQueueOutcome`,
`DeclaredBucketSegmentRequest`, `DeclaredBucketSegmentResponse`,
`EligibilityPolicy`, `EngineError`, `EngineResult`, `EntryOutcome`, `FilterOp`,
`FinalizeKind`, `GroupByField`, `GroupedAggregateRequest`,
`GroupedAggregateResponse`, `IndexDeclaration`, `IndexHit`, `IndexType`,
`EntityEdit`, `EntityEditOperation`, `EntityPredicateValue`, `GateChange`,
`GateKeyDelta`, `InstanceFence`, `ItemId`, `ItemMutationOperation`,
`ItemMutationOutcome`, `ItemMutationPrecondition`, `ItemMutationRequest`,
`ItemMutationResponse`, `ItemMutationResult`, `ItemMutationReturning`,
`ItemMutationSelectorAggregate`, `ItemMutationSnapshot`, `ItemMutationSummary`,
`ItemPatch`, `ItemPredicate`, `ItemSelector`, `ItemSelectorScope`, `LeaseGuard`,
`LeaseToken`, `LifecyclePatch`, `LiveItemView`, `Metadata`,
`MetadataValue`, `MetricsByQueryRequest`, `Nack`, `NewItem`, `OrderField`,
`OrderingMode`, `PriorityModel`, `PriorityValue`, `QueryCapabilityFlags`,
`QueryCursor`, `QueryFilter`, `QueueDefinition`, `QueueId`, `QueueIndex`,
`QueueKey`, `QueueMetrics`, `RangeScanRequest`, `RangeScanResponse`,
`RecurrencePolicy`, `RequestId`, `RetryPolicy`, `ScheduleUpdate`, `SideRecord`,
`SortDirection`, `TenantId`, `TimeBucket`, `TypedValue`, `UpsertOutcome`,
`SelectedMutation`, `TimestampComparison`, `UtcTimestamp`, and `WorkerId`.

### Snorri acceptance slice

Before the release candidate is usable, Snorri must compile against one
non-generic `Fireweed` using these operations:

`create_queue`, `push`, `push_with_request_id`,
`push_batch_with_request_id`, `upsert`, `claim_with`, `claim_by_query`,
`claim_by_query_at`, `ack`, `nack`, `commit`, `commit_capabilities`,
`explain_commit`, `side_record`, `live_item`, `query_index_unique_typed`,
`batch_update`, `mutate_items`, `update`, `purge`, `claimed`, `metrics`, `metrics_by_query`,
`hot_projection_capabilities`, `range_scan`, `grouped_aggregate`, and
`declared_bucket_segment`.

Object-log compositions (filesystem / s3 log with a disposable projection)
additionally consume `projection_control` and its four operations. No Snorri
public or private type may retain a `LibBackend` bound.

The Snorri-critical method signatures MUST remain type-equivalent in parameter
ownership and result shape to the current public facade contract. Through v0.21.0
the request-id push path returned only item ids; from v0.22.0 it also reports
per-request replay-vs-fresh disposition (downstream snorri create/enqueue
counters). In particular:

```rust
pub async fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome>;
pub async fn push(&self, queue: &QueueKey, item: NewItem) -> EngineResult<ItemId>;
pub async fn push_with_request_id(&self, queue: &QueueKey, request_id: RequestId, item: NewItem) -> EngineResult<(ItemId, PushDisposition)>;
pub async fn push_batch_with_request_id(&self, queue: &QueueKey, request_id: RequestId, items: Vec<NewItem>) -> EngineResult<PushBatchOutcome>;
pub async fn upsert(&self, queue: &QueueKey, key: ClientItemKey, item: NewItem) -> EngineResult<UpsertOutcome>;
pub async fn claim_with(&self, queue: &QueueKey, max: usize, lease_ms: u64, compatibility: ClaimCompatibility) -> EngineResult<Vec<ClaimedItem>>;
pub async fn claim_by_query(&self, queue: &QueueKey, request: ClaimByQueryRequest) -> EngineResult<Claimed>;
pub async fn claim_by_query_at(&self, queue: &QueueKey, request: ClaimByQueryRequest, at: ClaimByQueryAt) -> EngineResult<Claimed>;
/// API-001 `BatchClaimByItemIds`: lease exactly the caller-supplied item ids
/// (partial per-id outcomes). Resulting leases are ordinary claim leases
/// (inspect / lease timeout+reclaim / API-002 force).
pub async fn claim_by_item_ids(&self, queue: &QueueKey, request: ClaimByItemIdsRequest) -> EngineResult<ClaimByItemIdsResponse>;
pub async fn ack(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>) -> EngineResult<()>;
pub async fn nack(&self, queue: &QueueKey, ids: impl IntoIterator<Item = ItemId>, how: Nack) -> EngineResult<()>;
pub async fn commit(&self, queue: &QueueKey, request: CommitRequest) -> EngineResult<Vec<EntryOutcome>>;
pub fn commit_capabilities(&self, queue: &QueueKey) -> EngineResult<CommitCapabilities>;
pub async fn explain_commit(&self, queue: &QueueKey, request_id: RequestId) -> EngineResult<Option<CommitRecovery>>;
pub async fn side_record(&self, queue: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>>;
pub async fn live_item(&self, queue: &QueueKey, key: ClientItemKey) -> EngineResult<Option<LiveItemView>>;
pub async fn query_index_unique_typed(&self, queue: &QueueKey, index: &str, values: &[serde_json::Value]) -> EngineResult<Option<IndexHit>>;
pub async fn batch_update(&self, queue: &QueueKey, request: BatchUpdateRequest) -> EngineResult<BatchUpdateResponse>;
pub async fn mutate_items(&self, queue: &QueueKey, request: ItemMutationRequest) -> EngineResult<ItemMutationResponse>;
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
  names, signatures, construction, export closure, and the product execution
  model for the public facade (native-async end-state; concurrency semantics).
- ADR-015 and ADR-017 govern async storage boundaries, per-queue gates, and
  owned-task dispatch. API-005 binds those decisions to the single public
  `Fireweed` type and forbids treating `BlockingLibBackend` as the product
  end-state architecture.
- `orthogonal-storage-matrix-brief` governs the public log × projection matrix
  and durability classes; API-005 is the Rust binding of that construction
  model via `StorageConfig`.
- Returning `Fireweed` is a deliberate source break from inferred
  `Fireweed<impl LibBackend>` return types. Migration guidance MUST show removal
  of downstream backend parameters.
- ADR-023 forbids package aliases and retired Rust facade types; API-005 exposes
  only the concrete `Fireweed` surface listed here.
- Adding a `Fireweed` method or capability bit is compatible. Removing or
  changing a supported method or DTO requires a breaking pre-1.0 minor and
  migration guidance.
- Convenience constructors and `ObjectLogRuntimeConfig` remain compatible
  subset surfaces; full-matrix cells that those conveniences do not cover are
  opened through `StorageConfig` / `open` / `open_async`.
- Removing the residual facade blocking bridge is compatible for embedders that
  already use only public `Fireweed` async methods; it must not introduce a
  second public root type.

## Error semantics

| Condition | Error / outcome | Retry | Recovery expectation |
| --- | --- | --- | --- |
| Supported composition lacks a core operation | Construction/release validation failure | No | Do not expose or release the incomplete composition |
| Invalid endpoint, barrier, tuple, feature, or durability combination | Structured construction error before I/O | No until corrected | Fix `StorageConfig`; no partial resources or runtime handle exist |
| History/change-record request uses Class B memory log | `EngineError::ChangeRecordsRequireDurableLog` | No | Select a Class A log at construction; no history is fabricated |
| Core operation is transiently unavailable at runtime | Existing structured `EngineError::Unavailable` | Per API-001 | Retry without selecting a different storage implementation |
| Projection maintenance is not owned | `projection_control()` returns `None` | No | Queue operations and hot-query capability checks remain independent |
| Projection is offline or maintenance fails | Structured `EngineError` from the control operation | Per the existing recovery contract | Re-inspect or rebuild through the same borrowed control |
| Synchronous object-log/Postgres open occurs inside Tokio | `EngineError::Invalid` directing the caller to `open_objectlog_postgres_async` | Yes | No partially opened `Fireweed` escapes |

## Examples

```rust
use std::sync::Arc;
use fireweed::{EngineError, Fireweed, QueueKey};

async fn activate(fireweed: Arc<Fireweed>, queue: QueueKey) -> Result<(), EngineError> {
    let _execution_characteristics = fireweed.commit_capabilities(&queue)?;
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
- [ ] Product execution end-state is native async composition; one public
      `Fireweed` with async methods; `BlockingLibBackend` is not the end-state
      architecture.
- [ ] Concurrency semantics document per-queue serialization vs cross-queue
      progress; process-wide blocking worker pool is not the product model.
- [ ] v0.24 async-only composition vs residual facade bridge is stated; residual
      work is bridge removal after adapters are runtime-safe.
- [ ] Non-goals exclude dual public types and re-exporting `fireweed-engine`
      async modules as the embedder surface.
- [ ] `StorageConfig` / `LogConfig` / `ProjectionStoreConfig` document the full
      5×3 matrix; filesystem and s3 are first-class logs; no profile SKU model.
- [ ] Durability Class A vs Class B is documented (memory log = Class B).
- [ ] Environment variables are not the facade construction surface.
- [ ] `AsyncProjectionSpec` is provider-neutral; SQLite deferred-flush chunking
      is a separate projection option valid under both barriers.
- [ ] Configuration validation follows endpoint → barrier → tuple coherence →
      feature → durability precedence and performs no storage I/O.
- [ ] Startup-only configuration errors cannot escape the commit path even when
      exhaustively represented by `CommitRejection`.
- [ ] `ObjectLogRuntimeConfig` and `open_*` conveniences map to / are described
      as conveniences over `StorageConfig`.
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
