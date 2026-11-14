---
ddx:
  id: api-queue-implementation-interface
  depends_on:
    - api-native-client-interface
    - adr-queue-interface-implementation-boundary
    - api-hot-projection-query-surface
    - api-operator-repair-contract
    - api-workload-integration-profiles
  status: draft
---

# API-006: Queue implementation interface

**Type**: Rust trait contract (implementation boundary)
**Version**: v0 (draft)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, API-003, API-004, API-005, ADR-025

## Purpose

This contract defines the Rust traits that every Fireweed implementation
implements, and the rules that make implementations interchangeable. API-001
defines what each operation means. This contract defines how those operations
are typed, which guarantees an implementation may vary and must declare, how
optional features and operation families are exposed, how crates may depend
on each other, and how an implementation proves it is correct.

A caller programs against these traits, directly or through `Fireweed`
(API-005). An implementation author implements them and runs the conformance
suite. Neither needs to know how any other implementation stores data.

## Scope and boundaries

- In scope: the `fireweed-interface` crate; the core trait `QueueProvider`;
  extension traits; `Guarantees`, `Capabilities` and `Clock`; the error type;
  dependency rules; conformance obligations; the mapping from the current
  `Fireweed` methods.
- Out of scope: operation semantics (API-001, API-002, API-004 own them);
  wire bindings (TD-006); each implementation's configuration, storage
  layout, recovery and maintenance API.
- Owning crate: `fireweed-interface` (working name).
- Non-goals: a storage-port SPI; exposing log, command, projection, snapshot,
  epoch or ownership types; a second public facade type.

## Terms

| Term | Meaning |
|------|---------|
| Implementation | A type implementing `QueueProvider`, shipped in its own crate. |
| Returns | The operation's future resolved to `Ok`. |
| Safety effect | A mutation effect that removes an item from claim eligibility or changes who may act on it: a new lease, a renew or reassign, a finalize, a purge, a gate set to `blocked`, an update that adds a blocking gate key or a future `not_before`, a queue pause. |
| New-work effect | A mutation effect that makes an item claimable: a push, a release, a retry or re-arm reaching its `not_before`, an update that removes a blocking condition, a gate set to `open`. |
| Reference | The PostgreSQL implementation (ADR-025 §8). |

## Normative surface

### Crate and dependency rules

1. `fireweed-interface` depends on `fireweed-core` and general-purpose crates
   only. It depends on no other `fireweed-*` crate.
2. No public item in `fireweed-interface` names a log, command, command
   position, projection, snapshot, epoch, owner session or storage cell.
3. An implementation crate depends on `fireweed-interface`. It MUST NOT
   depend on another implementation crate. It MAY depend on shared internal
   libraries (for example the current log and projection engine).
4. `fireweed` (the facade) and `fireweed-resp` depend on `fireweed-interface`.
   They MUST NOT depend on an implementation crate except as a
   dev-dependency.
5. `fireweed-conformance` depends on `fireweed-interface` only.

A workspace test enforces rules 1, 3, 4 and 5 from `cargo metadata`.

### Common types

Request and response types live in `fireweed-interface`. Field meanings are
API-001's. Types already in `fireweed-core` (`TenantId`, `QueueId`, `ItemId`,
`ClientItemKey`, `RequestId`, `LeaseToken`, `WorkerId`, `PriorityValue`,
`UtcTimestamp`, `Metadata`, `QueueDefinition`, `ItemResult`,
`ItemResultStatus`) are re-exported unchanged. Types now in `fireweed-engine`
or `fireweed` that the traits need (`QueueKey`, `ClaimedItem`,
`LiveItemView`, `QueueMetrics`, `BatchUpdateRequest`, `ItemMutationRequest`,
`CommitRequest` and their parts) move into `fireweed-interface`.

Every request carries its target. Queue-scoped requests carry
`queue: QueueKey`. Mutating requests carry `request_id: RequestId`. Claim
requests also carry `worker_id: WorkerId`.

```rust
pub type QueueResult<T> = Result<T, QueueError>;
pub type QueueFuture<'a, T> =
    Pin<Box<dyn Future<Output = QueueResult<T>> + Send + 'a>>;

pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}
```

Every implementation MUST take an `Arc<dyn Clock>` at construction and MUST
use it for every time-dependent decision: lease expiry, `not_before`,
retention windows, cohort expiry, eligible age. No operation takes a
timestamp to override the clock, except where API-001 defines one
(`MutateItems.evaluated_at`).

### Core trait

```rust
pub trait QueueProvider: Send + Sync {
    fn guarantees(&self) -> Guarantees;
    fn capabilities(&self) -> Capabilities;

    // Queue definition (API-001 Queue Definition)
    fn create_queue(&self, request: CreateQueueRequest) -> QueueFuture<'_, CreateQueueResponse>;
    fn get_queue(&self, queue: QueueKey) -> QueueFuture<'_, Option<QueueDefinition>>;

    // Writes
    fn push(&self, request: PushRequest) -> QueueFuture<'_, PushResponse>;                 // BatchPush
    fn update(&self, request: UpdateRequest) -> QueueFuture<'_, UpdateResponse>;           // BatchUpdate
    fn set_gates(&self, request: SetGatesRequest) -> QueueFuture<'_, SetGatesResponse>;    // SetGates
    fn purge(&self, request: PurgeRequest) -> QueueFuture<'_, PurgeResponse>;              // PurgeItems

    // Claim and lease
    fn claim(&self, request: ClaimRequest) -> QueueFuture<'_, ClaimResponse>;              // BatchClaim
    fn claim_by_item_ids(&self, request: ClaimByItemIdsRequest)
        -> QueueFuture<'_, ClaimByItemIdsResponse>;                                       // BatchClaimByItemIds
    fn renew(&self, request: RenewRequest) -> QueueFuture<'_, RenewResponse>;              // BatchRenewLeases
    fn reassign(&self, request: ReassignRequest) -> QueueFuture<'_, ReassignResponse>;     // ReassignLeases
    fn finalize(&self, request: FinalizeRequest) -> QueueFuture<'_, FinalizeResponse>;     // BatchFinalize

    // Reads
    fn get_live_items(&self, request: GetLiveItemsRequest)
        -> QueueFuture<'_, GetLiveItemsResponse>;                                         // BatchGetLiveItems
    fn metrics(&self, queue: QueueKey) -> QueueFuture<'_, QueueMetrics>;                  // GetQueueMetrics
    fn discover_active_scopes(&self, request: DiscoverRequest)
        -> QueueFuture<'_, DiscoverResponse>;                                             // DiscoverActiveScopes

    // Extension families. Default: not implemented.
    fn upsert(&self) -> Option<&dyn Upsert> { None }
    fn item_mutation(&self) -> Option<&dyn ItemMutation> { None }
    fn claimed_work_commit(&self) -> Option<&dyn ClaimedWorkCommit> { None }
    fn index_query(&self) -> Option<&dyn IndexQuery> { None }
    fn projection_query(&self) -> Option<&dyn ProjectionQuery> { None }
    fn multi_queue_claim(&self) -> Option<&dyn MultiQueueClaim> { None }
    fn operator_repair(&self) -> Option<&dyn OperatorRepair> { None }
}
```

The trait is dyn-compatible. `Arc<dyn QueueProvider>` is the form the facade
and RESP adapter hold. The boxed-future signature is the contract. An
implementation may write `async` bodies behind it.

Every implementation implements every core method for every queue it
accepts. A core method never returns `FeatureUnsupported` or `Unavailable`
because of what the implementation lacks.

**Request and response shapes.** Each core request is the API-001 operation
of the same name, as a struct: the envelope fields (`queue`, `request_id`,
`worker_id` where API-001 requires them) plus the operation's batch.
Responses hold per-item results in request order (API-001 "Batch-Centric
Operation Shape"). In addition:

- `get_live_items` addresses each entry by `item_id` or `client_item_key`.
  It replaces the current `live_item`, `live_items` and `claimed` reads.
- `ReassignLeases` moves an active lease to another worker, issuing a new
  token and charging one attempt (API-001 Common Types `attempt_count`). It
  is the operation the RESP `XCLAIM` cross-consumer path needs. API-001 gains
  a section for it when ADR-025 is accepted.
- `DiscoverRequest` carries an optional `authorize` predicate
  (`Arc<dyn Fn(&QueueKey) -> bool + Send + Sync>`). Authentication is outside
  this contract. A wire adapter supplies the predicate, and the
  implementation excludes queues it rejects (API-001 Authorization Rules).

### Guarantees

```rust
pub struct Guarantees {
    pub durability: Durability,
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}

pub enum Durability { Process, Durable }
pub enum Visibility { Immediate, Bounded { max_ms: u64 } }
pub enum Sharing { SingleProcess, MultiProcess }
```

`guarantees()` returns the same value for the life of the implementation.

| Guarantee | Rule |
|-----------|------|
| `Durability::Durable` | Every mutation that returned survives a crash of the process and a reopen of the same storage. Idempotent replay of its `request_id` after reopen returns the recorded result. |
| `Durability::Process` | Mutations survive only while the process lives. Reopen yields empty storage. |
| `Visibility::Immediate` | A claim that starts after a mutation returned observes its new-work effects. |
| `Visibility::Bounded { max_ms }` | A claim that starts more than `max_ms` after a mutation returned observes its new-work effects. Before that, an empty or smaller claim result is a normal poll, not an error. |
| `Sharing::MultiProcess` | Several instances opened on the same storage, in one or many processes, together satisfy this contract as one implementation would. |
| `Sharing::SingleProcess` | Opening a second instance on storage already open elsewhere MUST fail at construction. It MUST NOT corrupt state or split leases. |

These rules hold for every implementation, whatever it declares:

1. **Safety effects are immediate.** Once a mutation returns, no claim that
   starts afterwards returns an item that the mutation removed from
   eligibility. No two active leases exist on one item. A stale lease token
   is rejected.
2. **Reads see returned writes.** `get_live_items` and `get_queue` observe
   every mutation that returned before the read started. `metrics` follows
   API-001's accuracy rules, and `oldest_eligible_age_ms` is exact.
3. **Replay is exact.** A repeated `request_id` returns the recorded result
   whether or not the original's effects are visible yet.

### Capabilities

```rust
pub struct Capabilities {
    pub priority_kinds: BTreeSet<PriorityModelKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub features: BTreeSet<QueueFeature>,
    pub limits: ImplementationLimits,
}

pub enum QueueFeature {
    DynamicGates,
    MetadataBlockers,
    Recurrence,
    GroupBatching,
    Cohorts,
    TypedPayloadSchema,   // ADR-011 entity schema
    SecondaryIndexes,     // ADR-010 / ADR-011 index declarations
}

pub struct ImplementationLimits {
    pub max_push_batch_size: u32,
    pub max_claim_batch_size: u32,
    pub max_lease_duration_ms: u64,
}
```

Every implementation MUST support `PriorityModelKind::Timestamp`,
`PriorityModelKind::Int64` and `OrderingMode::Strict`.

`create_queue` MUST reject, before creating anything, a definition that
needs a priority kind, ordering mode or feature the implementation does not
list, with `QueueError::FeatureUnsupported`. A queue that was created
supports every operation API-001 defines for its definition.

`ImplementationLimits` are deployment caps in API-001's sense. A queue
definition may set lower per-queue limits.

### Extension families

An extension trait groups operations that only some consumers need. An
implementation that returns `Some` from an accessor implements every method
of that trait, for every queue whose definition the family applies to.

| Accessor | Trait | Operations | Semantics |
|----------|-------|------------|-----------|
| `upsert` | `Upsert` | `upsert` | Replace a live item by `client_item_key`, or push it if absent. To be specified in API-001. |
| `item_mutation` | `ItemMutation` | `mutate_items` | API-001 Atomic Item Mutation. |
| `claimed_work_commit` | `ClaimedWorkCommit` | `commit`, `commit_multi_claim`, `explain_commit`, `get_side_records` | Atomically validate one or more leases, finalize them, enqueue new items, write side records and advance an instance fence. Today specified only in Rust doc comments. It needs a written contract before it is normative. |
| `index_query` | `IndexQuery` | `query_index` (unique, multi and typed forms as request options) | ADR-010 and ADR-011 index lookups. |
| `projection_query` | `ProjectionQuery` | `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `bounded_mutation`, `claim_by_query`, `metrics_by_query` | API-004. Its capability flags move into `Capabilities`. |
| `multi_queue_claim` | `MultiQueueClaim` | `claim_across_queues` | Claim from several queues in one call. To be specified in API-001. |
| `operator_repair` | `OperatorRepair` | API-002 operations | API-002. |

Adding an extension trait, a `QueueFeature` value or a `Capabilities` field
is a compatible change. Adding a method to an existing trait is breaking.

**Profiles.** A consumer states which families it needs as a profile
(API-003). An implementation satisfies a profile when every named accessor
returns `Some`.

| Profile | Required families | Required features |
|---------|-------------------|-------------------|
| `core` | none | none |
| `resp-worker` | none | none |
| `snorri` (proposed) | `Upsert`, `ItemMutation`, `ClaimedWorkCommit`, `IndexQuery`, `ProjectionQuery` | `TypedPayloadSchema`, `SecondaryIndexes`, `DynamicGates` |

### Errors

```rust
pub enum QueueError {
    QueueNotFound,
    QueueForbidden,
    QueueDefinitionConflict,
    QueuePaused { drain_intake: bool },
    FeatureUnsupported(UnsupportedFeature),
    GatesNotEnabled,
    InvalidRequest(String),
    BatchTooLarge,
    RequestIdConflict,
    RequestExpired,
    CommitTimeout,
    RateLimited,
    Unavailable(String),
    Internal(String),
}
```

The variants are API-001's envelope errors, plus three:

- `FeatureUnsupported` is returned only by `create_queue`.
- `Unavailable` means a transient runtime failure (storage unreachable,
  shutdown in progress). It is always safe to retry with the same
  `request_id`. It never means the operation is missing.
- `Internal` means the implementation detected a broken invariant and failed
  closed. It is not retryable. It carries no recovery instruction for the
  caller.

Per-item outcomes use `ItemResultStatus`, extended with `GroupTooLarge`, and
the `BatchClaimByItemIds` reasons `NotEligible` and `Leased`. No error or
status names a storage mechanism.

### Maintenance, coordination and time

The following are not part of this contract. An implementation that needs
them offers them as methods on its own concrete type:

- projection verify, delete and rebuild;
- snapshot policy, snapshot creation, log compaction and rebuild statistics;
- ownership acquisition and renewal, epoch fencing;
- sweeping expired leases (expiry itself is a contract rule: an expired
  lease's item is eligible again under API-001 Eligibility Precedence);
- command positions and other progress markers.

A `Fireweed` built on an implementation does not expose these. An embedder
that needs them keeps an `Arc` of the concrete type as well.

## Conformance

### Black-box suite

`fireweed-conformance` exposes one entry point per trait:

```rust
pub trait Harness {
    type Provider: QueueProvider + 'static;
    /// A fresh, isolated implementation driven by `clock`.
    fn open(&self, clock: Arc<ManualClock>) -> BoxFuture<'_, Self::Provider>;
    /// Drop `provider` as a crash would, then open the same storage again.
    /// Required when `guarantees().durability == Durable`.
    fn crash_and_reopen(&self, provider: Self::Provider) -> BoxFuture<'_, Self::Provider>;
    /// Open a second instance on the same storage.
    /// Required when `guarantees().sharing == MultiProcess`.
    fn open_peer(&self, provider: &Self::Provider) -> BoxFuture<'_, Self::Provider>;
}

pub async fn run_core<H: Harness>(harness: H);
pub async fn run_extensions<H: Harness>(harness: H); // each family whose accessor returns Some
```

The suite uses only `fireweed-interface`. It reads `guarantees()` and
`capabilities()`, runs every scenario that applies, and checks:

- every API-001 rule for each operation and feature the implementation
  declares;
- the three always-on rules above;
- each declared guarantee: crash and reopen for `Durable`, peer instances
  racing on claim, renew and finalize for `MultiProcess`, and new-work
  visibility at the declared bound;
- that `create_queue` rejects every undeclared feature with
  `FeatureUnsupported`.

An implementation is releasable only when `run_core` and `run_extensions`
pass from its own crate's tests.

### Differential test against the reference

`fireweed-conformance` also provides a differential test. It generates
operation sequences (proptest), applies each one under a manual clock to the
reference and to the implementation under test, and compares per-item
results and final live state. The comparison is limited to deterministic
behaviour:

- queues use `OrderingMode::Strict`;
- after each new-work mutation the clock advances past the implementation's
  `Visibility::Bounded` limit before the next claim;
- server-assigned identifiers and lease tokens are compared by role, not
  value.

A difference is a defect in one of the two. The reference is not right by
definition. A disputed case is settled by API-001, and API-001 is corrected
if it is silent.

## Planned implementations

| Implementation | Crate | Durability | New-work visibility | Sharing | Families |
|----------------|-------|------------|---------------------|---------|----------|
| PostgreSQL reference | new crate | `Durable` | `Immediate` | `MultiProcess` | all |
| Object log × Turso | wraps the current runtime | `Durable` | `Bounded` | `MultiProcess` | all it has today |
| In-memory | new crate | `Process` | `Immediate` | `SingleProcess` | core |

## Facade binding (summary for API-005)

`Fireweed` wraps `Arc<dyn QueueProvider>`:

```rust
impl Fireweed {
    pub fn new(provider: Arc<dyn QueueProvider>) -> Fireweed;
    pub fn guarantees(&self) -> Guarantees;
    pub fn capabilities(&self) -> Capabilities;
    // One method per core operation, delegating to the provider.
    // Convenience helpers (single-item push, ack by id, claim_with, retry_after)
    // are implemented here over the core operations, never in the interface.
    // Extension accessors mirror QueueProvider's and return Option.
}
```

`Fireweed` does not expose the implementation's type, configuration or
maintenance methods. API-005 is rewritten to this shape when ADR-025 is
accepted. `StorageConfig` and `RETIRED_STORAGE_CELL` leave the facade.

## Mapping from the current facade

| Current `Fireweed` methods | New home |
|----------------------------|----------|
| `create_queue`, `ensure_queue`, `queue_definition` | core `create_queue`, `get_queue` |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | core `push`; single-item forms become facade helpers |
| `batch_update`, `reschedule` | core `update` |
| `set_gates` | core `set_gates` |
| `purge` | core `purge` |
| `claim`, `claim_with`, `claim_response_with`, `claim_at`, `claim_response_at` | core `claim`; the `_at` forms are removed (time comes from `Clock`) |
| `claim_by_item_ids` | core `claim_by_item_ids` |
| `renew` | core `renew` |
| `reassign` | core `reassign` |
| `ack`, `complete`, `nack`, `retry`, `release`, `fail`, `nack_retry_after`, `retry_after`, `rearm`, `rearm_at`, `rearm_after` | core `finalize`; named forms become facade helpers |
| `live_item`, `live_items`, `claimed` | core `get_live_items` |
| `metrics` | core `metrics` |
| `discover_active_scopes`, `discover_active_scopes_stamped`, `discover` | core `discover_active_scopes` |
| `upsert` | `Upsert` |
| `mutate_items` | `ItemMutation` |
| `commit`, `commit_multi_claim`, `explain_commit`, `side_record`, `side_records`, `side_records_by_prefix` | `ClaimedWorkCommit` |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | `IndexQuery` |
| `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `bounded_mutation`, `claim_by_query`, `metrics_by_query`, `hot_projection_capabilities` | `ProjectionQuery`; capability flags move into `Capabilities` |
| `claim_by_query_at` | removed (time comes from `Clock`) |
| `claim_across_queues` | `MultiQueueClaim` |
| `retained_items` | `OperatorRepair` (API-002 item inspection) |
| `commit_capabilities` | removed; replaced by `guarantees()` |
| `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `snapshot_now`, `latest_snapshot_info`, `compact_log_behind_snapshot`, `last_rebuild_stats` | the object-log implementation's own type |
| `ownership`, `renew_owned`, `reclaim_expired`, `reclaim_expired_at`, `current_position`, `peek` | implementation-specific; off the interface |

## Error semantics

| Condition | Error / outcome | Retry | Recovery |
|-----------|-----------------|-------|----------|
| Queue definition needs an undeclared feature, priority kind or ordering mode | `FeatureUnsupported` from `create_queue` | No | Choose another implementation or change the definition |
| Transient storage or runtime failure | `Unavailable` | Yes, same `request_id` | None beyond retry |
| Implementation detects a broken invariant | `Internal`; the operation has no effect | No | Operator investigates the implementation |
| Claim returns fewer items than are eligible, within the declared visibility bound | Normal result | Poll again | None |
| Second instance on `SingleProcess` storage | Construction error from the implementation's own constructor | No | Use one instance, or a `MultiProcess` implementation |
| Every API-001 envelope and per-item condition | As API-001 | As API-001 | As API-001 |

## Validation checklist

- [ ] `cargo metadata` shows `fireweed-interface` depending on no `fireweed-*`
      crate other than `fireweed-core`.
- [ ] `fireweed`, `fireweed-resp` and `fireweed-conformance` depend on no
      implementation crate outside `[dev-dependencies]`.
- [ ] No implementation crate depends on another implementation crate.
- [ ] No public item in `fireweed-interface` names a log, command,
      projection, snapshot, epoch, owner session or cell.
- [ ] `QueueProvider` is dyn-compatible, and `Arc<dyn QueueProvider>` is
      `Send + Sync`.
- [ ] Every implementation passes `run_core`, plus `run_extensions` for each
      family it exposes, from its own tests.
- [ ] The differential test passes for every implementation against the
      reference.
- [ ] `create_queue` rejects each undeclared feature with
      `FeatureUnsupported` on every implementation.
- [ ] No core method returns `FeatureUnsupported`, and none returns
      `Unavailable` for a missing operation.
- [ ] `Upsert`, `ClaimedWorkCommit` and `MultiQueueClaim` have written
      semantics in API-001 or their own contracts before this contract moves
      to accepted.
