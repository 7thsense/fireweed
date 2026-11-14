---
ddx:
  id: api-queue-implementation-interface
  depends_on:
    - api-native-client-interface
    - adr-queue-interface-implementation-boundary
    - api-hot-projection-query-surface
    - api-operator-repair-contract
    - api-workload-integration-profiles
    - td-resp-wire-adapter
  status: draft
---

# API-006: Queue implementation interface

**Type**: Rust trait contract (implementation boundary)
**Version**: v0 (draft, revision 2)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, API-003, API-004, API-005, ADR-025, TD-006

The 2026-11-14 addendum to revision 2 adds evidence from the Cayce storage
spike: the durability point, atomic steps and per-call bounds. It does not
answer the owner questions still open in ADR-025.

## Purpose

This contract defines the Rust traits every Fireweed implementation
implements, and the rules that make implementations interchangeable. API-001
defines what each core operation means. This contract defines how operations
are typed, how time and ordering work, which guarantees an implementation
declares, how optional features and families are exposed, how crates depend
on each other, and how an implementation proves it is correct.

Where this contract and API-001 differ, ADR-025 lists the API-001 change.
Until those changes land, this contract governs the interface and API-001
governs everything else.

## Scope and boundaries

- In scope: the `fireweed-interface` crate; `QueueProvider`; extension
  family traits; the time, ordering and visibility model; `Guarantees`,
  `Capabilities`; types and errors; dependency rules; conformance; the
  mapping from the current `Fireweed` methods.
- Out of scope: wire encodings (TD-006); each implementation's
  configuration, storage, recovery and maintenance API; authentication.
- Owning crate: `fireweed-interface` (working name).
- Non-goals: a storage-port SPI; a second public facade type.

## Terms

| Term | Meaning |
|------|---------|
| Implementation | A type implementing `QueueProvider`, in its own crate. |
| Instance | One constructed value of an implementation, attached to some storage. |
| Returns | The operation's future resolved to `Ok`. |
| Linearization point | The single instant, between call and return, at which an operation takes effect. |
| Safety effect | An effect that removes an item from claim eligibility or changes who may act on it: a new lease, renew, reassign, finalize, purge, a gate set to `blocked`, an update that adds a blocking gate key or later `not_before`, a queue pause. |
| New-work effect | An effect of a mutation that makes an item claimable: push, upsert, release, an update that removes a blocking condition, a gate set to `open`, a queue resume. |
| Time-driven transition | A state change caused by time alone: lease expiry, `not_before` or retry backoff passing, cohort expiry, retention expiry. |
| Time authority | The single source of time an implementation uses for every transition. |

## Crate and dependency rules

1. `fireweed-interface` depends on `fireweed-core` and general-purpose
   crates only.
2. No public item in `fireweed-interface` names a log, command, command
   position, projection, snapshot, epoch, owner or storage cell.
3. An implementation crate depends on `fireweed-interface`. It MUST NOT
   depend on another implementation crate. It MAY depend on shared internal
   libraries.
4. `fireweed` (the facade), `fireweed-resp` and `fireweed-conformance` depend
   on `fireweed-interface` and on no implementation crate outside
   `[dev-dependencies]`.
5. `fireweed-server` is the composition root. It is the only published crate
   that names concrete implementations.
6. Test-only crates (`publish = false`), such as the differential test
   crate, MAY depend on several implementations.

A workspace test enforces rules 1 and 3 to 6 from `cargo metadata`.

## Time, ordering and visibility

### Time authority

1. An implementation has one time authority. Every transition samples time
   from it **after** the implementation has serialized that transition
   against conflicting transitions (for example, after taking the row locks
   a claim needs).
2. All instances sharing storage use the same authority (for example,
   database time).
3. Every implementation accepts an `Arc<dyn Clock>` at construction. Under
   conformance it is the authority. An implementation's own configuration
   MAY select a different authority for production (the Postgres reference
   uses database time) and MUST document it.
4. No operation takes a timestamp that overrides the authority, except
   API-001 `MutateItems.evaluated_at`.

```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> UtcTimestamp;
}
```

### Time-driven transitions

Observable state is a function of the returned mutations, each at its
linearization point, and the current time.

1. A time-driven transition takes effect at its due instant and is
   evaluated against queue state as of that instant.
2. Before an operation whose linearization point is at time `t` observes or
   changes a queue, the result MUST equal having processed every transition
   due at or before `t`, in due-instant order (ties in item-id order).
3. Implementations MAY realize this with sweepers, lazily on access, or
   both. No caller drives a tick, and conformance never calls one.

Consequences, applied to API-001:

- **Lease expiry** at `lease_expires_at`: the item is `pending` with no
  active lease. Its token is stale. It counts as pending in reads and
  metrics. `item_version` does not change. `attempt_count` follows API-001
  (expiry charges nothing; the next delivery charges one). The
  implementation remembers that the lease ended by expiry (the
  `StreamGroup` family needs this).
- **Cohort expiry** at the cohort deadline: if the cohort is not complete
  and claim-eligible as of the deadline, every member becomes `failed` with
  code `cohort-incomplete`.
- **Retention expiry**: a request id older than `request_id_retention_ms`
  is forgotten; a tombstone or terminal item older than
  `client_item_key_retention_ms` stops being addressable by key.
- **Recurring items** need no timer: a re-armed item's `not_before` is
  evaluated like any other.

### Linearization and safety

Operations on one queue are linearizable. Each takes effect at one point
between call and return, consistent with real-time order.

1. A claim or read observes every safety effect whose linearization point
   precedes its own. This holds for every implementation, whatever it
   declares.
2. No item has two active leases. A stale lease token is rejected.
3. Exception carried from API-001: replaying a claim's `request_id` while
   its leases are active returns the original claimed set, even if a later
   safety effect would now exclude some of those items.

Operations on different queues have no ordering relationship, except as
`MultiQueueClaim` states.

### Atomic steps and per-call bounds

1. Every core operation is one atomic step on storage: one request in, one
   guarded write (or one read) out. No core operation requires a storage
   transaction held open across more than one call, or across a round trip
   to the caller. An implementation on a storage engine without interactive
   multi-call transactions (for example a Durable Object, or SQLite driven
   as a single guarded batch) can therefore implement the whole core.
   Families that need more than one step (`ClaimedWorkCommit`,
   `MultiQueueClaim`) are optional for this reason.
2. Every mutating operation has a per-call bound declared in
   `ImplementationLimits`. A request over the bound fails with
   `BatchTooLarge` and has no effect; the caller splits it. No core
   operation mutates an unbounded set of items in one step.
3. On an implementation with one writer per storage unit, a mutation
   delays every claim on that unit for as long as its step runs. The
   bounds exist to cap that delay. In the Cayce storage spike a claim on
   SQLite waited 2.3 s behind one 500,000-row `UPDATE`.
4. Selector-scoped family operations (`ItemMutation::mutate_items`,
   `OperatorRepair::purge` and `archive`) are capped by
   `max_selector_affected`. API-001 requires `MutateItems` to apply the
   whole request under one serialization boundary, so the cap is also the
   largest single step such an operation can take. Whether a larger
   selector operation may run as a sequence of bounded steps, each its own
   linearization point, is an open question in ADR-025.

### New-work visibility

```rust
#[non_exhaustive]
pub enum Visibility { Immediate, Bounded { max_ms: u64 } }
```

- `Immediate`: a claim or read observes every new-work effect whose
  linearization point precedes its own.
- `Bounded { max_ms }`: a claim or read observes every new-work effect of a
  mutation that returned more than `max_ms` of elapsed wall time before the
  claim or read started. `max_ms` is not measured on the `Clock`.
- A `Bounded` implementation that has a returned mutation older than
  `max_ms` whose new-work effects are not yet visible MUST fail every claim
  and read on that queue with `QueueError::Unavailable` until they are
  visible.
- Time-driven transitions are not subject to the bound. Once an item is
  visible, its `not_before`, backoff and lease expiry are evaluated against
  the time authority at the operation's linearization point.
- Under `Bounded`, "eligible" in API-001 means eligible and visible. Strict
  ordering applies over the visible eligible set. A whole-group claim
  returns all visible eligible members of each selected group; a member not
  yet visible may be claimed later on its own. A cohort is complete only
  when all `cohort_size` members are visible.
- Metrics and discovery observe the same visible set as claims. They never
  report an item a claim at the same point could not return.

### Replay

Within `request_id_retention_ms`, a repeated `request_id` with the same
body returns the recorded result, whether or not that result's new-work
effects are visible yet, with these exceptions from API-001:

1. A claim replay after every lease it returned has ended (finalized,
   released, reassigned or expired) fails with `RequestExpired`.
2. `MutateItems` with `dry_run = true` records nothing; its replay
   re-evaluates.
3. A repeated `request_id` with a different body fails with
   `RequestIdConflict`.

After the retention window a repeated `request_id` is a new request.

### Reads: snapshot and `as_of`

1. `get_items`, `get_queue` and `metrics` read one queue at a single
   linearization point. `as_of` in a response is the time authority's value
   at that point.
2. `discover_active_scopes` with a `queue_id` is a single-queue snapshot in
   the same sense.
3. Tenant-wide discovery reads each queue at its own point. `as_of` is the
   earliest of those points. There is no cross-queue snapshot. A queue the
   implementation cannot read is omitted, and the response sets
   `partial = true`.
4. `oldest_eligible_age_ms` is exact as of the snapshot. Counts marked
   approximate in API-001 MAY be approximate; each implementation documents
   which.

## Guarantees and capabilities

```rust
#[non_exhaustive]
pub struct Guarantees {
    pub durability: Durability,
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}

#[non_exhaustive]
pub enum Durability { Process, Durable }

#[non_exhaustive]
pub enum Sharing { SingleProcess, MultiProcess }
```

`guarantees()` returns the same value for the life of the instance.

| Value | Rule |
|-------|------|
| `Durable` | Every mutation that returned survives the process being killed and the storage being reopened. Replay of its `request_id` after reopen returns the recorded result. The durability point is the implementation's own commit to its primary storage (for example an fsynced local SQLite commit, or a Postgres commit under the configured `synchronous_commit`). `Durable` says nothing about the loss of that storage's host or disk. |
| `Process` | State lives only as long as the instance. |
| `MultiProcess` | Several instances, in one or many processes, may serve the same storage at once. Together they satisfy this contract as one instance would. |
| `SingleProcess` | At most one instance serves the storage. Opening a second instance either fails, or supersedes the first: every later operation on the first fails with `InstanceSuperseded` and has no effect. Mutations the first returned before supersession keep their durability. |

An implementation that copies its storage elsewhere after commit (for
example a shipper that uploads SQLite pages or WAL segments to object
storage on an interval) declares `Durable` for the local commit and
documents the copy's lag. It does not claim durability against host loss:
in the Cayce storage spike no asynchronous shipping design made an
acknowledged write durable in object storage at return. Durability against
host loss at return needs synchronous replication before the mutation
returns. Whether `Durability` gains a variant for that (for example
`Replicated`) is an open question in ADR-025; adding one is compatible
because the enum is `#[non_exhaustive]`.

```rust
#[non_exhaustive]
pub struct Capabilities {
    pub priority_kinds: BTreeSet<PriorityModelKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub features: BTreeSet<QueueFeature>,
    pub limits: ImplementationLimits,
}

#[non_exhaustive]
pub enum QueueFeature {
    DynamicGates, MetadataBlockers, Recurrence, GroupBatching, Cohorts,
    TypedPayloadSchema, // ADR-011
    SecondaryIndexes,   // ADR-010, ADR-011
}

#[non_exhaustive]
pub struct ImplementationLimits {
    pub max_push_batch_size: u32,
    pub max_claim_batch_size: u32,
    /// Entries per call for update, purge, claim_by_item_ids, renew,
    /// reassign, finalize, upsert and family entry-list operations.
    pub max_mutation_batch_size: u32,
    /// Hard cap on items a selector-scoped family operation may affect.
    pub max_selector_affected: u32,
    pub max_lease_duration_ms: u64,
    pub default_max_gate_keys_per_item: u32,
    pub default_max_gates_per_request: u32,
}
```

Every implementation supports `Timestamp` and `Int64` priority kinds and
`Strict` ordering. `create_queue` rejects, before creating anything, a
definition that needs an unlisted priority kind, ordering mode or feature,
with `FeatureUnsupported`. A created queue supports every core operation and
every operation of each family the implementation exposes that applies to
its definition.

## Types

All types live in `fireweed-interface`. Types already in `fireweed-core`
(`TenantId`, `QueueId`, `ItemId`, `ClientItemKey`, `RequestId`,
`LeaseToken`, `WorkerId`, `CohortId`, `GroupKey`, `PriorityValue`,
`UtcTimestamp`, `Metadata`, `QueueDefinition`) are re-exported. Every
public struct and enum here is `#[non_exhaustive]` and has a constructor;
field lists below are the minimum.

### Targets, schedules and outcomes

```rust
pub struct QueueKey { pub tenant_id: TenantId, pub queue_id: QueueId }

pub enum ItemRef { Id(ItemId), Key(ClientItemKey) }
pub struct LeaseRef { pub item_id: ItemId, pub lease_token: LeaseToken }
pub struct CohortLeaseRef { pub cohort_id: CohortId, pub cohort_lease_token: LeaseToken }
pub enum LeaseTarget { Item(LeaseRef), Cohort(CohortLeaseRef) }

/// Resolved by the implementation at the transition's time.
pub enum Schedule { At(UtcTimestamp), After { delay_ms: u64 } }

pub enum ItemStatus {
    Accepted, Updated, Duplicate, Replaced, Claimed, Renewed, Reassigned,
    Completed, Failed, Retried, Released, Rearmed, Purged,
    NotFound, Invalid { reason: String }, Conflict, StaleLease, Terminal,
    GroupTooLarge, NotEligible, Leased, Superseded, RateLimited, Unavailable,
}

pub struct Outcome<T> {
    pub target: T,
    pub status: ItemStatus,
    pub item_id: Option<ItemId>,
    pub item_version: Option<u64>,
}
```

Batch responses hold one `Outcome` per request entry, in request order.
Cohort targets use the same statuses as item targets. An ADR-011 schema
violation is a per-item `Invalid` whose reason names the violation.

### Items, claims and reads

```rust
pub struct NewItem {
    pub client_item_key: ClientItemKey,
    pub priority: PriorityValue,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub cohort_size: Option<u32>,
    pub gate_keys: Vec<String>,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
    pub entity: Option<serde_json::Value>, // TypedPayloadSchema queues
}

pub struct ClaimedItem { /* exactly API-001 "Claimed Item Response Shape" */ }

pub struct LeaseSummary {
    pub worker_id: WorkerId,
    pub lease_expires_at: UtcTimestamp,
    pub ended_by_expiry: bool,
}

pub struct ItemView {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub item_version: u64,
    pub lifecycle_state: LifecycleState, // pending | leased
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub group_key: Option<GroupKey>,
    pub attempt_count: u32,
    pub lease: Option<LeaseSummary>,      // never the token
    pub gate_keys: Vec<String>,
    pub payload: Option<Bytes>,
    pub fields: BTreeMap<String, Bytes>,
    pub metadata: Metadata,
}

pub struct QueueMetrics {
    pub as_of: UtcTimestamp,
    pub pending: u64,
    pub leased: u64,
    pub complete: u64,
    pub failed: u64,
    pub retry_backlog: u64,
    pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64,
    pub active_leases: u64,
    pub recurring_pending: Option<u64>,
    pub recurring_leased: Option<u64>,
}
```

## Core trait

```rust
pub type QueueResult<T> = Result<T, QueueError>;
pub type QueueFuture<'a, T> = Pin<Box<dyn Future<Output = QueueResult<T>> + Send + 'a>>;

pub trait QueueProvider: Send + Sync {
    fn guarantees(&self) -> Guarantees;
    fn capabilities(&self) -> Capabilities;

    fn create_queue(&self, r: CreateQueueRequest) -> QueueFuture<'_, CreateQueueResponse>;
    fn get_queue(&self, queue: QueueKey) -> QueueFuture<'_, Option<QueueDefinition>>;

    fn push(&self, r: PushRequest) -> QueueFuture<'_, PushResponse>;
    fn update(&self, r: UpdateRequest) -> QueueFuture<'_, UpdateResponse>;
    fn set_gates(&self, r: SetGatesRequest) -> QueueFuture<'_, SetGatesResponse>;
    fn purge(&self, r: PurgeRequest) -> QueueFuture<'_, PurgeResponse>;

    fn claim(&self, r: ClaimRequest) -> QueueFuture<'_, ClaimResponse>;
    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> QueueFuture<'_, ClaimByItemIdsResponse>;
    fn renew(&self, r: RenewRequest) -> QueueFuture<'_, RenewResponse>;
    fn reassign(&self, r: ReassignRequest) -> QueueFuture<'_, ReassignResponse>;
    fn finalize(&self, r: FinalizeRequest) -> QueueFuture<'_, FinalizeResponse>;

    fn get_items(&self, r: GetItemsRequest) -> QueueFuture<'_, GetItemsResponse>;
    fn metrics(&self, queue: QueueKey) -> QueueFuture<'_, QueueMetrics>;
    fn discover_active_scopes(&self, r: DiscoverRequest) -> QueueFuture<'_, DiscoverResponse>;

    // Families. Default: not provided. Adding a defaulted accessor is compatible.
    fn upsert(&self) -> Option<&dyn Upsert> { None }
    fn item_mutation(&self) -> Option<&dyn ItemMutation> { None }
    fn claimed_work_commit(&self) -> Option<&dyn ClaimedWorkCommit> { None }
    fn index_query(&self) -> Option<&dyn IndexQuery> { None }
    fn projection_query(&self) -> Option<&dyn ProjectionQuery> { None }
    fn multi_queue_claim(&self) -> Option<&dyn MultiQueueClaim> { None }
    fn stream_group(&self) -> Option<&dyn StreamGroup> { None }
    fn operator_repair(&self) -> Option<&dyn OperatorRepair> { None }
}
```

`QueueProvider` and every family trait are dyn-compatible and
`Send + Sync`. Every request carries its target queue and, when mutating, its
`request_id`. No core method returns `FeatureUnsupported`, and none returns
`Unavailable` for anything other than a transient condition.

### Core operations

| Method | API-001 operation | Request (minimum) | Response |
|--------|-------------------|-------------------|----------|
| `create_queue` | CreateQueue | `queue`, `definition` | stored definition, `created` |
| `get_queue` | (read) | `queue` | definition or `None` |
| `push` | BatchPush | `queue`, `request_id`, `items: Vec<NewItem>` | `Vec<Outcome<ClientItemKey>>` |
| `update` | BatchUpdate | `queue`, `request_id`, entries of `item: ItemRef`, `expected_item_version`, and optional replacements of `priority`, `not_before: Option<Schedule>`, `payload`, `fields`, `metadata`, `gate_keys` | `Vec<Outcome<ItemRef>>` |
| `set_gates` | SetGates | `queue`, `request_id`, `gates: Vec<(String, GateState)>` | `gate_epoch: u64`, canonical gates |
| `purge` | PurgeItems | `queue`, `request_id`, `items: Vec<ItemRef>`, `force` | `Vec<Outcome<ItemRef>>` |
| `claim` | BatchClaim | `queue`, `request_id`, `worker_id`, `max_items`, `lease_duration_ms`, filter (`group_key`, `same_group_key`, `metadata_equals`), unit (`Items`, `WholeGroups { max_groups }`, `WholeCohort`) | `items: Vec<ClaimedItem>`, `cohort: Option<CohortLease>`, `queue_paused: bool` |
| `claim_by_item_ids` | BatchClaimByItemIds | `queue`, `request_id`, `worker_id`, `item_ids`, `lease_duration_ms` | per id: `ClaimedItem` or reason `NotFound` / `NotEligible` / `Leased` / `Terminal` |
| `renew` | BatchRenewLeases | `queue`, `request_id`, `leases: Vec<LeaseTarget>`, `lease_duration_ms` | `Vec<Outcome<LeaseTarget>>` with new `lease_expires_at` |
| `reassign` | ReassignLeases (new in API-001) | `queue`, `request_id`, `leases: Vec<LeaseRef>`, `to_worker`, `lease_duration_ms` | per lease: new `ClaimedItem` (new token, one attempt charged) or status |
| `finalize` | BatchFinalize | `queue`, `request_id`, entries of `target: LeaseTarget` and outcome `Complete`, `Fail { failure_code, metadata }`, `Retry { not_before: Schedule, priority }`, `Release`, `Rearm { not_before: Schedule, priority }` | `Vec<Outcome<LeaseTarget>>` |
| `get_items` | BatchGetLiveItems, extended | `queue`, `items: Vec<ItemRef>` | `as_of`, `Vec<Option<ItemView>>` aligned with request |
| `metrics` | GetQueueMetrics | `queue` | `QueueMetrics` |
| `discover_active_scopes` | DiscoverActiveScopes | `tenant_id`, optional `queue_id`, `granularity`, `group_key`, `max_results`, `page_token`, `authorize` | `as_of`, `active_scopes`, `next_page_token`, `partial` |

Notes:

- **Pause.** API-002 pause is observable in the core: a claim on a paused
  queue returns no items and `queue_paused = true`; pushes are accepted.
  There is no pause error.
- **`get_items`** returns live items only (`pending`, `leased`), addressed
  by id or key. It never returns a lease token.
- **`reassign`** moves an active lease to `to_worker`. It is how an operator
  or worker takes over in-flight work while the lease still runs. Work
  whose lease has expired is simply pending and is claimed normally.
- **`authorize`** is `Option<Arc<dyn Fn(&QueueKey) -> bool + Send + Sync>>`.
  `None` means the caller is trusted for every queue of `tenant_id`. The
  implementation evaluates the predicate once per candidate queue, before
  ranking and before computing `as_of`, and only for queues of `tenant_id`.
  The predicate must be pure. A named `queue_id` it rejects fails with
  `QueueNotFound`.

## Extension families

Each family is a trait; signatures follow the core pattern (one request
struct in, one response struct out, `QueueFuture`). An implementation that
returns `Some` from an accessor implements every method of the family for
every queue the family applies to.

| Accessor | Trait and methods | Semantics |
|----------|-------------------|-----------|
| `upsert` | `Upsert::upsert` | Below. |
| `item_mutation` | `ItemMutation::mutate_items` | API-001 Atomic Item Mutation. Lease-preserving reschedule of a leased item uses a lease guard of `match_token` or `require_active`. |
| `claimed_work_commit` | `ClaimedWorkCommit::{commit, commit_multi_claim, explain_commit, get_side_records}` | Below. |
| `index_query` | `IndexQuery::query_index` (unique, multi and typed forms are request options) | ADR-010, ADR-011. |
| `projection_query` | `ProjectionQuery::{range_scan, grouped_aggregate, declared_bucket_segment, bounded_mutation, claim_by_query, metrics_by_query, query_capabilities}` | API-004. `query_capabilities(&QueueKey)` is synchronous and per queue. |
| `multi_queue_claim` | `MultiQueueClaim::claim_across_queues` | Below. |
| `stream_group` | `StreamGroup::{pending_summary, pending_page, claim_for_worker, autoclaim, ack, claimed}` | Below. |
| `operator_repair` | `OperatorRepair::{pause, resume, admin_state, inspect_item, repair, redrive, purge, archive, get_operation, audit_records, retained_items}` | API-002. |

### `Upsert`

`upsert(queue, request_id, items: Vec<NewItem>)`, per item by
`client_item_key`:

- no live item with the key: push, status `Accepted`;
- a pending item with the key: atomically replace it. The old `item_id`
  becomes superseded (any later operation addressing it reports
  `Superseded`), a new `item_id` is assigned, status `Replaced`;
- a leased item with the key: `Conflict`, nothing changes.

### `ClaimedWorkCommit`

`commit(queue, request_id, entries)` applies a vector of entries with
independent per-entry outcomes. There is no atomicity across entries. Each
entry is atomic and contains:

- `claim: LeaseRef`, validated (token and expiry) at the entry's
  linearization point;
- `finalize`: the finalize outcome for that claim;
- `side_records: Vec<(key, value)>`: opaque records, never claimable, read
  through `get_side_records`;
- `new_items: Vec<NewItem>`: enqueued as ordinary work, `not_before`
  allowed (timers and awaits);
- `instance_fence: Option<{ key, expected, next }>`: the entry commits only
  if the stored fence for `key` equals `expected` (absent reads as 0) and
  `next > expected`. A stale `expected` gives `Conflict`; `next <= expected`
  gives `Invalid`.

If any part of an entry fails, nothing of that entry is applied.
`commit_multi_claim` is the same, except one entry may consume several
claims, all validated before any effect. `request_id` replay covers the
whole body. `explain_commit(queue, request_id)` returns, for a retained
request, the per-entry outcome, consumed claims, side-record keys and new
item ids, reconstructed from durable state. `get_side_records` reads by key
or key prefix, paginated.

These are invariants of the family, not optional flags. They replace
`commit_capabilities`.

### `MultiQueueClaim`

`claim_across_queues(targets)` claims from several queues in one call.
Validation of every target happens before any claim. Each target then runs
as an independent `claim` with its own outcome, in caller order. It is not
atomic across queues. Every target uses one lease instant.

### `StreamGroup`

Redis Streams consumer-group semantics for RESP (TD-006). A lease in this
family is identified by `item_id` and held by `worker_id`. The family does
not use lease tokens.

- `claim_for_worker(queue, worker_id, item_ids, lease_duration_ms)`
  (`XCLAIM`): for each id, lease a pending item to `worker_id`, renew if
  `worker_id` already holds it, or reassign it from another worker.
- `autoclaim(queue, worker_id, after: Option<ItemId>, count,
  lease_duration_ms)` (`XAUTOCLAIM`): in item-id order after the cursor,
  lease up to `count` pending items whose last lease ended by expiry.
  Returns the leased items and the next cursor.
- `ack(queue, item_ids)` (`XACK`): complete each leased item by id,
  whichever worker holds it. Per-item outcomes.
- `pending_summary(queue)` (`XPENDING` summary): leased count, lowest and
  highest leased `item_id`, count per worker.
- `pending_page(queue, after, count, worker_id: Option, min_idle_ms:
  Option)` (`XPENDING` extended): leased items in item-id order with holder,
  `lease_expires_at`, `attempt_count`, and next cursor.
- `claimed(queue, item_ids)`: `ClaimedItem` for each id currently leased.

**Weaker fencing, by design.** Any caller may acknowledge or take over any
lease by id. Core-path callers (token-addressed) and `StreamGroup` callers
can be mixed on one queue: a `StreamGroup` takeover or ack makes the core
holder's token stale.

## Errors

```rust
#[non_exhaustive]
pub enum QueueError {
    QueueNotFound,
    QueueDefinitionConflict,
    FeatureUnsupported(UnsupportedFeature),
    GatesNotEnabled,
    InvalidRequest(String),
    BatchTooLarge,
    RequestIdConflict,
    RequestExpired,
    CommitTimeout,
    RateLimited { retry_after_ms: Option<u64> },
    Unavailable(String),
    InstanceSuperseded,
    Family(FamilyError),
    Internal(String),
}

#[non_exhaustive]
pub enum FamilyError {
    CursorInvalidated,                                  // API-004
    CapabilityUnavailable(&'static str),                // API-004 side_record_query
    InvalidSelector(String),                            // API-001 MutateItems, API-002
    MatchCountMismatch { expected: u64, actual: u64 },  // API-002
    OperatorForbidden,                                  // API-002
    OperationNotFound,                                  // API-002
}
```

| Error | Meaning | Retry |
|-------|---------|-------|
| `QueueNotFound` | Queue absent, or rejected by `authorize` | No |
| `QueueDefinitionConflict` | API-001 | No |
| `FeatureUnsupported` | Only from `create_queue` | No |
| `GatesNotEnabled`, `InvalidRequest`, `BatchTooLarge` | API-001; `InvalidRequest` also covers a request that can never fit a hard limit | After fixing the request |
| `RequestIdConflict`, `RequestExpired` | API-001 replay rules | With a new `request_id` |
| `CommitTimeout` | Durability not confirmed in time; outcome unknown | Same `request_id` |
| `RateLimited` | Capacity or backpressure | Same `request_id`, after `retry_after_ms` |
| `Unavailable` | Transient: storage unreachable, shutting down, `Bounded` visibility exceeded | Same `request_id` |
| `InstanceSuperseded` | `SingleProcess` instance replaced | No; use the new instance |
| `Family` | As the family's contract | As the family's contract |
| `Internal` | Broken invariant detected; the operation had no effect | No |

`QueueForbidden` is not an interface error. Authentication and
authorization belong to the wire adapter, which maps its own decisions to
API-001's `queue-forbidden`.

## Maintenance and coordination

Not part of this contract; offered by an implementation on its own type if
it needs them:

- projection verify, delete, rebuild;
- snapshot policy and creation, log compaction, rebuild statistics;
- ownership acquisition and renewal, epoch fencing, redirects;
- command positions and other progress markers.

## Profiles

A profile names what a consumer needs. An implementation satisfies a
profile when every listed family accessor returns `Some`, every listed
feature is in `capabilities().features`, its guarantees are at least the
listed minimum, and its limits are at least the listed minimum.

| Profile | Families | Features | Minimum guarantees |
|---------|----------|----------|--------------------|
| `core` | none | none | none |
| `resp-worker` | `Upsert`, `StreamGroup` | none | none |
| `snorri` (to confirm with Snorri) | `Upsert`, `ItemMutation`, `ClaimedWorkCommit`, `IndexQuery`, `ProjectionQuery` | `TypedPayloadSchema`, `SecondaryIndexes`, `DynamicGates` | `Durable` |

A deployment marked production-supported requires `Durable`.

## Conformance

### Harness

```rust
pub trait Harness: Send + Sync + 'static {
    type Provider: QueueProvider + 'static;

    /// A fresh instance on fresh, isolated storage. `clock` is the time authority.
    fn open<'a>(&'a self, clock: Arc<ManualClock>)
        -> BoxFuture<'a, Result<Self::Provider, HarnessError>>;

    /// Another instance on the same storage as `existing`, in this process.
    fn open_peer<'a>(&'a self, existing: &'a Self::Provider, clock: Arc<ManualClock>)
        -> BoxFuture<'a, Result<Self::Provider, HarnessError>>;

    /// Returns once every returned mutation's effects are visible (wall time).
    /// `Immediate` implementations return at once.
    fn settle<'a>(&'a self, provider: &'a Self::Provider) -> BoxFuture<'a, ()>;

    /// How to run this implementation in a child process. Required for
    /// `Durable` and `MultiProcess`.
    fn child_process(&self) -> Option<ChildProcessSpec>;
}
```

`fireweed-conformance` provides `run_child_agent(harness)`, a `main` helper
for a small binary in the implementation's crate. The suite drives that
binary over stdin and stdout.

### Suites

```rust
pub async fn run_core<H: Harness>(harness: H);
pub async fn run_families<H: Harness>(harness: H); // each family whose accessor returns Some
```

The suite uses only `fireweed-interface`. It checks:

1. Every API-001 rule, as amended by ADR-025, for each operation and each
   declared feature.
2. The time-driven transitions, using `ManualClock` only, with no tick.
3. Linearization and safety: concurrent claims never produce two active
   leases; every safety effect excludes items from claims that start after
   it; claim replay after leases end gives `RequestExpired`.
4. `Durable`: the child process is killed (`SIGKILL`) at chosen points
   around acknowledged mutations and around lost responses; storage is
   reopened in the parent; every returned mutation is present and every
   `request_id` replays its recorded result.
5. `MultiProcess`: peers in separate child processes race claim, renew,
   reassign and finalize on one queue.
6. `SingleProcess`: a second `open_peer` either fails or supersedes the
   first, which then fails every operation with `InstanceSuperseded`.
7. `Bounded`: effects are visible after `settle`; with apply held back by a
   harness fault hook, claims and reads fail with `Unavailable` once the
   bound passes.
8. `create_queue` rejects every undeclared feature with
   `FeatureUnsupported`, and only `create_queue` does.

An implementation is releasable only when `run_core` and `run_families`
pass from its own crate's tests.

### Differential test

A test-only crate depends on the Postgres reference and on each other
implementation. It generates sequential histories (proptest) and applies
each under a `ManualClock` to the reference and to the implementation under
test. Rules:

- queues use `Strict` ordering. Each history gives equal-priority items
  distinct push order and uses the `created_sequence` tie-breaker, so order
  is determined by the history;
- after each mutation the harness calls `settle`;
- item ids and lease tokens are compared by the `client_item_key` and
  claim they belong to, not by value;
- compared: every `Outcome` status and `item_version`, every claimed set
  and its order, `ItemView` lifecycle, `attempt_count` and lease summary,
  and the exact `QueueMetrics` fields. Fields API-001 allows to be
  approximate are not compared.

Concurrency is covered by the conformance suites, not by the differential
test. A difference is a defect in one of the two implementations, settled
by API-001. If API-001 is silent, it is amended first.

## Planned implementations

| Implementation | Durability | Visibility | Sharing | Families |
|----------------|------------|------------|---------|----------|
| PostgreSQL reference (new crate, from TD-002) | `Durable` | `Immediate` | `MultiProcess` | core first, then all |
| Object log × Turso (current runtime; future open in ADR-025) | `Durable` | `Bounded` (value set by evidence) | `SingleProcess` | all eight |
| In-memory (new crate) | `Process` | `Immediate` | `SingleProcess` | core, `Upsert`, `StreamGroup` |

## Facade binding (summary for API-005)

```rust
impl Fireweed {
    pub fn new(provider: Arc<dyn QueueProvider>) -> Fireweed;
    pub fn guarantees(&self) -> Guarantees;
    pub fn capabilities(&self) -> Capabilities;
    // One method per core operation, delegating to the provider.
    // Helpers over core operations (single-item push, complete(lease_refs),
    // retry_after(lease_refs, delay)) use `Schedule::After` and
    // token-addressed targets, and return per-item outcomes.
    // Family accessors mirror QueueProvider's and return Option.
}
```

`Fireweed` exposes no implementation type, configuration or maintenance
method. `StorageConfig` and `RETIRED_STORAGE_CELL` leave the facade.

## Mapping from the current facade

`Fireweed` has 84 public methods today: 74 in `crates/fireweed/src/facade.rs`
and 10 `operator_*` methods in `crates/fireweed/src/operator.rs`.

| Current methods | New home | Behaviour change |
|-----------------|----------|------------------|
| `create_queue`, `ensure_queue`, `queue_definition` | core `create_queue`, `get_queue` | none |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | core `push`; single-item forms are facade helpers | none |
| `upsert` | `Upsert` | none |
| `batch_update` | core `update` | none |
| `reschedule` | `ItemMutation` with a lease guard | needs the `ItemMutation` family |
| `set_gates` | core `set_gates` | response drops command position |
| `purge` | core `purge` | none |
| `claim`, `claim_with`, `claim_response_with` | core `claim` | none |
| `claim_at`, `claim_response_at`, `claim_by_query_at`, `reclaim_expired_at` | removed | they override evaluation time |
| `claim_by_item_ids` | core `claim_by_item_ids` | none |
| `renew`, `reassign` | core `renew`, `reassign` | token-addressed, per-item outcomes (were id-only, all-or-nothing) |
| `ack`, `complete`, `nack`, `retry`, `release`, `fail`, `rearm` | core `finalize`; helpers take `LeaseTarget`s | token-addressed, per-item outcomes (were id-only, all-or-nothing) |
| `nack_retry_after`, `retry_after`, `rearm_after`, `rearm_at` | core `finalize` with `Schedule::After` / `Schedule::At` | token-addressed, per-item |
| `live_item`, `live_items` | core `get_items` | adds lease summary |
| `claimed` | `StreamGroup::claimed` | needs the `StreamGroup` family |
| `metrics` | core `metrics` | adds API-001 fields |
| `discover_active_scopes`, `discover_active_scopes_stamped`, `discover` | core `discover_active_scopes` | `as_of` per the snapshot rule; `partial` flag |
| `mutate_items` | `ItemMutation` | none |
| `commit`, `commit_multi_claim`, `explain_commit`, `side_record`, `side_records`, `side_records_by_prefix` | `ClaimedWorkCommit` | `side_record*` become `get_side_records` |
| `commit_capabilities` | removed | family invariants replace it |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | `IndexQuery` | none |
| `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `bounded_mutation`, `claim_by_query`, `metrics_by_query`, `hot_projection_capabilities` | `ProjectionQuery` | none |
| `claim_across_queues` | `MultiQueueClaim` | none |
| `operator_pause`, `operator_resume`, `operator_admin_state`, `operator_inspect_item`, `operator_repair`, `operator_redrive`, `operator_purge`, `operator_archive`, `operator_get_operation`, `operator_audit_records`, `retained_items` | `OperatorRepair` | none |
| `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `snapshot_now`, `latest_snapshot_info`, `compact_log_behind_snapshot`, `last_rebuild_stats` | object-log implementation type | leave `Fireweed` |
| `ownership`, `renew_owned`, `reclaim_expired`, `current_position`, `peek` | object-log implementation type | leave `Fireweed`; expiry follows the time rule |

## Compatibility

- Adding a family, a defaulted accessor, an enum variant or a struct field
  is compatible, because the types are `#[non_exhaustive]`.
- Adding a required method to an existing trait, or changing a signature,
  is breaking.
- Changing a declared guarantee of an implementation is a breaking change
  for that implementation.

## Validation checklist

- [ ] The dependency test enforces rules 1 and 3 to 6.
- [ ] No public item in `fireweed-interface` names a log, command,
      projection, snapshot, epoch, owner or cell.
- [ ] `QueueProvider` and every family trait are dyn-compatible;
      `Arc<dyn QueueProvider>` is `Send + Sync`.
- [ ] Every public type is `#[non_exhaustive]` with a constructor.
- [ ] Every implementation passes `run_core` and `run_families` from its
      own tests, including child-process crash and peer scenarios where
      declared.
- [ ] Time-driven scenarios pass under `ManualClock` with no tick.
- [ ] Every mutating operation rejects a request over its declared bound
      with `BatchTooLarge` and no effect.
- [ ] Each implementation documents its durability point and any
      post-commit copy lag.
- [ ] The differential test passes for every implementation against the
      reference.
- [ ] The object-log implementation passes the safety-effect scenarios
      under induced apply lag for every safety effect, not only gates.
- [ ] Each mapping-table behaviour change is in the API-005 migration notes.
- [ ] ADR-025's document changes have landed before this contract moves to
      accepted.
