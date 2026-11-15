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

# API-006: Fireweed data-plane interface

**Type**: Rust trait and request-type contract (implementation boundary)
**Version**: v0 (draft, revision 3)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, API-003, API-004, API-005, ADR-025, TD-002, TD-006

Revision 3 (2026-11-15) replaces the method-per-operation queue trait of
revision 2 with spaces, four storage shapes, typed requests, group commit
and declared reads (ADR-025 D8 to D12).

## Purpose

This contract defines the Rust types and traits every Fireweed
implementation implements, and the rules that make implementations
interchangeable. It covers the data model (spaces, shapes, schema), the
request model (verbs, write operations, expressions, reads), time and
ordering, guarantees, limits, errors, conformance, and the binding to the
`Fireweed` facade.

API-001 defines queue semantics (eligibility, leases, retries, gates,
groups, cohorts, recurrence, idempotency). This contract refers to it for
those and states every change. Where they differ, ADR-025 lists the API-001
change, and until it lands this contract governs.

## Scope and boundaries

- In scope: the `fireweed-interface` crate; the `DataPlane` trait; all
  request, response and schema types; the expression language; time,
  ordering and durability; limits; errors; conformance; the facade binding.
- Out of scope: wire encodings beyond serde (TD-006, open question 12 in
  ADR-025); each implementation's configuration, storage layout, recovery,
  replication and maintenance API; authentication and authorization.
- Owning crate: `fireweed-interface` (working name).
- Non-goals: SQL; joins; a storage-port SPI; history and analytics beyond
  live data (Niflheim).

## Terms

| Term | Meaning |
|------|---------|
| Implementation | A type implementing `DataPlane`, in its own crate. |
| Instance | One constructed value of an implementation, attached to some storage. |
| Space | One atomic and ordering domain owned by one tenant, holding named objects. |
| Object | A queue, table, event log or the blob store of a space. |
| Request | One call on `DataPlane`. A write request carries a `request_id`. |
| Commits | A write request takes effect, at its linearization point. A rejected or definitively failed request never commits. |
| Linearization point | The single instant at which a committed request takes effect. |
| Time authority | The single source of time for a space. |
| Time-driven transition | A state change caused by time alone: lease expiry, cohort deadline, retention and row expiry. |

## Crate and dependency rules

1. `fireweed-interface` depends on `fireweed-core` id types and
   general-purpose crates only (`serde`, `bytes`, `serde_json`, `futures`).
2. No public item exposes an implementation's internal mechanism: command
   log, command position, projection, apply frontier, ownership epoch,
   storage cell. API-001 domain fields such as `gate_epoch` are allowed.
3. Every public type in `fireweed-interface` is owned by it,
   `#[non_exhaustive]`, has a constructor, and derives `Serialize` and
   `Deserialize`. It does not re-export exhaustive `fireweed-core` enums or
   structs; it defines its own and converts.
4. An implementation crate depends on `fireweed-interface` and on no other
   implementation crate. It may depend on shared internal libraries.
5. `fireweed`, `fireweed-resp` and `fireweed-conformance` depend on no
   implementation crate outside `[dev-dependencies]`.
6. `fireweed-server` is the only published crate that names concrete
   implementations. Test-only crates (`publish = false`) may name several.

A workspace test enforces rules 1, 4, 5 and 6 from `cargo metadata`.

## Data model

### Spaces and objects

```rust
pub struct SpaceKey { pub tenant_id: TenantId, pub space_id: SpaceId }
pub struct ObjectName(String);   // [a-z][a-z0-9_]{0,62}, unique within a space
pub struct FieldName(String);    // [a-z][a-z0-9_]{0,62}, unique within an object
```

A space is created by its first `apply_schema`. All objects of a space are
declared in its schema. Every write request names one space, and every
operation in it names an object of that space.

### Schema

```rust
pub struct SpaceSchema {
    pub version: u64,                 // strictly increasing per space
    pub queues: Vec<QueueDecl>,
    pub tables: Vec<TableDecl>,
    pub event_logs: Vec<EventLogDecl>,
    pub blobs: Option<BlobPolicy>,    // None: the space has no blob store
    pub reads: Vec<ReadDecl>,
}

pub struct FieldDecl {
    pub name: FieldName,
    pub ty: FieldType,
    pub nullable: bool,
    pub source: FieldSource,
    pub index: IndexHint,
}

pub enum FieldType { Bool, Int, Float, Text, Bytes, Timestamp, Json, TextSet, BlobRef }
pub enum FieldSource { Stored, BodyPointer(JsonPointer) } // BodyPointer: derived, read-only
pub enum IndexHint { None, Lookup, Range, Unique }

pub enum BodyKind { Bytes, Json }

pub struct QueueDecl {
    pub name: ObjectName,
    pub definition: QueueDefinition,  // API-001: priority kind, ordering, retry,
                                      // retention, features (gates, cohorts, …)
    pub body: BodyKind,
    pub fields: Vec<FieldDecl>,
    pub history: Option<ObjectName>,  // an event log receiving transition history
}

pub struct TableDecl {
    pub name: ObjectName,
    pub key: Vec<FieldName>,          // 1 to 4 fields of Int, Text, Bytes or Timestamp
    pub fields: Vec<FieldDecl>,       // includes the key fields
    pub expiry: Option<RowExpiry>,
}
pub enum RowExpiry { AtField(FieldName), AfterWrite { ttl_ms: u64 } }

pub struct EventLogDecl {
    pub name: ObjectName,
    pub partition_key: FieldName,     // Text or Int
    pub fields: Vec<FieldDecl>,
    pub body: BodyKind,
    pub retention: LogRetention,      // max_age_ms and/or max_entries_per_partition
}

pub struct BlobPolicy { pub orphan_grace_ms: u64 }
```

Rules:

1. `apply_schema(space, schema, expected_version)` succeeds only if the
   stored version equals `expected_version` (`None` for a new space) and
   `schema.version` is greater. Otherwise `SchemaConflict`. Replaying the
   identical schema at its own version succeeds with no change (ADR-018
   exact ensure).
2. v1 accepts additive changes only: new objects, new nullable fields, new
   indexes, new read versions, raised retention. Anything else is
   `SchemaConflict`.
3. A declaration needing an unsupported feature, shape or limit fails with
   `FeatureUnsupported` before anything changes.
4. Indexes may build after `apply_schema` returns. A field or read whose
   index is not ready fails with `IndexNotReady`. `get_schema` reports each
   index's state.
5. Queue items have system fields, always declared, usable in expressions
   and reads: `item_id`, `client_item_key`, `state` (`pending`, `leased`,
   `complete`, `failed`), `priority`, `not_before`, `group_key`,
   `attempt_count`, `item_version`, `lease_expires_at`, `worker_id`,
   `created_at`, `updated_at`, `terminal_at`, `gate_keys` (a `TextSet`).
   `state`, `not_before`, `group_key`, `client_item_key`, `terminal_at` and
   `gate_keys` are indexed. Tables have `row_version`, `created_at` and
   `updated_at`. Event log entries have `seq` and `appended_at`.
6. A `TextSet` field holds at most `limits.max_set_values` distinct values.
   An indexed `TextSet` is indexed per value.
7. A `BodyPointer` field is computed from the item's or entry's JSON body
   and is read-only. If the pointer is absent, or the value has the wrong
   type, the field is null.

### Values

```rust
pub enum Value {
    Null, Bool(bool), Int(i64), Float(f64), Text(String), Bytes(Bytes),
    Timestamp(UtcMillis), Json(serde_json::Value), TextSet(BTreeSet<String>),
    BlobRef(BlobRef),
}
pub struct BlobRef { pub sha256: [u8; 32], pub size: u64 }
```

`Float` values are finite. Timestamps are milliseconds since the Unix
epoch, UTC. `Text` is UTF-8.

## Expressions

```rust
pub enum Expr {
    Lit(Value),
    Field(FieldName),
    Param(ParamName),
    Now,
    Json { field: FieldName, pointer: JsonPointer }, // RFC 6901; yields Json or Null
    Cast { expr: Box<Expr>, to: ScalarType },
    Cmp { op: CmpOp, l: Box<Expr>, r: Box<Expr> },   // = != < <= > >=
    And(Vec<Expr>), Or(Vec<Expr>), Not(Box<Expr>), IsNull(Box<Expr>),
    In { expr: Box<Expr>, list: Vec<Expr> },
    Contains { set: Box<Expr>, value: Box<Expr> },   // TextSet membership
    Arith { op: ArithOp, l: Box<Expr>, r: Box<Expr> }, // + - * / %
    Coalesce(Vec<Expr>),
    Bucket { expr: Box<Expr>, width_ms: u64 },
}

pub enum Aggregate {
    Count, CountOf(Expr), Sum(Expr), Min(Expr), Max(Expr), Avg(Expr),
    Percentile { expr: Expr, p: f64 },               // 0 < p <= 1
}
```

Every implementation evaluates expressions with exactly these semantics.

| Topic | Rule |
|-------|------|
| Typing | Expressions are type-checked against the schema before anything runs. `Int` and `Float` do not mix without `Cast`. `Json` must be `Cast` before comparison or arithmetic. A type error is `InvalidRequest`. |
| Null | SQL three-valued logic. A comparison or arithmetic with a null operand is null. `And`, `Or`, `Not` follow SQL. A predicate selects a row only when it is true. `In` follows SQL, including a null in the list. |
| Text order | Unicode code-point order (bytewise UTF-8). No locale collation. |
| `Int` arithmetic | 64-bit signed. Overflow is an evaluation error. Division truncates toward zero; `%` takes the sign of the dividend; division or `%` by zero is an evaluation error. |
| `Float` arithmetic | IEEE 754 double. A NaN or infinite result is an evaluation error. |
| Timestamps | `Timestamp ± Int` (milliseconds) is a `Timestamp`; `Timestamp − Timestamp` is an `Int` of milliseconds. |
| `Bucket` | `floor(t / width_ms) * width_ms`, flooring toward negative infinity. |
| `Now` | The time authority at the request's linearization point; one value for the whole request. |
| `Cast` | `Json` number to `Int` only if integral and in range; `Json` string to `Text` or `Timestamp` (RFC 3339); otherwise null. `Int` to `Float` exact where representable, else round to nearest even. |
| Aggregates | Ignore nulls, except `Count`. Over no rows, `Count` is 0 and others are null. `Sum` of `Int` is `Int` with overflow as an error; `Avg` is `Float`. `Percentile` is nearest-rank: the smallest value v with at least ⌈p·n⌉ values ≤ v. |

An evaluation error fails the whole request with `Evaluation`, and nothing
is applied.

**Index rule.** The predicate of every `Where` selection, claim filter and
declared read must be a conjunction (possibly of one term) in which at
least one term compares an indexed field with literals or parameters:
`=` or `In` for `Lookup` and `Unique`; also `<`, `<=`, `>`, `>=` for
`Range`; `Contains` for an indexed `TextSet`. Otherwise `SelectorNotIndexed`.
Other terms are evaluated as residual filters.

## Time, ordering and durability

### Time authority

1. Each space has one time authority. A request samples it once, after the
   implementation has serialized the request against conflicting requests
   (for example, after taking the row locks it needs).
2. All instances serving a space use the same authority.
3. Every implementation accepts an `Arc<dyn Clock>` at construction. Under
   conformance it is the authority. An implementation's own configuration
   may select another authority for production and must document it. The
   Postgres implementation uses `clock_timestamp()` read after its locks.
4. `claim` and `claim_by_item_ids` take an optional `eligibility_time`. It
   replaces the authority only when deciding whether `not_before` and retry
   backoff have passed for that claim. Lease expiry, attempt timestamps,
   cohort deadlines, retention, row expiry and `Now` always use the
   authority.
5. No other request takes a time that overrides the authority. API-001
   `MutateItems.evaluated_at` is removed; a fixed instant is a parameter.
6. Relative schedules (`Schedule::After`) are resolved against the authority
   at the linearization point.

```rust
pub trait Clock: Send + Sync { fn now(&self) -> UtcMillis; }
```

### Time-driven transitions

Observable state is a function of committed requests, each at its
linearization point, and time.

1. A time-driven transition takes effect at its due instant.
2. A request whose linearization point is at time `t` observes and acts on
   the state after every transition due at or before `t`.
3. Transitions due at the same instant are processed in this order: lease
   expiries, then cohort deadlines, then retention and row expiries. Within
   one kind: by object name, then by item id, cohort id or primary key.
4. Implementations may sweep, evaluate lazily, or both. No caller drives a
   tick, and conformance never calls one.

Applied to API-001:

- **Lease expiry** at `lease_expires_at`: the item is `pending` with no
  active lease. Its token is stale. `item_version` does not change.
  `attempt_count` follows API-001.
- **Cohort deadline**: at the deadline, if fewer than `cohort_size` members
  were committed at or before it, every member becomes `failed` with code
  `cohort-incomplete`. Whether the cohort was eligible or leased does not
  matter. (Changed from API-001; ADR-025 open question 7.)
- **Retention**: a request id older than `request_id_retention_ms` is
  forgotten. A tombstone or terminal item older than
  `client_item_key_retention_ms` stops being addressable by key. A row past
  its expiry is gone. An event log entry past retention is gone.
- **Recurrence**: a re-armed item's `not_before` is evaluated like any
  other.

### Linearization

1. Requests on one space are linearizable. A committed request takes effect
   at one point between its invocation and its response.
2. Every read observes every effect committed before its linearization
   point, across all shapes of the space.
3. No item has two active leases. A stale lease token is rejected.
4. A request whose future is dropped, or that ends in an indeterminate
   error (`CommitTimeout`, `Unavailable`), may commit later or never. A
   replay of its `request_id` returns the recorded result if it committed.
   Otherwise the replay runs it, and the original can then never commit.
5. A definitive error (every other error) means the request never commits.
6. Requests on different spaces have no ordering relationship.

### Replay

Within `request_id_retention_ms`, a repeated `request_id` in the same space
with the same body returns the recorded result, with these exceptions:

1. A claim replay after every lease it returned has ended fails with
   `RequestExpired`.
2. A rejected commit and a `dry_run` record nothing. Their replay runs them
   again.
3. Each step of a chunked `Mutate` is its own request with its own
   `request_id`.
4. A replay with a higher `min_durability` than the original waits until the
   committed result reaches that level, or fails with `CommitTimeout`.
5. The same `request_id` with a different body fails with
   `RequestIdConflict`.

### Durability

```rust
#[non_exhaustive]
pub enum DurabilityLevel { Process, Local, Replicated }  // ordered
```

| Level | Rule |
|-------|------|
| `Process` | Survives only as long as the instance. |
| `Local` | Survives a process crash and reopen of the implementation's own storage. On Postgres: flushed to the primary's WAL. |
| `Replicated` | Also survives loss of that storage: a copy outside it was durable before the response. On Postgres: a synchronous standby confirmed the flush. On the local engine: the commit is in the S3 log. |

Every committing response reports the level reached (`durability`). Every
write request may set `min_durability`. The response is then sent only once
that level is reached, or the request fails with `CommitTimeout` (the commit
may exist at a lower level). A `min_durability` above
`capabilities().max_durability` fails with `InvalidRequest`.

## Guarantees and capabilities

```rust
pub struct Guarantees {
    pub durability: DurabilityLevel,   // minimum for every acknowledged commit
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}
pub enum Visibility { Immediate, Bounded { max_ms: u64 } }
pub enum Sharing { SingleProcess, MultiProcess }

pub struct Capabilities {
    pub max_durability: DurabilityLevel,
    pub shapes: BTreeSet<ShapeKind>,          // Queue and Table always; EventLog, Blob
    pub queue_features: BTreeSet<QueueFeature>,
    pub queue_history: bool,
    pub priority_kinds: BTreeSet<PriorityModelKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub limits: ImplementationLimits,
}

pub enum QueueFeature { DynamicGates, MetadataBlockers, Recurrence, GroupBatching, Cohorts, TypedPayloadSchema }

pub struct ImplementationLimits {
    pub max_ops_per_commit: u32,
    pub max_entries_per_commit: u32,       // summed over ops; also per verb call
    pub max_commit_bytes: u64,
    pub max_body_bytes: u64,               // queue item and log entry bodies
    pub max_value_bytes: u64,              // any other single value
    pub max_set_values: u32,
    pub max_blob_bytes: u64,
    pub max_selector_affected: u32,        // per Mutate, and per chunk step
    pub max_dry_run_count: u64,
    pub max_read_rows: u32,                // per declared-read page
    pub max_aggregate_scan_rows: u64,
    pub max_claim_batch_size: u32,
    pub max_lease_duration_ms: u64,
    pub request_id_retention_ms: u64,
    pub default_max_gate_keys_per_item: u32,
    pub default_max_gates_per_request: u32,
}
```

`guarantees()` and `capabilities()` return the same values for the life of
the instance. v1 conformance defines `Immediate` only; no v1 implementation
declares `Bounded`.

| Sharing | Rule |
|---------|------|
| `MultiProcess` | Several instances, in one or many processes, may serve a space at once and together behave as one. |
| `SingleProcess` | At most one instance serves a space. Opening a second either fails, or supersedes the first, whose every later request fails with `InstanceSuperseded` and has no effect. The implementation documents which. |

## The trait

```rust
pub type DpResult<T> = Result<T, DataPlaneError>;
pub type DpFuture<'a, T> = Pin<Box<dyn Future<Output = DpResult<T>> + Send + 'a>>;

pub trait DataPlane: Send + Sync {
    fn guarantees(&self) -> Guarantees;
    fn capabilities(&self) -> Capabilities;

    // Schema
    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse>;
    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>>;

    // Hot-path verbs
    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse>;
    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse>;
    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse>;
    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse>;
    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse>;

    // Group commit
    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse>;

    // Reads
    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse>;
    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse>;
    fn read_log(&self, r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse>;
    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics>;
    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse>;

    // Blobs (capabilities().shapes contains Blob)
    fn put_blob(&self, r: PutBlobRequest) -> DpFuture<'_, PutBlobResponse>;
    fn get_blob(&self, r: GetBlobRequest) -> DpFuture<'_, Bytes>;
}
```

The trait is dyn-compatible; `Arc<dyn DataPlane>` is `Send + Sync`. A
method on a shape the implementation does not support fails with
`FeatureUnsupported`; `apply_schema` never accepts such an object.

Every write request starts with a header:

```rust
pub struct WriteHeader {
    pub space: SpaceKey,
    pub request_id: RequestId,
    pub min_durability: Option<DurabilityLevel>,
}
```

## Hot-path verbs

Each verb is equivalent to a `commit` of the matching operation, except that
its entries have independent outcomes, as API-001 specifies: one stale lease
or duplicate key affects only its own entry.

| Verb | API-001 operation | Request (minimum) | Response |
|------|-------------------|-------------------|----------|
| `enqueue` | BatchPush / upsert | header, `queue`, `items: Vec<NewItem>`, `on_key_conflict` | per item `EntryResult`, `durability` |
| `claim` | BatchClaim | header, `queue`, `worker_id`, `max_items`, `lease_duration_ms`, `eligibility_time`, `filter: Option<Expr>` (index rule; `params`), `group_key`, `same_group_key`, unit (`Items`, `WholeGroups { max_groups }`, `WholeCohort`) | `items: Vec<ClaimedItem>`, `cohort`, `queue_paused`, `as_of` |
| `claim_by_item_ids` | BatchClaimByItemIds | header, `queue`, `worker_id`, `item_ids`, `lease_duration_ms`, `eligibility_time` | per distinct id, in first-occurrence order: `ClaimedItem` or `NotFound`, `NotEligible`, `Leased`, `Terminal` |
| `renew` | BatchRenewLeases | header, `queue`, `leases: Vec<LeaseTarget>`, `lease_duration_ms` | per lease: status and new `lease_expires_at` |
| `finalize` | BatchFinalize | header, `queue`, `entries: Vec<FinalizeEntry>` | per entry `EntryResult`, `durability` |

```rust
pub struct NewItem {
    pub client_item_key: ClientItemKey,
    pub priority: PriorityValue,
    pub not_before: Option<Schedule>,
    pub group_key: Option<GroupKey>,
    pub cohort: Option<CohortSpec>,
    pub gate_keys: BTreeSet<String>,
    pub body: Option<Body>,                 // matches the queue's BodyKind
    pub fields: BTreeMap<FieldName, Value>, // Stored fields only
}
pub enum Body { Bytes(Bytes), Json(serde_json::Value) }
pub enum KeyConflict { Duplicate, ReplacePending, Reject }
pub enum Schedule { At(UtcMillis), After { delay_ms: u64 } }

pub enum ItemRef { Id(ItemId), Key(ClientItemKey), IdAndKey(ItemId, ClientItemKey) }
pub struct LeaseRef { pub item_id: ItemId, pub lease_token: LeaseToken }
pub struct CohortLeaseRef { pub cohort_id: CohortId, pub cohort_lease_token: LeaseToken }
pub enum LeaseTarget { Item(LeaseRef), Cohort(CohortLeaseRef) }

pub struct FinalizeEntry {
    pub target: LeaseTarget,
    pub expected_item_version: Option<u64>,
    pub outcome: FinalizeOutcome,
    pub metadata: Metadata,
    pub set: Vec<(FieldName, Expr)>,        // applied with the outcome
    pub patch: Vec<BodyPatch>,
    pub body: Option<Body>,                 // replaces the body
}
pub enum FinalizeOutcome {
    Complete,
    Fail { failure_code: String },
    Retry { not_before: Schedule, priority: Option<PriorityValue> },
    Release,
    Rearm { not_before: Schedule, priority: Option<PriorityValue> },
}
```

- `KeyConflict::Duplicate` is API-001 push dedupe: the existing item is
  returned with status `Duplicate`. `ReplacePending` replaces a pending item
  with the same key (new `item_id`; the old id reports `Superseded`); a
  leased item gives `Conflict`. `Reject` gives `Conflict` for any live or
  retained key.
- `ItemRef::IdAndKey` fails with `Conflict` unless both name the same item.
- `ClaimedItem` is API-001's claimed-item response shape, with the body and
  declared fields. It is the only response that carries a lease token.
- `claim` with `filter` is API-001 claim-by-query: the filter narrows the
  candidates; ordering is unchanged.

## Group commit

```rust
pub struct CommitRequest {
    pub header: WriteHeader,
    pub ops: Vec<WriteOp>,
    pub params: BTreeMap<ParamName, Value>,
    pub dry_run: bool,
}

pub enum WriteOp {
    Enqueue { queue: ObjectName, items: Vec<NewItem>, on_key_conflict: KeyConflict },
    Finalize { queue: ObjectName, entries: Vec<FinalizeEntry> },
    Renew { queue: ObjectName, leases: Vec<LeaseTarget>, lease_duration_ms: u64 },
    Reassign { queue: ObjectName, leases: Vec<LeaseRef>, to_worker: WorkerId, lease_duration_ms: u64 },
    SetGates { queue: ObjectName, gates: Vec<(String, GateState)> },
    SetQueueState { queue: ObjectName, paused: Option<bool>, drain_intake: Option<bool> },
    PutRows { table: ObjectName, rows: Vec<BTreeMap<FieldName, Value>>, on_conflict: RowConflict },
    Append { log: ObjectName, entries: Vec<LogEntry> },
    Mutate(Mutate),
}

pub enum RowConflict { Reject, Skip, Replace, Merge }
pub struct LogEntry { pub fields: BTreeMap<FieldName, Value>, pub body: Option<Body> }

pub struct Mutate {
    pub target: ObjectName,           // a queue or table
    pub selection: Selection,
    pub action: Action,
    pub limit: u32,                   // <= limits.max_selector_affected
    pub expect: Option<MatchExpect>,  // { min: u64, max: u64 } on the matched count
    pub lease_policy: LeasePolicy,    // queues only
    pub chunk: Option<Chunk>,
}

pub enum Selection {
    Keys { keys: Vec<Keyed>, filter: Option<Expr> },   // filter needs no index
    Where { predicate: Expr },                          // index rule applies
}
pub struct Keyed { pub key: KeyRef, pub expected_version: Option<u64> }
pub enum KeyRef { Item(ItemRef), Row(Vec<Value>) }

pub enum Action {
    Set(Vec<(FieldName, Expr)>),       // Stored fields; Expr may read the old value
    Patch(Vec<BodyPatch>),             // queue items with Json bodies
    Reschedule { not_before: Option<Expr>, priority: Option<Expr> },
    Transition(Transition),            // queue items
    Gates { add: BTreeSet<String>, remove: BTreeSet<String> },
    Retire,                            // queue: purge with tombstone; table: delete
}
pub enum Transition { Complete { code: Option<String> }, Fail { failure_code: String }, Redrive }

pub enum BodyPatch {
    Set { pointer: JsonPointer, value: Expr },
    Remove { pointer: JsonPointer },
    Append { pointer: JsonPointer, value: Expr, keep_last: Option<u32> },
}

pub enum LeasePolicy { Skip, Reject, Preserve, MatchToken(LeaseToken) }
pub enum Chunk { Start, Continue(Continuation) }
```

`params` supplies the values of every `Expr::Param` in the request.
`ClaimRequest` and `ReadRequest` carry the same field.

### Semantics

1. **Atomic.** All operations apply at one linearization point, in list
   order; each sees the effects of those before it. Either every operation
   applies or none does.
2. **Guards.** A group is rejected, and nothing is applied, if any of these
   holds for any entry: a stale or unknown lease token; an
   `expected_item_version` or `expected_version` that differs from the
   current one; `KeyConflict::Reject` or `RowConflict::Reject` meeting an
   existing key; a `MatchExpect` not met; matches over `limit` (without
   `chunk`); a leased match under `LeasePolicy::Reject`; a `BlobRef` naming
   a blob the space does not hold; a value over a size limit; an
   `ItemRef::IdAndKey` mismatch. Statuses that are not guards (`Duplicate`
   under `KeyConflict::Duplicate`, `Skip`ped rows, keys not found when
   there is no `expect`) do not reject.
3. **Rejection.** A rejected group returns `Ok` with `status: Rejected` and
   the failure of each failing entry. It writes nothing, including no
   request record.
4. **Lease policy** for a `Mutate` matching a leased item: `Skip` (default)
   leaves it out and counts it in `skipped_leased`; `Reject` rejects the
   group; `Preserve` applies `Set`, `Patch`, `Reschedule` and `Gates` while
   the lease stays with its token (a rescheduled `not_before` applies after
   the lease ends); `MatchToken` applies any action when the token matches
   and rejects otherwise. `Preserve` with `Transition` or `Retire` is
   `InvalidRequest`. Every applied `Mutate` on an item increments its
   `item_version` once.
5. **`Transition`** applies to pending items (and leased items under
   `MatchToken`). `Redrive` returns a terminal item within retention to
   `pending` with `attempt_count = 0`.
6. **`Retire`** on a queue item is API-001 `PurgeItems`, leaving a tombstone
   for key retention. On a table row it deletes the row.
7. **`Patch`** applies in order. `Append` adds to the array at `pointer`
   (creating it) and, with `keep_last`, drops the oldest elements beyond
   that count. A pointer through a non-object or non-array is an evaluation
   error.
8. **`dry_run`** evaluates everything, applies nothing, records nothing,
   and returns per-operation `matched` counts, exact up to
   `max_dry_run_count`.
9. **Chunking.** A `Mutate` with `chunk` must be the only operation in its
   commit and must use `Where`. Each step selects up to `limit` matching
   items or rows in key order (item id, or primary key) after the
   continuation, applies the action, and commits. The response carries the
   next `Continuation` while more may match. Each step re-evaluates the
   predicate at its own point. Items or rows the continuation has passed are
   not revisited.
10. **Durability.** One level covers the whole group.

```rust
pub struct CommitResponse {
    pub status: CommitStatus,               // Committed | Rejected | DryRun
    pub as_of: UtcMillis,
    pub durability: Option<DurabilityLevel>, // Committed only
    pub ops: Vec<OpResult>,                  // aligned with request ops
    pub continuation: Option<Continuation>,
}
pub struct OpResult {
    pub matched: u64,
    pub affected: u64,
    pub skipped_leased: u64,
    pub entries: Vec<EntryResult>,           // per entry or per addressed key
    pub failure: Option<OpFailure>,
}
pub struct EntryResult {
    pub status: EntryStatus,  // Accepted, Duplicate, Replaced, Updated, Completed, Failed,
                              // Retried, Released, Rearmed, Renewed, Reassigned, Purged,
                              // Deleted, Skipped, NotFound, Conflict, StaleLease,
                              // VersionMismatch, Terminal, Superseded, Invalid { reason }
    pub item_id: Option<ItemId>,
    pub version: Option<u64>,  // item_version or row_version after the op
    pub lease: Option<NewLease>, // Reassign: the new token and expiry
}
```

## Shapes

### Queue

API-001 semantics, with the changes in this contract: declared fields;
`Json` bodies patched by pointer; `claim` filters over declared fields;
`eligibility_time`; lease policies; the cohort deadline rule; selector
writes in place of `MutateItems`. Key retention
(`client_item_key_retention_ms`) is the dedupe window for event items
keyed by a provider's event id. Pause is observable: a claim on a paused
queue returns no items and `queue_paused = true`.

With `history` declared (and `capabilities().queue_history`), every
committed transition of an item appends one entry to the named event log in
the same commit: `item_id`, `client_item_key`, transition kind, outcome,
`item_version`, `attempt_count`. Time-driven transitions are not recorded
in v1.

### Table

Rows keyed by the declared primary key. `PutRows` inserts with the conflict
policy; `Merge` sets only the given fields of an existing row. `Mutate`
`Set` with `Arith` over the old value is an atomic increment. A conditional
increment or compare-and-swap is a `Mutate` with `Keys`, a `filter` (for
example `count < limit`) and `expect: { min: 1, max: 1 }`. `row_version`
increments on every change. A row past its expiry is invisible at once and
deleted later.

### Event log

```rust
pub struct ReadLogRequest {
    pub space: SpaceKey,
    pub log: ObjectName,
    pub from: LogCursor,           // Start | After(seq) | Partition { key: Value, after: Option<u64> }
    pub max_entries: u32,
}
```

1. Every entry gets a `seq`, unique in the log and increasing in commit
   order. A reader that has seen `seq = n` never later sees a new entry with
   a smaller `seq`. Gaps are allowed.
2. `read_log` returns entries after the cursor in `seq` order, filtered to
   one partition if asked, with the cursor to continue from.
3. A cursor before the earliest retained entry fails with
   `CursorExpired { earliest }`.
4. Consumers keep their cursors in a table and advance them in the same
   group commit as their processing.

### Blob

1. `put_blob(space, bytes, media_type, min_durability)` stores immutable
   bytes and returns `BlobRef { sha256, size }`. Putting the same bytes again
   returns the same reference. Up to `max_blob_bytes`.
2. A blob is referenced by any `BlobRef` field value in any shape of the
   space. References inside a JSON body do not count.
3. A commit that writes a `BlobRef` the space does not hold is rejected.
4. A blob with no reference is deleted `orphan_grace_ms` after its put or
   after its last reference was removed, whichever is later.
5. `get_blob` returns the bytes, or `BlobNotFound`.

## Reads

### Point reads

`get(space, targets)` reads several targets at one linearization point:
queue items by `ItemRef` (live items, plus terminal items within retention
when `include_terminal`), table rows by key, and blob metadata. Items are
returned as `ItemView`: system fields (lease summary, never the token),
declared fields and body. A target not found is `None`.

### Declared reads

```rust
pub struct ReadDecl {
    pub name: ReadName,
    pub version: u32,
    pub source: ObjectName,           // a queue, table or event log
    pub params: Vec<(ParamName, FieldType)>,
    pub filter: Expr,                 // index rule applies
    pub output: ReadOutput,
    pub order: Vec<(FieldName, Direction)>,
    pub max_rows: u32,                // <= limits.max_read_rows
}
pub enum ReadOutput {
    Rows { fields: Vec<FieldName> },
    Aggregate { group_by: Vec<(FieldName, Expr)>, values: Vec<(FieldName, Aggregate)> },
}

pub struct ReadRequest {
    pub space: SpaceKey,
    pub name: ReadName,
    pub version: u32,
    pub params: BTreeMap<ParamName, Value>,
    pub page_token: Option<PageToken>,
}
```

1. A read is declared in the schema and invoked by name and version. A
   declared version never changes. `ReadNotDeclared` otherwise.
2. A page is read at one linearization point and reports `as_of`. Pages of
   one read are not one snapshot; the page token continues in `order`, then
   key order.
3. An aggregate that would scan more than `max_aggregate_scan_rows` rows
   fails with `ScanLimitExceeded`.
4. Queue rows report `state` by the time rule: an item whose lease has
   expired reads as `pending`.
5. Built-in reads exist in every space: `fw_items` (v1, a queue's items by
   state, group or key), `fw_gates` (v1, blocked gates of a queue), and
   `fw_queue_counts` (v1, counts by state per queue). Their declarations are
   in the conformance crate and fixed per version.

Implementations backed by SQL may name a declared read `fw_read_v<N>_<name>`
internally. Consumers never see engine tables or views.

### Metrics and discovery

`metrics(space, queue)` returns `QueueMetrics` at one linearization point
(fields as revision 2: `as_of`, `pending`, `leased`, `complete`, `failed`,
`retry_backlog`, `oldest_eligible_age_ms`, `progress_bound_risk_count`,
`active_leases`, `recurring_pending`, `recurring_leased`).

- Lifecycle counts follow API-001's definitions: `pending` includes items
  not yet due and gated items; `leased` counts unexpired leases; terminal
  counts cover retention.
- `oldest_eligible_age_ms` and `progress_bound_risk_count` cover exactly the
  items a claim with no filter and no `eligibility_time` could return at
  that point, ignoring pause.

`discover_active_scopes` follows API-001, except that each returned scope
carries its own `observed_at`, and there is no response-wide `as_of`.
Queues the implementation cannot read are omitted and `partial` is set.
`authorize` is `Option<Arc<dyn Fn(&ObjectName) -> bool + Send + Sync>>`,
evaluated once per candidate queue before ranking.

## Errors

```rust
#[non_exhaustive]
pub enum DataPlaneError {
    SpaceNotFound, ObjectNotFound(ObjectName),
    SchemaConflict { current_version: Option<u64> },
    FeatureUnsupported(Unsupported), IndexNotReady(FieldName),
    UndeclaredField(FieldName), SelectorNotIndexed,
    ReadNotDeclared { name: ReadName, version: u32 }, ScanLimitExceeded,
    GatesNotEnabled, InvalidRequest(String), Evaluation(EvalError),
    BatchTooLarge(LimitKind),
    RequestIdConflict, RequestExpired,
    CursorExpired { earliest: u64 }, InvalidContinuation, BlobNotFound,
    CommitTimeout, RateLimited { retry_after_ms: Option<u64> }, Unavailable(String),
    InstanceSuperseded, Internal(String),
}
```

| Error | Kind | Retry |
|-------|------|-------|
| `SpaceNotFound`, `ObjectNotFound`, `UndeclaredField`, `ReadNotDeclared`, `FeatureUnsupported` | Definitive | After a schema change |
| `SchemaConflict` | Definitive | Re-read the schema |
| `IndexNotReady` | Definitive | Later, same `request_id` |
| `SelectorNotIndexed`, `GatesNotEnabled`, `InvalidRequest`, `Evaluation`, `BatchTooLarge`, `ScanLimitExceeded`, `InvalidContinuation` | Definitive | After fixing the request |
| `RequestIdConflict`, `RequestExpired` | Definitive | With a new `request_id` |
| `CursorExpired`, `BlobNotFound` | Definitive | No |
| `RateLimited` | Definitive | Same `request_id`, after `retry_after_ms` |
| `CommitTimeout`, `Unavailable` | Indeterminate | Same `request_id` |
| `InstanceSuperseded` | Definitive | No; use the new instance |
| `Internal` | Definitive: a broken invariant was detected and nothing was applied | No |

A rejected group is not an error. `QueueForbidden` is not an interface
error; authorization belongs to the wire adapter.

## Consumer inventory

What Snorri and Cayce would otherwise keep in memory, in opaque blobs or in
their own stores, and the shape that holds it.

### Snorri

From Snorri TD-012 revision 6 §3.7 (uncommitted). Rates are per recipient of
a single-send campaign.

| # | State | Size and rate | Shape and mechanism |
|---|-------|---------------|---------------------|
| 1 | Instance body: variables, node, waits, effect references, inbox, audit ring, engagement profile | Body up to 2 MiB; transition output up to 1 MiB; rewritten about 3 times per recipient plus once per profile update | Queue item with a `Json` body (the instance head, D8). Field-level changes are `Patch`; a profile update or inbox append is a `Mutate` with `Patch` and `LeasePolicy::Preserve`. Outputs above a threshold go to a blob, referenced by a `BlobRef` field. |
| 2 | Consumed inputs | Up to 16 in the body, each up to 64 KiB; one per external event | Event items in an inputs queue, finalized `Complete` in the transition's group; or kept in the body and trimmed with `Patch`. |
| 3 | Waits and await routing | Up to 128 per instance; names of 71 bytes | An indexed `TextSet` field on the instance item; routing is a `get` or read with `Contains`. |
| 4 | Effects: request, intent, receipt (the send-intent ledger) | Up to 256 KiB per effect item; intent commit about 1.2 KiB; 3 commits per send | Effect items in an effects queue. The intent commit sets `min_durability: Replicated`. |
| 5 | Handler idempotency: receipts, outcomes and fingerprints per idempotency key | Small; one write per handler call | Table keyed by idempotency key with `RowExpiry::AfterWrite`. |
| 6 | External events and dedupe | Up to 64 KiB per event; 31-day dedupe; at most 3 claims per event | Event items keyed by provider event id, with `client_item_key_retention_ms` of 31 days. |
| 7 | Audit and history | One per transition | Hash chain and last 16 summaries in the body (`Patch` `Append` with `keep_last: 16`). Full history is exhaust for Niflheim; an event log can serve as the outbox that ships it. |
| 8 | Definitions | 31–43 KB, up to 8 MiB; once per artifact per queue | Blob, referenced from a table row keyed by artifact digest and queue. |
| 9 | Admission in-flight chunk | About 100 B per run per shard; twice per chunk of up to 4,096 members | Table row keyed by run and shard, updated with `expected_version` (ADR-025 open question 6). |
| 10 | Rate-limit counters (today Dragonfly in Cayce) | One counter per slot key (account, contact, hour); one conditional increment per effect intent | Table rows keyed by slot and hour bucket with expiry; conditional increment as in Table (ADR-025 open question 5). |

Not shapes: the compiled WASM cache, the open-runs list and next-wake time
(derived, read through `fw_queue_counts` and `get`), and the old adapter's
caches.

### Cayce (draft; Cayce to confirm)

| State | Shape and mechanism |
|-------|---------------------|
| Per-recipient, per-run lifecycle | Queue item per recipient, with declared fields (campaign, step, domain, status) for claims, bulk changes and UI reads |
| Recipient profile, suppression flags | Table keyed by recipient |
| Frequency caps and send-rate windows | Table counters keyed by bucket |
| Delivery and engagement events | Event items (dedupe by provider id) that drive workflows; an event log for timelines and export |
| UI counts by state, lag, send rates | Declared aggregate reads over queue items, tables and event logs |
| Rendered messages and attachments | Blobs |

## Profiles

A profile names what a consumer needs: shapes, queue features, minimum
limits, and the maximum durability it will request. An implementation
satisfies a profile when `capabilities()` covers all of it.

| Profile | Shapes and features | Minimum limits | `max_durability` |
|---------|---------------------|----------------|------------------|
| `snorri` | Queue, Table, Blob; `DynamicGates`; `Json` bodies | `max_body_bytes` 2 MiB; `max_set_values` 128; `max_blob_bytes` 8 MiB; key retention 31 days | `Replicated` in production |
| `cayce-ui` | Queue, Table, EventLog; declared reads with aggregates | `max_read_rows` 1,000; `max_aggregate_scan_rows` 5,000,000 | — |
| `recipient-lifecycle` | Queue, Table; chunked `Mutate` | `max_entries_per_commit` 10,000; `max_selector_affected` 10,000 | — |

## Conformance

### Harness

```rust
pub trait Harness: Send + Sync + 'static {
    type Impl: DataPlane + 'static;

    /// Fresh, isolated storage, described by a serializable spec.
    fn create_storage(&self) -> BoxFuture<'_, Result<StorageSpec, HarnessError>>;

    /// An instance on existing storage, in this process.
    fn open<'a>(&'a self, storage: &'a StorageSpec, clock: SharedClock)
        -> BoxFuture<'a, Result<Self::Impl, HarnessError>>;

    /// A binary that serves `open` in a child process. Required for `Local`,
    /// `Replicated` and `MultiProcess`.
    fn child_binary(&self) -> Option<PathBuf>;

    /// Optional fault injection; unsupported faults skip their scenarios.
    fn inject(&self, instance: &Self::Impl, fault: Fault) -> Result<(), HarnessError> {
        Err(HarnessError::Unsupported)
    }

    /// Optional: destroy the local copy of the storage, for `Replicated`.
    fn lose_local_storage<'a>(&'a self, storage: &'a StorageSpec)
        -> BoxFuture<'a, Result<(), HarnessError>> {
        Box::pin(async { Err(HarnessError::Unsupported) })
    }
}

pub struct StorageSpec(serde_json::Value);   // opaque to the suite
pub enum Fault { BeforeCommit, AfterCommitBeforeResponse, StallCommit { ms: u64 } }
```

- `SharedClock` is a manual clock backed by a file the suite owns. Every
  process reads the same file, and only the suite advances it.
- The child binary's `main` calls `fireweed_conformance::child_main(harness)`,
  which opens the storage named on its command line and serves requests
  read as JSON from stdin, writing responses to stdout. The request types'
  serde form is the protocol.
- Skipped scenarios are reported by name; a release requires that none of
  the scenarios its declared guarantees need are skipped.

### Suites

```rust
pub async fn run<H: Harness>(harness: H);
```

The suite uses only `fireweed-interface`. It checks:

1. API-001 queue rules, as changed here, for each verb and each declared
   queue feature.
2. Group commit: atomicity, ordering within the group, every guard, the
   rejection response, nothing recorded on rejection, `dry_run`, and
   chunked steps.
3. Tables, event logs (ordering, cursor, partitions, retention,
   `CursorExpired`) and blobs (dedupe, reference rule, orphan deletion).
4. Expressions and aggregates: every row of the semantics table, including
   errors.
5. Schema: versioning, additive-only changes, `FeatureUnsupported`, the
   index rule, index state.
6. Time rules under `SharedClock` only, with no tick, including same-instant
   ordering and `eligibility_time`.
7. Linearization: concurrent claims never produce two active leases; reads
   observe prior commits across shapes.
8. Lost responses: with `AfterCommitBeforeResponse`, a replay returns the
   committed result; with `BeforeCommit`, a replay commits once.
9. `Local`: a child is killed (`SIGKILL`) around acknowledged commits, the
   storage is reopened in the parent, every acknowledged commit is present,
   and every `request_id` replays (subject to the replay exceptions).
10. `Replicated`: after `lose_local_storage`, reopening restores every
    commit acknowledged at `Replicated`.
11. `MultiProcess`: peers in separate child processes race claims and group
    commits on one space. `SingleProcess`: a second open fails or
    supersedes.

### Differential test

A test-only crate depends on the Postgres implementation and each other
implementation. It generates sequential histories (proptest) including
schemas, group commits, generated expressions, selector writes, aggregates
and clock advances, and applies each to both under `SharedClock`.

- Item ids are compared by the `client_item_key` they belong to, and lease
  tokens by the claim that returned them.
- Within a space, item ids increase in commit order, so id-ordered ties
  follow the history.
- Compared: every response status, count, version, claimed set and order,
  read rows and aggregates. `Float` results are compared with a relative
  tolerance of 1e-9.
- A difference is a defect in one of the two, settled by this contract and
  API-001; if both are silent, they are amended first.

## Planned implementations

| Implementation | Durability (min / max) | Visibility | Sharing | Shapes |
|----------------|------------------------|------------|---------|--------|
| PostgreSQL (shared server; new crate from TD-002) | `Local` / `Replicated` with a synchronous standby | `Immediate` | `MultiProcess` | all |
| Local engine (Turso MVCC on NVMe, S3 in the commit path; SQLite comparison arm) | `Replicated` / `Replicated` | `Immediate` | `SingleProcess` per space | all |
| In-memory (new crate) | `Process` / `Process` | `Immediate` | `SingleProcess` | all |

Implementation notes (normative for the named implementation):

- **Postgres.** A commit is one transaction. Guards are checked under row
  locks taken before time is sampled. Claims use `FOR UPDATE SKIP LOCKED`.
  Text columns use the `C` collation. Event log appends serialize per log on
  a head row so `seq` follows commit order. Declared fields become columns
  or expression indexes. Blobs are `bytea` rows with a reference count.
  Reads that span targets use one `REPEATABLE READ` snapshot.
- **Local engine.** One database per space (or per shard of spaces) with one
  writer. A request is one write transaction, acknowledged after its commit
  is in the S3 log. Lease expiry is lazy. Reads run on MVCC snapshots. Speed
  gates B1 to B4 apply.

## Facade binding (summary for API-005)

```rust
impl Fireweed {
    pub fn new(data_plane: Arc<dyn DataPlane>) -> Fireweed;
    pub fn guarantees(&self) -> Guarantees;
    pub fn capabilities(&self) -> Capabilities;
    // One method per DataPlane method, delegating.
    // Helpers: single-item enqueue; complete / fail / retry_after / release /
    // rearm over LeaseTargets (Schedule::After for relative times); typed
    // builders for SpaceSchema, Expr, Mutate and ReadRequest.
}
```

`Fireweed` exposes no implementation type, configuration or maintenance
method. `StorageConfig` and `RETIRED_STORAGE_CELL` leave the facade.

## Mapping from the current facade

`Fireweed` has 84 public methods today: 74 in `crates/fireweed/src/facade.rs`
and 10 `operator_*` methods in `crates/fireweed/src/operator.rs`.

| Current methods | New home | Behaviour change |
|-----------------|----------|------------------|
| `create_queue`, `ensure_queue`, `queue_definition` | `apply_schema`, `get_schema` | queues are declared in a space schema |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | `enqueue` (single-item forms are helpers) | none |
| `upsert` | `enqueue` with `KeyConflict::ReplacePending` | none |
| `batch_update` | `commit` with `Mutate` by `Keys` | per-key `expected_version` |
| `reschedule` | `Mutate` `Reschedule` with `LeasePolicy::Preserve` | none |
| `set_gates` | `commit` with `SetGates` | response drops command position |
| `purge` | `Mutate` `Retire` by `Keys` | none |
| `claim`, `claim_with`, `claim_response_with` | `claim` | none |
| `claim_at`, `claim_response_at` | `claim` with `eligibility_time` | lease time comes from the authority |
| `claim_by_query_at` | `claim` with `filter` and `eligibility_time` | filter needs an indexed field |
| `reclaim_expired_at` | removed | the time rule |
| `claim_by_item_ids` | `claim_by_item_ids` | none |
| `renew` | `renew` | token-addressed, per-item outcomes (was id-only, all-or-nothing) |
| `reassign` | `commit` with `Reassign` | token-addressed |
| `ack`, `complete`, `nack`, `retry`, `release`, `fail`, `rearm`, `nack_retry_after`, `retry_after`, `rearm_after`, `rearm_at` | `finalize` (helpers over `LeaseTarget`s and `Schedule`) | token-addressed, per-item (was id-only, all-or-nothing) |
| `live_item`, `live_items` | `get` | adds lease summary and declared fields |
| `claimed` | `read` of `fw_items` by `worker_id` | no lease tokens |
| `metrics` | `metrics` | adds API-001 fields |
| `discover_active_scopes`, `discover_active_scopes_stamped`, `discover` | `discover_active_scopes` | per-scope `observed_at`; `partial` |
| `mutate_items` | `commit` with `Mutate` operations | one selection per `Mutate`; no `evaluated_at` |
| `commit`, `commit_multi_claim` | `commit` | all-or-nothing group (was per-entry); item-version guard kept; side records become table rows |
| `explain_commit` | `commit` replay by `request_id` | — |
| `side_record`, `side_records`, `side_records_by_prefix` | `get` and `read` on a table | prefix reads need a `Range` index |
| `commit_capabilities` | removed | `capabilities()` |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | `read` (declared) or `get` by key | declared in the schema |
| `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `metrics_by_query` | `read` (declared) | declared in the schema |
| `bounded_mutation` | `Mutate` with `limit` or `chunk` | none |
| `claim_by_query` | `claim` with `filter` | none |
| `hot_projection_capabilities` | `capabilities()`, `get_schema` | — |
| `claim_across_queues` | facade helper: one `claim` per queue with a shared `eligibility_time` | no common lease instant |
| `operator_pause`, `operator_resume`, `operator_admin_state` | `SetQueueState`; `get_schema` | none |
| `operator_inspect_item`, `retained_items` | `get` with `include_terminal` | none |
| `operator_repair`, `operator_redrive` | `Mutate` with `Set` or `Transition::Redrive` | none |
| `operator_purge`, `operator_archive` | `Mutate` `Retire` | `max_affected` becomes `limit` with `expect` |
| `operator_get_operation` | replay by `request_id` | — |
| `operator_audit_records` | `read_log` on the queue's history log | needs `history` declared |
| `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `snapshot_now`, `latest_snapshot_info`, `compact_log_behind_snapshot`, `last_rebuild_stats`, `ownership`, `renew_owned`, `reclaim_expired`, `current_position`, `peek` | object-log implementation type | leave `Fireweed` (ADR-025 open question 3) |

## Compatibility

- Adding an enum variant, a struct field, a shape, a queue feature or a
  `DataPlane` method with a default body is compatible.
- Changing a signature, a request's meaning, or an expression rule is
  breaking.
- Changing an implementation's declared guarantees is breaking for that
  implementation.
- A declared read version and a built-in read version never change.

## Validation checklist

- [ ] The dependency test enforces rules 1, 4, 5 and 6.
- [ ] Every public type is owned by `fireweed-interface`,
      `#[non_exhaustive]`, constructible and serde-serializable.
- [ ] `DataPlane` is dyn-compatible.
- [ ] Every implementation passes `run` from its own tests with no skipped
      scenario its guarantees need.
- [ ] The differential test passes for every implementation against
      Postgres, including generated expressions and aggregates.
- [ ] Every Snorri inventory item (1 to 10) runs on the `snorri` profile with
      no Snorri-owned store.
- [ ] Each mapping-table behaviour change is in the API-005 migration notes.
- [ ] ADR-025's document changes have landed before this contract is
      accepted.
