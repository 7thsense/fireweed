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
**Version**: v0 (draft, revision 4)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, API-003, API-004, API-005, ADR-025, TD-002, TD-006

Revision 4 (2026-11-15) pins the request model for cross-engine behaviour:
expression evaluation, typing, casts and aggregates; scan budgets; the group
commit decision table; request records for rejected requests; the time and
isolation rules; durability levels per implementation; every referenced
type; and a conformance list that checks each rule. Text marked
**[draft pending owner]** follows a recommendation the owner has not ruled
on (ADR-025, Open questions).

## Purpose

This contract defines the Rust types and traits every Fireweed
implementation implements, and the rules that make implementations
interchangeable: the data model (spaces, shapes, schema), the request model
(verbs, write operations, expressions, reads), time and ordering,
durability, limits, errors, conformance, and the binding to the `Fireweed`
facade.

API-001 defines queue semantics (eligibility, leases, retries, gates,
groups, cohorts, recurrence). This contract refers to it for those and
states every change; ADR-025 lists the API-001 amendments.

## Scope and boundaries

- In scope: the `fireweed-interface` crate; the `DataPlane` trait; every
  request, response and schema type; the expression language; time,
  ordering, isolation and durability; limits; errors; conformance; the
  facade binding; rules each named implementation must follow.
- Out of scope: wire encodings beyond serde; an implementation's
  configuration, storage layout, recovery, replication and maintenance API;
  authentication and authorization (the adapter in front of the interface).
- Owning crate: `fireweed-interface` (working name).
- Non-goals: SQL; joins; a storage-port SPI; history and analytics beyond
  live data (Niflheim).

## Terms

| Term | Meaning |
|------|---------|
| Implementation | A type implementing `DataPlane`, in its own crate. |
| Instance | One constructed value of an implementation, attached to some storage. |
| Space | One atomic and ordering domain owned by one tenant, holding named objects. |
| Object | A queue, table or event log of a space, or its blob store. |
| Record | One queue item, table row, event log entry, cohort, gate, blob or request record. |
| Request | One call on `DataPlane`. A write request carries a `request_id`. |
| Commits | A write request takes effect at its linearization point. |
| Conflict | Two requests conflict if both touch a common record and at least one writes it. |
| Access term | The conjunct of a predicate that the index rule selects (§Index rule). |
| Candidate | A record that satisfies the access term. |
| Request time | The single instant from the time authority that a request uses for every decision. |

## Crate and dependency rules

1. `fireweed-interface` depends on `fireweed-core` id newtypes (`TenantId`,
   `ItemId`, `ClientItemKey`, `RequestId`, `LeaseToken`, `WorkerId`,
   `CohortId`, `GroupKey`) and on `serde`, `serde_json`, `bytes`, `futures`
   only.
2. No public item exposes an implementation's internal mechanism: command
   log, command position, projection, apply frontier, ownership epoch,
   storage cell. API-001 domain fields such as `gate_epoch` are allowed.
3. Every other public type is owned by `fireweed-interface`, is
   `#[non_exhaustive]`, has a constructor, and derives `Serialize`,
   `Deserialize`, `Clone`, `Debug` and `PartialEq`. No public type holds a
   function, closure or trait object, so every request can cross a process
   boundary.
4. An implementation crate depends on `fireweed-interface` and on no other
   implementation crate. It may depend on shared internal libraries.
5. `fireweed`, `fireweed-resp` and `fireweed-conformance` depend on no
   implementation crate outside `[dev-dependencies]`.
6. `fireweed-server` is the only published crate that names concrete
   implementations. Test-only crates (`publish = false`) may name several.

A workspace test enforces rules 1, 4, 5 and 6 from `cargo metadata`, and a
compile test checks rule 3.

## Data model

### Names

```rust
pub struct SpaceId(String);     // [a-z0-9][a-z0-9_-]{0,62}
pub struct SpaceKey { pub tenant_id: TenantId, pub space_id: SpaceId }
pub struct ObjectName(String);  // [a-z][a-z0-9_]{0,39}; prefix fw_ reserved
pub struct FieldName(String);   // [a-z][a-z0-9_]{0,39}; prefix fw_ reserved
pub struct ReadName(String);    // [a-z][a-z0-9_]{0,39}; prefix fw_ reserved for built-ins
pub struct ParamName(String);   // [a-z][a-z0-9_]{0,39}
pub struct JsonPointer(String); // RFC 6901
pub struct UtcMillis(i64);      // milliseconds since the Unix epoch, UTC
```

The 40-character limit keeps every derived engine identifier
(`fw_read_v<N>_<name>`, index names) within PostgreSQL's 63-byte limit.

### Schema

```rust
pub struct SpaceSchema {
    pub version: u64,                      // strictly increasing per space
    pub request_id_retention_ms: u64,      // <= limits.max_request_id_retention_ms
    pub queues: Vec<QueueDecl>,
    pub tables: Vec<TableDecl>,
    pub event_logs: Vec<EventLogDecl>,
    pub blobs: Option<BlobPolicy>,         // None: the space has no blob store
    pub reads: Vec<ReadDecl>,
}

pub struct FieldDecl {
    pub name: FieldName,
    pub ty: ScalarType,
    pub nullable: bool,
    pub source: FieldSource,
    pub index: IndexHint,
}

pub enum ScalarType {
    Bool, Int, Float, Text, Bytes, Timestamp, Json, TextSet, BlobRef, BlobRefSet,
}
pub enum FieldSource { Stored, BodyPointer(JsonPointer) }   // BodyPointer: derived, read-only
pub enum IndexHint { None, Lookup, Range, Unique }
pub enum BodyKind { Bytes, Json }

pub struct QueueDecl {
    pub name: ObjectName,
    pub spec: QueueSpec,
    pub body: BodyKind,
    pub fields: Vec<FieldDecl>,
    pub history: Option<ObjectName>,        // an event log for transition history
}

pub struct QueueSpec {
    pub priority_kind: PriorityKind,        // Timestamp | Int64
    pub ordering: OrderingMode,             // Strict | BestEffort
    pub max_attempts: u32,
    pub retry_backoff: Backoff,             // Fixed { ms } | Exponential { base_ms, max_ms }
    pub default_lease_duration_ms: u64,
    pub progress_bound_ms: Option<u64>,
    pub client_item_key_retention_ms: u64,
    pub terminal_retention_ms: u64,
    pub gates: Option<GatePolicy>,          // DynamicGates: max keys per item and per request
    pub metadata_blockers: BTreeMap<String, Vec<serde_json::Value>>, // API-001 eligibility_policy
    pub cohorts: Option<CohortPolicy>,      // completion_bound_ms
    pub group_batching: bool,
    pub recurrence: bool,
}

pub struct TableDecl {
    pub name: ObjectName,
    pub key: Vec<FieldName>,                // 1 to 4 fields
    pub fields: Vec<FieldDecl>,             // includes the key fields
    pub expiry: Option<RowExpiry>,
}
pub enum RowExpiry { AtField(FieldName), AfterWrite { ttl_ms: u64 } }

pub struct EventLogDecl {
    pub name: ObjectName,
    pub partition_key: FieldName,           // Text or Int, not nullable
    pub fields: Vec<FieldDecl>,
    pub body: BodyKind,
    pub retention_ms: u64,
}

pub struct BlobPolicy { pub orphan_grace_ms: u64 }
```

`QueueSpec` replaces `fireweed-core::QueueDefinition` at the interface. The
queue's identity is its `SpaceKey` and `ObjectName`. Index declarations live
only in `FieldDecl`. Request-id retention is per space. Where `QueueSpec`
and `ImplementationLimits` overlap, the stricter value applies, and
`apply_schema` rejects a spec that exceeds a limit.

Schema rules:

1. `apply_schema(space, schema, expected_version)` succeeds only if the
   stored version equals `expected_version` (`None` for a new space) and
   `schema.version` is greater. Otherwise `SchemaConflict`. Applying the
   identical schema at its stored version succeeds with no change.
2. v1 accepts additive changes only: new objects, new nullable fields, new
   indexes, new read versions, raised retention. Anything else is
   `SchemaConflict`.
3. A declaration needing an unsupported shape, feature or limit fails with
   `FeatureUnsupported` before anything changes.
4. Indexes may build after `apply_schema` returns. A request whose access
   term or declared read needs an index that is not ready fails with
   `IndexNotReady`. `get_schema` reports each index's state.
5. Table key fields are `Int`, `Text`, `Bytes` or `Timestamp`, not nullable,
   and immutable: no action may change them. `Unique` fields treat nulls as
   distinct. `TextSet`, `BlobRefSet`, `Json` and `Float` fields cannot be
   `Unique` or table keys. `AtField` names a nullable or non-null
   `Timestamp`; a null value never expires.
6. A `TextSet` or `BlobRefSet` holds at most `limits.max_set_values`
   distinct values. An indexed `TextSet` is indexed per value. `BlobRefSet`
   cannot be indexed.
7. A `BodyPointer` field is computed from the JSON body and read-only. If
   the pointer is absent or the value does not cast to the field's type
   (§Casts), the field is null.
8. Fields a write does not supply are null; a non-nullable field missing
   from a `PutRows` row or `NewItem` rejects that entry (`MissingField`).

### System fields

Always present, usable in expressions and reads, never set directly.

| Shape | Field | Type | Indexed |
|-------|-------|------|---------|
| Queue | `fw_item_id` | Int | Unique |
| Queue | `fw_key` (client item key) | Text | Unique among live and retained items |
| Queue | `fw_state` (`pending`, `leased`, `complete`, `failed`) | Text | Lookup |
| Queue | `fw_priority` | Int or Timestamp per `priority_kind` | — |
| Queue | `fw_not_before` | Timestamp | Range |
| Queue | `fw_group_key` | Text | Lookup |
| Queue | `fw_cohort_id` | Text | Lookup |
| Queue | `fw_attempt_count`, `fw_item_version` | Int | — |
| Queue | `fw_lease_expires_at` | Timestamp | — |
| Queue | `fw_worker_id` | Text | Lookup |
| Queue | `fw_created_at`, `fw_updated_at` | Timestamp | — |
| Queue | `fw_terminal_at` | Timestamp | Range |
| Queue | `fw_gate_keys` | TextSet | per value |
| Queue | `fw_metadata` | Json | — |
| Table | `fw_row_version` | Int | — |
| Table | `fw_created_at`, `fw_updated_at` | Timestamp | — |
| Event log | `fw_seq` | Int | Range |
| Event log | `fw_appended_at` | Timestamp | Range |

`fw_state`, `fw_lease_expires_at` and `fw_worker_id` are evaluated by the
time rule: an item whose lease expired at or before request time reads as
`pending`, with null lease fields.

### Values

```rust
pub enum Value {
    Null(ScalarType),
    Bool(bool), Int(i64), Float(f64), Text(String), Bytes(Bytes),
    Timestamp(UtcMillis), Json(serde_json::Value), TextSet(BTreeSet<String>),
    BlobRef(BlobRef), BlobRefSet(BTreeSet<BlobRef>),
}
pub struct BlobRef { pub sha256: [u8; 32], pub size: u64 }
```

`Float` values are finite. A null carries its type.

### Canonical JSON

Every JSON value Fireweed stores (bodies, `Json` fields, metadata) is
canonical:

1. Object keys are unique; a duplicate key rejects the write. Key order is
   not significant and is not preserved.
2. A number is stored as an `Int` if it is integral and within `i64`,
   otherwise as an `f64`. A number outside `f64` range, or an integral
   number outside `i64` with a fractional part lost, rejects the write.
3. Two JSON values are equal when they are equal after rule 2, with object
   keys compared as sets.

Encoding a `Value` into JSON (for `Patch` values): `Bool`, `Int`, `Float`
and `Text` map to their JSON forms; `Bytes` to base64 (standard, padded);
`Timestamp` to an RFC 3339 string `YYYY-MM-DDTHH:MM:SS.sssZ`; `TextSet` to a
sorted array of strings; `BlobRef` to `{"sha256": "<hex>", "size": n}`;
`BlobRefSet` to a sorted array of those; `Null` to `null`.

## Expressions

```rust
pub enum Expr {
    Lit(Value),
    Field(FieldName),
    Param(ParamName),
    Now,
    Json { field: FieldName, pointer: JsonPointer },
    Cast { expr: Box<Expr>, to: ScalarType },
    Cmp { op: CmpOp, l: Box<Expr>, r: Box<Expr> },
    And(Vec<Expr>), Or(Vec<Expr>), Not(Box<Expr>), IsNull(Box<Expr>),
    In { expr: Box<Expr>, list: Vec<Expr> },
    Contains { set: Box<Expr>, value: Box<Expr> },
    SetAdd { set: Box<Expr>, values: Vec<Expr> },
    SetRemove { set: Box<Expr>, values: Vec<Expr> },
    Arith { op: ArithOp, l: Box<Expr>, r: Box<Expr> },
    Coalesce(Vec<Expr>),
    Bucket { expr: Box<Expr>, width_ms: u64 },
}
pub enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }
pub enum ArithOp { Add, Sub, Mul, Div, Rem }

pub enum Aggregate {
    Count, CountOf(Expr), Sum(Expr), Min(Expr), Max(Expr), Avg(Expr),
    Percentile { expr: Expr, p_millionths: u32 },   // 1 ..= 1_000_000
}
pub enum Direction { Asc, Desc }
```

Every request carries `params: BTreeMap<ParamName, Value>`. A parameter's
type is the type of its `Value`, including a typed `Null`.

### Typing

Expressions are type-checked against the schema and parameters before
anything runs. A type error, an unknown field or a missing parameter fails
the request with `InvalidRequest` (definitive, not recorded).

| Expression | Operand types | Result |
|------------|---------------|--------|
| `Field`, `Param`, `Lit` | — | the declared or carried type |
| `Now` | — | Timestamp |
| `Json` | field of type Json | Json |
| `Cast` | per the cast table | target type |
| `Cmp` `Eq`, `Ne` | same type: Bool, Int, Float, Text, Bytes, Timestamp, BlobRef | Bool |
| `Cmp` `Lt`, `Le`, `Gt`, `Ge` | same type: Int, Float, Text, Bytes, Timestamp | Bool |
| `And`, `Or` | Bool, 1 to 64 operands | Bool |
| `Not` | Bool | Bool |
| `IsNull` | any | Bool (never null) |
| `In` | `expr` and every list item of one type allowed for `Eq`; 1 to 1,000 items | Bool |
| `Contains` | TextSet and Text, or BlobRefSet and BlobRef | Bool |
| `SetAdd`, `SetRemove` | TextSet with Text values, or BlobRefSet with BlobRef values | the set type |
| `Arith` `Add`, `Sub`, `Mul`, `Div` | Int and Int → Int; Float and Float → Float | as stated |
| `Arith` `Rem` | Int and Int | Int |
| `Arith` `Add` | Timestamp and Int (ms) | Timestamp |
| `Arith` `Sub` | Timestamp and Int → Timestamp; Timestamp and Timestamp → Int (ms) | as stated |
| `Coalesce` | 1 to 16 operands of one type | that type |
| `Bucket` | Timestamp; `width_ms` 1 to 315,576,000,000 (10 years) | Timestamp |

Json, TextSet and BlobRefSet values are not comparable with `Cmp`; Json is
`Cast` first. Bool is not ordered.

### Evaluation

1. **Order.** `And`, `Or` and `Coalesce` evaluate their operands left to
   right and stop at the first operand that decides the result (`false`,
   `true`, non-null). Other operators evaluate all operands.
2. **Null.** Any operator other than `And`, `Or`, `Not`, `IsNull`,
   `Coalesce`, `SetAdd` and `SetRemove` yields null if an evaluated operand
   is null. `And`, `Or` and `Not` follow SQL three-valued logic. `In` yields
   true if any item equals, else null if `expr` or any item is null, else
   false. `SetAdd` and `SetRemove` on a null set treat it as empty. A
   predicate selects a record only when it evaluates to true.
3. **Faults.** These are faults: `Int` overflow; `Div` or `Rem` by zero;
   a `Float` result that is NaN or infinite; `Bucket` overflow; a set that
   would exceed `max_set_values`. Where a fault occurs decides its effect:
   - in a predicate (selection, claim filter, read filter, `MatchExpect`
     filter) or a read output expression: the faulting expression yields
     null. No error is raised.
   - in a value written by an action (`Set`, `Patch`, `Reschedule`,
     `FinalizeEntry.set`): the group is rejected with `Evaluation`.
4. **Domain.** A predicate is evaluated only for candidates (records
   matching the access term), each independently. Because faults in
   predicates yield null, the result does not depend on the order in which
   an engine evaluates conjuncts or records.
5. **`Now`** is the request time, one value for the whole request.
6. **`Int` arithmetic** is 64-bit signed. `Div` truncates toward zero;
   `Rem` takes the sign of the dividend. `i64::MIN / -1` overflows.
7. **Text** compares by Unicode code point (equivalently, bytewise UTF-8).
   **Bytes** compare bytewise, unsigned, a proper prefix first.
8. **`Bucket`** is `floor(t / width_ms) * width_ms`, flooring toward
   negative infinity.

SQL implementations must compile error-capable operators so that the
engine cannot raise an error or change a result by reordering, for example
by guarding each `Div` with `CASE WHEN divisor = 0 THEN NULL`.

### Casts

| From \ To | Bool | Int | Float | Text | Timestamp | Json |
|-----------|------|-----|-------|------|-----------|------|
| Bool | = | — | — | `true` / `false` | — | JSON bool |
| Int | — | = | nearest, ties to even | decimal, no leading zeros | ms since epoch | JSON number |
| Float | — | truncate toward zero; null if out of `i64` | = | shortest round-trip decimal (Rust `{}`) | — | JSON number |
| Text | `true` / `false` exactly, else null | optional `-` and digits, in range, else null | decimal or exponent form, finite, else null | = | RFC 3339 (below), else null | JSON string |
| Timestamp | — | ms since epoch | — | RFC 3339 `…sssZ` | = | RFC 3339 string |
| Json | JSON bool, else null | JSON integral number in range, else null | JSON number, else null | JSON string, else null | JSON string per RFC 3339, else null | = |

`—` is a type error. RFC 3339 parsing accepts any offset, converts to UTC,
truncates fractional seconds to milliseconds, and yields null for a leap
second (`:60`) or a date outside `UtcMillis`. `Bytes`, `TextSet`, `BlobRef`
and `BlobRefSet` cast only to Json (§Canonical JSON).

### Aggregates

| Aggregate | Operand | Result |
|-----------|---------|--------|
| `Count` | — | Int: rows |
| `CountOf(e)` | any | Int: rows where `e` is not null |
| `Sum(e)` | Int | Int, accumulated exactly; a final value outside `i64` fails the read with `Evaluation` |
| `Sum(e)` | Float | Float (approximate, below) |
| `Min(e)`, `Max(e)` | Int, Float, Text, Bytes, Timestamp | operand type |
| `Avg(e)` | Int, Float | Float (approximate) |
| `Percentile` | Int, Float, Timestamp | operand type |

1. Aggregates ignore null operands. Over no non-null operands, `Count` and
   `CountOf` are 0; the others are null.
2. With `group_by` empty, an aggregate read returns one row, even over no
   input rows. With `group_by` non-empty, it returns one row per group that
   has at least one input row.
3. `Percentile` is nearest-rank over the n non-null values sorted
   ascending: the value at 1-based rank `r = ceil(p_millionths × n /
   1,000,000)` computed in integer arithmetic.
4. **Approximate results.** `Float` `Sum` and `Avg` may be computed in any
   order. Two implementations' results for n values `x` differ by at most
   `2 × n × 2⁻⁵³ × Σ|x|` (for `Avg`, divided by n). `Int` `Avg` is the exact
   quotient rounded to the nearest `f64`.

### Index rule and access term

The predicate of every `Where` selection, claim filter and declared read is
a conjunction (`And` of terms, or a single term). Its **access term** is the
first term, in list order, that compares an indexed field with literals or
parameters:

- `Eq` or `In` on a `Lookup`, `Unique` or `Range` field;
- `Lt`, `Le`, `Gt`, `Ge` on a `Range` field, or two such terms on the same
  field (both are then the access term);
- `Contains` on an indexed `TextSet`.

A predicate with no access term fails with `SelectorNotIndexed`. A `Keys`
selection needs no access term: its candidates are the addressed records.

## Work limits

Every request does bounded work on the writer.

| Limit | Counts | When exceeded |
|-------|--------|---------------|
| `max_ops_per_commit` | operations in a commit | `BatchTooLarge` (validation) |
| `max_entries_per_commit` | explicit entries over all operations: new items, finalize entries, leases, gates, rows, log entries, addressed keys | `BatchTooLarge` (validation) |
| `max_request_bytes` | serialized request size | `BatchTooLarge` (validation) |
| `max_in_list` | `In` items | `InvalidRequest` (validation) |
| `max_scan_rows` | candidates examined by one `Where` selection, claim filter, read page or aggregate | see below |
| `max_selector_affected` | records a `Where` `Mutate` matches (unchunked) or affects (per chunk step) | rejection `LimitExceeded`, or chunk step boundary |
| `max_body_bytes`, `max_value_bytes`, `max_set_values` | the resulting value after an action, not the request | rejection `ValueTooLarge` |
| `max_read_rows` | rows in one read page | page boundary |
| `max_claim_batch_size` | items one claim returns | `InvalidRequest` (validation) |

When `max_scan_rows` is reached:

- an unchunked `Mutate` rejects the group with `ScanLimitExceeded`;
- a chunk step ends there and returns a continuation after the last
  candidate examined, so every step makes progress even if nothing matched;
- a claim returns the items found so far and sets `scan_limited = true`;
- a read page ends there and returns a page token after the last candidate
  examined, with `scan_limited = true`;
- an aggregate read fails with `ScanLimitExceeded`.

Candidates are examined in access-term order: the access field's value,
ascending, then the record key (item id, primary key, `fw_seq`). Writes the
implementation makes on its own (history entries, index entries) do not
count against these limits; each implementation sets its limits to absorb
them.

## Time

### Time authority and request time

1. Each space has one time authority. Under conformance it is the injected
   `Clock`. An implementation's production configuration may select another
   authority and must document it.
2. Each request has one request time `t`. It is sampled once and used for
   every decision in the request: eligibility, lease expiry, cohort
   deadlines, retention, row expiry, `Now`, and timestamps written.
3. **Monotonic along conflicts.** If request B conflicts with request A and
   follows it in linearization order, then `t_B ≥ t_A`. An implementation
   achieves this by taking `t = max(sampled, latest timestamp written to any
   record it locks or reads for update)`, or, on a single writer, by keeping
   time non-decreasing for the space.
4. **Selection with time.** An implementation may sample a provisional time
   to choose candidates before it locks them, then fix `t` by rule 3. A
   record is selected only if it qualifies at `t`. Records that became
   eligible after the provisional sample may be missed by that request.
5. `claim` and `claim_by_item_ids` take an optional `eligibility_time`. It
   replaces `t` only when deciding whether `fw_not_before` and retry backoff
   have passed. It may be earlier or later than `t`. Everything else uses
   `t`. **[draft pending owner: Snorri O2]**
6. No other request overrides time. A fixed instant in a predicate is a
   parameter.
7. A `Clock` that moves backward does not move request time backward
   (rule 3 holds).

```rust
pub trait Clock: Send + Sync { fn now(&self) -> UtcMillis; }
```

### Time-driven transitions

Observable state is a function of committed requests and time.

1. A time-driven transition takes effect at its due instant.
2. A request with request time `t` acts on the state after every transition
   due at or before `t`.
3. Transitions due at the same instant are processed in this order: lease
   expiries, then cohort deadlines, then retention and row expiries. Within
   one kind: by object name, then item id, cohort id or primary key.
4. Implementations may sweep, evaluate lazily, or both. Before a request
   acts on a record, the implementation processes every transition of that
   record due at or before `t`. No caller drives a tick.

Applied to API-001:

- **Lease expiry** at `lease_expires_at`: the item is `pending`, its token is
  stale, `item_version` does not change, `attempt_count` follows API-001.
- **Cohort deadline** **[draft pending owner]**. The deadline is API-001's
  `min(cohort_created_at, first_eligible_at) + completion_bound_ms`. At the
  deadline, unless the cohort has been claimed at least once, the cohort
  fails if fewer than `cohort_size` of its members are non-terminal, or if
  any non-terminal member is not eligible (API-001 Eligibility Precedence
  A) at that instant. A failed cohort's non-terminal members become
  `failed` with code `cohort-incomplete`. A member enqueued after its
  cohort failed is rejected (`CohortFailed`). An implementation processes a
  queue's due cohort deadlines before any request that touches the cohort,
  its members, or the gates its members carry.
- **Retention**: a request record older than `request_id_retention_ms` is
  forgotten. A terminal item or tombstone older than its retention stops
  being addressable. A row is gone at its expiry. An event log entry is
  gone `retention_ms` after `fw_appended_at`.
- **Recurrence**: a re-armed item's `not_before` is evaluated like any
  other.

## Ordering, request records and replay

### Linearization

1. Requests on one space are linearizable. A committed request takes effect
   at one point between its invocation and its response. Every read
   observes every request committed before its linearization point, across
   all shapes of the space.
2. No item has two active leases. A stale lease token never succeeds.
3. Requests on different spaces have no ordering relationship.

### Request records

1. Every write request with an outcome (`Committed` or `Rejected`) leaves a
   request record in its space: the request fingerprint and the response.
   **[draft pending owner: rejected requests recorded]**
2. The fingerprint is the SHA-256 of the canonical JSON serialization of the
   request with `min_durability` removed.
3. A request that fails validation (definitive `Err`) leaves no record.
   `dry_run` requests leave no record and do not use their `request_id`.
4. Recording is atomic with the outcome. Once a request id has a record, no
   other attempt with that id can commit.
5. `get_request(space, request_id)` returns the record, or `None`, and never
   executes anything.

### Replay

Within `request_id_retention_ms`, a request whose `request_id` has a record
returns the recorded response, with these exceptions:

1. A claim replay after every lease it returned has ended fails with
   `RequestExpired`. A claim that returned no items replays its empty
   response.
2. The response's `durability` reports the level the commit has reached
   now, not when it was recorded. A replay with a higher `min_durability`
   waits for that level (§Durability).
3. A different fingerprint fails with `RequestIdConflict`.
4. Each step of a chunked `Mutate` is its own request with its own
   `request_id`.

A request id with no record runs as a new request. After the retention
window, a repeated id is a new request.

### Lost responses

A request whose future is dropped, or that ends in an indeterminate error,
may still commit, or be rejected and recorded. The caller learns the
outcome by replaying the same request or by `get_request`.

## Durability

```rust
pub enum DurabilityLevel { Process, Local, Replicated }   // ordered
```

| Level | Rule |
|-------|------|
| `Process` | Survives only as long as the instance. |
| `Local` | Survives a process crash and a reopen of the implementation's own storage. |
| `Replicated` | Also survives loss of that storage: a copy outside it was durable before the response. |

1. Every committing response, and every claim, renew and finalize response,
   reports `durability`: the level reached when the response was sent.
2. Every write request may set `min_durability`. A level above
   `capabilities().max_durability` fails with `InvalidRequest`.
3. With `min_durability`, the response is sent once the commit reaches that
   level. If it is committed but has not reached the level within the
   implementation's documented wait, the request fails with
   `DurabilityPending { reached }`: the commit exists, at `reached`. A
   replay waits again.
4. `CommitTimeout` means the implementation does not know whether the
   request committed.
5. Reads observe committed requests whatever their level. After the loss
   of a `Local` copy, effects that were read but never reached
   `Replicated` can disappear.
6. A blob referenced by a commit is brought to the commit's level before the
   commit reports that level.

Levels per implementation:

| Implementation | Minimum | Maximum | Notes |
|----------------|---------|---------|-------|
| PostgreSQL | `Local` (flushed to the primary's WAL) | `Replicated` only with a synchronous standby; otherwise `Local` | `Replicated` means a synchronous standby confirmed the WAL flush. **[draft pending owner: Snorri O15]** |
| Local engine | `Local` (fsynced locally) | `Replicated` (the commit is in the S3 log) | Commits ship to S3 in batches; `min_durability: Replicated` waits for the batch. **[draft pending owner]** |
| In-memory | `Process` | `Process` | — |

## Guarantees and capabilities

```rust
pub struct Guarantees {
    pub durability: DurabilityLevel,          // minimum for every acknowledged commit
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}
pub enum Visibility { Immediate, Bounded { max_ms: u64 } }
pub enum Sharing { SingleProcess, MultiProcess }

pub struct Capabilities {
    pub max_durability: DurabilityLevel,
    pub shapes: BTreeSet<ShapeKind>,          // Queue and Table always
    pub queue_features: BTreeSet<QueueFeature>,
    pub queue_history: bool,
    pub priority_kinds: BTreeSet<PriorityKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub limits: ImplementationLimits,
}
pub enum ShapeKind { Queue, Table, EventLog, Blob }
pub enum QueueFeature { DynamicGates, MetadataBlockers, Recurrence, GroupBatching, Cohorts }

pub struct ImplementationLimits {
    pub max_ops_per_commit: u32,
    pub max_entries_per_commit: u32,
    pub max_request_bytes: u64,
    pub max_in_list: u32,
    pub max_scan_rows: u64,
    pub max_selector_affected: u32,
    pub max_body_bytes: u64,
    pub max_value_bytes: u64,
    pub max_set_values: u32,
    pub max_blob_bytes: u64,
    pub max_dry_run_count: u64,
    pub max_read_rows: u32,
    pub max_claim_batch_size: u32,
    pub max_lease_duration_ms: u64,
    pub max_request_id_retention_ms: u64,
    pub max_objects_per_space: u32,
}
```

`guarantees()` and `capabilities()` return the same values for the life of
the instance. v1 conformance defines `Immediate` only; no v1 implementation
declares `Bounded`.

| Sharing | Rule |
|---------|------|
| `MultiProcess` | Several instances, in one or many processes, may serve a space at once and together behave as one. |
| `SingleProcess` | At most one live instance serves a space. Opening a second while the first lives either fails with `SpaceBusy` or supersedes the first, whose later requests fail with `InstanceSuperseded` and have no effect. After the holding process dies, a new open succeeds. The implementation documents which it does. |

## The trait

```rust
pub type DpResult<T> = Result<T, DataPlaneError>;
pub type DpFuture<'a, T> = Pin<Box<dyn Future<Output = DpResult<T>> + Send + 'a>>;

pub trait DataPlane: Send + Sync {
    fn guarantees(&self) -> Guarantees;
    fn capabilities(&self) -> Capabilities;

    fn apply_schema(&self, r: ApplySchemaRequest) -> DpFuture<'_, ApplySchemaResponse>;
    fn get_schema(&self, space: SpaceKey) -> DpFuture<'_, Option<SchemaState>>;

    fn enqueue(&self, r: EnqueueRequest) -> DpFuture<'_, EnqueueResponse>;
    fn claim(&self, r: ClaimRequest) -> DpFuture<'_, ClaimResponse>;
    fn claim_by_item_ids(&self, r: ClaimByItemIdsRequest) -> DpFuture<'_, ClaimByItemIdsResponse>;
    fn renew(&self, r: RenewRequest) -> DpFuture<'_, RenewResponse>;
    fn finalize(&self, r: FinalizeRequest) -> DpFuture<'_, FinalizeResponse>;

    fn commit(&self, r: CommitRequest) -> DpFuture<'_, CommitResponse>;
    fn get_request(&self, r: GetRequestRequest) -> DpFuture<'_, Option<RequestRecord>>;

    fn get(&self, r: GetRequest) -> DpFuture<'_, GetResponse>;
    fn read(&self, r: ReadRequest) -> DpFuture<'_, ReadResponse>;
    fn read_log(&self, r: ReadLogRequest) -> DpFuture<'_, ReadLogResponse>;
    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics>;
    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse>;

    fn put_blob(&self, r: PutBlobRequest) -> DpFuture<'_, PutBlobResponse>;
    fn get_blob(&self, r: GetBlobRequest) -> DpFuture<'_, Bytes>;
}
```

The trait is dyn-compatible; `Arc<dyn DataPlane>` is `Send + Sync`. A
method for a shape the implementation does not support fails with
`FeatureUnsupported`.

## Request and response types

### Common

```rust
pub struct WriteHeader {
    pub space: SpaceKey,
    pub request_id: RequestId,
    pub min_durability: Option<DurabilityLevel>,
    pub actor: Option<String>,     // recorded in history entries; not authorization
    pub reason: Option<String>,
}

pub enum ItemRef { Id(ItemId), Key(ClientItemKey), IdAndKey(ItemId, ClientItemKey) }
pub struct LeaseRef { pub item_id: ItemId, pub lease_token: LeaseToken }
pub struct CohortLeaseRef { pub cohort_id: CohortId, pub cohort_lease_token: LeaseToken }
pub enum LeaseTarget { Item(LeaseRef), Cohort(CohortLeaseRef) }
pub enum Schedule { At(UtcMillis), After { delay_ms: u64 } }   // After: from request time
pub enum GateState { Open, Blocked }
pub type Metadata = BTreeMap<String, serde_json::Value>;        // canonical JSON scalars
pub enum Body { Bytes(Bytes), Json(serde_json::Value) }
pub struct CohortSpec { pub cohort_id: CohortId, pub cohort_size: u32 }
pub struct PageToken(String);       // opaque
pub struct Continuation(String);    // opaque; bound to its request (below)

pub struct EntryResult {
    pub status: EntryStatus,
    pub item_id: Option<ItemId>,
    pub version: Option<u64>,        // item_version or fw_row_version after the entry
    pub lease: Option<NewLease>,     // claim, renew, reassign
}
pub struct NewLease { pub lease_token: LeaseToken, pub lease_expires_at: UtcMillis, pub item_version: u64 }

pub enum EntryStatus {
    Accepted, Duplicate, Replaced, Updated, Completed, Failed, Retried, Released,
    Rearmed, Renewed, Reassigned, Redriven, Purged, Inserted, Deleted, Appended,
    Skipped, Inapplicable, NotFound,
    // only as per-entry statuses of verbs, or in OpFailure of a rejected group:
    Conflict, StaleLease, VersionMismatch, Terminal, Superseded, NotEligible, Leased,
    CohortFailed, MissingField, ValueTooLarge, BlobMissing, Invalid { reason: String },
}
```

### Schema

```rust
pub struct ApplySchemaRequest { pub space: SpaceKey, pub schema: SpaceSchema, pub expected_version: Option<u64> }
pub struct ApplySchemaResponse { pub version: u64, pub changed: bool }
pub struct SchemaState { pub schema: SpaceSchema, pub indexes: Vec<IndexState> }
pub struct IndexState { pub object: ObjectName, pub field: FieldName, pub ready: bool }
```

### Hot-path verbs

Each verb's entries have independent outcomes, as API-001 specifies: one
stale lease or duplicate key affects only its own entry. Each verb leaves a
request record.

```rust
pub struct EnqueueRequest {
    pub header: WriteHeader, pub queue: ObjectName,
    pub items: Vec<NewItem>, pub on_key_conflict: KeyConflict,
}
pub struct EnqueueResponse { pub entries: Vec<EntryResult>, pub durability: DurabilityLevel }

pub struct NewItem {
    pub client_item_key: Option<ClientItemKey>,   // None: no dedupe; key = item id
    pub priority: PriorityValue,                   // Int(i64) | Timestamp(UtcMillis)
    pub not_before: Option<Schedule>,
    pub group_key: Option<GroupKey>,
    pub cohort: Option<CohortSpec>,
    pub gate_keys: BTreeSet<String>,
    pub metadata: Metadata,
    pub body: Option<Body>,
    pub fields: BTreeMap<FieldName, Value>,        // Stored fields only
}
pub enum KeyConflict { Duplicate, ReplacePending, Reject }

pub struct ClaimRequest {
    pub header: WriteHeader, pub queue: ObjectName,
    pub worker_id: WorkerId, pub max_items: u32, pub lease_duration_ms: u64,
    pub eligibility_time: Option<UtcMillis>,
    pub filter: Option<Expr>, pub params: BTreeMap<ParamName, Value>,
    pub group_key: Option<GroupKey>, pub same_group_key: bool,
    pub unit: ClaimUnit,                           // Items | WholeGroups { max_groups } | WholeCohort
}
pub struct ClaimResponse {
    pub items: Vec<ClaimedItem>, pub cohort: Option<CohortLease>,
    pub queue_paused: bool, pub scan_limited: bool,
    pub as_of: UtcMillis, pub durability: DurabilityLevel,
}
pub struct ClaimedItem {
    pub item_id: ItemId, pub client_item_key: ClientItemKey,
    pub lease_token: LeaseToken, pub lease_expires_at: UtcMillis,
    pub item_version: u64, pub attempt_count: u32,
    pub priority: PriorityValue, pub not_before: Option<UtcMillis>,
    pub group_key: Option<GroupKey>, pub cohort_id: Option<CohortId>,
    pub gate_keys: BTreeSet<String>, pub metadata: Metadata,
    pub body: Option<Body>, pub fields: BTreeMap<FieldName, Value>,
}
pub struct CohortLease {
    pub cohort_id: CohortId, pub cohort_lease_token: LeaseToken,
    pub lease_expires_at: UtcMillis, pub member_ids: Vec<ItemId>,
}

pub struct ClaimByItemIdsRequest {
    pub header: WriteHeader, pub queue: ObjectName, pub worker_id: WorkerId,
    pub item_ids: Vec<ItemId>, pub lease_duration_ms: u64,
    pub eligibility_time: Option<UtcMillis>,
}
pub struct ClaimByItemIdsResponse {
    pub results: Vec<(ItemId, Result<ClaimedItem, EntryStatus>)>,  // distinct ids, first-occurrence order
    pub durability: DurabilityLevel,
}

pub struct RenewRequest {
    pub header: WriteHeader, pub queue: ObjectName,
    pub leases: Vec<LeaseTarget>, pub lease_duration_ms: u64,
}
pub struct RenewResponse { pub entries: Vec<EntryResult>, pub durability: DurabilityLevel }

pub struct FinalizeRequest { pub header: WriteHeader, pub queue: ObjectName, pub entries: Vec<FinalizeEntry> }
pub struct FinalizeResponse { pub entries: Vec<EntryResult>, pub durability: DurabilityLevel }

pub struct FinalizeEntry {
    pub target: LeaseTarget,
    pub expected_item_version: Option<u64>,         // item target only
    pub outcome: FinalizeOutcome,
    pub metadata: Option<Metadata>,                  // replaces item metadata
    pub body: Option<Body>,                          // replaces the body, then:
    pub patch: Vec<BodyPatch>,                       // then:
    pub set: Vec<(FieldName, Expr)>,
}
pub enum FinalizeOutcome {
    Complete,
    Fail { failure_code: String },
    Retry { not_before: Schedule, priority: Option<PriorityValue> },
    Release,
    Rearm { not_before: Schedule, priority: Option<PriorityValue> },
}
```

Rules:

- `KeyConflict::Duplicate` is API-001 push dedupe: the existing item's id
  with status `Duplicate`. `ReplacePending` replaces a pending item with the
  same key (new `item_id`; later use of the old id reports `Superseded`); a
  leased item gives `Conflict`. `Reject` gives `Conflict` for any live or
  retained key.
- `ItemRef::IdAndKey` gives `Conflict` unless both name the same item.
- `ClaimedItem` is the only type that carries a lease token.
- A `FinalizeEntry` applies, in order: `metadata`, `body`, `patch`, `set`,
  then the outcome. `expected_item_version` is compared before any of them.
- A cohort target applies its outcome to every member.

### Group commit

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
    pub target: ObjectName,                 // a queue or table
    pub selection: Selection,
    pub action: Action,
    pub limit: u32,                          // 1 ..= max_selector_affected
    pub expect: Option<MatchExpect>,
    pub lease_policy: LeasePolicy,           // queues only
    pub chunk: Option<Chunk>,
}
pub struct MatchExpect { pub min: u64, pub max: u64 }   // on `matched`, below

pub enum Selection {
    Keys { keys: Vec<Keyed>, filter: Option<Expr> },
    Where { predicate: Expr },
}
pub struct Keyed { pub key: KeyRef, pub expected_version: Option<u64> }  // Some(0): must not exist
pub enum KeyRef { Item(ItemRef), Row(Vec<Value>) }

pub enum Action {
    Set(Vec<(FieldName, Expr)>),
    Patch(Vec<BodyPatch>),
    Reschedule { not_before: Option<Expr>, priority: Option<Expr> },
    Transition(Transition),
    Gates { add: BTreeSet<String>, remove: BTreeSet<String> },
    Retire,
}
pub enum Transition {
    Complete { code: Option<String> },
    Fail { failure_code: String },
    Retry { not_before: Expr },
    Release,
    Redrive,
}
pub enum BodyPatch {
    Set { pointer: JsonPointer, value: Expr },
    Remove { pointer: JsonPointer },
    Append { pointer: JsonPointer, value: Expr, keep_last: Option<u32> },
}
pub enum LeasePolicy { Skip, Reject, Preserve, MatchToken(LeaseToken), Break }
pub enum Chunk { Start, Continue(Continuation) }

pub struct CommitResponse {
    pub status: CommitStatus,                // Committed | Rejected | DryRun
    pub as_of: UtcMillis,                    // request time
    pub durability: Option<DurabilityLevel>, // Committed only
    pub ops: Vec<OpResult>,                  // aligned with request ops
    pub failure: Option<OpFailure>,          // Rejected only
    pub continuation: Option<Continuation>,
}
pub struct OpResult {
    pub evaluated: bool,                     // false after the failing op
    pub matched: u64,
    pub affected: u64,
    pub skipped_leased: u64,
    pub inapplicable: u64,
    pub entries: Vec<EntryResult>,           // explicit entries and addressed keys
}
pub struct OpFailure {
    pub op_index: u32,
    pub entry_index: Option<u32>,
    pub reason: FailureReason,
}
pub enum FailureReason {
    Entry(EntryStatus),                      // a rejecting entry status
    MatchExpectUnmet { matched: u64 },
    LimitExceeded { matched_at_least: u64 },
    ScanLimitExceeded,
    LeasedMatch,
    CohortSplit { cohort_id: CohortId },
    Evaluation(EvalError),
}
pub enum EvalError { IntOverflow, DivideByZero, NonFiniteFloat, BucketOverflow, SetTooLarge, PointerConflict }
```

#### Phases

A commit runs in two phases.

**Validation** (before any state is read): typing, schema references,
access terms, lease-policy combinations, chunk rules, parameter types, and
the validation caps. A failure is a definitive `Err`; nothing is recorded.

**Execution** at one linearization point with one request time:

1. Operations run in list order. Each sees the effects of the operations
   before it.
2. Within an operation, entries run in list order, and a `Where` selection's
   matches are processed in access-term order.
3. The first rejecting condition (table below) stops execution. Nothing is
   applied. The response has `status: Rejected`, `failure` naming the
   operation, entry and reason, `evaluated = true` for operations up to and
   including the failing one, and `evaluated = false` after it. Counts in
   `ops` describe the evaluation up to the failure.
4. Otherwise every effect applies, with `status: Committed`.
5. Either outcome leaves a request record.

#### Rejecting conditions

| Operation | Rejects the group | Does not reject (recorded as entry status) |
|-----------|-------------------|--------------------------------------------|
| `Enqueue` | `Conflict` (`Reject` policy, or `ReplacePending` on a leased item); `CohortFailed`; `MissingField`; `ValueTooLarge`; `BlobMissing`; gate keys over the queue's limit (`Invalid`) | `Duplicate`, `Replaced`, `Accepted` |
| `Finalize` | `StaleLease`; `VersionMismatch`; `Terminal`; `Superseded`; an `Evaluation` fault in `set` or `patch`; `ValueTooLarge` | — |
| `Renew`, `Reassign` | `StaleLease`; `Terminal` | — |
| `SetGates`, `SetQueueState` | none at execution | — |
| `PutRows` | `Conflict` (`Reject` policy); `MissingField`; `ValueTooLarge`; `BlobMissing` | `Skipped` (`Skip` policy), `Inserted`, `Updated` |
| `Append` | `MissingField`; `ValueTooLarge`; `BlobMissing` | `Appended` |
| `Mutate` | `VersionMismatch` on a `Keyed` guard (including `Some(0)` on an existing record); `MatchExpectUnmet`; `LimitExceeded` (unchunked); `ScanLimitExceeded` (unchunked); `LeasedMatch` (`Reject` policy); `StaleLease` (`MatchToken` mismatch); `CohortSplit`; an `Evaluation` fault in an action value; `ValueTooLarge`; `BlobMissing`; `Conflict` from `IdAndKey` | `NotFound` for an addressed key; `Skipped` leased items; `Inapplicable` |

#### Mutate semantics

1. **Counting.** `matched` counts records that satisfy the selection
   (addressed keys found, and their `filter` if any), before leased items
   are skipped. A key repeated in `Keys` counts once and is processed once.
   `MatchExpect` and `limit` apply to `matched`. `affected` counts records
   the action changed. `skipped_leased` and `inapplicable` count the rest.
2. **Lease policy** for a matched leased item:
   - `Skip` (default): not affected.
   - `Reject`: rejects the group.
   - `Preserve`: `Set`, `Patch`, `Reschedule` and `Gates` apply; the lease
     and token stay. A rescheduled `not_before` applies after the lease
     ends. With `Transition` or `Retire`: validation error.
   - `MatchToken`: any action applies when the token matches; a mismatch
     rejects. Only with a single-key `Keys` selection.
   - `Break`: the lease ends (its token becomes stale, no attempt is
     charged), then the action applies. For operator force actions
     (API-002).
3. **Inapplicable.** A record on which the action has no meaning is
   `Inapplicable`: a `Transition` other than `Redrive` on a terminal item, a
   `Redrive` on a live item, `Reschedule` or `Gates` on a terminal item.
4. **`Set`** evaluates every right-hand side against the record as it was
   before the action, then assigns. It may not name a system field, a
   `BodyPointer` field or a table key field.
5. **`Transition`.** `Complete` and `Fail` make the item terminal. `Retry`
   returns it to `pending` with `not_before` and charges nothing. `Release`
   returns a leased item to `pending` (needs `MatchToken` or `Break`).
   `Redrive` returns a terminal item within retention to `pending` with
   `attempt_count = 0`.
6. **`Retire`** on a queue item is API-001 `PurgeItems`: the item becomes a
   tombstone for key retention. On a table row, it deletes the row.
7. **Cohorts.** An action that would make some but not all non-terminal
   members of a cohort terminal, or move some but not all of them between
   live states, rejects the group with `CohortSplit`, unless the selection
   includes every non-terminal member. A chunk step that would split a
   cohort ends before that cohort and leaves it for the next step.
8. **`item_version`** increments once per item per request in which the
   item is affected by any operation: enqueue replacement, claim, renew,
   reassign, finalize, `Mutate`. Lease expiry does not change it. Responses
   return the new value.
9. **Rows.** `fw_row_version` starts at 1 on insert and increments on every
   change. A deleted or expired row that is inserted again starts at 1 with
   a new incarnation, so `expected_version: Some(n)` taken from the old row
   is a `VersionMismatch` (the implementation keeps an incarnation counter
   per key for `request_id_retention_ms`). `RowExpiry::AfterWrite` is reset
   by every applied write to the row, and not by `Skip` or a `Merge` that
   changes nothing.

#### `Patch`

Patches apply in order to the JSON body (a `None` body starts as `{}`).
Values are encoded per §Canonical JSON.

| Patch | Target state | Effect |
|-------|--------------|--------|
| `Set` | pointer `""` | replaces the whole body |
| `Set` | parent objects missing | creates them as objects |
| `Set` | parent is a scalar | fault `PointerConflict` |
| `Set` | array index `0 ..= len-1` | replaces the element |
| `Set` | array index `len` or `-` | appends |
| `Set` | array index `> len` | fault `PointerConflict` |
| `Remove` | pointer absent | no effect |
| `Remove` | pointer `""` | body becomes `None` |
| `Append` | target absent | creates an array of one element |
| `Append` | target is an array | appends; with `keep_last: Some(k)`, drops the oldest beyond k |
| `Append` | target is not an array | fault `PointerConflict` |

Patches reduce request size. Engines that store JSON as one value (Postgres
`jsonb`, SQLite text) still rewrite the stored value; large, independently
changing data belongs in table rows or blobs.

#### `dry_run`

`dry_run` runs validation and execution, applies nothing, records nothing,
and reports `status: DryRun` with per-operation counts, exact up to
`max_dry_run_count`, or the `failure` execution would have produced. It
may not be combined with `chunk`.

#### Chunking

1. A `Mutate` with `chunk` must be the only operation in its commit and must
   use `Where`. Its action may not change the access-term field or a key
   field, except a `Transition` or `Retire` when the access term is an
   equality or `In` on `fw_state`.
2. Each step examines candidates in access-term order after the
   continuation, up to `max_scan_rows`, and affects up to `limit` matches.
   It commits them and returns a `Continuation` positioned after the last
   candidate examined, or `None` when the access range is exhausted.
3. Each step re-evaluates the predicate at its own request time. Candidates
   the continuation has passed are not revisited.
4. A `Continuation` is bound to the fingerprint of the selection, action,
   lease policy and parameters. A step presenting it with any of those
   changed fails with `InvalidContinuation`. `expect` applies to each step.

## Shapes

### Queue

API-001 semantics, with the changes in this contract. Pause is observable:
a claim on a paused queue returns no items and `queue_paused = true`.
`claim` with `filter` narrows the candidates; ordering is unchanged; the
filter's access term bounds the scan. Key retention
(`client_item_key_retention_ms`) is the dedupe window for event items keyed
by a provider's event id.

**History.** With `history` declared, every committed change of an item
(enqueue, claim, renew, reassign, finalize, `Mutate`, lease break) and every
`SetGates` and `SetQueueState` appends one entry to the named log in the
same commit: kind, `fw_item_id`, `fw_key`, outcome, `item_version`,
`attempt_count`, `actor`, `reason`, `request_id`, and request time.
Time-driven transitions are not recorded in v1. On Postgres every append
takes the log's head-row lock (§PostgreSQL), so a history log should serve
one queue.

### Table

Rows by primary key. `PutRows` inserts with the conflict policy; `Replace`
overwrites every field; `Merge` sets only the given fields of an existing
row and inserts otherwise. `Set` with `Arith` over the old value is an
atomic increment. A conditional increment or compare-and-swap is a `Mutate`
with `Keys`, a `filter` (for example `count < limit`), and
`expect: { min: 1, max: 1 }`. A row past its expiry is invisible at once
and deleted later.

### Event log

```rust
pub struct ReadLogRequest {
    pub space: SpaceKey, pub log: ObjectName,
    pub from: LogCursor, pub max_entries: u32,
}
pub enum LogCursor { Start, After(u64), Partition { key: Value, after: Option<u64> } }
pub struct ReadLogResponse {
    pub entries: Vec<LogEntryView>, pub next: LogCursor,
    pub earliest_retained: Option<u64>, pub as_of: UtcMillis,
}
pub struct LogEntryView {
    pub seq: u64, pub appended_at: UtcMillis,
    pub fields: BTreeMap<FieldName, Value>, pub body: Option<Body>,
}
```

1. Every entry gets `fw_seq`, unique in the log and increasing in commit
   order. A reader that has seen `seq = n` never later sees a new entry with
   a smaller `seq`. Gaps are allowed.
2. Retention is by age only. `fw_appended_at` is non-decreasing in `fw_seq`
   (appends to one log conflict, §Time rule 3), so retention removes a
   prefix of the log.
3. `Start` reads from the earliest retained entry. `After(n)` fails with
   `CursorExpired { earliest }` if an entry with `seq > n` has been removed
   by retention. On an empty log, `next` is the cursor given.
4. A `Partition` cursor reads one partition in `seq` order, with the same
   expiry check against that partition.
5. Consumers keep their cursors in a table and advance them in the same
   group commit as their processing.

### Blob

1. `put_blob(space, bytes, media_type, min_durability)` stores immutable
   bytes and returns `BlobRef { sha256, size }` and the level reached.
   Putting the same bytes again returns the same reference and restarts its
   grace period. Up to `max_blob_bytes`.
2. A reference is a value in a `BlobRef` or `BlobRefSet` field of a live
   queue item, terminal item within retention, live table row or retained
   log entry. References inside JSON bodies do not count. Tombstones hold
   none.
3. A reference ends when its value is overwritten or removed, or when its
   record is retired, deleted, expires or leaves retention, at that
   transition's instant.
4. A blob with no reference is deleted after `orphan_grace_ms` measured from
   the later of its last put and its last reference ending. Deletion is
   serialized with commits: a commit that references a blob being deleted is
   rejected with `BlobMissing`, never left dangling.
5. `get_blob` returns the bytes, or `BlobNotFound`.

## Reads

### Point reads

```rust
pub struct GetRequest { pub space: SpaceKey, pub targets: Vec<GetTarget> }
pub enum GetTarget {
    Item { queue: ObjectName, item: ItemRef, include_terminal: bool },
    Row { table: ObjectName, key: Vec<Value> },
    QueueState { queue: ObjectName },
    Blob { blob: BlobRef },
}
pub struct GetResponse { pub as_of: UtcMillis, pub results: Vec<Option<GetResult>> }
pub enum GetResult {
    Item(ItemView), Row(RowView), QueueState(QueueStateView), Blob { size: u64, media_type: String },
}
pub struct ItemView {
    pub item_id: ItemId, pub client_item_key: ClientItemKey, pub state: ItemState,
    pub item_version: u64, pub attempt_count: u32, pub priority: PriorityValue,
    pub not_before: Option<UtcMillis>, pub group_key: Option<GroupKey>,
    pub cohort_id: Option<CohortId>, pub gate_keys: BTreeSet<String>,
    pub lease: Option<LeaseSummary>, pub terminal: Option<TerminalSummary>,
    pub metadata: Metadata, pub body: Option<Body>, pub fields: BTreeMap<FieldName, Value>,
}
pub struct LeaseSummary { pub worker_id: WorkerId, pub lease_expires_at: UtcMillis }  // never the token
pub struct TerminalSummary { pub outcome: ItemState, pub code: Option<String>, pub terminal_at: UtcMillis }
pub struct RowView { pub fields: BTreeMap<FieldName, Value>, pub row_version: u64 }
pub struct QueueStateView { pub paused: bool, pub drain_intake: bool, pub blocked_gates: Vec<(String, u64)> }
```

All targets are read at one linearization point.

### Declared reads

```rust
pub struct ReadDecl {
    pub name: ReadName, pub version: u32,
    pub source: ObjectName,                       // a queue, table or event log
    pub params: Vec<(ParamName, ScalarType)>,
    pub filter: Expr,                             // index rule applies
    pub output: ReadOutput,
    pub order: Vec<(FieldName, Direction)>,       // rows only; key order breaks ties
    pub max_rows: u32,
}
pub enum ReadOutput {
    Rows { fields: Vec<FieldName> },
    Aggregate { group_by: Vec<(FieldName, Expr)>, values: Vec<(FieldName, Aggregate)> },
}
pub struct ReadRequest {
    pub space: SpaceKey, pub name: ReadName, pub version: u32,
    pub params: BTreeMap<ParamName, Value>, pub page_token: Option<PageToken>,
}
pub struct ReadResponse {
    pub as_of: UtcMillis, pub rows: Vec<BTreeMap<FieldName, Value>>,
    pub next_page_token: Option<PageToken>, pub scan_limited: bool,
}
```

1. A read is declared in the schema and invoked by name and version. A
   declared version never changes. `ReadNotDeclared` otherwise.
2. Each page is read at one linearization point. Pages of one read are not
   one snapshot. The page token continues in `order`, then key order.
3. Aggregate reads return one page.
4. Implementations backed by SQL may name a declared read
   `fw_read_v<N>_<name>` internally. Consumers never see engine tables or
   views.

**Built-in reads** are defined in `fireweed-interface` and exist in every
space. Each takes the queue as a parameter `queue`.

| Read | Version | Parameters | Output |
|------|---------|------------|--------|
| `fw_items` | 1 | `queue`; one of `state`, `group_key`, `worker_id`, `key_prefix` | item rows (system fields and declared fields, no body), key order |
| `fw_gates` | 1 | `queue` | blocked gate keys with `gate_epoch` |
| `fw_queue_counts` | 1 | `queue` | counts by `fw_state` |

### Metrics and discovery

`metrics(space, queue)` returns at one linearization point:

```rust
pub struct MetricsRequest { pub space: SpaceKey, pub queue: ObjectName }
pub struct QueueMetrics {
    pub as_of: UtcMillis, pub pending: u64, pub leased: u64, pub complete: u64,
    pub failed: u64, pub retry_backlog: u64, pub oldest_eligible_age_ms: Option<u64>,
    pub progress_bound_risk_count: u64, pub active_leases: u64,
    pub recurring_pending: Option<u64>, pub recurring_leased: Option<u64>,
}
```

Lifecycle counts follow API-001: `pending` includes items not yet due and
gated items; `leased` counts unexpired leases; terminal counts cover
retention. `oldest_eligible_age_ms` and `progress_bound_risk_count` cover
items eligible under API-001 Eligibility Precedence A (base eligibility,
including pause) at `as_of`, without claim-unit resolution.

```rust
pub struct DiscoverRequest {
    pub space: SpaceKey,
    pub queues: Option<BTreeSet<ObjectName>>,   // allow-list from the adapter; None = all
    pub granularity: Granularity,               // Queue | Group
    pub group_key: Option<GroupKey>,
    pub max_results: u32,
    pub page_token: Option<PageToken>,
}
pub struct DiscoverResponse { pub scopes: Vec<ActiveScope>, pub next_page_token: Option<PageToken>, pub partial: bool }
pub struct ActiveScope {
    pub queue: ObjectName, pub group_key: Option<GroupKey>,
    pub oldest_eligible_age_ms: u64, pub eligible_count: Option<u64>,
    pub observed_at: UtcMillis,
}
```

Discovery covers one space; tenant-wide discovery is the facade looping over
spaces. The adapter computes the allow-list from its own authorization; the
interface has no authorization callback.

## Errors

```rust
#[non_exhaustive]
pub enum DataPlaneError {
    SpaceNotFound, ObjectNotFound(ObjectName),
    SchemaConflict { current_version: Option<u64> },
    FeatureUnsupported(Unsupported), IndexNotReady(FieldName),
    SelectorNotIndexed, ReadNotDeclared { name: ReadName, version: u32 },
    GatesNotEnabled, InvalidRequest(String), BatchTooLarge(LimitKind),
    Evaluation(EvalError),            // reads only (Int Sum overflow)
    ScanLimitExceeded,                // aggregate reads only
    RequestIdConflict, RequestExpired,
    CursorExpired { earliest: u64 }, InvalidContinuation, BlobNotFound,
    DurabilityPending { reached: DurabilityLevel },
    Contention,
    CommitTimeout, RateLimited { retry_after_ms: Option<u64> }, Unavailable(String),
    SpaceBusy, InstanceSuperseded, Internal(String),
}
pub enum Unsupported { Shape(ShapeKind), QueueFeature(QueueFeature), Durability(DurabilityLevel), Limit(LimitKind) }
pub enum LimitKind { Ops, Entries, RequestBytes, InList, ClaimBatch, LeaseDuration, RequestIdRetention, Objects }
```

| Error | Kind | Retry |
|-------|------|-------|
| `SpaceNotFound`, `ObjectNotFound`, `ReadNotDeclared`, `FeatureUnsupported` | Definitive | After a schema change |
| `SchemaConflict` | Definitive | After re-reading the schema |
| `IndexNotReady` | Definitive | Later, same `request_id` |
| `SelectorNotIndexed`, `GatesNotEnabled`, `InvalidRequest`, `BatchTooLarge`, `Evaluation`, `ScanLimitExceeded`, `InvalidContinuation` | Definitive | After changing the request |
| `RequestIdConflict`, `RequestExpired` | Definitive | With a new `request_id` |
| `CursorExpired`, `BlobNotFound` | Definitive | No |
| `DurabilityPending` | Definitive: committed at `reached` | Same `request_id` to wait again |
| `Contention`, `RateLimited`, `SpaceBusy` | Definitive: nothing committed | Same `request_id` (`RateLimited` after `retry_after_ms`) |
| `CommitTimeout`, `Unavailable` | Indeterminate | Same `request_id`, or `get_request` |
| `InstanceSuperseded` | Definitive | No; use the new instance |
| `Internal` | Definitive: a broken invariant was detected; nothing committed | No |

A rejected group is a recorded outcome, not an error.

## Implementation rules

### PostgreSQL

1. **Transactions.** Each request is one transaction. Verbs (`enqueue`,
   `claim`, `claim_by_item_ids`, `renew`, `finalize`) run at `READ
   COMMITTED`, each as single statements that lock what they change. A
   `commit`, a chunk step and a multi-target `get` run at `REPEATABLE READ`.
   On a serialization failure the implementation retries the whole request
   up to a documented bound, then returns `Contention`. Nothing from a
   failed attempt survives.
2. **Request records.** A write request inserts its request row first. A
   rejected group rolls back to a savepoint after that insert and commits
   the record.
3. **Time.** The provisional time is `clock_timestamp()` at the start of
   the request. After locking, `t = max(provisional, clock_timestamp(),
   greatest fw_updated_at of the locked records)`, passed to every later
   statement as a parameter. Rule 4 of §Time applies to claims.
4. **Claims** use `FOR UPDATE SKIP LOCKED` over the access-term index.
5. **Text** columns and indexes use the `C` collation.
6. **JSON** is stored as `jsonb`; numbers are checked against §Canonical
   JSON before writing.
7. **`Int` `Sum`** accumulates in `numeric` and checks the final range.
8. **Event logs** take the log's head-row lock to assign `fw_seq`, so `seq`
   follows commit order. Appends to one log serialize.
9. **Durability.** `Local` is `synchronous_commit = on` without a
   synchronous standby. `Replicated` requires `synchronous_standby_names`
   to name at least one standby; then every commit reports `Replicated`.
10. **Blobs** are `bytea` rows with a reference count maintained in the
    referencing transaction.

### Local engine

1. One database per space, with one writer. A request is one write
   transaction. Time is non-decreasing per space.
2. Commits are fsynced locally (`Local`) and shipped to the S3 log in
   batches. `min_durability: Replicated` waits for its batch.
   **[draft pending owner]**
3. A second open fails with `SpaceBusy` while the holder lives (a lock on
   the database file). Fencing across hosts uses a conditional put on the
   next S3 log segment, so a stale writer's next commit fails before it is
   acknowledged. The local engine's TD specifies it.
4. Lease expiry and row expiry are lazy. Reads run on MVCC snapshots.
5. Speed gates B1 to B4 apply.

## Consumer inventory

What Snorri and Cayce would otherwise keep in memory, in opaque blobs or in
their own stores, and the shape that holds it.

### Snorri

From Snorri TD-012 revision 6 §3.7 (uncommitted, not verified in this
repository). Rates are per recipient of a single-send campaign.

| # | State | Size and rate | Shape and mechanism |
|---|-------|---------------|---------------------|
| 1 | Instance body: variables, node, waits, effect references, inbox, audit ring, engagement profile | Body up to 2 MiB; output up to 1 MiB; rewritten about 3 times per recipient plus once per profile update | Queue item with a `Json` body (D8). Field-level changes are `Patch`; a profile update or inbox append is a `Mutate` with `Patch` and `LeasePolicy::Preserve`, which bumps `item_version` so the worker's guarded commit sees it. Large outputs go to blobs, referenced from a `BlobRefSet` field. |
| 2 | Consumed inputs | Up to 16, each up to 64 KiB; one per external event | Event items in an inputs queue, finalized `Complete` in the transition's group; or kept in the body and trimmed with `Patch`. |
| 3 | Waits and await routing | Up to 128 per instance; names of 71 bytes | An indexed `TextSet` field on the instance item, changed with `SetAdd` and `SetRemove`; routing is a declared read with `Contains`. |
| 4 | Effects: request, intent, receipt | Up to 256 KiB per effect item; intent commit about 1.2 KiB; 3 commits per send | Effect items in an effects queue. The intent commit sets `min_durability: Replicated` (§Durability; Snorri O15). |
| 5 | Handler idempotency per idempotency key | Small; one write per handler call | Table keyed by idempotency key with `RowExpiry::AfterWrite`. |
| 6 | External events and dedupe | Up to 64 KiB per event; 31-day dedupe; at most 3 claims per event | Event items keyed by provider event id, with `client_item_key_retention_ms` of 31 days. |
| 7 | Audit and history | One per transition | Hash chain and last 16 summaries in the body (`Patch` `Append`, `keep_last: 16`). Full history is exhaust for Niflheim (Snorri's TransitionObserver). |
| 8 | Definitions | 31–43 KB, up to 8 MiB; once per artifact per queue | Blob, referenced from a table row keyed by artifact digest and queue. |
| 9 | Admission in-flight chunk | About 100 B per run per shard; twice per chunk | Table row keyed by run and shard, created with `expected_version: Some(0)` and updated with the current version. **[pending owner: Snorri O16]** |
| 10 | Rate-limit counters (today Dragonfly in Cayce) | One counter per slot key (account, contact, hour); one conditional increment per effect intent | Table rows keyed by slot and hour bucket with expiry; conditional increment as in §Table. **[pending owner: Snorri O16]** |

Not shapes: the compiled WASM cache; the open-runs list and next-wake time
(derived through `fw_queue_counts` and a declared read ordered by
`fw_not_before`); the old adapter's caches.

### Cayce (draft; Cayce to confirm)

| State | Shape and mechanism |
|-------|---------------------|
| Per-recipient, per-run lifecycle | Queue item per recipient, with declared fields (campaign, step, domain, status) for claims, bulk changes and UI reads |
| Recipient profile, suppression flags | Table keyed by recipient |
| Frequency caps and send-rate windows | Table counters keyed by bucket |
| Delivery and engagement events | Event items deduplicated by provider id that drive workflows; an event log for timelines and export |
| UI counts by state, lag, send rates | Declared aggregate reads over queue items, tables and event logs |
| Rendered messages and attachments | Blobs |

## Profiles

A profile names what a consumer needs. An implementation satisfies a
profile when `capabilities()` covers all of it.

| Profile | Shapes and features | Minimum limits | `max_durability` |
|---------|---------------------|----------------|------------------|
| `snorri` | Queue, Table, Blob; `DynamicGates`; `Json` bodies | `max_body_bytes` 2 MiB; `max_value_bytes` 256 KiB; `max_set_values` 128; `max_blob_bytes` 8 MiB; `max_request_bytes` 4 MiB; `max_entries_per_commit` 4,096; key retention 31 days | `Replicated` for the zero-loss send path |
| `cayce-ui` | Queue, Table, EventLog; declared reads with aggregates | `max_read_rows` 1,000; `max_scan_rows` 5,000,000 | — |
| `recipient-lifecycle` | Queue, Table; chunked `Mutate` | `max_entries_per_commit` 10,000; `max_selector_affected` 10,000 | — |

## Conformance

### Harness

```rust
pub trait Harness: Send + Sync + 'static {
    type Impl: DataPlane + 'static;
    fn create_storage(&self) -> BoxFuture<'_, Result<StorageSpec, HarnessError>>;
    fn open<'a>(&'a self, storage: &'a StorageSpec, clock: SharedClock)
        -> BoxFuture<'a, Result<Self::Impl, HarnessError>>;
    fn child_binary(&self) -> Option<PathBuf>;
    /// Optional: destroy the implementation's local copy, for Replicated scenarios.
    fn lose_local_storage<'a>(&'a self, storage: &'a StorageSpec)
        -> BoxFuture<'a, Result<(), HarnessError>> { Box::pin(async { Err(HarnessError::Unsupported) }) }
}
pub struct StorageSpec(serde_json::Value);    // opaque to the suite
pub struct SharedClock { pub path: PathBuf }  // file holding the current UtcMillis; suite-owned
pub enum HarnessError { Unsupported, Setup(String) }

pub trait FaultHooks: Send + Sync {           // implemented inside the implementation crate
    fn arm(&self, fault: Fault);
}
pub enum Fault { BeforeCommit, AfterCommitBeforeResponse, StallBeforeCommit { until_released: bool } }
```

1. Every process reads time from the `SharedClock` file; only the suite
   advances it.
2. The child binary's `main` calls `fireweed_conformance::child_main`,
   which opens the storage named on its command line and serves framed JSON
   messages on stdin and stdout. Each message carries an id; requests may be
   concurrent. Messages are either `DataPlane` requests (the request types'
   serde form) or control messages: `ArmFault`, `ReleaseStall`, `Exit`.
3. Faults act inside the implementation's commit path through `FaultHooks`,
   which the implementation compiles in for test builds. A wrapper around
   `DataPlane` does not satisfy them.
4. Scenarios run by declared guarantees and capabilities. A scenario
   required by what the implementation declares may not be skipped; the
   `Replicated` scenarios are required when `max_durability` is
   `Replicated`.
5. For Postgres, the harness owns the database server process; `Local` and
   `Replicated` crash scenarios kill the server (and, for `Replicated`,
   promote the standby), not only the client.

### Rule-pinning tests

Each rule has scenarios in `fireweed-conformance`; both engines run the same
vectors. IDs are stable and cited in failures.

| ID | Rule | Scenario |
|----|------|----------|
| TYP-1 | Typing table | Every operator with every disallowed operand type fails validation with `InvalidRequest`; every allowed signature passes. |
| TYP-2 | Typed nulls and params | `Lit(Null(Int))` compares as Int; a parameter of the wrong type fails validation. |
| EVAL-1 | Short-circuit order | `And([false, 1/0 = 0])` is false; `Or([true, …])` true; `Coalesce([1, 1/0])` is 1. |
| EVAL-2 | Faults in predicates are null | `Where x / 0 = 1` matches nothing and raises nothing, with the faulting conjunct first or last. |
| EVAL-3 | Faults in action values reject | `Set n = i64::MAX + 1` rejects the group with `Evaluation(IntOverflow)`; nothing applied; record written. |
| EVAL-4 | Int division | `-7 / 2 = -3`, `-7 % 2 = -1`, `i64::MIN / -1` faults. |
| EVAL-5 | Float faults | `1.0 / 0.0` and an overflowing product fault. |
| EVAL-6 | Three-valued logic | Truth tables for `And`, `Or`, `Not`, `In` with nulls. |
| EVAL-7 | Text and Bytes order | Code-point order with non-ASCII strings that sort differently under locale collation; byte prefix order. |
| EVAL-8 | Bucket | Negative timestamps floor toward negative infinity; width 0 and > 10 years fail validation. |
| EVAL-9 | Now | Every `Now` in one request, including across group operations, is the same value. |
| EVAL-10 | Sets | `SetAdd` past `max_set_values` faults; `SetRemove` of an absent value is a no-op. |
| CAST-1 | Cast table | Every cell, including null results for out-of-range and unparsable inputs. |
| CAST-2 | RFC 3339 | Offsets, sub-millisecond truncation, leap second → null. |
| JSON-1 | Canonical JSON | Duplicate keys rejected; key order not significant; large integers and non-finite numbers rejected. |
| JSON-2 | Value to JSON | Each `Value` variant's encoding. |
| AGG-1 | Aggregate types | Each aggregate's operand and result types; null handling; empty input with and without `group_by`. |
| AGG-2 | Exact Int sum | `Sum` over values whose running sum overflows but final sum fits returns the final sum; a final overflow fails `Evaluation`. |
| AGG-3 | Float tolerance | `Sum` and `Avg` over cancelling values fall within the stated bound. |
| AGG-4 | Percentile rank | Ranks at n = 1, 2, 3, 100 for p = 1, 500,000, 999,999, 1,000,000 millionths. |
| IDX-1 | Access term | First qualifying conjunct is chosen; a predicate with none fails `SelectorNotIndexed`; `Keys` needs none. |
| IDX-2 | Index readiness | A request on a building index fails `IndexNotReady`. |
| SCAN-1 | Unchunked scan limit | A `Where` whose matches lie beyond `max_scan_rows` candidates rejects with `ScanLimitExceeded`. |
| SCAN-2 | Chunk progress | Steps over a range with no matches advance and finish; each step examines at most `max_scan_rows`. |
| SCAN-3 | Claim scan limit | A filtered claim over sparse matches returns partial results with `scan_limited`. |
| SCAN-4 | Read scan limit | A rows read pages at the limit with `scan_limited`; an aggregate fails. |
| LIM-1 | Validation caps | Each `LimitKind` over its cap fails with `BatchTooLarge` and no record. |
| LIM-2 | Resulting sizes | A small `Patch` `Append` that grows a body past `max_body_bytes` rejects with `ValueTooLarge`. |
| GRP-1 | Atomicity | A group whose last operation rejects leaves no effect from earlier operations. |
| GRP-2 | List order | An `Enqueue` followed by a `Mutate` on the same key sees the new item. |
| GRP-3 | Decision table | One scenario per row and column of §Rejecting conditions. |
| GRP-4 | Failure report | The response names the first failing operation and entry; later operations have `evaluated = false`. |
| GRP-5 | Verb vs group | The same stale lease is a per-entry `StaleLease` in `finalize` and a rejection in `commit`. |
| GRP-6 | Lease policies | `Skip`, `Reject`, `Preserve`, `MatchToken`, `Break` on leased matches, including invalid combinations. |
| GRP-7 | Set reads old values | `Set [a = b, b = a]` swaps. |
| GRP-8 | Counting | Repeated keys count once; leased skips count in `matched`; `expect` uses `matched`. |
| GRP-9 | Cohort split | A `Retire` of one member of a live cohort rejects with `CohortSplit`; selecting all members succeeds; a chunk step stops before the cohort. |
| GRP-10 | Version bumps | Each operation's effect on `item_version` and `fw_row_version`; renew returns the new version; lease expiry does not bump. |
| GRP-11 | Row incarnation | Delete, re-insert, then a guard with the old version mismatches; `Some(0)` on an existing row mismatches. |
| PATCH-1 | Patch matrix | Every row of the §Patch table. |
| FIN-1 | Finalize order | `metadata`, `body`, `patch`, `set`, outcome, with `expected_item_version` checked first. |
| CHK-1 | Continuation binding | Changing selection, action, policy or params with a continuation fails `InvalidContinuation`. |
| CHK-2 | Chunk restrictions | A chunked `Mutate` with another operation, a `Keys` selection, or an action on the access field fails validation. |
| DRY-1 | Dry run | Counts match a real run; nothing applied or recorded; the `request_id` stays unused. |
| REC-1 | Rejections recorded | A rejected group's replay returns the same rejection, even after state changes that would let it commit. |
| REC-2 | Validation not recorded | A request failing validation can be resent with the same id after fixing it. |
| REC-3 | Fingerprint | Changing `min_durability` is not a conflict; changing anything else is `RequestIdConflict`. |
| REC-4 | Stalled original | With `StallBeforeCommit`, a replay commits or rejects; the released original then returns the recorded result and does not commit. |
| REC-5 | Lost response | With `AfterCommitBeforeResponse`, `get_request` and replay return the committed result. |
| REC-6 | Claim replay | Replay while leases are active returns the same claim; after they end, `RequestExpired`; an empty claim replays empty. |
| TIME-1 | Time-driven transitions | Lease expiry, cohort deadline, retention and row expiry under `SharedClock`, with no tick. |
| TIME-2 | Same-instant order | Lease expiry, cohort deadline and row expiry due at one instant process in the stated order. |
| TIME-3 | Monotonic along conflicts | With the clock moved backward, a conflicting request's `as_of` does not decrease. |
| TIME-4 | eligibility_time | Earlier and later than the authority; lease expiry still from the authority. |
| TIME-5 | Lazy before act | A request touching a record first applies that record's due transitions. |
| COH-1 | Cohort deadline | Retired member, gated member and missing member each fail the cohort at the deadline; a claimed cohort is exempt; enqueue after failure is `CohortFailed`. |
| DUR-1 | Levels reported | Every write response reports a level within the declared range. |
| DUR-2 | min_durability above max | Fails `InvalidRequest`. |
| DUR-3 | DurabilityPending | With replication held back, the request returns `DurabilityPending { reached: Local }`; a replay after release returns `Replicated`. |
| DUR-4 | Local crash | Kill the server or child around acknowledged commits; every acknowledged commit is present after reopen. |
| DUR-5 | Replicated loss | After `lose_local_storage`, every commit acknowledged at `Replicated` is present. |
| LIN-1 | Concurrent claims | Peers never hold two active leases on one item. |
| LIN-2 | Read after commit | A read starting after a commit's response observes it, across shapes. |
| SHARE-1 | SingleProcess | A second open fails or supersedes while the holder lives; after the holder is killed, open succeeds. |
| LOG-1 | Seq order | Concurrent appenders never let a reader see a smaller `seq` after a larger one. |
| LOG-2 | Retention and cursors | `CursorExpired` exactly when an entry after the cursor was removed; `Start`; empty log. |
| BLOB-1 | Reference lifecycle | Overwrite, retire, expiry and retention end references at the transition instant; orphan deletion after grace; re-put restarts grace. |
| BLOB-2 | Deletion race | A commit referencing a blob concurrently being deleted rejects with `BlobMissing`. |
| BLOB-3 | Blob durability | A `Replicated` commit referencing a fresh blob survives `lose_local_storage` with the blob. |
| SCH-1 | Schema versioning | Expected-version conflict; identical re-apply; non-additive change rejected; unsupported feature rejected. |
| READ-1 | Declared reads | Version immutability; pagination; built-in reads by each parameter. |
| MET-1 | Metrics populations | Lifecycle counts include not-yet-due and gated items; progress metrics exclude paused queues and use base eligibility. |
| PG-1 | Postgres isolation | Under concurrent group commits, `Contention` is returned only after retries, and no partial effect survives. |
| PG-2 | Postgres time | `as_of` of conflicting requests never decreases in commit order. |

### Differential test

A test-only crate runs generated sequential histories (proptest) against
the Postgres implementation and each other implementation under one
`SharedClock`: schemas, group commits, generated expressions over every
type, selector writes, chunk sequences, aggregates and clock moves.

- In a sequential history, item ids increase in request order in every
  implementation.
- Item ids are compared by `(queue, client key, incarnation)`; lease tokens
  by the claim, renew or reassign that returned them.
- Compared exactly: statuses, failures, counts, versions, claimed sets and
  order, rows, `Int` aggregates and every error. `Float` aggregates are
  compared within the §Aggregates bound.
- Concurrency, crash and fault rules are covered by the rule-pinning tests,
  not here.
- A difference is a defect in one of the two, settled by this contract and
  API-001; if both are silent, they are amended first.

## Planned implementations

| Implementation | Durability (min / max) | Visibility | Sharing | Shapes |
|----------------|------------------------|------------|---------|--------|
| PostgreSQL (shared server; new crate from TD-002) | `Local` / `Replicated` with a synchronous standby, else `Local` | `Immediate` | `MultiProcess` | all |
| Local engine (Turso MVCC on NVMe, S3 log; SQLite comparison arm) | `Local` / `Replicated` [draft pending owner] | `Immediate` | `SingleProcess` per space | all |
| In-memory (new crate) | `Process` / `Process` | `Immediate` | `SingleProcess` | all |

## Facade binding (summary for API-005)

```rust
impl Fireweed {
    pub fn new(data_plane: Arc<dyn DataPlane>) -> Fireweed;
    pub fn guarantees(&self) -> Guarantees;
    pub fn capabilities(&self) -> Capabilities;
    // One method per DataPlane method, delegating.
    // Helpers: single-item enqueue (generating a request id when none is given);
    // complete / fail / retry_after / release / rearm over LeaseTargets;
    // typed builders for SpaceSchema, Expr, Mutate and ReadRequest;
    // tenant-wide discovery over spaces; claim across queues.
}
```

`Fireweed` exposes no implementation type, configuration or maintenance
method. `StorageConfig` and `RETIRED_STORAGE_CELL` leave the facade.

## Mapping from the current facade

`Fireweed` has 84 public methods today: 74 in `crates/fireweed/src/facade.rs`
and 10 `operator_*` methods in `crates/fireweed/src/operator.rs`.

| Current methods | New home | Behaviour change |
|-----------------|----------|------------------|
| `create_queue`, `ensure_queue`, `queue_definition` | `apply_schema`, `get_schema` | queues are declared in a space schema; `QueueDefinition` becomes `QueueSpec` |
| `push`, `push_with_request_id`, `push_batch`, `push_batch_with_request_id` | `enqueue` | a request id is required (the helper generates one); key optional as today |
| `upsert` | `enqueue` with `KeyConflict::ReplacePending` | none |
| `batch_update` | `commit` with `Mutate` by `Keys` | now all-or-nothing across entries (was per entry); per-key `expected_version` |
| `reschedule` | `Mutate` `Reschedule` with `LeasePolicy::Preserve` | none |
| `set_gates` | `commit` with `SetGates` | response drops command position |
| `purge` | `Mutate` `Retire` by `Keys`; `force` maps to `LeasePolicy::Break` | none |
| `claim`, `claim_with`, `claim_response_with` | `claim` | none |
| `claim_at`, `claim_response_at` | `claim` with `eligibility_time` | lease time comes from the authority |
| `claim_by_query_at` | `claim` with `filter` and `eligibility_time` | the filter needs an access term |
| `reclaim_expired_at` | removed | the time rule |
| `claim_by_item_ids` | `claim_by_item_ids` | none |
| `renew` | `renew` | token-addressed, per-item outcomes (was id-only, all-or-nothing); returns the new `item_version` |
| `reassign` | `commit` with `Reassign` | token-addressed |
| `ack`, `complete`, `nack`, `retry`, `release`, `fail`, `rearm`, `nack_retry_after`, `retry_after`, `rearm_after`, `rearm_at` | `finalize` (helpers over `LeaseTarget`s and `Schedule`) | token-addressed, per-item (was id-only, all-or-nothing) |
| `live_item`, `live_items` | `get` | adds lease summary, declared fields and metadata |
| `claimed` | `get` of the given ids | returns `ItemView` with lease summary; no lease tokens |
| `metrics` | `metrics` | adds API-001 fields; progress metrics by base eligibility |
| `discover_active_scopes`, `discover_active_scopes_stamped`, `discover` | `discover_active_scopes` per space; facade loops over spaces | per-scope `observed_at`; allow-list replaces the authorization callback |
| `mutate_items` | `commit` with `Mutate` operations | one selection per `Mutate`; API-001 first-match ownership across ordered selectors is removed (later operations see earlier effects instead); no `evaluated_at` |
| `commit`, `commit_multi_claim` | `commit` | all-or-nothing group (was per entry); item-version guard kept; side records become table rows; the instance fence becomes a table row guarded by `expected_version` (`Some(0)` for absent) |
| `explain_commit` | `get_request` | — |
| `side_record`, `side_records`, `side_records_by_prefix` | `get` and declared reads on a table | prefix reads need a `Range` index |
| `commit_capabilities` | removed | `capabilities()` |
| `query_index`, `query_index_unique`, `query_index_typed`, `query_index_unique_typed` | declared `read` or `get` by key | declared in the schema |
| `range_scan`, `grouped_aggregate`, `declared_bucket_segment`, `metrics_by_query` | declared `read` | declared in the schema |
| `bounded_mutation` | `Mutate` with `limit` or `chunk` | none |
| `claim_by_query` | `claim` with `filter` | none |
| `hot_projection_capabilities` | `capabilities()`, `get_schema` | — |
| `claim_across_queues` | facade helper: one `claim` per queue with a shared `eligibility_time` | no common lease instant; not atomic |
| `operator_pause`, `operator_resume` | `SetQueueState` | none |
| `operator_admin_state` | `get` of `QueueState` | none |
| `operator_inspect_item`, `retained_items` | `get` with `include_terminal` | none |
| `operator_repair`, `operator_redrive` | `Mutate` with `Set`, `Transition` (`Release`, `Retry`, `Redrive`) and `LeasePolicy::Break` for forced actions | none |
| `operator_purge`, `operator_archive` | `Mutate` `Retire` | `max_affected` becomes `limit` with `expect`; archive as a separate store is removed |
| `operator_get_operation` | `get_request` | read-only |
| `operator_audit_records` | `read_log` on the queue's history log | needs `history`; principal and reason come from `actor` and `reason` |
| `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `snapshot_now`, `latest_snapshot_info`, `compact_log_behind_snapshot`, `last_rebuild_stats`, `ownership`, `renew_owned`, `reclaim_expired`, `current_position`, `peek` | object-log implementation type | leave `Fireweed` (ADR-025 open question 3) |

## Compatibility

- Adding an enum variant, a struct field, a shape, a queue feature, a
  built-in read version or a `DataPlane` method with a default body is
  compatible.
- Changing a signature, a request's meaning, an expression rule or a
  rule-pinning test's expected result is breaking.
- Changing an implementation's declared guarantees is breaking for that
  implementation.

## Validation checklist

- [ ] The dependency test enforces rules 1, 4, 5 and 6; the compile test
      enforces rule 3.
- [ ] `DataPlane` is dyn-compatible.
- [ ] Every rule-pinning test passes on every implementation, with no
      required scenario skipped.
- [ ] The differential test passes for every implementation against
      Postgres.
- [ ] Every Snorri inventory item runs on the `snorri` profile with no
      Snorri-owned store.
- [ ] Each mapping-table behaviour change is in the API-005 migration notes.
- [ ] ADR-025's contract amendments have landed before this contract is
      accepted.
