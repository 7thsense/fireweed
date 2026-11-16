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
**Version**: v0 (draft, revision 8)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, API-003, API-004, API-005, ADR-025, TD-002, TD-006

Revision 4 (2026-11-15) pins the request model for cross-engine behaviour:
expression evaluation, typing, casts and aggregates; scan budgets; the group
commit decision table; request records for rejected requests; the time and
isolation rules; durability levels per implementation; every referenced
type; and a conformance list that checks each rule. Text marked
**[draft pending owner]** follows a recommendation the owner has not ruled
on, and **[owner question]** marks a choice left open (ADR-025, Open
questions).

Revision 5 (2026-11-15) fixes the review round four defects: scanning in
result order, Postgres group commits at `SERIALIZABLE`, chunk steps that
skip cohorts they cannot take whole, row version high-water marks, guards
on absent records, pause and gate changes as cohort-deadline triggers,
schema and rejection durability, the stall point for replay tests, unique
and non-null violations, U+0000, and archive as a metadata change. The
crate `fireweed-interface` is now the normative definition of every type
named here; `fireweed-interface-memory` implements it in memory; and the
rule-pinning tests are executable in `fireweed-interface-conformance`.

Revision 6 (2026-11-15) fixes review round five findings and records the
first PostgreSQL implementation: scan counting by the access path read,
cohort-whole chunk steps that cannot revisit, replay before state-dependent
validation, one entry per addressed key, row versions once per request, the
crate's error enums, the `Text` to `Float` grammar, key release on
replacement of a retained item, and Postgres verb/group isolation. Request
logic is now the shared internal library `fireweed-interface-engine`, used
by `fireweed-interface-memory` and `fireweed-interface-postgres`. A
second review pass in the same revision fixed: request time fixed together
with the read snapshot, page tokens bound to their read and shape-checked,
schema changes applying expiries due under the old policy first,
constraints on body-derived fields, set cardinality on every written value,
`In` evaluating every item, JSON pointers with empty member names,
already-expired rows invisible within a group, durations bounded by the
Timestamp span, request-size limits on every verb, claim-by-id replay
expiry, filtered same-group claims, the full range access term in reads,
and paging for `fw_gates`.

Revision 7 (2026-11-15) applies the owner's rulings (ADR-025 D13 to D28;
D29 to D32 were added on 2026-11-16).

Revision 8 (2026-11-16, draft for review) follows ADR-025 D33 and D34:
Fireweed becomes `fireweed-compiler` and `fireweed-engine`, in the pattern
of weft and truss. §Compilation model states the new structure: requests
are programs in a typed IR, space schemas are UMF documents, and each
backend lowers the compiler's logical plan to SQL or refuses. The request,
expression, time, durability and conformance rules below keep their
meaning and become the IR's semantics. Sections that describe the
revision 7 verbs, the `DataPlane` methods and §Implementation rules
§PostgreSQL are restated by that section and are rewritten once the
review accepts it. The owner's rulings D36 to D40 narrow the `DataPlane`
trait to `apply_schema`, `get_schema`, `execute(program)`, `read(query)`,
`get_request` and `metrics`, keep YAML or JSON as the only syntax in v1,
and publish the `umf.fireweed` extension in UMF.
Durability levels are Niflheim's `FireAndForget`, `Tracked` and `Durable`,
chosen per request; PostgreSQL commits are `Durable`. The Blob and
EventLog shapes and queue history are removed; a table with
`KeyAssignment::Sequence` replaces them. `WriteOp::Assert` is a guard that
never writes. Table fields may declare `MergeRule::AddOnly` or
`NonDecreasing`. Lease expiry fails an item that has used up
`max_attempts`. The conformance harness can open a second instance on the
same storage, and fourteen DSL scenario tests (DSL-1 to DSL-14) drive
Snorri workflow patterns through edge cases.

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
- Non-goals: SQL as the consumer surface (Fireweed generates SQL; consumers
  never send it); joins in v1; a storage-port SPI; large payloads (ADR-025
  D16); history and analytics beyond live data (Niflheim).

## Terms

| Term | Meaning |
|------|---------|
| Implementation | A type implementing `DataPlane`, in its own crate. |
| Instance | One constructed value of an implementation, attached to some storage. |
| Space | One atomic and ordering domain owned by one tenant, holding named objects. |
| Object | A queue or table of a space. |
| Record | One queue item, table row, cohort, gate or request record. |
| Request | One call on `DataPlane`. A write request carries a `request_id`. |
| Commits | A write request takes effect at its linearization point. |
| Conflict | Two requests conflict if both touch a common record and at least one writes it. |
| Access term | The conjunct of a predicate that the index rule selects (§Index rule). |
| Candidate | A record that satisfies the access term. |
| Request time | The single instant from the time authority that a request uses for every decision. |

## Crate and dependency rules

1. `fireweed-interface` depends on no `fireweed-*` crate. It defines its own
   id newtypes (`TenantId`, `ItemId`, `ClientItemKey`, `RequestId`,
   `LeaseToken`, `WorkerId`, `CohortId`, `GroupKey`) and depends only on
   `serde`, `serde_json`, `bytes`, `futures`, `sha2` and `base64`.
2. No public item exposes an implementation's internal mechanism: command
   log, command position, projection, apply frontier, ownership epoch,
   storage cell. API-001 domain fields such as `gate_epoch` are allowed.
3. Every other public type is owned by `fireweed-interface` and derives
   `Serialize`, `Deserialize`, `Clone`, `Debug` and `PartialEq`. Public
   enums are `#[non_exhaustive]`. In v0, public structs have public fields
   and are not `#[non_exhaustive]`, so adding a struct field is a breaking
   change until builders exist (§Compatibility). No public type holds a
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

Under revision 8 (§Compilation model), rule 1 also allows `umf-core`, and
rules 4 to 6 apply to `fireweed-engine` backends in place of implementation
crates. `fireweed-compiler` depends on `fireweed-interface`, `umf-core` and
the crates it shares with weft (ADR-025 D35), and on no I/O, clock or
storage crate.

## Compilation model (revision 8, draft)

ADR-025 §2, §4 and §6 give the reasons; this section states the contract.

### Parts

| Part | Input | Output | May not |
|------|-------|--------|---------|
| Schema | A UMF document with the `umf.fireweed` extension, and one UMF physical binding document per backend (UMF CONTRACT-042) | A resolved, versioned space schema; DDL per backend | Change meaning per backend: a binding chooses layout and indexes only |
| Compiler | A program (§Programs), the resolved schema, a backend's capability manifest | A typed logical plan, then that backend's target plan, or a refusal with diagnostics | Read storage or a clock; fall back to another plan; return a partial plan |
| Engine | A target plan, request time, the requested durability level | The program's results and its request record, committed in one transaction | Reinterpret names or types; skip an obligation the plan lists |

### Programs

A program is a list of statements for one space, run in list order as one
commit (§Request and response types, group commit rules). Draft statements:
`Insert`, `Claim`, `Settle`, `Renew`, `Reassign`, `Mutate`, `Assert`,
`Query`, `SetGates`, `SetQueueState` (ADR-025 §4). Programs and schemas are
YAML or JSON documents read by one reader (ADR-025 D37). Each
revision 7 verb maps to a one-statement program with the same outcomes:

| Revision 7 | Revision 8 |
|------------|------------|
| `enqueue` | `Insert` into a queue |
| `claim`, `claim_by_item_ids` | `Claim` with a selection, or with item ids |
| `renew`, `finalize` | `Renew`; `Settle` with the outcome |
| `commit` with `WriteOp`s | A program with several statements |
| `get`, `read` | `Query`; a declared read is a stored `Query` |

A standalone `Settle` keeps per-item outcomes (a stale lease affects that
entry only); inside a program with other statements, staleness rejects the
program, as group commit does today.

### Types

Every field and expression has a UMF type. The revision 7 `ScalarType`
values map to UMF core types with facets; where UMF core has no admitted
meaning (temporal facets, UMF CONTRACT-047; text sets; the JSON body), the
`umf.fireweed` extension states it (ADR-025 D38, DocumentDrivenDX/umf#172). `Timestamp` is a
UTC instant in milliseconds. The rules in §Expressions (evaluation order,
nulls, overflow, casts, collation, aggregate precision) become the typing
and evaluation rules of the IR; there are no implicit casts.

### Logical plan

The plan is typed data with no table names, column names or SQL. The
compiler expands queue semantics into it:

1. Every `Claim` gets the eligibility predicate of §Time (due, not leased or
   lease expired, gates open, not exhausted) and the queue's claim order.
2. `Settle`, `Renew` and `Reassign` get a lease-token check; `Mutate` gets
   its lease policy.
3. Attempt charges, `item_version` bumps and `attempts-exhausted` failure on
   expiry are plan nodes, not backend choices.
4. An item's system fields (state, lease token, lease expiry, attempt count,
   item version) are written only by `Claim`, `Settle`, `Renew` and
   `Reassign`. A `Mutate` or `Insert` that names one is refused.
5. Time-driven state is a predicate on stored fields and the request-time
   parameter. The engine writes a derived transition back when a statement
   touches the record or a sweep passes; the processing order of §Time
   applies.
6. Every selection keeps its access term (§Index rule) and every statement
   its caps (§Work limits); the plan records the scan budget it may spend.

### Backends

A backend publishes a capability manifest: the statements, expression
forms, types and isolation it supports, each with conformance evidence, as
weft's backend manifests do. Lowering a plan that needs a missing
capability is a refusal. Each lowered plan lists its SQL, bound
parameters, result decoders and obligations (for example: run at
`SERIALIZABLE`, or hold the space's writer lease).

| Backend | Dialect | Layout | Claim lowering |
|---------|---------|--------|----------------|
| PostgreSQL | PostgreSQL 16+ | Real tables from each schema version (ADR-025 question 31) | `UPDATE … FROM (SELECT … ORDER BY … LIMIT $n FOR UPDATE SKIP LOCKED) … RETURNING` |
| Turso | Turso SQL | One database per space | Same, without `SKIP LOCKED`; one writer per space |
| SQLite | SQLite 3 | One database per space | As Turso |
| Memory | None | In-memory records | Interprets the logical plan; the reference for the differential test |

Compiled plans are cached by schema version, statement shape and backend,
and prepared once per connection.

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
    pub reads: Vec<ReadDecl>,
}

pub struct FieldDecl {
    pub name: FieldName,
    pub ty: ScalarType,
    pub nullable: bool,
    pub source: FieldSource,
    pub index: IndexHint,
    pub merge: MergeRule,                   // tables only; default Any
}

pub enum ScalarType { Bool, Int, Float, Text, Bytes, Timestamp, Json, TextSet }
pub enum MergeRule { Any, AddOnly, NonDecreasing }
pub enum FieldSource { Stored, BodyPointer(JsonPointer) }   // BodyPointer: derived, read-only
pub enum IndexHint { None, Lookup, Range, Unique }
pub enum BodyKind { Bytes, Json }

pub struct QueueDecl {
    pub name: ObjectName,
    pub spec: QueueSpec,
    pub body: BodyKind,
    pub fields: Vec<FieldDecl>,
}

pub struct QueueSpec {
    pub priority_kind: PriorityKind,        // Timestamp | Int64
    pub priority_direction: Direction,      // API-001 priority_model.direction
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
    pub key_assignment: KeyAssignment,      // default Caller
}
pub enum RowExpiry { AtField(FieldName), AfterWrite { ttl_ms: u64 } }
pub enum KeyAssignment { Caller, Sequence }
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
   `Lookup` or `Range` indexes on existing fields, new read versions,
   raised retention. Anything else is `SchemaConflict`, including a new
   `Unique` index on an existing field (existing values would need checking).
   Before a change takes effect, the implementation applies every
   expiration due under the old policy, so raising a retention never brings
   back a record that had already expired. Every duration in a schema
   (retentions, TTLs, bounds, backoff) is at most the span of the Timestamp
   domain, so time arithmetic cannot overflow.
3. A declaration needing an unsupported shape, feature or limit (including
   request-id retention, lease duration or object count above the
   implementation's limits) fails with
   `FeatureUnsupported` before anything changes.
4. Indexes may build after `apply_schema` returns. A request whose access
   term or declared read needs an index that is not ready fails with
   `IndexNotReady`. `get_schema` reports each index's state.
5. Table key fields are `Int`, `Text`, `Bytes` or `Timestamp`, not nullable,
   and immutable: no action may change them. `Unique` fields treat nulls as
   distinct. `TextSet`, `Json` and `Float` fields cannot be
   `Unique` or table keys. `AtField` names a nullable or non-null
   `Timestamp`; a null value never expires.
6. A `TextSet` holds at most `limits.max_set_values` distinct values. An
   indexed `TextSet` is indexed per value.
7. A `BodyPointer` field is computed from the JSON body and read-only. If
   the pointer is absent or the value does not cast to the field's type
   (§Casts), the field is null.
8. Fields a write does not supply are null; a non-nullable field missing
   from a `PutRows` row or `NewItem` rejects that entry (`MissingField`).
9. **Merge rules** apply to stored, non-key table fields: `AddOnly` to
   `TextSet`, `NonDecreasing` to `Int`, `Float` and `Timestamp`; any other
   placement fails `InvalidSchema`. Every change to an existing row is
   checked (`PutRows` `Replace` and `Merge`, `Mutate` `Set`): `AddOnly`
   requires the new set to contain the old one, `NonDecreasing` requires the
   new value to be at least the old one. An old null allows any value; a
   new null over a non-null value violates; a field a `Replace` omits is
   null. A violation rejects the entry with `MergeRuleViolated { field }`,
   which rejects the group. Only a `Mutate` with `override_merge_rules:
   true` may break a rule (a lift); `PutRows` never can, and the flag on a
   queue target is `InvalidRequest`. Adding or changing a field's rule is
   not additive.
10. **Sequenced tables.** With `KeyAssignment::Sequence`, the key is one
    non-nullable `Int` field. A `PutRows` row must omit it (supplying it
    rejects the entry `Invalid`); the implementation assigns 1, 2, 3, ... per
    table, in commit order, and returns the value in `EntryResult.row_key`.
    A number is consumed only by a row actually stored, so a rejected group
    or a `Skip` leaves no gap, and a number is never reused after `Retire`
    or expiry. Rows are immutable: `Set` on a sequenced table fails
    validation. To page, declare the key `Range` and read with a range on
    it (`seq > $after`, ascending); a reader paging forward never later
    sees a lower number than one it has read. Changing `key_assignment` is
    not additive.

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

`fw_state`, `fw_lease_expires_at` and `fw_worker_id` are evaluated by the
time rule: an item whose lease expired at or before request time reads as
`pending`, with null lease fields.

### Values

```rust
pub enum Value {
    Null(ScalarType),
    Bool(bool), Int(i64), Float(f64), Text(String), Bytes(Bytes),
    Timestamp(UtcMillis), Json(serde_json::Value), TextSet(BTreeSet<String>),
}
```

A null carries its type. Value domains, enforced on every write and by
every expression (a value outside its domain is a fault, §Evaluation):

- `Float`: finite, with magnitude at most 1e290. Sums of up to 2⁴⁰ such
  values cannot overflow.
- `Timestamp`: years 0000 to 9999 (`-62167219200000` to `253402300799999`
  milliseconds), so every timestamp has an RFC 3339 form.
- `Text`, JSON strings and JSON object keys: valid UTF-8 without U+0000,
  which PostgreSQL cannot store. A request containing U+0000 in a literal,
  parameter or supplied value fails validation with `InvalidRequest`.

### Canonical JSON

Every JSON value Fireweed stores (bodies, `Json` fields, metadata) is
normalized before it is stored and compared:

1. JSON reaches the interface as `serde_json::Value`, which has no
   duplicate keys. Rejecting duplicate keys in a wire encoding is the wire
   adapter's job. Key order is not significant and is not preserved.
2. Numbers are normalized: a number that is integral and within `i64` is
   stored as an integer (so `3.0` becomes `3`); any other number is stored
   as the nearest `f64`, which must be within the `Float` domain. An
   unsigned integer above `i64::MAX` becomes an `f64` if that conversion is
   exact, and otherwise rejects the write.
3. Two JSON values are equal when they are equal after rule 2, with object
   keys compared as sets.

Encoding a `Value` into JSON (for `Patch` values): `Bool`, `Int`, `Float`
and `Text` map to their JSON forms; `Bytes` to base64 (standard, padded);
`Timestamp` to an RFC 3339 string `YYYY-MM-DDTHH:MM:SS.sssZ`; `TextSet` to a
sorted array of strings; `Null` to `null`.

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
| `Cmp` `Eq`, `Ne` | same type: Bool, Int, Float, Text, Bytes, Timestamp | Bool |
| `Cmp` `Lt`, `Le`, `Gt`, `Ge` | same type: Int, Float, Text, Bytes, Timestamp | Bool |
| `And`, `Or` | Bool, 1 to 64 operands | Bool |
| `Not` | Bool | Bool |
| `IsNull` | any | Bool (never null) |
| `In` | `expr` and every list item of one type allowed for `Eq`; at least one item (more than `max_in_list` is `BatchTooLarge`) | Bool |
| `Contains` | TextSet and Text | Bool |
| `SetAdd`, `SetRemove` | TextSet with Text values | TextSet |
| `Arith` `Add`, `Sub`, `Mul`, `Div` | Int and Int → Int; Float and Float → Float | as stated |
| `Arith` `Rem` | Int and Int | Int |
| `Arith` `Add` | Timestamp and Int (ms) | Timestamp |
| `Arith` `Sub` | Timestamp and Int → Timestamp; Timestamp and Timestamp → Int (ms) | as stated |
| `Coalesce` | 1 to 16 operands of one type | that type |
| `Bucket` | Timestamp; `width_ms` 1 to 315,576,000,000 (10 years) | Timestamp |

Json and TextSet values are not comparable with `Cmp`; Json is
`Cast` first. Bool is not ordered. `Set` with the same field named twice
fails validation. `Json { field, pointer }` yields `Null(Json)` when the
pointer is absent and `Json(null)` when the JSON value there is `null`;
every cast of `Json(null)` to a type other than `Json` yields null.

Ordering and grouping in reads: `order` accepts the types `Cmp` can order
plus Bool (`false < true`); null sorts before every value in `Asc` and
after every value in `Desc`. `group_by` accepts Bool, Int, Text, Bytes and
Timestamp expressions; all nulls form one group.

### Evaluation

1. **Order.** `And`, `Or` and `Coalesce` evaluate their operands left to
   right and stop at the first operand that decides the result (`false`,
   `true`, non-null). Other operators evaluate all operands.
2. **Null.** Any operator other than `And`, `Or`, `Not`, `IsNull`,
   `Coalesce`, `SetAdd` and `SetRemove` yields null if an evaluated operand
   is null. `And`, `Or` and `Not` follow SQL three-valued logic. `In` yields
   true if any item equals, else null if `expr` or any item is null, else
   false. `SetAdd` and `SetRemove` on a null set treat it as empty, and
   ignore null values. A predicate selects a record only when it evaluates
   to true.
3. **Faults.** These are faults: `Int` overflow; `Div` or `Rem` by zero;
   a `Float` result outside the `Float` domain; a `Timestamp` result
   outside the `Timestamp` domain; a set that would exceed
   `max_set_values`. Where a fault occurs decides its effect:
   - in a predicate (selection, claim filter, read filter, `MatchExpect`
     filter) or a read output expression: the faulting expression yields
     null. No error is raised.
   - in any expression in an action position (`Set`, `Patch`,
     `Reschedule`, `Transition::Retry`, `FinalizeEntry.set` and `patch`):
     the group is rejected with `Evaluation`; in a standalone verb, that
     entry fails with `EntryStatus::EvaluationFault`.
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
by guarding each `Div` with `CASE WHEN divisor = 0 THEN NULL`. They must
implement the cast table and comparisons as written, not with engine casts
(PostgreSQL's `int8` input accepts `+`, whitespace and underscores; SQLite
casts `'12abc'` to 12), and must apply `COLLATE "C"` to every compiled text
comparison and sort.

### Casts

| From \ To | Bool | Int | Float | Text | Timestamp | Json |
|-----------|------|-----|-------|------|-----------|------|
| Bool | = | — | — | `true` / `false` | — | JSON bool |
| Int | — | = | nearest, ties to even | decimal, no leading zeros | ms since epoch | JSON number |
| Float | — | truncate toward zero; null if out of `i64` | = | shortest round-trip decimal (Rust `{}`) | — | JSON number |
| Text | `true` / `false` exactly, else null | optional `-` and digits, in range, else null | optional `+` or `-`, digits with an optional point (`.5` and `5.` allowed), optional exponent; finite and in the `Float` domain, else null | = | RFC 3339 (below), else null | JSON string |
| Timestamp | — | ms since epoch | — | RFC 3339 `…sssZ` | = | RFC 3339 string |
| Json | JSON bool, else null | JSON integral number in range, else null | JSON number, else null | JSON string, else null | JSON string per RFC 3339, else null | = |

`—` is a type error. RFC 3339 parsing accepts any offset, converts to UTC,
truncates fractional seconds to milliseconds, and yields null for a leap
second (`:60`) or a date outside `UtcMillis`. `Bytes` and `TextSet` cast
only to Json (§Canonical JSON).

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
   `2 × n × 2⁻⁵³ × Σ|x|` (for `Avg`, divided by n). The `Float` domain
   keeps every such sum finite. `Int` `Avg` is the exact quotient rounded
   to the nearest `f64`.

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

Two further restrictions keep examination order equal to result order:

- A **claim filter's** access term must be `Eq` or `In` on a `Lookup` or
  `Unique` field. The implementation keeps a composite index on (field,
  claim order), so candidates for each value arrive in claim order; for
  `In`, the per-value streams are merged in claim order.
- A **declared read with `order`** must have an access term that is `Eq` or
  `In` on a field, followed by `order`; or a range on the first `order`
  field itself. `apply_schema` creates the composite index (access field,
  `order` fields, key) and rejects a read it cannot serve that way with
  `SelectorNotIndexed`.

## Work limits

Every request does bounded work on the writer.

| Limit | Counts | When exceeded |
|-------|--------|---------------|
| `max_ops_per_commit` | operations in a commit | `BatchTooLarge` (validation) |
| `max_entries_per_commit` | explicit entries over all operations: new items, finalize entries, leases, gates, rows, addressed keys (`Mutate` and `Assert`) | `BatchTooLarge` (validation) |
| `max_request_bytes` | serialized request size | `BatchTooLarge` (validation) |
| `max_in_list` | `In` items | `BatchTooLarge` (validation) |
| `max_claim_batch_size` | items one claim returns | `BatchTooLarge` (validation) |
| `max_cohort_size` | members of one cohort | `BatchTooLarge` (validation), or rejection `Invalid` on enqueue |
| `max_writer_scan_rows` | records examined on the writer by one claim, `Where` `Mutate` or chunk step | see below |
| `max_read_scan_rows` | records examined by one read page or aggregate | see below |
| `max_selector_affected` | records a `Where` `Mutate` matches (unchunked) or affects (per chunk step) | rejection `LimitExceeded`, or chunk step boundary |
| `max_body_bytes`, `max_value_bytes`, `max_set_values` | the resulting value after an action, not the request | rejection `ValueTooLarge` |
| `max_read_rows` | rows in one read page | page boundary |

Records examined are the records the request's access path reads,
counted physically: a row that has expired but is still stored
counts. A claim's access path is the claim-order index of pending items
(with expired leases), so a pending item skipped because it is gated or not
yet due counts, and a leased item does not. When a scan budget is reached:

- an unchunked `Mutate` rejects the group with `ScanLimitExceeded`;
- a chunk step ends there and returns a continuation after the last record
  examined, so every step makes progress even if nothing matched;
- a claim, with or without a filter, returns the items found so far, which
  are a prefix of the claim order, and sets `scan_limited = true`;
- a rows read page ends there, with a page token after the last record
  examined, in result order, and `scan_limited = true`;
- an aggregate read fails with `ScanLimitExceeded`.

Examination order is result order: claim order for claims; `order` then key
for ordered reads; for `Mutate` and unordered reads, the access field's
value ascending, then the record key (item id or primary key).

Writes the implementation makes on its own (index entries,
due time-driven transitions) do not count against these limits. Due
transitions may run as separate internal commits ordered before the
request, each bounded by `max_cohort_size` and `max_writer_scan_rows`.

## Time

### Time authority and request time

1. Each space has one time authority, and it never moves backward. Under
   conformance it is the injected `Clock`, which the suite only advances.
   An implementation's production configuration may select another
   authority (the PostgreSQL server clock), must document it, and must keep
   it non-decreasing.
2. Each request has one request time `t`, fixed before the request
   evaluates any expression, and used for every decision in the request:
   eligibility, lease expiry, cohort deadlines, retention, row expiry,
   `Now`, and timestamps written.
3. **Monotonic along visibility.** If request B observes or conflicts with
   the effects of request A, then `t_B ≥ t_A`. With a non-decreasing
   authority this holds when each request samples `t` after every effect it
   can observe has committed: at the start of its snapshot (`SERIALIZABLE`
   and single-writer engines), or after acquiring its locks (`READ
   COMMITTED` verbs, rule 4).
4. **Selection with time.** A verb that locks rows may sample a provisional
   time to choose candidates, lock them, then fix `t` after the locks. A
   record is selected only if it qualifies at both times. Records that
   became eligible between the two samples may be missed by that request.
5. `claim` and `claim_by_item_ids` take an optional `eligibility_time`. It
   replaces `t` only when deciding whether `fw_not_before` and retry backoff
   have passed. It may be earlier or later than `t`. Everything else uses
   `t`.
6. No other request overrides time. A fixed instant in a predicate is a
   parameter.

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

- **Lease expiry**: a lease has expired when `lease_expires_at <= t` (at
  equality the lease is already expired). Its token is stale and
  `item_version` does not change. Each claim has already charged one
  attempt. If `attempt_count >= max_attempts`, the item becomes `failed`
  with code `attempts-exhausted` and `terminal_at = lease_expires_at`;
  otherwise it is `pending` (ADR-025 D17). This amends API-001, where expiry
  returned the item to pending and `max_attempts` was checked only on a
  retry finalize. A retry finalize still checks it as before.
- **Cohort deadline** (ADR-025 D19). API-001's deadline is
  `min(cohort_created_at, first_eligible_at) + completion_bound_ms`; since
  no member is eligible before the cohort exists, this is
  `cohort_created_at + completion_bound_ms`. At the deadline, unless the
  cohort has been claimed at least once, the cohort fails if fewer than
  `cohort_size` of its members are non-terminal, or if any non-terminal
  member is not eligible (API-001 Eligibility Precedence A, including
  pause) at that instant. A failed cohort's non-terminal members become
  `failed` with code `cohort-incomplete`. A member enqueued after its
  cohort failed is rejected (`CohortFailed`). Before a request that touches
  a cohort, one of its members, a gate one of its members carries, or its
  queue's state (`SetQueueState`), the implementation processes every due
  deadline of the cohorts affected.
- **Retention**: a request record older than `request_id_retention_ms` is
  forgotten. A terminal item or tombstone older than its retention stops
  being addressable. A row is gone at its expiry, including a row of a
  sequenced table; its sequence number is never reused.
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
   request record in its space: the request fingerprint and the response
   (ADR-025 D19).
2. The fingerprint is the SHA-256 of the canonical JSON serialization of the
   request with `min_durability` removed.
3. A request that fails validation (definitive `Err`) leaves no record.
   `dry_run` requests leave no record and do not use their `request_id`.
4. Recording is atomic with the outcome. Once a request id has a record, no
   other attempt with that id can commit.
5. `get_request(space, request_id)` returns the record, or `None`, and never
   executes anything. It returns the response with every lease token
   removed; only a replay of the same request returns tokens.
6. A request record is durable to the level reported in its response,
   whether the outcome is `Committed` or `Rejected`.

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

A request whose id has a record replays before any validation that
depends on stored state (schema version, continuations, object existence
beyond the space), so a lost response can always be recovered by replay.
A request id with no record runs as a new request. After the retention
window, a repeated id is a new request.

### Lost responses

A request whose future is dropped, or that ends in an indeterminate error,
may still commit, or be rejected and recorded. The caller learns the
outcome by replaying the same request or by `get_request`.

## Durability

```rust
pub enum DurabilityLevel { FireAndForget, Tracked, Durable }   // ordered
```

The levels are Niflheim's write modes (Niflheim ADR-002, architecture
§Write Modes; ADR-025 D14), used as levels a commit reaches.

| Level | Acknowledged when | Rule |
|-------|-------------------|------|
| `FireAndForget` | the commit is visible | May be lost if the process or machine fails. |
| `Tracked` | the commit is fsynced locally | Survives a process crash and a reopen of the implementation's own storage. |
| `Durable` | a copy outside that storage is durable | Also survives loss of that storage. |

1. Every write response (verbs, `commit` whether `Committed` or
   `Rejected`, `apply_schema`) reports `durability`: the level its effects
   and request record reached when the response was sent. `apply_schema`
   has no `request_id`; it is idempotent by schema version.
2. Every write request may set `min_durability`, the level it needs. With
   none, the implementation's default applies. The commit is acknowledged
   at the lowest level the implementation offers at or above the one asked
   for. A level above `capabilities().max_durability` fails with
   `InvalidRequest`. `guarantees().durability` is the lowest level any
   acknowledgement can have.
3. If the commit has not reached its level within the implementation's
   documented wait, the request fails with `DurabilityPending { reached }`:
   the commit exists, at `reached`. A replay waits again.
4. `CommitTimeout` means the implementation does not know whether the
   request committed.
5. Reads observe committed requests whatever their level. After a failure,
   effects that were read but never reached a level that survives it can
   disappear.
6. Levels are prefix-closed within a space: when a commit reaches a level,
   every commit before it in linearization order has too. Write-ahead logs
   and in-order shipping both give this. A `Durable` send intent therefore
   makes the state it was derived from durable as well.

Levels per implementation:

| Implementation | Offered | Default | Notes |
|----------------|---------|---------|-------|
| PostgreSQL | `FireAndForget`, `Durable` | `Durable` | The server is assumed fully replicated (ADR-025 D14): every committed transaction is `Durable`, and keeping the server so is the operator's job. `FireAndForget` commits with `SET LOCAL synchronous_commit = off`. A `Tracked` request is acknowledged at `Durable`. |
| Local engine | `FireAndForget`, `Tracked`, `Durable` | `Tracked` | One fsync per batch; `Durable` once the shipped watermark passes the commit (Turso-queue plan §5.1). The engine rejects writes in every mode when shipping falls behind. |
| In-memory | `FireAndForget` | `FireAndForget` | — |

## Guarantees and capabilities

```rust
pub struct Guarantees {
    pub durability: DurabilityLevel,          // lowest level any acknowledgement can have
    pub new_work_visibility: Visibility,
    pub sharing: Sharing,
}
pub enum Visibility { Immediate, Bounded { max_ms: u64 } }
pub enum Sharing { SingleProcess, MultiProcess }

pub struct Capabilities {
    pub max_durability: DurabilityLevel,
    pub shapes: BTreeSet<ShapeKind>,          // Queue and Table always
    pub queue_features: BTreeSet<QueueFeature>,
    pub priority_kinds: BTreeSet<PriorityKind>,
    pub ordering_modes: BTreeSet<OrderingMode>,
    pub limits: ImplementationLimits,
}
pub enum ShapeKind { Queue, Table }
pub enum QueueFeature { DynamicGates, MetadataBlockers, Recurrence, GroupBatching, Cohorts }

pub struct ImplementationLimits {
    pub max_ops_per_commit: u32,
    pub max_entries_per_commit: u32,
    pub max_request_bytes: u64,
    pub max_in_list: u32,
    pub max_writer_scan_rows: u64,
    pub max_read_scan_rows: u64,
    pub max_cohort_size: u32,
    pub max_selector_affected: u32,
    pub max_body_bytes: u64,
    pub max_value_bytes: u64,
    pub max_set_values: u32,
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
| `SingleProcess` | At most one live instance serves a space. Opening a second while the first lives either fails with `SpaceBusy` or supersedes the first, whose later requests fail with `InstanceSuperseded` and have no effect. After the holding process dies, a new open succeeds. The implementation documents which it does, and how it fences a holder on another host; for the local engine, a holder fenced by a higher ownership epoch returns `InstanceSuperseded` (§Local engine). |

## Types

The crate `fireweed-interface` is the normative definition of every type
this contract names, including those summarized here only by their fields.
Where this document and the crate differ, the difference is a defect to
fix in one of them before acceptance.

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
    fn metrics(&self, r: MetricsRequest) -> DpFuture<'_, QueueMetrics>;
    fn discover_active_scopes(&self, r: DiscoverRequest) -> DpFuture<'_, DiscoverResponse>;
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
    pub actor: Option<String>,     // recorded in the request record; not authorization
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
    pub superseded_item_id: Option<ItemId>, // ReplacePending: the replaced item
    pub version: Option<u64>,        // item_version or fw_row_version after the entry
    pub lease: Option<NewLease>,     // claim, renew, reassign
    pub row_key: Option<Vec<Value>>, // PutRows into a Sequence table: the assigned key
}
pub struct NewLease { pub lease_token: LeaseToken, pub lease_expires_at: UtcMillis, pub item_version: u64 }

pub enum EntryStatus {
    Accepted, Duplicate, Replaced, Updated, Completed, Failed, Retried, Released,
    Rearmed, Renewed, Reassigned, Redriven, Purged, Inserted, Deleted,
    Skipped, Inapplicable, NotFound,
    // only as per-entry statuses of verbs, or in OpFailure of a rejected group:
    Conflict, UniqueViolation { field: FieldName }, StaleLease, VersionMismatch,
    Terminal, Superseded, NotEligible, Leased, CohortFailed, MissingField { field: FieldName },
    ValueTooLarge, MergeRuleViolated { field: FieldName }, EvaluationFault(EvalError),
    Invalid { reason: String },
}
```

### Schema

```rust
pub struct ApplySchemaRequest {
    pub space: SpaceKey, pub schema: SpaceSchema, pub expected_version: Option<u64>,
    pub min_durability: Option<DurabilityLevel>,
}
pub struct ApplySchemaResponse { pub version: u64, pub changed: bool, pub durability: DurabilityLevel }
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
    pub lease_token: Option<LeaseToken>,   // None for members of a cohort lease
    pub lease_expires_at: UtcMillis,
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

pub struct FinalizeRequest {
    pub header: WriteHeader, pub queue: ObjectName, pub entries: Vec<FinalizeEntry>,
    pub params: BTreeMap<ParamName, Value>,
}
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
- `ItemRef::IdAndKey` gives `Conflict` unless both name the same item,
  including when the key names no item.
- `ReplacePending` on a key reserved by a retained terminal item or
  tombstone releases that reservation (the old item stays retained but is no
  longer addressable by key) and creates the new item.
- A keyless item reports `client_item_key` as `fw#<item_id>`; client keys
  may not start with `fw#`.
- `ClaimedItem` and `CohortLease` are the only types that carry a lease
  token. A whole-cohort claim issues one cohort token (API-001) and no
  per-member tokens.
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
    Mutate(Mutate),
    Assert { target: ObjectName, selection: Selection, expect: MatchExpect },
}
pub enum RowConflict { Reject, Skip, Replace, Merge }

pub struct Mutate {
    pub target: ObjectName,                 // a queue or table
    pub selection: Selection,
    pub action: Action,
    pub limit: u32,                          // 1 ..= max_selector_affected
    pub expect: Option<MatchExpect>,
    pub lease_policy: LeasePolicy,           // queues only
    pub chunk: Option<Chunk>,
    pub override_merge_rules: bool,          // tables only: may break merge rules (a lift)
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
    ReplaceBody(Option<Body>),
    SetMetadata(BTreeMap<String, Option<serde_json::Value>>), // None removes a key
    Reschedule { not_before: ScheduleChange, priority: Option<Expr> },
    Transition(Transition),
    Gates { add: BTreeSet<String>, remove: BTreeSet<String> },
    Retire,
}
pub enum ScheduleChange { Keep, Clear, Set(Expr) }
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
pub enum LeasePolicy { Skip, Reject, Preserve, MatchToken(LeaseToken), MatchCohortToken(LeaseToken), Break }
pub enum Chunk { Start, Continue(Continuation) }

pub struct CommitResponse {
    pub status: CommitStatus,                // Committed | Rejected | DryRun
    pub as_of: UtcMillis,                    // request time
    pub durability: Option<DurabilityLevel>, // Committed and Rejected (the record's level)
    pub ops: Vec<OpResult>,                  // aligned with request ops
    pub failure: Option<OpFailure>,          // Rejected only
    pub continuation: Option<Continuation>,
}
pub struct OpResult {
    pub evaluated: bool,                     // false after the failing op
    pub matched: u64,
    pub affected: u64,
    pub skipped_leased: u64,
    pub skipped_cohorts: u64,                // chunk steps only
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
pub enum EvalError { IntOverflow, DivideByZero, FloatOutOfRange, TimestampOutOfRange, SetTooLarge, PointerConflict }
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
| `Enqueue` | `Conflict` (`Reject` policy, or `ReplacePending` on a leased item); `UniqueViolation`; `CohortFailed`; `MissingField`; `ValueTooLarge`; gate keys over the queue's limit or a cohort over `max_cohort_size` (`Invalid`) | `Duplicate`, `Replaced`, `Accepted` |
| `Finalize` | `StaleLease`; `VersionMismatch`; `Terminal`; `Superseded`; `EvaluationFault` in `set` or `patch`; `UniqueViolation`; `MissingField` (null into a non-nullable field); `ValueTooLarge` | — |
| `Renew`, `Reassign` | `StaleLease`; `Terminal` | — |
| `SetGates`, `SetQueueState` | none at execution | — |
| `PutRows` | `Conflict` (`Reject` policy); `UniqueViolation`; `MissingField`; `ValueTooLarge`; `MergeRuleViolated`; a key supplied to a sequenced table (`Invalid`) | `Skipped` (`Skip` policy), `Inserted`, `Updated` |
| `Assert` | `VersionMismatch` on a `Keyed` guard, as `Mutate`; `MatchExpectUnmet`; `ScanLimitExceeded` | — |
| `Mutate` | `VersionMismatch` on a `Keyed` guard: `Some(0)` on an existing record, or `Some(n > 0)` on an absent record or a different version; `MatchExpectUnmet`; `LimitExceeded` (unchunked); `ScanLimitExceeded` (unchunked); `LeasedMatch` (`Reject` policy); `StaleLease` (`MatchToken` or `MatchCohortToken` mismatch); `CohortSplit` (unchunked); `EvaluationFault` in any action expression; `UniqueViolation`; `MissingField` (null into a non-nullable field); gate keys over the queue's limit (`Invalid`); `ValueTooLarge`; `MergeRuleViolated` (without `override_merge_rules`); `Conflict` from `IdAndKey` | `NotFound` for an addressed key with no guard; `Skipped` leased items; skipped cohorts (chunk steps); `Inapplicable` |

Constraints (unique fields, non-nullable fields, size limits, merge rules)
are checked on each record after each operation's change to
it, so a later operation in the group cannot repair an earlier violation.

#### Mutate semantics

0. **Entries.** A `Keys` selection reports one entry per addressed key, in
   key order: `NotFound`, `Skipped` (a repeated key or one its filter
   excludes), or the action's status. A `Where` selection reports one entry
   per matched record, in examination order. `OpFailure.entry_index` is that
   entry's index.
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
     rejects. Only with a single-key `Keys` selection of an item that is not
     leased as part of a cohort.
   - `MatchCohortToken`: any action applies to every member of the cohort
     whose lease the token names; the selection must address exactly those
     members.
   - `Break`: the lease ends (its token becomes stale, no attempt is
     charged), then the action applies. For operator force actions
     (API-002).
3. **Inapplicable.** A record on which the action has no meaning is
   `Inapplicable`: a `Transition` other than `Redrive` on a terminal item, a
   `Redrive` on a live item, `Reschedule` or `Gates` on a terminal item.
4. **`Set`** evaluates every right-hand side against the record as it was
   before the action, then assigns. It may not name a system field, a
   `BodyPointer` field or a table key field, or name one field twice.
   `SetMetadata` sets or removes metadata keys; `ReplaceBody` replaces a
   bytes or JSON body. Both apply to items in any state, including terminal
   items within retention.
5. **`Transition`.** `Complete` and `Fail` make the item terminal. `Retry`
   returns it to `pending` with `not_before` and charges nothing. `Release`
   returns a leased item to `pending` (needs `MatchToken` or `Break`).
   `Redrive` returns a terminal item within retention to `pending` with
   `attempt_count = 0`.
6. **`Retire`** on a queue item is API-001 `PurgeItems`: the item becomes a
   tombstone for key retention. On a table row, it deletes the row.
7. **Cohorts.** An action that would make some but not all non-terminal
   members of a cohort terminal, or move some but not all of them between
   live states, rejects an unchunked group with `CohortSplit`, unless the
   selection includes every non-terminal member. A chunk step that reaches
   a cohort it cannot take whole within the step (not every non-terminal
   member matches, or the members would exceed the step's remaining
   `limit` or scan budget, or a member lies outside the candidates the
   step examines) skips every member of that cohort, counts it in
   `skipped_cohorts`, and moves on; a cohort is taken only when every
   member is inside the step's examined window, so no later step can meet
   a taken member again. The caller handles skipped cohorts with a `Keys`
   selection of their members.
8. **`item_version`** increments once per item per request in which the
   item is affected by any operation: enqueue replacement, claim, renew,
   reassign, finalize, `Mutate`. Lease expiry does not change it. Responses
   return the new value. Renew and reassign change it, as API-001 says
   (ADR-025 D18), so a heartbeat makes a concurrently prepared
   `expected_item_version` guard fail. A lease expiry that fails an item
   (attempts used up) does not change it either.
9. **Rows.** `fw_row_version` increments once per row per request in
   which the row changes, like `item_version`. It never repeats for a key within
   `request_id_retention_ms` of the key's last deletion: the implementation
   keeps a per-key high-water mark for that long, and a re-inserted row
   starts at the mark plus one. A guard older than the retention window is
   not supported. `RowExpiry::AfterWrite` is reset by every applied write to
   the row, and not by `Skip` or a `Merge` that changes nothing.
10. **Guards on absent records.** `Keyed { expected_version: Some(0) }`
    requires the record to be absent and creates nothing; to create a row
    if absent, use `PutRows` with `RowConflict::Reject` or `Skip`.
    `Some(n > 0)` on an absent record rejects with `VersionMismatch`.

#### `Patch`

Patches apply in order to the JSON body (a `None` body starts as `{}`,
including after a root `Remove`). Each patch's value expression sees the
record as changed by the patches before it. Values are encoded per
§Canonical JSON. `keep_last` must be at least 1. `Append` creates missing
parent objects as `Set` does.

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
changing data belongs in table rows; large payloads are stored outside
Fireweed (ADR-025 D16).

#### `Assert`

`Assert { target, selection, expect }` selects exactly as a `Mutate` with
the same selection does (`Keys` with optional `filter` and
`expected_version` guards, or `Where` within `max_writer_scan_rows`; no
chunking) and rejects the group with `MatchExpectUnmet { matched }` when
the count of matches is outside `expect`. Leased items count whatever
their lease. It applies no action: nothing is written, no version changes,
no expiry is refreshed, and its `OpResult` has `affected = 0` and no
entries. It is the guard for read-your-writes checks inside a group, such
as a suppression check before a send, or the wait-and-event race (DSL-5).
`min > max` fails validation.

#### `dry_run`

`dry_run` runs validation and execution, applies nothing, records nothing,
and reports `status: DryRun` with per-operation counts, exact up to
`max_dry_run_count`, or the `failure` execution would have produced. It
may not be combined with `chunk`.

#### Chunking

1. A `Mutate` with `chunk` must be the only operation in its commit and must
   use `Where`. Its action may not change the access-term field or a key
   field, except a `Transition` or `Retire` when the access term is an
   equality or `In` on `fw_state` and the resulting state is not one of the
   access term's values.
2. Each step examines candidates in access-term order after the
   continuation, up to `max_writer_scan_rows`, and affects up to `limit`
   matches.
   It commits them and returns a `Continuation` positioned after the last
   candidate examined, or `None` when the access range is exhausted.
3. Each step re-evaluates the predicate at its own request time. Candidates
   the continuation has passed are not revisited.
4. A `Continuation` is bound to the space, target object, schema version,
   and the fingerprint of the selection, action, lease policy and
   parameters. A step presenting it with any of those changed fails with
   `InvalidContinuation`. `expect` applies to each step.

## Shapes

### Queue

API-001 semantics, with the changes in this contract. Pause is observable:
a claim on a paused queue returns no items and `queue_paused = true`.
`claim` with `filter` narrows the candidates; ordering is unchanged; the
filter's access term bounds the scan. Key retention
(`client_item_key_retention_ms`) is the dedupe window for event items keyed
by a provider's event id.

### Table

Rows by primary key. `PutRows` inserts with the conflict policy; `Replace`
overwrites every field; `Merge` sets only the given fields of an existing
row and inserts otherwise. `Set` with `Arith` over the old value is an
atomic increment. A conditional increment or compare-and-swap is a `Mutate`
with `Keys`, a `filter` (for example `count < limit`), and
`expect: { min: 1, max: 1 }`. A row past its expiry is invisible at once
and deleted later.

### Sequenced table

A table declared with `KeyAssignment::Sequence` (§Schema rule 10) replaces
the event log and queue history of earlier revisions (ADR-025 D16):

1. Writers append with `PutRows`, omitting the key; the implementation
   assigns the next number per table, in commit order, gapless, never
   reused. `EntryResult.row_key` returns it.
2. Rows are immutable. `Retire` and row expiry remove them.
3. Readers page with a declared read whose filter ranges on the key
   (`seq > $after`) in ascending order, keeping their cursor in a table row
   advanced in the same group as their processing. A reader that has read
   `seq = n` never later sees a row with a lower number appear.
4. A timeline or transition history is rows the consumer appends in the
   same group as the transition it records. The implementation writes
   none of its own; time-driven transitions (lease expiry, cohort
   deadlines) are not recorded.

## Reads

### Point reads

```rust
pub struct GetRequest { pub space: SpaceKey, pub targets: Vec<GetTarget> }
pub enum GetTarget {
    Item { queue: ObjectName, item: ItemRef, include_terminal: bool },
    Row { table: ObjectName, key: Vec<Value> },
    QueueState { queue: ObjectName },
}
pub struct GetResponse { pub as_of: UtcMillis, pub results: Vec<Option<GetResult>> }
pub enum GetResult {
    Item(ItemView), Row(RowView), QueueState(QueueStateView),
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

All targets are read at one linearization point. A request has at most
`max_read_rows` targets; more is `BatchTooLarge`.

### Declared reads

```rust
pub struct ReadDecl {
    pub name: ReadName, pub version: u32,
    pub source: ObjectName,                       // a queue or table
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
   one snapshot. The page token is the position of the last record
   examined, in result order (`order`, or the access field when no `order`
   is declared, then key); it is bound to the read's name, version and
   parameters, and a token from another read or of the wrong shape fails
   with `InvalidRequest`.
3. Aggregate reads return one page.
4. Implementations backed by SQL may name a declared read
   `fw_read_v<N>_<name>` internally. Consumers never see engine tables or
   views.

**Built-in reads** are defined in `fireweed-interface` and exist in every
space. Each takes the queue as a parameter `queue`.

| Read | Version | Parameters | Output |
|------|---------|------------|--------|
| `fw_items` | 1 | `queue`; exactly one of `state` (including `complete` and `failed` within retention), `group_key`, `worker_id`, `key_prefix` | item rows: every system field including `fw_priority`, and declared fields, no body; item-id order |
| `fw_gates` | 1 | `queue` | blocked gate keys with `gate_epoch`, in gate-key order, paged by `max_read_rows` |
| `fw_queue_counts` | 1 | `queue` | counts by `fw_state`, from the same source as `metrics`, with no scan budget |

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
    InvalidContinuation,
    DurabilityPending { reached: DurabilityLevel },
    Contention,
    CommitTimeout, RateLimited { retry_after_ms: Option<u64> }, Unavailable(String),
    SpaceBusy, InstanceSuperseded, Internal(String),
}
pub enum Unsupported { Shape(ShapeKind), QueueFeature(QueueFeature), Durability(DurabilityLevel), Limit(LimitKind) }
pub enum LimitKind { Ops, Entries, RequestBytes, InList, ClaimBatch, CohortSize, LeaseDuration, RequestIdRetention, Objects }
```

| Error | Kind | Retry |
|-------|------|-------|
| `SpaceNotFound`, `ObjectNotFound`, `ReadNotDeclared`, `FeatureUnsupported` | Definitive | After a schema change |
| `SchemaConflict` | Definitive | After re-reading the schema |
| `IndexNotReady` | Definitive | Later, same `request_id` |
| `SelectorNotIndexed`, `GatesNotEnabled`, `InvalidRequest`, `BatchTooLarge`, `Evaluation`, `ScanLimitExceeded`, `InvalidContinuation` | Definitive | After changing the request |
| `RequestIdConflict`, `RequestExpired` | Definitive | With a new `request_id` |
| `DurabilityPending` | Definitive: committed at `reached` | Same `request_id` to wait again |
| `Contention`, `RateLimited`, `SpaceBusy` | Definitive: nothing committed | Same `request_id` (`RateLimited` after `retry_after_ms`) |
| `CommitTimeout`, `Unavailable` | Indeterminate | Same `request_id`, or `get_request` |
| `InstanceSuperseded` | Definitive: this instance no longer owns the space, and the request did not commit on the current owner. A local engine fenced by a higher ownership epoch returns it for every later request and for requests still waiting for `Durable`. | No; route to the new owner |
| `Internal` | Definitive: a broken invariant was detected; nothing committed | No |

A rejected group is a recorded outcome, not an error.

## Implementation rules

### PostgreSQL

Revision 8: these rules become obligations of the PostgreSQL backend's
lowered plans (§Compilation model). The first increment,
`fireweed-interface-postgres`, does not follow rules 1, 3 and 4 yet: every
write to a space takes a lock on that space's row, so writes to one space
are serialized across all processes, and a request loads the whole queues
and tables it references, runs the shared request logic, and writes back
what changed. Reads hold the space row `FOR SHARE` and fix their time after
that lock, so a read and a write to one space are ordered. That meets this contract's ordering and time rules trivially
and passes the same conformance suite, but costs a full object load per
request; indexed loading and the protocol below are the next steps.

1. **Transactions.** Each request is one transaction. `commit` and chunk
   steps run at `SERIALIZABLE`, so guards on absent keys and `MatchExpect`
   over a range cannot be defeated by write skew. On a serialization
   failure the implementation retries the whole request up to a documented
   bound, then returns `Contention`. Nothing from a failed attempt survives.
   Verbs (`enqueue`, `claim`, `claim_by_item_ids`, `renew`, `finalize`) run
   at `READ COMMITTED` with this lock protocol, in several statements: lock
   the queue-state row `FOR SHARE`; lock the gate rows of candidate items
   `FOR SHARE`; lock cohort and group rows the verb changes `FOR UPDATE`;
   lock items `FOR UPDATE` (`SKIP LOCKED` for claims). `SetGates` and
   `SetQueueState` lock those rows `FOR UPDATE`, so a claim and a gate or
   pause change are ordered. Reads that span several targets use one
   `REPEATABLE READ` read-only snapshot.
   PostgreSQL's serializable checking does not cover `READ COMMITTED`
   writers, so the guarantee against write skew between a verb and a group
   rests on the lock protocol. Every row a verb changes is locked, and every
   verb locks its queue's state row `FOR SHARE`. A group whose operation
   selects or checks a queue by predicate (`Where`, `MatchExpect`) locks that
   queue's state row `FOR UPDATE` first, so it and any verb on the same
   queue are ordered. (Review round five showed that without this a group
   checking "no leased items" and pausing the queue could commit alongside a
   concurrent claim.) PG-3 covers group against group; a verb-against-group
   vector is open work.
2. **Request records.** The request record is written in the same
   transaction as the effects. A rejected group rolls back to a savepoint
   taken before its effects and commits the record. Concurrent attempts
   with one `request_id` conflict on the record's unique key; the loser
   reads the winner's record.
3. **Time.** The authority is the server clock, read with
   `clock_timestamp()` (under conformance, the `SharedClock` value read by
   the implementation at the same points). `commit` and chunk steps sample
   `t` as their first action. Verbs sample a provisional time, lock, then
   sample `t` (§Time rule 4). `t` is passed to every statement as a
   parameter; no statement calls `now()` or `clock_timestamp()` itself.
4. **Claims** use `FOR UPDATE SKIP LOCKED` over the access-term index.
5. **Text** columns, indexes and compiled comparisons use the `C`
   collation.
6. **JSON** is stored as `jsonb` after normalization (§Canonical JSON).
7. **`Int` `Sum`** accumulates in `numeric` and checks the final range.
8. **Sequenced tables** assign the key from a per-table counter in the same
   transaction, so numbers follow commit order and a rolled-back group
   leaves no gap. Under the target protocol the counter row is locked
   `FOR UPDATE`, which serializes appends to one sequenced table.
9. **Durability.** Every acknowledged request is one committed
   transaction; the server is assumed fully replicated, so it is `Durable`
   (§Durability). A `FireAndForget` request sets `synchronous_commit = off`
   for its transaction only.

### Local engine

1. One database per space, with one writer. A request is one write
   transaction. Time is non-decreasing per space.
2. Durability follows Niflheim's write modes (§Durability): one local fsync
   per batch, and commit frames shipped to S3 in order and in batches.
3. Ownership belongs to the embedder's control plane (ADR-025 D15), which
   grants an epoch per space. The engine takes a space at the granted epoch
   through a method on its own type (`LocalDataPlane::acquire`), not a
   `DataPlane` operation. Before accepting writes to the space, the engine
   writes a takeover record at the next S3 log sequence of that space. Log objects
   are written with `If-None-Match: *`, so a stale writer's next upload
   fails and that engine stops. `FireAndForget` and `Tracked` commits are
   protected only by the control plane's rule: no new epoch while the old
   process may still run. A second open on one host fails with `SpaceBusy`
   while the holder lives. Clients recheck placement on an interval and
   after `SpaceBusy` or `InstanceSuperseded`.
4. **Fenced.** When an upload fails because a higher epoch wrote the next
   sequence of a space, the engine stops serving that space for good: every
   later request to it, and every request to it still waiting for
   `Durable`, fails with `InstanceSuperseded`
   (definitive; it did not commit on the current owner). A space opened at a
   higher epoch restores from the S3 log up to its takeover record; commits
   the old writer acknowledged at `FireAndForget` or `Tracked` after its last
   shipped frame are lost, as with machine loss.
5. **Item state across an epoch change.** Item state, including
   `attempt_count`, `item_version` and leases, is space data and moves with
   the log. A clean handover (close, ship, open at the next epoch) keeps all
   of it. After a crash, the new owner has every commit up to the last
   shipped frame; claims acknowledged at `Tracked` after it are lost, so
   their attempt charges are lost too. A consumer that needs every charge to
   survive machine loss claims at `Durable`. Item leases are Fireweed's;
   the control plane's ownership lease covers only which node writes the
   space.
6. Lease expiry and row expiry are lazy. Reads run on MVCC snapshots.
7. Speed gates B1 to B4 apply.

## Consumer inventory

What Snorri and Cayce would otherwise keep in memory, in opaque JSON or in
their own stores, and the shape that holds it.

### Snorri

From Snorri TD-012 revision 6 §3.7 (uncommitted, not verified in this
repository). Rates are per recipient of a single-send campaign.

| # | State | Size and rate | Shape and mechanism |
|---|-------|---------------|---------------------|
| 1 | Instance body: variables, node, waits, effect references, inbox, audit ring, engagement profile | Body up to 2 MiB; output up to 1 MiB; rewritten about 3 times per recipient plus once per profile update | Queue item with a `Json` body (D8). Field-level changes are `Patch`; a profile update or inbox append is a `Mutate` with `Patch` and `LeasePolicy::Preserve`, which bumps `item_version` so the worker's guarded commit sees it. Large outputs are stored outside Fireweed and referenced from a `Text` field (ADR-025 D16). |
| 2 | Consumed inputs | Up to 16, each up to 64 KiB; one per external event | Event items in an inputs queue, finalized `Complete` in the transition's group; or kept in the body and trimmed with `Patch`. |
| 3 | Waits and await routing | Up to 128 per instance; names of 71 bytes | An indexed `TextSet` field on the instance item, changed with `SetAdd` and `SetRemove`; routing is a declared read with `Contains`. |
| 4 | Effects: request, intent, receipt | Up to 256 KiB per effect item; intent commit about 1.2 KiB; 3 commits per send | Effect items in an effects queue. The intent commit sets `min_durability: Durable` (§Durability). |
| 5 | Handler idempotency per idempotency key | Small; one write per handler call | Table keyed by idempotency key with `RowExpiry::AfterWrite`. |
| 6 | External events and dedupe | Up to 64 KiB per event; 31-day dedupe; at most 3 claims per event | Event items keyed by provider event id, with `client_item_key_retention_ms` of 31 days. |
| 7 | Audit and history | One per transition | Hash chain and last 16 summaries in the body (`Patch` `Append`, `keep_last: 16`). The timeline the UI and event triggers need is a sequenced table written in each transition's group (ADR-025 D16). Full history is exhaust for Niflheim (Snorri's TransitionObserver). |
| 8 | Definitions | 31–43 KB measured; capped at 256 KiB (ADR-025 D31); once per artifact per queue | Table row keyed by artifact digest and queue, with the definition in a `Bytes` or `Text` field within `max_value_bytes`. Shipped templates are 939 to 2,428 bytes. An oversized definition is rejected when it is created; a write past the cap gets `ValueTooLarge`. |
| 9 | Admission in-flight chunk | About 100 B per run per shard; twice per chunk | Table row keyed by run and shard: created with `PutRows` and `RowConflict::Reject`, then updated by `Mutate` with `Keys` and `expected_version` set to the version last read (ADR-025 D21). |
| 10 | Rate-limit counters (today Dragonfly in Cayce) | One counter per slot key (account, contact, hour); one conditional increment per effect intent | Table rows keyed by slot and hour bucket with expiry. In one group: `PutRows` with `RowConflict::Skip` and `count = 0` (creates the bucket if absent), then `Mutate` with `Keys`, `filter: count < limit`, `Set count = count + 1` and `expect: { min: 1, max: 1 }`. `count` is `NonDecreasing`. Keyed by account, so it lives outside a campaign space (ADR-025 open questions 5 and 25). |
| 11 | Enrolment history for re-entry across runs | One row per (workflow family, subject); one write at admission and one at the enrolment's end | Table in a family space (ADR-025 D29): `rule`, and `NonDecreasing` `last_enrolled_at` and `last_ended_at`. A first enrolment is `PutRows` with `RowConflict::Reject`. A re-entry is a `Mutate` with `Keys`, a filter on the rule and the timestamps, `Set last_enrolled_at = now()` and `expect: { min: 1, max: 1 }`. The end is a `Mutate` filtered on `(last_ended_at IS NULL OR last_ended_at < $ended_at) AND last_enrolled_at = $enrolled_at` with `expect: { min: 0, max: 1 }`, so a stale or repeated end commits as a no-op. An enrolment is open while `last_ended_at` is null or earlier than `last_enrolled_at`. |

Durability by class (Snorri TD-012 revision 7, uncommitted there):
`Durable` for effect intents, admission chunks, event delivery and
operator writes; `Tracked` for transitions, event consumption, receipts,
poison parking and sweeper writes; no `FireAndForget`. On PostgreSQL every
one of these commits is `Durable` (§Durability).

Snorri's queues declare `max_attempts = u32::MAX` (ADR-025 D30): Snorri
parks an item after K claims without a commit and reports it to Cayce's
control plane, instead of letting lease expiry fail it. On the local engine
it claims at `Durable`, so a crash cannot lose a charged attempt.

Not shapes: the compiled WASM cache; the open-runs list and next-wake time
(derived through `fw_queue_counts` and a declared read ordered by
`fw_not_before`); the old adapter's caches.

### Cayce (draft; Cayce to confirm)

| State | Shape and mechanism |
|-------|---------------------|
| Per-recipient, per-run lifecycle | Queue item per recipient, with declared fields (campaign, step, domain, status) for claims, bulk changes and UI reads |
| Recipient profile, suppression flags | Table keyed by recipient |
| Frequency caps and send-rate windows | Table counters keyed by bucket |
| Delivery and engagement events | Event items deduplicated by provider id that drive workflows; a sequenced table for timelines and export |
| UI counts by state, lag, send rates | Declared aggregate reads over queue items and tables |
| Rendered messages and attachments | Outside Fireweed (ADR-025 D16) |

### Suppression (Cayce; tested by SUPP-1 to SUPP-3)

Suppression is an authoritative keyed store, key (account, channel,
address), written by events (permanent email bounce, SMS STOP, probably
complaint). As a Fireweed table `supp` with fields `reasons` (`TextSet`) and
`lifted_at` (`Timestamp`):

| Need | Mechanism | Status |
|------|----------|--------|
| (1) Keyed upsert with set-union, reasons only added | `reasons` declares `MergeRule::AddOnly`. One group: `PutRows` with `RowConflict::Skip` (creates the row if absent), then `Mutate` by `Keys` with `Set reasons = SetAdd(reasons, [$reason])`. Both run at one linearization point, so there is no read-modify-write race. A write that removes a reason rejects the group with `MergeRuleViolated`. A lift is a `Mutate` with `override_merge_rules: true` that clears `reasons` and sets `lifted_at`; authorizing it belongs to the adapter. | Works (SUPP-1). |
| (2) Idempotent under duplicate and out-of-order events | The event id is the `request_id`, so a duplicate replays within `request_id_retention_ms`; `SetAdd` is idempotent and order-free, so later duplicates converge anyway. The `Mutate` filter `Coalesce(lifted_at, MIN) < $event_at` drops events older than the last lift. | Works (SUPP-1). |
| (3) Durable before the event is acknowledged | `min_durability: Durable` on the event's commit; `DurabilityPending` tells the caller the commit exists. | Works on PostgreSQL, where every default commit is `Durable`, and on the local engine on request. |
| (4) Read-your-writes at the moment of the effect | Put the check in the effect's own group: `Assert` on the suppression key with `filter: reasons is not null` and `expect: { min: 0, max: 0 }`, followed by the effect's operations. The group rejects with `MatchExpectUnmet` if the address is suppressed at that instant. `Assert` writes nothing and changes no version. | Works (SUPP-2) when the suppression table and the effect's queue are in one space. With one space per campaign (ADR-025 D13) suppression is account-scoped, so this needs a copy of the suppression row in each campaign space (ADR-025 open question 25, option a). A claim filter cannot read another object (no joins); to keep suppressed recipients from being claimed at all, the suppression event can also block a gate key such as `supp:<address>` carried by those items, in the same group. |
| (5) Bounded batch read at claim time | `get` with up to `max_read_rows` `Row` targets, one snapshot; more fails with `BatchTooLarge`. A declared read over `address` with `In` also works, bounded by `max_in_list` and the scan budget. | Works (SUPP-3). |

## Profiles

A profile names what a consumer needs. An implementation satisfies a
profile when `capabilities()` covers all of it.

| Profile | Shapes and features | Minimum limits | `max_durability` |
|---------|---------------------|----------------|------------------|
| `snorri` | Queue, Table (including sequenced tables); `DynamicGates`, `Recurrence`; `Json` bodies | `max_body_bytes` 2 MiB; `max_value_bytes` 256 KiB; `max_set_values` 128; `max_request_bytes` 4 MiB; `max_entries_per_commit` 4,096; key retention 31 days | `Durable` for the zero-loss send path |
| `cayce-ui` | Queue, Table (including sequenced tables); declared reads with aggregates | `max_read_rows` 1,000; `max_read_scan_rows` 5,000,000 (snapshot reads, not the writer) | — |
| `recipient-lifecycle` | Queue, Table; chunked `Mutate` | `max_entries_per_commit` 10,000; `max_selector_affected` 10,000; `max_writer_scan_rows` 50,000 | — |

## Conformance

### Harness

**Built today.** `fireweed_interface_conformance::run` takes a factory
that returns an `Instance`: the implementation on fresh storage, and
optionally a `Peer`, a function that opens another instance on the same
storage (in-memory: shared state; PostgreSQL: a new connection to the same
schema). Tests that need concurrent instances (REC-5, MULTI-1, TIME-3,
PG-1 to PG-3, DSL-5, DSL-8) run on threads across peers when a peer
exists. The time authority is a `ManualClock` shared by every instance.

An `Instance` may also carry `Controls`, the engine's own handles for the
durability and ownership tests. Each is optional, and a test whose control
is missing is skipped with "engine supplies no `<control>` control", so
every engine runs the same tests from the shared suite and reports the
same gaps:

| Control | Does | Tests |
|---------|------|-------|
| `hold_replication(bool)` | Stops commits reaching `Durable`, then releases them | DUR-3, DUR-6 |
| `crash_and_reopen(clock)` | Abandons the instance without a clean close (a killed child process where the engine runs one) and opens the same storage | DUR-4, SHARE-1 |
| `lose_local_storage(clock)` | Abandons the instance, discards local storage, opens from what reached `Durable` | DUR-5 |
| `open_competing(clock)` | Opens the space a second time on one host without a new grant; must fail | SHARE-1 |
| `take_over(clock)` | The control plane grants the space to a new owner at a higher epoch | FENCE-1 |

The child-process protocol below remains the design for faults inside one
request (`StallAtReceive`, REC-4) and for SIGKILL; an engine may implement
`crash_and_reopen` with it.

```rust
pub trait Harness: Send + Sync + 'static {
    type Impl: DataPlane + 'static;
    fn create_storage(&self) -> BoxFuture<'_, Result<StorageSpec, HarnessError>>;
    fn open<'a>(&'a self, storage: &'a StorageSpec, clock: SharedClock)
        -> BoxFuture<'a, Result<Self::Impl, HarnessError>>;
    fn child_binary(&self) -> Option<PathBuf>;
    /// Optional: destroy the implementation's local copy, for Durable scenarios.
    fn lose_local_storage<'a>(&'a self, storage: &'a StorageSpec)
        -> BoxFuture<'a, Result<(), HarnessError>> { Box::pin(async { Err(HarnessError::Unsupported) }) }
}
pub struct StorageSpec(serde_json::Value);    // opaque to the suite
pub struct SharedClock { pub path: PathBuf }  // file holding the current UtcMillis; suite-owned
pub enum HarnessError { Unsupported, Setup(String) }

pub trait FaultHooks: Send + Sync {           // implemented inside the implementation crate
    fn arm(&self, fault: Fault);
}
pub enum Fault {
    BeforeCommit,                // fail the attempt before anything is written
    AfterCommitBeforeResponse,   // commit, then drop the response
    StallAtReceive,              // hold the request after receipt, before its record is read
    HoldReplication,             // keep commits below Durable until released
}
```

`StallAtReceive` holds the request before it reads or writes its request
record or takes any lock, so a replay can run to completion meanwhile.
`FaultHooks` are compiled in with the implementation crate's
`conformance-faults` cargo feature, which the child binary enables.

1. Every process reads time from the `SharedClock` file; only the suite
   advances it.
2. The child binary's `main` calls `fireweed_conformance::child_main`,
   which opens the storage named on its command line and serves framed JSON
   messages on stdin and stdout. Each message carries an id; requests may be
   concurrent. Messages are either `DataPlane` requests (the request types'
   serde form) or control messages: `ArmFault`, `FaultReached` (sent by the
   child when an armed fault is hit), `Release`, `Exit`.
3. Faults act inside the implementation's commit path through `FaultHooks`,
   which the implementation compiles in for test builds. A wrapper around
   `DataPlane` does not satisfy them.
4. Scenarios run by declared guarantees and capabilities. A scenario
   required by what the implementation declares may not be skipped, except
   that PostgreSQL's `Durable` rests on the server's replication (ADR-025
   D14), which the suite does not test.
5. For the local engine, `Tracked` crash scenarios kill the child process,
   and `Durable` scenarios also destroy its local storage.

### Rule-pinning tests

Each rule has scenarios in `fireweed-conformance`; both engines run the same
vectors. IDs are stable and cited in failures.

| ID | Rule | Scenario |
|----|------|----------|
| TYP-1 | Typing table | Every operator with every disallowed operand type fails validation with `InvalidRequest`; every allowed signature passes. |
| TYP-2 | Typed nulls and params | `Lit(Null(Int))` compares as Int; a parameter of the wrong type fails validation. |
| EVAL-1 | Short-circuit order | In an action value, where a fault would reject: `Set n = Coalesce([1, 1/0])` sets 1; a `Set` of `And([false, 1/0 = 0])` sets false; `Or([true, …])` sets true. |
| EVAL-2 | Faults in predicates are null | `Where status = 'x' AND n / 0 = 1` (indexed `status`) matches nothing and raises nothing, with the faulting conjunct second or last. |
| EVAL-3 | Faults in action values reject | `Set n = i64::MAX + 1` rejects the group with `Evaluation(IntOverflow)`; nothing applied; record written. |
| EVAL-4 | Int division | `-7 / 2 = -3`, `-7 % 2 = -1`, `i64::MIN / -1` faults. |
| EVAL-5 | Float faults | `1.0 / 0.0` and an overflowing product fault. |
| EVAL-6 | Three-valued logic | Truth tables for `And`, `Or`, `Not`, `In` with nulls. |
| EVAL-7 | Text and Bytes order | Code-point order with non-ASCII strings that sort differently under locale collation; byte prefix order. |
| EVAL-8 | Bucket | Negative timestamps floor toward negative infinity; width 0 and > 10 years fail validation. |
| EVAL-11 | Value domains | U+0000 in a parameter fails validation; a Float above 1e290 and a Timestamp after year 9999 are faults. |
| EVAL-9 | Now | Every `Now` in one request, including across group operations, is the same value. |
| EVAL-10 | Sets | `SetAdd` past `max_set_values` faults; `SetRemove` of an absent value is a no-op. |
| CAST-1 | Cast table | Every cell, including null results for out-of-range and unparsable inputs. |
| CAST-2 | RFC 3339 | Offsets, sub-millisecond truncation, leap second → null. |
| JSON-1 | Canonical JSON | `3.0` is stored and read back as `3`; key order not significant; an unsigned integer above `i64::MAX` that `f64` cannot hold exactly is rejected. |
| JSON-2 | Value to JSON | Each `Value` variant's encoding. |
| JSON-3 | Null versus absent | `Json { pointer }` on a JSON `null` yields `Json(null)`; on an absent pointer yields `Null(Json)`; both cast to null. |
| AGG-1 | Aggregate types | Each aggregate's operand and result types; null handling; empty input with and without `group_by`. |
| AGG-2 | Exact Int sum | `Sum` over values whose running sum overflows but final sum fits returns the final sum; a final overflow fails `Evaluation`. |
| AGG-3 | Float tolerance | `Sum` and `Avg` over cancelling values fall within the stated bound. |
| AGG-4 | Percentile rank | Ranks at n = 1, 2, 3, 100 for p = 1, 70,000, 140,000, 500,000, 550,000, 999,999, 1,000,000 millionths; (70,000, 100) is rank 7, which floating-point rank computation gets wrong. |
| IDX-1 | Access term | First qualifying conjunct is chosen; a predicate with none fails `SelectorNotIndexed`; `Keys` needs none. |
| IDX-2 | Index readiness | A request on a building index fails `IndexNotReady`. |
| SCAN-1 | Unchunked scan limit | A `Where` whose matches lie beyond `max_writer_scan_rows` records rejects with `ScanLimitExceeded`. |
| SCAN-2 | Chunk progress | Steps over a range with no matches advance and finish; each step examines at most `max_writer_scan_rows`. |
| SCAN-3 | Claim scan limit | A claim over a queue whose head is gated returns a prefix of claim order with `scan_limited`. |
| SCAN-4 | Read scan limit | A rows read pages at the limit with `scan_limited`, and the concatenated pages are in `order`; an aggregate fails. |
| SCAN-5 | Strict filtered claim | A filtered claim on a `Strict` queue returns exactly the best-priority matches, also when scan-limited. |
| SCAN-6 | Physical counting | A `Where` over many expired-but-stored rows reaches the scan budget. |
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
| GRP-9 | Cohort split | A `Retire` of one member of a live cohort rejects with `CohortSplit`; selecting all members succeeds. |
| GRP-10 | Version bumps | Each operation's effect on `item_version` and `fw_row_version`; renew returns the new version; lease expiry does not bump. |
| GRP-11 | Row incarnation | Delete, re-insert: the new row's version is above the old one, and a guard with the old version mismatches. |
| GRP-12 | Absent guards | `Some(0)` on an existing row and `Some(3)` on an absent row both reject with `VersionMismatch`. |
| GRP-13 | Chunk and cohorts | A chunk step over a cohort that is partly unmatched, or larger than `limit`, skips it, counts it in `skipped_cohorts`, and still advances. |
| GRP-14 | Constraints | A `Set` that duplicates a `Unique` value rejects with `UniqueViolation`; a `Set` of null into a non-nullable field rejects with `MissingField`. |
| PATCH-1 | Patch matrix | Every row of the §Patch table. |
| FIN-1 | Finalize order | `metadata`, `body`, `patch`, `set`, outcome, with `expected_item_version` checked first. |
| CHK-1 | Continuation binding | Changing selection, action, policy or params with a continuation fails `InvalidContinuation`. |
| CHK-2 | Chunk restrictions | A chunked `Mutate` with another operation, a `Keys` selection, or an action on the access field fails validation; a `Transition` out of an `fw_state` access term is allowed; one back into it is not. |
| DRY-1 | Dry run | Counts match a real run; nothing applied or recorded; the `request_id` stays unused. |
| REC-1 | Rejections recorded | A rejected group's replay returns the same rejection, even after state changes that would let it commit. |
| REC-2 | Validation not recorded | A request failing validation can be resent with the same id after fixing it. |
| REC-3 | Fingerprint | Changing `min_durability` is not a conflict; changing anything else is `RequestIdConflict`. |
| REC-4 | Stalled original | With `StallAtReceive` on the original, a replay commits or rejects; after `Release`, the original returns the recorded result and does not commit. |
| REC-7 | Verb replay | A `finalize` and a `renew` with mixed per-entry outcomes replay the same per-entry outcomes. |
| REC-8 | Token redaction | `get_request` of a claim returns no tokens; a replay returns them. |
| REC-5 | Lost response | A committed response is dropped; `get_request` and replay, on this instance and on a peer, return the committed result. |
| MULTI-1 | Instances agree | Two instances on one storage see each other's commits at once. |
| REC-6 | Claim replay | Replay while leases are active returns the same claim; after they end, `RequestExpired`; an empty claim replays empty. |
| TIME-1 | Time-driven transitions | Lease expiry, cohort deadline, retention and row expiry under `SharedClock`, with no tick. |
| TIME-3 | Monotonic along visibility | Under concurrent conflicting requests, `as_of` never decreases along observed effects (peers in child processes). |
| TIME-4 | eligibility_time | Earlier and later than the authority; lease expiry still from the authority. |
| TIME-5 | Lazy before act | A request touching a record first applies that record's due transitions. |
| COH-1 | Cohort deadline | A missing member, a gated member and a paused queue each fail the cohort at the deadline, including when the gate or pause is removed after the deadline before any request touches the cohort; a claimed cohort is exempt; enqueue after failure is `CohortFailed`. |
| DUR-1 | Levels reported | Every write response, including rejections and `apply_schema`, reports a level within the declared range. |
| DUR-2 | min_durability above max | Fails `InvalidRequest`. |
| DUR-3 | DurabilityPending | With `hold_replication`, a commit asked for at `Durable` returns `DurabilityPending { reached }` with `reached` below `Durable`; a replay after release returns `Durable`. |
| DUR-6 | Prefix-closed | A commit acknowledged below `Durable` while replication is held reports `Durable` on replay once a later commit is `Durable`. |
| DUR-4 | Crash | After `crash_and_reopen`, every commit acknowledged at `Tracked` or above is present. |
| DUR-5 | Storage loss | After `lose_local_storage`, every commit acknowledged at `Durable` is present. |
| DUR-7 | Requested level | Each offered level is acknowledged at or above what was asked and at most the maximum. |
| LEASE-MAX | Redelivery bound | Lease expiry returns the item to pending until its claims have used `max_attempts`; the expiry after that fails it with `attempts-exhausted` at the expiry instant, without a version bump. |
| LEASE-MAX-2 | Claim past the limit | After `max_attempts` claims each ended by `Release`, the item is pending with `attempt_count = max_attempts` and still claimable; that claim charges `max_attempts + 1`. Its lease expiry fails the item with `attempts-exhausted` at `lease_expires_at` with `item_version` unchanged; a `Retry` on it instead fails it the same way. |
| LIN-1 | Concurrent claims | Peers never hold two active leases on one item. |
| LIN-2 | Read after commit | A read starting after a commit's response observes it, across shapes. |
| SHARE-1 | SingleProcess | `open_competing` fails with `SpaceBusy` while the holder lives; after `crash_and_reopen`, the space opens and has its schema. |
| FENCE-1 | Fenced writer | After `take_over`, the new owner has every commit acknowledged at `Durable`; the old owner's next `Durable` commit, and every request after it, fails with `InstanceSuperseded`; the new owner commits. |
| ASSERT-1 | Assert writes nothing | A passing `Assert` changes no version and refreshes no expiry. |
| ASSERT-2 | Assert rejects | A failing `Assert` rejects the group and rolls back earlier operations; a version guard fails with `VersionMismatch`. |
| ASSERT-3 | Assert and leases | `Assert` counts leased items and leaves `item_version` unchanged, so the worker's guarded finalize still commits. |
| MERGE-1 | NonDecreasing | Equal or larger values apply; smaller or null values reject, through `Set`, `Replace` and `Merge`, on Int, Float and Timestamp. |
| MERGE-2 | AddOnly | Unions apply; removing a member or clearing the set rejects with `MergeRuleViolated`. |
| MERGE-3 | Override | `override_merge_rules` lets a `Mutate` break a rule; it is refused on a queue. |
| MERGE-4 | Rule placement | Rules on queue fields, key fields or mismatched types fail schema validation. |
| SEQ-1 | Assignment | Keys are 1, 2, 3, ... across groups, returned in `row_key`, and stored. |
| SEQ-2 | No gaps, no reuse | A rejected group assigns nothing; a supplied key is `Invalid`; `Set` is refused; a retired number is never reused. |
| SEQ-3 | Cursor paging | A reader paging by key never goes backwards and sees later appends after its cursor. |
| SEQ-4 | Declaration | `Sequence` needs one non-null `Int` key; key assignment cannot change. |
| SUPP-1 | Suppression upsert | Set-union of reasons, duplicates and out-of-order events converge; a plain clear is refused; a lift needs the override. |
| SUPP-2 | Suppression guard | An `Assert` in the send's group rejects a send to a suppressed address and leaves the row's version unchanged. |
| SUPP-3 | Batch read | `get` reads up to `max_read_rows` rows in one snapshot; more is `BatchTooLarge`. |
| DSL-1 | Timer instant | A timer is due exactly at its instant, not a millisecond before; instances due at one instant are claimed in priority order. |
| DSL-2 | Timer attempts | Waiting through `Retry` charges attempts and ends in `attempts-exhausted`; waiting through `Rearm` does not. |
| DSL-3 | Wait then event | The instance parks on an await; the event finds the waiter and wakes it with the payload. |
| DSL-4 | Event then wait | The event is stored first; the instance's wait consumes it instead of parking. |
| DSL-5 | Wait and event race | On threads and across peers, exactly one side delivers the payload, once. |
| DSL-6 | Two awaits | The first event removes only its own await; the second wakes again; payloads arrive in order; an unawaited event is stored. |
| DSL-7 | Duplicate and out-of-order events | Replays by event id never add a second payload or recreate a consumed event. |
| DSL-8 | Fan-in | Children completing on several threads decrement a join counter; only the last wakes the parent; a duplicate completion is refused. |
| DSL-9 | Suppressed send | The send intent commits with the step only if the address is not suppressed at that instant, including suppression that arrives after the claim. |
| DSL-10 | Crash loop | A worker that crashes every time ends with the instance failed after `max_attempts`; a crash after a successful step does not count against the next. |
| DSL-11 | Cancel a run | A chunked `Mutate` cancels a run while some instances are leased, under each lease policy. |
| DSL-12 | Schedule tick | A recurring entry ticks, replays by request id, and handles missed ticks. |
| DSL-13 | Timeline | Each transition appends a timeline row in its own group; a reader pages them by cursor. |
| DSL-14 | Rate cap | A per-contact counter with `NonDecreasing` and a `count < cap` filter admits exactly `cap` sends. |
| DSL-15 | Suppression copy across spaces | An account space S holds authoritative suppression and numbers each change from a sequenced table; a campaign space C holds a copy. Activation seeds C and opens the send gate in one group. Out-of-order fan-out converges. A send's `Assert` blocks while C's copy is suppressed. The lift is written to S first, and C clears only by an override lift marker carrying S's number; a plain clear is `MergeRuleViolated`. Between S's lift and the marker, sends stay blocked (fail closed). A late pre-lift copy is dropped by S's number even with a provider time skewed ahead, and a post-lift bounce re-suppresses. |
| DSL-16 | Duplicate admission | A repeated subject in one batch is `Duplicate`, naming the first item. After completion the subject stays `Duplicate` (re-entry `never`), including as a tombstone, up to `client_item_key_retention_ms`, where it is admitted again. |
| DSL-17 | Effect outcome unknown | Recording an unknown outcome replays identically. Re-enqueueing the key is `Duplicate`, so one effect exists. The reconciler sees the same id, key and body, and completes it once; a second completion is rejected. |
| DSL-18 | Crash before the provider call | An intent committed at `max_durability` reports at least that level. After the worker's lease expires, the effect returns with the same id, key and body and one more attempt. Replaying the intent returns the identical response and creates no second effect. |
| SCH-1 | Schema versioning | Expected-version conflict; identical re-apply; non-additive change rejected; unsupported feature rejected. |
| READ-1 | Declared reads | Version immutability; pagination; built-in reads by each parameter. |
| READ-2 | Null ordering and grouping | Nulls sort first ascending and last descending; nulls form one group. |
| ENQ-1 | Key conflicts | `Duplicate`, `ReplacePending` (with `superseded_item_id`, and `Superseded` on the old id) and `Reject`. |
| TBL-1 | Expiry refresh | `AfterWrite` resets on an applied write, not on `Skip` or a no-op `Merge`. |
| MET-1 | Metrics populations | Lifecycle counts include not-yet-due and gated items; progress metrics exclude paused queues and use base eligibility. |
| PG-1 | Postgres isolation | Under concurrent group commits, `Contention` is returned only after retries, and no partial effect survives. |
| PG-2 | Postgres time | `as_of` of conflicting requests never decreases in commit order. |
| PG-3 | Write skew | Two groups that each require the other's key absent and insert their own: at most one commits. |

### Status (revision 7)

`fireweed-interface-conformance` implements the rule-pinning tests and the
DSL scenarios as executable checks through `DataPlane` only. Expression
rules run through `Mutate` `Set` (action position) and `Where` dry runs
(predicate position), so each implementation is tested on its own
evaluation. The DSL scenarios (`scenarios.rs`) express Snorri workflow
patterns from the Turso-queue session's DSL check as Fireweed requests and
assert on final state through `get` and `read`.

| Implementation | Pass | Fail | Skip |
|----------------|------|------|------|
| `fireweed-interface-memory` | 137 | 0 | 12 |
| `fireweed-interface-postgres` (PostgreSQL 16) | 137 | 0 | 12 |

Skipped on both: GRP-9, GRP-13 and COH-1 (cohorts are not declared yet);
IDX-2 (harness gap: an index still building); REC-4 (harness gap:
`StallAtReceive`); SCAN-6 (local engine only: expired rows still stored);
FENCE-1 (no `take_over` control: neither has ownership epochs). In-memory
also skips DUR-3 to DUR-6 (`Durable` and `Tracked` not declared) and
SHARE-1 (no `open_competing` control). PostgreSQL also skips DUR-2
(`Durable` is offered), DUR-3, DUR-5 and DUR-6 (no `hold_replication` or
`lose_local_storage` control: the server is assumed fully replicated, so
neither fault exists) and SHARE-1 (`MultiProcess`; MULTI-1 covers it). Its
`crash_and_reopen` opens a new instance on the same database, so DUR-4
runs. The Turso-queue session's `fireweed-local` reports 137/0/11 on
Turso and SQLite before these controls; wiring its engine tests into them
moves DUR-3 to DUR-6, SHARE-1 and FENCE-1 into the shared suite.
Several tests still cover only part of what their row claims (TYP-1,
CAST-1, GRP-3); generating them from the tables is open work.

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
| `fireweed-engine` PostgreSQL backend (today `fireweed-interface-postgres`, an interpreter over loaded records) | `FireAndForget` / `Durable` (default `Durable`) | `Immediate` | `MultiProcess` | Queue, Table |
| `fireweed-engine` Turso backend (MVCC on NVMe, S3 log) | `FireAndForget` / `Durable` (default `Tracked`) | `Immediate` | `SingleProcess` per space | Queue, Table |
| `fireweed-engine` SQLite backend | `FireAndForget` / `Tracked` (default `Tracked`) | `Immediate` | `SingleProcess` per space | Queue, Table |
| `fireweed-engine` memory backend (today `fireweed-interface-memory`) | `FireAndForget` / `FireAndForget` | `Immediate` | `SingleProcess` | Queue, Table |

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
| `upsert` | `enqueue` with `KeyConflict::ReplacePending` | needs a request id (helper generates one); `superseded_item_id` returned in `EntryResult` |
| `batch_update` | `commit` with `Mutate` by `Keys` (`Set`, `SetMetadata`, `ReplaceBody`, `Reschedule`, `Gates`) | now all-or-nothing across entries (was per entry); per-key `expected_version` |
| `reschedule` | `Mutate` `Reschedule` with `LeasePolicy::Preserve`; clearing `not_before` is `ScheduleChange::Clear` | none |
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
| `claim_by_query` | `claim` with `filter` | the filter needs an `Eq`/`In` access term on a `Lookup` field; results may be partial (`scan_limited`) |
| `hot_projection_capabilities` | `capabilities()`, `get_schema` | — |
| `claim_across_queues` | facade helper: one `claim` per queue with a shared `eligibility_time` | no common lease instant; not atomic |
| `operator_pause`, `operator_resume` | `SetQueueState` | none |
| `operator_admin_state` | `get` of `QueueState` | none |
| `operator_inspect_item` | `get` with `include_terminal` | none |
| `retained_items` | built-in read `fw_items` with `state` `complete` or `failed`, paged in key order | none |
| `operator_repair`, `operator_redrive` | `Mutate` with `Set`, `Transition` (`Release`, `Retry`, `Redrive`) and `LeasePolicy::Break` for forced actions | none |
| `operator_purge` | `Mutate` `Retire` | `max_affected` becomes `limit` with `expect` |
| `operator_archive` | `Mutate` `SetMetadata` (`archived: true`) on retained terminal items | none: the item stays retained; nothing is deleted |
| `operator_get_operation` | `get_request` | read-only |
| `operator_audit_records` | declared read on an audit sequenced table the operator path appends to in each action's group | principal and reason are fields of the row |
| `projection_control`, `snapshot_policy`, `set_snapshot_policy`, `should_snapshot`, `snapshot_now`, `latest_snapshot_info`, `compact_log_behind_snapshot`, `last_rebuild_stats`, `ownership`, `renew_owned`, `reclaim_expired`, `current_position`, `peek` | object-log implementation type | leave `Fireweed` (ADR-025 open question 3) |

## Compatibility

- Adding an enum variant, a shape, a queue feature, a built-in read
  version or a `DataPlane` method with a default body is compatible.
- In v0, adding a public struct field is breaking (crate rule 3); it
  becomes compatible once structs are `#[non_exhaustive]` with builders.
- Changing a signature, a request's meaning, an expression rule or a
  rule-pinning test's expected result is breaking.
- Changing an implementation's declared guarantees is breaking for that
  implementation.

## Validation checklist

- [ ] The dependency test enforces rules 1, 4, 5 and 6; the compile test
      enforces rule 3.
- [ ] `DataPlane` is dyn-compatible.
- [ ] Every rule-pinning test passes on every implementation, with no
      required scenario skipped. Not met yet: the harness controls
      (`StorageSpec`, `SharedClock`, `FaultHooks`, child processes) are not
      built, so IDX-2, REC-4, REC-5, DUR-4, SHARE-1 and TIME-3 cannot run;
      this blocks acceptance.
- [ ] The differential test passes for every implementation against
      Postgres.
- [ ] Every Snorri inventory item runs on the `snorri` profile with no
      Snorri-owned store.
- [ ] Each mapping-table behaviour change is in the API-005 migration notes.
- [ ] ADR-025's contract amendments have landed before this contract is
      accepted.
