---
ddx:
  id: adr-queue-interface-implementation-boundary
  depends_on:
    - api-native-client-interface
    - adr-hexagonal-architecture-and-two-interfaces
    - adr-queue-as-shard-unit-and-projection-families
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - adr-concrete-fireweed-facade-and-optional-controls
    - adr-s3-turso-public-cell
    - td-postgres-native-reference-mode
  links:
    - {kind: informed_by, to: adr-hexagonal-architecture-and-two-interfaces}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
  status: proposed
---

# ADR-025: Fireweed's data-plane interface is the implementation boundary

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-11-13, revised 2026-11-15 | Proposed | Project owner | API-001, API-002, API-003, API-004, API-005, API-006, ADR-007, ADR-008, ADR-009, ADR-010, ADR-011, ADR-012, ADR-013, ADR-014, ADR-018, ADR-022, ADR-024, TD-002, TD-003, TD-006, TD-007, TD-008 | Medium |

Revision 3 (2026-11-15) widens the boundary from a priority-queue interface
to the data-plane storage interface for Snorri and Cayce. It follows owner
decisions D8 to D12, which the Cayce project relayed with the owner's words
(Owner decisions, below). They were not given in a Fireweed session, so the
owner confirms them before this ADR is accepted. Revision 3 also fixes the
round-two review defects that still apply.

Revision 4 (2026-11-15) pins the request model for cross-engine behaviour
(API-006 revision 4) and applies four recommendations from review round
three, each marked **draft pending owner**: the local engine's durability
range (§7), Postgres `Replicated` only with a synchronous standby (§7), the
cohort deadline rule (§8), and recording rejected requests (§5). Snorri's
open items O2, O14, O15 and O16 remain with the owner; text that depends on
them is marked.

Revision 5 (2026-11-15) fixes the review round four defects (API-006
revision 5) and starts the code: the crate `fireweed-interface` now defines
every type, `fireweed-interface-memory` implements the interface in memory,
and `fireweed-interface-conformance` runs the rule-pinning tests against
it. Three new owner questions are recorded (16 to 19).

Revision 6 (2026-11-15) fixes review round five findings (API-006 revision
6), adds the first PostgreSQL implementation, `fireweed-interface-postgres`,
and moves the request logic into the shared internal library
`fireweed-interface-engine`, so the in-memory and PostgreSQL
implementations run the same code. Both pass the same 96 executable
rule-pinning tests (23 skipped), after fixes for two review passes. It also records the Turso-queue session's
inputs to open questions 13, 16, 17 and 18, and adds questions 20 to 22.

## Context

### Intent

The owner's statements of intent, in order:

- "The intention was for fireweed to provide an optimized interface for
  priority queues with multiple implementations. Postgres provides a good
  reference implementation." (2026-11-13, this repository.)
- "My hope had been that fireweed provides the complete data plane storage
  layer for snorri via an api that is detailed enough to abstract the
  implementation details out — for instance using a shared database server
  for an 'oracle' while we work out the details of a local nvme db with s3
  durability." (2026-11-14, relayed.)
- "All durable workflow 'data' (high volume, high rate of change) necessary
  to operate cayce and snorri must go through fireweed. The control plane
  can be separate, but must be largely static and low volume. 'Exhaust' data
  such as logs go to niflheim." (relayed.)
- "To allow for bulk changes and analytics on the queue we need a lot of
  flexibility. Maybe it should be a DSL instead of a series of methods?"
  (relayed.)
- "We also need to address ancillary storage needs … multiple 'shapes' using
  the same storage abstraction … I don't want cayce and snorri re-inventing
  fireweed in memory or jamming huge json blobs into a record over and over
  again to work around gaps in our design." (relayed.)

### What the repository does instead

The queue semantics are written down in API-001, but no implementation can
be written against them. Five decisions put the boundary somewhere else.

1. **ADR-007 drew the hexagon around storage.** Driven adapters implement
   storage ports. The engine above them owns the claim path, idempotency,
   fencing and validation. A second implementation plugs in below the
   engine, not beside it.
2. **ADR-008 made one owner node per queue, fenced by a log epoch, product
   law.** API-001 justifies whole-group and whole-cohort atomicity by that
   owner. A database that gets the same atomicity from row locks does not
   fit the model.
3. **ADR-012 made every backend `LogStore × ProjectionStore × ControlPlane`**,
   the internal structure of one design.
4. **ADR-013 made the command log mandatory.**
5. **ADR-024 reduced the product to one cell**, S3 object log × Turso
   projection, and API-005 bound the public Rust API to it.

TD-002 drifted the same way. It put Postgres queue state in relational
tables with a `FOR UPDATE SKIP LOCKED` claim, but also required a command log
in the same transaction, `LeaseExpired` commands and epoch-fenced sweepers.
The hexagonal migration then deleted the crate.

| Observation | Evidence |
|-------------|----------|
| The trait set an implementer faces is one design's internals | `fireweed-engine` exports 53 public traits: queue ports beside `LogStore`, `ProjectionStore`, `CommitStrategy`, `UnifiedAtomicCommitter`, `SnapshotStore` and others. |
| There is no public trait to implement | The facade's backend bound, `LibBackend` (21 traits), is `pub(crate)` in `crates/fireweed/src/lib.rs`. |
| The facade owns implementation code | `crates/fireweed` depends on the memory, objectlog, postgres, projection and turso crates, and holds 7.8k lines of Turso composition in `turso_compose.rs`. |
| Postgres is forced into another implementation's shape | `fireweed-postgres` (24.5k lines) depends on `fireweed-objectlog` and `fireweed-projection`. |
| Conformance tests the log seam, not the queue | `run_conformance<B: ConformanceBackend>` requires `SnapshotStore + LogRead`. |
| RESP is bound to engine ports | `RespBackend` requires `UpsertPort`, `ReclaimDriver`, `ControlPlaneStore`, `HotProjectionQueryPort` and `ProjectionRead`. |
| Consumers work around missing shapes | Snorri keeps instance state as one JSON blob rewritten in full on every commit, input and await markers forever, and handler idempotency and seen-event sets in unbounded process memory (Snorri TD-012 rev 6 §3.7). |

### Storage spike evidence

The Cayce project's storage spike (2026-11-14) measured candidate engines.
Its report is held by that project; the figures are as it reported them.

| Observation | Consequence |
|-------------|-------------|
| With one writer per database, a claim waited 2.3 s behind a 500,000-row `UPDATE`. | Every write carries a cap (§10). |
| No asynchronous shipping design made an acknowledged write durable in S3 at return. | Durability is reported per commit as `Local` or `Replicated` (§7). |
| A Durable Object has no interactive multi-call transactions. | Every request, including a group commit, is one atomic step (§5). |
| A single-statement claim (`UPDATE … RETURNING` over a `UNION ALL` candidate set) works on SQLite, tursodb and a Durable Object. | The claim verb is one step on all three. |
| The object-log implementation reached about 100 recipients/s end to end, against Cayce's target of about 17,000/s. | Object-log is not the first implementation behind the interface (§11). |

### Consumer inventory

Snorri's TD-012 revision 6 §3.7 (branch `claude/snorri-api-encapsulation-03e4a8`
in the Snorri repository, not yet committed) lists every piece of state the
Snorri engine keeps, with size and rate. API-006 maps each item to a shape.
Scale targets: campaigns of up to 1M recipients; claim p99 gate at about
125,000 due items per shard; about 18 KiB of commit log per single-send
recipient. Cayce's inventory is drafted in API-006 from what is known and
needs Cayce's confirmation.

## Owner decisions

| # | Decision | Source | Status in revision 5 |
|---|----------|--------|----------------------|
| D1 | Time-driven transitions are in the contract; no caller tick. | Owner, 2026-11-13 | Kept (§8). |
| D2 | One time authority per storage. | Owner, 2026-11-13 | Kept. Claims take an optional eligibility time; lease time always comes from the authority (§8), pending Snorri O2. The Cayce project withdrew its "clock as an input to claim and lease" wording on 2026-11-15. |
| D3 | ADR-008 ownership becomes internal to object-log, which declares `SingleProcess`. | Owner, 2026-11-13 | Conditional on object-log's future (open question 3). |
| D4 | `Bounded` new-work visibility allowed, fails closed. | Owner, 2026-11-13 | Kept in the type. No v1 implementation declares it, and v1 conformance defines `Immediate` only (§7). |
| D5 | RESP gets a `StreamGroup` family. | Owner, 2026-11-13 | Deferred past v1 (open question 11). |
| D6 | Core leases are token-fenced, with per-item outcomes. | Owner, 2026-11-13 | Kept (§9). |
| D7 | `MutateItems` and operator repair become optional families. | Owner, 2026-11-13 | Superseded by D12: selector writes are part of the request model every implementation provides. |
| D8 | Snorri owns no tables. Its workflow state lives in Fireweed items; Fireweed provides group commit. | Owner via Cayce, 2026-11-15 | Adopted (§5). |
| D9 | The Postgres implementation is first-class, not only an oracle. A local engine (Turso MVCC on NVMe, S3 in the commit path; SQLite as comparison arm) follows and passes the same suite. Speed gates B1 to B4 apply to the local engine only. Durable Objects stay possible, not scheduled. | Owner via Cayce, 2026-11-15 | Adopted (§11). |
| D10 | Cayce reads queue data only through versioned, named, declared reads, never engine tables. | Owner via Cayce, 2026-11-15 | Adopted (§6). |
| D11 | All high-volume, fast-changing durable workflow data goes through Fireweed. The control plane is separate. Exhaust goes to Niflheim. | Owner via Cayce, 2026-11-15 | Adopted (§1). |
| D12 | Requests are typed data with a selector, an action and a limit; a group commit is a list of them. Claim, bulk enqueue and complete stay fixed verbs. No SQL. Small expression language. Every write capped. Durability reported per commit. v1 covers what the Cayce UI and the recipient-lifecycle bulk operations need. Fireweed also provides ancillary storage shapes. | Owner via Cayce, 2026-11-15 | Adopted (§3 to §6, §10). |

## Decision

### 1. Fireweed is the data plane for Snorri and Cayce

Fireweed stores all durable workflow data that is high in volume or rate of
change and is needed to operate Snorri and Cayce: work items, workflow
state, per-recipient state, counters, ledgers, operational events and large
payloads.

Outside Fireweed:

- the control plane (tenants, campaign definitions, configuration), which is
  largely static and low in volume, in its own Postgres;
- exhaust (logs, traces, full audit history, long-term analytics, joins
  across data sets), in Niflheim. Snorri's full transition history is
  exhaust.

Fireweed's analytics cover live data only: counts by state, lag, and rate
windows.

### 2. The boundary is one interface crate

API-006 defines the interface as Rust types and traits in one crate,
`fireweed-interface` (working name). It depends on `fireweed-core` id types
and general-purpose crates only. No public item exposes an implementation's
internal mechanism: command log, command position, projection, apply
frontier, ownership epoch, storage cell.

An implementation is a crate that implements the interface with whatever
internal design suits it. It depends on no other implementation crate.
Consumers (`Fireweed` facade, Snorri, Cayce, `fireweed-resp`,
`fireweed-conformance`) depend on the interface, not on implementations.
`fireweed-server` and the embedding application are composition roots.

### 3. Spaces and shapes

A **space** is one atomic and ordering domain, owned by one tenant. It holds
named objects of four shapes, declared in the space's schema:

| Shape | Holds | Covers |
|-------|-------|--------|
| Queue | Work items: priority, schedule, leases, retries, gates, groups, cohorts, recurrence, declared typed fields, a JSON body patched by path, and key retention for dedupe | Recipient and workflow work; Snorri instance heads and their await fields (D8); effect items and send intents; external events deduplicated by provider id |
| Table | Rows by typed primary key, with typed columns; atomic set, increment and conditional update; row expiry | Keyed state (per-recipient, per-run), handler idempotency records, admission chunk state, suppression flags, rollups, rate and slot counters, consumer cursors |
| Event log | Append-only entries with a log-wide sequence in commit order and a declared partition key; read by partition or from a cursor; retention by age | Delivery and engagement events, outbox feeds (including export to Niflheim), queue transition history |
| Blob | Immutable byte objects, content-addressed, referenced from other shapes | Large transition outputs, workflow definitions, rendered messages, attachments |

Counters are table columns with atomic increment, and a conditional
increment is a guarded `Mutate` (§4). Rate and slot windows are table rows
keyed by a time bucket, with expiry. A separate counter shape is open
question 5, needed only if hot-key increments prove too slow.

Across shapes in one space: a group commit is atomic; every read observes
one snapshot; `request_id` is scoped to the space; one durability level
covers the whole commit; caps apply to the whole commit. Across spaces there
is no atomicity or ordering. How spaces map to tenants, campaigns or Snorri
shards is open question 2.

### 4. Requests are typed data

Every request is a Rust value, serializable as JSON and protobuf. There is
no method per operation and no SQL or SQL subset.

- **Hot-path verbs** stay fixed and hand-tuned: `enqueue` (bulk push),
  `claim`, `claim_by_item_ids`, `renew`, `finalize` (complete, fail, retry,
  release, rearm).
- **Writes** are operations: `Enqueue`, `Finalize`, `Renew`, `Reassign`,
  `SetGates`, `SetQueueState`, `PutRows`, `Append`, and `Mutate`. A `Mutate`
  has a target object, a selection (keys, or a predicate over declared
  fields), an action (set or increment fields, patch a JSON body by path,
  reschedule, transition, change item gates, retire), a limit, and an
  optional expectation on the number of matches. An unmet expectation
  rejects the group, which gives compare-and-swap and conditional
  increment.
- **Reads** are point gets by id or key, and declared reads (§6).

The expression language has literals, declared-field and JSON-path access,
parameters, `now()`, comparison and boolean operators, `in`, arithmetic,
`coalesce`, `is_null` and time bucketing. Aggregates are count, sum, min,
max, avg and percentile, with group by. There are no joins, no user
functions and no loops. API-006 fixes null, overflow, collation and
time-bucket semantics so implementations agree.

### 5. Group commit

`commit` applies a list of write operations in one space, in list order, at
one linearization point, all or nothing. Later operations see earlier
operations' effects. A commit runs in two phases. Validation (types, schema
references, structural caps) fails with an error and records nothing.
Execution stops at the first rejecting condition in API-006's decision
table (stale lease, version mismatch, key conflict, unmet match
expectation, scan or match limit, cohort split, a fault in a written
value), applies nothing, and reports that operation and entry.

**[Draft pending owner]** A rejected group leaves a request record, so its
replay returns the same rejection and no stalled earlier attempt can commit
afterwards. Revision 3 recorded nothing; review round three showed that a
replay could then be rejected while the original still committed.

A Snorri transition is one group: finalize the instance-head item with a
patch to its body and await fields (token and `expected_item_version`
guarded), finalize the consumed event item, enqueue effect items and
follow-on work, and update table rows such as counters.

The standalone verbs keep API-001's per-item outcomes: a stale lease in a
`finalize` call affects that entry only. Inside a group the same staleness
rejects the group.

### 6. Declared schema and declared reads

Each space has a versioned schema: its objects, their typed fields, index
hints, retention, and its declared reads. Only declared fields can appear in
selectors, filters and reads. A field may be multi-valued (a set of text
values, such as an instance's await keys) and indexed per value. The
implementation turns declarations into indexes. A field becomes usable once
its index is ready, and `get_schema` reports index state. v1 schema changes
are additive; other changes are open question 9.

A declared read has a name, a version, a source object, parameters, a
filter, a projection or aggregate, an order and a row cap. Consumers invoke
reads by name and version. A version never changes once declared. Cayce
reads only through declared reads (D10). Built-in reads cover queue items,
gates and queue metrics.

Every `Where` selector, claim filter and declared read must constrain at
least one indexed field by equality, range, `in` or set membership: its
access term. A request that does not fails. A `Keys` selection is bounded by
its keys and needs no access term. The access term alone does not bound
work; the scan budget in §10 does.

### 7. Guarantees and durability

Each implementation declares:

| Guarantee | Values | Meaning |
|-----------|--------|---------|
| Durability | `Process`, `Local`, `Replicated` | The minimum level of every acknowledged commit. `Local`: survives a process crash and reopen of the implementation's own storage. `Replicated`: also survives loss of that storage, because a copy outside it was durable before the acknowledgement. |
| New-work visibility | `Immediate`, `Bounded { max_ms }` | v1 implementations are `Immediate`, and v1 conformance defines only `Immediate`. |
| Sharing | `SingleProcess`, `MultiProcess` | Whether several instances may serve one space at once. |

Every write response reports the level that commit reached. A request may
require `Replicated`; the commit is then acknowledged only once it reaches
that level. If it committed but has not reached the level in time, the
request fails with `DurabilityPending { reached }`, which tells the caller
the commit exists. `CommitTimeout` is kept for "outcome unknown". Replaying
the `request_id` waits again, so a caller can wait for `Replicated` after a
`Local` acknowledgement.

| Implementation | Minimum | Maximum |
|----------------|---------|---------|
| Postgres | `Local`: flushed to the primary's WAL | `Replicated` only when a synchronous standby is configured (a standby confirmed the flush); otherwise `Local`, and a request requiring `Replicated` fails. **[Draft pending owner; Snorri O15]** |
| Local engine | `Local`: fsynced locally | `Replicated`: the commit is in the S3 log. Commits ship in batches; a request requiring `Replicated` waits for its batch. **[Draft pending owner]** |

Revision 3 made every local-engine commit `Replicated`, which puts an S3
round trip on every claim and conflicts with speed gate B2. Without a
synchronous standby, Snorri's zero-loss send path cannot run on Postgres;
the owner either requires a standby in production or accepts `Local` send
intents and possible re-sends after losing the primary.

Operations on one space are linearizable. Every read observes every effect
committed before its linearization point, whatever its durability level.

### 8. Time

**Observable state is a function of committed operations and time.** Lease
expiry, `not_before` and backoff, cohort deadlines and retention take effect
at their due instant. An implementation may sweep, evaluate lazily, or both.
No caller drives a tick. API-006 fixes the processing order for transitions
due at the same instant.

**One time authority per space.** Each request uses one request time for
every decision. Request time is monotonic along conflicts: a request that
follows a conflicting one in linearization order never has an earlier
time. An implementation may sample a provisional time to choose
candidates, then fix the request time after locking them. Under
conformance the authority is the injected `Clock`. The target Postgres
protocol samples `clock_timestamp()` at the start of a `SERIALIZABLE`
group, and after taking its locks in a `READ COMMITTED` verb (API-006
§PostgreSQL rule 3). The first increment keeps request time on the space's
row, which every write locks.

`claim` and `claim_by_item_ids` take an optional `eligibility_time`. It
replaces the authority only when deciding whether `not_before` and backoff
have passed for that claim. Lease expiry, attempt timestamps, cohort
deadlines, retention and `now()` in expressions always use the authority.
This carries Snorri CONTRACT-005's separate execution and lease times. No
other request overrides time. A selector that needs a fixed instant uses a
parameter.

**[Draft pending owner]** Cohort deadlines follow API-001, evaluated at
the deadline instant: unless the cohort has been claimed, it fails with
`cohort-incomplete` if fewer than `cohort_size` members are non-terminal,
or if a non-terminal member is not eligible. Revision 3 counted committed
members only; review showed that let a cohort with a retired member never
expire, and let one gated member withhold its siblings indefinitely.

### 9. Leases are fenced by token

Renew, reassign and finalize address a lease by item id and token (or
cohort id and cohort token). Finalize and `Mutate` may also carry
`expected_item_version`. Reads never return a lease token. The id-only,
all-or-nothing facade helpers are removed.

A `Mutate` that selects a leased item applies its lease policy: skip it
(default), reject the group, preserve the lease (only for reschedule,
field, body and gate changes), require a matching token, or break the lease
(operator force actions, API-002).

### 10. Every write is capped

`ImplementationLimits` declares the caps: operations, entries and bytes per
request; the resulting size of every value; records a selector may match or
affect; cohort size; and two scan budgets on records examined, one for the
writer (claims, `Mutate`, chunk steps) and one for snapshot reads.
Structural caps fail validation. A scan or match over budget rejects an
unchunked write, ends a chunk step or page with a continuation, or returns
a partial claim that is a prefix of claim order. Records are examined in
result order, which composite indexes make possible.

`dry_run` evaluates a commit's guards and selections, returns the counts it
would affect, and records nothing.

A `Mutate` whose selection exceeds the cap may instead run as a sequence of
bounded steps: the response returns a continuation, and the caller submits
the next step. Each step is its own linearization point and its own commit,
and steps advance in access-term order (the access field's value, then
key). A chunked `Mutate` must be the only operation in its commit. A step
that reaches a cohort it cannot take whole skips that cohort and counts it,
so every step makes progress.

### 11. Implementations

| Implementation | Status | Durability | Sharing |
|----------------|--------|------------|---------|
| PostgreSQL (shared server; new crate from TD-002) | First-class and production-supported. Defines correct semantics for the differential test. Snorri and Cayce build against it now. | `Local`; `Replicated` only with a synchronous standby | `MultiProcess` |
| Local engine: Turso MVCC on NVMe, S3 log; SQLite as comparison arm | Later, in the Turso-queue session's phases L0 to L5 (ADR and spike; the engine with a SQLite arm; gates; the S3 log; event logs, blobs and cohorts; moving spaces off Postgres). Passes the same suite and the differential test. Speed gates B1 to B4 (Turso queue plan §3.8) apply here only. | `Local`; `Replicated` on request [draft pending owner] | `SingleProcess` per space |
| In-memory (`fireweed-interface-memory`) | For consumers' tests, so that nobody re-implements Fireweed in memory. Queues and tables today. | `Process` | `SingleProcess` |
| Object log × Turso (current runtime) | Not wrapped. Its future is open question 3. | — | — |
| Durable Objects | Possible: every request is one atomic step. Not scheduled. | — | — |

The Postgres implementation keeps TD-002's queue tables, indexes, `SKIP
LOCKED` claim and group and cohort locking, and drops the command log,
`LeaseExpired` commands, `assignment_epoch` fencing and epoch-fenced
sweepers. Group commits and chunk steps run at `SERIALIZABLE`, so absence
guards and range expectations cannot be defeated by write skew; verbs run
at `READ COMMITTED` with an explicit lock protocol (API-006).

### 12. Conformance

The conformance suite tests through the interface only. It reads
`Guarantees` and `Capabilities`, and covers every shape, group commit
atomicity, caps and chunking, declared reads, the expression semantics, the
time rules under a manual clock, concurrent claims, and crash and reopen in
a killed child process. Each rule is pinned by a named test (API-006
§Rule-pinning tests) that every implementation runs. A test-only crate runs
a differential test against the Postgres implementation, including
generated expressions and aggregates. The suite starts as
`fireweed-interface-conformance` beside the existing `fireweed-conformance`,
which keeps testing the current runtime until migration step 6.

### 13. The facade and RESP

`Fireweed` (API-005) becomes a concrete wrapper over `Arc<dyn DataPlane>`,
with convenience helpers over the verbs and request types. It names no
implementation. `fireweed-resp` stays on the current runtime until RESP's
place is decided (open question 11).

ADR-007's two driving interfaces and ADR-022's single concrete `Fireweed`
stand. What sits under them changes.

### 14. Maintenance belongs to the implementation

Projection control, snapshots, compaction, ownership, S3 shipping, index
builds and storage placement are methods on the implementation's own type,
not on the interface or on `Fireweed`.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep ADR-024: one cell, storage ports as the seam | No rework | Contradicts the intent; no second implementation | Rejected |
| A queue-only interface; Snorri keeps its own tables (Turso queue plan Part 2) | Small interface | Snorri and Cayce each build a storage layer; contradicts D8 and D11 | Rejected (D8) |
| One method per operation (revision 2) | Simple to type-check | Every bulk change and analytic needs a new method; the owner asked for flexibility | Rejected (D12) |
| Expose SQL or a SQL subset | Maximum flexibility | Ties every implementation to SQL semantics; uncapped work on a single writer | Rejected (D12) |
| Postgres as test oracle only | Less to support | Snorri and Cayce cannot build until the local engine exists | Rejected (D9) |
| A separate store per shape | Each store tuned to its shape | No atomic commit across shapes; consumers coordinate stores themselves | Rejected |
| Caller-supplied lease time, or a two-reading `Clock` (Turso queue plan §1.4 options A and C) | Matches CONTRACT-005 directly | Two time sources for lease fencing | Rejected for lease time; eligibility time adopted |
| **One data-plane interface: spaces, four shapes, typed requests, group commit, declared reads, Postgres first** | Matches the intent; one abstraction for all workflow data | Large interface to specify and test | **Proposed** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Snorri and Cayce get one storage abstraction for all fast-changing workflow data, with atomic commits across shapes. |
| Positive | Snorri and Cayce can build now on Postgres. The local engine slots in behind the same interface and suite. |
| Positive | Bulk changes and live analytics need no new methods, only declarations and requests. |
| Positive | Body patches shrink requests, and blob references and table rows take large or independently changing data out of the instance body. Engines still rewrite a stored JSON value on update. |
| Positive | Expression, aggregate, group-commit and time rules are pinned by named conformance tests that both engines run. |
| Positive | Correctness is defined by observable behaviour and checked against Postgres. |
| Negative | The interface is much larger than a queue API. The expression language must behave identically on every implementation. |
| Negative | Snorri migrates its side records, fences and in-memory idempotency to Fireweed shapes, and to token-addressed finalize. |
| Negative | Every selector and read needs an index-backed conjunct, which constrains ad-hoc queries. |
| Negative | The current object-log runtime stays behind the old facade until the new facade lands, so two boundaries coexist for a while. |
| Neutral | Joins and full history stay in Niflheim. |

### Documents this ADR changes on acceptance

| Document | Change |
|----------|--------|
| ADR-024 | Superseded. |
| ADR-007 | Amended: the driven side is the data-plane interface. |
| ADR-008, ADR-012, ADR-013, ADR-016, ADR-017 | Rescoped to the object-log implementation, pending open question 3. ADR-008's atomicity outcomes move into API-001 without an owner. |
| ADR-009 | Amended: the facade holds a `dyn DataPlane`; its "no dyn backend" and crate-private-constructor rules and the publish topology change accordingly. |
| ADR-010, ADR-011 | Restated as schema declarations: declared fields, index hints, typed payload schemas. |
| ADR-014 | The Kafka consumer surface reads event logs and queue transition history. |
| ADR-018 | Exact ensure becomes `apply_schema` with an expected version. |
| ADR-022 | Stands for the single concrete `Fireweed`, except `projection_control()`, `StorageConfig` and maintenance leave it. |
| API-001 | Major version. Remove storage language and `SetGates.applied_command_position`. Replace `MutateItems` with `Mutate` requests, drop `evaluated_at` and ordered-selector first-match ownership. Add `eligibility_time` to claims, declared-field filters, `ReassignLeases`, lease policies including lease break, the cohort deadline rule evaluated at the deadline instant (§8), the time rule, request records for rejected requests, and lease expiry not changing `item_version`. Define progress metrics by base eligibility. Restate atomicity without an owner. Move discovery authorization to the adapter. |
| API-002 | Operator actions become requests: pause and resume are `SetQueueState`; redrive and repair are `Mutate` transitions with lease break where forced; purge is `Mutate` retire; archive sets item metadata and deletes nothing; audit is an event log; operation lookup is `request_id` replay. `max_affected` becomes the per-request limit. |
| API-003 | Profiles are restated in API-006 terms. |
| API-004 | Replaced by declared reads. |
| API-005 | Rewritten as the binding of API-006 to `Fireweed`. |
| TD-002 | Revised per §11, extended to tables, event logs, blobs and declared reads. |
| TD-003 | Ownership and fencing become object-log internals. |
| TD-006 | Bound to RESP's decision (open question 11). Its required `ReclaimDriver` is replaced by the time rule. |
| TD-007 | Durability classes become §7's levels; `ReclaimDriver` is replaced by the time rule; `UpsertPort` becomes `Enqueue` with a key-conflict policy. |
| TD-008 | Queue change records become queue transition history in an event log. |
| Turso queue implementation plan | Part 2 (Snorri tables) and §3.4a (SQL read views) give way to Fireweed shapes and declared reads. |
| AGENTS.md | The projection-lifecycle test rule names the object-log implementation's own projection control. |

The contract changes (API-001 to API-005, TD-002, TD-007) land with
acceptance, before consumers adopt the interface in migration step 4. The
object-log, RESP and TD-003 changes land at step 6.

### Migration order

1. Create `fireweed-interface` with API-006's v1 types and traits. Add the
   dependency test. (Started in revision 5.)
2. Write `fireweed-interface-conformance` against it, and the in-memory
   implementation `fireweed-interface-memory` alongside, so the suite has a
   first target. (Started in revision 5.)
3. Build the Postgres implementation: queues and verbs, then group commit,
   tables, declared reads, event logs, then blobs. Add the differential test;
   Postgres becomes the reference once it passes the suite. (Started in
   revision 6: queues, tables, group commit, request records, claims,
   selector writes and declared reads, with a per-space lock and whole-object
   loads; indexed loading and the concurrent protocol come next.)
4. Cut `Fireweed` over to the interface. Snorri and Cayce adopt it on
   Postgres. The old facade and runtime stay frozen until this step lands.
5. Build the local engine and run it through the suite, the differential
   test and gates B1 to B4.
6. Decide object-log and RESP (open questions 3 and 11). Retire
   `StorageConfig`, `RETIRED_STORAGE_CELL` and whatever the decisions remove.
   Apply the document changes above.

## Open questions for the owner

1. **Names.** `fireweed-interface`, `DataPlane`, "space", and the shape names
   are working names.
2. **Space granularity.** Per tenant, per campaign, or per Snorri shard?
   Group commit is atomic only within a space, and a single-writer engine's
   throughput is per space. A suppression check inside an effect's group
   (API-006 §Suppression) needs the suppression table in the same space as
   the effect's queue; per-account or per-tenant spaces allow that,
   per-shard spaces do not without copying suppression into each shard.
3. **Object-log's future.** The owner said it is "likely going to
   deprecate". Not decided.
4. **v1 scope of blobs, event logs and queue transition history.** All are
   specified. Snorri's inventory needs blobs (large outputs, definitions) but
   sends its full history to Niflheim. Does v1 need event logs?
5. **Counters.** Are table increments enough, or is a dedicated counter shape
   needed for hot keys? Moving Cayce's rate-limit counters out of Dragonfly
   is Snorri open item O16.
6. **Admission chunk state.** Snorri proposes a per-run, per-shard table row
   instead of a Postgres control-plane compare-and-swap (Snorri O16).
7. **Cohort deadline rule** (§8). Draft: API-001's rule, evaluated at the
   deadline instant, with claimed cohorts exempt.
8. **Rejected requests recorded** (§5). Draft: yes.
9. **Schema evolution** beyond additive changes.
10. **Percentile** exact (v1 draft) or approximate.
11. **RESP and `StreamGroup`.** When, and over which implementation?
12. **Protobuf schema.** Who owns it, and in which release?
13. **Local engine durability** (§7). Draft: `Local` minimum, `Replicated`
    on request. The Turso-queue session agrees: `Local` by default,
    `Replicated` on request through a shipped watermark, send intents
    require `Replicated`. Shipping every commit to S3 costs 17 to 64% on
    gate B2 and is the upper bound.
14. **Postgres `Replicated`** (§7; Snorri O15). Draft: only with a
    synchronous standby. Does production Postgres require one, or does the
    owner accept `Local` send intents?
15. **Snorri's open items.** O2 (claim time), O14, O15 (durability) and O16
    (counters and admission state in Fireweed) are with the owner. ADR-025
    and API-006 text that depends on them is marked.
16. **Local engine stale-writer fencing** (for the Turso queue session). How
    does the local engine fence a stale writer on another host before it
    acknowledges a `Local` commit? Shipping to S3 after a `Local`
    acknowledgement cannot do it. Options from the Turso-queue session, for
    the owner: (a) as the rule, no takeover while the old process may be
    alive (kill it or fence its host); (b) as defence in depth, a
    time-bounded writer lease in S3; (c) always write sequence-numbered log
    keys with S3 conditional puts (`If-None-Match: *`, `If-Match`), which
    fences `Replicated` commits exactly (a takeover record at the next
    sequence) but cannot fence `Local` ones; (d) require `Replicated` for
    loss-intolerant requests such as send intents. Turso has no lease or
    epoch, but its `on_log_write_complete` hook can fail a commit. S3
    conditional writes are documented by AWS but untested in the spike.
17. **Renew and `item_version`.** API-001 makes renew and reassign change
    `item_version`, so a heartbeat makes a worker's version-guarded commit
    fail. Keep that, or exclude lease-only changes (an API-001 amendment)?
    Purely semantic: the local engine is indifferent.
18. **Event log sequence on Postgres.** One head-row lock per log (appends to
    one log serialize, which matters at Cayce's event volumes and for queues
    with history), or a sequence plus a visibility watermark? Either meets
    the reader rule (a reader never later sees an entry with a lower
    sequence than one already returned); the local engine assigns gapless
    numbers in commit order inside its single writer.
19. **Postgres per-request durability.** `Replicated` per request
    (per-transaction `synchronous_commit`, with the commit LSN recorded for
    replay waits), or cluster-wide (every commit waits for the standby,
    claims included)?
20. **Bounding redelivery.** Under API-001, lease expiry returns an item to
    pending and charges nothing; `max_attempts` is checked only on a retry
    finalize. An item whose worker crashes before finalizing is redelivered
    without limit unless the worker checks `attempt_count`. A server-side
    bound would be an API-001 amendment for every implementation. The
    reference and both implementations follow API-001. The Turso-queue plan
    earlier marked such items failed; it now agrees with API-001 and leaves
    the bound to this question.
21. **Choices made without a ruling.** (a) A chunk step that reaches a
    cohort it cannot take whole skips and counts it, rather than rejecting
    the step. (b) Postgres group commits run at `SERIALIZABLE` (target
    protocol), rather than with explicit predicate locks. (c) An expired
    lease returns the item to pending regardless of attempts (question 20).
22. **Moving spaces from Postgres to the local engine.** No data-migration
    design exists yet (Turso-queue open item 15); it is needed before
    Snorri and Cayce spaces move off Postgres.
23. **Declared merge policies on fields.** Cayce's suppression store needs
    reasons that can only be added, never cleared except by an explicit
    lift. The request language expresses the add (`SetAdd` in one group)
    but cannot forbid a clear. Should a schema declare per-field merge
    policies (add-only sets, increment-only counters, max-only timestamps),
    rejecting writes that break them unless the request is a declared lift?
    This would also settle question 5 without a separate counter shape.
24. **A guard-only operation.** A check that a record does or does not
    match, inside a group, is expressed today as a `Mutate` with a no-op
    `Set` and `expect`. Should there be a `WriteOp::Assert` (selection plus
    expectation, no action)?

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Expression and aggregate results differ between Postgres and the local engine (collation, nulls, overflow, float rounding) | H | H | API-006 fixes evaluation order, faults, casts and aggregate precision; rule-pinning tests and the differential test check them. |
| Postgres group commits at `SERIALIZABLE` retry under contention | M | M | Bounded retries, then `Contention`; hot paths use the verbs at `READ COMMITTED` with explicit locks. The first increment serializes writes per space instead. |
| The interface grows into a general database | M | H | No joins, capped requests, index-backed selectors, live data only; history to Niflheim. |
| A hot table row (counter, rollup) serializes commits on Postgres | M | M | Open question 5; rate windows keyed by bucket spread the load. |
| A single-writer engine stalls claims behind bulk work | H | M | Caps, chunked `Mutate`, and B4. |
| Waiting for `Replicated` on every send-intent commit limits throughput | M | M | Only commits that require it wait; the local engine reaches `Replicated` by batching shipments to S3. |
| Snorri and Cayce adopt Postgres and never move to the local engine | M | M | Same interface and suite; the facade names no implementation. |
| Two boundaries coexist while the old runtime is frozen | M | M | No new features on the old facade; step 4 cuts over. |
| Relayed decisions are misread | L | H | The owner confirms D8 to D12 before acceptance. |

## Validation

| Success metric | Review trigger |
|----------------|----------------|
| `fireweed-interface` depends on no `fireweed-*` crate except `fireweed-core` | A storage or engine crate enters its tree. |
| No consumer crate depends on an implementation outside `[dev-dependencies]` | A consumer needs an implementation type. |
| Every implementation passes the suite for what it declares; each passes the differential test against Postgres | An implementation released without both. |
| Snorri runs with no Snorri-owned tables and no unbounded in-memory state (TD-012 §3.7 items 1 to 10 mapped) | Snorri adds a table or cache to work around a gap. |
| Cayce's UI uses declared reads only | Cayce reads an engine table. |
| The local engine meets B1 to B4 | A gate misses its floor. |

## Concern Impact

- `resilience`: durability is declared per implementation and reported per
  commit.
- `technology-radar`: storage engines are chosen per implementation.

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/contracts/API-006-queue-implementation-interface.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-008-queue-as-shard-unit-and-projection-families.md`
- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
- `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md`
- `docs/helix/02-design/technical-designs/TD-006-resp-wire-adapter.md`
- `docs/helix/04-build/turso-queue-implementation-plan.md` (branch
  `claude/fireweed-sqlite-s3-logs-37f26c`, commit `1b943868`)
- Snorri `CONTRACT-005-workflow-state-store-port.md`; Snorri TD-012 rev 6 §3.7
  (branch `claude/snorri-api-encapsulation-03e4a8`, uncommitted)
