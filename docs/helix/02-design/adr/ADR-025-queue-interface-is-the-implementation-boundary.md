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
| 2026-11-13, revised 2026-11-16 | Proposed | Project owner | API-001, API-002, API-003, API-004, API-005, API-006, ADR-007, ADR-008, ADR-009, ADR-010, ADR-011, ADR-012, ADR-013, ADR-014, ADR-018, ADR-022, ADR-024, TD-002, TD-003, TD-006, TD-007, TD-008 | Medium |

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

Revision 7 (2026-11-15) applies the owner's rulings on the open questions,
given in this repository's interface session, and the rulings the owner
gave the Turso-queue session (decisions D13 to D28). A space is one
workflow or one campaign. Durability uses Niflheim's write modes, chosen per
request, and PostgreSQL is assumed fully replicated. The control plane
fences writers. Blobs and event logs leave the interface: a space holds
queues and tables, and history and event feeds are sequenced tables. Lease
expiry fails an item that has used up `max_attempts`. Tables gain field
merge rules, and group commits gain a guard-only `Assert`. RESP is
deprecated in favour of the request DSL; object-log is deprecated once the
local engine ships its S3 log. Eighteen questions are resolved; questions 5
and 15 wait on background for the owner, and questions 25 to 29 are new.

A 2026-11-16 amendment to revision 7 records two more owner rulings on
Snorri's items. Re-entry across runs is checked against a keyed record in
a family space (D29), and a queue may opt out of the store's redelivery
bound when its embedder parks crash-looping items itself (D30). Snorri's
TD-012 revision 7 text answers question 15, which moves to the resolved
table. Definitions larger than one value were question 30, settled the
same day by capping their size (D31). The owner also accepted Snorri's
space granularity, one space per (run, shard) (D32), which closes
question 29.

Revision 8 (2026-11-16, draft for review) recasts Fireweed as a compiler
and an engine, in the pattern of weft and truss (D33, D34).
`fireweed-compiler` checks a typed request IR against a space schema
written in UMF and lowers it to SQL for one backend. `fireweed-engine` owns
storage layout and runs the compiled SQL under Fireweed's rules for time,
leases, durability and request records, on Turso, PostgreSQL, SQLite or in
memory. Claims, bulk changes and queries become statements of one IR
instead of fixed verbs beside a request model. Sections 2, 4, 6, 8, 11 and
12 change, as do the alternatives, consequences and migration order; the
rest of revision 7 stands. Where weft already has the machinery, Fireweed
shares it, and weft changes to make that possible (D35). Questions 31 to 36
are new.

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
| No asynchronous shipping design made an acknowledged write durable in S3 at return. | Durability is reported per commit, and chosen per request (§7). |
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

| # | Decision | Source | Status in revision 7 |
|---|----------|--------|----------------------|
| D1 | Time-driven transitions are in the contract; no caller tick. | Owner, 2026-11-13 | Kept (§8). |
| D2 | One time authority per storage. | Owner, 2026-11-13 | Kept. Claims take an optional eligibility time; lease time always comes from the authority (§8). Snorri accepts this as its O2 (TD-012 revision 7). The Cayce project withdrew its "clock as an input to claim and lease" wording on 2026-11-15. |
| D3 | ADR-008 ownership becomes internal to object-log, which declares `SingleProcess`. | Owner, 2026-11-13 | Moot once object-log is deprecated (D24). |
| D4 | `Bounded` new-work visibility allowed, fails closed. | Owner, 2026-11-13 | Kept in the type. No v1 implementation declares it, and v1 conformance defines `Immediate` only (§7). |
| D5 | RESP gets a `StreamGroup` family. | Owner, 2026-11-13 | Superseded by D25: RESP is deprecated. |
| D6 | Core leases are token-fenced, with per-item outcomes. | Owner, 2026-11-13 | Kept (§9). |
| D7 | `MutateItems` and operator repair become optional families. | Owner, 2026-11-13 | Superseded by D12: selector writes are part of the request model every implementation provides. |
| D8 | Snorri owns no tables. Its workflow state lives in Fireweed items; Fireweed provides group commit. | Owner via Cayce, 2026-11-15 | Adopted (§5). |
| D9 | The Postgres implementation is first-class, not only an oracle. A local engine (Turso MVCC on NVMe, S3 in the commit path; SQLite as comparison arm) follows and passes the same suite. Speed gates B1 to B4 apply to the local engine only. Durable Objects stay possible, not scheduled. | Owner via Cayce, 2026-11-15 | Adopted (§11). |
| D10 | Cayce reads queue data only through versioned, named, declared reads, never engine tables. | Owner via Cayce, 2026-11-15 | Adopted (§6). |
| D11 | All high-volume, fast-changing durable workflow data goes through Fireweed. The control plane is separate. Exhaust goes to Niflheim. | Owner via Cayce, 2026-11-15 | Adopted (§1). |
| D12 | Requests are typed data with a selector, an action and a limit; a group commit is a list of them. Claim, bulk enqueue and complete stay fixed verbs. No SQL. Small expression language. Every write capped. Durability reported per commit. v1 covers what the Cayce UI and the recipient-lifecycle bulk operations need. Fireweed also provides ancillary storage shapes. | Owner via Cayce, 2026-11-15 | Adopted (§3 to §6, §10); the fixed verbs are amended by D33. |
| D13 | "atomic writes should be per workflow or per campaign." | Owner, 2026-11-15 | A space never spans workflows or campaigns (§3); D32 splits Snorri's runs by shard. Account-scoped records are open question 25. |
| D14 | Durability is chosen per request with Niflheim's write modes: "we need to be able to control the level of durability required and wait accordingly. niflheim has a spec for this, we should try to use the same if possible - durable, tracked, fire and forget". For PostgreSQL: "Just assume postgres is fully replicated and design the protocol to ensure transactions are used to achieve that assuming perfect server durability." | Owner via the Turso-queue session, and owner, 2026-11-15 | Adopted (§7). Resolves questions 13, 14 and 19. |
| D15 | "fencing should happen by the control plane. we'll have to have some sort of recheck interval to force readers to switch to the new node." Via the Turso-queue session: "the same applies to stopping old writers. the lease can be managed by the control plane via the embedder." | Owner, 2026-11-15 | Adopted (§11). Resolves question 16. |
| D16 | "this system isn't really meant to store blobs. even history will need to be supported for timeline and event triggering … fireweed is exposing a generic interface similar to a sql database but with more constraints. define another table, be done." | Owner, 2026-11-15 | Shapes are queues and tables; history and event feeds are sequenced tables (§3). Resolves questions 4 and 18. |
| D17 | "yes, add max_attempts": lease expiry counts toward `max_attempts`. | Owner, 2026-11-15 | Adopted (§9). Resolves question 20. The Turso-queue session relayed "crashes can be handled by the control plane via the embedder"; both hold: the store bounds redelivery, and the embedder may park an item sooner. |
| D18 | "yes, bump item_version" on renew and reassign. | Owner, 2026-11-15 | API-001's rule stands (§9). Resolves question 17. |
| D19 | Cohort deadline draft: "yes". Rejected requests recorded: "yes." Choices made without a ruling: "yes, as needed. serializable transactions seems pretty aggressive but if it's necessary..." | Owner, 2026-11-15 | Drafts adopted (§5, §8, §10, §11). Resolves questions 7, 8 and 21. |
| D20 | Field merge rules: "yes add merge rules". A guard-only operation: "agree". | Owner, 2026-11-15 | Adopted (§4, §6). Resolves questions 23 and 24. |
| D21 | "all high-volume ops should be in the data plane, not control plane." | Owner, 2026-11-15 | Admission chunk state is a table row (§1). Resolves question 6. |
| D22 | "no need for complex schema changes. these spaces will not have infinite lifetime" | Owner, 2026-11-15 | Schema changes stay additive (§6). Resolves question 9. |
| D23 | Exact percentile: "agree." Names: "these are ok". | Owner, 2026-11-15 | Resolves questions 1 and 10. |
| D24 | "object-log can be deprecated assuming we can implement the logs-to-s3 without it." | Owner, 2026-11-15 | Deprecated once the local engine's S3 log passes (§11). Resolves question 3. |
| D25 | "deprecate RESP for now in favor of the DSL"; "drop protubuf unless required for the DSL". | Owner, 2026-11-15 | §4, §13. Resolves questions 11 and 12. |
| D26 | "no need to migrate spaces"; via the Turso-queue session: "we don't need a migration path". | Owner, 2026-11-15 | A space stays on the implementation it starts on (§11). Resolves question 22. |
| D27 | Via the Turso-queue session: "we do need to ensure that the dsl can be fully implemented on this model". | Owner, 2026-11-15 | The Turso-queue session mapped every Snorri DSL construct onto revision 6 (its plan, Part 2). The one gap, a guard-only operation, is D20; three smaller additions are questions 26 to 28. |
| D28 | Crashes are the embedder's: "crashes can be handled by the control plane via the embedder (snorri/cayce)". | Owner via the Turso-queue session, 2026-11-15 | The control plane grants ownership; the store still bounds redelivery (D17). |
| D29 | "use the family space record" for re-entry across runs. | Owner, 2026-11-16 | Adopted (§3). |
| D30 | "accept Snorri's max_attempts opt-out". | Owner, 2026-11-16 | Adopted (§9). Amends D17: the store's bound is the default, and a queue may opt out. |
| D31 | "Yes, cap at 256 KiB": Snorri caps workflow definitions at 256 KiB, one table value, and keeps transition output at 1 MiB inside the 2 MiB body. | Owner, 2026-11-16 | Adopted. Resolves question 30. No interface change. Cayce measured its shipped templates at 939 to 2,428 bytes in Snorri form, but enforces no size limit; a chain of about 245 send steps crosses 256 KiB. A cap rejects, it does not chunk: Cayce or Snorri's compiler rejects an oversized definition with a typed error when it is created, and Fireweed's `ValueTooLarge` is the backstop. |
| D32 | "Yes, per run-shard": Snorri's spaces are one per (run, shard). | Owner, 2026-11-16 | Adopted (§3). Narrows D13: a space never spans workflows or campaigns, and a campaign may be split over several spaces. Resolves question 29. Events correlated to instances in several runs are written once per run space, deduplicated per space; cancelling a run is one commit per shard; on the local engine each space and each family space needs its own writer grant from the control plane, which may cover several of one holder's spaces. |
| D33 | "We are trying to define a DSL that allows for the claim, bulk change, and query flexibility we need without the limitations of a fixed function calling api. It may not need to be sql, and it might be typed, but it's going to need to compile down to sql. Fireweed *is* effectively a compiler from 'some typed ir' to 'some database backend (turso, sqlite, postgres)'." The IR: "Own IR, weft style". The shape: "Fireweed is effectively becomming a similar system to truss + weft: fireweed-compiler + fireweed-engine[turso,postgres,sqlite,memory]". | Owner, 2026-11-16 | Draft in revision 8 (§2, §4, §11, §12). Amends D12: the fixed verbs become statements; consumers still write no SQL. |
| D34 | "We should leverage umf for the typesystem, underlying schemas, ddl, etc" | Owner, 2026-11-16 | Draft in revision 8 (§6). |
| D35 | "If there is a machinery we can share with weft, we shuold do so. I own weft and can make channges to it." On types: "the missing types are obvious additions to weft". | Owner, 2026-11-16 | Draft in revision 8 (§2). Fireweed's IR stays its own (D33); the machinery under it is shared with weft where weft has it. |

## Decision

### 1. Fireweed is the data plane for Snorri and Cayce

Fireweed stores all durable workflow data that is high in volume or rate of
change and is needed to operate Snorri and Cayce: work items, workflow
state, per-recipient state, counters, ledgers, admission chunk state (D21),
operational events and timelines. Large payloads are not stored (D16); a
field holds a reference to wherever the consumer keeps them.

Outside Fireweed:

- the control plane (tenants, campaign definitions, configuration), which is
  largely static and low in volume, in its own Postgres;
- exhaust (logs, traces, full audit history, long-term analytics, joins
  across data sets), in Niflheim. Snorri's full transition history is
  exhaust.

Fireweed's analytics cover live data only: counts by state, lag, and rate
windows.

### 2. A compiler and an engine

Fireweed has the shape of weft and truss (D33). Weft compiles a query over
UMF models into SQL for a registered backend and never runs it; truss owns
a storage layout on PostgreSQL. Fireweed has three parts:

| Crate | Role |
|-------|------|
| `fireweed-interface` | The IR types, the space schema types, the result and error types, and the `DataPlane` trait. Depends on `fireweed-core` id types, `umf-core` and general-purpose crates only. |
| `fireweed-compiler` | Resolves and type-checks a program against the space schema, builds a logical plan, and lowers it to one backend's SQL with bound parameters and result decoders, or refuses. Pure: no I/O, no clock, no storage crate. |
| `fireweed-engine` | Owns each backend's storage layout and DDL, and runs compiled plans: transactions, request time, leases, durability, request records, replay, fencing and sweeps. Backends: Turso, PostgreSQL, SQLite and memory. |

Fireweed shares weft's machinery wherever weft has it (D35), moved into
crates both projects depend on, with weft changed as needed. Candidates,
from weft's `weft-core` and `weft-postgresql`:

- UMF model loading, name resolution to UMF identities, and typing by UMF
  type families and facets;
- exact literals, exact arithmetic and exact instants;
- the typed expression and predicate core (field references, literals,
  comparisons, boolean operators, arithmetic) and weft's selection format
  for bulk updates (weft CONTRACT-009);
- the backend boundary: capability manifests, binding validation, assess,
  lower and emit, and refusal diagnostics (weft CONTRACT-002);
- PostgreSQL emission: quoting, parameter slots, typed casts, collation and
  result decoders;
- canonical JSON encoding, golden plan fixtures and the conformance runner.

Types weft lacks today, such as nullable fields, timestamps and floats, are
added to weft (D35). Fireweed adds what weft has no reason to hold: queue
statements and their
semantics, leases, time, durability, request records, and execution.
SQLite and Turso emission are new for both projects. Question 36 sets the
order of extraction.

The memory backend interprets the logical plan directly, without SQL. It is
the reference interpreter: consumers test against it, and the differential
test compares every SQL backend with it. Today's
`fireweed-interface-engine`, the request logic that the in-memory and
PostgreSQL implementations share, becomes that interpreter.

No public item exposes a backend's internal mechanism: log, log position,
ownership epoch or physical table name. Consumers (`Fireweed` facade,
Snorri, Cayce, `fireweed-conformance`) depend on `fireweed-interface`.
`fireweed-server` and the embedding application are composition roots that
choose a backend.

### 3. Spaces and shapes

A **space** is one atomic and ordering domain, owned by one tenant, and
never wider than one workflow or one campaign (D13). Snorri's spaces are
one per (run, shard), where a campaign is a run and members are spread
over a run's shards by subject hash (D32); every operation on one
instance is then one commit. It holds named objects of two shapes,
queues and tables, declared in the space's schema. A sequenced table is a
table whose key the implementation assigns:

| Shape | Holds | Covers |
|-------|-------|--------|
| Queue | Work items: priority, schedule, leases, retries, gates, groups, cohorts, recurrence, declared typed fields, a JSON body patched by path, and key retention for dedupe | Recipient and workflow work; Snorri instance heads and their await fields (D8); effect items and send intents; external events deduplicated by provider id |
| Table | Rows by typed primary key, with typed columns; atomic set, increment and conditional update; field merge rules; row expiry | Keyed state (per-recipient, per-run), handler idempotency records, admission chunk state, suppression flags, rollups, rate and slot counters, consumer cursors |
| Sequenced table | A table whose single Int key the implementation assigns: gapless per table, in commit order, never reused. Rows are immutable; they leave by expiry or retire. Read by range from a cursor. | Timelines and transition history, written in the same group as the transition they record; delivery and engagement events; outbox feeds, including export to Niflheim |

Blobs and event logs, specified in revisions 3 to 6, are removed (D16). A
sequenced table gives an event log's guarantee: a reader paging forward
never later sees a lower sequence than one it has already read. Queue
transition history is not written by the implementation; the consumer
writes the timeline rows it needs in the transition's own group.

Counters are table columns with atomic increment, and a conditional
increment is a guarded `Mutate` (§4). Rate and slot windows are table rows
keyed by a time bucket, with expiry. A separate counter shape is open
question 5, needed only if hot-key increments prove too slow.

Across shapes in one space: a group commit is atomic; every read observes
one snapshot; `request_id` is scoped to the space; one durability level
covers the whole commit; caps apply to the whole commit. Across spaces there
is no atomicity or ordering. Records that belong to an account rather than a
campaign, such as suppression and per-contact send caps, cannot share a
group with a campaign's effects; open question 25.

A rule that spans runs, such as Snorri's re-entry rules `never` and
`after_duration`, is checked against one keyed record per (workflow family,
subject) in a family space (D29). Admission updates that record with a
guarded `Mutate` and then enqueues in the run's space. Two runs admitting
the same subject at once serialize on the record, so only one wins. A
crash between the two writes spends an enrolment without admitting the
subject; replaying the admission with the same request ids repairs it.

### 4. Requests are programs in a typed IR

A consumer sends a **program**: typed data, serialized as JSON, never SQL
text (D12). Together the IR and its expressions are Fireweed's request DSL,
which replaces RESP as the way consumers drive Fireweed (D25). A program is
a list of statements run in one space as one commit (§5). Draft statement
set (question 32):

- `Insert`: enqueue items or put table rows, with a key-conflict policy.
- `Claim`: a selection over a queue, a claim order, a limit and a lease
  duration; returns the leased items with their tokens.
- `Settle`: complete, fail, retry, release or rearm items by id and token.
  `Renew` and `Reassign` change a lease.
- `Mutate`: a target object, a selection (keys, or a
  predicate over declared fields), actions (set or increment fields, patch a
  JSON body by path, reschedule, transition, change item gates, retire), a
  lease policy, a limit, and an optional expectation on the number of
  matches. An unmet expectation rejects the program, which gives
  compare-and-swap and conditional increment.
- `Assert`: a selection and an expectation with no action; a guard that
  never writes and never changes a version (D20).
- `Query`: a selection, a projection or aggregate with group by, an order
  and a row cap. A declared read (§6) is a named, versioned `Query` stored
  in the schema.
- `SetGates` and `SetQueueState`.

The `enqueue`, `claim`, `renew` and `finalize` calls of revisions 3 to 7
become one-statement programs, and the facade keeps helpers that build
them (§13).

Compilation follows weft's layers (D33):

1. **Resolve and type.** Names resolve to the space schema's UMF
   identities, and every expression gets a UMF type (§6). There are no
   implicit casts. An unknown, ambiguous or unsupported construct is
   refused, never guessed.
2. **Logical plan.** A typed tree with no table names, column names or
   backend SQL. Queue semantics are expanded here. The compiler adds the
   eligibility predicate to every claim, adds lease-token checks to settle,
   renew and reassign, and adds attempt charges and version bumps. Only
   `Claim`, `Settle`, `Renew` and `Reassign` may write an item's system
   fields (state, lease token, lease expiry, attempt count, item version);
   a `Mutate` that names one is refused. So D6, D17 and D18 hold without
   fixed functions.
3. **Lowering.** A backend lowers the plan to its SQL dialect with bound
   parameters, result decoders and obligations, or refuses. Each backend
   publishes a capability manifest; a missing capability is a refusal,
   never a fallback to another strategy.
4. **Execution.** The engine runs the SQL in one transaction, passes
   request time as a bound parameter, and writes the request record in the
   same transaction.

A `Claim` on PostgreSQL lowers to one statement of the form `UPDATE items
SET <lease, attempt, version> FROM (SELECT id FROM items WHERE <eligible>
AND <filter> ORDER BY <claim order> LIMIT $n FOR UPDATE SKIP LOCKED) …
RETURNING …`. SQLite and Turso use the same statement without `SKIP
LOCKED`, since one writer owns a space. The engine caches compiled plans by
schema version and statement shape, so the hot statements are compiled and
prepared once; that cache replaces the hand-written verbs of revision 7.

The JSON IR is the wire format. A text syntax is a later frontend that
compiles to the IR, in weft's style (question 32).

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

A rejected group leaves a request record (D19), so its replay returns the
same rejection and no stalled earlier attempt can commit afterwards.

A Snorri transition is one group: finalize the instance-head item with a
patch to its body and await fields (token and `expected_item_version`
guarded), finalize the consumed event item, enqueue effect items and
follow-on work, and update table rows such as counters.

The standalone verbs keep API-001's per-item outcomes: a stale lease in a
`finalize` call affects that entry only. Inside a group the same staleness
rejects the group.

### 6. The schema is UMF

A space's schema is a UMF document (D34). UMF core types are the IR's type
system. A published UMF extension, `umf.fireweed`, declares what UMF core
does not: queues and their system fields, priority, leases, gates,
cohorts, recurrence, merge rules, sequenced keys, row expiry, key retention
and declared reads (question 33). A UMF physical binding document per
backend (UMF CONTRACT-042) holds layout and index choices. The engine
generates each backend's DDL from the schema and its binding, the way UMF's
DDD-to-PostgreSQL projection does (UMF CONTRACT-043). Fireweed reads UMF
through the Rust crate `umf-core`, as weft does.

Each space has a versioned schema: its objects, their typed fields, index
hints, retention, and its declared reads. Only declared fields can appear in
selectors, filters and reads. A field may be multi-valued (a set of text
values, such as an instance's await keys) and indexed per value. The
implementation turns declarations into indexes. A field becomes usable once
its index is ready, and `get_schema` reports index state. Schema changes
are additive only (D22): spaces have finite lives, and a consumer that needs
a different shape starts a new space. UMF's core-evolution check (UMF
CONTRACT-083) decides whether a new schema version only adds.

A table field may declare a **merge rule** (D20): `AddOnly` for a set (a
write may add values, never remove them) or `NonDecreasing` for an integer,
float or timestamp. A write that breaks a rule rejects the group, unless it
is a `Mutate` that declares it overrides merge rules, which is how a
suppression is lifted or a counter reset.

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
| Durability | `FireAndForget`, `Tracked`, `Durable` | Niflheim's write modes (Niflheim ADR-002), used as the levels a commit reaches (D14). `FireAndForget`: visible, may be lost if the process or machine fails. `Tracked`: survives a process crash and a reopen of the implementation's own storage. `Durable`: also survives loss of that storage. The declared value is the lowest level any acknowledgement can have. |
| New-work visibility | `Immediate`, `Bounded { max_ms }` | v1 implementations are `Immediate`, and v1 conformance defines only `Immediate`. |
| Sharing | `SingleProcess`, `MultiProcess` | Whether several instances may serve one space at once. |

Every write request may name the level it needs; with none, the
implementation's default applies (`Tracked` where offered, as in Niflheim).
The commit is acknowledged at the lowest level the implementation offers at
or above the one asked for, and the response reports that level. A level
above the implementation's maximum fails validation. If a commit has not
reached the level in time, the request fails with
`DurabilityPending { reached }`, which tells the caller the commit exists;
replaying the `request_id` waits again. `CommitTimeout` is kept for "outcome
unknown". Levels are prefix-closed in a space: when a commit is `Durable`,
so is every earlier commit, which is what makes a `Durable` send intent
safe even though it was derived from `FireAndForget` or `Tracked` state.

| Implementation | Levels offered | Default | How |
|----------------|----------------|---------|-----|
| PostgreSQL | `FireAndForget`, `Durable` | `Durable` | The server is assumed fully replicated (D14): a committed transaction is `Durable`, and keeping the server so is the operator's job. A `FireAndForget` request commits with `synchronous_commit = off`. A `Tracked` request is acknowledged at `Durable`. |
| Local engine | `FireAndForget`, `Tracked`, `Durable` | `Tracked` | `FireAndForget` replies once the commit is visible, `Tracked` after the batch fsync, `Durable` once the commit's log frame and every earlier one is in S3 (Turso-queue plan §5.1). |
| In-memory | `FireAndForget` | `FireAndForget` | — |

Snorri's send intents are `Durable`. On PostgreSQL every default commit
already is.

Operations on one space are linearizable. Every read observes every effect
committed before its linearization point, whatever its durability level.

### 8. Time

**Observable state is a function of committed operations and time.** Lease
expiry, `not_before` and backoff, cohort deadlines and retention take effect
at their due instant. An implementation may sweep, evaluate lazily, or both.
No caller drives a tick. Under compilation, time-driven state is derived:
the compiler turns "lease expired", "due" and "past retention" into
predicates that compare stored fields with the request-time parameter, so
every statement sees due transitions. The engine writes a derived
transition back when a statement touches the record or a sweep passes. API-006 fixes the processing order for transitions
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

A `Claim` (and `claim_by_item_ids`) takes an optional `eligibility_time`. It
replaces the authority only when deciding whether `not_before` and backoff
have passed for that claim. Lease expiry, attempt timestamps, cohort
deadlines, retention and `now()` in expressions always use the authority.
This carries Snorri CONTRACT-005's separate execution and lease times. No
other request overrides time. A selector that needs a fixed instant uses a
parameter.

Cohort deadlines follow API-001 (D19), evaluated at the deadline instant: unless the cohort has been claimed, it fails with
`cohort-incomplete` if fewer than `cohort_size` members are non-terminal,
or if a non-terminal member is not eligible. Revision 3 counted committed
members only; review showed that let a cohort with a retired member never
expire, and let one gated member withhold its siblings indefinitely.

### 9. Leases are fenced by token

Renew, reassign and finalize address a lease by item id and token (or
cohort id and cohort token). Finalize and `Mutate` may also carry
`expected_item_version`. Renew and reassign change `item_version`, as
API-001 says (D18). Reads never return a lease token. The id-only,
all-or-nothing facade helpers are removed.

Each claim charges an attempt. When a lease expires, the item returns to
pending, unless its claims have used up `max_attempts`: then it fails with
`attempts-exhausted` at the expiry instant (D17). A worker that crashes on
an item every time therefore cannot loop on it forever. This amends
API-001, where expiry charged nothing and `max_attempts` was checked only on
a retry finalize.

A queue may opt out of that bound by declaring `max_attempts = u32::MAX`
(D30). Snorri does: it parks an item after K claims without a commit and
reports it to the control plane, so the item's pending effect goes through
recovery instead of failing silently. An embedder that opts out takes over
the bound, and its count is only as good as the claims' durability: on
the local engine a claim lost in a crash leaves no attempt charged, so
Snorri claims at `Durable` there (one durable write per claim batch), or
at `Tracked` except for two lease periods after a crash takeover, which
needs a `previous_released` flag on the control plane's grant. Neither
needs an interface change. On PostgreSQL every claim is `Durable`.

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

### 11. Engine backends

| Implementation | Status | Durability | Sharing |
|----------------|--------|------------|---------|
| PostgreSQL | First-class and production-supported. Snorri and Cayce build against it first. Real tables generated from each space schema (layout: question 31). | `FireAndForget`; `Durable` by default | `MultiProcess` |
| Turso: MVCC on NVMe, S3 log | The local engine, in the Turso-queue session's phases L0 to L4. Speed gates B1 to B4 (Turso-queue plan §3.8) apply here only. One database per space. | All three; `Tracked` by default | `SingleProcess` per space |
| SQLite | The local engine's comparison arm and a single-process backend. One database per space. | `FireAndForget`, `Tracked` | `SingleProcess` per space |
| Memory | The reference interpreter (§2), for consumers' tests and the differential test. | `FireAndForget` | `SingleProcess` |
| Object log × Turso (current runtime) | Deprecated once the local engine's S3 log passes (D24). Not wrapped. | — | — |
| Durable Objects | Possible: every request is one atomic step. Not scheduled. | — | — |

The PostgreSQL backend replaces the revision 6 increment
(`fireweed-interface-postgres`), which loads whole queues and tables and
runs the shared request logic in Rust. It keeps TD-002's indexes, `SKIP
LOCKED` claim and group and cohort locking, and drops the command log,
`LeaseExpired` commands, `assignment_epoch` fencing and epoch-fenced
sweepers. Group commits and chunk steps run at `SERIALIZABLE` where they
need it (D19), so absence guards and range expectations cannot be defeated
by write skew; verbs run at `READ COMMITTED` with an explicit lock protocol
(API-006). Every acknowledged commit is one transaction, so the server's
replication makes it `Durable` (D14).

**Ownership.** A space on the local engine has one writer. The embedder's
control plane (Snorri or Cayce) decides which node owns a space and fences
old writers (D15, D28); Fireweed runs no lease or clock for it. The engine
opens a space with the ownership epoch the control plane granted, and
writes a takeover record at the next S3 log sequence before it accepts
writes; log keys are written with `If-None-Match: *`, so a stale writer's
next upload fails and no `Durable` commit is lost or forked. `FireAndForget`
and `Tracked` commits touch only the local disk, so only the control
plane's rule protects them: no new epoch while the old process may still
run. Clients recheck placement on an interval and after a refusal, so
readers move to the new node. Ownership is a constructor parameter, not a
`DataPlane` operation. A fenced engine fails every later request with the
definitive `InstanceSuperseded`, and a space opened at a higher epoch
restores from the S3 log. Item state, including attempt counts, moves with
the log: a clean handover keeps all of it, and a crash keeps everything up
to the last shipped frame. Item leases stay with Fireweed; the control
plane's lease covers only which node writes the space.

A space stays on the implementation it starts on; there is no migration
between implementations (D26).

### 12. Conformance

The conformance suite tests through the interface only. It reads
`Guarantees` and `Capabilities`, and covers every shape, group commit
atomicity, caps and chunking, declared reads, the expression semantics, the
time rules under a manual clock, concurrent claims, and crash and reopen in
a killed child process. Each rule is pinned by a named test (API-006
§Rule-pinning tests) that every backend runs. As weft requires of its
backends, the suite is one corpus of programs, expected results and
expected refusals that every backend must pass before it claims a
capability. A test-only crate runs a differential test of every SQL backend
against the memory backend's interpreter, including generated expressions,
aggregates and programs; the compiler's logical plans are checked against
golden fixtures. The suite starts as
`fireweed-interface-conformance` beside the existing `fireweed-conformance`,
which keeps testing the current runtime until migration step 6.

### 13. The facade and RESP

`Fireweed` (API-005) becomes a concrete wrapper over `Arc<dyn DataPlane>`,
with helpers that build the common programs (enqueue, claim, settle,
renew). It names no
implementation. RESP is deprecated for now in favour of the request DSL
(D25): `fireweed-resp` stays frozen on the current runtime and is not
ported.

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
| Interpret requests in Rust over records loaded from storage (revisions 5 to 7) | One code path for every implementation | Loads whole objects; cannot use indexes or `SKIP LOCKED`; every database is a store, not an engine | Rejected (D33); kept as the memory backend's reference interpreter |
| Fixed verbs beside a request model (revisions 3 to 7) | Hand-tuned hot paths | Each new claim or bulk need is a new verb | Rejected (D33); the plan cache keeps hot statements fast |
| Extend weft with queue statements | One language across truss, ashlar and Fireweed | Weft has no writes beyond a bulk `UPDATE` and no nulls, timestamps or floats; queue semantics would enter a read-and-transform compiler | Rejected for the IR (D33) |
| Share weft's machinery under Fireweed's own IR | One implementation of UMF resolution, typing, exactness, backend manifests and PostgreSQL emission; fixes land in both | Weft's code is sliced by dialect version and feature-gated, so shared parts must first move into stable crates | Adopted (D35) |
| Fireweed's own schema language and type system | No dependency | Duplicates UMF's types, evolution check and DDL generation | Rejected (D34) |
| **One data-plane IR over spaces, queues and tables, typed by UMF, compiled to SQL per backend, with group commit and declared reads; PostgreSQL first** | Matches the intent; one abstraction for all workflow data; claims, bulk changes and queries in one language | A compiler and four backends to specify and test | **Proposed** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Snorri and Cayce get one storage abstraction for all fast-changing workflow data, with atomic commits across shapes. |
| Positive | Snorri and Cayce can build now on Postgres. The local engine slots in behind the same interface and suite. |
| Positive | Bulk changes and live analytics need no new methods, only declarations and requests. |
| Positive | Body patches shrink requests, and table rows take independently changing data out of the instance body. Engines still rewrite a stored JSON value on update. |
| Negative | Large payloads (Snorri's transition outputs, workflow definitions) need a store outside Fireweed, referenced by a field. |
| Negative | Records scoped to an account cannot be checked atomically with a campaign's effects (open question 25). |
| Negative | With one space per (run, shard) (D32), an event for instances in several runs is one commit per run space, deduplicated per space only, and cancelling a run is one commit per shard. |
| Positive | Expression, aggregate, group-commit and time rules are pinned by named conformance tests that both engines run. |
| Positive | Correctness is defined by observable behaviour and checked against the reference interpreter on every backend. |
| Positive | New claim shapes, bulk changes and queries need no new API: they are programs the compiler checks and lowers. |
| Positive | Schemas, types and DDL come from UMF, so the Cayce UI, generated types and Niflheim exports can read the same schema. |
| Negative | Fireweed takes on a compiler: name resolution, typing, plan building and lowering for three SQL dialects, with golden fixtures for each. |
| Negative | Fireweed depends on UMF's Rust crates and on UMF types it needs that are not in core yet (question 33). |
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
| ADR-008, ADR-012, ADR-013, ADR-016, ADR-017 | Rescoped to the object-log implementation, which is deprecated (D24). ADR-008's atomicity outcomes move into API-001 without an owner. |
| ADR-009 | Amended: the facade holds a `dyn DataPlane`; its "no dyn backend" and crate-private-constructor rules and the publish topology change accordingly. |
| ADR-010, ADR-011 | Restated as schema declarations: declared fields, index hints, typed payload schemas. |
| ADR-014 | The Kafka consumer surface reads sequenced tables. |
| ADR-018 | Exact ensure becomes `apply_schema` with an expected version. |
| ADR-022 | Stands for the single concrete `Fireweed`, except `projection_control()`, `StorageConfig` and maintenance leave it. |
| API-001 | Major version. Remove storage language and `SetGates.applied_command_position`. Replace `MutateItems` with `Mutate` requests, drop `evaluated_at` and ordered-selector first-match ownership. Add `eligibility_time` to claims, declared-field filters, `ReassignLeases`, lease policies including lease break, the cohort deadline rule evaluated at the deadline instant (§8), the time rule, request records for rejected requests, and lease expiry not changing `item_version`, and lease expiry failing an item whose claims used up `max_attempts` (D17). Define progress metrics by base eligibility. Restate atomicity without an owner. Move discovery authorization to the adapter. |
| API-002 | Operator actions become requests: pause and resume are `SetQueueState`; redrive and repair are `Mutate` transitions with lease break where forced; purge is `Mutate` retire; archive sets item metadata and deletes nothing; audit is a sequenced table; operation lookup is `request_id` replay. `max_affected` becomes the per-request limit. |
| API-003 | Profiles are restated in API-006 terms. |
| API-004 | Replaced by declared reads. |
| API-005 | Rewritten as the binding of API-006 to `Fireweed`. |
| TD-002 | Revised per §11, extended to tables, sequenced tables and declared reads. |
| TD-003 | Ownership and fencing become object-log internals. |
| TD-006 | Deprecated with RESP (D25). |
| TD-007 | Durability classes become §7's levels; `ReclaimDriver` is replaced by the time rule; `UpsertPort` becomes `Enqueue` with a key-conflict policy. |
| TD-008 | Queue change records become timeline rows the consumer writes to a sequenced table. |
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
3. Build `fireweed-compiler` and `fireweed-engine` (revision 8): the space
   schema as UMF with the `umf.fireweed` extension; the IR and logical plan;
   the memory backend as the reference interpreter, from today's
   `fireweed-interface-engine`; then the PostgreSQL backend as SQL lowering,
   with DDL generated from UMF. Add the differential test. Revision 6's
   PostgreSQL increment, which interprets requests over loaded records, is
   superseded and kept only until the PostgreSQL backend passes the
   suite.
4. Cut `Fireweed` over to the interface. Snorri and Cayce adopt it on
   Postgres. The old facade and runtime stay frozen until this step lands.
5. Build the SQLite and Turso backends and run them through the suite, the
   differential test and, for Turso, gates B1 to B4.
6. Deprecate object-log once the local engine's S3 log passes (D24), and
   RESP (D25). Retire
   `StorageConfig`, `RETIRED_STORAGE_CELL` and whatever the decisions remove.
   Apply the document changes above.

## Open questions for the owner

### Resolved in revision 7

| # | Question | Ruling |
|---|----------|--------|
| 1 | Names | Kept (D23). |
| 2 | Space granularity | At most one workflow or campaign (D13); Snorri uses one per (run, shard) (D32); consequence in question 25. |
| 3 | Object-log's future | Deprecated once the local engine's S3 log passes (D24). |
| 4 | Blobs, event logs, history in v1 | No blobs; history and event feeds are sequenced tables (D16). |
| 6 | Admission chunk state | A table row in the data plane (D21). |
| 7 | Cohort deadline rule | Draft adopted (D19). |
| 8 | Rejected requests recorded | Yes (D19). |
| 9 | Schema evolution | Additive only (D22). |
| 10 | Percentile | Exact (D23). |
| 11 | RESP and `StreamGroup` | RESP deprecated in favour of the request DSL (D25). |
| 12 | Protobuf | Dropped unless the DSL needs it (D25). |
| 13 | Local engine durability | Niflheim write modes per request; `Tracked` default (D14). |
| 14 | Postgres `Replicated` | PostgreSQL is assumed fully replicated; commits are `Durable` (D14). |
| 16 | Local engine stale-writer fencing | The control plane fences; S3 conditional writes back up `Durable` commits; clients recheck placement (D15). |
| 17 | Renew and `item_version` | Renew bumps it (D18). |
| 18 | Event log sequence on Postgres | Event logs are gone; a sequenced table's counter is assigned under the space's writer lock today, and its row lock under the target protocol (D16). |
| 19 | Postgres per-request durability | Per request: `synchronous_commit = off` for `FireAndForget`, a normal commit otherwise (D14). |
| 15 | Snorri's open items | Answered by Snorri TD-012 revision 7 (below). |
| 20 | Bounding redelivery | Lease expiry counts toward `max_attempts` (D17); a queue may opt out with `u32::MAX` (D30). |
| 30 | Definitions larger than one value | Snorri caps definitions at 256 KiB and transition output at 1 MiB (D31). |
| 29 | A space per Snorri shard | One space per (run, shard) (D32); re-entry across runs uses the family record (D29). |
| 21 | Choices made without a ruling | Adopted as needed, including `SERIALIZABLE` where it is required (D19). |
| 22 | Moving spaces between implementations | Not needed (D26). |
| 23 | Merge policies | `AddOnly` and `NonDecreasing` field rules, with an explicit override (D20). |
| 24 | Guard-only operation | `WriteOp::Assert` (D20). |

Snorri's TD-012 revision 7 (uncommitted there; sent to this session on
2026-11-16) answers question 15:

- O2, claim time: callers pass only an eligibility time; lease time comes
  from the store (D2).
- O14, how Cayce tells Snorri where the queues live: a Snorri-defined
  store configuration from which Snorri builds the implementation. It is
  between Snorri and Cayce and needs nothing from this interface.
- O15, durability: withdrawn; Snorri uses per-request levels (D14) and its
  per-class choices are in API-006's Snorri inventory.
- O16, ancillary state: counters and admission state become tables (D21,
  question 5); history goes to Niflheim and to sequenced tables (D16);
  idempotency maps become a table with row expiry. Definitions of 31 to
  43 KB fit a table row, and Snorri caps them at 256 KiB (D31).
- O17, what a space is: one per (run, shard) (D32).
- O18 and O19, the ownership-epoch and poison-report ports: interfaces
  between Snorri and Cayce's control plane. They fit §11's fencing, and
  O19's attempt counting is D30.

### Still open

5. **Counters.** The only counter Snorri or Cayce keeps today is Cayce's
   per-contact send-cap ("slotting") counter: one row per account,
   contact and UTC hour, incremented only while below the cap, read in
   batches of up to 168 hours for daily and time-of-day caps, expiring 7
   days after its first write (Cayce `crates/scheduler/src/slotting.rs`,
   `crates/snorri-effects/src/slotting_state.rs`). Snorri needs no counters;
   its counts are aggregates over records. Contention is per contact per
   hour, so there are many keys and no hot ones. A table row with a
   `NonDecreasing` count and a `Mutate` filtered on `count < cap` covers
   it; a dedicated counter shape would only matter for hot keys, and none
   are documented. Two things need a ruling: the counter is keyed by
   account, so it falls under question 25; and Snorri's TD-011 recorded an
   owner decision that "cap counters stay in Dragonfly", shared with 7th
   Sense, which D21 reverses unless 7th Sense must keep reading them.
25. **Account-scoped records across campaign spaces.** Suppression and
    per-contact send caps belong to an account, but a space is one
    workflow or campaign (D13), and a group cannot span spaces. Options:
    (a) copy suppression into every campaign space of the account, so each
    send's group asserts it atomically (a suppression event writes one row
    per active campaign; the copies converge because the reasons are
    `AddOnly`); (b) keep account-scoped tables in an account space and check
    them in a separate request before the send, accepting a race window and
    re-checking after; (c) for send caps, increment in the account space
    first and give the slot back if the send's commit fails, as Cayce's
    design does today (it accepts no refund and fails open). Draft: (a) for
    suppression, which must be exact at send time; (c) for send caps.
26. **Claim order with sleeping items.** A claim's scan budget counts items
    that are gated or not yet due, and claim order is priority only, so
    sleeping Snorri instances at the head of a queue can exhaust the budget
    and starve claims. Snorri's workaround is priority = due time. Add a
    claim order of due time first, or require implementations to skip items
    not yet due without charging the budget? (Turso-queue session.)
27. **Several actions on one record.** A Snorri wake patches the inbox,
    removes the await key and reschedules, which today takes three `Mutate`
    operations on one key in one group. Add `Action::Seq(Vec<Action>)`?
    (Turso-queue session.)
28. **Timers that do not charge attempts.** A Snorri `wait` through
    `Retry { not_before }` counts toward `max_attempts`. On a queue
    declared with `recurrence`, `Rearm` reschedules without charging, and
    the DSL scenarios use it (DSL-2 shows both). Is declaring instance
    queues recurring acceptable, or should any queue get a non-charging
    reschedule? Draft: use `Rearm`; no new outcome. (Turso-queue session.)
31. **PostgreSQL layout.** With real tables per space schema, does each
    space get its own PostgreSQL schema (namespace), or do spaces with the
    same schema version share tables with the space id leading every key
    and index? At 1,024 or more Snorri spaces, the first means thousands
    of tables; the second keeps the table count at the number of schema
    versions. Draft: shared tables per schema version.
32. **Statement set and surface.** Which revision 7 features become v1
    statements: `claim_by_item_ids`, cohorts, group batching and
    recurrence? Is a text syntax in v1 or later, and does it reuse weft's
    grammar where it can? Draft: the §4 list, JSON IR only in v1.
33. **The `umf.fireweed` extension.** Is it published in the UMF
    repository, as UMF expects of target-shaped meaning, or kept in
    Fireweed? UMF's temporal facets are a candidate core ideal, not yet
    admitted (UMF CONTRACT-047), so Fireweed's `Timestamp` (UTC,
    milliseconds) needs a stated meaning in the extension until they are.
    The same holds for `TextSet` and the JSON body.
34. **UMF declarative actions.** UMF specifies mutation contracts
    executed by a qualified executor (UMF CONTRACT-900, CONTRACT-901). Should
    Fireweed's statements be UMF actions, with the engine as their
    executor? Draft: not in v1; the IR stays Fireweed's own (D33) and is
    revisited with weft alignment.
35. **The engine trait.** Does `DataPlane` keep the verb methods, or narrow
    to `apply_schema`, `execute(program)`, `read(query)`, `get_request`
    and `get_schema`? Draft: narrow it; the facade builds programs.
36. **What to extract from weft first.** Draft order: UMF model loading,
    resolution and typing; exact values; the expression and predicate
    core; the backend boundary; PostgreSQL emission. Each moves into a
    crate in the weft repository with its own version, and weft's
    dialect slices depend on it as Fireweed does. Which crates, and in
    what order? The types weft's v0.1 language refuses (nullable fields,
    timestamps, floats) are weft additions, not Fireweed extensions:
    "the missing types are obvious additions to weft" (owner, 2026-11-16).

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Expression and aggregate results differ between Postgres and the local engine (collation, nulls, overflow, float rounding) | H | H | API-006 fixes evaluation order, faults, casts and aggregate precision; rule-pinning tests and the differential test check them. |
| Compiled SQL diverges from the reference interpreter on one backend | H | H | Golden plan fixtures, the shared corpus with expected refusals, and generated programs in the differential test. A backend refuses what it cannot honour. |
| Postgres group commits at `SERIALIZABLE` retry under contention | M | M | Bounded retries, then `Contention`; hot paths use the verbs at `READ COMMITTED` with explicit locks. The first increment serializes writes per space instead. |
| The interface grows into a general database | M | H | No joins, capped requests, index-backed selectors, live data only; history to Niflheim. |
| A hot table row (counter, rollup) serializes commits on Postgres | M | M | Open question 5; rate windows keyed by bucket spread the load. |
| A suppression or send cap checked outside the effect's group lets a send through in the gap | M | H | Open question 25. |
| A single-writer engine stalls claims behind bulk work | H | M | Caps, chunked `Mutate`, and B4. |
| Waiting for `Durable` on every send-intent commit limits throughput on the local engine | M | M | Only commits that ask for it wait, and the engine ships frames to S3 in batches. |
| The control plane grants a new epoch while the old local-engine writer lives | L | H | `Tracked` commits since the last shipped frame are lost; `Durable` ones are fenced by S3 regardless (§11). |
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

- `resilience`: durability is chosen per request, declared per
  implementation and reported per commit.
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
