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

Revision 9 (2026-11-16) puts the DSL first and makes the executor plain
SQL. Consumers describe their data in a **schema** document: queues,
tables, fields, indexes and named reads. They change it with **programs**:
lists of typed statements, such as claim, settle, update and select, that
run as one transaction. Both are YAML or JSON documents (API-006).

An executor creates real tables and indexes from the schema, and runs each
statement as a few SQL statements over them. The database does the work:
indexes choose rows, row locks order writers, and its clock decides when
something is due. The executor keeps nothing in the process between calls.

Revisions 5 to 8 went the other way, and revision 9 removes that design:

- The implementations loaded whole queues and tables into Rust and ran an
  in-memory request engine over them. The PostgreSQL implementation stored
  items as JSON blobs and only read and wrote them.
- A lock on each space's row serialized every request to that space, and the
  space's request time was stored on that row.
- Revision 8 added a reference interpreter, a logical plan, a plan cache, a
  differential test against the interpreter, and weft's layered backend
  boundary.

The owner's rulings D1 to D40 stand except where the table below says
revision 9 changes them. API-006 revision 9 is the DSL reference.

A fresh-eyes review of the first revision 9 code found the following
(D42). Every one is fixed in the same PR:
- the sweep could clear a lease another worker had just taken;
- a claim walked the whole claim index on a mostly sleeping queue;
- the index rule admitted ranges, partial-index columns and unbounded
  aggregates;
- stepped updates re-read every match;
- statements cost several round trips each.

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
- "We are trying to define a DSL that allows for the claim, bulk change, and
  query flexibility we need without the limitations of a fixed function
  calling api. It may not need to be sql, and it might be typed, but it's
  going to need to compile down to sql." (2026-11-16, D33.)
- "This really shouldn't be much more than some sql to manipulate groups of
  rows in the database. If there are a bunch of in memory stores, caches,
  memos, or full table scans, the design is broken." (2026-11-16, D41.)

### What the repository does instead

API-001 writes down the queue semantics, but nothing can implement them,
because five decisions put the boundary inside one design:

1. **ADR-007** drew the hexagon around storage ports, below an engine that
   owns claims, idempotency, fencing and validation.
2. **ADR-008** made one owner node per queue, fenced by a log epoch, product
   law.
3. **ADR-012** made every backend `LogStore × ProjectionStore ×
   ControlPlane`.
4. **ADR-013** made the command log mandatory.
5. **ADR-024** reduced the product to one cell, the S3 object log with a
   Turso projection.

| Observation | Evidence |
|-------------|----------|
| The traits an implementer faces are one design's internals | `fireweed-engine` exports 53 public traits, among them `LogStore`, `ProjectionStore`, `CommitStrategy` and `SnapshotStore`. |
| There is no public trait to implement | The facade's backend bound, `LibBackend`, is `pub(crate)`. |
| PostgreSQL is forced into another implementation's shape | `fireweed-postgres` depends on `fireweed-objectlog` and `fireweed-projection`. |
| Consumers work around missing shapes | Snorri rewrites instance state as one JSON blob on every commit, and keeps idempotency and seen-event sets in unbounded process memory (Snorri TD-012 rev 6 §3.7). |

The Cayce storage spike (2026-11-14) found that a single-statement claim
(`UPDATE … RETURNING` over a candidate subselect) works on SQLite, tursodb
and a Durable Object. It also found that a claim waits behind a large
`UPDATE` when there is one writer, and that the object-log runtime reached
about 100 recipients/s against Cayce's target of about 17,000/s.

## Owner decisions

| # | Decision | Source | Status in revision 9 |
|---|----------|--------|----------------------|
| D1 | Time-driven transitions are in the contract; no caller tick. | Owner, 2026-11-13 | Kept: they are predicates on stored times (§7). |
| D2 | One time authority per storage. | Owner, 2026-11-13 | Kept: the database's clock (§7). Claims take an optional eligibility time; lease time always comes from the authority. |
| D3 | ADR-008 ownership becomes internal to object-log. | Owner, 2026-11-13 | Moot (D24). |
| D4 | `Bounded` new-work visibility allowed, fails closed. | Owner, 2026-11-13 | Not used: every implementation is `Immediate`. |
| D5 | RESP gets a `StreamGroup` family. | Owner, 2026-11-13 | Superseded by D25. |
| D6 | Core leases are token-fenced, with per-item outcomes. | Owner, 2026-11-13 | Kept (§8). |
| D7 | `MutateItems` and operator repair become optional families. | Owner, 2026-11-13 | Superseded by D12. |
| D8 | Snorri owns no tables. Its workflow state lives in Fireweed items; Fireweed provides group commit. | Owner via Cayce, 2026-11-15 | Kept (§5). |
| D9 | PostgreSQL is first-class, not only an oracle. A local engine (Turso MVCC on NVMe, S3 in the commit path; SQLite as comparison arm) follows and passes the same suite. Speed gates B1 to B4 apply to the local engine only. | Owner via Cayce, 2026-11-15 | Kept (§10). |
| D10 | Cayce reads queue data only through versioned, named, declared reads, never engine tables. | Owner via Cayce, 2026-11-15 | Kept (§4). |
| D11 | All high-volume, fast-changing durable workflow data goes through Fireweed. The control plane is separate. Exhaust goes to Niflheim. | Owner via Cayce, 2026-11-15 | Kept (§1). |
| D12 | Requests are typed data with a selector, an action and a limit; a group commit is a list of them. No SQL from consumers. Small expression language. Every write capped. Durability reported per commit. | Owner via Cayce, 2026-11-15 | Kept (§4, §9). D33 replaces its fixed verbs with statements. |
| D13 | "atomic writes should be per workflow or per campaign." | Owner, 2026-11-15 | Kept (§3). |
| D14 | Durability per request with Niflheim's write modes (durable, tracked, fire and forget). "Just assume postgres is fully replicated and design the protocol to ensure transactions are used to achieve that assuming perfect server durability." | Owner, 2026-11-15 | Kept (§6). |
| D15 | "fencing should happen by the control plane. we'll have to have some sort of recheck interval to force readers to switch to the new node." Also: "the same applies to stopping old writers. the lease can be managed by the control plane via the embedder." | Owner, 2026-11-15 | Kept (§10). |
| D16 | "this system isn't really meant to store blobs. even history will need to be supported for timeline and event triggering … fireweed is exposing a generic interface similar to a sql database but with more constraints. define another table, be done." | Owner, 2026-11-15 | Kept: queues and tables; history is a sequenced table (§3). |
| D17 | "yes, add max_attempts": lease expiry counts toward `max_attempts`. | Owner, 2026-11-15 | Kept (§8). |
| D18 | "yes, bump item_version" on renew and reassign. | Owner, 2026-11-15 | Kept (§8). |
| D19 | Cohort deadline draft: "yes". Rejected requests recorded: "yes." Choices made without a ruling: "yes, as needed. serializable transactions seems pretty aggressive but if it's necessary..." | Owner, 2026-11-15 | Rejected requests are recorded (§5). `SERIALIZABLE` turned out not to be necessary: guards lock the rows they count (§5). Cohorts leave v1, so the deadline rule waits with them (question 37). |
| D20 | Field merge rules: "yes add merge rules". A guard-only operation: "agree". | Owner, 2026-11-15 | Kept (§3, §4). |
| D21 | "all high-volume ops should be in the data plane, not control plane." | Owner, 2026-11-15 | Kept: admission chunk state is a table row. |
| D22 | "no need for complex schema changes. these spaces will not have infinite lifetime" | Owner, 2026-11-15 | Kept: schema changes only add (§3). |
| D23 | Exact percentile: "agree." Names: "these are ok". | Owner, 2026-11-15 | Kept. |
| D24 | "object-log can be deprecated assuming we can implement the logs-to-s3 without it." | Owner, 2026-11-15 | Kept (§10). |
| D25 | "deprecate RESP for now in favor of the DSL"; "drop protubuf unless required for the DSL". | Owner, 2026-11-15 | Kept. |
| D26 | "no need to migrate spaces"; "we don't need a migration path". | Owner, 2026-11-15 | Kept. |
| D27 | "we do need to ensure that the dsl can be fully implemented on this model". | Owner via the Turso-queue session, 2026-11-15 | Kept: API-006's Snorri inventory maps each construct to a statement. |
| D28 | "crashes can be handled by the control plane via the embedder (snorri/cayce)". | Owner via the Turso-queue session, 2026-11-15 | Kept. |
| D29 | "use the family space record" for re-entry across runs. | Owner, 2026-11-16 | Kept (§3). |
| D30 | "accept Snorri's max_attempts opt-out". | Owner, 2026-11-16 | Kept (§8). |
| D31 | "Yes, cap at 256 KiB" for Snorri workflow definitions. | Owner, 2026-11-16 | Kept. No interface change. |
| D32 | "Yes, per run-shard": Snorri's spaces are one per (run, shard). | Owner, 2026-11-16 | Kept (§3). |
| D33 | "Fireweed *is* effectively a compiler from 'some typed ir' to 'some database backend (turso, sqlite, postgres)'." The IR: "Own IR, weft style". | Owner, 2026-11-16 | Kept: the DSL is the typed IR, checked against the schema and lowered to SQL (§2, §4). There is no logical plan, plan cache or interpreter. |
| D34 | "We should leverage umf for the typesystem, underlying schemas, ddl, etc" | Owner, 2026-11-16 | Staged: v1 uses UMF's scalar types in Fireweed's own schema document; a UMF loader is question 39 (§2). |
| D35 | "If there is a machinery we can share with weft, we shuold do so." The missing types are "obvious additions to weft". | Owner, 2026-11-16 | Kept. v1 needs none of weft's machinery: statements become SQL directly (§4). The shared crates serve a UMF loader (question 39). |
| D36 | "we need to measure. i'm worried deletes and indices will become problematic if we don't use indpendent tables" | Owner, 2026-11-16 | Open question 31. |
| D37 | "yaml (json) syntax is ok for now." | Owner, 2026-11-16 | Kept (§4). |
| D38 | The `umf.fireweed` extension "can live in umf". | Owner, 2026-11-16 | Superseded by D44: Fireweed's extension lives in the Fireweed repository. |
| D39 | No UMF declarative actions in v1; a narrow engine trait; claim indexes lead with due time; no combined-action statement; `Rearm` for non-charging waits. | Owner, 2026-11-16 | Kept: one claim index, `(due_at, priority, id)`, and claims in that order (D42). `Rearm` is a `release` settle with `after`. |
| D40 | Extract shared crates from weft and evolve weft's tooling with them. | Owner, 2026-11-16 | Done: DocumentDrivenDX/weft#70, merged. |
| D41 | "This really shouldn't be much more than some sql to manipulate groups of rows in the database. If there are a bunch of in memory stores, caches, memos, or full table scans, the design is broken." | Owner, 2026-11-16 | Adopted: revision 9. |
| D42 | "Fix them all", on the review findings, with claims ordered by due time and priority breaking ties, as recommended. | Owner, 2026-11-16 | Adopted in revision 9 (§3, §4, §5, §9). |
| D43 | "Full UMF documents"; "there is no reason I can see that fireweed schemas shouldn't be UMF schemas"; "if we need umf improvements we should get them." | Owner, 2026-11-16 | Accepted, in progress (question 39). A space schema becomes a UMF core 0.10 document. Its physical tables are `umf.tables` (UMF CONTRACT-081), and the queue semantics go in a Fireweed extension. |
| D44 | "We should be targetting postgres 18." "The additional types and additions to umf tables should go straight to core." Asked whether the extension must live in UMF: it may live in Fireweed, registered through `umf-core`'s registry. | Owner, 2026-11-16 | Adopted. PostgreSQL 18 is the target, matching `umf.tables`' `postgresql18/0.1` profile, and the executor's tests run on PostgreSQL 18.7. The JSON value, string sets, `jsonb` and array columns, GIN, and partial and descending indexes go into UMF core and `umf.tables` (DocumentDrivenDX/umf#172, rewritten). |

## Decision

### 1. Fireweed is the data plane for Snorri and Cayce

Fireweed stores the durable workflow data that is high in volume or rate
of change: work items, workflow state, per-recipient state, counters,
admission chunk state, operational events and timelines. Large payloads are
not stored (D16); a field holds a reference.

Outside Fireweed: the control plane (tenants, campaign definitions,
configuration) in its own PostgreSQL, and exhaust (logs, traces, full
history, analytics across data sets) in Niflheim. Fireweed's analytics
cover live data: counts by state, lag and rate windows.

### 2. The DSL is the interface

Consumers write two kinds of documents, in YAML or JSON (D37). API-006 is
the reference for both.

- A **schema** declares a space's queues and tables, their typed fields,
  which fields are indexed, merge rules, retention, and named, versioned
  reads (D10). Changes only add (D22).
- A **program** is a list of statements that run in order as one
  transaction:
  - `insert`;
  - `claim`;
  - `settle`, which ends a lease as `complete`, `fail`, `retry` or
    `release`;
  - `renew`;
  - `update`, `delete` and `assert`;
  - `select` and `read`;
  - `pause` and `resume`.

  Statements take predicates, assignments and body patches written as
  typed data, never SQL text (D12).

```yaml
request_id: transition-r1-7
statements:
  - settle: {queue: instances, outcome: complete, items: [{id: 41, token: "6f1c…"}], set: {step: 3}, require_all: true}
  - insert:   {into: effects, rows: [{key: "r1-send-1", body: {template: welcome}}], on_conflict: reject}
  - update:
      target: send_slots
      keys: [{contact: c1, hour: "2026-11-16T10:00:00Z"}]
      where: {lt: [count, 3]}
      set: {count: {add: [{field: count}, 1]}}
      expect: {min: 1, max: 1}
```

The DSL is the compiler D33 asks for. A program is type-checked against the
space's schema: names resolve, operand types match with no implicit casts,
system columns are read-only, and every selection uses an index (§9).
Then each statement lowers to a fixed SQL shape with bound parameters
(§4). There is no logical plan, plan cache or interpreter.

The schema's types (`text`, `int`, `float`, `bool`, `timestamp`, `json`,
`text_set`) are a subset of UMF's scalar types. In v1 the schema is
Fireweed's own small document. Reading UMF documents with the
Fireweed extension (D34, D43, D44) comes later, as a loader that produces
the same schema (question 39).

### 3. Spaces and shapes

A **space** is one atomic domain owned by one tenant, never wider than one
workflow or one campaign (D13). Snorri uses one space per (run, shard)
(D32). A space holds named queues and tables, declared in its schema, and
each becomes a real table:

| Shape | Columns | Covers |
|-------|---------|--------|
| Queue | System columns `id`, `key`, `state`, `priority`, `due_at`, `attempts`, `version`, `lease_owner`, `lease_token`, `created_at`, `updated_at`, `finished_at`, and `body` (JSON); then one column per declared field. `due_at` is when the item next needs attention: when it becomes claimable, or when its lease expires. | Recipient and workflow work, Snorri instance heads and their await fields (D8), effect items, external events deduplicated by key |
| Table | `id`, `version`, `created_at`, `updated_at`, `expires_at`, and one column per declared field; the key fields get a unique index. | Keyed state, idempotency records, admission chunk state, suppression flags, counters |
| Sequenced table | A table whose `id` is assigned from a counter row in the same transaction, so ids are gapless and in commit order. Rows are immutable and leave by expiry. | Timelines, transition history, delivery events, outbox feeds |

The executor creates for every queue:
- the claim index `(due_at, priority, id)` over pending and leased items
  (D39). Claims run in that order, so a claim reads about `limit` index
  entries whether most items are due or most are sleeping. Priority
  breaks ties among items due at the same instant (D42);
- a unique index on `key`;
- an index on `finished_at` for retention;
- one index per declared indexed field. A btree index is `(field, id)`, so
  a stepped update on the field is a range scan; a `text_set` gets a GIN
  index, such as for Snorri's await keys.

Queue tables leave 20% free space per page (`fillfactor = 80`), so updates
that change no indexed column can stay on their page. Indexes come only
with new objects. A field added to a live object is unindexed, because an
index build would block the object's writes.

Whether each space gets its own PostgreSQL tables, or spaces share tables
keyed by space id, is decided by measurement (question 31). The v1
executor gives each space its own PostgreSQL schema.

A table field may declare a merge rule (D20): `add_only` for a set,
`non_decreasing` for a number or time. An update that would break it
rejects the program unless it explicitly overrides the rule.

A rule that spans runs, such as Snorri's re-entry rules, is checked against
one keyed row per (workflow family, subject) in a family space (D29).
Counters are table columns updated with `{add: …}`; rate and slot windows
are rows keyed by a time bucket, with row expiry.

### 4. The executor is plain SQL

| Crate | Role |
|-------|------|
| `fireweed-dsl` | The schema and program types, YAML and JSON reading, and the checker. No I/O. |
| `fireweed-dsl-postgres` | The PostgreSQL executor: DDL from a schema, SQL for each statement, and the functions `apply_schema`, `get_schema`, `execute` and `sweep`. |

A SQLite and Turso executor follows, with the same statement shapes
without `FOR UPDATE SKIP LOCKED`, since one writer holds a space's
database (§10).

The executor's functions take a database connection and hold nothing
between calls:
- no loaded copies of queues or tables;
- no caches;
- no in-process locks;
- no stored "last time".

Each call reads the space's schema row, checks the program, and runs it.

| Statement | SQL on PostgreSQL |
|-----------|-------------------|
| `insert` | `INSERT … SELECT … FROM jsonb_to_recordset($rows) … ON CONFLICT (key) DO NOTHING / DO UPDATE … WHERE <expired or merge rules hold> RETURNING`, joined back to the rows in the same statement |
| `claim` | `UPDATE q SET state='leased', lease_token=gen_random_uuid(), due_at=now+lease, attempts=attempts+1, version=version+1 FROM (SELECT id FROM q WHERE <claimable> AND <where> ORDER BY due_at, priority, id LIMIT $n FOR UPDATE SKIP LOCKED) c WHERE q.id=c.id RETURNING …` |
| `settle`, `renew` | `UPDATE q SET … FROM jsonb_to_recordset($items) i WHERE q.id=i.id AND q.lease_token=i.token AND <lease unexpired> RETURNING q.id` |
| `update` | One statement: a `FOR UPDATE` CTE of up to `limit + 1` rows in id order, their count, and `UPDATE … FROM` the first `limit` when the count allows, returning merge-rule checks |
| `delete` | The same shape with `DELETE … USING` |
| `assert` | `SELECT count(*) FROM (SELECT 1 FROM obj WHERE <where> LIMIT $max+1 FOR KEY SHARE)` |
| `select`, `read` | `SELECT jsonb_build_object(…) FROM obj WHERE … LIMIT $n`; sorted or aggregating selects read through a CTE capped at 100,000 rows |
| `pause`, `resume` | `INSERT … ON CONFLICT DO UPDATE` on the space's queue-state row |

Only `claim`, `settle` and `renew` write an item's lease columns. An
`update` may set `priority`, `due_at` and `state` on items that aren't
leased, which gives bulk reschedule, reprioritise and cancel. A `settle` or
`renew` whose token is stale, or whose lease has expired, reports that item
`stale` and leaves the others alone (D6). `update` on a queue skips leased
items unless it asks to preserve or break the lease (API-002).

Each statement is one round trip. Queries are sent unprepared with text
parameters, so nothing is prepared or cached per connection. A program
with a request id adds four fixed round trips:
- writing the request record;
- the settings and savepoint;
- writing the response;
- the commit.

### 5. One program is one transaction

A program's statements run in order in one transaction, and later
statements see earlier ones' effects. A statement can reject the program:
an unmet `expect`, a merge-rule break, a key conflict under `reject`, a
`limit` overrun, or a stale lease under `require_all`. Then every
statement rolls back and the response names the statement and reason.

Request ids give idempotent replay. The space's `fw_requests` table holds
(`request_id`, `fingerprint`, `response`, `recorded_at`):

1. The transaction starts with `INSERT INTO fw_requests … ON CONFLICT DO
   NOTHING`. If the row exists, the stored response is returned. A
   different fingerprint fails with `request_id_conflict`. A concurrent
   program with the same id waits on the row lock, then replays.
2. The statements run inside a savepoint.
3. On success, the response is written to the row and the transaction
   commits. On rejection, the savepoint is rolled back, the rejection is
   written to the row, and the transaction commits. A replay therefore
   returns the same rejection (D19).

Programs run at `READ COMMITTED`. Guards lock the rows they count:
- `update` and `delete` lock them `FOR UPDATE`;
- `assert` locks them `FOR KEY SHARE`.

A count can't change under a guard before commit. An `assert` that
expects no row can't lock a row that doesn't exist yet. A concurrent
insert of that key can therefore commit beside it, and the two order as if
the guard ran first. A program retries deadlocks a bounded number of
times, then fails with `contention`.

A Snorri transition is one program:
- settle the instance-head item, patching its body and await fields;
- settle the consumed event item;
- insert effect items;
- update counter rows;
- insert its timeline row last.

### 6. Durability

| Level | Meaning |
|-------|---------|
| `fire_and_forget` | Visible; lost if the process or machine fails. |
| `tracked` | Survives a process crash and a reopen of local storage. |
| `durable` | Also survives loss of that storage. |

These are Niflheim's write modes (D14). A program may name the level it
needs, and the response reports the level it reached.

| Backend | Levels | Default | How |
|---------|--------|---------|-----|
| PostgreSQL | `fire_and_forget`, `durable` | `durable` | The server is assumed fully replicated, so a commit is `durable`. `fire_and_forget` uses `SET LOCAL synchronous_commit = off`. |
| Local engine (Turso, S3 log) | All three | `tracked` | `tracked` after the fsync; `durable` once the commit's log frame and every earlier one is in S3. |

### 7. Time is the database's clock

Every statement reads the time from `fireweed.now()`. On PostgreSQL it is a
SQL function that returns `now()`, the transaction's start time, unless the
session setting `fireweed.now` fixes an instant, as tests do. This is the
one time authority per storage (D2).

Time-driven changes (D1) are conditions on stored times:

- A pending item is claimable once `due_at <= now`.
- A leased item's `due_at` is its lease expiry. Once that passes, the
  item is claimable again if it has attempts left. If not, it counts as
  failed (D17).
- Statements that read `state` see this effective state, computed with a
  `CASE` on the stored columns.
- `sweep` writes effective states back and deletes rows past retention:
  finished items, expired table rows and old request records. It works
  in bounded batches. Results don't depend on when it runs, because every
  statement already applies these conditions.

A `claim` may pass `eligible_at`, which replaces now only when deciding
whether a pending item is due (Snorri CONTRACT-005's execution time).
Leases, attempts and retention always use now.

Revisions 4 to 8 also required request time never to decrease along
conflicting requests, and kept that time on a locked space row. Revision 9
drops that rule (question 38). Lease safety rests on tokens: a stale token
never settles or renews, whatever the clocks say.

### 8. Leases

`settle` and `renew` address an item by id and lease token (D6), and bump
its `version` (D18). Only `claim` returns a token.

Each claim charges an attempt. When a lease expires with attempts used up,
the item fails (D17). A queue may opt out with `max_attempts: unlimited`,
and its embedder then bounds redelivery itself (D30). Snorri parks an item
after K claims without a commit. `release` gives the attempt back, so a
timer that charges nothing is a `release` settle with `after` (D39).

### 9. Every statement is bounded

- Every statement that reads or writes rows has a `LIMIT`. API-006 fixes
  the caps on statements, rows and bytes per program, checked before any
  SQL runs.
- Every `where` in `select`, `update`, `delete` and `assert` must compare
  an indexed column with a value by equality, `in` or `contains`. Ranges
  don't count, because a range alone can match everything. `due_at`
  doesn't count either, because its only index is partial. The checker
  refuses a program without such a term. A selection by ids or keys is
  bounded by them, and `claim` uses the claim index.
- A `select` that sorts or aggregates reads at most 100,000 rows and
  rejects past that, so `limit` bounds rows read, not only rows returned.
- Every transaction runs under a statement timeout.
- An `update` larger than its limit either rejects, or runs as steps. Each
  step changes up to `limit` rows in id order, returns the last id it
  changed, and is its own commit. The next step adds `{gt: [id, last]}` to
  its `where`, a range scan on the `(field, id)` index.

### 10. Backends

| Backend | Status | Sharing |
|---------|--------|---------|
| PostgreSQL 18 | First, and production-supported (D44). Snorri and Cayce build against it. | Many processes per space; row locks order writers. |
| Turso on NVMe with an S3 log | The local engine (Turso-queue plan, phases L0 to L4). Gates B1 to B4 apply here. One database per space. | One writer per space. |
| SQLite | The local engine's comparison arm; in memory for consumers' tests. | One writer per space. |
| Object log × Turso (current runtime) | Deprecated once the local engine's S3 log passes (D24). | — |

**Local-engine ownership.** The embedder's control plane decides which node
writes a space and fences old writers (D15, D28). The engine opens a space
at the epoch the control plane granted. It writes S3 log keys with
`If-None-Match: *`, so a stale writer's next upload fails and no `durable`
commit is lost or forked. Fencing is per space. Clients recheck placement
on an interval and after a refusal. A space stays on the backend it starts
on (D26).

### 11. Conformance

Each executor's tests run DSL documents against a real database and check
the results and the rows left behind. They cover:
- the DDL and indexes a schema creates, and additive changes;
- every statement;
- rejection with the request record kept;
- replay;
- concurrent claims from several connections;
- lease expiry and attempt exhaustion under a fixed `fireweed.now`;
- merge rules, expectations and stepping;
- checks that a claim on a mostly sleeping queue reads a handful of index
  entries, and that claims, selections and keyed updates over thousands of
  items do no sequential scans.

The SQLite and Turso executors run the same documents.

### 12. The facade

`Fireweed` (API-005) wraps an executor and keeps helpers that build common
programs: enqueue, claim, complete and renew. RESP is deprecated (D25).
Maintenance belongs to each executor: sweeps, index builds, S3 shipping
and ownership.

## Alternatives

| Option | Evaluation |
|--------|------------|
| Keep ADR-024: one cell, storage ports as the seam | Rejected: contradicts the intent, and there is no second implementation. |
| A queue-only interface, with Snorri keeping its own tables | Rejected (D8). |
| One method per operation | Rejected (D12): every bulk change would need a new method. |
| Let consumers send SQL | Rejected (D12): unbounded work, and every backend is tied to one dialect. |
| Run requests in Rust over rows loaded from storage (revisions 5 to 8) | Rejected (D41): it loads whole objects, cannot use indexes or `SKIP LOCKED`, and serializes each space behind one lock. |
| A logical plan, plan cache, reference interpreter and differential test (revision 8) | Rejected (D41): fixed statement templates make them unnecessary. |
| weft's backend pipeline (manifest, assess, lower, emit) under Fireweed's IR | Not used in v1: each statement has one SQL shape per dialect, built directly. weft's shared crates stay available if UMF loading (question 39) or shared emission needs them. |
| A separate in-memory backend | Rejected: SQLite in memory runs the same SQL. |
| **A DSL of schema and program documents, checked against the schema and run as plain SQL over generated tables, PostgreSQL first** | **Proposed.** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Work per request scales with the rows it touches, through indexes, not with the size of a queue. |
| Positive | Many processes can claim from one PostgreSQL queue at once, with `SKIP LOCKED`. |
| Positive | Bulk changes, claims and live queries need no new methods, only DSL statements over declared fields. |
| Positive | One SQL path per dialect. Nothing in memory has to agree with the database. |
| Negative | Behaviour follows each database's SQL semantics. When the SQLite executor lands, API-006 must pin where it differs from PostgreSQL. |
| Negative | Time ordering across conflicting programs is weaker than revisions 4 to 8 required. Tokens keep leases safe. |
| Negative | Large payloads need a store outside Fireweed. |
| Negative | Records scoped to an account can't be checked in the same transaction as a campaign's effects (question 25). |
| Negative | Every selection needs an index-backed condition, which limits ad-hoc queries. |
| Negative | The revision 5 to 8 crates (`fireweed-interface`, `-engine`, `-memory`, `-postgres`, `-conformance`) are replaced, and the Turso-queue session's local engine, written against them, must move to the DSL. |
| Negative | v1 leaves out cohorts, group ordering, gates and recurrence (API-006 §Not in v1). |

### Documents this ADR changes on acceptance

| Document | Change |
|----------|--------|
| API-006 | Rewritten in revision 9 as the DSL reference and the PostgreSQL executor's rules. |
| ADR-024 | Superseded. |
| ADR-007 | The driven side is the data-plane interface. |
| ADR-008, ADR-012, ADR-013, ADR-016, ADR-017 | Rescoped to the deprecated object-log runtime (D24). |
| ADR-009, ADR-022 | The facade wraps an executor; maintenance leaves it. |
| ADR-010, ADR-011 | Restated as schema declarations. |
| ADR-014 | The Kafka consumer surface reads sequenced tables. |
| ADR-018 | Exact ensure becomes `apply_schema` with an expected version. |
| API-001 | Restated without storage or an owner node. Adds `eligible_at`, lease policies on `update`, lease expiry charging `max_attempts` (D17) and recorded rejections. Removes cohorts until question 37 is ruled. |
| API-002 | Operator actions become statements: pause is `pause`; redrive and repair are `update` with `leased: break`; purge is `delete`. |
| API-004 | Replaced by declared reads. |
| API-005 | The binding of API-006 to `Fireweed`. |
| TD-002 | Revised to §3 and §4: its tables and `SKIP LOCKED` claim, without the command log, `LeaseExpired` commands or epoch-fenced sweepers. |
| TD-006 | Deprecated with RESP. |

### Migration order

1. `fireweed-dsl` and the PostgreSQL executor `fireweed-dsl-postgres`, with
   tests against PostgreSQL. Remove the revision 5 to 8 crates.
2. Cut `Fireweed` over to the executor. Snorri and Cayce adopt the DSL on
   PostgreSQL.
3. The SQLite and Turso executor, running the same test documents, and
   gates B1 to B4.
4. Deprecate object-log (D24) and RESP (D25), and apply the document
   changes above.

## Open questions for the owner

Questions 1 to 4, 6 to 24 and 26 to 36 were resolved in revisions 7 and 8
(D13 to D40).

5. **Counters.** Cayce's per-contact send-cap counter is one row per
   account, contact and UTC hour, incremented while below the cap. A table
   row with `SET n = n + 1 WHERE n < $cap` covers it, and no hot keys are
   documented. It is keyed by account, so it falls under question 25.
   Snorri's TD-011 recorded "cap counters stay in Dragonfly", which D21
   reverses unless 7th Sense must keep reading them.
25. **Account-scoped records across campaign spaces.** Suppression and
    per-contact send caps belong to an account, but a transaction can't
    span spaces. Draft: copy suppression into every campaign space of the
    account, so each send's transaction checks it (`AddOnly` reasons
    converge). Increment send caps in an account space first, and give the
    slot back if the send fails.
31. **PostgreSQL layout.** Each space gets its own tables, or spaces share
    tables with the space id leading every key and index. Both are being
    measured (D36): claim and commit latency, index size and bloat, vacuum
    load, the cost of creating and deleting a space, and catalog cost with
    thousands of tables.
37. **Cohorts.** API-001's cohorts (claim a group whole, fail it at a
    deadline) are in no Snorri or Cayce inventory item, and no
    implementation supports them. Draft: leave them out of v1.
38. **Time ordering.** Revision 9 drops the rule that request time never
    decreases along conflicting requests (§7). Draft: accept, since tokens
    fence leases.
39. **UMF schema documents.** v1 schemas are Fireweed's own small document,
    with UMF's scalar types (§2). Draft: add a loader from a UMF document
    with Fireweed's extension (D34, D43, D44) once a consumer keeps its
    schemas in UMF.
    - **Where index choices live.** UMF keeps them out of the logical field,
      in a physical binding (CONTRACT-042), and the loader takes them from
      there into `index: true`. Fireweed's own document keeps `index` on
      the field.
    - **weft.** Its shared crates have no `json` or set types, and no `not`,
      `in` or `contains`, so v1 doesn't use them. Its PostgreSQL identifier
      and parameter helpers fit once published. Until then, names are
      validated `[a-z0-9_]` and quoted.
40. **A narrow lease table.** Every claim, settle and renew changes
    indexed columns (`state`, `due_at`), so it writes every index on the
    item, including the field indexes and the GIN index. Splitting the
    lease columns into a narrow table beside the item would keep lease
    churn off those indexes, at the cost of a join on reads. Draft: measure
    it in question 31's run before deciding.

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| PostgreSQL and SQLite disagree on an expression result | M | H | API-006 pins each difference and the SQL that removes it; the suite runs on both. |
| Every transition in a space waits on its sequenced table's counter row | M | M | Sequenced inserts come last in a program, so the row is held only for the final round trips. Question 31 measures it. |
| An absence `assert` misses a concurrent insert of the same key | L | M | The two order as if the guard ran first. Copying suppression into each campaign space (question 25) keeps checks keyed. |
| The interface grows into a general database | M | H | No joins, capped statements, index-backed selections, live data only. |
| A hot counter row serializes commits on PostgreSQL | L | M | Question 5; buckets spread the load. |
| A suppression checked outside the send's transaction lets a send through | M | H | Question 25. |
| A single writer stalls claims behind a bulk change on the local engine | H | M | Statement caps, stepped bulk changes, gate B4. |
| Thousands of per-space tables strain PostgreSQL's catalog | M | M | Question 31's measurement. The review measured 119 ms to plan a first query on a new table and 0.4 ms warm: a cost per connection and per space. |

## Validation

| Success metric | Review trigger |
|----------------|----------------|
| No backend loads an object whole or holds request state in memory | A request's work grows with queue size, or a cache appears. |
| Statements read about as many index entries as they return; the suite checks a claim on a mostly sleeping queue and selects over thousands of items | A sequential scan or a whole-index walk. |
| `fireweed-dsl` depends on no storage crate | A storage crate enters its tree. |
| Every backend passes the suite for what it declares | A backend released without it. |
| Snorri runs with no tables of its own and no unbounded in-memory state | Snorri adds a table or cache to work around a gap. |
| Cayce's UI uses declared reads only | Cayce reads an engine table. |
| The local engine meets B1 to B4 | A gate misses its floor. |

## References

- `docs/helix/02-design/contracts/API-001-native-client-interface.md`
- `docs/helix/02-design/contracts/API-006-queue-implementation-interface.md`
- `docs/helix/02-design/adr/ADR-007-hexagonal-architecture-and-two-interfaces.md`
- `docs/helix/02-design/adr/ADR-024-s3-turso-public-cell.md`
- `docs/helix/02-design/technical-designs/TD-002-postgres-native-reference-mode.md`
- `docs/helix/04-build/turso-queue-implementation-plan.md` (branch
  `claude/fireweed-sqlite-s3-logs-37f26c`)
- Snorri `CONTRACT-005-workflow-state-store-port.md`; Snorri TD-012 §3.7
- DocumentDrivenDX/umf#172; DocumentDrivenDX/weft#70
