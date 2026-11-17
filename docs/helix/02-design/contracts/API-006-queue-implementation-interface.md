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

# API-006: The Fireweed data-plane DSL

**Type**: Document language (schemas and programs) and its PostgreSQL executor
**Version**: v0 (draft, revision 9)
**Status**: draft. Proposed by ADR-025. Not normative until ADR-025 is accepted.
**Related**: API-001, API-002, ADR-025, TD-002

Revision 9 replaces revisions 3 to 8. A consumer works with Fireweed in two
kinds of documents:
- a **schema** declares a space's queues, tables and reads, and the
  executor creates real tables and indexes from it;
- a **program** is a list of typed statements that run against those tables
  as one transaction.

The PostgreSQL executor turns each statement into plain SQL. It keeps no
state in the process: everything it needs to run a program is in the
database. The Rust types are in the crate `fireweed-dsl`, and the executor
is `fireweed-dsl-postgres`.

## Documents

Both kinds of document are YAML or JSON (ADR-025 D37), read into the same
types. Unknown keys are refused.

**Names.** Space ids are any non-empty text. Object, field, parameter and
read names match `[a-z][a-z0-9_]{0,47}`.

**Types.**

| Type | Values | PostgreSQL column |
|------|--------|-------------------|
| `text` | string | `text COLLATE "C"` |
| `int` | 64-bit integer | `bigint` |
| `float` | finite 64-bit float | `double precision` |
| `bool` | true or false | `boolean` |
| `timestamp` | RFC 3339 string, stored to the microsecond | `timestamptz` |
| `json` | any JSON value | `jsonb` |
| `text_set` | array of strings, kept sorted and without duplicates | `text[]` |

Every declared field may be null.

**Durations** are a string with a unit (`250ms`, `30s`, `5m`, `2h`, `7d`) or
an integer number of milliseconds.

## Schema documents

```yaml
version: 2                 # increases with every change
request_retention: 1d      # how long request ids are remembered (default 1d)
queues:
  instances:
    max_attempts: unlimited  # or an integer; default 10
    retention: 31d           # finished items are deleted this long after they finish (default 7d)
    priority: asc            # asc (default) or desc: breaks ties among items due at the same instant
    fields:
      campaign: {type: text, index: true}
      awaits:   {type: text_set, index: true}
      step:     {type: int}
tables:
  suppression:
    key: [account, channel, address]
    fields:
      account:   {type: text}
      channel:   {type: text}
      address:   {type: text}
      reasons:   {type: text_set, merge: add_only}
      lifted_at: {type: timestamp}
  chunks:
    key: [run, chunk]
    fields:
      run:    {type: text}
      chunk:  {type: int}
      cursor: {type: int, merge: non_decreasing}
  timeline:
    sequenced: true
    expire_after_write: 31d
    fields:
      instance: {type: int, index: true}
      event:    {type: json}
reads:
  - name: awaiting
    version: 1
    params: {wait: text}
    select:
      from: instances
      where: {contains: [awaits, {param: wait}]}
      fields: [id, key, campaign]
      limit: 500
```

### Queues

A queue holds work items. Every item has these system columns, which
programs can read. Only `claim`, `settle` and `renew` change the lease
columns; an `update` may also set `priority`, `due_at` and `state` on items
that aren't leased:

| Column | Meaning |
|--------|---------|
| `id` | Assigned at insert; never reused in the queue. |
| `key` | Optional client key, unique in the queue while the item exists. |
| `state` | `pending`, `leased`, `complete` or `failed` (see Time). |
| `priority` | Integer; orders items due at the same instant. |
| `due_at` | When the item next needs attention. For a pending item, when it becomes claimable. For a leased item, when its lease expires. |
| `attempts` | Claims charged so far. |
| `version` | Increases on every change to the item. |
| `lease_owner` | The worker holding the lease. |
| `created_at`, `updated_at`, `finished_at` | Times. |
| `body` | Optional JSON document. |

Declared fields are further columns. `index: true` creates an index on a
field (a GIN index for a `text_set`).

The executor creates for each queue:
- the table;
- a unique index on `key`;
- the claim index `(due_at, priority, id) WHERE state IN ('pending', 'leased')`,
  which leads with due time (ADR-025 D39) and is the claim order;
- an index on `finished_at` for retention;
- an index on `due_at` over leased items, for the sweep;
- one index per indexed field: `(field, id)` for a btree, so stepping
  through a field's matches with one `eq` is a range scan.

A queue table is created with `fillfactor = 80`. That keeps updates of
unindexed fields on their page. Claims, settles and renews change indexed
columns, so they write every index (ADR-025 question 40).

`max_attempts: unlimited` turns off the store's redelivery bound
(ADR-025 D30).

### Tables

A table holds rows. Every row has `id` (assigned), `version`,
`created_at`, `updated_at` and, with `expire_after_write`, `expires_at`.
`key` lists the fields that identify a row; they must be non-null and get
a unique index. A **sequenced** table has no `key`: `id` is assigned from a
counter row in the same transaction, so ids are gapless and in commit
order (ADR-025 D16). Rows of a sequenced table are never updated.

`merge` on a field (ADR-025 D20):
- `add_only` (a `text_set`): an update may add values, never remove them;
- `non_decreasing` (an `int`, `float` or `timestamp`): an update may not
  lower it.

An update that would break a merge rule rejects the program, unless it
sets `override_merge: true`.

### Reads

A declared read is a named, versioned `select` (below) with typed
parameters (ADR-025 D10). A name and version, once declared, never change;
a new version is a new entry.

### Changing a schema

`apply_schema` takes a schema and the version the caller expects to
replace (none for a new space). Changes may only add (ADR-025 D22). They
can add:
- queues and tables, with any indexes;
- unindexed fields on existing queues and tables;
- reads and new read versions.

An existing field never changes, and a field added to a live object can't
be indexed: building an index blocks the object's writes.

They can also raise `request_retention` and a queue's `retention`. Anything
else is refused. A schema identical to the stored one is accepted
unchanged.

## Programs

```yaml
request_id: transition-r1-7      # required if any statement writes
durability: durable              # optional: fire_and_forget or durable
params:
  inst: 41
  token: "6f1c…"
statements:
  - settle:
      queue: instances
      outcome: complete
      items: [{id: {param: inst}, token: {param: token}}]
      set: {step: 3}
      body:
        - {set: /node, value: "send_welcome"}
        - {append: /audit, value: {at: "2026-11-16T10:00:00Z"}, keep_last: 16}
      require_all: true
  - insert:
      into: effects
      rows: [{key: "r1-send-1", body: {template: "welcome"}}]
      on_conflict: reject
  - update:
      target: chunks
      keys: [{run: "r1", chunk: 3}]
      where: {eq: [version, 7]}
      set: {cursor: 1200}
      expect: {min: 1, max: 1}
```

Statements run in order, and each sees the effects of those before it.
The response has one result per statement.

### Operands

| Form | Meaning |
|------|---------|
| `"name"` (first operand of a comparison) | A column of the target object. |
| a literal (second operand) | A value of the other operand's type. |
| `{field: name}` | A column, anywhere. |
| `{param: name}` | A program parameter. |
| `{value: v}` | A literal, written out where a bare object would read as an operand (a `json` value such as `{param: 1}`). |
| `{now: {}}` or `{now: "-5m"}` | The request time, optionally shifted. |
| `{path: /a/b, type: text}` | A value inside a queue item's body, read as `type`. |

There are no implicit casts. An operand whose type does not match is
refused when the program is checked.

### Predicates (`where`)

`{and: [p, …]}`, `{or: [p, …]}`, `{not: p}`;
`{eq|ne|lt|le|gt|ge: [a, b]}`; `{in: [a, [v, …]]}`;
`{contains: [set_field, v]}`; `{is_null: a}`; `{not_null: a}`;
`{exists: {in: object, match: {…}, where: …}}` (below).

### Lookups (`exists`)

```yaml
where:
  not:
    exists:
      in: suppression                    # another queue or table in the same space
      match: {address: {field: email}}   # its field = a value or the outer row's field
      where: {is_null: lifted_at}        # optional, over the other object
```

`exists` is true when a row of `in` matches every `match` pair and passes
its own `where`. A `match` value is a literal, a `{param}` or a `{field}`
of the statement's own object. One `match` field must be indexed in the
other object. A lookup tests existence only: it returns no columns, and
there are no joins (ADR-025 §14).

A claim's `where` made only of `not exists` exclusions needs no access
term. Its claim reads past the few items it excludes.

A comparison with a null is false, as in SQL.

**Index rule.** A `where` in `select`, `update`, `delete` and `assert` must
contain, directly or inside a top-level `and`, an **access term**: an
`eq`, `in` or `contains` between an indexed column and a value (a literal,
parameter or `now`). The indexed columns are:
- `id`, and `key` on a queue;
- the first key field of a table;
- an `index: true` field.

Ranges are not access terms, because a range alone can match the whole
object. They can still narrow an access term, as in a stepped update. Nor
is `due_at`, whose only index is the partial claim index. Selecting by
`ids` or `keys` needs no `where`. A program that breaks the rule is
refused.

A `select` with `order_by` or `aggregate` reads at most 100,000 matching
rows; past that it rejects with `limit_exceeded`. Without them, its `limit`
bounds the rows read.

### Assignments (`set`)

A map from declared field to a value: a literal, `null`, an operand, or
`{add: [a, b]}`, `{sub: [a, b]}`, `{coalesce: [a, b]}`,
`{set_add: [values]}`, `{set_remove: [values]}`. System columns can't be
set, except that an `update` may set a queue item's `priority`, `due_at`
and `state` (`pending`, `complete` or `failed`) when it doesn't preserve
leases.

### Body patches (`body`, queues only)

A list applied in order:

| Patch | Effect |
|-------|--------|
| `{set: /a/b, value: v}` | Sets the value at the path, creating parents. |
| `{remove: /a/b}` | Removes it. |
| `{append: /a, value: v, keep_last: n}` | Appends to the array at the path, keeping the last `n` elements. |
| `{replace: v}` | Replaces the whole body. |

## Statements

Every statement names one object. "Rejects" means the whole program rolls
back and the response reports the statement and reason (see Transactions).

### `insert`

```yaml
insert:
  into: <queue or table>
  rows: [ {…}, … ]
  on_conflict: skip        # skip (default), reject, or update (tables only)
```

- **Queue rows** may have `key`, `priority` (default 0), `due` (a timestamp)
  or `after` (a duration from now; default due now), `body`, and declared
  fields.
- **Table rows** have declared fields, and must include the key fields.
- **Keys.** A key may appear only once in one `insert`.
- **Conflicts.** A row whose key exists is skipped and reported `exists`.
  With `reject`, the program rejects with `key_conflict`. With `update` on
  a table, the row's given fields are set under merge rules, and it is
  reported `updated`. Every row must give the same fields, and a
  merge-rule break rejects with `merge_violation`.
- **Expired rows.** A finished item past its queue's `retention`, or a table
  row past `expires_at`, gives up its key. The new row replaces it.
- **Sequenced tables.** An insert into a sequenced table holds the table's
  counter row until commit. So it must come after every other write in its
  program.

Result: `[{id, key?, status: inserted | exists | updated}]` in row order.

### `claim`

```yaml
claim: {from: <queue>, limit: 10, lease: 30s, worker: w1, where: <pred>, eligible_at: <timestamp>}
```

Leases up to `limit` claimable items in claim order: `due_at`, then
priority, then `id`. This is the claim index's order, so a claim reads about
`limit` index entries whether most items are due or most are sleeping.
Priority breaks ties among items due at the same instant (ADR-025 D42). Each one becomes `leased`, gets a new random lease
token, `due_at` = now + `lease`, one more attempt, and a new version.
Items that other transactions hold are skipped, not waited for. `where`
filters further and follows the index rule, so a rare value narrows
through its index. Exclusions written as `not exists` are allowed without
an access term (§Lookups). `eligible_at` replaces now only when
testing whether a pending item is due (Snorri CONTRACT-005); leases use
now. A paused queue returns no items.

Result: `[{id, key, token, attempts, priority, version, body, <fields>}]`.

### `settle`

```yaml
settle: {queue: <queue>, outcome: retry, items: [{id, token}, …], after: 1m, set: {…}, body: […], require_all: false}
```

Each item must be leased with that token and an unexpired lease;
otherwise it is reported `stale` and left alone (ADR-025 D6). With
`require_all: true`, a stale item rejects the program with `stale_lease`.
The lease ends and the item's version increases.

| `outcome` | Result state |
|-----------|--------------|
| `complete` | `complete`, `finished_at` = now |
| `fail` | `failed`, `finished_at` = now |
| `retry` | `pending`, due after `after` (default now); `failed` instead if attempts are used up |
| `release` | `pending`, due after `after`, with the claim's attempt given back (a timer that charges nothing, ADR-025 D39) |

`set` and `body` apply in the same step. Result: `[{id, status: ok | stale}]`.

### `renew`

```yaml
renew: {queue: <queue>, items: [{id, token}, …], lease: 30s, require_all: false}
```

Extends unexpired leases to now + `lease`, and increases the version
(ADR-025 D18). Result as above.

### `update`

```yaml
update:
  target: <queue or table>
  where: <pred>                 # and/or one of:
  ids: [1, 2]                   #   ids
  keys: [{k1: v, k2: v}]        #   table keys, or queue client keys as strings
  set: {…}
  body: […]                     # queues only
  leased: skip                  # queues: skip (default), preserve, or break
  limit: 1000                   # default 1000
  on_limit: reject              # reject (default) or step
  expect: {min: 1, max: 1}      # optional; on_unmet: reject (default) or report
  override_merge: false
```

Matches the rows, locks them in `id` order, and applies `set` and `body`.
On queues, leased items with unexpired leases are:
- skipped (`skip`);
- included, keeping the lease (`preserve`);
- included and returned to `pending` with the lease cleared (`break`, for
  operator repair; API-002).

If more than `limit` rows match:
- `reject` rejects with `limit_exceeded`;
- `step` changes the first `limit` rows and returns `more: true` and
  `last`, the last id it changed. The caller sends the next step as a new
  program, adding `{gt: [id, last]}` to its `where`.

The whole update is one SQL statement. It locks up to `limit + 1` matching
rows in id order, counts them, and updates the first `limit` only if the
count passes `limit` and `expect`.

`expect` bounds the number of matched rows; outside it, the program rejects
with `expect_unmet`. This gives compare-and-set (`where: {eq: [version,
7]}`, `expect: {min: 1, max: 1}`) and conditional increments.

**`on_unmet`.** An `expect` may say `on_unmet: report`. When the count
misses, that statement changes nothing and its result says `met: false`,
and the rest of the program runs and commits. This gives a batch of
guards in one program a result per guard. With the default `reject`, a
miss rejects the whole program. `assert` and `delete` take it too. With
an `expect`, a result carries `met: true` or `met: false`.

Result: `{matched, met?, more?, last?}`.

### `delete`

```yaml
delete: {target: …, where | ids | keys, limit, expect}
```

Deletes matching rows (queue items in any state). Result: `{matched, met?}`.

### `assert`

```yaml
assert: {target: …, where | ids | keys, expect: {min: 0, max: 0}}
```

Counts matching rows and rejects with `expect_unmet` outside `expect`,
or reports `met: false` with `on_unmet: report`. Writes nothing (ADR-025
D20). Result: `{matched, met}`.

### `select`

```yaml
select:
  from: <queue or table>
  where: <pred>
  fields: [id, key, state, campaign]          # or:
  aggregate: {n: count, total: {sum: amount}, p95: {percentile: [latency, 0.95]}}
  group_by: [campaign]
  order_by: [{field: due_at, desc: false}]
  limit: 100                                   # required, at most 10000
```

Aggregates: `count`, `{sum|min|max|avg: field}`, and
`{percentile: [field, p]}`, which is exact (`percentile_disc`). Result: a
list of row objects.

### `read`

```yaml
read: {name: awaiting, version: 1, params: {wait: "evt:42"}}
```

Runs a declared read. Result: a list of row objects.

### `pause`, `resume`

```yaml
pause: {queue: <queue>}
```

A paused queue's claims return nothing. Result: `{}`.

## Transactions and request ids

A program runs in one database transaction:
- **Isolation.** Programs run at `READ COMMITTED`.
  - Guards lock what they count: `update` and `delete` lock their matched
    rows `FOR UPDATE`, and `assert` locks them `FOR SHARE`. So no counted
    row changes or leaves before commit.
  - A guard can't lock rows that don't exist yet. A concurrent insert can
    therefore raise a count past any `max`, and the two order as if the
    guard ran first.
  - A program retries a deadlock up to 5 times, then fails with
    `contention`.
- **Request records.** A program with a `request_id` is recorded with a
  fingerprint (SHA-256 of the program's canonical JSON) and its response.
  - A program whose id has a record returns the recorded response, if its
    fingerprint matches, without running again. Otherwise it fails with
    `request_id_conflict`.
  - A program running concurrently with the same id waits for the first
    to commit, under the statement timeout, then returns its record.
  - A claim's items are recorded as ids and tokens. A replay re-reads
    their current fields and returns them with the tokens the claim
    granted.
  - Records older than `request_retention` are forgotten.
- **Rejections.** A rejection rolls back every statement but keeps the
  request record, so a replay returns the same rejection (ADR-025 D19).
  Rejection reasons: `key_conflict`, `stale_lease`, `limit_exceeded`,
  `expect_unmet` and `merge_violation`.

Response:

```yaml
request_id: transition-r1-7
status: committed          # or rejected
rejected: {statement: 2, reason: expect_unmet}   # when rejected
results: [ … ]             # empty when rejected
durability: durable
replayed: false
```

Errors are not recorded and change nothing:
- `invalid` (the program doesn't check against the schema; carries the
  reason);
- `space_not_found`, `schema_conflict`, `request_id_conflict` and
  `contention`;
- `unavailable` (database errors).

## Time

The request time is the database's: `fireweed.now()`, which returns the
transaction's start time (`now()`). Tests set the session setting
`fireweed.now` to an instant to fix it. There is one time authority per
database (ADR-025 D2).

Time-driven changes are conditions on stored times, so no caller drives a
clock (ADR-025 D1):

- A `pending` item is claimable when `due_at <= now`.
- A `leased` item whose `due_at` (its lease expiry) is past is claimable
  again if it has attempts left. If it has none, it counts as `failed`
  (ADR-025 D17).
- Every statement that reads `state` sees this effective state.
  `fireweed.sweep(space)` writes it back and deletes three things:
  - finished items past `retention`;
  - table rows past `expires_at`;
  - request records past `request_retention`.
  It works in bounded batches. Results don't depend on when it runs:
  every statement already applies these conditions.

## Durability

| Level | PostgreSQL |
|-------|------------|
| `durable` (default) | A normal commit. The server is assumed fully replicated (ADR-025 D14). |
| `fire_and_forget` | `SET LOCAL synchronous_commit = off`. |

The response reports the level. ADR-025 §Durability gives the local
engine's levels.

## Limits

| Limit | Value |
|-------|-------|
| Statements per program | 64 |
| Rows per `insert`, items per settle or `renew` | 1,000 |
| `claim` limit | 1,000 |
| `update` and `delete` limit | 10,000 |
| `select` limit | 10,000 |
| Program size | 8 MiB |
| Rows read by a sorted or aggregating `select` | 100,000 |
| Statement timeout | 5 s |

## The PostgreSQL executor

The target is PostgreSQL 18 (ADR-025 D44).

The executor is a set of functions over a PostgreSQL connection:
- `apply_schema(tenant, space, schema, expected_version)`;
- `get_schema(tenant, space)`;
- `execute(tenant, space, program)`;
- `sweep(tenant, space, batch)`.

They hold no state between calls. Each call reads what it needs from the
database.

**Layout.**
- **Catalog.** `fireweed.spaces` maps (tenant, space) to the PostgreSQL
  schema holding the space and the stored schema document.
- **One PostgreSQL schema per space.** It holds:
  - one table per queue (`q_<name>`) and per table (`t_<name>`), with
    declared fields as columns named `f_<name>`;
  - `fw_requests`;
  - `fw_queue_state` (paused flags);
  - `fw_sequences` (sequenced-table counters).

  Whether spaces should instead share tables is ADR-025 question 31.

**Running a program.**
1. BEGIN.
2. In one pipelined flight, set the timeouts, call
   `fireweed.open_space(tenant, space, request_id, fingerprint)`, which
   reads the schema row and writes the request record, and set the
   savepoint.
3. Check the program against the schema in Rust.
4. Run each statement as one SQL statement.
5. Write the response, and commit.

Queries are sent unprepared, with every parameter typed `text`, so each
is one round trip and nothing is cached per connection. `apply_schema`
sets a 5 s lock timeout, so an `ALTER TABLE` never leaves claims queued
behind an unbounded lock wait.

Each statement's SQL:

| Statement | SQL |
|-----------|-----|
| `insert` | `INSERT … SELECT … FROM jsonb_to_recordset($rows) … ON CONFLICT … DO NOTHING / DO UPDATE … WHERE <expired or merge rules hold> RETURNING`, joined back to the input rows |
| `claim` | `UPDATE q SET … FROM (SELECT id FROM q WHERE <claimable> AND <where> ORDER BY due_at, priority, id LIMIT $n FOR UPDATE SKIP LOCKED) c WHERE q.id = c.id RETURNING …` |
| `settle`, `renew` | `UPDATE q SET … FROM jsonb_to_recordset($items) i WHERE q.id = i.id AND q.lease_token = i.token AND <unexpired> RETURNING q.id` |
| `update` | `WITH m AS (SELECT id, <old merge fields> … ORDER BY id LIMIT $limit + 1 FOR UPDATE), total AS (…), u AS (UPDATE … FROM <first $limit of m> WHERE <count passes> RETURNING <merge checks>) SELECT …` |
| `delete` | The same shape with `DELETE … USING` |
| `assert` | `SELECT count(*) FROM (SELECT 1 … LIMIT $max + 1 FOR SHARE)` |
| `select`, `read` | `SELECT jsonb_build_object(…) FROM … WHERE … LIMIT $n`; with `order_by` or `aggregate`, over a CTE capped at 100,001 rows |

Literals and parameters are always bound, never spliced into SQL text.
Names in SQL are validated object and field names, and quoted.

## Consumer mapping

### Snorri (TD-012 revision 6 §3.7)

| # | State | In the DSL |
|---|-------|-----------|
| 1 | Instance body | Item in an `instances` queue with a `json` body. A transition is one program: a `settle` of the head item with `body` patches and `set`. |
| 2 | Consumed inputs | Items in an `inputs` queue, settled `complete` in the transition's program. |
| 3 | Waits and routing | `awaits: text_set, index: true`; changed with `set_add` and `set_remove`; routed by a read with `contains`. |
| 4 | Effects | Items in an `effects` queue, inserted in the transition's program, which uses `durability: durable`. |
| 5 | Handler idempotency | Table keyed by idempotency key, with `expire_after_write`. |
| 6 | External events, 31-day dedupe | Items keyed by provider event id in a queue with `retention: 31d`; `insert` with `on_conflict: skip`. |
| 7 | Timeline | A sequenced table, inserted last in each transition's program. Full history goes to Niflheim. |
| 8 | Definitions (up to 256 KiB, D31) | Table row keyed by digest, with a `text` field. |
| 9 | Admission chunk | Table row keyed by run and chunk: `insert` with `on_conflict: reject`, then `update` by keys with `where: {eq: [version, v]}` and `expect: {min: 1, max: 1}`. |
| 10 | Send-rate counters | Not Fireweed's (ADR-025 D49). |
| 11 | Re-entry across runs (D29) | Table in the workflow's space, keyed by subject, with `non_decreasing` timestamps, updated with `expect`. Admission and the re-entry check share one program, so one transaction (ADR-025 D51). |

Snorri's queues declare `max_attempts: unlimited` (D30). A Snorri space is one
workflow with all its runs (ADR-025 D50). Its queues and tables carry an
indexed `run` field, so run-scoped selects, cancels and retirement meet the
index rule and step through `(run, id)`.

### Cayce (draft)

| State | In the DSL |
|-------|-----------|
| Per-recipient lifecycle | Queue items with indexed fields (campaign, step, domain, status) |
| Suppression | A copy in each workflow space (ADR-025 D54): a `suppression` table (`reasons` add_only, `lifted_at`, `seq` non_decreasing) and a `host_cursors` table, kept current by Cayce's background propagation (ADR-025 §14). A send's claim excludes suppressed contacts with `not exists`, or a send's program runs `assert` on the address with `expect: {max: 0}`. |
| Dedupe by resolved entity | Cayce stamps an indexed `entity` field at admission. Guard: `not exists` an item with the same entity in a done state (ADR-025 §14). |
| Caps and rate windows | Not Fireweed's (ADR-025 D49) |
| Delivery and engagement events | Queue items keyed by provider id; a sequenced table for timelines |
| UI counts and lists | Declared reads |

## Not in v1

These are left out of v1:
- cohorts (ADR-025 question 37);
- group keys and per-group ordering;
- dynamic gates and metadata blockers;
- recurrence beyond `release` with `after`;
- joins;
- maintained counts by state (an aggregate over a whole queue needs an
  index-backed condition).

Each can be added as a statement or schema option when a consumer needs
it.

## Conformance

The executor's tests run programs against PostgreSQL and check results and
the resulting rows. They cover:
- the DDL each schema creates, its indexes, and additive changes;
- every statement;
- rejections rolling back while keeping the request record;
- replay and fingerprint conflicts;
- concurrent claims from several connections never leasing an item twice;
- lease expiry, reclaim and attempt exhaustion under a fixed `fireweed.now`;
- merge rules, expectations and stepping;
- that claims, indexed selects and keyed updates over 5,000 items add
  nothing to PostgreSQL's sequential-scan count for the table;
- that a claim on a queue of 5,000 sleeping items and 3 due ones reads
  fewer than 50 index entries;
- that a filtered claim, a grouped count and a sweep over 5,000 items do no
  sequential scans;
- that an update leaves no matched item holding a lapsed lease;
- that an insert waiting on a same-key insert reports the winner's id;
- that a replayed claim re-reads its items, and its record holds no bodies.
