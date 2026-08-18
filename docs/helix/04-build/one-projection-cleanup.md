# Cleanup: one log, one projection

**Status**: plan (2026-08-18), revised after review. Not implemented.

This document **supersedes** the “planner map as produce-path authority”
section in `ss-objectlog-turso-memory-goal.md`. That map was a locking
workaround. It is not the product.

The object log is the durable record. The **SQL projection** is the
queryable picture of that log, including payloads. There is no second
in-process item store.

This pattern is **shared by every SQL projection**: SQLite, Turso, and
Postgres. Same schema ideas, same Class L / Class S contract, same
claim/lease transaction, same outbox, same apply rules — implemented
once in `fireweed-relational` and executed by each driver. Driver
differences are only how a write transaction locks (SQLite/Turso
`IMMEDIATE`, Postgres `FOR UPDATE SKIP LOCKED`).

The **in-memory projection** is the exception: it *is* a map. It does
not grow SQL side tables or pretend to be a database. Every other
projection must not grow an in-memory map to “support” planning, claim
exclusivity, tokens, unapplied keys, or replay. If the projection
cannot answer it, fix the projection (schema + SQL), do not cache
around it.

## What is wrong

Claim was split into “choose” (`SELECT` pending) and “lease” (log command,
applied later). Between those steps the rows stay pending, so two workers
get the same items. Then we waited for Turso to apply the claim we just
wrote before returning bodies that were already in the row.

A planner map papered over writer-lock contention. Two authorities, a
recovery twin, and delivery that is still apply-bound. Terrible.

Choosing work without leasing it is not a claim.

## Two classes of mutation

Not every op is the same.

### Class L — log first, apply in the background

Push, `BatchUpdate`, complete/fail/retry/release. No “pick from a shared
pending set.”

```
read the SQL projection (reader) if you need current rows
append one command to the object log
ack (if the client asked for log durability)
apply later on the projection writer
```

### Class S — lease in the SQL projection, then record it on the log

Claim, expired-lease reclaim, reassign, and whole-group / whole-cohort
claim: anything that **selects from a contended set** and must exclude
others immediately. Choose and lease are one projection transaction.
After that transaction **commits**, produce a log record of what was
claimed.

Do not hold the projection writer open across the object PUT (Turso:
apply re-takes the same mutex and deadlocks; it also starves apply for
packer linger).

```
BEGIN write txn on the SQL projection
  -- optional: apply this queue's unapplied log tail so the Claim
  -- is the next position (live == rebuild)
  SELECT next due rows + payloads
    (Postgres: FOR UPDATE SKIP LOCKED)
  UPDATE those rows to leased (token hash, expiry, version+1, attempt+1)
  INSERT the full Claim envelope into fireweed_claim_outbox
COMMIT
drop the writer
append that envelope to the object log
  if PUT is not known-durable: retry the same outbox row until it is
  then DELETE the outbox row
  do not unlease — the lease already committed
return the rows read in the txn, with lease fields from the UPDATE
```

Postgres already picks and locks in one statement (`FOR UPDATE SKIP
LOCKED`). SQLite and Turso do the same contract with `IMMEDIATE` +
`UPDATE`. The SQL and outbox live in `fireweed-relational`. Adapters
only supply the transaction and lock dialect.

Exclusivity is the committed projection lease (row lock / `IMMEDIATE`).
The next claim’s `SELECT` cannot see those rows. On SQLite/Turso two
claims cannot both hold the write txn; the second waits, then sees
leased rows. On Postgres, `SKIP LOCKED` skips rows the first txn still
holds.

The log is the durable history of that lease. After `COMMIT`, the PUT
is **required**, not optional. Unknown append (produced, then error) is
not “failed, give up”: treat as unknown, retry the same envelope, and
do not run another Class S `SELECT` on that queue until the log and
the projection agree (outbox empty or those `Claim`s applied).

Crash rules:

- Crash or error **before** projection `COMMIT`: nothing leased, nothing
  logged. Client error. Retry is a new claim.
- Projection `COMMIT` succeeded, PUT not yet durable: rows are leased
  and the envelope is in `fireweed_claim_outbox`. Restart: **drain the
  outbox** (same envelopes) before serving that queue. Do not scan
  every leased row — applied claims have no outbox row. Until the
  outbox is empty (or those `Claim`s are on the log), do not Class S
  claim again on that queue.
- PUT durable, client never heard: retry with request-id / token
  replay, or a new `SELECT` that no longer sees those rows.

Apply of `Claim` is **idempotent**: already leased with this token →
no-op; leased by another token → keep the first, do not fail the pack.
Fused claim+complete must accept a row already `Leased` with this token
and must not poison on row-count mismatch.

The item row keeps `lease_token_hash` only. The **outbox** holds the
full envelope (ids, token, expiry, request-id, fingerprint, worker,
unit, cohort id) for reconcile. Do not put bearer tokens on every
leased row. Same outbox table on SQLite, Turso, and Postgres.

Cancellation: dropping the future mid-txn must roll back; dropping
after `COMMIT` must still finish the log PUT (or leave the queue
unserved until reconcile).

Complete is **Class L**, not Class S. The worker already holds the lease.
`Finalize` is one log command. Do not wait for apply. Validate token and
version with a **reader** `SELECT` (the Class S commit already made the
row `Leased`). Carry token/version on the finalize command so replay
cannot complete an item whose lease has since been released or expired.

`BatchUpdate` stays banned on leased items. After Class S `COMMIT`, a
reader sees `Leased` and planning refuses. Apply of an update that was
acked while the row was still pending, then races a claim, must **skip
the row** (not fail the pack). That is a lost update of an item that
became leased — the client already got an ack; document it, or promote
update-of-pending to Class S if we cannot accept that. Do not convert
the race into shard poison.

## What to delete

- `crates/fireweed/src/planner_map.rs`
- `crates/fireweed/src/map_push_planner.rs`
- `PlannerMap`, `MapPushPlanner`, `Reservation`, `PlannedReservation`
- `dispatch_claim_from_map`, `dispatch_finalize_from_map`,
  `planner_update_snapshot`, `reserve_planned_updates`, `finish_push`,
  `finish_planned`, `apply_recovered` into a map
- Per-claim `catch_up_projection` after append
- `validate_push` / `finalize_validate` **write** transactions that exist
  only to roll back

Delete process-local `live_tokens` / `live_tokens_by_consumer` on
**Turso and SQLite** (they are in-memory maps used as token authority
today). Claim response comes from the Class S transaction. Pending /
renew / finalize read lease identity from SQL (`lease_token_hash` plus
whatever column the shared schema needs for consumer listing — not a
process `HashMap`). Postgres must not grow an equivalent map.

`rg` gate: no `planner_map`, `MapPushPlanner`, `dispatch_claim_from_map`
under `crates/fireweed`; no `live_tokens` as serving authority under
`fireweed-sqlite` or `fireweed-turso`. Shared
`impl_turso_product_ports!` stubs on `AtomicTursoBackend` go with them.

Shared claim/outbox/apply SQL lands in `fireweed-relational`. Adapter
crates execute it; they do not each invent a cache.

This plan supersedes the map bullets in
`ss-objectlog-turso-memory-goal.md`.

## What to change

### 1. Reader vs writer (Class L)

- **Writer**: apply, schema/open, and Class S transactions (claim).
- **Reader**: a **second** connection (`database.connect()` / extra
  rusqlite or postgres client) for `SELECT` by key/id,
  pause, idempotency lookup, `live_item`, peek, pending, metrics on the
  ack-after-log cell (applied state only).
- `validate_push` is a read: do these keys exist? No `IMMEDIATE`, no
  rollback, no index/cohort writes that get thrown away.

**Gate before anything else:** probe Turso 0.7 — hold an `IMMEDIATE`
writer txn on connection A, `SELECT` on connection B from
`database.connect()`. Record whether B returns pre-txn rows without
waiting for A. If it cannot, Class L plan-reads wait for the current
writer txn to commit (bounded). They still must not start their own
write txn. Do not call a lock on `writer` a “reader.”

Convert every `writer.lock()` used for a `SELECT` (including
`recovery_high_water` on the Strict wait path). One shared reader
connection is enough; no connection per queue.

### 2. Class S claim

Short writer txn, then the log — never the reverse, never an open txn
across `packed_append`.

`SELECT` includes payloads. `UPDATE` writes lease hash/expiry/version.
The **outbox** in the same txn holds the full `Claim` envelope.
Response uses the updated row fields. Do not `render_claimed` after
catch-up.

After `COMMIT`, PUT that envelope. Retry until durable, then delete
the outbox row. Reconcile on open: drain the outbox.

Classify PUT failure: I/O → retry; `EpochFenced` / backpressure /
poison → queue claim-blocked, outbox remains, named repair. Gate
`reserve()` **before** `BEGIN` so those reject with nothing committed.

Before the `SELECT`, apply this queue’s unapplied log tail so the
`Claim` is the next position (live == rebuild).

`LeaseExpired` / reassign apply must carry token+version and no-op on
mismatch (never poison). They are Class S if they select a contended
set (expired leases).

Apply of `Claim` and fused claim+complete: never poison on “already
leased” or unexpected row count.

Test: two `claim(100)` on 1,000 items, apply paused, disjoint ids;
kill after Turso commit before PUT, reopen, log contains the claim,
no second lease of those ids.

### 3. Unapplied Class L — in the projection, not a process map

Ack-after-log push: a second push of the same `client_item_key` can
`SELECT` a miss while the first is unapplied. Both in the log → unique
index on apply can poison a pack.

Do **not** keep a process-local key set. Either:

- **Apply this queue’s unapplied tail** before a push/claim `SELECT`
  (same idea as Class S optional prefix), so the SQL unique index and
  `fireweed_request_idempotency` are the truth, or
- Record reserved keys / request-ids in a **SQL table** in the same
  database, written in a short txn, same schema on SQLite / Turso /
  Postgres.

Prefer apply-the-tail: no extra table, live == rebuild. Bound by
existing apply-lag limits so the wait cannot grow without bound.

Claim exclusivity is Class S (row lease), not this.

### Alignment (SQLite, Turso, Postgres)

| | In-memory projection | SQLite / Turso / Postgres |
|---|---|---|
| What is the item store | The map | The SQL tables |
| Claim | Mutate the map, then log | Class S txn + outbox + log |
| Tokens / pending-by-consumer | Map fields | SQL columns / indexes |
| Duplicate push while apply lags | Map already has the key | Apply tail or SQL reservation table |
| Side HashMaps for “support” | N/A (it is the map) | **Forbidden** |

Postgres `FOR UPDATE SKIP LOCKED` is the lock dialect, not a different
product. Shared tests: two claims disjoint; kill after projection
commit before log PUT, reopen, outbox drains, one `Claim` on the log;
rebuild equals live after claim+complete. Run on all three SQL
adapters.

### 4. Apply

- Push: insert the command’s rows. Duplicate active key: skip the row,
  do not fail the pack.
- Update: only columns the command changes. Refuse `Leased`.
- Claim: lease those ids if still `Pending`; if already this token,
  no-op; if another token, leave it. No group-member scan on the
  delivery path. Group-summary: incremental, or skip on item-level
  claim and refresh on grouped read.
- Complete/fail: mark terminal. No group-summary refresh.

One runtime hop per packed apply object, not per statement. That is its
own slice (writer), not a store.

### 5. Catch-up

Allowed: rebuild/reopen; client asked for apply-strict (`open()` default
`Strict`). Capacity cell stays ack-after-log.

Not allowed: after claim, update, or push on the ack-after-log cell to
“see” the command you just appended.

`metrics` / `live_item` / `peek` on that cell are **applied state**.
Correctness checks that need exact complete counts must drain apply
first or use Strict. Say so in the harness; do not sneak catch-up back
into claim.

## Order of work

Each step must leave the product exclusive and unpoisoned. No commit
where claim is a racy `SELECT` and the map is already gone.

1. **Reader probe** (recorded). Second connection `SELECT` during a
   writer txn; also: WAL truncate with a reader open, drop of an open
   txn, `:memory:` and file. Or document the bounded wait.
2. **Unapplied key set** for Push (and rebuild from log tail). No
   commit may exist where duplicate keys can reach the log after the
   map stops guarding push.
3. **Reader connection** for Class L plan/validate/`live_item`. Prove
   push planning does not take a write txn.
4. **Class S claim** in `fireweed-relational` + each SQL driver: apply
   tail, short txn, outbox, **COMMIT**, log PUT, delete outbox. Remove
   post-claim catch-up. Idempotent apply including fused claim+complete.
   Two `claim(100)` disjoint. Do not hold writer across `packed_append`.
   Same tests on SQLite, Turso, Postgres.
5. **Delete planner maps and `live_tokens` authority** on every SQL
   adapter. `rg` gate.
6. **Apply cost** (no group-member scan on claim apply; hop-per-pack
   on Turso).
7. **Measure** object log × Turso (production pair), and a SQLite
   projection smoke of the same claim tests:
   - ingest / enrich / schedule same order of magnitude at 1k and 10k;
   - claim returns from the Class S txn (bodies + lease) without
     waiting for background apply of that `Claim`;
   - two in-flight claims never share ids;
   - 100k ingest-then-pull-and-complete **finishes**, and pull does not
     collapse vs 10k.

## Non-goals

- Collapsing the object log into a SQLite command log.
- Serving payloads from process memory (except the in-memory projection).
- In-memory HashMaps on SQLite/Turso/Postgres to stand in for rows.
- Changing `open_sqlite` default `synchronous=FULL`.
- Letting `BatchUpdate` mutate leased items.
- Requiring env vars for the production cell.

## Done when

One item store per SQL cell: the SQL projection. Claim is a projection
transaction (lease + outbox) then a log record. SQLite, Turso, and
Postgres share that contract. The in-memory projection stays a map and
does not grow SQL crutches. No planner-map or `live_tokens` authority
on SQL adapters. Delivery does not wait for background apply of the
claim. Ingest does not use a write transaction to decide a push.
Dual-claim and duplicate-key packs do not poison the shard.
