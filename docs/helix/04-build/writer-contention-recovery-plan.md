---
ddx:
  id: build-writer-contention-recovery
  type: implementation-plan
  links:
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: concerns}
  review:
    round: 5
    claude: BLOCK
    codex: BLOCK
    folded: "docs/helix/04-build/reviews/writer-contention-round5/"
    disposition: "text contradictions fixed; remaining race scoped as pre-existing SS-sequential; implementing"
---

# One writer txn leases the chunk. Reader for produce. Log after commit.

The SQL projection is the database. Claim **picks and leases** the next N pending rows in one writer transaction, then records that envelope on the object log. Produce **reads** on a second connection. Apply is background.

No process map. No reservation table. No `SKIP LOCKED` on Turso (one writer is the lock). No wait for Complete before the next claim.

## Why it is slow

`filesystem--turso` N=10k: ingest 115/s, enrich 73/s, schedule 73/s, deliver 86/s. Claim p50 1.13s / 100 items.

- `validate_push` takes `writer.lock()` + `BEGIN IMMEDIATE` and rolls back.
- Update planning calls `catch_up_projection`.
- Claim calls `catch_up_projection` for the **whole** tail, including completes.

Postgres already picks and leases in one statement (`CLAIM_CTE`). Turso does SELECT then UPDATE. That split is not required.

## Claim (item-level)

**Invariant:** pick and lease are atomic. Exactly one writer acquisition before the log append.

Probe first (record pass/fail): Turso `UPDATE fireweed_items SET … WHERE item_id IN (SELECT … LIMIT n) RETURNING …`. If that works, use it. If not: keep today’s **same-txn** `SELECT` due ids then `UPDATE` those ids (the writer mutex + `IMMEDIATE` is the lock). Do **not** re-select by `lease_token_hash` alone (tokens are not unique). Eligibility predicates stay today’s `SELECT_CLASS_S_DUE` (pending, not superseded, no cohort, due, no blocked gate).

Returned rows include `priority_sort` and `created_seq`. Sort the response in that order (Postgres already does; Turso must too).

Same txn: insert bearer rows and the outbox envelope for those ids. `COMMIT`. Drop the writer.

`reserve()` the apply slot **before** `BEGIN`, with debt bounded by `max_items` (today’s stub envelope; do not wait for selected ids). Read the epoch before `BEGIN`. After commit: `packed_append` the same envelope. I/O error: retry that envelope. `EpochFenced` / backpressure / poison: leave the outbox, mark the queue claim-blocked, do not retry forever, do not unlease. Delete outbox only after a durable PUT. Drain outbox before another item-level claim on that queue. Apply of a duplicate drained Claim is a no-op (already this token).

Do **not** wait for apply of this claim. Do **not** wait for Complete.

**Do wait for unapplied Push and Update on this queue** before the lease txn, so the claim is the next produce position (live == rebuild). An unapplied `BatchUpdate` still sits on a `Pending` row; if we lease first, live skips the update and rebuild applies it. Completes can lag: they do not change pending rows.

Implement that wait with a **produce cursor**, not by inspecting apply envelopes. On each Push/Update append, store `last_produce = max(last_produce, that position)` for the queue. Claim waits until the projection cursor is ≥ `last_produce` (reuse `catch_up_projection`’s loop, swap the target). Completes do not move `last_produce`. This is one position per queue, not an item map. Set it at append, before enqueue.

Scope: this closes **appended-but-unapplied** produce. A produce still inside `packed_append` linger is the same window as today; SS phases are sequential so that window is empty at the produce→claim boundary. Do not add a new permit/map to close it.

This plan’s SS path is unfiltered item-level claim. Grouped / cohort / reclaim stay Class S (same txn contract) but are not the first cut.

## Produce

One long-lived reader (`connect()` at open). `validate_push`, update snapshot, `live_item`, pending, metrics, `recovery_high_water` use it. No `IMMEDIATE`. No `writer.lock()`.

Checks stay today’s: key, item id, retention, group size, typed uniques, request-id.

If a push/update reader miss might be apply lag, wait until the projection cursor is ≥ `last_produce` for that queue, then read again. Same cursor as claim. If the key is still absent, it is a real miss.

Do not ack a push and later drop the row.

Reader-while-writer probe already exists and passed (`tools/fireweed-turso-compat-probe`). Cut 2 uses that result: B returns pre-txn rows; plan from committed SQL plus the coordinator wait above.

## Apply

Background, log order. Claim already this token: no-op. Other token: keep the first. Update of `Leased`: skip that row. Do not poison a pack.

Item-level claim apply does not refresh group summaries.

`fireweed_lease_bearers` is a **derived index** of plaintext tokens for pending-by-consumer. `fireweed_items` keeps `lease_token_hash` only. Outbox holds the Claim envelope. Hash-only is scoped to the item row. Move pending listing onto bearers, then delete `live_tokens`.

## Cuts

0. **Probe** Turso `UPDATE…IN (SELECT…LIMIT) RETURNING` (file + `:memory:`). Record the result. Choose RETURNING or today’s same-txn SELECT-ids-then-UPDATE-those-ids. Never SELECT-by-hash.
1. **Claim txn + no complete-wait.** One writer txn as above. Remove `catch_up_projection` from claim. Wait only for `last_produce`. Tests: two claims disjoint; kill-after-COMMIT-before-PUT; sequential update-then-claim rebuild equals live.
2. **Reader for produce.** Shared reader. Delete `live_tokens` after bearer queries work.
3. **Apply does not poison.** Mixed hop in log order. Skip `Leased` updates.
4. **Measure and iterate** (below).

Turso first. SQLite gets the same reader + one-txn claim. Postgres already has the CTE; drop catch-up-before-claim and use a reader for produce.

## Performance

Control that exists: `filesystem--memory` SS, same N, same batch (object-log + in-memory projection). That is an **upper bound** on log cost, not a pure packer microbench.

`filesystem--turso` produce chase target: ≥ **90%** of that control’s ingest / enrich / schedule **after** plan is off the Turso writer. If apply CPU and log PUT share a core, the physical bound is lower than the memory-projection control; prove that with a profile (apply CPU + disk vs `writer.lock()` / catch-up). Stop when the remaining gap is physical (packer linger, disk, Turso apply CPU), not a mutex. Hard gate if 90% of memory is unreachable: produce ≥ **50%** of `filesystem--memory` and no `writer.lock()` on plan.

Deliver target: 90% of `min(lease-txn rate, packed claim PUT rate)` measured on this host (100-item batches). Include time to drain apply until `complete == n` in the end-to-end wall; do not move apply cost past the measurement and call it a win.

Loop: measure N=10k → if below the chase target, profile who holds the writer / apply hop / PUT wait → cut that → re-measure. Cap 8 iterations. Then N=100k must finish; deliver items/s ≥ 50% of the N=10k deliver rate.

Baseline: v0.31.17 N=10k 115 / 73 / 73 / 86 /s.

## Not this plan

- `FOR UPDATE SKIP LOCKED` on Turso
- Planner map, reservation table, ack-then-skip push
- Waiting for Complete before claim
- Writer held across `packed_append`
- Changing `open_sqlite` `synchronous=FULL`
- `BatchUpdate` of leased items
- Collapsing object-log into sqlite-log

## Done when

- Item-level claim: one writer txn, then log PUT
- Plan/validate do not take `writer.lock()`
- Claim does not call `catch_up_projection`
- Two claims on 1,000 items never share ids
- Rebuild matches live after update-then-claim
- Pending listing works without `live_tokens` as serving authority
- N=10k produce ≥ 90% of `filesystem--memory`, or ≥ 50% plus a profile showing the rest is apply CPU/disk (not `writer.lock()` / catch-up)
- Deliver meets the lease/PUT bound; wall includes drain to `complete == n` (harness change if today’s timer excludes it)
- N=100k ingest-then-deliver finishes
