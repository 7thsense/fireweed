---
ddx:
  id: build-later-phase-speed
  type: implementation-plan
  links:
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
---

# Pipeline produce. One seam before lease.

Baseline `v0.31.19` / `1787169221`, `filesystem--turso`, N=10k, inflight=8:

| phase | items/s | call p50 | target |
|---|---:|---:|---|
| ingest | 30569 | 23 ms | floor |
| enrich | 3042 | 117 ms | **faster than ingest** |
| schedule | 2720 | 150 ms | **faster than ingest** |
| deliver | 316 | 316 / 279 ms | **faster than ingest** |

Ingest is already a pipeline: append, ack, apply in the background. Enrich, schedule, and deliver should ride that same pipeline. They do not. Each BatchUpdate **stops and waits for SQL to look like the log**, and each claim **stops the writer for work that is not the lease**.

Success: same cell, same N / inflight / batch, every later phase ≥ ingest. Do not change harness payloads, groups, or drop complete to move the scoreboard.

## The pipeline

```
ingest append ──► ack ──► ingest append ──► ack ──► …
     \ apply inserts
enrich append ──► ack ──► enrich append ──► ack ──► …
     \ apply column/payload updates
schedule append ──► ack ──► …
     \ apply a few columns
──────── seam: SQL covers last Push/Update ────────
claim lease+append ──► ack ──► claim lease+append ──► …
complete append ──► ack     (overlaps the next claim)
     \ apply is background; never on these clocks
```

Log order is the order apply will see. If ingest is already on the log, enrich does not need those rows in SQL to append. Apply of the update runs after apply of the push. That is the whole trick.

The only required stop is the **Class S seam**: a lease mutates SQL *before* its Claim is on the log. If a Push/Update is logged but not applied, a lease can take a stale Pending row and live diverges from rebuild (`BatchUpdate` must not touch `Leased`). So: drain produce apply once, then lease. Completes do not affect pending rows and do not belong on that seam.

Default `open()` stays `ResponseBarrier::Strict` (apply before ack; snapshot is free). SS / `AsyncProjection` is the pipeline.

## What breaks the pipeline today

**Enrich/schedule.** `batch_update` reads SQL (`planner_update_snapshot`) and **loops until the keys exist**. Missing keys are treated as `NotFound`, so the loop waits on apply — including the 300 ms first-apply sleep on `q-ss`. The call clock is apply lag, not the update. Eight in-flight calls serialize on one reader mutex, so they also stop overlapping at the log.

`plan_batch_update` needs a snapshot only to turn `ClientItemKey` into `item_id` and to reject leased/missing *now*. Under `AsyncProjection` that “now” is the wrong time. The right time is apply.

**Deliver.** Every claim calls `catch_up_produce` (fine once, waste every time). Class S RelTx `block_in_place`s on the 2-thread runtime. Outbox delete runs *after* apply is enqueued, so it queues behind apply of **this** claim. Claim apply still scans group members. Finalize `finalize_validate` takes the writer, `BEGIN IMMEDIATE`, SELECT, `ROLLBACK`. Complete is Class L; that validate is a bubble.

**Apply (makes the seam expensive).** Schedule Keep-payload still SELECTs the blob. First schedule hop rewrites every group representative and `refresh_group_summaries` reads ~10k pending rows on the writer. First claim then sits on the seam behind that.

Not a work item: sharing one object-log write across in-flight appends. That already happens when the pipeline is full. Do not build a “packer feature.”

## Cuts

### 1. BatchUpdate appends. It does not wait for apply.

Under `AsyncProjection` only:

- Validate the request (batch size, types, gates, reserved fields). No projection.
- One optional reader peek, **no retry, no `wait_for_progress`**. Hits fill `item_id` / version in the response.
- Misses still go on the log, addressed by the client’s `item_ref` (`ClientItemKey` or `ItemId`).
- `UpdateFieldsCommand` carries that ref (keep `item_id` for old envelopes). Apply resolves against `fireweed_items` in log order.
- Apply: missing → no-op that row; `Leased` / terminal / version mismatch → skip that row. The envelope stays durable. Class L does not reject after append.
- Response: `Updated` for every syntactically valid entry (SS already matches that). Strict path unchanged (snapshot after apply).

Delete `apply_start_delay_ms.max(300)`. It is a sleep in the apply pipeline.

Do **not** add a process map. The log is the queue; SQL is the consumer.

### 2. Apply is a few columns, in the background.

Off the produce clock, but the Class S seam waits for it, so keep it cheap.

- All-Keep payload: do not read or write the blob. All-Set: write the new blob, do not read the old one.
- `UPDATE` only changed columns. Schedule: `priority`, `priority_sort`, `not_before`, `metadata`.
- Group rerank from the updated refs + the existing summary row. No `refresh_group_summaries` over the whole group because the representative moved.
- Item-level Claim apply: cursor + idempotent no-op. No `load_grouped_items`. If a grouped claim needs a live summary, do it in the Class S lease txn.

### 3. Claim leases, then the pipeline continues.

- Seam once: wait until projection ≥ last Push/Update (`last_produce`). Completes do not move it. After a catch-up, later claims compare the cursor and go.
- Class S: one IMMEDIATE txn (select due + lease + outbox + bearers). Run it on `run_reltx_blocking`, same as apply. Then append. Then drop the writer.
- Delete outbox after the durable append, **before** enqueue apply. Crash before delete: drain re-appends; apply of the same token is a no-op.
- `finalize_validate` on the reader. No IMMEDIATE, no rollback. Plan SELECT is token + version + attempts — no payload.
- Complete appends and returns. Overlap with the next claim. Do not wait for apply of this claim or this complete.
- Harness inflight=8 on claim, same as ingest. That is workers in the pipeline, not a workaround. Prove one claim is a lease+append first (p50 in the ingest band).

No `SKIP LOCKED`. No reservation table. One writer is the lock. Payloads stay in the Class S SELECT (the worker needs them).

## Out of scope

Planner map. Process item HashMap. `SKIP LOCKED`. Reservation table. Sync apply on produce. Collapsing sqlite-log vs object-log. Changing harness payloads or dropping groups.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

Cut 1 done: enrich and schedule ≥ ingest, p99 off ~294 ms, ingest not regressed.  
Cuts 2–3 done: deliver ≥ ingest. First claim may include the produce seam; the rest must be lease+append.
