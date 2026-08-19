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

# Later phases must beat ingest

Baseline `v0.31.19` / `1787169221`, `filesystem--turso`, N=10k, inflight=8:

| phase | items/s | call p50 | should be |
|---|---:|---:|---|
| ingest BatchPush | 30569 | 23 ms | floor |
| enrich BatchUpdate | 3042 | 117 ms | **faster than ingest** |
| schedule BatchUpdate | 2720 | 150 ms | **faster than ingest** |
| deliver claim+finalize | 316 | 316 / 279 ms | **faster than ingest** |

Same packer on `filesystem--memory` already has enrich/schedule **faster** than ingest (56k / 95k vs 26k). The Turso pair is losing because produce **waits to plan** and deliver **re-takes the writer** for work that is not the lease.

Success: each later phase ≥ ingest on this cell, same N / inflight / batch. Do not hide cost in the harness (keep 1 KiB enrich payload, metadata, groups, unfiltered claim+complete).

## What ingest actually does

`prepare_push` (reader `validate_push`) → eight waiters hit `packed_append` together (8 / 4 MiB / 20 ms) → ack after the PUT → coalesced insert later.

No wait for prior apply. No snapshot of existing rows. Class L is intact.

## Why the later phases lose

### Enrich and schedule (produce path)

`batch_update` always `planner_update_snapshot` then `packed_append`.

- Enrich starts the instant ingest’s **log** acks. Keys are not in SQL yet. Snapshot loops until they appear (or `projected ≥ last_produce`). First produce apply on `q-ss` still sleeps **300 ms** (`apply_start_delay_ms.max(300)`). Enrich p99 294 ms is that delay, not the 1 KiB payload.
- Eight in-flight snapshots share **one** `Mutex<Connection>` reader. They stagger into the packer and miss the 8-waiter seal. Ingest does not snapshot, so it packs. p50 117–150 ms is **missed packing**, not column-update SQL.
- Snapshot SELECT is metadata only (no payload). `persist_request_outcome_sql` is a no-op (`request_outcome: None`). Group rerank is off on enrich (`Keep` priority / `not_before`).
- Schedule should not wait for enrich apply (`expected_item_version` is `None`; keys already exist). It is still 150 ms/call because it uses the same snapshot+packer path.
- SS items **are** grouped: `job_key(i, n)` → 100 groups at N=10k. That is apply work, not the BatchUpdate call clock, except it holds the writer and delays the first deliver claim.

### Schedule (apply path — starves deliver)

`apply_update_fields_batch_sql`:

- `need_payload` is true if **any** row is Keep. Schedule is all-Keep: it **SELECT**s the 1 KiB blob and then omits it from the UPDATE.
- UPDATE still copies `fields` and other Keep columns.
- First schedule window rewrites the representative of every group (`i % 100`). `apply_group_summary_rerank` falls through to `refresh_group_summaries` and SELECTs every pending member of those groups (~10k rows) on the writer.

### Deliver

Class S is: catch up produce → one IMMEDIATE lease txn → log PUT → delete outbox.

- **Every** claim calls `catch_up_produce`. First claim waits for leftover enrich/schedule apply (including the 10k-row group refresh). Later claims should be one cursor compare; they still contend with the writer.
- Class S RelTx runs on the SS runtime via `block_in_place`, not `run_reltx_blocking`.
- After the PUT, `append_class_s_claim` **enqueues apply then deletes outbox**. Delete takes the writer behind apply of **this** claim. Class S is forbidden to wait for that apply.
- Claim apply still `load_grouped_items` + `apply_group_summary_remove` (`apply.rs` Claim arm). The writer-contention plan already said item-level claim apply does not refresh summaries.
- Finalize plan: `render_claimed` loads payloads; **`finalize_validate` takes `writer.lock()`, `BEGIN IMMEDIATE`, SELECT, `ROLLBACK`**. That is the 279 ms finalize. Complete is Class L; this validate is not the lease.

Not the cause: SKIP LOCKED, reservation table, planner map, two mutations vs one (overlapped 23+23 ms would still beat ingest).

## Cuts

Instrument one N=10k run first: packer waiters/seal per phase; time in snapshot vs `packed_append`; time in `catch_up_produce` / Class S txn / outbox delete / `finalize_validate`. No extra load.

### 1. Stop waiting

1. Drop Turso `apply_start_delay_ms.max(300)`. Linger is already 20 ms. Fallback: delay = `PACK_LINGER`, not 300. Ingest must stay ~30k and still pack.
2. Snapshot wait is “these keys exist,” never `last_produce` of a later Update. Schedule must not wait for enrich apply.
3. `catch_up_produce` once per queue until the next Push/Update (`note_produce_positions` already clears that). Completes do not move the cursor.

### 2. Snapshots must not unseal the packer

Reader pool (4–8 `query_only` + `read_uncommitted` connections). One writer. Re-run the reader-vs-IMMEDIATE probe on the **SS file**, not only `:memory:`.

Target: waiters/seal ≈ 8; enrich/schedule p50 in ingest’s 23 ms band; items/s ≥ ingest.

### 3. Deliver holds the writer only for the lease

1. `finalize_validate` on the reader. No IMMEDIATE, no rollback. Complete apply stays idempotent.
2. Finalize plan SELECT without payload/fields/metadata/entity. Token + version + attempts only.
3. Delete outbox after the durable PUT, **before** enqueue apply. Crash before delete: drain re-appends; apply of the same token is a no-op.
4. Class S SQL on `run_reltx_blocking` (same hop as apply).
5. Item-level Claim apply: cursor + idempotent no-op. No `load_grouped_items`, no group-summary remove. If grouped claim needs a live summary, do it **inside the Class S txn**.

Then raise deliver inflight to 8 (same cell as ingest). Today’s inflight=1 is a leftover map/catch-up workaround. Prove single-claim p50 first.

### 4. SQL is the columns that changed

1. All-Keep payload (schedule): do not SELECT or write the blob. All-Set (enrich): write the new blob, do not read the old one.
2. UPDATE SET only changed columns. Schedule: `priority`, `priority_sort`, `not_before`, `metadata`.
3. Group rerank stays O(touched groups) from in-memory refs + the previous summary row. No `refresh_group_summaries` over the whole group because the representative moved.

Class S still returns payloads (product). `SELECT_CLASS_S_DUE` already has `fireweed_items_pending_order_idx`. No SKIP LOCKED.

## Out of scope

Planner map. Process item HashMap. SKIP LOCKED. Reservation table. Sync apply on produce. Collapsing sqlite-log vs object-log. Changing harness payloads or dropping groups to make the scoreboard move.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

Cuts 1–2 done: enrich and schedule ≥ ingest, p99 off ~294 ms, ingest not regressed.  
Cuts 3–5 done: deliver ≥ ingest at inflight=8. If inflight stays 1, the honest floor is one PUT per claim (~ ingest p50 → ~4k/s), not 30k — say so in the ladder, then raise inflight.
