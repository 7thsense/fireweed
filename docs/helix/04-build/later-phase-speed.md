---
ddx:
  id: build-later-phase-speed
  type: implementation-plan
  links:
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
  review:
    round: 1
    claude: BLOCK
    codex: BLOCK
    folded: docs/helix/04-build/reviews/later-phase-speed-round1/
    disposition: "v0.31.20 shipped the peek/Keep-payload/reader-finalize fold; remaining stalls below"
---

# Pipeline the later phases for real

Latest `1787259713`, `filesystem--turso`, N=10k, inflight=8 (P4 still one claim at a time):

| phase | items/s | call p50 | vs ingest |
|---|---:|---:|---|
| ingest | 31692 | 23 ms | floor |
| enrich | 33465 | 19 ms | ≥ ingest |
| schedule | 51361 | 12 ms | ≥ ingest |
| deliver | 382 | claim 256 / complete 90 | **serial** |

Target unchanged: every later phase ≥ ingest. Do not drop groups or payloads. Keep `apply_start_delay_ms.max(300)` (without it ingest is ~300/s). No planner map, no `SKIP LOCKED`, no reservation table. Default `open()` stays Strict.

## Why it is still slow

Ingest is 12.5 waves of 8 appends in 0.32 s (~25 ms/wave). Enrich is 2.4 s (~190 ms/wave). Deliver is 28.6 s.

1. **Deliver is serial.** Overlapping complete N with claim N+1 packed two appends. The follower **cancels** its apply reservation before the leader enqueues the combined batch. Apply then sees high-water 300 and a batch starting at 302 and poisons. The harness is sequential claim→complete to avoid that. 100 × (189+90) ms = 28 s. That is the run.
2. **Claim still holds the writer for a fat SELECT** (payload, fields, metadata, entity, index_fields, gate anti-join) plus apply of *this* Claim (`load_grouped_items`). Next lease waits on that writer.
3. **Claim/schedule apply used to dump the group.** First 100-key window rewrites every representative (`job_key = i % 100`). That path now re-elects with COUNT + `LIMIT 1`; there is no member dump.
4. **Enrich/schedule still peek SQL** on the single reader mutex before they append. Eight in-flight updates line up there instead of going to the log.

## Cuts

### 1. Apply consumes in log order. Arrival order may differ.

`crates/fireweed-objectlog/src/async_projection_apply.rs`, packer waiters in `log_engine_store.rs` / `append_class_s_claim` / `ObjectLogTursoCommitter`.

Today: follower of a packed PUT gets `apply_batch: None` and **cancels** its reservation. If that cancel wins the race with the leader’s enqueue, sequence N is in nobody’s Ready queue. Next batch is N+2 → poison.

- Leader enqueues the full pack first. Followers wait for that enqueue, then drop their reservation (the sequences are already in the leader’s Ready batch). Never cancel a sequence that is not yet in a Ready batch.
- Coordinator: a Ready batch that is not contiguous with `applied_high_water` is **held**, not poisoned. Apply it when the hole is filled. Poison only if the hole is still empty after the shard has no Reserved/Ready entries (true gap).
- Test: concurrent claim+complete on one shard; apply high-water is contiguous; no poison. Then restore SS overlap of complete N with claim N+1, then inflight=8 claims (writer serializes the lease txn; appends overlap).

This is the pipeline. Without it, deliver cannot overlap and cannot match ingest.

### 2. BatchUpdate does not read SQL to append.

Under `AsyncProjection` (`pipeline_unresolved_updates`):

- If no entry has `expected_item_version`, **do not peek**. `plan_batch_update_pipelined` with an empty snapshot. Every `ClientItemKey` is logged with `item_id = 0` + `client_item_key`. Apply already resolves that.
- `expected_item_version: Some` keeps today’s one peek for those entries only (SS does not set it).
- Strict `open()` unchanged (snapshot after apply).
- Duplicate keys in one request: `Conflict` the extras in the planner, still no SQL.

SS already matches `Updated { .. }`. Advisory `item_id`/`item_version` 0 is accepted.

### 3. Re-elect a group with an index seek, not a table dump.

When the representative moved, do **not** `SELECT` every pending member.

`fireweed_items_group_due_idx` is `(tenant_id, queue_id, lifecycle_state, group_key, not_before, priority_sort, created_seq)`.

For each touched group whose rep is in the batch:

```sql
SELECT item_id, eligible_since, priority_sort, created_at, created_seq
FROM fireweed_items
WHERE tenant_id=? AND queue_id=? AND lifecycle_state='Pending' AND superseded=0
  AND group_key=? AND (not_before IS NULL OR not_before<=?)
ORDER BY priority_sort, created_seq, item_id
LIMIT 1
```

plus `COUNT(*)` with the same predicate (or one scan of LIMIT 1 + a count query). 100 groups → 100 index seeks, not 10k rows.

Blocked gates use the same COUNT + `LIMIT 1` with the claim anti-join. There is no member-dump helper. Do not store extra process maps.

Claim apply: if the row is already `Leased` (Class S), do not `load_grouped_items`. `SELECT group_key FROM fireweed_items WHERE item_id IN (...)` (the 100 ids in the command) and `apply_group_summary_remove` those refs. Rebuild of a *Pending* Claim still leases in apply as today.

### 4. Lease is the only writer work on the claim clock.

- Class S `SELECT` returns what the worker needs: `item_id`, `client_item_key`, `payload`, `item_version`, `priority`, `group_key`, `not_before`, `retry_count`, `max_attempts`. Drop `entity_document` / `index_fields` from this SELECT (SS has neither; load them only if the queue has a schema / typed indexes).
- After cut 1, overlap complete N with claim N+1, then inflight=8 claims. First claim of the phase still drains produce (`catch_up_produce` once). Later claims compare the remembered cursor and go.
- Next lease must not wait for apply of this Claim. That follows from cut 3 (cheap Claim apply) plus cut 1 (apply not blocking the wrong sequence). Writer is still one connection: lease N+1 runs as soon as lease N commits, while apply of Claim N is in `spawn_blocking` behind it only if that apply is still in the writer. Keep Claim apply to the no-op item UPDATE + O(batch) group-remove above so it is milliseconds.

No change to `SELECT` payloads. The worker needs bodies.

## Out of scope

Planner map. Process item HashMap. `SKIP LOCKED`. Reservation table. Dropping `apply_start_delay_ms.max(300)`. Changing harness payloads or group cardinality. Closing the linger window of a produce that has not yet appended vs a concurrent claim.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

| cut | done when |
|---|---|
| 1 | overlapping P4 does not poison; then inflight=8 claims |
| 2 | enrich p50 in ingest’s ~23 ms band; ingest not regressed |
| 3 | schedule ≥ enrich (no 10k-row group SELECT on the writer) |
| 4 | deliver ≥ ingest at inflight=8; first claim may include the produce seam |

Report per-phase items/s **and** total wall. Cut 2+3 without cut 1 will not move deliver. Cut 1 without 2 will not move enrich. Do them in order 1 → 2+3 (parallel) → 4 (harness overlap after 1).
