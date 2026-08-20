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
    disposition: "wire/outcome/seam/rerank/rebuild folds landed; implementing"
---

# Pipeline produce. One seam before lease.

Baseline `v0.31.19` / `1787169221`, `filesystem--turso`, N=10k, inflight=8.

Target: every later phase ≥ ingest on this cell. Apply of enrich/schedule **overlaps** those phases (it does not move onto the deliver clock).

Round-1 Claude and Codex both **BLOCK**ed. The text below is the fold, not the pre-review cuts.

## The pipeline

```
ingest append ──► ack ──► …
     \ apply inserts (overlaps the next appends)
enrich append ──► ack ──► …
     \ apply updates (overlaps)
schedule append ──► ack ──► …
     \ apply a few columns (overlaps)
──────── seam: projection ≥ last appended Push/Update ────────
claim lease+append ──► ack ──► …
complete append ──► ack     (overlaps the next claim)
```

Default `open()` stays Strict. SS / `AsyncProjection` is the pipeline.

**Seam scope:** appended-but-unapplied produce. SS phases are sequential, so at the produce→claim boundary every produce is on the log. An in-flight linger vs a concurrent claim is pre-existing and not this cut.

## Cuts

### 1. BatchUpdate appends. One peek, no wait.

Under `AsyncProjection` only (`crates/fireweed/src/turso_compose.rs`):

- Validate shape/types/gates from the request. No wait loop.
- One reader peek. **No `wait_for_progress`.**
- Peek-hit Pending: plan as today (`item_id`, advisory `item_version+1`). Log those commands.
- Peek-hit leased / terminal / version mismatch / Both-mismatch / duplicate: today’s `Conflict` / `Terminal` / `Invalid`. **Do not log.**
- Peek-miss on `ClientItemKey` (apply lag, not absence): log the entry. Add

  ```
  #[serde(default)]
  pub client_item_key: Option<ClientItemKey>
  ```

  on `UpdateFieldsCommand`. Keep `item_id` required. Unresolved rows use `item_id = ItemId::from_u64(0)` and `client_item_key = Some(key)`. Old envelopes have `client_item_key: None` and a real id.
- Response for a logged miss: `Updated { item_id: 0, client_item_key, item_version: 0 }`. Under `AsyncProjection` those two numbers are advisory (they already were: apply has not run). SS matches `Updated { .. }`.
- `expected_item_version: Some` + peek hit + mismatch → `Conflict`, not logged. Peek miss → log the expected version on the command (`#[serde(default)] expected_item_version: Option<u64>`); apply skips on mismatch.

Apply (`apply_update_fields_batch_sql`):

- If `client_item_key` is set and `item_id` is 0, resolve the live (`superseded=0`) row by key. None → skip that row.
- `api001_batch`: skip `Leased` / terminal. Non-`api001_batch` (FAC-1) still updates leased rows. Do **not** change the `IN ('Pending','Leased')` filter globally.
- Envelope `item_ids`: real ids when known; omit `0`.

Keep `apply_start_delay_ms.max(300)`. Removing it drops ingest to ~300/s (apply contends for the same disk as produce). The sleep is one-shot per shard on first produce apply.

Do not add a process map.

### 2. Apply stays cheap so the seam is a no-op.

Apply runs **during** enrich/schedule, not after they ack the last batch.

- All-Keep payload: do not SELECT or write the blob. All-Set: write the new blob, do not read the old one.
- `UPDATE` only columns the batch sets. Homogeneous SS batches: schedule writes `priority`, `priority_sort`, `not_before`, `metadata` only.
- **Keep `refresh_group_summaries` when the representative moved.** Incremental refs+summary cannot elect an unchanged member. SS first schedule window touches 100 groups; that refresh is correct.
- Claim apply **keeps** lease + bearers + `load_grouped_items` / group-remove. Rebuild has no live lease txn. Live Class S does not duplicate group work.

### 3. Claim leases, then continues.

- `catch_up_produce`: wait until projection ≥ `last_produce`. Remember that position per shard. Later claims skip the wait while `last_produce` has not advanced. Completes do not move `last_produce`. A new Push/Update invalidates the memory via `note_produce_positions`.
- Class S RelTx on `run_reltx_blocking` (same hop as apply). Writer held only for that txn.
- Delete outbox after the durable append, **before** enqueue apply. Drain may re-append; Claim apply of the same token is a no-op on already-Leased rows; bearers upsert is idempotent.
- `finalize_validate` on the reader. No IMMEDIATE, no rollback. Finalize apply no-ops if the row is not leased / token mismatch (TOCTOU is apply’s problem, not validate’s).
- Plan SELECT for finalize: token + version + attempts. No payload.
- SS P4 keeps one claim in flight overlapping complete of batch N. Concurrent claim appends can enqueue out of sequence and poison apply (`307 -> 313`). Inflight=8 on claim needs ordered enqueue, not this cut.

No `SKIP LOCKED`. No reservation table.

## Out of scope

Planner map. Process item HashMap. `SKIP LOCKED`. Reservation table. Sync apply on produce. Collapsing log axes. Changing payloads or dropping groups. Inventing a group-rerank that cannot see unchanged members. Closing the in-flight linger vs claim race.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

Cut 1: enrich and schedule ≥ ingest, ingest not regressed, p99 not a 300 ms sleep.  
Cuts 2–3: deliver ≥ ingest at inflight=8; first claim may include a short seam; later claims are lease+append. Report total wall as well as per-phase items/s.
