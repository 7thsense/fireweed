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
    disposition: "ingest/enrich/schedule ≥ 30k/s; deliver still serial on two writer txns per claim"
---

# Deliver: one lease txn, then pipeline

Latest `1787259713`, `filesystem--turso`, N=10k:

| phase | items/s | call p50 |
|---|---:|---:|
| ingest | 31692 | 23 ms |
| enrich | 33465 | 19 ms |
| schedule | 51361 | 12 ms |
| deliver | 382 | claim 256 / complete 90 |

Target: deliver ≥ ingest. Payloads and groups stay. `apply_start_delay_ms.max(300)` stays (produce only). No planner map, `SKIP LOCKED`, or reservation table. Class S remains lease-then-log. Default `open()` stays Strict.

Enrich/schedule already match ingest. This plan is only deliver.

## What we got wrong

Ingest acks after the log. Claim cannot: the worker needs leased rows and bodies from SQL. That does **not** justify what the code does next.

A Class S claim today is **two (sometimes three) writer transactions** for work the lease already did, then the next claim waits for all of them:

1. **Lease txn** — `SELECT` 100 rows (payload + `fields` + `metadata` + gate anti-join) , `UPDATE` Leased, bearers, outbox. Necessary core; extra columns and the anti-join are not.
2. **Log PUT** — ~20 ms, same as ingest. Fine.
3. **Enqueue apply**, then **`delete_claim_outbox` takes `writer.lock()` again.** Apply of this Claim also takes that lock and re-does the lease: no-op `Pending→Leased` UPDATE, `load_grouped_items`, `COUNT` of every remaining member of 100 groups, persist bearers again. The response path waits on `delete_claim_outbox`, so **claim p50 includes this apply**.
4. **Harness issues one claim at a time** because inflight=8 hung. Overlapping complete does not matter: complete is 90 ms, claim is 256 ms.

100 × 256 ms = 26 s. That is the phase.

Turso has one writer. You cannot run lease N+1 and apply N at the same time. The fix is to **stop giving apply N enough work to matter**, and **stop waiting for it**.

`COUNT(*)` of remaining members is still a scan of the group. Incremental remove already knows the count (`summary.count - leaving`). A new head is `LIMIT 1`. A new oldest is `LIMIT 1` on `eligible_since` (`fireweed_items_active_scope_idx`), not `MIN` over the residents.

## Cuts

### 1. One writer txn: the lease.

`class_s_claim` / `SELECT_CLASS_S_DUE` / `apply_group_summary_remove`.

In **that same IMMEDIATE txn**, after the lease UPDATE:

- Thin SELECT: `item_id`, `client_item_key`, `payload`, `item_version`, `priority`, `group_key`, `not_before`, `retry_count`, `max_attempts`. No `fields`/`metadata` unless the queue has a schema or typed indexes. No gate anti-join unless `has_blocked_gates`. Payloads stay.
- Group heads from the selected rows. The 100 ids and `group_key`s are already in hand. Decrement `eligible_item_count`. If a claimed id is `rep_item_id`, `LIMIT 1` for the new head (`fireweed_items_group_due_idx`). If it was `oldest_eligible_at`, `LIMIT 1` `ORDER BY eligible_since`. Do **not** `COUNT` remaining members. Do **not** `load_grouped_items`. Rebuild of a *Pending* Claim (non-Class S) still leases in apply.

Done when: a Class S claim of 100 SS keys does one `BEGIN IMMEDIATE` and no second group scan. Claim p50 drops toward log PUT + the SELECT (ingest’s ~23 ms band, plus first-claim produce catch-up).

### 2. Apply of an already-leased Claim does not touch items or groups.

`QueueCommand::Claim` when the rows are already `Leased`:

- Bump `relational_cursor` / `last_command_sequence`.
- Delete the outbox row **here** (same apply txn).
- Tokens if needed.
- No item `UPDATE`, no bearers `INSERT`, no group relect.

`append_class_s_claim` returns after lease commit + packed PUT + `enqueue_reserved`. It does **not** call `delete_claim_outbox_row` on the response path (that is what serializes the caller with apply today).

Done when: `writer.lock()` around apply of Claim N is milliseconds, so lease N+1 is not waiting on group work. Overlapping complete N ∥ claim N+1 stays; wall should follow `max(claim, complete)` not `claim+apply`.

### 3. Pipeline eight claims.

`ss_phased_capacity.rs` P4 loop, packer reservations.

Writer still serializes the **lease** txns (that is how they get disjoint items). Appends already pack. With cut 2, eight leases should be eight short IMEDIATEs then one PUT.

- Harness: waves of 8 claims, then complete those ids. One empty result does **not** mean the phase is done; join the wave and continue while any claim returned items or pending remains.
- Reservations: leader of a packed PUT enqueues the whole pack; followers wait for that enqueue, then drop. Do not cancel a hole that is not yet in a Ready batch. Hold non-contiguous Ready; poison only a true gap.
- Test: 8 concurrent Class S claims on one shard, 800 items, contiguous apply high-water, no hang under 2 s at N=800.

The earlier inflight=8 hang was almost certainly eight callers plus apply plus `delete_claim_outbox` stacked on `writer.lock()` while apply still re-elected 100 groups. Cuts 1–2 first, then this. Do not leave P4 at one claim.

Done when: SS N=10k deliver ≥ ingest. First claim of the phase may include produce catch-up; later claims must not.

## Out of scope

Planner map. Process item HashMap. `SKIP LOCKED`. Reservation table. Dropping `apply_start_delay_ms.max(300)`. Changing harness payloads or group cardinality. Postgres dialect.

## Measure

```sh
cargo test -p fireweed --test ss_phased_capacity --release -- --nocapture
```

| cut | done when |
|---|---|
| 1 | one IMMEDIATE per Class S claim; no remaining-member COUNT; claim p50 ≪ 256 ms |
| 2 | claim return path does not `writer.lock()` after enqueue; complete overlap no longer waits on Claim apply |
| 3 | inflight=8 P4; deliver ≥ ingest at N=10k |

Report per-phase items/s **and** total wall. 1 then 2 then 3. 1 without 2 still pays apply on the next lease. 3 without 1–2 will hang again.
