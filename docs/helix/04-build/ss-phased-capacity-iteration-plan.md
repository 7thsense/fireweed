---
ddx:
  id: build-ss-phased-capacity-iteration
  depends_on:
    - discover-seventh-sense-phased-capacity-benchmark
    - discover-first-principles-performance-model
    - prd
    - api-workload-integration-profiles
    - adr-cqrs-log-projection-storage-model
    - adr-async-commit-strategy-and-dispatch
    - adr-log-single-source-of-truth
  links:
    - {kind: informed_by, to: discover-seventh-sense-phased-capacity-benchmark}
    - {kind: informed_by, to: discover-first-principles-performance-model}
    - {kind: informs, to: tp-fireweed-performance-matrix}
  review:
    self_hash: 69ce160750f9be3d293d1dbb3340780b41f0d443602487f069a24a9359e52f1d
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      api-workload-integration-profiles: 3c3dd594f1723e987015d4790634b1088016f5f41a049e661eba4b752cfb4c39
      discover-first-principles-performance-model: cda6f175ad5931d1307460863d730e5ca9ea8e4c9c247a5266386d4bcf8ccfdb
      discover-seventh-sense-phased-capacity-benchmark: 91217608181d46431be5faec30afc0b7d04bb34de7cde7778754c323a5f7f8e2
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
    reviewed_at: "2026-08-15T00:43:57Z"
---

# Implementation Plan: SS phased capacity iteration

**Status**: adopted after adversarial review (claude + codex, 2026-08-15).  
**Round-1 evidence**: `docs/helix/04-build/reviews/ss-phased-round1/`.  
**Mode**: measure first, one slice per commit, re-measure, stop on declared-host gates.  
**Does not authorize**: changing CQRS, the strict response barrier, `synchronous=FULL` on the sqlite log, claim exclusivity, retiring the 13k sealer ratchet, or making projection state a function of read traffic (ADR-013).

## Review deltas (adopted)

Both harnesses **BLOCK**ed the draft. Adopted before any bead is filed:

1. **P2/P3 are pending `BatchUpdate` by known keys**, not claim → update → release. Public `BatchUpdate` rejects leased items (`batch_update_preflight`, API-003 producer obligation 5). The native facade does **not** have lease-scoped batch update. Per-item `update_fields` would be 100 commits per 100 items and would make G2 a vanity bar.
2. **P4 is unfiltered `BatchClaim` + `complete`** (timestamp eligibility order = next delivery date). Do **not** put `metadata_equals` on the gated P4 path. `select_item_claim` with a predicate calls `eligible_candidates(now, usize::MAX)` and materializes the whole eligible set (`compose_impls.rs:578-607`).
3. **`phase` is metadata only** in the gated harness. No typed `phase` index. Typed `job_id` is optional and unused for claim selection.
4. **I-select is a required slice** before any *optional* filtered-claim arm. It is not on the G-gate critical path.
5. **Delete I4 lazy `claim_indexes`.** Options A/B key projection state off a non-logged read (ADR-013). Unique occupancy stays on `indexes`.
6. **I2 must add read-time echo** in `InMemoryProjection::to_claimed` and must not make `index_fields`-only items invisible to query / unique-validate paths. Write-time `rehydrate_entity_document` is not “free to delete.”
7. **G-gates are per-phase.** A 90 s wall is stretch, not an AND that secretly demands P1 ≥ 250k.
8. **I0 is a real test**, default `SS_N=10000`, not `--ignored` with a 200-row verify. I1 may record N=100k as calibration only; G-gates require N=1,000,000.
9. **One worker** on the gated run. In-flight = 1 batch. Serial-section budget is explicit.
10. **Blocked stop** names a profiler and an attempt cap.

## Goal

On a **declared quiet host** that matches the model’s **H-server** class (32-class CPU, enterprise NVMe with PLP, or an explicit recorded exception), cell **`sqlite--memory`** (`open_sqlite`), public facade, **N = 1,000,000**, **batch = 100**, **workers = 1**, telemetry on.

**Stop (success) when all of G1–G5 hold on the same N=1M run:**

| Gate | Metric | Floor |
|---|---|---|
| G1 | P4 deliver items/s | **≥ 50,000** |
| G2 | P2 enrich items/s | **≥ 40,000** |
| G3 | P3 schedule items/s | **≥ 40,000** |
| G4 | P1 ingest items/s | **≥ 80,000** |
| G5 | Correctness | exact N through every phase; 0 duplicate leases; residual eligible = 0; sampled P2 profile blob and P3 delivery timestamp match |

**Stretch (record, do not block stop):** P4 ≥ 100,000; P1 ≥ 100,000; P1–P4 wall ≤ 90 s.

Smoke **N = 10,000** is CI / slice regression only. **N = 100,000** is calibration only and **must not** appear as a G-gate baseline or be compared to a 1M row.

If the host is not H-server, still run the protocol and publish same-host before/after slices. Do not move G1–G5; annotate the host.

### Serial-section budget

Gated runs use **one queue and one worker**. In-flight public calls = 1. The exclusive section must stay **≤ 20 µs/item** on P4 (50k items/s) and **≤ 25 µs/item** on P2 (40k items/s). That is the budget I2/I3/I5 spend. Do not promise 8× from more workers on one queue.

## Non-goals

- Beating or replacing the 13k `claim_finalize_push_cycle` scoreboard (keep as sealer non-collapse ratchet).
- 19 typed indexes, 2.3 KiB-from-ingest, claim-batch 500.
- RESP as the capacity driver.
- Object-log / Postgres / Turso in this program.
- Changing API-001 so `BatchUpdate` applies to leased items (separate product decision).
- Multi-queue scale as a substitute for G1–G5.
- Lazy or traffic-triggered `claim_indexes`.

## Phases (gated harness)

Queue: timestamp ascending, `ordering_mode=strict`, `max_rank_error=0`, `max_claim_batch_size=100`, `max_push_batch_size=1000`. One typed index allowed (`job_id`); **not used for claim**. `metadata.phase` is the only phase token.

| Phase | Public calls | Item bytes | Notes |
|---|---|---|---|
| P1 ingest | `BatchPush` 100 or 1000 (report which) | stub **512 B** payload, `phase=needs_profile`, `not_before=now` | Keys retained for P2/P3 |
| P2 enrich | `BatchUpdate` 100 **pending** keys | payload **1024 B** profile blob, `phase=needs_schedule` | No claim, no lease |
| P3 schedule | `BatchUpdate` 100 pending keys | `priority` + `not_before` = due-now delivery ts, `phase=ready` | No claim |
| P4 deliver | `BatchClaim` 100 (**no** metadata predicate) + `BatchFinalize` `complete` | unchanged | Timestamp order is next-delivery-date |

Construction of the next batch is **inside** the phase clock. Queue create, **10,000-item warmup** (pushed and purged or on a side queue — do not leave it in the N=1M set), and teardown are **outside**.

Sampled reads (G5): after P2, 100 random keys `live_items` show 1024 B payload; after P3, 100 keys show the delivery timestamp.

## Diagnosis (revised)

Bounded commits already work (13k probe w1 16 seals → w8 5 seals, tps flat). Two different costs remain:

**A. Claim selection (only if a predicate is used).** `select_item_claim` + `metadata_equals` scans the entire eligible set. Gated P4 does **not** use a predicate, so this is off the G-path. Optional filtered-claim arm requires **I-select** first.

**B. Per-item apply/encode on the serial queue** (this program’s G-path):

1. `insert_pending` synthesizes JSON via `rehydrate_entity_document` (`lib.rs:1970`).
2. Dual `indexes` + `claim_indexes` inserts with `to_vec` keys.
3. `transition` re-encodes keys on Claim (P4 finalize path).
4. Hot records are `BTreeMap<String, TypedValue>` plus optional JSON.
5. `to_claimed` returns stored `entity_document` with **no** read-time echo (`lib.rs:265`). I2 must fix that before dropping write-time rehydrate.

## Iteration protocol

Every slice is one commit (or test+impl pair) and **must**:

1. Keep `cargo test -p fireweed-projection --lib` green.
2. Keep claim-echo / unique-index / `claim_by_query` tests green when the slice touches those paths.
3. Run SS smoke `SS_N=10000` and write `docs/perf/evidence/ss-phased/<utc>/summary.json`.
4. If the slice is meant to move a G-gate, run N=1M on the declared host (best-of-2) and append a ladder row.
5. Revert if smoke correctness fails, or if **any** of P1–P4 drops **>10%** vs the previous same-host 1M best-of-2 without a written reason.
6. Keep the 13k sealer probe runnable; do not treat its TPS as a gate.

Do not stack two apply-path slices in one commit.

## Work slices

### I0 — Harness + smoke (no engine change)

**In scope**
- `crates/fireweed/tests/ss_phased_capacity.rs` — **not ignored**. Default `SS_N=10000`. `SS_N=1000000` for capacity.
- Env: `SS_N`, `SS_PUSH_BATCH` (default 100), `SS_CLAIM_BATCH` (default 100), log path under `std::env::temp_dir()` (record path).
- Cell: `open_sqlite` only for gated runs (`SS_CELL` must be `sqlite--memory` or omit).
- Workers: 1. Pin payload lengths (512 / 1024).
- Evidence: versioned `summary.json` (schema in the test module header) + ladder append helper.
- Warmup 10k outside the clock.

**Out of scope:** engine changes; RESP; metadata_equals claim; 20-cell matrix.

**Verify**
```
SS_N=10000 cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture
```
exits 0; summary has four phases, exact counts, residual 0, sampled reads pass.

### I1 — Baseline on the declared host

N=10k then N=1M (best-of-2). No N=100k G-row. Classify the long pole. No code changes.

**Exit:** `docs/perf/evidence/ss-phased/ladder.md` with a `baseline` row including: utc, sha, host, loadavg, cpu model, disk model, fs, durability class, cell, `ordering_mode`, workers, N, push_batch, claim_batch, per-phase items/s, mutations/s, batch p50/p95/p99 per public call, wall_s, residual.

### I-select — Bounded predicate claim (not on G-path)

Required **before** any arm that uses `metadata_equals` or `claim_by_query` at N≥100k.

**In scope:** `select_item_claim` must not call `eligible_candidates(now, usize::MAX)`. Walk the eligibility index and **stop at `max` matches**, or use a `phase`/`claim_indexes` lookup that is rebuilt only from logged applies.

**Forbidden:** lazy build on first read; any flag that is not in the log.

**Verify:** unit test that claiming 100 of 1M matching-predicate items does not allocate a 1M id vec; existing `claim_by_query` tests; SS smoke unchanged (gated path does not use this).

### I2 — Lazy write-time echo, required read-time echo

**In scope**
- Stop calling `rehydrate_entity_document` in `insert_pending` when `entity_document` is `None`.
- **Must** echo at read time in `ItemRecord::to_claimed` (`lib.rs:265`) via `echo_entity_document`.
- **Must** key unique validation and query filters from `index_fields` when entity is `None` (`index_validate_with_entity`, `index_validate_update_with_entity`, `batch_update_preflight` empty-map comment at `lib.rs:4060`, `consider` early-returns at `lib.rs:4612` and siblings, `plan_bounded_mutation`).
- Snapshot: export may store `entity_document=None`; import may rehydrate for image compat or keep None if query paths use `index_fields`. Pick one and test export/import.

**Tests (required in the bead)**
- Push `index_fields` only → claim entity object equals today’s echo.
- Unique collision still rejected on `update_fields` / `BatchUpdate` of an index-fields-only item.
- `claim_by_query` / range scan still see index-fields-only items.

**Out of scope:** SQL projection unless sharing the helper is the only safe fix; changing echo JSON shape.

**Expected:** P1/P2 up. If P2 moves <5% on N=1M, write that and continue.

### I3 — Cache framed index keys (derived only)

Store framed keys on `ItemRecord` at insert. Reuse on claim-index remove / `transition`. **Never** use cached keys as unique-occupancy authority (always `indexes`).

**Invalidate/rebuild** on every arm that already recomputes keys: `UpdateFields`, `ReplacePending`, supersede, entity/index_field edits. Rebuild on snapshot import (do not persist cache). Note `ItemRecord` is `Clone` — keep the cache small; if clone shows up in P2 profiles, store keys in a side map keyed by `ItemId`.

**Verify:** projection lib + recovery; SS smoke; ladder `i3-cached-keys`; 13k probe still runs.

### I5 — Denser `index_fields` only if G1–G5 still miss

Intern declared field names; store values in declaration order. Public API remains `BTreeMap`. Log codec remains readable (rebuild map on decode) or is versioned.

Skip if G1–G5 already hold after I3.

### I4 — removed

Do not land lazy `claim_indexes`. If dual-insert still shows in the P1 profile after I2/I3, a **logged** “queue has no query indexes” skip of `claim_indexes` (definition is in the log) is a new slice, not I4 A/B.

## Measurement standard

Every ladder row includes the I1 field list. Compare slices to the **previous same-host N=1M best-of-2**, never to the 13k probe and never across N.

13k probe: optional quiet-host `w8/w1 ≥ 1.0` non-collapse. Not a G-gate.

## Risk and rollback

| Risk | Mitigation |
|---|---|
| I2 drops claim echo on `open_sqlite` | Read-time echo in `to_claimed` is in-scope and tested first |
| I2 skips unique / query | Native `index_fields` keying tests required |
| I3 stale keys | Invalidation matrix; cache never authoritative for unique |
| Host noise | Best-of-2 at N=1M; 10% any-phase revert |
| Quadratic filtered claim | Gated path has no predicate; I-select before any filtered arm |
| Infinite micro-opts | Attempt cap 6 engine slices (I2, I3, I5, plus at most 3 follow-ons). Then blocked-stop |

## Program stop / fail

- **Success:** G1–G5 on one N=1M best-of-2.
- **Stretch recorded:** success plus P4 ≥ 100k or wall ≤ 90 s.
- **Blocked:** 6 engine slices landed, G1–G5 still miss. Harness must print apply_ns vs other_ns (two `Instant` spans around `strategy.commit` / apply vs the rest of the phase). If apply_ns < 40% of the long-pole phase, stop and write “next program is log/encode or multi-queue.” If apply_ns ≥ 40%, one more named slice is allowed only with a profile-backed hypothesis. Then stop.

## Agent sequence

1. File beads I0, I1, I-select, I2, I3, I5 (I-select may proceed in parallel with I2 after I0; G-gates do not wait on I-select).
2. Execute I0, then I1.
3. Execute I2, measure, then I3 if needed, then I5 if needed.
4. Stop on G1–G5 or the blocked rule.

`ddx work` / `ddx try` on ready beads. Never I2+I3 in one attempt.
