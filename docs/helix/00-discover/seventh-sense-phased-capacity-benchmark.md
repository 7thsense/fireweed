---
ddx:
  id: discover-seventh-sense-phased-capacity-benchmark
  type: analysis
  depends_on:
    - product-vision
    - prd
    - discover-first-principles-performance-model
    - api-workload-integration-profiles
  links:
    - {kind: informs, to: tp-fireweed-performance-matrix}
    - {kind: informs, to: tp-scale-substantiation}
  review:
    self_hash: 16b2187e923f0e0d5a09ee5aded3a71ef2343f8d7c77dbb470a686e5a358757e
    deps:
      api-workload-integration-profiles: 3c3dd594f1723e987015d4790634b1088016f5f41a049e661eba4b752cfb4c39
      discover-first-principles-performance-model: cda6f175ad5931d1307460863d730e5ca9ea8e4c9c247a5266386d4bcf8ccfdb
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      product-vision: 745a023af9f66c4b71312a0271dbea18b3947970eb47e051d4312bb6222befeb
    reviewed_at: "2026-08-15T00:19:55Z"
---

# Seventh Sense phased capacity benchmark

- **Status**: discovery spec; not a harness and not a release SLA
- **Date**: 2026-08-14
- **Replaces, as a *capacity question***: the snorri-shaped
  `claim_finalize_push_cycle` probe (19 typed indexes, 2.3 KiB, one
  claim+finalize+push counted as 1 TPS)
- **Does not replace**: TP-002 correctness / 10M / 1,000-queue evidence;
  TP-005 `million-cycle-v1` (insert / modify / read digest); the RESP
  black-box suite

The first-principles model
([first-principles-performance-model](./first-principles-performance-model.md))
said a high-performance single-node bar is ~100k completed ~1 KiB items/s
in batches of 100. The 13k number came from a different job: one hot
queue cycling 500-item claim/finalize/push with 19 indexes. This note
defines the benchmark that matches the **basic Seventh Sense worker
loop**, so capacity talk uses the same operations the product must run.

## Authority boundary

- Work definition: [PRD](../01-frame/prd.md) FR-44..FR-47a, FR-46
  (ingest then update schedule), API-003 Scheduled Batch Delivery.
- Mapping: `tenant_id` = account isolation; one queue per logical stream;
  `group_key` = `job_id`; `priority` / `not_before` = timestamps;
  `account` / `connector` / `campaign` stay in metadata. No Seventh Sense
  types in the engine.
- Public operations only: `BatchPush`, `BatchClaim` (with a metadata
  predicate), `BatchUpdate`, `BatchFinalize` (`release` / `complete`).
  No `claim_finalize_push_cycle`. No internal sealer API.
- Absolute rates are host-bound observations. Correctness is fail-closed.

## Why the 13k probe is the wrong question

| 13k probe | This benchmark |
|---|---|
| One fused cycle counted as 1 TPS | Four named phases, each reported |
| Claim 500 + finalize + push 500, one fsync | Worker batches of **100**, separate commits |
| 19 typed indexes, ~2.3 KiB from the first byte | Stub ingest, profile blob only after enrich |
| Never reads, never reschedules | Enrich, schedule, deliver are distinct |
| `w8 ≈ w1` on one queue is the goal | One queue is expected; rates are per phase |

Use the 13k probe to stop a sealer regression. Do not use it to decide
whether the engine is “fast enough” for Seventh Sense.

## Shape

**N** = 1,000,000 items (smoke **N = 10,000**).  
**Batch** = **100** for every worker loop. Ingest MAY use 100 or 1,000;
report the batch size with the rate.  
**Queues**: one timestamp-ascending queue (strict or bounded-relaxed).
`group_key` = `job_id` (N/100 distinct jobs, min 50).  
**Indexes**: at most **two** typed indexes (`job_id`, optional `phase`).
Not 19. Priority order is the timestamp model, not a secondary index.  
**Host**: declare the cell (`sqlite--memory` is the product comparison
cell; `memory--memory` is a serving ceiling only).

### Item

| Phase | Encoded record (approx.) | Contents |
|---|---|---|
| After ingest | **~400–600 B** | `client_item_key`, `job_id`, stub payload (~200 B), `phase=needs_profile` |
| After enrich | **~1.0–1.5 KiB** | plus opaque profile blob (~800 B–1 KiB) |
| After schedule | same | `priority` + `not_before` = delivery timestamp |
| After deliver | terminal | `complete` (no payload growth) |

That is the model’s **S → M** record, not the Snorri hot record.

### Phase token

Workers must not steal each other’s work. Carry `metadata.phase` (or a
single typed field) and claim with `metadata_equals`:

| Phase | Claim predicate | Mutation |
|---|---|---|
| P2 enrich | `phase=needs_profile` | `BatchUpdate` payload + `phase=needs_schedule`; `release` |
| P3 schedule | `phase=needs_schedule` | `BatchUpdate` `priority`/`not_before` + `phase=ready`; `release` |
| P4 deliver | `phase=ready` (due) | `BatchFinalize` `complete` |

Items start eligible. Enrich and schedule **release** so the item stays
pending. Delivery **completes**. Do not claim-to-delete on enrich.

`BatchUpdate` of known keys without a lease is a valid *producer*
enricher; it is a different benchmark. The default here is the worker
shape the user described: **pull 100, amend, put back, until none
remain**.

## Phases (timed separately)

Construction of the next batch (keys, profile bytes) stays **inside**
the phase clock. Queue create, 10k warmup, and teardown stay **outside**.

### P1 — Ingest 1M

`BatchPush` N stub items. `not_before` = now (eligible for enrich).
`priority` = ingest order or a dummy timestamp. Stable `client_item_key`.

**Report:** items/s, batch p50/p95/p99, accepted == N.

### P2 — Amend with profiles

Loop until a claim returns 0:

1. `BatchClaim` `max_items=100`, `phase=needs_profile`
2. `BatchUpdate` those ids: set profile payload, `phase=needs_schedule`
3. `BatchFinalize` `release` (same lease)

**Report:** items/s, claims/s, batch p50/p95/p99 for claim / update /
release, items updated == N, no double claim.

### P3 — Scheduler

Same loop, `phase=needs_schedule`:

1. `BatchClaim` 100
2. `BatchUpdate` `priority` and `not_before` to a delivery timestamp
   (default: all due now, so P4 is a pure drain; optional spread for a
   latency-under-schedule arm)
3. `release`

**Report:** items/s, batch percentiles, all N have `phase=ready`.

### P4 — Deliver by next-delivery-date

Loop until empty:

1. `BatchClaim` 100, `phase=ready`, timestamp order (earliest due first)
2. `BatchFinalize` `complete`

**Report:** items/s, claim+finalize p50/p95/p99, residual eligible == 0,
complete count == N, 0 duplicate leases.

A later arm MAY mix `retry` with `not_before` backoff (10%) to exercise
re-delivery. Default is 100% `complete`.

## What to publish

One row per phase, plus a rollup. Never a single “durable TPS”.

| Column | Meaning |
|---|---|
| `phase` | P1..P4 |
| `items` | items that finished the phase |
| `mutations` | P1: 1; P2: 3; P3: 3; P4: 2 |
| `items_per_s` | items / wall |
| `mutations_per_s` | mutations × items / wall |
| `batch_p50/p95/p99_ms` | per public call |
| `cell` / `host` / `batch` | topology |

Rollup wall is the sum of P1–P4. Rollup items/s is N / rollup wall
(one full lifecycle, not a fusion cycle).

### Honest H-server bands (from the primitive model)

These are **expectations for a lean apply**, sqlite log × memory
projection, batch 100, declared host. They are not gates.

| Phase | If apply is lean | If apply is still 13k-probe-expensive |
|---|---|---|
| P1 ingest (S) | 100–400k items/s | 20–50k |
| P2 enrich (claim+update+release) | 30–80k items/s | 8–15k |
| P3 schedule (same shape, small writes) | 40–100k items/s | 8–15k |
| P4 deliver (claim+complete) | 50–120k items/s | 10–20k |
| Full lifecycle N / wall | 1M in ~30–90 s | 1M in ~4–8 min |

P2/P3 are three public commits per 100 items unless a later design
coalesces update+release. That is the real worker cost; do not hide it
inside `claim_finalize_push_cycle`.

100k **completed deliveries/s** (P4 alone, 1 KiB, batch 100) remains the
high-performance *headline* from the model. This benchmark asks a
narrower, more honest question: **how long does one million Seventh
Sense actions take to ingest, enrich, schedule, and deliver?**

## Correctness (fail-closed)

- Accepted push count == N.
- Each phase consumes exactly the items the previous phase produced.
- 0 duplicate active leases; 0 finalize on a foreign lease.
- After P4: eligible depth 0, `complete == N`.
- Sampled live reads after P2 show the profile blob; after P3 show the
  delivery timestamp.
- Telemetry on. A crash or `Unavailable` fails the run.

## Explicitly out of scope

- 19 typed indexes, 2.3 KiB-from-ingest, claim-batch 500.
- `claim_finalize_push_cycle` and sealer `w8/w1` ratios.
- RESP-only black box (cannot express filtered claim, `release`, or
  `BatchUpdate` by lease).
- TP-005 `million-cycle-v1` digest (no claim/finalize).
- Group-cardinality / Marketo 300 / cohort `callback_id` (later arms).
- Recurring `jobs_queue` / `connectors_queue` (FR-55; later arm).
- Multi-queue density, 10M resident, object-log cost curves.

## Relationship to existing harnesses

| Artifact | Overlap | Gap this spec fills |
|---|---|---|
| `examples/python-resp` P01–P05 | 1M insert, update half, claim/ack chunks | No profile amend, no scheduler vs delivery, RESP cannot `release`/`BatchUpdate` under lease |
| `SS01` / `SS02` | Three queues, smoke correctness | N=5k default; not a capacity envelope |
| TP-005 `million-cycle-v1` | 1M insert + 500k update + read | No claim, no finalize, batch 1,000 |
| `sqlite_multi_worker_tps_probe` | Durable sqlite×memory | Wrong shape; keep as sealer ratchet only |

## Implementation sketch (not authorized by this note)

A later bead would add an in-process facade driver (preferred) next to
the TP-005 runner:

```text
cargo test -p fireweed --test ss_phased_capacity --release --features sqlite -- --nocapture
# SS_N=10000 for smoke; SS_N=1000000 on a quiet declared host
```

One queue, four timed phases, JSON/Markdown evidence under
`docs/perf/evidence/ss-phased/`. Cell default `open_sqlite`
(sqlite×memory). Optional second cell `open_sqlite_sqlite_projection`.

Do not fold this into the 13k scoreboard. New file, new columns.
