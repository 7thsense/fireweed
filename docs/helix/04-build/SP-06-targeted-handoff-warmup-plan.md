---
ddx:
  id: build-sp06-targeted-handoff-warmup
  depends_on: [build-sp01-global-buffer-byte-admission, build-sp04-object-store-observability, build-sp05-maintenance-policy-execution, td-sharding-and-shard-ownership]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: verified_by, to: tp-scale-substantiation}
  review:
    self_hash: 676d83770b1b88a293c614f6886fdc524cdd898c5200d9dd3e4d307197e4f42e
    deps:
      build-sp01-global-buffer-byte-admission: 97d1032e2b1bbd9ecae2df5daed4350d88364b2bb4d9e7b3c643677f665d8280
      build-sp04-object-store-observability: b75fdf641cb7d51d5baedf66abe2569b7ae19d2722fec456710c887204508706
      build-sp05-maintenance-policy-execution: 1d89282c8fae482f99334b909d45dea15768f6b4ab5ddf7dd57180092e19d8e9
      td-sharding-and-shard-ownership: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
    reviewed_at: "2026-07-20T19:53:12Z"
---

# Technical Spike Plan: SP-06 Targeted Ownership-Handoff Warmup

## Scope

Measure and, only if justified, implement targeted prefetch of metadata and bounded replay inputs needed by a
new queue owner. This is not a generic block cache and must not warm payload segments unrelated to the first
serve/claim path. Profiling may begin after SP-04; prototype behavior waits for SP-01 budget types and SP-05
maintenance interactions.

## Shared Constraints

- Default trigger is post-fence, pre-serve, pipelined through the existing ownership hydration seam. Speculative
  pre-acquire/target-owner warmup is excluded unless a separate experiment and design review proves it.
- Safety never depends on warm data; epoch fence publication and authoritative recovery precede serving.
  Mutable head/horizon metadata is only a hint validated by an authoritative post-fence read. Immutable
  snapshot/segment bytes enter recovery only after checksum/content-identity match against that authoritative
  manifest; warm misses fall back to the normal read.
- Warmup has byte, object, concurrency, and deadline budgets and yields immediately to foreground recovery.
- Admission uses a separate instance of SP-01's runtime-neutral budget/permit types, never the command-byte
  budget. Config declares nonzero warmup bytes, object, concurrency, and deadline caps; foreground recovery
  skips warmup immediately under budget pressure.
- Cache identity reuses SP-03 typed queue/object class, command sequence, manifest index/head version, epoch,
  content identity, and format version. SP-07 v2/v3 changes invalidate by format/content identity.
- Metrics compare cold and warm ownership handoff with identical durable state and fault schedule.

## Implementation Slices

| Slice | Change | Gate |
|---|---|---|
| 0 | **Complete:** define measurement protocol and extend E2 failover evidence to optional schema-v2 `handoff_object_store_profile` | isolated SP-04 snapshot deltas reconcile with named requests |
| 1 | **Complete:** profile clean and one-unapplied-tail handoff with dedicated recorders | 200 samples × two queue sizes × 25/100 ms schedules |
| 2 | **Stopped by gate:** immutable candidates are identifiable, but projected relative p95 gain is below 20%; do not prototype | no payload cache or dormant warmup code |
| 3 | **Not applicable:** no intervention exists to compare against the numeric adoption gates | no production intervention to benchmark |
| 4 | **Negative integration decision:** prerequisites are absent and cache benefit gate failed | production behavior unchanged |
| 5 | **Complete:** record decision and update TD-003/TD-004/TP-002/roadmap | reviewed evidence |

## Issue Decomposition

The default outcome is a spike. Scripted arms fix `progress_bound_ms=2000` before measurement. The harness runs at least 200 handoffs per arm on two queue sizes against a
scripted S3-compatible store at 25ms and 100ms request latency, plus the existing live/local evidence arm where
available. "Avoidable" means an immutable, identity-addressable object read whose bytes were durable before the
fence and are fetched again on cold recovery. Isolated runs use before/after SP-04 pull-snapshot deltas; mass
handoff runs compare total recorder deltas per arm and harness-recorded per-request first-claim latency, never
tenant/queue metric labels. Scripted `FaultCutPoint`/`DuringOwnerReassignment` schedules and E2-failover records
are the baseline comparison mechanism; SP-02 can search/shrink additional schedules but is not required.

Production code lands only when every numeric gate passes and does not add a per-queue task. Reuse SP-01 budget
types, SP-03 identities, SP-04 recorder, `ResidentQueues`-style LRU bounding, and the shared dispatcher.
"Shared dispatcher" means the node-global bounded background-work scaffold introduced by SP-05, not the
commit-specific `OwnedTaskDispatcher`. Cooperative jobs carry queue epoch plus a local owner generation;
generation invalidates unwind/drain within the same epoch, while epoch protects durable ownership. Acquire unwind or `renew_sessions` drain invalidates
the generation. A late completion inserts nothing usable and releases all permits. Negative evidence is a
valid completed iteration.

## Spike Evidence and Decision

Command: `cargo test -p pqueue-server sp06_full_handoff_profile_classifies_metadata_and_required_tail -- --ignored --nocapture`.
The final explicit matrix passed in 45.75 seconds. Each arm ran 200 handoffs for 256 and 1,000 resident items.

| Arm | 25 ms total / p95 | 100 ms total / p95 | Physical requests | Immutable GETs | Avoidable/repeated GETs | Tail replay |
|---|---:|---:|---:|---:|---:|---:|
| Clean SQLite high-water | 5,667,500 / 52,950 ms | 22,670,000 / 211,800 ms | 226,700 | 20,300 | 20,100 / 20,099 | 0 |
| One unapplied segment per handoff | 8,687,500 / 81,475 ms | 34,750,000 / 325,900 ms | 347,500 | 40,600 | 40,400 / 39,999 | 200 commands |

Queue item count did not change object-read shape because resident state and first selection are local to the
SQLite projection; this matrix does not measure active-queue density. Content-addressed manifest candidates
make the avoidable share exceed 70%. Every immutable tail GET named a distinct required segment and fed
exactly one replay. At 25 ms, perfect removal of avoidable reads projects clean p95 from 52,950 to 48,200 ms
(4,750 ms, 8.97%) and tail p95 from 81,475 to 71,950 ms (9,525 ms, 11.69%); the same relative gains apply at
100 ms. Identification, cold-latency, and absolute-gain gates pass, but both arms fail the required 20%
relative gain, so no prototype was created.

The ignored deterministic BlobStore harness uses a single-page listing model and prints its reproducible
matrix; it does not emit or claim live E2 evidence. The live TP-002 script emits schema v2 with a null handoff
profile because the modeled negative spike rejected the intervention. Keeping the harness in an isolated test module avoids
coupling production fixtures to this deliberately manual, 200-handoff matrix.

The request totals expose a different problem: authoritative head reads walk retained version history, so
post-fence metadata work grows with queue lifetime. That mutable authority cannot be served from a warm hint.
Amend the design separately toward constant-time conditional-head access and full async bounded-parallel
required-tail recovery. SP-05 did not introduce the node-global dispatcher assumed by this plan; current
hydration also executes synchronous object-store/SQLite work behind a ready future. Those prerequisites block
any future warmup integration independently of this negative cache result.

## Validation Plan

- [x] Clean and one-tail recovery preserve logical projection state and high-water.
- [x] Equivalence uses logical high-water and row/state behavior, not byte-identical SQLite images.
- [x] No cache exists; mutable metadata remains authoritative on every handoff.
- [x] Cache authentication is N/A because the profiling gate rejected the cache before a prototype.
- [x] No warmup permits, tasks, or cache insertions were introduced.
- [x] The 1,000-item arm has the same bounded recovery object shape as the 256-item arm.

## Risks and Rollbacks

Warmup can pollute cache or amplify object traffic. Limit it to named recovery objects and compare physical
attempts/bytes. Disable by configuration or revert the isolated iteration; no durable format changes.

## Exit Criteria

**Met by negative spike.** pqueue retains cold authoritative recovery. Immutable candidates were identified,
but projected relative p95 gain was only 8.97% to 11.69%, below the 20% adoption gate. No live comparison is
applicable because no intervention landed, and production contains no dormant warmup code.
