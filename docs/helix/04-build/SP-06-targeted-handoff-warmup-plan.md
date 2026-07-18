---
ddx:
  id: build-sp06-targeted-handoff-warmup
  depends_on: [build-sp01-global-buffer-byte-admission, build-sp04-object-store-observability, build-sp05-maintenance-policy-execution, td-sharding-and-shard-ownership]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: verified_by, to: tp-scale-substantiation}
  review:
    self_hash: 732941914c2eeedacd664547e719a3ec45330304c37a06f6efa3d521bb0016ce
    deps:
      build-sp01-global-buffer-byte-admission: 6211670110ed7f75c2ffb82a3ba5bde0aad9573d7a1963266b93a2b42065a8f1
      build-sp04-object-store-observability: 8b8a380a443ed798b0fb8fe0a5fa9884e0ead76418df42af8ba99f910773b4ca
      build-sp05-maintenance-policy-execution: ebc977e1755ccf228635818ae7c76061ba0817ad6b473180e42fb194fea1b3a3
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-18T16:20:32Z"
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
| 0 | Define measurement protocol and SP-04 warmup operation class; extend E2-failover evidence schema | reproducible isolated snapshot deltas and scripted schedules |
| 1 | Profile cold handoff using dedicated-recorder instances: operation counts, bytes, recovery and first-claim latency | Identify reads covering >=70% of defined avoidable latency/bytes |
| 2 | Prototype bounded post-fence warmup for head hint, latest snapshot metadata/bytes, manifest replay range, and selected immutable tail segments | No payload-wide prefetch; authoritative validation and exact invalidation tests |
| 3 | Run queue-density, concurrent-handoff, stale-epoch, ownership-loss, and slow-store experiments | >=20% and >=50ms p95 gain when cold p95 >=25% of progress bound; <=2% steady-state cost |
| 4 | If gates pass, integrate generation-stamped cooperative jobs with owner lifecycle; otherwise remove prototype | memory stays within declared warmup budget |
| 5 | Record decision and update TD-003/TD-004/TP-002 | reviewed evidence |

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

## Validation Plan

- [ ] Warm and cold recovery produce identical projection image and high-water.
- [ ] Equivalence means identical logical high-water and row/state set, not byte-identical SQLite images.
- [ ] Stale/corrupt cached metadata is rejected and fetched authoritatively.
- [ ] Before SP-07, v2 cache authentication uses its manifest checksum plus format version; after SP-07 it uses
      content identity and invalidates v2-only entries on the writer-format transition.
- [ ] Cancelled/drained handoff releases every byte permit and task slot.
- [ ] Warmup completion after ownership loss produces zero usable cache insertions.
- [ ] 1000 active queues and concurrent reassignments remain within node bounds.

## Risks and Rollbacks

Warmup can pollute cache or amplify object traffic. Limit it to named recovery objects and compare physical
attempts/bytes. Disable by configuration or revert the isolated iteration; no durable format changes.

## Exit Criteria

Either a bounded targeted warmup meets every performance/safety gate and lands with an off switch, or a
negative spike records why pqueue should retain cold authoritative recovery.
