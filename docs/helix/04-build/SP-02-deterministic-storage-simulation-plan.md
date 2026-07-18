---
ddx:
  id: build-sp02-deterministic-storage-simulation
  depends_on: [build-sp01-global-buffer-byte-admission, td-s3-object-log-sqlite-projection-mode, td-sharding-and-shard-ownership]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: a7e7545464a051bd23046ffbb1b0f04fece7c450eb071e593a82084a08ed66ff
    deps:
      build-sp01-global-buffer-byte-admission: 6211670110ed7f75c2ffb82a3ba5bde0aad9573d7a1963266b93a2b42065a8f1
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-18T16:20:32Z"
---

# Technical Spike Plan: SP-02 Deterministic Storage Simulation

## Scope

Spike, then retain only the minimum useful deterministic operation-level harness for seal, manifest CAS, shipped epoch fencing,
read-horizon advance, segment/manifest deletion, and restart. The harness models observable durable state;
it is not a production scheduler, a replacement for integration tests, or a proof of distributed consensus.

## Shared Constraints

- One seed determines the generated operation interleaving, clock, store responses, crashes, and retry outcomes.
  This spike does not claim deterministic Tokio task scheduling. It drives the synchronous production
  `SegmentedObjectLog` transition surface through a test adapter and models async boundaries as explicit
  operations/cut points. `async_commit` coordination is excluded from production mutation claims in this
  iteration and represented only by its mapped durable cut points. No production spawn/time seam or
  simulation runtime dependency is introduced.
- Every failure prints a replayable seed and compact operation trace; shrinking preserves the violated invariant.
- The model oracle is independent of production transition code.
- Put the pure model in a test-support crate that cannot depend on `pqueue-engine` or `pqueue-objectlog`;
  it defines its own minimal identifiers/state rather than importing domain transition types. Production code
  and its adapter are the system under test.
- Named cut points sit at durable boundaries, not arbitrary source lines.
- Extend/map the existing `FaultCutPoint`, `HybridFaultCutPoint`, `RawCommitFault`, and `FlushPhase`
  vocabulary. A new cut point requires a documented missing durable event, not a parallel taxonomy.
- The fake versioned `BlobStore` models success, failure-before-effect, durable-effect-then-error, ambiguous
  create, CAS loss, and stale/incomplete list results using the existing store trait surface.
- CI runs a bounded seed corpus; long campaigns remain explicit release/local evidence and avoid quiet-host tests.

## Implementation Slices

| Slice | Change | Gate |
|---|---|---|
| 1 | Map existing fault vocabularies to TD-004/AC-TXN-4 and add a TP-003 deterministic-model section | Model crate dependency graph is mechanically independent; relation to process-kill harness is explicit |
| 2 | Prototype deterministic clock, versioned fake `BlobStore`, production transition adapter, and trace replay | Same seed produces byte-identical trace on 100 reruns locally and on one CI host |
| 3 | Encode seal/CAS/shipped fencing/horizon/GC transitions and invariants | Mutants in the production adapter/transition layer—not the model—are detected |
| 4 | Add shrinking/delta-debugging and a versioned checked-in regression corpus | Seeded mutant traces shrink to at most 32 operations |
| 5 | Integrate a small CI seed set through `repeat-suite.sh` with an explicit zero-flake threshold and record evidence | <=5 minutes wall time and <=1 GiB incremental target-dir usage; zero nondeterministic reruns |

## Issue Decomposition

This is evidence-gated. Stop after slice 2 if replay is not deterministic across local and CI hosts. Stop
after slice 3 if the independent oracle cannot detect mutants injected into the system-under-test adapter or
production transition layer. Required mutants reconstruct the historical group-seal `committed_at` race and
stale manifest-deletion-watermark cache, plus synthetic stale-writer and delete-before-advance faults. Scope
epoch handoff to the shipped single-owner plus manifest-CAS fencing surface; full acquire-to-fence handoff is
forward-looking model coverage and carries no production mutation-detection claim.

SP-02 is deterministic model-based DST supporting AC-TXN-4. It feeds, but does not replace, TP-003's separate
process-kill `fault_injection_harness_tests`: the model searches/shrinks durable schedules; the process harness
replays selected schedules against process boundaries. Corpus entries carry a schema version, seed, operation
trace, expected violated invariant, and minimum compatible harness version.
The fake store tracks versions internally while exposing only the existing create-only CAS behavior; ambiguous
success is represented as a durable effect followed by `Err` for recovery resolution.

## Validation Plan

- [ ] INV-1, INV-2, INV-10, INV-12, and INV-14 have executable predicates.
- [ ] No acknowledged command disappears; no stale epoch commits; no visible prefix is deleted early.
- [ ] Restart at every named cut reconstructs the model-equivalent durable prefix.
- [ ] Seed corpus includes the previously fixed manifest watermark and group-seal races.
- [ ] The harness detects at least one generated mutant class not covered by the hand-enumerated segment-commit tests.
- [ ] Fake-store traces cover durable-success/lost-response, ambiguous create, CAS loss, and list staleness.
- [ ] Command, environment, seed, trace schema, and result are recorded in the TP-003 verification ledger.

## Risks and Rollbacks

A coupled oracle can certify the same bug twice. Keep model types separate and review transitions against
TD-003/TD-004. If the spike fails its gates, commit only the findings/decision and remove prototype code.

## Exit Criteria

A retained harness must be deterministic, mutation-sensitive, bounded in CI, directly reusable by SP-03 and
SP-07 (and transitively by SP-05 through SP-03), rediscover both historical bugs from untargeted seed search,
and detect at least one mutant class missed by existing enumerated tests. Otherwise the spike records a
negative decision and removes prototype code.
