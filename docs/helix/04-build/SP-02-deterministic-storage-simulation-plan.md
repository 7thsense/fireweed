---
ddx:
  id: build-sp02-deterministic-storage-simulation
  depends_on: [build-sp01-global-buffer-byte-admission, td-s3-object-log-sqlite-projection-mode, td-sharding-and-shard-ownership]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 822bd8ebd2a9e07bdec818a12eb2ea8c21a2feca965422830eae41a839a407c8
    deps:
      build-sp01-global-buffer-byte-admission: 5cfbe42a94ec4813e4855e431f0319152c6c8d11c5b081dcc77954a1ecf933b7
      td-s3-object-log-sqlite-projection-mode: f3ce514406d6394b25a637b03b4661e5cd112ef18dbb0d86b0a7d372526dfa4e
      td-sharding-and-shard-ownership: b3983f017f7907e900d79cfb08a8cd7ff66786835e66c5d2c1a87589a9db57db
    reviewed_at: "2026-07-18T19:52:55Z"
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

- [x] INV-1, INV-2, INV-10, INV-12, and INV-14 have executable storage predicates.
- [x] No acknowledged command disappears; no stale epoch commits; no visible prefix is deleted early.
- [x] Every executed durable boundary compares disposition, epoch/sequence, floor/horizon, visible IDs,
  acknowledgements, unknown outcomes, committed time, and physical deletion progress with the independent model.
- [x] The versioned seed corpus includes the previously fixed manifest watermark and group-seal races.
- [ ] Untargeted seed search rediscovers both historical bugs. The typed corpus detects both historical bugs
  and all required synthetic mutants; broader untargeted discovery remains a retained-spike condition.
- [x] Fake-store traces cover durable-success/lost-response, ambiguous create, CAS loss, and list staleness/incomplete pagination.
- [x] Command, environment, seed, trace schema, and result are recorded in the TP-003 verification ledger below.

## Spike Result and Evidence (2026-07-18): GO with conditions

The spike is retained. `pqueue-sim-support` has no dependencies and provides the runtime-free model,
seeded generator, stable trace renderer, invariant predicates, and deterministic shrinker. The
`deterministic_storage_simulation` adapter applies the same operations to the real synchronous
`SegmentedObjectLog` over a phase-addressed scripted versioned `BlobStore`; it compares model predicates and
rich recovered production snapshots after every executed seal, fence, floor advance, deletion, crash,
restart, and retry boundary. `FaultHook` interrupts the real production pipeline at before-segment,
after-segment, candidate-before-head, manifest-before-ack, owner-reassignment, and segment-expiry cuts.

Focused evidence on Rust 1.92, local in-memory object store, seed `0x5eed`, trace schema/harness v2:

- `cargo test -p pqueue-sim-support`: 5 passed, including distinct invariant negative controls, per-record
  floor/suffix behavior, 100 byte-identical replays, generated crash
  operations, and invariant-identity-preserving shrink.
- `cargo test -p pqueue-objectlog --test deterministic_storage_simulation`: 8 passed, including 128
  independently seeded 48-operation production/model traces with generated crashes.
- Typed JSONL corpus: committed-time manifest corruption; incomplete-delete plus stale compatibility-cache
  authority; stale writer; delete-before-advance; ambiguous-create retry; acknowledged loss; next-read hiding.
- Required INV-1, INV-2, INV-10, INV-12, and INV-14 predicates are distinct and non-vacuous. Named SUT/store
  mutants trip their expected invariant; GC progress has a separate identity. Minimized failures preserve
  identity and print schema, harness, seed, full trace, failing index, and <=32-operation minimized trace.
- Phase scripts record the target, result, durable effect, and key for segment, manifest candidate/head CAS,
  epoch head, floor/horizon, deletion, and paginated LIST. Ambiguous effect-then-error and CAS loss are
  asserted at the manifest-head phase; stale/incomplete pages drive real recovery.
- `scripts/ci/deterministic-simulation-suites.toml` is the bounded repeat-suite entrypoint. It contains no
  quiet-host benchmark or Tokio scheduler simulation.
- `repeat-suite.sh --count 100 --max-flaky-rate 0`: v2 passed 100/100, 0 failures, 0.000000 flaky rate,
  82.38 seconds elapsed (below five minutes), 101,068 KiB maximum resident set. Incremental target-dir
  growth was not isolated from the prewarmed shared workspace and remains a clean-CI measurement.
- After the final oracle/adapter reconciliation, the complete eight-test deterministic integration target
  passed another 100/100 process invocations with zero failures.

The separate TP-003 process-kill matrix, cross-host 100-repeat evidence, and long release seed campaigns
remain deferred to their existing release lanes. This iteration makes no claim about deterministic Tokio
scheduling, full multi-owner acquire-to-fence handoff, or lease-level INV-1 beyond the storage prerequisite
of one durable transition per request. The suite TOML is deliberately not added to a broad GitHub Actions
workflow; the exact local/release command above is the integration seam until CI cost and clean target growth
are measured. Therefore the spike is retained as a local GO-with-conditions, not a completed release gate.

## Risks and Rollbacks

A coupled oracle can certify the same bug twice. Keep model types separate and review transitions against
TD-003/TD-004. If the spike fails its gates, commit only the findings/decision and remove prototype code.

## Exit Criteria

A retained harness must be deterministic, mutation-sensitive, bounded in CI, directly reusable by SP-03 and
SP-07 (and transitively by SP-05 through SP-03), rediscover both historical bugs from untargeted seed search,
and detect at least one mutant class missed by existing enumerated tests. The local determinism,
mutation-sensitivity, reuse, and bounded-runtime gates pass. Untargeted rediscovery, cross-host repeat, and
clean target-growth gates remain explicit conditions before this becomes a release gate.
