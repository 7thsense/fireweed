---
ddx:
  id: build-sp08-object-log-economics-composed-faults
  depends_on:
    - product-vision
    - prd
    - td-s3-object-log-sqlite-projection-mode
    - td-sharding-and-shard-ownership
    - build-sp02-deterministic-storage-simulation
  links:
    - {kind: informed_by, to: product-vision}
    - {kind: informed_by, to: prd}
    - {kind: informed_by, to: td-s3-object-log-sqlite-projection-mode}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: informed_by, to: build-sp02-deterministic-storage-simulation}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  status: complete
---

# SP-08: Object-Log Economics and Composed-Path Fault Simulation

## Goal

Make two existing fireweed capabilities explicit and testable:

1. Extend the TP-002 E3 cost model with a workload-driven object-granularity model. Inputs are active queue
   count, per-queue command rate, downstream primitive batch size, encoded command bytes,
   `segment_target_bytes`, `segment_max_latency_ms`, starting lifetime recovery-index entries, and retention window. Physical
   PUTs per seal are derived from the current authority-head algorithm; payload storage and request cost use
   cited prices,
   retention window, and cited request/storage prices. Outputs include commands and bytes per segment, fill
   ratio, seal trigger, objects and PUT cost per month, ingress bytes, and retained-log bytes.
2. Extend SP-02 from object-log transitions to the complete queue-owner acquire -> durable epoch fence ->
   serving -> loss/reassignment path. Deterministic schedules must prove stale-owner rejection, request replay,
   exact recovery, and unrelated-queue progress.

This plan changes no queue semantics or production architecture. The durable substrate remains one
S3-compatible object store. Striping, quorum replication, cross-region replication, and durability stronger
than S3 are out of scope by operator decision (2026-07-21).

## Scope

In scope are a deterministic cost calculator, a generated assumptions artifact, composed-path fault tests,
and traceability updates. Production protocol, storage topology, and durability semantics do not change.

## Shared Constraints

- Preserve the product vision's batch-centric contract and tunable latency/cost dial.
- Preserve PRD P0-11..16, FR-9..12, FR-23..35, and FR-43: exact outcomes, bounded progress under ordinary
  load, one active lease, queue-as-shard ownership, and backend-independent transaction integrity.
- Measured E3 request/byte counters remain release authority. Workload-derived curves are labeled modelled
  sensitivity and cannot replace live evidence.
- Object granularity is an economic and operational optimization. It must not weaken the manifest commit
  boundary, response barrier, request-id replay, epoch fencing, or bounded byte admission.
- Fault simulation uses explicit logical time and named durable boundaries. It must not use host timing,
  quiet-host assumptions, or absolute performance thresholds.
- A queue whose fence fails before effect must not stop a second queue from committing and advancing before
  the failed queue retries.
- Sealed-generation backend migration is explanatory follow-up only. It requires a separately reviewed ADR or
  technical design before implementation.

## Work Breakdown

### Implementation Slices

| Slice | Area | Governing artifacts | Depends on | Validation gate |
|---|---|---|---|---|
| SP08-1 | Workload-driven object granularity calculator and report | Product Vision, PRD goal 6/P0-16, TD-004 | None | `cargo test -p fireweed-release` |
| SP08-2 | Deterministic acquire-to-fence/reassignment simulation using real control-plane and object-log transitions | PRD P0-11..15, ADR-008, TD-003, TD-004, SP-02 | None | `acquire_and_fence` returns no session before durable fence/confirmation; focused suite and Clippy pass |
| SP08-3 | Traceability and release evidence integration | TP-002 E3, TP-003 section 3.11 | SP08-1, SP08-2 | `ddx doc validate`; focused release and simulation suites |

## Issue Decomposition

| Area | Goal | Non-scope |
|---|---|---|
| Economics | Derive object count and cost from active queues, per-queue rate, downstream batch size, encoded command size, target bytes, latency bound, lifetime recovery-index growth, and PUT amplification; show production defaults and sensitivity cases | Choosing one universal segment size; replacing measured E3 evidence |
| Fault simulation | Exercise acquire, pending fence, durable fence confirmation, serving, owner expiry, reassignment, stale append, retry, recovery, and unrelated-queue progress | A deterministic Tokio runtime; consensus proof; new production coordination |
| Sealed generation | Document what the paper mechanism does and when fireweed might need it | Implementation or architecture approval |

## Validation Plan

- [x] Granularity math has unit tests for size-triggered, latency-triggered, low-rate one-command, and invalid
  input cases.
- [x] Generated economics output states every input, includes the PRD 1,000-active-queue density shape, and distinguishes measured evidence from modelled curves.
- [x] Production defaults (`262144` bytes, `20` ms) appear in the model and match `env_config.rs`.
- [x] Deterministic acquire-to-fence tests use logical timestamps and real `InMemoryControlPlane` plus
  `SegmentedObjectLog` fencing/append operations.
- [x] Fault schedules cover failure-before-fence, effect-then-error/unknown response, owner expiry,
  reassignment, stale epoch rejection, retry convergence, and exact reopen.
- [x] A second queue commits before the failure-before-effect queue retries; the effect-then-error fence
  resolves to one confirmed session and a same-owner retry preserves it.
- [x] No GitHub Actions workflow is added; bounded local/release suite entrypoints are used.

## Open Questions

None block this plan. Any later sealed-generation migration proposal must separately answer its switching
authority, rollback, compatibility, and progress-bound questions in an operator-reviewed ADR or technical
design before implementation.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|---|---|---|---|
| Model implies false precision | High | Emit fixed-batch/regular-arrival assumptions and formulas; expose batch overshoot and index height; label curves sensitivity; retain measured E3 rows as authority | Remove derived curve section without changing counters |
| Simulator shares logic with the SUT | High | Assert control-plane state, durable epoch, returned session, and recovered history at each named boundary; keep SP-02's separate generated model/mutant suite | Retain only accurately scoped enumerated integration claims |
| Fault suite becomes slow or flaky | Medium | Logical time, fixed seeds, bounded traces, no external services | Keep corpus local/release-only and reduce bounded seed count |
| Granularity optimization creates one-object-per-command at low rate | Medium | Show the crossover explicitly; treat the latency bound as the operator-selected economic tradeoff | Preserve current defaults and report the cost |

## Exit Criteria

- [x] Operators can answer, from explicit inputs, how command size and arrival rate determine segment size,
  object count, retained bytes, and S3 request cost.
- [x] Measured E3 cost rows and workload-derived sensitivity curves are visibly distinct and reproducible.
- [x] The acquire-to-fence/reassignment path has deterministic fault coverage with stale-owner rejection,
  exact recovery, request replay, and unrelated-queue progress.
- [x] S3 remains the sole required durability boundary; no stronger durability profile is introduced.
- [x] Source, tests, plan, and TP-003 traceability pass focused validation.
