# Adversarial review: writer-contention recovery round 11

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the plan, round-6 through round-10 Claude reviews, governing artifacts,
source release `v0.31.20` plus `0567e232`, evidence `1787274546`, and cited
implementation paths.

Round 10 was folded by:

- globally enforcing metadata-permit→produce-lock for every object-log produce
  path, with permit-held high-water and concurrent Complete/acquire/produce
  tests;
- making coordinator `applied_high_water` the sole live exact-wait authority,
  seeded after recovery and coupled to RAII publish-or-cancel reservations;
- replacing enumerated fence membership with exhaustive no-wildcard
  `affects_candidates(QueueCommand)` plus SetGates/Purge/reclaim tests;
- enforcing authority-first full-row movement in normal and fused
  Claim+Complete apply;
- adding S4b `ObjectLogTursoLifecycle` and a flavor-safe Drop→close_and_drain
  bridge owned by `lib.rs`/`turso_compose.rs`;
- explicitly measuring the existing global cross-queue PUT lock;
- stamping expiry from requested duration at seal time;
- bounding the deferred committed snapshot (p99 <100 ms, ≤16 MiB WAL growth),
  testing stability, and closing it before admission/I/O;
- making all existing pack leaders owned/drop-safe and adding S3 non-regression;
- shaping S-1 full-row mapping as the reusable bulk helper consumed by S5.

Verify global lock ordering, exact-wait liveness, exhaustive classifier coverage,
fused integrity, lifecycle shutdown, snapshot/lease semantics, and safe slice
order. Treat any slice that needs a later correctness fix as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-10 audit

For every round-10 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
