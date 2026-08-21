# Adversarial review: writer-contention recovery round 7

Act as a specification enforcer, distributed-systems critic, and performance
engineer. Review the plan; do not implement it.

Read:

1. `docs/helix/04-build/writer-contention-recovery-plan.md`
2. `docs/helix/04-build/reviews/writer-contention-round6/claude.md`
3. `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
4. `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
5. `docs/helix/02-design/adr/ADR-017-async-commit-strategy-and-dispatch.md`
6. `docs/helix/03-test/test-plans/TP-005-fireweed-performance-matrix.md`
7. `docs/helix/04-build/ss-objectlog-turso-memory-goal.md`
8. `docs/releases/v0.31.20.md`, commit `0567e232`, evidence
   `docs/perf/evidence/ss-phased/1787274546/summary.json`, and the implementation
   paths cited by the plan/review.

Round 6 requested changes for exact `eligibility_at` and `expected_epoch`
compatibility; append-time fencing; coordinator-before-fence ordering; avoiding
the existing non-reentrant gate; preserving producer packing and
Claim/Complete overlap; and scheduling the N=100k memory control. The plan now
introduces a weak/reclaimed shared-producer/exclusive-Claim selection fence that
ends after durable append publication, before apply. Verify that this actually
linearizes candidate selection with Push/Update/retry/reclaim while allowing
Complete and ordinary producer packing to overlap.

Also verify every round-6 WARNING was folded: composed linger, a present-code
failing S3 delta, per-row lease values, non-rejecting Complete, fusion counts,
pre-selection replay, cancelled durable leases, evidence schema v4, diagnostic
scaling ratio, exact termination, and old-path outbox scoping.

Pressure-test bead readiness. Each slice must be independently implementable,
have a test that fails before it, and avoid relying on a later slice for its own
correctness. Cite plan sections and repository paths/symbols for every BLOCKING
or WARNING finding.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` when a severity has no findings. A WARNING must be fixed or accepted
with concrete rationale before convergence.

### Round-6 audit

For each BLOCKING and WARNING area from round 6, state `RESOLVED` or
`UNRESOLVED` with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only when there are no BLOCKING findings and no unaddressed WARNINGs.
Otherwise `NO`.

### Summary

Two to five sentences. Do not soften a blocking defect.
