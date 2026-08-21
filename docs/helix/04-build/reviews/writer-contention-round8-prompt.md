# Adversarial review: writer-contention recovery round 8

Act as a specification enforcer, distributed-systems critic, and performance
engineer. Review the plan; do not implement it.

Read the plan, round-6 and round-7 Claude reviews, governing TD-010/ADR-013/
ADR-017/TP-005/goal artifacts, release `v0.31.20`, commit `0567e232`, evidence
`1787274546`, and the cited implementation paths.

Round 7 was folded as follows:

- only one coordinator driver exists per key; it exactly waits `last_claim`
  outside the exclusive selection fence, then exactly waits the drained
  candidate-mutation frontier inside it;
- exact waits do not use the current empty/not-ready shortcut; shared holders
  release after apply publication;
- S3 owns Push/Update/retry/reclaim shared-fence wiring in `turso_compose.rs`,
  exact waits, and force-sealed driver append; Complete bypasses;
- replay/fingerprint resolution moved after exact catch-up and S4 adds request
  identity/outcome to Class-S envelopes;
- S0 owns a same-SHA mixed producer/Claim/Complete baseline used by S4/S5;
- vector admission degrades to stable smaller prefixes; selection uses a
  committed reader and lightweight lengths before body materialization;
- S6 tests direct relational mixed vectors rather than claiming an unreachable
  mixed object-log lane;
- legacy outbox drain/schema survives one migration release after writes stop;
- S7 defines strict-log-order no-starvation behavior with the one-shot delay;
- the plan names the exact transition for the three superseded open beads and
  a parent-SHA failing test/check for every slice.

Verify Claim-vs-Claim disjointness, candidate ordering, replay, append-time
fencing, admission equivalence, cancellation, and shutdown under this exact
sequence. Check that `last_claim` and candidate frontiers have clear ownership,
monotonic update points, and recoverable behavior without a process-lifetime
item map. Check every implementation slice can land safely without relying on a
later correctness fix.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-7 audit

For every round-7 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
