# Adversarial review: writer-contention recovery round 9

Act as a specification enforcer, distributed-systems critic, and performance
engineer. Review the plan; do not implement it.

Read the plan; round-6, round-7, and round-8 Claude reviews; governing
TD-010/ADR-013/ADR-017/TP-005/goal artifacts; source release `v0.31.20` plus
commit `0567e232`; evidence `1787274546`; and cited implementation paths.

Round 8 was folded by:

- replacing exact-`now` ordinary keys with an ordinary lane: up to eight FIFO
  bounded SELECTs on one committed snapshot preserve each request's exact
  `now`; scheduled claims still key on exact explicit eligibility epoch;
- making `last_claim` queue-scoped across keys, seeded only after open/epoch/
  recovery catches projection up to authoritative tail;
- assigning S3 a seal-time `Option<epoch>` check under the per-shard metadata
  permit held through PUT and a shard/epoch/lane-scoped force-seal API;
- running the elected driver as owned/drainable work with duplicate request-ID
  attachment and drop-safe append+publish;
- prewaiting candidate frontier outside the fence, bounding in-fence delta wait
  at 500 ms/three attempts, and instrumenting hold causes/starvation;
- using a dedicated committed selection connection while preserving the shared
  `read_uncommitted=ON` reader;
- keeping the provisional serving pack through additive S2/S3 and landing
  set-based Claim apply in S4 before log-first cutover in S5;
- repairing `produce_caught_up`, extending retry/reclaim frontier recording,
  retaining migration drain, and asserting `TokenOp::Set` at cutover.

Verify public outcome equivalence for the ordinary-lane multi-SELECT algorithm,
queue-wide Claim disjointness, lock ordering at seal-time fencing, owned-driver
cancellation/shutdown, bounded convoy behavior, and safe slice ordering. Treat
any slice that requires a later correctness fix as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-8 audit

For every round-8 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
