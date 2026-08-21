# Adversarial review: writer-contention recovery round 10

Act as a specification enforcer, distributed-systems critic, and performance
engineer. Review the plan; do not implement it.

Read the plan; round-6 through round-9 Claude reviews; governing TD/ADR/test/
goal artifacts; source release `v0.31.20` plus `0567e232`; evidence
`1787274546`; and all cited implementation paths.

Round 9 was folded by:

- adding prerequisite S-1 to restore Class-S fields/metadata/entity response
  fidelity and requiring S0 to establish the only valid performance baseline;
- re-reading and exactly waiting queue `last_claim` inside the exclusive fence
  before replay/selection, with prior apply amortized across up to 800 items;
- enforcing metadata-permit→produce-lock order, a permit-held high-water helper,
  seal-time epoch check, and concurrent epoch/produce tests;
- adding a coordinator-owned active-driver registry and product teardown
  `close_and_drain`, independent of the best-effort object-log dispatcher drain;
- marking new authority-first Claim commands and requiring every named row to
  move from Pending before token/bearer effects; legacy outbox commands default
  to migration behavior;
- making every Update take the shared fence;
- bounding only pre-append fence work, cancelling reservation on expiry, and
  timing uninterruptible append separately;
- using a dedicated `read_uncommitted=OFF` deferred selection connection with a
  held-writer non-blocking test;
- rejecting requests below 50% remaining lease duration before append;
- separating command/debt admission from a 4 MiB aggregate rendered-response
  prefix bound, with solo fallback;
- keeping T2/B8 open unless the goal passes or is explicitly amended/reviewed.

Verify the lock graph, in-fence frontier ordering, fidelity-first baseline,
authority-first migration coexistence, real shutdown ownership, and safe
S-1→S0→S2/S3→S4→S5 cutover order. Treat any slice that needs a later
correctness fix as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-9 audit

For every round-9 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
