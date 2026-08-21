# Adversarial review: writer-contention recovery round 13

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the current plan, round-6 through round-12 Claude reviews, governing
artifacts, source release `v0.31.21`, evidence `1787274546`, and the cited
implementation paths.

Round 12 was folded by:

- replacing the boolean classifier with a normative three-way disposition for
  every `QueueCommand`, every `FinalizeKind`, mutation, schedule, payload, and
  gate disposition;
- classifying item, grouped, and cohort Pending-consuming claims as exclusive,
  wiring their existing append paths without moving them into the microbatch,
  and adding CohortClaim × item-Claim disjointness/no-poison coverage;
- splitting S3a into independently revertible S3a global lock/high-water and
  S3p force-seal/debt/owned-publication slices;
- making B3b depend on B3a and B3p before any fence holder can wait for owned
  publication, with B3c depending on B3p and B3b;
- explicitly testing Pause→Claim and CohortFinalize(Retry)→Claim ordering;
- preventing a fresh shard's Ready batch from passing any earlier same-shard
  Ready or Reserved entry, while retaining position-gap poison as a residual
  backstop;
- making coordinator `applied_high_water` the only live coverage authority in
  both exact waits and `snapshot_live_items`;
- putting `committed_deferred_reader_is_nonblocking_and_snapshot_stable` in the
  workspace `fireweed-turso` crate, naming its test command, and naming the
  standalone probe command;
- measuring realistic fidelity-restored response size, achieved pack fill, and
  byte-bound splits in S0/S5, without relaxing T2 when fill is below eight;
- assigning the bypassed `validate_claim_plan`, commit-outcome, and render
  invariants to a named S5 differential driver test;
- requiring fused grouped Claim+Complete to remove and re-elect its group
  summary through the solo-path helper.

Concentrate on whether every round-12 blocker/warning is closed and whether the
fold creates a lock cycle, unsafe bypass, untestable slice, or dependency gap.
Treat any slice that relies on a later correctness repair as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-12 audit

For every round-12 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
