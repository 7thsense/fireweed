# Adversarial review: writer-contention recovery round 17

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the current plan, round-6 through round-16 Claude reviews, governing
artifacts, source release `v0.31.21`, evidence `1787274546`, and cited source.

Round 16 was folded by:

- adding Renew/Reassign `claimed_targets` to S3c: fence bypass never exempts its
  `render_claimed` validation read from exact Claim/candidate coverage;
- making S3p own a Fireweed outer `OBJECT_LOG_APPEND_TIMEOUT_SECS` around the
  actual filesystem/third-party-S3 record `packed_append` future, including its
  internal retries and metadata conflict loop; timeout is ambiguous append,
  poisons, forbids position reuse, and recovers from the authoritative log;
- adding `record_append_budget_wraps_actual_blob_put_and_all_retries`, with the
  default 30 s used in production, benchmarks, and tests, and keeping S3c
  dependent on S3p;
- deriving both acquisition and drain bounds separately from S3m measured p99
  (2×, next power of two, 500 ms floor, 5 s ceiling), capturing arbitrary
  concurrent shared holders rather than using a one-append analytic bound;
- making S3m run N=100k, so qualification scale cannot exceed a lower-scale
  calibration with no recalibration path;
- adding the S0 ≤10% rate non-regression gate to S3c and explicitly charging
  exact pending/residual coverage to settled wall instead of moving debt;
- splitting S3f after S3p to own the request-id probe and AC-TXN-4 expectations
  for poison→reopen/rebuild after durable unpublished append;
- disclosing that the 1,024 cap intentionally counts active/attached response
  channels and is stricter than the old queued-only gate;
- correcting normal follower wording: it transfers reservation/debt to the
  leader; poison is only for ambiguous append or publisher loss.

Concentrate on whether every round-16 blocker/warning is closed and whether any
new correctness, liveness, testability, or dependency defect remains. Treat any
slice that relies on a later correctness repair as blocking.

## Output contract

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | ... | ... | ... |
| WARNING | ... | ... | ... |
| NOTE | ... | ... | ... |

Use `None` for an empty severity. A WARNING must be folded or explicitly
accepted with concrete rationale before convergence.

### Round-16 audit

For every round-16 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
