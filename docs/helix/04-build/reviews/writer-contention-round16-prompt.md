# Adversarial review: writer-contention recovery round 16

Act as a specification enforcer and distributed-systems/performance critic.
Review the plan; do not implement it.

Read the current plan, round-6 through round-15 Claude reviews, governing
artifacts, source release `v0.31.21`, evidence `1787274546`, and cited source.

Round 15 was folded by:

- extending S3c's exact coverage to `select_eligible`/`eligible_candidates`,
  `claimed_view`/`render_claimed`, `live_items`, `planner_update_snapshot`,
  `peek`, every `pending*`, metrics, and terminal-emission metrics, while
  explicitly exempting synchronous atomic projection and authoritative-log
  metadata reads and deleting the dead derived snapshot helper;
- adding S3m after S3c/S3b to calibrate acquire/drain p50/p95/p99 with
  applied-high-water-only shadow Claim-serialized load; only S3m can configure
  S5, and a required bound above five seconds blocks activation;
- naming Tokio's fair, write-preferring FIFO RwLock, bounding acquisition by the
  retry-inclusive append budget plus linger/slack, metering acquisition, and
  gating both Claim-side and shared-side starvation;
- taking the maximum disposition within a mixed Finalize and across every
  command in a sealed vector, with S3b and S6 tests;
- defining retry-inclusive production append budgets: filesystem uses 30 s in
  benchmarks/production, S3 uses its whole-operation timeout including retries,
  and the reservation watchdog adds linger/slack;
- capping all queued/in-flight/attached Claim callers at the existing configured
  `max_queued_commits` (1,024), rejecting waiter 1,025 before planning with the
  named Backpressure resource, and reclaiming slots on completion/cancellation;
- disclosing the global apply worker/deque alongside global PUT serialization,
  limiting cross-queue independence to correctness, and recording
  `cross_queue_apply_wait_ms`;
- rewriting the stale structural gate against the reachable S3c reads;
- disclosing that grouped/cohort retains the old queue gate through apply/render
  after its selection fence releases;
- adapting both `AfterAppendBeforeApply` and durable follower cancellation in
  `publish_packed_apply` to the durable-poison rule.

Concentrate on whether every round-15 blocker/warning is closed and whether any
new correctness, liveness, fairness, testability, or dependency defect remains.
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

### Round-15 audit

For every round-15 BLOCKING and WARNING area, state `RESOLVED` or `UNRESOLVED`
with one sentence.

### Verdict

`APPROVE`, `REQUEST_CHANGES`, or `BLOCK`.

### Convergence

`YES` only with no BLOCKING findings and no unaddressed WARNINGs.

### Summary

Two to five sentences. Do not soften a blocking defect.
