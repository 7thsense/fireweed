# Claude review — round 6

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Compatibility key | The key omits caller-resolved `eligibility_at()`. One driver clock cannot preserve scheduled-epoch and ordinary claims. | Key on the exact resolved eligibility value; never substitute the driver's clock. |
| BLOCKING | Epoch fencing | A stale-epoch pre-check is TOCTOU and conflates `None` with `Some(epoch)`; the append is the real fence. | Key on exact `expected_epoch`, keep `None` distinct, and pass the shared expected epoch to packed append. |
| BLOCKING | Gate protocol | Joining after the existing exclusive non-reentrant `KeyedQueueGate` prevents bucket fill; inner `submit_operation`/`submit_commit` can deadlock. The Class-S path has no current spanning gate. | Specify join/election/gate order, acquire once, forbid nested acquisition, and identify the spanning ordering mechanism as new work. |
| BLOCKING | Gate scope | Holding an exclusive gate across selection, append, and apply serializes the P1 packing and Claim/Complete overlap on which current throughput depends. | Narrow exclusion to the selection/append ordering window and add early non-regression gates. |
| BLOCKING | M1 evidence | M1 is a ratio against `filesystem--memory` at N=100k, but no N=100k memory control is scheduled. | Run the same-SHA N=100k control or record an explicit OOM/abort disposition. |
| WARNING | Double linger | A coordinator linger plus object-log `PACK_LINGER` is additive. | Measure composed latency and force-seal a driver-supplied vector. |
| WARNING | S3 double-counting | Landed code already gives one leader a packed apply batch and coalesces several command types. | Name a delta that fails today: one driver-supplied multi-envelope vector through reservation, position slicing, seal, and apply. |
| WARNING | Claim SQL | Different requests retain distinct token, worker, and expiry values. | Bind token/worker/expiry per row and compare each result with a solo call. |
| WARNING | Complete atomicity | Packed Finalize must remain non-rejecting/idempotent after pre-append validation. | Test expiry between validation and apply without poisoning neighbors. |
| WARNING | Claim+Complete fusion | Eight Claims followed by eight Completes can lose existing adjacent fusion. | Record fused/unfused statement counts and compare exact model outcomes. |
| WARNING | Replay | Replayed request IDs must be resolved before partitioning or they consume new rows. | Resolve replay/fingerprint per request before selection and exclude replayed maxima. |
| WARNING | Cancelled waiter | A dispatched cancelled waiter leaves a durable lease whose token no caller received. | State that the lease stands until expiry/reclaim and test metrics. |
| WARNING | Evidence schema | New timing fields need a schema bump; `sampled_ok` and `residual_eligible` are currently literals. | Bump the schema and measure both fields. |
| WARNING | Unsupported scale ratio | P4 N=100k ≥50% of N=10k is not in the governing goal. | Add it to the goal or make it diagnostic. |

### Prior-blocker audit

- Log ordering — **RESOLVED**: selection is read-only and append precedes apply.
- Apply admission — **RESOLVED**: exact encoded debt is reserved after selection and before append.
- Performance boundary — **RESOLVED** for ack-versus-settled attribution; the schema/control gaps above remain.
- Acceptance ambiguity — **UNRESOLVED**: S3 double-counts landed work, S4 cites a gate that does not exist, and S7 lacks a termination rule.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The log-first protocol genuinely removes the previous commit-before-PUT and
under-reserved-debt faults. It is not yet safe to drive beads: exact eligibility
and epoch values must be part of compatibility; coordinator/gate ordering must
be non-reentrant and batchable; exclusion must not serialize measured packing
and Claim/Complete overlap; and the N=100k memory control must be scheduled.

Full raw session: `.ddx/agent-logs/svc-1787275124332498521.jsonl`.
