# Claude review — round 15

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Reachable reads | Claim render and grouped/cohort eligibility reads still lack exact coverage. | Cover `claimed_view`/`render_claimed` and `select_eligible`/`eligible_candidates`; classify every remaining port read. |
| BLOCKING | Drain calibration | S0 measures before S3c removes false-coverage shortcuts. | Calibrate exact applied-high-water waits after S3c and before S5. |
| WARNING | Fence acquisition | Shared traffic can starve exclusive Claim without a stated discipline/bound. | Use fair writer-preferring admission, meter acquisition, and gate Claim starvation. |
| WARNING | Packed disposition | A sealed vector can mix bypass and shared commands. | Take the maximum disposition across the vector and test it. |
| WARNING | Reservation deadline | Filesystem has no declared retry-inclusive production append deadline. | Define it for benchmark/production and derive the watchdog from it. |
| WARNING | Admission | Coordinator batch limits do not bound queued callers. | Add a global waiter limit and named backpressure rejection. |
| WARNING | Cross-queue apply | One global apply worker serializes unrelated queues. | Disclose and meter cross-queue apply wait; make independence correctness-only. |
| WARNING | Stale gate | Validation cites a helper S3c deletes. | Rewrite the gate against reachable reads. |
| NOTE | Residual queue gate | Group/cohort still holds `KeyedQueueGate` through apply/render. | Disclose the residual serialization. |
| NOTE | Durable follower cancel | A follower cancel site also cancels after append. | Adapt it in S3p with the fault path. |

### Round-14 audit

- **Classifier aggregation — RESOLVED.**
- **Exclusive fairness — RESOLVED**, with acquisition fairness still open.
- **Reachable reads — UNRESOLVED.**
- **Pre-append budget — UNRESOLVED** because calibration precedes S3c.
- **Reservation timeout — UNRESOLVED** for filesystem/retries.
- **Byte-bound suffix — RESOLVED.**
- **Provisional-pack retirement — RESOLVED.**
- **S5 revert unit — RESOLVED.**

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The previous folds are sound. Exact coverage must include Claim render and
group/cohort selection, calibration must move after S3c, and the listed
fairness/admission/serialization warnings must be closed.

Full raw session: `.ddx/agent-logs/svc-1787283299809658513.jsonl`.
