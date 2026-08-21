# Claude review — round 16

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Reachable reads | Renew/Reassign `claimed_targets` bypasses the covered render path. | Exact-wait its Claim/candidate frontier before render; bypass applies to fence disposition, not read coverage. |
| BLOCKING | Actual append budget | The cited S3 timeout belongs to metadata, not record PUT. | Wrap the actual record append with a Fireweed-owned retry-inclusive deadline. |
| BLOCKING | Budget ownership | No slice creates the budget API S3c consumes. | Own it in S3p with a named test and keep S3c dependent. |
| WARNING | Acquisition bound | One append budget can understate K shared holders. | Derive acquisition from S3m measurements or bound K analytically. |
| WARNING | S3c regression | Exact blocking reads lack the S0 rate non-regression gate. | Add it and state pending/residual harness effects. |
| WARNING | Calibration scale | N=10k may understate the N=100k activation regime. | Calibrate at N=100k or permit scheduled re-calibration. |
| WARNING | Fault ownership | Conformance/probe expectations live outside S3p's file set. | Assign them to S3p or a dependent slice. |
| NOTE | Admission basis | The new cap is stricter than KeyedQueueGate and counts attachments. | Disclose the intentional basis. |
| NOTE | Follower wording | Normal durable followers transfer reservation ownership; they do not poison. | Reserve poison for publisher loss/ambiguity. |

### Round-15 audit

- **Reachable reads — UNRESOLVED:** `claimed_targets` remains uncovered.
- **Drain calibration — RESOLVED.**
- **Fence acquisition — RESOLVED** except its new bound derivation warning.
- **Packed disposition — RESOLVED.**
- **Reservation deadline — UNRESOLVED:** actual record PUT has no owned budget.
- **Admission — RESOLVED.**
- **Cross-queue apply — RESOLVED.**
- **Stale gate — RESOLVED.**
- **Residual queue gate — RESOLVED.**
- **Durable follower cancel — RESOLVED in intent.**

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

Most of the plan is now source-consistent. Convergence requires coverage for
Renew/Reassign validation and an owned timeout around the real record append,
plus the warning folds above.

Full raw session: `.ddx/agent-logs/svc-1787284300322477250.jsonl`.
