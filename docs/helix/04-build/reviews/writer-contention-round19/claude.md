# Claude review — round 19

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Admission map | Most derived Turso append sites bypass KeyedQueueGate. | Replace the uniform lock story with the actual per-site admission/fence map. |
| BLOCKING | Validation isolation | Coverage does not make a read-uncommitted three-statement render coherent. | Use asserted committed isolation and one snapshot/single statement for validation. |
| WARNING | Strict commit | Coverage Backpressure can become a per-entry rejection; writer `commit_validate` is unclassified. | Fail retryably before outcomes and cover the transactional writer validation. |
| WARNING | Watchdog text | Structural formula is stale. | Use the two-phase sum plus scheduling slack. |
| WARNING | Validation wait | The wait can outlive its lease. | Cap by remaining caller lease/deadline and gate the metric. |
| NOTE | Phase race | Flip/check must be atomic. | State it. |
| NOTE | Postgres deferral | Equivalent postgres read defect is out of scope but untracked. | File a follow-up bead. |

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787286236484767029.jsonl`.
