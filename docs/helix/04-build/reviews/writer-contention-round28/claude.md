# Claude review — round 28

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Committed coverage | Push/planning reads lacked explicit exact coverage, risking duplicate durable identity and apply poison. | Give every public/planning read coverage or define non-poisoning conflict apply. |
| WARNING | Calibration thresholds | A 100 ms combined shared-slot/pool gate cannot hold when the slot spans a 500 ms–5 s fence cycle. | Derive slot threshold separately or exclude fence-blocked hold. |
| NOTE | Pool partition | Driver expiry was structurally prevented while outcome demand was unmeasured. | State the partition and measure outcome concurrency/waits. |
| NOTE | Activation | Driver-read admissions lacked an activation slice. | Name S3c/S5 ownership. |
| NOTE | S3c files | Projection helpers were outside S3c. | Make S3r prepare borrowed-connection helpers so S3c is caller-side. |
| NOTE | Claim calibration | Claim demand equaled four-slot capacity. | Oversubscribe it. |
| NOTE | Composed budgets | Worst-case Claim and strict expiry dispositions were unstated. | State ceiling and durability/retry behavior. |

### Prior-round audit

All round-27 findings were folded. Remaining warnings concerned exact coverage
semantics and a combined threshold that contradicted the declared lock order.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787293294709484776.jsonl`.
