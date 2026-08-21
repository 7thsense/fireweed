# Claude review — round 29

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Mutation serialization | Holding one per-queue sequencer per request prevents same-queue Push/BatchUpdate co-sealing and contradicts pack-fill/non-regression gates. | Sequence compatible co-sealing generations or budget the loss. |
| WARNING | Slot deadlines | Fixed 5 s slot acquisition is below legitimate 10.5–60 s holds. | Derive from hold/wait evidence and reconcile ceilings. |
| WARNING | Shared release | Global semaphore versus per-queue sequencer release points were ambiguous. | Release the global slot with the connection and retain only the sequencer through publication. |
| WARNING | Mutation coverage wait | An unbounded in-hold exact wait could block Claim. | Derive a bound, release/retry, and meter it. |
| WARNING | Calibration order | Shared admission activated in S3c before S3m calibrated it. | Add preactivation calibration or explicit fallback. |
| NOTE | Activation wording | Claim-driver activation ownership conflicted. | Keep it inert until S5. |
| NOTE | Sequencer membership | Prose membership could drift from command variants. | Add an exhaustive classifier. |
| NOTE | Shared metrics | Same-queue load conflated sequencer and capacity waits. | Report them separately and declare queue cardinality. |
| NOTE | Threshold/deadline | Fixed deadlines and derived thresholds were ambiguous. | State which derived values configure deadlines. |

### Prior-round audit

All round-28 findings were folded, but per-request sequencing regressed the
high-rate packed mutation path and fixed slot deadlines contradicted legal holds.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787294058083367867.jsonl`.
