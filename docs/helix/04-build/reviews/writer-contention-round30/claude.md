# Claude review — round 30

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Fence acquisition | A 5 s Claim acquire bound was shorter than a legal 60 s shared publication hold. | Derive acquisition from the legal maximum or shorten the hold. |
| WARNING | Sequencer order | Sequencer position differed between direct and keyed paths, permitting inversion. | Fix one canonical position and test it. |
| WARNING | Calibration composition | S3s omitted interim Claim borrowers and allowed queue depth far beyond the sequencer premise. | Match S3c composition and bound per-queue generations. |
| NOTE | Sequencer expiry | Missing resource and retry/FIFO disposition. | Add both. |
| NOTE | Fill gate | Mutation fill was absent from structural gates. | Add it. |
| NOTE | Work bound | 500 ms work was fixed rather than measured. | Derive it. |
| NOTE | Admission row | Default Claim omitted slot/pool. | Make it symmetric. |
| NOTE | Mutation ceiling | Activated-path mutation ceiling was absent. | Restate it in S5. |

### Prior-round audit

Round 29's co-sealing fix held. Remaining warnings concerned publication-aware
bounds, one lock order, and matching calibration to the activation composition.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787294800864045076.jsonl`.
