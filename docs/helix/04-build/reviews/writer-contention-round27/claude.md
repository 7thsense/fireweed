# Claude review — round 27

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Push snapshots | One Push snapshot would span object-log epoch/blob and counter work, and the engine seam was outside S3r. | Split the snapshots around I/O and assign the engine-side change. |
| WARNING | Isolation sequencing | The proposed temporary catch-up exits on empty/not-ready rather than exact coverage. | Activate committed reads only with exact coverage or define a real exact intermediate. |
| WARNING | Shared capacity | Shared borrowers were unbounded and calibration load equaled capacity. | Bound demand and test above capacity. |
| NOTE | Strict append amplification | Read hoisting does not reduce per-entry strict append/fence acquisitions. | Meter and bound them. |
| NOTE | S3m framing | A pre-S5 slice cannot be the activated production path. | Call it shadow reconstruction and re-derive in S5. |
| NOTE | Reader configuration | Pooled 100 ms readers need their own configure/verify path and busy-error mapping. | Add both. |
| NOTE | WAL disposition | Autocheckpoint is disabled and no explicit checkpoint exists. | Record disabled/no-checkpoint and bounded monotonic growth. |

### Prior-round audit

Most round-26 folds were accurate. Push phase placement, the exact intermediate
coverage claim, and the shared-demand test still required architectural fixes.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787292455694268007.jsonl`.
