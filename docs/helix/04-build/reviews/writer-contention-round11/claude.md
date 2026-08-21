# Claude review — round 11

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Fused grouped/token integrity | Fused Claim+Complete omits group-summary removal and clears tokens for rows it did not move. | Extend S4 to grouped maintenance and moved-row-only token effects, or refuse grouped fusion. |
| BLOCKING | Snapshot probe placement | Deferred committed non-blocking behavior is unproven until the cutover slice. | Probe it before implementation and name the safe fallback. |
| BLOCKING | S3 decomposition | S3 bundles too many subsystems/files to be independently revertible or bisectable. | Split lock/seal, classifier/fence, and exact-wait/publication work. |
| WARNING | Nested classifier exhaustiveness | Top-level `QueueCommand` matching can miss new Finalize/mutation dispositions. | Match nested `FinalizeKind` and mutation action enums without wildcards; include Release/Rearm. |
| WARNING | Exact-wait no progress | A Ready position gap can stop the worker without poison and hang exact wait. | Add a no-progress deadline/position-gap poison test. |
| WARNING | Packed debt accounting | Leader publishes the whole group on one waiter's reservation while followers cancel, undercharging debt. | Merge/recharge all participant reservations to actual packed contents. |
| WARNING | Metadata amplification | Global metadata→produce order couples epoch/emission metadata to unrelated PUTs. | Measure/accept or shard the lock. |
| WARNING | Release drift | HEAD is now `v0.31.21`; 16 commits separate it from v0.31.20 through `0567e232`, including fidelity regression `5999aa77`. | Reconcile release/count/base explicitly. |

### Round-10 audit

Resolved: global lock order in intent, coordinator high-water authority,
top-level candidate classification, lifecycle ownership, cross-queue PUT
measurement, seal-time expiry, snapshot bounds, owned leaders, and S3
non-regression intent.

Unresolved: fused grouped/token behavior, early snapshot feasibility, S3 bounded
decomposition, nested classifier coverage, no-progress poison, and exact packed
debt accounting.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The architecture is now coherent, but the fused path still diverges on grouped
and foreign-token rows, the committed snapshot needs an earlier probe/fallback,
and S3 must be split before it is execution-ready. Exact wait and packed debt
also need explicit no-progress/accounting gates.

Full raw session: `.ddx/agent-logs/svc-1787279474957787654.jsonl`.
