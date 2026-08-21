# Claude review — round 14

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Classifier aggregation | One Finalize command can mix terminal and Pending-restoring outcomes. | Aggregate to the maximum disposition; any Retry/Release/Rearm makes the command shared. |
| BLOCKING | Exclusive fairness | Group/cohort claims may hold the exclusive fence through unbounded apply/render. | Release at append publication like item Claim, or add explicit bounds and mixed controls. |
| WARNING | Reachable reads | The named derived `snapshot_live_items` repair targets a dead method. | Apply committed coverage to reachable `live_items` and `planner_update_snapshot` reads. |
| WARNING | Pre-append budget | The 500 ms budget includes an unmeasured exact wait. | Give the wait its own bucket/bound derived from evidence. |
| WARNING | Reservation timeout | A fixed 30 s poison can fire during a legal S3 PUT. | Couple the bound to the backend append timeout. |
| WARNING | Byte-bound suffix | Deferred requests lack explicit FIFO re-drive and anti-starvation. | Specify driver re-election and bounded rounds. |
| WARNING | Provisional-pack retirement | The replacement lacks the historical eight-way zero-hang gate. | Name `47b1a223` and require no recurrence. |
| WARNING | S5 revert unit | Atomic activation needs four real files and conflicts with the normal three-file rule. | Declare the exception, true file set, and one-step rollback. |
| NOTE | Fidelity scope | S-1 omits gate keys and ambiguously claims the whole regression. | Name every restored response field and leave other changes to S3c/S7. |
| NOTE | Fault adaptation | `AfterAppendBeforeApply` currently cancels after durability. | Change it to durable poison in S3p. |
| NOTE | Dependency drift | Several slice rows omit graph dependencies. | Mirror the graph in the slice table. |

### Round-13 audit

- **Exclusive acquisition — RESOLVED**, with a new unbounded-hold fairness defect.
- **Interim live Claim — RESOLVED.**
- **Gap predicate — RESOLVED.**
- **Dependency order — RESOLVED.**
- **Committed connection ownership — RESOLVED.**
- **Durable unpublished position — RESOLVED.**
- **Reserved head blocking — RESOLVED**, with timeout coupling still required.
- **Live snapshot shortcut — UNRESOLVED** because the named method is unreachable.
- **Debt units — RESOLVED.**
- **S8 mapping — RESOLVED.**

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The atomic activation and dependency design are feasible. Convergence still
requires mixed-Finalize aggregation, append-publication release for every
exclusive Claim, and the warning folds above.

Full raw session: `.ddx/agent-logs/svc-1787282445572211750.jsonl`.
