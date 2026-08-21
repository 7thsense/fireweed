# Claude review — round 12

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Claim-class fence disposition | Item-level Claim leaves the old queue gate, but grouped/cohort Pending-consuming claims have no safe disposition in the new fence domain. | Classify every Pending-consuming Claim/CohortClaim/grouped claim as exclusive, wire their append sites, and prove concurrent cohort and item claims are disjoint and healthy. |
| BLOCKING | S3 split dependencies | B3b can land before the drop-safe publisher that its fence-release rule requires. | Make the fence slice depend on the owned-publication slice. |
| WARNING | Classifier coverage | Several top-level and finalize dispositions are not assigned. | State every `QueueCommand`/`FinalizeKind` disposition and test Pause→Claim plus CohortFinalize(Retry)→Claim. |
| WARNING | Out-of-order Ready admission | A later Ready entry can run before an earlier Reserved entry on a fresh shard. | On the no-high-water branch, wait behind earlier Ready or Reserved entries; retain gap poison only as a residual backstop. |
| WARNING | S3a residual size | Lock order, sealing, debt, and publication are still bundled. | Split lock/high-water from packer scope/debt/owned publication. |
| WARNING | Live coverage | `snapshot_live_items` can still trust uncommitted `recovery_high_water`. | Make coordinator `applied_high_water` the only live coverage authority there too. |
| WARNING | S-0 CI home | The excluded compatibility tool is not exercised by workspace tests. | Name an in-workspace mode test and the exact standalone probe command. |
| WARNING | Fidelity versus fill | Restored response bodies and the 4 MiB bound can reduce achieved fill. | Measure realistic-payload fill and state the T2 disposition when it is below eight. |
| NOTE | Claim plan validation | Direct driver envelopes bypass `validate_claim_plan`. | Assign those invariants to the driver and name its differential replacement test. |
| NOTE | Fused grouped wording | Group-summary removal also re-elects a representative. | Say remove and re-elect explicitly. |

### Round-11 audit

- **Fused grouped/token integrity — RESOLVED.**
- **Snapshot probe placement — RESOLVED**, with an in-workspace test still required.
- **S3 decomposition — RESOLVED**, with a dependency defect and residual bundling to fix.
- **Nested classifier exhaustiveness — UNRESOLVED** because dispositions remain unassigned.
- **Exact-wait no progress — RESOLVED.**
- **Packed debt accounting — RESOLVED.**
- **Metadata amplification — RESOLVED.**
- **Release drift — RESOLVED.**

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

### Summary

The round-11 fold closed the fused-apply, debt, release, no-progress, and probe
placement defects. Pending-consuming grouped/cohort claims must join the
exclusive fence domain, and fence wiring must not precede drop-safe
publication. The warnings above also require folding before convergence.

Full raw session: `.ddx/agent-logs/svc-1787280474118191889.jsonl`.
