# Claude review — round 26

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Shared-appender capacity | Four reserved driver connections were not derived from measured Push demand; Push has three committed reads. | Coalesce Push reads, reconcile pool/admission capacity, and size from evidence. |
| WARNING | Pool geometry | Pool counts, Claim slots, and cache size were not exercised together before S5. | Extend S3m to the real total order and record pool/read behavior. |
| WARNING | Isolation sequencing | S3r retires uncommitted reads before S3c adds coverage, allowing an unsafe intermediate state. | Pair the changes or give S3r bounded apply-lag handling and non-regression gates. |
| NOTE | Claim-slot scope | Group/cohort Claim slot ownership was ambiguous. | Cover every Pending-consuming Claim and test them. |
| NOTE | Canonical order | The shared constraint omitted the Claim-driver slot. | Add it. |
| NOTE | Carrier wording | A residual false compile-time claim survived. | Call it the S2a source-audit/test gate. |
| NOTE | Reader pragmas | Pooled busy timeout exceeded the work bound and readback omitted claimed values. | Set <=100 ms and read back every required pragma. |
| NOTE | Strict borrow amplification | Strict commit could borrow/snapshot once per entry. | Hoist both above the public request loop. |
| NOTE | WAL evidence | The liveness gate did not assert WAL/checkpoint behavior. | Record WAL bytes and checkpoint disposition. |

### Prior-round audit

Seven of eight round-25 findings were fully folded. Remaining problems were the
uncalibrated shared-appender reserve and independently unsafe S3r intermediate.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787291817894129691.jsonl`.
