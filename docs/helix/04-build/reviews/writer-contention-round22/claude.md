# Claude review — round 22

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Pool/fence lock order | Borrow-before-fence existed only for Claim while shared outcome readers could take the reverse order, permitting pool/fence deadlock. | Define one global acquisition order and add a mixed shared/exclusive exhaustion gate. |
| BLOCKING | Admission map | Typed Finalize, Renew, Purge, and cohort operations already enter `KeyedQueueGate`; adding SelectionFenceAdmission double-admits them. Atomic Push is also already gated. | Re-derive admission per product from the real call graph and keep one class per request. |
| BLOCKING | Keyed admission arithmetic | Counting every active distinct key against 1,024 creates a new multi-tenant ceiling, and the proposed below-threshold test misses it. | Preserve a queued-waiter cap or bound active keys separately; test at and above the threshold. |
| WARNING | Shared-reader contradiction | Protocol step 5 retained `read_uncommitted=ON` after S3r retired that reader. | Remove the clause and assert only committed pooled readers. |
| WARNING | Renew/Reassign rejection | Turso-only rejection of `new_expiry<=now` creates composition-dependent outcomes. | Clamp the coverage wait to zero or validate the rule in every backend. |
| WARNING | Fallback feasibility | A joined-autocommit fallback cannot coherently implement the expanded chunked, multi-shape outcome reads. | Make committed Deferred snapshots a hard prerequisite or specify every site. |
| NOTE | Pool ownership | No slice clearly created the eight-connection pool. | Assign construction and its test to S3r/`local.rs`. |
| NOTE | S-1 scope | Entity was a pre-existing Class-S gap and public `ClaimedItem` exposes no index-fields member. | Describe index fields as the internal entity-rehydration carrier and state the actual regression scope. |
| NOTE | Macro-shared ports | Atomic and derived products share generated port bodies. | Make the coverage/admission branch an explicit per-product capability. |
| NOTE | Rebaseline bound | S5 referenced an undefined non-regression bound. | Give the S0-relative rate and latency bounds numbers. |

### Prior-round audit

Round 21's findings were textually folded, but the pool-order and admission
folds introduced the blocking issues above. Isolation, validation-only
transaction scope, pragma readback, S2 file ownership, strict-wait hoisting,
terminology, and S5 SQL ownership were otherwise present.

### Verdict

`REQUEST_CHANGES`

### Convergence

`NO`

Full raw session: `.ddx/agent-logs/svc-1787288623674740329.jsonl`.
