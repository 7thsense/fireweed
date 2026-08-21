# Claude review — round 13

### Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| BLOCKING | Exclusive acquisition | Group/cohort claims acquire at append rather than continuously from selection, and do not wait/advance `last_claim`. | Every exclusive holder must wait inside the fence, hold it from selection through publication, and advance the queue Claim frontier. |
| BLOCKING | Interim live Claim | S3b claims live coverage even though SQL-first item Claim remains active until S5. | Keep fence machinery inert until an atomic S5 activation, or safely fence the interim path; move live overlap gates accordingly. |
| BLOCKING | Gap predicate | Reservation index order is not log-position order. | Treat any outstanding same-shard reservation as a possible gap filler and use a bounded no-progress deadline before poison. |
| BLOCKING | Dependency order | Fence ordering gates rely on exact-wait repairs that land later in S3c. | Land exact committed coverage before activating/wiring the fence. |
| WARNING | Committed connection ownership | S3c excludes the file that owns its committed recovery reader. | Add `local.rs` or give an earlier slice explicit ownership. |
| WARNING | Durable unpublished position | A durable position may stall if its publisher dies. | Cancel only before durability; a durable unpublished guard poisons immediately. |
| WARNING | Reserved head blocking | A pre-append reservation can block a shard without a bound or metric. | Bound, instrument, and gate the wait. |
| WARNING | Live snapshot shortcut | `snapshot_live_items` returns early when all rows exist without checking coverage. | Remove that live shortcut or assign it to another explicit slice. |
| NOTE | Debt units | Packer estimates and apply coordinator serialized bytes differ. | Name the authoritative debt unit. |
| NOTE | S8 mapping | Qualification and cleanup share S8 without clause ownership. | Split their exit gates and dependency order. |

### Round-12 audit

- **Claim-class fence disposition — UNRESOLVED:** selection is not protected continuously.
- **S3 split dependencies — RESOLVED.**
- **Classifier coverage — RESOLVED.**
- **Out-of-order Ready admission — UNRESOLVED:** reservation-index ordering is unsound.
- **S3a residual size — RESOLVED.**
- **Live coverage — RESOLVED** for the cursor branch, with the all-rows shortcut still open.
- **S-0 CI home — RESOLVED.**
- **Fidelity versus fill — RESOLVED.**
- **Claim-plan validation — RESOLVED.**
- **Fused grouped wording — RESOLVED.**

### Verdict

`BLOCK`

### Convergence

`NO`

### Summary

The classifier and prior decomposition are sound, but production fence
activation is ordered incorrectly. Exclusive claims must protect selection,
wait exact frontiers, publish, and advance `last_claim` as one operation; exact
coverage must exist before that activation, and gap handling cannot infer log
order from reservation index.

Full raw session: `.ddx/agent-logs/svc-1787281467470584406.jsonl`.
