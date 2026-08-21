# Claude review — round 35

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Grouped/cohort post-publication render | The universal pre-materialization rule covers item Claim, but grouped/cohort Claim still uses its legacy post-commit `finish_rendered_claim`/`render_claimed` read. | Pre-materialize grouped/cohort `ClaimedItem` vectors inside the exclusive fence before append, or add a non-rejecting counted post-publication lane. |
| NOTE | Keyed-gate ceiling basis | `submit_operation` holds its permit through response completion, so same-key predecessors occupy the gate for 540 s, not 505 s. | Use 16×540 = 8,640 s and recompose the 32-command cohort. |
| NOTE | Keyed-gate rejection identity | Per-key and global queue-full failures are indistinguishable. | Add a distinct per-key resource/error and preserve it through S2e. |
| NOTE | Realistic-lane cliff | Item/byte sealing may require a third generation before deterministic request 17. | Report the observed first third-generation index in realistic lanes. |
| NOTE | S5 ceiling coverage | S5 omits the same-key cohort ceiling named elsewhere. | Add it explicitly. |
| NOTE | Memory accounting | Retained pre-materialized outcomes and the dedicated recovery-seeding connection are absent from M-gate records. | Record them and state their ceiling treatment. |
| NOTE | Suffix-round admission | An owned suffix round can receive pre-position Backpressure, but waiter resolution/retry ownership is unstated. | Resolve attached waiters and name the retry cadence assumed by tail ceilings. |

## Prior-round audit

Round-34’s structural and dependency changes were folded. The only remaining
warning is the grouped/cohort exception to the pre-materialized-response rule.

## Verdict

`REQUEST_CHANGES`

## Convergence

`NO`

## Summary

Move grouped/cohort response construction before append, recompose same-key
ceilings on 540-second gate occupancy, and tighten the remaining accounting.
