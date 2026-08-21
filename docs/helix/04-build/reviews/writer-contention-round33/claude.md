# Claude review — round 33

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Global driver-slot capacity | Four-active/four-queued Claim and twelve-active/twelve-queued shared admissions reject the ninth Claim queue and twenty-fifth mutation queue. Evidence stops exactly at those cliffs, and rejection occurs after broader caller admission. | Either justify a larger concurrent-queue count and derive waves, or test above the retained caps and reject at driver ingress. |
| WARNING | Claim-side overload evidence | ClaimQueueTurn tightens a queue from up to 1,024 queued callers to two concurrent Claim compatibility drivers without the symmetric baseline/retry/completion evidence used for mutations. | Add a same-queue Claim compatibility overload cohort to S0, S3s/S3m, and S5 with a composed completion ceiling. |
| WARNING | Outcome pool derivation | Eight outcome connections have unbounded contenders and a rejecting five-second borrow deadline, violating the plan’s acquisition-depth rule. | Add bounded outcome admission or derive from a bounded contender count, then test above the bound. |
| WARNING | Callerless internal appends | Reclaim commands gain new retryable rejection points, but `reclaim_tick` aborts the rest of its page on the first error and no public caller can retry. | Give reclaim an internal retry owner, isolate per-queue failures, and prove drain under same-queue saturation. |
| WARNING | Oversize sequencer loan | The byte-budget loan has no clear scope, priority, or bounded wait and can starve under continuous input. | Define a bounded priority discipline and test it, or remove the loan/budget design. |
| NOTE | Claim service ceiling | One driver service is 505 s, but a byte-split suffix request may need eight rounds. | State and gate the per-request ceiling separately. |
| NOTE | Transitive gate serialization | KeyedQueueGate can be held across the full composed service, but the promised bound is unnamed. | Meter and state the full bound. |
| NOTE | One-attempt rule residue | A failure path still waits for progress before returning Backpressure. | Release and return immediately. |
| NOTE | S0 latency baseline | S0 does not explicitly record the admitted-service percentiles S5 compares. | Add those percentiles. |
| NOTE | First fence measurement | S3m precedes live fence activation. | State that S3m takes the real fence only on isolated shadow queues. |
| NOTE | Work-bound derivation | Reservation split/retry rounds are missing from the work calibration. | Include the split path. |
| NOTE | Cross-queue byte exhaustion | Four byte-heavy queues can exhaust the 32 MiB process budget, but no gate covers the fifth. | Add cross-queue byte-exhaustion evidence or remove the design. |

## Prior-round audit

All round-32 findings were folded correctly on their own terms. The predecessor
depth and 505/2,021.075 s arithmetic are sound. The new blocker is the public
and internal rejection surface introduced to obtain those bounds.

## Verdict

`REQUEST_CHANGES`

## Convergence

`NO`

## Summary

Measure and justify the global and per-queue capacity cliffs, bound outcome
readers with the same rigor, remove or define the oversize loan, and ensure
callerless reclaim owns retry rather than abandoning a page.
