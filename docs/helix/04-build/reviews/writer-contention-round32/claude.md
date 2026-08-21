# Claude review — round 32

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Acquisition rule premise | The derived caps assume one legal predecessor, but only the mutation sequencer proves that depth. Multiple same-queue Claim drivers can contend for the fence, while the global Claim/shared slot waiter sets can contain many waves. The 95 s slot, 255 s sequencer, 505 s mutation, and 630 s Claim ceilings inherit the unsupported premise. | Bound predecessor count structurally per resource, such as one exclusive Claim turn per queue and bounded global slot contenders, or derive from the real depth. |
| WARNING | Overload gate arithmetic | A 32-request cohort needs four sequential eight-request generation services, but its completion gate uses one 505 s service ceiling. The p95/p99 comparison also mixes original-request retry age with the pre-cap S0 latency distribution. | Compose the cohort ceiling across four generations plus retry cadence, and scope the relative latency gate to comparable admitted-service distributions. |
| NOTE | Shared retry count | One internal attempt is normative in the slice table, but a shared-mutation constraint still says release-and-retry after expiry. | Make one attempt the single normative statement. |
| NOTE | Sequencer byte accounting | Queued generations cannot charge packed-append debt because that reservation occurs after planning. The process-wide sequencer therefore lacks the claimed byte ceiling. | Add an independent process-wide retained-byte ceiling or drop the debt claim. |
| NOTE | Safety caps versus T2 | Five-second safety caps can pass while the serialized Claim cycle is far too slow for T2. At fill 800, 4,000 items/s requires roughly a 200 ms cycle. | Add a T2-derived Claim-cycle diagnostic stop to S3m/S5. |
| NOTE | Overload composition | The plan does not pin Claim borrowers and the mutation overload cohort to the same queue, and S3s calls the inert fence bound measured. | Run the contending lanes on the same queue and mark the pre-S3m fence term as carried, not measured. |
| NOTE | Retry cadence | Capacity retry cadence is unspecified, so retry counts, age, and throughput are not reproducible. | State the evidence-client cadence. |
| NOTE | Pool-versus-fence wording | One sentence says a driver snapshot is obtained after fence acquisition, contradicting the canonical pool-before-fence order. | Say the snapshot starts after fence acquisition on the already-borrowed connection. |

## Prior-round audit

All round-31 findings were folded. The residual blocker is the repeated
predecessor-count assumption: it is now centralized, but remains false for the
fence and global slot semaphores. Sequencer-owned capacity, protocol constants,
frontier symmetry, and outcome wait ordering are clean. Claude also verified
the 224 MiB pool arithmetic and noted that `QueueCommand` currently has
twenty-three variants; the plan’s twenty-four count refers only to pooled
readers.

## Verdict

`REQUEST_CHANGES`

## Convergence

`NO`

## Summary

Structurally bound the Claim/fence and global-slot contender depths, compose the
overload deadline across four generation services, and add an early T2-derived
cycle budget before activation.
