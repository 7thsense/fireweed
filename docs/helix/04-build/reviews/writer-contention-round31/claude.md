# Claude review — round 31

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | Sequencer capacity basis | The two-generation/16-request cap is attached to SelectionFenceAdmission only. The KeyedQueueGate site class joins the same per-queue sequencer uncapped, so reclaim, purge, and retry/release/rearm cohort-finalize work can queue unbounded singleton generations. The “at most one predecessor” basis for the 65 s sequencer deadline therefore does not hold. | Enforce the per-queue generation/request cap at the sequencer itself, or derive the deadline from the real predecessor depth of every joining site class. |
| WARNING | Acquisition versus hold | Claim/shared slot, fence, and sequencer acquisition caps are below the composed hold of those resources. Round 30 raised the fence cap without fixing the rule that produced the defect. | Derive every acquisition cap from the composed downstream hold of that resource, or shorten the resource hold. State the rule once rather than per site. |
| WARNING | Public admission tightening | The per-queue 16-request mutation cap replaces a prepare-phase KeyedQueueGate whose basis was 1,024, but no evidence lane exceeds 16 same-queue mutation requests. The inflight-eight harness cannot detect the regression, and capacity-rejected retries have no starvation accounting. | Justify 16 on a memory basis, add a greater-than-16 same-queue mutation lane to S0/S3s, and meter capacity-rejection starvation separately from deadline expiry. |
| NOTE | Composed ceilings | The 240 s mutation ceiling omits the retry multiplier used by the Claim ceiling, and the shared coverage retry count is unstated. | State the shared retry count and recompute the mutation ceiling on the same rule. |
| NOTE | Protocol constants | Claim protocol steps 2 and 4 retain old 10.5 s connection-hold and fixed 500 ms work constants. | Restate both against the current acquisition, drain, and derived work bounds. |
| NOTE | Claim ceiling terms | The Claim ceiling omits the pre-fence coverage term. | Bound the pre-fence wait explicitly and add its term. |
| NOTE | Outcome pool holds | Public reads wait request-entry high-water, but the plan does not say that wait finishes before borrowing from the outcome pool. | State wait-before-borrow for public reads. |
| NOTE | Claim frontier asymmetry | Shared mutation validation waits only the candidate-mutation frontier, so a published-but-unapplied Claim may be invisible to Purge/Replace validation. | Make the safety argument explicit or wait the Claim frontier too. |

## Prior-round audit

All eight round-30 findings were folded. Three folds left residual defects: the
fence bound was raised without covering in-fence work, the derived work bound
did not reach the protocol steps, and the calibration composition capped only
the direct site class. Claude independently verified that the disposition table
covers all twenty-four `QueueCommand` variants, the direct and composed call
sites match the admission map, the 224 MiB page-cache sum is accurate, and the
release reconciliation identifies sixteen commits and `v0.31.21` at
`91f94ef1`.

## Verdict

`REQUEST_CHANGES`

## Convergence

`NO`

## Summary

Round 30 fixed instances rather than the underlying admission and timeout
rules. The two-generation cap must live at the sequencer shared by every site,
each waiter must be bounded against the legal holder ahead of it, and the
16-request public tightening needs a measured overload/retry lane.
