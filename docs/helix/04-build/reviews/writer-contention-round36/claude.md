# Claude review — round 36

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| NOTE | Gate rejection normalization site | Composition submit-error mappers currently flatten gate admission errors to storage errors. | Extend S2e to own public mapping and distinguish per-key from global queue-full. |
| NOTE | Retained-response ceiling | Slot counts understate responses retained after slot release; use eight Claim plus twenty-four mutation admissions. | Record a 128 MiB normal retained-response ceiling and name grouped/cohort lanes. |
| NOTE | Outcome-slot arithmetic | A 100 ms pool wait plus 5 s work and 5 s slack exceeds the stated 10 s cap. | Include structural pool wait inside the 5 s work cap, or raise the cap. |
| NOTE | S3c grouped/cohort window | Pre-materialization activates before the selection fence. | Name exact coverage and the temporary concurrent-ordering window until S5. |
| NOTE | Retired render invariants | The old renderer validates count/order/token/expiry and cohort shaping beyond fields/content. | Preserve these invariants in S3g tests. |
| NOTE | Work composition | Full-row materialization now occurs inside the fence but is absent from the work-bound derivation. | Measure it in S3m/S5. |
| NOTE | Caller test identity | A 1,025-distinct-queue caller test hits the eight-driver cliff first. | Test 1,025 callers within admitted buckets instead. |
| NOTE | Reclaim ceiling | S3i says eventually without a retry-age ceiling. | Add a closed-lane injected reclaim ceiling. |

## Prior-round audit

Every round-35 finding was folded. Grouped/cohort post-publication rendering is
closed by S3g plus S3c activation; keyed-gate, response, memory, suffix, and
cliff arithmetic are structurally sound.

## Verdict

`APPROVE`

## Convergence

`YES`

## Summary

The plan has converged. Remaining notes fit existing slice acceptance criteria
and do not change architecture or public contract.
