# Claude review — round 34

## Findings

| Severity | Area | Finding | Required change |
| --- | --- | --- | --- |
| WARNING | KeyedQueueGate transitive bound | Its per-key FIFO can contain the global maximum of 1,024 waiters, so a 505 s depth-one Complete-delay bound is unsupported. | Bound per-key depth and compose the delay, or state and gate the 1,024-depth bound. |
| WARNING | Outcome admission after durable publication | Claim render can borrow the outcome pool after append/apply; rejecting reader 17 there turns durable work into retryable capacity Backpressure. | Reserve a non-rejecting counted render lane, or eliminate post-publication capacity rejection while keeping pool contention bounded. |
| NOTE | S3r dependency | S3r uses OutcomeReadAdmission from S2 but depends only on S-0. | Add S2/B2 to S3r/B3r. |
| NOTE | Above-cliff arithmetic | Concurrent cliff lanes shift the stated ninth/twenty-fifth rejection points. | Run cliff lanes in isolation or restate the concurrent points. |
| NOTE | Completion ceilings | Publication ceilings omit apply wait and post-publication render. | Name separate response ceilings. |
| NOTE | Deep Claim bucket | Up to 1,024 compatible attached callers may require 128 sequential eight-request rounds with no named ceiling. | Name the tail completion ceiling or cap bucket depth. |
| NOTE | Reclaim routing | S3i retries re-enter the live KeyedQueueGate path, but the site map does not name reclaim. | Name the route and cadence measurement. |

## Prior-round audit

All round-33 findings were folded. Ingress capacity, zero-copy retention,
callerless retry, and separated service/cohort/suffix ceilings are sound. The
remaining blockers are the one unbounded per-key FIFO and possible capacity
rejection after durable publication.

## Verdict

`REQUEST_CHANGES`

## Convergence

`NO`

## Summary

Bound KeyedQueueGate per key, ensure all capacity rejection happens before
durable publication, and reconcile dependency/lane/response bookkeeping.
