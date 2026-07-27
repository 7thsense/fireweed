# Claude Fable Review: SP-04 Object-Store Observability

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. Trait-level LIST calls can issue multiple physical pages, so one trait call is not one physical request.
2. Default composite methods could bypass instrumentation or double-attribute their primitive work.
3. Current `Storage(String)` errors cannot safely produce retryable/throttle/timeout classes.
4. Existing `SegmentCounters` and the E3 `MeasuredBlobStore` already disagree and require reconciliation.
5. No shared metrics export path exists; the plan was actually defining the first pull snapshot surface.
6. Protocol retry iterations are multi-call and cannot equal physical calls minus one.
7. Multiple production/bypass constructors use `Arc<dyn BlobStore>`, so type-directed wrapping was not enforced.

## Incorporated Corrections

The revision derives paginated LIST request counts, defines a policy for every trait method, extracts composite
helpers, adds structured provider faults with reserved-zero timeouts and an in-flight gauge, reconciles old
counters/E3 measurement into one recorder source, scopes a new pull snapshot/ledger feed, counts protocol-loop
iterations separately, names every construction root and a single wrapping funnel, and distinguishes service
latency from actor queue/ack latency.

## Re-review Result

Claude Fable confirmed every blocker resolved. Non-blocking follow-ups are incorporated by preserving optimized
provider `stats` as labeled introspection rather than fabricating workload GETs, removing an ungrounded
"backfill" term, and naming the wrapper-newtype/private-funnel plus `cfg(test)` bypass mechanism.
