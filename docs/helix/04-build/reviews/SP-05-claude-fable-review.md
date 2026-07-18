# Claude Fable Review: SP-05 Maintenance Policy and Execution Separation

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. The plan repeated advance-before-delete even though watermark is delete-before-advance.
2. Orphan predicates, grace windows, and recovery interaction had no governing definition.
3. Revalidate-then-delete cannot replace the existing exclusion guard or two-sided pin protocol.
4. The proposed snapshot omitted health, high-water, epoch, and in-memory idempotency pins.
5. Cursor persistence, permanent frontier failures, and filters were undefined.
6. Dry-run/live parity ignored revalidation changes.
7. API-002/operator authority was inconsistent with the internal scope.
8. Deduplication targets were unnamed and risked pulling lease reclaim into scope.

## Incorporated Corrections

The revision adds dual ordering, normative orphan definitions, a per-class safety table, complete frontier
inputs, soft cursor/rescan and permanent-failure rules, downgrade-only parity, owner/fence behavior, explicit
internal-only scope, named driver/selector/orphan-GC dedup targets, SP-02 fallback, and object-request gates.

## Re-review Result

Claude Fable confirmed every blocker resolved. Its graph/limit follow-ups are incorporated by linking SP-02
as `informed_by` and making object-request count a first-class nonzero per-run bound. Driver deduplication stays
at the scaffolding layer and must not alter excluded lease-reclaim effects.
