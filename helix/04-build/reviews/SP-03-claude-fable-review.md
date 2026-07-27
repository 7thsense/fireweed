# Claude Fable Review: SP-03 Typed Sequenced Metadata Boundary

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. Retention floor is advance-then-delete, but deletion watermark is deliberately delete-then-advance.
2. Read horizon and deletion watermark are one durable value with marker authority plus a cache blob, not two classes.
3. `BlobStore` has create-only CAS and no lower retry wrapper or typed ambiguity outcome.
4. Failure semantics differ: fenced head/floor versus unfenced max-merge watermark.
5. Blocking HCAS-F1/F2 findings sit on the migration surface and must be resolved first.
6. The claimed deduplication targets and universal post-create check did not match current code.
7. Generic delete types could free manifest addresses and break the stale-writer fence.

## Incorporated Corrections

The revision defines the real classes, both ordering disciplines, per-class failure/authority rules, retained
versus freed address types, branch-pin-specific post-create validation, create-only CAS with protocol-owned
retry and typed ambiguity, HCAS closure as slice zero, concrete eligibility/head deduplication targets, SP-02
fallback interleavings, and GET/PUT/LIST performance gates.

## Re-review Result

Claude Fable confirmed every blocker resolved. Its non-blocking follow-ups are incorporated by correcting the
roadmap summary and explicitly recording that the retention floor is a manifest entry. Slice 5 must establish
baseline request counts before declaring the object-request budget.
