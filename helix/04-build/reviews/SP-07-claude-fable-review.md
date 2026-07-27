# Claude Fable Review: SP-07 Segment Integrity Version 3

**Verdict**: NO-GO on the first draft; **GO** after correction and Claude Fable re-review.

## Blocking Findings

1. V2 actually uses JSON despite postcard comments; v3 payload/evolution rules were unspecified.
2. Manifest dispatch authority and header/manifest cross-checks were missing; v2 header bytes are uncovered.
3. V2 count allocation was unbounded.
4. Manifest fields, non-data exemptions, and serde-default scope were undefined.
5. Rollback ignored the binary downgrade cliff and v3-then-v2 interleaving.
6. BLAKE3 duplicated existing SHA-256 content identity and hashing.
7. TD-004's per-command checksum requirement is currently fictional and needed disposition.
8. TP-003 lacked golden/fuzz/version/crash evidence integration.

## Incorporated Corrections

The revision pins JSON records, full-frame/per-record Castagnoli CRC32C, SHA-256 stored-object identity,
manifest-authoritative dispatch and exact fields, v2/v3 bounds/cross-checks, non-data exemptions, a two-release
runtime-controlled rollout with arbitrary interleaving, per-record checksum spec correction, TP-003 mapping,
typed/redacted errors, per-PR fuzzing, and SP-02 fallback coverage.

## Re-review Result

Claude Fable confirmed every blocker resolved. Its sole stale-text note is fixed: the codec slice now requires
CRC32C/SHA-256 vectors, with no BLAKE3 residue.
