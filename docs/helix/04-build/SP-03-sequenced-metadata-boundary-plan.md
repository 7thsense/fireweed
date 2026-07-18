---
ddx:
  id: build-sp03-sequenced-metadata-boundary
  depends_on: [build-sp02-deterministic-storage-simulation, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: c212bb092c036690b331e446a3b53ee8d5d5ae47eb6237524d038b6e7fdb53db
    deps:
      build-sp02-deterministic-storage-simulation: a7e7545464a051bd23046ffbb1b0f04fece7c450eb071e593a82084a08ed66ff
      td-s3-object-log-sqlite-projection-mode: f77b249de99163d5b3031b174f2ff1a7833b45d1a68646a1a9da206e847a5fd0
    reviewed_at: "2026-07-18T16:20:32Z"
---

# Implementation Plan: SP-03 Typed Sequenced Metadata Boundary

## Scope

Create typed operation families for monotonic metadata publication and bounded deletion. The real durable
classes are manifest head (authoritative and legacy-compatible protocols), retention floor, and deletion
watermark (append-only marker authority plus `read_horizon.json` compatibility cache). Include only the
branch-pin/floor interaction needed to formalize its validate-after-copy race closure.

Two ordering disciplines are explicit: retention floor uses advance-then-delete because the bound grants
deletion eligibility; deletion watermark uses delete-then-advance because the marker records a completed,
contiguous delete prefix and must never hide work still needed by deletion.

## Shared Constraints

- Encode command sequence, manifest index, head version, epoch, and object class in distinct types; never pass unrelated `u64` values
  positionally across the boundary.
- Head publication uses fenced conditional CAS and fails closed on precondition/epoch loss. Floor advance is
  fenced, conditional, and idempotent at equality. Watermark persistence is an unfenced monotone max-merge;
  stale/regressing attempts are safe no-ops, not failures.
- Branch-pin publication keeps its existing post-create authoritative-floor check and rollback; this property
  is not imposed on segment creation classes that do not need it.
- Object-class types distinguish address-retaining reclamation (manifest addresses remain occupied so stale
  `put_if_absent` collides) from address-freeing deletion (segments, candidates, compatibility mirrors).
- Policy decides eligibility; the metadata boundary performs conditional publication and deletion protocol.

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 0 | Close or formally discharge HCAS-F1/F2, including crash at the fence-entry gap and legacy-path disposition | no known blocking defect in migration baseline |
| 1 | Amend TD-004 with per-class state machines, authority, ordering, recovery, head-protocol compatibility, and typed key map | HELIX and Fable review |
| 2 | Add `FencedPublication` and `MonotoneMarker` families, retained/free address types, and independent models | compile-time type separation; generated state-machine tests |
| 3 | Implement over create-only `put_if_absent` CAS and versioned-head convention; bounded protocol loops own retry | CAS/precondition loss, typed ambiguity, list staleness, address collision |
| 4 | Migrate floor, authoritative head with legacy dispatch/mirroring, then watermark marker+cache; consolidate duplicate eligibility classification | deterministic simulation or explicit interleaving fallback; legacy recovery |
| 5 | Benchmark steady-state seal and bounded GC metadata operations | no extra GET/PUT/LIST on hot path beyond budget; roadmap bars |

## Issue Decomposition

Do not force snapshots or unrelated branch-pin operations into these families. Each migrated class states its
typed key, authority, ordering discipline, failure semantics, deletion predicate, address-retention rule, and
recovery source. The current retention floor is a manifest entry, not a standalone floor blob. Typed outcomes distinguish `PreconditionLost` from `AmbiguousOutcome`; ambiguity resolves by
authoritative recovery, never blind retry.

The concrete deduplication targets are: (1) the three below-floor eligibility walks in partial expiry,
contiguous watermark derivation, and reclamation-candidate selection, consolidated as one pure classifier;
and (2) the duplicated publication mechanics beneath authoritative-head and legacy-compatible head paths,
while preserving authority markers, winner mirroring, and fail-closed total-authority-loss behavior.

## Validation Plan

- [ ] Floor advance is durable before eligible segment deletion; watermark advance occurs only after the
      entire contiguous eligible prefix is deleted.
- [ ] Post-create checks prevent late candidates from escaping the bound.
- [ ] Concurrent creators/advancers/deleters converge without hiding live metadata.
- [ ] Recovery never trusts cache-only metadata and never falls below the durable horizon.
- [ ] Reclaimed manifest addresses remain occupied and still reject stale create-only writes.
- [ ] Watermark never advances past an undeleted, failed-delete, or branch-pinned entry.
- [ ] If SP-02 stops negatively, hand-written interleavings over the independent models cover the same
      INV-2/INV-10/INV-12 cases before migration.
- [ ] Seal and bounded-GC benchmarks gate GET, PUT, and LIST counts as well as latency.

## Risks and Rollbacks

Over-generalization can erase important authority differences. Keep typed class markers and migrate only
proven-isomorphic paths. Roll back by restoring the previous adapters; durable object shapes remain readable.

## Exit Criteria

The three eligibility walks use one pure classifier; authoritative and legacy-compatible head paths share one
typed publication core without losing compatibility behavior; no hot path adds an unbudgeted object request;
and HCAS-F1/F2 have closed evidence rather than being copied into the new boundary.
