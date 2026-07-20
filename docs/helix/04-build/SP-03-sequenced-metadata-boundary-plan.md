---
ddx:
  id: build-sp03-sequenced-metadata-boundary
  depends_on: [build-sp02-deterministic-storage-simulation, td-s3-object-log-sqlite-projection-mode]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 6634c5fd29d1980929354abc206f44a274102462a3fb210f9a4842a8e985e280
    deps:
      build-sp02-deterministic-storage-simulation: b25a30432dff7ec1d44e7c1951d3d0552937636ed43582e1a26b549e560571e5
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
    reviewed_at: "2026-07-20T20:00:41Z"
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
| 0 | **Complete:** close HCAS-F1/F2 with versioned authority and crash-after-fence/reopen evidence | CP `PendingFence` stops new routing/admission; an already-admitted old-epoch operation may linearize before the storage fence while the new owner is non-serving; old-epoch retries after the storage fence are rejected |
| 1 | Amend TD-004 with per-class state machines, authority, ordering, recovery, head-protocol compatibility, and typed key map | HELIX and Fable review |
| 2 | **Complete:** typed create-only, advance→delete, delete→advance, retained/free address families | compile-time separation; boundary unit tests |
| 3 | **Complete:** create-only CAS ambiguity resolves by exact-address reread | effect-then-error success; CAS loss rejection; no success-path GET |
| 4 | **Complete:** migrate composed trim, head/floor, marker/cache, and shared classifier | deterministic simulation and legacy recovery green |
| 5 | **Local condition met:** successful seal request shape unchanged; maintenance adds physical-absence GETs | full release performance matrix remains pending |

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

- [x] Floor advance is durable before eligible segment deletion; watermark advance occurs only after the
      entire contiguous eligible prefix is deleted.
- [x] Post-create checks prevent late candidates from escaping the bound.
- [x] Concurrent creators/advancers/deleters converge without hiding live metadata.
- [x] Recovery never trusts cache-only metadata and never falls below the durable horizon.
- [x] Reclaimed manifest addresses remain occupied and still reject stale create-only writes.
- [x] Watermark never advances past an undeleted, failed-delete, or branch-pinned entry.
- [x] SP-02 remains positive; generated independent-model traces cover the same
      INV-2/INV-10/INV-12 cases before migration.
- [ ] Full release-lane seal and bounded-GC benchmarks gate GET, PUT, LIST, and latency; focused inspection
      confirms no successful-hot-path request increase.

## Implementation Evidence

`pqueue-engine::sequenced_metadata` owns the reusable types, so `ComposedBackend` enforces floor publication
before deletion without depending on the adapter. The segmented adapter uses retained create-only publication
for authority/compatibility heads and a private, proof-minted completed-prefix token before watermark
advancement; no downstream crate can forge it. Watermark publication is typed as `DeletionWatermarkClass`,
while `FreeAddress` applies only to the ordered deletion closure for segment/legacy objects. The
three real eligibility walks share one classifier. Standalone watermark persistence verifies segment absence,
so a stale claimed boundary or compatibility cache cannot suppress unfinished work. SP-02's independent model
and corpus now model exact-reread-confirmed effect-then-error as success.

The completed-prefix proof receives authority mode from the manifest read that already established it; it
does not rescan retained authority-head versions. Its underlying-store request cost is therefore proportional
to the reclaimed prefix and independent of 8 versus 128 retained head versions. This is an incremental proof
bound, not an end-to-end authority-maintenance claim: the default authority-head recovery read still scans
retained versions and remains a release-scale optimization/benchmark condition.

HCAS-F1 is discharged by the PendingFence linearization rule, not by claiming immediate storage rejection:
CP `PendingFence` is non-serving. An operation admitted at the old epoch may finish before the storage fence;
the new owner cannot respond until fence, hydration, and CP confirm complete. The non-skipping in-memory test
`pending_fence_gap_has_one_safe_old_prefix_then_fences_stale_retry` and live Postgres/S3 counterpart pause
between CP reservation and storage fencing, prove old routing is unavailable, commit one already-admitted
prefix, then prove the stale retry fails after the fence and the new owner sees the prefix. The in-memory
object-log backend now performs the same reset-and-replay ownership hydration as the durable projection
adapters instead of inheriting the no-op control-plane default.

## Risks and Rollbacks

Over-generalization can erase important authority differences. Keep typed class markers and migrate only
proven-isomorphic paths. Roll back by restoring the previous adapters; durable object shapes remain readable.

## Exit Criteria

The three eligibility walks use one pure classifier; authoritative and legacy-compatible head paths share one
typed publication core without losing compatibility behavior; no hot path adds an unbudgeted object request;
and HCAS-F1/F2 have closed evidence rather than being copied into the new boundary.
