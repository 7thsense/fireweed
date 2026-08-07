---
ddx:
  id: review-hybrid-plan-convergence
  depends_on:
    - plan-hybrid-sqlite-inmemory-projection
  review:
    self_hash: 332cf5a6832e5127728f02055524a4fbdd3173fd7f66780c423543059f9f1a3a
    deps:
      plan-hybrid-sqlite-inmemory-projection: 92334351be658f312fa8b7551eb3c1b4c22421a4a2ceb3154c6ee62e67a50df5
    reviewed_at: "2026-08-07T11:25:30Z"
---
> **Status (P19 / storage-closure): SUPERSEDED as current product guidance.** Hybrid is not a public projection matrix row; Turso is the default projection. This document is retained as historical review/evidence lineage only.

# Hybrid Plan Review Convergence

## Review Target

`docs/helix/04-build/hybrid-sqlite-inmemory-projection-plan.md`

## Round 1 Findings

The first adversarial DDx review returned `BLOCK` with these blockers:

- The recovery contract was unsafe because the existing `ProjectionStore`
  recovery API could skip the object-log prefix before memory was hydrated.
- The plan did not resolve whether local SQLite or object-store snapshots were
  the authority for segment expiry and disk-loss recovery.
- The plan did not define same-process behavior when SQLite apply succeeded but
  memory apply failed.
- The runtime path was ambiguous between generic `ComposedBackend` wiring and a
  new segmented monolith.
- Request-id replay after committed-but-unreturned object-log pushes was named
  as a test but not specified as a durable contract.
- Performance evidence was reporting-only, without pass/fail thresholds.

## Changes Made

The plan was revised to require:

- a concrete `ProjectionImage` import/export seam and memory hydration before
  returning SQLite high-water;
- object log as authority, local SQLite as owner-local restart accelerator, and
  no segment expiry based only on local SQLite;
- fail-closed poisoning after SQLite success plus memory apply failure;
- generic `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>`
  runtime wiring for `objectlog/hybrid`;
- durable push request-id replay across restart;
- explicit hot-path and recovery performance gates, including 100k smoke and
  10M release-tier restart thresholds.

## Final Check

A second DDx review found one remaining blocker: recovery performance still had
no numeric pass/fail gate. The plan was updated with concrete smoke and
release-tier recovery thresholds. The final threshold-focused review returned:

`NO BLOCKING FINDINGS`
