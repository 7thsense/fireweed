---
ddx:
  id: review-hybrid-plan-convergence
  depends_on:
    - plan-hybrid-sqlite-inmemory-projection
  review:
    self_hash: 575b3529a97ffd63e4b01a1cb1420d531295134f1996b323ff74bb3faec61a61
    deps:
      plan-hybrid-sqlite-inmemory-projection: 5695ee9c00997c12cf2df378af020ae38c2b1e095b2d7e9bc1b3083fc6fb6b65
    reviewed_at: "2026-07-06T00:56:00Z"
---

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
