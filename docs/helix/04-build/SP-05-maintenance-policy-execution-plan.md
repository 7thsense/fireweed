---
ddx:
  id: build-sp05-maintenance-policy-execution
  depends_on: [build-sp03-sequenced-metadata-boundary, build-sp04-object-store-observability]
  links:
    - {kind: part_of, to: build-slatedb-pattern-adoption-roadmap}
    - {kind: informed_by, to: build-sp02-deterministic-storage-simulation}
    - {kind: informed_by, to: api-operator-repair-contract}
    - {kind: verified_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: ebc977e1755ccf228635818ae7c76061ba0817ad6b473180e42fb194fea1b3a3
    deps:
      build-sp03-sequenced-metadata-boundary: c212bb092c036690b331e446a3b53ee8d5d5ae47eb6237524d038b6e7fdb53db
      build-sp04-object-store-observability: 8b8a380a443ed798b0fb8fe0a5fa9884e0ead76418df42af8ba99f910773b4ca
    reviewed_at: "2026-07-18T16:20:32Z"
---

# Implementation Plan: SP-05 Maintenance Policy and Execution Separation

## Scope

Separate pure maintenance selection from effectful execution for orphan cleanup, segment/manifest reclaim,
and related bounded object-log work. Provide limits, soft resumable cursors, filters, and dry-run reports. Do not
merge queue lifecycle mutations, projection repair, lease reclaim, or public operator APIs into this iteration.

## Shared Constraints

- The planner consumes an immutable authority snapshot and returns typed decisions with reason codes and the
  exact inputs used. Snapshot assembly includes floor, manifest entries, pin registry, current epoch,
  `retention_may_advance`, recovery high-water, request-id horizon including in-memory claim-by-query pins, and
  the complete TD-004 five-way async retention frontier. It performs no I/O.
- Executor safety is per class: fenced floor CAS grants segment eligibility; orphan branches are re-derived
  under the create/GC exclusion guard; branch pin/floor races use the two-sided SP-03 protocol. Revalidation
  alone is never the safety argument.
- Every run is bounded by nonzero validated object, byte, object-request, elapsed-time, and page-size limits. Cursors are
  opaque, versioned, soft/in-memory hints; cursor loss causes an idempotent safe rescan and no durable schema.
- Dry-run follows the same discovery/planning path and performs zero writes/deletes.
- Retry is idempotent; partial completion resumes without skipping an undecided candidate.
- Execution is owner-only under a current epoch. A mid-run fence aborts without advancing a frontier/cursor
  past unresolved work and reports completed effects plus the fence.

| Decision class | Safety mechanism | Ordering |
|---|---|---|
| Retention floor and segment eligibility | Epoch-fenced CAS and full frontier snapshot | Advance floor, then delete eligible segments |
| Completed deletion/reclaimed-prefix watermark | Contiguous successful effects only | Delete, then advance watermark |
| Orphan branch | Reclassify under `create_gc_guard` across classification and delete | Guarded delete; planner decision advisory |
| Branch pin/floor interaction | Pin publication plus post-create authoritative-floor check/rollback | Two-sided SP-03 protocol |

## Implementation Slices

| Slice | Change | Validation |
|---|---|---|
| 1 | Amend TD-004/TP-003 with orphan predicates/grace windows, authority inputs, safety table, decisions, bounds, soft cursor and report schema | contract review |
| 2 | Extract pure `MaintenancePolicy` and decision types from current reclaim helpers | table tests for every retain/delete reason |
| 3 | Add owner-fenced executor with per-class protocol and checkpoint-after-effect soft cursor semantics | crash/restart, fence, and retry tests |
| 4 | Add dry-run and filters for object class, queue scope, age, and reason | zero mutation; dry-run/live decision parity |
| 5 | Migrate named reclaim entry points, wire orphan GC, and remove named duplicate loops | conformance and deterministic simulation/fallback |
| 6 | Measure requests/work/latency/memory bounds using SP-04 metrics | boundedness and no hot-path regression |

## Issue Decomposition

Extract one policy at a time. A policy/executor pair lands together with compatibility wrappers, then wrappers
are deleted after callers migrate. Generalize the existing `AsyncReclaimPlan` / `ProjectionReclaimPlanner`
and phase-aware executor shape rather than creating a second framework.

Orphan definitions land normatively before code: manifest candidate, branch, and segment each require an
unreferenced predicate, in-flight writer grace window, recovery-window interaction, and authoritative evidence.
The concrete dedup targets are `ReclaimDriver::tick` versus `emit_change_record_tick`, inline expiry eligibility
versus `manifest_reclamation_candidates_from_entries`, and currently unwired/unbounded orphan-branch GC, which
must become paged and join the single driver. Backend lease `reclaim_expired` implementations remain excluded.

Public operator exposure is deferred. API-002 is not amended by this iteration; a future proposal must choose
permission, audit, tenant scope, and async-operation semantics independently.

## Validation Plan

- [ ] Floor is advance-then-delete; completed-deletion watermark is delete-then-advance; pins and horizons remain authoritative.
- [ ] Same snapshot/config yields the same ordered decision list.
- [ ] Crash after effect but before cursor persistence causes only idempotent replay.
- [ ] Limits stop promptly and resume at the first unresolved candidate; losing a soft cursor safely rescans.
- [ ] Reports distinguish scanned, retained-by-reason, deleted, retryable failure, and permanent failure.
- [ ] A permanent failure at a contiguity frontier blocks advancement, emits an SP-04 alert/counter, and is
      retried on later runs; filtered-out candidates never authorize a frontier to skip them.
- [ ] Dry-run and live planning over the same snapshot match; executor revalidation may only downgrade delete
      to retain. Dry-run persists no cursor/report and advances no frontier.
- [ ] If SP-02 stops negatively, hand-written guard/fence/pin interleavings cover the same safety schedules.

## Risks and Rollbacks

A stale plan could delete newly pinned data. Class-specific fenced/guarded/two-sided protocols—not a naked
check-then-delete—close that race. Roll back entry-point migration while retaining pure policy tests; soft
cursors require no durable schema change.

## Exit Criteria

Maintenance is policy-driven, bounded, resumable, dry-runnable, observable, and deduplicated; existing safety
tests and deterministic GC/handoff schedules remain green.
