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
    self_hash: 19aa494aeeef8822839e5dc70dee309c87a9ad5d3d9d094adb26e4e4a03f64c8
    deps:
      build-sp03-sequenced-metadata-boundary: 6634c5fd29d1980929354abc206f44a274102462a3fb210f9a4842a8e985e280
      build-sp04-object-store-observability: 7fc689fb0f1334fee08304160a66d3215372c754dddf679ae4411c4c0d625926
    reviewed_at: "2026-07-19T01:26:09Z"
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

## Implementation Status — 2026-07-18

Implemented locally: the engine-owned pure policy and typed reasons; immutable authority assembly; actual
segment-prefix, manifest-entry, losing-manifest, orphan-segment, and orphan-branch adapters; owner fencing;
bounded paged branch cleanup with pin-last partial replay, dry-run, filters, and reports; and removal of the
duplicate emission-side reclaim loop. Lease reclaim and public operator APIs remain excluded as planned.

Focused policy and single-scheduler tests are green. Bounded-GC tests cover `page_size = 1` convergence,
hard reported request caps, partial-effect reports for retryable errors and epoch fencing, corrupt redirect
metadata, and restart completion from persisted object-size inventory. Stores must declare a one-attempt
primitive-call bound; unknown or larger hidden-retry bounds fail closed. Legacy partial branches may use one
reported, budgeted GET per unknown object size. The local owner token captures the exact authority-object key
and body digest; every destructive check budgets an exact GET plus successor LIST and fences on missing,
changed, corrupt, or superseded authority. Observed provider faults retain their structured result and
retryability in partial-effect reports. Soft live cursors resume the first unresolved branch; a dry
run that exhausts its limits safely rescans on its next invocation rather than persisting state.

Segment expiry is also bounded in the production composition path. The engine receives a typed maintenance
summary from each bounded page and merges it with orphan cleanup instead of reporting only deleted segments.
Large-prefix tests enforce object/request/page caps and convergence, while a reopen test proves that loss of
the soft traversal cursor causes reconciliation from durable reclaimed markers and delays read-horizon
publication until the traversal completes. The legacy unbounded helper remains for compatibility tests only;
the composed scheduler has no call site to it.
Soft expiry progress is keyed by queue and target sequence. Target changes restart discovery; live pins retain
the first unresolved candidate and force an incomplete report; and branch-registry discovery resumes from a
separate cursor across request-bounded passes. Regression tests cover target growth, pin-TTL release, and a
registry larger than one pass. Watermark admission reserves the ambiguity ceiling but reports the actual
observed create-only and horizon-write attempts.

The global exit criterion is not met. Full hybrid-async frontier assembly stopped as a negative spike:
current adapters cannot atomically prove committed object-snapshot recovery coverage and every five-way
durable replay minimum. Async maintenance therefore retains affected segment/manifest objects and reports
missing authority. Consequence: storage growth until an owner-fenced complete-frontier API, recovery tests,
and a retained-work/storage-growth alert land. Rollback is conservative retention or disabling that profile.
SP-04's same-run overhead comparison remains outstanding; no Niflheim or host-capacity benchmark was run or changed here.
