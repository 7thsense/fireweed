---
ddx:
  id: build-unified-packed-mutation
  type: implementation-plan
  links:
    - {kind: informed_by, to: td-unified-packed-mutation-path}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: ss-objectlog-turso-memory-goal}
    - {kind: informed_by, to: tp-fireweed-performance-matrix}
---

# Build: unified packed add / update / delete

Governing design: `docs/helix/02-design/technical-designs/TD-016-unified-packed-mutation-path.md`.

Seventh Sense P1–P4 are the same path under load. Do not file per-phase
protocols. If a stage is slow, instrument snapshot / realize / append / apply
on this path.

## Shared Constraints

- Log is authority (ADR-013). Envelopes stay distinct. Apply is derived.
- Generation caps stay 8 requests, 800 items, 4 MiB, 20 ms linger, 2 gens / 16
  requests per queue.
- No SQL-first Claim, exclusive item-claim fence, Complete Bypass
  `KeyedQueueGate`, `SKIP LOCKED`, or apply-wait-before-return.
- Settled rates only. Ack-only is not success. T3 exact after P4.

## Implementation Slices

| Slice | Area | Governing | Depends | Validation |
|---|---|---|---|---|
| U1 | Two-kind dispatch + overlay | TD-016 pipeline | None | Classifier + overlay unit tests; compose audits: item Claim and Complete call `drive_candidate_mutation`; no `ClaimCoordinator` on the item path |
| U2 | In-flight Claim exclude | TD-016 overlay bound | U1 | SELECT bind cap ≤ 1600; N=10k P4 does not poison `moved 0 of N` |
| U3 | Set-based packed apply | TD-016 apply | U1 | Packed Claim/Complete/UpdateFields apply matches solo model in one IMMEDIATE |
| U4 | Qualify 10k/s | TD-016 performance | U2, U3 | `ss_phased_capacity_smoke` N=10k filesystem--turso: each phase `settled_items_per_s` ≥ 10000 and T3 exact |

U2 and U3 may proceed in parallel after U1.

## Issue Decomposition

Labels: `area:turso`, `area:performance`, `kind:task`, `activity:build`,
`plan-2026-09-03`. `spec-id`: `docs/helix/02-design/technical-designs/TD-016-unified-packed-mutation-path.md`.

| Slice | Goal |
|---|---|
| U1 | One sequencer, two kinds, one driver |
| U2 | Claim SELECT cost is O(in-flight), not O(N) |
| U3 | Packed vector apply is set-based |
| U4 | All four measured phases ≥ 10k settled items/s |

## Validation Plan

- Classifier/overlay tests exist before treating U1 closed.
- U4 uses the exact harness command in TD-016. Child closure of U1–U3 without
  U4 evidence does not close the epic.
- `cargo fmt --all --check` and `git diff --check` on every slice.

## Risks and Rollbacks

| Risk | Response | Rollback |
|---|---|---|
| Overlay miss → double-claim | Keep in-flight leased ids; do not wait apply as a substitute | Revert U2 only |
| Coalesced apply reorders mixed kinds | Adjacent same-kind runs only | Revert U3 |
| P2/P3 still apply-bound after U3 | Profile apply on this path; new slice under TD-016, not a new protocol | Keep U1/U2 |
