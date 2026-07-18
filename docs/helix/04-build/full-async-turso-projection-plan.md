---
ddx:
  id: build-full-async-turso-projection
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 843f7c3c379521279ef58c4b9ad54b8f6f77d25b94f851c08763246f80a03af9
    deps: {}
    reviewed_at: "2026-07-18T02:29:41Z"
---

# Build Plan: Full-Async Storage and Turso Projection

## Scope

**Governing artifacts**: ADR-015, ADR-016, TD-001, TD-010, TP-003.

**Excluded**: Niflheim; quiet-host tests; Turso as log/control-plane authority; remote/sync/MVCC Turso;
new broad Actions matrix dimensions; release/push activity.

## Shared Constraints

- Domain ports stay runtime-neutral and return `Send` futures.
- No standard mutex guard, blocking I/O, or borrowed blocking transaction crosses `.await`.
- Blocking adapters offload a complete transaction; native Turso awaits directly.
- ADR-013 log authority, response barrier, request-id replay, and retention rules remain unchanged.
- Each slice lands tests with the behavior it introduces and preserves DDx execution history.

## Implementation Slices

| Slice | Area | Depends On | Validation Gate |
|-------|------|------------|-----------------|
| AT-01 | Typed async raw commit and cancellation fault contract | None | engine + conformance fault tests |
| AT-02 | Async axis traits and memory/reference composition | AT-01 | engine/projection/memory conformance; Send-future checks |
| AT-03 | Whole-transaction blocking adapters | AT-02 | SQLite/object-log/Postgres tests; runtime heartbeat |
| AT-04 | Driver-neutral relational substrate extraction | AT-03 | SQLite relational/conformance unchanged |
| AT-05 | Native-async `pqueue-turso` adapter | AT-04 | probe regressions + full differential/reopen/cancellation suite |
| AT-06 | Object-log + Turso server profile | AT-05 | server end-to-end, recovery, feature-disabled error |
| AT-07 | Legacy sync seam removal and focused CI/config validation | AT-06 | repository search gate, clippy, workspace tests, HELIX validation |

AT-03 and early AT-04 preparation may run independently after AT-02 only if their file scopes do not
overlap. AT-05 must not copy SQLite schema or SQL; AT-06 must not enable Turso before AT-05 differential
conformance is green.

## Issue Decomposition

Every bead carries labels `helix`, `activity:build`, `kind:build`,
`plan:full-async-turso`, exact file scope, named tests, and `spec-id` pointing to TD-010 or ADR-015.
Dependencies mirror AT-01 through AT-07. Terra is an execution routing constraint, not a tracker label.

## Validation Plan

- [ ] Each bead's focused tests pass before close.
- [ ] Cancellation cuts yield zero or one recoverable outcome.
- [ ] SQLite remains the differential reference and rollback path.
- [ ] No Niflheim or quiet-host test file changes.
- [ ] No new broad CI matrix dimension.
- [ ] Final `cargo fmt`, workspace clippy/tests, `ddx doc validate`, and traceability checks pass.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|------|--------|----------|----------|
| Trait migration causes a large conflict surface | H | Land additive contract then migrate by adapter | Retain explicit legacy wrapper until its removal bead |
| Turso misses full behavior parity | H | Stop profile enablement; retain evidence | Disable feature and rebuild SQLite from log |
| Blocking work remains above async boundary | H | Heartbeat and repository search gates | Restore bounded whole-transaction wrapper |
| CI cost expands unexpectedly | M | One focused job with path filtering | Remove optional job; keep local/release command |

## Exit Criteria

- [ ] All seven labeled beads are closed with reviewed commits.
- [ ] Turso is usable only as the feature-gated object-log-derived projection.
- [ ] The legacy synchronous storage seam and redundant composition-root blocking wrappers are absent.
- [ ] Full backend and document validation is green, with any environment-only live test gap recorded.

