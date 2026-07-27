---
ddx:
  id: build-full-async-turso-projection
  depends_on:
    - adr-full-async-storage-boundaries
    - adr-async-commit-strategy-and-dispatch
    - adr-turso-derived-projection
    - td-storage-architecture-backend-contracts
    - td-object-log-turso-projection
    - tp-verification-acceptance-criteria
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-async-commit-strategy-and-dispatch}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: td-storage-architecture-backend-contracts}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: tp-verification-acceptance-criteria}
  review:
    self_hash: 7297e95a6d2b447ba76ed6dd6c306be2b80b51f86f61df74d5a4947c026acdd9
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
      adr-turso-derived-projection: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
      td-object-log-turso-projection: 1e3623771c800e9d2c6874c19e94103d00c165f1afdd27ece4760fb43f6f7f69
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
      tp-verification-acceptance-criteria: 450177278bfc6a0d50fa4c5395dea18fc6dc7738087d88bef7b062ce5fce81ab
    reviewed_at: "2026-07-20T20:00:41Z"
---

# Build Plan: Full-Async Storage and Turso Projection

## Scope

**Governing artifacts**: ADR-015, ADR-016, ADR-017, TD-001, TD-010, TP-003.

**Excluded**: Niflheim; deployment-capacity benchmarks; Turso as log/control-plane authority; remote/sync/MVCC Turso;
new broad Actions matrix dimensions; release/push activity.

## Shared Constraints

- Domain ports stay runtime-neutral and return `Send` futures.
- Async axes use shared receivers; adapter-owned synchronization must permit unrelated queues to progress.
- Composition injects `UnifiedAtomicCommit` or `SeparateReplayCommit` plus a runtime-neutral owned-task
  dispatcher; durability metadata never substitutes for the commit strategy.
- No standard mutex guard, blocking I/O, or borrowed blocking transaction crosses `.await`.
- Blocking adapters offload a complete transaction; native Turso awaits directly.
- ADR-013 log authority, response barrier, request-id replay, and retention rules remain unchanged.
- Each slice lands tests with the behavior it introduces and preserves DDx execution history.

## Implementation Slices

| Slice | Area | Depends On | Validation Gate |
|-------|------|------------|-----------------|
| AT-01 | Typed raw commit request, owned-task lifecycle, and cancellation faults (`fireweed-engine`, conformance fault modules) | None | engine fault tests; cancellation before/start/during commit |
| AT-02 | `AsyncLogStore`, `AsyncProjectionStore`, `AsyncControlPlane` plus explicit immediate/blocking adapters (`fireweed-engine`) | AT-01 | compile-time `Send` future tests; no blanket impl |
| AT-03 | Async `ComposedBackend`, typed commit strategies, owned-task dispatcher, queue-local gates, and memory/reference projection (`fireweed-engine`, `fireweed-projection`, `fireweed-memory`) | AT-02 | engine/projection/memory conformance; AC-TXN-11 |
| AT-04 | SQLite whole-transaction adapter (`fireweed-sqlite`) | AT-03 | SQLite tests plus single-thread heartbeat |
| AT-05 | Object-log/Postgres whole-transaction adapters and composition-root consumer migration | AT-03 | object-log, Postgres, server focused tests; heartbeat |
| AT-06 | Driver-neutral relational schema/codecs/rows (`fireweed-relational`, SQLite imports) | AT-04 | SQLite relational/conformance byte-for-byte parity |
| AT-07 | Native-async Turso schema/apply/query/recovery (`fireweed-turso`) | AT-06 | exact probe regressions and adapter unit tests |
| AT-08 | Full SQLite/Turso differential, reopen, cancellation, and concurrency conformance | AT-07 | AC-TURSO-1..4 |
| AT-09 | Feature-gated object-log + Turso server configuration/profile | AT-05, AT-08 | AC-TURSO-5; end-to-end recovery/reopen |
| AT-10 | Legacy sync seam removal and focused CI/config validation | AT-04, AT-05, AT-09 | structural search, AC-TURSO-6, clippy, workspace tests, HELIX validation |

AT-04 and AT-05 may run independently after AT-03. AT-07 must not copy SQLite schema or SQL; AT-09 must
not enable Turso before AT-08 differential conformance is green. Each filed bead expands its row into an
exact file list and named test commands before it can be claimed.

## Issue Decomposition

The existing beads remain the audit decomposition. Because the configured DDx harness inventory has no
Terra route, this implementation round executes the same slices through isolated sub-agents without DDx
claims or tracker mutation. Each handoff records exact file scope and tests; the primary agent reviews and
commits each non-overlapping slice and removes only worktrees/processes created by this round.

## Validation Plan

- [ ] Each bead's focused tests pass before close.
- [ ] Cancellation cuts yield zero or one recoverable outcome.
- [ ] SQLite remains the differential reference and rollback path.
- [ ] No Niflheim or deployment-capacity benchmark file changes.
- [ ] No new broad CI matrix dimension.
- [ ] Final `cargo fmt`, workspace clippy/tests, HELIX structure, and traceability checks pass without
      relying on a DDx execution route.

## Risks and Rollbacks

| Risk | Impact | Response | Rollback |
|------|--------|----------|----------|
| Trait migration causes a large conflict surface | H | Land additive contract then migrate by adapter | Retain explicit legacy wrapper until its removal bead |
| Turso misses full behavior parity | H | Stop profile enablement; retain evidence | Disable feature and rebuild SQLite from log |
| Blocking work remains above async boundary | H | Heartbeat and repository search gates | Restore bounded whole-transaction wrapper |
| CI cost expands unexpectedly | M | One focused job with path filtering | Remove optional job; keep local/release command |

## Exit Criteria

- [ ] All ten labeled beads are closed with reviewed commits.
- [ ] Turso is usable only as the feature-gated object-log-derived projection.
- [ ] The legacy synchronous storage seam and redundant composition-root blocking wrappers are absent.
- [ ] Full backend and document validation is green, with any environment-only live test gap recorded.
