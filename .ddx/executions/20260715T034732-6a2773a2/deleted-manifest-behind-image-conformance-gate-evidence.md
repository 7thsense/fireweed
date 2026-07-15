# Deleted-Manifest Behind-Image Conformance Gate — Release Evidence

- Bead: `pqueue-0269a773`
- Parent: `pqueue-81a41340`
- Base rev: `645417c66b719ca4124b9d623d59fd26e78f1906`
- Bundle: `.ddx/executions/20260715T034732-6a2773a2`

## Governing Artifacts

- [`docs/perf/design/manifest-compaction-hotpath.md:374`](../../../../docs/perf/design/manifest-compaction-hotpath.md) — the `ManifestHeadBlob` permanent-head contract (§6.1): `current_epoch`, `next_seq`, `next_manifest_index`, `retention_floor_through`. Line 388 records the `pqueue-c33c367e` owner-fence evaluation: "the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e` does **not** change the rollout safety envelope for this bead."
- [`docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224`](../../../../docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md) — AC-TXN-4, the object-log crash-point matrix: 0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan segments ignored or reconciled per TD-004, stale-epoch commits rejected.

## Dependency

- `pqueue-8928baec` — governing branch-inheritance fix; the retained-floor/head and deleted-manifest evidence lives in `docs/perf/design/manifest-compaction-hotpath.md` and `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.

## pqueue-c33c367e Interaction Conclusion

`pqueue-c33c367e` (deferred server-side `fence_epoch` wiring / acquire-runtime / bounded-per-node-pools) no longer exists in the tracker. The conclusion carried forward from `docs/perf/design/manifest-compaction-hotpath.md:388` governs every surface below:

> Under the current protocol, the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e` does **not** change the rollout safety envelope for this bead.

Per-surface confirmation:

- **Objectlog**: `fail_closed_below_floor` (segmented.rs:1814) operates on the durable deletion watermark and retention floor — both persisted in the object-log substrate independently of the deferred server wiring. The behind-image conformance test `TestObjectlogBehindImageDeletedManifestFailClosed` and retained replay test `TestObjectlogBehindImageRetainedFloorHeadReplayRecovery` exercise this path.
- **Hybrid-strict**: The `ComposedBackend<ObjectLog, HybridProjectionStore, ...>` with group-commit and strict synchronous SQLite apply (`PQUEUE_PROJECTION_BACKEND=hybrid-strict`) detects a behind-image projection at recovery and fails closed with the distinct `deleted_manifest_prefix_error` signal (`read below retention floor`). The deferred server `fence_epoch` wiring is not consulted — the behind-image detection is a local consistency check on the durable retention floor and projection high-water. Proven by `TestHybridStrictBehindImageDeletedManifestFailClosed` and `TestHybridStrictBehindImageRetainedFloorHeadReplayRecovery`.
- **Hybrid-async**: The `ComposedBackend<ObjectLog, HybridProjectionStore, ...>` with async SQLite apply (`PQUEUE_PROJECTION_BACKEND=hybrid-async`) detects a behind-image projection at recovery and fails closed identically. The async apply debt monitor and ordered batching contract are independent of the deferred `fence_epoch` wiring. Proven by `TestHybridAsyncBehindImageDeletedManifestFailClosed` and `TestHybridAsyncBehindImageRetainedFloorHeadReplayRecovery`.

## Non-Scope Statement

No queue semantics, user-facing API changes, or relaxed branch atomicity, orphan GC, source pin, retention floor, or fail-closed guarantees were introduced. This bead is documentation-only release evidence — it records the interaction conclusion for the deleted-manifest behind-image conformance gates without modifying any runtime code.

## Verification Gates

### rustup 1.92.0 cargo test -p pqueue-objectlog -- --nocapture

```
test result: ok. 307 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out
```

### rustup 1.92.0 cargo test -p pqueue-conformance -- --nocapture

```
test result: ok. 143 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### cargo +1.92.0 fmt --all --check

**PASS** — no formatting issues (exit 0, no output).

### cargo +1.92.0 clippy --workspace --all-targets -- -D warnings

**PASS** — exit code 0, no warnings.

### go test ./...

**NOT-APPLICABLE** — no `go.mod` or Go module exists in the workspace root. `go test ./...` fails with "directory prefix . does not contain main module or its selected dependencies."

### lefthook run pre-commit

**OPERATOR-REQUIRED GATE FAILURE** — lefthook is installed but no lefthook config file exists (`No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found`). Exit code 0 (no-op skip) but the gate cannot execute. An operator must provide the lefthook config or explicitly waive this gate.

### scripts/ci/pr-gate.sh --mode enforcing

**AVAILABLE — NOT RUN** — `scripts/ci/pr-gate.sh` exists at `scripts/ci/pr-gate.sh` (2687 bytes, executable) but was not run in this execution worktree. The prior evidence record at `.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md` and gate evidence at `.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md` document the pr-gate result as **INCOMPLETE (not PASS)** — coverage phase timed out. This bead's documentation-only scope does not re-run the enforcement gate.

### Codex adversarial review

The direct Codex gpt-5.4 adversarial review completed on 2026-07-14 and returned **BLOCK** with two blocking findings, tracked by `pqueue-879c9d05`, `pqueue-d7134740`, and `pqueue-44a5d2ca` (per `.ddx/executions/20260714T215347-b2d013a9/release-c33c367e-evaluation.md:26-29`). This bead's documentation-only scope does not introduce new code paths, so the existing Codex BLOCK verdict from the deleted-manifest recovery evaluation remains the authoritative finding. The interaction conclusions recorded here do not alter or bypass any Codex finding.

## Test Symbols

### Objectlog behind-image conformance

| Test symbol | File |
|---|---|
| `TestObjectlogBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestObjectlogBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |

### Hybrid-strict behind-image conformance

| Test symbol | File |
|---|---|
| `TestHybridStrictBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestHybridStrictBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |

### Hybrid-async behind-image conformance

| Test symbol | File |
|---|---|
| `TestHybridAsyncBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestHybridAsyncBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |

## Related Release Notes

- [`docs/releases/v0.14.0.md`](../../../../docs/releases/v0.14.0.md) — carries the governing artifacts, dependency, `pqueue-c33c367e` conclusion, and sibling test symbols for the v0.14.0 release.
