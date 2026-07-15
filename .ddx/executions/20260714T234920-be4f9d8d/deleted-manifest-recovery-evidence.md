# Deleted-Manifest Recovery — Verification Evidence

- Bead: `pqueue-043c477d` (child of `pqueue-9b89f4a0`)
- Base revision: `5c4f81ed6279acc679adfbeadbd86d9c16c5e745`
- Parent bead: `pqueue-9b89f4a0`

## Governing artifacts

- [`docs/perf/design/manifest-compaction-hotpath.md:374`](../../../../docs/perf/design/manifest-compaction-hotpath.md) — the `ManifestHeadBlob` permanent-head contract (§6.1): `current_epoch`, `next_seq`, `next_manifest_index`, `retention_floor_through`. Line 388 records the `pqueue-c33c367e` owner-fence evaluation: "the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e` does **not** change the rollout safety envelope for this bead."
- [`docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224`](../../../../docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md) — AC-TXN-4, the object-log crash-point matrix: 0 lost accepted items, 0 duplicate active leases, committed commands replay exactly once, orphan segments ignored or reconciled per TD-004, stale-epoch commits rejected.

## Dependency

- `pqueue-8928baec` ("objectlog manifest compaction: reclaim tombstone + superseded floor entries below the retention floor") — **status: closed**. The durable read-horizon watermark, range-list (`list_from`), fail-closed floor guard, and physical deletion of below-floor manifest entries are the foundation every sibling deleted-manifest recovery bead consumes.

## `pqueue-c33c367e` evaluation conclusion

`pqueue-c33c367e` (deferred server-side `fence_epoch` wiring / acquire-runtime / bounded-per-node-pools) no longer exists in the tracker (`ddx bead show pqueue-c33c367e` returns `bead: not found`). The conclusion carried forward from `docs/perf/design/manifest-compaction-hotpath.md:388` governs every surface below:

> Under the current protocol, the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e` does **not** change the rollout safety envelope for this bead.

Per-surface confirmation:
- **Objectlog**: `fail_closed_below_floor` (segmented.rs:1814) operates on the durable deletion watermark and retention floor — both persisted in the object-log substrate independently of the deferred server wiring.
- **SQLite**: The `ComposedBackend` guard (compose.rs:1782-1798) emits `deleted_manifest_prefix_error` when the projection image high-water is behind the durable floor — a local consistency check independent of `fence_epoch` wiring.
- **Engine**: `ComposedBackend::recover` fails closed when the recovered projection image is behind a deleted manifest prefix, and resumes from the retained floor/head when available — neither path consults the deferred server wiring.
- **Conformance**: All conformance tests in this evidence run against `InProcessControlPlane` which does not implement the deferred wiring; pass/fail status is independent of `pqueue-c33c367e`.
- **Branch GC deletion proof (bead `pqueue-c2f0b050`)**: Deletion eligibility depends only on the persisted source-pin registry and inherited floor/head metadata, not on the deferred server wiring. The sibling branch-GC tests (`TestBranchGcDeletesBelowFloorAfterLastReadableBranch*`) prove this.

## Sibling deleted-manifest recovery test symbols

### Objectlog (`pqueue-objectlog`)

| Test symbol | File |
|---|---|
| `TestBehindImageFailClosedWithDeletedManifests` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestObjectlogDeletedManifestFailClosedSignal` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestObjectlogRetainedFloorHeadReplayStillSucceeds` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestObjectlogPqueueC33c367eInteractionRecorded` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestObjectlogPqueueC33c367eReleaseNote` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestManifestDeletionWatermarkFailClosedBelowFloor` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestBranchGcDeletesBelowFloorAfterLastReadableBranch` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestBranchGcDeletesBelowFloorAfterLastReadableBranchFailClosed` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinal` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestBranchGcDeletesBelowFloorAfterLastReadableBranchFinalConservative` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |
| `TestPartialExpireVisibilityStatePreservesFailClosedBelowFloorReads` | `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` |

### SQLite (`pqueue-sqlite`)

| Test symbol | File |
|---|---|
| `TestSqliteObjectlogDeletedManifestRecovery` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestSqliteDeletedManifestErrorPreservesGuarantees` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestSqlitePropagationPqueueC33c367eInteractionRecorded` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestSqliteObjectlogFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestSqliteFloorHeadReplayPreservesFailClosedBoundary` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestSqlitePqueueC33c367eInteractionRecorded` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |
| `TestConformanceRetentionFloorSourcePinSqliteInvariant` | `crates/pqueue-conformance/tests/sqlite_retention_floor_source_pin_conformance.rs` |

### Engine (`pqueue-engine`)

| Test symbol | File |
|---|---|
| `TestEngineObjectlogDeletedManifestRecovery` | `crates/pqueue-conformance/tests/objectlog_engine_deleted_manifest_recovery.rs` |
| `TestEngineObjectlogFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_engine_deleted_manifest_recovery.rs` |
| `TestSqliteEnginePqueueC33c367eReleaseNote` | `crates/pqueue-conformance/tests/objectlog_engine_deleted_manifest_recovery.rs` |
| `TestDeletedManifestReleaseNoteArtifacts` | `crates/pqueue-conformance/tests/objectlog_engine_deleted_manifest_recovery.rs` |

### Conformance + reclamation

| Test symbol | File |
|---|---|
| `TestBehindImageFailClosedWithDeletedManifests` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestObjectlogDeletedManifestSourcePinRetentionFloor` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestSqliteEngineBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestSqliteEngineBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestObjectlogBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestObjectlogBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestHybridStrictBehindImageDeletedManifestFailClosed` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |
| `TestHybridStrictBehindImageRetainedFloorHeadReplayRecovery` | `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` |

## Surface coverage

### Objectlog
- `fail_closed_below_floor` guard at `crates/pqueue-objectlog/src/segmented.rs:1814` — the primary fail-closed mechanism
- `deleted_manifest_prefix_error` / `is_deleted_manifest_prefix_error` at `segmented.rs:745-754` — distinct error signal
- `branch_uncommitted` at `segmented.rs:1772` — prevents GET of reclaimed source objects
- Covered by 11+ objectlog-level tests (see sibling table above)

### SQLite
- `ComposedBackend` guard at `crates/pqueue-engine/src/compose.rs:1782-1798` — emits `deleted_manifest_prefix_error`
- SQLite projection recovery fails closed when high-water is behind the durable retention floor
- Covered by 7+ SQLite-level tests (see sibling table above)

### Engine
- `ComposedBackend::recover` — fails closed for behind-projection-image, resumes from retained floor/head
- Covered by 4+ engine-level conformance tests (see sibling table above)

### Conformance
- Cross-crate conformance tests exercise objectlog + SQLite + engine combinations
- 8+ conformance-level tests (see sibling table above)

### Formatting, linting, Go, lefthook, PR gate, Codex adversarial review

| Gate | Status |
|---|---|
| `cargo fmt --all --check` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | clean, 0 warnings |
| `go test ./...` | Not applicable — no `go.mod` or Go packages found |
| `lefthook run pre-commit` | Not configured — no lefthook config found; operator-required gate failure (config absent) |
| PR gate (enforcing mode) | Command `scripts/ci/pr-gate.sh --mode enforcing` exists but was not run in this worktree (the evidence worktree does not have the full CI pipeline); operator-required gate — see report 2 for observed evidence |
| Codex adversarial review | Direct Codex gpt-5.4 review completed on 2026-07-14 and returned **BLOCK** with two blocking findings. See details below. |

## Gate results

### TestDeletedManifestEvidenceGate — `pqueue-objectlog`

```
$ rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture
```

```
test result: ok. 121 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### TestDeletedManifestEvidenceGate — `pqueue-sqlite`

```
$ rustup run 1.92.0 cargo test -p pqueue-sqlite -- --nocapture
```

```
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running tests/sqlite_projection_tests.rs
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### TestDeletedManifestEvidenceGate — `pqueue-engine`

```
$ rustup run 1.92.0 cargo test -p pqueue-engine -- --nocapture
```

```
test result: ok. 94 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running tests/dependency_direction.rs
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

Running tests/read_as_of.rs
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### TestDeletedManifestEvidenceGate — `pqueue-conformance` (evidence tests)

```
$ rustup run 1.92.0 cargo test -p pqueue-conformance -- --nocapture TestDeletedManifest
```

```
test TestDeletedManifestVerificationEvidence ... ok
test TestDeletedManifestEvidenceSurfaces ... ok
test TestDeletedManifestReleaseNoteArtifacts ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out
```

### Formatting

```
$ rustup run 1.92.0 cargo fmt --all --check
```

(no output — clean)

### Clippy

```
$ rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings
```

(no warnings — clean)

### Go test

```
$ go test ./...
```

Not applicable — no `go.mod` or Go packages found in this repository.

### Lefthook

```
$ lefthook run pre-commit
```

Not configured — `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found`. Recorded as **operator-required gate failure**: lefthook config is absent, not skipped. Every prior bead in this queue reports the same. An operator must provide the lefthook config before this gate passes.

### Codex adversarial review

A direct read-only Codex gpt-5.4 adversarial review was completed on 2026-07-14 with verdict **BLOCK**. The review identified two blocking findings, preserved here until fixing dependencies dispose them:

**Finding 1 — physical deleted-prefix/head behavior not proven by deleting projection.sqlite only**
The engine test (`TestEngineObjectlogDeletedManifestRecovery`) proves fail-closed recovery after deleting the local SQLite projection files (`projection.sqlite`, `-wal`, `-shm`), but that is a projection-only deletion — not a physical manifest/head prefix deletion on the blob store. The test does not exercise the actual deleted-manifest-prefix path where blob-store objects under `manifest/` and `manifest_head/` are removed. The fail-closed signal (`read below retention floor`) was produced by a projection-image high-water behind the durable floor, not by a blob-level manifest deletion event. So the code path that detects and signals deleted manifest prefixes at the blob storage layer is not proven by this test.

**Finding 2 — live source-pin replay across reopen unproved**
The SQLite/engine tests never reopen with a live source pin present. The test `TestEngineObjectlogFloorHeadReplayRecovery` opens a pristine new backend, trims it, drops it, reopens, and reads — but there is no source pin in play during reopen because no concurrent branch is holding one. The `ComposedBackend::recover` path that replays source-pin state from the object-log into the projection on reopen is therefore untested. A source pin that was live at crash time could be lost across reopen, silently dropping the pin's segment-retention guarantee. No test creates a branch, retains its pin, crashes the backend, reopens, and asserts the pin is still recognized.

Both findings are tracked as follow-up dependencies (`pqueue-879c9d05`, `pqueue-d7134740`, `pqueue-44a5d2ca`) per the record at `.ddx/executions/20260714T215347-b2d013a9/release-c33c367e-evaluation.md:26-29`.

## Related release notes

- [`docs/releases/v0.14.0.md`](../../../../docs/releases/v0.14.0.md) — carries the governing artifacts, dependency, `pqueue-c33c367e` conclusion, and sibling test symbols for the v0.14.0 release.
