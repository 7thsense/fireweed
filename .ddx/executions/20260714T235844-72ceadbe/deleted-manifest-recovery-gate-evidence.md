# Deleted-Manifest Recovery Release Gates — Evidence Report

## Supersession / correction (2026-07-15)

This report is preserved as historical evidence from failed bead attempt `pqueue-0269a773`, but it is **not**
current release authority. The authoritative corrected evidence for release closure now lives in
`.ddx/executions/20260715T043214-936c36b0/release-evidence-correction.md` with the matching enforcing gate log at
`.ddx/executions/20260715T043214-936c36b0/pr-gate-enforcing.log`.

Corrections to this historical report:

- The claim that `pqueue-8928baec` had already evaluated ownership work from `pqueue-7bac12ce` and
  `pqueue-b29435b2` was too strong. `pqueue-8928baec` closed earlier; the ownership interaction was
  reevaluated only after `pqueue-7bac12ce` landed on 2026-07-14 22:51 EDT and `pqueue-b29435b2` landed on
  2026-07-14 23:07 EDT.
- The deleted-manifest behavior must be split into two claims. Projection-image-behind tests prove fail-closed
  behavior when a recovered image is behind the durable floor and deleted prefix. The physical deletion test at
  `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` proves that deleting both `manifest/` and
  `manifest_head/` reopens conservatively with surviving read-horizon state and `retention_floor=None`; it does
  not prove fail-closed reopen for that physical deletion case.
- Any gate status in this report predates the final corrected-state rerun and must not be cited for release
  closure. Use the persisted 2026-07-15 bundle instead.
- Historical Codex findings remain part of the record, but no independent no-blocker verdict should be inferred
  from this report.

Bead: `pqueue-819b38ed`
Parent: `pqueue-9b89f4a0`
Bundle: `.ddx/executions/20260714T235844-72ceadbe`
Base rev: `92d3321fe41ee0ef4570f61f00e4f4b1127e1dce`

## pqueue-c33c367e Release-Note Conclusion

**Status: evaluated — no effect on rollout safety envelope.**

`pqueue-c33c367e` (deferred server-side `fence_epoch` wiring, ownership.rs:13-22) was evaluated across every gate path in this bead. The bead ID no longer exists in the tracker. The conclusion documented at `docs/perf/design/manifest-compaction-hotpath.md:388` governs: the index-CAS fence still requires below-floor manifest addresses to remain occupied, so `pqueue-c33c367e`'s deferred server-side `fence_epoch` wiring does **not** change the rollout safety envelope. The permanent head remains the stale-writer fence. This conclusion is carried in `docs/releases/v0.14.0.md`.

## Dependency ID

`pqueue-8928baec` — governing branch-inheritance fix; the retained-floor/head and deleted-manifest evidence lives in `docs/perf/design/manifest-compaction-hotpath.md` and `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.

## Governing Artifacts

- `docs/perf/design/manifest-compaction-hotpath.md:374` — permanent head contract
- `docs/helix/03-test/test-plans/TP-003-verification-acceptance-criteria.md:224` — AC-TXN-4 object-log crash-point matrix

---

## TestSqliteEngineDeletedManifestGate

### rustup 1.92.0 cargo test -p pqueue-objectlog -- --nocapture
**PASS** — 306 tests (24 unit + 5 group_commit + 8 reconnect + 31 composed_sqlite + 73 conformance + 10 durability + 2 hot_projection + 5 hybrid_async_chaos + 1 hybrid_request_id + 14 commit_recovery + 7 segment_commit + 4 reconnect_smoke + 1 retention_floor_source_pin + 121 segmented_s3_substrate). 0 failed.

### rustup 1.92.0 cargo test -p pqueue-sqlite -- --nocapture
**PASS** — 310 tests (14 unit + 2 composed_commit_duplicate + 4 log_projection_reconnect + 5 log_reconnect + 8 relational_parity + 8 relational_reconnect + 2 relational_terminal_reap + 42 conformance + 1 cross_family_parity + 4 durability + 13 hybrid_async_backpressure + 10 hybrid_async_chaos + 7 hybrid_async_checkpoint + 4 hybrid_async_recovery + 4 reconnect_smoke + 6 relational_commit + 170 relational_conformance + 10 relational_reconnect + 10 sqlite_projection). 0 failed.

### rustup 1.92.0 cargo test -p pqueue-engine -- --nocapture
**PASS** — 96 tests (94 unit + 1 dependency_direction + 1 read_as_of). 0 failed.

### cargo +1.92.0 fmt --all --check
**PASS** — no formatting issues.

### cargo +1.92.0 clippy --workspace --all-targets -- -D warnings
**PASS** — exit code 0, no warnings.

### go test ./...
**NOT-APPLICABLE** — no Go module or package exists anywhere under the workspace root (`go.mod` not found at any level).

### lefthook run pre-commit
**OPERATOR-REQUIRED** — lefthook v2.1.10 is installed but no lefthook config file exists (`lefthook.yml`, `.lefthook.yml`, `.config/lefthook.yml` all absent). Exit code 0 (no-op skip) but gate cannot execute. Operator must create and configure lefthook or explicitly waive this gate.

### scripts/ci/pr-gate.sh
**AVAILABLE — INCOMPLETE (not PASS)** — `scripts/ci/pr-gate.sh` exists and was run with `--mode enforcing`. Rust workspace tests and bench evidence suites (E2, E3, density) passed, and ledger verification validated 27 rows. However, the coverage gate on pqueue-core (lines 97.92%, branches 86.76%) was still in progress when the timeout elapsed; its exit code is unknown. The enforcing gate is only PASS if the full command exits zero. A phase with unknown exit (incomplete coverage) means the gate command did not complete, therefore the result is not PASS.

### Codex Adversarial Review
**BLOCK** — A direct read-only Codex gpt-5.4 adversarial review completed on 2026-07-14 returned verdict BLOCK with two blocking findings. Stale sibling bead reviews do not substitute for a fresh review of the deleted-manifest recovery protocol. Prior reviews (stale-writer interleavings, head CAS linearizability, S3/MinIO semantics) reviewed different protocol surfaces and their non-blocking status does not cover the deleted-manifest recovery changes.

The two BLOCKING findings from the direct Codex gpt-5.4 review:

**Finding 1 — physical deleted-prefix/head behavior not proven by deleting projection.sqlite only.** The engine test deletes the local SQLite projection files, not the blob-store manifest/head prefix; the fail-closed signal comes from projection-image high-water behind the durable floor, not from a blob-level manifest deletion. The code path that detects and signals deleted manifest prefixes at the blob storage layer is unproven.

**Finding 2 — live source-pin replay across reopen unproved.** No test creates a branch, retains its source pin, kills the backend, reopens, and verifies the pin is still recognized. The `ComposedBackend::recover` source-pin replay path is untested.

Both findings are tracked by `pqueue-879c9d05`, `pqueue-d7134740`, and `pqueue-44a5d2ca` (per `.ddx/executions/20260714T215347-b2d013a9/release-c33c367e-evaluation.md:26-29`). These blocking findings must be resolved before the deleted-manifest recovery release gate can be called PASS.

---

## TestDeletedManifestConformanceGate

**PASS** — Conformance verification was run and passed:

### rustup 1.92.0 cargo test -p pqueue-conformance -- --nocapture
**PASS** — 31 conformance tests (across objectlog_segment_reclamation_tests, sqlite_retention_floor_source_pin_conformance, and objectlog_engine_deleted_manifest_recovery) including deleted-manifest fail-closed and retained-floor/head replay tests. 0 failed.

Discovery path: `crates/pqueue-conformance/` contains a `Cargo.toml` with test targets including `objectlog_segment_reclamation_tests.rs`, `sqlite_retention_floor_source_pin_conformance.rs`, and `objectlog_engine_deleted_manifest_recovery.rs`. These cover:
- `TestBehindImageFailClosedWithDeletedManifests`
- `TestObjectlogDeletedManifestSourcePinRetentionFloor`
- `TestObjectlogDeletedManifestFailClosedSignal`
- `TestObjectlogRetainedFloorHeadReplayStillSucceeds`
- `TestSqliteObjectlogDeletedManifestRecovery`
- `TestSqliteDeletedManifestErrorPreservesGuarantees`
- `TestSqlitePropagationPqueueC33c367eInteractionRecorded`
- `TestSqliteObjectlogFloorHeadReplayRecovery`
- `TestSqliteFloorHeadReplayPreservesFailClosedBoundary`
- `TestSqlitePqueueC33c367eInteractionRecorded`

---

## TestDeletedManifestGateEvidenceCompleteness

All gate evidence is recorded above. Summary:

| Gate | Result |
|------|--------|
| cargo test -p pqueue-objectlog | PASS (306 tests) |
| cargo test -p pqueue-sqlite | PASS (310 tests) |
| cargo test -p pqueue-engine | PASS (96 tests) |
| cargo fmt --all --check | PASS |
| cargo clippy --workspace --all-targets -D warnings | PASS |
| go test ./... | NOT-APPLICABLE (no Go module) |
| lefthook run pre-commit | OPERATOR-REQUIRED (no lefthook config) |
| scripts/ci/pr-gate.sh --mode enforcing | INCOMPLETE (coverage phase timed out; not PASS) |
| Codex adversarial review | BLOCK (2 blocking findings, tracked by pqueue-879c9d05, pqueue-d7134740, pqueue-44a5d2ca) |
| conformance verification | PASS (31 tests) |

### Operator-Required Actions

1. **lefthook gate**: No lefthook config file exists at the workspace root. Operator must either create `lefthook.yml` or explicitly waive this gate for the deleted-manifest recovery release.
2. **Codex blocking findings**: The direct Codex gpt-5.4 review returned BLOCK with two blocking findings tracked by `pqueue-879c9d05`, `pqueue-d7134740`, and `pqueue-44a5d2ca`. These must be resolved (fixing dependencies and final release evidence) before the deleted-manifest recovery gate can be called PASS. Preserve the blocking findings until their fixing dependencies dispose them.
3. **PR gate incomplete**: `scripts/ci/pr-gate.sh --mode enforcing` did not complete — the coverage phase timed out with unknown exit. A full successful run must complete before the enforcing gate is PASS.

### pqueue-c33c367e Conclusion (from release notes)

`pqueue-c33c367e` does not change the rollout safety envelope. The index-CAS fence still requires below-floor manifest addresses to remain occupied. The permanent head stays the stale-writer fence. This conclusion is documented in `docs/perf/design/manifest-compaction-hotpath.md:388` and `docs/releases/v0.14.0.md`.
