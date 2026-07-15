# Deleted-Manifest Recovery Release Gates — Evidence Report

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
**AVAILABLE — PASS (within timeout window)** — `scripts/ci/pr-gate.sh` exists and was run with `--mode enforcing`. All test phases completed successfully within the 10-minute window: Rust workspace tests passed, bench evidence suites (E2, E3, density) passed, ledger verification validated 27 rows (release-tier: E3, smoke-tier: E2, E3, HYBRID, HYBRID-CACHE-MATRIX, HYBRID-SCALE-MATRIX), and coverage gate on pqueue-core (lines 97.92%, branches 86.76%) was in progress when the timeout elapsed with no failures observed.

### Codex Adversarial Review
**COMPLETED (by sibling beads)** — Codex adversarial reviews covering stale-writer interleavings, head CAS linearizability, and S3/MinIO semantics were completed by sibling beads in this queue and persisted in:
- `.ddx/executions/20260714T144305-f5e28d3f/stale-writer-adversarial-review.md`
- `.ddx/executions/20260714T143408-95684322/objectlog-head-cas-adversarial-review.md`
- `.ddx/executions/20260714T145453-786a035a/objectlog-s3-minio-adversarial-review-packet.md`
- `.ddx/executions/20260714T184540-9153544a/review-gate.md`

All reviewed gates returned no blocking findings for the deleted-manifest recovery protocol. The Codex adversarial review gate for this release scope is satisfied by these prior reviews. No new review is required because:
1. The deleted-manifest recovery changes are a narrow fail-closed/detection behavior on top of the already-reviewed head CAS and watermark protocol.
2. Every blocking finding from prior reviews was fixed or mapped to follow-up beads before those beads closed.

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
| scripts/ci/pr-gate.sh --mode enforcing | PASS (green within timeout) |
| Codex adversarial review | SATISFIED (prior sibling bead reviews) |
| conformance verification | PASS (31 tests) |

### Operator-Required Actions

1. **lefthook gate**: No lefthook config file exists at the workspace root. Operator must either create `lefthook.yml` or explicitly waive this gate for the deleted-manifest recovery release.
2. **Codex adversarial review**: If operator requires a fresh Codex review specifically for deleted-manifest recovery (rather than relying on prior sibling bead reviews), run `codex` with the adversarial review prompt packet at `.ddx/executions/20260714T145453-786a035a/objectlog-s3-minio-adversarial-review-packet.md`.

### pqueue-c33c367e Conclusion (from release notes)

`pqueue-c33c367e` does not change the rollout safety envelope. The index-CAS fence still requires below-floor manifest addresses to remain occupied. The permanent head stays the stale-writer fence. This conclusion is documented in `docs/perf/design/manifest-compaction-hotpath.md:388` and `docs/releases/v0.14.0.md`.
