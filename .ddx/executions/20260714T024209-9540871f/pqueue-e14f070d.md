# pqueue-e14f070d execution report

## Scope
- Added exact acceptance-test entry points in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.

## Acceptance evidence
1. `TestPartialExpireWatermarkStopsBeforeUndeletedBelowFloorSegment`
   - Evidence: wrapper test exists in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.
   - Underlying coverage: `TestManifestDeletionWatermarkPersistsAfterPhysicalDelete` simulates a partial expire that deletes an earlier below-floor segment, faults on a later below-floor segment delete, and asserts the durable watermark stays at `Some(0)` while the later segment remains present.
2. `TestPartialExpireWatermarkRetryEnumeratesRemainingSegment`
   - Evidence: wrapper test exists in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.
   - Underlying coverage: `TestInterruptedManifestReclaimRecovery` simulates the partial failure, verifies the undeleted below-floor tail remains present, reopens the log, reruns expiry, and confirms the watermark advances only after the remaining segment is reclaimed.

## Verification
- Rust targeted gate:
  - `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestPartialExpireWatermark -- --nocapture`
  - Result: passed.
- Workspace gates:
  - `rustup run 1.92.0 cargo fmt --all --check`
  - Result: passed.
  - `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
  - Result: passed.
  - `rustup run 1.92.0 cargo test --workspace`
  - Result: passed.
- Go gate:
  - `go test ./...`
  - Result: not applicable for this repository layout. The command failed with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`.
- Lefthook gate:
  - `lefthook run pre-commit`
  - Result: missing config in this worktree. Lefthook reported no config files found under the repo root.
