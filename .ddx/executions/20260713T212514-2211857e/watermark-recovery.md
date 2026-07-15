# pqueue-54544d8f execution report

## Scope

Persist the manifest deletion watermark through reopen/recovery and make the acceptance criteria explicit in-tree.

## What changed

- Added exact-name wrapper tests in `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs`:
  - `TestManifestWatermarkRecoveryPersistence`
  - `TestManifestWatermarkRecoveryKeepsPresentEntriesReadable`
- No production logic was changed. The durable watermark persistence/recovery path already exists in `crates/pqueue-objectlog/src/segmented.rs`.

## Evidence

- `TestManifestWatermarkRecoveryPersistence` exercises reopen recovery of the durable watermark and the live-tail read after reopen.
- `TestManifestWatermarkRecoveryKeepsPresentEntriesReadable` exercises interrupted reclaim recovery, proving unreclaimed below-floor entries remain present across reopen until the durable watermark advances.
- Owner-fence evaluation is documented in `crates/pqueue-objectlog/src/segmented.rs`:
  - `read_read_horizon` notes that the `read_horizon.json` blob is a cache and the append-only `manifest_head/*~watermark.json` history is authoritative.
  - `persist_manifest_deletion_watermark` notes that correctness does not depend on the deferred `pqueue-c33c367e` owner-fence wiring.

## Verification

- `cargo test -p pqueue-objectlog --test object_log_commit_recovery_tests -- --nocapture`
  - Passed.
- `cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests -- --nocapture`
  - Passed.
- `go test ./...`
  - Not applicable in this repository: no `go.mod` or Go packages are present, and the command exits with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`.
- `lefthook run pre-commit`
  - Operator-required gate failure in this repository: no lefthook config files are present.
