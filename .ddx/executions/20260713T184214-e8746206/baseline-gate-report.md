# Baseline Gate Evidence

Bead: `pqueue-20150328`

Scope note:
- This bead preserves the dependency trace to `pqueue-4157c36f`.
- Governing references called out in the bead metadata/description: TD-004 S3 Object-Log + SQLite Projection Mode and ADR-003 Rust Workspace and Toolchain Policy.

## Verification

- `rustup run 1.92.0 cargo fmt --all --check`
  - Pass.
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
  - Pass.
- `rustup run 1.92.0 cargo test --workspace`
  - Pass.
- `go test ./...`
  - Not applicable in this workspace: `go.mod` is absent and Go reports `pattern ./...: directory prefix . does not contain main module or selected dependencies`.
- `lefthook run pre-commit`
  - Operator-required failure: no lefthook config files were found in the workspace root.

## Code/Tests Touched

- `crates/pqueue-objectlog/src/segmented.rs`
- `crates/pqueue-objectlog/tests/object_log_commit_recovery_tests.rs`
- `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`
