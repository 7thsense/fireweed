# Branch Inheritance Release Verification

Bead: `pqueue-794856a9`

## Dependency Interaction

- `pqueue-8928baec` remains the dependency that governs retained branch inheritance and the retained-floor/head metadata path.
- `pqueue-c33c367e` was evaluated for the release note and does not widen the delete-safe envelope under the current permanent-head/index-CAS protocol.
- The retained floor remains a read/recovery aid, not an ownership fence.

## Verification Evidence

- Retained floor/head inheritance tests:
  - `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestBranchInheritanceRetainedFloorMetadataAvailable -- --nocapture`
  - `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestBranchInheritanceRetainedFloorMetadataFailClosed -- --nocapture`
- Objectlog verification:
  - `rustup run 1.92.0 cargo test --workspace`
  - Coverage includes `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`,
    `crates/pqueue-objectlog/tests/objectlog_segment_reclamation_tests.rs`, and
    `crates/pqueue-objectlog/tests/objectlog_hybrid.rs`
- SQLite verification:
  - `rustup run 1.92.0 cargo test --workspace`
- Engine verification:
  - `rustup run 1.92.0 cargo test --workspace`
- Conformance verification:
  - `rustup run 1.92.0 cargo test --workspace`
- Format gate:
  - `rustup run 1.92.0 cargo fmt --all --check`
- Clippy gate:
  - `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
- Go gate:
  - `go test ./...` returned `pattern ./...: directory prefix . does not contain main module or its selected dependencies`
  - No `go.mod` is present in this worktree, so this gate is not applicable.
- Lefthook gate:
  - `lefthook run pre-commit` reported no config files in this worktree.
- Release gate:
  - `scripts/ci/pr-gate.sh --mode enforcing`
  - The gate ran the SMOKE lane, including fmt, clippy, workspace tests, and the release-gate wrapper.
- Codex adversarial review:
  - `APPROVE` in `.ddx/executions/20260714T020946-ee4d72df/pqueue-7041bb45-review.md`

## Semantics

- This documentation-only update records release and verification traceability only.
- It does not change queue semantics, branch atomicity, retention-floor behavior, compaction behavior, or user-facing APIs.
