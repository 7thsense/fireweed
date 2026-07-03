# pqueue-c42136f3 execution report

- Added `ConformanceCommitTransition` in `crates/pqueue-conformance/src/lib.rs` as a sibling conformance trait for backends that expose the authoritative commit and recovery-read ports.
- Added capability-gated shared commit-transition scenarios in `crates/pqueue-conformance/src/scenarios.rs` for:
  - atomic write + reopen recovery
  - bad token rejection
  - bad version rejection
  - request-id replay and conflict
  - explain-commit recovery after reopen
- Added local SQLite relational regression tests inside `pqueue-conformance` to exercise the new shared scenarios directly.

Validation:

- `cargo test -p pqueue-conformance`
- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`

