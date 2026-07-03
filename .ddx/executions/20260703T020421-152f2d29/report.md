# pqueue-0ef6807b — commit-transition explicit-decline tests

## What changed

- Added an explicit-decline regression test to `crates/pqueue-postgres/tests/hot_projection_queries.rs`.
- Added an explicit-decline regression test to `crates/pqueue-objectlog/tests/hot_projection_queries.rs`.
- Each test asserts `CommitCapabilities::default()` and verifies `commit_transition`, `explain_commit`, and `side_record` return `EngineError::Unavailable`.
- The postgres test covers both `PostgresBackend` and `PostgresRelationalBackend`.

## Acceptance evidence

- `cargo test -p pqueue-postgres commit_transition_capabilities_are_explicit`
- `cargo test -p pqueue-objectlog commit_transition_capabilities_are_explicit`
- `rg -n 'CommitCapabilities::default\\(\\)' crates/pqueue-postgres/tests crates/pqueue-objectlog/tests`
- `cargo fmt --check`

## Notes

- The tests remain env-gated and skip loudly when the required backend configuration is absent.
