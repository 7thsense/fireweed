# Verification Ledger Implementation

Bead: `pqueue-4fd1f057`

## Verified Commands

- `cargo test -p pqueue-service verification_ledger_tests`
- `cargo run -p pqueue-service --bin pqueue-verify-ledger -- --strict --ledger crates/pqueue-service/tests/fixtures/ledger_valid.jsonl`
- `cargo fmt --all --check`
- `cargo clippy -p pqueue-service --all-targets -- -D warnings`

## Evidence

- `crates/pqueue-service/src/verification_ledger.rs` defines the strict JSONL row validator and field-specific errors.
- `crates/pqueue-service/src/bin/pqueue-verify-ledger.rs` wires the CLI entrypoint to the shared validator.
- `crates/pqueue-service/tests/verification_ledger_tests.rs` validates the happy path and each missing-field fixture.
- `crates/pqueue-service/tests/fixtures/ledger_*.jsonl` provide one valid row plus one missing-required-field row for each required field.

## Acceptance Coverage

- Valid ledger fixture passes the library validator and the CLI entrypoint.
- Missing `ac_ids`, `command`, `exit_status`, `backend_profile`, `scale`, `seed`, `environment`, `suite`, `measurements`, and `pass_bar` each produce a field-specific strict-validation failure.
