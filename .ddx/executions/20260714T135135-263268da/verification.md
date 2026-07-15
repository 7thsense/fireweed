# Workspace Quality Gate Evidence

Bead: `pqueue-b98548aa`

## Commands Run

| Command | Outcome | Notes |
| --- | --- | --- |
| `rustup run 1.92.0 cargo fmt --all --check` | pass | Ran successfully under toolchain `1.92.0`. |
| `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | pass | Ran successfully under toolchain `1.92.0`. |
| `rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture` | pass | Replay-related crate coverage completed successfully. |
| `rustup run 1.92.0 cargo test -p pqueue-engine -- --nocapture` | pass | Replay-related crate coverage completed successfully. |
| `go test ./...` | not applicable | No `go.mod` or Go packages exist in this checkout. |
| `lefthook run pre-commit` | gate unavailable | Lefthook reported no config files in the repository root. |
| `scripts/ci/pr-gate.sh --mode enforcing` | pass | Enforcing PR gate completed successfully. |

## Evidence Paths

- `logs/cargo-fmt-check.log`
- `logs/cargo-clippy.log`
- `logs/cargo-test-pqueue-objectlog.log`
- `logs/cargo-test-pqueue-engine.log`
- `logs/lefthook-pre-commit.log`
- `logs/pr-gate-enforcing.log`

## Notes

- The workspace `cargo` binary in this checkout does not accept the `+1.92.0` syntax directly, so the equivalent `rustup run 1.92.0 cargo ...` form was used.
- The direct `lefthook run pre-commit` invocation exited cleanly after reporting missing config files, which is the only available evidence for that gate in this checkout.
