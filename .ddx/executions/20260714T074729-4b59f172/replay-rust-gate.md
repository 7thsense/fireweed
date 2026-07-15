# Replay Rust Gate

Bead: `pqueue-34a76fe9`

Verification commands:

- `rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture`
- `rustup run 1.92.0 cargo test -p pqueue-engine -- --nocapture`

Results:

- `pqueue-objectlog`: passed, exit code `0`
- `pqueue-engine`: passed, exit code `0`

Notes:

- Both commands completed successfully in this execution worktree.
- Test output included existing integration skips for environment-gated cases, but the overall commands passed.
