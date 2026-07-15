# Verification

- `rustup run 1.92.0 cargo fmt --all --check` passed.
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` passed.
- `rustup run 1.92.0 cargo test --workspace` passed.
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests read_horizon_bounds_enumeration_to_live_and_is_monotonic -- --nocapture` passed.
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests partial_expire_does_not_hide_undeleted_below_floor_segments -- --nocapture` passed.
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test object_log_commit_recovery_tests -- --nocapture` passed.
- `go test ./...` is not applicable here: no Go module or Go packages exist in the repository root.
- `lefthook run pre-commit` reported no config files in this worktree.
