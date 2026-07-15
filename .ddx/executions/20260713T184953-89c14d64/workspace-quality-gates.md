# Workspace Quality Gates

- Targeted bead test: `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestNoTailValidateRollbackSubstituteForFenceMarker -- --nocapture` passed.
- Additional bead test: `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestFenceMarkerDesignReferences -- --nocapture` passed as part of the full substrate suite run.
- Full Rust formatting gate: `rustup run 1.92.0 cargo fmt --all --check` passed.
- Full workspace lint gate: `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` passed.
- Full workspace test gate: `rustup run 1.92.0 cargo test --workspace` passed.
- Go gate: not applicable. `find . -name go.mod -o -name go.work` returned no results.
- Lefthook gate: `lefthook run pre-commit` completed with exit code 0 but reported no config files found in this workspace, so it is an operator-required gate gap.
