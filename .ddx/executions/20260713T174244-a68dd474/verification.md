# Verification

- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestManifestWatermarkPartialExpireVisibility -- --nocapture`: passed
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestBranchPinRulesUnchanged -- --nocapture`: passed
- `rustup run 1.92.0 cargo fmt --all --check`: passed
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`: passed
- `rustup run 1.92.0 cargo test --workspace`: passed
- `go test ./...`: not applicable, repository root is not a Go module (`pattern ./...: directory prefix . does not contain main module or its selected dependencies`)
- `lefthook run pre-commit`: operator gate failure, no lefthook config files found in the workspace
