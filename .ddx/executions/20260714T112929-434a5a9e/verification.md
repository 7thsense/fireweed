# Verification

## Targeted regression

- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestPartialExpireVisibilityDecision -- --nocapture`
- Result: passed

## Workspace gates

- `rustup run 1.92.0 cargo fmt --all --check`
- Result: passed

- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
- Result: passed

- `rustup run 1.92.0 cargo test --workspace`
- Result: passed

## Non-Rust gates

- `go test ./...`
- Result: not applicable. No `go.mod` or Go package root is present in this workspace.

- `lefthook run pre-commit`
- Result: operator-required gate note. `lefthook` is installed, but no config file was found in the workspace (`lefthook`, `.lefthook`, or `.config/lefthook`).
