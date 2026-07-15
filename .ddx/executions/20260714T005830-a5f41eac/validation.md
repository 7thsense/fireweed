# Validation Report

Bead: `pqueue-b6870b2e`

## Workspace Gates

- `rustup run 1.92.0 cargo fmt --all --check` - passed.
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` - passed.
- `rustup run 1.92.0 cargo test --workspace` - passed on the final run.
- `go test ./...` - not applicable; no Go module or Go packages were present in the workspace.
- `lefthook run pre-commit` - operator gate failure recorded because no `lefthook` config files were present in the repo root.

## Notes

- An earlier `cargo test --workspace` run hit a transient timeout in `objectlog_hybrid_async_push_claim_finalize_and_recovers_on_reopen`; a targeted rerun of that test passed, and the final full workspace rerun passed.
