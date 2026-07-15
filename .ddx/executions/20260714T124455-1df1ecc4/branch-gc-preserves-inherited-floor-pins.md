# pqueue-37a550c6 verification report

## Change

- Added `TestBranchGcPreservesInheritedFloorPins` in `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.
- Updated `docs/releases/v0.14.0.md` with the branch-GC / `pqueue-c33c367e` interaction conclusion.

## Evidence

- `rustup run 1.92.0 cargo fmt --all --check`
- `rustup run 1.92.0 cargo test -p pqueue-objectlog TestBranchGcPreservesInheritedFloorPins -- --nocapture`
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
- `bash scripts/ci/pr-gate.sh --mode enforcing`

## Notes

- `go test ./...` was not applicable in this workspace: no `go.mod` file or Go packages were present.
- `lefthook` was not available as an executable gate in this workspace, and no `lefthook.yml`/`.lefthook.*` config was present.
- `pqueue-c33c367e` does not change the current branch-GC safety envelope; branch GC continues to rely on persisted branch metadata and source pin registry state, not on legacy source manifest recovery.
