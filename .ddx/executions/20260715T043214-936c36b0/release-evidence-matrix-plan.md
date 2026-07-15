# Release Evidence Matrix Plan

Date: 2026-07-15
Bead: `pqueue-60472d3f`
Bundle: `.ddx/executions/20260715T043214-936c36b0`

## Exact-state verification commands

1. `rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture`
2. `rustup run 1.92.0 cargo test -p pqueue-conformance -- --nocapture`
3. `rustup run 1.92.0 cargo fmt --all --check`
4. `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
5. `go test ./...`
6. `lefthook run pre-commit`
7. `bash scripts/ci/pr-gate.sh --mode enforcing`

## Output paths

- `pr-gate-enforcing.log` — complete stdout/stderr for the enforcing gate run on this corrected state.
- `release-evidence-correction.md` — authoritative matrix, chronology correction, final audit disposition, and citations to the exact persisted artifacts.

## Completion criteria

- The corrected release note cites only persisted evidence in this bundle for the final gate authority.
- The historical failed report is preserved but marked superseded with explicit corrections.
- Every command above is either run on this exact state or truthfully classified as not applicable / operator-required.
- The final audit findings from bead `pqueue-60472d3f` are dispositioned with exact file/test evidence.
