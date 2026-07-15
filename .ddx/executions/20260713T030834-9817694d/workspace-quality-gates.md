# Workspace Quality Gate Evidence

Execution: `pqueue-ef1f8731`
Bundle: `.ddx/executions/20260713T030834-9817694d`

## Results

| Gate | Command | Outcome | Evidence |
| --- | --- | --- | --- |
| Rust fmt | `rustup run 1.92.0 cargo fmt --all --check` | pass | Command exited `0`. Local `cargo` was the Homebrew binary, so the pinned toolchain was exercised through `rustup run 1.92.0`. |
| Rust clippy | `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | pass | Command exited `0` after completing workspace checks. |
| Go test | `go test ./...` | not-applicable | Command failed with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`, and the workspace contains no `go.mod`. |
| Lefthook pre-commit | `lefthook run pre-commit` | operator_required | Command reported `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "<repo>"`. The tool is installed, but no lefthook config exists in this workspace. |

## Notes

- The bead asked for `cargo +1.92.0 ...`; the local `cargo` binary does not support `+toolchain` syntax directly, so the same pinned toolchain was invoked via `rustup run 1.92.0 cargo ...`.
- No source files were changed for this bead.
