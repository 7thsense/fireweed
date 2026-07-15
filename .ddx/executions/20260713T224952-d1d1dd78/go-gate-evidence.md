# Go Gate Evidence

Bead: `pqueue-2b307802`

Scope note:
- Dependency trace preserved to `pqueue-4157c36f`.
- Governing references called out in the bead description: TD-004 S3 Object-Log + SQLite Projection Mode and ADR-003 Rust Workspace and Toolchain Policy.

## Verification

- `go test ./...`
  - Not applicable in this workspace: no `go.mod` or Go packages are present at the repository root.
  - Exact output:
    ```text
    FAIL	./... [setup failed]
    # ./...
    pattern ./...: directory prefix . does not contain main module or its selected dependencies
    FAIL
    ```
  - Evidence log: [`logs/go-test.log`](logs/go-test.log)
- `lefthook run pre-commit`
  - Operator-required gate failure: Lefthook is installed, but no config files are present in this worktree.
  - Exact output:
    ```text
    │  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-2b307802-20260713T224952-d1d1dd78"
    ```
  - Evidence log: [`logs/lefthook-pre-commit.log`](logs/lefthook-pre-commit.log)

## Summary

The Go gate is not applicable in this workspace because there is no Go module or package graph to evaluate.
The Lefthook pre-commit gate fails as an operator-required environment/configuration issue, not a repo code failure.

