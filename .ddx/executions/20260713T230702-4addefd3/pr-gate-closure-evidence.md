# PR Gate Closure Evidence

- Bead: `pqueue-dbe7878e`
- Attempt: `20260713T230702-4addefd3`
- Bundle: `.ddx/executions/20260713T230702-4addefd3`
- Reviewed commit/state: `bed2d6676a76bea5ae1be73d501d93fad130cefb`
- Worktree state at evidence-record time: clean

## Governing References

- Dependency bead: `pqueue-4157c36f`
- Technical design: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`
- Toolchain policy: `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`

## Gates

### PR gate probe

- Command: `bash scripts/ci/pr-gate.sh --mode enforcing`
- Exit status: `101`
- Result: `operator_required`
- Last output:

  ```text
  === pr-gate [mode=enforcing] ===
  --- fmt ---
  error: no such command: `+1.92.0`

  help: invoke `cargo` through `rustup` to handle `+toolchain` directives
  ```

### Go gate

- Go module discovery: no `go.mod` or `go.work` files were present in this checkout.
- Command: `go test ./...`
- Exit status: `1`
- Result: `not_applicable`
- Output:

  ```text
  FAIL	./... [setup failed]
  # ./...
  pattern ./...: directory prefix . does not contain main module or its selected dependencies
  FAIL
  ```

### Lefthook gate

- Command: `lefthook run pre-commit`
- Exit status: `0`
- Result: `operator_required`
- Output:

  ```text
  │  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-dbe7878e-20260713T230702-4addefd3"
  ```

## Scope

This bundle will record the exact local outputs for the PR gate probe, Go gate,
and lefthook pre-commit gate, or classify a gate as `operator_required` or
`not_applicable` where the repository state makes execution non-actionable.

## Notes

- The worktree was clean when this evidence was recorded.
