# Lefthook Closure Gate Evidence

Bead: `pqueue-fbcccc5a`
Bundle: `.ddx/executions/20260713T230046-f5a0d44c`
Reviewed commit/state: `e3751752b50b3db9e6eccacf7d945b34afaf358e`

## Go gate prerequisite

The Go gate evidence required by the bead exists for this same reviewed state because `go test ./...` was run in this checkout at `HEAD=e3751752b50b3db9e6eccacf7d945b34afaf358e`.

- Command: `go test ./...`
- Exit status: `1`
- Output:

  ```text
  FAIL	./... [setup failed]
  # ./...
  pattern ./...: directory prefix . does not contain main module or its selected dependencies
  FAIL
  ```

This is the local Go-gate evidence used as the prerequisite for the lefthook gate classification.

## Lefthook gate

- Command: `lefthook run pre-commit`
- Exit status: `0`
- Output:

  ```text
  │  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-fbcccc5a-20260713T230046-f5a0d44c"
  ```

Classification: `operator_required`

The gate could not be evaluated as a normal pre-commit run because the checkout contains no lefthook config files.

## Evidence scope

- Dependency: `pqueue-4157c36f`
- Governing references: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md` and `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`

