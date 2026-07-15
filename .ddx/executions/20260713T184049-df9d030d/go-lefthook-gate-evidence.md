# pqueue-0758381a Gate Evidence

Scope: objectlog Go package discovery gate evidence.

Dependency trace preserved for this bead:
- dependency `pqueue-4157c36f`
- governing reference: `TD-004 S3 Object-Log + SQLite Projection Mode`
- governing reference: `ADR-003 Rust Workspace and Toolchain Policy`

## Workspace Surface Check

I checked the workspace for Go and lefthook entry points before running the gates:

```text
rg --files -g 'go.mod' -g '*.go' -g '.lefthook*' -g 'lefthook*' .
```

Result: no Go module, Go packages, or lefthook config files are present in this workspace.

## Go Gate

Command:

```text
go test ./...
```

Observed output:

```text
FAIL	./... [setup failed]
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL
```

Classification: `not-applicable`

Reason: the workspace does not contain a Go module or any Go packages, so `go test ./...` cannot execute package tests here. The command fails at module discovery time rather than exposing a real Go test failure.

## Lefthook Gate

Command:

```text
lefthook run pre-commit
```

Observed output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-0758381a-20260713T184049-df9d030d"
```

Classification: `operator_required`

Reason: the lefthook binary is available, but the workspace does not ship a lefthook config file, so this gate cannot run locally and requires operator action.

## Evidence Summary

- `go test ./...` was attempted from the workspace root and classified as `not-applicable`.
- `lefthook run pre-commit` was attempted from the workspace root and classified as `operator_required`.
- The bead's dependency trace and governing references are preserved above for later audit.
