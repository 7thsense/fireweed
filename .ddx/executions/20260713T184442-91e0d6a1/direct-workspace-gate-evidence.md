# Direct Workspace Gate Evidence

Bead: `pqueue-b4370979`
Bundle: `.ddx/executions/20260713T184442-91e0d6a1`

Dependency trace preserved for this bead:
- dependency `pqueue-4157c36f`
- governing reference: `TD-004 S3 Object-Log + SQLite Projection Mode`
- governing reference: `ADR-003 Rust Workspace and Toolchain Policy`

## Workspace surface check

Command:

```text
rg --files -g 'go.mod' -g '*.go' -g '.lefthook*' -g 'lefthook*' .
```

Observed result:

```text
<no output>
```

Interpretation:
- No Go module, Go packages, or lefthook config files are present in this workspace root.

## Direct Go gate

Command:

```text
go test ./...
```

Exit status:

```text
1
```

Relevant output:

```text
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL	./... [setup failed]
FAIL
```

Classification:

```text
not-applicable
```

Reason:
- The workspace does not contain a Go module or any Go packages, so `go test ./...` cannot execute package tests here.
- The failure is module-discovery setup failure, not a project test failure.

## Direct lefthook gate

Command:

```text
lefthook run pre-commit
```

Exit status:

```text
0
```

Relevant output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-b4370979-20260713T184442-91e0d6a1"
```

Classification:

```text
operator_required
```

Reason:
- The lefthook binary is available, but the workspace does not ship a lefthook config file, so the pre-commit gate cannot be meaningfully executed here without operator-provided configuration.

## Evidence summary

- `go test ./...` was attempted from the workspace root and recorded as `not-applicable`.
- `lefthook run pre-commit` was attempted from the workspace root and recorded as `operator_required`.
- Both gates are recorded separately from `scripts/ci/pr-gate.sh --mode enforcing` so the aggregate wrapper cannot hide the direct workspace gate status.
