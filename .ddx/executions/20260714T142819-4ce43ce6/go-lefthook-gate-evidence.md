# pqueue-96bc566f Gate Evidence

Scope: acceptance-only gate evidence for the bead's Go and lefthook checks.

## Workspace Surface Check

I checked the workspace root for Go module entry points and lefthook config files before classifying the gates:

```text
find .. -name go.mod -o -name go.work -o -name lefthook.yml -o -name lefthook.yaml -o -name lefthook.toml -o -name .lefthook.yml -o -name .lefthook.yaml -o -name .lefthook.toml -o -name .pre-commit-config.yaml
```

Result: no repo-local Go module files or lefthook config files were present in this worktree.

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

Reason: the workspace does not contain a Go module or Go packages, so the repository-wide Go test gate cannot run here.

## Lefthook Gate

Command:

```text
lefthook run pre-commit
```

Observed output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-96bc566f-20260714T142819-4ce43ce6"
```

Classification: `operator_required`

Reason: the lefthook binary is available, but this worktree does not ship a lefthook config file, so the pre-commit gate cannot execute locally.

## Evidence Summary

- `go test ./...` was attempted from the workspace root and classified as `not-applicable`.
- `lefthook run pre-commit` was attempted from the workspace root and classified as `operator_required`.
