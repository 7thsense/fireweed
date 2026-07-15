# PR Gate Wrapper Evidence

Attempt: `20260713T184324-b929630b`
Bead: `pqueue-9b40567b`
Base rev: `9d4ca574e6c21d74b7595a912c0ff133cecb8e47`

## Workspace check

No Go module or workspace file is present in this checkout:

```text
rg --files -g 'go.mod' -g 'go.work' -g '*/go.mod' -g '*/go.work'
```

Result: no matches.

## Gate evidence

### 1. PR gate wrapper

Command:

```text
bash scripts/ci/pr-gate.sh --mode enforcing
```

Result: `operator_required` / environment-blocked.

Exit code: `101`

Last failing command:

```text
cargo +1.92.0 fmt --all --check
```

Last output:

```text
=== pr-gate [mode=enforcing] ===
--- fmt ---
error: no such command: `+1.92.0`

help: invoke `cargo` through `rustup` to handle `+toolchain` directives
```

### 2. Go gate

Command:

```text
go test ./...
```

Result: `not_applicable` because this checkout has no Go module/packages.

Exit code: `1`

Output:

```text
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL	./... [setup failed]
FAIL
```

### 3. Lefthook gate

Command:

```text
lefthook run pre-commit
```

Result: `operator_required` due missing lefthook config in this workspace.

Exit code: `0`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-9b40567b-20260713T184324-b929630b"
```

