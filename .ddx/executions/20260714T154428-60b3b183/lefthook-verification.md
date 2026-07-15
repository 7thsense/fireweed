# Objectlog Lefthook Verification Evidence

- Bead: `pqueue-be871917` (child 1 of 3 of `pqueue-82bb2905`)
- Dependency: `pqueue-4157c36f`
- Governing refs: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`,
  `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`
- Base revision: `b6f54e3a4d765d1dd1ca27c33b73a375d3995f70`

## TestObjectlogLefthookVerificationCommand

Command: `lefthook run pre-commit`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-be871917-20260714T154428-60b3b183"
```

Exit status: `0`

## TestObjectlogLefthookVerificationOperatorFallback

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this
worktree does not ship a `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file, so lefthook has no
pre-commit hooks to execute. A repo-wide check (`find . -iname "lefthook*" -not -path "./.git/*"`) turns up
only prior evidence docs referencing lefthook, not a config file. Per this acceptance criterion, the missing
config is recorded as an `operator_required` gate failure rather than a passing gate — the process exit code
of `0` reflects lefthook's own "no config found" no-op, not a successful pre-commit run. The exact failing
command is `lefthook run pre-commit`; its full output is the block above.

## TestObjectlogLefthookVerificationGoGate

Command: `go test ./...`

Output:

```text
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL	./... [setup failed]
FAIL
```

Exit status: `1`

Classification: `not-applicable`

Reason: no `go.mod` exists anywhere in the repository (`find . -name go.mod -not -path "./.git/*"` returns
nothing) and there are no `.go` source files (`find . -name "*.go" -not -path "./.git/*"` returns nothing).
No Go module or packages exist for `go test` to run against.

## Evidence Summary

- `lefthook run pre-commit` was run from the workspace root and classified `operator_required` (missing
  config file; the tool itself is present and runnable).
- `go test ./...` was run from the workspace root and classified `not-applicable` (no Go module/packages in
  this repository).
- Dependency `pqueue-4157c36f` ("objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening"; open) and governing references TD-004 S3 Object-Log + SQLite Projection Mode
  (manifest commit as CAS/fencing enforcement point, line 188; conditional-write primitives, line 218;
  deletion precondition, line 570; provider-specific live S3 hardening limited to deployment certification,
  line 730) and ADR-003 Rust Workspace and Toolchain Policy (governs the toolchain/workspace gate commands
  this probe relies on) are recorded here alongside the gate outcomes above.
- See sibling children of `pqueue-82bb2905` for the remaining parent ACs: `pqueue-d748645a` (PR gate context
  and Go verification gate) and `pqueue-138f60ed` (consolidated evidence index).
