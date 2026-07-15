# Objectlog Local Validation Gates (Independent of PR Probe)

- Bead: `pqueue-4af087ec` — "objectlog: record local validation gates independently" (child of `pqueue-d4699907`)
- Dependency: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening" (open)
- Governing refs:
  - **TD-004 S3 Object-Log + SQLite Projection Mode**
    (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:570`) — retention/
    expiry section calling for reproducible verification of the deletion-frontier and replay guarantees this
    validation pass backstops.
  - `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:730` — the required
    evidence surface / validation boundary section, whose repository-gate half (`go test ./...`,
    `lefthook run pre-commit`) this bead isolates from the PR-probe half.
  - **ADR-003 Rust Workspace and Toolchain Policy**
    (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`) — governs the toolchain and
    workspace-quality-gate expectations these repository gates are drawn from.
- Base revision: `c17b8048d61d1286e5532dd5bb4437c762f4725e` (this bead's `base-rev`, current `HEAD`).

This bead isolates the *local repository gate* half of validation (Go, lefthook) from the *enforcing PR gate*
probe recorded independently in sibling bead `pqueue-9f17b246`
(`.ddx/executions/20260714T172554-b973bc8a/pr-gate-probe.md`), per this bead's non-scope: "Do not rerun the
enforcing PR gate probe." Both gates below were executed fresh in this worktree at the base revision above.

## TestObjectlogValidationGoGate

Command: `go test ./...`

Output (`.ddx/executions/20260714T173037-98595ea8/go-gate.txt`):

```text
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL	./... [setup failed]
FAIL
```

Exit status: `1`

Classification: **not-applicable**. `find . -name go.mod -not -path "./.git/*"` and
`find . -name "*.go" -not -path "./.git/*"` both return no matches — no Go module or `.go` source files exist
anywhere in this repository for `go test` to run against. `go` itself is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/go`, `go version go1.26.5 linux/amd64`); the failure is solely the absence of
a module, not tool unavailability.

## TestObjectlogValidationLefthookGate

Command: `lefthook run pre-commit`

Output (`.ddx/executions/20260714T173037-98595ea8/lefthook-gate.txt`):

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-4af087ec-20260714T173037-98595ea8"
```

Exit status: `0`

Classification: `operator_required`. The `lefthook` binary is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/lefthook`, version `2.1.10`), but this execution worktree ships no
`lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file (`find . -iname "lefthook*" -not -path
"./.git/*"` in this worktree turns up only prior evidence docs referencing lefthook, never a config file), so
lefthook has no pre-commit hooks to execute. The process exit code of `0` reflects lefthook's own "no config
found" no-op, not a successful pre-commit run — recorded here as an `operator_required` gate failure per this
acceptance criterion's instruction to record missing config/tool as such.

## TestObjectlogValidationTraceability

- Dependency: `pqueue-4157c36f` (recorded above and in bead metadata `parent`/description).
- Governing references: `TD-004 S3 Object-Log + SQLite Projection Mode` (lines 570, 730, recorded above) and
  `ADR-003 Rust Workspace and Toolchain Policy` (recorded above).

## TestObjectlogValidationScopeFence

Command: `bash scripts/ci/pr-gate.sh --mode enforcing` — **not executed by this bead.**

Classification: `operator_required`. Per this bead's explicit non-scope ("Do not rerun the enforcing PR gate
probe"), the enforcing PR gate is out of scope here and is recorded as `operator_required` in this child so
that the local-gate validation work above remains independently executable and reviewable without depending on
a PR-probe re-run. The enforcing PR gate probe itself is owned and recorded independently by sibling bead
`pqueue-9f17b246` (`.ddx/executions/20260714T172554-b973bc8a/pr-gate-probe.md`), which found the gate
"available and last known-passing at this code revision" by reference to the unchanged-fingerprint prior run
at `.ddx/executions/20260714T164924-f2b3fc7f/pr-gate-run.log`. That prior finding stands unchanged: `git diff
--stat c17b8048d61d1286e5532dd5bb4437c762f4725e HEAD -- ':!.ddx'` (this bead's base-rev vs. current HEAD)
produces no output — no source files differ.

## Evidence Summary

| Gate | Command | Result | Classification |
|------|---------|--------|-----------------|
| Go gate | `go test ./...` | `FAIL` / exit `1` (no module) | not-applicable |
| Lefthook gate | `lefthook run pre-commit` | no config found / exit `0` | operator_required |
| Enforcing PR gate | `bash scripts/ci/pr-gate.sh --mode enforcing` | not executed (non-scope) | operator_required (recorded independently in `pqueue-9f17b246`) |

Dependency `pqueue-4157c36f` and governing references TD-004 (lines 570, 730) and ADR-003 are recorded above,
satisfying this bead's traceability acceptance criterion independently of the PR probe evidence bundled in
`.ddx/executions/20260714T172554-b973bc8a/`.
