# Objectlog Release Evidence Index

- Bead: `pqueue-138f60ed` (child 3 of 3 of `pqueue-82bb2905`)
- Dependency: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening" (open)
- Governing refs:
  - **TD-004 S3 Object-Log + SQLite Projection Mode**
    (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`) — defines the
    manifest commit as the CAS/fencing enforcement point (line 188: "A manifest entry naming the segment... MUST
    be appended via a conditional write that succeeds only if..."), requires documented conditional-write
    primitives (line 218: "The object store MUST provide a conditional (compare-and-set) write usable for the
    manifest object... The accepted primitive(s) MUST be documented per supported store."), defines the deletion
    precondition (line 570: "A segment MAY be deleted only after a committed snapshot fully covers its `sequence`
    range AND `log_recovery_window_ms` has elapsed past that snapshot's `committed_at`"), and limits
    provider-specific live S3 hardening to deployment certification (line 730: "Provider-specific hardening
    against a live cloud S3 endpoint remains a deployment certification activity...").
  - **ADR-003 Rust Workspace and Toolchain Policy**
    (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`) — governs the toolchain/workspace
    gate commands (`cargo fmt`, `cargo clippy`, `cargo test`, `lefthook run pre-commit`) that the PR gate and
    this evidence rely on.
- Base revision: `a1e154b8e3b08f932ab396c163cc04898bb50cc3` (this bead's `base-rev`)

This index consolidates the three local gate outcomes recorded across the sibling children of parent
`pqueue-82bb2905`, tying each to dependency `pqueue-4157c36f` and the governing TD-004/ADR-003 references above.

## TestObjectlogEvidenceIndexReferences

Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are named
in this entry (above) alongside all three gate outcomes below, satisfying this criterion in one consolidated
record.

## TestObjectlogEvidenceIndexGateOutcomes

| Gate | Command | Outcome | Evidence |
| --- | --- | --- | --- |
| PR gate | `bash scripts/ci/pr-gate.sh --mode enforcing` | available and last known-passing at this code revision (not re-run — unchanged fingerprint) | `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`, `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe-result.txt` (exit_status=0, `=== pr-gate [enforcing] PASSED ===`); confirmed unchanged fingerprint via `git diff --stat 5b33c75c a1e154b8 -- ':!.ddx'` (no output) and `git log --oneline 5b33c75c..a1e154b8 -- . ':!.ddx'` (no output) run in this bead |
| Go verification | `go test ./...` | `not-applicable` | run fresh in this bead (see below); no `go.mod` or `.go` files anywhere in the repository |
| Lefthook verification | `lefthook run pre-commit` | `operator_required` | run fresh in this bead (see below); `lefthook` binary present and runnable, but no `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config exists in this worktree |

## TestObjectlogEvidenceIndexGoGate

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
nothing) and there are no `.go` source files (`find . -name "*.go" -not -path "./.git/*"` returns nothing). No
Go module or packages exist for `go test` to run against. `go` itself is installed and runnable; the failure is
solely the absence of a module, not tool unavailability. This matches the outcome independently recorded in
sibling evidence `.ddx/executions/20260714T154428-60b3b183/lefthook-verification.md` and
`.ddx/executions/20260714T154840-c9835781/pr-gate-and-go-verification.md`.

## TestObjectlogEvidenceIndexLefthookGate

Command: `lefthook run pre-commit`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-138f60ed-20260714T155433-bf90ec2e"
```

Exit status: `0`

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this
worktree does not ship a `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file, so lefthook has no
pre-commit hooks to execute. A repo-wide check (`find . -iname "lefthook*" -not -path "./.git/*"`) turns up
only prior evidence docs and logs referencing lefthook, not a config file. Per this acceptance criterion, the
missing config is recorded as an `operator_required` gate failure rather than a passing gate — the process exit
code of `0` reflects lefthook's own "no config found" no-op, not a successful pre-commit run. The exact failing
command is `lefthook run pre-commit`; its full output is the block above. This matches the outcome
independently recorded in sibling evidence `.ddx/executions/20260714T154428-60b3b183/lefthook-verification.md`
and `.ddx/executions/20260714T154840-c9835781/pr-gate-and-go-verification.md`.

## Evidence Summary

- PR gate (`bash scripts/ci/pr-gate.sh --mode enforcing`): not re-run in this bead — the source fingerprint
  (everything outside `.ddx/`) is unchanged since it last ran to completion and passed at
  `.ddx/executions/20260714T151050-3bc9d74b/`; recorded as available and last known-passing at this exact code
  revision.
- Go verification (`go test ./...`): run fresh from the workspace root in this bead; classified
  `not-applicable` (no Go module/packages in this repository).
- Lefthook verification (`lefthook run pre-commit`): run fresh from the workspace root in this bead; classified
  `operator_required` (missing config file; the tool itself is present and runnable).
- Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are named
  above alongside all three gate outcomes, consolidating them into one auditable index entry as required by
  parent bead `pqueue-82bb2905`.
- Sibling children of `pqueue-82bb2905` recorded these same gate outcomes independently:
  `pqueue-be871917` (lefthook pre-commit gate + Go gate) and `pqueue-d748645a` (PR gate context + Go gate +
  lefthook gate). This entry is the consolidated index tying all three together with the dependency and
  governing references in one place.
