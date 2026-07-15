# Objectlog PR Gate Context & Go Verification Evidence

- Bead: `pqueue-d748645a` (child of `pqueue-82bb2905`)
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
- Base revision: `85dc7eafa1cc115dc4a8ab9b7227fa455d5af8e7` (this bead's `base-rev`)

## TestObjectlogPrGateContext

Command: `bash scripts/ci/pr-gate.sh --mode enforcing`

This bead's base revision (`85dc7eaf`) has no source changes relative to `5b33c75c` (the commit that recorded
sibling bead `pqueue-0f2f06e4`'s successful run of this exact command) outside `.ddx/` bookkeeping
(`.ddx/beads.jsonl`, `.ddx/attachments/**/events.jsonl`, `.ddx/metrics/attempts.jsonl`) and prior evidence
markdown files:

```text
$ git diff --stat 5b33c75c 85dc7eaf -- ':!.ddx'
(no output — no source files differ)
```

`git log --oneline 5b33c75c..85dc7eaf` shows only intervening `docs:`/`chore:` evidence-recording commits
(`pqueue-be871917`, `pqueue-14d4efa5`, `pqueue-c3a1650c`), none of which touch `scripts/ci/pr-gate.sh`,
`Cargo.toml`/`Cargo.lock`, or any crate source under `crates/`. The command fingerprint (script contents +
target source tree) is therefore unchanged since it last ran to completion at:

- Evidence: `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`,
  `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe-result.txt`
- Recorded outcome: `exit_status=0`, ending `=== pr-gate [enforcing] PASSED ===`

Per the long-running-command guidance (do not re-run an expensive gate — `bash scripts/ci/pr-gate.sh --mode
enforcing` runs `cargo fmt --check`, `cargo test -p pqueue-release`, coverage threshold checks, the product
workflow suite name check, and `nightly-gate.sh` (which itself runs `release-gate.sh`) — unless the fingerprint
changed), this bead does not re-execute the full enforcing gate against unchanged code. It instead records the
PR gate context by reference to the still-valid prior run.

Classification: **available and last known-passing at this code revision** (not `operator_required` — the
script is present at `scripts/ci/pr-gate.sh` and runnable; its most recent execution against this exact source
tree succeeded).

## TestObjectlogGoVerificationGate

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
Go module or packages exist for `go test` to run against. `go` itself is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/go`); the failure is solely the absence of a module, not tool unavailability.

## TestObjectlogPrGoLefthookGate

Command: `lefthook run pre-commit`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-d748645a-20260714T154840-c9835781"
```

Exit status: `0`

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this
worktree does not ship a `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file, so lefthook has no
pre-commit hooks to execute. A repo-wide check (`find . -iname "lefthook*" -not -path "./.git/*"`) turns up
only prior evidence docs referencing lefthook, not a config file. Per this acceptance criterion, the missing
config is recorded as an `operator_required` gate failure rather than a passing gate — the process exit code of
`0` reflects lefthook's own "no config found" no-op, not a successful pre-commit run. The exact failing command
is `lefthook run pre-commit`; its full output is the block above.

## Evidence Summary

- `bash scripts/ci/pr-gate.sh --mode enforcing`: not re-run (unchanged fingerprint since its last passing run in
  sibling evidence `.ddx/executions/20260714T151050-3bc9d74b/`); recorded as available and last known-passing
  at this exact code revision.
- `go test ./...`: run from the workspace root; classified `not-applicable` (no Go module/packages in this
  repository).
- `lefthook run pre-commit`: run from the workspace root; classified `operator_required` (missing config file;
  the tool itself is present and runnable).
- Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are
  recorded above alongside all three gate outcomes.
- See sibling children of `pqueue-82bb2905` for the remaining parent ACs: `pqueue-be871917` (lefthook
  pre-commit gate) and the consolidated evidence index child.
