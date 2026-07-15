# Objectlog Closure Prerequisite Evidence: PR Gate Probe State

- Bead: `pqueue-be8d5328` (child of `pqueue-01518ea7`, which is itself a child of `pqueue-a7844773`)
- Dependency: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening" (open)
- Governing refs:
  - **TD-004 S3 Object-Log + SQLite Projection Mode**
    (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`):
    - line 188: manifest commit is the CAS/fencing enforcement point — "A manifest entry naming the segment,
      its `[first_sequence, last_sequence]` range, its checksum, and the writer's `assignment_epoch` MUST be
      appended via a conditional write that succeeds only if (a) the manifest's tail still equals the writer's
      expected tail AND (b) the writer's `assignment_epoch` is the **current** epoch for the queue..."
    - line 218: conditional-write primitive requirement — "The object store MUST provide a conditional
      (compare-and-set) write usable for the manifest object — e.g., `If-Match`/ETag-conditional PUT,
      conditional-on-absence PUT for monotonic manifest objects, or an equivalent guaranteed atomic CAS. The
      accepted primitive(s) MUST be documented per supported store."
    - line 570: deletion precondition — "A segment MAY be deleted only after a committed snapshot fully covers
      its `sequence` range AND `log_recovery_window_ms` has elapsed past that snapshot's `committed_at` (ADR-001
      step 8)."
    - line 730: scope limit on live-provider hardening — "Provider-specific hardening against a live cloud S3
      endpoint remains a deployment certification activity..."
  - **ADR-003 Rust Workspace and Toolchain Policy**
    (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`) — governs the toolchain/workspace
    gate commands (`cargo fmt`, `cargo clippy`, `cargo test`, `lefthook run pre-commit`) that the PR gate and
    this evidence rely on.
- Reviewed commit/state: `cdd09ace7e22877f968f8e385e330bc10c43f218` (this bead's `base-rev`, and current `HEAD`
  of the worktree at evidence-recording time).

## TestObjectlogClosurePrerequisiteEvidenceState / TestObjectlogClosurePrerequisiteEvidenceOutput

Prerequisite: confirm the PR gate probe state (recorded, or `operator_required`) *before* this bead's sibling
`pqueue-77f4adb8` (post-probe lefthook evidence, tracked under parent `pqueue-01518ea7`) draws any conclusion
from post-probe lefthook output.

Command run to establish whether the reviewed commit changed anything relevant to the PR gate script since its
last recorded pass:

```text
$ git diff --stat 5b33c75c cdd09ace -- ':!.ddx'
(no output — no source files differ)
```

`5b33c75c` is the commit at which sibling bead `pqueue-0f2f06e4` recorded a successful
`bash scripts/ci/pr-gate.sh --mode enforcing` run (`.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`,
`.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe-result.txt`; recorded outcome `exit_status=0`, ending
`=== pr-gate [enforcing] PASSED ===`). `git log --oneline 5b33c75c..cdd09ace` shows only intervening
`docs:`/`chore:` evidence-recording and tracker-update commits; none touch `scripts/ci/pr-gate.sh`,
`Cargo.toml`/`Cargo.lock`, or any crate source under `crates/`.

Per the long-running-command guidance (do not re-run an expensive gate whose fingerprint is unchanged —
`bash scripts/ci/pr-gate.sh --mode enforcing` runs `cargo fmt --check`, `cargo test -p pqueue-release`, coverage
threshold checks, the product workflow suite name check, and `nightly-gate.sh`, which itself runs
`release-gate.sh`), this bead does not re-execute the full enforcing gate against unchanged code.

**PR gate probe state: recorded** (not `operator_required` — the script is present at `scripts/ci/pr-gate.sh`
and runnable; its most recent execution against this exact source tree, unchanged through this bead's reviewed
commit `cdd09ace`, succeeded). Evidence chain: `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`,
`.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe-result.txt`, reaffirmed by sibling
`.ddx/executions/20260714T154840-c9835781/pr-gate-and-go-verification.md`.

This satisfies the prerequisite: the PR gate probe state is recorded as **available and last known-passing** at
the reviewed commit, before any post-probe lefthook conclusion is claimed by the sibling closure bead.

## TestObjectlogClosurePrerequisiteLefthookGate

Command: `lefthook run pre-commit` (run from the workspace root, after the prerequisite state above was
established)

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-be8d5328-20260714T155858-1a3ca4b2"
```

Exit status: `0`

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable, but this worktree ships no `lefthook.yml`/
`.lefthook.yml`/`.config/lefthook` config file, so lefthook has no pre-commit hooks to execute
(`find . -iname "lefthook*" -not -path "./.git/*"` turns up only prior evidence docs and logs referencing
lefthook, not a config file). The exit code `0` reflects lefthook's own "no config found" no-op, not a
successful pre-commit run; per this acceptance criterion it is recorded as an `operator_required` gate failure.

## TestObjectlogClosurePrerequisiteGoGate

Command: `go test ./...` (run from the workspace root, after the prerequisite state above was established)

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
Go module or packages exist for `go test` to run against.

## TestObjectlogClosurePrerequisiteDependencyReference

Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are recorded
above alongside the PR gate probe state, the lefthook gate outcome, and the Go gate outcome.

## Evidence Summary

- PR gate probe state: **recorded** (not re-run; unchanged source fingerprint since its last passing run at
  `5b33c75c`, reaffirmed through reviewed commit `cdd09ace`). This prerequisite is satisfied before sibling
  bead `pqueue-77f4adb8` draws any post-probe lefthook conclusion.
- `lefthook run pre-commit`: run from the workspace root at reviewed commit `cdd09ace`; classified
  `operator_required` (missing config file; the tool itself is present and runnable).
- `go test ./...`: run from the workspace root at reviewed commit `cdd09ace`; classified `not-applicable` (no Go
  module/packages in this repository).
- Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are named
  throughout.
