# Objectlog Closure Evidence: Post-Probe Lefthook and Go Verification

- Bead: `pqueue-77f4adb8` (child of `pqueue-01518ea7`)
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
    gate commands (`cargo fmt`, `cargo clippy`, `cargo test`, `lefthook run pre-commit`) that this evidence
    relies on.
- Reviewed commit/state: `744aa6f7f4a0f3b76ee29577b8a26b5a4fbd9709` (this bead's `base-rev`, and `HEAD` of this
  worktree at evidence-recording time).

## TestObjectlogClosurePostProbePrerequisite

Prerequisite: confirm the PR gate probe was recorded or classified `operator_required` *before* running
`lefthook run pre-commit` in this bead.

Sibling bead `pqueue-be8d5328` recorded this prerequisite at
`.ddx/executions/20260714T155858-1a3ca4b2/pr-gate-prerequisite-evidence.md` (commit `fd6a13fcfebd8a621e2ee5b6686318a4cba9f0f7`,
"docs: record objectlog PR gate prerequisite & protocol references [pqueue-be8d5328]"). That evidence
established:

- **PR gate probe state: recorded** (not `operator_required`) — `scripts/ci/pr-gate.sh` is present and runnable;
  its most recent execution succeeded (`.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`,
  `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe-result.txt`, `exit_status=0`, `=== pr-gate [enforcing]
  PASSED ===`), reaffirmed by `.ddx/executions/20260714T154840-c9835781/pr-gate-and-go-verification.md`.
- The source fingerprint is unchanged from that recorded pass through this bead's reviewed commit:

```text
$ git log --oneline fd6a13fc..744aa6f7
744aa6f7 chore: update tracker (execute-bead 20260714T155858-1a3ca4b2)
```

Only a tracker-update commit intervenes between the recorded prerequisite (`fd6a13fc`) and this bead's reviewed
`HEAD` (`744aa6f7`); it touches no `scripts/ci/pr-gate.sh`, `Cargo.toml`/`Cargo.lock`, or crate source. The PR
gate probe prerequisite therefore remains **recorded** at this bead's reviewed commit, confirmed before running
`lefthook run pre-commit` below.

## TestObjectlogClosurePostProbeLefthookExecution

Command: `lefthook run pre-commit` (run from the workspace root at reviewed commit `744aa6f7`, after the
prerequisite above was confirmed)

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-77f4adb8-20260714T160319-8a816c1e"
```

Exit status: `0`

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this
worktree ships no `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file
(`find . -iname "lefthook*" -not -path "./.git/*"` turns up only prior evidence docs/logs referencing lefthook,
not a config file). The exit code `0` reflects lefthook's own "no config found" no-op, not a successful
pre-commit run; per this acceptance criterion it is recorded as an `operator_required` gate failure.

## TestObjectlogClosurePostProbeGoGate

Command: `go test ./...` (run from the workspace root at reviewed commit `744aa6f7`, after the lefthook evidence
above was recorded)

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

## TestObjectlogClosurePostProbeDependencyReference

Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are recorded
above alongside the prerequisite confirmation, the lefthook gate outcome, and the Go gate outcome.

## Evidence Summary

- PR gate probe prerequisite: **confirmed recorded** (sibling bead `pqueue-be8d5328`, commit `fd6a13fc`;
  unchanged source fingerprint through this bead's reviewed commit `744aa6f7`) before running `lefthook run
  pre-commit`.
- `lefthook run pre-commit`: run from the workspace root at reviewed commit `744aa6f7`; classified
  `operator_required` (missing config file; the tool itself is present and runnable).
- `go test ./...`: run from the workspace root at reviewed commit `744aa6f7`; classified `not-applicable` (no Go
  module/packages in this repository).
- Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003 are named
  throughout.
