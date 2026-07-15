# PR Gate Probe Evidence — Lefthook Gate & References (part 3)

- Bead: `pqueue-14d4efa5` (child 3 of 3 of `pqueue-eb1f90ab`)
- Dependency: `pqueue-4157c36f`
- Governing refs: `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`, `docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`

## TestObjectlogPrGateProbeLefthookGate

Command: `lefthook run pre-commit`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-14d4efa5-20260714T153959-f674b3c9"
```

Exit status: `0`

Classification: `operator_required`

Reason: the `lefthook` binary is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this
worktree does not ship a `lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file, so lefthook has no
pre-commit hooks to execute. A repo-root check (`find . -iname "lefthook*"`) confirms no lefthook config file
exists anywhere in this worktree. Per this acceptance criterion, the missing config is recorded as an
`operator_required` gate failure rather than a passing gate — the process exit code being `0` reflects
lefthook's own "no config found" no-op, not a successful pre-commit run.

## TestObjectlogPrGateProbeEvidenceReferences

This evidence file, together with sibling child evidence for `pqueue-eb1f90ab`, records:

- Dependency: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening" (open; its own `TestWorkspaceQualityGate` AC requires the same
  `lefthook run pre-commit` gate, or an `operator-required` classification when config/tool is missing, be
  recorded — consistent with the classification above).
- Governing reference: **TD-004 S3 Object-Log + SQLite Projection Mode**
  (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`) — defines the
  manifest commit as the CAS/fencing enforcement point (line 188), requires documented conditional-write
  primitives (line 218), defines the deletion precondition (line 570), and limits provider-specific live S3
  hardening to deployment certification (line 730).
- Governing reference: **ADR-003 Rust Workspace and Toolchain Policy**
  (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`) — governs the toolchain/workspace
  gate commands (`cargo fmt`, `cargo clippy`, `lefthook run pre-commit`) that this probe evidence and the
  release quality gate rely on.

## Evidence Summary

- `lefthook run pre-commit` was attempted from the workspace root and classified as `operator_required`
  (missing config file; tool itself is present).
- Dependency `pqueue-4157c36f` and governing references TD-004 and ADR-003 are recorded above alongside this
  bead's PR gate evidence, satisfying `TestObjectlogPrGateProbeEvidenceReferences`.
- See sibling evidence for the remaining parent ACs: `.ddx/executions/20260714T151050-3bc9d74b/pr-gate-probe.md`
  (child 1, `TestObjectlogPrGateProbeCommand`) and
  `.ddx/executions/20260714T153518-a51a4eed/pr-gate-probe-fallback.md` (child 2,
  `TestObjectlogPrGateProbeOperatorFallback` and `TestObjectlogPrGateProbeGoGate`).
