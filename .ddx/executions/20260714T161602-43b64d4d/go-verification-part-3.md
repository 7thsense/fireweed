# pqueue-8d8c7ebb Go verification applicability evidence (part 3 of 3)

## Scope

- Bead: `pqueue-8d8c7ebb`
- Parent: `pqueue-5863fc36` (split into three child beads; this is part 3 of 3)
- Dependency preserved: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch
  inheritance, restart replay, and release hardening" (open)
- Governing references preserved:
  - `TD-004 S3 Object-Log + SQLite Projection Mode`
    (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md`), specifically:
    - line 188 — manifest commit as the CAS/fencing enforcement point
    - line 218 — documented conditional-write primitives requirement
    - line 570 — deletion precondition
    - line 730 — provider-specific live S3 hardening limited to deployment certification
  - `ADR-003 Rust Workspace and Toolchain Policy`
    (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`)
- Reviewed commit / worktree base: `f1e47973746194cc4b319ea2468487533f444414` (this bead's `base-rev`, and
  `HEAD` of this worktree at evidence-recording time).

## AC1 — TestObjectlogGoVerificationLefthookGate

Command run from the repository root:

```bash
lefthook run pre-commit
```

Observed output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-8d8c7ebb-20260714T161602-43b64d4d"
```

Exit status: `0`

Discovery commands confirming no lefthook config file exists anywhere under the repository root:

```bash
$ find . -iname "*lefthook*" -not -path "./.git/*" -not -path "./.ddx/*"
# (no output)
```

`lefthook` itself is installed and runnable (`/home/linuxbrew/.linuxbrew/bin/lefthook`, `lefthook version` →
`2.1.10`), so the "no config files found" message is a tool-level no-op, not a successful pre-commit run.

**Classification: `operator_required`.** The tool is present, but this worktree ships no
`lefthook.yml`/`.lefthook.yml`/`.config/lefthook` config file, so `lefthook run pre-commit` cannot execute any
gate hooks. Per the bead's acceptance criterion, the missing config is recorded as an `operator_required` gate
failure rather than treated as a passing run. Consistent with sibling evidence in
`.ddx/executions/20260714T160319-8a816c1e/post-probe-verification-evidence.md`
(`TestObjectlogClosurePostProbeLefthookExecution`).

## AC2 — TestObjectlogGoVerificationEvidenceReferences

This evidence record names, alongside the Go verification applicability finding below:

- Dependency: `pqueue-4157c36f`.
- Governing references: `TD-004 S3 Object-Log + SQLite Projection Mode` (lines 188, 218, 570, 730) and
  `ADR-003 Rust Workspace and Toolchain Policy`.

Supporting Go verification context (for completeness; classified by sibling parts 1 and 2 of this same
decomposition and re-confirmed here):

```bash
$ find . -iname "go.mod" -o -iname "go.sum"
# (no output)
$ find . -iname "*.go" -not -path "./.git/*"
# (no output)
```

No `go.mod`/`go.work` and no `.go` source file exist anywhere under the repository root; this repository is a
Rust workspace (`Cargo.toml`, `crates/`, `rust-toolchain.toml`) governed by ADR-003. Consistent with
`.ddx/executions/20260714T160733-38d67af2/go-verification-part-1.md` (part 1) and
`.ddx/executions/20260714T161131-29a67855/go-verification-part-2.md` (part 2), which classify `go test ./...`
as **not-applicable** (exit status 1, "directory prefix . does not contain main module or its selected
dependencies").

## Summary

- AC1: `lefthook run pre-commit` classified **`operator_required`** — the binary is installed and runnable, but
  no lefthook config file exists in this worktree, so no pre-commit gate hooks can execute; command and output
  recorded above.
- AC2: dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218, 570, 730) and ADR-003
  recorded alongside the Go verification applicability finding (not-applicable, per parts 1 and 2 of this
  decomposition).
- No production protocol code was altered; no provider-specific AWS certification or broader Rust release
  matrix was run, per the parent's non-scope constraints.
