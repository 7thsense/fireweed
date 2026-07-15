# Objectlog Enforcing PR Gate Probe Evidence

- Bead: `pqueue-9f17b246` — "objectlog: capture enforcing pr-gate probe evidence" (child of `pqueue-d4699907`)
- Dependency: `pqueue-4157c36f` — "objectlog: integrate head-based compaction with branch inheritance, restart
  replay, and release hardening" (open)
- Governing refs:
  - **TD-004 S3 Object-Log + SQLite Projection Mode**
    (`docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:188`) — defines the
    manifest-commit conditional-write closure condition: "A manifest entry naming the segment... MUST be
    appended via a conditional write that succeeds only if (a) the manifest's tail still equals the writer's
    expected tail AND (b) the writer's `assignment_epoch` is the **current** epoch for the queue... A failed
    CAS MUST abort the commit, roll back the in-flight reservation, and the writer MUST treat itself as raced
    or fenced."
  - `docs/helix/02-design/technical-designs/TD-004-s3-object-log-sqlite-projection-mode.md:218` — defines the
    object-store capability closure condition: "The object store MUST provide a conditional (compare-and-set)
    write usable for the manifest object... The accepted primitive(s) MUST be documented per supported store."
  - **ADR-003 Rust Workspace and Toolchain Policy**
    (`docs/helix/02-design/adr/ADR-003-rust-workspace-and-toolchain-policy.md`) — governs the toolchain
    (`rustup run 1.92.0 ...`) and release-gate expectations that `pr-gate.sh --mode enforcing` enforces.
- Base revision: `470df3e5ab731db28d695aca4207f51ddfb4647d` (this bead's `base-rev`, current `HEAD` at probe
  time).

## TestObjectlogPrGateProbeExecution

Command: `bash scripts/ci/pr-gate.sh --mode enforcing`

This bead's base revision (`470df3e5`) has **no source-tree changes** relative to `08c55b4f` (the commit that
was `HEAD` when sibling bead `pqueue-5e025a05` last ran this exact enforcing gate to completion), outside
`.ddx/` bookkeeping:

```text
$ git log --oneline 08c55b4f..470df3e5
470df3e5 chore: update tracker (execute-bead 20260714T164924-f2b3fc7f)
83b952af docs(objectlog): record branch inheritance quality & review gate evidence [pqueue-5e025a05]

$ git diff --stat 08c55b4f 470df3e5 -- ':!.ddx'
(no output — no source files differ)
```

The only intervening commit (`83b952af`) is a `docs:` evidence-recording commit; it touches no
`scripts/ci/pr-gate.sh`, `Cargo.toml`/`Cargo.lock`, or crate source under `crates/`. The command fingerprint
(script contents + target source tree) is therefore unchanged since it last ran to completion, at:

- Evidence: `.ddx/executions/20260714T164924-f2b3fc7f/pr-gate-run.log`
  (full captured output, 3984 lines), `.ddx/executions/20260714T164924-f2b3fc7f/review-gate.md`,
  `.ddx/executions/20260714T164924-f2b3fc7f/workspace-quality-gate.md`
- Recorded outcome, from the tail of that log:

```text
=== release gate (SMOKE lane) PASSED ===
    Smoke evidence E2,E3 present + well-formed; coverage bars met.
    RELEASE-tier E0-E3 remains DEFERRED to pqueue-d3371502 (E0/E1),
    pqueue-f1d107de (E2), pqueue-2f9ebac3 (E3) — NOT claimed green here.
nightly gate passed
=== pr-gate [enforcing] PASSED ===
EXIT_STATUS=0
```

Per the long-running-command guidance (do not re-run an expensive gate — `bash scripts/ci/pr-gate.sh --mode
enforcing` runs `cargo fmt --check`, `cargo test -p pqueue-release`, coverage threshold checks, the product
workflow suite name check, and `nightly-gate.sh` which itself runs the full `release-gate.sh`) unless the
command fingerprint changed, this bead does not re-execute the full enforcing gate against unchanged code. It
records the probe result by reference to the still-valid prior run at this bead's exact `base-rev`.

Classification: **available and last known-passing at this code revision** (not `operator_required` — the
script is present at `scripts/ci/pr-gate.sh`, is runnable, and its most recent execution against this exact
source tree — commit `08c55b4f`, unchanged through `470df3e5` — completed with `exit_status=0`).

## TestObjectlogPrGateProbeTraceability

- Dependency: `pqueue-4157c36f` (recorded above and in bead metadata `parent`/description).
- Governing references: `TD-004 S3 Object-Log + SQLite Projection Mode` (lines 188, 218, quoted above) and
  `ADR-003 Rust Workspace and Toolchain Policy` (recorded above).

## TestObjectlogPrGateProbeGoGate

This child bead is responsible for the Go gate (no sibling has claimed it in this bead's lineage).

Command: `go test ./...`

Output:

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
(`/home/linuxbrew/.linuxbrew/bin/go`); the failure is solely the absence of a module, not tool unavailability.

## TestObjectlogPrGateProbeLefthookGate

This child bead is responsible for the lefthook gate (no sibling has claimed it in this bead's lineage).

Command: `lefthook run pre-commit`

Output:

```text
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-9f17b246-20260714T172554-b973bc8a"
```

Exit status: `0`

Classification: `operator_required`. The `lefthook` binary is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this execution worktree ships no `lefthook.yml`/
`.lefthook.yml`/`.config/lefthook` config file (`find . -iname "lefthook*" -not -path "./.git/*"` in this
worktree turns up only prior evidence docs referencing lefthook, never a config file), so lefthook has no
pre-commit hooks to execute. The process exit code of `0` reflects lefthook's own "no config found" no-op, not
a successful pre-commit run — recorded here as an `operator_required` gate failure per this acceptance
criterion's instruction to record missing config/tool as such.

## Evidence Summary

| Gate | Command | Result | Classification |
|------|---------|--------|-----------------|
| Enforcing PR gate | `bash scripts/ci/pr-gate.sh --mode enforcing` | not re-run; unchanged fingerprint since last passing run (`exit_status=0`) at `.ddx/executions/20260714T164924-f2b3fc7f/pr-gate-run.log` | available / last known-passing at this revision |
| Go gate | `go test ./...` | `FAIL` / exit `1` (no module) | not-applicable |
| Lefthook gate | `lefthook run pre-commit` | no config found / exit `0` | operator_required |

Dependency `pqueue-4157c36f` and governing references TD-004 (lines 188, 218) and ADR-003 are recorded above
alongside all gate outcomes, satisfying this bead's traceability acceptance criterion independently of any
downstream validation work bundled elsewhere.
