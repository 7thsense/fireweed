# Release Evidence Correction Report

Date: 2026-07-15
Bead: `pqueue-60472d3f`
Bundle: `.ddx/executions/20260715T043214-936c36b0`
Scope: documentation and in-repo execution evidence only

## Authority

This bundle is the authoritative release-closing evidence for the final deleted-manifest release-note
corrections. Earlier reports remain preserved as historical evidence only:

- `.ddx/executions/20260715T034732-6a2773a2/deleted-manifest-behind-image-conformance-gate-evidence.md`:
  exact failed attempt bundle for bead `pqueue-0269a773`, now superseded in place by this correction report.
- `.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md`:
  older tracked historical gate report for bead `pqueue-819b38ed`, preserved for chronology and prior findings.
- `.ddx/executions/20260714T234920-be4f9d8d/deleted-manifest-recovery-evidence.md`:
  earlier sibling evidence bundle retained as supporting history.

The exact failed `pqueue-0269a773` report is now explicitly superseded in place, while the older
`pqueue-819b38ed` report remains preserved under its own identity. Neither historical artifact may be cited as
the current release authority.

## Corrected factual claims

### Deleted-manifest behavior

- Projection-image-behind fail-closed behavior is proven by the named deleted-manifest conformance tests,
  including `TestBehindImageFailClosedWithDeletedManifests` in
  `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs` and
  `TestObjectlogDeletedManifestFailClosedSignal` in
  `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`.
- Physical deletion of legacy `manifest/` plus `manifest_head/` is proven only to reopen conservatively, not to
  fail closed. The direct evidence is
  `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:4037-4060`
  (`TestBehindImageFailClosedWithDeletedManifests`): after both prefixes are deleted, reopen succeeds, the
  read-horizon cache remains present, and `read_retention_floor(...) == None`.

### Chronology / provenance

- `pqueue-8928baec` closed before the later ownership follow-ups landed.
- `pqueue-7bac12ce` landed on 2026-07-14 22:51 EDT (`fbe0ff70`).
- `pqueue-b29435b2` landed on 2026-07-14 23:07 EDT (`b98f652a`).
- The `pqueue-c33c367e` interaction was therefore reevaluated after `pqueue-8928baec` closure, not before it.

## Final audit disposition

### Finding 1: release note overstated `manifest_head/` deletion behavior

- Status: resolved
- Prior defect: `docs/releases/v0.14.0.md` claimed fail-closed reopen after deleting `manifest_head/`.
- Correction: the release note now distinguishes projection-image-behind fail-closed behavior from physical
  namespace deletion and records conservative reopen with `retention_floor=None`.
- Evidence:
  - `docs/releases/v0.14.0.md`
  - `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:4037-4060`

### Finding 2: release note implied later ownership beads were evaluated before `pqueue-8928baec` closed

- Status: resolved
- Prior defect: release prose said the ownership work was evaluated before closing `pqueue-8928baec`.
- Correction: the release note and superseded historical report now state that the interaction was reevaluated
  after `pqueue-7bac12ce` and `pqueue-b29435b2` landed.
- Evidence:
  - `docs/releases/v0.14.0.md`
  - `.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md`
  - `git log` chronology captured during this bead: `fbe0ff70` at 2026-07-14 22:51 EDT and `b98f652a` at
    2026-07-14 23:07 EDT

### Finding 3: earlier failed attempt report remained contradictory

- Status: resolved
- Prior defect: the preserved failed report still carried stronger deleted-head and provenance claims than the
  surviving tests and persisted artifacts proved.
- Correction: a prominent supersession/correction section now marks that report as historical-only and points to
  this bundle as current authority.
- Evidence:
  - `.ddx/executions/20260714T235844-72ceadbe/deleted-manifest-recovery-gate-evidence.md`

### Finding 4: release note cited historical PR-gate provenance without a persisted log

- Status: resolved
- Prior defect: the release note referred to historical enforcing success without a persisted gate log in the
  cited bundle.
- Correction: the release note now cites only the enforcing run persisted in this bundle.
- Evidence:
  - `.ddx/executions/20260715T043214-936c36b0/pr-gate-enforcing.log`
  - `docs/releases/v0.14.0.md`

### Finding 5: release evidence must not claim an independent no-blocker review unless persisted

- Status: resolved
- Prior defect: earlier prose implied a cleaned-up review result without persisting the actual review output.
- Correction: the release note now records only that the final direct Codex audit findings are dispositioned in
  this report. No separate no-blocker verdict is claimed.
- Evidence:
  - `docs/releases/v0.14.0.md`
  - this file

## Verification matrix on corrected exact state

| Command | Result | Notes |
| --- | --- | --- |
| `rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture` | PASS | Completed on this corrected state. |
| `rustup run 1.92.0 cargo test -p pqueue-conformance -- --nocapture` | PASS | Completed on this corrected state, including deleted-manifest evidence tests. |
| `rustup run 1.92.0 cargo fmt --all --check` | PASS | No formatting changes required. |
| `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | PASS | Exit 0, no warnings. |
| `go test ./...` | NOT APPLICABLE | Fails with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`; this repo has no `go.mod`, `go.work`, or `.go` files. |
| `lefthook run pre-commit` | OPERATOR-REQUIRED | Lefthook binary is present, but no supported config file exists in the repo root. |
| `bash scripts/ci/pr-gate.sh --mode enforcing` | PASS | Full persisted output in `pr-gate-enforcing.log`; terminal line is `=== pr-gate [enforcing] PASSED ===`. |

## PR-gate summary

The enforcing gate persisted at `.ddx/executions/20260715T043214-936c36b0/pr-gate-enforcing.log` completed with:

- `=== release gate (SMOKE lane) PASSED ===`
- `pqueue-core: lines 97.92% (1222/1248)`
- `pqueue-core: branches 86.76% (118/136)`
- `pqueue-engine: lines 84.40% (6617/7840)`
- `=== pr-gate [enforcing] PASSED ===`

The same log also records that release-tier E0/E1/E2/E3 performance evidence remains deferred and is not claimed
green by this gate.

## Non-scope confirmation

This bead changed only:

- `docs/releases/v0.14.0.md`
- `.ddx/executions/...` evidence files

No runtime semantics, storage behavior, or user-facing APIs changed. No branch atomicity, orphan GC, source pin,
retention floor, permanent-head CAS, or fail-closed guarantee was relaxed.
