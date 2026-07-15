# Branch Inheritance Creation — Workspace Quality Gate

- Bead: `pqueue-5e025a05` (child 3 of 3 of `pqueue-92a2e386`)
- Base revision: `08c55b4f6afb20f1d0530d265318e3a356a756b5` (this bead's `base-rev`, `HEAD` of this worktree)
- This bead makes no source changes — it only runs and records the AC1/AC2 gate evidence for the branch
  inheritance work already implemented and tested by sibling children `pqueue-f2b2e9e3` (tests:
  `branch_inheritance_uses_retained_floor_metadata`, `branch_inheritance_seed_floor_edge`) and
  `pqueue-151257a3` (test: `branch_inheritance_source_pins_preserved`, all three run under
  `TestBranchInheritanceCreationRustGate`).

## TestBranchInheritanceCreationWorkspaceQualityGate

### `cargo +1.92.0 fmt --all --check`

Command actually run (the `cargo` on `PATH` is a Homebrew stable build that does not understand `+1.92.0`
toolchain directives, so `rustup run 1.92.0 cargo ...` is the equivalent invocation — confirmed
`rustc 1.92.0` is an installed rustup toolchain):

```text
$ rustup run 1.92.0 cargo fmt --all --check
```

Exit status: `0`. No output (clean).

### `cargo +1.92.0 clippy --workspace --all-targets -- -D warnings`

```text
$ rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings
```

Exit status: `0`. Compiled every workspace crate (`pqueue-core`, `pqueue-engine`, `pqueue-objectlog`,
`pqueue-sqlite`, `pqueue-conformance`, `pqueue-projection`, `pqueue-postgres`, `pqueue-memory`, `pqueue-resp`,
`pqueue`, `pqueue-server`, `pqueue-release`, `pqueue-loadgen`, `pqueue-bench` is a separate self-contained
workspace and not included here). `Finished dev profile ... in 41.13s` with zero warnings/errors.

### `go test ./...`

```text
$ go test ./...
# ./...
pattern ./...: directory prefix . does not contain main module or its selected dependencies
FAIL	./... [setup failed]
FAIL
```

Exit status: `1`. Classification: **not-applicable**. `find . -iname go.mod -not -path "./.git/*"` and
`find . -iname "*.go" -not -path "./.git/*"` both return nothing anywhere in the repository — there is no Go
module or package for `go test` to run against. `go` itself is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/go`), so this is an absence-of-module result, not a missing-tool result, and
is consistent with every prior sibling-bead evidence record in this queue (e.g.
`.ddx/executions/20260714T154840-c9835781/pr-gate-and-go-verification.md`).

### `lefthook run pre-commit`

```text
$ lefthook run pre-commit
│  No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-5e025a05-20260714T164924-f2b3fc7f"
```

Exit status: `0`. Classification: **operator_required**. The `lefthook` binary is installed and runnable
(`/home/linuxbrew/.linuxbrew/bin/lefthook`), but this worktree ships no `lefthook.yml` / `.lefthook.yml` /
`.config/lefthook` config file (`find . -iname "lefthook*" -not -path "./.git/*"` turns up no config, only
prior evidence docs referencing lefthook). The exit code `0` is lefthook's own "no config found" no-op, not a
passing pre-commit run, so this is recorded as an operator-required gate failure per the AC's own instruction,
consistent with every prior sibling evidence record.

### `scripts/ci/pr-gate.sh --mode enforcing`

`scripts/ci/pr-gate.sh` is present and executable. It was run to completion (not skipped/deferred), because
the source fingerprint changed since the last recorded pass (`5b33c75c`): sibling children `pqueue-f2b2e9e3`
and `pqueue-151257a3` added `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs` (+541 lines,
confirmed via `git diff --stat 5b33c75c 08c55b4f -- ':!.ddx'`), so the "unchanged fingerprint, do not re-run"
exemption used by earlier siblings does not apply here.

Full output captured at `.ddx/executions/20260714T164924-f2b3fc7f/pr-gate-run.log` (3984 lines).

**First attempt** (superseded — see below): the very first run of this command in this bead executed
concurrently with this bead's own direct `cargo test -p pqueue-objectlog / -p pqueue-sqlite / -p pqueue-engine
/ -p pqueue-conformance` invocations, which this worktree's own `crates/pqueue-bench/Cargo.toml` explicitly
warns against ("pqueue-bench is a SELF-CONTAINED workspace, deliberately separate from the root workspace: ...
at-scale throughput harness ... must run in ISOLATION — not interleaved with the root `cargo test --workspace`
run"). That run failed at `nightly-gate.sh` → `release-gate.sh`'s `performance_cross_queue_scale_out_tests`
smoke bar (`8 owners = 2.19x the 2-owner aggregate, below the 2.40x bar`), `EXIT_STATUS=101`. Re-running just
that one test in isolation (nothing else running) immediately afterward produced `8 owners = 3.16x` — well
above the `2.40x` bar — confirming the failure was CPU contention from concurrent test execution on this
shared machine (other concurrently-running bead-execution agents were also observed via `ps aux` at the time),
not a regression in anything this bead's scope touches.

**Second attempt** (the recorded result): re-run with no other `cargo`/test commands executing concurrently in
this worktree.

```text
=== pr-gate [mode=enforcing] ===
--- fmt ---
--- ledger validator tests ---
   ...
--- coverage threshold parser fixtures ---
pqueue-core: lines 92.00% (92/100)
pqueue-core: branches 86.00% (86/100)
pqueue-engine: lines 81.00% (81/100)
--- product workflow suite names ---
product workflow suite names verified
--- nightly gate (wraps release gate) ---
=== pqueue release gate (SMOKE lane) ===
    ...
--- fmt ---
--- clippy ---
   ...
--- workspace tests (emits product_validation + E3 smoke rows) ---
   ...
--- bench evidence suites (separate workspace; emits E2 smoke rows) ---
   ...
--- tier-aware ledger verification (smoke lane: E2,E3 present + well-formed) ---
   ...
--- live coverage gate ---
   ...
pqueue-engine: lines 84.94% (6507/7661)
--- build-closure integrity ---
pqueue-131eadfa: live closure verified
=== release gate (SMOKE lane) PASSED ===
    Smoke evidence E2,E3 present + well-formed; coverage bars met.
    RELEASE-tier E0-E3 remains DEFERRED to pqueue-d3371502 (E0/E1),
    pqueue-f1d107de (E2), pqueue-2f9ebac3 (E3) — NOT claimed green here.
nightly gate passed
=== pr-gate [enforcing] PASSED ===
EXIT_STATUS=0
```

`grep -n "error\[\|warning:\|FAILED\|^error"` over the full log returns only `warning: --branch option is
unstable` (a `cargo llvm-cov` informational notice, not a build/test failure). `grep -n "^test result:" | grep
-v "0 failed"` returns no output — every `test result:` line across the entire enforcing run (workspace tests,
bench E2/E2-density suites, ledger tests) reports `0 failed`.

**Classification: PASSED** at this exact code revision, on the second (isolated) run. This bead's own gate
evidence recommends future gate runs in this shared execution environment avoid running other `cargo
test`/`cargo clippy` invocations concurrently with `pr-gate.sh`, since `pqueue-bench`'s throughput-scaling
assertions are, by the crate's own documented design, sensitive to core contention from concurrently-scheduled
processes (including sibling bead-execution agents sharing this machine).

## Evidence Summary

| Gate | Command | Result |
|---|---|---|
| fmt | `rustup run 1.92.0 cargo fmt --all --check` | PASS (exit 0, no diff) |
| clippy | `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings` | PASS (exit 0, 0 warnings) |
| Go | `go test ./...` | `not-applicable` (no Go module/packages in this repository) |
| lefthook | `lefthook run pre-commit` | `operator_required` (no lefthook config in this worktree; tool itself present) |
| PR gate | `bash scripts/ci/pr-gate.sh --mode enforcing` | PASS (exit 0, `=== pr-gate [enforcing] PASSED ===`); first attempt's failure was a same-machine CPU-contention artifact from this bead's own concurrent test runs, not a code regression — confirmed by isolated re-run of the specific failing assertion and by a clean, isolated re-run of the entire gate |
