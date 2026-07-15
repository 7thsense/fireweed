# Release Matrix Evidence

Bead: `pqueue-00311bb9`

Toolchain:

- `rustup toolchain list` showed `1.92.0-x86_64-unknown-linux-gnu (active)`.
- The matrix commands were run with `rustup run 1.92.0 cargo ...` because plain `cargo +1.92.0` was not accepted by this environment.

Rust matrix:

- `cargo test -p pqueue-objectlog -- --nocapture` passed.
- `cargo test -p pqueue-sqlite -- --nocapture` passed.
- `cargo test -p pqueue-engine -- --nocapture` passed.
- `cargo test -p pqueue-conformance -- --nocapture` passed.
- `cargo test --workspace` passed.

Evidence:

- Objectlog log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-objectlog.log`
- SQLite log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-sqlite.log`
- Engine log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-engine.log`
- Conformance log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-conformance.log`
- Workspace log: `.ddx/executions/20260714T104257-87125772/logs/workspace.log`

Notable release-scope confirmation:

- The pqueue-c33c367e interaction does not change the release envelope for this bead. The design note in `docs/perf/design/manifest-compaction-hotpath.md` says the permanent head CAS remains the stale-writer fence and that pqueue-c33c367e does not widen the rollout safety envelope. The release note in `docs/releases/v0.14.0.md` records the same conclusion.

Non-Rust gates:

- Go gate: not applicable. Repository search found no `go.mod`, `go.work`, or `*.go` files, so there is no Go module/package tree to test.
- Lefthook gate: operator-required failure. `lefthook run pre-commit` reported: `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "/home/erik/.cache/ddx/exec-wt/.execute-bead-wt-pqueue-00311bb9-20260714T104257-87125772"`.

Log for the lefthook gate:

- `.ddx/executions/20260714T104257-87125772/logs/lefthook-pre-commit.log`

Summary:

- Required Rust release matrix: passed.
- Go gate: not applicable.
- Lefthook gate: operator-required failure due to missing config.
