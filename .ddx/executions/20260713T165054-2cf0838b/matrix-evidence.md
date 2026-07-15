# pqueue-80bb468c matrix evidence

Bead: `pqueue-80bb468c`
Toolchain: `rustup run 1.92.0`

## Rust gates

| Gate | Command | Result | Log |
| --- | --- | --- | --- |
| objectlog | `rustup run 1.92.0 cargo test -p pqueue-objectlog -- --nocapture` | pass | [`logs/pqueue-objectlog.log`](logs/pqueue-objectlog.log) |
| sqlite | `rustup run 1.92.0 cargo test -p pqueue-sqlite -- --nocapture` | pass | [`logs/pqueue-sqlite.log`](logs/pqueue-sqlite.log) |
| engine | `rustup run 1.92.0 cargo test -p pqueue-engine -- --nocapture` | pass | [`logs/pqueue-engine.log`](logs/pqueue-engine.log) |
| conformance | `rustup run 1.92.0 cargo test -p pqueue-conformance -- --nocapture` | pass | [`logs/pqueue-conformance.log`](logs/pqueue-conformance.log) |
| workspace | `rustup run 1.92.0 cargo test --workspace` | pass | [`logs/workspace.log`](logs/workspace.log) |

## Non-Rust gates

| Gate | Command | Result | Evidence |
| --- | --- | --- | --- |
| Go matrix | `go test ./...` | not applicable | Repo has no `go.mod` or Go packages. The command exits with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`. See [`logs/go-test.log`](logs/go-test.log). |
| Lefthook pre-commit | `lefthook run pre-commit` | operator_required | `lefthook` is installed, but no config files are present in this tree. The command reports: `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in "...".` See [`logs/lefthook-pre-commit.log`](logs/lefthook-pre-commit.log). |
| PR gate probe | `bash scripts/ci/pr-gate.sh --mode enforcing` | operator_required | The local run fails in the fmt stage before completion. Last output: `error: no such command: \`+1.92.0\`` and `help: invoke \`cargo\` through \`rustup\` to handle \`+toolchain\` directives`. See [`logs/pr-gate-enforcing.log`](logs/pr-gate-enforcing.log). |

## Summary

All requested Rust package gates and the full workspace gate passed under the pinned 1.92.0 toolchain.
The Go gate is not applicable in this repository.
The pre-commit and PR gate probes surfaced operator-required follow-up rather than repo failures that can be repaired inside this bead.
