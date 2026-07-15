# Rust Release Matrix Plan

Objective: collect release-grade evidence for bead `pqueue-00311bb9`.

Commands and outputs:

1. `cargo +1.92.0 test -p pqueue-objectlog -- --nocapture`
   - Log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-objectlog.log`
   - Completion: exit 0

2. `cargo +1.92.0 test -p pqueue-sqlite -- --nocapture`
   - Log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-sqlite.log`
   - Completion: exit 0

3. `cargo +1.92.0 test -p pqueue-engine -- --nocapture`
   - Log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-engine.log`
   - Completion: exit 0

4. `cargo +1.92.0 test -p pqueue-conformance -- --nocapture`
   - Log: `.ddx/executions/20260714T104257-87125772/logs/pqueue-conformance.log`
   - Completion: exit 0

5. `cargo +1.92.0 test --workspace`
   - Log: `.ddx/executions/20260714T104257-87125772/logs/workspace.log`
   - Completion: exit 0

6. `go test ./...`
   - Outcome: not applicable if no Go module/packages exist in the repository.

7. `lefthook run pre-commit`
   - Outcome: pass, or record missing lefthook config/tooling as an operator-required gate failure.

Completion criteria:

- Every Rust command above exits 0.
- Go gate is recorded as pass or not-applicable with evidence.
- Lefthook gate is recorded as pass or operator-required with evidence.
- A summary report is written in this execution bundle before commit.
