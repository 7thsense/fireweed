# TD-008 Terminal Reap Frontier Evidence

## Verified

- `cargo test -p pqueue-projection reap_waits_for_emission -- --nocapture`
- `cargo test -p pqueue-projection reap_ignores_emission_when_disabled -- --nocapture`
- `PQUEUE_LEDGER_DIR=docs/perf/evidence cargo test -p pqueue-release --test td008_evidence td008_evidence_bundle_recorded -- --nocapture`
- `go test ./... -run 'TestTerminalReapWaitsForEmissionCursor|TestTerminalReapAllowsOptOutAfterRetentionOnly|TestTD008ConformanceSuiteGreen|TestTD008EvidenceBundleRecorded'`

## Evidence

- Rust frontier coverage: `crates/pqueue-projection/src/lib.rs`
- Go wrappers for the bead-named acceptance tests: `go_root_test.go`
- Evidence bundle ledger: `docs/perf/evidence/td008-terminal-reap-frontier.jsonl`
- Evidence-row writer and verifier: `crates/pqueue-release/tests/td008_evidence.rs`

## Notes

- The opted-in path still blocks terminal reap until the emission cursor passes the terminal
  `CommandPosition`.
- The opted-out path remains retention-only.
