---
ddx:
  id: tp-formal-analysis
  type: test-plan
  depends_on: [contract-formal-queue-semantics, td-acquire-fence-fault-simulation]
  status: accepted
---

# TP-006: Bounded formal analysis and production conformance

Run `python3 scripts/formal/check.py --output /tmp/fireweed-formal-evidence.json`
and `python3 -m unittest discover -s scripts/formal -p 'test_*.py'`.
The gate must explore each declared reachable domain completely and reject
truncation. Negative controls must produce replayable invariant counterexamples
or fairness-qualified nonprogress cycles. Python is an executable finite-state
specification; it does not constitute a TLA+ proof or Rust refinement proof.

| Contract | Model family | Production obligation |
|---|---|---|
| F-ACK/PREFIX/FENCE/EVENTUAL | recovery | objectlog publication/reopen fault simulation; async commit and apply conformance |
| F-LEASE/REPLAY | lifecycle | current lease-token guards and retained receipt replay, including expiry/renewal |
| F-BATCH | batching | planned selections and per-request outcome conservation through packed append/apply |
| F-BUDGET | admission | dispatcher ownership, caller cancellation, charged retained bytes and rejection cleanup |
| F-DEADLINE | timed progress | eligible-age preservation and finite service-envelope fixture; TP-002 workload evidence remains independent |

## Required mapping and evidence

The implementation evidence supplements this table with exact source seams,
executed test names/commands, model configuration, state/edge counts, model
hashes, failures/fixes and explicit exclusions. Existing coverage counts only
when executed against this task's source identity. Non-durable memory whitebox
checks do not qualify the S3/Turso public cell. Model mutations validate the
oracles only; they are not production defects unless reproduced in real code.

Do not delete projection files for lifecycle tests: verify/delete/rebuild uses
`Fireweed::projection_control()`. Physical process loss is a separate recovery
suite. Historical unsupported backend defects are seeds, not current gates.

## Generated implementation trace replay (CONTRACT-007)

`generate_replay.py` independently explores the complete combined graph (6,386
states / 20,913 edges). Its checked-in canonical smoke corpus has 91 traces and
83 intended semantic edges, including both handoff/reopen orders, lost-response
replay, sequential/concurrent claims under apply debt, and concurrent current /
stale credentials. `--profile edges` emits prefixes intended for every graph edge;
actual implementation edge coverage remains separately measured.

Execute `scripts/formal/with_rustfs.py --binary /absolute/path/to/rustfs
--replay-profile smoke --output target/model-replay.json` (optional `--offline`).
All selected traces must pass on real S3 × Turso, with zero ignored cases. Retain
canonical corpus, source/corpus/log hashes, native CAS preflight, each step's
result/normalized state/physical diagnostics, attempted and passed case counts,
and intended versus matched edge coverage. Observers must preserve exact modeled
applied-prefix cursor, active Assigned CP lease, history/receipt multiplicity,
worker/token/expiry/identity/version, and side-effect-free projection watermarks.
Receipts contain worker/token but no lease expiry; expiry is checked on returned
items, authoritative Claim commands and applied item rows.

Python controls reject corrupt schema/source/observations/transitions/selection,
empty/truncated generation, omitted cases and forged counts; canonical regeneration
is deterministic. Rust controls reject concrete token, worker, expiry, request,
fingerprint and history corruption, and a legal alternate concurrency continuation
that cannot follow the observed first branch. Claim-tail guard tests reject gaps,
truncation, expired leases, duplicate items, epoch regression, non-authority-first
or mixed command tails, foreign queues and bounds overflow. The Turso guard must
reject uncovered/foreign minimum cursors before SELECT and preserve Pending rows.
Engine regressions cover equal-epoch ephemeral restart, different-owner handoff,
same-session reaffirmation and expired PendingFence reacquisition in both APIs.

The CI correctness lane runs the smoke replay with pinned RustFS and uploads all
artifacts even on failure. Deeper replay is an explicit command, not a claimed
completed qualification. These witnesses do not discharge universal Rust
refinement, multi-process control-plane safety, process kill, production cloud
S3 or the numeric FR-12 deadline.
