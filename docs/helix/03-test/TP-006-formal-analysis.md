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

### Full all-edge run

Run `with_rustfs.py --replay-profile edges --workers 8`
on an isolated committed-source checkout. Require 17,002/17,002 completed traces,
127,208 successful action observations, all Rust controls passing, zero ignored,
strict canonical/progress/case/action reconciliation and matching before/after
source hashes. Progress is a flushed JSONL event stream; final compressed evidence
retains exact byte hashes for corpus, observations, report, progress, Cargo log
and CAS preflight. Intended coverage is20,913 edges; actual concurrent branches
are independently observed and unwitnessed edges are reported explicitly.
Eight-world execution must first pass the smoke corpus. A timeout under load is
retained as a failed qualified run, never converted to a permitted model result.

The fixture-prefix regression requires `generation_scan_prefix` of the actual
`fwlog/fwmeta` pair to equal the single world's namespace parent. Retain the
incomplete pre-fix progress and source-bound diagnostic as fixture-scaling
evidence, not a passing qualification or product safety counterexample. After
correction rerun smoke and the entire full corpus on the corrected source.

### Scheduled complete witnessed-edge qualification

First run `with_rustfs.py --replay-profile schedule-smoke --workers 8`, then run
`--replay-profile exhaustive --workers 8` on the same frozen source. Require all
20,920 cases and 153,819 steps, six Rust controls, zero ignored, both real backend
calls Pending before either completion, and actual per-case schedule-path equality.
Independently reconstruct the complete graph and require exact witnessed-edge set
equality for all 20,913 edges. All 3,918 previously unwitnessed edges must have
physical observations: 26 concurrent transitions and 3,892 downstream transitions.

Negative controls reject sequential/immediate-ready concurrency impostors,
wrong polling order, duplicate completion events, malformed/missing schedules,
dropped reverse cases, fabricated aggregate coverage/exhaustiveness, unrealized
schedule paths and incompatible branch switching. Retain source hashes, all
scheduled identities, actual polling telemetry, physical frontiers, native CAS
preflight and exact-byte artifacts. Astra Ultra independently reviews selection
and audits final source, transition paths and graph set equality. The model,
normalization, compatible-state oracle and 20-second deadline stay unchanged.


The isolated RustFS wrapper accepts `--fixture-parent /absolute/disk/path`.
Complete replay must choose a filesystem with sufficient inode and byte headroom;
free bytes alone do not establish capacity. Before launching, require conservative
operational headroom of 64 inodes and 64KiB per selected case plus 8,192 inodes and
128MiB reserve. This is a fixture capacity guard, not a formal footprint theorem.
Filesystems reporting no fixed inode total are not rejected merely for reporting
zero available inodes. Record device identity, parent, unique owned root, sibling
`s3/` and `worlds/` directories, child TMPDIR, and filesystem statistics before,
after execution and after confirmed cleanup. Every world's S3 objects remain
available until final qualification and owned fixture cleanup. The wrapper must
preserve the oracle, model and per-trace deadline when changing fixture placement.

For example, run the schedule-smoke and exhaustive commands with
`--fixture-parent /mnt/scratch/Projects/fireweed/target/formal-fixtures` on this host.
The /tmp tmpfs has a one-million-inode ceiling; accumulating independent worlds can
exhaust that limit despite many free gigabytes. A resource-failed run with missing
detailed observations is unqualified and contributes no final edge witnesses.
