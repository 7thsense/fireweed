# Fireweed legacy bounded formal regression analysis

The current CONTRACT-006/007 and TP-006 require API-007 public-facade correspondence.
These existing models and the paused-apply/ephemeral-CP backend interpreter are
legacy regression controls; their results do not qualify the current contract.
Historical exact-source results are retained in `artifacts/formal/legacy-replay/`.
Write new runtime evidence outside active Helix documents. Run from repository root:

```sh
python3 -m unittest discover -s scripts/formal -p 'test_*.py'
python3 scripts/formal/check.py --output target/formal-analysis.json
python3 scripts/formal/conformance.py --output target/formal-conformance.json
```

`--offline` is available on the conformance runner when dependencies are cached.
Public S3 tests use the repository's RustFS fixture. Set `FIREWEED_RUSTFS_BIN` to
a real installed binary when a mise shim is not active. Reports retain command,
source hashes (including dirty edits), attribution, positive test counts and log
hashes. Zero tests, ignored tests, a failing process or changing source fails the
conformance gate. Source hashes accompany both before/after identities.

The formal gate is dependency-free Python. It exhaustively enumerates reachable
finite states, checks transition/state invariants, and records shortest safety
counterexamples. A fixed command's eventual apply is checked in the no-further-
crashes environmental suffix under weak fairness of the labeled `Apply` action.
SCC candidates become explicit closed walks; each fair label must execute or be
disabled somewhere on the walk. The checker validates that witness. Observe
stuttering makes terminal and deadlocked states explicit. Tests exercise unfair
starvation, intermittent enabling, fair stalled service and deadlocks.

Epoch/history bounds close input, not required repair. Timed models are safety
fixtures only. A search cap returns FAIL, never a qualified or truncated PASS.
Baseline model reports give exact bounds, states, edges, frontiers and assumptions.
All faulty controls must trip their intended oracle. Their traces are part of
JSON evidence; they are deliberately injected model faults, not discovered Rust
bugs. Cross-model litmus tests and real adapter witnesses check selected
interactions without asserting whole-system composition/refinement.

The logical-item-key model has one scope and two requests; a separate namespace
model has two scopes and one request each. Splitting these obligations keeps the
finite product small while retaining independent key-convergence and scope
oracles. Two abstract scopes stand for disjoint tenant/queue namespaces. Different
operation-specific receipt formats remain real-code conformance obligations.

Generic memory-projection and filesystem-publication tests remain separately
attributed. They cannot qualify public S3 × Turso. A local RustFS/Turso test is
public-cell conformance, not a production-cloud or process-kill qualification.
The service-envelope fixture does not discharge the numeric PRD progress bound.

## Generated model-to-S3/Turso replay

The legacy interpreter uses an independent combined model: 6,386 reachable states and
20,913 edges. The smoke selection contains 91 traces targeting 83 semantic edge
classes plus required compositions. Every selected trace executes against real
local S3 and Turso; both concurrent serial outcomes remain permitted. Reports
record actual matched edges separately from intended coverage.

```sh
python3 scripts/formal/generate_replay.py --output target/replay-corpus.json
python3 scripts/formal/generate_replay.py --validate target/replay-corpus.json
CARGO_INCREMENTAL=0 python3 scripts/formal/with_rustfs.py \
  --binary /absolute/path/to/rustfs --replay-profile smoke \
  --output target/model-replay.json
# Full corpus: 17,002 prefixes / 127,208 steps, intended for all 20,913 edges.
CARGO_INCREMENTAL=0 python3 scripts/formal/with_rustfs.py \
  --binary /absolute/path/to/rustfs --replay-profile edges --workers 8 \
  --output target/model-replay-edges.json
```

Use `--offline` with cached Cargo dependencies. The runner retains the canonical
corpus, actual per-step observations, failure prefix, Cargo log and native CAS
preflight. Canonical validation recomputes graph, state IDs, observations,
transitions and selection metadata. A search cap, changed source, stale corpus,
zero/ignored cases or missing evidence fails closed. Execution covers one process
with an ephemeral CP kernel, graceful reopen and simulated client nonreceipt;
it does not qualify cloud S3 or process-kill recovery.

`--workers` accepts 1–16 (default1); worlds have independent S3/Turso/CP state.
The `*-progress.jsonl` file records each completed trace and elapsed milliseconds,
with immediate flush; it is progress rather than final qualification. On failure
no new traces start and already-running traces drain. The final report reconciles
all corpus identities and action counts, and records intended/matched/unwitnessed
edge coverage. Eight-world smoke is qualified before deeper execution. Compact
reports retain all observations; evidence may be gzip-compressed with original
and compressed SHA-256 hashes. Concurrent prefixes permit either order, so a full
corpus pass alone cannot witness both mutually exclusive targeted edges.

For complete **witnessed** bounded graph coverage, use `--replay-profile
schedule-smoke --workers 8` before `--replay-profile exhaustive --workers 8` with
`with_rustfs.py`. Exhaustive selects 20,920 cases / 153,819 steps from the complete
independent model, adding 3,918 reverse polling cases to canonical forward paths.
Backend polling telemetry must prove overlapping calls, observed paths must realize
the requested schedules, and actual per-step edge witnesses must set-equal all
20,913 graph edges. Missing edges or forged aggregate coverage fail closed. Full
oracle alternatives remain intact; schedule prediction only defines the stimulus
coverage obligation. This is bounded backend graph coverage, not universal refinement.


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
