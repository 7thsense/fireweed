# Fireweed bounded formal analysis

Authority: CONTRACT-006 / TP-006 / build-formal-analysis. Run from repository root:

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
