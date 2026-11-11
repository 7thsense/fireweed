# Fireweed legacy bounded formal regression analysis

The current CONTRACT-006/007 and TP-006 require API-007 public-facade correspondence.
These existing models and the paused-apply/ephemeral-CP backend interpreter are
legacy regression controls; their results do not qualify the current contract.
Historical exact-source results are retained in `artifacts/formal/legacy-replay/`.
Write new runtime evidence outside active Helix documents. Run from repository root:

```sh
python3 -m unittest discover -s scripts/formal -p 'test_*.py'
# large tier: tests over 10 s live in large_test_*.py (run by the `large` job in the nightly workflow, not on pull requests)
python3 -m unittest discover -s scripts/formal -p 'large_test_*.py'
python3 scripts/formal/check.py --output target/formal-analysis.json
```

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

## Lease-owner queue path

`owner_models.py` bounds the native path where writes publish to the immutable
log and reads serve from the projection of one lease owner. Two handles share a
control-plane lease; a handle's wall clock, which stamps its lease requests, runs
ahead of real time by a tick; the lease clock is real time. The owner measures its
deadline from before the request was sent, waits the skew allowance after
acquiring, and exits on any failed publish, passed deadline, rejected renewal or
crash. Four scenarios keep each graph complete: `base`, `crash`, `renew` and
`lost` (a renewal whose reply is lost). Its oracles require that a read is not
acknowledged after a write it misses (O-READ), a superseded fence cannot commit
(O-FENCE), nothing is planned over an unresolved publish (O-UNRESOLVED), an
untrusted projection never serves (O-TRUST) and no identity is published twice
(O-UNIQUE). The faulty controls include `serve_past_deadline`,
`deadline_from_after_send`, `new_owner_skips_skew`,
`keep_projection_after_publish_failure` and `ack_without_recheck`. `check.py` runs
the models and controls. `python3 scripts/formal/check_owner.py` adds the completed
witnesses and writes `target/api007-formal/owner.json`; `test_owner.py` exercises
that runner. Its production mapping is documentary and unexecuted.
