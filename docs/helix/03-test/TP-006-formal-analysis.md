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
