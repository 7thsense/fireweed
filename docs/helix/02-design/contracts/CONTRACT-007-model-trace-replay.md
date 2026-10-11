---
ddx:
  id: contract-model-trace-replay
  type: contract
  depends_on: [contract-formal-queue-semantics, architecture, api-queue-claim-commit-contract]
  status: accepted
---

# CONTRACT-007: Model trace replay and bounded correspondence

An independent interpreter predicts API-007 observations from the CONTRACT-006 abstract history. Each observed successor must match a permitted successor from the previous matching state set. Retain every matching branch; do not greedily choose a branch or switch to an unrelated state. Stable bootstrap rank mapping is permitted; deriving expectations from arbitrary SUT results is not.

## Common model and backend refinements

The common model includes insert, natively filtered priority claim, atomic group commit with state CAS and next-work inserts, renewal/current-record replay, state/query/admin/statistics, identity expiration and lease expiration. Fixed logical times are explicit inputs to model transitions. Implementation correspondence verifies that real ownership uses a freshly sampled independent operational clock at linearization.

PostgreSQL correspondence maps the abstract commit to one native transaction over work, state and receipts. S3/Turso correspondence maps it to conditional durable publication and atomic projection application. Public reads/claims cover previously acknowledged history on BOTH. Physical projection diagnostics may inspect a lagging internal prefix, but are not consumer observations and cannot excuse stale successful public reads.

## Observations and replay

Compare full namespace/key identities, lifecycle, selected membership/order, attempts, current versions/expiry, stable tokens, workflow/audit values and versions, newly inserted work, exact request fingerprints, dispositions and receipt multiplicity. Digest collisions must not alias keys. Fingerprints are independently computed from the documented format rather than by calling the SUT fingerprint function.

Equal request replay creates no second effect. Changed body/group identity conflicts. Expired identities never authorize reuse. A claim replay fails as a whole if any original member is settled, deleted or expired; renewal returns current versions and expiry. Failed group preconditions leave no partial work/state/new-work effects. Concurrent operations admit only serial outcomes consistent with real-time completed calls.

## Required interaction witnesses and controls

Include lost response then replay; commit mixed state/work/new work then crash/reopen; partial member settlement then whole-claim replay; renewal then replay/commit; expired request then equal/changed retry; queued clock delay; stale S3 epoch/head; original-genesis corruption; contiguous recovery and interrupted apply; cancellation with retained worker/permit ownership; key deletion/reinsertion and deliberate hash collision; filtered priority ordering under wide/sparse predicates. For each safety property, retain a positive witness and a negative control that the checker actually rejects.

Backend-specific reopen witnesses are distinct from OS process-kill/provider-loss qualification. A modelled lost response is not packet-loss injection. Native PostgreSQL requires no command-log cursor or external owner session; S3 epoch/head is not a consumer transaction credential.

## Bounded evidence

Each executable analysis declares finite domains, time/request/version/frontier bounds, transition source hashes, assumptions, invariant IDs, implementation mapping and exclusions. Record corpus hash, attempted/succeeded traces, actual matching edges and shortest failing prefix. Zero cases, unknown schema/actions, duplicate/unknown IDs, malformed observations, unmatched transitions or exhausted search/resource caps fail qualification.

Exhaustive finite graph exploration differs from selected semantic replay and edge replay. Reports distinguish intended edges from actually matched edges. Finite safety results do not establish unbounded refinement, provider durability, liveness or production numeric progress. TP-004 real backend faults and TP-002/005 numeric/settled qualification remain required.

The former paused-apply/ephemeral-CP/leased-tail corpus is superseded for API-007 correspondence. It may run as legacy regression coverage; its green result is not new-contract qualification. Updated models, correspondence and negative controls are required before readiness claims.
