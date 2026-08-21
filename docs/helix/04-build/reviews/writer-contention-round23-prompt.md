# Adversarial review: writer-contention recovery round 23

Review the current plan against round 22 and source; do not implement.

Round 22 was folded by:

- defining one lock order for every serving path needing both resources:
  pool→fence→committed Deferred snapshot; the snapshot closes and the
  connection returns before object-log append while the fence remains held;
  bypass paths take only the pool and no fence holder may begin a pool borrow;
- adding a mixed shared/exclusive, pool-exhaustion, live-apply zero-hang gate;
- replacing the admission table with a per-product call-graph map: derived
  default item Claim uses ClaimCoordinator; only derived Push and the legacy
  grouped/cohort overrides that bypass `submit_operation` use
  SelectionFenceAdmission; typed derived operations and all atomic operations
  keep their existing KeyedQueueGate; recovery is pre-serving;
- preserving KeyedQueueGate's queued-waiter cap and per-key active ownership,
  explicitly testing both 1,025 blocked waiters and 1,025 distinct active keys,
  and removing the false 3,072 total-composition claim;
- retiring the uncommitted shared reader consistently in the protocol;
- changing Renew/Reassign coverage to
  `max(0,min(5 s,new_expiry-now))`, with immediate validation and no
  Turso-only Invalid outcome for nonpositive remaining time;
- making stable, nonblocking committed Deferred snapshots a hard S-0
  prerequisite and deleting the infeasible joined-autocommit fallback;
- assigning committed-pool construction and effective-pragma tests to
  S3r/`local.rs`;
- describing Class-S `index_fields` as an internal entity-rehydration carrier,
  not a public Claim member, and distinguishing the `5999aa77` regression from
  the pre-existing entity gap;
- making atomic-versus-derived coverage/admission an explicit per-product
  capability in the shared port macro; and
- defining the S5 non-regression bound as at least 90% of every S0 mixed-control
  ack/settled median and at most 125% of its p95/p99 latency, without weakening
  absolute T2.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is already implementable
within a named acceptance criterion and does not require an architectural or
contract change, classify it as NOTE.
