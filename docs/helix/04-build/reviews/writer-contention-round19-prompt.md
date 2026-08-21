# Adversarial review: writer-contention recovery round 19

Review the current plan against round 18 and source; do not implement.

Round 18 was folded by:

- splitting a 30 s pre-position phase (linger/produce-lock/encode/leader
  election, retryable cancellation) from the post-position phase entered
  immediately before `engine.produce` (produce/high-water error or timeout is
  ambiguous poison); followers inherit the leader result and never time out;
- naming `PackedAppendError::{BeforePosition,PostPositionAmbiguous}` as the
  typed disposition broadcast to every co-sealed waiter, with branch tests;
- removing the stale create-only/CAS retry claim and avoiding double-counted
  linger in the reservation-head watchdog;
- making S3m shared Push/Update/Retry participants actually take the shadow read
  guard while an item Claim holds the write guard through a real packed append
  on a dedicated calibration queue excluded from T/M counts;
- making Turso `claimed_targets` capture authoritative shard high-water at
  entry, wait it under the retry-inclusive reservation-head budget, report
  `claimed_targets_coverage_wait_ms`, and return projection-coverage
  Backpressure—not StaleLease—on expiry for Renew/Reassign/Finalize;
- scoping the read claim to object-log×Turso, exempting synchronous atomic
  Turso, and leaving postgres read coverage outside this Turso performance plan;
- stating grouped/cohort uses only KeyedQueueGate while item Claim uses only the
  coordinator, so the additive caps never double-charge one request.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. `Convergence: YES` requires no BLOCKING and no WARNING.
