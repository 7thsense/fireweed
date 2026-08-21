# Adversarial review: writer-contention recovery round 35

Review the current plan against round 34 and source; do not implement.

Round 34 was folded by:

- adding a sixteen-request active-plus-queued per-key KeyedQueueGate cap while
  preserving its 1,024 global queued cap and distinct-active-key behavior;
- deriving the conservative transitive same-key bound as 16×505 s = 8,080 s
  and adding a 32-command same-key S0/S3s/S5 closed cohort with fixed retry;
- requiring all durable operations to fully materialize their bounded public
  outcome before append; post-publication completion exact-waits apply and sends
  retained data without calling `render_claimed` or borrowing either pool;
- keeping OutcomeReadAdmission solely on pre-position replay/observation/bypass
  reads, so reader-17 rejection always precedes durable work;
- adding the source/fault gate
  `post_publication_response_never_borrows_committed_pool`;
- naming separate publication/response ceilings: 505/540 s service,
  2,021.075/2,056.075 s four-service cohort,
  16,161.775/16,196.775 s 32-service same-key cohort,
  4,040/4,075 s byte-split request, and 64,675 s theoretical 1,024-attached
  Claim tail;
- adding S2/B2 to S3r/B3r dependencies;
- running every exact cliff in an isolated subtest and using a separate
  one-below-cap combined soak;
- naming reclaim/LeaseExpired in the live-KeyedQueueGate site row and stating
  that S3i re-enters and meters that gate; and
- updating structural gates to distinguish publication from response ceilings.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
