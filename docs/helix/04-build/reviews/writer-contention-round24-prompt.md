# Adversarial review: writer-contention recovery round 24

Review the current plan against round 23 and source; do not implement.

Round 23 was folded by:

- redefining admission at the derived append site, not the public-operation
  name or an earlier prepare phase;
- assigning ClaimCoordinator to the derived default item-Claim append,
  SelectionFenceAdmission to every derived direct
  `commit_strategy().commit()`/`commit_prepared()` append without a live gate
  (including Push, BatchUpdate, prepared Finalize, and legacy grouped/cohort
  Claim), and KeyedQueueGate only when its permit is live through append;
- asserting `derived_turso_admission_map_covers_every_append_site` where the
  fence is acquired and allowing earlier prepare gating only when it is
  non-overlapping;
- stating that atomic Turso direct and macro-generated paths get no derived
  coordinator, SelectionFenceAdmission, selection fence, or coverage wait;
- defining the total order as append admission/gate→bounded pool→fence→snapshot,
  requiring pre-gate validation snapshots to close before `submit_operation`,
  and forbidding connection→gate/admission and fence→connection edges;
- bounding committed-pool borrow at 5 s with retryable
  `Backpressure { resource: "committed read pool" }`, returning connections
  before append, and extending the mixed exhaustion gate to a gate-held pool
  borrower;
- moving the eight-reader WAL/liveness gate into S3r, where the pool is built;
- explicitly accepting and testing ClaimCoordinator's process-wide 1,024
  active+queued ceiling across distinct queues;
- covering the pre-existing satisfied-gate-key response gap in S-1;
- requiring a zero-time coverage probe to return Backpressure before an
  apply-lag StaleLease; and
- making S3m depend on S4 so calibration observes the packed one-transaction
  Claim apply shape, while deleting stale B-0 fallback wording.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
