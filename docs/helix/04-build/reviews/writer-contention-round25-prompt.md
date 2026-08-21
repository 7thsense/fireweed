# Adversarial review: writer-contention recovery round 25

Review the current plan against round 24 and source; do not implement.

Round 24 was folded by:

- replacing the shared committed pool with two fixed eight-connection pools:
  fence-taking selection/validation uses the driver pool, while render,
  observation, and bypass reads use the outcome pool; both have a fixed 5 s
  retryable borrow deadline;
- requiring a more-than-eight-concurrent-Claim-queues test to prove driver-pool
  exhaustion cannot starve outcome rendering, while accepting bounded
  Backpressure for the ninth driver;
- changing SelectionFenceAdmission to queued-only accounting, allowing 1,025
  distinct active queues, rejecting only blocked waiter 1,025, and skipping it
  entirely for bypass vectors;
- clarifying that the exclusive fence does not directly cover Complete, while
  an existing queue permit held by a shared-fence waiter can transitively delay
  Complete; S5 now measures and bounds that gate wait instead of denying it;
- calibrating S3m with a real eight-envelope/800-item Claim vector plus
  concurrent same-queue apply-deque traffic after S4;
- adding `fireweed-turso/src/local.rs` to S-1 and requiring
  `claimed_from_class_s` to rehydrate index-only entities through
  `echo_entity_document`;
- adding inert S2a with an exhaustive `AppendAdmissionClass` carrier on
  `RawCommitRequest` and the dedicated Class-S append request, observable by
  `ObjectLogTursoCommitter::commit_replayable`;
- extending the total order to append admission/gate→driver
  pool→fence→snapshot→release connection→metadata permit→produce lock, with no
  metadata/produce holder allowed to borrow a pool or fence;
- renaming command/debt `admission` and the `admit` timing bucket to
  reservation/reserve; and
- adding S2e so strict `CommitRejection` normalization preserves the four new
  named Backpressure resources while still normalizing unknown strings.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
