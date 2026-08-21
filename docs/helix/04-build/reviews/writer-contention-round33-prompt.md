# Adversarial review: writer-contention recovery round 33

Review the current plan against round 32 and source; do not implement.

Round 32 was folded by:

- adding queue-scoped ClaimQueueTurn across every Pending-consuming Claim form,
  with one active and one queued driver per queue and the active turn retained
  through publication;
- bounding ClaimDriverReadAdmission to four active plus four queued drivers and
  SharedDriverReadAdmission to twelve active plus twelve queued generations, so
  an admitted semaphore waiter has at most one holder wave;
- showing that ClaimQueueTurn plus the one-active mutation sequencer leave at
  most two fence contenders per queue, hence one fence predecessor;
- deriving both Claim/mutation turn caps at 255 s, slot caps at 95 s, and fence
  cap at 75 s from those structural depths;
- using one internal pre-append attempt for both Claim and mutations, with
  retryable Backpressure returned after resource release;
- adding an independent 8 MiB per-queue/32 MiB process sequencer retained-input
  budget instead of claiming unplanned work reserved apply debt;
- composing the 32-request cohort ceiling as four 505 s services plus three
  fixed 25 ms re-admission intervals and 1 s scheduling slack (2,021.075 s);
- scoping the p95/p99 ratio to comparable admitted-service latency while giving
  overload its own settled-rate and original-request-age gates;
- running two incompatible Claim contenders on the mutation cohort queue,
  separately oversubscribing Claim slots across eight queues and shared slots
  across twenty-four queues;
- marking the S3s fence term carried-not-measured until S3m;
- adding the necessary T2 Claim-cycle diagnostic budget, 200 ms at full
  800-item fill, as a stop before S5; and
- clarifying that the snapshot begins after fence acquisition on the already
  borrowed driver connection.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
