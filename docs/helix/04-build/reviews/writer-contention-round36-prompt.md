# Adversarial review: writer-contention recovery round 36

Review the current plan against round 35 and source; do not implement.

Round 35 was folded by:

- adding S3g/B3g to prepare an inert grouped/cohort full-row result carrier and
  bulk helper, then making S3c atomically activate grouped/cohort
  pre-materialization and retire `finish_rendered_claim`/post-append
  `render_claimed` before committed pools serve;
- requiring S5 item/group/cohort continuation to have no projection handle or
  post-publication pool borrow;
- recomputing KeyedQueueGate occupancy on the 540 s response term:
  16×540 = 8,640 s transitive and 17,246.775/17,281.775 s for the 32-command
  same-key publication/response cohort;
- adding distinct `QueueGateError::PerKeyFull` and
  `Backpressure { resource: "keyed queue per-key waiters" }`, preserving it
  through S2e and metrics while keeping global queue-full distinct;
- making deterministic request 17 specific to the zero-body lane and requiring
  realistic lanes to record the observed first third-generation index;
- naming the same-key ceiling explicitly in S5;
- recording a transient 4 MiB recovery-seeding connection that closes before
  serving readers, 132/224 MiB recovery/serving page-cache ceilings, up to
  64 MiB normal retained-response heap, and exact run-alone response bytes;
- stating that rejected owned suffix rounds resolve every attached waiter with
  retryable Backpressure/no durable effect, with public evidence retry at fixed
  25 ms; admitted-round and injected-rejection timing are reported separately;
  and
- adding S3g to the issue and tracker decomposition.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
