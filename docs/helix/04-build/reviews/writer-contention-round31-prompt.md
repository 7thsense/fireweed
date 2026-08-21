# Adversarial review: writer-contention recovery round 31

Review the current plan against round 30 and source; do not implement.

Round 30 was folded by:

- deriving selection-fence acquisition above 2×p99 with a 500 ms floor and
  65 s cap, covering one legal two-phase shared publication; drain/delta
  coverage remains independently capped at 5 s;
- fixing the canonical order for every site as append admission/gate→mutation
  sequencer when candidate-mutating→Claim/shared read slot→driver pool→fence→
  snapshot→release slot/connection→metadata permit→produce lock;
- capping each queue at sixteen mutation requests/two eight-request
  generations; request 17 gets retryable pre-plan capacity Backpressure, so the
  65 s sequencer deadline has at most one predecessor;
- making sequencer expiry atomically reject the unplanned generation with named
  retryable Backpressure/no durable effect; same request IDs retry at the FIFO
  tail without overtaking in-process work;
- calibrating S3s with exactly two admitted one-queue generations plus request
  17 rejection, a separate twenty-four-request/twelve-queue lane, eight
  grouped/cohort planning borrowers over four Claim slots, and sixteen outcome
  readers—the actual S3c composition;
- activating ClaimDriverReadAdmission for existing grouped/cohort planning in
  S3c, then extending it to provisional-replacement item Claim in S5;
- adding `mutation sequencer wait` to S2e and explicit release/retry semantics;
- deriving Claim select/reserve/encode work from 800-item/4 MiB p99 with a 5 s
  cap and adding generation fill/byte split to structural gates;
- deriving Claim-slot at 30 s, fence acquisition at 65 s, drain/delta/work at
  5 s, and re-deriving on the activated S5 path; and
- restating activated hard ceilings: 240 s mutation and 390 s Claim, with
  pre-position retry/no-effect versus post-position poison/replay.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
