# Adversarial review: writer-contention recovery round 29

Review the current plan against round 28 and source; do not implement.

Round 28 was folded by:

- adding universal exact coverage: every derived public read captures
  authoritative request-entry high-water once and waits before an outcome
  snapshot; every candidate-mutating plan additionally holds a per-queue
  mutation sequencer, waits the prior mutation frontier, validates, and retains
  the sequencer through append publication;
- explicitly covering Push identity, retention, unique-index, group-size,
  pause/intake/idempotency, Update planning, live/render/metrics,
  Renew/Reassign/Finalize/strict/cohort/purge/expiry, replay/fence/index/side
  records, and Claim selection, with reserved-unpublished/duplicate-Push poison
  tests;
- combining SharedDriverReadAdmission's global twelve-slot semaphore with one
  active candidate-mutation sequencer per queue across direct and keyed call
  sites; S3c activates it with committed reads, while S5 activates the
  Claim-driver slots and selection fence;
- stating that S3r converts every projection helper to a borrowed connection/
  snapshot, making S3c's activation caller-side within its bounded files;
- oversubscribing calibration to eight Claims, twenty-four shared mutators, and
  sixteen outcome readers;
- separating structural driver-pool wait (p99 <=100 ms after slot, zero expiry)
  from independently derived Claim-slot, shared-slot, outcome-pool,
  fence-acquire, and drain thresholds (next power of two above 2×p99, 500 ms
  floor, 5 s ceiling), with zero outcome expiry;
- requiring S5 to re-derive all thresholds on the activated path;
- retaining existing strict per-entry durability/outcomes without a new
  cumulative abort, while metering acquisitions until S6; and
- stating the 121.5 s injected worst-case Claim ceiling and pre-position versus
  post-position retry/poison disposition.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
