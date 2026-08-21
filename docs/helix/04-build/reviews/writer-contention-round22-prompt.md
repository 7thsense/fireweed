# Adversarial review: writer-contention recovery round 22

Review the current plan against round 21 and source; do not implement.

Round 21 was folded by:

- expanding S3r from Claim rendering to every Turso projection read that can
  decide a public outcome or observation, including pause/intake, Push
  validation and idempotency, replay/fence/index/side records,
  renew/finalize/cohort/purge/expiry validation, eligibility, item state and
  version, `commit_validate`, and Claim rendering; the old uncommitted shared
  reader is retired;
- making `commit_validate` a validation-only committed snapshot that closes
  before object-log append, leaving atomic apply synchronous and assigning the
  validation/apply TOCTOU case to S6;
- ordering S3r before S3c so exact coverage is built over coherent committed
  reads rather than repaired afterward;
- rejecting Renew and Reassign expiry values at or before `now`, and bounding
  each wait by `min(5 s, new_expiry-now)`;
- extending S-0 with effective `query_only` and `read_uncommitted` readback on
  both connection classes while an `IMMEDIATE` writer is live, with an explicit
  stop condition when the required isolation is unsupported;
- assigning the KeyedQueueGate active-plus-queued accounting change to S2 in
  `async_commit.rs`, with 1,025-caller, many-queue fan-out, and close/drain
  tests for memory, SQLite, and Postgres products;
- borrowing a committed pooled reader before taking the selection fence and
  opening its snapshot only after coverage, with an eight-reader/live-apply
  zero-hang regression;
- hoisting strict coverage once per public request above the entry loop and
  keeping atomic Turso outside derived SelectionFenceAdmission;
- changing the remaining S3b terminology from gate-to-fence to
  admission-to-fence; and
- making S5 an explicit reviewed five-file atomic activation slice so the new
  candidate-selection SQL has an owner in `projection.rs`.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern can be implemented
within an existing acceptance criterion without changing the architecture,
classify it as NOTE rather than manufacturing a warning.
