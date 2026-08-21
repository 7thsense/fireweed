# Adversarial review: writer-contention recovery round 28

Review the current plan against round 27 and source; do not implement.

Round 27 was folded by:

- splitting Push into a short outcome-pool idempotency snapshot, then
  object-log epoch/blob and counter work with no reader transaction, then an
  append-admitted/shared-fenced driver snapshot for validate-push plus
  pause/intake;
- assigning that engine-side phase change to new three-file S3q, with
  differential/fault tests and atomic-product exclusion;
- making S3r purely preparatory/inert: it constructs and tests the committed
  pools/helpers but leaves the existing serving reader in place;
- making S3c the atomic activation/revert unit that seeds exact committed
  high-water after authoritative tail equality, removes empty/not-ready
  shortcuts, and switches every derived public read to committed helpers in the
  same slice, including reserved-unpublished apply-lag tests and S0
  non-regression/zero-hang;
- adding a twelve-slot SharedDriverReadAdmission before the sixteen-connection
  driver pool, alongside four Pending-consuming Claim slots;
- driving twenty-four concurrent shared borrowers (2× shared capacity), four
  real 800-item Claim vectors, outcome readers, and apply-deque contention in
  S3m; any shared/outcome borrow expiry or p99 slot+pool wait above 100 ms blocks
  S5 for redesign/re-review;
- calling S3m a shadow reconstruction and requiring S5 to re-derive the bounds
  on the activated path;
- adding a reader-specific configure/verify path for 4 MiB, query-only,
  committed, <=100 ms readers, with busy failures mapped to retryable pool
  Backpressure;
- recording WAL/checkpoint disposition as disabled/no checkpoint with bounded
  monotonic WAL growth under known writes; and
- metering strict per-entry fence acquisitions under one cumulative 5 s public
  request budget until S6 coalesces Complete.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
