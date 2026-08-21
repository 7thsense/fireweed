# Adversarial review: writer-contention recovery round 27

Review the current plan against round 26 and source; do not implement.

Round 26 was folded by:

- changing the committed layout to sixteen driver plus eight outcome readers,
  each with a 4 MiB cache, for 224 MiB including the 128 MiB writer—below the
  current 256 MiB configured ceiling;
- retaining four Claim slots for every Pending-consuming item/group/cohort
  Claim, leaving at least twelve driver connections for shared appenders;
- coalescing Push validation, pause/intake, and idempotency into one connection
  and one committed Deferred snapshot per request;
- making S3m exercise the real admission→Claim-slot→16-driver-pool→fence path
  with four real 800-item Claim vectors, at least twelve shared
  Push/Update/Retry/Purge callers, outcome reads, and apply-deque contention;
  shared/outcome borrow expiry must be zero and p99 wait <=100 ms or S5 blocks
  pending redesign/re-review;
- making S3r independently safe: migrated derived reads use one existing
  bounded authoritative-tail catch-up before their committed snapshot until
  S3c replaces it with exact coordinator coverage; S0 non-regression and
  zero-hang are required;
- extending the canonical order and Claim-driver slot test to all
  Pending-consuming Claim forms;
- restating the derived append-site coverage check as a source-audit/test gate,
  while retaining the true no-wildcard command compile guard;
- configuring and reading back pooled `query_only=ON`,
  `read_uncommitted=OFF`, `cache_size=-4096`, and `busy_timeout<=100 ms`;
- hoisting strict commit's outcome borrow and Deferred snapshot once per public
  request above its entry loop; and
- raising the WAL gate to all twenty-four readers with WAL bytes and checkpoint
  disposition asserted under live writer/apply.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract. Audit the whole plan, not only these deltas. `Convergence:
YES` requires no BLOCKING and no WARNING. If a concern is implementable inside
an existing named acceptance criterion without changing architecture or public
contract, classify it as NOTE.
