# Adversarial review: writer-contention recovery round 40

Review the current plan, evidence, and diagnostic assertions against round 39
and v0.31.21 source. Do not implement.

Round 39 was folded by:

- running and asserting exact Turso 0.7.0 dispositions:
  `wal_autocheckpoint=0` setter `Ok([])`/no readback;
  `cache_spill=1` setter `Ok([])`/readback `1`; query-only write
  `turso::Error::Error` containing
  `Cannot execute write statement in query_only mode`;
- keeping `wal_autocheckpoint` non-fatal and making S3r's no-explicit-checkpoint
  source audit/counter plus file-backed WAL liveness/bounds authoritative;
- naming `adversarial_spill_unavailable` as a non-stopping diagnostic branch,
  and replacing the overclaim for no pre-commit growth with
  `no_uncommitted_wal_growth_observed` or
  `inconclusive-no-growth-observed` when the knobs cannot be attested;
- parameterizing cache size independently of busy timeout;
- preserving the 4 MiB shared-reader trial but making regression a legal branch:
  restore 128 MiB, record a 352 MiB interim configured ceiling through S3c, and
  still construct pools; a passing trial records 228 MiB; both become 224 MiB
  after S3c retires the shared reader;
- adding a bounded `server_pending_page`/`server_pending_range`/
  `server_live_items`/`server_metrics` rate and latency cohort to S0 and naming
  it in S3r's before/after trial;
- explicitly requiring the separate reader to see writer 3 after commit;
- defining stale-page detection as every row in the >4 MiB result set carrying
  the current committed round number; and
- updating review frontmatter and the compatibility addendum.

Audit the whole plan and explicitly re-evaluate every round-39 finding. Use the
same findings table, prior-round audit, verdict, convergence, and summary
contract. `Convergence: YES` requires no BLOCKING and no WARNING. A concern
implementable inside an existing named acceptance criterion without changing
architecture or public contract is a NOTE.
