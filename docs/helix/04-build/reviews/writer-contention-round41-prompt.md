# Adversarial review: writer-contention recovery round 41

Review the current plan and evidence against round 40 and the v0.31.21 source.
Do not implement or modify unrelated worktree changes.

Round 40 was folded by:

- defining the executable adapter manifest plus root lock on the claimed
  v0.31.21 base as B-0's normative pin: Turso 0.7.0;
- explicitly classifying the uncommitted TD-010/ADR-016/legal/benchmark-lock
  0.7.2 edits as concurrent out-of-scope work that cannot change the adapter
  pin without one separate atomic manifest/root-lock/governed-artifact bump and
  a complete S-0 rerun; those user changes remain untouched;
- adding `server_pending_range` to the serving-reader gate;
- syncing the addendum's WAL labels, third-writer expected value,
  `adversarial_spill_unavailable`, and diagnostic reproduction command;
- treating unavailable `wal_autocheckpoint` attestation and non-monotonic WAL
  deltas as inconclusive diagnostic branches, never semantic failure;
- defining the adversarial writer outside the query-only reader helper with
  exact file-backed WAL/synchronous/cache/busy/cache-spill settings and
  readbacks; and
- making a failed shared-reader cache trial a predicted S3s pool-cache risk,
  requiring profiling/re-derivation inside the 224 MiB post-S3c envelope and
  blocking S3c activation only if that cannot be achieved; the risk table now
  names detection and rollback.

Audit the whole plan and explicitly re-evaluate every round-40 finding. Treat
the concurrent uncommitted 0.7.2 edits as preserved external work, not as
permission to rewrite them. Use the same findings table, prior-round audit,
verdict, convergence, and summary contract. `Convergence: YES` requires no
BLOCKING and no WARNING. A concern implementable inside an existing named
acceptance criterion without changing architecture or public contract is a
NOTE.
