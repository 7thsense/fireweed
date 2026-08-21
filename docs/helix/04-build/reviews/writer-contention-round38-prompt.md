# Adversarial review: writer-contention recovery round 38

Review the current plan and evidence against round 37 and v0.31.21 source. Do
not implement.

Round 37 was folded by:

- replacing the writer-connection `fresh_value` over-claim with two freshness
  requirements: every one of the twenty-four candidate connections must commit
  its held snapshot, begin a second Deferred transaction on the same connection,
  and see the newest commit; a separately configured non-writer connection must
  agree;
- changing the live writer from a single-row update to a forced-spill,
  800-row/4 MiB-class multi-page transaction and requiring recorded WAL
  byte/frame growth;
- adding a second writer transaction that opens, updates, and commits wholly
  inside the readers' held snapshot window;
- introducing the production committed-reader configure/verify helper in S-0,
  requiring S3r to reuse it, setting numeric `query_only=1` last, using a 4 MiB
  cache and at most 100 ms busy timeout, and never issuing `read_uncommitted`;
- proving query-only enforcement with a rejected write on the same candidate
  connections, not only readback;
- setting the first-SELECT deadline to 90 ms under the 100 ms busy timeout and
  recording maximum latency;
- recording the exact old 0.7.0 keyword disposition:
  `pragma_update("read_uncommitted", "ON")` returns `Ok([])` while
  `PRAGMA read_uncommitted` returns no row;
- keeping the serving-reader correction in S-0 but gating the corrected
  autocommit reader at its product timeout with
  `serving_reader_is_query_only_committed_and_nonblocking`,
  `push_preappend_and_durable_idempotency_are_native_async`, and
  `finalize_dispositions_match_sqlite_for_terminal_retry_release_and_rearm`;
  S0 establishes the only rate baseline after that correction;
- lowering the live shared serving reader to a 4 MiB cache, making S3r's
  interim writer + shared reader + pools ceiling 228 MiB and its post-S3c
  ceiling 224 MiB;
- restating the protocol so correctness depends only on opening the snapshot
  after in-fence coverage, not an unproven first-SELECT pinning point; and
- requiring every evidence line to record adapter/probe pins with the probe pin
  never below the adapter pin.

Audit the whole plan, but explicitly re-evaluate every round-37 finding. Check
whether forced WAL growth is an observable, implementable gate in Turso's local
mode; whether the 90 ms deadline is both strict and robust; whether the second
writer and second same-connection snapshot sequencing proves the exact
freshness/stability properties needed by S3r/S5; and whether the named serving
tests adequately bound the pre-S0 behavior correction.

Use the same findings table, prior-round audit, verdict, convergence, and
summary contract as round 37. `Convergence: YES` requires no BLOCKING and no
WARNING. A concern implementable inside an existing named acceptance criterion
without changing architecture or public contract is a NOTE.
