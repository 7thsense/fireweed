# Adversarial review: writer-contention recovery round 39

Review the current plan and evidence against round 38 and v0.31.21 source. Do
not implement.

Round 38 was folded by:

- making the S-0 semantic test file-backed and its 800-row/4 MiB-class writer
  deliberately adversarial: `cache_size=-4096`, `cache_spill=1`;
- naming non-mutating `-wal` file-length samples immediately before the writer,
  after all updates but strictly before commit, and after commit; no checkpoint
  is invoked;
- making both pre-commit observations valid: if bytes grow, readers must reject
  published uncommitted frames; if they do not grow, record
  `uncommitted_frames_published=false` as the stronger disposition. Semantic
  no-dirty-read behavior, not growth, is the pass condition;
- preserving the shared serving reader's 128 MiB cache through S0 while making
  it query-only, then moving the 128→4 MiB reduction into S3r with a same-SHA
  before/after S0-harness >=90% rate and <=125% p95/p99 gate;
- enumerating shared-reader validation, `render_claimed`, `server_pending_page`,
  `server_live_items`, `server_metrics`, and recovery consumers in S-0's named
  file-backed serving test;
- adding a third live uncommitted writer while every candidate opens its second
  Deferred snapshot on the same connection, which must see the prior newest
  commit and not the dirty value;
- requiring S3r to run eight successive commit→snapshot rounds per pooled
  connection over more than 4 MiB with a live next writer, proving no stale-page
  reuse;
- requiring no `Busy`/`BusySnapshot`, live-writer and no-writer latency maxima,
  typed query-only rejection inside a transaction and again in autocommit;
- parameterizing the production helper by serving/pool role and naming the
  complete supported pragma contract and stop disposition, with numeric
  `query_only=1` last and no `read_uncommitted` call;
- declaring the exact-pin in-crate test authoritative and the separately pinned
  standalone probe corroborative;
- labeling cache totals configured ceilings, recording S3r RSS across pool
  construction and warm reads, and leaving M1/M2/M3 authoritative; and
- turning the historical `read_uncommitted="ON"` diagnostic into assertions and
  updating review frontmatter to the latest completed review.

Audit the whole plan and explicitly re-evaluate every round-38 finding. Use the
same findings table, prior-round audit, verdict, convergence, and summary
contract. `Convergence: YES` requires no BLOCKING and no WARNING. A concern
implementable inside an existing named acceptance criterion without changing
architecture or public contract is a NOTE.
