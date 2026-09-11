# Public API workflow acceptance and capacity

This crate depends on `fireweed`, not on its engine, projection, relational
adapter, internal SQL, or log implementation. Handler inputs come from queue
claims. Deterministic enrichment and delivery stubs replace external services.
No producer-to-consumer channel tells a downstream stage what work to do.

Run correctness tests:

```sh
cargo test -p fireweed-workload --release
```

Run capacity separately from compilation and other tests:

```sh
cargo build -p fireweed-workload --release
target/release/fireweed-workload --profile primitives --items 10000 --batch 100 --deadline-seconds 600
target/release/fireweed-workload --profile mutable --items 10000 --batch 100 --workers 2 --deadline-seconds 600
scripts/perf/workflow-capacity.py --profile mutable --items 100000 --batch 1000 --shards 8 --recycle --cycles 5 --deadline-seconds 900
target/release/fireweed-workload --profile snorri --items 10000 --batch 100 --workers 2 --shards 4 --deadline-seconds 600
```

Add `--qualify` to the Python runner to fail unless the targets are met. For
`primitives`, it requires at least one million resident rows and 10k records/sec
for insertion and both individually addressed update phases. For `mutable`, it
requires at least one million completed workflows across at least three recycling
cycles, deterministic faults, retention, and 9.5k workflows/sec overall and at every
shard's fair share in every cycle. Across the last three cycles, RSS must vary by
at most 10% and each projection's size by at most 5%. Both gates require physical
sharding and disk-backed storage and reject external I/O overrides. These are
measured stability checks; retain the raw cycle reports for longer-run analysis.

```sh
scripts/perf/workflow-capacity.py --qualify --profile primitives --items 1000000 --batch 1000 --shards 16 --workers 8 --deadline-seconds 900
scripts/perf/workflow-capacity.py --qualify --profile mutable --items 500000 --batch 1000 --purge-batch 8000 --shards 16 --workers 8 --load-workers 4 --recycle --cycles 6 --deadline-seconds 1200
```

The repeat-qualified configuration is 16 physical shards with eight workers per
shard. On the measured host it completed 7.9–8.0k full workflows/sec across two
three-million-workflow runs; both million-row insert/update runs exceeded 10k/sec.
See the [review and raw evidence](../../docs/helix/04-build/workflow-capacity-review.md).
Run the exact qualification pair twice with one command (the output directory
must not exist):

```sh
bash scripts/perf/qualify-workflow-capacity.sh target/workflow-qualification
```

The v6 gate additionally samples WAL sizes every 100 ms, requires sampling coverage,
and checks a 512 MiB peak budget per shard. Normal WAL truncation/reuse is allowed;
this is an observed acceptance budget, not a hard engine cap.

`--root` requires an empty directory and retains the log/projection for diagnosis.
Without it a temporary directory is removed after the run. Check its filesystem:
`/tmp` can be RAM-backed. For disk-backed capacity, use
`scripts/perf/workflow-capacity.py` with the same arguments; it defaults data to
`target/workflow-capacity/`, records the mount and file sizes, and removes its
temporary data after measurement. `--projection-root NEW_DIRECTORY` places only
Turso files in a separate location; the authoritative log remains under `--root`.
The runner records both filesystems and storage footprints. Use this to isolate
projection I/O, and label RAM-filesystem results separately from disk projections.
`FIREWEED_SQL_TRACE=1` optionally records slow SQL timings without bound values.
`FIREWEED_WORKLOAD_TIMING=1` records public API wait times and backpressure resources;
`FIREWEED_LOG_TRACE=1` separates log preparation, durable produce, and metadata time.
The composed Turso projection uses filesystem I/O without stable-storage sync;
all authoritative-log syncs remain enabled. Turso uses NORMAL checkpoint accounting
to avoid repeatedly backfilling the same pages. After machine/power failure, a
projection may need deletion and rebuilding from the log. Projection files are
never an independent durability source. Ordinary standalone `TursoConfig::local`
keeps its existing I/O behavior; the composition explicitly opts into log-backed
projection I/O.

The default cell is
`filesystem--turso` with asynchronous projection; `--memory` is a semantic/control
comparison, not a durable performance result. Every shard has its own physical
log and projection. Multiple queues in one SQLite-family database would still
share a writer and are not what `--shards` measures.

Profiles:

* `bulk`: fully load a scheduled backlog, then independent continuous delivery
  loops. Due priorities vary; batches are not assigned one convenient priority.
* `mutable`: the primary original-row workload. Overlap loading, two enrichment
  stages, and delivery. A shared priority-ordered claim loop dispatches each row
  using its stage metadata; both enrichments update that same row. Completion
  and failure also mutate those rows, including outcome metadata. Every handler
  batch uses the public `mutate_items` API with the claimed item version and
  lease token: enrichment and returning to Pending share one durable command.
  Mixed retry/success/failure outcomes share a batch. Workers run concurrently
  without a process-local job mutex. This path currently supports addressed,
  ungrouped rows on queues without secondary indexes, typed indexes, or an
  entity schema; other mutation shapes remain unavailable on composed Turso.
  `--recycle --cycles N` runs complete workflows and retention purge repeatedly
  on the same stores, reusing keys after expiry and checking zero retained queue
  rows after every cycle. It includes purge in the reported rate and reports
  per-cycle projection size. This mode currently supports mutable and bulk.
* `snorri`: one shared worker pool reclaims expired leases, claims transition
  inputs, and dispatches the stub handler indicated by each input's stage. This
  matches the sibling adapter's ordinary-claim fallback; it does not add a
  per-stage metadata filter. Transitions atomically finalize an input, write an
  opaque instance record, advance an instance fence, and enqueue the next input.
  This uses Fireweed's existing commit interface, not the sibling Snorri adapter
  or its preferred typed transition-index path.
* `retention`: repeat load, enrichment, delivery, and purge on the same physical
  store (`--cycles`, default 3). Request IDs and item keys are reused after their
  retention period. Each completed cycle reports process RSS and projection size;
  the authoritative log is expected to grow. Run capacity separately from tests.
* `primitives`: a component capacity ladder through the same public API. Load,
  enrich by client key, schedule by item ID, drain in priority order, then purge.
  Unlike autonomous workflows, update addresses come from the load response;
  this is an explicit microbenchmark, not a claim about worker discovery cost.
  Every phase waits for projection coverage using a public metrics read.

Workflow delivery stubs fail once for recipient IDs divisible by 19 and fail
permanently for IDs divisible by 31. Assertions check exact payloads and terminal
outcomes, retry counts, duplicate deliveries, and final queue counts. `--no-faults`
provides an explicitly labelled capacity calibration. The acceptance oracle is
never a source of work for handlers. Equal retry outcomes are submitted as one
bounded batch per claim response. Workflow report schema v6 records shared
dispatch and atomic original-row mutations, cycle counts, and whether purge is
included. Earlier v3 mutable runs used release plus batch update under a local
owner mutex; v2 used three stage-filtered pools and Snorri commits for delivery
receipts; v1 also used individual retry calls. Rates across these workload
versions are not comparable as backend-only improvements. The Snorri profile is a separate integration
workload, not a prerequisite for qualifying the basic row operations.

An injected logical clock makes eligibility deterministic. Timings use real
`Instant` wall time, including API waits and final projection coverage. Capacity
runs make all scheduled work due; separate contracts test future eligibility,
FIFO ties, expired lease reclamation, stale commits, payload Keep/Replace/clear,
idempotent update replay, purge, and key reuse. A separate child process exits
without shutdown after acknowledged calls; the parent rebuilds a projection
using only the copied log and checks original-row IDs, leases, versions, enriched
payloads, and exact mutation replay.

## Source mapping

The reviewed Seventh Sense source was `telepathdata/7thsense` master
`30c9e4bd817c53c8918215f9b94dae01b8f89fe8`:

* `actions-queue`: callback scheduler claims a scheduler job using
  `FOR UPDATE SKIP LOCKED`, reads queued actions in creation order, schedules a
  bounded batch, and deletes successfully scheduled inputs. The executor loops
  over due scheduled actions, sends a bounded batch, and persists outcomes/retries.
* `jobs-scheduled-actions`: load recipients, assign schedules, atomically mark
  a due priority-ordered batch Executing, deliver, and update statuses. Archival
  later aggregates and deletes old scheduled rows.

Concrete reference files include
`modules/actions/src/main/scala/ss/actions/CallbackJobSchedulerWorker.scala`,
`modules/actions/src/main/scala/ss/actions/CallbackJobExecutorWorker.scala`,
`modules/lists/src/main/scala/ss/lists/jobs/MultiCohortsDirectActionScheduler.scala`,
`modules/akka/src/main/scala/ss/akka/lists/ScheduledActionQueueManager.scala`, and
`modules/jobs/src/main/scala/ss/jobs/ScheduledActionArchiving.scala`.

These are job-level and record-level coordination, not an assumption that every
row in both implementations is independently SKIP LOCKED. Stub handlers model
the observable queue/state transitions, not the PostgreSQL execution mechanism.

Sibling Cayce revision `20c31168d079ae5e4c5ddb407922373bc0dcd283` resolves runtime
profiles and runs bounded delivery transitions/effects. The sibling Snorri
`state_store_claim` and `state_store_commit` code establishes the transition
input, lease/version, opaque state, lifecycle input, and fence boundary. Its
Fireweed adapter still references the retired SQLite opening API; this work does
not migrate that adapter. Recycling behavior deferred in Cayce is not asserted
as implemented here.
