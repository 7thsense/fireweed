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
target/release/fireweed-workload --profile snorri --items 10000 --batch 100 --workers 2 --shards 4 --deadline-seconds 600
```

`--root` requires an empty directory and retains the log/projection for diagnosis.
Without it a temporary directory is removed after the run. Check its filesystem:
`/tmp` can be RAM-backed. For disk-backed capacity, use
`scripts/perf/workflow-capacity.py` with the same arguments; it defaults data to
`target/workflow-capacity/`, records the mount and file sizes, and removes its
temporary data after measurement. The default cell is
`filesystem--turso` with asynchronous projection; `--memory` is a semantic/control
comparison, not a durable performance result. Every shard has its own physical
log and projection. Multiple queues in one SQLite-family database would still
share a writer and are not what `--shards` measures.

Profiles:

* `bulk`: fully load a scheduled backlog, then independent continuous delivery
  loops. Due priorities vary; batches are not assigned one convenient priority.
* `mutable`: overlap loading, two preparation stages, and delivery. Preparation
  discovers work through claims. Fireweed's batch update accepts Pending rows,
  so each stage serializes its scheduler job, releases the batch, then updates it.
  This mirrors Seventh Sense's outer scheduler-job ownership. It is not an
  atomic lease-guarded mutation; `mutate_items` is currently unavailable on the
  composed Turso backend. The job owner must recover an interrupted preparation
  using the unchanged stage metadata. These stages are deterministic and repeatable.
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
bounded batch per claim response. Workflow report schema v2 records worker pools
explicitly; v1 diagnostics used individual retry calls and stage-filtered Snorri
consumers, so their rates are not comparable as backend-only improvements.

An injected logical clock makes eligibility deterministic. Timings use real
`Instant` wall time, including API waits and final projection coverage. Capacity
runs make all scheduled work due; separate contracts test future eligibility,
FIFO ties, expired lease reclamation, stale commits, payload Keep/Replace/clear,
idempotent update replay, purge, and key reuse. A separate child process exits
without shutdown after acknowledged calls; the parent rebuilds a projection
using only the copied log and checks IDs, leases, payloads, and side records.

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
