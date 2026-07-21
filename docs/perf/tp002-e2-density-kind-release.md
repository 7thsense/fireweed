# TP-002 E2 live single-node queue-density proof

Run the named release command from the repository revision being measured:

```sh
scripts/perf/tp002-e2-density-kind.sh
```

The command builds the revision's `Dockerfile.e2` image, deploys one
`pqueue-service` pod to kind with the durable local object-log authority and
the governed direct SQLite projection, and provisions 1,001 queues
through the generated bootstrap
inventory (`density:q0` through `density:q1000`). `density:q1000` is the hot
queue; the other 1,000 queues are cold neighbors.

The pod mounts `/data` from a bounded 64 GiB disk-backed `emptyDir`. This keeps
object-log segments and SQLite files outside the container's fixed 4 GiB memory
cgroup; a memory-backed volume would charge those files to the same limit and
OOM the canonical 300,000-item workload. The service executes the real
object-log fsync and SQLite projection code paths. Host-disk contention changes
reported capacity only because elapsed time and absolute throughput are not
release gates. The 64 GiB bound covers the canonical ingest plus durable
claim/finalize command streams and their object-log metadata without relying on
unbounded node storage. This one-node diagnostic excludes pod or node failover,
so it makes no persistence claim beyond the live deployment.
The wrapper does not set the retired `PQUEUE_OBJECT_LOG_MODE` pseudo-axis. The
row identifies the exact `object_log_sqlite_projection` success barrier and
does not relabel a hybrid projection as direct SQLite evidence.

The release command fails closed if the worktree is dirty. It always rebuilds
the image, labels it with the full 40-character Git revision, records the
image's SHA-256 ID, and confirms that both the service pod and load Job run that
exact image. There is no release-evidence build-skip path.

The in-cluster load generator brackets the loaded hot measurement with control
measurements before and after it in the same process and deployment. Each
control arm uses the fixed 10,000-item diagnostic size; the governed loaded arm
remains exactly 300,000 items. The ledger records both counts, and the validator
rejects substitutions. A bounded
worker pool makes every cold queue complete a live
claim/finalize cycle, reseeds it with an immediately eligible item, and keeps
cycling cold queues while the hot workload runs. A cold queue counts as active
only when its final live `XLEN` is greater than zero. It counts as
progress-eligible only when its immediately eligible item is claimed and
finalized during the hot phase. The harness measures elapsed time from the
item's eligibility to that claim/finalize; every cold queue must make progress
and no cold claim may return empty. The row reports the maximum observed
latency and configured 60,000 ms progress bound as capacity/configuration data,
not as a host-speed gate. It also reports hot-queue ingest and claim/finalize
retention against the harmonic mean of the two same-run control windows.
If the canonical 300,000-item loaded window completes before all cold queues
have progressed, the load generator runs additional exact 300,000-item hot
sustain windows before ending the loaded phase. The row records and reconciles
the window count and total sustain items, so a fast host cannot fail merely
because its first hot window was shorter than one complete cold-queue cycle.
The load generator also records exact accepted, claimed, and finalized item-ID
sets for the measured hot and cold workloads; setup warm-ups are explicitly
outside these measured lifecycle totals. After the workers stop, it sums live cold-queue
`XLEN` values and requires exactly one retained eligible item per cold queue.
The semantic validator reconciles accepted = finalized + pending for the cold
workload, accepted = claimed = finalized for the hot workload, and requires
zero lost items, duplicate transitions, empty post-reseed claims, and
queue-global progress violations.

The run writes
`target/pqueue-ledger/tp002-e2-density-kind.jsonl`. Its row records the exact
command, Git revision and image digest, seed, measured duration, one-node kind
topology and node image, host/node/container CPU and RAM descriptions,
active/progress-eligible queue counts, both hot throughput rates, maximum
progress latency and violations, and both noisy-neighbor retention percentages.
The wrapper retains the latest Job object plus load/server logs under
`target/pqueue-ledger/tp002-e2-density-kind-diagnostics/` before deleting its
namespace. Only the top-level wrapper process owns cleanup; background log and
sampler subshells cannot fire the namespace-deletion trap.
The load generator emits explicit `HOT_START` and `HOT_END` phase markers.
Resource correctness does not depend on landing a periodic sample inside that
interval. Density instrumentation enforces the connection cap at handler
allocation, aborts the service on a worker/task bound violation, and exports
process-lifetime high-water counters in its atomic in-container snapshot. The
row records the actual worker-pool size and maximum live async tasks
(detached object-log flushers, background loops, and connection handlers), and
the allocation-observed maximum live RESP connections. The same snapshot reads
the service container's cgroup v2 `memory.current`, `memory.peak`, and
`memory.max` counters, which cover the complete process (RESP buffers and
backend state together). The sampler carries those counters through the hot
phase and the row names `cgroup_v2` as their accounting source. Canonical
evidence fails closed unless current <= peak <= the fixed 4 GiB container
limit; memory is a governed resource bound, not a host-performance gate.
The governed maxima are four Tokio workers, 32 connections, and 64 live tasks;
these limits are fixed in the semantic validator rather than selected by the
run. The snapshot reporter is disabled in normal service deployments and is
enabled only by the density Deployment's explicit
`PQUEUE_RUNTIME_RESOURCE_METRICS_PATH`. The focused validator is:

```sh
cargo run -p pqueue-release --bin pqueue-verify-density-evidence -- \
  target/pqueue-ledger/tp002-e2-density-kind.jsonl
```

The validator requires release scale/tier, `bars_met=true`, exactly 1,001
queues, all 1,000 cold queues active and observed making non-empty progress
during hot load, exact lifecycle reconciliation with zero correctness or
progress violations, complete positive baseline/load measurements whose derived
controls and retention values reconcile, all resource counts within their
governed bounds with continuous enforcement enabled. Absolute rates,
latency, and retention percentages are capacity evidence only. The validator
also rejects quiet-host requirements and host-speed gates, and rejects
any substitution for the canonical 1,001 queues, 300,000 hot items, eight hot
connections, 10,000 control items, eight cold workers, four server workers, seed 42, or 60,000 ms
progress bound. A failed run remains smoke evidence and cannot be promoted by
editing the ledger.

This is intentionally a one-node density proof. It does not claim 1,000x
aggregate throughput, ownership failover, fencing, or `MOVED` behavior.
Multi-owner failover is tracked separately by `pqueue-0a1d4386`.

The governed release configuration uses 1,001 queues, 300,000 hot items, eight
hot connections, eight cold workers, four server workers, seed 42, and a
60,000 ms progress bound. Environment overrides are useful for diagnostic
reproduction, but the focused validator rejects a changed governed
configuration as release evidence. The fixed work and resource shape makes the
diagnostic reproducible without assuming a particular machine's speed or an
idle host. `CLUSTER`, `IMAGE`, and `LEDGER_OUT` may be
changed as operational locations without weakening a semantic bar. Immediately
before row emission, before validation, and after the final verifier returns,
the command rechecks both `HEAD` and the clean worktree. The emitter and
verifier run with Cargo's `--locked` mode. Together these checks close the
build-to-attestation time-of-check/time-of-use window.

The prerequisite bottleneck/cold-progress reproduction is a separate finite
diagnostic:

```sh
scripts/perf/tp002-d5-density-diagnostic-kind.sh
```

It fixes the shape at 1,001 queues, 10,000 hot and control items, 64 hot
connections, eight cold workers, four server workers, and seed 42. It writes a
non-release JSON artifact with exact terminal stage counters, the full result,
phase-log SHA-256, exact revision/image, and observed resource highs. Its gate
requires all 1,000 cold queues to progress with zero empty claims, but imposes
no quiet-host, elapsed-time, latency, or throughput floor.
