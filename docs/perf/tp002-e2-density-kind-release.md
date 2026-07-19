# TP-002 E2 live single-node queue-density proof

Run the named release command from the repository revision being measured:

```sh
scripts/perf/tp002-e2-density-kind.sh
```

The command builds the revision's `Dockerfile.e2` image, deploys one
`pqueue-service` pod to kind with the durable local object-log authority and a
SQLite projection, and provisions 1,001 queues through the generated bootstrap
inventory (`density:q0` through `density:q1000`). `density:q1000` is the hot
queue; the other 1,000 queues are cold neighbors.

The release command fails closed if the worktree is dirty. It always rebuilds
the image, labels it with the full 40-character Git revision, records the
image's SHA-256 ID, and confirms that both the service pod and load Job run that
exact image. There is no release-evidence build-skip path.

The in-cluster load generator brackets the loaded hot measurement with control
measurements before and after it in the same process and deployment. A bounded
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
The load generator emits explicit `HOT_START` and `HOT_END` phase markers. A
separate sampler must record at least one sample strictly between those markers.
The load pod and sampling host share the kind node's system clock; the row
records the hot-phase start/end and first/last accepted sample timestamps, and
the validator requires the sample interval to be contained by the hot interval.
It reads Tokio's live runtime metrics from the service's atomic in-container
snapshot, including the actual worker-pool size and all live async tasks
(detached object-log flushers, background loops, and connection handlers), and
counts established port-8080 TCP connections from the server network namespace.
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
during hot load, complete positive baseline/load measurements whose derived
controls and retention values reconcile, all resource counts within their
governed bounds, and at least one hot-phase resource sample. Absolute rates,
latency, and retention percentages are capacity evidence only. The validator
also rejects quiet-host requirements and host-speed gates, and rejects
any substitution for the canonical 1,001 queues, 300,000 hot items, eight hot
connections, eight cold workers, four server workers, seed 42, or 60,000 ms
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
