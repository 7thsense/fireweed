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

The in-cluster load generator first measures the hot queue without neighbor
work. A bounded worker pool then makes every cold queue complete a live
claim/finalize cycle, reseeds it with an immediately eligible item, and keeps
cycling cold queues while the hot workload runs. A cold queue counts as active
only when its final live `XLEN` is greater than zero. It counts as
progress-eligible only when its immediately eligible item is claimed and
finalized during the hot phase. The harness measures elapsed time from the
item's eligibility to that claim/finalize; every cold queue must progress and
the maximum observed latency must remain within the 60,000 ms bound. The
release row also reports hot-queue ingest and claim/finalize retention against
the unloaded baseline.

The run writes
`target/pqueue-ledger/tp002-e2-density-kind.jsonl`. Its row records the exact
command, Git revision and image digest, seed, measured duration, one-node kind
topology and node image, host/node/container CPU and RAM descriptions,
active/progress-eligible queue counts, both hot throughput rates, maximum
progress latency and violations, and both noisy-neighbor retention percentages.
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
run. The focused validator is:

```sh
cargo run -p pqueue-release --bin pqueue-verify-density-evidence -- \
  target/pqueue-ledger/tp002-e2-density-kind.jsonl
```

The validator requires release scale/tier, `bars_met=true`, exactly 1,001
queues, both hot rates at or above 2,777.78 items/s, zero progress-bound
violations, maximum progress latency within 60 seconds, all resource counts
within their governed bounds, at least one hot-phase resource sample, and ingest
and claim/finalize retention each at or above 100%. The validator also rejects
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
configuration as release evidence. `CLUSTER`, `IMAGE`, and `LEDGER_OUT` may be
changed as operational locations without weakening a semantic bar. Immediately
before row emission and again before validation, the command rechecks both
`HEAD` and the clean worktree to close the build-to-attestation
time-of-check/time-of-use window.
