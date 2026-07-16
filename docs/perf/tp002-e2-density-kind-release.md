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

The in-cluster load generator first measures the hot queue without neighbor
work. A bounded worker pool then makes every cold queue complete a live
claim/finalize cycle, reseeds it with an immediately eligible item, and keeps
cycling cold queues while the hot workload runs. A cold queue counts as active
and progress-eligible only after that cycle succeeds. The release row requires
all 1,000 cold queues to meet that definition and reports numeric hot-queue
ingest and claim/finalize retention against the unloaded baseline.

The run writes
`target/pqueue-ledger/tp002-e2-density-kind.jsonl`. Its row records the exact
command and Git revision, seed, measured duration, kind topology and node image,
host CPU/RAM description, active/progress-eligible queue counts, both hot
throughput rates, progress-bound violations, bounded worker/connection/task
counts, and both noisy-neighbor retention percentages. The focused validator is:

```sh
cargo run -p pqueue-release --bin pqueue-verify-density-evidence -- \
  target/pqueue-ledger/tp002-e2-density-kind.jsonl
```

The validator requires release scale/tier, `bars_met=true`, at least 1,001
queues, both hot rates at or above 2,777.78 items/s, zero progress-bound
violations, all resource counts within their recorded bounds, and positive
numeric retention values. A failed run remains smoke evidence and cannot be
promoted by editing the ledger.

This is intentionally a one-node density proof. It does not claim 1,000x
aggregate throughput, ownership failover, fencing, or `MOVED` behavior.
Multi-owner failover is tracked separately by `pqueue-0a1d4386`.

Useful reproducibility overrides are `CLUSTER`, `IMAGE`, `QUEUE_COUNT`,
`ITEMS`, `HOT_CONNECTIONS`, `NOISY_WORKERS`, `SERVER_WORKERS`, `SEED`, and
`LEDGER_OUT`. Release evidence must retain `QUEUE_COUNT >= 1001`; other values
are captured in the row or the command environment and must not be used to
weaken the semantic validator.
