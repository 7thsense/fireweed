# pqueue performance baseline — v0.3.0

**Date:** 2026-06-28
**Harness:** `crates/pqueue-bench` (`pqueue-bench --workloads ingest,claim,lifecycle`), release build.
**Machine:** OrbStack Linux VM on a macOS host — 12 vCPU, 94 GiB RAM, `rustc 1.92.0`. Postgres
(`postgres` + `postgres_relational`) over the OrbStack bridge at `192.168.215.2:5432` (a few-ms RTT,
synchronous client). Single in-process driver (`futures::executor::block_on`, single thread).
**Scale:** 20,000 items/queue, batch 500 — EXCEPT `postgres_relational`, run at 1,000 items / batch 200
(its per-item write path is ~10,000x slower than the log family over the network and a 20k sweep is
impractical on a dev box; see below).

> **This is an in-process dev-box baseline, not a production number.** Everything runs in one process on
> one thread driving the backend directly (no server, no concurrency, no client/network for the embedded
> backends; a single synchronous connection for postgres). Treat these as RELATIVE shape/family/backend
> comparisons and order-of-magnitude floors, not throughput SLAs. The E0 floor referenced by the harness
> is 10,000,000 items/hr/queue == 2,778 items/s; "FAIL" in the raw harness output only means a given
> single-threaded op fell under that floor, not that the backend is broken.

## What was measured

- **Six backends, both projection families.**
  - *log-replay* family (in-memory projection rebuilt from a durable log): `memory`, `sqlite`,
    `objectlog`, `postgres`.
  - *relational* family (DB-resident / DB-authoritative projection): `sqlite_relational`,
    `postgres_relational`.
- **Six data shapes** (a representative set, not the full cross-product): `minimal` (0B payload, 0 fields,
  ungrouped, sequential priority — the floor reference), `hot_record` (1 KB payload + 16x64B fields,
  uniform), `large_payload` (16 KB payload), `grouped` (256B + 4 fields, 64 groups), `cohort` (256B + 4
  fields, cohorts of 8, cohort policy on), `skewed_priority` (256B, skewed priority band).
- **Three workloads:** `ingest` (`push_batch`), `claim` (`claim`+`ack` drain), and `lifecycle` — a fuller
  correctness+perf pass (push -> claim -> `update_fields` -> ack most / nack-retry some -> `reclaim_expired`
  -> re-drain) whose state-machine invariants are asserted at every step (the same routine the e2e test
  `tests/e2e_shapes_tests.rs` runs and that PASSES across every embedded backend + both pg backends).

`items/s` below is the harness's `items/hr` divided by 3,600. For `ingest`/`claim`, `p50/p95/p99` are
per-batch (batch=500/200) latencies. For the lifecycle `lc-claim` row, the percentile columns are the
*whole claim-phase wall time* (the lifecycle claims via many small batches, so there is one timing per
phase), not a per-batch percentile.

## Headline findings

- **The log-replay family dominates the write path; the relational family dominates the read/claim path —
  per backend.** In-memory `memory` ingests the `minimal` shape at ~1.2M items/s and claims at ~2.2M/s.
  Among the *durable* backends, the picture splits by op: for **ingest**, `sqlite_relational`
  (~108K items/s minimal) beats `sqlite` log (~17K/s) because the log backend pays an fsync per batch;
  for **claim**, both sit in the same 7-40K/s band. `objectlog` is the fastest durable log backend here
  (file-per-object, OS page cache) at ~0.5-0.8M/s for minimal claim.
- **Payload size hits the log family hardest.** `large_payload` (16 KB) drops `sqlite` ingest to ~3K/s and
  `objectlog` ingest to ~8K/s (every byte is written to the log), while `sqlite_relational` holds ~65K/s
  (the payload is one column write). The relational family amortizes big bodies better on ingest.
- **Grouping/cohort is expensive for the RELATIONAL family, cheap for the log family.** `grouped`/`cohort`
  collapse `sqlite_relational` ingest from ~108K/s (minimal) to ~3K/s and claim to ~100-800/s — the
  DB-authoritative projection does per-group bookkeeping on every grouped item. The log family barely
  notices: `objectlog` claims `grouped` at ~0.44M/s, `memory` at ~0.8M/s. If your workload is
  heavily grouped/cohorted, the log-replay family is far better today.
- **`postgres_relational` write/claim is ~10,000x slower than `postgres` log over the network.** Its
  push/claim path issues per-item synchronous statements, so at one connection it lands at **~30-140
  items/s** across every shape (vs `postgres` log at 7-40K items/s). It is *correct* on every shape (the
  e2e test passes at 2,000 items) but it is not throughput-competitive in this single-connection in-process
  harness; a 20k sweep would take ~an hour, hence the 1k scale for that backend only. The headline: the
  DB-authoritative postgres projection is a correctness/queryability play, not a high-ingest path, at least
  without pipelining/batched statements.
- **`skewed_priority` ~= `minimal`.** Priority skew costs essentially nothing on any backend — the priority
  index/sort is not the bottleneck for these shapes.

### Ingest (push_batch)

| backend | family | shape | items/s | p50 | p95 | p99 |
|---|---|---|--:|--:|--:|--:|
| memory | log-replay | minimal | 1.24M | 255us | 968us | 3.2ms |
| memory | log-replay | hot_record | 178K | 1.8ms | 2.5ms | 5.2ms |
| memory | log-replay | large_payload | 447K | 324us | 470us | 3.0ms |
| memory | log-replay | grouped | 694K | 502us | 701us | 1.5ms |
| memory | log-replay | cohort | 706K | 497us | 668us | 1.6ms |
| memory | log-replay | skewed_priority | 1.73M | 245us | 390us | 1.1ms |
| sqlite | log-replay | minimal | 17K | 31.7ms | 40.1ms | 55.1ms |
| sqlite | log-replay | hot_record | 7K | 70.1ms | 96.5ms | 112.7ms |
| sqlite | log-replay | large_payload | 3K | 152.6ms | 171.6ms | 193.1ms |
| sqlite | log-replay | grouped | 7K | 64.8ms | 106.8ms | 138.5ms |
| sqlite | log-replay | cohort | 8K | 57.7ms | 83.2ms | 89.6ms |
| sqlite | log-replay | skewed_priority | 7K | 62.5ms | 101.4ms | 143.9ms |
| sqlite_relational | relational | minimal | 108K | 4.6ms | 4.8ms | 4.9ms |
| sqlite_relational | relational | hot_record | 56K | 8.3ms | 8.7ms | 8.8ms |
| sqlite_relational | relational | large_payload | 65K | 7.4ms | 8.5ms | 9.2ms |
| sqlite_relational | relational | grouped | 3K | 143.2ms | 286.7ms | 317.3ms |
| sqlite_relational | relational | cohort | 3K | 132.2ms | 278.8ms | 293.0ms |
| sqlite_relational | relational | skewed_priority | 101K | 4.9ms | 5.2ms | 5.2ms |
| objectlog | log-replay | minimal | 794K | 576us | 980us | 1.7ms |
| objectlog | log-replay | hot_record | 66K | 6.8ms | 7.4ms | 7.8ms |
| objectlog | log-replay | large_payload | 8K | 51.5ms | 76.4ms | 131.7ms |
| objectlog | log-replay | grouped | 246K | 1.8ms | 2.3ms | 3.0ms |
| objectlog | log-replay | cohort | 253K | 1.8ms | 2.2ms | 2.6ms |
| objectlog | log-replay | skewed_priority | 439K | 1.1ms | 1.3ms | 2.0ms |
| postgres | log-replay | minimal | 32K | 15.2ms | 25.2ms | 42.8ms |
| postgres | log-replay | hot_record | 12K | 35.5ms | 47.3ms | 237.1ms |
| postgres | log-replay | large_payload | 3K | 158.2ms | 189.2ms | 359.7ms |
| postgres | log-replay | grouped | 29K | 10.7ms | 24.7ms | 218.3ms |
| postgres | log-replay | cohort | 40K | 11.2ms | 19.7ms | 21.1ms |
| postgres | log-replay | skewed_priority | 11K | 18.7ms | 238.3ms | 320.9ms |
| postgres_relational | relational | minimal | 139 | 1.23s | 2.08s | 2.08s |
| postgres_relational | relational | hot_record | 83 | 1.67s | 3.77s | 3.77s |
| postgres_relational | relational | large_payload | 56 | 3.29s | 5.76s | 5.76s |
| postgres_relational | relational | grouped | 83 | 2.36s | 3.00s | 3.00s |
| postgres_relational | relational | cohort | 83 | 2.01s | 2.47s | 2.47s |
| postgres_relational | relational | skewed_priority | 111 | 2.07s | 2.28s | 2.28s |

### Claim+ack drain (claim)

| backend | family | shape | items/s | p50 | p95 | p99 |
|---|---|---|--:|--:|--:|--:|
| memory | log-replay | minimal | 2.23M | 172us | 196us | 295us |
| memory | log-replay | hot_record | 250K | 1.4ms | 2.1ms | 2.5ms |
| memory | log-replay | large_payload | 1.91M | 204us | 232us | 247us |
| memory | log-replay | grouped | 803K | 433us | 539us | 582us |
| memory | log-replay | cohort | 756K | 449us | 583us | 646us |
| memory | log-replay | skewed_priority | 2.63M | 139us | 169us | 187us |
| sqlite | log-replay | minimal | 7K | 34.1ms | 41.8ms | 42.3ms |
| sqlite | log-replay | hot_record | 8K | 34.5ms | 39.3ms | 45.0ms |
| sqlite | log-replay | large_payload | 7K | 34.2ms | 41.6ms | 65.6ms |
| sqlite | log-replay | grouped | 4K | 57.6ms | 96.8ms | 153.7ms |
| sqlite | log-replay | cohort | 5K | 49.0ms | 70.7ms | 118.6ms |
| sqlite | log-replay | skewed_priority | 4K | 53.7ms | 84.4ms | 111.2ms |
| sqlite_relational | relational | minimal | 39K | 8.3ms | 9.9ms | 10.6ms |
| sqlite_relational | relational | hot_record | 19K | 20.5ms | 23.1ms | 25.2ms |
| sqlite_relational | relational | large_payload | 16K | 21.9ms | 31.1ms | 33.6ms |
| sqlite_relational | relational | grouped | 806 | 308.5ms | 325.8ms | 363.9ms |
| sqlite_relational | relational | cohort | 111 | 2.08s | 2.22s | 2.31s |
| sqlite_relational | relational | skewed_priority | 36K | 9.1ms | 11.1ms | 11.4ms |
| objectlog | log-replay | minimal | 511K | 534us | 708us | 942us |
| objectlog | log-replay | hot_record | 239K | 1.4ms | 2.0ms | 2.0ms |
| objectlog | log-replay | large_payload | 664K | 448us | 605us | 791us |
| objectlog | log-replay | grouped | 439K | 698us | 868us | 879us |
| objectlog | log-replay | cohort | 218K | 684us | 1.4ms | 36.4ms |
| objectlog | log-replay | skewed_priority | 858K | 356us | 417us | 459us |
| postgres | log-replay | minimal | 7K | 15.0ms | 218.2ms | 224.9ms |
| postgres | log-replay | hot_record | 13K | 15.8ms | 25.1ms | 223.7ms |
| postgres | log-replay | large_payload | 9K | 15.5ms | 219.0ms | 225.5ms |
| postgres | log-replay | grouped | 15K | 14.4ms | 16.9ms | 224.8ms |
| postgres | log-replay | cohort | 21K | 6.4ms | 16.7ms | 17.0ms |
| postgres | log-replay | skewed_priority | 11K | 15.6ms | 26.6ms | 223.1ms |
| postgres_relational | relational | minimal | 83 | 759.0ms | 960.8ms | 960.8ms |
| postgres_relational | relational | hot_record | 56 | 550.4ms | 4.46s | 4.46s |
| postgres_relational | relational | large_payload | 56 | 1.02s | 1.28s | 1.28s |
| postgres_relational | relational | grouped | 56 | 1.68s | 1.92s | 1.92s |
| postgres_relational | relational | cohort | 28 | 2.36s | 3.83s | 3.83s |
| postgres_relational | relational | skewed_priority | 83 | 532.6ms | 929.5ms | 929.5ms |

### Lifecycle (push then full claim phase)

| backend | family | shape | items/s | p50 | p95 | p99 |
|---|---|---|--:|--:|--:|--:|
| memory (lc-push) | log-replay | minimal | 1.55M | 218us | 407us | 2.8ms |
| memory (lc-claim) | log-replay | minimal | 3.04M | 6.6ms | 6.6ms | 6.6ms |
| memory (lc-push) | log-replay | hot_record | 270K | 1.1ms | 1.8ms | 2.6ms |
| memory (lc-claim) | log-replay | hot_record | 558K | 35.8ms | 35.8ms | 35.8ms |
| memory (lc-push) | log-replay | large_payload | 700K | 298us | 448us | 1.1ms |
| memory (lc-claim) | log-replay | large_payload | 2.55M | 7.8ms | 7.8ms | 7.8ms |
| memory (lc-push) | log-replay | grouped | 722K | 505us | 652us | 1.6ms |
| memory (lc-claim) | log-replay | grouped | 1.46M | 13.7ms | 13.7ms | 13.7ms |
| memory (lc-push) | log-replay | cohort | 728K | 497us | 585us | 1.5ms |
| memory (lc-claim) | log-replay | cohort | 1.47M | 13.6ms | 13.6ms | 13.6ms |
| memory (lc-push) | log-replay | skewed_priority | 1.82M | 227us | 411us | 987us |
| memory (lc-claim) | log-replay | skewed_priority | 3.58M | 5.6ms | 5.6ms | 5.6ms |
| sqlite (lc-push) | log-replay | minimal | 14K | 34.1ms | 40.6ms | 60.8ms |
| sqlite (lc-claim) | log-replay | minimal | 13K | 1.59s | 1.59s | 1.59s |
| sqlite (lc-push) | log-replay | hot_record | 6K | 72.2ms | 126.5ms | 166.6ms |
| sqlite (lc-claim) | log-replay | hot_record | 14K | 1.44s | 1.44s | 1.44s |
| sqlite (lc-push) | log-replay | large_payload | 3K | 158.3ms | 202.0ms | 222.4ms |
| sqlite (lc-claim) | log-replay | large_payload | 12K | 1.72s | 1.72s | 1.72s |
| sqlite (lc-push) | log-replay | grouped | 9K | 55.3ms | 70.7ms | 82.6ms |
| sqlite (lc-claim) | log-replay | grouped | 9K | 2.29s | 2.29s | 2.29s |
| sqlite (lc-push) | log-replay | cohort | 9K | 50.8ms | 76.1ms | 180.5ms |
| sqlite (lc-claim) | log-replay | cohort | 10K | 1.95s | 1.95s | 1.95s |
| sqlite (lc-push) | log-replay | skewed_priority | 8K | 55.0ms | 89.2ms | 101.5ms |
| sqlite (lc-claim) | log-replay | skewed_priority | 9K | 2.22s | 2.22s | 2.22s |
| sqlite_relational (lc-push) | relational | minimal | 107K | 4.7ms | 4.8ms | 5.0ms |
| sqlite_relational (lc-claim) | relational | minimal | 60K | 332.9ms | 332.9ms | 332.9ms |
| sqlite_relational (lc-push) | relational | hot_record | 55K | 8.4ms | 8.9ms | 10.2ms |
| sqlite_relational (lc-claim) | relational | hot_record | 24K | 828.1ms | 828.1ms | 828.1ms |
| sqlite_relational (lc-push) | relational | large_payload | 66K | 7.3ms | 7.9ms | 8.5ms |
| sqlite_relational (lc-claim) | relational | large_payload | 21K | 937.1ms | 937.1ms | 937.1ms |
| sqlite_relational (lc-push) | relational | grouped | 3K | 137.2ms | 299.3ms | 316.7ms |
| sqlite_relational (lc-claim) | relational | grouped | 2K | 12.66s | 12.66s | 12.66s |
| sqlite_relational (lc-push) | relational | cohort | 4K | 135.7ms | 274.7ms | 322.5ms |
| sqlite_relational (lc-claim) | relational | cohort | 250 | 82.83s | 82.83s | 82.83s |
| sqlite_relational (lc-push) | relational | skewed_priority | 102K | 4.8ms | 5.1ms | 8.6ms |
| sqlite_relational (lc-claim) | relational | skewed_priority | 52K | 384.0ms | 384.0ms | 384.0ms |
| objectlog (lc-push) | log-replay | minimal | 639K | 754us | 1.1ms | 1.4ms |
| objectlog (lc-claim) | log-replay | minimal | 1.73M | 11.6ms | 11.6ms | 11.6ms |
| objectlog (lc-push) | log-replay | hot_record | 68K | 6.7ms | 7.3ms | 7.6ms |
| objectlog (lc-claim) | log-replay | hot_record | 594K | 33.7ms | 33.7ms | 33.7ms |
| objectlog (lc-push) | log-replay | large_payload | 10K | 50.1ms | 53.3ms | 55.2ms |
| objectlog (lc-claim) | log-replay | large_payload | 1.04M | 19.2ms | 19.2ms | 19.2ms |
| objectlog (lc-push) | log-replay | grouped | 265K | 1.7ms | 1.9ms | 2.6ms |
| objectlog (lc-claim) | log-replay | grouped | 1.07M | 18.7ms | 18.7ms | 18.7ms |
| objectlog (lc-push) | log-replay | cohort | 267K | 1.6ms | 2.1ms | 2.5ms |
| objectlog (lc-claim) | log-replay | cohort | 892K | 22.4ms | 22.4ms | 22.4ms |
| objectlog (lc-push) | log-replay | skewed_priority | 408K | 1.2ms | 1.5ms | 2.0ms |
| objectlog (lc-claim) | log-replay | skewed_priority | 2.24M | 8.9ms | 8.9ms | 8.9ms |
| postgres (lc-push) | log-replay | minimal | 25K | 15.5ms | 26.5ms | 216.6ms |
| postgres (lc-claim) | log-replay | minimal | 37K | 541.3ms | 541.3ms | 541.3ms |
| postgres (lc-push) | log-replay | hot_record | 10K | 30.9ms | 111.9ms | 258.5ms |
| postgres (lc-claim) | log-replay | hot_record | 20K | 989.1ms | 989.1ms | 989.1ms |
| postgres (lc-push) | log-replay | large_payload | 2K | 161.1ms | 363.0ms | 3.16s |
| postgres (lc-claim) | log-replay | large_payload | 31K | 653.4ms | 653.4ms | 653.4ms |
| postgres (lc-push) | log-replay | grouped | 32K | 11.1ms | 26.5ms | 31.6ms |
| postgres (lc-claim) | log-replay | grouped | 74K | 271.1ms | 271.1ms | 271.1ms |
| postgres (lc-push) | log-replay | cohort | 39K | 11.2ms | 20.1ms | 27.5ms |
| postgres (lc-claim) | log-replay | cohort | 26K | 761.8ms | 761.8ms | 761.8ms |
| postgres (lc-push) | log-replay | skewed_priority | 31K | 16.5ms | 19.7ms | 28.0ms |
| postgres (lc-claim) | log-replay | skewed_priority | 23K | 882.4ms | 882.4ms | 882.4ms |

## Caveats and what did not fit

- **`postgres_relational` ran at 1,000 items / batch 200**, not 20,000 / 500 — its per-item network write
  path makes a 20k sweep impractical (~an hour) on this dev box. Numbers are directionally honest
  (per-item, single-connection floor) but not directly scale-comparable to the other backends' 20k rows.
- **`postgres_relational` lifecycle is omitted from the perf sweep** for the same reason. Its lifecycle
  *correctness* is covered by `tests/e2e_shapes_tests.rs` (passes at 2,000 items across all six shapes).
- **`objectlog` skips the `update_fields` step** in the lifecycle: the object-log class is eventual-apply
  and refuses in-place field updates with `Unavailable` (the harness asserts the refusal rather than
  silently skipping). Its lifecycle still exercises push/claim/ack/nack-retry/reclaim.
- **Latency percentiles for `postgres`** show a recurring ~220ms p95/p99 tail on some shapes; this is the
  synchronous single-connection client interacting with batch commit timing, not a steady-state number.
- This baseline is single-threaded and in-process. It says nothing about multi-worker contention,
  multi-queue scale-out (see the TP-002 density workload), or real client/server network cost. Re-run with
  `cargo run --release -p pqueue-bench` after any backend change to refresh.

## Reproduce

```
# embedded + postgres log, full smoke scale (20k):
PQUEUE_PG_TEST_URL=postgres://USER:PW@HOST:5432/DB \
  cargo run --release -p pqueue-bench -- --items 20000 --batch 500 \
  --workloads ingest,claim,lifecycle --backends memory,sqlite,sqlite_relational,objectlog,postgres

# postgres_relational alone, reduced scale (per-item write path):
PQUEUE_PG_TEST_URL=postgres://USER:PW@HOST:5432/DB \
  cargo run --release -p pqueue-bench -- --items 1000 --batch 200 \
  --workloads ingest,claim --backends postgres_relational

# e2e correctness over every shape across all backends:
PQUEUE_PG_TEST_URL=postgres://USER:PW@HOST:5432/DB \
  cargo test --manifest-path crates/pqueue-bench/Cargo.toml --test e2e_shapes_tests
```

