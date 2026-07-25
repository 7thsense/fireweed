# TP-002 hybrid-async workload harness

**Bead:** `pqueue-3d5bb3df`. **Suite:**
`crates/fireweed-server/tests/performance_object_log_hybrid_tests.rs`.

This document pins the seeded workload generator, cache/concurrency/batch sweep,
and resident scale matrix that let the hybrid-async perf suite emit release-grade
evidence across scale, cache state, concurrency, and workload shape. It closes the
gap where the suite previously pushed one uniform payload through a single
sequential producer/consumer at one resident count and one repetition.

## Seeded workload generator

The generator is a `splitmix64` PRNG (`WorkloadGen`) — the single source of
randomness. No `rand` crate, no wall-clock, no OS entropy, so a workload pinned at
**`seed = 0`** replays byte-for-byte across runs. Each item draws four independent
variates in a fixed order (payload-size, key, retry, error):

| Field | Distribution (pinned, seed=0) |
|---|---|
| `payload_size` | non-uniform: 50% 64 B, 30% 256 B, 15% 1 KiB, 4% 4 KiB, 1% 16 KiB |
| `client_item_key` | uniform over a bounded cardinality of **64** logical keys |
| `inject_retry` | Bernoulli, ~5% (`1/20`) |
| `inject_error` | Bernoulli, ~2% (`1/50`) |

`performance_object_log_hybrid_distribution_pins` asserts two `seed=0` runs produce
identical `(payload_size, key, retry, error)` sequences, that payload sizes are
non-uniform (5 distinct sizes), that keys span the bounded cardinality, that the
retry/error distributions fire, and that a different seed changes the stream.

When the generator drives real pushes, the payload is padded to the drawn size
(so segment/object-log IO reflects the non-uniform distribution) and the
`client_item_key` carries the drawn cardinality bucket but is suffixed with the
global item index so key-dedup never suppresses a resident. The injected
retry/error counts are recorded into each ledger cell as pinned-distribution
evidence; wiring retry/error into the live finalize path is the sibling
"Close hybrid-async hot-path gap" bead.

## Cache × concurrency × batch matrix

`performance_object_log_hybrid_cache_matrix_smoke` runs, at `resident <= 1000`,
the cross product of:

- **cache state** — `cold` (fresh backend open, nothing cached) vs `warm`
  (projection pre-touched by fully loading+draining a warmup queue first);
- **concurrency** — `N` tokio producer tasks over disjoint workload slices, then
  `N` consumer tasks claiming+finalizing until every resident is terminal
  (swept over `{1, 4}`, `FIREWEED_HYBRID_CONCURRENCY`-shaped);
- **batch** — `(load_batch, claim_batch)` swept over `{(50,50), (200,200)}`.

It emits **one ledger cell per `(cache, concurrency, batch)`** — 8 cells — each
recording push/s, ack p50/p95/p99, claim/finalize p95, drain wall, and the
injected retry/error counts + distinct payload sizes for the seeded workload.

## Resident scale matrix, reps, and outlier trim

`performance_object_log_hybrid_scale_matrix` parametrizes resident over
**{10k, 100k, 1M, 10M}** and runs **>= 5 repetitions per release cell**
(`FIREWEED_HYBRID_SCALE_REPS`, min 5).

### Capacity guard

Each scale is gated by an RSS capacity guard: estimated resident RSS
(`EST_BYTES_PER_ITEM = 4096` × resident) must fit the budget, else the scale is
**skipped-with-log**. The budget is `FIREWEED_HYBRID_RSS_BUDGET_BYTES` if set; in the
release lane (`FIREWEED_PERF_ENV=1`) it defaults to 3/4 of `/proc/meminfo`
`MemAvailable`; otherwise a conservative fixed 128 MiB, which admits the 10k scale
and skips 100k/1M/10M so the default lane stays fast. A provisioned perf box runs
the larger scales by raising the budget (or via detected memory in the release
lane).

### Median, coefficient of variation, and trim policy

Per cell, throughput (push/s) and claim/finalize p95 are summarized as **median**
and **coefficient of variation** (CoV = sample stddev / mean) under the documented
outlier-trim policy:

> **trimmed-extremes** — with >= 5 reps, drop the single lowest and single highest
> sample, then compute the median and CoV over the retained samples. With < 5 reps
> no trim is applied.

Each cell records `reps`, `trimmed_reps`, `outlier_trim_policy`,
`push_per_s_median`, `push_per_s_cov`, `claim_finalize_p95_ms_median`, and
`claim_finalize_p95_ms_cov`.

## Commands

```text
# AC1 — distribution pins
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_distribution_pins -- --nocapture

# AC2 — warm/cold × concurrency × batch cells
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_cache_matrix_smoke -- --nocapture

# AC3 — resident scale matrix, >=5 reps, capacity guard
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_scale_matrix -- --nocapture

# Release lane (provisioned box): raise the budget / reps as needed
FIREWEED_PERF_ENV=1 FIREWEED_HYBRID_RSS_BUDGET_BYTES=68719476736 \
  cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_scale_matrix -- --nocapture
```

Ledger rows land under `$FIREWEED_LEDGER_DIR/<suite>.jsonl` (or
`target/fireweed-ledger/<suite>.jsonl`) and are strict-validated by
`fireweed_release::verify_ledger`.
