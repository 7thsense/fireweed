# pqueue-3d5bb3df — hybrid-async perf harness

Closed the gap where the hybrid-async perf suite pushed one uniform payload
through a single sequential producer/consumer at one resident and one repetition.

## Changes

- `crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`
  - `WorkloadGen` (splitmix64, seed=0): pinned non-uniform payload-size,
    bounded client_item_key cardinality, retry- and error-injection distributions.
  - `measure_workload`: N tokio producer/consumer tasks (concurrency sweep) with
    load/claim batch sweep; drains fully and asserts pending invariants.
  - Warm (projection pre-touched) vs cold (fresh open) cache variants.
  - RSS capacity guard (`/proc/meminfo` MemAvailable, env-overridable) that
    skips-with-log any resident over the machine budget.
  - Rep stats: median + coefficient-of-variation under a documented
    trimmed-extremes outlier policy.
- `docs/perf/tp002-hybrid-async-workload.md`: documents seed=0, the distributions,
  the sweep, the scale matrix, reps, CoV, trim policy, and commands.

## AC evidence

| AC | Test / command | Result |
|---|---|---|
| 1 | `performance_object_log_hybrid_distribution_pins` | pass — identical seed=0 sequences, 5 distinct payload sizes |
| 2 | `performance_object_log_hybrid_cache_matrix_smoke` | pass — 8 cells (2 cache × 2 concurrency × 2 batch) |
| 3 | `performance_object_log_hybrid_scale_matrix` | pass — {10k,100k,1M,10M}, 10k runs 5 reps (median+CoV+trim), 100k/1M/10M skip-with-log |
| 4 | `performance_object_log_hybrid_smoke` | pass |
| 5 | `cargo fmt --check` | exit 0 |

## Non-scope (sibling beads)

Live retry/error requeue on the finalize hot path, bounded-debt/segment/attribution
gates, running the release matrix + emitting final docs, and the version bump are
out of scope. The harness records injected retry/error counts as pinned-distribution
evidence per cell.
