# TP-002 objectlog/hybrid-async smoke evidence and release lane

**Beads:** `pqueue-1363098f` (original smoke + blocker), `pqueue-81c5c29e`
(release-grade evidence, closed gaps). **Date:** 2026-07-01.
**Suite:** `crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`.

This evidence covers the hybrid-async contract: the `objectlog/hybrid` profile
emitted by the test binary runs its projection apply asynchronously (SQLite may
lag) while success remains legal only after object-log manifest commit plus
synchronous memory apply/render. It compares hybrid with `objectlog/inmemory`
and `objectlog/sqlite` under the same local segmented object-log config. The
smoke lane is release-safe by default and writes a JSONL ledger row when
`PQUEUE_LEDGER_DIR` points at `docs/perf/evidence`; the release lane writes to
`docs/perf/evidence/hybrid-scale`.

Both lanes now emit `bars_met=true` with all gates active. The 10k release lane
that previously failed the hot-path gate (`bars_met=false`) now passes: the ack
p99 ratio dropped from **2.836x → 1.101x** and the claim/finalize p95 ratio
dropped from **33.359x → 1.167x** versus `objectlog/inmemory`, both under the
`<= 1.20` gate. The prior blocker log
(`docs/perf/evidence/performance_object_log_hybrid_release_10m.blocker.log`) is
removed and superseded by the passing release-tier JSONL below.

`objectlog/hybrid-async` has a different success barrier: success is legal only
after object-log manifest commit plus synchronous memory apply/render, while
SQLite projection apply may lag. Async-mode release evidence MUST report the
same hot-path fields plus max/p99 SQLite lag, unknown-outcome retry convergence,
and request_id matrix coverage for push, claim, renew, finalize, retry/release,
update, purge, and operator-style mutations before it can reuse this lane.
It must also report ordered batching evidence: sealed batch sequence ranges,
`sqlite_high_water` after each batch, exactly-once async apply/replay after a
partial batch restart, and proof that readers/claims/metrics are served from
memory while SQLite lags. `sqlite_high_water` is a logical high-water for
applied commands only; SQLite WAL, checkpoint, page-cache, and fsync state are
local durability details and never authorize object-log trimming.
The async evidence row must additionally include lineage validation from
manifest entry to segment checksum/range, command `request_id` fingerprint,
memory `ProjectionImage`, and SQLite `ProjectionImage`; it must report the
retention frontier computed from committed snapshot coverage, active manifest
tail, request_id outcome retention, client item-key retention, and async SQLite
lag. A run that cannot prove retained outcome replay records through
`request_id_retention_ms`, or that advances retention from local SQLite
high-water alone, is not release evidence.

## Smoke command

```text
PQUEUE_LEDGER_DIR="$PWD/docs/perf/evidence" \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_smoke -- --nocapture
```

Result: **pass**, emitted
`docs/perf/evidence/performance_object_log_hybrid_smoke.jsonl`.

## Smoke results

| profile | push/s | ack p50 | ack p95 | ack p99 | claim/finalize p95 | segments | objects PUT | mean commands/segment |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| objectlog/hybrid | 50,760.684 | 1.766 ms | 2.608 ms | 2.608 ms | 0.859 ms | 30 | 60 | 1.0 |
| objectlog/inmemory | 50,118.947 | 2.028 ms | 2.327 ms | 2.327 ms | 0.760 ms | 30 | 60 | 1.0 |
| objectlog/sqlite | 49,059.613 | 1.938 ms | 2.354 ms | 2.354 ms | 2.821 ms | 30 | 60 | 1.0 |

Hybrid ratios:

| comparison | ratio |
|---|---:|
| ack p99 vs objectlog/inmemory | 1.121 |
| claim/finalize p95 vs objectlog/inmemory | 1.130 |
| ack p99 vs objectlog/sqlite | 1.108 |
| claim/finalize p95 vs objectlog/sqlite | 0.305 |

`hybrid_ack_p99_vs_inmemory_ratio`: `1.121` (`<= 1.20`);
`hybrid_claim_finalize_p95_vs_inmemory_ratio`: `1.130` (`<= 1.20`).

The smoke hot-path row passed: ack p99 was 1.121x and claim/finalize p95 was
1.130x `objectlog/inmemory`, both below the 1.200x gate, and the emitted ledger
row has `bars_met=true`. The smoke recovery and disk-loss reconstruction gates
passed (`<= 5s`, `<= 1000` tail commands, exact reconstruction of the resident
count); those recovery paths are exercised at scale in the release lane below.

## Release-tier command

The 10M resident release command is implemented as an ignored test:

```text
PQUEUE_LEDGER_DIR="$PWD/docs/perf/evidence" PQUEUE_PERF_ENV=1 \
  PQUEUE_HYBRID_RESIDENT=10000000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

The 10M lane requires a provisioned perf host; the release lane is validated
here at 10k resident (the capacity of this execution host), with the same gates
active. The 10M command remains the target for a provisioned lane.

Release gate: `objectlog/hybrid` hot path must be within 20% of
`objectlog/inmemory`, normal restart recovery must be `<= 60s`, tail must be
`<= max(10000, 0.1% resident)`, and disk-loss reconstruction must be exact.
`objectlog/hybrid-async` must additionally prove its async SQLite lag bound and
unknown-outcome replay contract before being cited as release evidence. Its
ledger must include ordered batching fields (`batch_sequence`, covered command
range, `sqlite_high_water`, replay count) and must show WAL/fsync/checkpoint
state is not used as a logical high-water or retention authority. It must also
include lineage fields (`manifest_tail`, segment range/checksum,
`request_id_fingerprint_count`, memory image high-water, SQLite image
high-water) plus `retention_frontier` inputs for committed snapshots, active
manifest tail, request_id outcome retention, client item-key retention, and
async SQLite lag. It must record bounded debt/backpressure metrics:
`hybrid_async_apply_debt_bytes`, pending logical batches, oldest unapplied
`batch_sequence`, `sqlite_apply_lag_ms`, replay debt, configured thresholds,
typed backpressure count/duration, and whether admission, high-water, recovery,
or retention advancement failed closed while debt was over budget. Future
`objectlog/hybrid-async` release evidence must also carry the release-lane
performance fields used here: resident count,
hybrid/inmemory hot-path ratios, restart hydrate + tail time, restart pending
count, disk-loss reconstruction wall time, disk-loss pending count, and
`bars_met`.

Raw ledger: `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl`.

## 10k release lane — PASS (closed gaps)

The release-tier lane now passes at 10k resident with all gates active. The
earlier failing run (`pqueue-1363098f`, recorded in the now-removed blocker log)
missed the hot-path gate at `bars_met=false`. After the hot-path fix
(`042287b`, child `pqueue-8f47d542`) and the apply-debt / segment-density /
attribution gates (`c93d991`, child `pqueue-21d63f09`), the same lane passes:

```text
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 \
  PQUEUE_HYBRID_RESIDENT=10000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Result: **pass**, `bars_met=true`, wrote
`docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl`.

Closed hot-path gaps (vs `objectlog/inmemory`, gate `<= 1.20`):

| metric | prior (fail) | now (pass) | gate |
|---|---:|---:|---:|
| hybrid ack p99 ratio | 2.836x | **1.101x** | `<= 1.20` |
| hybrid claim/finalize p95 ratio | 33.359x | **1.167x** | `<= 1.20` |

Full release-lane row at 10k resident:

| metric | value |
|---|---:|
| resident items | 10,000 |
| hybrid push/s | 60,922.583 |
| hybrid ack p50 / p95 / p99 | 1.780 / 2.370 / 2.898 ms |
| hybrid claim/finalize p95 | 1.698 ms |
| inmemory ack p99 / claim-finalize p95 | 2.633 ms / 1.455 ms |
| sqlite ack p99 / claim-finalize p95 | 14.881 ms / 5.732 ms |
| normal restart hydrate + tail | 68.616 ms (`<= 60s` gate) |
| normal restart tail commands | 0 (`<= max(10000, 0.1% resident)`) |
| normal restart pending after | 10,000 |
| disk-loss reconstruction wall | 142.912 ms |
| disk-loss pending after | 10,000 |

### Variance / outlier policy

Latencies are reported as fixed percentiles (p50/p95/p99) over the full drained
hot-path sample, not means, so a single slow syscall cannot move the reported
figure below p99. The hot-path gate compares p99 (ack) and p95
(claim/finalize) ratios against `objectlog/inmemory` measured in the same
process and segment config on the same run, which cancels host-level noise that
would otherwise inflate an absolute-latency threshold. A run is release evidence
only if the gate holds on that run's own paired baseline; we do not average
across runs or drop outlier runs. Ratios must clear `<= 1.20` with margin
(observed 1.101 / 1.167), and every emitted row must carry `bars_met=true`.

### Gate inputs folded into `bars_met`

`bars_met` is the pure conjunction of seven gate inputs (see `compute_bars_met`
in the suite); flipping any one off flips `bars_met` off. This 10k release row
reports each:

| gate | field(s) | 10k value | status |
|---|---|---:|:--:|
| ack p99 hot path | `hybrid_ack_p99_vs_inmemory_ratio` | 1.101 (`<= 1.20`) | pass |
| claim/finalize hot path | `hybrid_claim_finalize_p95_vs_inmemory_ratio` | 1.167 (`<= 1.20`) | pass |
| normal restart recovery | `objectlog_hybrid_recovery_wall_ms` | 68.616 ms (`<= 60s`) | pass |
| disk-loss reconstruction | `objectlog_hybrid_disk_loss_pending_after` | 10,000 (exact) | pass |
| bounded apply-debt | `bounded_debt_apply_lag_max` / `_ceiling` | 242 / 1024 | pass |
| segment density | `objectlog_hybrid_mean_commands_per_segment`, `objects_put` | 1.0, 600 (`<= 80,000` upper) | pass |
| hot-path attribution | `hybrid_attr_phase_sum_ms` vs `total_hot_ms` | 364.369 == 364.369 ms | pass |

Bounded-debt: the async SQLite apply lag stayed non-growing and under its
ceiling (max 242 vs ceiling 1024 across 32 samples), so admission/high-water did
not need to fail closed. Segment-density: 300 segments sealed at mean 1.0
commands/segment with 600 objects PUT, under the `segment_density_objects_put_upper`
bound of 80,000. Attribution: the five hot-path phases (serialize 0.632,
lock_wait 0.003, fsync 333.957, sqlite_apply 28.505, scheduler 1.272 ms) sum to
the measured total hot time (364.369 ms) within tolerance, so the hot path is
accounted for by fsync-dominated object-log commit rather than unattributed
overhead.

This lane proves the hybrid hot path, restart recovery, and disk-loss
reconstruction at 10k resident with every gate green. The full 10M run remains
the target for a provisioned perf host; the command above with
`PQUEUE_HYBRID_RESIDENT=10000000` drives it unchanged.
