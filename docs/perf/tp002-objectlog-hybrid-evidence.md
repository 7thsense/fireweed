# TP-002 objectlog/hybrid-strict smoke evidence and 10M release lane

**Bead:** `pqueue-1363098f`. **Date:** 2026-07-01.
**Suite:** `crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`.

This evidence covers the strict hybrid contract:
`objectlog/hybrid-strict` (the historical `objectlog/hybrid` spelling in the
test binary at the time this row was produced). It compares strict hybrid with
`objectlog/inmemory` and `objectlog/sqlite` under the same local segmented
object-log config. The smoke lane is release-safe by default and writes a strict
JSONL ledger row when `PQUEUE_LEDGER_DIR` points at `docs/perf/evidence`.

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
| objectlog/hybrid-strict | 47,003.927 | 2.107 ms | 3.985 ms | 3.985 ms | 2.137 ms | 30 | 60 | 1.0 |
| objectlog/inmemory | 41,771.761 | 2.111 ms | 3.887 ms | 3.887 ms | 2.355 ms | 30 | 60 | 1.0 |
| objectlog/sqlite | 66,857.371 | 1.862 ms | 2.251 ms | 2.251 ms | 2.986 ms | 30 | 60 | 1.0 |

Hybrid ratios:

| comparison | ratio |
|---|---:|
| ack p99 vs objectlog/inmemory | 1.025 |
| claim/finalize p95 vs objectlog/inmemory | 0.907 |
| ack p99 vs objectlog/sqlite | 1.770 |
| claim/finalize p95 vs objectlog/sqlite | 0.716 |

`hybrid_claim_finalize_p95_vs_inmemory_ratio`: `0.907` (`<= 1.20`).

Smoke recovery:

| metric | value |
|---|---:|
| resident items | 1,000 |
| normal restart hydrate + tail | 5.423 ms |
| normal restart tail commands | 0 |
| normal restart pending after | 1,000 |
| disk-loss reconstruction wall | 101.999 ms |
| disk-loss pending after | 1,000 |

The smoke recovery gate passed (`<= 5s`, `<= 1000` tail commands) and disk-loss
reconstruction was exact for the resident count. The smoke hot-path row passed:
claim/finalize p95 was 0.907x `objectlog/inmemory`, below the 1.200x gate, and
the emitted ledger row has `bars_met=true`.

## Release-tier command

The 10M resident release command is implemented as an ignored test:

```text
PQUEUE_LEDGER_DIR="$PWD/docs/perf/evidence" PQUEUE_PERF_ENV=1 \
  PQUEUE_HYBRID_RESIDENT=10000000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Release gate: `objectlog/hybrid-strict` hot path must be within 20% of
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

Blocker and raw command output:
`docs/perf/evidence/performance_object_log_hybrid_release_10m.blocker.log`.

## 10k release-lane preflight

After the `v0.6.0` release, the release-tier lane was run with a smaller
resident count before attempting the full 10M run:

```text
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 \
  PQUEUE_HYBRID_RESIDENT=10000 PQUEUE_HYBRID_LOAD_BATCH=1000 \
  PQUEUE_HYBRID_CLAIM_BATCH=1000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Result: **fail**. The release-lane correctness checks completed and wrote
`docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl`,
but the test failed its release assertion because `bars_met=false`.

| metric | value |
|---|---:|
| resident items | 10,000 |
| hybrid ack p99 vs objectlog/inmemory | 2.836 |
| hybrid claim/finalize p95 vs objectlog/inmemory | 33.359 |
| normal restart hydrate + tail | 46.948 ms |
| normal restart pending after | 10,000 |
| disk-loss reconstruction wall | 73.986 ms |
| disk-loss pending after | 10,000 |

This run proves restart and disk-loss reconstruction at 10k resident, but it
does **not** prove the hybrid hot path at scale. Do not claim multi-million
hybrid performance until the release-lane hot-path regression is fixed and this
matrix passes at increasing resident counts through the full 10M run.
