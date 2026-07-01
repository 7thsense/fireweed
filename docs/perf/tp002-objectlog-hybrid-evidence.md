# TP-002 objectlog/hybrid smoke evidence and 10M release lane

**Bead:** `pqueue-1363098f`. **Date:** 2026-07-01.
**Suite:** `crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`.

This suite compares `objectlog/hybrid` with `objectlog/inmemory` and
`objectlog/sqlite` under the same local segmented object-log config. The smoke
lane is release-safe by default and writes a strict JSONL ledger row when
`PQUEUE_LEDGER_DIR` points at `docs/perf/evidence`.

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
| objectlog/hybrid | 47,003.927 | 2.107 ms | 3.985 ms | 3.985 ms | 2.137 ms | 30 | 60 | 1.0 |
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

Release gate: `objectlog/hybrid` hot path must be within 20% of
`objectlog/inmemory`, normal restart recovery must be `<= 60s`, tail must be
`<= max(10000, 0.1% resident)`, and disk-loss reconstruction must be exact.

Blocker and raw command output:
`docs/perf/evidence/performance_object_log_hybrid_release_10m.blocker.log`.
