# pqueue-81c5c29e — hybrid-async release-grade evidence

Emitted release-grade hybrid-async evidence with all gates active and closed the
open ACs. No harness/gate/fix logic was authored here (children's scope); this
bead ran the matrix and recorded the results.

## AC results

- **AC1** `PQUEUE_LEDGER_DIR=docs/perf/evidence cargo test -p pqueue-server --release
  --test performance_object_log_hybrid_tests performance_object_log_hybrid_smoke
  -- --nocapture` → **pass**, `bars_met=true`. ack p99 1.121x, claim/finalize p95
  1.130x vs inmemory (both `<= 1.20`).
- **AC2** `PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1
  PQUEUE_HYBRID_RESIDENT=10000 cargo test -p pqueue-server --release --test
  performance_object_log_hybrid_tests performance_object_log_hybrid_release_10m
  -- --ignored --nocapture` → **pass**, `bars_met=true`. ack p99 1.101x,
  claim/finalize p95 1.167x vs inmemory.
- **AC3** `rg -n 'bars_met.*true|hybrid_ack_p99_vs_inmemory_ratio|hybrid_claim_finalize_p95_vs_inmemory_ratio'
  docs/perf/evidence docs/perf` → **succeeds**.
- **AC4** blocker log
  `docs/perf/evidence/performance_object_log_hybrid_release_10m.blocker.log`
  removed and superseded by the passing release-tier JSONL;
  `docs/perf/tp002-objectlog-hybrid-evidence.md` documents the closed 10k ack
  (2.836x → 1.101x) and claim/finalize (33.359x → 1.167x) gaps, now `<= 1.20`,
  plus the variance/outlier policy and the bounded-debt / segment-density /
  object-PUT / attribution gates.
- **AC5** `cargo fmt --check` → **pass**.

## Test gate

`cargo test -p pqueue-server` → all 7 binaries ok, 0 failed.

## Closed gaps (10k release lane, vs objectlog/inmemory, gate `<= 1.20`)

| metric | prior (fail) | now (pass) |
|---|---:|---:|
| hybrid ack p99 ratio | 2.836x | 1.101x |
| hybrid claim/finalize p95 ratio | 33.359x | 1.167x |

Enabled by children `pqueue-8f47d542` (hot-path decouple, `042287b`) and
`pqueue-21d63f09` (apply-debt / segment-density / attribution gates, `c93d991`).
The full 10M lane remains the target for a provisioned perf host; validated here
at 10k resident (this host's capacity) with every gate green.
