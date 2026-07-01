# Add hybrid async backpressure and poison controls (pqueue-6da52695)

## What was implemented

Bounded async-apply **debt metrics**, **backpressure**, and **fail-closed poison** for the
`objectlog/hybrid-async` profile (TD-004 §"Async apply debt, backpressure, and poison thresholds").
Genuine, exported, unit-tested primitives — plus wiring the poison/high-water fail-closed rule into the
composition's recovery-on-open path.

### `pqueue-sqlite` (`crates/pqueue-sqlite/src/relational.rs`)

New library surface for the async SQLite apply pipeline:

- **`HybridAsyncThresholds`** — validated per-queue HARD bounds: `apply_lag_max_commands`,
  `apply_debt_max_bytes`, `apply_queue_depth_max`, `oldest_unapplied_max_ms`, and the
  `apply_poison_retry_threshold`. `new(..)` rejects a zero bound (would be instantly backpressured). The
  normative 75%/50% soft/clear bands are derived from the hard limit.
- **`HybridAsyncDebt`** — a sampled snapshot of the five TD-004 debt metrics (lag commands, debt bytes,
  queue depth, oldest-unapplied age).
- **`BackpressureLevel`** (`Clear`/`Soft`/`Hard`) and **`HybridAsyncMetrics`** — the exported
  observability surface (lag/bytes/depth/age + apply-retry count, cumulative checkpoint errors, WAL size,
  backpressure level/count/duration, poisoned flag).
- **`HybridAsyncMonitor`** — the runtime controller:
  - `observe(debt, now_ms)` folds a debt sample into a typed level with hysteresis — a queue in `Hard`
    only releases once ALL metrics fall below 50% AND an ordered batch has applied cleanly ("Clear
    threshold"); tracks backpressure event count and cumulative Hard duration.
  - `admit_mutation()` — the mutation gate: `Err(Unavailable)` (retryable) under `Hard` backpressure,
    `Err(Storage)` (fail closed) when poisoned. ("Reject or pause mutations when thresholds are
    exceeded before recovery SLOs are violated.")
  - `record_checkpoint_error()` poisons after `apply_poison_retry_threshold` consecutive failures;
    `poison()` latches immediately for non-contiguous/divergent apply. ("Persistent checkpoint errors,
    corruption, or unresolved replay poison must stop serving.")
  - `recovery_high_water_safe()` withholds the lagging `sqlite_high_water` under `Hard`/poison ("high-water
    must not advance past poison"); `retention_may_advance()` gates segment expiry.
- **`SqliteCheckpointStore::wal_size_bytes()`** — WAL-size gauge (`frames * (page_size + 24)`, 0 on
  `:memory:`); **`SqliteCheckpointStore::apply_lag_commands(shard, log_head)`** — the
  `hybrid_async_sqlite_apply_lag` metric.
- **`HybridProjectionStore`** — `poison_reason()`/`mark_poisoned()` accessors and a `recovery_poison`
  override that surfaces its latched poison to the composition recovery gate.

### `pqueue-engine` (`crates/pqueue-engine/src/compose.rs`, `lib.rs`)

- **`RecoveryStart`** + **`resolve_recovery_start(poison, hard_backpressure, high_water)`** — the recovery
  replay-start decision: a poisoned projection **fails closed** (`Err(Storage)`, unresolved replay poison
  stops serving; high-water never advances past poison), a hard-backpressured one replays **from genesis**
  (its lagging high-water is not a safe skip point), and a healthy one trusts its recorded high-water.
- `ProjectionStore::recovery_poison` / `recovery_backpressured` trait seams (safe `None`/`false` defaults),
  wired into `ComposedBackend::run_recovery` so the recorded high-water passes through the poison/backpressure
  gate before any tail replay. Default projections are unchanged (no-op defaults → identical behavior).

### `pqueue-server` (`crates/pqueue-server/src/env_config.rs`, `lib.rs`)

- `Config::hybrid_async: HybridAsyncThresholds`, populated from the `PQUEUE_HYBRID_ASYNC_*` env names
  (`APPLY_LAG_MAX_COMMANDS`, `APPLY_DEBT_MAX_BYTES`, `APPLY_QUEUE_DEPTH_MAX`, `OLDEST_UNAPPLIED_MAX_MS`,
  `APPLY_POISON_RETRY_THRESHOLD`), with library defaults and fail-closed rejection of a zero bound.

## Acceptance evidence

| AC | Command | Result |
|----|---------|--------|
| 1 | `cargo test -p pqueue-server -- env_config hybrid_async --nocapture` † | 13 passed (9 env_config + 4 `hybrid_async_*`) |
| 2 | `cargo test -p pqueue-sqlite hybrid_async_backpressure -- --nocapture` | 13 passed |
| 3 | `cargo test -p pqueue-engine poison -- --nocapture` | 6 passed |
| 4 | `cargo fmt --check` | clean |

† The AC text lists two positional filters (`env_config hybrid_async`); `cargo test` accepts only one
positional before `--`, so the two filters go after `--` (libtest multi-filter form). Both single-filter
forms also pass on their own: `cargo test -p pqueue-server env_config` (13) and
`cargo test -p pqueue-server hybrid_async` (4). The four new `hybrid_async_*` tests live in the
`env_config::tests` module, so both filters select the config-threshold tests.

New tests:
- `crates/pqueue-sqlite/tests/hybrid_async_backpressure.rs` (13) — threshold validation, soft/hard band
  crossings, hysteresis, admission gate, poison-on-retry-threshold + fail-closed, immediate poison,
  recovery-high-water withholding, retention gating, backpressure count/duration, metrics surface, WAL
  size + apply-lag.
- `crates/pqueue-engine/src/compose.rs` `poison_tests` (6) — `resolve_recovery_start` fail-closed on
  poison, high-water-not-past-poison, genesis-replay under hard backpressure, healthy trust, precedence.
- `crates/pqueue-server/src/env_config.rs` (4 `hybrid_async_*`) — default/parsed thresholds, zero-bound
  and zero-poison-threshold rejection.

Regression: `cargo build --workspace` clean; `cargo clippy -p pqueue-engine -p pqueue-sqlite
-p pqueue-server --tests` clean; full `pqueue-engine` / `pqueue-sqlite` / `pqueue-server` suites green.

Out of scope (per bead): performance-matrix evidence, and runtime wiring of the monitor/admission gate
into the live hybrid flusher (a later child of epic pqueue-b207e65d).
