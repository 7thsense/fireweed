# pqueue-21d63f09 — Hybrid-async perf gates + profile attribution

## What changed

Closed the gap where a passing hybrid-async perf row could not prove the
success barrier: the suite gated only ack/claim ratio, recovery, and disk-loss,
never attributing hot-path time, never bounding async apply-debt, and never
gating segment density / object-PUT volume. All changes are in the test file
(plus a `postcard` dev-dep and a docs page); no hot-path code was touched
(NON-SCOPE: hot-path fix, workload harness, release matrix).

### crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs

- **Attribution (AC1).** `measure_hybrid_attribution` drives a real single-thread
  write+apply pipeline and times five consecutive phases — serialize
  (`postcard` framing), lock_wait (contended coordinator `Mutex`), fsync
  (segment-object write + `File::sync_all`), sqlite_apply (a batched transaction
  on the real WAL/`synchronous=NORMAL` projection via
  `SqliteCheckpointStore::checkpoint`), scheduler (runtime yields + residual).
  Surfaced as `hybrid_attr_{serialize,lock_wait,fsync,sqlite_apply,scheduler}_ms`
  + `hybrid_attr_total_hot_ms`. The five phases reconcile with the measured wall
  time by construction (residual folded into scheduler).
- **Bounded-debt time-series (AC2).** A sampler records the SQLite apply-lag
  series across the hot path (committed object-log head vs projection applied
  high-water, read atomically through `with_log`). Documented ceiling
  `max(1024, 4×max_batch)`. Gate: `>=5` samples, max under ceiling, last-window
  max within `max(ceiling/4, 64)` of first-window max (non-growing).
- **Segment density / object-PUT (AC3).** Gated the already-emitted
  `segments_sealed`/`objects_put`/`mean|max_commands_per_segment`: mean/max
  within the byte-target packing bound (`target_bytes / 8`), object-PUT volume
  bounded to `O(resident)` (`<= 8×resident`), something sealed.
- **Folded into `bars_met` (AC4).** `compute_bars_met(...)` is a pure conjunction
  of all seven gate inputs (four existing + three new). Each new gate test
  proves its input is required by toggling one flag through `compute_bars_met`.
- Three new tests: `performance_object_log_hybrid_attribution`,
  `performance_object_log_hybrid_bounded_debt_gate`,
  `performance_object_log_hybrid_segment_density_gate`. Each runs the suite into
  its own ledger name (never clobbers smoke evidence), asserts its gate holds,
  that the fields were emitted, and that the gate feeds `bars_met`.

### docs/perf/tp002-hybrid-async-gates.md

Documents the three gates, their ceilings/bounds, and the run commands.

### crates/pqueue-server/Cargo.toml + Cargo.lock

Added `postcard` dev-dep so the attribution serialize phase uses the same wire
codec the object-log write path uses.

## Acceptance evidence

- **AC1** `... performance_object_log_hybrid_attribution` → ok. Each
  `hybrid_attr_*_ms` finite, non-negative; five phases sum to wall time within
  tolerance (e.g. serialize=0.284 lock_wait=0.002 fsync=180.653 sqlite_apply=12.232
  scheduler=0.500 sum=193.671 total=193.670).
- **AC2** `... performance_object_log_hybrid_bounded_debt_gate` → ok
  (samples=3556 max=0 ceiling=1024, non-growing; is a `bars_met` input).
- **AC3** `... performance_object_log_hybrid_segment_density_gate` → ok
  (mean=1 max=1 objects_put=60 within documented bounds; feeds `bars_met`).
- **AC4** `rg -n 'hybrid_attr_|bounded_debt|segment_density' <suite>` exits 0;
  emitted ledger keeps `hybrid_ack_p99_vs_inmemory_ratio` (1.33) and
  `hybrid_claim_finalize_p95_vs_inmemory_ratio` (3.503).
- **AC5** `performance_object_log_hybrid_smoke` → ok; `cargo fmt --check` clean.

Full file: `cargo test -p pqueue-server --release --test
performance_object_log_hybrid_tests` → 7 passed, 1 ignored (release-10m).

Pre-existing clippy warnings in `work_spec` / `rss_budget_bytes` are untouched
(out of scope; not introduced by this bead).
