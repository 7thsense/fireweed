# Hybrid-async recovery replay gap — root-caused, fixed, and verified

**Bead:** `pqueue-864b1c74` (Fix hybrid-async recovery replay gap at release scale (1M+)),
child of `pqueue-b207e65d`.
**Date:** 2026-07-01.

## Root cause (proven)

`ObjectLog::read_from` (`crates/pqueue-objectlog/src/compose_log.rs:121-139`, the `LogStore`
impl backing the object-log/hybrid backend) computed its pagination resume cursor one
sequence too far forward:

```rust
let next = if has_more {
    entries.last().map(|(p, _)| CommandPosition::new(shard.clone(), p.backend_epoch, p.sequence + 1))
} else { None };
```

`from`'s contract (enforced by this same function's `let from_seq = from.as_ref().map(|p|
p.sequence + 1).unwrap_or(0);`, and matched by every other position producer, e.g.
`HybridProjectionStore::recovery_high_water` returning `next_seq - 1`) is "the last consumed
sequence" — the CALLER always adds `+1` to resume. By also adding `+1` when building `next`,
every page boundary silently skipped exactly one record. The recovery loop
(`crates/pqueue-engine/src/compose.rs:1092-1131`) pages with `limit=256`, so this only bites
when a single recovery replay spans more than one page — i.e. when the durable tail behind the
projection's recorded high-water exceeds 256 records. At 100k resident the hybrid-async apply
backlog never got that large during the measurement window, so recovery was always a single
page (`page.next == None`) and the bug was invisible; at 1M+ the backlog crosses that
threshold, so recovery always spans 2+ pages and the bug fires deterministically — exactly
matching `.ddx/executions/20260701T224118-e73883ca/hybrid-scale-1m-recovery-gap.md`'s
3/3-reproducible `"expected sequence 256, got 257"` panic.

The manifest/index-ordering hypothesis floated in that investigation doc was checked and ruled
out: `seal()` always derives `first_seq`/`last_seq`/`index` from the same `recover_manifest()`
read (single-writer-per-shard, whole operation under the composed backend's mutex), so index
order and sequence order cannot diverge in this backend.

**Fix:** `crates/pqueue-objectlog/src/compose_log.rs:138` — carry the last returned entry's own
`sequence` into the resume cursor, not `sequence + 1`.

Note: the identical off-by-one pattern (`consumed = start + entries.len()` fed straight into
the next cursor) also exists in `crates/pqueue-sqlite/src/compose_log.rs:225-228` and
`crates/pqueue-postgres/src/compose_log.rs:248-251`. Those `LogStore` axes are unused by the
`object-log/hybrid` composition this bead targets and are out of this bead's scope, but they
have the same latent bug once a single recovery replay for those backends spans more than one
256-record page. Flagged for a follow-up (not filed as a bead here since it is speculative until
someone hits it — no reproduction attempted against those backends).

## AC1 — regression test — PASS

```text
cargo test -p pqueue-server --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_tail_replay_after_partial_sqlite_high_water -- --nocapture
```

New test (`crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`) drives a small
(300-command), fully deterministic hybrid backend through `flush_deferred_projection()`
(bounded chunking, not a background timer) to produce a partial SQLite high-water with a
>256-record un-flushed tail, then reopens and asserts `.recover()` succeeds and every pushed
item survives. Verified this test actually catches the regression: reverting the fix
(`p.sequence` → `p.sequence + 1`) reproduces the exact bug class —
`"expected sequence 266, got 267"` — deterministically and fast (no 1M-item run needed).

## AC2 — 1M resident release evidence — the targeted bug is fixed; a DIFFERENT, unrelated gate now blocks `bars_met=true`

```text
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=1000000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

The exact symptom this bead targets is gone: no more
`Storage("sqlite projection replay gap ...")` panic. Recovery and disk-loss reconstruction both
now complete correctly at 1M resident:

- `objectlog_hybrid_recovery_pending_after: 1000000` (exact match, was previously an
  unreachable panic).
- `objectlog_hybrid_disk_loss_pending_after: 1000000` (exact match).
- `objectlog_hybrid_recovery_wall_ms: 9503.459` (well under the `recovery_bar_ms: 60000` gate).

However the overall suite still reports `"bars_met": false`, this time because
`bounded_debt_ok: false` (`bounded_debt_apply_lag_max: 5999` vs `bounded_debt_apply_lag_ceiling:
2000`) — every other gate (`attribution_ok`, `segment_density_ok`,
`hybrid_within_20pct_inmemory_hot_path`, recovery, disk-loss) passes. This is a previously-masked,
UNRELATED defect: the 1M run never reached the point of measuring bounded debt before (it always
panicked mid-recovery), so this gate has never actually been exercised end-to-end at 1M scale
until this fix landed.

Root-cause lead (not proven, out of this bead's scope per its own "Out of scope: performance
tuning unrelated to this correctness gap"): `pqueue-8e5e7846` (commit `9d15873`) changed the test
harness's background deferred-projection-flush tick from a hardcoded 250ms interval to
`env_u64("PQUEUE_HYBRID_DEFERRED_FLUSH_INTERVAL_MS", 60_000)` — a 240x increase in the default —
specifically to stop that background tick from stealing lock/CPU time during the 100k ack-p99
latency measurement. At 100k resident the whole hot-path measurement finishes well inside that
60s window, so the change is harmless there. At 1M the hot path plausibly runs long enough for
the 60s tick to fire, but `spawn_composed_flusher`'s deferred-tick branch calls
`try_flush_deferred_projection()` — ONE bounded chunk (`DEFAULT_DEFERRED_FLUSH_CHUNK = 250`) per
tick, not a drain loop — so a single catch-up cannot meaningfully reduce a backlog that grows
with resident volume. The bounded-debt ceiling (`apply_lag_ceiling`, 4x batch size, documented
as "the composed hybrid backend applies each sealed segment to the projection synchronously...
so a healthy run keeps this lag structurally near zero") assumes the deferred SQLite checkpoint
stays caught up; at scale, with this cadence, it does not.

Filed as bead `pqueue-e523813a` (child of `pqueue-b207e65d`, dependency of `pqueue-d6453cdd`) to
root-cause and fix the bounded-debt-at-scale gate; it blocks `pqueue-d6453cdd`'s AC2/AC3 exactly
as this bug did, but is a distinct defect from the one this bead was scoped to fix.

## AC3 — `cargo fmt --check` — PASS

## Full test suite

`cargo test -p pqueue-objectlog` and `cargo test -p pqueue-server --test
performance_object_log_hybrid_tests` (all non-ignored tests, including the previously-passing
100k-equivalent smoke/gate tests) — all green, no regressions.

## Evidence artifacts

- `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl` — the 1M run
  discussed above (recovery/disk-loss now correct; `bars_met=false` on the new, unrelated
  bounded-debt gate).
