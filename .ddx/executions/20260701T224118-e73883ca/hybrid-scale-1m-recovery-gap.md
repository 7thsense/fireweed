# Hybrid-async 1M/10M release evidence — recovery replay gap blocker

**Bead:** `pqueue-d6453cdd` (Run full hybrid async release evidence), child of `pqueue-b207e65d`.
**Date:** 2026-07-01.

## Summary

AC1 (100k resident) passes cleanly and reproducibly. AC2 (1M resident) fails
with a **deterministic, reproducible correctness bug** in the hybrid-async
normal-restart recovery path — not host noise, not a hardware/disk limit. This
is a genuine data-durability defect: after restart, recovery refuses to
replay because it finds a gap in the object log's committed sequence, which
means either an acknowledged command is unrecoverable after restart, or the
recovery reader is mis-skipping a record that is actually present. Either way
this is a release blocker for the hybrid-async backend, out of scope for this
evidence-collection bead to fix.

## AC1 — 100k resident — PASS

```text
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=100000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Result: **pass**, `bars_met=true`, ack p99 ratio 0.625 (gate `<=1.20`),
claim/finalize p95 ratio 0.918 (gate `<=1.20`), finished in 11.98s. Snapshot:
`docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_100k.jsonl`.

This confirms the fix landed by `pqueue-8e5e7846` (non-blocking deferred
projection flush + owned `apply_live_owned` avoiding a clone on the ack path)
resolved the previously-flaky 100k ack-p99 gate: 3/3 runs across this bead and
`pqueue-8e5e7846`'s closing evidence now pass with comfortable margin, versus
the prior 1/5-pass rate documented in
`.ddx/executions/20260701T215131-a31efea1/hybrid-100k-chunk-tuning-investigation.md`.

## AC2 — 1M resident — FAIL (reproducible correctness bug)

```text
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=1000000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Fails on **every** attempt (3/3 runs, byte-for-byte identical panic each
time):

```text
thread 'performance_object_log_hybrid_release_10m' panicked at
crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs:488:6:
recover hybrid normal restart: Storage("sqlite projection replay gap for
tp002:hybrid-recovery: expected sequence 256, got 257")
```

Backtrace confirms the panic is in the normal-restart recovery reopen inside
`run_hybrid` (test harness `.expect("recover hybrid normal restart")` at
`performance_object_log_hybrid_tests.rs:488`), which calls
`ComposedBackend::recover()` → `run_recovery()` →
`projection.apply_recovery(&positions, &envelopes)` (`compose.rs:1125`), which
hits the hard gap check in `apply_committed_batch_sql`
(`crates/pqueue-sqlite/src/relational.rs:4756-4761`).

### Why this is not a false alarm

The recovery position math on the compose.rs side is correct:
`HybridProjectionStore::recovery_high_water` returns `next_seq - 1` (the last
*applied* position, `crates/pqueue-sqlite/src/relational.rs:7101-7105`), and
`ObjectLog::read_from` (`crates/pqueue-objectlog/src/compose_log.rs:127`)
resumes at `from.sequence + 1`. So if SQLite's cursor says `next_seq=256`
(last applied = 255), recovery correctly asks the log for `seq >= 256`. The
panic means **the log itself returns 257 as the first record at or after
256** — i.e. the durable object log's manifest-driven replay is skipping
sequence 256.

The suspect is the pagination/manifest-ordering interaction in the
segmented, group-commit object log:

- `SegmentedObjectLog::read_manifest` sorts entries by **manifest index**,
  not by sequence (`crates/pqueue-objectlog/src/segmented.rs:596-607`):
  `entries.sort_by_key(|e| e.index)`.
- `SegmentedObjectLog::read_from` (`segmented.rs:881-913`) iterates manifest
  entries in that (index) order and appends every record with `seq >=
  from_seq`, implicitly assuming index order == sequence order.
- `ObjectLog::read_from` (`compose_log.rs:121-139`, the `LogStore` trait impl
  used by the composed backend) then **paginates** that result: it truncates
  to `limit` entries and computes the next resume cursor from
  `entries.last()`'s sequence, i.e. `entries[limit-1].sequence + 1`. The
  compose.rs recovery loop calls this with `limit=256`
  (`crates/pqueue-engine/src/compose.rs:1102`, `log.read_from(&key, from, 256)`).

  **If manifest index order and sequence order can diverge under group-commit
  sealing** (plausible: the composed backend co-buffers concurrent pushes and
  seals segments from a background flusher tick racing the foreground push
  path — see `spawn_composed_flusher`/`gc_distribute`/`try_flush_deferred_projection`
  added by `pqueue-8e5e7846`, commit `9d15873`), then a segment holding a
  lower sequence could be sorted after one holding a higher sequence, so
  truncating the first `256` manifest-ordered entries can silently drop a
  record and/or advance the pagination cursor past a sequence that was never
  actually returned. The observed gap (`expected 256, got 257`, i.e., exactly
  one record short, right at the harness's own `limit=256` boundary) is
  consistent with this failure mode — this is the strongest lead but has not
  been proven with an isolated repro; that is fix-bead work, not
  evidence-collection work.

### Why this doesn't show up at 100k but does at 1M/10M

The recovery push loop for the dedicated `hybrid-recovery` queue only
triggers this if the **background periodic flusher** (250ms tick,
`try_flush_deferred_projection`) gets a chance to partially advance SQLite's
`next_seq` above 0 *before* the push loop finishes and `rec_flusher.abort()`
is called. At 100k resident the whole push completes in a few seconds — often
too fast for a background tick to land — so `recovery_high_water` is `None`
and recovery replays from genesis (a different, apparently-safe code path).
At 1M+ the push loop runs long enough (tens of seconds) for the flusher to
land at least once, producing a non-trivial partial high-water and routing
recovery through the tail-replay/pagination path where the gap appears. The
observed gap position (sequence 256) was **identical across all 3 runs**
(1M, byte-for-byte, deterministic seeded workload with fixed `load_batch`),
consistent with a structural/count-based trigger rather than host-timing
noise.

### AC3 — 10M resident

Same command with `PQUEUE_HYBRID_RESIDENT=10000000`, run in background
(`.output`/log captured under this bundle) since 1M already takes ~63s and
10M exceeds the tool's 2-minute default timeout. Expected to hit the same bug
class (recovery push loop for 10M resident runs long enough to guarantee at
least one background flush). See the companion log for the actual result;
this AC is blocked on the same underlying defect as AC2 regardless of outcome.

## Disposition

This is **not** a hardware/disk blocker (100k passes with comfortable margin,
disk/TMPDIR headroom is 287G, host has 12 cores / 94G RAM). It is a real
correctness defect in the hybrid-async object log's recovery-replay pagination
that only manifests once a release-scale (1M+) resident count gives the
background deferred-flush tick a chance to run during the push phase. Filed
as a new, higher-priority bug bead (see tracker) blocking AC2/AC3 of this
evidence bead. Fixing it is out of this bead's scope (evidence collection
only; "Out of scope: code changes except test harness fixes required by
evidence integrity" — this is a backend correctness fix, not a harness fix).

## Evidence artifacts

- `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_100k.jsonl` — AC1 pass snapshot.
- `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl` — last successful run's raw ledger (100k, since 1M/10M never reach `emit_ledger`).
