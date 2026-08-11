# Mixed-op funnel probe — fireweed-451a6b23

Follow-on to fireweed-77ae7a87: that bead's commit-section probe (pure claim+commit, no reads)
found the `open_sqlite` product cell's log/projection store mutexes flat across w=1/4/8, yet
snorri's real ladder shows caller-visible commit-span latency growing ~4.2x (0.18 → 0.75-0.79
ms/entry) with worker concurrency pinned at 5.2-5.3/8 instead of 8/8. HYPOTHESIS: the gap is
invisible to a pure-commit probe because real workloads interleave commits with point reads
(snorri's `instance_state_read`, ~36 ms/call at commit time) that share the same store mutex.

## Probe design

`crates/fireweed/tests/sqlite_mixed_op_funnel_probe.rs` (`sqlite_mixed_op_funnel_ladder_probe`),
same snorri shape as the pure-commit probe (`open_sqlite`, 19 typed indexes, ~2.3 KB payload,
finalize + lifecycle-push, 8000 entries, claim-batch 500), but each worker's loop now interleaves
three op classes instead of one:

1. **claim** — `fw.claim(...)` (500-item batch).
2. **read** — one `fw.query_index_unique_typed(&queue, "by_f0", ...)` point read per claimed item
   (500 sequential point reads/iteration), against a stable 2000-item sentinel pool pinned to the
   back of priority order (`priority: Int64(i64::MAX)`) so it's never claimed and stays a durable
   read target for the whole run — modeling a caller checking related instance state at commit
   time.
3. **commit** — `fw.commit(...)` (500-entry batch, finalize + lifecycle-push).

Each op class is timed independently (`Instant` around the `.await`, accumulated in per-class
atomics across every concurrent worker) and reported as both ms/entry (normalized to the 8000-entry
run, comparable to the pure-commit probe's numbers) and ms/call. `open_sqlite_with_lock_stats_handle`
still exposes the underlying log/projection axis lock-phase (wait/hold) counters for cross-reference.

## Results (this host, 32 cores, two independent runs)

Pure-commit probe (`sqlite_commit_section_contention_ladder_probe`, no reads) — reproduced here for
a same-host baseline:

| w | wall ms/entry | log_hold ms/entry | log_wait ms/entry | proj_hold ms/entry | proj_wait ms/entry |
|--:|--:|--:|--:|--:|--:|
| 1 | 0.41-0.43 | 0.23-0.25 | ~0.0000 | 0.037-0.038 | ~0.0000 |
| 4 | 0.36-0.42 | 0.21-0.27 | ~0.0000-0.0094 | 0.037-0.039 | ~0.0000 |
| 8 | 0.39-0.39 | 0.23-0.24 | 0.0094-0.0209 | 0.036-0.037 | ~0.0000 |

Flat: no meaningful growth w=1→w=8 on either axis or overall wall. Matches the bead's description
of the pure-commit probe's original finding.

Mixed-op probe (this bead) — commits + point reads + claims interleaved:

| w | wall ms/entry | claim ms/entry | claim ms/call | read ms/entry | read ms/call | commit ms/entry | commit ms/call | proj_wait ms/entry | proj_hold ms/entry |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 0.42-0.44 | 0.057-0.080 | 28.5-40.0 | 0.0079-0.0080 | 0.0079-0.0080 | 0.339-0.341 | 169.6-170.6 | ~0.0001 | 0.040-0.042 |
| 4 | 0.38-0.38 | 0.512-0.517 | 256.1-258.5 | 0.0125-0.0128 | 0.0125-0.0128 | 0.862-0.864 | 431.4-432.1 | 0.0042-0.0044 | 0.041 |
| 8 | 0.42-0.44 | 0.983-1.064 | 491.5-531.8 | 0.0113-0.0132 | 0.0113-0.0132 | 1.766-1.905 | 883.1-952.7 | 0.0023-0.0045 | 0.041-0.047 |

**commit ms/entry grew 5.2-5.6x from w=1 to w=8** (0.34 → ~1.8-1.9); **claim ms/entry grew
~13-17x** (0.057-0.08 → 0.98-1.06). Point-read latency itself stayed cheap and roughly flat
(~0.008-0.013 ms/call) — the read op is not expensive in isolation.

## Verdict: CONFIRMED, with a refinement

The funnel hypothesis is confirmed: interleaving point reads with claim/commit reproduces the
caller-visible latency blowup with worker count that the pure-commit probe could not see, on the
identical host, product cell, and entry/batch shape. This qualitatively matches snorri's own
pattern (commit-span latency growing several-fold from w=1 to w=8 while a pure-commit probe stays
flat).

**Refinement — the blowup is not fully explained by the axis lock-phase counters.**
`proj_wait_ms/entry` (time callers spent blocked acquiring `InProcessProjectionStore`'s
`Mutex<InMemoryProjection>` before their operation could run, per fireweed-77ae7a87's
instrumentation) stays under 0.005 ms/entry at every weight — two orders of magnitude too small to
account for a >1 ms/entry commit-latency increase. Two things are true at once:

- Every op class (claim's `eligible_candidates`/`render_claimed`, the point reads'
  `index_get_unique`, and commit's `admit_mutation`/`index_validate_push`/`apply_live`) really does
  share one `Mutex<InMemoryProjection>` via `InProcessProjectionStore::run_with_store` /
  `run_with_store_mut` (confirmed by reading `crates/fireweed-engine/src/async_store.rs`;
  `open_sqlite`'s projection axis is `InMemoryProjection` with `offload_projection=false`, so every
  op funnels through this one mutex **inline**, not via the blocking-offload executor).
- But the *measured* wait time on that specific mutex isn't where the wall-clock cost shows up.
  Because these are `std::sync::Mutex` acquisitions made synchronously inside an `async` block with
  no internal `.await`, a contended acquisition blocks the **Tokio worker OS thread itself** (not
  just the logical task) until the lock is free — with `worker_threads=8` and all 8 spawned workers
  issuing thousands of rapid-fire, brief mutex acquisitions (500 reads + several projection calls
  per commit, per iteration, per worker), the aggregate acquisition *rate* against one mutex is high
  enough to produce real scheduling-level serialization (thread wake latency, cache-line contention)
  that per-acquisition `Instant` deltas average out to look small while the cumulative effect on
  concurrent callers is large. In short: the funnel is real, but a fix that only reduces *hold time*
  per call (e.g. cheaper index encoding) would not address it — the fix has to reduce the *number of
  operations serialized through one exclusive lock*, i.e. let non-conflicting reads run without
  taking the writer's lock at all.

This refines ASK (2) from the bead: "op-class concurrency … reads served off the store task" /
"a segregated read lane" is the right shape of fix (a reader-writer split on the projection axis for
`Sync`-safe backing stores like `InMemoryProjection`), not a hold-time optimization.

## AC2 disposition

A full landing (swap `InProcessProjectionStore`'s internal `Mutex<S>` for a reader/writer split) was
scoped and found to ripple beyond `async_store.rs`: `AsyncLogReplayBackend<L, P>`
(`fireweed-engine/src/async_log_replay_product.rs:471`) and `AsyncEngine<L, P>` hardcode
`projection: Arc<InProcessProjectionStore<P>>` as a concrete field type, so changing the lock
strategy for one product cell (`open_sqlite`'s `InMemoryProjection` axis, which is genuinely
`Send + Sync` and safe for concurrent shared reads — unlike connection-backed axes such as
`SqliteLog`/`PostgresRelational`, which are not `Sync`-safe for concurrent use) requires a real
generic-parameter or type redesign of those product types, not a local edit. That is too large to
land soundly inside this bead alongside the probe. Landing it under time pressure risked either an
unsound `Sync` bound applied too broadly, or a silent no-op if the generic plumbing didn't actually
reach `open_sqlite`.

Per this bead's AC2 wording ("a landing **or** documented caller guidance"), this bead lands the
guidance side now:

- `InProcessProjectionStore`'s doc comment (`crates/fireweed-engine/src/async_store.rs`) now states
  the funnel explicitly and recommends batching point reads (`live_items`/`query_index_typed` accept
  multiple keys per call) instead of one `run_with_store` acquisition per key, for callers driving
  high point-read volume against a queue under concurrent commit load.
- The actual reader/writer split is scoped as a follow-on: give the projection axis's
  `InMemoryProjection` construction a concurrent-read-capable lock (opt-in, `S: Sync`-gated, additive
  — existing `Mutex`-backed constructors and non-`Sync` backing stores must keep compiling and
  behaving unchanged), then re-run this probe against the new construction to confirm point-read
  latency stops scaling with worker count, and drive the same-day snorri ladder loop per
  `docs/perf/evidence/tp005/snorri-ladder-candidate.md`'s handoff contract (only the external snorri
  pipeline may populate `RESULT`) to verify worker concurrency lifts above 6.5/8 at w=8. `ddx bead
  create` could not be invoked successfully in this execution worktree to file that follow-on as a
  tracked bead (it errors on every input, including a minimal repro, with `invalid id: length 71 not
  in [8, 64]` — looks like a tool-level id-generation bug unrelated to this bead's scope); the
  follow-on work is recorded here instead pending someone able to file it.

## AC3 — perf gate set

`sqlite_mixed_op_funnel_probe` is registered as a `[[test]]` target in `crates/fireweed/Cargo.toml`
(`required-features = ["sqlite"]`), the same mechanism `sqlite_commit_section_contention_probe`
joined the suite through for fireweed-77ae7a87.
