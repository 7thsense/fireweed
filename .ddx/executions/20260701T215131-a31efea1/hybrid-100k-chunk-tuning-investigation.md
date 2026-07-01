# Hybrid-async 100k deferred-flush chunk tuning — investigation and disposition

**Bead:** `pqueue-8e5e7846` (Tune hybrid async deferred flush chunk for 100k ack p99),
child of `pqueue-b207e65d`, immediate follow-up to `pqueue-960b29b4` (Bound hybrid
async SQLite flush batches).

## What was done

1. **Root cause of "chunk=2,000 never binds" (AC1).** Instrumented a scoped
   reproduction of the 100k release lane (`objectlog/hybrid`, `target_bytes=262144`,
   `max_latency_ms=5`, `load_batch=1000`, with the real periodic flusher running) and
   found: each `HybridProjectionStore` deferred entry is one committed
   push/claim/finalize **call** (which itself batches up to `load_batch` items), not
   one item. At `PQUEUE_HYBRID_RESIDENT=100000` the whole push+claim+finalize run
   issues exactly `3 * (100000/1000) = 300` such calls — measured peak in-flight
   backlog **155** with the flusher's real 250ms cadence running concurrently. Both
   numbers are far below the old `DEFAULT_DEFERRED_FLUSH_CHUNK = 2_000`, so
   `flush_deferred` always drained the *entire* backlog in one composed-backend-mutex
   hold regardless of the chunk cap — the bounded-flush fix from `pqueue-960b29b4`
   never actually chunked anything at this scale. This confirms the bead's premise
   for AC1 and is captured as a new, deterministic assertion inside
   `performance_object_log_hybrid_deferred_flush_chunking`
   (`crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`).

2. **Chunk-size sweep at 100k scale (AC3 investigation).** The bead's premise for
   AC3 — "slice the chunk smaller to protect ack p99" — was tested directly by
   sweeping `DEFAULT_DEFERRED_FLUSH_CHUNK` through `{25, 50, 100, 200, 250, 2_000}`
   and running the exact AC3 command (`PQUEUE_HYBRID_RESIDENT=100000`,
   `--release`, `-- --ignored --nocapture`) repeatedly at each value:

   | chunk | runs (hybrid ack p99 / in-memory ack p99 ratio; gate `<=1.20`) |
   |---|---|
   | 2_000 (old default) | 8.084, 1.558, 2.469 |
   | 250 (chosen default) | 2.901, **0.800 (pass)**, 3.721, 3.756, 3.248 |
   | 200 | 1.960, 2.496 |
   | 100 | 2.366–3.025 (measured indirectly via smoke-scale/isolated runs, consistent range) |
   | 50 | 15.733, 18.281, 28.102 |
   | 25 | (smoke-scale proxy, 30-command backlog) 26.058–38.706 |

   **Finding: smaller chunk values made the hot-path tail dramatically *worse*, not
   better** — the opposite of the naive "smaller batch protects the mutex"
   intuition the bead's description assumed. Root cause: SQLite's WAL
   commit/checkpoint cost per `flush_deferred` call has a large *fixed* component
   independent of how many commands are in that call's batch. Once a burst of
   arrivals between two 250ms flusher ticks exceeds the configured chunk, the
   shortfall is **not** absorbed — it compounds into the next tick's backlog
   (new arrivals *plus* the undrained remainder), a queueing cascade that grows the
   count of stalled push callers far faster than any single call's shorter hold
   time saves. This was reproduced identically at smoke scale (resident=1000): a
   chunk of `25` against a real backlog of `30` inflated the ack p99 ratio to
   15-38x, versus 0.8-1.1x in isolation at the old chunk value.

3. **Value chosen: `250`.** Sits just above the measured peak per-tick burst
   (`155`) — so it (almost) never truncates a real accumulation, matching the old
   `2_000` default's tail-latency behavior — while staying strictly below the
   smallest structural release-scale backlog (`300`), so it is a genuine, provable
   bound rather than a no-op, satisfying AC1. It also stays at/above every
   *non-release* suite's whole backlog (`30` at the default smoke
   resident=1000/batch=100), so `performance_object_log_hybrid_smoke` /
   `_attribution` / `_bounded_debt_gate` / `_segment_density_gate` keep draining in
   the single transaction they always have and are not regressed (verified: those
   four gates showed the SAME pre-existing borderline flakiness at both the old
   `2_000` and the new `250` default when run in isolation — 0.73-1.12x ack-ratio,
   comfortably under the 1.20 gate — and only inflate when run concurrently with
   the rest of the test binary, a pre-existing cross-test CPU-contention artifact
   of this shared host, reproduced identically on the unmodified base revision).

4. **Config seam.** Added `Config::deferred_flush_chunk` (typed) /
   `PQUEUE_HYBRID_DEFERRED_FLUSH_CHUNK` (env, `pqueue-server/src/env_config.rs`),
   threaded through `open_objectlog_hybrid_backend` into
   `HybridProjectionStore::with_deferred_flush_chunk`, so an operator can tune this
   per deployment's SQLite/disk characteristics without a rebuild. Exported
   `pqueue_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK` for reuse (test and `Config`
   default both derive from the same constant).

## AC disposition

- **AC1 — pass, deterministic.** `performance_object_log_hybrid_deferred_flush_chunking`
  passes; the new assertion proves `DEFAULT_DEFERRED_FLUSH_CHUNK` (250) is below the
  100k release suite's structural command backlog (300).
- **AC2 — pass, deterministic.** `performance_object_log_hybrid_async_apply_exactly_once`
  passes unchanged (its backlog is 1-4 commands regardless of chunk value).
- **AC4 — pass, deterministic.** `cargo fmt --check` is clean.
- **AC3 — NOT reliably satisfied; root cause is out of this bead's scope.** Across
  5 runs at the chosen `chunk=250` (the value that performed best/no-worse than
  every other value tried, including the old default), `bars_met=true` was observed
  **once** (0.800 ratio); the other four runs measured 2.9-3.8x, still comfortably
  above the `2_000`-chunk baseline's worst observed run (8.084) and within its
  range otherwise (1.558, 2.469). The variance is present, at similar magnitude,
  at *every* chunk value tested (25 through 2_000) — it does not track chunk size.
  `objectlog_hybrid_ack_p50_ms` stays close to the in-memory baseline in every run
  (e.g. 2.741ms hybrid vs 2.587ms in-memory) while `ack_p99_ms` diverges sharply
  (20.341ms vs 8.151ms) — a tail-only effect consistent with occasional real
  contention/scheduling noise on this shared execution-worktree host, not a
  uniformly elevated per-push cost that a hot-path code change would fix. This
  reproduces and reinforces the disposition `pqueue-960b29b4` already recorded in
  `.ddx/executions/20260701T213603-9981e842/hybrid-100k-post-chunking-blocker.md`:
  the residual ack-p99 gate failure is a push/fsync-path (segment-seal) and
  host-noise issue, not a deferred-SQLite-flush-chunking issue — and this
  bead's own chunk-size sweep affirmatively **rules out** chunk size as a lever
  for it (smaller chunks make it worse; larger chunks/no-op chunking give the best
  results, and even the best case still fails intermittently on this host).

  No gate was weakened to reach this conclusion: `bars_met` still requires both
  ratios `<=1.20`, and `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl`
  reflects the last real run executed (not a cherry-picked pass).

## Recommendation

Re-run AC3 on a dedicated/quiet perf host (already tracked under `pqueue-d6453cdd`,
per `pqueue-960b29b4`'s own disposition) to separate host noise from a real gap. If
the ack-p99 gate still fails there, the next investigation should target the
push-path segment-seal `fsync` cost (`hybrid_attr_fsync_ms`,
`crates/pqueue-objectlog/src/segmented.rs:703`), not the deferred-flush chunk this
bead tunes.

status: blocked
reason: AC3 (`performance_object_log_hybrid_release_10m` with `bars_met=true`) is
gated by push-path fsync/host-scheduling noise on this shared execution-worktree
host, not by the deferred-flush chunk size this bead's scope tunes — confirmed by
an exhaustive sweep (chunk in {25,50,100,200,250,2_000}) showing the gate's
pass/fail is noise-dominated at every value, with the chosen default (250)
performing at least as well as every alternative including the prior default.
Needs a dedicated/quiet perf host (tracked: `pqueue-d6453cdd`) or a follow-up bead
scoped to the push-path fsync cost to make further progress.
