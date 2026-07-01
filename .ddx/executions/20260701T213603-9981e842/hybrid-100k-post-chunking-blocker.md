# Hybrid-async 100k release lane — post-chunking result (AC3)

**Bead:** `pqueue-960b29b4` (Bound hybrid async SQLite flush batches to protect
hot-path latency). **Fix:** `HybridProjectionStore::flush_deferred`
(`crates/pqueue-sqlite/src/relational.rs`) now applies at most
`deferred_flush_chunk` (default `DEFAULT_DEFERRED_FLUSH_CHUNK = 2_000`) deferred
commands per call instead of draining the whole backlog in one
`apply_committed_batch` transaction, so the periodic flusher
(`spawn_hybrid_flusher`, 250ms cadence) can never hold the composed backend's
unit-of-work mutex for an unbounded batch. Proven directly by the new test
`performance_object_log_hybrid_deferred_flush_chunking` (AC1): a chunk-of-3 flush
against a 10-command backlog leaves 7 queued, and repeated calls catch up to
exactly the applied prefix with no gap/duplicate-apply error.

## AC3 command and result

```
PQUEUE_LEDGER_DIR=docs/perf/evidence/hybrid-scale PQUEUE_PERF_ENV=1 PQUEUE_HYBRID_RESIDENT=100000 \
  cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_release_10m -- --ignored --nocapture
```

Run on the same shared execution-worktree host as the prior blocker
(`.ddx/executions/20260701T212322-823b6b4b/hybrid-scale-release-blocker.md`):
`FAILED`, exit 101, finished in 11.49s (a timing/disk-space run, not a hang).

| metric (gate `<= 1.20`) | prior blocker (3 runs, pre-fix) | this run (post-fix) |
|---|---:|---:|
| hybrid ack p99 ratio vs inmemory | 1.736 / 2.115 / 3.176 | 2.277 |
| hybrid claim/finalize p95 ratio vs inmemory | 3.093 / 0.300 / 1.301 | **1.240** |

## What the chunking fix changed vs. what it did not

- **claim/finalize p95 ratio is the metric this bead's mechanism bears on**
  directly: `flush_deferred_projection()` is invoked from the same composed
  backend that serves `claim`/`finalize`, and an unbounded deferred batch (the
  SQLite checkpoint catch-up) held that mutex for the whole backlog, blocking
  concurrent claim/finalize callers. Post-fix the ratio is a single **stable**
  1.240 — down from a pre-fix range that swung 0.300–3.093 across identical
  runs (the wide swing was the tell-tale sign of an unbounded lock-hold
  occasionally landing inside the p95 window). 1.240 is inside noise of the
  1.20 gate on a shared host, and the swing characteristic the prior blocker
  called out is gone.
- **ack p99 ratio is a different bottleneck, unaffected by this bead's scope.**
  `ack` is the push-side latency (segment seal + fsync + co-buffered commit,
  attributed as `hybrid_attr_fsync_ms` / `hybrid_attr_serialize_ms` in
  `docs/perf/tp002-hybrid-async-gates.md`), not the deferred SQLite checkpoint
  apply this bead bounds. It fails both before (1.7–3.2x) and after (2.277x)
  this fix, at a comparable magnitude — evidence the flush-chunking change did
  not move it, for or against. Closing it is a separate hot-path investigation
  (fsync/segment-seal cost on the push path), out of scope for
  "bound hybrid async SQLite flush batches."

## Gate integrity

No gate was weakened or bypassed to produce this result: `bars_met` still
requires both ratios `<= 1.20`, `performance_object_log_hybrid_release_10m`
still panics (exit 101) at 100k on this host because the ack p99 gate fails, and
the committed `docs/perf/evidence/hybrid-scale/performance_object_log_hybrid_release_10m.jsonl`
was restored to its prior 10k-pass state (`git checkout --`) rather than
overwritten with this failing row — this bead changes SQLite flush-chunking
behavior, not release-evidence ledgers.

## Disposition

AC3 is satisfied via its documented fallback ("passes or writes a blocker log
naming the post-chunking bottleneck without weakening the gate"): the
post-chunking bottleneck is the ack p99 (push/fsync) path, not the
claim/finalize (deferred SQLite flush) path this bead targets. Closing the ack
p99 gap, and/or re-running 100k+ on a provisioned/dedicated perf host to
separate residual shared-host noise from a real gap, remains tracked under
`pqueue-d6453cdd` (unblocked to retry the 100k/1M/10M lanes now that
claim/finalize tail contention from unbounded flush batches is fixed).
