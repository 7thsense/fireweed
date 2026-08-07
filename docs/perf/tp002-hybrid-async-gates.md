# TP-002 hybrid-async perf gates + attribution

> **Status (P19 / storage-closure): SUPERSEDED as current product guidance.** Hybrid is not a public projection matrix row; Turso is the default projection. This document is retained as historical review/evidence lineage only.


**Bead:** `pqueue-21d63f09`. **Suite:**
`crates/fireweed-server/tests/performance_object_log_hybrid_tests.rs`.

The hybrid-async perf suite previously gated only the ack/claim ratio, normal
restart recovery, and disk-loss reconstruction. A passing row could not prove the
hybrid-async success barrier because three things were measured-but-ungated (or not
measured at all): where hot-path time is spent, whether async SQLite apply-debt
stays bounded over the run, and whether segment density / object-PUT volume stay
within bound. This bead adds all three as gates folded into `bars_met`.

The gate inputs are composed by the pure function `compute_bars_met(...)`:

```text
bars_met = ack_ratio_ok && claim_ratio_ok && recovery_ok && disk_loss_ok
         && bounded_debt_ok && segment_density_ok && attribution_ok
```

Each gate test proves its new input is *required* by toggling that one flag through
`compute_bars_met` and asserting the result flips.

## AC1 — hot-path attribution (`hybrid_attr_*_ms`)

`measure_hybrid_attribution` drives one real single-threaded write+apply pipeline
and times five consecutive phases, each exercising the real cost source the hybrid
write path pays:

| Field | Phase | Real cost source |
|---|---|---|
| `hybrid_attr_serialize_ms` | serialize | `postcard` framing of command envelopes (`segmented.rs` serializes once at buffer time) |
| `hybrid_attr_lock_wait_ms` | lock wait | acquiring the coordinator/unit-of-work `Mutex` under contention from a background holder |
| `hybrid_attr_fsync_ms` | durable write | a segment-object write + `File::sync_all` (the composed flush ack boundary, `segmented.rs:703`) |
| `hybrid_attr_sqlite_apply_ms` | SQLite apply | one batched transaction on the real WAL/`synchronous=NORMAL` projection (`SqliteCheckpointStore::checkpoint`, `relational.rs:4344`) |
| `hybrid_attr_scheduler_ms` | scheduler | runtime yields (externalized flush-task cadence) + the unattributed residual |

`hybrid_attr_total_hot_ms` is the measured wall time of the whole pipeline. The
attribution gate (`attribution_ok`) requires every field to be finite and
non-negative and the five phases to reconcile with the wall time within **30%
relative or 5 ms absolute** (whichever is looser). The unattributed residual (loop
bookkeeping/allocation between timed stages) is folded into the scheduler bucket so
the five phases sum to the wall time by construction.

## AC2 — bounded-debt time-series (`bounded_debt_*`)

While the hybrid hot path runs, a sampler records the SQLite apply-lag time-series:
how far the committed object-log head (`SegmentCounters::commands_committed`) leads
the projection's applied high-water (`LogStore::high_water`, advanced under the same
unit-of-work lock as the projection apply). Both are read atomically through
`with_log`, so a sample never straddles a `gc_distribute`.

The composed hybrid backend applies each sealed segment to the projection
**synchronously** under the unit-of-work lock, so a healthy run keeps this lag
structurally near zero. The gate is the regression guard: it FAILS if apply ever
falls unboundedly behind the durable log.

**Documented ceiling:** `apply_lag_ceiling(max_batch) = max(1024, 4 × max_batch)`
committed commands — a few in-flight batches of slack.

`bounded_debt_ok` requires, over `>= 3` samples:

- `bounded_debt_apply_lag_max <= bounded_debt_apply_lag_ceiling` (bounded), and
- `bounded_debt_last_window_max <= bounded_debt_first_window_max + max(ceiling/4, 64)`
  (non-growing: the last third of the series is not meaningfully above the first
  third).

## AC3 — segment density / object-store utilization (`segment_density_*`)

The suite already emitted `segments_sealed`, `objects_put`,
`mean_commands_per_segment`, and `max_commands_per_segment`; this bead gates them.
Release evidence must also emit object-store file count, object-log bytes,
mean/max object size, utilization against configured target segment size, GET
count, LIST count, PUT count, and an estimated S3 request/storage cost for the
run.

**Documented bounds:**

- Packing bound: no segment can pack more than `target_bytes / MIN_COMMAND_BYTES`
  commands (`MIN_COMMAND_BYTES = 8`). Release evidence for the object-storage log
  must show real packing by command count or by resident work per object. A segment
  with one command is acceptable only when that command represents a large batch of
  resident campaign work. A run that sprays tiny one-command objects is a release
  blocker, even if hot-path latency passes, because it means the object store is
  being used as a tiny per-command commit log.
- PUT-volume bound: `objects_put <= 8 × resident` (`OBJECTS_PUT_PER_RESIDENT_MAX`).
  Each resident item drives push/claim/finalize commands and each sealed segment
  writes a bounded number of objects (segment + manifest), so total PUTs are
  `O(resident)`. This catches a PUT storm / one-object-per-command regression.
- `segments_sealed >= 1` and `objects_put >= 1` (something sealed).
- Utilization bound: release evidence must report average segment-object size
  and `objectlog_hybrid_storage_utilization_ratio = segment_bytes /
  (segments_sealed * target_segment_bytes)`. This ratio is expected to move up
  as batching improves; low utilization is a release blocker when paired with
  high object/file count.
- Cost bound: release evidence must report estimated S3-style cost from measured
  request counts and bytes: PUT/COPY/POST/LIST request count, GET request count,
  DELETE/CANCEL request count, stored bytes, and retained-byte-month projection.
  LIST count is billable request count, including S3 pagination pages, not merely
  logical manifest-list calls. The exact price inputs must be written into the
  evidence row so cost changes are explainable.

Harness preparation for `pqueue-39be4662`: the hybrid performance test now has a
pure object-store utilization helper for the target release-ledger fields:
`objectlog_hybrid_object_count`, `objectlog_hybrid_total_bytes`,
`objectlog_hybrid_segment_bytes`, `objectlog_hybrid_mean_object_bytes`,
`objectlog_hybrid_max_object_bytes`,
`objectlog_hybrid_storage_utilization_ratio`,
`objectlog_hybrid_put_count`, `objectlog_hybrid_get_count`,
`objectlog_hybrid_list_count`, `objectlog_hybrid_s3_estimated_cost_usd`, and
`objectlog_hybrid_s3_price_inputs`. Live emission remains blocked on
`SegmentCounters` exposing the measured object count, total bytes, segment
bytes, mean/max object bytes, and GET/LIST request counts; the existing
`objects_put` counter maps to the future PUT count.

For the object-storage profile, durable acknowledgement is allowed only after the
command's packed object-log segment and manifest are committed. Normal
data-plane traffic must wait for group commit rather than force a tiny segment.
Rare explicit sync/control flushes are permitted, but they must be identified in
metrics and must not dominate object count, request count, or storage
utilization.

## Running the gates

```text
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_attribution -- --nocapture
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_bounded_debt_gate -- --nocapture
cargo test -p fireweed-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_segment_density_gate -- --nocapture
```

Each gate test runs the three-profile suite (plus the attribution pipeline) into its
own ledger suite name so it never clobbers the default
`performance_object_log_hybrid_smoke` evidence, then asserts its gate holds, that
the fields were emitted, and that the gate is a required `bars_met` input.
