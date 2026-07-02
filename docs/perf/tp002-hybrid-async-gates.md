# TP-002 hybrid-async perf gates + attribution

**Bead:** `pqueue-21d63f09`. **Suite:**
`crates/pqueue-server/tests/performance_object_log_hybrid_tests.rs`.

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

`bounded_debt_ok` requires, over `>= 5` samples:

- `bounded_debt_apply_lag_max <= bounded_debt_apply_lag_ceiling` (bounded), and
- `bounded_debt_last_window_max <= bounded_debt_first_window_max + max(ceiling/4, 64)`
  (non-growing: the last third of the series is not meaningfully above the first
  third).

## AC3 — segment density / object-PUT volume (`segment_density_*`)

The suite already emitted `segments_sealed`, `objects_put`,
`mean_commands_per_segment`, and `max_commands_per_segment`; this bead gates them.

**Documented bounds:**

- Packing bound: no segment can pack more than `target_bytes / MIN_COMMAND_BYTES`
  commands (`MIN_COMMAND_BYTES = 8`). Release evidence for the object-storage log
  must show real packing: `mean_commands_per_segment > 1` and
  `max_commands_per_segment > 1` for normal data-plane traffic. A run with
  `mean == 1` is a release blocker, even if hot-path latency passes, because it
  means the object store is being used as a tiny per-command commit log.
- PUT-volume bound: `objects_put <= 8 × resident` (`OBJECTS_PUT_PER_RESIDENT_MAX`).
  Each resident item drives push/claim/finalize commands and each sealed segment
  writes a bounded number of objects (segment + manifest), so total PUTs are
  `O(resident)`. This catches a PUT storm / one-object-per-command regression.
- `segments_sealed >= 1` and `objects_put >= 1` (something sealed).

If a transactional path cannot safely batch before acknowledgement, that path must
use a local transactional log/checkpoint layer or another non-object-storage log
implementation. It must not force one command per object-storage segment under the
`objectlog/hybrid-async` release profile.

## Running the gates

```text
cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_attribution -- --nocapture
cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_bounded_debt_gate -- --nocapture
cargo test -p pqueue-server --release --test performance_object_log_hybrid_tests \
  performance_object_log_hybrid_segment_density_gate -- --nocapture
```

Each gate test runs the three-profile suite (plus the attribution pipeline) into its
own ledger suite name so it never clobbers the default
`performance_object_log_hybrid_smoke` evidence, then asserts its gate holds, that
the fields were emitted, and that the gate is a required `bars_met` input.
