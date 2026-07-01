# Wire hybrid async recovery and lineage validation (pqueue-45cbb98e)

## What was implemented

Recovery-on-open for the `objectlog/hybrid-async` profile already hydrated hot memory from the
validated durable SQLite image, replayed the object-log tail beyond `sqlite_high_water`, and
rebuilt request-id outcomes (via `rebuild_push_idempotency_from_log` on the `EventualApply` class).
The genuine gap this bead closes is the **cross-validation of the SQLite lineage against the
object-log's identity, failing closed on mismatch** (TD-004 "Async lineage validation").

### `pqueue-engine` (`crates/pqueue-engine/src/compose.rs`, `lib.rs`)

- New `LogLineageIdentity { shard, current_epoch, high_water }` — the object-log's durable identity
  (namespace / manifest-generation epoch / segment-chain committed head) presented to the projection
  during recovery. Exported from the crate.
- New `ProjectionStore::validate_recovery_lineage(&mut self, &LogLineageIdentity)` trait seam,
  default `Ok` (in-memory / relational projections record no lineage).
- `ComposedBackend::run_recovery` now builds the identity from the `LogStore`
  (`current_epoch` + `high_water`) after the shard is hydrated and BEFORE any tail replay, and calls
  `validate_recovery_lineage`; a failure aborts `recover()` (fail closed).

### `pqueue-sqlite` (`crates/pqueue-sqlite/src/relational.rs`)

- `SqliteProjectionStore::checkpoint_lineage` reads the durably recorded `CheckpointLineage`.
- `HybridProjectionStore::validate_recovery_lineage` fails closed (poisons the projection) when:
  1. the recorded checkpoint lineage's `source_epoch` is NEWER than the log's current epoch (the
     image cannot descend from this log — rolled-back/foreign log, or an image restored over the
     wrong namespace); or
  2. the SQLite logical high-water is AHEAD of the log's committed head (the projection absorbed
     commands the durable log does not contain).
  The lenient direction (recorded epoch older, SQLite high-water behind) is the normal recovery case
  — tail replay catches memory up.

## Acceptance evidence

| AC | Command | Result |
|----|---------|--------|
| 1 | `cargo test -p pqueue-sqlite hybrid_async_recovery -- --nocapture` | 4 passed |
| 2 | `cargo test -p pqueue-objectlog hybrid_request_id -- --nocapture` | 1 passed |
| 3 | `cargo test -p pqueue-conformance objectlog_hybrid -- --nocapture` | 1 passed (real end-to-end recovery test added) |
| 4 | `cargo fmt --check` | clean |

New tests:
- `crates/pqueue-sqlite/tests/hybrid_async_recovery.rs` — projection-side recovery contract:
  hydrate + high-water barrier + happy-path lineage validation + fail-closed on newer epoch +
  fail-closed on SQLite-ahead + no-lineage high-water guard.
- `crates/pqueue-conformance/tests/objectlog_hybrid.rs` — added
  `objectlog_hybrid_recovery_hydrates_replays_tail_and_rebuilds_request_id`, exercising the full
  recovery contract over the REAL `ObjectLog` substrate on reopen.

Regression: full `pqueue-engine` / `pqueue-sqlite` / `pqueue-objectlog` / `pqueue-conformance` suites
green; `cargo build --workspace` and `cargo clippy` clean.
