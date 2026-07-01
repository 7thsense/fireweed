# pqueue-16b85e28 — Build async SQLite checkpoint store

## What shipped

`SqliteCheckpointStore` (`crates/pqueue-sqlite/src/relational.rs`): the off-hot-path async
logical checkpoint worker for the `objectlog/hybrid-async` profile. The object log stays the
durability authority; this store is the owner-local restart accelerator.

`checkpoint(shard, positions, envelopes, lineage)` consumes committed object-log entries **in
order** and, per batch, in **one SQLite transaction** (`checkpoint_batch_sql`):

1. applies every command to the durable projection (idempotently skipping an already-absorbed
   prefix; an out-of-order position is a hard `checkpoint replay gap` error);
2. persists request-id **idempotency/outcome** rows for committed request-id-bearing pushes
   (`persist_request_outcome_sql` → `pqueue_request_idempotency`), so a committed-but-unreturned
   retry converges after restart (`replay_push`);
3. records **object-log lineage** (`pqueue_checkpoint_lineage`: source epoch + opaque
   segment/manifest reference + cumulative applied-command count);
4. advances the **logical high-water LAST** (`relational_cursor.next_seq`), so the cursor can
   never be ahead of the applied projection + persisted lineage; a crash mid-checkpoint replays
   the uncommitted tail.

`wal_checkpoint()` runs `PRAGMA wal_checkpoint(TRUNCATE)` — the **physical** SQLite WAL
checkpoint, deliberately distinct from the logical high-water: it reclaims WAL frames and never
advances the command cursor.

pqueue-sqlite gains no dependency on pqueue-objectlog: lineage crosses the crate boundary as
opaque metadata (`CheckpointLineage { source_epoch, source_segment }`).

## Acceptance evidence

- AC1 `cargo test -p pqueue-sqlite hybrid_async_checkpoint` — 7 tests pass
  (`crates/pqueue-sqlite/tests/hybrid_async_checkpoint.rs`): ordered batches + logical high-water,
  idempotent prefix skip, idempotency rows persisted through high-water, object-log lineage,
  logical-vs-WAL distinction, reopen/rehydrate survival, wrap-existing-store.
- AC2 `cargo test -p pqueue-projection projection_image` — pass (unchanged).
- AC3 `cargo test -p pqueue-sqlite sqlite_projection_image` — pass (unchanged).
- AC4 `cargo fmt --check` — clean.

Also verified: `cargo build --workspace` and `cargo clippy -p pqueue-sqlite --all-targets` clean;
full `pqueue-sqlite` + `pqueue-projection` suites green.

Out of scope (not touched): server runtime configuration.
