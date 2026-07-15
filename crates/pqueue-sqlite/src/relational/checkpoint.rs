use pqueue_core::{ItemId, QueueDefinition, RequestId};
use pqueue_engine::{
    CommandEnvelope, CommandPosition, CreateQueueOutcome, EngineError, EngineResult, QueueKey,
};

use super::*;

// ---------------------------------------------------------------------------
// Async SQLite logical checkpoint store (bead pqueue-16b85e28, backend:objectlog-hybrid-async)
// ---------------------------------------------------------------------------

/// Object-log lineage for one async checkpoint: which committed object-log segment/manifest the durable
/// SQLite logical high-water was advanced from. `source_segment` is an opaque object-log reference (a
/// segment object name or manifest id) stored verbatim — pqueue-sqlite deliberately does NOT depend on
/// pqueue-objectlog types, so lineage crosses the crate boundary as opaque metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointLineage {
    pub source_epoch: u64,
    pub source_segment: String,
}

/// Durable progress recorded by the async SQLite checkpoint worker: the LOGICAL high-water (the next
/// command sequence the projection expects, `relational_cursor.next_seq`) plus the cumulative object-log
/// lineage it was derived from. This is distinct from the PHYSICAL SQLite WAL checkpoint (see
/// [`SqliteCheckpointStore::wal_checkpoint`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointProgress {
    /// `relational_cursor.next_seq`: the next command sequence the projection expects. `None` until the
    /// queue's projection row exists.
    pub logical_high_water: Option<u64>,
    /// Cumulative object-log commands absorbed into the recorded lineage.
    pub applied_commands: u64,
    /// The object-log lineage recorded for the last checkpoint, if any.
    pub lineage: Option<CheckpointLineage>,
}

/// Physical SQLite WAL checkpoint result (`PRAGMA wal_checkpoint`). Deliberately SEPARATE from the logical
/// high-water: this reclaims WAL frames into the main database file; it does NOT advance the logical
/// command cursor or mutate the projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCheckpointStats {
    /// `1` if the checkpoint could not run to completion because another connection held a lock.
    pub busy: i64,
    /// Total frames in the WAL before the checkpoint.
    pub wal_frames: i64,
    /// Frames successfully moved into the database file.
    pub checkpointed_frames: i64,
}

/// The async SQLite **logical checkpoint** store for the `objectlog/hybrid-async` profile.
///
/// The object log is the durability authority; this store is the owner-local restart accelerator. Off the
/// hot request path, the checkpoint worker consumes committed object-log entries IN ORDER and, for each
/// batch, in ONE SQLite transaction: applies every command to the durable projection, persists request-id
/// idempotency/outcome rows so a committed-but-unreturned push converges after restart, records the
/// object-log lineage, and advances the LOGICAL high-water LAST. Because the high-water advances inside the
/// same transaction, a crash mid-checkpoint leaves the cursor behind the object-log head, so the
/// uncommitted tail is replayed (never skipped) on recovery.
///
/// The LOGICAL high-water (which command sequence is durably materialized) is distinct from the PHYSICAL
/// SQLite WAL checkpoint (which reclaims WAL frames): [`Self::wal_checkpoint`] is a storage-file concern
/// that never advances the command cursor.
pub struct SqliteCheckpointStore {
    store: SqliteProjectionStore,
}

impl SqliteCheckpointStore {
    /// Open (or create) a durable checkpoint store at `path`.
    pub fn open(path: &str) -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::open(path)?))
    }

    /// An ephemeral `:memory:` checkpoint store for tests.
    pub fn in_memory() -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::in_memory()?))
    }

    pub fn new(store: SqliteProjectionStore) -> Self {
        Self { store }
    }

    /// The wrapped durable projection store (hot reads / image export for hydration).
    pub fn projection(&self) -> &SqliteProjectionStore {
        &self.store
    }

    /// Create or validate the queue projection metadata this worker checkpoints into.
    pub fn create_queue_projection(
        &self,
        definition: QueueDefinition,
    ) -> EngineResult<CreateQueueOutcome> {
        self.store.create_queue_projection(definition)
    }

    /// Consume a batch of committed object-log entries for `shard` IN ORDER and checkpoint them durably.
    ///
    /// In ONE SQLite transaction: apply each command to the projection, persist request-id
    /// idempotency/outcome rows, upsert the object-log `lineage`, and advance the LOGICAL high-water LAST.
    /// `positions[i]` is the object-log position of `envelopes[i]`; positions MUST be contiguous from the
    /// queue's current logical high-water (an already-applied prefix is skipped idempotently, an
    /// out-of-order position is a hard gap error). Every position MUST belong to `shard`.
    pub async fn checkpoint(
        &self,
        shard: &QueueKey,
        positions: &[CommandPosition],
        envelopes: &[CommandEnvelope],
        lineage: &CheckpointLineage,
    ) -> EngineResult<CheckpointProgress> {
        if positions.len() != envelopes.len() {
            return Err(EngineError::Storage(
                "checkpoint: positions/envelopes length mismatch".into(),
            ));
        }
        let mut g = self.store.inner.lock().expect("projection store poisoned");
        checkpoint_batch_sql(&mut g, shard, positions, envelopes, lineage)
    }

    /// The durable LOGICAL high-water for `shard`: the next command sequence the projection expects
    /// (`relational_cursor.next_seq`). `None` if the queue has no projection row yet. This is the cursor a
    /// restart resumes the object-log tail from — NOT the physical WAL checkpoint.
    pub fn logical_high_water(&self, shard: &QueueKey) -> EngineResult<Option<u64>> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        recovery_high_water_sql(&g.conn, shard)
    }

    /// The recorded checkpoint progress (logical high-water + cumulative object-log lineage) for `shard`.
    pub fn progress(&self, shard: &QueueKey) -> EngineResult<CheckpointProgress> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        let logical_high_water = recovery_high_water_sql(&g.conn, shard)?;
        let lineage = read_checkpoint_lineage_sql(&g.conn, shard)?;
        Ok(CheckpointProgress {
            logical_high_water,
            applied_commands: lineage.as_ref().map(|(_, n)| *n).unwrap_or(0),
            lineage: lineage.map(|(l, _)| l),
        })
    }

    /// Replay the durably persisted push response ids for `(shard, request_id)`, or `None` if no
    /// idempotency row survives (unknown request id or expired). This is the restart-convergence seam: a
    /// same-body retry after a crash returns the original ids without re-appending to the object log.
    pub fn replay_push(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
    ) -> EngineResult<Option<Vec<ItemId>>> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        read_push_replay_sql(&g.conn, shard, request_id)
    }

    /// Run a PHYSICAL SQLite WAL checkpoint (`PRAGMA wal_checkpoint(TRUNCATE)`): reclaim WAL frames into
    /// the main database file. This is a storage-file concern, DELIBERATELY distinct from advancing the
    /// logical high-water — it never changes the command cursor or the projection. A no-op on `:memory:` /
    /// non-WAL databases (reports zero frames).
    pub async fn wal_checkpoint(&self) -> EngineResult<WalCheckpointStats> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        let (busy, wal_frames, checkpointed_frames) =
            st(g.conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }))?;
        Ok(WalCheckpointStats {
            busy,
            wal_frames,
            checkpointed_frames,
        })
    }

    /// Approximate current SQLite **WAL size in bytes**: `wal_frame_count * (page_size + 24)` (the WAL
    /// frame header is 24 bytes). This is the `wal_size` gauge the async-apply debt monitor surfaces
    /// (TD-004 §"Async apply debt": "WAL size where available"). It observes the WAL without truncating it
    /// (a non-truncating `PRAGMA wal_checkpoint(PASSIVE)` reports the frame count). Returns `0` on
    /// `:memory:` / non-WAL databases, which have no WAL file.
    pub fn wal_size_bytes(&self) -> EngineResult<u64> {
        let g = self.store.inner.lock().expect("projection store poisoned");
        let page_size: i64 = st(g.conn.query_row("PRAGMA page_size", [], |row| row.get(0)))?;
        let wal_frames: i64 = st(g
            .conn
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| row.get(1)))
        .unwrap_or(0);
        if wal_frames <= 0 {
            return Ok(0);
        }
        Ok(wal_frames as u64 * (page_size.max(0) as u64 + 24))
    }

    /// The current async SQLite **apply lag in committed commands** for `shard`: how many committed
    /// object-log command sequences are covered by the log but not yet by `sqlite_high_water`. Given the
    /// log's committed head sequence (`log_head_seq`, the highest committed command sequence, `None` for an
    /// empty log), lag `= (log_head_seq + 1) - logical_high_water` (clamped at 0). This is the
    /// `hybrid_async_sqlite_apply_lag` metric (TD-004).
    pub fn apply_lag_commands(
        &self,
        shard: &QueueKey,
        log_head_seq: Option<u64>,
    ) -> EngineResult<u64> {
        let logical_high_water = self.logical_high_water(shard)?.unwrap_or(0);
        let log_next_seq = log_head_seq.map_or(0, |s| s.saturating_add(1));
        Ok(log_next_seq.saturating_sub(logical_high_water))
    }
}
