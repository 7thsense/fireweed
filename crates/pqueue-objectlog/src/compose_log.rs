//! The segmented object-log command-log axis (ADR-012, Phase 1b-i).
//!
//! [`ObjectLog`] wraps the **production** segmented group-commit substrate
//! ([`SegmentedObjectLog`]`<`[`LocalFsBlobStore`]`>`) as a [`pqueue_engine::LogStore`], so the orthogonal
//! [`ComposedBackend`] can assemble an object-log backend as `ObjectLog × InMemoryProjection ×
//! InProcessControlPlane` — the eventual-apply log-replay composition that inherits the shared conformance
//! suite, identical to the monolithic `ObjectLogBackend`.
//!
//! ## Group-commit ack-after-seal inside `LogStore::append`
//!
//! The substrate's durable-commit unit is a SEALED SEGMENT, not a command (TD-004): a command is only acked
//! after its segment's manifest entry is durably committed. The synchronous `LogStore::append` contract
//! requires the committed positions to be returned in order, so `append` buffers the batch and then
//! force-seals it into one segment — the manifest commit is the ack boundary AND the epoch fence (a stale
//! `expected_epoch` is rejected `EpochFenced` before any segment object is written, and the buffer is
//! discarded, so a fenced append commits nothing). The log axis is therefore [`DurabilityClass::EventualApply`],
//! which makes the composition refuse the atomic-only ports (upsert / update_fields / reschedule /
//! commit_transition) — exactly the monolith's capability set.

use pqueue_engine::{
    CommandEnvelope, CommandPage, CommandPosition, ComposedBackend, DurabilityClass, EngineResult,
    InProcessControlPlane, LogStore, ProjectionSnapshot, QueueKey, SnapshotRef,
};
use pqueue_projection::InMemoryProjection;

use crate::segmented::{LocalFsBlobStore, SegmentConfig, SegmentedObjectLog};

/// A large segment target so [`SegmentedObjectLog::enqueue`] never auto-seals mid-`append`; the append path
/// force-seals exactly one segment per call, so the whole batch is one group commit.
const APPEND_TARGET_BYTES: usize = 1 << 30;
/// A large latency budget — the append path force-seals synchronously, so the time trigger never fires.
const APPEND_MAX_LATENCY_MS: u64 = u64::MAX;

/// The segmented object-log command-log axis (ADR-012): the production group-commit substrate over a local
/// filesystem blob store, surfaced as a [`LogStore`].
pub struct ObjectLog {
    log: SegmentedObjectLog<LocalFsBlobStore>,
    config: SegmentConfig,
    /// Whether this axis exposes the [`LogStore`] group-commit facet (ack-after-seal co-buffering). `false`
    /// for [`ObjectLog::open`] — the synchronous force-seal-per-`append` path (every conformance/durability/
    /// reconnect test runs on it, UNCHANGED). `true` for [`ObjectLog::open_group_commit`] — the composition
    /// then co-buffers concurrent pushes into one sealed segment.
    group_commit: bool,
}

impl ObjectLog {
    /// Open (or recover) a segmented object log rooted at `root` on the synchronous force-seal `append` path
    /// (group-commit facet OFF): a large segment target so `enqueue` never auto-seals mid-`append` and a
    /// huge latency budget so the time trigger never fires — `append` force-seals exactly one segment per call.
    pub fn open(root: impl Into<std::path::PathBuf>) -> EngineResult<Self> {
        let store = LocalFsBlobStore::open(root)?;
        let config = SegmentConfig::new(APPEND_TARGET_BYTES, APPEND_MAX_LATENCY_MS)?;
        Ok(Self {
            log: SegmentedObjectLog::open(store, config),
            config,
            group_commit: false,
        })
    }

    /// Open (or recover) a segmented object log rooted at `root` with the ack-after-seal group-commit facet
    /// ON, using the real `config` (byte-size + latency seal triggers). The composition then co-buffers
    /// concurrent pushes into one sealed segment (`gc_enqueue`/`gc_seal`/`gc_flush_due`) and an externalized
    /// flusher seals latency-due segments via the composition's `flush_tick`.
    pub fn open_group_commit(
        root: impl Into<std::path::PathBuf>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let store = LocalFsBlobStore::open(root)?;
        Ok(Self {
            log: SegmentedObjectLog::open(store, config),
            config,
            group_commit: true,
        })
    }

    /// A snapshot of the substrate's measured group-commit segment counters (segments sealed, commands
    /// committed, per-segment batch sizes) — the co-buffering proof surface.
    pub fn counters(&self) -> crate::segmented::SegmentCounters {
        self.log.counters()
    }

    /// Install (or clear, with `None`) a test-only fault hook on the underlying segmented substrate
    /// (TP-003 §3.10 AC-TXN-4). See [`crate::segmented::FaultHook`] / [`crate::segmented::FaultCutPoint`].
    pub fn set_fault_hook(&self, hook: Option<std::sync::Arc<dyn crate::segmented::FaultHook>>) {
        self.log.set_fault_hook(hook);
    }
}

impl LogStore for ObjectLog {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        self.log.ensure_shard(shard)
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.log.current_epoch(shard)
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        // The fence entry's `committed_at_ms` is audit-only; pass 0 (the composition supplies no wall clock).
        self.log.acquire_epoch(shard, 0)
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // Buffer the batch, then force-seal it into one segment (the ack boundary). A stale epoch is fenced at
        // the seal, before any segment object is written, and the buffer is discarded — nothing is acked.
        let out = self.log.enqueue(shard, commands, expected_epoch, 0)?;
        let mut positions = out.committed;
        let sealed = self.log.seal(shard, expected_epoch, 0)?;
        positions.extend(sealed);
        // Advance the durable high-water to the last acked position (the per-commit high-water advance the
        // conformance suite asserts; the explicit `set_high_water` setter is for snapshot truncation).
        if let Some(last) = positions.last() {
            self.log.advance_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }

    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> EngineResult<CommandPage> {
        let from_seq = from.as_ref().map(|p| p.sequence + 1).unwrap_or(0);
        let page_limit = limit.max(1);
        let mut entries = self
            .log
            .read_from_limited(shard, from_seq, page_limit + 1)?;
        let has_more = entries.len() > page_limit;
        entries.truncate(page_limit);
        let next = if has_more {
            // `from`'s contract is "last consumed sequence" (the caller re-adds +1 above), so the
            // resume cursor must carry the LAST RETURNED entry's own sequence, not one past it —
            // otherwise every page boundary silently skips exactly one record (the caller's +1
            // compounds with an extra +1 baked in here).
            entries
                .last()
                .map(|(p, _)| CommandPosition::new(shard.clone(), p.backend_epoch, p.sequence))
        } else {
            None
        };
        Ok(CommandPage { entries, next })
    }

    fn high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        self.log.read_high_water(shard)
    }

    fn set_high_water(&mut self, shard: &QueueKey, position: CommandPosition) -> EngineResult<()> {
        self.log.set_high_water(shard, position)
    }

    fn write_snapshot(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> EngineResult<SnapshotRef> {
        let ref_id = self
            .log
            .write_snapshot(shard, position.clone(), &snapshot.payload)?;
        Ok(SnapshotRef {
            queue: shard.clone(),
            position,
            ref_id,
        })
    }

    fn latest_snapshot(&self, shard: &QueueKey) -> EngineResult<Option<SnapshotRef>> {
        Ok(self
            .log
            .latest_snapshot(shard)?
            .map(|(ref_id, position)| SnapshotRef {
                queue: shard.clone(),
                position,
                ref_id,
            }))
    }

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> EngineResult<Option<SnapshotRef>> {
        Ok(self
            .log
            .snapshot_at_or_before(shard, position)?
            .map(|(ref_id, position)| SnapshotRef {
                queue: shard.clone(),
                position,
                ref_id,
            }))
    }

    fn read_snapshot(&self, snapshot_ref: &SnapshotRef) -> EngineResult<ProjectionSnapshot> {
        Ok(ProjectionSnapshot {
            payload: self
                .log
                .read_snapshot(&snapshot_ref.queue, &snapshot_ref.ref_id)?,
        })
    }

    fn persist_definition(
        &mut self,
        definition: &pqueue_core::QueueDefinition,
    ) -> EngineResult<()> {
        self.log.persist_definition(definition)
    }

    fn recover_definitions(&self) -> EngineResult<Vec<pqueue_core::QueueDefinition>> {
        self.log.recover_definitions()
    }

    // -- group-commit facet: delegate to the substrate's existing &self primitives (ADR-012 P2) -----------

    fn supports_group_commit(&self) -> bool {
        self.group_commit
    }

    fn gc_enqueue(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // Buffer; the substrate seals + returns positions only if a SIZE trigger fired during this enqueue.
        Ok(self
            .log
            .enqueue(shard, commands, expected_epoch, now_ms)?
            .committed)
    }

    fn gc_seal(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.log.seal(shard, expected_epoch, now_ms)
    }

    fn gc_flush_due(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.log.flush_due(shard, expected_epoch, now_ms)
    }

    fn gc_advance_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        self.log.advance_high_water(shard, position)
    }

    fn gc_max_latency_ms(&self) -> u64 {
        self.config.max_latency_ms
    }
}

/// The composed object-log backend: `ComposedBackend<ObjectLog, InMemoryProjection, InProcessControlPlane>`
/// — the eventual-apply log-replay composition (ADR-012 Phase 1b-i), capability-equivalent to the
/// monolithic `ObjectLogBackend`.
pub type ComposedObjectLogBackend =
    ComposedBackend<ObjectLog, InMemoryProjection, InProcessControlPlane>;

/// Assemble a composed object-log backend rooted at `root`. Runs recovery-on-open (ADR-012 P2): a reopen
/// enumerates the durable queue catalog and rebuilds the in-memory projection by replaying the object log.
pub fn composed_objectlog_backend(
    root: impl Into<std::path::PathBuf>,
) -> EngineResult<ComposedObjectLogBackend> {
    ComposedBackend::new(
        ObjectLog::open(root)?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    )
    .recover()
}

/// Assemble a composed object-log backend rooted at `root` with the ack-after-seal GROUP-COMMIT write path
/// ON (ADR-012 P2): concurrent pushes co-buffer into one sealed segment (one durable object + one
/// manifest-CAS + one batched projection apply) instead of force-sealing one segment per append. The caller
/// (which has a runtime) must drive [`ComposedBackend::flush_tick`] on an interval so a buffer below the
/// segment size threshold still acks within ~one latency window. Runs recovery-on-open like
/// [`composed_objectlog_backend`].
pub fn composed_objectlog_backend_group_commit(
    root: impl Into<std::path::PathBuf>,
    config: SegmentConfig,
) -> EngineResult<ComposedObjectLogBackend> {
    ComposedBackend::new(
        ObjectLog::open_group_commit(root, config)?,
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
}
