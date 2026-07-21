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
    CommandEnvelope, CommandPage, CommandPosition, ComposedBackend, DefinitionCursor,
    DefinitionPage, DetachedLogMaintenance, DetachedRetentionOutcome, DetachedRetentionRequest,
    DetachedTrimWatermark, DurabilityClass, EngineError, EngineResult, InProcessControlPlane,
    LogStore, ProjectionSnapshot, QueueKey, SnapshotRef,
};
use pqueue_projection::InMemoryProjection;

use std::sync::Arc;

use crate::segmented::{
    BlobStore, LocalFsBlobStore, SegmentConfig, SegmentedObjectLog, SerializedCommandEnvelope,
};

fn maintenance_summary(
    report: crate::maintenance::MaintenanceReport,
    count_completed_as_orphan_branches: bool,
) -> pqueue_engine::MaintenanceSummary {
    let stopped_by = match report.stopped_by {
        Some(crate::maintenance::MaintenanceExecutionReason::BudgetExhausted) => {
            Some(pqueue_engine::MaintenanceStopReason::BudgetExhausted)
        }
        Some(crate::maintenance::MaintenanceExecutionReason::EpochChanged) => {
            Some(pqueue_engine::MaintenanceStopReason::EpochFenced)
        }
        Some(crate::maintenance::MaintenanceExecutionReason::RetryableFailure) => {
            Some(pqueue_engine::MaintenanceStopReason::RetryableFailure)
        }
        Some(crate::maintenance::MaintenanceExecutionReason::PermanentFailure) => {
            Some(pqueue_engine::MaintenanceStopReason::PermanentFailure)
        }
        Some(
            crate::maintenance::MaintenanceExecutionReason::CommittedBranch
            | crate::maintenance::MaintenanceExecutionReason::Filtered
            | crate::maintenance::MaintenanceExecutionReason::InFlightWriterGrace,
        )
        | None => None,
    };
    pqueue_engine::MaintenanceSummary {
        scanned: report.scanned as u64,
        retained: report.retained as u64,
        objects_deleted: report.deleted as u64,
        bytes_deleted: report.bytes_deleted,
        object_requests: report.requests as u64,
        retryable_failures: report.retryable_failures as u64,
        permanent_failures: report.permanent_failures as u64,
        fenced: report.fenced,
        cursor_pending: report.cursor.is_some(),
        stopped_by,
        orphan_branches_reclaimed: if count_completed_as_orphan_branches {
            report.completed_candidates as u64
        } else {
            0
        },
    }
}

/// Convert a command envelope's `created_at` to epoch-milliseconds (bead pqueue-b5cc2bc7 bug 1): the raw
/// append path stamps a segment's `committed_at_ms` from the max of these so `created_at <= committed_at_ms`
/// holds for the retention-floor trim. Mirrors `pqueue_engine`'s internal `ts_to_ms`.
fn ts_to_ms(ts: pqueue_core::UtcTimestamp) -> i64 {
    ts.seconds
        .saturating_mul(1000)
        .saturating_add((ts.nanoseconds / 1_000_000) as i64)
}

/// A large segment target so [`SegmentedObjectLog::enqueue`] never auto-seals mid-`append`; the append path
/// force-seals exactly one segment per call, so the whole batch is one group commit.
const APPEND_TARGET_BYTES: usize = crate::segment_integrity::MAX_SEGMENT_BYTES;
/// A large latency budget — the append path force-seals synchronously, so the time trigger never fires.
const APPEND_MAX_LATENCY_MS: u64 = u64::MAX;

/// The segmented object-log command-log axis (ADR-012): the production group-commit substrate over a local
/// filesystem blob store, surfaced as a [`LogStore`].
pub struct ObjectLog {
    log: Arc<SegmentedObjectLog<Arc<dyn BlobStore>>>,
    config: SegmentConfig,
    durability_class: DurabilityClass,
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
        let store: Arc<dyn BlobStore> = Arc::new(LocalFsBlobStore::open(root)?);
        let config = SegmentConfig::new(APPEND_TARGET_BYTES, APPEND_MAX_LATENCY_MS)?;
        Ok(Self {
            log: Arc::new(SegmentedObjectLog::open(store, config)),
            config,
            durability_class: DurabilityClass::EventualApply,
            group_commit: false,
        })
    }

    /// Open the synchronous force-seal path over a caller-selected production blob store.
    pub fn open_with_blob_store(store: Arc<dyn BlobStore>) -> EngineResult<Self> {
        let config = SegmentConfig::new(APPEND_TARGET_BYTES, APPEND_MAX_LATENCY_MS)?;
        Ok(Self {
            log: Arc::new(SegmentedObjectLog::open(store, config)),
            config,
            durability_class: DurabilityClass::EventualApply,
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
        let store: Arc<dyn BlobStore> = Arc::new(LocalFsBlobStore::open(root)?);
        Self::open_group_commit_with_blob_store(store, config)
    }

    /// Open the group-commit path over a caller-selected production blob store.
    pub fn open_group_commit_with_blob_store(
        store: Arc<dyn BlobStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Ok(Self {
            log: Arc::new(SegmentedObjectLog::open(store, config)),
            config,
            durability_class: DurabilityClass::EventualApply,
            group_commit: true,
        })
    }

    /// Open the group-commit path as an authoritative embedded log axis.
    ///
    /// The write mechanics stay the same as [`ObjectLog::open_group_commit_with_blob_store`], but the
    /// returned durability class is [`DurabilityClass::Atomic`] so an embedded strict composition can
    /// truthfully advertise the atomic commit-transition surface only after recovery and replay are
    /// exercised by the public embedded tests.
    pub fn open_group_commit_authoritative_with_blob_store(
        store: Arc<dyn BlobStore>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        Ok(Self {
            log: Arc::new(SegmentedObjectLog::open(store, config)),
            config,
            durability_class: DurabilityClass::Atomic,
            group_commit: true,
        })
    }

    /// Open (or recover) the authoritative embedded group-commit path rooted at `root`.
    pub fn open_group_commit_authoritative(
        root: impl Into<std::path::PathBuf>,
        config: SegmentConfig,
    ) -> EngineResult<Self> {
        let store: Arc<dyn BlobStore> = Arc::new(LocalFsBlobStore::open(root)?);
        Self::open_group_commit_authoritative_with_blob_store(store, config)
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

    /// Create a copy-on-write branch of `source` cut at `position` on the underlying substrate (pins the
    /// source segments at/below the cut against retention trimming while the branch is live). Passthrough to
    /// [`SegmentedObjectLog::branch`] — the object log's branching capability, surfaced on the adapter.
    pub fn branch(
        &self,
        source: &QueueKey,
        branch_def: &pqueue_core::QueueDefinition,
        position: &CommandPosition,
        ttl_ms: u64,
        now_ms: i64,
    ) -> EngineResult<u64> {
        self.log
            .branch(source, branch_def, position, ttl_ms, now_ms)
    }

    /// Discard a branch and release its retention pins. Passthrough to [`SegmentedObjectLog::discard_branch`].
    pub fn discard_branch(&self, source: &QueueKey, branch: &QueueKey) -> EngineResult<()> {
        self.log.discard_branch(source, branch)
    }

    pub(crate) fn shared_ensure_shard(&self, shard: &QueueKey) -> EngineResult<()> {
        self.log.ensure_shard(shard)
    }

    pub(crate) fn shared_acquire_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.log.acquire_epoch(shard, 0)
    }

    pub(crate) fn shared_append(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let seal_ms = commands
            .iter()
            .map(|env| ts_to_ms(env.created_at))
            .max()
            .unwrap_or(0);
        let out = self.log.enqueue(shard, commands, expected_epoch, seal_ms)?;
        let mut positions = out.committed;
        positions.extend(self.log.seal(shard, expected_epoch, seal_ms)?);
        if let Some(last) = positions.last() {
            self.log.advance_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }

    pub(crate) fn shared_set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        self.log.set_high_water(shard, position)
    }

    pub(crate) fn shared_is_group_commit(&self) -> bool {
        self.group_commit
    }

    pub(crate) fn shared_segment_target_bytes(&self) -> usize {
        self.config.target_bytes
    }

    pub(crate) fn shared_segment_writer_format(&self) -> crate::SegmentWriterFormat {
        self.config.writer_format()
    }

    pub(crate) fn shared_pending(&self, shard: &QueueKey) -> usize {
        self.log.pending(shard)
    }

    pub(crate) fn shared_gc_enqueue(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        Ok(self
            .log
            .enqueue(shard, commands, expected_epoch, now_ms)?
            .committed)
    }

    pub(crate) fn shared_gc_seal(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        self.log.seal(shard, expected_epoch, now_ms)
    }

    pub(crate) fn shared_gc_flush_due(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        self.log.flush_due(shard, expected_epoch, now_ms)
    }

    pub(crate) fn shared_gc_advance_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        self.log.advance_high_water(shard, position)
    }

    pub(crate) fn shared_gc_enqueue_and_advance(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let positions = self.shared_gc_enqueue(shard, commands, expected_epoch, now_ms)?;
        if let Some(last) = positions.last() {
            self.log.set_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }

    pub(crate) fn shared_gc_enqueue_serialized_and_advance(
        &self,
        shard: &QueueKey,
        commands: Vec<SerializedCommandEnvelope>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        let positions = self
            .log
            .enqueue_serialized(shard, commands, expected_epoch, now_ms)?
            .0
            .committed;
        if let Some(last) = positions.last() {
            self.log.set_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }

    pub(crate) fn shared_gc_seal_and_advance(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let positions = self.shared_gc_seal(shard, expected_epoch, now_ms)?;
        if let Some(last) = positions.last() {
            self.log.set_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }

    pub(crate) fn shared_gc_enqueue_seal_and_advance(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let mut positions =
            self.shared_gc_enqueue_and_advance(shard, commands, expected_epoch, now_ms)?;
        positions.extend(self.shared_gc_seal_and_advance(shard, expected_epoch, now_ms)?);
        Ok(positions)
    }

    pub(crate) fn shared_gc_enqueue_serialized_seal_and_advance(
        &self,
        shard: &QueueKey,
        commands: Vec<SerializedCommandEnvelope>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let mut positions =
            self.shared_gc_enqueue_serialized_and_advance(shard, commands, expected_epoch, now_ms)?;
        positions.extend(self.shared_gc_seal_and_advance(shard, expected_epoch, now_ms)?);
        Ok(positions)
    }

    pub(crate) fn shared_gc_flush_due_and_advance(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let positions = self.shared_gc_flush_due(shard, expected_epoch, now_ms)?;
        if let Some(last) = positions.last() {
            self.log.set_high_water(shard, last.clone())?;
        }
        Ok(positions)
    }
}

struct ObjectLogMaintenance {
    log: Arc<SegmentedObjectLog<Arc<dyn BlobStore>>>,
}

impl ObjectLogMaintenance {
    fn fenced_outcome(expected_epoch: u64) -> DetachedRetentionOutcome {
        let mut summary = pqueue_engine::MaintenanceSummary::default();
        summary.fenced = true;
        summary.stopped_by = Some(pqueue_engine::MaintenanceStopReason::EpochFenced);
        DetachedRetentionOutcome {
            expected_epoch,
            summary,
            watermark: DetachedTrimWatermark::Clear,
        }
    }
}

impl DetachedLogMaintenance for ObjectLogMaintenance {
    fn execute_retention(
        &self,
        request: DetachedRetentionRequest,
    ) -> EngineResult<DetachedRetentionOutcome> {
        if self.log.maintenance_owner_epoch(&request.shard) != Some(request.expected_epoch)
            || self.log.current_epoch(&request.shard)? != request.expected_epoch
        {
            return Ok(Self::fenced_outcome(request.expected_epoch));
        }

        let mut summary = pqueue_engine::MaintenanceSummary::default();
        let mut watermark = DetachedTrimWatermark::Unchanged;
        let durable_floor = self.log.read_retention_floor(&request.shard)?;

        // Finish a crash-interrupted delete before attempting a new floor. The durable floor is already the
        // safety barrier, so this replay is idempotent and remains owner-fenced inside the segmented substrate.
        if let Some(floor) = &durable_floor
            && request
                .completed_through
                .is_none_or(|completed| completed < floor.sequence)
        {
            let pass = self.log.expire_segments_through_bounded_default(
                &request.shard,
                floor.sequence,
                request.now_ms,
            )?;
            let complete = pass.cursor.is_none() && pass.stopped_by.is_none();
            summary.merge(maintenance_summary(pass, false));
            if !complete {
                return Ok(DetachedRetentionOutcome {
                    expected_epoch: request.expected_epoch,
                    summary,
                    watermark,
                });
            }
            // The bounded expiry pass pages the complete pin registry and remains incomplete while a live
            // branch blocks the target, so its completion is the pin proof carried into finalization.
            watermark = DetachedTrimWatermark::Set(floor.sequence);
        }

        if !request.allow_floor_advance {
            return Ok(DetachedRetentionOutcome {
                expected_epoch: request.expected_epoch,
                summary,
                watermark,
            });
        }
        let Some(checkpoint) = request.checkpoint else {
            return Ok(DetachedRetentionOutcome {
                expected_epoch: request.expected_epoch,
                summary,
                watermark,
            });
        };
        let Some(time_expired_seq) = self
            .log
            .max_trimmable_seq_before(&request.shard, request.cutoff_ms)?
        else {
            return Ok(DetachedRetentionOutcome {
                expected_epoch: request.expected_epoch,
                summary,
                watermark,
            });
        };
        let trim_through = checkpoint.sequence.min(time_expired_seq);
        let floor_position = CommandPosition::new(
            request.shard.clone(),
            checkpoint.backend_epoch,
            trim_through,
        );

        // The manifest CAS is the crash barrier and owner fence. No segment object is deleted unless this
        // owner durably published (or idempotently observed) the floor first.
        match self.log.advance_retention_floor(
            &request.shard,
            floor_position,
            request.expected_epoch,
        ) {
            Ok(()) => {}
            Err(EngineError::EpochFenced) | Err(EngineError::Conflict) => {
                return Ok(Self::fenced_outcome(request.expected_epoch));
            }
            Err(error) => return Err(error),
        }
        let pass = self.log.expire_segments_through_bounded_default(
            &request.shard,
            trim_through,
            request.now_ms,
        )?;
        let complete = pass.cursor.is_none() && pass.stopped_by.is_none();
        summary.merge(maintenance_summary(pass, false));
        if complete {
            watermark = DetachedTrimWatermark::Set(trim_through);
        }
        Ok(DetachedRetentionOutcome {
            expected_epoch: request.expected_epoch,
            summary,
            watermark,
        })
    }

    fn execute_orphan_gc(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<pqueue_engine::MaintenanceSummary> {
        if self.log.maintenance_owner_epoch(shard) != Some(expected_epoch)
            || self.log.current_epoch(shard)? != expected_epoch
        {
            return Ok(Self::fenced_outcome(expected_epoch).summary);
        }
        let limits = crate::maintenance::MaintenanceLimits::new(
            256,
            64 * 1024 * 1024,
            2_048,
            std::time::Duration::from_millis(50),
            64,
        )?;
        let report = self.log.gc_orphaned_branches_bounded(
            shard,
            expected_epoch,
            now_ms,
            60_000,
            limits,
            false,
        )?;
        Ok(maintenance_summary(report, true))
    }
}

impl LogStore for ObjectLog {
    fn durability_class(&self) -> DurabilityClass {
        self.durability_class
    }

    fn ensure_shard(&mut self, shard: &QueueKey) -> EngineResult<()> {
        self.shared_ensure_shard(shard)
    }

    fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        self.log.current_epoch(shard)
    }

    fn maintenance_owner_epoch(&self, shard: &QueueKey) -> Option<u64> {
        self.log.maintenance_owner_epoch(shard)
    }

    fn supports_objectlog_maintenance(&self) -> bool {
        true
    }

    fn detached_maintenance(&self) -> Option<Arc<dyn DetachedLogMaintenance>> {
        Some(Arc::new(ObjectLogMaintenance {
            log: Arc::clone(&self.log),
        }))
    }

    fn acquire_epoch(&mut self, shard: &QueueKey) -> EngineResult<u64> {
        // The fence entry's `committed_at_ms` is audit-only; pass 0 (the composition supplies no wall clock).
        self.shared_acquire_epoch(shard)
    }

    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // Stamp the sealed segment's `committed_at_ms` with a SOUND upper bound on the `created_at` of every
        // envelope in the batch (bead pqueue-b5cc2bc7 bug 1): the max `created_at` over the batch. The
        // composition supplies no wall clock on the raw append path, but `created_at <= committed_at_ms` MUST
        // hold for the retention-floor trim to be AC-TXN-3-safe (a segment is age-expired only when it holds
        // ONLY request_ids past retention). A `0` here (the old value) would mark every raw-append segment as
        // infinitely old and let the trim reclaim a within-retention request_id. Empty batches keep `0` (no
        // segment is sealed).
        self.shared_append(shard, commands, expected_epoch)
    }

    fn append_serialized(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let seal_ms = commands
            .iter()
            .map(|env| ts_to_ms(env.created_at))
            .max()
            .unwrap_or(0);
        let prepared = commands
            .iter()
            .cloned()
            .zip(serialized)
            .map(|(envelope, record)| SerializedCommandEnvelope::from_parts(envelope, record))
            .collect();
        let (outcome, _) = self
            .log
            .enqueue_serialized(shard, prepared, expected_epoch, seal_ms)?;
        let mut positions = outcome.committed;
        positions.extend(self.log.seal(shard, expected_epoch, seal_ms)?);
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
        self.shared_set_high_water(shard, position)
    }

    // -- retention floor (bounded-recovery segment-object reclamation, bead pqueue-b5cc2bc7) -------------
    // Passthroughs to the segmented substrate's durable floor blob + manifest-scan + segment-object trim.

    fn retention_floor(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        self.log.read_retention_floor(shard)
    }

    fn advance_retention_floor(
        &mut self,
        shard: &QueueKey,
        position: CommandPosition,
        expected_epoch: u64,
    ) -> EngineResult<()> {
        self.log
            .advance_retention_floor(shard, position, expected_epoch)
    }

    fn max_trimmable_seq_before(
        &self,
        shard: &QueueKey,
        cutoff_ms: i64,
    ) -> EngineResult<Option<u64>> {
        self.log.max_trimmable_seq_before(shard, cutoff_ms)
    }

    fn expire_segments_through_bounded(
        &mut self,
        shard: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<pqueue_engine::MaintenanceSummary> {
        let report =
            self.log
                .expire_segments_through_bounded_default(shard, through_seq, now_ms)?;
        Ok(maintenance_summary(report, false))
    }

    fn lowest_branch_pinned_below(
        &self,
        shard: &QueueKey,
        through_seq: u64,
        now_ms: i64,
    ) -> EngineResult<Option<u64>> {
        self.log
            .lowest_branch_pinned_below(shard, through_seq, now_ms)
    }

    fn gc_orphaned_branches_bounded(
        &mut self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<pqueue_engine::MaintenanceSummary> {
        let limits = crate::maintenance::MaintenanceLimits::new(
            256,
            64 * 1024 * 1024,
            2_048,
            std::time::Duration::from_millis(50),
            64,
        )?;
        let report = self.log.gc_orphaned_branches_bounded(
            shard,
            expected_epoch,
            now_ms,
            60_000,
            limits,
            false,
        )?;
        Ok(maintenance_summary(report, true))
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

    fn recover_definitions_page(
        &self,
        cursor: Option<&DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<DefinitionPage> {
        self.log
            .recover_definitions_page(cursor, limit, worker_partition)
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
        self.shared_gc_enqueue(shard, commands, expected_epoch, now_ms)
    }

    fn gc_enqueue_serialized(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        serialized: Vec<Vec<u8>>,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        if !self.group_commit {
            return Err(pqueue_engine::EngineError::Unavailable);
        }
        let prepared = commands
            .iter()
            .cloned()
            .zip(serialized)
            .map(|(envelope, record)| SerializedCommandEnvelope::from_parts(envelope, record))
            .collect();
        Ok(self
            .log
            .enqueue_serialized(shard, prepared, expected_epoch, now_ms)?
            .0
            .committed)
    }

    fn gc_seal(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.shared_gc_seal(shard, expected_epoch, now_ms)
    }

    fn gc_flush_due(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        self.shared_gc_flush_due(shard, expected_epoch, now_ms)
    }

    fn gc_advance_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> EngineResult<()> {
        self.shared_gc_advance_high_water(shard, position)
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
