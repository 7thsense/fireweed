//! LEGACY COMPATIBILITY BOUNDARY.
//!
//! Retained only for pre-cutover server arms until P12a migrates them to the provider-neutral
//! `AsyncProjectionSpec` product. Canonical object-log × SQLite composition lives in
//! `fireweed-objectlog::async_product_sqlite`; new production call sites must not use these names.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse,
    GroupedAggregateRequest, GroupedAggregateResponse, ItemId, ItemState, LeaseToken,
    MetricsByQueryRequest, QueryCapabilityFlags, QueueDefinition, RangeScanRequest,
    RangeScanResponse, RequestId, UtcTimestamp,
};
use fireweed_engine::TerminalEmissionMetrics;
use fireweed_engine::{
    ActiveScope, BatchUpdateItemRef, BatchUpdateSnapshotItem, ClaimCompatibility, ClaimRef,
    ClaimUnit, ClaimedItem, CommandEnvelope, CommandPosition, DiscoveryGranularity, EngineError,
    EngineResult, FinalizeOutcome, IndexHit, ItemView, LeaseView, LiveItemView, LogLineageIdentity,
    PendingPage, PendingSummary, PushItem, QueueKey, QueueMetrics, RichClaimSelection,
    UpdateFieldsCommand,
};
use fireweed_engine::{AsOfProjectionStore, ProjectionSnapshot, ProjectionStore};
use fireweed_projection::InMemoryProjection;

use super::*;

/// Durable SQLite projection plus hot in-memory serving projection.
///
/// `HybridProjectionStore` is the object-log/hybrid projection axis: every committed batch is durably
/// absorbed by [`SqliteProjectionStore`] first, then applied to [`InMemoryProjection`]. Reads and
/// pre-commit validation use only the in-memory projection after `ensure_shard` has hydrated it from
/// SQLite's exported image. If SQLite advances but memory rejects the same batch, the store is poisoned so
/// the current process cannot serve or mutate from a memory image that is behind the durable cursor.
/// Default cap on how many deferred commands one [`ProjectionStore::flush_deferred`] call applies
/// (TD flush-chunking). `flush_deferred` runs under the composed backend's unit-of-work mutex, so an
/// unbounded batch there blocks every concurrent push/claim for as long as the whole backlog takes to
/// apply. Bounding the per-call batch bounds that worst-case hold time; the periodic flusher cadence
/// (`spawn_hybrid_flusher`, 250ms) drains the remainder over subsequent ticks.
///
/// pqueue-8e5e7846: the original `2_000` default never actually bound anything at release scale. Each
/// deferred entry is one committed push/claim/finalize *call* (which may itself batch up to
/// one async-projection apply batch of items), not one item — so the 100k-resident release lane's whole
/// push+claim+finalize backlog tops out at `3 * (resident / release_default_batch) = 600` deferred entries
/// (with a 500-item release batch and 100,000-item selected-projection target), comfortably under the
/// old chunk. `flush_deferred` therefore always drained the entire backlog in one composed-backend-mutex
/// hold, exactly the unbounded-batch problem the chunking mechanism was meant to prevent.
///
/// Measured across `chunk` in `{25, 50, 100, 200, 250, 2_000}` at 100k-resident release scale: chunk values
/// well below the realistic backlog (25, 50) made hot-path tail latency dramatically WORSE, not better
/// (ack p99 vs in-memory ratio 15-38x vs. a 2_000-chunk baseline of 1.6-8x across repeated runs) — the
/// opposite of the naive "smaller chunk protects the mutex" intuition. Root cause: SQLite WAL commit has a
/// large fixed per-transaction cost, and once a burst of arrivals between 250ms flusher ticks exceeds the
/// chunk, the shortfall compounds into the next tick's now-larger backlog (arrivals-since-last-tick PLUS the
/// undrained remainder), a queueing cascade that grows the number of stalled pushes far faster than any
/// per-call hold-time savings. `250` sits just above the measured peak per-tick burst (~155) so it almost
/// keeps individual flushes bounded without causing the fixed-cost cascade seen with tiny chunks, while
/// staying below the smallest structural release-scale backlog (600) so it remains a genuine, provable
/// bound rather than a no-op. The residual ack-p99-vs-in-memory gate at 100k is dominated by host-fsync
/// noise unrelated to this parameter (see `docs/perf/evidence/hybrid-scale/` and the bundle evidence for
/// this bead) — chunk size was ruled out, not confirmed, as the lever for that gate.
pub const DEFAULT_DEFERRED_FLUSH_CHUNK: usize = 250;

// ---------------------------------------------------------------------------
// Internal fault-injection seam (TP-003 §3.10 AC-TXN-5/5A)
// ---------------------------------------------------------------------------
//
// The public `ProjectionStore` seam (`apply`/`apply_live`/`flush_deferred`) does not let a caller strike a
// fault strictly BETWEEN the durable SQLite commit and the in-memory apply, or strictly inside the deferred
// async SQLite checkpoint apply — those instants are internal to [`HybridProjectionStore`]'s own commit
// pipeline. This test-only hook lets a test strike a "process died right here" fault at each of those named
// instants and observe the durable/poison contract, mirroring `fireweed_objectlog::segmented`'s `FaultHook`
// (AC-TXN-4) for the projection-apply side of the hybrid substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HybridFaultCutPoint {
    /// Struck at the top of the SQLite-first `apply_durable_then_memory` ordering, BEFORE the durable SQLite
    /// checkpoint commits (models "the SQLite apply itself failed"). A fault here aborts the whole apply with
    /// NO durable projection effect, NO memory effect, and — crucially — NO poison: the object-log manifest
    /// entry is already durable, so recovery replays the tail beyond the prior SQLite high-water and the
    /// command is neither lost nor duplicated (TD-004 §"hybrid-strict apply path", "SQLite failure" row).
    BeforeSqliteApply,
    /// The SQLite checkpoint for this batch committed durably, but the in-memory apply that makes it
    /// client-visible has not run yet (the "hybrid-strict" `apply` ordering: SQLite, then memory).
    AfterSqliteCommitBeforeMemoryApply,
    /// Struck at the top of the in-memory apply step, shared by every apply path (`apply`, `apply_live`,
    /// `apply_live_owned`, `apply_recovery`) — the success barrier every hybrid profile applies before
    /// returning to the caller.
    DuringMemoryApply,
    /// Struck immediately before the deferred queue's batched SQLite checkpoint transaction (the
    /// "hybrid-async" background apply that catches SQLite up to the in-memory high-water).
    DuringAsyncSqliteApply,
}

/// A test-only fault hook for [`HybridProjectionStore`] (TP-003 §3.10 AC-TXN-5/5A). Returning `Err` aborts
/// the pipeline at that instant; `Ok(())` (the default no-op behavior of not installing a hook at all) lets
/// the pipeline run normally. Never invoked from any production call site — only `set_fault_hook` installs
/// one, and nothing in this crate calls it outside tests.
pub trait HybridFaultHook: Send + Sync {
    fn fault_point(&self, cut: HybridFaultCutPoint) -> EngineResult<()>;
}

struct DeferredCheckpoint {
    position: CommandPosition,
    command: CommandEnvelope,
    encoded_bytes: u64,
}

struct DeferredBatch {
    shard: QueueKey,
    entries: VecDeque<DeferredCheckpoint>,
    enqueued_at_ms: i64,
}

pub struct HybridProjectionStore {
    sqlite: SqliteProjectionStore,
    memory: InMemoryProjection,
    hydrated: HashSet<QueueKey>,
    memory_next_seq: HashMap<QueueKey, u64>,
    deferred: VecDeque<DeferredBatch>,
    deferred_flush_chunk: usize,
    /// Deterministic clock seam for async-debt tests. Production samples the wall clock.
    async_debt_now_override_ms: Option<i64>,
    /// `objectlog/hybrid-strict` (TD-004): when set, the group-commit write path (`apply_live`,
    /// `apply_live_owned`, `apply_recovery`) commits the sealed batch DURABLY to SQLite BEFORE applying it to
    /// hot memory — the `apply_durable_then_memory` ordering — instead of the default `objectlog/hybrid`
    /// memory-first-then-deferred-checkpoint ordering. This puts the `AfterSqliteCommitBeforeMemoryApply`
    /// poison cut and the durable-before-visible barrier on the real server write pipeline.
    strict: bool,
    /// The disposable SQLite image was deliberately removed and has not yet completed an authoritative
    /// rebuild. Every mutation fails admission while it is offline; otherwise a new append could create an
    /// ordered checkpoint suffix against the deliberately reset image.
    durable_projection_offline: bool,
    checkpoint_errors: HashMap<QueueKey, String>,
    poisoned: Option<String>,
    /// Test-only fault-injection hook (TP-003 §3.10 AC-TXN-5/5A). `None` in every production path.
    fault_hook: Mutex<Option<Arc<dyn HybridFaultHook>>>,
    /// `objectlog/hybrid-async` (TD-004) admission/high-water/retention gate. `Some` ONLY for the
    /// `hybrid-async` server profile (wired by `with_async_monitor`); `None` for `objectlog/hybrid` and
    /// `objectlog/hybrid-strict`, which do not gate on async-apply debt. When present, each queue's async
    /// apply debt (the deferred-checkpoint backlog) is folded into its monitor on live apply, admission, and
    /// every deferred flush attempt, so [`ProjectionStore::admit_mutation`] fails new admission under Hard
    /// backpressure and [`ProjectionStore::recovery_high_water`] withholds the lagging high-water until debt
    /// drains below the release band.
    async_thresholds: Option<HybridAsyncThresholds>,
    async_monitors: HashMap<QueueKey, HybridAsyncMonitor>,
}

impl HybridProjectionStore {
    pub fn open(path: &str) -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::open(path)?))
    }

    pub fn in_memory() -> EngineResult<Self> {
        Ok(Self::new(SqliteProjectionStore::in_memory()?))
    }

    pub fn new(sqlite: SqliteProjectionStore) -> Self {
        Self {
            sqlite,
            memory: InMemoryProjection::new(),
            hydrated: HashSet::new(),
            memory_next_seq: HashMap::new(),
            deferred: VecDeque::new(),
            deferred_flush_chunk: DEFAULT_DEFERRED_FLUSH_CHUNK,
            async_debt_now_override_ms: None,
            strict: false,
            durable_projection_offline: false,
            checkpoint_errors: HashMap::new(),
            poisoned: None,
            fault_hook: Mutex::new(None),
            async_thresholds: None,
            async_monitors: HashMap::new(),
        }
    }

    /// Support constructor for recovery and fail-closed tests that need explicit parts.
    pub fn from_parts(sqlite: SqliteProjectionStore, memory: InMemoryProjection) -> Self {
        Self {
            sqlite,
            memory,
            hydrated: HashSet::new(),
            memory_next_seq: HashMap::new(),
            deferred: VecDeque::new(),
            deferred_flush_chunk: DEFAULT_DEFERRED_FLUSH_CHUNK,
            async_debt_now_override_ms: None,
            strict: false,
            durable_projection_offline: false,
            checkpoint_errors: HashMap::new(),
            poisoned: None,
            fault_hook: Mutex::new(None),
            async_thresholds: None,
            async_monitors: HashMap::new(),
        }
    }

    /// Select the `objectlog/hybrid-strict` apply ordering (TD-004): the group-commit write path commits the
    /// sealed batch durably to SQLite BEFORE applying it to hot memory, so a SQLite failure returns no success
    /// and a SQLite-commit-then-memory-fail poisons the store fail-closed. `false` (the default) is the
    /// `objectlog/hybrid` / `objectlog/hybrid-async` memory-first + deferred-checkpoint ordering.
    pub fn with_strict_apply(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Whether this store runs the `objectlog/hybrid-strict` SQLite-durable-before-memory apply ordering.
    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Install the `objectlog/hybrid-async` async-apply debt/backpressure/poison monitor (TD-004) with the
    /// server-configured `thresholds`. This is the ONLY constructor that arms monitoring: the default
    /// `objectlog/hybrid` and `objectlog/hybrid-strict` profiles have no thresholds and are unaffected. Once
    /// armed, live apply, admission, and flush attempts fold each queue's async-apply debt (the deferred-
    /// checkpoint backlog) into its monitor, so [`ProjectionStore::admit_mutation`] fails new
    /// mutating admission closed under Hard backpressure and [`ProjectionStore::recovery_high_water`]
    /// withholds the lagging high-water until the backlog drains below the release band.
    pub fn with_async_monitor(mut self, thresholds: HybridAsyncThresholds) -> Self {
        self.async_thresholds = Some(thresholds);
        for shard in &self.hydrated {
            self.async_monitors
                .insert(shard.clone(), HybridAsyncMonitor::new(thresholds));
        }
        self
    }

    /// Whether the `objectlog/hybrid-async` debt monitor is armed on this store (test/observability seam).
    pub fn has_async_monitor(&self) -> bool {
        self.async_thresholds.is_some()
    }

    /// The current async-apply backpressure level, or `None` when no monitor is armed (test/observability
    /// seam for the AC-TXN-5A server-wired proof).
    pub fn async_backpressure_level(&self) -> Option<BackpressureLevel> {
        self.async_thresholds?;
        Some(
            self.async_monitors
                .values()
                .map(HybridAsyncMonitor::level)
                .max_by_key(|level| match level {
                    BackpressureLevel::Clear => 0,
                    BackpressureLevel::Soft => 1,
                    BackpressureLevel::Hard => 2,
                })
                .unwrap_or(BackpressureLevel::Clear),
        )
    }

    /// Whether segment expiry / retention advancement is currently allowed under the async-apply monitor
    /// (TD-004 "Retention backpressure"). `true` when no monitor is armed (the non-async profiles have no
    /// async retention gate); otherwise deferred to [`HybridAsyncMonitor::retention_may_advance`], which is
    /// `false` while debt is over budget or the worker is poisoned.
    pub fn async_retention_may_advance(&self) -> bool {
        self.async_monitors
            .values()
            .all(HybridAsyncMonitor::retention_may_advance)
    }

    /// The count of terminal (Complete/Failed) items currently resident in the DURABLE, checkpointed SQLite
    /// image for `shard` — a durable-state observable for the TD-004 retention proof (it DROPS when
    /// [`ProjectionStore::reap_terminal_items`] reclaims past-retention terminal rows, and is FROZEN while
    /// async-apply debt is Hard because the composition withholds the reap). Distinct from the hot-memory
    /// `metrics().resident_terminal_count`, which reflects the (unreaped) in-memory serving image.
    pub fn durable_resident_terminal_count(&self, shard: &QueueKey) -> EngineResult<u64> {
        Ok(metrics_sql(&self.sqlite.lock().conn, shard)?.resident_terminal_count)
    }

    /// Fold the store's CURRENT async-apply debt into the armed monitor (no-op when unarmed). The genuine
    /// debt signal for `objectlog/hybrid-async` is the deferred-checkpoint backlog — committed commands
    /// already durable on the object log and applied to hot memory but not yet checkpointed into the durable
    /// SQLite image. Each queued checkpoint retains its encoded command size and enqueue timestamp, so the
    /// monitor sees the actual command count, bytes, queue depth, and oldest age trailing the SQLite
    /// high-water. This is NOT a test-only poke: it is called from the real `apply_live` /
    /// `apply_live_owned` / `apply_recovery` write path (backlog grows) and from `flush_deferred` (backlog
    /// drains), so production apply-lag drives the backpressure level.
    fn async_debt_now_ms(&self) -> i64 {
        self.async_debt_now_override_ms.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
                .unwrap_or(0)
        })
    }

    fn ensure_async_monitor(&mut self, shard: &QueueKey) {
        if let Some(thresholds) = self.async_thresholds {
            self.async_monitors
                .entry(shard.clone())
                .or_insert_with(|| HybridAsyncMonitor::new(thresholds));
        }
    }

    fn debt_for(&self, shard: &QueueKey, now_ms: i64) -> HybridAsyncDebt {
        let batches: Vec<&DeferredBatch> = self
            .deferred
            .iter()
            .filter(|batch| &batch.shard == shard)
            .collect();
        let apply_lag_commands = batches.iter().map(|batch| batch.entries.len() as u64).sum();
        let apply_debt_bytes = batches.iter().fold(0_u64, |total, batch| {
            batch.entries.iter().fold(total, |subtotal, entry| {
                subtotal.saturating_add(entry.encoded_bytes)
            })
        });
        let oldest_unapplied_age_ms = batches.first().map_or(0, |batch| {
            now_ms.saturating_sub(batch.enqueued_at_ms).max(0) as u64
        });
        HybridAsyncDebt {
            apply_lag_commands,
            apply_debt_bytes,
            apply_queue_depth: batches.len() as u64,
            oldest_unapplied_age_ms,
        }
    }

    fn observe_async_debt(&mut self, shard: &QueueKey) {
        self.ensure_async_monitor(shard);
        let now_ms = self.async_debt_now_ms();
        let debt = self.debt_for(shard, now_ms);
        if let Some(monitor) = self.async_monitors.get_mut(shard) {
            monitor.observe(debt, now_ms);
        }
    }

    fn observe_all_async_debt(&mut self) {
        let mut shards: HashSet<QueueKey> = self.async_monitors.keys().cloned().collect();
        shards.extend(self.deferred.iter().map(|batch| batch.shard.clone()));
        for shard in shards {
            self.observe_async_debt(&shard);
        }
    }

    fn deferred_batch(
        &self,
        positions: impl IntoIterator<Item = CommandPosition>,
        commands: impl IntoIterator<Item = CommandEnvelope>,
    ) -> EngineResult<DeferredBatch> {
        let enqueued_at_ms = self.async_debt_now_ms();
        let entries: VecDeque<DeferredCheckpoint> = positions
            .into_iter()
            .zip(commands)
            .map(|(position, command)| {
                let encoded_bytes = serde_json::to_vec(&command)
                    .map_err(|error| EngineError::Storage(error.to_string()))?
                    .len() as u64;
                Ok(DeferredCheckpoint {
                    position,
                    command,
                    encoded_bytes,
                })
            })
            .collect::<EngineResult<_>>()?;
        let shard = entries
            .front()
            .map(|entry| entry.position.queue.clone())
            .ok_or_else(|| EngineError::Storage("deferred batch must not be empty".into()))?;
        if entries.iter().any(|entry| entry.position.queue != shard) {
            return Err(EngineError::Storage(
                "deferred batch spans multiple queue shards".into(),
            ));
        }
        Ok(DeferredBatch {
            shard,
            entries,
            enqueued_at_ms,
        })
    }

    /// Deterministic test seam for byte/age debt classification. Production callers never set it.
    #[doc(hidden)]
    pub fn set_async_debt_now_ms_for_test(&mut self, now_ms: Option<i64>) {
        self.async_debt_now_override_ms = now_ms;
    }

    /// Export the armed async monitor's current measured backlog metrics.
    pub fn async_metrics(&self) -> Option<HybridAsyncMetrics> {
        (self.async_monitors.len() == 1)
            .then(|| {
                self.async_monitors
                    .values()
                    .next()
                    .map(HybridAsyncMonitor::metrics)
            })
            .flatten()
    }

    pub fn async_metrics_for(&self, shard: &QueueKey) -> Option<HybridAsyncMetrics> {
        self.async_monitors
            .get(shard)
            .map(HybridAsyncMonitor::metrics)
    }

    /// Install (or clear, with `None`) a test-only fault hook (TP-003 §3.10 AC-TXN-5/5A). Never called from
    /// any production call site.
    pub fn set_fault_hook(&self, hook: Option<Arc<dyn HybridFaultHook>>) {
        *self.fault_hook.lock().expect("hybrid fault hook poisoned") = hook;
    }

    /// Invoke the installed fault hook (if any) at `cut`. `Ok(())` when no hook is installed.
    fn fault(&self, cut: HybridFaultCutPoint) -> EngineResult<()> {
        let hook = self
            .fault_hook
            .lock()
            .expect("hybrid fault hook poisoned")
            .clone();
        match hook {
            Some(h) => h.fault_point(cut),
            None => Ok(()),
        }
    }

    /// Bound how many deferred commands a single `flush_deferred` call applies. Test/tuning seam for
    /// the flush-chunking bound (defaults to [`DEFAULT_DEFERRED_FLUSH_CHUNK`]); `0` is treated as `1`
    /// so a flush always makes forward progress.
    pub fn with_deferred_flush_chunk(mut self, chunk: usize) -> Self {
        self.deferred_flush_chunk = chunk.max(1);
        self
    }

    pub fn sqlite(&self) -> &SqliteProjectionStore {
        &self.sqlite
    }

    /// Remove the disposable SQLite image while preserving the authoritative hot image. Deferred entries
    /// are discarded because the subsequent rebuild replays their commands from the object log; retaining
    /// them would attempt to apply a high-position suffix to an empty database.
    pub fn delete_durable_projection(&mut self) -> EngineResult<()> {
        self.sqlite.reset_projection()?;
        self.deferred.clear();
        self.durable_projection_offline = true;
        self.observe_all_async_debt();
        Ok(())
    }

    /// Start an authoritative rebuild from an empty SQLite image. Safe to call after an earlier delete or
    /// directly as repair: queued async suffixes are superseded by the full log replay performed while the
    /// composition lock is held.
    pub fn begin_durable_rebuild(&mut self) -> EngineResult<()> {
        self.delete_durable_projection()
    }

    /// Mark a fully replayed and verified SQLite image online and clear async worker poison/debt.
    pub fn finish_durable_rebuild(&mut self) {
        self.deferred.clear();
        self.durable_projection_offline = false;
        self.checkpoint_errors.clear();
        self.poisoned = None;
        for monitor in self.async_monitors.values_mut() {
            monitor.clear_after_rebuild();
        }
    }

    pub fn durable_projection_offline(&self) -> bool {
        self.durable_projection_offline
    }

    pub fn checkpoint_error(&self) -> Option<&str> {
        self.checkpoint_errors.values().next().map(String::as_str)
    }

    /// The latched poison reason, or `None` when healthy. Fed to the composition's recovery gate via
    /// [`ProjectionStore::recovery_poison`] so a poisoned store fails closed instead of serving a divergent
    /// image.
    pub fn poison_reason(&self) -> Option<&str> {
        self.poisoned.as_deref().or_else(|| {
            self.async_monitors
                .values()
                .find_map(HybridAsyncMonitor::poison_reason)
        })
    }

    /// Latch poison from an out-of-band failure the async apply pipeline detected (persistent checkpoint
    /// error at the poison threshold, corruption, or an unresolved replay-apply gap). Idempotent — the
    /// first reason wins. After this the store fails all reads/validation/writes closed until restart.
    pub fn mark_poisoned(&mut self, reason: impl Into<String>) {
        if self.poisoned.is_none() {
            self.poisoned = Some(reason.into());
        }
    }

    fn shard_for(definition: &QueueDefinition) -> QueueKey {
        QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone())
    }

    fn poison_error(reason: &str) -> EngineError {
        EngineError::Storage(format!("async projection poisoned: {reason}"))
    }

    fn check_healthy(&self) -> EngineResult<()> {
        match &self.poisoned {
            Some(reason) => Err(Self::poison_error(reason)),
            None => Ok(()),
        }
    }

    fn poison<T>(&mut self, reason: String) -> EngineResult<T> {
        self.poisoned = Some(reason.clone());
        Err(Self::poison_error(&reason))
    }

    fn require_hydrated(&self, shard: &QueueKey) -> EngineResult<()> {
        self.check_healthy()?;
        if let Some(reason) = self
            .async_monitors
            .get(shard)
            .and_then(HybridAsyncMonitor::poison_reason)
        {
            return Err(Self::poison_error(reason));
        }
        if self.hydrated.contains(shard) {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "hybrid projection shard {}/{} is not hydrated",
                shard.tenant_id, shard.queue_id
            )))
        }
    }

    fn hydrate_from_sqlite(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        let shard = Self::shard_for(definition);
        let image = self.sqlite.export_projection_image(&shard)?;
        let expected_high_water = image.high_water.clone();
        let sqlite_high_water = self.sqlite.recovery_high_water(&shard)?;
        let high_water_matches = match (&sqlite_high_water, &expected_high_water) {
            (Some(cursor), Some(image)) => {
                cursor.queue == image.queue && cursor.sequence == image.sequence
            }
            (None, None) => true,
            _ => false,
        };
        if !high_water_matches {
            return Err(EngineError::Storage(format!(
                "hybrid projection hydration high-water mismatch for {}/{}: cursor {:?}, image {:?}",
                shard.tenant_id, shard.queue_id, sqlite_high_water, expected_high_water
            )));
        }
        // `hydrate_shard` builds ProjectionData from the complete image before its single infallible map
        // insertion, including metrics parity validation. Do every fallible durable read above first, then
        // publish the hot replacement.
        self.memory.hydrate_shard(definition, image)?;
        self.memory_next_seq.insert(
            shard.clone(),
            expected_high_water.map_or(0, |pos| pos.sequence.saturating_add(1)),
        );
        self.hydrated.insert(shard);
        Ok(())
    }

    fn apply_memory(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        let mut advanced: HashMap<QueueKey, u64> = HashMap::new();
        let apply_result: EngineResult<()> = (|| {
            self.fault(HybridFaultCutPoint::DuringMemoryApply)?;
            for (pos, env) in positions.iter().zip(commands.iter()) {
                let next_seq = self.memory_next_seq.get(&pos.queue).copied().unwrap_or(0);
                if pos.sequence >= next_seq {
                    self.memory
                        .apply_borrowed(std::slice::from_ref(pos), std::slice::from_ref(env))?;
                    let candidate = pos.sequence.saturating_add(1);
                    advanced
                        .entry(pos.queue.clone())
                        .and_modify(|next| *next = (*next).max(candidate))
                        .or_insert(candidate);
                }
            }
            Ok(())
        })();
        match apply_result {
            Ok(()) => {
                self.memory_next_seq.extend(advanced);
                Ok(())
            }
            Err(err) => self.poison(format!(
                "memory apply failed after object-log commit: {err}"
            )),
        }
    }

    fn apply_durable_then_memory(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.check_healthy()?;
        if self.durable_projection_offline {
            return Err(EngineError::Unavailable);
        }
        // A "SQLite apply failed" fault aborts BEFORE the durable commit: no durable projection effect, no
        // memory effect, and NO poison. The object-log manifest entry is already durable, so recovery replays
        // the tail beyond the prior SQLite high-water and the command is neither lost nor duplicated (TD-004
        // hybrid-strict "SQLite failure" row). Unlike the post-commit cut below, the store stays healthy.
        self.fault(HybridFaultCutPoint::BeforeSqliteApply)?;
        self.sqlite.apply_committed_batch(positions, commands)?;
        if let Err(e) = self.fault(HybridFaultCutPoint::AfterSqliteCommitBeforeMemoryApply) {
            // The SQLite checkpoint already committed durably; a memory apply that never runs would leave
            // memory silently behind the durable image. Poison so every subsequent read/write fails closed
            // until a restart re-hydrates memory from the (already-consistent) SQLite ProjectionImage.
            return self.poison(format!(
                "memory apply skipped after durable SQLite commit (fault injected): {e}"
            ));
        }
        self.apply_memory(positions, commands)
    }

    /// Number of committed commands waiting for SQLite checkpoint apply. Test/observability seam.
    pub fn deferred_command_count(&self) -> usize {
        self.deferred.iter().map(|batch| batch.entries.len()).sum()
    }
}

impl ProjectionStore for HybridProjectionStore {
    fn supports_gates(&self) -> bool {
        self.poisoned.is_none() && self.memory.supports_gates()
    }

    fn hot_projection_capabilities(&self) -> QueryCapabilityFlags {
        if self.poisoned.is_some() {
            QueryCapabilityFlags::default()
        } else {
            self.memory.hot_projection_capabilities()
        }
    }

    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> EngineResult<Vec<ActiveScope>> {
        self.require_hydrated(shard)?;
        self.memory.discover_active_scopes(shard, granularity, now)
    }

    fn select_rich_claim(
        &self,
        shard: &QueueKey,
        unit: ClaimUnit,
        compatibility: &ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> EngineResult<RichClaimSelection> {
        self.require_hydrated(shard)?;
        self.memory
            .select_rich_claim(shard, unit, compatibility, now, max_items)
    }

    fn ensure_shard(&mut self, definition: &QueueDefinition) -> EngineResult<()> {
        self.check_healthy()?;
        let shard = Self::shard_for(definition);
        self.ensure_async_monitor(&shard);
        self.sqlite.create_queue_projection(definition.clone())?;
        // fireweed-2ad3a030: create_queue is idempotent. Re-exporting SQLite into hot memory on every
        // ensure would drop process-local lease cleartext (SQLite stores only lease_token_hash). Snorri
        // (and other adapters) call create_queue before claim and again before commit; the second call
        // must not wipe the lease that claim just applied.
        if self.hydrated.contains(&shard) {
            return Ok(());
        }
        self.hydrate_from_sqlite(definition)
    }

    fn plan_item_mutation(
        &self,
        shard: &QueueKey,
        request: &fireweed_engine::ItemMutationRequest,
    ) -> EngineResult<fireweed_engine::ItemMutationPlan> {
        self.check_healthy()?;
        self.require_hydrated(shard)?;
        self.memory.plan_item_mutation(shard, request)
    }

    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.apply_durable_then_memory(positions, commands)
    }

    fn apply_live(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.check_healthy()?;
        if positions.len() != commands.len() {
            return Err(EngineError::Storage(
                "hybrid apply_live: positions/commands length mismatch".into(),
            ));
        }
        if positions.is_empty() {
            return Ok(());
        }
        // `objectlog/hybrid-strict`: commit durably to SQLite BEFORE hot memory (no deferral); the default
        // `objectlog/hybrid` ordering applies memory first and defers the SQLite checkpoint.
        if self.strict {
            return self.apply_durable_then_memory(positions, commands);
        }
        let deferred = self.deferred_batch(positions.iter().cloned(), commands.iter().cloned())?;
        let shard = deferred.shard.clone();
        self.apply_memory(positions, commands)?;
        self.deferred.push_back(deferred);
        self.observe_async_debt(&shard);
        Ok(())
    }

    fn apply_live_owned(
        &mut self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        self.check_healthy()?;
        if positions.len() != commands.len() {
            return Err(EngineError::Storage(
                "hybrid apply_live_owned: positions/commands length mismatch".into(),
            ));
        }
        if positions.is_empty() {
            return Ok(());
        }
        // `objectlog/hybrid-strict`: SQLite-durable-before-memory on the group-commit write path (this is the
        // method the composed group-commit distribute path calls, so the `AfterSqliteCommitBeforeMemoryApply`
        // poison cut lands on the real server write pipeline).
        if self.strict {
            return self.apply_durable_then_memory(&positions, &commands);
        }
        let deferred = self.deferred_batch(positions.iter().cloned(), commands.iter().cloned())?;
        let shard = deferred.shard.clone();
        self.apply_memory(&positions, &commands)?;
        self.deferred.push_back(deferred);
        self.observe_async_debt(&shard);
        Ok(())
    }

    fn apply_recovery(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.check_healthy()?;
        if positions.len() != commands.len() {
            return Err(EngineError::Storage(
                "hybrid apply_recovery: positions/commands length mismatch".into(),
            ));
        }
        if positions.is_empty() {
            return Ok(());
        }
        // `objectlog/hybrid-strict`: replay the recovered tail SQLite-durable-first so the durable image and
        // hot memory stay in lockstep (idempotent — already-applied prefixes are skipped by both stores).
        if self.strict {
            return self.apply_durable_then_memory(positions, commands);
        }
        let deferred = self.deferred_batch(positions.iter().cloned(), commands.iter().cloned())?;
        let shard = deferred.shard.clone();
        self.apply_memory(positions, commands)?;
        self.deferred.push_back(deferred);
        self.observe_async_debt(&shard);
        Ok(())
    }

    fn install_recovery_shard(
        &mut self,
        definition: &QueueDefinition,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        self.check_healthy()?;
        // Create-loser installation is deliberately synchronous even for the async profile: SQLite applies
        // the complete replay in one transaction, then the hot image is replaced from that durable snapshot.
        // An SQLite error rolls back all commands; hydration performs all fallible reads before map insertion.
        self.sqlite.apply_committed_batch(positions, commands)?;
        self.hydrate_from_sqlite(definition)
    }

    /// Apply at most [`Self::deferred_flush_chunk`] deferred commands from the oldest non-poisoned shard,
    /// preserving that shard's batch order, then return. A poisoned shard remains queued for repair but
    /// cannot head-of-line block healthy shards. This runs under the composed backend's
    /// unit-of-work mutex, so bounding the batch bounds how long one call can block concurrent
    /// push/claim callers waiting on the same lock; the periodic flusher cadence drains a large
    /// backlog over several calls instead of one unbounded transaction.
    fn flush_deferred(&mut self) -> EngineResult<()> {
        self.check_healthy()?;
        // A flush tick is also a debt observation tick, even when the worker is idle/offline or the
        // attempted checkpoint fails. This lets quiet backlog age into Hard without another mutation.
        self.observe_all_async_debt();
        if self.durable_projection_offline {
            return Ok(());
        }
        if self.deferred.is_empty() {
            return Ok(());
        }

        let Some(batch_index) = self.deferred.iter().position(|batch| {
            self.async_monitors
                .get(&batch.shard)
                .is_none_or(|monitor| !monitor.is_poisoned())
        }) else {
            return Ok(());
        };
        let shard = self
            .deferred
            .get(batch_index)
            .expect("selected batch exists")
            .shard
            .clone();
        let mut remaining = self.deferred_flush_chunk;
        let mut positions = Vec::new();
        let mut commands = Vec::new();
        for batch in self
            .deferred
            .iter()
            .skip(batch_index)
            .take_while(|batch| batch.shard == shard)
        {
            let take = batch.entries.len().min(remaining);
            positions.extend(
                batch
                    .entries
                    .iter()
                    .take(take)
                    .map(|entry| entry.position.clone()),
            );
            commands.extend(
                batch
                    .entries
                    .iter()
                    .take(take)
                    .map(|entry| entry.command.clone()),
            );
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        if let Err(e) = self.fault(HybridFaultCutPoint::DuringAsyncSqliteApply) {
            // The deferred batch is untouched (still queued for the next flush attempt) but the async
            // apply pipeline is no longer trustworthy: poison so it fails closed instead of silently
            // retrying forever against a possibly-corrupt SQLite image.
            let reason = format!("async SQLite checkpoint apply faulted: {e}");
            self.checkpoint_errors.insert(shard.clone(), reason.clone());
            if let Some(monitor) = self.async_monitors.get_mut(&shard) {
                monitor.poison(reason.clone());
                self.observe_async_debt(&shard);
                return Err(EngineError::Storage(reason));
            }
            return self.poison(reason);
        }
        if let Err(error) = self.sqlite.apply_committed_batch(&positions, &commands) {
            self.checkpoint_errors
                .insert(shard.clone(), error.to_string());
            if let Some(monitor) = self.async_monitors.get_mut(&shard) {
                monitor.record_checkpoint_error(error.to_string());
            }
            self.observe_async_debt(&shard);
            return Err(error);
        }
        let mut applied = positions.len();
        while applied > 0 {
            let batch = self
                .deferred
                .get_mut(batch_index)
                .expect("applied batch exists");
            let drained = batch.entries.len().min(applied);
            batch.entries.drain(..drained);
            applied -= drained;
            if batch.entries.is_empty() {
                self.deferred.remove(batch_index);
            }
        }
        self.checkpoint_errors.remove(&shard);
        // An ordered batch checkpointed cleanly into the durable SQLite image: satisfy the clean-batch
        // precondition for releasing Hard backpressure, then re-fold the (now-smaller) backlog so the level
        // can drop once debt falls below the release band. Only the `hybrid-async` profile arms the monitor.
        if let Some(monitor) = self.async_monitors.get_mut(&shard) {
            monitor.record_apply_success();
        }
        self.observe_all_async_debt();
        Ok(())
    }

    fn recovery_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        self.require_hydrated(shard)?;
        let high_water = self.sqlite.recovery_high_water(shard)?;
        // `objectlog/hybrid-async` (TD-004 "Recovery/high-water backpressure"): while async-apply debt is
        // Hard (or the worker is poisoned) the lagging SQLite high-water MUST NOT be advertised as a safe
        // replay-skip point, so the monitor withholds it (returns `None`) until the backlog drains below the
        // release band. The non-async profiles have no monitor and pass the high-water through unchanged.
        Ok(match self.async_monitors.get(shard) {
            Some(monitor) => monitor.recovery_high_water_safe(high_water),
            None => high_water,
        })
    }

    /// Fail closed unless the durable SQLite image provably descends from the object log it is about to be
    /// replayed against (TD-004 async lineage validation, "Manifest to segment" / "Command sequence to
    /// SQLite" rows). Two mismatches poison the local projection so the composition never serves a divergent
    /// image:
    ///
    /// 1. **Manifest generation / epoch** — a recorded checkpoint lineage whose `source_epoch` is NEWER than
    ///    the log's current `assignment_epoch` cannot descend from this log (a rolled-back or foreign log,
    ///    or an image restored over the wrong namespace).
    /// 2. **Segment chain / high-water identity** — a SQLite logical high-water AHEAD of the log's committed
    ///    head means the projection absorbed commands the durable log does not contain; its high-water can
    ///    never be a safe replay-skip point.
    ///
    /// The lenient direction is intentional: a recorded epoch OLDER than the log's (the log was re-acquired
    /// at a higher epoch) and a SQLite high-water BEHIND the log's head (async apply lagging) are the normal
    /// recovery cases — tail replay catches memory up. Only a projection ahead of, or forked from, the log
    /// fails closed.
    fn validate_recovery_lineage(&mut self, identity: &LogLineageIdentity) -> EngineResult<()> {
        self.check_healthy()?;
        let shard = &identity.shard;
        let recorded = self.sqlite.checkpoint_lineage(shard)?;
        let sqlite_high_water = self.sqlite.recovery_high_water(shard)?;
        let sqlite_next_seq = sqlite_high_water
            .as_ref()
            .map_or(0, |pos| pos.sequence.saturating_add(1));
        // The exclusive upper bound on any applied prefix: the next sequence the log will assign.
        let log_next_seq = identity
            .high_water
            .as_ref()
            .map_or(0, |pos| pos.sequence.saturating_add(1));

        if let Some(lineage) = recorded
            .as_ref()
            .filter(|l| l.source_epoch > identity.current_epoch)
        {
            return self.poison(format!(
                "hybrid recovery lineage for {}/{} records object-log epoch {} (segment {}) newer than \
                 the log's current epoch {}; the SQLite image does not descend from this log",
                shard.tenant_id,
                shard.queue_id,
                lineage.source_epoch,
                lineage.source_segment,
                identity.current_epoch,
            ));
        }

        if sqlite_next_seq > log_next_seq {
            return self.poison(format!(
                "hybrid recovery SQLite high-water {:?} for {}/{} is ahead of the object-log head {}; the \
                 projection absorbed commands the durable log does not contain",
                sqlite_high_water, shard.tenant_id, shard.queue_id, log_next_seq,
            ));
        }
        Ok(())
    }

    /// Surface the hybrid store's latched poison to the composition's recovery gate: a poisoned hybrid
    /// projection must stop serving (fail closed) rather than hydrate + advertise a divergent image
    /// (TD-004 §backpressure/poison). Feeds [`fireweed_engine::resolve_recovery_start`].
    fn recovery_poison(&self, shard: &QueueKey) -> Option<String> {
        self.poisoned.clone().or_else(|| {
            self.async_monitors
                .get(shard)
                .and_then(HybridAsyncMonitor::poison_reason)
                .map(str::to_owned)
        })
    }

    /// `objectlog/hybrid-async` (TD-004 "Recovery/high-water backpressure"): a store whose async-apply debt
    /// is Hard reports backpressure so recovery-on-open replays from an earlier authoritative source instead
    /// of trusting the lagging SQLite high-water. `false` when no monitor is armed (the non-async profiles).
    fn recovery_backpressured(&self, shard: &QueueKey) -> bool {
        matches!(
            self.async_monitors
                .get(shard)
                .map(HybridAsyncMonitor::level),
            Some(BackpressureLevel::Hard)
        )
    }

    /// The `objectlog/hybrid-async` mutation-admission gate (TD-004 "Hard debt threshold"). Fails new
    /// mutating admission CLOSED — with the typed retryable [`EngineError::Unavailable`] backpressure error
    /// (or a `Storage` poison error) — once that queue's async-apply backlog is over its hard budget, so a
    /// push/claim cannot pile more debt onto a queue already at risk of an SLO violation. The non-async
    /// profiles arm no monitor and admit unconditionally (the default trait impl semantics).
    fn admit_mutation(&mut self, shard: &QueueKey) -> EngineResult<()> {
        self.observe_async_debt(shard);
        self.check_healthy()?;
        if self.durable_projection_offline {
            return Err(EngineError::Unavailable);
        }
        match self.async_monitors.get(shard) {
            Some(monitor) => monitor.admit_mutation(),
            None => Ok(()),
        }
    }

    /// `objectlog/hybrid-async` (TD-004 "Retention backpressure"): withhold terminal-item retention /
    /// segment-expiry advancement while async-apply debt is over budget or the worker is poisoned. `true`
    /// when no monitor is armed (the non-async profiles have no async retention gate).
    fn retention_may_advance(&self, shard: &QueueKey) -> bool {
        self.async_monitors
            .get(shard)
            .is_none_or(HybridAsyncMonitor::retention_may_advance)
    }

    fn requires_complete_retention_frontier(&self) -> bool {
        self.async_thresholds.is_some()
    }

    fn complete_retention_frontier_is_proven(&self, _shard: &QueueKey) -> bool {
        // The current projection exposes ordered SQLite health/high-water, but the LogStore seam does not yet
        // supply committed object-snapshot recovery-window time plus durable item-key/request replay minima as
        // one immutable snapshot. Fail closed until that authority adapter lands; never promote local SQLite
        // state into object-log deletion authority.
        self.async_thresholds.is_none()
    }

    /// `objectlog/hybrid-async` (TD-004 "Retention advancement"): reclaim terminal-item retention from the
    /// DURABLE, checkpointed SQLite image. The composition gates this on [`Self::retention_may_advance`], so a
    /// reap tick under Hard async-apply debt (or a poisoned worker) is WITHHELD entirely (this override is not
    /// even reached) and only advances once the backlog drains below the release band.
    ///
    /// Gated to the armed-monitor profile: the non-async `objectlog/hybrid` and `objectlog/hybrid-strict`
    /// profiles keep the trait-default no-op, so their retention behavior is UNCHANGED (no async-apply debt
    /// gate, no reap).
    ///
    /// SAFE against recovery: [`reap_terminal_items_sql`] deletes only rows the DURABLE image shows terminal
    /// — an item is terminal in the durable image only once its terminal transition has been CHECKPOINTED
    /// (i.e. is at or below the durable SQLite high-water), so restart rehydrates neither from the SQLite
    /// image (row deleted) nor from the object-log tail replay (which resumes STRICTLY AFTER that high-water).
    /// Hot memory is intentionally left untouched: it is rebuilt from the durable image on restart, so
    /// reclaiming the durable rows IS the durable retention advancement, whereas over-reaping the hot set
    /// (which runs ahead of the checkpoint) could drop a terminal item whose finalize is still deferred and
    /// would then be resurrected by tail replay.
    fn reap_terminal_items(
        &mut self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<Vec<ItemId>> {
        if self.async_thresholds.is_none() {
            return Ok(Vec::new());
        }
        self.check_healthy()?;
        ProjectionStore::reap_terminal_items(
            &mut self.sqlite,
            shard,
            now,
            terminal_retention_ms,
            emit_change_records,
            emission_cursor,
        )
    }

    fn recover_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        self.check_healthy()?;
        Ok(self.sqlite.lock().queues.values().cloned().collect())
    }

    fn recover_definitions_page(
        &self,
        cursor: Option<&fireweed_engine::DefinitionCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<fireweed_engine::DefinitionPage> {
        self.check_healthy()?;
        fireweed_engine::definition_page_from_sorted_rows(
            self.sqlite.lock().queues.values().cloned(),
            cursor,
            limit,
            worker_partition,
        )
    }

    fn recovery_counter_high_water(&self, shard: &QueueKey) -> EngineResult<Option<ItemId>> {
        // Push apply advances the durable row atomically; retention reaping advances the same row. Return
        // the ceiling without requiring a hot image or touching live counters so create-loser publication
        // remains atomic.
        self.sqlite.recovery_counter_high_water(shard)
    }

    fn eligible_candidates(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.eligible_candidates(shard, now, max)
    }

    fn eligible_candidates_after(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        after: Option<ItemId>,
        max: usize,
    ) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory
            .eligible_candidates_after(shard, now, after, max)
    }

    fn render_claimed(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<ClaimedItem>> {
        self.require_hydrated(shard)?;
        self.memory.render_claimed(shard, ids)
    }

    fn lookup_by_key(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
    ) -> EngineResult<Option<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.lookup_by_key(shard, client_item_key)
    }

    fn item_state(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<ItemState>> {
        self.require_hydrated(shard)?;
        self.memory.item_state(shard, id)
    }

    fn item_version(&self, shard: &QueueKey, id: &ItemId) -> EngineResult<Option<u64>> {
        self.require_hydrated(shard)?;
        self.memory.item_version(shard, id)
    }

    fn batch_update_snapshot(
        &self,
        shard: &QueueKey,
        refs: &[BatchUpdateItemRef],
    ) -> EngineResult<Vec<BatchUpdateSnapshotItem>> {
        self.require_hydrated(shard)?;
        self.memory.batch_update_snapshot(shard, refs)
    }

    fn batch_update_preflight(
        &self,
        shard: &QueueKey,
        commands: &[UpdateFieldsCommand],
    ) -> EngineResult<Vec<bool>> {
        self.require_hydrated(shard)?;
        self.memory.batch_update_preflight(shard, commands)
    }

    fn expired_leases(&self, shard: &QueueKey, now: UtcTimestamp) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.expired_leases(shard, now)
    }

    fn all_expired_leases(&self, now: UtcTimestamp) -> Vec<(QueueKey, Vec<ItemId>)> {
        if self.poisoned.is_some() {
            return Vec::new();
        }
        self.memory.all_expired_leases(now)
    }

    fn expired_leases_page(
        &self,
        now: UtcTimestamp,
        cursor: Option<&fireweed_engine::ExpiredLeaseCursor>,
        limit: usize,
        worker_partition: Option<(usize, usize)>,
    ) -> EngineResult<fireweed_engine::ExpiredLeasePage> {
        if self.poisoned.is_some() {
            return Ok(fireweed_engine::ExpiredLeasePage::default());
        }
        self.memory
            .expired_leases_page(now, cursor, limit, worker_partition)
    }

    fn finalize_validate(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.finalize_validate(shard, outcomes)
    }

    fn renew_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.renew_validate(shard, ids)
    }

    fn reassign_validate(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.reassign_validate(shard, ids)
    }

    fn update_fields_validate(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        expected_item_version: Option<u64>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .update_fields_validate(shard, id, expected_item_version)
    }

    fn index_validate(
        &self,
        shard: &QueueKey,
        item_id: &ItemId,
        fields: &BTreeMap<String, Bytes>,
        entity: Option<&serde_json::Value>,
        exclude: Option<&ItemId>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .index_validate(shard, item_id, fields, entity, exclude)
    }

    fn index_validate_push(&self, shard: &QueueKey, items: &[PushItem]) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.index_validate_push(shard, items)
    }

    fn index_validate_replace(
        &self,
        shard: &QueueKey,
        existing_id: &ItemId,
        item: &PushItem,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.index_validate_replace(shard, existing_id, item)
    }

    fn index_validate_update(
        &self,
        shard: &QueueKey,
        id: &ItemId,
        field_ops: &BTreeMap<String, Option<Bytes>>,
        entity: Option<&serde_json::Value>,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory
            .index_validate_update(shard, id, field_ops, entity)
    }

    fn supports_commit_transition(&self) -> bool {
        self.poisoned.is_none() && self.memory.supports_commit_transition()
    }

    fn commit_validate(
        &self,
        shard: &QueueKey,
        refs: &[ClaimRef],
        now: UtcTimestamp,
    ) -> EngineResult<()> {
        self.require_hydrated(shard)?;
        self.memory.commit_validate(shard, refs, now)
    }

    fn instance_fence(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<u64>> {
        self.require_hydrated(shard)?;
        self.memory.instance_fence(shard, key)
    }

    fn side_record(&self, shard: &QueueKey, key: &[u8]) -> EngineResult<Option<Bytes>> {
        self.require_hydrated(shard)?;
        self.memory.side_record(shard, key)
    }

    fn side_records_by_prefix(
        &self,
        shard: &QueueKey,
        prefix: &[u8],
        page_size: usize,
        cursor: Option<Vec<u8>>,
    ) -> EngineResult<fireweed_engine::SideRecordPage> {
        self.require_hydrated(shard)?;
        self.memory
            .side_records_by_prefix(shard, prefix, page_size, cursor)
    }

    /// Prefer durable SQLite retained-commit rows when present (Strict/async checkpoint); fall back to
    /// the in-process default (None) only when the durable image has nothing for this request_id.
    fn replay_durable_commit(
        &mut self,
        shard: &QueueKey,
        request_id: &RequestId,
        fingerprint: u64,
        now: UtcTimestamp,
    ) -> EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>> {
        if let Some(entries) =
            self.sqlite
                .replay_durable_commit(shard, request_id, fingerprint, now)?
        {
            return Ok(Some(entries));
        }
        Ok(None)
    }

    fn read_durable_commit(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
    ) -> EngineResult<Option<Vec<fireweed_engine::CommitOutcomeEntry>>> {
        self.sqlite.read_durable_commit(shard, request_id)
    }

    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> EngineResult<Vec<ItemId>> {
        self.require_hydrated(shard)?;
        self.memory.select_eligible(shard, now, limit)
    }

    fn peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        self.require_hydrated(shard)?;
        self.memory.peek(shard, limit)
    }

    fn pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        self.require_hydrated(shard)?;
        self.memory.pending(shard)
    }

    fn pending_summary(&self, shard: &QueueKey) -> EngineResult<PendingSummary> {
        self.require_hydrated(shard)?;
        self.memory.pending_summary(shard)
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        self.require_hydrated(shard)?;
        self.memory.pending_page(shard, start, limit)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        self.require_hydrated(shard)?;
        self.memory
            .pending_range(shard, start, end, consumer, limit)
    }

    fn pending_by_ids(&self, shard: &QueueKey, ids: &[ItemId]) -> EngineResult<Vec<LeaseView>> {
        self.require_hydrated(shard)?;
        self.memory.pending_by_ids(shard, ids)
    }

    fn metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        self.require_hydrated(shard)?;
        self.memory.metrics(shard)
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> EngineResult<QueueMetrics> {
        self.require_hydrated(shard)?;
        self.memory.metrics_by_query(shard, request)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> EngineResult<TerminalEmissionMetrics> {
        self.require_hydrated(shard)?;
        let metrics = self.memory.metrics(shard)?;
        Ok(TerminalEmissionMetrics {
            resident_terminal_count: metrics.resident_terminal_count,
            emission_lag_commands: 0,
            emission_oldest_unemitted_age_ms: 0,
        })
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        self.require_hydrated(shard)?;
        self.memory.live_items(shard, keys)
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> EngineResult<RangeScanResponse> {
        self.require_hydrated(shard)?;
        self.memory.range_scan(shard, request)
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> EngineResult<GroupedAggregateResponse> {
        self.require_hydrated(shard)?;
        self.memory.grouped_aggregate(shard, request)
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> EngineResult<DeclaredBucketSegmentResponse> {
        self.require_hydrated(shard)?;
        self.memory.declared_bucket_segment(shard, request)
    }

    fn plan_bounded_mutation(
        &self,
        shard: &QueueKey,
        request: fireweed_core::BoundedMutationRequest,
    ) -> EngineResult<fireweed_engine::BoundedMutationPlan> {
        self.require_hydrated(shard)?;
        self.memory.plan_bounded_mutation(shard, request)
    }

    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Option<IndexHit>> {
        self.require_hydrated(shard)?;
        self.memory.index_get_unique(shard, index, key)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> EngineResult<Vec<IndexHit>> {
        self.require_hydrated(shard)?;
        self.memory.index_lookup(shard, index, key)
    }
}

impl AsOfProjectionStore for HybridProjectionStore {
    type AsOfProjection = InMemoryProjection;

    fn reconstruct_as_of(
        &self,
        definition: &QueueDefinition,
        snapshot: Option<ProjectionSnapshot>,
    ) -> EngineResult<Self::AsOfProjection> {
        self.check_healthy()?;
        self.sqlite.reconstruct_as_of(definition, snapshot)
    }
}
