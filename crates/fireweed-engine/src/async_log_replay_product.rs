//! Generic async atomic log-replay product (program B).
//!
//! One composition implements the full product-port surface for any
//! [`LogStore`] × [`ProjectionStore`] pair behind [`InProcessLogStore`] /
//! [`InProcessProjectionStore`]. Adapter crates only open axes and call
//! [`assemble_async_log_replay`] / [`AsyncLogReplayBackend::recover`].

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata,
    MetricsByQueryRequest, PriorityValue, QueueDefinition, QueueId, QueryCapabilityFlags, RequestId,
    TenantId, UtcTimestamp,
};

use crate::{
    AsOfProjectionStore, AsyncClaimError, AsyncComposedBackend, AsyncControlPlane,
    AsyncLifecycleError, AsyncLogStore, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError,
    AsyncPushRequest, AsyncReclaimRequest, Backend, ClaimPort, ClaimRequest, Claimed, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    FinalizePort, HotProjectionQueryPort, IdGen, IdempotencyDecision, InProcessControlPlane,
    InProcessLogStore, InProcessProjectionStore, IndexHit, IndexQueryPort, InlineOwnedTaskDispatcher,
    ItemView, LeaseView, LiveItemView, LogRead, LogStore, OwnedTask, PayloadUpdate, PendingPage,
    PendingSummary, ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner,
    ProjectionRead, ProjectionReclaimPlanner, ProjectionSnapshot, ProjectionStore, PurgePort,
    PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, QueueMetrics, RawCommitFault, RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeaseCommand, RenewLeasePort,
    ReplacePendingCommand, SnapshotRef, SnapshotStore, TerminalEmissionMetrics, TickReport,
    UnifiedAtomicCommit, UnifiedAtomicCommitter, UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome,
    UpsertPort, compile_entity_schema, validate_api001_reserved_write_fields, validate_entity,
};

/// Sequential id generation for async log-replay products.
#[derive(Default)]
pub struct SeqIdGen {
    counter: AtomicU64,
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        ItemId::from_u64(n)
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

/// Shared push request-id cache (parity with sync `ComposedBackend` in-memory idempotency).
type PushIdempotency = Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>;

/// Append + apply under the composition's queue gate (atomic memory profile).
#[derive(Clone)]
pub struct AtomicLogReplayCommitter<L, P> {
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    push_idempotency: Arc<PushIdempotency>,
}

impl<L, P> AtomicLogReplayCommitter<L, P> {
    fn new(
        log: Arc<InProcessLogStore<L>>,
        projection: Arc<InProcessProjectionStore<P>>,
        control: Arc<InProcessControlPlane>,
        push_idempotency: Arc<PushIdempotency>,
    ) -> Self {
        Self {
            log,
            projection,
            control,
            push_idempotency,
        }
    }
}

impl<L, P> UnifiedAtomicCommitter for AtomicLogReplayCommitter<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let push_idempotency = Arc::clone(&self.push_idempotency);
        Box::pin(async move {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            match fault {
                RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                RawCommitFault::None | RawCommitFault::AfterAppendBeforeApply => {}
            }
            let definition =
                AsyncControlPlane::queue_definition(control.as_ref(), shard.clone()).await?;
            for env in &commands {
                crate::validate_gate_command_definition(&definition, &env.command)?;
            }
            let positions = AsyncLogStore::append(
                log.as_ref(),
                shard.clone(),
                commands.clone(),
                expected_epoch,
            )
            .await?;
            if matches!(fault, RawCommitFault::AfterAppendBeforeApply) {
                return Ok(RawCommitOutcome::appended(positions));
            }
            AsyncProjectionStore::apply_live(projection.as_ref(), positions.clone(), commands.clone())
                .await?;
            // Record push request-id outcomes after a successful atomic apply (sync composition parity).
            let mut cache = push_idempotency
                .lock()
                .expect("push idempotency mutex poisoned");
            for env in &commands {
                let Some(request_id) = env.request_id.clone() else {
                    continue;
                };
                let QueueCommand::Push(_) = &env.command else {
                    continue;
                };
                let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                let expires_at = UtcTimestamp::new(
                    env.created_at.seconds.saturating_add(60),
                    env.created_at.nanoseconds,
                )
                .unwrap_or(env.created_at);
                let ids = match &env.request_outcome {
                    Some(crate::RequestOutcome::Push { item_ids }) => item_ids.clone(),
                    _ => env.item_ids.clone(),
                };
                cache.entry(shard.clone()).or_default().record(
                    request_id,
                    fingerprint,
                    ids,
                    expires_at,
                );
            }
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

/// Projection axis with in-process push request-id cache (AsyncProjectionStore).
#[derive(Clone)]
struct PushIdempotentProjection<P> {
    inner: Arc<InProcessProjectionStore<P>>,
    push_idempotency: Arc<PushIdempotency>,
}

impl<P> AsyncProjectionStore for PushIdempotentProjection<P>
where
    P: ProjectionStore + Send + 'static,
{
    fn supports_gates(&self) -> bool {
        self.inner.supports_gates()
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::ensure_shard(self.inner.as_ref(), definition)
    }

    fn admit_mutation(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::admit_mutation(self.inner.as_ref(), shard)
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::validate_push(self.inner.as_ref(), shard, items, now)
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<bool>> + Send {
        AsyncProjectionStore::pause_blocks_intake(self.inner.as_ref(), shard)
    }

    fn push_idempotency(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: crate::PushFingerprint,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
    {
        let cache = Arc::clone(&self.push_idempotency);
        std::future::ready({
            let guard = cache.lock().expect("push idempotency mutex poisoned");
            Ok(guard
                .get(&shard)
                .map(|c| c.check(&request_id, fingerprint.legacy_body_hash, now))
                .unwrap_or(IdempotencyDecision::Proceed))
        })
    }

    fn renew_validate(
        &self,
        shard: QueueKey,
        targets: Vec<crate::RenewTarget>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::renew_validate(self.inner.as_ref(), shard, targets, now)
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<crate::FinalizeTarget>,
        now: UtcTimestamp,
        default_max_attempts: u32,
    ) -> impl std::future::Future<
        Output = EngineResult<Vec<crate::FinalizeLeaseMember>>,
    > + Send {
        AsyncProjectionStore::finalize_validate(
            self.inner.as_ref(),
            shard,
            targets,
            now,
            default_max_attempts,
        )
    }

    fn purge_validate(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::purge_validate(self.inner.as_ref(), shard, ids, force)
    }

    fn expired_leases(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::expired_leases(self.inner.as_ref(), shard, now, max)
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::apply_live(self.inner.as_ref(), positions, commands)
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::apply_recovery(self.inner.as_ref(), positions, commands)
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::eligible_candidates(self.inner.as_ref(), shard, now, max)
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: crate::ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::select_item_claim(self.inner.as_ref(), shard, compatibility, now, max)
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: crate::ClaimUnit,
        compatibility: crate::ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl std::future::Future<Output = EngineResult<crate::RichClaimSelection>> + Send
    {
        AsyncProjectionStore::select_rich_claim(
            self.inner.as_ref(),
            shard,
            unit,
            compatibility,
            now,
            max_items,
        )
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ClaimedItem>>> + Send
    {
        AsyncProjectionStore::render_claimed(self.inner.as_ref(), shard, ids)
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send {
        AsyncProjectionStore::item_state(self.inner.as_ref(), shard, id)
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        AsyncProjectionStore::item_version(self.inner.as_ref(), shard, id)
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        AsyncProjectionStore::recovery_high_water(self.inner.as_ref(), shard)
    }

    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        AsyncProjectionStore::recover_definitions(self.inner.as_ref())
    }
}

type Strategy<L, P> = UnifiedAtomicCommit<AtomicLogReplayCommitter<L, P>>;
type ClaimPlanner<L, P> = ProjectionClaimPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type PushPlanner<L, P> = ProjectionPushPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type LifecyclePlanner<L, P> = ProjectionLifecyclePlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type ReclaimPlanner<L, P> = ProjectionReclaimPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type AsyncEngine<L, P> = AsyncComposedBackend<
    Strategy<L, P>,
    InlineOwnedTaskDispatcher,
    ClaimPlanner<L, P>,
    PushPlanner<L, P>,
    LifecyclePlanner<L, P>,
    ReclaimPlanner<L, P>,
>;

/// Atomic async log-replay product: full product ports over any log × projection axes.
pub struct AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    engine: AsyncEngine<L, P>,
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    push_idempotency: Arc<PushIdempotency>,
    commit_idempotency:
        Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<crate::EntryRecovery>>>>>,
}

/// Assemble a fresh async log-replay product over the given axes (no recovery).
pub fn assemble_async_log_replay<L, P>(
    log: L,
    projection: P,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    let log = Arc::new(InProcessLogStore::new(log));
    let projection = Arc::new(InProcessProjectionStore::new(projection));
    let push_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let commit_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let control = Arc::new(InProcessControlPlane::new());
    let ids = Arc::new(SeqIdGen::default());
    let counters = Arc::new(QueueCounters::default());
    assemble_async_log_replay_from_parts(
        log,
        projection,
        control,
        ids,
        counters,
        push_idempotency,
        commit_idempotency,
        node_id,
    )
}

/// Assemble from already-shared axis handles (used by [`AsyncLogReplayBackend::with_node_id`]).
pub fn assemble_async_log_replay_from_parts<L, P>(
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    push_idempotency: Arc<PushIdempotency>,
    commit_idempotency: Arc<
        Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<crate::EntryRecovery>>>>,
    >,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    let axis = Arc::new(PushIdempotentProjection {
        inner: Arc::clone(&projection),
        push_idempotency: Arc::clone(&push_idempotency),
    });
    let committer = AtomicLogReplayCommitter::new(
        Arc::clone(&log),
        Arc::clone(&projection),
        Arc::clone(&control),
        Arc::clone(&push_idempotency),
    );
    let strategy = UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer)
        .map_err(|e| EngineError::Storage(e.to_string()))?;
    let claim = ProjectionClaimPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
    );
    let push = ProjectionPushPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
        Arc::clone(&counters),
        node_id,
    );
    let lifecycle = ProjectionLifecyclePlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
    );
    let reclaim = ProjectionReclaimPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
    );
    let engine = AsyncComposedBackend::new_with_planners(
        strategy,
        InlineOwnedTaskDispatcher::new(),
        claim,
        push,
        1024,
    )
    .with_lifecycle_planner(lifecycle)
    .with_reclaim_planner(reclaim);
    Ok(AsyncLogReplayBackend {
        engine,
        log,
        projection,
        control,
        ids,
        counters,
        node_id,
        push_idempotency,
        commit_idempotency,
    })
}

impl<L, P> AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    /// Rebuild planners so push minting uses `node_id` (preserves log + projection state).
    pub fn with_node_id(self, node_id: u8) -> Self {
        assemble_async_log_replay_from_parts(
            self.log,
            self.projection,
            self.control,
            self.ids,
            self.counters,
            self.push_idempotency,
            self.commit_idempotency,
            node_id,
        )
        .expect("rebuild with_node_id")
    }


    /// Observability/test seam: run `f` against the log under its mutex.
    pub fn with_log<R>(&self, f: impl FnOnce(&L) -> R) -> R {
        self.log.with_store(f)
    }

    /// Observability/test seam: run `f` against the projection under its mutex.
    pub fn with_projection<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        self.projection.with_store(f)
    }

    /// Emit durable change-record tail from the log emission cursor (parity with sync composition).
    pub fn emit_change_record_tail<S: crate::ChangeRecordSink + ?Sized>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize> {
        use crate::{LogStore, command_envelope_change_records};

        let cursor = self
            .log
            .with_store(|log| LogStore::emission_cursor(log, shard))?;
        let page = self
            .log
            .with_store(|log| LogStore::read_from(log, shard, cursor, limit))?;
        if page.entries.is_empty() {
            return Ok(0);
        }
        let mut records = Vec::new();
        for (position, env) in &page.entries {
            records.extend(command_envelope_change_records(
                shard,
                position,
                env,
                emitted_at,
                source_owner_id.clone(),
            ));
        }
        sink.emit(shard, &records)?;
        if let Some((position, _)) = page.entries.last() {
            let position = position.clone();
            self.log.with_store_mut(|log| {
                LogStore::set_emission_cursor(log, shard, position)
            })?;
        }
        Ok(records.len())
    }

    /// Replay the durable log into the in-memory projection and control plane (ADR-012 recovery-on-open).
    pub fn recover(self) -> EngineResult<Self> {
        use crate::{ControlPlane, LogStore, RequestOutcome, request_expires_at};

        let definitions = self
            .log
            .with_store(|log| LogStore::recover_definitions(log))?;
        for definition in definitions {
            let retention_ms = definition.request_id_retention_ms;
            let shard = QueueKey::new(
                definition.tenant_id.clone(),
                definition.queue_id.clone(),
            );
            let _ = ControlPlane::create_queue(self.control.as_ref(), definition.clone());
            self.projection
                .with_store_mut(|p| ProjectionStore::ensure_shard(p, &definition))?;
            let mut from = None;
            loop {
                let page = self.log.with_store(|log| {
                    LogStore::read_from(log, &shard, from.clone(), 256)
                })?;
                if page.entries.is_empty() {
                    break;
                }
                let positions: Vec<_> = page.entries.iter().map(|(p, _)| p.clone()).collect();
                let commands: Vec<_> = page.entries.iter().map(|(_, e)| e.clone()).collect();
                self.projection.with_store_mut(|p| {
                    ProjectionStore::apply_recovery(p, &positions, &commands)
                })?;
                // Rebuild request-id caches from durable envelopes (push + commit-transition).
                {
                    let mut push_cache = self
                        .push_idempotency
                        .lock()
                        .expect("push idempotency poisoned");
                    for (_, env) in &page.entries {
                        let Some(request_id) = &env.request_id else {
                            continue;
                        };
                        if let QueueCommand::Push(_) = &env.command {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at =
                                request_expires_at(env.created_at, retention_ms);
                            let ids = match &env.request_outcome {
                                Some(RequestOutcome::Push { item_ids }) => item_ids.clone(),
                                _ => env.item_ids.clone(),
                            };
                            push_cache.entry(shard.clone()).or_default().record(
                                request_id.clone(),
                                fingerprint,
                                ids,
                                expires_at,
                            );
                        }
                        if let Some(RequestOutcome::CommitTransition { entries }) =
                            &env.request_outcome
                        {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at =
                                request_expires_at(env.created_at, retention_ms);
                            let recovery = entries
                                .iter()
                                .cloned()
                                .map(crate::recovery_from_outcome_entry)
                                .collect::<Vec<_>>();
                            self.commit_idempotency
                                .lock()
                                .expect("commit idempotency poisoned")
                                .entry(shard.clone())
                                .or_default()
                                .record(
                                    request_id.clone(),
                                    fingerprint,
                                    recovery,
                                    expires_at,
                                );
                        }
                    }
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
            // Seed mint counters past every item id materialised by recovery so a post-reopen
            // push/upsert never re-mints a live id (would corrupt the eligibility index via
            // insert_pending overwrite — fireweed-6e38e2b4).
            self.projection.with_store(|p| {
                ProjectionStore::restore_counters(p, &shard, &self.counters)
            })?;
        }
        Ok(self)
    }

    /// No-op group-commit flush (memory / non-buffered log axes have no buffered segments).
    pub async fn flush_tick_async(&self, _now_ms: i64) -> EngineResult<()> {
        Ok(())
    }

    /// Drain deferred projection work when the projection axis supports it (hybrid checkpoint).
    pub fn flush_deferred_projection(&self) -> EngineResult<()> {
        self.projection
            .with_store_mut(|p| ProjectionStore::flush_deferred(p))
    }

    /// Async entrypoint for deferred projection drain.
    pub async fn flush_deferred_projection_async(&self) -> EngineResult<()> {
        self.flush_deferred_projection()
    }

    /// No-op object-log retention trim (no segment store).
    pub async fn trim_reclaimable_segments_async(
        &self,
        _shard: QueueKey,
        _request_id_retention_ms: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<crate::MaintenanceSummary> {
        Ok(crate::MaintenanceSummary::default())
    }

    /// No-op recover (in-process memory has no durable reopen path).
    pub async fn recover_async(&self) -> EngineResult<()> {
        Ok(())
    }

    fn map_claim(error: AsyncClaimError) -> EngineError {
        match error {
            AsyncClaimError::BeforeCommit(error) | AsyncClaimError::Commit(error) => error,
            AsyncClaimError::AfterCommit { source, .. } => source,
            AsyncClaimError::Submit(error) => {
                EngineError::Storage(format!("async claim submission failed: {error:?}"))
            }
        }
    }

    fn map_push(error: AsyncPushError) -> EngineError {
        match error {
            AsyncPushError::BeforeCommit(error) | AsyncPushError::Commit(error) => error,
            AsyncPushError::AfterCommit { source, .. } => source,
            AsyncPushError::Submit(error) => {
                EngineError::Storage(format!("async push submission failed: {error:?}"))
            }
        }
    }

    fn map_lifecycle(error: AsyncLifecycleError) -> EngineError {
        match error {
            AsyncLifecycleError::BeforeCommit(error) | AsyncLifecycleError::Commit(error) => error,
            AsyncLifecycleError::AfterCommit { source, .. } => source,
            AsyncLifecycleError::Submit(error) => {
                EngineError::Storage(format!("async lifecycle submission failed: {error:?}"))
            }
        }
    }

    fn make_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    async fn resolve_epoch(
        &self,
        shard: &QueueKey,
        expected_epoch: Option<u64>,
    ) -> EngineResult<u64> {
        let epoch = AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
        if expected_epoch.is_some_and(|expected| expected != epoch) {
            return Err(EngineError::EpochFenced);
        }
        Ok(epoch)
    }

    async fn commit_envelope(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<RawCommitOutcome> {
        let epoch = self.resolve_epoch(shard, expected_epoch).await?;
        self.engine
            .submit_commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
            .await
            .map_err(|error| {
                EngineError::Storage(format!("async commit submission failed: {error:?}"))
            })?
    }

}

impl<L, P> Backend for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }

    fn commit_capabilities(&self) -> crate::CommitCapabilities {
        // P advertises full commit-transition support (Snorri StateStore parity).
        crate::CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic durable log batch with synchronous projection apply (async log-replay composition)",
        }
    }

    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<RawCommitOutcome>> + Send {
        async move {
            self.engine.submit_commit(request).await.map_err(|error| {
                EngineError::Storage(format!("async raw commit submission failed: {error:?}"))
            })?
        }
    }
}

impl<L, P> ControlPlaneStore for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            use crate::{ControlPlane, LogStore};

            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let mut outcome = ControlPlane::create_queue(self.control.as_ref(), definition)?;
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            // Durable catalog so reopen / second handle can recover without re-create_queue.
            if let Some(durable) = self.log.with_store_mut(|log| {
                LogStore::create_or_read_definition(log, &outcome.definition)
            })? {
                let matches = durable.definition == outcome.definition;
                ControlPlane::cache_authoritative_definition(
                    self.control.as_ref(),
                    durable.definition.clone(),
                )?;
                outcome = durable;
                if !matches {
                    return Err(EngineError::QueueDefinitionConflict);
                }
            }
            // Ensure projection exists; if this handle is late to an already-created durable
            // queue (no local serving image yet), replay the log tail so the serving image
            // catches commands committed before create.
            //
            // Replay ONLY when the projection shard is missing (`needs_replay`). An
            // idempotent re-create (`!outcome.created`) against a live image must not re-apply
            // the durable log: `apply_recovery` defaults to plain `apply`, so replaying Push
            // (including commit_transition lifecycle items) would insert a second eligibility
            // index row per item_id (stale created_seq) and make ClaimPort::claim fail with
            // Invalid("invalid async claim plan") after select_item_claim returns duplicates
            // (fireweed-6e38e2b4 / snorri v0.24 regression).
            let needs_replay = self
                .projection
                .with_store(|p| ProjectionStore::metrics(p, &shard).is_err());
            AsyncProjectionStore::ensure_shard(
                self.projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            if needs_replay {
                let mut from = None;
                loop {
                    let page = self.log.with_store(|log| {
                        LogStore::read_from(log, &shard, from.clone(), 256)
                    })?;
                    if page.entries.is_empty() {
                        break;
                    }
                    let positions: Vec<_> =
                        page.entries.iter().map(|(p, _)| p.clone()).collect();
                    let commands: Vec<_> =
                        page.entries.iter().map(|(_, e)| e.clone()).collect();
                    self.projection.with_store_mut(|p| {
                        ProjectionStore::apply_recovery(p, &positions, &commands)
                    })?;
                    match page.next {
                        Some(next) => from = Some(next),
                        None => break,
                    }
                }
                // Late-join create_queue replayed into an empty image — seed mint counters
                // the same way recover() does after full-log rebuild.
                self.projection.with_store(|p| {
                    ProjectionStore::restore_counters(p, &shard, &self.counters)
                })?;
            }
            Ok(outcome)
        }
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        AsyncControlPlane::queue_definition(self.control.as_ref(), key.clone())
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        AsyncControlPlane::list_queues(self.control.as_ref(), tenant.clone())
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone())
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        AsyncLogStore::acquire_epoch(self.log.as_ref(), shard.clone())
    }

    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            let mut current =
                AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
            if current > target_epoch {
                return Err(EngineError::EpochFenced);
            }
            while current < target_epoch {
                current = AsyncLogStore::acquire_epoch(self.log.as_ref(), shard.clone()).await?;
            }
            Ok(current)
        }
    }
}

impl<L, P> PushPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            let outcome = self
                .engine
                .push(AsyncPushRequest {
                    shard: shard.clone(),
                    request_id: None,
                    items,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_push)?;
            Ok(outcome.into_item_ids())
        }
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<crate::PushBatchOutcome>> + Send
    {
        async move {
            self.engine
                .push(AsyncPushRequest {
                    shard: shard.clone(),
                    request_id: Some(request_id),
                    items,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_push)
        }
    }
}

impl<L, P> ClaimPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl<L, P> FinalizePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // Match sync ComposedBackend: lease-state validate then append Finalize (no token pre-gate).
        async move {
            self.projection.with_store(|projection| {
                ProjectionStore::finalize_validate(projection, shard, &outcomes)
            })?;
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let outcomes = outcomes
                .into_iter()
                .map(|mut outcome| {
                    outcome.applied_state = match outcome.kind {
                        FinalizeKind::Complete => Some(ItemState::Complete),
                        FinalizeKind::Fail => Some(ItemState::Failed),
                        FinalizeKind::Retry => None,
                        FinalizeKind::Release | FinalizeKind::Rearm => Some(ItemState::Pending),
                    };
                    outcome
                })
                .collect::<Vec<_>>();
            let envelope = self.make_envelope(
                QueueCommand::Finalize(FinalizeCommand { outcomes }),
                item_ids,
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            Ok(())
        }
    }
}

impl<L, P> RenewLeasePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            self.projection.with_store(|projection| {
                ProjectionStore::renew_validate(projection, shard, &item_ids)
            })?;
            let envelope = self.make_envelope(
                QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            Ok(())
        }
    }
}

impl<L, P> ReassignLeasePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            self.projection.with_store(|projection| {
                ProjectionStore::reassign_validate(projection, shard, &item_ids)
            })?;
            let envelope = self.make_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            Ok(())
        }
    }
}

impl<L, P> PurgePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            self.engine
                .purge(AsyncPurgeRequest {
                    shard: shard.clone(),
                    item_ids,
                    force,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl<L, P> UpsertPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let client_item_key = client_item_key.clone();
        async move {
            let def = AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone())
                .await?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            let max_attempts = def.retry_policy.max_attempts;
            let epoch = self.resolve_epoch(shard, expected_epoch).await?;
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: entity,
            };

            let plan = self.projection.with_store(|projection| -> EngineResult<_> {
                let existing = ProjectionStore::lookup_by_key(projection, shard, &client_item_key)?;
                match existing {
                    None => {
                        ProjectionStore::index_validate(
                            projection,
                            shard,
                            &item.item_id,
                            &item.fields,
                            item.entity_document.as_ref(),
                            None,
                        )?;
                        Ok(UpsertPlan::Insert(item))
                    }
                    Some(existing_id) => {
                        let state = ProjectionStore::item_state(projection, shard, &existing_id)?
                            .ok_or(EngineError::NotFound)?;
                        match state {
                            ItemState::Pending => {
                                ProjectionStore::index_validate_replace(
                                    projection,
                                    shard,
                                    &existing_id,
                                    &item,
                                )?;
                                Ok(UpsertPlan::Replace {
                                    existing_id,
                                    item,
                                })
                            }
                            ItemState::Leased => {
                                Err(EngineError::Invalid("collision with claimed item"))
                            }
                            ItemState::Complete | ItemState::Failed => Err(EngineError::Terminal),
                        }
                    }
                }
            })?;

            match plan {
                UpsertPlan::Insert(item) => {
                    let envelope = self.make_envelope(
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        vec![new_item_id],
                        now,
                    );
                    self.commit_envelope(shard, envelope, Some(epoch)).await?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                UpsertPlan::Replace { existing_id, item } => {
                    let envelope = self.make_envelope(
                        QueueCommand::ReplacePending(ReplacePendingCommand {
                            client_item_key,
                            superseded_item_id: existing_id,
                            replacement: item,
                        }),
                        vec![new_item_id],
                        now,
                    );
                    self.commit_envelope(shard, envelope, Some(epoch)).await?;
                    Ok(UpsertOutcome::Replaced {
                        new_item_id,
                        superseded_item_id: existing_id,
                    })
                }
            }
        }
    }
}

enum UpsertPlan {
    Insert(PushItem),
    Replace {
        existing_id: ItemId,
        item: PushItem,
    },
}

impl<L, P> UpdateFieldsPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            validate_api001_reserved_write_fields(&field_ops)?;
            let def = AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone())
                .await?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            self.projection.with_store(|projection| {
                ProjectionStore::update_fields_validate(
                    projection,
                    shard,
                    &item_id,
                    expected_item_version,
                )?;
                ProjectionStore::index_validate_update(
                    projection,
                    shard,
                    &item_id,
                    &field_ops,
                    entity.as_ref(),
                )
            })?;
            let envelope = self.make_envelope(
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops,
                    payload,
                    set_priority: Default::default(),
                    set_not_before: Default::default(),
                    set_entity_document: entity,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![item_id],
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            self.projection
                .with_store(|projection| {
                    ProjectionStore::item_version(projection, shard, &item_id)?
                        .ok_or(EngineError::NotFound)
                })
        }
    }
}

impl<L, P> ReclaimPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            self.engine
                .reclaim_expired(AsyncReclaimRequest {
                    shard: shard.clone(),
                    limit,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl<L, P> ReclaimDriver for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        async move {
            // Scan every projection shard for expired leases (parity with sync composition's
            // expired_leases_page sweep — definitions are not required).
            let expired = self
                .projection
                .with_store(|projection| ProjectionStore::all_expired_leases(projection, now));
            let mut leases_reclaimed = 0u64;
            for (shard, _ids) in expired {
                let reclaimed = ReclaimPort::reclaim_expired(self, &shard, None, now, None)
                    .await?
                    .len() as u64;
                leases_reclaimed += reclaimed;
            }
            Ok(TickReport {
                leases_reclaimed,
                ..TickReport::default()
            })
        }
    }
}

impl<L, P> ProjectionRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::select_eligible(projection, shard, now, limit)
        }))
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::peek(projection, shard, limit)),
        )
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::pending(projection, shard)),
        )
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::pending_summary(projection, shard)),
        )
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::pending_page(projection, shard, start, limit)
        }))
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let consumer = consumer.cloned();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::pending_range(
                projection,
                shard,
                start,
                end,
                consumer.as_ref(),
                limit,
            )
        }))
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let ids = ids.to_vec();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::pending_by_ids(projection, shard, &ids)
        }))
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ClaimedItem>>> + Send
    {
        let ids = ids.to_vec();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::render_claimed(projection, shard, &ids)
        }))
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let keys = keys.to_vec();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::live_items(projection, shard, &keys)
        }))
    }

    fn metrics(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::metrics(projection, shard)),
        )
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let emission_cursor = emission_cursor.cloned();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::terminal_emission_metrics(
                projection,
                shard,
                now,
                emit_change_records,
                emission_cursor.as_ref(),
            )
        }))
    }
}

impl<L, P> LogRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        AsyncLogStore::read_from(self.log.as_ref(), shard.clone(), from, limit)
    }
}

impl<L, P> SnapshotStore for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        AsyncLogStore::write_snapshot(self.log.as_ref(), shard.clone(), position, snapshot)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        AsyncLogStore::latest_snapshot(self.log.as_ref(), shard.clone())
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        AsyncLogStore::read_snapshot(self.log.as_ref(), snapshot_ref.clone())
    }

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let position = position.clone();
        AsyncLogStore::snapshot_at_or_before(self.log.as_ref(), shard.clone(), position)
    }

    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncLogStore::set_high_water(self.log.as_ref(), shard.clone(), position)
    }
}

impl<L, P> IndexQueryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let index = index.to_string();
        let key = key.to_vec();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::index_get_unique(projection, shard, &index, &key)
        }))
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let index = index.to_string();
        let key = key.to_vec();
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::index_lookup(projection, shard, &index, &key)
        }))
    }
}

impl<L, P> HotProjectionQueryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        self.projection
            .with_store(ProjectionStore::hot_projection_capabilities)
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: fireweed_core::RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::RangeScanResponse>> + Send
    {
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::range_scan(projection, shard, request)),
        )
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: fireweed_core::GroupedAggregateRequest,
    ) -> impl std::future::Future<
        Output = EngineResult<fireweed_core::GroupedAggregateResponse>,
    > + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::grouped_aggregate(projection, shard, request)
        }))
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::metrics_by_query(projection, shard, request)
        }))
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: fireweed_core::DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<
        Output = EngineResult<fireweed_core::DeclaredBucketSegmentResponse>,
    > + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::declared_bucket_segment(projection, shard, request)
        }))
    }
}

impl<L, P> crate::SetGatesPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: crate::SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let envelope = self.make_envelope(QueueCommand::SetGates(command), Vec::new(), now);
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            Ok(())
        }
    }
}

impl<L, P> crate::ReschedulePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn reschedule(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        set_priority: crate::ScheduleUpdate<PriorityValue>,
        set_not_before: crate::ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            self.projection.with_store(|projection| {
                ProjectionStore::update_fields_validate(
                    projection,
                    shard,
                    &item_id,
                    expected_item_version,
                )
            })?;
            let envelope = self.make_envelope(
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops: BTreeMap::new(),
                    payload: PayloadUpdate::Keep,
                    set_priority,
                    set_not_before,
                    set_entity_document: None,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![item_id],
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch).await?;
            self.projection.with_store(|projection| {
                ProjectionStore::item_version(projection, shard, &item_id)?
                    .ok_or(EngineError::NotFound)
            })
        }
    }
}

impl<L, P> crate::DiscoveryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: crate::DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<
        Output = EngineResult<Vec<crate::ActiveScope>>,
    > + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::discover_active_scopes(projection, shard, granularity, now)
        }))
    }
}

impl<L, P> crate::RecoveryReadPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<
        Output = EngineResult<Option<crate::CommitRecovery>>,
    > + Send {
        let cache = Arc::clone(&self.commit_idempotency);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            if let Some(recovery) = cache
                .lock()
                .expect("commit idempotency poisoned")
                .get(&shard)
                .and_then(|c| c.peek(&request_id))
            {
                return Ok(Some(crate::CommitRecovery {
                    request_id,
                    entries: recovery,
                }));
            }
            let durable = projection.with_store(|p| {
                ProjectionStore::read_durable_commit(p, &shard, &request_id)
            })?;
            Ok(durable.map(|entries| crate::CommitRecovery {
                request_id,
                entries: entries
                    .into_iter()
                    .map(crate::recovery_from_outcome_entry)
                    .collect(),
            }))
        }
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let key = key.to_vec();
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::side_record(projection, shard, &key)),
        )
    }
}

impl<L, P> crate::HistoricalProjectionRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + AsOfProjectionStore + Send + 'static,
    P::AsOfProjection: ProjectionStore + Send + 'static,
{
    type AsOfProjection = P::AsOfProjection;

    fn current_position(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<CommandPosition>> + Send {
        async move {
            AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
                .await?
                .ok_or(EngineError::NotFound)
        }
    }

    fn read_as_of<T, F>(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static,
    {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let shard = shard.clone();
        async move {
            use crate::AsOfProjectionStore;
            if !projection.with_store(|p| p.supports_as_of()) {
                return Err(EngineError::Unavailable);
            }
            let definition =
                AsyncControlPlane::queue_definition(control.as_ref(), shard.clone()).await?;
            let snapshot_ref =
                AsyncLogStore::snapshot_at_or_before(log.as_ref(), shard.clone(), position.clone())
                    .await?;
            let snapshot = match snapshot_ref.as_ref() {
                Some(r) => Some(AsyncLogStore::read_snapshot(log.as_ref(), r.clone()).await?),
                None => None,
            };
            let mut as_of = projection.with_store(|p| {
                AsOfProjectionStore::reconstruct_as_of(p, &definition, snapshot)
            })?;
            let mut from = snapshot_ref.map(|s| s.position);
            loop {
                let page =
                    AsyncLogStore::read_from(log.as_ref(), shard.clone(), from.clone(), 256).await?;
                if page.entries.is_empty() {
                    break;
                }
                let mut positions = Vec::new();
                let mut envelopes = Vec::new();
                let mut reached_target = false;
                for (entry_position, env) in page.entries {
                    if entry_position == position || entry_position.precedes(&position) {
                        positions.push(entry_position.clone());
                        envelopes.push(env);
                    } else {
                        reached_target = true;
                        break;
                    }
                }
                if !positions.is_empty() {
                    ProjectionStore::apply_recovery(&mut as_of, &positions, &envelopes)?;
                }
                if reached_target || page.next.is_none() {
                    break;
                }
                from = page.next;
            }
            query(&as_of)
        }
    }
}

impl<L, P> crate::BatchUpdatePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{}

impl<L, P> crate::ItemMutationPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: crate::ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<
        Output = EngineResult<crate::ItemMutationResponse>,
    > + Send {
        let shard = shard.clone();
        async move {
            use crate::{RequestOutcome, item_mutation_fingerprint};

            let fingerprint = BodyHash(item_mutation_fingerprint(&request)?);
            let request_id = request.request_id.clone();
            let evaluated_at = request.evaluated_at;

            if let Some(response) = self.projection.with_store_mut(|p| {
                ProjectionStore::replay_durable_item_mutation(
                    p,
                    &shard,
                    &request_id,
                    fingerprint.0,
                    evaluated_at,
                )
            })? {
                return Ok(response);
            }

            // Scan log for retained request-id outcome.
            let mut from = self
                .log
                .with_store(|l| crate::LogStore::retention_floor(l, &shard))?;
            loop {
                let page = AsyncLogStore::read_from(
                    self.log.as_ref(),
                    shard.clone(),
                    from.clone(),
                    256,
                )
                .await?;
                for (position, envelope) in &page.entries {
                    if envelope.request_id.as_ref() != Some(&request_id) {
                        continue;
                    }
                    if envelope.request_fingerprint != Some(fingerprint.0) {
                        return Err(EngineError::RequestIdConflict);
                    }
                    let Some(RequestOutcome::ItemMutation { response_payload }) =
                        envelope.request_outcome.as_ref()
                    else {
                        return Err(EngineError::RequestIdConflict);
                    };
                    let mut response: crate::ItemMutationResponse =
                        serde_json::from_str(response_payload)
                            .map_err(|e| EngineError::Storage(e.to_string()))?;
                    response.position = Some(position.clone());
                    return Ok(response);
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }

            let mut plan = self.projection.with_store_mut(|p| {
                ProjectionStore::plan_item_mutation(p, &shard, &request)
            })?;
            if request.dry_run {
                return Ok(plan.response);
            }
            let response_payload = serde_json::to_string(&plan.response)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            let item_ids = plan.command.items.iter().map(|i| i.item_id).collect();
            let mut envelope = self.make_envelope(
                QueueCommand::MutateItems(plan.command),
                item_ids,
                evaluated_at,
            );
            envelope.request_id = Some(request_id);
            envelope.request_fingerprint = Some(fingerprint.0);
            envelope.request_outcome = Some(RequestOutcome::ItemMutation { response_payload });
            self.commit_envelope(&shard, envelope, expected_epoch).await?;
            plan.response.position = Some(
                AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
                    .await?
                    .ok_or(EngineError::NotFound)?,
            );
            Ok(plan.response)
        }
    }
}

impl<L, P> crate::CommitTransitionPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: crate::CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<
        Output = EngineResult<Vec<crate::CommitEntryOutcome>>,
    > + Send {
        let shard = shard.clone();
        async move {
            use crate::{
                AdvanceInstanceFenceCommand, CommitEntryStatus, CommitOutcomeEntry,
                CommitTransitionEntry, EntryRecovery, FinalizeOutcome, RequestOutcome,
                WriteSideRecordsCommand, build_push_items, commit_body_hash,
                outcome_entry_from_recovery, outcomes_from_recovery, request_expires_at,
                validate_distinct_commit_claims, validate_entity, validate_instance_fence,
            };
            use std::collections::HashSet;

            let crate::CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_body_hash(&entries)?;
            let definition =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            let max_attempts = definition.retry_policy.max_attempts;
            let retention = definition.request_id_retention_ms;
            let schema = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;

            if let Some(rid) = &request_id {
                let cached = {
                    let cache = self
                        .commit_idempotency
                        .lock()
                        .expect("commit idempotency poisoned");
                    cache
                        .get(&shard)
                        .map(|c| c.check(rid, fingerprint, now))
                };
                if let Some(decision) = cached {
                    match decision {
                        IdempotencyDecision::Replay(recovery) if recovery.len() == entries.len() => {
                            return Ok(outcomes_from_recovery(&recovery));
                        }
                        IdempotencyDecision::Conflict => {
                            return Err(EngineError::RequestIdConflict);
                        }
                        IdempotencyDecision::Replay(_)
                        | IdempotencyDecision::Proceed
                        | IdempotencyDecision::Expired => {}
                    }
                }
                if let Some(entries) = self.projection.with_store_mut(|p| {
                    ProjectionStore::replay_durable_commit(p, &shard, rid, fingerprint.0, now)
                })? {
                    let recovery = entries
                        .into_iter()
                        .map(crate::recovery_from_outcome_entry)
                        .collect::<Vec<_>>();
                    self.commit_idempotency
                        .lock()
                        .expect("commit idempotency poisoned")
                        .entry(shard.clone())
                        .or_default()
                        .record(
                            rid.clone(),
                            fingerprint,
                            recovery.clone(),
                            request_expires_at(now, retention),
                        );
                    return Ok(outcomes_from_recovery(&recovery));
                }
            }

            let commit_fingerprint = fingerprint.0;
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
            let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
            let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
            let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut committed_pushes: Vec<PushItem> = Vec::new();

            for entry in entries {
                let CommitTransitionEntry {
                    claim_ref,
                    additional_claim_refs,
                    finalize,
                    side_records,
                    lifecycle_items,
                    instance_fence,
                } = entry;
                let consumed_input_id = claim_ref.item_id;
                let additional_consumed_input_ids = additional_claim_refs
                    .iter()
                    .map(|c| c.item_id)
                    .collect::<Vec<_>>();
                let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
                claim_refs.push(claim_ref);
                claim_refs.extend(additional_claim_refs);
                let reject = |e: EngineError| EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids: additional_consumed_input_ids.clone(),
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(e),
                };

                if let Err(error) =
                    validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..])
                {
                    recovery.push(reject(error));
                    continue;
                }
                if claim_refs
                    .iter()
                    .any(|c| finalized_in_commit.contains(&c.item_id))
                {
                    recovery.push(reject(EngineError::Terminal));
                    continue;
                }
                if let Err(e) = self.projection.with_store(|p| {
                    ProjectionStore::commit_validate(p, &shard, &claim_refs, now)
                }) {
                    recovery.push(reject(e));
                    continue;
                }
                if let Some(fence) = &instance_fence {
                    let stored = match staged_fences.get(&fence.instance_key) {
                        Some(v) => *v,
                        None => self
                            .projection
                            .with_store(|p| {
                                ProjectionStore::instance_fence(p, &shard, &fence.instance_key)
                            })?
                            .unwrap_or(0),
                    };
                    if let Err(e) = validate_instance_fence(stored, fence) {
                        recovery.push(reject(e));
                        continue;
                    }
                }

                let side_record_keys: Vec<Vec<u8>> =
                    side_records.iter().map(|r| r.key.clone()).collect();
                let instance = instance_fence
                    .as_ref()
                    .map(|f| (f.instance_key.clone(), f.next));
                let mut envelopes: Vec<CommandEnvelope> = Vec::new();
                let mk_env = |command: QueueCommand, item_ids: Vec<ItemId>| CommandEnvelope {
                    command_id: self.ids.next_command_id(),
                    request_id: request_id.clone(),
                    request_fingerprint: Some(commit_fingerprint),
                    request_outcome: None,
                    item_ids,
                    command,
                    checksum: CommandChecksum(0),
                    created_at: now,
                };

                if !side_records.is_empty() {
                    envelopes.push(mk_env(
                        QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                            records: side_records,
                        }),
                        Vec::new(),
                    ));
                }
                if let Some(fence) = instance_fence {
                    envelopes.push(mk_env(
                        QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                            instance_key: fence.instance_key,
                            expected: fence.expected,
                            next: fence.next,
                        }),
                        Vec::new(),
                    ));
                }

                let mut lifecycle_item_ids = Vec::new();
                let mut entry_pushes: Vec<PushItem> = Vec::new();
                if !lifecycle_items.is_empty() {
                    if let Some(e) = lifecycle_items
                        .iter()
                        .find_map(|item| validate_entity(schema.as_ref(), item.entity.as_ref()).err())
                    {
                        recovery.push(reject(e));
                        continue;
                    }
                    let counter_base = self.counters.reserve(
                        &shard,
                        epoch,
                        lifecycle_items.len() as u32,
                    );
                    let (push_items, ids) = build_push_items(
                        lifecycle_items,
                        epoch,
                        self.node_id,
                        counter_base,
                        max_attempts,
                    );
                    let mut candidate = committed_pushes.clone();
                    candidate.extend(push_items.iter().cloned());
                    if let Err(e) = self.projection.with_store(|p| {
                        ProjectionStore::index_validate_push(p, &shard, &candidate)
                    }) {
                        recovery.push(reject(e));
                        continue;
                    }
                    lifecycle_item_ids = ids.clone();
                    entry_pushes = push_items.clone();
                    envelopes.push(mk_env(
                        QueueCommand::Push(PushCommand { items: push_items }),
                        ids,
                    ));
                }

                envelopes.push(mk_env(
                    QueueCommand::Finalize(FinalizeCommand {
                        outcomes: claim_refs
                            .iter()
                            .map(|c| FinalizeOutcome::new(c.item_id, finalize))
                            .collect(),
                    }),
                    claim_refs.iter().map(|c| c.item_id).collect(),
                ));

                finalized_in_commit.extend(claim_refs.iter().map(|c| c.item_id));
                if let Some((key, next)) = &instance {
                    staged_fences.insert(key.clone(), *next);
                }
                committed_pushes.extend(entry_pushes);
                committed_envelopes.append(&mut envelopes);
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance,
                    side_record_keys,
                    lifecycle_item_ids,
                    status: CommitEntryStatus::Committed,
                });
            }

            let mut batch = committed_envelopes;
            if let Some(rid) = &request_id {
                let outcome_entries: Vec<CommitOutcomeEntry> =
                    recovery.iter().map(outcome_entry_from_recovery).collect();
                batch.push(CommandEnvelope {
                    command_id: self.ids.next_command_id(),
                    request_id: Some(rid.clone()),
                    request_fingerprint: Some(commit_fingerprint),
                    request_outcome: Some(RequestOutcome::CommitTransition {
                        entries: outcome_entries,
                    }),
                    item_ids: Vec::new(),
                    command: QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                        records: Vec::new(),
                    }),
                    checksum: CommandChecksum(0),
                    created_at: now,
                });
            }
            if !batch.is_empty() {
                self.engine
                    .submit_commit(RawCommitRequest::new(shard.clone(), batch, epoch))
                    .await
                    .map_err(|e| {
                        EngineError::Storage(format!("async commit_transition failed: {e:?}"))
                    })??;
            }

            let outcomes = outcomes_from_recovery(&recovery);
            if let Some(rid) = request_id {
                self.commit_idempotency
                    .lock()
                    .expect("commit idempotency poisoned")
                    .entry(shard)
                    .or_default()
                    .record(
                        rid,
                        fingerprint,
                        recovery,
                        request_expires_at(now, retention),
                    );
            }
            Ok(outcomes)
        }
    }
}

