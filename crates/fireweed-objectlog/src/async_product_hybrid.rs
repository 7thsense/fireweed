//! Async object-log × hybrid projection over crates.io [`object_log::LogEngine`].
//!
//! Eventual-apply composition: [`ObjectLogEngineStore`] append then
//! [`fireweed_sqlite::HybridProjectionStore`] apply (hot memory + durable SQLite checkpoint).

use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition,
    RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncComposedBackend, AsyncControlPlane, AsyncFinalizeRequest, AsyncLogStore,
    AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest, AsyncRenewRequest,
    Backend, ClaimPort, ClaimRequest, Claimed, CommandChecksum, CommandEnvelope, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort,
    FinalizeTarget, IdGen, InProcessControlPlane, InProcessProjectionStore,
    OwnedTask, ProjectionClaimPlanner, ProjectionLifecyclePlanner,
    ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner, ProjectionStore, PurgePort,
    PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, RawCommitOutcome, RawCommitRequest,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort, RenewTarget,
    SeparateReplayCommit, SeparateReplayCommitter, TickReport, UpsertOutcome, UpsertPort,
};
use fireweed_sqlite::{HybridAsyncThresholds, HybridProjectionStore};
use object_log::FlushConfig;

use crate::ObjectLogEngineStore;
use crate::async_product::SeqIdGen;

type Proj = InProcessProjectionStore<HybridProjectionStore>;

#[derive(Clone)]
struct Committer {
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
}

impl SeparateReplayCommitter for Committer {
    type Request = RawCommitRequest;
    type PreparedRequest = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn prepare_replayable(
        &self,
        request: Self::Request,
    ) -> OwnedTask<EngineResult<Self::PreparedRequest>> {
        Box::pin(std::future::ready(Ok(request)))
    }

    fn commit_prepared_replayable(
        &self,
        request: Self::PreparedRequest,
    ) -> OwnedTask<Self::Output> {
        self.commit_replayable(request)
    }

    fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        Box::pin(async move {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            match fault {
                fireweed_engine::RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                fireweed_engine::RawCommitFault::None
                | fireweed_engine::RawCommitFault::AfterAppendBeforeApply => {}
            }
            let positions =
                AsyncLogStore::append(log.as_ref(), shard, commands.clone(), expected_epoch)
                    .await?;
            if matches!(
                fault,
                fireweed_engine::RawCommitFault::AfterAppendBeforeApply
            ) {
                return Ok(RawCommitOutcome::appended(positions));
            }
            AsyncProjectionStore::apply_live(projection.as_ref(), positions.clone(), commands)
                .await?;
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

type Strategy = SeparateReplayCommit<Committer>;
type Engine = AsyncComposedBackend<
    Strategy,
    crate::ObjectLogTaskDispatcher,
    ProjectionClaimPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionPushPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionLifecyclePlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionReclaimPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
>;

/// LogEngine × hybrid (hot-memory + durable SQLite checkpoint) projection (async composition).
pub struct AsyncObjectLogHybridBackend {
    engine: Engine,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
}

/// Open knobs for the hybrid projection axis (strict apply, deferred-flush chunk, async monitor).
#[derive(Debug, Clone)]
pub struct HybridProductConfig {
    pub deferred_flush_chunk: usize,
    pub strict: bool,
    pub async_monitor: Option<HybridAsyncThresholds>,
}

impl Default for HybridProductConfig {
    fn default() -> Self {
        Self {
            deferred_flush_chunk: fireweed_sqlite::DEFAULT_DEFERRED_FLUSH_CHUNK,
            strict: false,
            async_monitor: None,
        }
    }
}

impl AsyncObjectLogHybridBackend {
    pub async fn open(
        log_root: impl AsRef<std::path::Path>,
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        hybrid: HybridProductConfig,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = configure_hybrid(HybridProjectionStore::open(projection_path)?, &hybrid);
        Self::from_parts(log, projection_store, node_id).await
    }

    pub async fn open_memory_log(
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        hybrid: HybridProductConfig,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_memory(flush).await?);
        let store = if projection_path == ":memory:" {
            HybridProjectionStore::in_memory()?
        } else {
            HybridProjectionStore::open(projection_path)?
        };
        let projection_store = configure_hybrid(store, &hybrid);
        Self::from_parts(log, projection_store, node_id).await
    }

    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection_store: HybridProjectionStore,
        node_id: u8,
    ) -> EngineResult<Self> {
        Self::from_parts(Arc::new(log), projection_store, node_id).await
    }

    /// Drain deferred SQLite checkpoint work (bounded by `deferred_flush_chunk`).
    pub fn try_flush_deferred_projection(&self) -> EngineResult<bool> {
        self.projection.with_store_mut(|p| {
            ProjectionStore::flush_deferred(p)?;
            Ok(true)
        })
    }

    /// Observability/test seam against the hybrid projection.
    pub fn with_projection<R>(&self, f: impl FnOnce(&HybridProjectionStore) -> R) -> R {
        self.projection.with_store(f)
    }

    async fn from_parts(
        log: Arc<ObjectLogEngineStore>,
        projection_store: HybridProjectionStore,
        node_id: u8,
    ) -> EngineResult<Self> {
        let projection = Arc::new(InProcessProjectionStore::new(projection_store));
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let committer = Committer {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
        };
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
        let claim = ProjectionClaimPlanner::from_shared(
            Arc::clone(&control),
            Arc::clone(&log),
            Arc::clone(&projection),
            Arc::clone(&ids),
        );
        let push = ProjectionPushPlanner::from_shared(
            Arc::clone(&control),
            Arc::clone(&log),
            Arc::clone(&projection),
            Arc::clone(&ids),
            counters,
            node_id,
        );
        let lifecycle = ProjectionLifecyclePlanner::from_shared(
            Arc::clone(&control),
            Arc::clone(&log),
            Arc::clone(&projection),
            Arc::clone(&ids),
        );
        let reclaim = ProjectionReclaimPlanner::from_shared(
            Arc::clone(&control),
            Arc::clone(&log),
            Arc::clone(&projection),
            Arc::clone(&ids),
        );
        let engine = AsyncComposedBackend::new_with_planners(
            strategy,
            crate::ObjectLogTaskDispatcher::new(),
            claim,
            push,
            1024,
        )
        .with_lifecycle_planner(lifecycle)
        .with_reclaim_planner(reclaim);

        let definitions = AsyncLogStore::recover_definitions(log.as_ref()).await?;
        for definition in definitions {
            let _ = AsyncControlPlane::create_queue(control.as_ref(), definition.clone()).await;
            AsyncProjectionStore::ensure_shard(projection.as_ref(), definition.clone()).await?;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let mut from = None;
            loop {
                let page =
                    AsyncLogStore::read_from(log.as_ref(), shard.clone(), from.clone(), 256)
                        .await?;
                if page.entries.is_empty() {
                    break;
                }
                let positions: Vec<_> = page.entries.iter().map(|(p, _)| p.clone()).collect();
                let commands: Vec<_> = page.entries.iter().map(|(_, e)| e.clone()).collect();
                AsyncProjectionStore::apply_recovery(projection.as_ref(), positions, commands)
                    .await?;
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
        }
        Ok(Self {
            engine,
            log,
            projection,
            control,
            ids,
        })
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

    fn map_lifecycle(error: fireweed_engine::AsyncLifecycleError) -> EngineError {
        match error {
            fireweed_engine::AsyncLifecycleError::BeforeCommit(error)
            | fireweed_engine::AsyncLifecycleError::Commit(error) => error,
            fireweed_engine::AsyncLifecycleError::AfterCommit { source, .. } => source,
            fireweed_engine::AsyncLifecycleError::Submit(error) => {
                EngineError::Storage(format!("async lifecycle submission failed: {error:?}"))
            }
        }
    }

    async fn claimed_targets(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<fireweed_engine::ClaimedItem>> {
        let claimed = AsyncProjectionStore::render_claimed(
            self.projection.as_ref(),
            shard.clone(),
            ids.to_vec(),
        )
        .await?;
        if claimed.len() != ids.len() {
            return Err(EngineError::StaleLease);
        }
        Ok(claimed)
    }
}

impl Backend for AsyncObjectLogHybridBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }
    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }
    fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
        fireweed_engine::CommitCapabilities {
            atomic_transition_commit: false,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::EventualApply,
            consistency: "object-log sequenced append then hybrid projection apply (LogEngine)",
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

impl ControlPlaneStore for AsyncObjectLogHybridBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard).await?;
            AsyncProjectionStore::ensure_shard(self.projection.as_ref(), definition.clone())
                .await?;
            AsyncControlPlane::create_queue(self.control.as_ref(), definition).await
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_core::QueueId>>> + Send {
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

impl PushPort for AsyncObjectLogHybridBackend {
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
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PushBatchOutcome>> + Send
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

impl ClaimPort for AsyncObjectLogHybridBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl FinalizePort for AsyncObjectLogHybridBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let claimed = self.claimed_targets(shard, &ids).await?;
            let targets = outcomes
                .into_iter()
                .zip(claimed)
                .map(|(outcome, item)| {
                    Ok(FinalizeTarget {
                        item_id: outcome.item_id,
                        lease_token: item.lease_token.ok_or(EngineError::StaleLease)?,
                        item_version: item.item_version,
                        kind: outcome.kind,
                        not_before: outcome.not_before,
                    })
                })
                .collect::<EngineResult<Vec<_>>>()?;
            self.engine
                .finalize(AsyncFinalizeRequest {
                    shard: shard.clone(),
                    targets,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl RenewLeasePort for AsyncObjectLogHybridBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let claimed = self.claimed_targets(shard, &item_ids).await?;
            let targets = claimed
                .into_iter()
                .map(|item| {
                    Ok(RenewTarget {
                        item_id: item.item_id,
                        lease_token: item.lease_token.ok_or(EngineError::StaleLease)?,
                    })
                })
                .collect::<EngineResult<Vec<_>>>()?;
            self.engine
                .renew(AsyncRenewRequest {
                    shard: shard.clone(),
                    targets,
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl ReassignLeasePort for AsyncObjectLogHybridBackend {
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
            self.claimed_targets(shard, &item_ids).await?;
            let epoch = match expected_epoch {
                Some(epoch) => epoch,
                None => AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?,
            };
            let envelope = CommandEnvelope {
                command_id: self.ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids,
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            self.engine
                .submit_commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                .await
                .map_err(|error| {
                    EngineError::Storage(format!("async reassign submission failed: {error:?}"))
                })??;
            Ok(())
        }
    }
}

impl PurgePort for AsyncObjectLogHybridBackend {
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

impl UpsertPort for AsyncObjectLogHybridBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: std::collections::BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimPort for AsyncObjectLogHybridBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            self.engine
                .reclaim_expired(fireweed_engine::AsyncReclaimRequest {
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

impl ReclaimDriver for AsyncObjectLogHybridBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

fn configure_hybrid(
    store: HybridProjectionStore,
    hybrid: &HybridProductConfig,
) -> HybridProjectionStore {
    let mut store = store
        .with_deferred_flush_chunk(hybrid.deferred_flush_chunk)
        .with_strict_apply(hybrid.strict);
    if let Some(thresholds) = hybrid.async_monitor.clone() {
        store = store.with_async_monitor(thresholds);
    }
    store
}

impl ProjectionRead for AsyncObjectLogHybridBackend {
    fn metrics(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::QueueMetrics>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::metrics(p, shard)),
        )
    }
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::select_eligible(p, shard, now, limit)),
        )
    }
    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ItemView>>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::peek(p, shard, limit)),
        )
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::pending(p, shard)),
        )
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PendingSummary>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::pending_summary(p, shard)),
        )
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PendingPage>> + Send {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::pending_page(p, shard, start, limit)),
        )
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send {
        let consumer = consumer.cloned();
        std::future::ready(self.projection.with_store(|p| {
            ProjectionStore::pending_range(p, shard, start, end, consumer.as_ref(), limit)
        }))
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send {
        let ids = ids.to_vec();
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::pending_by_ids(p, shard, &ids)),
        )
    }
    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send {
        let ids = ids.to_vec();
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::render_claimed(p, shard, &ids)),
        )
    }
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<fireweed_engine::LiveItemView>>>>
    + Send {
        let keys = keys.to_vec();
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::live_items(p, shard, &keys)),
        )
    }
    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&fireweed_engine::CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::TerminalEmissionMetrics>>
    + Send {
        let emission_cursor = emission_cursor.cloned();
        std::future::ready(self.projection.with_store(|p| {
            ProjectionStore::terminal_emission_metrics(
                p,
                shard,
                now,
                emit_change_records,
                emission_cursor.as_ref(),
            )
        }))
    }
}

// LibBackend / facade ports: defaults where available; explicit Unavailable for required methods.
impl fireweed_engine::UpdateFieldsPort for AsyncObjectLogHybridBackend {
    fn update_fields(
        &self,
        _shard: &QueueKey,
        _item_id: ItemId,
        _field_ops: std::collections::BTreeMap<String, Option<Bytes>>,
        _payload: fireweed_engine::PayloadUpdate,
        _entity: Option<serde_json::Value>,
        _expected_item_version: Option<u64>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}
impl fireweed_engine::CommitTransitionPort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::RecoveryReadPort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::BatchUpdatePort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::ItemMutationPort for AsyncObjectLogHybridBackend {
    fn mutate_items(
        &self,
        _shard: &QueueKey,
        _request: fireweed_engine::ItemMutationRequest,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ItemMutationResponse>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}
impl fireweed_engine::SetGatesPort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::ReschedulePort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::DiscoveryPort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::IndexQueryPort for AsyncObjectLogHybridBackend {
    fn index_get_unique(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::IndexHit>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
    fn index_lookup(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogHybridBackend {}
impl fireweed_engine::HistoricalProjectionRead for AsyncObjectLogHybridBackend {
    type AsOfProjection = fireweed_projection::InMemoryProjection;
    fn current_position(
        &self,
        _shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CommandPosition>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
    fn read_as_of<T, F>(
        &self,
        _shard: &QueueKey,
        _position: fireweed_engine::CommandPosition,
        _query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static,
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl AsyncObjectLogHybridBackend {
    /// Borrow the authoritative log axis (lifecycle / diagnostics).
    pub fn with_log<R>(&self, f: impl FnOnce(&ObjectLogEngineStore) -> R) -> R {
        f(self.log.as_ref())
    }

    /// Mutably borrow the hybrid projection axis (lifecycle rebuild / delete).
    pub fn with_projection_mut<R>(&self, f: impl FnOnce(&mut HybridProjectionStore) -> R) -> R {
        self.projection.with_store_mut(f)
    }
}

#[cfg(test)]
mod tests {
    use fireweed_core::{
        EligibilityPolicy, LeaseToken, OrderingMode, PriorityModel, QueueDefinition, QueueId,
        RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, FinalizeKind,
        FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec,
    };
    use object_log::FlushConfig;

    use super::{AsyncObjectLogHybridBackend, HybridProductConfig};

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    #[tokio::test]
    async fn object_log_hybrid_push_claim_finalize() {
        let backend = AsyncObjectLogHybridBackend::open_memory_log(
            ":memory:",
            FlushConfig {
                linger: std::time::Duration::ZERO,
                ..FlushConfig::default()
            },
            0,
            HybridProductConfig::default(),
        )
        .await
        .unwrap();
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let ids = backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"x")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("lease-1").unwrap(),
                lease_expires_at: UtcTimestamp::new(100, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);
        backend
            .finalize(
                &shard,
                vec![FinalizeOutcome::new(ids[0], FinalizeKind::Complete)],
                UtcTimestamp::new(3, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.leased, 0);
    }
}
