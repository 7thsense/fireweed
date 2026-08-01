//! Async object-log (LogEngine) × Postgres relational projection product.
//!
//! Public open requires `ResponseBarrier::Strict` (atomic response-after-apply). See
//! `fireweed_objectlog::commit_surface`.

use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition,
    RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncCommitStrategy, AsyncComposedBackend, AsyncControlPlane, AsyncLogStore,
    AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest, Backend, ClaimPort,
    ClaimRequest, Claimed, CommandChecksum, CommandEnvelope, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort, IdGen,
    InProcessControlPlane, InProcessProjectionStore, InlineOwnedTaskDispatcher, OwnedTask,
    ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead,
    ProjectionReclaimPlanner, ProjectionStore, PurgePort, PushPort, PushSpec, QueueCommand,
    QueueCounters, QueueKey, RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort, SeparateReplayCommit,
    SeparateReplayCommitter, TickReport, UpsertOutcome, UpsertPort,
};
use fireweed_objectlog::{
    CommitIdempotency, FlushConfig, ObjectLogEngineStore, SeqIdGen,
    finish_prepared_commit_transition, map_submit_error, new_commit_idempotency,
    prepare_commit_transition, strict_commit_capabilities,
};
use fireweed_objectlog::{explain_commit_if_authoritative, side_record as objectlog_side_record};

use crate::PostgresRelational;

type Proj = InProcessProjectionStore<PostgresRelational>;

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
    InlineOwnedTaskDispatcher,
    ProjectionClaimPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionPushPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionLifecyclePlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
    ProjectionReclaimPlanner<InProcessControlPlane, ObjectLogEngineStore, Proj, SeqIdGen>,
>;

/// LogEngine × durable Postgres relational projection (async composition).
pub struct AsyncObjectLogPostgresBackend {
    engine: Engine,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    commit_idempotency: CommitIdempotency,
}

impl AsyncObjectLogPostgresBackend {
    /// Local filesystem LogEngine × Postgres projection at `projection_url`.
    pub async fn open_local(
        log_root: impl AsRef<std::path::Path>,
        projection_url: &str,
        flush: FlushConfig,
        node_id: u8,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = PostgresRelational::connect(projection_url)?;
        Self::from_parts(log, projection_store, node_id).await
    }

    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection_store: PostgresRelational,
        node_id: u8,
    ) -> EngineResult<Self> {
        Self::from_parts(Arc::new(log), projection_store, node_id).await
    }

    async fn from_parts(
        log: Arc<ObjectLogEngineStore>,
        projection_store: PostgresRelational,
        node_id: u8,
    ) -> EngineResult<Self> {
        let projection = Arc::new(InProcessProjectionStore::new(projection_store));
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let commit_idempotency = new_commit_idempotency();
        let committer = Committer {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
        };
        let strategy = SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer)
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
            Arc::clone(&counters),
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
            InlineOwnedTaskDispatcher::new(),
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
                let page = AsyncLogStore::read_from(log.as_ref(), shard.clone(), from.clone(), 256)
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
            counters,
            node_id,
            commit_idempotency,
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

impl Backend for AsyncObjectLogPostgresBackend {
    fn durability_class(&self) -> DurabilityClass {
        // Public open requires ResponseBarrier::Strict — response-after-apply.
        DurabilityClass::Atomic
    }
    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }
    fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
        strict_commit_capabilities(
            "Strict: object-log append then postgres projection apply (response-after-apply, LogEngine)",
        )
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

impl ControlPlaneStore for AsyncObjectLogPostgresBackend {
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

impl PushPort for AsyncObjectLogPostgresBackend {
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

impl ClaimPort for AsyncObjectLogPostgresBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl FinalizePort for AsyncObjectLogPostgresBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // fireweed-c8e0a7a5 / fireweed-2be744bd: resolve leases under the same queue permit as plan+commit.
        async move {
            self.engine
                .finalize_outcomes(shard.clone(), outcomes, now, expected_epoch)
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl RenewLeasePort for AsyncObjectLogPostgresBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            self.engine
                .renew_item_ids(
                    shard.clone(),
                    item_ids,
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                )
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl ReassignLeasePort for AsyncObjectLogPostgresBackend {
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

impl PurgePort for AsyncObjectLogPostgresBackend {
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

impl UpsertPort for AsyncObjectLogPostgresBackend {
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

impl ReclaimPort for AsyncObjectLogPostgresBackend {
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

impl ReclaimDriver for AsyncObjectLogPostgresBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

impl ProjectionRead for AsyncObjectLogPostgresBackend {
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ItemView>>> + Send
    {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::peek(p, shard, limit)),
        )
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send
    {
        std::future::ready(
            self.projection
                .with_store(|p| ProjectionStore::pending(p, shard)),
        )
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PendingSummary>> + Send
    {
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send
    {
        let consumer = consumer.cloned();
        std::future::ready(self.projection.with_store(|p| {
            ProjectionStore::pending_range(p, shard, start, end, consumer.as_ref(), limit)
        }))
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send
    {
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send
    {
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<fireweed_engine::LiveItemView>>>> + Send
    {
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
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::TerminalEmissionMetrics>> + Send
    {
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
impl fireweed_engine::UpdateFieldsPort for AsyncObjectLogPostgresBackend {
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
impl fireweed_engine::CommitTransitionPort for AsyncObjectLogPostgresBackend {
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: fireweed_engine::CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::CommitEntryOutcome>>> + Send
    {
        let shard = shard.clone();
        async move {
            let epoch = match expected_epoch {
                Some(epoch) => {
                    let current =
                        AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
                    if current != epoch {
                        return Err(EngineError::EpochFenced);
                    }
                    epoch
                }
                None => AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?,
            };
            // fireweed-5497780d: prepare + append/apply under one queue-local permit.
            let strategy = self.engine.commit_strategy();
            let projection = Arc::clone(&self.projection);
            let control = Arc::clone(&self.control);
            let ids = Arc::clone(&self.ids);
            let counters = Arc::clone(&self.counters);
            let commit_idempotency = Arc::clone(&self.commit_idempotency);
            let node_id = self.node_id;
            self.engine
                .submit_operation(shard.clone(), move || {
                    Box::pin(async move {
                        let prepared = prepare_commit_transition(
                            projection.as_ref(),
                            control.as_ref(),
                            ids.as_ref(),
                            counters.as_ref(),
                            node_id,
                            &commit_idempotency,
                            epoch,
                            &shard,
                            transition,
                            now,
                        )
                        .await?;
                        finish_prepared_commit_transition(
                            &shard,
                            epoch,
                            prepared,
                            &commit_idempotency,
                            now,
                            |request| {
                                let strategy = Arc::clone(&strategy);
                                async move { strategy.commit(request).await }
                            },
                        )
                        .await
                    })
                })
                .await
                .map_err(map_submit_error)?
        }
    }
}

impl fireweed_engine::RecoveryReadPort for AsyncObjectLogPostgresBackend {
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::CommitRecovery>>> + Send
    {
        std::future::ready(explain_commit_if_authoritative(
            true,
            self.projection.as_ref(),
            &self.commit_idempotency,
            shard,
            request_id,
        ))
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let key = key.to_vec();
        std::future::ready(objectlog_side_record(self.projection.as_ref(), shard, &key))
    }
}

impl fireweed_engine::BatchUpdatePort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::ItemMutationPort for AsyncObjectLogPostgresBackend {
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
impl fireweed_engine::SetGatesPort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::ReschedulePort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::DiscoveryPort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::IndexQueryPort for AsyncObjectLogPostgresBackend {
    fn index_get_unique(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::IndexHit>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
    fn index_lookup(
        &self,
        _shard: &QueueKey,
        _index: &str,
        _key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send
    {
        std::future::ready(Err(EngineError::Unavailable))
    }
}
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::HistoricalProjectionRead for AsyncObjectLogPostgresBackend {
    type AsOfProjection = fireweed_projection::InMemoryProjection;
    fn current_position(
        &self,
        _shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CommandPosition>> + Send
    {
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

impl AsyncObjectLogPostgresBackend {
    /// Borrow the authoritative log axis (lifecycle / diagnostics).
    pub fn with_log<R>(&self, f: impl FnOnce(&ObjectLogEngineStore) -> R) -> R {
        f(self.log.as_ref())
    }

    /// Borrow the projection axis (lifecycle / diagnostics).
    pub fn with_projection<R>(&self, f: impl FnOnce(&PostgresRelational) -> R) -> R {
        self.projection.with_store(f)
    }

    /// Mutably borrow the projection axis (lifecycle rebuild / delete).
    pub fn with_projection_mut<R>(&self, f: impl FnOnce(&mut PostgresRelational) -> R) -> R {
        self.projection.with_store_mut(f)
    }
}
