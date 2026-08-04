//! Async object-log (LogEngine) × Postgres relational projection product.
//!
//! Strict waits for durable Postgres and serving-memory apply. AsyncProjection synchronously updates
//! serving memory and defers bounded, ordered Postgres apply through the shared coordinator.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByQueryRequest, ClientItemKey,
    DeclaredBucketSegmentRequest, DeclaredBucketSegmentResponse, GroupKey, GroupedAggregateRequest,
    GroupedAggregateResponse, ItemId, LeaseToken, Metadata, MetricsByQueryRequest, PriorityValue,
    QueryCapabilityFlags, QueueDefinition, RangeScanRequest, RangeScanResponse, RequestId,
    TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncCommitStrategy, AsyncComposedBackend, AsyncControlPlane, AsyncLogStore,
    AsyncProjectionSpec, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest,
    Backend, BoundedMutationContext, ClaimByQueryContext, ClaimPort, ClaimRequest, Claimed,
    CommandChecksum, CommandEnvelope, ControlPlane, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort, IdGen,
    IdempotencyDecision, InProcessControlPlane, OwnedTask, ProjectionClaimPlanner,
    ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner,
    ProjectionStore, PurgePort, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey,
    RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver,
    ReclaimPort, RenewLeasePort, SeparateReplayCommit, SeparateReplayCommitter, TickReport,
    UpsertOutcome, UpsertPort, claim_by_query_body_hash, generate_query_lease_token,
    item_mutation_fingerprint, request_expires_at,
};
use fireweed_objectlog::{
    AsyncProjectionApplyCoordinator, AsyncProjectionApplySnapshot, ClaimByQueryIdempotency,
    CommitIdempotency, FlushConfig, ObjectLogEngineStore, SeqIdGen, eventual_commit_capabilities,
    finish_prepared_commit_transition, make_envelope, map_submit_error,
    new_batch_update_idempotency, new_claim_by_item_ids_idempotency,
    new_claim_by_query_idempotency, new_commit_idempotency, prepare_commit_transition,
    rebuild_process_idempotency_from_log, record_claim_by_query_idempotency,
    retained_item_mutation_response, strict_commit_capabilities,
};
use fireweed_objectlog::{explain_commit_if_authoritative, side_record as objectlog_side_record};
use fireweed_projection::{AsyncInMemoryProjection, InMemoryProjection};

use crate::AsyncPostgresRelationalProjection;

type Proj = AsyncInMemoryProjection;

/// Keep the existing product-planning call sites asynchronous while the serving projection is an
/// in-process image. The operation completes synchronously and never enters the Postgres actor.
trait InMemoryProjectionExecutor {
    fn execute<T, F>(&self, operation: F) -> std::future::Ready<EngineResult<T>>
    where
        F: FnOnce(&mut InMemoryProjection) -> EngineResult<T>;
}

impl InMemoryProjectionExecutor for AsyncInMemoryProjection {
    fn execute<T, F>(&self, operation: F) -> std::future::Ready<EngineResult<T>>
    where
        F: FnOnce(&mut InMemoryProjection) -> EngineResult<T>,
    {
        std::future::ready(self.with_store_mut(operation))
    }
}

#[derive(Clone)]
struct Committer {
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
    postgres_projection: Arc<AsyncPostgresRelationalProjection>,
    async_apply: Option<AsyncProjectionApplyCoordinator<AsyncPostgresRelationalProjection>>,
    strict_poison: Arc<std::sync::RwLock<std::collections::HashMap<QueueKey, String>>>,
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
        let postgres_projection = Arc::clone(&self.postgres_projection);
        let async_apply = self.async_apply.clone();
        let strict_poison = Arc::clone(&self.strict_poison);
        Box::pin(async move {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            match fault {
                fireweed_engine::RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                fireweed_engine::RawCommitFault::None
                | fireweed_engine::RawCommitFault::AfterAppendBeforeApply => {}
            }
            {
                let poisoned = strict_poison.read().map_err(|_| {
                    EngineError::Storage("Postgres projection poison registry lock failed".into())
                })?;
                if let Some(reason) = poisoned.get(&shard) {
                    return Err(EngineError::Storage(format!(
                        "Postgres projection poisoned: {reason}"
                    )));
                }
            }
            let reservation = match &async_apply {
                Some(coordinator) => Some(coordinator.reserve(shard.clone(), &commands).await?),
                None => None,
            };
            let positions = match AsyncLogStore::append(
                log.as_ref(),
                shard.clone(),
                commands.clone(),
                expected_epoch,
            )
            .await
            {
                Ok(positions) => positions,
                Err(error) => {
                    if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                        coordinator.cancel(reservation).await;
                    }
                    return Err(error);
                }
            };
            if matches!(
                fault,
                fireweed_engine::RawCommitFault::AfterAppendBeforeApply
            ) {
                if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                    coordinator.cancel(reservation).await;
                }
                return Ok(RawCommitOutcome::appended(positions));
            }
            if let Some(coordinator) = &async_apply {
                if let Err(error) = AsyncProjectionStore::apply_live(
                    projection.as_ref(),
                    positions.clone(),
                    commands.clone(),
                )
                .await
                {
                    if let Some(reservation) = reservation {
                        coordinator.cancel(reservation).await;
                    }
                    return Err(error);
                }
                if let Some(reservation) = reservation {
                    coordinator
                        .enqueue_reserved(reservation, positions.clone(), commands)
                        .await?;
                }
            } else {
                AsyncProjectionStore::apply_live(
                    postgres_projection.as_ref(),
                    positions.clone(),
                    commands.clone(),
                )
                .await?;
                if let Err(error) = AsyncProjectionStore::apply_live(
                    projection.as_ref(),
                    positions.clone(),
                    commands,
                )
                .await
                {
                    if let Ok(mut poisoned) = strict_poison.write() {
                        poisoned.insert(
                            shard,
                            format!(
                                "serving memory apply failed after durable Postgres apply: {error}"
                            ),
                        );
                    }
                    return Err(error);
                }
            }
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

type Strategy = SeparateReplayCommit<Committer>;
type Engine = AsyncComposedBackend<
    Strategy,
    fireweed_objectlog::ObjectLogTaskDispatcher,
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
    postgres_projection: Arc<AsyncPostgresRelationalProjection>,
    async_apply: Option<AsyncProjectionApplyCoordinator<AsyncPostgresRelationalProjection>>,
    strict_poison: Arc<std::sync::RwLock<std::collections::HashMap<QueueKey, String>>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    commit_idempotency: CommitIdempotency,
    claim_by_query_idempotency: ClaimByQueryIdempotency,
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
        let projection_store = AsyncPostgresRelationalProjection::connect(projection_url).await?;
        Self::from_parts(log, projection_store, node_id, None).await
    }

    /// Local filesystem LogEngine × Postgres projection with a bounded deferred apply barrier.
    pub async fn open_local_with_async_projection(
        log_root: impl AsRef<std::path::Path>,
        projection_url: &str,
        flush: FlushConfig,
        node_id: u8,
        spec: AsyncProjectionSpec,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = AsyncPostgresRelationalProjection::connect(projection_url).await?;
        Self::from_parts(log, projection_store, node_id, Some(spec)).await
    }

    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection_store: AsyncPostgresRelationalProjection,
        node_id: u8,
    ) -> EngineResult<Self> {
        Self::from_parts(Arc::new(log), projection_store, node_id, None).await
    }

    pub async fn from_log_and_projection_with_async_projection(
        log: ObjectLogEngineStore,
        projection_store: AsyncPostgresRelationalProjection,
        node_id: u8,
        spec: AsyncProjectionSpec,
    ) -> EngineResult<Self> {
        Self::from_parts(Arc::new(log), projection_store, node_id, Some(spec)).await
    }

    async fn from_parts(
        log: Arc<ObjectLogEngineStore>,
        projection_store: AsyncPostgresRelationalProjection,
        node_id: u8,
        async_spec: Option<AsyncProjectionSpec>,
    ) -> EngineResult<Self> {
        let projection = Arc::new(AsyncInMemoryProjection::new(InMemoryProjection::new()));
        let postgres_projection = Arc::new(projection_store);
        let async_apply = async_spec
            .map(|spec| {
                AsyncProjectionApplyCoordinator::new(Arc::clone(&postgres_projection), spec)
            })
            .transpose()?;
        let strict_poison = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let commit_idempotency = new_commit_idempotency();
        let batch_update_idempotency = new_batch_update_idempotency();
        let claim_by_query_idempotency = new_claim_by_query_idempotency();
        let claim_by_item_ids_idempotency = new_claim_by_item_ids_idempotency();
        let committer = Committer {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
            postgres_projection: Arc::clone(&postgres_projection),
            async_apply: async_apply.clone(),
            strict_poison: Arc::clone(&strict_poison),
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
            fireweed_objectlog::ObjectLogTaskDispatcher::new(),
            claim,
            push,
            1024,
        )
        .with_lifecycle_planner(lifecycle)
        .with_reclaim_planner(reclaim);

        let definitions = AsyncLogStore::recover_definitions(log.as_ref()).await?;
        for definition in definitions {
            let _ = AsyncControlPlane::create_queue(control.as_ref(), definition.clone()).await;
            AsyncProjectionStore::ensure_shard(postgres_projection.as_ref(), definition.clone())
                .await?;
            AsyncProjectionStore::ensure_shard(projection.as_ref(), definition.clone()).await?;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            fireweed_objectlog::replay_log_into_projection(
                log.as_ref(),
                postgres_projection.as_ref(),
                &shard,
                true,
            )
            .await?;
            fireweed_objectlog::replay_log_into_projection(
                log.as_ref(),
                projection.as_ref(),
                &shard,
                false,
            )
            .await?;
            projection.with_store(|projection| {
                ProjectionStore::restore_counters(projection, &shard, counters.as_ref())
            })?;
            let live_token_candidates = rebuild_process_idempotency_from_log(
                log.as_ref(),
                &shard,
                definition.request_id_retention_ms,
                &commit_idempotency,
                &batch_update_idempotency,
                &claim_by_query_idempotency,
                &claim_by_item_ids_idempotency,
            )
            .await?;
            postgres_projection
                .restore_live_tokens(shard.clone(), live_token_candidates)
                .await?;
            if let Some(coordinator) = &async_apply {
                coordinator
                    .seed_high_water(
                        shard.clone(),
                        AsyncProjectionStore::recovery_high_water(
                            postgres_projection.as_ref(),
                            shard.clone(),
                        )
                        .await?,
                    )
                    .await;
            }
        }
        Ok(Self {
            engine,
            log,
            projection,
            postgres_projection,
            async_apply,
            strict_poison,
            control,
            ids,
            counters,
            node_id,
            commit_idempotency,
            claim_by_query_idempotency,
        })
    }

    async fn resolve_epoch(
        &self,
        shard: &QueueKey,
        expected_epoch: Option<u64>,
    ) -> EngineResult<u64> {
        self.ensure_projection_healthy(shard)?;
        let epoch = AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
        if expected_epoch.is_some_and(|expected| expected != epoch) {
            return Err(EngineError::EpochFenced);
        }
        Ok(epoch)
    }

    fn ensure_projection_healthy(&self, shard: &QueueKey) -> EngineResult<()> {
        if let Some(coordinator) = &self.async_apply {
            coordinator.ensure_healthy(shard)?;
        }
        let poisoned = self.strict_poison.read().map_err(|_| {
            EngineError::Storage("Postgres projection poison registry lock failed".into())
        })?;
        match poisoned.get(shard) {
            Some(reason) => Err(EngineError::Storage(format!(
                "Postgres projection poisoned: {reason}"
            ))),
            None => Ok(()),
        }
    }

    fn read_healthy_projection<T>(
        &self,
        shard: &QueueKey,
        query: impl FnOnce(&InMemoryProjection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        self.ensure_projection_healthy(shard)?;
        self.projection.with_store(query)
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
        if self.async_apply.is_some() {
            DurabilityClass::EventualApply
        } else {
            DurabilityClass::Atomic
        }
    }
    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }
    fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
        if self.async_apply.is_some() {
            eventual_commit_capabilities(
                "AsyncProjection: object-log append plus serving-memory apply, then bounded durable Postgres apply (LogEngine)",
            )
        } else {
            strict_commit_capabilities(
                "Strict: object-log append then durable Postgres plus serving-memory apply (response-after-apply, LogEngine)",
            )
        }
    }
    async fn commit_raw(&self, request: RawCommitRequest) -> EngineResult<RawCommitOutcome> {
        self.engine.submit_commit(request).await.map_err(|error| {
            EngineError::Storage(format!("async raw commit submission failed: {error:?}"))
        })?
    }
}

impl ControlPlaneStore for AsyncObjectLogPostgresBackend {
    async fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome> {
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        self.ensure_projection_healthy(&shard)?;
        let outcome = self
            .log
            .create_or_read_definition(definition.clone())
            .await?;
        ControlPlane::cache_authoritative_definition(
            self.control.as_ref(),
            outcome.definition.clone(),
        )?;
        if outcome.definition != definition {
            return Err(EngineError::QueueDefinitionConflict);
        }
        AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
        AsyncProjectionStore::ensure_shard(
            self.postgres_projection.as_ref(),
            outcome.definition.clone(),
        )
        .await?;
        AsyncProjectionStore::ensure_shard(self.projection.as_ref(), outcome.definition.clone())
            .await?;
        if let Some(coordinator) = &self.async_apply {
            coordinator
                .seed_high_water(
                    shard.clone(),
                    AsyncProjectionStore::recovery_high_water(
                        self.postgres_projection.as_ref(),
                        shard,
                    )
                    .await?,
                )
                .await;
        }
        Ok(outcome)
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
    async fn fence_epoch(&self, shard: &QueueKey, target_epoch: u64) -> EngineResult<u64> {
        let mut current = AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
        if current > target_epoch {
            return Err(EngineError::EpochFenced);
        }
        while current < target_epoch {
            current = AsyncLogStore::acquire_epoch(self.log.as_ref(), shard.clone()).await?;
        }
        Ok(current)
    }
}

impl PushPort for AsyncObjectLogPostgresBackend {
    async fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<Vec<ItemId>> {
        self.ensure_projection_healthy(shard)?;
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
    async fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<fireweed_engine::PushBatchOutcome> {
        self.ensure_projection_healthy(shard)?;
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

impl ClaimPort for AsyncObjectLogPostgresBackend {
    async fn claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        self.ensure_projection_healthy(&request.shard)?;
        self.engine.claim(request).await.map_err(Self::map_claim)
    }
}

impl FinalizePort for AsyncObjectLogPostgresBackend {
    async fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        self.ensure_projection_healthy(shard)?;
        // fireweed-c8e0a7a5 / fireweed-2be744bd: resolve leases under the same queue permit as plan+commit.
        self.engine
            .finalize_outcomes(shard.clone(), outcomes, now, expected_epoch)
            .await
            .map_err(Self::map_lifecycle)
    }
}

impl RenewLeasePort for AsyncObjectLogPostgresBackend {
    async fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        self.ensure_projection_healthy(shard)?;
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

impl ReassignLeasePort for AsyncObjectLogPostgresBackend {
    async fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        self.ensure_projection_healthy(shard)?;
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

impl PurgePort for AsyncObjectLogPostgresBackend {
    async fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<u64> {
        self.ensure_projection_healthy(shard)?;
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
    async fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<Vec<ItemId>> {
        self.ensure_projection_healthy(shard)?;
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
            self.read_healthy_projection(shard, |p| ProjectionStore::metrics(p, shard)),
        )
    }
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        std::future::ready(self.read_healthy_projection(shard, |p| {
            ProjectionStore::select_eligible(p, shard, now, limit)
        }))
    }
    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ItemView>>> + Send
    {
        std::future::ready(
            self.read_healthy_projection(shard, |p| ProjectionStore::peek(p, shard, limit)),
        )
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::LeaseView>>> + Send
    {
        std::future::ready(
            self.read_healthy_projection(shard, |p| ProjectionStore::pending(p, shard)),
        )
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PendingSummary>> + Send
    {
        std::future::ready(
            self.read_healthy_projection(shard, |p| ProjectionStore::pending_summary(p, shard)),
        )
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::PendingPage>> + Send {
        std::future::ready(self.read_healthy_projection(shard, |p| {
            ProjectionStore::pending_page(p, shard, start, limit)
        }))
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
        std::future::ready(self.read_healthy_projection(shard, |p| {
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
            self.read_healthy_projection(shard, |p| {
                ProjectionStore::pending_by_ids(p, shard, &ids)
            }),
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
            self.read_healthy_projection(shard, |p| {
                ProjectionStore::render_claimed(p, shard, &ids)
            }),
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
            self.read_healthy_projection(shard, |p| ProjectionStore::live_items(p, shard, &keys)),
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
        std::future::ready(self.read_healthy_projection(shard, |p| {
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
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
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
        let projection = Arc::clone(&self.projection);
        let commit_idempotency = Arc::clone(&self.commit_idempotency);
        let shard = shard.clone();
        async move {
            self.ensure_projection_healthy(&shard)?;
            explain_commit_if_authoritative(
                true,
                projection.as_ref(),
                &commit_idempotency,
                &shard,
                request_id,
            )
            .await
        }
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let key = key.to_vec();
        async move {
            self.ensure_projection_healthy(&shard)?;
            objectlog_side_record(projection.as_ref(), &shard, &key).await
        }
    }
}

impl fireweed_engine::BatchUpdatePort for AsyncObjectLogPostgresBackend {}
impl fireweed_engine::ItemMutationPort for AsyncObjectLogPostgresBackend {
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: fireweed_engine::ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ItemMutationResponse>> + Send
    {
        let shard = shard.clone();
        async move {
            self.ensure_projection_healthy(&shard)?;
            let fingerprint = BodyHash(item_mutation_fingerprint(&request)?);
            let request_id = request.request_id.clone();
            let evaluated_at = request.evaluated_at;
            let projection = Arc::clone(&self.projection);
            let log = Arc::clone(&self.log);
            let ids = Arc::clone(&self.ids);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        let replay_shard = shard.clone();
                        let replay_request_id = request_id.clone();
                        if let Some(response) = projection
                            .execute(move |store| {
                                ProjectionStore::replay_durable_item_mutation(
                                    store,
                                    &replay_shard,
                                    &replay_request_id,
                                    fingerprint.0,
                                    evaluated_at,
                                )
                            })
                            .await?
                        {
                            return Ok(response);
                        }
                        if let Some(response) = retained_item_mutation_response(
                            log.as_ref(),
                            &shard,
                            &request_id,
                            fingerprint.0,
                        )
                        .await?
                        {
                            return Ok(response);
                        }
                        if request.dry_run {
                            let plan_shard = shard.clone();
                            return projection
                                .execute(move |store| {
                                    ProjectionStore::plan_item_mutation(
                                        store,
                                        &plan_shard,
                                        &request,
                                    )
                                    .map(|plan| plan.response)
                                })
                                .await;
                        }
                        let epoch =
                            AsyncLogStore::current_epoch(log.as_ref(), shard.clone()).await?;
                        if expected_epoch.is_some_and(|expected| expected != epoch) {
                            return Err(EngineError::EpochFenced);
                        }
                        let plan_shard = shard.clone();
                        let mut plan = projection
                            .execute(move |store| {
                                ProjectionStore::plan_item_mutation(store, &plan_shard, &request)
                            })
                            .await?;
                        let response_payload = serde_json::to_string(&plan.response)
                            .map_err(|error| EngineError::Storage(error.to_string()))?;
                        let item_ids = plan
                            .command
                            .items
                            .iter()
                            .map(|item| item.item_id)
                            .collect::<Vec<_>>();
                        let mut envelope = make_envelope(
                            ids.as_ref(),
                            QueueCommand::MutateItems(plan.command),
                            item_ids,
                            evaluated_at,
                        );
                        envelope.request_id = Some(request_id);
                        envelope.request_fingerprint = Some(fingerprint.0);
                        envelope.request_outcome =
                            Some(fireweed_engine::RequestOutcome::ItemMutation {
                                response_payload,
                            });
                        strategy
                            .commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                            .await?;
                        plan.response.position =
                            AsyncLogStore::high_water(log.as_ref(), shard.clone()).await?;
                        Ok(plan.response)
                    })
                })
                .await
                .map_err(|error| {
                    EngineError::Storage(format!("async mutate_items submission failed: {error:?}"))
                })?
        }
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
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogPostgresBackend {
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        QueryCapabilityFlags {
            range_scan: true,
            grouped_aggregate: true,
            declared_bucket_segment: true,
            bounded_mutation: true,
            claim_by_query: true,
            side_record_query: false,
            claim_by_item_ids: false,
        }
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<RangeScanResponse>> + Send {
        let health = self.ensure_projection_healthy(shard);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            health?;
            projection
                .execute(move |store| ProjectionStore::range_scan(store, &shard, request))
                .await
        }
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<GroupedAggregateResponse>> + Send {
        let health = self.ensure_projection_healthy(shard);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            health?;
            projection
                .execute(move |store| ProjectionStore::grouped_aggregate(store, &shard, request))
                .await
        }
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::QueueMetrics>> + Send {
        let health = self.ensure_projection_healthy(shard);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            health?;
            projection
                .execute(move |store| ProjectionStore::metrics_by_query(store, &shard, request))
                .await
        }
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<Output = EngineResult<DeclaredBucketSegmentResponse>> + Send {
        let health = self.ensure_projection_healthy(shard);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            health?;
            projection
                .execute(move |store| {
                    ProjectionStore::declared_bucket_segment(store, &shard, request)
                })
                .await
        }
    }

    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: BoundedMutationRequest,
        context: BoundedMutationContext,
    ) -> impl std::future::Future<Output = EngineResult<BoundedMutationResponse>> + Send {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let projection = Arc::clone(&self.projection);
            let ids = Arc::clone(&self.ids);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        let plan_shard = shard.clone();
                        let plan = projection
                            .execute(move |store| {
                                let plan = ProjectionStore::plan_bounded_mutation(
                                    store,
                                    &plan_shard,
                                    request,
                                )?;
                                for update in &plan.updates {
                                    ProjectionStore::update_fields_validate(
                                        store,
                                        &plan_shard,
                                        &update.command.item_id,
                                        Some(update.expected_item_version),
                                    )?;
                                    ProjectionStore::index_validate_update(
                                        store,
                                        &plan_shard,
                                        &update.command.item_id,
                                        &update.command.field_ops,
                                        update.command.set_entity_document.as_ref(),
                                    )?;
                                }
                                Ok(plan)
                            })
                            .await?;
                        let response = plan.response;
                        for update in plan.updates {
                            let item_id = update.command.item_id;
                            let envelope = make_envelope(
                                ids.as_ref(),
                                QueueCommand::UpdateFields(update.command),
                                vec![item_id],
                                context.now,
                            );
                            strategy
                                .commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                                .await?;
                        }
                        Ok(response)
                    })
                })
                .await
                .map_err(map_submit_error)?
        }
    }

    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: ClaimByQueryRequest,
        context: ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let projection = Arc::clone(&self.projection);
            let control = Arc::clone(&self.control);
            let ids = Arc::clone(&self.ids);
            let idempotency = Arc::clone(&self.claim_by_query_idempotency);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        let definition =
                            AsyncControlPlane::queue_definition(control.as_ref(), shard.clone())
                                .await?;
                        if request.max_items == 0
                            || u64::from(request.max_items) > definition.max_claim_batch_size
                        {
                            return Err(EngineError::Invalid("invalid claim_by_query max_items"));
                        }
                        if request.lease_duration_ms == 0
                            || request.lease_duration_ms > definition.max_lease_duration_ms
                        {
                            return Err(EngineError::Invalid(
                                "invalid claim_by_query lease_duration_ms",
                            ));
                        }
                        let request_id = request
                            .request_id
                            .clone()
                            .ok_or(EngineError::Invalid("claim_by_query request_id required"))?;
                        let fingerprint = claim_by_query_body_hash(&request)?;
                        let expires_at =
                            request_expires_at(context.now, definition.request_id_retention_ms);

                        let decision = {
                            idempotency
                                .lock()
                                .expect("claim_by_query idempotency poisoned")
                                .entry(shard.clone())
                                .or_default()
                                .check_conflict_first(&request_id, fingerprint, context.now)
                        };
                        match decision {
                            IdempotencyDecision::Replay((item_ids, lease_token)) => {
                                let items = AsyncProjectionStore::render_claimed(
                                    projection.as_ref(),
                                    shard.clone(),
                                    item_ids.clone(),
                                )
                                .await?;
                                if items.len() != item_ids.len()
                                    || items
                                        .iter()
                                        .any(|item| item.lease_expires_at <= context.now)
                                    || items
                                        .iter()
                                        .any(|item| item.lease_token.as_ref() != Some(&lease_token))
                                {
                                    return Err(EngineError::RequestExpired);
                                }
                                return Ok(Claimed {
                                    items,
                                    ..Default::default()
                                });
                            }
                            IdempotencyDecision::Conflict => {
                                return Err(EngineError::RequestIdConflict);
                            }
                            IdempotencyDecision::Expired => {
                                return Err(EngineError::RequestExpired);
                            }
                            IdempotencyDecision::Proceed => {}
                        }

                        let eligible: HashSet<ItemId> = AsyncProjectionStore::eligible_candidates(
                            projection.as_ref(),
                            shard.clone(),
                            context.eligibility_at(),
                            usize::MAX,
                        )
                        .await?
                        .into_iter()
                        .collect();
                        let page_size = request.max_items.clamp(1, 1_000);
                        let mut cursor = None;
                        let mut item_ids = Vec::new();
                        while item_ids.len() < request.max_items as usize {
                            let scan_shard = shard.clone();
                            let scan_request = RangeScanRequest {
                                index: request.index.clone(),
                                filters: request.filters.clone(),
                                order_by: vec![request.order_by.clone()],
                                page_size,
                                cursor,
                            };
                            let page = projection
                                .execute(move |store| {
                                    ProjectionStore::range_scan(store, &scan_shard, scan_request)
                                })
                                .await?;
                            item_ids.extend(
                                page.rows
                                    .into_iter()
                                    .map(|row| row.item_id)
                                    .filter(|item_id| eligible.contains(item_id)),
                            );
                            item_ids.truncate(request.max_items as usize);
                            cursor = page.next_cursor;
                            if cursor.is_none() {
                                break;
                            }
                        }

                        let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
                        let (lease_token, claim_item_ids) = if item_ids.is_empty() {
                            (
                                LeaseToken::new("empty-claim").expect("valid token"),
                                Vec::new(),
                            )
                        } else {
                            (generate_query_lease_token()?, item_ids)
                        };
                        let mut envelope = make_envelope(
                            ids.as_ref(),
                            QueueCommand::Claim(fireweed_engine::ClaimCommand {
                                item_ids: claim_item_ids.clone(),
                                lease_token: lease_token.clone(),
                                lease_expires_at,
                                worker_id: Some(request.worker_id.clone()),
                            }),
                            claim_item_ids.clone(),
                            context.now,
                        );
                        envelope.request_id = Some(request_id.clone());
                        envelope.request_fingerprint = Some(fingerprint.0);
                        envelope.request_outcome =
                            Some(fireweed_engine::RequestOutcome::ClaimByQuery {
                                item_ids: claim_item_ids.clone(),
                                lease_token: lease_token.clone(),
                                worker_id: Some(request.worker_id),
                            });
                        strategy
                            .commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                            .await?;
                        let items = AsyncProjectionStore::render_claimed(
                            projection.as_ref(),
                            shard.clone(),
                            claim_item_ids.clone(),
                        )
                        .await?;
                        let replay_expires_at = if claim_item_ids.is_empty() {
                            expires_at
                        } else {
                            expires_at.max(lease_expires_at)
                        };
                        record_claim_by_query_idempotency(
                            &idempotency,
                            &shard,
                            request_id,
                            fingerprint,
                            claim_item_ids,
                            lease_token,
                            replay_expires_at,
                        );
                        Ok(Claimed {
                            items,
                            ..Default::default()
                        })
                    })
                })
                .await
                .map_err(|error| {
                    EngineError::Storage(format!(
                        "async claim_by_query submission failed: {error:?}"
                    ))
                })?
        }
    }
}
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

    /// Clone the authoritative log handle for asynchronous lifecycle operations.
    pub fn log_store(&self) -> Arc<ObjectLogEngineStore> {
        Arc::clone(&self.log)
    }

    /// Clone the owned asynchronous projection handle for lifecycle validation.
    pub fn projection_store(&self) -> AsyncPostgresRelationalProjection {
        self.postgres_projection.as_ref().clone()
    }

    /// Read the disposable projection queue catalog on its owned adapter thread.
    pub async fn projection_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        AsyncProjectionStore::recover_definitions(self.postgres_projection.as_ref()).await
    }

    /// Read the disposable projection recovery cursor on its owned adapter thread.
    pub async fn projection_high_water(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Option<fireweed_engine::CommandPosition>> {
        AsyncProjectionStore::recovery_high_water(self.postgres_projection.as_ref(), shard.clone())
            .await
    }

    /// Ensure a projection shard through the owned adapter thread.
    pub async fn ensure_projection_shard(&self, definition: QueueDefinition) -> EngineResult<()> {
        AsyncProjectionStore::ensure_shard(self.postgres_projection.as_ref(), definition).await
    }

    /// Apply a recovery batch through the owned adapter thread.
    pub async fn apply_projection_recovery(
        &self,
        positions: Vec<fireweed_engine::CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        AsyncProjectionStore::apply_recovery(self.postgres_projection.as_ref(), positions, commands)
            .await
    }

    /// Delete the disposable projection on its owned adapter thread.
    pub async fn delete_projection(&self) -> EngineResult<()> {
        self.postgres_projection.delete_projection().await
    }

    pub fn pause_async_projection_apply(&self) -> EngineResult<()> {
        let coordinator = self.async_apply.as_ref().ok_or(EngineError::Invalid(
            "async-projection-control-requires-async-barrier",
        ))?;
        coordinator.pause();
        Ok(())
    }

    pub fn resume_async_projection_apply(&self) -> EngineResult<()> {
        let coordinator = self.async_apply.as_ref().ok_or(EngineError::Invalid(
            "async-projection-control-requires-async-barrier",
        ))?;
        coordinator.resume();
        Ok(())
    }

    pub async fn async_projection_snapshot(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<AsyncProjectionApplySnapshot> {
        let coordinator = self.async_apply.as_ref().ok_or(EngineError::Invalid(
            "async-projection-control-requires-async-barrier",
        ))?;
        Ok(coordinator.snapshot(shard).await)
    }

    pub async fn wait_for_async_projection_catch_up(&self, shard: &QueueKey) -> EngineResult<()> {
        let coordinator = self.async_apply.as_ref().ok_or(EngineError::Invalid(
            "async-projection-control-requires-async-barrier",
        ))?;
        coordinator.wait_for_catch_up(shard).await
    }
}

#[cfg(test)]
mod async_projection {
    use std::sync::atomic::{AtomicU64, Ordering};

    use fireweed_core::{
        EligibilityPolicy, OrderingMode, PriorityModel, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy,
    };

    use super::*;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);

    fn fixture(tag: &str) -> (String, std::path::PathBuf) {
        let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        (
            format!("fireweed_async_pg_{tag}_{}_{}", std::process::id(), id),
            std::env::temp_dir().join(format!(
                "fireweed-async-projection-postgres-{tag}-{}-{id}",
                std::process::id()
            )),
        )
    }

    fn flush() -> FlushConfig {
        FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        }
    }

    fn spec() -> AsyncProjectionSpec {
        AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()
    }

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

    async fn open(
        url: &str,
        schema: &str,
        log_root: &std::path::Path,
        async_spec: Option<AsyncProjectionSpec>,
    ) -> AsyncObjectLogPostgresBackend {
        let log = ObjectLogEngineStore::open_local(log_root, flush())
            .await
            .unwrap();
        let projection = AsyncPostgresRelationalProjection::connect_in_schema(url, schema)
            .await
            .unwrap();
        match async_spec {
            Some(spec) => {
                AsyncObjectLogPostgresBackend::from_log_and_projection_with_async_projection(
                    log, projection, 0, spec,
                )
                .await
                .unwrap()
            }
            None => AsyncObjectLogPostgresBackend::from_log_and_projection(log, projection, 0)
                .await
                .unwrap(),
        }
    }

    async fn create(backend: &AsyncObjectLogPostgresBackend) -> QueueKey {
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        shard
    }

    async fn push_one(
        backend: &AsyncObjectLogPostgresBackend,
        shard: &QueueKey,
        timestamp: i64,
    ) -> EngineResult<Vec<ItemId>> {
        backend
            .push(
                shard,
                vec![PushSpec::default()],
                UtcTimestamp::new(timestamp, 0).unwrap(),
                None,
            )
            .await
    }

    async fn selected_pending(backend: &AsyncObjectLogPostgresBackend, shard: &QueueKey) -> u64 {
        let shard = shard.clone();
        backend
            .postgres_projection
            .execute(move |store| ProjectionStore::metrics(store, &shard))
            .await
            .unwrap()
            .pending
    }

    fn assert_backpressure(error: EngineError, expected_resource: &'static str) {
        assert!(matches!(
            error,
            EngineError::Backpressure { resource } if resource == expected_resource
        ));
    }

    #[test]
    fn async_projection_all_five_bounds_are_required_without_a_live_fixture() {
        assert!(AsyncProjectionSpec::new(0, 1, 1, 1, 1).is_err());
        assert!(AsyncProjectionSpec::new(1, 0, 1, 1, 1).is_err());
        assert!(AsyncProjectionSpec::new(1, 1, 0, 1, 1).is_err());
        assert!(AsyncProjectionSpec::new(1, 1, 1, 0, 1).is_err());
        assert!(AsyncProjectionSpec::new(1, 1, 1, 1, 0).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_projection_both_barriers_and_ordered_watermark_catch_up() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES ASYNC PROJECTION SKIPPED (barriers/catch-up) — set FIREWEED_PG_TEST_URL"
            );
            return;
        };

        let (strict_schema, strict_root) = fixture("strict");
        let strict = open(&url, &strict_schema, &strict_root, None).await;
        let strict_shard = create(&strict).await;
        push_one(&strict, &strict_shard, 1).await.unwrap();
        assert_eq!(strict.durability_class(), DurabilityClass::Atomic);
        assert_eq!(selected_pending(&strict, &strict_shard).await, 1);

        let (async_schema, async_root) = fixture("async");
        let asynchronous = open(&url, &async_schema, &async_root, Some(spec())).await;
        let shard = create(&asynchronous).await;
        asynchronous.pause_async_projection_apply().unwrap();
        push_one(&asynchronous, &shard, 1).await.unwrap();
        push_one(&asynchronous, &shard, 2).await.unwrap();
        assert_eq!(
            asynchronous.durability_class(),
            DurabilityClass::EventualApply
        );
        assert_eq!(asynchronous.metrics(&shard).await.unwrap().pending, 2);
        assert_eq!(selected_pending(&asynchronous, &shard).await, 0);
        let debt = asynchronous
            .async_projection_snapshot(&shard)
            .await
            .unwrap();
        assert_eq!(debt.apply_lag_commands, 2);
        assert_eq!(debt.apply_queue_depth, 2);
        asynchronous.resume_async_projection_apply().unwrap();
        asynchronous
            .wait_for_async_projection_catch_up(&shard)
            .await
            .unwrap();
        assert_eq!(selected_pending(&asynchronous, &shard).await, 2);
        assert_eq!(
            asynchronous
                .projection_high_water(&shard)
                .await
                .unwrap()
                .unwrap()
                .sequence,
            1
        );

        strict.postgres_projection.close_and_drain().await.unwrap();
        asynchronous
            .postgres_projection
            .close_and_drain()
            .await
            .unwrap();
        std::fs::remove_dir_all(strict_root).unwrap();
        std::fs::remove_dir_all(async_root).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_projection_common_bounds_and_poison_fail_closed() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!(
                "POSTGRES ASYNC PROJECTION SKIPPED (bounds/poison) — set FIREWEED_PG_TEST_URL"
            );
            return;
        };

        let cases = [
            (
                "lag",
                AsyncProjectionSpec::new(1, 1024 * 1024, 16, 30_000, 3).unwrap(),
                "async-projection-apply-lag-commands",
            ),
            (
                "depth",
                AsyncProjectionSpec::new(32, 1024 * 1024, 1, 30_000, 3).unwrap(),
                "async-projection-apply-queue-depth",
            ),
        ];
        for (tag, bound, resource) in cases {
            let (schema, root) = fixture(tag);
            let backend = open(&url, &schema, &root, Some(bound)).await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            push_one(&backend, &shard, 1).await.unwrap();
            assert_backpressure(push_one(&backend, &shard, 2).await.unwrap_err(), resource);
            backend.postgres_projection.close_and_drain().await.unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }

        let (debt_schema, debt_root) = fixture("debt");
        let debt = open(
            &url,
            &debt_schema,
            &debt_root,
            Some(AsyncProjectionSpec::new(32, 1, 16, 30_000, 3).unwrap()),
        )
        .await;
        let debt_shard = create(&debt).await;
        debt.pause_async_projection_apply().unwrap();
        assert_backpressure(
            push_one(&debt, &debt_shard, 1).await.unwrap_err(),
            "async-projection-apply-debt-bytes",
        );
        debt.postgres_projection.close_and_drain().await.unwrap();
        std::fs::remove_dir_all(debt_root).unwrap();

        let (age_schema, age_root) = fixture("age");
        let age = open(
            &url,
            &age_schema,
            &age_root,
            Some(AsyncProjectionSpec::new(32, 1024 * 1024, 16, 1, 3).unwrap()),
        )
        .await;
        let age_shard = create(&age).await;
        age.pause_async_projection_apply().unwrap();
        push_one(&age, &age_shard, 1).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_backpressure(
            push_one(&age, &age_shard, 2).await.unwrap_err(),
            "async-projection-oldest-unapplied-age",
        );
        age.postgres_projection.close_and_drain().await.unwrap();
        std::fs::remove_dir_all(age_root).unwrap();

        let (poison_schema, poison_root) = fixture("poison");
        let poison = open(
            &url,
            &poison_schema,
            &poison_root,
            Some(AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 2).unwrap()),
        )
        .await;
        let poison_shard = create(&poison).await;
        poison.pause_async_projection_apply().unwrap();
        push_one(&poison, &poison_shard, 1).await.unwrap();
        poison.delete_projection().await.unwrap();
        poison.resume_async_projection_apply().unwrap();
        assert!(matches!(
            poison
                .wait_for_async_projection_catch_up(&poison_shard)
                .await
                .unwrap_err(),
            EngineError::Storage(message) if message.contains("async projection poisoned")
        ));
        let snapshot = poison
            .async_projection_snapshot(&poison_shard)
            .await
            .unwrap();
        assert_eq!(snapshot.apply_retry_count, 2);
        assert!(snapshot.poison_reason.is_some());
        assert!(poison.metrics(&poison_shard).await.is_err());
        assert!(push_one(&poison, &poison_shard, 2).await.is_err());
        poison.postgres_projection.close_and_drain().await.unwrap();
        std::fs::remove_dir_all(poison_root).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_projection_reopen_resumes_transactional_cursor_without_duplicates() {
        let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
            eprintln!("POSTGRES ASYNC PROJECTION SKIPPED (reopen) — set FIREWEED_PG_TEST_URL");
            return;
        };
        let (schema, root) = fixture("reopen");
        let backend = open(&url, &schema, &root, Some(spec())).await;
        let shard = create(&backend).await;
        push_one(&backend, &shard, 1).await.unwrap();
        push_one(&backend, &shard, 2).await.unwrap();
        backend
            .wait_for_async_projection_catch_up(&shard)
            .await
            .unwrap();
        assert_eq!(selected_pending(&backend, &shard).await, 2);
        backend.postgres_projection.close_and_drain().await.unwrap();
        drop(backend);

        let reopened = open(&url, &schema, &root, Some(spec())).await;
        assert_eq!(reopened.metrics(&shard).await.unwrap().pending, 2);
        assert_eq!(selected_pending(&reopened, &shard).await, 2);
        assert_eq!(
            reopened
                .async_projection_snapshot(&shard)
                .await
                .unwrap()
                .apply_queue_depth,
            0
        );
        push_one(&reopened, &shard, 3).await.unwrap();
        reopened
            .wait_for_async_projection_catch_up(&shard)
            .await
            .unwrap();
        assert_eq!(selected_pending(&reopened, &shard).await, 3);
        assert_eq!(
            reopened
                .projection_high_water(&shard)
                .await
                .unwrap()
                .unwrap()
                .sequence,
            2
        );
        reopened
            .postgres_projection
            .close_and_drain()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
