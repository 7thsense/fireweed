//! Async object-log × sqlite projection over crates.io [`object_log::LogEngine`].
//!
//! SQLite apply is always durable on the success path, so this product is Strict-equivalent
//! (atomic response-after-apply) and advertises full commit-transition capabilities
//! (see [`crate::commit_surface`]).

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
    InProcessControlPlane, InProcessProjectionStore, OwnedTask, ProjectionClaimPlanner,
    ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner,
    ProjectionStore, PurgePort, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey,
    RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver,
    ReclaimPort, RenewLeasePort, SeparateReplayCommit, SeparateReplayCommitter, TickReport,
    UpsertOutcome, UpsertPort,
};
use fireweed_sqlite::SqliteProjectionStore;
use object_log::FlushConfig;

use crate::ObjectLogEngineStore;
use crate::async_product::SeqIdGen;
use crate::commit_surface::{
    self, CommitIdempotency, new_commit_idempotency, strict_commit_capabilities,
};
use crate::port_surface::{
    self, BatchUpdateIdempotency, ClaimByItemIdsIdempotency, ClaimByQueryIdempotency,
    PreparedBatchUpdate, PreparedClaimByItemIds, PreparedClaimByQuery, PreparedUpsert,
    new_batch_update_idempotency, new_claim_by_item_ids_idempotency,
    new_claim_by_query_idempotency,
};
use crate::recovery_stats::{
    RecoveryStats, RecoveryStatsMap, rebuild_process_idempotency_from_log,
    replay_log_into_projection,
};

type Proj = InProcessProjectionStore<SqliteProjectionStore>;

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

/// LogEngine × durable sqlite projection (async composition).
pub struct AsyncObjectLogSqliteBackend {
    engine: Engine,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    commit_idempotency: CommitIdempotency,
    batch_update_idempotency: BatchUpdateIdempotency,
    claim_by_query_idempotency: ClaimByQueryIdempotency,
    claim_by_item_ids_idempotency: ClaimByItemIdsIdempotency,
    recovery_stats: RecoveryStatsMap,
}

impl AsyncObjectLogSqliteBackend {
    pub async fn open(
        log_root: impl AsRef<std::path::Path>,
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = SqliteProjectionStore::open(projection_path)?;
        Self::from_parts(log, projection_store, node_id).await
    }

    pub async fn open_memory_log(
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_memory(flush).await?);
        let projection_store = if projection_path == ":memory:" {
            SqliteProjectionStore::in_memory()?
        } else {
            SqliteProjectionStore::open(projection_path)?
        };
        Self::from_parts(log, projection_store, node_id).await
    }

    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection_store: SqliteProjectionStore,
        node_id: u8,
    ) -> EngineResult<Self> {
        Self::from_parts(Arc::new(log), projection_store, node_id).await
    }

    async fn from_parts(
        log: Arc<ObjectLogEngineStore>,
        projection_store: SqliteProjectionStore,
        node_id: u8,
    ) -> EngineResult<Self> {
        let projection = Arc::new(InProcessProjectionStore::new(projection_store));
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
            crate::ObjectLogTaskDispatcher::new(),
            claim,
            push,
            1024,
        )
        .with_lifecycle_planner(lifecycle)
        .with_reclaim_planner(reclaim);

        let recovery_stats = RecoveryStatsMap::new();
        let definitions = AsyncLogStore::recover_definitions(log.as_ref()).await?;
        for definition in definitions {
            let _ = AsyncControlPlane::create_queue(control.as_ref(), definition.clone()).await;
            AsyncProjectionStore::ensure_shard(projection.as_ref(), definition.clone()).await?;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            // Durable SQLite projection: snapshot high-water + bounded tail replay.
            let stats =
                replay_log_into_projection(log.as_ref(), projection.as_ref(), &shard, true).await?;
            recovery_stats.insert(shard.clone(), stats);
            projection
                .with_store(|p| ProjectionStore::restore_counters(p, &shard, counters.as_ref()))?;
            // Process-local request-id maps are not part of the SQLite snapshot; rebuild from log.
            rebuild_process_idempotency_from_log(
                log.as_ref(),
                &shard,
                definition.request_id_retention_ms,
                &commit_idempotency,
                &batch_update_idempotency,
                &claim_by_query_idempotency,
                &claim_by_item_ids_idempotency,
            )
            .await?;
            // High-water open never re-applies historical commands into process-local caches:
            // lease cleartext (hashes only in SQLite) and push request-id maps on the wrapper.
            {
                let mut from = None;
                let mut envelopes = Vec::new();
                loop {
                    let page =
                        AsyncLogStore::read_from(log.as_ref(), shard.clone(), from.clone(), 256)
                            .await?;
                    if page.entries.is_empty() {
                        break;
                    }
                    for (_, env) in page.entries {
                        envelopes.push(env);
                    }
                    match page.next {
                        Some(next) => from = Some(next),
                        None => break,
                    }
                }
                projection.with_store(|p| p.rehydrate_lease_cleartext(&shard, &envelopes))?;
                projection.rehydrate_push_idempotency(
                    &shard,
                    &envelopes,
                    definition.request_id_retention_ms,
                )?;
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
            batch_update_idempotency,
            claim_by_query_idempotency,
            claim_by_item_ids_idempotency,
            recovery_stats,
        })
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

    async fn submit_envelopes(
        &self,
        shard: &QueueKey,
        envelopes: Vec<CommandEnvelope>,
        epoch: u64,
    ) -> EngineResult<()> {
        if envelopes.is_empty() {
            return Ok(());
        }
        self.engine
            .submit_commit(RawCommitRequest::new(shard.clone(), envelopes, epoch))
            .await
            .map_err(|error| {
                EngineError::Storage(format!("async product port submission failed: {error:?}"))
            })??;
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

impl Backend for AsyncObjectLogSqliteBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }
    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }
    fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
        // Durable SQLite apply completes before success — Strict-equivalent response-after-apply.
        strict_commit_capabilities(
            "Strict: object-log append then durable sqlite projection apply (response-after-apply, LogEngine)",
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

impl ControlPlaneStore for AsyncObjectLogSqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            // Persist definition so reopen can discover and recover this shard.
            self.log.register_definition(definition.clone()).await?;
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            AsyncProjectionStore::ensure_shard(self.projection.as_ref(), definition.clone())
                .await?;
            // Recover any durable tail not yet applied (first create is a no-op tail).
            if self.recovery_stats.get(&shard).is_none() {
                let stats = replay_log_into_projection(
                    self.log.as_ref(),
                    self.projection.as_ref(),
                    &shard,
                    true,
                )
                .await?;
                self.recovery_stats.insert(shard, stats);
            }
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

impl PushPort for AsyncObjectLogSqliteBackend {
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

impl ClaimPort for AsyncObjectLogSqliteBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl FinalizePort for AsyncObjectLogSqliteBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // fireweed-c8e0a7a5 / fireweed-2be744bd: resolve leases under the same queue permit as
        // plan+commit so a concurrent reclaim/claim cannot invalidate a freshly observed token.
        async move {
            self.engine
                .finalize_outcomes(shard.clone(), outcomes, now, expected_epoch)
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl RenewLeasePort for AsyncObjectLogSqliteBackend {
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

impl ReassignLeasePort for AsyncObjectLogSqliteBackend {
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

impl PurgePort for AsyncObjectLogSqliteBackend {
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

impl UpsertPort for AsyncObjectLogSqliteBackend {
    fn replace_if_pending(
        &self,
        shard: &QueueKey,
        client_item_key: &ClientItemKey,
        priority: Option<PriorityValue>,
        group_key: Option<GroupKey>,
        not_before: Option<UtcTimestamp>,
        payload: Option<Bytes>,
        fields: std::collections::BTreeMap<String, Bytes>,
        metadata: Metadata,
        entity: Option<serde_json::Value>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        let client_item_key = client_item_key.clone();
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let PreparedUpsert { envelopes, outcome } = port_surface::prepare_upsert(
                self.projection.as_ref(),
                self.control.as_ref(),
                self.ids.as_ref(),
                self.counters.as_ref(),
                self.node_id,
                epoch,
                &shard,
                client_item_key,
                priority,
                group_key,
                not_before,
                payload,
                fields,
                metadata,
                entity,
                now,
            )
            .await?;
            self.submit_envelopes(&shard, envelopes, epoch).await?;
            Ok(outcome)
        }
    }
}

impl ReclaimPort for AsyncObjectLogSqliteBackend {
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

impl ReclaimDriver for AsyncObjectLogSqliteBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

impl ProjectionRead for AsyncObjectLogSqliteBackend {
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

// LibBackend / facade ports: full product surface (parity with AsyncLogReplayBackend).
impl fireweed_engine::UpdateFieldsPort for AsyncObjectLogSqliteBackend {
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: std::collections::BTreeMap<String, Option<Bytes>>,
        payload: fireweed_engine::PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let envelope = port_surface::prepare_update_fields(
                self.projection.as_ref(),
                self.control.as_ref(),
                self.ids.as_ref(),
                &shard,
                item_id,
                field_ops,
                payload,
                entity,
                expected_item_version,
                now,
            )
            .await?;
            self.submit_envelopes(&shard, vec![envelope], epoch).await?;
            port_surface::item_version_after(self.projection.as_ref(), &shard, item_id)
        }
    }
}
impl fireweed_engine::CommitTransitionPort for AsyncObjectLogSqliteBackend {
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
            // fireweed-5497780d: prepare (instance-fence validation) + append/apply must share the
            // queue-local admission permit. submit_commit alone only serializes append+apply, so
            // concurrent prepares could both pass the same fence and LWW-overwrite side records.
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
                        let prepared = commit_surface::prepare_commit_transition(
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
                        commit_surface::finish_prepared_commit_transition(
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
                .map_err(commit_surface::map_submit_error)?
        }
    }
}

impl fireweed_engine::RecoveryReadPort for AsyncObjectLogSqliteBackend {
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::CommitRecovery>>> + Send
    {
        std::future::ready(commit_surface::explain_commit_if_authoritative(
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
        std::future::ready(commit_surface::side_record(
            self.projection.as_ref(),
            shard,
            &key,
        ))
    }
}

impl fireweed_engine::BatchUpdatePort for AsyncObjectLogSqliteBackend {
    fn batch_update(
        &self,
        shard: &QueueKey,
        request: fireweed_engine::BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::BatchUpdateResponse>> + Send
    {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            match port_surface::prepare_batch_update(
                self.projection.as_ref(),
                self.control.as_ref(),
                self.ids.as_ref(),
                &self.batch_update_idempotency,
                self.supports_gates(),
                &shard,
                request,
                now,
            )
            .await?
            {
                PreparedBatchUpdate::Replay(response) => Ok(response),
                PreparedBatchUpdate::Proceed {
                    envelopes,
                    response,
                    request_id,
                    fingerprint,
                    expires_at,
                } => {
                    self.submit_envelopes(&shard, envelopes, epoch).await?;
                    port_surface::record_batch_update_idempotency(
                        &self.batch_update_idempotency,
                        &shard,
                        request_id,
                        fingerprint,
                        response.clone(),
                        expires_at,
                    );
                    Ok(response)
                }
            }
        }
    }
}
impl fireweed_engine::ItemMutationPort for AsyncObjectLogSqliteBackend {
    fn mutate_items(
        &self,
        _shard: &QueueKey,
        _request: fireweed_engine::ItemMutationRequest,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ItemMutationResponse>> + Send
    {
        // ItemMutationPort remains un-wired (selector evaluation / dry-run plan); not in AC.
        std::future::ready(Err(EngineError::Unavailable))
    }
}
impl fireweed_engine::SetGatesPort for AsyncObjectLogSqliteBackend {
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: fireweed_engine::SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let envelope = port_surface::make_envelope(
                self.ids.as_ref(),
                QueueCommand::SetGates(command),
                Vec::new(),
                now,
            );
            self.submit_envelopes(&shard, vec![envelope], epoch).await
        }
    }
}
impl fireweed_engine::ReschedulePort for AsyncObjectLogSqliteBackend {
    fn reschedule(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        set_priority: fireweed_engine::ScheduleUpdate<PriorityValue>,
        set_not_before: fireweed_engine::ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let envelope = port_surface::prepare_reschedule(
                self.projection.as_ref(),
                self.ids.as_ref(),
                &shard,
                item_id,
                set_priority,
                set_not_before,
                expected_item_version,
                now,
            )?;
            self.submit_envelopes(&shard, vec![envelope], epoch).await?;
            port_surface::item_version_after(self.projection.as_ref(), &shard, item_id)
        }
    }
}
impl fireweed_engine::DiscoveryPort for AsyncObjectLogSqliteBackend {
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: fireweed_engine::DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ActiveScope>>> + Send
    {
        std::future::ready(
            self.projection.with_store(|p| {
                ProjectionStore::discover_active_scopes(p, shard, granularity, now)
            }),
        )
    }
}
impl fireweed_engine::IndexQueryPort for AsyncObjectLogSqliteBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::IndexHit>>> + Send
    {
        std::future::ready(port_surface::index_get_unique(
            self.projection.as_ref(),
            shard,
            index,
            key,
        ))
    }
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send
    {
        std::future::ready(port_surface::index_lookup(
            self.projection.as_ref(),
            shard,
            index,
            key,
        ))
    }
}
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogSqliteBackend {
    fn hot_projection_capabilities(&self, shard: &QueueKey) -> fireweed_core::QueryCapabilityFlags {
        port_surface::hot_projection_capabilities(self.projection.as_ref(), shard)
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: fireweed_core::RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::RangeScanResponse>> + Send
    {
        std::future::ready(port_surface::range_scan(
            self.projection.as_ref(),
            shard,
            request,
        ))
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: fireweed_core::GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::GroupedAggregateResponse>> + Send
    {
        std::future::ready(port_surface::grouped_aggregate(
            self.projection.as_ref(),
            shard,
            request,
        ))
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: fireweed_core::MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::QueueMetrics>> + Send {
        std::future::ready(port_surface::metrics_by_query(
            self.projection.as_ref(),
            shard,
            request,
        ))
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: fireweed_core::DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<
        Output = EngineResult<fireweed_core::DeclaredBucketSegmentResponse>,
    > + Send {
        std::future::ready(port_surface::declared_bucket_segment(
            self.projection.as_ref(),
            shard,
            request,
        ))
    }

    fn bounded_mutation(
        &self,
        shard: &QueueKey,
        request: fireweed_core::BoundedMutationRequest,
        context: fireweed_engine::BoundedMutationContext,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::BoundedMutationResponse>> + Send
    {
        let shard = shard.clone();
        async move {
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let prepared = port_surface::prepare_bounded_mutation(
                self.projection.as_ref(),
                self.ids.as_ref(),
                &shard,
                request,
                context,
            )?;
            for (envelope, _, _) in prepared.envelopes {
                self.submit_envelopes(&shard, vec![envelope], epoch).await?;
            }
            Ok(prepared.response)
        }
    }

    fn claim_by_query(
        &self,
        shard: &QueueKey,
        request: fireweed_core::ClaimByQueryRequest,
        context: fireweed_engine::ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let shard = shard.clone();
        // fireweed-2ad3a030 / snorri: claim_by_query → commit_transition must observe the
        // post-apply lease under the same serialized view as prepare (parity with claim_by_item_ids).
        async move {
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let projection = Arc::clone(&self.projection);
            let control = Arc::clone(&self.control);
            let ids = Arc::clone(&self.ids);
            let claim_by_query_idempotency = Arc::clone(&self.claim_by_query_idempotency);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        match port_surface::prepare_claim_by_query(
                            projection.as_ref(),
                            control.as_ref(),
                            ids.as_ref(),
                            &claim_by_query_idempotency,
                            &shard,
                            request,
                            context,
                        )
                        .await?
                        {
                            PreparedClaimByQuery::Replay(claimed) => Ok(claimed),
                            PreparedClaimByQuery::Proceed {
                                envelope,
                                item_ids,
                                lease_token,
                                request_id,
                                fingerprint,
                                replay_expires_at,
                            } => {
                                strategy
                                    .commit(RawCommitRequest::new(
                                        shard.clone(),
                                        vec![envelope],
                                        epoch,
                                    ))
                                    .await?;
                                let items = port_surface::render_claimed(
                                    projection.as_ref(),
                                    &shard,
                                    &item_ids,
                                )?;
                                port_surface::record_claim_by_query_idempotency(
                                    &claim_by_query_idempotency,
                                    &shard,
                                    request_id,
                                    fingerprint,
                                    item_ids,
                                    lease_token,
                                    replay_expires_at,
                                );
                                Ok(Claimed {
                                    items,
                                    ..Default::default()
                                })
                            }
                        }
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

    fn claim_by_item_ids(
        &self,
        shard: &QueueKey,
        request: fireweed_core::ClaimByItemIdsRequest,
        context: fireweed_engine::ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ClaimByItemIdsResponse>> + Send
    {
        let shard = shard.clone();
        // fireweed-2be744bd: prepare eligibility against the same serialized projection view
        // that apply will see — hold the queue permit across prepare + append/apply.
        async move {
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let projection = Arc::clone(&self.projection);
            let control = Arc::clone(&self.control);
            let ids = Arc::clone(&self.ids);
            let claim_by_item_ids_idempotency = Arc::clone(&self.claim_by_item_ids_idempotency);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        match port_surface::prepare_claim_by_item_ids(
                            projection.as_ref(),
                            control.as_ref(),
                            ids.as_ref(),
                            &claim_by_item_ids_idempotency,
                            &shard,
                            request,
                            context,
                        )
                        .await?
                        {
                            PreparedClaimByItemIds::Replay(response) => Ok(response),
                            PreparedClaimByItemIds::Proceed {
                                envelope,
                                claim_item_ids,
                                lease_token,
                                outcomes,
                                request_id,
                                fingerprint,
                                replay_expires_at,
                            } => {
                                strategy
                                    .commit(RawCommitRequest::new(
                                        shard.clone(),
                                        vec![envelope],
                                        epoch,
                                    ))
                                    .await?;
                                let items = port_surface::render_claimed(
                                    projection.as_ref(),
                                    &shard,
                                    &claim_item_ids,
                                )?;
                                port_surface::record_claim_by_item_ids_idempotency(
                                    &claim_by_item_ids_idempotency,
                                    &shard,
                                    request_id,
                                    fingerprint,
                                    claim_item_ids,
                                    lease_token,
                                    outcomes.clone(),
                                    replay_expires_at,
                                );
                                Ok(fireweed_engine::ClaimByItemIdsResponse { items, outcomes })
                            }
                        }
                    })
                })
                .await
                .map_err(|error| {
                    EngineError::Storage(format!(
                        "async claim_by_item_ids submission failed: {error:?}"
                    ))
                })?
        }
    }
}
impl fireweed_engine::HistoricalProjectionRead for AsyncObjectLogSqliteBackend {
    type AsOfProjection = fireweed_projection::InMemoryProjection;
    fn current_position(
        &self,
        _shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CommandPosition>> + Send
    {
        // Historical as-of reads remain Unavailable on LogEngine products for now.
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

impl fireweed_engine::LogRead for AsyncObjectLogSqliteBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<fireweed_engine::CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CommandPage>> + Send {
        crate::request_id_probe::read_from_log(self.log.as_ref(), shard.clone(), from, limit)
    }
}

impl fireweed_engine::RequestIdReplayProbe for AsyncObjectLogSqliteBackend {
    fn build_request_id_push_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, Vec<ItemId>)> {
        crate::request_id_probe::probe_axes(
            &self.log,
            &self.projection,
            &self.control,
            &self.ids,
            &self.counters,
            self.node_id,
        )
        .build_request_id_push_envelope(shard, request_id, items, now, expected_epoch)
    }

    fn build_request_id_commit_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        claim_ref: fireweed_engine::ClaimRef,
        finalize: fireweed_engine::FinalizeKind,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, fireweed_core::BodyHash)> {
        crate::request_id_probe::probe_axes(
            &self.log,
            &self.projection,
            &self.control,
            &self.ids,
            &self.counters,
            self.node_id,
        )
        .build_request_id_commit_envelope(
            shard,
            request_id,
            claim_ref,
            finalize,
            now,
            expected_epoch,
        )
    }

    fn build_request_id_commit_envelopes(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        entries: Vec<fireweed_engine::CommitTransitionEntry>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, fireweed_core::BodyHash)> {
        crate::request_id_probe::probe_axes(
            &self.log,
            &self.projection,
            &self.control,
            &self.ids,
            &self.counters,
            self.node_id,
        )
        .build_request_id_commit_envelopes(shard, request_id, entries, now, expected_epoch)
    }
}

impl AsyncObjectLogSqliteBackend {
    /// Borrow the authoritative log axis (lifecycle / diagnostics).
    pub fn with_log<R>(&self, f: impl FnOnce(&ObjectLogEngineStore) -> R) -> R {
        f(self.log.as_ref())
    }

    /// Borrow the projection axis (lifecycle / diagnostics).
    pub fn with_projection<R>(&self, f: impl FnOnce(&SqliteProjectionStore) -> R) -> R {
        self.projection.with_store(f)
    }

    /// Mutably borrow the projection axis (lifecycle rebuild / delete).
    pub fn with_projection_mut<R>(&self, f: impl FnOnce(&mut SqliteProjectionStore) -> R) -> R {
        self.projection.with_store_mut(f)
    }

    /// Production recovery telemetry captured at open / first create_queue for `shard`.
    pub fn recovery_stats(&self, shard: &QueueKey) -> Option<RecoveryStats> {
        self.recovery_stats.get(shard)
    }

    /// Bounded page of authoritative pending order (E3 recovery fingerprint path).
    pub fn recovery_order_page(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<fireweed_engine::ItemView>> {
        self.projection
            .with_store(|p| p.peek_page(shard, after, limit))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use fireweed_core::{
        EligibilityPolicy, LeaseToken, OrderingMode, PriorityModel, QueueDefinition, QueueId,
        RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        ClaimCompatibility, ClaimPort, ClaimRef, ClaimRequest, CommitEntryOutcome,
        CommitTransition, CommitTransitionEntry, CommitTransitionPort, ControlPlaneStore,
        EngineError, FinalizeKind, FinalizeOutcome, FinalizePort, InstanceFence, ProjectionRead,
        ProjectionStore, PushPort, PushSpec, RecoveryReadPort, SideRecord,
    };
    use object_log::FlushConfig;

    use super::AsyncObjectLogSqliteBackend;

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

    async fn open_backend() -> AsyncObjectLogSqliteBackend {
        AsyncObjectLogSqliteBackend::open_memory_log(
            ":memory:",
            FlushConfig {
                linger: std::time::Duration::ZERO,
                ..FlushConfig::default()
            },
            0,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn object_log_sqlite_push_claim_finalize() {
        let backend = open_backend().await;
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

    /// fireweed-2ad3a030: snorri path is claim_by_query then commit_transition with ClaimRef.
    #[tokio::test]
    async fn claim_by_query_then_immediate_commit_transition_succeeds() {
        use fireweed_core::{
            ClaimByQueryRequest, FilterOp, IndexDeclaration, IndexDef, IndexType, OrderField,
            QueryFilter, QueueIndex, SortDirection, TypedValue, WorkerId,
        };
        use fireweed_engine::{ClaimByQueryContext, HotProjectionQueryPort};

        let backend = open_backend().await;
        let mut def = qdef();
        def.typed_indexes = vec![QueueIndex {
            name: "by_rank".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "rank".into(),
                index_type: IndexType::Integer,
                unique: false,
            }),
        }];
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"payload")),
                    entity: Some(serde_json::json!({"rank": 1})),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let claimed = backend
            .claim_by_query(
                &shard,
                ClaimByQueryRequest {
                    index: Some("by_rank".into()),
                    filters: vec![QueryFilter {
                        field: "rank".into(),
                        op: FilterOp::Gte,
                        value: TypedValue::Integer(0),
                    }],
                    order_by: OrderField {
                        field: "rank".into(),
                        direction: SortDirection::Ascending,
                    },
                    max_items: 1,
                    lease_duration_ms: 60_000,
                    worker_id: WorkerId::new("worker").unwrap(),
                    request_id: Some(fireweed_core::RequestId::new("rid-query").unwrap()),
                },
                ClaimByQueryContext {
                    now: UtcTimestamp::new(2, 0).unwrap(),
                    eligibility_time: None,
                    expected_epoch: None,
                },
            )
            .await
            .expect("claim_by_query");
        assert_eq!(claimed.items.len(), 1);
        let item = &claimed.items[0];
        let token = item.lease_token.clone().expect("lease token");
        let outcomes = backend
            .commit_transition(
                &shard,
                CommitTransition {
                    request_id: None,
                    entries: vec![CommitTransitionEntry {
                        claim_ref: ClaimRef {
                            item_id: item.item_id,
                            lease_token: token,
                            lease_expires_at: item.lease_expires_at,
                            item_version: item.item_version,
                        },
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: Vec::new(),
                        lifecycle_items: Vec::new(),
                        instance_fence: None,
                    }],
                },
                UtcTimestamp::new(3, 0).unwrap(),
                None,
            )
            .await
            .expect("claim_by_query ClaimRef must commit_transition");
        assert!(matches!(
            outcomes.as_slice(),
            [CommitEntryOutcome::Committed { .. }]
        ));
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.leased, 0);
        assert!(metrics.complete >= 1);
    }

    /// fireweed-c8e0a7a5: claim then immediate Strict commit_transition must not see a stale lease.
    #[tokio::test]
    async fn claim_then_immediate_commit_transition_succeeds() {
        let backend = open_backend().await;
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let ids = backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"payload")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let lease = LeaseToken::new("lease-fresh").unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("worker").unwrap(),
                max_items: 1,
                lease_token: lease.clone(),
                lease_expires_at: UtcTimestamp::new(10_000, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), 1);
        let item = &claimed.items[0];
        let outcomes = backend
            .commit_transition(
                &shard,
                CommitTransition {
                    request_id: None,
                    entries: vec![CommitTransitionEntry {
                        claim_ref: ClaimRef {
                            item_id: item.item_id,
                            lease_token: lease,
                            lease_expires_at: item.lease_expires_at,
                            item_version: item.item_version,
                        },
                        additional_claim_refs: Vec::new(),
                        finalize: FinalizeKind::Complete,
                        side_records: Vec::new(),
                        lifecycle_items: Vec::new(),
                        instance_fence: None,
                    }],
                },
                UtcTimestamp::new(3, 0).unwrap(),
                None,
            )
            .await
            .expect("fresh claim must commit_transition under Strict");
        assert!(matches!(
            outcomes.as_slice(),
            [CommitEntryOutcome::Committed { .. }]
        ));
        let _ = ids;
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.leased, 0);
        assert!(metrics.complete >= 1);
    }

    /// fireweed-2be744bd: competing claim/finalize workers must not append illegal lifecycle
    /// transitions (log-apply must never reject an admitted command).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn competing_workers_claim_finalize_never_illegal_lifecycle() {
        const ITEMS: usize = 32;
        const WORKERS: usize = 8;
        let backend = Arc::new(open_backend().await);
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let mut specs = Vec::with_capacity(ITEMS);
        for i in 0..ITEMS {
            specs.push(PushSpec {
                payload: Some(bytes::Bytes::from(format!("p{i}"))),
                ..PushSpec::default()
            });
        }
        backend
            .push(&shard, specs, UtcTimestamp::new(1, 0).unwrap(), None)
            .await
            .unwrap();

        let mut handles = Vec::new();
        for w in 0..WORKERS {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            handles.push(tokio::spawn(async move {
                let mut finalized = 0usize;
                for tick in 0..ITEMS {
                    let lease = LeaseToken::new(format!("w{w}-t{tick}")).unwrap();
                    let claimed = backend
                        .claim(ClaimRequest {
                            shard: shard.clone(),
                            worker_id: WorkerId::new(format!("w{w}")).unwrap(),
                            max_items: 1,
                            lease_token: lease.clone(),
                            lease_expires_at: UtcTimestamp::new(50_000, 0).unwrap(),
                            now: UtcTimestamp::new(10 + tick as i64, 0).unwrap(),
                            eligibility_time: None,
                            compatibility: ClaimCompatibility::default(),
                            expected_epoch: None,
                        })
                        .await
                        .expect("claim must not poison or return transport error");
                    if claimed.items.is_empty() {
                        continue;
                    }
                    let item = &claimed.items[0];
                    // Finalize immediately (the snorri worker-pool pattern).
                    backend
                        .finalize(
                            &shard,
                            vec![FinalizeOutcome::new(item.item_id, FinalizeKind::Complete)],
                            UtcTimestamp::new(11 + tick as i64, 0).unwrap(),
                            None,
                        )
                        .await
                        .expect("finalize after own claim must succeed; no illegal lifecycle");
                    finalized += 1;
                }
                finalized
            }));
        }
        let mut total = 0usize;
        for h in handles {
            total += h.await.expect("worker join");
        }
        assert_eq!(
            total, ITEMS,
            "every item finalized exactly once across workers"
        );
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.pending, 0);
        assert_eq!(metrics.leased, 0);
        assert_eq!(metrics.complete as usize, ITEMS);
    }

    /// fireweed-5497780d: N concurrent fenced commits on one shard must not lose side-record
    /// updates. Without prepare under the queue permit, every candidate can pass
    /// `validate_instance_fence` against the same stored fence and `WriteSideRecords` last-writer-wins;
    /// with the fix, only the fence-ordered winner survives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_fenced_side_record_commits_preserve_fence_ordered_winner() {
        const N: usize = 8;
        let backend = Arc::new(open_backend().await);
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();

        // One claimed item per racer so each commit can finalize its own lease.
        let mut push_specs = Vec::with_capacity(N);
        for i in 0..N {
            push_specs.push(PushSpec {
                payload: Some(Bytes::from(format!("item-{i}"))),
                ..PushSpec::default()
            });
        }
        let _ids = backend
            .push(&shard, push_specs, UtcTimestamp::new(1, 0).unwrap(), None)
            .await
            .unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: N,
                lease_token: LeaseToken::new("lease-race").unwrap(),
                lease_expires_at: UtcTimestamp::new(10_000, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(claimed.items.len(), N);

        let instance_key = b"wf-race".to_vec();
        let side_key = b"state/instance".to_vec();
        let barrier = Arc::new(tokio::sync::Barrier::new(N));
        let mut handles = Vec::with_capacity(N);
        for (i, item) in claimed.items.into_iter().enumerate() {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            let barrier = Arc::clone(&barrier);
            let instance_key = instance_key.clone();
            let side_key = side_key.clone();
            let payload = Bytes::from(format!("candidate-{i}"));
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let claim_ref = ClaimRef {
                    item_id: item.item_id,
                    lease_token: item.lease_token.clone().expect("lease token"),
                    lease_expires_at: item.lease_expires_at,
                    item_version: item.item_version,
                };
                let outcomes = backend
                    .commit_transition(
                        &shard,
                        CommitTransition {
                            request_id: None,
                            entries: vec![CommitTransitionEntry {
                                claim_ref,
                                additional_claim_refs: Vec::new(),
                                finalize: FinalizeKind::Complete,
                                side_records: vec![SideRecord {
                                    key: side_key,
                                    payload: payload.clone(),
                                }],
                                lifecycle_items: Vec::new(),
                                instance_fence: Some(InstanceFence {
                                    instance_key,
                                    expected: 0,
                                    next: 1,
                                }),
                            }],
                        },
                        UtcTimestamp::new(10 + i as i64, 0).unwrap(),
                        None,
                    )
                    .await
                    .expect("commit_transition transport ok");
                (i, payload, outcomes)
            }));
        }

        let mut committed_payloads = Vec::new();
        let mut rejected = 0usize;
        for handle in handles {
            let (i, payload, outcomes) = handle.await.expect("join");
            assert_eq!(outcomes.len(), 1, "racer {i}");
            match &outcomes[0] {
                CommitEntryOutcome::Committed { .. } => committed_payloads.push(payload),
                CommitEntryOutcome::Rejected(EngineError::Conflict) => rejected += 1,
                other => panic!("racer {i}: unexpected outcome {other:?}"),
            }
        }

        assert_eq!(
            committed_payloads.len(),
            1,
            "exactly one fenced candidate must win the CAS; got {committed_payloads:?}"
        );
        assert_eq!(
            rejected,
            N - 1,
            "stale same-expected racers must Conflict, not overwrite"
        );

        let winner = &committed_payloads[0];
        let durable = backend
            .side_record(&shard, &side_key)
            .await
            .unwrap()
            .expect("side record present");
        assert_eq!(
            durable.as_ref(),
            winner.as_ref(),
            "surviving side record must match the fence-ordered winner (no lost update)"
        );
        let fence = backend
            .with_projection(|p| ProjectionStore::instance_fence(p, &shard, &instance_key))
            .unwrap();
        assert_eq!(fence, Some(1), "fence advances exactly once");
    }

    /// Sequential fence chain: side record tracks fence-ordered history (no concurrent lost update).
    #[tokio::test]
    async fn sequential_fenced_side_records_preserve_order() {
        let backend = open_backend().await;
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let instance_key = b"wf-seq".to_vec();
        let side_key = b"state/instance".to_vec();
        backend
            .push(
                &shard,
                vec![PushSpec::default(), PushSpec::default()],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: 2,
                lease_token: LeaseToken::new("lease-seq").unwrap(),
                lease_expires_at: UtcTimestamp::new(10_000, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        for (step, item) in claimed.items.into_iter().enumerate() {
            let expected = step as u64;
            let next = expected + 1;
            let claim_ref = ClaimRef {
                item_id: item.item_id,
                lease_token: item.lease_token.clone().unwrap(),
                lease_expires_at: item.lease_expires_at,
                item_version: item.item_version,
            };
            let outcomes = backend
                .commit_transition(
                    &shard,
                    CommitTransition {
                        request_id: None,
                        entries: vec![CommitTransitionEntry {
                            claim_ref,
                            additional_claim_refs: Vec::new(),
                            finalize: FinalizeKind::Complete,
                            side_records: vec![SideRecord {
                                key: side_key.clone(),
                                payload: Bytes::from(format!("step-{next}")),
                            }],
                            lifecycle_items: Vec::new(),
                            instance_fence: Some(InstanceFence {
                                instance_key: instance_key.clone(),
                                expected,
                                next,
                            }),
                        }],
                    },
                    UtcTimestamp::new(10 + step as i64, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            assert!(
                matches!(outcomes[0], CommitEntryOutcome::Committed { .. }),
                "sequential step {next}"
            );
        }
        assert_eq!(
            backend
                .side_record(&shard, &side_key)
                .await
                .unwrap()
                .as_deref(),
            Some(b"step-2".as_slice())
        );
        let fence = backend
            .with_projection(|p| ProjectionStore::instance_fence(p, &shard, &instance_key))
            .unwrap();
        assert_eq!(fence, Some(2));
    }
}
