//! Async object-log × in-memory projection product over crates.io [`object_log::LogEngine`].
//!
//! Success waits for append+apply (Strict-equivalent response-after-apply for the live process).
//! Opens via [`AsyncObjectLogMemoryBackend::open_local`] / [`open_memory`]. Call sites type
//! against this concrete product only at the open edge; elsewhere use port traits.
//! See [`crate::commit_surface`] for the Strict capability decision.

#![allow(
    clippy::manual_async_fn,
    reason = "port traits deliberately expose explicit Send future return types"
)]

use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncCommitStrategy, AsyncComposedBackend, AsyncControlPlane, AsyncLogStore,
    AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest, Backend, ClaimPort,
    ClaimRef, ClaimRequest, Claimed, CommandEnvelope, CommandPage, CommandPosition,
    CommitTransitionEntry, ControlPlane, ControlPlaneStore, CreateQueueOutcome, DurabilityClass,
    EngineError, EngineResult, FinalizeKind, FinalizeOutcome, FinalizePort, IdGen,
    InProcessControlPlane, LogRead, OwnedTask, ProjectionClaimPlanner, ProjectionLifecyclePlanner,
    ProjectionPushPlanner, ProjectionRead, ProjectionStore, PurgePort, PushPort, PushSpec,
    QueueCommand, QueueCounters, QueueKey, RawCommitOutcome, RawCommitRequest, ReassignLeasePort,
    ReclaimDriver, ReclaimPort, RenewLeasePort, RequestIdReplayProbe, SeparateReplayCommit,
    SeparateReplayCommitter, TickReport, UpsertOutcome, UpsertPort,
};
use fireweed_projection::{AsyncInMemoryProjection, InMemoryProjection};
use object_log::FlushConfig;

use crate::ObjectLogEngineStore;
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

/// Sequential id generation for object-log async products.
#[derive(Default)]
pub struct SeqIdGen {
    counter: std::sync::atomic::AtomicU64,
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ItemId::from_u64(n)
    }

    fn next_command_id(&self) -> fireweed_engine::CommandId {
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        fireweed_engine::CommandId::new(format!("cmd-{n}"))
    }
}

/// Eventual-apply: append via LogEngine, then apply to projection.
#[derive(Clone)]
pub struct ObjectLogEngineProjectionCommitter {
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<AsyncInMemoryProjection>,
}

impl SeparateReplayCommitter for ObjectLogEngineProjectionCommitter {
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

type Strategy = SeparateReplayCommit<ObjectLogEngineProjectionCommitter>;
type ClaimPlanner = ProjectionClaimPlanner<
    InProcessControlPlane,
    ObjectLogEngineStore,
    AsyncInMemoryProjection,
    SeqIdGen,
>;
type PushPlanner = ProjectionPushPlanner<
    InProcessControlPlane,
    ObjectLogEngineStore,
    AsyncInMemoryProjection,
    SeqIdGen,
>;
type LifecyclePlanner = ProjectionLifecyclePlanner<
    InProcessControlPlane,
    ObjectLogEngineStore,
    AsyncInMemoryProjection,
    SeqIdGen,
>;
type ReclaimPlanner = fireweed_engine::ProjectionReclaimPlanner<
    InProcessControlPlane,
    ObjectLogEngineStore,
    AsyncInMemoryProjection,
    SeqIdGen,
>;
type AsyncEngine = AsyncComposedBackend<
    Strategy,
    crate::ObjectLogTaskDispatcher,
    ClaimPlanner,
    PushPlanner,
    LifecyclePlanner,
    ReclaimPlanner,
>;

/// Product: crates.io object-log × in-memory projection (async composition).
pub struct AsyncObjectLogMemoryBackend {
    engine: AsyncEngine,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<AsyncInMemoryProjection>,
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

impl AsyncObjectLogMemoryBackend {
    /// Open durable local object-log under `root` with in-memory projection.
    pub async fn open_local(
        root: impl AsRef<std::path::Path>,
        flush: FlushConfig,
    ) -> EngineResult<Self> {
        Self::open_local_with_node_id(root, flush, 0).await
    }

    /// Open durable local object-log under `root`, minting item ids with `node_id`.
    pub async fn open_local_with_node_id(
        root: impl AsRef<std::path::Path>,
        flush: FlushConfig,
        node_id: u8,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(root, flush).await?);
        Self::from_log(log, node_id).await
    }

    /// In-process memory blob store (tests).
    pub async fn open_memory(flush: FlushConfig) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_memory(flush).await?);
        Self::from_log(log, 0).await
    }

    /// Open from a pre-built log axis (e.g. shared blob store + segment flush knobs).
    pub async fn from_log_store(log: ObjectLogEngineStore, node_id: u8) -> EngineResult<Self> {
        Self::from_log(Arc::new(log), node_id).await
    }

    /// Borrow the authoritative log axis (lifecycle / diagnostics).
    pub fn with_log<R>(&self, f: impl FnOnce(&ObjectLogEngineStore) -> R) -> R {
        f(self.log.as_ref())
    }

    /// Borrow the in-memory projection axis (lifecycle / diagnostics).
    pub fn with_projection<R>(&self, f: impl FnOnce(&InMemoryProjection) -> R) -> R {
        self.projection.with_store(f)
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
            .with_store(|store| store.peek_page(shard, after, limit))
    }

    async fn from_log(log: Arc<ObjectLogEngineStore>, node_id: u8) -> EngineResult<Self> {
        let projection = Arc::new(AsyncInMemoryProjection::new(InMemoryProjection::new()));
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let commit_idempotency = new_commit_idempotency();
        let batch_update_idempotency = new_batch_update_idempotency();
        let claim_by_query_idempotency = new_claim_by_query_idempotency();
        let claim_by_item_ids_idempotency = new_claim_by_item_ids_idempotency();
        let committer = ObjectLogEngineProjectionCommitter {
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
        let reclaim = fireweed_engine::ProjectionReclaimPlanner::from_shared(
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
            // Ephemeral in-memory projection: exact genesis replay of the durable log.
            let stats =
                replay_log_into_projection(log.as_ref(), projection.as_ref(), &shard, false)
                    .await?;
            recovery_stats.insert(shard.clone(), stats);
            // Seed id mints past recovered item ids (parity with AsyncLogReplayBackend).
            projection
                .with_store(|p| ProjectionStore::restore_counters(p, &shard, counters.as_ref()))?;
            // Process-local request-id maps are not part of the projection; rebuild from markers.
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
}

impl Backend for AsyncObjectLogMemoryBackend {
    fn durability_class(&self) -> DurabilityClass {
        // Live-process response-after-apply (Strict-equivalent); Class A log rebuild still applies.
        DurabilityClass::Atomic
    }

    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }

    fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
        strict_commit_capabilities(
            "Strict: object-log append then in-memory projection apply (response-after-apply, LogEngine)",
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

impl ControlPlaneStore for AsyncObjectLogMemoryBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
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
                self.projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            if self.recovery_stats.get(&shard).is_none() {
                let stats = replay_log_into_projection(
                    self.log.as_ref(),
                    self.projection.as_ref(),
                    &shard,
                    false,
                )
                .await?;
                self.projection.with_store(|projection| {
                    ProjectionStore::restore_counters(projection, &shard, self.counters.as_ref())
                })?;
                rebuild_process_idempotency_from_log(
                    self.log.as_ref(),
                    &shard,
                    outcome.definition.request_id_retention_ms,
                    &self.commit_idempotency,
                    &self.batch_update_idempotency,
                    &self.claim_by_query_idempotency,
                    &self.claim_by_item_ids_idempotency,
                )
                .await?;
                self.recovery_stats.insert(shard, stats);
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

impl PushPort for AsyncObjectLogMemoryBackend {
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

impl ClaimPort for AsyncObjectLogMemoryBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl FinalizePort for AsyncObjectLogMemoryBackend {
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

impl RenewLeasePort for AsyncObjectLogMemoryBackend {
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

impl ReassignLeasePort for AsyncObjectLogMemoryBackend {
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
            self.engine
                .reassign_item_ids(
                    shard.clone(),
                    item_ids,
                    new_lease_token,
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                )
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl PurgePort for AsyncObjectLogMemoryBackend {
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

impl UpsertPort for AsyncObjectLogMemoryBackend {
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

impl ReclaimPort for AsyncObjectLogMemoryBackend {
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

impl ReclaimDriver for AsyncObjectLogMemoryBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        async move {
            crate::reclaim_tick::tick_expired_leases(
                self.projection.as_ref(),
                self.control.as_ref(),
                self,
                now,
            )
            .await
        }
    }
}

// LibBackend / facade ports: full product surface (parity with AsyncLogReplayBackend).
impl fireweed_engine::UpdateFieldsPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::CommitTransitionPort for AsyncObjectLogMemoryBackend {
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
            // fireweed-5497780d: prepare (instance-fence validation) + append/apply under one
            // queue-local permit so concurrent fenced side-record commits cannot LWW-lose updates.
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

impl fireweed_engine::RecoveryReadPort for AsyncObjectLogMemoryBackend {
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

impl fireweed_engine::BatchUpdatePort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::ItemMutationPort for AsyncObjectLogMemoryBackend {
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: fireweed_engine::ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ItemMutationResponse>> + Send
    {
        let shard = shard.clone();
        async move {
            use fireweed_engine::{RequestOutcome, item_mutation_fingerprint};

            let fingerprint = fireweed_core::BodyHash(item_mutation_fingerprint(&request)?);
            let request_id = request.request_id.clone();
            let evaluated_at = request.evaluated_at;

            if let Some(response) = self.projection.with_store_mut(|projection| {
                ProjectionStore::replay_durable_item_mutation(
                    projection,
                    &shard,
                    &request_id,
                    fingerprint.0,
                    evaluated_at,
                )
            })? {
                return Ok(response);
            }

            let mut from = None;
            loop {
                let page =
                    AsyncLogStore::read_from(self.log.as_ref(), shard.clone(), from.clone(), 256)
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
                    let mut response: fireweed_engine::ItemMutationResponse =
                        serde_json::from_str(response_payload)
                            .map_err(|error| EngineError::Storage(error.to_string()))?;
                    response.position = Some(position.clone());
                    return Ok(response);
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }

            if request.dry_run {
                let plan = self.projection.with_store_mut(|projection| {
                    ProjectionStore::plan_item_mutation(projection, &shard, &request)
                })?;
                return Ok(plan.response);
            }

            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let projection = Arc::clone(&self.projection);
            let log = Arc::clone(&self.log);
            let ids = Arc::clone(&self.ids);
            let strategy = self.engine.commit_strategy();
            let queue = shard.clone();
            self.engine
                .submit_operation(queue, move || {
                    Box::pin(async move {
                        let mut plan = projection.with_store_mut(|projection| {
                            ProjectionStore::plan_item_mutation(projection, &shard, &request)
                        })?;
                        let response_payload = serde_json::to_string(&plan.response)
                            .map_err(|error| EngineError::Storage(error.to_string()))?;
                        let item_ids = plan
                            .command
                            .items
                            .iter()
                            .map(|item| item.item_id)
                            .collect::<Vec<_>>();
                        let mut envelope = port_surface::make_envelope(
                            ids.as_ref(),
                            QueueCommand::MutateItems(plan.command),
                            item_ids,
                            evaluated_at,
                        );
                        envelope.request_id = Some(request_id);
                        envelope.request_fingerprint = Some(fingerprint.0);
                        envelope.request_outcome =
                            Some(RequestOutcome::ItemMutation { response_payload });
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
impl fireweed_engine::SetGatesPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::ReschedulePort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::DiscoveryPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::IndexQueryPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogMemoryBackend {
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
        // fireweed-2ad3a030: prepare + append under one queue permit (snorri claim_by_query path).
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
        // fireweed-2be744bd: prepare + append/apply under one queue permit.
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
impl fireweed_engine::HistoricalProjectionRead for AsyncObjectLogMemoryBackend {
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

impl ProjectionRead for AsyncObjectLogMemoryBackend {
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

impl LogRead for AsyncObjectLogMemoryBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        crate::request_id_probe::read_from_log(self.log.as_ref(), shard.clone(), from, limit)
    }
}

impl fireweed_engine::SnapshotStore for AsyncObjectLogMemoryBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: fireweed_engine::ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::SnapshotRef>> + Send {
        AsyncLogStore::write_snapshot(self.log.as_ref(), shard.clone(), position, snapshot)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::SnapshotRef>>> + Send
    {
        AsyncLogStore::latest_snapshot(self.log.as_ref(), shard.clone())
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &fireweed_engine::SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ProjectionSnapshot>> + Send
    {
        AsyncLogStore::read_snapshot(self.log.as_ref(), snapshot_ref.clone())
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

/// Harness probe for AC-TXN-3 mid-pipeline request_id cuts (append→apply window).
impl RequestIdReplayProbe for AsyncObjectLogMemoryBackend {
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
        claim_ref: ClaimRef,
        finalize: FinalizeKind,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, BodyHash)> {
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
        entries: Vec<CommitTransitionEntry>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, BodyHash)> {
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

/// Assemble async object-log × memory over a local root (program A product path).
pub async fn composed_objectlog_memory_async(
    root: impl AsRef<std::path::Path>,
    target_bytes: usize,
    max_latency_ms: u64,
) -> EngineResult<AsyncObjectLogMemoryBackend> {
    let flush = crate::log_engine_store::flush_config_from_segment(target_bytes, max_latency_ms);
    AsyncObjectLogMemoryBackend::open_local(root, flush).await
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

    use super::AsyncObjectLogMemoryBackend;

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

    async fn backend() -> AsyncObjectLogMemoryBackend {
        AsyncObjectLogMemoryBackend::open_memory(FlushConfig {
            linger: std::time::Duration::ZERO,
            ..FlushConfig::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn object_log_engine_push_claim_round_trip() {
        let backend = backend().await;
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let ids = backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"hello")),
                    ..PushSpec::default()
                }],
                now,
                None,
            )
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
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
        assert_eq!(claimed.items[0].item_id, ids[0]);
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.leased, 1);
    }

    #[tokio::test]
    async fn object_log_engine_finalize_after_claim() {
        let backend = backend().await;
        let def = qdef();
        let shard = fireweed_engine::QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        backend.create_queue(def).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let ids = backend
            .push(
                &shard,
                vec![PushSpec {
                    payload: Some(bytes::Bytes::from_static(b"x")),
                    ..PushSpec::default()
                }],
                now,
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
        assert!(metrics.complete >= 1);
    }
}
