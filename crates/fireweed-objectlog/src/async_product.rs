//! Async object-log × in-memory projection product over crates.io [`object_log::LogEngine`].
//!
//! Eventual-apply composition for program A. Opens via [`AsyncObjectLogMemoryBackend::open_local`]
//! / [`open_memory`]. Call sites type against this concrete product only at the open edge;
//! elsewhere use port traits.

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
    FinalizeTarget, IdGen, InProcessControlPlane, InlineOwnedTaskDispatcher, OwnedTask,
    ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead,
    ProjectionStore, PurgePort, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey,
    RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver,
    ReclaimPort, RenewLeasePort, RenewTarget, SeparateReplayCommit, SeparateReplayCommitter,
    TickReport, UpsertOutcome, UpsertPort,
};
use fireweed_projection::{AsyncInMemoryProjection, InMemoryProjection};
use object_log::FlushConfig;

use crate::ObjectLogEngineStore;

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
    InlineOwnedTaskDispatcher,
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
    pub async fn from_log_store(
        log: ObjectLogEngineStore,
        node_id: u8,
    ) -> EngineResult<Self> {
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

    async fn from_log(log: Arc<ObjectLogEngineStore>, node_id: u8) -> EngineResult<Self> {
        let projection = Arc::new(AsyncInMemoryProjection::new(InMemoryProjection::new()));
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let committer = ObjectLogEngineProjectionCommitter {
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
        let reclaim = fireweed_engine::ProjectionReclaimPlanner::from_shared(
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

impl Backend for AsyncObjectLogMemoryBackend {
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
            consistency: "object-log sequenced append then projection apply (crates.io LogEngine)",
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

impl ControlPlaneStore for AsyncObjectLogMemoryBackend {
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
        // Eventual object-log memory product does not advertise upsert (parity with ObjectLogTursoBackend).
        std::future::ready(Err(EngineError::Unavailable))
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
            // Reclaim is queue-scoped via ReclaimPort; driver tick is a no-op report for this product.
            let _ = now;
            Ok(TickReport::default())
        }
    }
}

// LibBackend / facade ports: defaults where available; explicit Unavailable for required methods.
impl fireweed_engine::UpdateFieldsPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::CommitTransitionPort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::RecoveryReadPort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::BatchUpdatePort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::ItemMutationPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::SetGatesPort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::ReschedulePort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::DiscoveryPort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::IndexQueryPort for AsyncObjectLogMemoryBackend {
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
impl fireweed_engine::HotProjectionQueryPort for AsyncObjectLogMemoryBackend {}
impl fireweed_engine::HistoricalProjectionRead for AsyncObjectLogMemoryBackend {
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
