//! Feature-gated native-async object-log + Turso server composition (TD-010 / AC-TURSO-5).

#![allow(clippy::manual_async_fn)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition, QueueId,
    RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncComposedBackend, AsyncControlPlane, AsyncFinalizeRequest,
    AsyncLifecycleError, AsyncLogStore, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError,
    AsyncPushRequest, AsyncRenewRequest, Backend, ClaimPort, ClaimRequest, Claimed,
    CommandChecksum, CommandEnvelope, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort, FinalizeTarget,
    IdGen, InProcessControlPlane, ItemView, LeaseView, LiveItemView, PendingPage, PendingSummary,
    ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead,
    PurgePort, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, QueueMetrics,
    RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, RenewLeasePort,
    RenewTarget, SeparateReplayCommit, TerminalEmissionMetrics, TickReport, UpsertOutcome,
    UpsertPort,
};
use fireweed_memory::SeqIdGen;
use fireweed_objectlog::segmented::{BlobStore, SegmentConfig};
use fireweed_objectlog::{AsyncObjectLog, GroupCommitObjectLogProjectionCommitter};
use fireweed_turso::{TursoConfig, TursoRelational};

use crate::TokioTaskDispatcher;

type Committer = GroupCommitObjectLogProjectionCommitter<TursoRelational>;
type Strategy = SeparateReplayCommit<Committer>;
type ClaimPlanner =
    ProjectionClaimPlanner<InProcessControlPlane, AsyncObjectLog, TursoRelational, SeqIdGen>;
type PushPlanner =
    ProjectionPushPlanner<InProcessControlPlane, AsyncObjectLog, TursoRelational, SeqIdGen>;
type LifecyclePlanner =
    ProjectionLifecyclePlanner<InProcessControlPlane, AsyncObjectLog, TursoRelational, SeqIdGen>;
type AsyncEngine = AsyncComposedBackend<
    Strategy,
    TokioTaskDispatcher,
    ClaimPlanner,
    PushPlanner,
    LifecyclePlanner,
>;

/// RESP-compatible adapter over the native-async engine/planner stack. Turso is the only serving
/// projection in this profile; the object log remains the durable/fencing authority.
pub struct ObjectLogTursoBackend {
    engine: AsyncEngine,
    committer: Committer,
    log: Arc<AsyncObjectLog>,
    projection: Arc<TursoRelational>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
}

impl ObjectLogTursoBackend {
    pub async fn open_with_blob_store(
        store: Arc<dyn BlobStore>,
        projection_path: &Path,
        segment_config: SegmentConfig,
    ) -> EngineResult<Self> {
        if projection_path.as_os_str().is_empty() {
            return Err(EngineError::Invalid(
                "Turso projection path must not be empty",
            ));
        }
        let projection = Arc::new(
            TursoRelational::open(TursoConfig::local(projection_path))
                .await
                .map_err(|error| EngineError::Storage(error.to_string()))?,
        );
        let log = Arc::new(
            AsyncObjectLog::open_group_commit_with_blob_store(store, segment_config).await?,
        );
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let committer = GroupCommitObjectLogProjectionCommitter::open_shared(
            log.as_ref().clone(),
            Arc::clone(&projection),
            Vec::new(),
            fireweed_objectlog::MAX_RECOVERY_PAGE_SIZE,
            1024,
        )
        .await?;
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer.clone())
                .map_err(|error| EngineError::Storage(error.to_string()))?;
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
            0,
        );
        let lifecycle = ProjectionLifecyclePlanner::from_shared(
            Arc::clone(&control),
            Arc::clone(&log),
            Arc::clone(&projection),
            Arc::clone(&ids),
        );
        let dispatcher = TokioTaskDispatcher::new(NonZeroUsize::new(8).expect("nonzero"), 1024);
        let engine =
            AsyncComposedBackend::new_with_planners(strategy, dispatcher, claim, push, 1024)
                .with_lifecycle_planner(lifecycle);
        Ok(Self {
            engine,
            committer,
            log,
            projection,
            control,
            ids,
        })
    }

    pub fn projection(&self) -> &Arc<TursoRelational> {
        &self.projection
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

impl Backend for ObjectLogTursoBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }
    fn commit_raw(
        &self,
        request: RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::RawCommitOutcome>> + Send
    {
        async move {
            self.engine.submit_commit(request).await.map_err(|error| {
                EngineError::Storage(format!("async raw commit submission failed: {error:?}"))
            })?
        }
    }
}

impl ControlPlaneStore for ObjectLogTursoBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let outcome = self.projection.create_or_read_queue(definition).await?;
            let authoritative = outcome.definition.clone();
            let control_outcome =
                AsyncControlPlane::create_queue(self.control.as_ref(), authoritative.clone())
                    .await?;
            if control_outcome.definition != authoritative {
                return Err(EngineError::QueueDefinitionConflict);
            }
            let shard = QueueKey::new(
                authoritative.tenant_id.clone(),
                authoritative.queue_id.clone(),
            );
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            self.committer.recover_projection(shard).await?;
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
    fn hydrate_projection_for_ownership(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        self.committer.recover_projection(shard.clone())
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

impl PushPort for ObjectLogTursoBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        async move {
            self.engine
                .push(AsyncPushRequest {
                    shard: shard.clone(),
                    request_id: None,
                    items,
                    now,
                    expected_epoch,
                })
                .await
                .map_err(Self::map_push)
        }
    }
    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
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

impl ClaimPort for ObjectLogTursoBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl fireweed_engine::CommitTransitionPort for ObjectLogTursoBackend {}
impl fireweed_engine::RecoveryReadPort for ObjectLogTursoBackend {}

impl FinalizePort for ObjectLogTursoBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let ids = outcomes
                .iter()
                .map(|outcome| outcome.item_id)
                .collect::<Vec<_>>();
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

impl RenewLeasePort for ObjectLogTursoBackend {
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

impl ReassignLeasePort for ObjectLogTursoBackend {
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

impl PurgePort for ObjectLogTursoBackend {
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

impl UpsertPort for ObjectLogTursoBackend {
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl ReclaimDriver for ObjectLogTursoBackend {
    fn tick(
        &self,
        _now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        std::future::ready(Ok(TickReport::default()))
    }
}

impl ProjectionRead for ObjectLogTursoBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::eligible_candidates(
            self.projection.as_ref(),
            shard.clone(),
            now,
            limit,
        )
    }
    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        self.projection.server_peek(shard, limit)
    }
    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.server_pending(shard)
    }
    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        self.projection.server_pending_summary(shard)
    }
    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        self.projection.server_pending_page(shard, start, limit)
    }
    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection
            .server_pending_range(shard, start, end, consumer, limit)
    }
    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        self.projection.server_pending_by_ids(shard, ids)
    }
    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ClaimedItem>>> + Send
    {
        AsyncProjectionStore::render_claimed(self.projection.as_ref(), shard.clone(), ids.to_vec())
    }
    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        self.projection.server_live_items(shard, keys)
    }
    fn metrics(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        self.projection.server_metrics(shard)
    }
    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        _now: UtcTimestamp,
        _emit_change_records: bool,
        _emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        self.projection.server_terminal_emission_metrics(shard)
    }
}
