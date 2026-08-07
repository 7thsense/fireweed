//! Async object-log × sqlite projection over crates.io [`object_log::LogEngine`].
//!
//! Strict waits for durable SQLite and serving-memory apply. AsyncProjection synchronously updates
//! serving memory and defers bounded, ordered SQLite apply through the shared coordinator.

#![allow(
    clippy::manual_async_fn,
    reason = "port traits deliberately expose explicit Send future return types"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueueDefinition,
    RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncCommitStrategy, AsyncComposedBackend, AsyncControlPlane, AsyncLogStore,
    AsyncProjectionSpec, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest,
    Backend, ClaimPort, ClaimRequest, Claimed, CommandEnvelope, ControlPlane, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort,
    InProcessControlPlane, OwnedTask, ProjectionClaimPlanner, ProjectionLifecyclePlanner,
    ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner, ProjectionStore, PurgePort,
    PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey, RawCommitOutcome, RawCommitRequest,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort, SeparateReplayCommit,
    SeparateReplayCommitter, TickReport, UpsertOutcome, UpsertPort,
};
use fireweed_projection::{AsyncInMemoryProjection, InMemoryProjection};
use fireweed_sqlite::{
    AsyncSqliteProjectionStore, DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY,
    DEFAULT_DEFERRED_FLUSH_CHUNK, SqliteProjectionStore,
};
use object_log::FlushConfig;

use crate::ObjectLogEngineStore;
use crate::async_product::SeqIdGen;
use crate::async_projection_apply::{
    AsyncProjectionApplyCoordinator, AsyncProjectionApplySnapshot,
};
use crate::commit_surface::{
    self, CommitIdempotency, eventual_commit_capabilities, new_commit_idempotency,
    strict_commit_capabilities,
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

type Proj = AsyncInMemoryProjection;

#[derive(Clone)]
struct Committer {
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<Proj>,
    sqlite_projection: Arc<AsyncSqliteProjectionStore>,
    async_apply: Option<AsyncProjectionApplyCoordinator<AsyncSqliteProjectionStore>>,
    projection_offline: Arc<AtomicBool>,
    projection_lifecycle_gate: Arc<tokio::sync::RwLock<()>>,
    strict_poison: Arc<std::sync::RwLock<std::collections::HashMap<QueueKey, String>>>,
    #[cfg(test)]
    strict_memory_failures: Arc<std::sync::atomic::AtomicU32>,
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
        let sqlite_projection = Arc::clone(&self.sqlite_projection);
        let async_apply = self.async_apply.clone();
        let projection_offline = Arc::clone(&self.projection_offline);
        let projection_lifecycle_gate = Arc::clone(&self.projection_lifecycle_gate);
        let strict_poison = Arc::clone(&self.strict_poison);
        #[cfg(test)]
        let strict_memory_failures = Arc::clone(&self.strict_memory_failures);
        Box::pin(async move {
            let _lifecycle_guard = projection_lifecycle_gate.read_owned().await;
            if projection_offline.load(Ordering::Acquire) {
                return Err(EngineError::Unavailable);
            }
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            match fault {
                fireweed_engine::RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                fireweed_engine::RawCommitFault::None
                | fireweed_engine::RawCommitFault::AfterAppendBeforeApply => {}
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
                    sqlite_projection.as_ref(),
                    positions.clone(),
                    commands.clone(),
                )
                .await?;
                #[cfg(test)]
                let injected_memory_failure = strict_memory_failures
                    .fetch_update(
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                        |remaining| remaining.checked_sub(1),
                    )
                    .is_ok();
                #[cfg(not(test))]
                let injected_memory_failure = false;
                let memory_result = if injected_memory_failure {
                    Err(EngineError::Storage(
                        "injected serving memory apply failure".into(),
                    ))
                } else {
                    AsyncProjectionStore::apply_live(
                        projection.as_ref(),
                        positions.clone(),
                        commands,
                    )
                    .await
                };
                if let Err(error) = memory_result {
                    if let Ok(mut poisoned) = strict_poison.write() {
                        poisoned.insert(
                            shard,
                            format!(
                                "serving memory apply failed after durable SQLite apply: {error}"
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
    sqlite_projection: Arc<AsyncSqliteProjectionStore>,
    async_apply: Option<AsyncProjectionApplyCoordinator<AsyncSqliteProjectionStore>>,
    projection_offline: Arc<AtomicBool>,
    projection_lifecycle_gate: Arc<tokio::sync::RwLock<()>>,
    strict_poison: Arc<std::sync::RwLock<std::collections::HashMap<QueueKey, String>>>,
    #[cfg(test)]
    strict_memory_failures: Arc<std::sync::atomic::AtomicU32>,
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
        Self::from_parts(
            log,
            projection_store,
            node_id,
            None,
            DEFAULT_DEFERRED_FLUSH_CHUNK,
        )
        .await
    }

    pub async fn open_with_async_projection(
        log_root: impl AsRef<std::path::Path>,
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        spec: AsyncProjectionSpec,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = SqliteProjectionStore::open(projection_path)?;
        Self::from_parts(
            log,
            projection_store,
            node_id,
            Some(spec),
            deferred_flush_chunk,
        )
        .await
    }

    pub async fn open_with_deferred_flush_chunk(
        log_root: impl AsRef<std::path::Path>,
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_local(log_root, flush).await?);
        let projection_store = SqliteProjectionStore::open(projection_path)?;
        Self::from_parts(log, projection_store, node_id, None, deferred_flush_chunk).await
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
        Self::from_parts(
            log,
            projection_store,
            node_id,
            None,
            DEFAULT_DEFERRED_FLUSH_CHUNK,
        )
        .await
    }

    pub async fn open_memory_log_with_async_projection(
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        spec: AsyncProjectionSpec,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_memory(flush).await?);
        let projection_store = if projection_path == ":memory:" {
            SqliteProjectionStore::in_memory()?
        } else {
            SqliteProjectionStore::open(projection_path)?
        };
        Self::from_parts(
            log,
            projection_store,
            node_id,
            Some(spec),
            deferred_flush_chunk,
        )
        .await
    }

    pub async fn open_memory_log_with_deferred_flush_chunk(
        projection_path: &str,
        flush: FlushConfig,
        node_id: u8,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        let log = Arc::new(ObjectLogEngineStore::open_memory(flush).await?);
        let projection_store = if projection_path == ":memory:" {
            SqliteProjectionStore::in_memory()?
        } else {
            SqliteProjectionStore::open(projection_path)?
        };
        Self::from_parts(log, projection_store, node_id, None, deferred_flush_chunk).await
    }

    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection_store: SqliteProjectionStore,
        node_id: u8,
    ) -> EngineResult<Self> {
        Self::from_parts(
            Arc::new(log),
            projection_store,
            node_id,
            None,
            DEFAULT_DEFERRED_FLUSH_CHUNK,
        )
        .await
    }

    pub async fn from_log_and_projection_with_async_projection(
        log: ObjectLogEngineStore,
        projection_store: SqliteProjectionStore,
        node_id: u8,
        spec: AsyncProjectionSpec,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        Self::from_parts(
            Arc::new(log),
            projection_store,
            node_id,
            Some(spec),
            deferred_flush_chunk,
        )
        .await
    }

    pub async fn from_log_and_projection_with_deferred_flush_chunk(
        log: ObjectLogEngineStore,
        projection_store: SqliteProjectionStore,
        node_id: u8,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        Self::from_parts(
            Arc::new(log),
            projection_store,
            node_id,
            None,
            deferred_flush_chunk,
        )
        .await
    }

    async fn from_parts(
        log: Arc<ObjectLogEngineStore>,
        projection_store: SqliteProjectionStore,
        node_id: u8,
        async_spec: Option<AsyncProjectionSpec>,
        deferred_flush_chunk: usize,
    ) -> EngineResult<Self> {
        if deferred_flush_chunk == 0 {
            return Err(EngineError::Invalid(
                "SQLite projection apply chunk must be positive",
            ));
        }
        let projection = Arc::new(AsyncInMemoryProjection::new(InMemoryProjection::new()));
        let sqlite_projection = Arc::new(
            AsyncSqliteProjectionStore::from_store_with_apply_chunk(
                projection_store,
                DEFAULT_ASYNC_PROJECTION_MAILBOX_CAPACITY,
                deferred_flush_chunk,
            )
            .await?,
        );
        let async_apply = async_spec
            .map(|spec| AsyncProjectionApplyCoordinator::new(Arc::clone(&sqlite_projection), spec))
            .transpose()?;
        let projection_offline = Arc::new(AtomicBool::new(false));
        let projection_lifecycle_gate = Arc::new(tokio::sync::RwLock::new(()));
        let strict_poison = Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        #[cfg(test)]
        let strict_memory_failures = Arc::new(std::sync::atomic::AtomicU32::new(0));
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
            sqlite_projection: Arc::clone(&sqlite_projection),
            async_apply: async_apply.clone(),
            projection_offline: Arc::clone(&projection_offline),
            projection_lifecycle_gate: Arc::clone(&projection_lifecycle_gate),
            strict_poison: Arc::clone(&strict_poison),
            #[cfg(test)]
            strict_memory_failures: Arc::clone(&strict_memory_failures),
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
            AsyncProjectionStore::ensure_shard(sqlite_projection.as_ref(), definition.clone())
                .await?;
            AsyncProjectionStore::ensure_shard(projection.as_ref(), definition.clone()).await?;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            // Durable SQLite projection: snapshot high-water + bounded tail replay.
            let stats =
                replay_log_into_projection(log.as_ref(), sqlite_projection.as_ref(), &shard, true)
                    .await?;
            replay_log_into_projection(log.as_ref(), projection.as_ref(), &shard, false).await?;
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
            if let Some(coordinator) = &async_apply {
                coordinator
                    .seed_high_water(
                        shard.clone(),
                        AsyncProjectionStore::recovery_high_water(
                            sqlite_projection.as_ref(),
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
            sqlite_projection,
            async_apply,
            projection_offline,
            projection_lifecycle_gate,
            strict_poison,
            #[cfg(test)]
            strict_memory_failures,
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
        self.ensure_projection_healthy(shard)?;
        fireweed_engine::resolve_write_epoch_async(expected_epoch, || {
            AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone())
        })
        .await
    }

    fn ensure_projection_healthy(&self, shard: &QueueKey) -> EngineResult<()> {
        if let Some(coordinator) = &self.async_apply {
            coordinator.ensure_healthy(shard)?;
        }
        let poisoned = self.strict_poison.read().map_err(|_| {
            EngineError::Storage("SQLite projection poison registry lock failed".into())
        })?;
        match poisoned.get(shard) {
            Some(reason) => Err(EngineError::Storage(format!(
                "SQLite projection poisoned: {reason}"
            ))),
            None => Ok(()),
        }
    }

    fn ensure_projection_writable(&self) -> EngineResult<()> {
        if self.projection_offline.load(Ordering::Acquire) {
            Err(EngineError::Unavailable)
        } else {
            Ok(())
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

impl Backend for AsyncObjectLogSqliteBackend {
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
                "AsyncProjection: object-log append plus serving-memory apply, then bounded durable SQLite apply (LogEngine)",
            )
        } else {
            strict_commit_capabilities(
                "Strict: object-log append then durable sqlite plus serving-memory apply (response-after-apply, LogEngine)",
            )
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

impl ControlPlaneStore for AsyncObjectLogSqliteBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            let _lifecycle_guard = self.projection_lifecycle_gate.read().await;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.ensure_projection_writable()?;
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
                self.sqlite_projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            AsyncProjectionStore::ensure_shard(
                self.projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            // Recover any durable tail not yet applied (first create is a no-op tail).
            if self.recovery_stats.get(&shard).is_none() {
                let stats = replay_log_into_projection(
                    self.log.as_ref(),
                    self.sqlite_projection.as_ref(),
                    &shard,
                    true,
                )
                .await?;
                replay_log_into_projection(
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
                if let Some(coordinator) = &self.async_apply {
                    coordinator
                        .seed_high_water(
                            shard.clone(),
                            AsyncProjectionStore::recovery_high_water(
                                self.sqlite_projection.as_ref(),
                                shard.clone(),
                            )
                            .await?,
                        )
                        .await;
                }
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
        let shard = shard.clone();
        async move {
            let _lifecycle_guard = self.projection_lifecycle_gate.read().await;
            self.ensure_projection_writable()?;
            AsyncLogStore::acquire_epoch(self.log.as_ref(), shard).await
        }
    }
    fn fence_epoch(
        &self,
        shard: &QueueKey,
        target_epoch: u64,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            let _lifecycle_guard = self.projection_lifecycle_gate.read().await;
            self.ensure_projection_writable()?;
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
}

impl ClaimPort for AsyncObjectLogSqliteBackend {
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move {
            self.ensure_projection_healthy(&request.shard)?;
            self.engine.claim(request).await.map_err(Self::map_claim)
        }
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
            self.ensure_projection_healthy(shard)?;
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
            self.ensure_projection_healthy(shard)?;
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
}

impl ReclaimDriver for AsyncObjectLogSqliteBackend {
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

impl ProjectionRead for AsyncObjectLogSqliteBackend {
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
            self.ensure_projection_healthy(&shard)?;
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
        let projection = Arc::clone(&self.projection);
        let commit_idempotency = Arc::clone(&self.commit_idempotency);
        let shard = shard.clone();
        async move {
            self.ensure_projection_healthy(&shard)?;
            commit_surface::explain_commit_if_authoritative(
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
            commit_surface::side_record(projection.as_ref(), &shard, &key).await
        }
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
        shard: &QueueKey,
        request: fireweed_engine::ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::ItemMutationResponse>> + Send
    {
        let shard = shard.clone();
        async move {
            use fireweed_engine::{RequestOutcome, item_mutation_fingerprint};

            self.ensure_projection_healthy(&shard)?;
            let fingerprint = fireweed_core::BodyHash(item_mutation_fingerprint(&request)?);
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
                        if let Some(response) = projection.with_store_mut(|projection| {
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
                        if let Some(response) = port_surface::retained_item_mutation_response(
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
                            let plan = projection.with_store_mut(|projection| {
                                ProjectionStore::plan_item_mutation(projection, &shard, &request)
                            })?;
                            return Ok(plan.response);
                        }
                        let epoch = fireweed_engine::resolve_write_epoch_async(
                            expected_epoch,
                            || AsyncLogStore::current_epoch(log.as_ref(), shard.clone()),
                        )
                        .await?;
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
        std::future::ready(self.read_healthy_projection(shard, |projection| {
            ProjectionStore::discover_active_scopes(projection, shard, granularity, now)
        }))
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
        std::future::ready(self.ensure_projection_healthy(shard).and_then(|()| {
            port_surface::index_get_unique(self.projection.as_ref(), shard, index, key)
        }))
    }
    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send
    {
        std::future::ready(
            self.ensure_projection_healthy(shard).and_then(|()| {
                port_surface::index_lookup(self.projection.as_ref(), shard, index, key)
            }),
        )
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
        std::future::ready(
            self.ensure_projection_healthy(shard)
                .and_then(|()| port_surface::range_scan(self.projection.as_ref(), shard, request)),
        )
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: fireweed_core::GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::GroupedAggregateResponse>> + Send
    {
        std::future::ready(self.ensure_projection_healthy(shard).and_then(|()| {
            port_surface::grouped_aggregate(self.projection.as_ref(), shard, request)
        }))
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: fireweed_core::MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::QueueMetrics>> + Send {
        std::future::ready(self.ensure_projection_healthy(shard).and_then(|()| {
            port_surface::metrics_by_query(self.projection.as_ref(), shard, request)
        }))
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: fireweed_core::DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<
        Output = EngineResult<fireweed_core::DeclaredBucketSegmentResponse>,
    > + Send {
        std::future::ready(self.ensure_projection_healthy(shard).and_then(|()| {
            port_surface::declared_bucket_segment(self.projection.as_ref(), shard, request)
        }))
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
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::CommandPosition>> + Send
    {
        async move {
            self.ensure_projection_healthy(shard)?;
            AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
                .await?
                .ok_or(EngineError::NotFound)
        }
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

impl fireweed_engine::SnapshotStore for AsyncObjectLogSqliteBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: fireweed_engine::CommandPosition,
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
    ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::CommandPosition>>> + Send
    {
        AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: fireweed_engine::CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncLogStore::set_high_water(self.log.as_ref(), shard.clone(), position)
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

    /// Clone the authoritative log handle for asynchronous lifecycle operations.
    pub fn log_store(&self) -> Arc<ObjectLogEngineStore> {
        Arc::clone(&self.log)
    }

    /// Read the disposable projection queue catalog on its owned adapter thread.
    pub async fn projection_definitions(&self) -> EngineResult<Vec<QueueDefinition>> {
        AsyncProjectionStore::recover_definitions(self.sqlite_projection.as_ref()).await
    }

    /// Read the disposable projection recovery cursor on its owned adapter thread.
    pub async fn projection_high_water(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Option<fireweed_engine::CommandPosition>> {
        AsyncProjectionStore::recovery_high_water(self.sqlite_projection.as_ref(), shard.clone())
            .await
    }

    /// Ensure a projection shard through the owned adapter thread.
    pub async fn ensure_projection_shard(&self, definition: QueueDefinition) -> EngineResult<()> {
        AsyncProjectionStore::ensure_shard(self.sqlite_projection.as_ref(), definition).await
    }

    /// Apply a recovery batch through the owned adapter thread.
    pub async fn apply_projection_recovery(
        &self,
        positions: Vec<fireweed_engine::CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> EngineResult<()> {
        AsyncProjectionStore::apply_recovery(self.sqlite_projection.as_ref(), positions, commands)
            .await
    }

    /// Whether this backend returns after bounded admission rather than durable SQLite apply.
    pub fn uses_async_projection(&self) -> bool {
        self.async_apply.is_some()
    }

    /// Delete the disposable projection through the owned adapter thread.
    ///
    /// New commits fail closed before authoritative append while deletion/rebuild owns the selected
    /// projection. Async apply is drained before the file is reset and remains paused until rebuild
    /// publishes a recovered high-water.
    pub async fn delete_projection(&self) -> EngineResult<()> {
        let _lifecycle_guard = self.projection_lifecycle_gate.write().await;
        self.projection_offline.store(true, Ordering::Release);
        if let Some(coordinator) = &self.async_apply {
            let definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
            for definition in definitions {
                let shard = QueueKey::new(definition.tenant_id, definition.queue_id);
                coordinator.wait_for_catch_up(&shard).await?;
            }
            coordinator.pause();
        }
        match self.sqlite_projection.delete_projection().await {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Some(coordinator) = &self.async_apply {
                    coordinator.resume();
                }
                self.projection_offline.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Publish successful lifecycle recovery and reopen projection-backed writes.
    pub async fn finish_projection_rebuild(
        &self,
        definitions: &[QueueDefinition],
    ) -> EngineResult<()> {
        if let Some(coordinator) = &self.async_apply {
            for definition in definitions {
                let shard =
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
                let high_water = AsyncProjectionStore::recovery_high_water(
                    self.sqlite_projection.as_ref(),
                    shard.clone(),
                )
                .await?;
                coordinator.reset_after_rebuild(shard, high_water).await;
            }
            coordinator.resume();
        }
        self.strict_poison
            .write()
            .map_err(|_| {
                EngineError::Storage("SQLite projection poison registry lock failed".into())
            })?
            .clear();
        self.projection_offline.store(false, Ordering::Release);
        Ok(())
    }

    /// Borrow the projection axis (lifecycle / diagnostics).
    pub fn with_projection<R>(&self, f: impl FnOnce(&InMemoryProjection) -> R) -> R {
        self.projection.with_store(f)
    }

    /// Mutably borrow the serving projection axis (lifecycle diagnostics).
    pub fn with_projection_mut<R>(&self, f: impl FnOnce(&mut InMemoryProjection) -> R) -> R {
        self.projection.with_store_mut(f)
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

    pub async fn export_sqlite_projection_image(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<fireweed_projection::ProjectionImage> {
        self.sqlite_projection
            .export_projection_image(shard.clone())
            .await
    }

    pub fn sqlite_projection_apply_chunk_size(&self) -> usize {
        self.sqlite_projection.apply_chunk_size()
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
        AsyncProjectionSpec, Backend, ClaimCompatibility, ClaimPort, ClaimRef, ClaimRequest,
        CommitEntryOutcome, CommitTransition, CommitTransitionEntry, CommitTransitionPort,
        ControlPlaneStore, DurabilityClass, EngineError, FinalizeKind, FinalizeOutcome,
        FinalizePort, InstanceFence, ProjectionRead, ProjectionStore, PushPort, PushSpec,
        ReclaimDriver, RecoveryReadPort, SideRecord,
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

    #[tokio::test]
    async fn tick_reclaims_multiple_queues_exactly_once_at_half_open_boundary() {
        let backend = open_backend().await;
        let mut first = qdef();
        first.queue_id = QueueId::new("q-first").unwrap();
        let mut second = qdef();
        second.queue_id = QueueId::new("q-second").unwrap();
        let first_shard =
            fireweed_engine::QueueKey::new(first.tenant_id.clone(), first.queue_id.clone());
        let second_shard =
            fireweed_engine::QueueKey::new(second.tenant_id.clone(), second.queue_id.clone());
        backend.create_queue(first).await.unwrap();
        backend.create_queue(second).await.unwrap();

        for (shard, item_count, lease) in [
            (&first_shard, 2, "lease-first"),
            (&second_shard, 1, "lease-second"),
        ] {
            backend
                .push(
                    shard,
                    vec![PushSpec::default(); item_count],
                    UtcTimestamp::new(1, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let claimed = backend
                .claim(ClaimRequest {
                    shard: shard.clone(),
                    worker_id: WorkerId::new("worker").unwrap(),
                    max_items: item_count,
                    lease_token: LeaseToken::new(lease).unwrap(),
                    lease_expires_at: UtcTimestamp::new(100, 0).unwrap(),
                    now: UtcTimestamp::new(2, 0).unwrap(),
                    eligibility_time: None,
                    compatibility: ClaimCompatibility::default(),
                    expected_epoch: None,
                })
                .await
                .unwrap();
            assert_eq!(claimed.items.len(), item_count);
        }

        assert_eq!(
            backend
                .tick(UtcTimestamp::new(100, 0).unwrap())
                .await
                .unwrap()
                .leases_reclaimed,
            0,
            "a lease remains valid at lease_expires_at"
        );
        assert_eq!(backend.metrics(&first_shard).await.unwrap().leased, 2);
        assert_eq!(backend.metrics(&second_shard).await.unwrap().leased, 1);

        assert_eq!(
            backend
                .tick(UtcTimestamp::new(101, 0).unwrap())
                .await
                .unwrap()
                .leases_reclaimed,
            3
        );
        assert_eq!(backend.metrics(&first_shard).await.unwrap().leased, 0);
        assert_eq!(backend.metrics(&first_shard).await.unwrap().pending, 2);
        assert_eq!(backend.metrics(&second_shard).await.unwrap().leased, 0);
        assert_eq!(backend.metrics(&second_shard).await.unwrap().pending, 1);
        assert_eq!(
            backend
                .tick(UtcTimestamp::new(101, 0).unwrap())
                .await
                .unwrap()
                .leases_reclaimed,
            0,
            "a repeated tick reports only newly committed reclaims"
        );
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

    mod async_projection_sqlite {
        use std::sync::atomic::{AtomicU64, Ordering};

        use super::*;

        fn flush() -> FlushConfig {
            FlushConfig {
                linger: std::time::Duration::ZERO,
                ..FlushConfig::default()
            }
        }

        fn spec() -> AsyncProjectionSpec {
            AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()
        }

        async fn async_backend(
            spec: AsyncProjectionSpec,
            chunk: usize,
        ) -> AsyncObjectLogSqliteBackend {
            AsyncObjectLogSqliteBackend::open_memory_log_with_async_projection(
                ":memory:",
                flush(),
                0,
                spec,
                chunk,
            )
            .await
            .unwrap()
        }

        async fn create(backend: &AsyncObjectLogSqliteBackend) -> fireweed_engine::QueueKey {
            let definition = qdef();
            let shard = fireweed_engine::QueueKey::new(
                definition.tenant_id.clone(),
                definition.queue_id.clone(),
            );
            backend.create_queue(definition).await.unwrap();
            shard
        }

        async fn push_one(
            backend: &AsyncObjectLogSqliteBackend,
            shard: &fireweed_engine::QueueKey,
            timestamp: i64,
        ) -> fireweed_engine::EngineResult<Vec<fireweed_core::ItemId>> {
            backend
                .push(
                    shard,
                    vec![PushSpec::default()],
                    UtcTimestamp::new(timestamp, 0).unwrap(),
                    None,
                )
                .await
        }

        fn assert_backpressure(error: EngineError, expected_resource: &'static str) {
            assert!(matches!(
                error,
                EngineError::Backpressure { resource } if resource == expected_resource
            ));
        }

        #[tokio::test]
        async fn both_barriers_preserve_independent_sqlite_chunk_tuning() {
            let strict = AsyncObjectLogSqliteBackend::open_memory_log_with_deferred_flush_chunk(
                ":memory:",
                flush(),
                0,
                7,
            )
            .await
            .unwrap();
            let strict_shard = create(&strict).await;
            push_one(&strict, &strict_shard, 1).await.unwrap();
            assert_eq!(strict.sqlite_projection_apply_chunk_size(), 7);
            assert_eq!(strict.durability_class(), DurabilityClass::Atomic);
            assert_eq!(
                strict
                    .export_sqlite_projection_image(&strict_shard)
                    .await
                    .unwrap()
                    .metrics
                    .pending,
                1
            );

            let asynchronous = async_backend(spec(), 3).await;
            assert_eq!(asynchronous.sqlite_projection_apply_chunk_size(), 3);
            assert_eq!(
                asynchronous.durability_class(),
                DurabilityClass::EventualApply
            );
        }

        #[tokio::test]
        async fn strict_sqlite_success_then_serving_apply_failure_poisons_fail_closed() {
            let backend = AsyncObjectLogSqliteBackend::open_memory_log_with_deferred_flush_chunk(
                ":memory:",
                flush(),
                0,
                1,
            )
            .await
            .unwrap();
            let shard = create(&backend).await;
            backend
                .strict_memory_failures
                .store(1, std::sync::atomic::Ordering::Release);
            assert!(matches!(
                push_one(&backend, &shard, 1).await.unwrap_err(),
                EngineError::Storage(message) if message.contains("injected serving memory apply failure")
            ));
            assert_eq!(
                backend
                    .export_sqlite_projection_image(&shard)
                    .await
                    .unwrap()
                    .metrics
                    .pending,
                1,
                "SQLite commit is durable before the injected serving-memory failure"
            );
            assert!(matches!(
                backend.metrics(&shard).await.unwrap_err(),
                EngineError::Storage(message) if message.contains("SQLite projection poisoned")
            ));
            assert!(matches!(
                push_one(&backend, &shard, 2).await.unwrap_err(),
                EngineError::Storage(message) if message.contains("SQLite projection poisoned")
            ));
        }

        #[tokio::test]
        async fn paused_sqlite_is_stale_until_ordered_watermark_catch_up() {
            let backend = async_backend(spec(), 2).await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            let first = push_one(&backend, &shard, 1).await.unwrap()[0];
            let second = push_one(&backend, &shard, 2).await.unwrap()[0];

            assert_eq!(backend.metrics(&shard).await.unwrap().pending, 2);
            let stale = backend
                .export_sqlite_projection_image(&shard)
                .await
                .unwrap();
            assert_eq!(stale.metrics.pending, 0);
            let debt = backend.async_projection_snapshot(&shard).await.unwrap();
            assert_eq!(debt.apply_lag_commands, 2);
            assert_eq!(debt.apply_queue_depth, 2);

            backend.resume_async_projection_apply().unwrap();
            backend
                .wait_for_async_projection_catch_up(&shard)
                .await
                .unwrap();
            let caught_up = backend
                .export_sqlite_projection_image(&shard)
                .await
                .unwrap();
            assert_eq!(caught_up.metrics.pending, 2);
            assert_eq!(
                caught_up
                    .items
                    .into_iter()
                    .map(|item| item.item_id)
                    .collect::<Vec<_>>(),
                vec![first, second]
            );
            assert_eq!(caught_up.high_water.unwrap().sequence, 1);
        }

        #[tokio::test]
        async fn lag_command_bound_rejects_before_append() {
            let backend = async_backend(
                AsyncProjectionSpec::new(1, 1024 * 1024, 16, 30_000, 3).unwrap(),
                1,
            )
            .await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            push_one(&backend, &shard, 1).await.unwrap();
            assert_backpressure(
                push_one(&backend, &shard, 2).await.unwrap_err(),
                "async-projection-apply-lag-commands",
            );
        }

        #[tokio::test]
        async fn debt_byte_bound_rejects_before_append() {
            let backend =
                async_backend(AsyncProjectionSpec::new(32, 1, 16, 30_000, 3).unwrap(), 1).await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            assert_backpressure(
                push_one(&backend, &shard, 1).await.unwrap_err(),
                "async-projection-apply-debt-bytes",
            );
        }

        #[tokio::test]
        async fn queue_depth_bound_rejects_before_append() {
            let backend = async_backend(
                AsyncProjectionSpec::new(32, 1024 * 1024, 1, 30_000, 3).unwrap(),
                1,
            )
            .await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            push_one(&backend, &shard, 1).await.unwrap();
            assert_backpressure(
                push_one(&backend, &shard, 2).await.unwrap_err(),
                "async-projection-apply-queue-depth",
            );
        }

        #[tokio::test]
        async fn oldest_unapplied_age_bound_rejects_before_append() {
            let backend = async_backend(
                AsyncProjectionSpec::new(32, 1024 * 1024, 16, 1, 3).unwrap(),
                1,
            )
            .await;
            let shard = create(&backend).await;
            backend.pause_async_projection_apply().unwrap();
            push_one(&backend, &shard, 1).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            assert_backpressure(
                push_one(&backend, &shard, 2).await.unwrap_err(),
                "async-projection-oldest-unapplied-age",
            );
        }

        #[tokio::test]
        async fn repeated_sqlite_apply_failure_poisons_reads_and_writes() {
            let backend = async_backend(
                AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 2).unwrap(),
                1,
            )
            .await;
            let shard = create(&backend).await;
            backend
                .async_apply
                .as_ref()
                .unwrap()
                .inject_apply_failures(2);
            push_one(&backend, &shard, 1).await.unwrap();
            assert!(matches!(
                backend
                    .wait_for_async_projection_catch_up(&shard)
                    .await
                    .unwrap_err(),
                EngineError::Storage(_)
            ));
            let poisoned = backend.async_projection_snapshot(&shard).await.unwrap();
            assert_eq!(poisoned.apply_retry_count, 2);
            assert!(poisoned.poison_reason.is_some());
            assert!(matches!(
                backend.metrics(&shard).await.unwrap_err(),
                EngineError::Storage(message) if message.contains("async projection poisoned")
            ));
            assert!(matches!(
                push_one(&backend, &shard, 2).await.unwrap_err(),
                EngineError::Storage(message) if message.contains("async projection poisoned")
            ));
        }

        #[tokio::test]
        async fn complete_item_mutation_surface_updates_serving_and_selected_projection() {
            let backend = async_backend(spec(), 1).await;
            let shard = create(&backend).await;
            let item_id = push_one(&backend, &shard, 1).await.unwrap()[0];
            let before = backend
                .with_projection(|projection| {
                    ProjectionStore::item_version(projection, &shard, &item_id)
                })
                .unwrap()
                .unwrap();
            let response = fireweed_engine::ItemMutationPort::mutate_items(
                &backend,
                &shard,
                fireweed_engine::ItemMutationRequest {
                    request_id: fireweed_core::RequestId::new("sqlite-async-mutation").unwrap(),
                    evaluated_at: UtcTimestamp::new(2, 0).unwrap(),
                    dry_run: false,
                    returning: fireweed_engine::ItemMutationReturning::Identity,
                    gate_changes: Vec::new(),
                    operation: fireweed_engine::ItemMutationOperation::Addressed {
                        entries: vec![fireweed_engine::AddressedMutation {
                            item_id,
                            expected_item_version: Some(before),
                            predicates: Vec::new(),
                            lease_guard: Default::default(),
                            patch: fireweed_engine::ItemPatch {
                                field_edits: std::collections::BTreeMap::from([(
                                    "owner".into(),
                                    Some(bytes::Bytes::from_static(b"worker-7")),
                                )]),
                                ..Default::default()
                            },
                        }],
                    },
                },
                None,
            )
            .await
            .unwrap();
            assert!(response.position.is_some());
            assert_eq!(
                backend
                    .with_projection(|projection| {
                        ProjectionStore::item_version(projection, &shard, &item_id)
                    })
                    .unwrap(),
                Some(before + 1)
            );
            backend
                .wait_for_async_projection_catch_up(&shard)
                .await
                .unwrap();
            let image = backend
                .export_sqlite_projection_image(&shard)
                .await
                .unwrap();
            let item = image
                .items
                .into_iter()
                .find(|item| item.item_id == item_id)
                .unwrap();
            assert_eq!(item.item_version, before + 1);
            assert_eq!(item.fields.get("owner").unwrap().as_ref(), b"worker-7");
        }

        #[tokio::test]
        async fn restart_resumes_from_sqlite_high_water_without_loss_or_duplicate_apply() {
            static NEXT_DIR: AtomicU64 = AtomicU64::new(1);
            let base = std::env::temp_dir().join(format!(
                "fireweed-async-projection-sqlite-{}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            let log_root = base.join("log");
            let sqlite_path = base.join("projection.sqlite");
            let backend = AsyncObjectLogSqliteBackend::open_with_async_projection(
                &log_root,
                sqlite_path.to_str().unwrap(),
                flush(),
                0,
                spec(),
                1,
            )
            .await
            .unwrap();
            let shard = create(&backend).await;
            let first = push_one(&backend, &shard, 1).await.unwrap()[0];
            let second = push_one(&backend, &shard, 2).await.unwrap()[0];
            backend
                .wait_for_async_projection_catch_up(&shard)
                .await
                .unwrap();
            backend.sqlite_projection.close_and_drain().await.unwrap();
            drop(backend);

            let reopened = AsyncObjectLogSqliteBackend::open_with_async_projection(
                &log_root,
                sqlite_path.to_str().unwrap(),
                flush(),
                0,
                spec(),
                1,
            )
            .await
            .unwrap();
            let image = reopened
                .export_sqlite_projection_image(&shard)
                .await
                .unwrap();
            assert_eq!(image.metrics.pending, 2);
            assert_eq!(
                image
                    .items
                    .into_iter()
                    .map(|item| item.item_id)
                    .collect::<Vec<_>>(),
                vec![first, second]
            );
            assert_eq!(
                reopened
                    .async_projection_snapshot(&shard)
                    .await
                    .unwrap()
                    .apply_queue_depth,
                0
            );
            let third = push_one(&reopened, &shard, 3).await.unwrap()[0];
            reopened
                .wait_for_async_projection_catch_up(&shard)
                .await
                .unwrap();
            let final_image = reopened
                .export_sqlite_projection_image(&shard)
                .await
                .unwrap();
            assert_eq!(final_image.metrics.pending, 3);
            assert_eq!(
                final_image
                    .items
                    .into_iter()
                    .map(|item| item.item_id)
                    .collect::<Vec<_>>(),
                vec![first, second, third]
            );
            reopened.sqlite_projection.close_and_drain().await.unwrap();
            drop(reopened);
            std::fs::remove_dir_all(base).unwrap();
        }
    }
}
