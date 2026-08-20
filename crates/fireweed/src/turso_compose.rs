//! Turso projection composition for the public 5×4 storage matrix.
//!
//! Composes each log axis with [`fireweed_turso::TursoRelational`] through the same
//! engine planners / commit strategies used by other derived projections:
//! - Atomic logs (memory / sqlite / postgres): [`UnifiedAtomicCommit`] (log-replay product shape)
//! - Object logs (filesystem / s3): [`SeparateReplayCommit`] (provider-neutral LogEngine constructors)
//!
//! This module deliberately avoids an `ObjectLogTursoBackend` public alias.

#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue,
    QueryCapabilityFlags, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AsyncClaimError, AsyncComposedBackend, AsyncControlPlane, AsyncFinalizeRequest,
    AsyncLifecycleError, AsyncLogStore, AsyncProjectionSpec, AsyncProjectionStore,
    AsyncPurgeRequest, AsyncPushError, AsyncPushRequest, AsyncReclaimRequest, AsyncRenewRequest,
    Backend, BatchUpdatePort, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed,
    CommandChecksum, CommandEnvelope, CommandPosition, ControlPlaneStore, CreateQueueOutcome,
    DEFAULT_BLOCKING_AXIS_IN_FLIGHT, DurabilityClass, EngineError, EngineResult,
    FinalizeOutcome, FinalizePort, FinalizeTarget, HistoricalProjectionRead,
    HotProjectionQueryPort, IdGen, InProcessControlPlane, InProcessLogStore, IndexQueryPort,
    InlineOwnedTaskDispatcher, ItemMutationPort, ItemMutationRequest, ItemMutationResponse,
    ItemView, LeaseView, LiveItemView, LogStore, OwnedTask, PendingPage, PendingSummary,
    PreparedClaim, PreparedFinalize, PreparedPush, ProjectionClaimPlanner,
    ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner,
    ProjectionSnapshot, PurgePort, PushPort, PushSpec, QueueCommand, QueueCounters, QueueKey,
    QueueMetrics, RawCommitFault, RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort, RenewTarget,
    SeparateReplayCommit, SeparateReplayCommitter, SeqIdGen, SetGatesPort, SnapshotRef,
    SnapshotStore, TerminalEmissionMetrics, TickReport, UnifiedAtomicCommit,
    UnifiedAtomicCommitter, UpdateFieldsBatchCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort,
};
use fireweed_projection::InMemoryProjection;
use fireweed_turso::{TursoConfig, TursoRelational, claimed_from_class_s};

#[cfg(feature = "objectlog")]
use fireweed_objectlog::{
    AsyncProjectionApplyCoordinator, ObjectLogEngineStore, ObjectLogTaskDispatcher,
};

#[cfg(feature = "objectlog")]
struct PlannedReservation;
#[cfg(not(feature = "objectlog"))]
struct PlannedReservation;

// ---------------------------------------------------------------------------
// Sync bridge for Turso open (safe inside or outside a Tokio runtime)
// ---------------------------------------------------------------------------

/// Drive a Turso future to completion without nesting reactors on a worker thread.
///
/// - Outside a runtime: private current-thread runtime.
/// - Inside a runtime: dedicated OS thread with its own current-thread runtime so the caller's
///   reactor is never blocked by `block_on`.
pub fn block_on_turso<F, T>(fut: F) -> EngineResult<T>
where
    F: std::future::Future<Output = EngineResult<T>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|e| {
                            EngineError::Storage(format!("turso open runtime failed: {e}"))
                        })?
                        .block_on(fut)
                })
                .join()
                .map_err(|_| EngineError::Storage("turso open thread panicked".into()))?
        })
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EngineError::Storage(format!("turso open runtime failed: {e}")))?
            .block_on(fut)
    }
}

pub async fn open_turso_projection_async(path: &Path) -> EngineResult<TursoRelational> {
    if path.as_os_str().is_empty() {
        return Err(EngineError::Invalid(
            "turso projection path must not be empty",
        ));
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| EngineError::Storage(format!("turso projection parent: {e}")))?;
    }
    TursoRelational::open(TursoConfig::local(path))
        .await
        .map_err(|e| EngineError::Storage(e.to_string()))
}

pub fn open_turso_projection(path: &Path) -> EngineResult<TursoRelational> {
    let path = path.to_path_buf();
    block_on_turso(async move { open_turso_projection_async(&path).await })
}

fn map_turso_storage(error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(error.to_string())
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

// ---------------------------------------------------------------------------
// Atomic log-replay × Turso (memory / sqlite / postgres logs)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AtomicTursoCommitter<L> {
    log: Arc<L>,
    projection: Arc<TursoRelational>,
    control: Arc<InProcessControlPlane>,
}

impl<L> UnifiedAtomicCommitter for AtomicTursoCommitter<L>
where
    L: AsyncLogStore + 'static,
{
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
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
                fireweed_engine::validate_gate_command_definition(&definition, &env.command)?;
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
            AsyncProjectionStore::apply_live(projection.as_ref(), positions.clone(), commands)
                .await?;
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

type AtomicEngine<L> = AsyncComposedBackend<
    UnifiedAtomicCommit<AtomicTursoCommitter<L>>,
    InlineOwnedTaskDispatcher,
    ProjectionClaimPlanner<InProcessControlPlane, L, TursoRelational, SeqIdGen>,
    ProjectionPushPlanner<InProcessControlPlane, L, TursoRelational, SeqIdGen>,
    ProjectionLifecyclePlanner<InProcessControlPlane, L, TursoRelational, SeqIdGen>,
    ProjectionReclaimPlanner<InProcessControlPlane, L, TursoRelational, SeqIdGen>,
>;

/// Generic atomic log × Turso product (Class A or B depending on the log axis).
pub struct AtomicTursoBackend<L: AsyncLogStore + 'static> {
    engine: AtomicEngine<L>,
    log: Arc<L>,
    projection: Arc<TursoRelational>,
    #[allow(dead_code)] // retained for reopen/delete-rebuild lifecycle helpers
    projection_path: PathBuf,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    /// Shared with push planners; recovery observes recovered item ids into this map.
    counters: Arc<QueueCounters>,
    #[allow(dead_code)]
    node_id: u8,
}

impl<L> AtomicTursoBackend<L>
where
    L: AsyncLogStore + 'static,
{
    async fn snapshot_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        self.projection.server_live_items(shard, keys).await
    }

    async fn planner_update_snapshot(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
        _ids: &[ItemId],
    ) -> EngineResult<Vec<fireweed_engine::BatchUpdateSnapshotItem>> {
        let views = self.snapshot_live_items(shard, keys).await?;
        Ok(views
            .into_iter()
            .flatten()
            .map(|view| fireweed_engine::BatchUpdateSnapshotItem {
                item_id: view.item_id,
                client_item_key: view.client_item_key,
                state: view.lifecycle_state,
                item_version: view.item_version,
                fenced: false,
                superseded: false,
            })
            .collect())
    }

    fn pipeline_unresolved_updates(&self) -> bool {
        false
    }

    fn reserve_planned_updates(
        &self,
        _shard: &QueueKey,
        _updates: &[fireweed_engine::UpdateFieldsCommand],
    ) -> EngineResult<Option<PlannedReservation>> {
        Ok(None)
    }

    fn finish_planned(&self, _planned: Option<PlannedReservation>, _ok: bool) {}

    async fn catch_up_projection(&self, _shard: &QueueKey) -> EngineResult<()> {
        Ok(())
    }

    async fn catch_up_produce(&self, _shard: &QueueKey) -> EngineResult<()> {
        Ok(())
    }

    pub async fn assemble(
        log: L,
        projection: TursoRelational,
        projection_path: PathBuf,
        node_id: u8,
    ) -> EngineResult<Self> {
        let log = Arc::new(log);
        let projection = Arc::new(projection);
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let committer = AtomicTursoCommitter {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
            control: Arc::clone(&control),
        };
        let strategy = UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer)
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

        let backend = Self {
            engine,
            log,
            projection,
            projection_path,
            control,
            ids,
            counters,
            node_id,
        };
        backend.recover_async().await?;
        Ok(backend)
    }

    async fn recover_async(&self) -> EngineResult<()> {
        let mut definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        let projection_owns_catalog = definitions.is_empty();
        if projection_owns_catalog {
            definitions =
                AsyncProjectionStore::recover_definitions(self.projection.as_ref()).await?;
        }
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let _ =
                AsyncControlPlane::create_queue(self.control.as_ref(), definition.clone()).await;
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            AsyncProjectionStore::ensure_shard(self.projection.as_ref(), definition).await?;
            let high_water =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), shard.clone())
                    .await?;
            if projection_owns_catalog && let Some(position) = high_water.clone() {
                AsyncLogStore::set_high_water(self.log.as_ref(), shard.clone(), position).await?;
            }
            // Class B (empty memory log): seed mint counters from the durable projection so
            // reopen never remints item ids that already exist in fireweed_items.
            // Class A still seeds from log envelopes below.
            if projection_owns_catalog
                && let Some(item_id) = self.projection.recovery_counter_high_water(&shard).await?
            {
                self.counters.observe(&shard, item_id);
            }
            let mut from = None;
            loop {
                let page =
                    AsyncLogStore::read_from(self.log.as_ref(), shard.clone(), from.clone(), 256)
                        .await?;
                if page.entries.is_empty() {
                    break;
                }
                // Seed QueueCounters past every recovered item id so reopen never remints.
                for (_, env) in &page.entries {
                    for item_id in &env.item_ids {
                        self.counters.observe(&shard, *item_id);
                    }
                }
                let tail: Vec<_> = page
                    .entries
                    .iter()
                    .filter(|(position, _)| {
                        high_water.as_ref().is_none_or(|hw| {
                            position.backend_epoch > hw.backend_epoch
                                || (position.backend_epoch == hw.backend_epoch
                                    && position.sequence > hw.sequence)
                        })
                    })
                    .cloned()
                    .collect();
                if !tail.is_empty() {
                    let positions: Vec<_> = tail.iter().map(|(p, _)| p.clone()).collect();
                    let commands: Vec<_> = tail.iter().map(|(_, e)| e.clone()).collect();
                    AsyncProjectionStore::apply_recovery(
                        self.projection.as_ref(),
                        positions,
                        commands,
                    )
                    .await?;
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
        }
        Ok(())
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

    #[allow(dead_code)]
    pub fn projection_path(&self) -> &Path {
        &self.projection_path
    }

    /// Borrow the Turso projection axis (rebuild/read diagnostics).
    pub fn projection(&self) -> &Arc<TursoRelational> {
        &self.projection
    }

    async fn dispatch_push(
        &self,
        request: AsyncPushRequest,
    ) -> EngineResult<fireweed_engine::PushBatchOutcome> {
        self.engine.push(request).await.map_err(map_push)
    }

    async fn dispatch_claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        self.engine.claim(request).await.map_err(map_claim)
    }

    async fn dispatch_finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
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
            .map_err(map_lifecycle)
    }
}

impl<S> AtomicTursoBackend<InProcessLogStore<S>>
where
    S: LogStore + Send + 'static,
{
    fn create_queue_impl(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send + '_ {
        async move {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let mut outcome = fireweed_engine::ControlPlane::create_queue(
                self.control.as_ref(),
                definition.clone(),
            )?;
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            if let Some(durable) = self
                .log
                .run_with_store_mut({
                    let definition = outcome.definition.clone();
                    move |log| LogStore::create_or_read_definition(log, &definition)
                })
                .await?
            {
                let matches = durable.definition == outcome.definition;
                fireweed_engine::ControlPlane::cache_authoritative_definition(
                    self.control.as_ref(),
                    durable.definition.clone(),
                )?;
                outcome = durable;
                if !matches {
                    return Err(EngineError::QueueDefinitionConflict);
                }
            }
            AsyncProjectionStore::ensure_shard(
                self.projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            // Projection-side create_or_read for durable projection catalog (Class B reopen).
            let proj_outcome = self
                .projection
                .create_or_read_queue(outcome.definition.clone())
                .await?;
            if proj_outcome.definition != outcome.definition {
                return Err(EngineError::QueueDefinitionConflict);
            }
            Ok(outcome)
        }
    }
}

// Port impls for AtomicTursoBackend — shared via macro-like duplication with object-log product.

macro_rules! impl_turso_product_ports {
    ($ty:ty, $durability:expr, $consistency:expr) => {
        impl Backend for $ty {
            fn durability_class(&self) -> DurabilityClass {
                $durability
            }
            fn supports_gates(&self) -> bool {
                true
            }
            fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
                fireweed_engine::CommitCapabilities {
                    atomic_transition_commit: true,
                    vectorized_commit: true,
                    lease_validation: true,
                    retained_commit_idempotency: true,
                    non_work_side_records: true,
                    authoritative_recovery_reads: true,
                    delayed_awaits_timers: true,
                    durability_class: $durability,
                    consistency: $consistency,
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

        impl ControlPlaneStore for $ty {
            fn create_queue(
                &self,
                definition: QueueDefinition,
            ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
                // Specialized create_queue lives on each concrete product (log catalog differs).
                self.create_queue_impl(definition)
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
                _shard: &QueueKey,
            ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
                std::future::ready(Ok(()))
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
                        current =
                            AsyncLogStore::acquire_epoch(self.log.as_ref(), shard.clone()).await?;
                    }
                    Ok(current)
                }
            }
        }

        impl PushPort for $ty {
            fn push(
                &self,
                shard: &QueueKey,
                items: Vec<PushSpec>,
                now: UtcTimestamp,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
                async move {
                    Ok(self
                        .dispatch_push(AsyncPushRequest {
                            shard: shard.clone(),
                            request_id: None,
                            items,
                            now,
                            expected_epoch,
                        })
                        .await?
                        .into_item_ids())
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
                    self.dispatch_push(AsyncPushRequest {
                        shard: shard.clone(),
                        request_id: Some(request_id),
                        items,
                        now,
                        expected_epoch,
                    })
                    .await
                }
            }
        }

        impl ClaimPort for $ty {
            fn claim(
                &self,
                request: ClaimRequest,
            ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
                async move { self.dispatch_claim(request).await }
            }
        }

        impl fireweed_engine::CommitTransitionPort for $ty {
            fn commit_transition(
                &self,
                shard: &QueueKey,
                transition: fireweed_engine::CommitTransition,
                now: UtcTimestamp,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<
                Output = EngineResult<Vec<fireweed_engine::CommitEntryOutcome>>,
            > + Send {
                let shard = shard.clone();
                async move {
                    // Per-entry lease validation so fabricated tokens become Rejected(StaleLease)
                    // (TP-005 preflight). Accepted plain finalizes reuse FinalizePort; richer
                    // side-record / fence / lifecycle entries stay Unavailable until the full
                    // Strict commit surface is wired for Turso products.
                    let mut outcomes = Vec::with_capacity(transition.entries.len());
                    for entry in transition.entries {
                        let mut refs = vec![entry.claim_ref.clone()];
                        refs.extend(entry.additional_claim_refs.iter().cloned());
                        match AsyncProjectionStore::commit_validate(
                            self.projection.as_ref(),
                            shard.clone(),
                            refs,
                            now,
                        )
                        .await
                        {
                            Ok(()) => {
                                if !entry.side_records.is_empty()
                                    || entry.instance_fence.is_some()
                                    || !entry.lifecycle_items.is_empty()
                                {
                                    outcomes.push(fireweed_engine::CommitEntryOutcome::Rejected(
                                        EngineError::Unavailable,
                                    ));
                                    continue;
                                }
                                match FinalizePort::finalize(
                                    self,
                                    &shard,
                                    vec![FinalizeOutcome {
                                        item_id: entry.claim_ref.item_id,
                                        kind: entry.finalize,
                                        applied_state: None,
                                        not_before: None,
                                    }],
                                    now,
                                    expected_epoch,
                                )
                                .await
                                {
                                    Ok(()) => outcomes.push(
                                        fireweed_engine::CommitEntryOutcome::Committed {
                                            lifecycle_item_ids: Vec::new(),
                                        },
                                    ),
                                    Err(error) => outcomes.push(
                                        fireweed_engine::CommitEntryOutcome::Rejected(error),
                                    ),
                                }
                            }
                            Err(error) => {
                                outcomes.push(fireweed_engine::CommitEntryOutcome::Rejected(
                                    error,
                                ));
                            }
                        }
                    }
                    Ok(outcomes)
                }
            }
        }
        impl fireweed_engine::RecoveryReadPort for $ty {}
        impl BatchUpdatePort for $ty {
            fn batch_update(
                &self,
                shard: &QueueKey,
                request: fireweed_engine::BatchUpdateRequest,
                now: UtcTimestamp,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<
                Output = EngineResult<fireweed_engine::BatchUpdateResponse>,
            > + Send {
                let shard = shard.clone();
                async move {
                    use fireweed_engine::{
                        BatchUpdateItemRef, CommandChecksum,
                        CommandEnvelope, QueueCommand, batch_update_body_hash, plan_batch_update,
                        plan_batch_update_pipelined,
                    };

                    if request.updates.is_empty() {
                        return Err(EngineError::Invalid("empty batch update"));
                    }
                    if request.updates.len() > 1_000 {
                        return Err(EngineError::BatchTooLarge);
                    }

                    let definition =
                        AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone())
                            .await?;
                    let request_id = request.request_id.clone();
                    let fingerprint = batch_update_body_hash(&request)?;

                    let mut keys = Vec::new();
                    let mut ids = Vec::new();
                    for update in &request.updates {
                        match &update.item_ref {
                            BatchUpdateItemRef::ClientItemKey(key)
                            | BatchUpdateItemRef::Both {
                                client_item_key: key,
                                ..
                            } => keys.push(key.clone()),
                            BatchUpdateItemRef::ItemId(item_id) => ids.push(*item_id),
                        }
                    }
                    let snapshot = self.planner_update_snapshot(&shard, &keys, &ids).await?;

                    let plan = if self.pipeline_unresolved_updates() {
                        plan_batch_update_pipelined(
                            &definition,
                            true,
                            request.updates,
                            snapshot,
                        )
                    } else {
                        plan_batch_update(&definition, true, request.updates, snapshot)
                    };
                    let updates: Vec<_> = plan
                        .commands
                        .into_iter()
                        .map(|(_idx, update)| update)
                        .collect();
                    let response = fireweed_engine::BatchUpdateResponse {
                        request_id: request_id.clone(),
                        results: plan.outcomes,
                    };
                    if !updates.is_empty() {
                        let planned = self.reserve_planned_updates(&shard, &updates)?;
                        let item_ids: Vec<_> = updates
                            .iter()
                            .map(|u| u.item_id)
                            .filter(|id| id.as_u64() != 0)
                            .collect();
                        let envelope = CommandEnvelope {
                            command_id: self.ids.next_command_id(),
                            request_id: Some(request_id),
                            request_fingerprint: Some(fingerprint.0),
                            request_outcome: None,
                            item_ids,
                            command: QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand {
                                updates,
                            }),
                            checksum: CommandChecksum(0),
                            created_at: now,
                        };
                        let epoch = match expected_epoch {
                            Some(e) => e,
                            None => {
                                AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone())
                                    .await?
                            }
                        };
                        use fireweed_engine::AsyncCommitStrategy;
                        let strategy = self.engine.commit_strategy();
                        let committed = strategy
                            .commit(RawCommitRequest::new(shard, vec![envelope], epoch))
                            .await;
                        self.finish_planned(planned, committed.is_ok());
                        committed?;
                    }
                    Ok(response)
                }
            }
        }

        impl FinalizePort for $ty {
            fn finalize(
                &self,
                shard: &QueueKey,
                outcomes: Vec<FinalizeOutcome>,
                now: UtcTimestamp,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
                async move {
                    self.dispatch_finalize(shard, outcomes, now, expected_epoch)
                        .await
                }
            }
        }

        impl RenewLeasePort for $ty {
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
                        .map_err(map_lifecycle)
                }
            }
        }

        impl ReassignLeasePort for $ty {
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
                        None => {
                            AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?
                        }
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
                            EngineError::Storage(format!(
                                "async reassign submission failed: {error:?}"
                            ))
                        })??;
                    Ok(())
                }
            }
        }

        impl PurgePort for $ty {
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
                        .map_err(map_lifecycle)
                }
            }
        }

        impl UpsertPort for $ty {
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

        impl UpdateFieldsPort for $ty {
            fn update_fields(
                &self,
                _shard: &QueueKey,
                _item_id: ItemId,
                _field_ops: BTreeMap<String, Option<Bytes>>,
                _payload: fireweed_engine::PayloadUpdate,
                _entity: Option<serde_json::Value>,
                _expected_item_version: Option<u64>,
                _now: UtcTimestamp,
                _expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
                std::future::ready(Err(EngineError::Unavailable))
            }
        }

        impl ReclaimPort for $ty {
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
                        .map_err(map_lifecycle)
                }
            }
        }

        impl ReclaimDriver for $ty {
            fn tick(
                &self,
                _now: UtcTimestamp,
            ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
                std::future::ready(Ok(TickReport::default()))
            }
        }

        impl SetGatesPort for $ty {}
        impl fireweed_engine::ReschedulePort for $ty {}
        impl fireweed_engine::DiscoveryPort for $ty {}
        impl HotProjectionQueryPort for $ty {
            fn hot_projection_capabilities(
                &self,
                _shard: &QueueKey,
            ) -> QueryCapabilityFlags {
                QueryCapabilityFlags::default()
            }
        }

        impl IndexQueryPort for $ty {
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

        impl ItemMutationPort for $ty {
            fn mutate_items(
                &self,
                _shard: &QueueKey,
                _request: ItemMutationRequest,
                _expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<ItemMutationResponse>> + Send {
                std::future::ready(Err(EngineError::Unavailable))
            }
        }

        impl HistoricalProjectionRead for $ty {
            type AsOfProjection = InMemoryProjection;
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
                _shard: &QueueKey,
                _position: CommandPosition,
                _query: F,
            ) -> impl std::future::Future<Output = EngineResult<T>> + Send
            where
                T: Send + 'static,
                F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static,
            {
                std::future::ready(Err(EngineError::Unavailable))
            }
        }

        // SnapshotStore: Turso products share the log-axis high-water / snapshot plane.
        impl SnapshotStore for $ty {
            fn write_snapshot(
                &self,
                shard: &QueueKey,
                position: CommandPosition,
                snapshot: ProjectionSnapshot,
            ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
                AsyncLogStore::write_snapshot(
                    self.log.as_ref(),
                    shard.clone(),
                    position,
                    snapshot,
                )
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

        impl ProjectionRead for $ty {
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
                AsyncProjectionStore::render_claimed(
                    self.projection.as_ref(),
                    shard.clone(),
                    ids.to_vec(),
                )
            }
            fn live_items(
                &self,
                shard: &QueueKey,
                keys: &[ClientItemKey],
            ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send
            {
                let shard = shard.clone();
                let keys = keys.to_vec();
                async move {
                    self.catch_up_produce(&shard).await?;
                    self.projection.server_live_items(&shard, &keys).await
                }
            }
            fn metrics(
                &self,
                shard: &QueueKey,
            ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
                let shard = shard.clone();
                async move {
                    self.catch_up_projection(&shard).await?;
                    self.projection.server_metrics(&shard).await
                }
            }
            fn terminal_emission_metrics(
                &self,
                shard: &QueueKey,
                _now: UtcTimestamp,
                _emit_change_records: bool,
                _emission_cursor: Option<&CommandPosition>,
            ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send
            {
                self.projection.server_terminal_emission_metrics(shard)
            }
        }
    };
}

impl_turso_product_ports!(
    AtomicTursoBackend<InProcessLogStore<fireweed_projection::MemoryLog>>,
    DurabilityClass::Atomic,
    "atomic durable log batch with synchronous Turso apply"
);

#[cfg(feature = "sqlite")]
impl_turso_product_ports!(
    AtomicTursoBackend<InProcessLogStore<fireweed_sqlite::SqliteLog>>,
    DurabilityClass::Atomic,
    "atomic durable log batch with synchronous Turso apply"
);

#[cfg(feature = "postgres")]
impl_turso_product_ports!(
    AtomicTursoBackend<InProcessLogStore<fireweed_postgres::PostgresLog>>,
    DurabilityClass::Atomic,
    "atomic durable log batch with synchronous Turso apply"
);

// ---------------------------------------------------------------------------
// Object-log × Turso (filesystem / s3)
// ---------------------------------------------------------------------------

#[cfg(feature = "objectlog")]
async fn note_produce_positions(
    last_produce: &tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>,
    positions: &[CommandPosition],
    commands: &[CommandEnvelope],
) {
    let mut guard = last_produce.lock().await;
    for (position, envelope) in positions.iter().zip(commands) {
        match &envelope.command {
            QueueCommand::Push(_)
            | QueueCommand::UpdateFields(_)
            | QueueCommand::UpdateFieldsBatch(_) => {
                guard
                    .entry(position.queue.clone())
                    .and_modify(|current| {
                        if position.backend_epoch > current.backend_epoch
                            || (position.backend_epoch == current.backend_epoch
                                && position.sequence > current.sequence)
                        {
                            *current = position.clone();
                        }
                    })
                    .or_insert_with(|| position.clone());
            }
            _ => {}
        }
    }
}

#[cfg(feature = "objectlog")]
#[derive(Clone)]
struct ObjectLogTursoCommitter {
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<TursoRelational>,
    apply_turn: Arc<tokio::sync::Notify>,
    async_apply: Option<AsyncProjectionApplyCoordinator<TursoRelational>>,
    last_produce: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
}

#[cfg(feature = "objectlog")]
impl SeparateReplayCommitter for ObjectLogTursoCommitter {
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
        let apply_turn = Arc::clone(&self.apply_turn);
        let async_apply = self.async_apply.clone();
        let last_produce = Arc::clone(&self.last_produce);
        Box::pin(async move {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            match fault {
                RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                RawCommitFault::None | RawCommitFault::AfterAppendBeforeApply => {}
            }
            let reservation = match &async_apply {
                Some(coordinator) => Some(coordinator.reserve(shard.clone(), &commands).await?),
                None => None,
            };
            let outcome = match log
                .packed_append(shard.clone(), commands.clone(), expected_epoch)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                        coordinator.cancel(reservation).await;
                    }
                    return Err(error);
                }
            };
            note_produce_positions(&last_produce, &outcome.positions, &commands).await;
            if matches!(fault, RawCommitFault::AfterAppendBeforeApply) {
                if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                    coordinator.cancel(reservation).await;
                }
                return Ok(RawCommitOutcome::appended(outcome.positions));
            }
            if let Some(batch) = outcome.apply_batch {
                if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                    coordinator
                        .enqueue_reserved(reservation, batch.positions, batch.commands)
                        .await?;
                } else {
                    wait_turso_apply_turn(
                        projection.as_ref(),
                        &shard,
                        &batch.positions,
                        &apply_turn,
                    )
                    .await?;
                    AsyncProjectionStore::apply_live(
                        projection.as_ref(),
                        batch.positions,
                        batch.commands,
                    )
                    .await?;
                    apply_turn.notify_waiters();
                }
            } else if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                coordinator.cancel(reservation).await;
            } else {
                wait_turso_apply_turn(projection.as_ref(), &shard, &outcome.positions, &apply_turn)
                    .await?;
            }
            Ok(if async_apply.is_some() {
                RawCommitOutcome::appended(outcome.positions)
            } else {
                RawCommitOutcome::applied(outcome.positions)
            })
        })
    }
}

#[cfg(feature = "objectlog")]
async fn wait_turso_apply_turn(
    projection: &TursoRelational,
    shard: &QueueKey,
    positions: &[CommandPosition],
    apply_turn: &tokio::sync::Notify,
) -> EngineResult<()> {
    let Some(first) = positions.first() else {
        return Ok(());
    };
    loop {
        let high_water =
            AsyncProjectionStore::recovery_high_water(projection, shard.clone()).await?;
        let expected = high_water
            .as_ref()
            .map(|position| position.sequence.saturating_add(1))
            .unwrap_or(0);
        if expected == first.sequence {
            return Ok(());
        }
        if expected > first.sequence {
            return Err(EngineError::Storage(format!(
                "Turso packed apply skipped sequence: expected {expected}, first {}",
                first.sequence
            )));
        }
        apply_turn.notified().await;
    }
}

#[cfg(feature = "objectlog")]
type ObjectLogEngine = AsyncComposedBackend<
    SeparateReplayCommit<ObjectLogTursoCommitter>,
    ObjectLogTaskDispatcher,
    ProjectionClaimPlanner<InProcessControlPlane, ObjectLogEngineStore, TursoRelational, SeqIdGen>,
    ProjectionPushPlanner<
        InProcessControlPlane,
        ObjectLogEngineStore,
        TursoRelational,
        SeqIdGen,
    >,
    ProjectionLifecyclePlanner<
        InProcessControlPlane,
        ObjectLogEngineStore,
        TursoRelational,
        SeqIdGen,
    >,
    ProjectionReclaimPlanner<
        InProcessControlPlane,
        ObjectLogEngineStore,
        TursoRelational,
        SeqIdGen,
    >,
>;

/// Provider-neutral object-log × Turso product (not a public `ObjectLogTursoBackend` alias).
#[cfg(feature = "objectlog")]
pub struct DerivedObjectLogTursoBackend {
    engine: ObjectLogEngine,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<TursoRelational>,
    #[allow(dead_code)]
    projection_path: PathBuf,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    /// Shared with push planners; recovery observes recovered item ids into this map.
    counters: Arc<QueueCounters>,
    #[allow(dead_code)]
    node_id: u8,
    async_apply: Option<AsyncProjectionApplyCoordinator<TursoRelational>>,
    last_produce: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
    produce_caught_up: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
}

#[cfg(feature = "objectlog")]
impl DerivedObjectLogTursoBackend {
    pub async fn from_log_and_projection(
        log: ObjectLogEngineStore,
        projection: TursoRelational,
        projection_path: PathBuf,
        node_id: u8,
        _async_spec: Option<AsyncProjectionSpec>,
    ) -> EngineResult<Self> {
        let log = Arc::new(log);
        let projection = Arc::new(projection);
        let control = Arc::new(InProcessControlPlane::new());
        let ids = Arc::new(SeqIdGen::default());
        let counters = Arc::new(QueueCounters::default());
        let async_apply = match _async_spec {
            Some(spec) => Some(AsyncProjectionApplyCoordinator::new(
                Arc::clone(&projection),
                fireweed_engine::AsyncProjectionSpec {
                    // Turso apply and object-log packing share a disk. SQLite
                    // and memory coordinators keep apply_start_delay_ms = 0.
                    apply_start_delay_ms: spec.apply_start_delay_ms.max(300),
                    ..spec
                },
            )?),
            None => None,
        };
        let last_produce = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let produce_caught_up = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let committer = ObjectLogTursoCommitter {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
            apply_turn: Arc::new(tokio::sync::Notify::new()),
            async_apply: async_apply.clone(),
            last_produce: Arc::clone(&last_produce),
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
            ObjectLogTaskDispatcher::new(),
            claim,
            push,
            1024,
        )
        .with_lifecycle_planner(lifecycle)
        .with_reclaim_planner(reclaim);

        let backend = Self {
            engine,
            log,
            projection,
            projection_path,
            control,
            ids,
            counters,
            node_id,
            async_apply,
            last_produce,
            produce_caught_up,
        };
        backend.recover_async().await?;
        Ok(backend)
    }

    async fn catch_up_projection(&self, shard: &QueueKey) -> EngineResult<()> {
        let Some(coordinator) = &self.async_apply else {
            return Ok(());
        };
        coordinator.ensure_healthy(shard)?;
        let target = AsyncLogStore::high_water(self.log.as_ref(), shard.clone()).await?;
        let Some(target) = target else {
            return Ok(());
        };
        self.wait_for_projection(shard, &target).await
    }

    async fn catch_up_produce(&self, shard: &QueueKey) -> EngineResult<()> {
        let Some(coordinator) = &self.async_apply else {
            return Ok(());
        };
        coordinator.ensure_healthy(shard)?;
        let target = self.last_produce.lock().await.get(shard).cloned();
        let Some(target) = target else {
            return Ok(());
        };
        if let Some(caught) = self.produce_caught_up.lock().await.get(shard)
            && (caught.backend_epoch > target.backend_epoch
                || (caught.backend_epoch == target.backend_epoch
                    && caught.sequence >= target.sequence))
        {
            return Ok(());
        }
        self.wait_for_projection(shard, &target).await?;
        self.produce_caught_up
            .lock()
            .await
            .insert(shard.clone(), target);
        Ok(())
    }

    async fn wait_for_projection(
        &self,
        shard: &QueueKey,
        target: &CommandPosition,
    ) -> EngineResult<()> {
        let Some(coordinator) = &self.async_apply else {
            return Ok(());
        };
        loop {
            coordinator.ensure_healthy(shard)?;
            let projected =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), shard.clone())
                    .await?;
            if let Some(projected) = projected
                && (projected.backend_epoch > target.backend_epoch
                    || (projected.backend_epoch == target.backend_epoch
                        && projected.sequence >= target.sequence))
            {
                return Ok(());
            }
            coordinator.wait_for_progress(shard).await?;
        }
    }

    async fn snapshot_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        loop {
            let views = self.projection.server_live_items(shard, keys).await?;
            if self.async_apply.is_none() || views.iter().all(|view| view.is_some()) {
                return Ok(views);
            }
            let target = self.last_produce.lock().await.get(shard).cloned();
            let Some(target) = target else {
                return Ok(views);
            };
            let projected = AsyncProjectionStore::recovery_high_water(
                self.projection.as_ref(),
                shard.clone(),
            )
            .await?;
            if let Some(projected) = projected
                && (projected.backend_epoch > target.backend_epoch
                    || (projected.backend_epoch == target.backend_epoch
                        && projected.sequence >= target.sequence))
            {
                return Ok(views);
            }
            if let Some(coordinator) = &self.async_apply {
                coordinator.ensure_healthy(shard)?;
                coordinator.wait_for_progress(shard).await?;
            } else {
                return Ok(views);
            }
        }
    }

    async fn planner_update_snapshot(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
        _ids: &[ItemId],
    ) -> EngineResult<Vec<fireweed_engine::BatchUpdateSnapshotItem>> {
        self.projection.server_update_snapshot(shard, keys).await
    }

    fn pipeline_unresolved_updates(&self) -> bool {
        self.async_apply.is_some()
    }

    fn reserve_planned_updates(
        &self,
        _shard: &QueueKey,
        _updates: &[fireweed_engine::UpdateFieldsCommand],
    ) -> EngineResult<Option<PlannedReservation>> {
        Ok(None)
    }

    fn finish_planned(&self, _planned: Option<PlannedReservation>, _ok: bool) {}

    async fn recover_async(&self) -> EngineResult<()> {
        let definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let _ =
                AsyncControlPlane::create_queue(self.control.as_ref(), definition.clone()).await;
            AsyncProjectionStore::ensure_shard(self.projection.as_ref(), definition).await?;
            let high_water =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), shard.clone())
                    .await?;
            let mut from = None;
            loop {
                let page =
                    AsyncLogStore::read_from(self.log.as_ref(), shard.clone(), from.clone(), 256)
                        .await?;
                if page.entries.is_empty() {
                    break;
                }
                // Seed QueueCounters past every recovered item id so reopen never remints.
                for (_, env) in &page.entries {
                    for item_id in &env.item_ids {
                        self.counters.observe(&shard, *item_id);
                    }
                }
                let tail: Vec<_> = page
                    .entries
                    .iter()
                    .filter(|(position, _)| {
                        high_water.as_ref().is_none_or(|hw| {
                            position.backend_epoch > hw.backend_epoch
                                || (position.backend_epoch == hw.backend_epoch
                                    && position.sequence > hw.sequence)
                        })
                    })
                    .cloned()
                    .collect();
                if !tail.is_empty() {
                    let positions: Vec<_> = tail.iter().map(|(p, _)| p.clone()).collect();
                    let commands: Vec<_> = tail.iter().map(|(_, e)| e.clone()).collect();
                    AsyncProjectionStore::apply_recovery(
                        self.projection.as_ref(),
                        positions,
                        commands,
                    )
                    .await?;
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
            self.drain_claim_outbox(&shard).await?;
        }
        Ok(())
    }

    async fn drain_claim_outbox(&self, shard: &QueueKey) -> EngineResult<()> {
        let pending = self
            .projection
            .pending_claim_outbox(shard.tenant_id.as_str(), shard.queue_id.as_str())
            .await?;
        for row in pending {
            let item_ids: Vec<ItemId> = serde_json::from_str::<Vec<String>>(&row.item_ids_json)
                .map_err(|e| EngineError::Storage(e.to_string()))?
                .into_iter()
                .map(|id| ItemId::new(id).map_err(|e| EngineError::Storage(e.to_string())))
                .collect::<EngineResult<_>>()?;
            let token = LeaseToken::new(row.lease_token)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            let worker_id = row
                .worker_id
                .map(fireweed_core::WorkerId::new)
                .transpose()
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            let envelope = CommandEnvelope {
                command_id: fireweed_engine::CommandId::new(row.outbox_id.clone()),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::Claim(ClaimCommand {
                    item_ids,
                    lease_token: token,
                    lease_expires_at: fireweed_core::UtcTimestamp::new(
                        row.lease_expires_at.div_euclid(1_000_000_000),
                        row.lease_expires_at.rem_euclid(1_000_000_000) as u32,
                    )
                    .map_err(|e| EngineError::Storage(e.to_string()))?,
                    worker_id,
                }),
                checksum: CommandChecksum(0),
                created_at: fireweed_core::UtcTimestamp::new(
                    row.created_at.div_euclid(1_000_000_000),
                    row.created_at.rem_euclid(1_000_000_000) as u32,
                )
                .map_err(|e| EngineError::Storage(e.to_string()))?,
            };
            let epoch = AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?;
            self.log
                .packed_append(shard.clone(), vec![envelope], epoch)
                .await?;
            self.projection
                .delete_claim_outbox_row(
                    shard.tenant_id.as_str(),
                    shard.queue_id.as_str(),
                    &row.outbox_id,
                )
                .await?;
        }
        Ok(())
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

    #[allow(dead_code)]
    pub fn projection_path(&self) -> &Path {
        &self.projection_path
    }

    /// Borrow the object-log axis (change-record emission and diagnostics).
    pub fn with_log<R>(&self, f: impl FnOnce(&ObjectLogEngineStore) -> R) -> R {
        f(self.log.as_ref())
    }

    /// Borrow the Turso projection axis (rebuild/read diagnostics).
    pub fn projection(&self) -> &Arc<TursoRelational> {
        &self.projection
    }

    async fn commit_prepared(&self, request: RawCommitRequest) -> EngineResult<()> {
        use fireweed_engine::AsyncCommitStrategy;
        self.engine.commit_strategy().commit(request).await?;
        Ok(())
    }

    async fn dispatch_push(
        &self,
        request: AsyncPushRequest,
    ) -> EngineResult<fireweed_engine::PushBatchOutcome> {
        match self.engine.prepare_push(request).await.map_err(map_push)? {
            PreparedPush::Replay(item_ids) => {
                Ok(fireweed_engine::PushBatchOutcome::replayed(item_ids))
            }
            PreparedPush::Commit { request, item_ids } => {
                self.commit_prepared(request).await?;
                Ok(fireweed_engine::PushBatchOutcome::fresh(item_ids))
            }
        }
    }

    async fn dispatch_claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        if request.compatibility != ClaimCompatibility::default() {
            return self.dispatch_claim_legacy(request).await;
        }
        self.dispatch_class_s_claim(request).await
    }

    async fn dispatch_class_s_claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        self.catch_up_produce(&request.shard).await?;
        let epoch = match request.expected_epoch {
            Some(epoch) => epoch,
            None => AsyncLogStore::current_epoch(self.log.as_ref(), request.shard.clone()).await?,
        };
        let command_id = self.ids.next_command_id();
        let stub = CommandEnvelope {
            command_id: command_id.clone(),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: Vec::new(),
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: Vec::new(),
                lease_token: request.lease_token.clone(),
                lease_expires_at: request.lease_expires_at,
                worker_id: Some(request.worker_id.clone()),
            }),
            checksum: CommandChecksum(0),
            created_at: request.now,
        };
        let reservation = match &self.async_apply {
            Some(coordinator) => Some(
                coordinator
                    .reserve(request.shard.clone(), std::slice::from_ref(&stub))
                    .await?,
            ),
            None => None,
        };
        let now_nanos = request
            .eligibility_at()
            .seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(request.eligibility_at().nanoseconds));
        let expires_nanos = request
            .lease_expires_at
            .seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(request.lease_expires_at.nanoseconds));
        let leased = match self
            .projection
            .class_s_claim_for_queue(
                request.shard.tenant_id.as_str(),
                request.shard.queue_id.as_str(),
                now_nanos,
                i64::try_from(request.max_items)
                    .map_err(|_| EngineError::Storage("claim limit".into()))?,
                &request.lease_token,
                expires_nanos,
                command_id.0.as_str(),
                Some(request.worker_id.as_str()),
            )
            .await
        {
            Ok(leased) => leased,
            Err(error) => {
                if let (Some(coordinator), Some(reservation)) = (&self.async_apply, reservation) {
                    coordinator.cancel(reservation).await;
                }
                return Err(error);
            }
        };
        if leased.items.is_empty() {
            if let (Some(coordinator), Some(reservation)) = (&self.async_apply, reservation) {
                coordinator.cancel(reservation).await;
            }
            return Ok(Claimed::default());
        }
        let item_ids: Vec<ItemId> = leased
            .items
            .iter()
            .map(|item| ItemId::new(&item.item_id).map_err(|e| EngineError::Storage(e.to_string())))
            .collect::<EngineResult<_>>()?;
        let envelope = CommandEnvelope {
            command_id,
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: item_ids.clone(),
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: item_ids.clone(),
                lease_token: request.lease_token.clone(),
                lease_expires_at: request.lease_expires_at,
                worker_id: Some(request.worker_id.clone()),
            }),
            checksum: CommandChecksum(0),
            created_at: request.now,
        };
        let committed = self
            .append_class_s_claim(
                request.shard.clone(),
                envelope,
                epoch,
                reservation,
                &leased.outbox_id,
            )
            .await;
        if let Err(error) = committed {
            return Err(error);
        }
        self.projection
            .remember_leases(&request.shard, &item_ids, request.lease_token.clone())
            .await;
        claimed_from_class_s(&request.lease_token, leased)
    }

    async fn append_class_s_claim(
        &self,
        shard: QueueKey,
        envelope: CommandEnvelope,
        epoch: u64,
        reservation: Option<fireweed_objectlog::AsyncProjectionApplyReservation>,
        outbox_id: &str,
    ) -> EngineResult<()> {
        let commands = vec![envelope];
        let outcome = match self
            .log
            .packed_append(shard.clone(), commands.clone(), epoch)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                if let (Some(coordinator), Some(reservation)) = (&self.async_apply, reservation) {
                    coordinator.cancel(reservation).await;
                }
                return Err(error);
            }
        };
        if let Some(batch) = outcome.apply_batch {
            if let (Some(coordinator), Some(reservation)) = (&self.async_apply, reservation) {
                coordinator
                    .enqueue_reserved(reservation, batch.positions, batch.commands)
                    .await?;
            } else {
                AsyncProjectionStore::apply_live(
                    self.projection.as_ref(),
                    batch.positions,
                    batch.commands,
                )
                .await?;
            }
        } else if let (Some(coordinator), Some(reservation)) = (&self.async_apply, reservation) {
            coordinator.cancel(reservation).await;
        }
        self.projection
            .delete_claim_outbox_row(
                shard.tenant_id.as_str(),
                shard.queue_id.as_str(),
                outbox_id,
            )
            .await?;
        Ok(())
    }

    async fn dispatch_claim_legacy(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        match self
            .engine
            .prepare_claim(request.clone())
            .await
            .map_err(map_claim)?
        {
            PreparedClaim::Empty => Ok(Claimed::default()),
            PreparedClaim::Commit {
                request: commit,
                item_ids,
                cohort_id,
            } => {
                self.commit_prepared(commit).await?;
                self.engine
                    .render_prepared_claim(request, item_ids, cohort_id)
                    .await
                    .map_err(map_claim)
            }
        }
    }

    async fn dispatch_finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<()> {
        if outcomes.is_empty() {
            return Err(EngineError::Invalid(
                "finalize item batch must not be empty",
            ));
        }
        let PreparedFinalize { request, .. } = self
            .engine
            .prepare_finalize(shard.clone(), outcomes, now, expected_epoch)
            .await
            .map_err(map_lifecycle)?;
        self.commit_prepared(request).await
    }

    fn create_queue_impl(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send + '_ {
        async move {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let outcome = self
                .log
                .create_or_read_definition(definition.clone())
                .await?;
            fireweed_engine::ControlPlane::cache_authoritative_definition(
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
            let proj_outcome = self
                .projection
                .create_or_read_queue(outcome.definition.clone())
                .await?;
            if proj_outcome.definition != outcome.definition {
                return Err(EngineError::QueueDefinitionConflict);
            }
            Ok(outcome)
        }
    }

    #[allow(dead_code)]
    pub async fn delete_projection_file(&self) -> EngineResult<()> {
        let path = self.projection_path.clone();
        // Drop is composition-owned; remove the durable projection file for rebuild.
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| EngineError::Storage(format!("delete turso projection: {e}")))?;
            for suffix in ["-wal", "-shm"] {
                let side = PathBuf::from(format!("{}{suffix}", path.display()));
                let _ = std::fs::remove_file(side);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "objectlog")]
impl_turso_product_ports!(
    DerivedObjectLogTursoBackend,
    DurabilityClass::EventualApply,
    "object-log append then Turso apply (SeparateReplayCommit)"
);

// ---------------------------------------------------------------------------
// Sync open helpers used by the facade matrix dispatch
// ---------------------------------------------------------------------------

pub fn assemble_memory_log_turso(
    projection_path: PathBuf,
) -> EngineResult<AtomicTursoBackend<InProcessLogStore<fireweed_projection::MemoryLog>>> {
    let projection = open_turso_projection(&projection_path)?;
    let log = InProcessLogStore::new(fireweed_projection::MemoryLog::new());
    block_on_turso(async move {
        AtomicTursoBackend::assemble(log, projection, projection_path, 0).await
    })
}

#[cfg(feature = "sqlite")]
pub fn assemble_sqlite_log_turso(
    log_path: &str,
    projection_path: PathBuf,
) -> EngineResult<AtomicTursoBackend<InProcessLogStore<fireweed_sqlite::SqliteLog>>> {
    let projection = open_turso_projection(&projection_path)?;
    let sqlite_log = fireweed_sqlite::SqliteLog::open(log_path).map_err(map_turso_storage)?;
    let log =
        InProcessLogStore::new_with_blocking_offload(sqlite_log, DEFAULT_BLOCKING_AXIS_IN_FLIGHT)?;
    block_on_turso(async move {
        AtomicTursoBackend::assemble(log, projection, projection_path, 0).await
    })
}

#[cfg(feature = "postgres")]
pub fn assemble_postgres_log_turso(
    log: fireweed_postgres::PostgresLog,
    projection_path: PathBuf,
    node_id: u8,
) -> EngineResult<AtomicTursoBackend<InProcessLogStore<fireweed_postgres::PostgresLog>>> {
    let projection = open_turso_projection(&projection_path)?;
    // Offload sync postgres LogStore calls so assemble/recover never runs the
    // blocking client on a Tokio worker (Client methods and Drop both panic
    // with nested-runtime when a handle is present on the thread).
    let log = InProcessLogStore::new_with_blocking_offload(log, DEFAULT_BLOCKING_AXIS_IN_FLIGHT)?;
    // Dedicated multi-thread runtime on this OS thread only for the async
    // assemble future. PostgresLog Drop offloads Client close to a bare thread.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .thread_name("fw-pg-turso-open")
        .build()
        .map_err(|e| EngineError::Storage(format!("postgres×turso open runtime: {e}")))?;
    let result = rt.block_on(AtomicTursoBackend::assemble(
        log,
        projection,
        projection_path,
        node_id,
    ));
    // Shut down workers before returning so any residual Drop cannot nest on them.
    drop(rt);
    result
}

#[cfg(feature = "objectlog")]
pub fn assemble_objectlog_turso(
    log: ObjectLogEngineStore,
    projection_path: PathBuf,
    async_spec: Option<AsyncProjectionSpec>,
) -> EngineResult<DerivedObjectLogTursoBackend> {
    let projection = open_turso_projection(&projection_path)?;
    fireweed_objectlog::block_on_objectlog(async move {
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            projection_path,
            0,
            async_spec,
        )
        .await
    })
}
