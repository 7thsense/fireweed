//! Turso projection composition for the public 5×4 storage matrix.
//!
//! Composes each log axis with [`fireweed_turso::TursoRelational`] through the same
//! engine planners / commit strategies used by other derived projections:
//! - Atomic logs (memory / sqlite / postgres): [`UnifiedAtomicCommit`] (log-replay product shape)
//! - Object logs (filesystem / s3): [`SeparateReplayCommit`] (provider-neutral LogEngine constructors)
//!
//! This module deliberately avoids an `ObjectLogTursoBackend` public alias.

#![allow(clippy::manual_async_fn)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use fireweed_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueryCapabilityFlags, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, AsyncCommitStrategy, CommitEntryOutcome, CommitEntryStatus,
    CommitTransition, CommitTransitionEntry, EntryRecovery, FinalizeCommand, IdempotencyDecision,
    PushCommand, QueueIdempotencyCache, RequestOutcome, WriteSideRecordsCommand, build_push_items,
    commit_body_hash, compile_entity_schema, outcome_entry_from_recovery, outcomes_from_recovery,
    request_expires_at, stage_unique_push_keys, validate_distinct_commit_claims, validate_entity,
    validate_instance_fence,
};
use fireweed_engine::{
    AppendAdmissionClass, AsyncClaimError, AsyncCommitSubmitError, AsyncComposedBackend,
    AsyncControlPlane, AsyncFinalizeRequest, AsyncLifecycleError, AsyncLogStore,
    AsyncProjectionSpec, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest,
    AsyncReclaimRequest, AsyncRenewRequest, Backend, BatchUpdatePort, ClaimCommand,
    ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, CommandChecksum, CommandEnvelope,
    CommandPosition, ControlPlaneStore, CreateQueueOutcome, DEFAULT_BLOCKING_AXIS_IN_FLIGHT,
    DurabilityClass, EngineError, EngineResult, FinalizeOutcome, FinalizePort, FinalizeTarget,
    HistoricalProjectionRead, HotProjectionQueryPort, IdGen, InProcessControlPlane,
    InProcessLogStore, IndexQueryPort, InlineOwnedTaskDispatcher, ItemMutationPort,
    ItemMutationRequest, ItemMutationResponse, ItemView, LeaseView, LiveItemView, LogStore,
    OwnedTask, PendingPage, PendingSummary, PreparedClaim, PreparedFinalize, PreparedPush,
    ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner, ProjectionRead,
    ProjectionReclaimPlanner, ProjectionSnapshot, PurgePort, PushPort, PushSpec, QueueCommand,
    QueueCounters, QueueGateError, QueueKey, QueueMetrics, RawCommitFault, RawCommitOutcome,
    RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort,
    RenewLeasePort, RenewTarget, SeparateReplayCommit, SeparateReplayCommitter, SeqIdGen,
    SetGatesPort, SnapshotRef, SnapshotStore, TerminalEmissionMetrics, TickReport,
    UnifiedAtomicCommit, UnifiedAtomicCommitter, UpdateFieldsBatchCommand, UpdateFieldsPort,
    UpsertOutcome, UpsertPort,
};
use fireweed_engine::{
    PushItem, ReplacePendingCommand, UpdateFieldsCommand, claim_by_query_body_hash,
    generate_query_lease_token, item_mutation_fingerprint, resolve_write_epoch_async,
    validate_api001_reserved_write_fields,
};
use fireweed_projection::InMemoryProjection;
use fireweed_turso::{TursoConfig, TursoRelational, claimed_from_class_s};

#[cfg(feature = "objectlog")]
use fireweed_objectlog::{
    AsyncProjectionApplyCoordinator, ObjectLogEngineStore, ObjectLogTaskDispatcher,
    PackedAppendOutcome,
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

fn map_submit(operation: &'static str, error: AsyncCommitSubmitError) -> EngineError {
    match error {
        AsyncCommitSubmitError::Admission(QueueGateError::PerKeyFull) => {
            EngineError::Backpressure {
                resource: "keyed queue per-key waiters",
            }
        }
        AsyncCommitSubmitError::Admission(QueueGateError::QueueFull) => EngineError::Backpressure {
            resource: "keyed queue waiters",
        },
        error => EngineError::Storage(format!("async {operation} submission failed: {error:?}")),
    }
}

fn map_claim(error: AsyncClaimError) -> EngineError {
    match error {
        AsyncClaimError::BeforeCommit(error) | AsyncClaimError::Commit(error) => error,
        AsyncClaimError::AfterCommit { source, .. } => source,
        AsyncClaimError::Submit(error) => map_submit("claim", error),
    }
}

fn map_push(error: AsyncPushError) -> EngineError {
    match error {
        AsyncPushError::BeforeCommit(error) | AsyncPushError::Commit(error) => error,
        AsyncPushError::AfterCommit { source, .. } => source,
        AsyncPushError::Submit(error) => map_submit("push", error),
    }
}

fn map_lifecycle(error: AsyncLifecycleError) -> EngineError {
    match error {
        AsyncLifecycleError::BeforeCommit(error) | AsyncLifecycleError::Commit(error) => error,
        AsyncLifecycleError::AfterCommit { source, .. } => source,
        AsyncLifecycleError::Submit(error) => map_submit("lifecycle", error),
    }
}

#[cfg(test)]
mod contention_mapping_tests {
    use super::*;

    fn per_key() -> AsyncCommitSubmitError {
        AsyncCommitSubmitError::Admission(QueueGateError::PerKeyFull)
    }

    fn global() -> AsyncCommitSubmitError {
        AsyncCommitSubmitError::Admission(QueueGateError::QueueFull)
    }

    #[test]
    fn map_claim_preserves_per_key_and_global_gate_capacity() {
        assert_eq!(
            map_claim(AsyncClaimError::Submit(per_key())),
            EngineError::Backpressure {
                resource: "keyed queue per-key waiters",
            }
        );
        assert_eq!(
            map_claim(AsyncClaimError::Submit(global())),
            EngineError::Backpressure {
                resource: "keyed queue waiters",
            }
        );
    }

    #[test]
    fn map_push_preserves_per_key_and_global_gate_capacity() {
        assert_eq!(
            map_push(AsyncPushError::Submit(per_key())),
            EngineError::Backpressure {
                resource: "keyed queue per-key waiters",
            }
        );
        assert_eq!(
            map_push(AsyncPushError::Submit(global())),
            EngineError::Backpressure {
                resource: "keyed queue waiters",
            }
        );
    }

    #[test]
    fn map_lifecycle_preserves_per_key_and_global_gate_capacity() {
        assert_eq!(
            map_lifecycle(AsyncLifecycleError::Submit(per_key())),
            EngineError::Backpressure {
                resource: "keyed queue per-key waiters",
            }
        );
        assert_eq!(
            map_lifecycle(AsyncLifecycleError::Submit(global())),
            EngineError::Backpressure {
                resource: "keyed queue waiters",
            }
        );
    }

    fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
        let (_, tail) = source
            .split_once(start)
            .unwrap_or_else(|| panic!("missing source-audit start marker: {start}"));
        let (body, _) = tail
            .split_once(end)
            .unwrap_or_else(|| panic!("missing source-audit end marker: {end}"));
        body
    }

    #[test]
    fn append_admission_carrier_audits_derived_dispatch_and_commit_sites() {
        let compose_file = include_str!("turso_compose.rs");
        let (_, compose) = compose_file
            .rsplit_once("// Atomic log-replay × Turso")
            .expect("production Turso composition boundary");
        let async_composed = include_str!("../../fireweed-engine/src/async_composed.rs");
        let production_async = async_composed
            .rsplit_once("#[cfg(test)]\nmod tests")
            .expect("async composed unit-test boundary")
            .0;

        assert!(compose.contains(".with_append_admission(AppendAdmissionClass::AtomicNative)"));
        assert!(compose.contains(".with_append_admission(AppendAdmissionClass::KeyedPermitLive)"));
        assert!(
            production_async
                .contains("let request = request.with_append_admission(self.append_admission);")
        );
        let commit_sites = production_async
            .lines()
            .filter(|line| line.trim_start().starts_with(".commit("))
            .count();
        let classified_commit_sites = production_async
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                line.starts_with(".commit(") && line.contains("with_append_admission")
            })
            .count();
        assert_eq!(
            commit_sites, 11,
            "refresh the source audit when commit sites change"
        );
        assert_eq!(classified_commit_sites, commit_sites);

        let atomic_commit = between(
            compose,
            "fn commit_atomic(&self, request: Self::Request)",
            "type AtomicEngine",
        );
        assert!(atomic_commit.contains("into_parts_with_append_admission"));
        assert!(atomic_commit.contains("AppendAdmissionClass::AtomicNative"));

        let direct_batch = between(compose, "impl BatchUpdatePort", "impl FinalizePort");
        assert!(direct_batch.contains("AppendAdmissionClass::AtomicNative"));
        assert!(direct_batch.contains("AppendAdmissionClass::SelectionRequired"));

        let object_log_commit = between(
            compose,
            "fn commit_replayable(&self, request: Self::Request)",
            "async fn publish_packed_apply",
        );
        for class in [
            "NonDerived",
            "KeyedPermitLive",
            "SelectionRequired",
            "Bypass",
            "AtomicNative",
            "RecoveryOnly",
            "ClaimCoordinatorLive",
        ] {
            assert!(
                object_log_commit.contains(&format!("AppendAdmissionClass::{class}")),
                "ObjectLogTursoCommitter must exhaustively observe {class}"
            );
        }

        let (_, derived) = compose
            .split_once("impl DerivedObjectLogTursoBackend {")
            .expect("derived Turso implementation");
        let recovery = between(
            derived,
            "async fn drain_claim_outbox",
            "async fn claimed_targets",
        );
        assert!(recovery.contains("AppendAdmissionClass::RecoveryOnly"));
        assert!(recovery.contains(".packed_append("));

        let push = between(derived, "async fn dispatch_push", "async fn dispatch_claim");
        assert!(push.contains("AppendAdmissionClass::SelectionRequired"));

        let class_s = between(
            derived,
            "async fn append_class_s_claim",
            "async fn dispatch_claim_legacy",
        );
        assert!(class_s.contains("AppendAdmissionClass::ClaimCoordinatorLive"));
        assert!(class_s.contains(".packed_append("));

        let finalize = between(
            derived,
            "async fn dispatch_finalize",
            "fn create_queue_impl",
        );
        assert!(finalize.contains("AppendAdmissionClass::Bypass"));
        assert!(finalize.contains("AppendAdmissionClass::SelectionRequired"));
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
            let (shard, commands, expected_epoch, fault, append_admission) =
                request.into_parts_with_append_admission();
            match append_admission {
                AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::NonDerived
                | AppendAdmissionClass::KeyedPermitLive
                | AppendAdmissionClass::SelectionRequired
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::RecoveryOnly
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
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
    commit_idempotency: TursoCommitIdempotency,
    claim_by_query_idempotency: TursoClaimByQueryIdempotency,
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
        .with_reclaim_planner(reclaim)
        .with_append_admission(AppendAdmissionClass::AtomicNative);

        let backend = Self {
            engine,
            log,
            projection,
            projection_path,
            control,
            ids,
            counters,
            node_id,
            commit_idempotency: TursoCommitIdempotency::default(),
            claim_by_query_idempotency: TursoClaimByQueryIdempotency::default(),
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

// ---------------------------------------------------------------------------
// Full Strict commit_transition surface for the Turso product families
// (bead fireweed-82211ac4; mirrors fireweed-objectlog's commit_surface)
// ---------------------------------------------------------------------------

/// In-process commit request-id cache shared by both Turso product families (parity with the
/// objectlog products' `commit_surface::CommitIdempotency`). The durable authority is the
/// `fireweed_request_idempotency` row the apply arm persists; this cache covers the window
/// before the derived composition's asynchronous apply lands and fast-paths replay.
type TursoCommitIdempotency =
    Arc<std::sync::Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<EntryRecovery>>>>>;

/// In-process claim_by_query request-id cache (parity with the objectlog products'
/// `port_surface::ClaimByQueryIdempotency`); the durable authority is the retained
/// `operation='claim_by_query'` idempotency row the apply arm persists.
type TursoClaimByQueryIdempotency =
    Arc<std::sync::Mutex<HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>>>;

/// What a permit-held claim_by_query submission resolved to (rendering happens post-permit,
/// after the projection has caught up to the committed claim).
enum TursoClaimByQueryOutcome {
    Replay {
        item_ids: Vec<ItemId>,
        lease_token: LeaseToken,
    },
    Committed {
        item_ids: Vec<ItemId>,
        lease_token: LeaseToken,
        request_id: RequestId,
        fingerprint: BodyHash,
        replay_expires_at: UtcTimestamp,
    },
}

fn record_turso_claim_by_query_idempotency(
    cache: &TursoClaimByQueryIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    item_ids: Vec<ItemId>,
    lease_token: LeaseToken,
    replay_expires_at: UtcTimestamp,
) {
    cache
        .lock()
        .expect("claim_by_query idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(
            request_id,
            fingerprint,
            (item_ids, lease_token),
            replay_expires_at,
        );
}

/// Result of planning a Turso `commit_transition` (before log append).
enum PreparedTursoCommitTransition {
    /// Replay prior outcomes (request_id hit, equal body).
    Replay(Vec<CommitEntryOutcome>),
    /// Fresh batch ready to append+apply; record idempotency after successful submit.
    Proceed {
        envelopes: Vec<CommandEnvelope>,
        recovery: Vec<EntryRecovery>,
        request_id: Option<RequestId>,
        fingerprint: BodyHash,
        retention_ms: u64,
    },
}

fn record_turso_commit_idempotency(
    commit_idempotency: &TursoCommitIdempotency,
    shard: &QueueKey,
    request_id: RequestId,
    fingerprint: BodyHash,
    recovery: Vec<EntryRecovery>,
    now: UtcTimestamp,
    retention_ms: u64,
) {
    commit_idempotency
        .lock()
        .expect("commit idempotency poisoned")
        .entry(shard.clone())
        .or_default()
        .record(
            request_id,
            fingerprint,
            recovery,
            request_expires_at(now, retention_ms),
        );
}

/// Plan a Turso `commit_transition`: per-entry lease/fence/index validation against the durable
/// projection, request-id replay/conflict resolution (in-process cache first, then the durable
/// retained row), and lowering of every accepted entry into one atomic command batch
/// (`WriteSideRecords` + `AdvanceInstanceFence` + lifecycle `Push` + `Finalize`, plus one
/// idempotency-marker envelope carrying `RequestOutcome::CommitTransition`). Direct port of
/// `fireweed-objectlog`'s `commit_surface::prepare_commit_transition` onto the Turso composition.
#[allow(clippy::too_many_arguments)]
async fn prepare_turso_commit_transition(
    projection: &TursoRelational,
    control: &InProcessControlPlane,
    ids: &SeqIdGen,
    counters: &QueueCounters,
    node_id: u8,
    commit_idempotency: &TursoCommitIdempotency,
    epoch: u64,
    shard: &QueueKey,
    transition: CommitTransition,
    now: UtcTimestamp,
) -> EngineResult<PreparedTursoCommitTransition> {
    let CommitTransition {
        request_id,
        entries,
    } = transition;
    let fingerprint = commit_body_hash(&entries)?;
    let definition = AsyncControlPlane::queue_definition(control, shard.clone()).await?;
    let max_attempts = definition.retry_policy.max_attempts;
    let retention_ms = definition.request_id_retention_ms;
    let schema = definition
        .entity_schema
        .as_ref()
        .and_then(|esd| esd.entity_schema.as_ref())
        .map(compile_entity_schema)
        .transpose()?;

    if let Some(rid) = &request_id {
        let cached = {
            let cache = commit_idempotency
                .lock()
                .expect("commit idempotency poisoned");
            cache.get(shard).map(|c| c.check(rid, fingerprint, now))
        };
        if let Some(decision) = cached {
            match decision {
                IdempotencyDecision::Replay(recovery) if recovery.len() == entries.len() => {
                    return Ok(PreparedTursoCommitTransition::Replay(
                        outcomes_from_recovery(&recovery),
                    ));
                }
                IdempotencyDecision::Conflict => {
                    return Err(EngineError::RequestIdConflict);
                }
                IdempotencyDecision::Replay(_)
                | IdempotencyDecision::Proceed
                | IdempotencyDecision::Expired => {}
            }
        }
        if let Some(entries) = AsyncProjectionStore::replay_durable_commit(
            projection,
            shard.clone(),
            rid.clone(),
            fingerprint.0,
            now,
        )
        .await?
        {
            let recovery = entries
                .into_iter()
                .map(fireweed_engine::recovery_from_outcome_entry)
                .collect::<Vec<_>>();
            record_turso_commit_idempotency(
                commit_idempotency,
                shard,
                rid.clone(),
                fingerprint,
                recovery.clone(),
                now,
                retention_ms,
            );
            return Ok(PreparedTursoCommitTransition::Replay(
                outcomes_from_recovery(&recovery),
            ));
        }
    }

    let commit_fingerprint = fingerprint.0;
    let requires_cross_entry_push_validation = definition.requires_cross_entry_push_validation();
    let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
    let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
    let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
    let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut staged_unique_keys: HashMap<(String, Vec<u8>), ItemId> = HashMap::new();

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

        if let Err(error) = validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..]) {
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
        if let Err(e) = AsyncProjectionStore::commit_validate(
            projection,
            shard.clone(),
            claim_refs.clone(),
            now,
        )
        .await
        {
            recovery.push(reject(e));
            continue;
        }
        if let Some(fence) = &instance_fence {
            let stored = match staged_fences.get(&fence.instance_key) {
                Some(v) => *v,
                None => AsyncProjectionStore::instance_fence(
                    projection,
                    shard.clone(),
                    fence.instance_key.clone(),
                )
                .await?
                .unwrap_or(0),
            };
            if let Err(e) = validate_instance_fence(stored, fence) {
                recovery.push(reject(e));
                continue;
            }
        }

        // fireweed-bf03cbf5: not retained; a pure function of the caller's own request.
        let side_record_keys: Vec<Vec<u8>> = Vec::new();
        let instance = instance_fence
            .as_ref()
            .map(|f| (f.instance_key.clone(), f.next));
        let mut envelopes: Vec<CommandEnvelope> = Vec::new();
        let mk_env = |command: QueueCommand, item_ids: Vec<ItemId>| CommandEnvelope {
            command_id: ids.next_command_id(),
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
        if !lifecycle_items.is_empty() {
            if let Some(e) = lifecycle_items
                .iter()
                .find_map(|item| validate_entity(schema.as_ref(), item.entity.as_ref()).err())
            {
                recovery.push(reject(e));
                continue;
            }
            let counter_base = counters.reserve(shard, epoch, lifecycle_items.len() as u32);
            let (push_items, push_ids) =
                build_push_items(lifecycle_items, epoch, node_id, counter_base, max_attempts);
            if let Err(e) = AsyncProjectionStore::index_validate_push(
                projection,
                shard.clone(),
                push_items.clone(),
            )
            .await
            {
                recovery.push(reject(e));
                continue;
            }
            if requires_cross_entry_push_validation
                && let Err(e) =
                    stage_unique_push_keys(&definition, &push_items, &mut staged_unique_keys)
            {
                recovery.push(reject(e));
                continue;
            }
            lifecycle_item_ids = push_ids.clone();
            envelopes.push(mk_env(
                QueueCommand::Push(PushCommand { items: push_items }),
                push_ids,
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

    let mut envelopes = committed_envelopes;
    if let Some(rid) = &request_id {
        let outcome_entries: Vec<_> = recovery.iter().map(outcome_entry_from_recovery).collect();
        envelopes.push(CommandEnvelope {
            command_id: ids.next_command_id(),
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

    Ok(PreparedTursoCommitTransition::Proceed {
        envelopes,
        recovery,
        request_id,
        fingerprint,
        retention_ms,
    })
}

/// Finish a prepared Turso commit: append+apply the batch via `commit` (which must already hold
/// the queue-local admission permit — `strategy.commit`, not `engine.submit_commit`), then record
/// request-id idempotency (parity with `commit_surface::finish_prepared_commit_transition`,
/// including the fireweed-5497780d prepare+append TOCTOU closure).
async fn finish_prepared_turso_commit_transition<Commit, CommitFut>(
    shard: &QueueKey,
    epoch: u64,
    prepared: PreparedTursoCommitTransition,
    commit_idempotency: &TursoCommitIdempotency,
    now: UtcTimestamp,
    commit: Commit,
) -> EngineResult<Vec<CommitEntryOutcome>>
where
    Commit: FnOnce(RawCommitRequest) -> CommitFut,
    CommitFut: std::future::Future<Output = EngineResult<RawCommitOutcome>>,
{
    match prepared {
        PreparedTursoCommitTransition::Replay(outcomes) => Ok(outcomes),
        PreparedTursoCommitTransition::Proceed {
            envelopes,
            recovery,
            request_id,
            fingerprint,
            retention_ms,
        } => {
            if !envelopes.is_empty() {
                commit(RawCommitRequest::new(shard.clone(), envelopes, epoch)).await?;
            }
            let outcomes = outcomes_from_recovery(&recovery);
            if let Some(rid) = request_id {
                record_turso_commit_idempotency(
                    commit_idempotency,
                    shard,
                    rid,
                    fingerprint,
                    recovery,
                    now,
                    retention_ms,
                );
            }
            Ok(outcomes)
        }
    }
}

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
            /// Full Strict commit surface (bead fireweed-82211ac4): per-entry lease/fence/index
            /// validation, side-record + instance-fence + lifecycle-push + finalize lowering into
            /// ONE atomic command batch, retained request-id idempotency, and per-entry
            /// Committed/Rejected outcomes — parity with the objectlog products'
            /// `commit_surface` path. Prepare and append+apply share the queue-local admission
            /// permit (fireweed-5497780d) so fence validation cannot race a concurrent commit.
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
                    let epoch = match expected_epoch {
                        Some(epoch) => {
                            let current =
                                AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone())
                                    .await?;
                            if current != epoch {
                                return Err(EngineError::EpochFenced);
                            }
                            epoch
                        }
                        None => {
                            AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?
                        }
                    };
                    let strategy = self.engine.commit_strategy();
                    let projection = Arc::clone(&self.projection);
                    let control = Arc::clone(&self.control);
                    let ids = Arc::clone(&self.ids);
                    let counters = Arc::clone(&self.counters);
                    let commit_idempotency = Arc::clone(&self.commit_idempotency);
                    let node_id = self.node_id;
                    let op_shard = shard.clone();
                    self.engine
                        .submit_operation(shard, move || {
                            Box::pin(async move {
                                let prepared = prepare_turso_commit_transition(
                                    projection.as_ref(),
                                    control.as_ref(),
                                    ids.as_ref(),
                                    counters.as_ref(),
                                    node_id,
                                    &commit_idempotency,
                                    epoch,
                                    &op_shard,
                                    transition,
                                    now,
                                )
                                .await?;
                                finish_prepared_turso_commit_transition(
                                    &op_shard,
                                    epoch,
                                    prepared,
                                    &commit_idempotency,
                                    now,
                                    |request| async move { strategy.commit(request).await },
                                )
                                .await
                            })
                        })
                        .await
                        .map_err(|error| map_submit("commit_transition", error))?
                }
            }
        }
        impl fireweed_engine::RecoveryReadPort for $ty {
            /// Reconstruct the committed transition addressed by `request_id` from the retained
            /// `fireweed_request_idempotency` row the Turso apply arm persists. The derived
            /// object-log composition applies asynchronously (`DurabilityClass::EventualApply`),
            /// so the projection is caught up to the log high-water first; the atomic
            /// compositions' `catch_up_projection` is a no-op.
            fn explain_commit(
                &self,
                shard: &QueueKey,
                request_id: RequestId,
            ) -> impl std::future::Future<
                Output = EngineResult<Option<fireweed_engine::CommitRecovery>>,
            > + Send {
                let shard = shard.clone();
                async move {
                    self.catch_up_projection(&shard).await?;
                    let durable = AsyncProjectionStore::read_durable_commit(
                        self.projection.as_ref(),
                        shard,
                        request_id.clone(),
                    )
                    .await?;
                    Ok(durable.map(|entries| fireweed_engine::CommitRecovery {
                        request_id,
                        entries: entries
                            .into_iter()
                            .map(fireweed_engine::recovery_from_outcome_entry)
                            .collect(),
                    }))
                }
            }

            fn side_record(
                &self,
                shard: &QueueKey,
                key: &[u8],
            ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
                let shard = shard.clone();
                let key = key.to_vec();
                async move {
                    self.catch_up_projection(&shard).await?;
                    AsyncProjectionStore::side_record(self.projection.as_ref(), shard, key).await
                }
            }

            fn side_records_by_prefix(
                &self,
                shard: &QueueKey,
                prefix: &[u8],
                page_size: usize,
                cursor: Option<Vec<u8>>,
            ) -> impl std::future::Future<Output = EngineResult<fireweed_engine::SideRecordPage>> + Send
            {
                let shard = shard.clone();
                let prefix = prefix.to_vec();
                async move {
                    self.catch_up_projection(&shard).await?;
                    AsyncProjectionStore::side_records_by_prefix(
                        self.projection.as_ref(),
                        shard,
                        prefix,
                        page_size,
                        cursor,
                    )
                    .await
                }
            }
        }
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
                    let needs_version_peek = request
                        .updates
                        .iter()
                        .any(|update| update.expected_item_version.is_some());
                    let snapshot = if self.pipeline_unresolved_updates() && !needs_version_peek
                    {
                        Vec::new()
                    } else {
                        self.planner_update_snapshot(&shard, &keys, &ids).await?
                    };

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
                        let append_admission = match $durability {
                            DurabilityClass::Atomic => AppendAdmissionClass::AtomicNative,
                            DurabilityClass::EventualApply => {
                                AppendAdmissionClass::SelectionRequired
                            }
                        };
                        let committed = strategy
                            .commit(
                                RawCommitRequest::new(shard, vec![envelope], epoch)
                                    .with_append_admission(append_admission),
                            )
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
            /// Full pending-key upsert (bead fireweed-82211ac4): plan against the caught-up
            /// projection (insert vs replace-if-pending vs collision), validate typed unique
            /// index keys pre-append on the insert path, and lower to one atomic
            /// `Push` / `ReplacePending` append (mirrors the objectlog products'
            /// `port_surface::prepare_upsert`).
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
                let shard = shard.clone();
                let client_item_key = client_item_key.clone();
                async move {
                    let definition = AsyncControlPlane::queue_definition(
                        self.control.as_ref(),
                        shard.clone(),
                    )
                    .await?;
                    let schema = definition
                        .entity_schema
                        .as_ref()
                        .and_then(|esd| esd.entity_schema.as_ref())
                        .map(compile_entity_schema)
                        .transpose()?;
                    validate_entity(schema.as_ref(), entity.as_ref())?;
                    let epoch = match expected_epoch {
                        Some(epoch) => epoch,
                        None => {
                            AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?
                        }
                    };
                    let counter_base = self.counters.reserve(&shard, epoch, 1);
                    let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
                    let item = PushItem {
                        client_item_key: client_item_key.clone(),
                        item_id: new_item_id,
                        priority,
                        not_before,
                        group_key,
                        max_attempts: definition.retry_policy.max_attempts,
                        payload,
                        fields,
                        metadata,
                        cohort_size: None,
                        gate_keys: Vec::new(),
                        index_fields: Default::default(),
                        entity_document: entity,
                    };
                    // The plan must observe applied state on the derived (EventualApply) family.
                    self.catch_up_projection(&shard).await?;
                    let existing = self
                        .projection
                        .lookup_active_by_key(&shard, &client_item_key)
                        .await?;
                    let (command, outcome) = match existing {
                        None => {
                            AsyncProjectionStore::index_validate_push(
                                self.projection.as_ref(),
                                shard.clone(),
                                vec![item.clone()],
                            )
                            .await?;
                            (
                                QueueCommand::Push(PushCommand { items: vec![item] }),
                                UpsertOutcome::Inserted {
                                    item_id: new_item_id,
                                },
                            )
                        }
                        Some(existing_id) => {
                            let state = AsyncProjectionStore::item_state(
                                self.projection.as_ref(),
                                shard.clone(),
                                existing_id,
                            )
                            .await?
                            .ok_or(EngineError::NotFound)?;
                            match state {
                                ItemState::Pending => (
                                    QueueCommand::ReplacePending(ReplacePendingCommand {
                                        client_item_key,
                                        superseded_item_id: existing_id,
                                        replacement: item,
                                    }),
                                    UpsertOutcome::Replaced {
                                        new_item_id,
                                        superseded_item_id: existing_id,
                                    },
                                ),
                                ItemState::Leased => {
                                    return Err(EngineError::Invalid(
                                        "collision with claimed item",
                                    ));
                                }
                                ItemState::Complete | ItemState::Failed => {
                                    return Err(EngineError::Terminal);
                                }
                            }
                        }
                    };
                    let envelope = CommandEnvelope {
                        command_id: self.ids.next_command_id(),
                        request_id: None,
                        request_fingerprint: None,
                        request_outcome: None,
                        item_ids: vec![new_item_id],
                        command,
                        checksum: CommandChecksum(0),
                        created_at: now,
                    };
                    let strategy = self.engine.commit_strategy();
                    let append_admission = match $durability {
                        DurabilityClass::Atomic => AppendAdmissionClass::AtomicNative,
                        DurabilityClass::EventualApply => AppendAdmissionClass::SelectionRequired,
                    };
                    strategy
                        .commit(
                            RawCommitRequest::new(shard, vec![envelope], epoch)
                                .with_append_admission(append_admission),
                        )
                        .await?;
                    Ok(outcome)
                }
            }
        }

        impl UpdateFieldsPort for $ty {
            /// FAC-1 live-item field/payload merge (bead fireweed-82211ac4): reserved-field and
            /// entity-schema validation, the projection's update guard (StaleLease / Terminal /
            /// Superseded / version Conflict / NotFound), then one atomic `UpdateFields` append;
            /// returns the bumped item_version (parity with `port_surface::prepare_update_fields`).
            fn update_fields(
                &self,
                shard: &QueueKey,
                item_id: ItemId,
                field_ops: BTreeMap<String, Option<Bytes>>,
                payload: fireweed_engine::PayloadUpdate,
                entity: Option<serde_json::Value>,
                expected_item_version: Option<u64>,
                now: UtcTimestamp,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
                let shard = shard.clone();
                async move {
                    validate_api001_reserved_write_fields(&field_ops)?;
                    let definition = AsyncControlPlane::queue_definition(
                        self.control.as_ref(),
                        shard.clone(),
                    )
                    .await?;
                    let schema = definition
                        .entity_schema
                        .as_ref()
                        .and_then(|esd| esd.entity_schema.as_ref())
                        .map(compile_entity_schema)
                        .transpose()?;
                    validate_entity(schema.as_ref(), entity.as_ref())?;
                    self.catch_up_projection(&shard).await?;
                    self.projection
                        .update_fields_validate(&shard, item_id, expected_item_version)
                        .await?;
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
                        item_ids: vec![item_id],
                        command: QueueCommand::UpdateFields(UpdateFieldsCommand {
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
                            client_item_key: None,
                            expected_item_version: None,
                        }),
                        checksum: CommandChecksum(0),
                        created_at: now,
                    };
                    let strategy = self.engine.commit_strategy();
                    let append_admission = match $durability {
                        DurabilityClass::Atomic => AppendAdmissionClass::AtomicNative,
                        DurabilityClass::EventualApply => AppendAdmissionClass::SelectionRequired,
                    };
                    strategy
                        .commit(
                            RawCommitRequest::new(shard.clone(), vec![envelope], epoch)
                                .with_append_admission(append_admission),
                        )
                        .await?;
                    self.catch_up_projection(&shard).await?;
                    AsyncProjectionStore::item_version(self.projection.as_ref(), shard, item_id)
                        .await?
                        .ok_or(EngineError::NotFound)
                }
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
                QueryCapabilityFlags {
                    claim_by_query: true,
                    ..QueryCapabilityFlags::default()
                }
            }

            /// API-004 claim-by-query (bead fireweed-82211ac4): request-id replay (in-process
            /// cache, then the durable retained row), declared-index selection via the shared
            /// in-memory planner over the materialized image, one atomic `Claim` append carrying
            /// the retained `RequestOutcome::ClaimByQuery` — selection and append under the
            /// queue-local admission permit; rendering after the projection catches up to the
            /// committed claim (the derived family applies asynchronously).
            fn claim_by_query(
                &self,
                shard: &QueueKey,
                request: fireweed_core::ClaimByQueryRequest,
                context: fireweed_engine::ClaimByQueryContext,
            ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
                let shard = shard.clone();
                async move {
                    let epoch = match context.expected_epoch {
                        Some(epoch) => epoch,
                        None => {
                            AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?
                        }
                    };
                    self.catch_up_projection(&shard).await?;
                    let strategy = self.engine.commit_strategy();
                    let append_admission = match $durability {
                        DurabilityClass::Atomic => AppendAdmissionClass::AtomicNative,
                        DurabilityClass::EventualApply => AppendAdmissionClass::SelectionRequired,
                    };
                    let projection = Arc::clone(&self.projection);
                    let control = Arc::clone(&self.control);
                    let ids = Arc::clone(&self.ids);
                    let cache = Arc::clone(&self.claim_by_query_idempotency);
                    let op_shard = shard.clone();
                    let outcome = self
                        .engine
                        .submit_operation(shard.clone(), move || {
                            Box::pin(async move {
                                let definition = AsyncControlPlane::queue_definition(
                                    control.as_ref(),
                                    op_shard.clone(),
                                )
                                .await?;
                                if request.max_items == 0
                                    || u64::from(request.max_items)
                                        > definition.max_claim_batch_size
                                {
                                    return Err(EngineError::Invalid(
                                        "invalid claim_by_query max_items",
                                    ));
                                }
                                if request.lease_duration_ms == 0
                                    || request.lease_duration_ms
                                        > definition.max_lease_duration_ms
                                {
                                    return Err(EngineError::Invalid(
                                        "invalid claim_by_query lease_duration_ms",
                                    ));
                                }
                                let request_id = request.request_id.clone().ok_or(
                                    EngineError::Invalid("claim_by_query request_id required"),
                                )?;
                                let fingerprint = claim_by_query_body_hash(&request)?;
                                let expires_at = request_expires_at(
                                    context.now,
                                    definition.request_id_retention_ms,
                                );
                                match cache
                                    .lock()
                                    .expect("claim_by_query idempotency poisoned")
                                    .entry(op_shard.clone())
                                    .or_default()
                                    .check_conflict_first(&request_id, fingerprint, context.now)
                                {
                                    IdempotencyDecision::Replay((item_ids, lease_token)) => {
                                        return Ok(TursoClaimByQueryOutcome::Replay {
                                            item_ids,
                                            lease_token,
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
                                if let Some((item_ids, lease_token)) = projection
                                    .replay_durable_claim_by_query(
                                        &op_shard,
                                        &request_id,
                                        &fingerprint.0.to_be_bytes(),
                                        context.now,
                                    )
                                    .await?
                                {
                                    return Ok(TursoClaimByQueryOutcome::Replay {
                                        item_ids,
                                        lease_token,
                                    });
                                }
                                let item_ids = projection
                                    .projection_data(&op_shard)
                                    .await?
                                    .select_claim_by_query(
                                        request.index.as_deref(),
                                        &request.filters,
                                        &request.order_by,
                                        request.max_items as usize,
                                        context.eligibility_at(),
                                    )?;
                                let lease_expires_at =
                                    context.lease_expires_at(request.lease_duration_ms);
                                let (lease_token, claim_item_ids) = if item_ids.is_empty() {
                                    (
                                        LeaseToken::new("empty-claim").expect("valid token"),
                                        Vec::new(),
                                    )
                                } else {
                                    (generate_query_lease_token()?, item_ids)
                                };
                                let envelope = CommandEnvelope {
                                    command_id: ids.next_command_id(),
                                    request_id: Some(request_id.clone()),
                                    request_fingerprint: Some(fingerprint.0),
                                    request_outcome: Some(RequestOutcome::ClaimByQuery {
                                        item_ids: claim_item_ids.clone(),
                                        lease_token: lease_token.clone(),
                                        worker_id: Some(request.worker_id.clone()),
                                    }),
                                    item_ids: claim_item_ids.clone(),
                                    command: QueueCommand::Claim(ClaimCommand {
                                        item_ids: claim_item_ids.clone(),
                                        lease_token: lease_token.clone(),
                                        lease_expires_at,
                                        worker_id: Some(request.worker_id.clone()),
                                    }),
                                    checksum: CommandChecksum(0),
                                    created_at: context.now,
                                };
                                strategy
                                    .commit(
                                        RawCommitRequest::new(
                                            op_shard.clone(),
                                            vec![envelope],
                                            epoch,
                                        )
                                        .with_append_admission(append_admission),
                                    )
                                    .await?;
                                let replay_expires_at = if claim_item_ids.is_empty() {
                                    expires_at
                                } else {
                                    expires_at.max(lease_expires_at)
                                };
                                Ok(TursoClaimByQueryOutcome::Committed {
                                    item_ids: claim_item_ids,
                                    lease_token,
                                    request_id,
                                    fingerprint,
                                    replay_expires_at,
                                })
                            })
                        })
                        .await
                        .map_err(|error| map_submit("claim_by_query", error))??;
                    self.catch_up_projection(&shard).await?;
                    match outcome {
                        TursoClaimByQueryOutcome::Replay {
                            item_ids,
                            lease_token,
                        } => {
                            if item_ids.is_empty() {
                                return Ok(Claimed::default());
                            }
                            let mut items = AsyncProjectionStore::render_claimed(
                                self.projection.as_ref(),
                                shard,
                                item_ids.clone(),
                            )
                            .await?;
                            if items.len() != item_ids.len()
                                || items.iter().any(|item| {
                                    item.lease_expires_at <= context.now
                                        || item
                                            .lease_token
                                            .as_ref()
                                            .is_some_and(|token| token != &lease_token)
                                })
                            {
                                return Err(EngineError::RequestExpired);
                            }
                            for item in &mut items {
                                item.lease_token = Some(lease_token.clone());
                            }
                            Ok(Claimed {
                                items,
                                ..Default::default()
                            })
                        }
                        TursoClaimByQueryOutcome::Committed {
                            item_ids,
                            lease_token,
                            request_id,
                            fingerprint,
                            replay_expires_at,
                        } => {
                            let items = if item_ids.is_empty() {
                                Vec::new()
                            } else {
                                let mut items = AsyncProjectionStore::render_claimed(
                                    self.projection.as_ref(),
                                    shard.clone(),
                                    item_ids.clone(),
                                )
                                .await?;
                                if items.len() != item_ids.len() {
                                    return Err(EngineError::Storage(
                                        "claim_by_query render lost committed leases".into(),
                                    ));
                                }
                                for item in &mut items {
                                    item.lease_token = Some(lease_token.clone());
                                }
                                items
                            };
                            record_turso_claim_by_query_idempotency(
                                &self.claim_by_query_idempotency,
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
                }
            }
        }

        impl IndexQueryPort for $ty {
            /// ADR-010 §6 typed-index reads over the durable `fireweed_item_index` rows the apply
            /// arm maintains (bead fireweed-82211ac4; mirrors the postgres relational lookups).
            fn index_get_unique(
                &self,
                shard: &QueueKey,
                index: &str,
                key: &[Vec<u8>],
            ) -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::IndexHit>>> + Send
            {
                let shard = shard.clone();
                let index = index.to_string();
                let key = key.to_vec();
                async move {
                    self.catch_up_projection(&shard).await?;
                    self.projection.index_get_unique(&shard, &index, &key).await
                }
            }
            fn index_lookup(
                &self,
                shard: &QueueKey,
                index: &str,
                key: &[Vec<u8>],
            ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send
            {
                let shard = shard.clone();
                let index = index.to_string();
                let key = key.to_vec();
                async move {
                    self.catch_up_projection(&shard).await?;
                    self.projection.index_lookup(&shard, &index, &key).await
                }
            }
        }

        impl ItemMutationPort for $ty {
            /// Backend-erased item mutation (bead fireweed-82211ac4): durable request-id replay
            /// from the retained `fireweed_request_idempotency` row, planning via the shared
            /// in-memory planner over the materialized image, then one atomic `MutateItems`
            /// append carrying the retained response — replay check, plan, and append all under
            /// the queue-local admission permit (parity with the objectlog products).
            fn mutate_items(
                &self,
                shard: &QueueKey,
                request: ItemMutationRequest,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<ItemMutationResponse>> + Send {
                let shard = shard.clone();
                async move {
                    let fingerprint = item_mutation_fingerprint(&request)?;
                    let request_id = request.request_id.clone();
                    let evaluated_at = request.evaluated_at;
                    self.catch_up_projection(&shard).await?;
                    let strategy = self.engine.commit_strategy();
                    let append_admission = match $durability {
                        DurabilityClass::Atomic => AppendAdmissionClass::AtomicNative,
                        DurabilityClass::EventualApply => AppendAdmissionClass::SelectionRequired,
                    };
                    let projection = Arc::clone(&self.projection);
                    let log = Arc::clone(&self.log);
                    let ids = Arc::clone(&self.ids);
                    let op_shard = shard.clone();
                    self.engine
                        .submit_operation(shard, move || {
                            Box::pin(async move {
                                if let Some(response) = projection
                                    .replay_durable_item_mutation(
                                        &op_shard,
                                        &request_id,
                                        fingerprint,
                                        evaluated_at,
                                    )
                                    .await?
                                {
                                    return Ok(response);
                                }
                                let mut plan =
                                    projection.plan_item_mutation(&op_shard, &request).await?;
                                if request.dry_run {
                                    return Ok(plan.response);
                                }
                                let epoch = resolve_write_epoch_async(expected_epoch, || {
                                    AsyncLogStore::current_epoch(log.as_ref(), op_shard.clone())
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
                                let envelope = CommandEnvelope {
                                    command_id: ids.next_command_id(),
                                    request_id: Some(request_id),
                                    request_fingerprint: Some(fingerprint),
                                    request_outcome: Some(RequestOutcome::ItemMutation {
                                        response_payload,
                                    }),
                                    item_ids,
                                    command: QueueCommand::MutateItems(plan.command),
                                    checksum: CommandChecksum(0),
                                    created_at: evaluated_at,
                                };
                                strategy
                                    .commit(
                                        RawCommitRequest::new(
                                            op_shard.clone(),
                                            vec![envelope],
                                            epoch,
                                        )
                                        .with_append_admission(append_admission),
                                    )
                                    .await?;
                                plan.response.position =
                                    AsyncLogStore::high_water(log.as_ref(), op_shard.clone())
                                        .await?;
                                Ok(plan.response)
                            })
                        })
                        .await
                        .map_err(|error| map_submit("mutate_items", error))?
                }
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
            let (shard, commands, expected_epoch, fault, append_admission) =
                request.into_parts_with_append_admission();
            match append_admission {
                AppendAdmissionClass::NonDerived
                | AppendAdmissionClass::KeyedPermitLive
                | AppendAdmissionClass::SelectionRequired
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::RecoveryOnly
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
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
                outcome.apply_published.notify();
                if let (Some(coordinator), Some(reservation)) = (&async_apply, reservation) {
                    coordinator.cancel(reservation).await;
                }
                return Ok(RawCommitOutcome::appended(outcome.positions));
            }
            let positions = publish_packed_apply(
                async_apply.as_ref(),
                reservation,
                outcome,
                projection.as_ref(),
                &shard,
                Some(&apply_turn),
            )
            .await?;
            Ok(if async_apply.is_some() {
                RawCommitOutcome::appended(positions)
            } else {
                RawCommitOutcome::applied(positions)
            })
        })
    }
}

fn position_covers(have: Option<&CommandPosition>, target: &CommandPosition) -> bool {
    have.is_some_and(|have| {
        have.backend_epoch > target.backend_epoch
            || (have.backend_epoch == target.backend_epoch && have.sequence >= target.sequence)
    })
}

#[cfg(feature = "objectlog")]
async fn publish_packed_apply(
    coordinator: Option<&AsyncProjectionApplyCoordinator<TursoRelational>>,
    reservation: Option<fireweed_objectlog::AsyncProjectionApplyReservation>,
    outcome: PackedAppendOutcome,
    projection: &TursoRelational,
    shard: &QueueKey,
    apply_turn: Option<&tokio::sync::Notify>,
) -> EngineResult<Vec<CommandPosition>> {
    let positions = outcome.positions.clone();
    if let Some(batch) = outcome.apply_batch {
        let result = if let (Some(coordinator), Some(reservation)) = (coordinator, reservation) {
            coordinator
                .enqueue_reserved(reservation, batch.positions, batch.commands)
                .await
        } else {
            if let Some(apply_turn) = apply_turn {
                wait_turso_apply_turn(projection, shard, &batch.positions, apply_turn).await?;
            }
            AsyncProjectionStore::apply_live(projection, batch.positions, batch.commands).await?;
            if let Some(apply_turn) = apply_turn {
                apply_turn.notify_waiters();
            }
            Ok(())
        };
        outcome.apply_published.notify();
        result?;
    } else {
        outcome.apply_published.wait().await;
        if let (Some(coordinator), Some(reservation)) = (coordinator, reservation) {
            coordinator.cancel(reservation).await;
        } else if let Some(apply_turn) = apply_turn {
            wait_turso_apply_turn(projection, shard, &positions, apply_turn).await?;
        }
    }
    Ok(positions)
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
    ProjectionPushPlanner<InProcessControlPlane, ObjectLogEngineStore, TursoRelational, SeqIdGen>,
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
    commit_idempotency: TursoCommitIdempotency,
    claim_by_query_idempotency: TursoClaimByQueryIdempotency,
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
        .with_reclaim_planner(reclaim)
        .with_append_admission(AppendAdmissionClass::KeyedPermitLive);

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
            commit_idempotency: TursoCommitIdempotency::default(),
            claim_by_query_idempotency: TursoClaimByQueryIdempotency::default(),
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
            let snap = coordinator.snapshot(shard).await;
            if position_covers(snap.applied_high_water.as_ref(), target) {
                return Ok(());
            }
            let projected =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), shard.clone())
                    .await?;
            if position_covers(projected.as_ref(), target) {
                return Ok(());
            }
            if snap.apply_queue_depth == 0 || !coordinator.has_ready(shard).await {
                // Reserved sibling claims are not produce work. The lease txn
                // uses the writer and sees committed SQL.
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
            let projected =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), shard.clone())
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
            let request = RawCommitRequest::new(shard.clone(), vec![envelope], epoch)
                .with_append_admission(AppendAdmissionClass::RecoveryOnly);
            let (append_shard, commands, append_epoch, fault, append_admission) =
                request.into_parts_with_append_admission();
            match append_admission {
                AppendAdmissionClass::RecoveryOnly
                | AppendAdmissionClass::NonDerived
                | AppendAdmissionClass::KeyedPermitLive
                | AppendAdmissionClass::SelectionRequired
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
            debug_assert_eq!(fault, RawCommitFault::None);
            self.log
                .packed_append(append_shard, commands, append_epoch)
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

    async fn commit_prepared(
        &self,
        request: RawCommitRequest,
        append_admission: AppendAdmissionClass,
    ) -> EngineResult<()> {
        use fireweed_engine::AsyncCommitStrategy;
        self.engine
            .commit_strategy()
            .commit(request.with_append_admission(append_admission))
            .await?;
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
                self.commit_prepared(request, AppendAdmissionClass::SelectionRequired)
                    .await?;
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
        let leased = self
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
            .await?;
        if leased.items.is_empty() {
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
        let reservation = match &self.async_apply {
            Some(coordinator) => Some(
                coordinator
                    .reserve(request.shard.clone(), std::slice::from_ref(&envelope))
                    .await?,
            ),
            None => None,
        };
        let committed = self
            .append_class_s_claim(
                RawCommitRequest::new(request.shard.clone(), vec![envelope], epoch)
                    .with_append_admission(AppendAdmissionClass::ClaimCoordinatorLive),
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
        request: RawCommitRequest,
        reservation: Option<fireweed_objectlog::AsyncProjectionApplyReservation>,
        _outbox_id: &str,
    ) -> EngineResult<()> {
        let (shard, commands, epoch, fault, append_admission) =
            request.into_parts_with_append_admission();
        match append_admission {
            AppendAdmissionClass::ClaimCoordinatorLive
            | AppendAdmissionClass::NonDerived
            | AppendAdmissionClass::KeyedPermitLive
            | AppendAdmissionClass::SelectionRequired
            | AppendAdmissionClass::Bypass
            | AppendAdmissionClass::AtomicNative
            | AppendAdmissionClass::RecoveryOnly => {}
        }
        match fault {
            RawCommitFault::None => {}
            RawCommitFault::BeforeAppend | RawCommitFault::AfterAppendBeforeApply => {
                return Err(EngineError::Invalid(
                    "fault injection is unavailable for Class-S claim append",
                ));
            }
        }
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
        publish_packed_apply(
            self.async_apply.as_ref(),
            reservation,
            outcome,
            self.projection.as_ref(),
            &shard,
            None,
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
                self.commit_prepared(commit, AppendAdmissionClass::SelectionRequired)
                    .await?;
                self.catch_up_projection(&request.shard).await?;
                // The default Class-S lane records this in-memory lease index
                // before returning its already-materialized response. Legacy
                // compatibility claims render from the projection after their
                // commit, so they must establish the same token mapping first.
                self.projection
                    .remember_leases(&request.shard, &item_ids, request.lease_token.clone())
                    .await;
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
        let append_admission = if outcomes.iter().all(|outcome| {
            matches!(
                outcome.kind,
                fireweed_engine::FinalizeKind::Complete | fireweed_engine::FinalizeKind::Fail
            )
        }) {
            AppendAdmissionClass::Bypass
        } else {
            AppendAdmissionClass::SelectionRequired
        };
        let PreparedFinalize { request, .. } = self
            .engine
            .prepare_finalize(shard.clone(), outcomes, now, expected_epoch)
            .await
            .map_err(map_lifecycle)?;
        self.commit_prepared(request, append_admission).await
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
