//! Generic async atomic log-replay product (program B).
//!
//! One composition implements the full product-port surface for any
//! [`LogStore`] × [`ProjectionStore`] pair behind [`InProcessLogStore`] /
//! [`InProcessProjectionStore`]. Adapter crates only open axes and call
//! [`assemble_async_log_replay`] / [`AsyncLogReplayBackend::recover`].

#![allow(
    clippy::manual_async_fn,
    reason = "port traits deliberately expose explicit Send future return types"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use fireweed_core::{
    BodyHash, BoundedMutationRequest, BoundedMutationResponse, ClaimByItemIdClass,
    ClaimByItemIdsDisposition, ClaimByItemIdsOutcome, ClaimByItemIdsRequest, ClaimByQueryRequest,
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, MetricsByQueryRequest,
    PriorityValue, QueryCapabilityFlags, QueueDefinition, QueueId, RangeScanRequest, RequestId,
    TenantId, UtcTimestamp,
};

use crate::{
    AsOfProjectionStore, AsyncClaimError, AsyncCommitStrategy, AsyncComposedBackend,
    AsyncControlPlane, AsyncLifecycleError, AsyncLogStore, AsyncProjectionStore, AsyncPurgeRequest,
    AsyncPushError, AsyncPushRequest, AsyncReclaimRequest, Backend, BatchUpdatePort,
    BatchUpdateRequest, BatchUpdateResponse, BoundedMutationContext, ClaimByItemIdsResponse,
    ClaimByQueryContext, ClaimCommand, ClaimPort, ClaimRef, ClaimRequest, Claimed, CommandChecksum,
    CommandEnvelope, CommandId, CommandPage, CommandPosition, CommitEntryStatus,
    CommitOutcomeEntry, CommitTransitionEntry, ComposeFaultHook, ComposeFaultPoint, ControlPlane,
    ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError, EngineResult,
    EntryRecovery, FinalizeCommand, FinalizeKind, FinalizeOutcome, FinalizePort,
    HotProjectionQueryPort, IdGen, IdempotencyDecision, InProcessControlPlane, InProcessLogStore,
    InProcessProjectionStore, IndexHit, IndexQueryPort, InlineOwnedTaskDispatcher, ItemView,
    LeaseView, LiveItemView, LogRead, LogStore, OwnedTask, PayloadUpdate, PendingPage,
    PendingSummary, ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner,
    ProjectionRead, ProjectionReclaimPlanner, ProjectionSnapshot, ProjectionStore, PurgePort,
    PushCommand, PushItem, PushPort, PushSpec, QueueCommand, QueueCounters, QueueIdempotencyCache,
    QueueKey, QueueMetrics, RawCommitFault, RawCommitOutcome, RawCommitRequest,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort,
    ReplacePendingCommand, RequestIdReplayProbe, RequestOutcome, SnapshotRef, SnapshotStore,
    TerminalEmissionMetrics, TickReport, UnifiedAtomicCommit, UnifiedAtomicCommitter,
    UpdateFieldsCommand, UpdateFieldsPort, UpsertOutcome, UpsertPort, WriteSideRecordsCommand,
    batch_update_body_hash, build_push_items, claim_by_item_ids_body_hash,
    claim_by_query_body_hash, commit_body_hash, compile_entity_schema, generate_query_lease_token,
    outcome_entry_from_recovery, plan_batch_update, stage_unique_push_keys,
    validate_api001_reserved_write_fields, validate_entity, validate_gate_push,
};

/// Resolve the push request-id body fingerprint for ledger record/rebuild.
///
/// Prefer the durable envelope field (written at plan time from the caller's [`PushSpec`]s). When
/// legacy envelopes omit it, recompute from the committed [`PushItem`]s so same-body Replayed and
/// changed-body RequestIdConflict survive recovery-on-open (fireweed-6486ed63).
fn push_envelope_body_hash(env: &CommandEnvelope) -> EngineResult<BodyHash> {
    if let Some(fp) = env.request_fingerprint {
        return Ok(BodyHash(fp));
    }
    match &env.command {
        QueueCommand::Push(PushCommand { items }) => crate::compose::push_item_body_hash(items),
        _ => Ok(BodyHash(0)),
    }
}

/// Sequential id generation for async log-replay products.
#[derive(Default)]
pub struct SeqIdGen {
    counter: AtomicU64,
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        ItemId::from_u64(n)
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

/// Shared push request-id cache (parity with sync `ComposedBackend` in-memory idempotency).
type PushIdempotency = Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>>;
/// Shared test-only hook slot for the two composed projection-apply crash instants.
type ComposeFaultHookSlot = Arc<Mutex<Option<Arc<dyn ComposeFaultHook>>>>;

async fn apply_with_compose_fault_hook<A, F>(
    fault_hook: Option<&Arc<dyn ComposeFaultHook>>,
    apply: A,
) -> EngineResult<()>
where
    A: FnOnce() -> F,
    F: std::future::Future<Output = EngineResult<()>> + Send,
{
    if let Some(hook) = fault_hook {
        hook.fault_point(ComposeFaultPoint::DuringProjectionApply)?;
    }
    apply().await?;
    if let Some(hook) = fault_hook {
        hook.fault_point(ComposeFaultPoint::AfterApplyBeforeResponse)?;
    }
    Ok(())
}

/// Append + apply under the composition's queue gate (atomic memory profile).
#[derive(Clone)]
pub struct AtomicLogReplayCommitter<L, P> {
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    push_idempotency: Arc<PushIdempotency>,
    fault_hook: ComposeFaultHookSlot,
}

impl<L, P> AtomicLogReplayCommitter<L, P> {
    fn new(
        log: Arc<InProcessLogStore<L>>,
        projection: Arc<InProcessProjectionStore<P>>,
        control: Arc<InProcessControlPlane>,
        push_idempotency: Arc<PushIdempotency>,
        fault_hook: ComposeFaultHookSlot,
    ) -> Self {
        Self {
            log,
            projection,
            control,
            push_idempotency,
            fault_hook,
        }
    }
}

impl<L, P> UnifiedAtomicCommitter for AtomicLogReplayCommitter<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    type Request = RawCommitRequest;
    type Output = EngineResult<RawCommitOutcome>;

    fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let push_idempotency = Arc::clone(&self.push_idempotency);
        let fault_hook = Arc::clone(&self.fault_hook);
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
                crate::validate_gate_command_definition(&definition, &env.command)?;
            }
            let fault_hook = fault_hook
                .lock()
                .expect("compose fault hook poisoned")
                .clone();
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
            apply_with_compose_fault_hook(fault_hook.as_ref(), || {
                AsyncProjectionStore::apply_live(
                    projection.as_ref(),
                    positions.clone(),
                    commands.clone(),
                )
            })
            .await?;
            // Record push request-id outcomes after a successful atomic apply (sync composition parity).
            // Expiry must honor the queue's request_id_retention_ms (not a hardcoded window).
            let retention_ms = definition.request_id_retention_ms;
            let mut cache = push_idempotency
                .lock()
                .expect("push idempotency mutex poisoned");
            for env in &commands {
                let Some(request_id) = env.request_id.clone() else {
                    continue;
                };
                let QueueCommand::Push(_) = &env.command else {
                    continue;
                };
                let fingerprint = push_envelope_body_hash(env)?;
                let expires_at = crate::request_expires_at(env.created_at, retention_ms);
                let ids = match &env.request_outcome {
                    Some(crate::RequestOutcome::Push { item_ids }) => item_ids.clone(),
                    _ => env.item_ids.clone(),
                };
                cache.entry(shard.clone()).or_default().record(
                    request_id,
                    fingerprint,
                    ids,
                    expires_at,
                );
            }
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

/// Projection axis with in-process push request-id cache (AsyncProjectionStore).
#[derive(Clone)]
struct PushIdempotentProjection<P> {
    inner: Arc<InProcessProjectionStore<P>>,
    push_idempotency: Arc<PushIdempotency>,
}

impl<P> AsyncProjectionStore for PushIdempotentProjection<P>
where
    P: ProjectionStore + Send + 'static,
{
    fn supports_gates(&self) -> bool {
        self.inner.supports_gates()
    }

    fn ensure_shard(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::ensure_shard(self.inner.as_ref(), definition)
    }

    fn admit_mutation(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::admit_mutation(self.inner.as_ref(), shard)
    }

    fn validate_push(
        &self,
        shard: QueueKey,
        items: Vec<PushItem>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::validate_push(self.inner.as_ref(), shard, items, now)
    }

    fn pause_blocks_intake(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<bool>> + Send {
        AsyncProjectionStore::pause_blocks_intake(self.inner.as_ref(), shard)
    }

    fn push_idempotency(
        &self,
        shard: QueueKey,
        request_id: RequestId,
        fingerprint: crate::PushFingerprint,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
    {
        let cache = Arc::clone(&self.push_idempotency);
        std::future::ready({
            let guard = cache.lock().expect("push idempotency mutex poisoned");
            Ok(guard
                .get(&shard)
                .map(|c| c.check(&request_id, fingerprint.legacy_body_hash, now))
                .unwrap_or(IdempotencyDecision::Proceed))
        })
    }

    fn renew_validate(
        &self,
        shard: QueueKey,
        targets: Vec<crate::RenewTarget>,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::renew_validate(self.inner.as_ref(), shard, targets, now)
    }

    fn finalize_validate(
        &self,
        shard: QueueKey,
        targets: Vec<crate::FinalizeTarget>,
        now: UtcTimestamp,
        default_max_attempts: u32,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::FinalizeLeaseMember>>> + Send
    {
        AsyncProjectionStore::finalize_validate(
            self.inner.as_ref(),
            shard,
            targets,
            now,
            default_max_attempts,
        )
    }

    fn purge_validate(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
        force: bool,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::purge_validate(self.inner.as_ref(), shard, ids, force)
    }

    fn expired_leases(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::expired_leases(self.inner.as_ref(), shard, now, max)
    }

    fn apply_live(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::apply_live(self.inner.as_ref(), positions, commands)
    }

    fn apply_recovery(
        &self,
        positions: Vec<CommandPosition>,
        commands: Vec<CommandEnvelope>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        AsyncProjectionStore::apply_recovery(self.inner.as_ref(), positions, commands)
    }

    fn eligible_candidates(
        &self,
        shard: QueueKey,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::eligible_candidates(self.inner.as_ref(), shard, now, max)
    }

    fn select_item_claim(
        &self,
        shard: QueueKey,
        compatibility: crate::ClaimCompatibility,
        now: UtcTimestamp,
        max: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        AsyncProjectionStore::select_item_claim(self.inner.as_ref(), shard, compatibility, now, max)
    }

    fn select_rich_claim(
        &self,
        shard: QueueKey,
        unit: crate::ClaimUnit,
        compatibility: crate::ClaimCompatibility,
        now: UtcTimestamp,
        max_items: usize,
    ) -> impl std::future::Future<Output = EngineResult<crate::RichClaimSelection>> + Send {
        AsyncProjectionStore::select_rich_claim(
            self.inner.as_ref(),
            shard,
            unit,
            compatibility,
            now,
            max_items,
        )
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ClaimedItem>>> + Send {
        AsyncProjectionStore::render_claimed(self.inner.as_ref(), shard, ids)
    }

    fn resolve_lease_targets(
        &self,
        shard: QueueKey,
        ids: Vec<ItemId>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ClaimedItem>>> + Send {
        // Forward so InProcessProjectionStore can apply renew_validate precedence
        // (absent → NotFound, not leased → structured error) instead of the default
        // render_claimed count-mismatch → StaleLease path (API-001 late finalize after purge).
        AsyncProjectionStore::resolve_lease_targets(self.inner.as_ref(), shard, ids)
    }

    fn item_state(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send {
        AsyncProjectionStore::item_state(self.inner.as_ref(), shard, id)
    }

    fn item_version(
        &self,
        shard: QueueKey,
        id: ItemId,
    ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
        AsyncProjectionStore::item_version(self.inner.as_ref(), shard, id)
    }

    fn recovery_high_water(
        &self,
        shard: QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        AsyncProjectionStore::recovery_high_water(self.inner.as_ref(), shard)
    }

    fn recover_definitions(
        &self,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
        AsyncProjectionStore::recover_definitions(self.inner.as_ref())
    }
}

type Strategy<L, P> = UnifiedAtomicCommit<AtomicLogReplayCommitter<L, P>>;
type ClaimPlanner<L, P> = ProjectionClaimPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type PushPlanner<L, P> = ProjectionPushPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type LifecyclePlanner<L, P> = ProjectionLifecyclePlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type ReclaimPlanner<L, P> = ProjectionReclaimPlanner<
    InProcessControlPlane,
    InProcessLogStore<L>,
    PushIdempotentProjection<P>,
    SeqIdGen,
>;
type AsyncEngine<L, P> = AsyncComposedBackend<
    Strategy<L, P>,
    InlineOwnedTaskDispatcher,
    ClaimPlanner<L, P>,
    PushPlanner<L, P>,
    LifecyclePlanner<L, P>,
    ReclaimPlanner<L, P>,
>;

/// Claim-by-query request-id cache: `(item_ids, lease_token)` while leases remain replayable.
type ClaimByQueryIdempotency =
    Mutex<HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken)>>>;
/// Claim-by-item-ids request-id cache: claimed ids + token + per-id outcomes while leases remain replayable.
type ClaimByItemIdsIdempotency = Mutex<
    HashMap<QueueKey, QueueIdempotencyCache<(Vec<ItemId>, LeaseToken, Vec<ClaimByItemIdsOutcome>)>>,
>;
/// BatchUpdate request-id cache (API-001 ordered outcomes).
type BatchUpdateIdempotency = Mutex<HashMap<QueueKey, QueueIdempotencyCache<BatchUpdateResponse>>>;

/// Atomic async log-replay product: full product ports over any log × projection axes.
pub struct AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    engine: AsyncEngine<L, P>,
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    node_id: u8,
    push_idempotency: Arc<PushIdempotency>,
    claim_by_query_idempotency: Arc<ClaimByQueryIdempotency>,
    claim_by_item_ids_idempotency: Arc<ClaimByItemIdsIdempotency>,
    batch_update_idempotency: Arc<BatchUpdateIdempotency>,
    commit_idempotency:
        Arc<Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<crate::EntryRecovery>>>>>,
    fault_hook: ComposeFaultHookSlot,
}

/// Assemble a fresh async log-replay product over the given axes (no recovery).
///
/// Axes use in-process ready futures (CPU-only / memory). Prefer
/// [`assemble_async_log_replay_with_axis_offload`] for durable blocking stores (sqlite).
pub fn assemble_async_log_replay<L, P>(
    log: L,
    projection: P,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    assemble_async_log_replay_with_axis_offload(log, projection, node_id, false, false)
}

/// Assemble log-replay with optional adapter-local blocking offload per axis.
///
/// When `offload_log` / `offload_projection` is true, that axis runs whole operations on a private
/// [`crate::BoundedBlockingExecutor`] (not process-wide `BlockingLibBackend`). Use for rusqlite
/// product cells so public Fireweed ports are non-blocking-under-poll.
pub fn assemble_async_log_replay_with_axis_offload<L, P>(
    log: L,
    projection: P,
    node_id: u8,
    offload_log: bool,
    offload_projection: bool,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    use crate::{DEFAULT_BLOCKING_AXIS_IN_FLIGHT, InProcessLogStore, InProcessProjectionStore};

    let log = Arc::new(if offload_log {
        InProcessLogStore::new_with_blocking_offload(log, DEFAULT_BLOCKING_AXIS_IN_FLIGHT)?
    } else {
        InProcessLogStore::new(log)
    });
    let projection = Arc::new(if offload_projection {
        InProcessProjectionStore::new_with_blocking_offload(
            projection,
            DEFAULT_BLOCKING_AXIS_IN_FLIGHT,
        )?
    } else {
        InProcessProjectionStore::new(projection)
    });
    let push_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let claim_by_query_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let claim_by_item_ids_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let batch_update_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let commit_idempotency = Arc::new(Mutex::new(HashMap::new()));
    let control = Arc::new(InProcessControlPlane::new());
    let ids = Arc::new(SeqIdGen::default());
    let counters = Arc::new(QueueCounters::default());
    assemble_async_log_replay_from_parts(
        log,
        projection,
        control,
        ids,
        counters,
        push_idempotency,
        claim_by_query_idempotency,
        claim_by_item_ids_idempotency,
        batch_update_idempotency,
        commit_idempotency,
        node_id,
    )
}

/// Assemble from already-shared axis handles (used by [`AsyncLogReplayBackend::with_node_id`]).
#[allow(
    clippy::too_many_arguments,
    reason = "assembly keeps each shared axis and idempotency cache explicit"
)]
pub fn assemble_async_log_replay_from_parts<L, P>(
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    push_idempotency: Arc<PushIdempotency>,
    claim_by_query_idempotency: Arc<ClaimByQueryIdempotency>,
    claim_by_item_ids_idempotency: Arc<ClaimByItemIdsIdempotency>,
    batch_update_idempotency: Arc<BatchUpdateIdempotency>,
    commit_idempotency: Arc<
        Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<crate::EntryRecovery>>>>,
    >,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    assemble_async_log_replay_from_parts_with_fault_hook(
        log,
        projection,
        control,
        ids,
        counters,
        push_idempotency,
        claim_by_query_idempotency,
        claim_by_item_ids_idempotency,
        batch_update_idempotency,
        commit_idempotency,
        Arc::new(Mutex::new(None)),
        node_id,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "assembly keeps each shared axis, cache, and test-only hook explicit"
)]
fn assemble_async_log_replay_from_parts_with_fault_hook<L, P>(
    log: Arc<InProcessLogStore<L>>,
    projection: Arc<InProcessProjectionStore<P>>,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    counters: Arc<QueueCounters>,
    push_idempotency: Arc<PushIdempotency>,
    claim_by_query_idempotency: Arc<ClaimByQueryIdempotency>,
    claim_by_item_ids_idempotency: Arc<ClaimByItemIdsIdempotency>,
    batch_update_idempotency: Arc<BatchUpdateIdempotency>,
    commit_idempotency: Arc<
        Mutex<HashMap<QueueKey, QueueIdempotencyCache<Vec<crate::EntryRecovery>>>>,
    >,
    fault_hook: ComposeFaultHookSlot,
    node_id: u8,
) -> EngineResult<AsyncLogReplayBackend<L, P>>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    let axis = Arc::new(PushIdempotentProjection {
        inner: Arc::clone(&projection),
        push_idempotency: Arc::clone(&push_idempotency),
    });
    let committer = AtomicLogReplayCommitter::new(
        Arc::clone(&log),
        Arc::clone(&projection),
        Arc::clone(&control),
        Arc::clone(&push_idempotency),
        Arc::clone(&fault_hook),
    );
    let strategy = UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer)
        .map_err(|e| EngineError::Storage(e.to_string()))?;
    let claim = ProjectionClaimPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
    );
    let push = ProjectionPushPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
        Arc::clone(&counters),
        node_id,
    );
    let lifecycle = ProjectionLifecyclePlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
        Arc::clone(&ids),
    );
    let reclaim = ProjectionReclaimPlanner::from_shared(
        Arc::clone(&control),
        Arc::clone(&log),
        Arc::clone(&axis),
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
    Ok(AsyncLogReplayBackend {
        engine,
        log,
        projection,
        control,
        ids,
        counters,
        node_id,
        push_idempotency,
        claim_by_query_idempotency,
        claim_by_item_ids_idempotency,
        batch_update_idempotency,
        commit_idempotency,
        fault_hook,
    })
}

impl<L, P> AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    /// Rebuild planners so push minting uses `node_id` (preserves log + projection state).
    pub fn with_node_id(self, node_id: u8) -> Self {
        assemble_async_log_replay_from_parts_with_fault_hook(
            self.log,
            self.projection,
            self.control,
            self.ids,
            self.counters,
            self.push_idempotency,
            self.claim_by_query_idempotency,
            self.claim_by_item_ids_idempotency,
            self.batch_update_idempotency,
            self.commit_idempotency,
            self.fault_hook,
            node_id,
        )
        .expect("rebuild with_node_id")
    }

    /// Install or clear the test-only composed projection-apply fault hook.
    ///
    /// Production assembly leaves this slot empty. A configured hook is shared with the atomic committer
    /// and survives [`Self::with_node_id`] rebuilds.
    pub fn set_fault_hook(&self, hook: Option<Arc<dyn ComposeFaultHook>>) {
        *self.fault_hook.lock().expect("compose fault hook poisoned") = hook;
    }

    /// Observability/test seam: run `f` against the log under its mutex.
    pub fn with_log<R>(&self, f: impl FnOnce(&L) -> R) -> R {
        self.log.with_store(f)
    }

    /// Observability/test seam: run `f` against the projection under its mutex.
    pub fn with_projection<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        self.projection.with_store(f)
    }

    /// Emit durable change-record tail from the log emission cursor (parity with sync composition).
    pub fn emit_change_record_tail<S: crate::ChangeRecordSink + ?Sized>(
        &self,
        shard: &QueueKey,
        sink: &S,
        limit: usize,
        emitted_at: UtcTimestamp,
        source_owner_id: Option<fireweed_core::OwnerId>,
    ) -> EngineResult<usize> {
        use crate::{LogStore, command_envelope_change_records};

        let cursor = self
            .log
            .with_store(|log| LogStore::emission_cursor(log, shard))?;
        let page = self
            .log
            .with_store(|log| LogStore::read_from(log, shard, cursor, limit))?;
        if page.entries.is_empty() {
            return Ok(0);
        }
        let mut records = Vec::new();
        for (position, env) in &page.entries {
            records.extend(command_envelope_change_records(
                shard,
                position,
                env,
                emitted_at,
                source_owner_id.clone(),
            ));
        }
        sink.emit(shard, &records)?;
        if let Some((position, _)) = page.entries.last() {
            let position = position.clone();
            self.log
                .with_store_mut(|log| LogStore::set_emission_cursor(log, shard, position))?;
        }
        Ok(records.len())
    }

    /// Reap terminal projection rows only after their retention and emission
    /// barriers are both satisfied.
    ///
    /// This synchronous orchestration seam is retained for maintenance callers
    /// over in-process axes. It does not activate reaping from [`ReclaimDriver::tick`].
    pub fn reap_terminal_items(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        terminal_retention_ms: u64,
        emit_change_records: bool,
    ) -> EngineResult<usize> {
        use crate::{ControlPlane, LogStore};

        ControlPlane::queue_definition(self.control.as_ref(), shard)?;
        if !self
            .projection
            .with_store(|projection| ProjectionStore::retention_may_advance(projection, shard))
        {
            return Ok(0);
        }

        let emission_cursor = if emit_change_records {
            self.log
                .with_store(|log| LogStore::emission_cursor(log, shard))?
        } else {
            None
        };
        if emit_change_records && emission_cursor.is_none() {
            return Ok(0);
        }

        self.projection.with_store_mut(|projection| {
            ProjectionStore::reap_terminal_items(
                projection,
                shard,
                now,
                terminal_retention_ms,
                emit_change_records,
                emission_cursor.as_ref(),
            )
            .map(|ids| ids.len())
        })
    }

    /// Replay the durable log into the in-memory projection and control plane (ADR-012 recovery-on-open).
    ///
    /// When the log has no durable catalog (Class B memory log) but the projection does, queue
    /// definitions are recovered from the projection so reopen can serve claim/metrics without a
    /// re-`create_queue` (memory×sqlite / memory×postgres Class B durable projection cells).
    pub fn recover(self) -> EngineResult<Self> {
        use crate::{ControlPlane, LogStore, RequestOutcome, request_expires_at};

        let mut definitions = self
            .log
            .with_store(|log| LogStore::recover_definitions(log))?;
        let projection_owns_catalog = definitions.is_empty();
        if projection_owns_catalog {
            definitions = self
                .projection
                .with_store(ProjectionStore::recover_definitions)?;
        }
        for definition in definitions {
            let retention_ms = definition.request_id_retention_ms;
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            self.log
                .with_store_mut(|log| LogStore::ensure_shard(log, &shard))?;
            let projection_high_water = self.projection.with_store(|projection| {
                if projection.recovery_backpressured(&shard) {
                    Ok(None)
                } else {
                    ProjectionStore::recovery_high_water(projection, &shard)
                }
            })?;
            if projection_owns_catalog && let Some(position) = projection_high_water.clone() {
                self.log
                    .with_store_mut(|log| LogStore::set_high_water(log, &shard, position))?;
            }
            let _ = ControlPlane::create_queue(self.control.as_ref(), definition.clone());
            self.projection
                .with_store_mut(|p| ProjectionStore::ensure_shard(p, &definition))?;
            let mut from = None;
            loop {
                let page = self
                    .log
                    .with_store(|log| LogStore::read_from(log, &shard, from.clone(), 256))?;
                if page.entries.is_empty() {
                    break;
                }
                let tail = page.entries.iter().filter(|(position, _)| {
                    projection_high_water.as_ref().is_none_or(|high_water| {
                        position.backend_epoch > high_water.backend_epoch
                            || (position.backend_epoch == high_water.backend_epoch
                                && position.sequence > high_water.sequence)
                    })
                });
                let (positions, commands): (Vec<_>, Vec<_>) = tail
                    .map(|(position, envelope)| (position.clone(), envelope.clone()))
                    .unzip();
                self.projection.with_store_mut(|projection| {
                    if !positions.is_empty() {
                        ProjectionStore::apply_recovery(projection, &positions, &commands)?;
                    }
                    let all_commands = page
                        .entries
                        .iter()
                        .map(|(_, envelope)| envelope.clone())
                        .collect::<Vec<_>>();
                    ProjectionStore::restore_process_state(projection, &shard, &all_commands)
                })?;
                // Rebuild request-id caches from durable envelopes (push, claim-by-query,
                // batch-update, commit-transition).
                {
                    let mut push_cache = self
                        .push_idempotency
                        .lock()
                        .expect("push idempotency poisoned");
                    let mut claim_cache = self
                        .claim_by_query_idempotency
                        .lock()
                        .expect("claim_by_query idempotency poisoned");
                    let mut claim_by_item_ids_cache = self
                        .claim_by_item_ids_idempotency
                        .lock()
                        .expect("claim_by_item_ids idempotency poisoned");
                    let mut batch_cache = self
                        .batch_update_idempotency
                        .lock()
                        .expect("batch_update idempotency poisoned");
                    for (_, env) in &page.entries {
                        // Renew extends active query/item-id claim replay retention.
                        if let QueueCommand::RenewLease(renew) = &env.command {
                            let renewed: HashSet<ItemId> = renew.item_ids.iter().copied().collect();
                            claim_cache
                                .entry(shard.clone())
                                .or_default()
                                .extend_expiry_matching(renew.lease_expires_at, |(item_ids, _)| {
                                    !item_ids.is_empty()
                                        && item_ids.iter().all(|item_id| renewed.contains(item_id))
                                });
                            claim_by_item_ids_cache
                                .entry(shard.clone())
                                .or_default()
                                .extend_expiry_matching(
                                    renew.lease_expires_at,
                                    |(item_ids, _, _)| {
                                        !item_ids.is_empty()
                                            && item_ids
                                                .iter()
                                                .all(|item_id| renewed.contains(item_id))
                                    },
                                );
                        }

                        let Some(request_id) = &env.request_id else {
                            continue;
                        };
                        if let QueueCommand::Push(_) = &env.command {
                            let fingerprint = push_envelope_body_hash(env)?;
                            let expires_at = request_expires_at(env.created_at, retention_ms);
                            let ids = match &env.request_outcome {
                                Some(RequestOutcome::Push { item_ids }) => item_ids.clone(),
                                _ => env.item_ids.clone(),
                            };
                            push_cache.entry(shard.clone()).or_default().record(
                                request_id.clone(),
                                fingerprint,
                                ids,
                                expires_at,
                            );
                        }
                        if let Some(RequestOutcome::ClaimByQuery {
                            item_ids,
                            lease_token,
                            ..
                        }) = &env.request_outcome
                        {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at = match (&env.command, item_ids.is_empty()) {
                                (QueueCommand::Claim(claim), false) => {
                                    request_expires_at(env.created_at, retention_ms)
                                        .max(claim.lease_expires_at)
                                }
                                _ => request_expires_at(env.created_at, retention_ms),
                            };
                            claim_cache.entry(shard.clone()).or_default().record(
                                request_id.clone(),
                                fingerprint,
                                (item_ids.clone(), lease_token.clone()),
                                expires_at,
                            );
                        }
                        if let Some(RequestOutcome::ClaimByItemIds {
                            claimed_item_ids,
                            lease_token,
                            outcomes,
                            ..
                        }) = &env.request_outcome
                        {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at = match (&env.command, claimed_item_ids.is_empty()) {
                                (QueueCommand::Claim(claim), false) => {
                                    request_expires_at(env.created_at, retention_ms)
                                        .max(claim.lease_expires_at)
                                }
                                _ => request_expires_at(env.created_at, retention_ms),
                            };
                            claim_by_item_ids_cache
                                .entry(shard.clone())
                                .or_default()
                                .record(
                                    request_id.clone(),
                                    fingerprint,
                                    (
                                        claimed_item_ids.clone(),
                                        lease_token.clone(),
                                        outcomes.clone(),
                                    ),
                                    expires_at,
                                );
                        }
                        if let Some(RequestOutcome::BatchUpdate { response_payload }) =
                            &env.request_outcome
                        {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at = request_expires_at(env.created_at, retention_ms);
                            let response: BatchUpdateResponse =
                                serde_json::from_str(response_payload)
                                    .map_err(|e| EngineError::Storage(e.to_string()))?;
                            batch_cache.entry(shard.clone()).or_default().record(
                                request_id.clone(),
                                fingerprint,
                                response,
                                expires_at,
                            );
                        }
                        if let Some(RequestOutcome::CommitTransition { entries }) =
                            &env.request_outcome
                        {
                            let fingerprint = BodyHash(env.request_fingerprint.unwrap_or(0));
                            let expires_at = request_expires_at(env.created_at, retention_ms);
                            let recovery = entries
                                .iter()
                                .cloned()
                                .map(crate::recovery_from_outcome_entry)
                                .collect::<Vec<_>>();
                            self.commit_idempotency
                                .lock()
                                .expect("commit idempotency poisoned")
                                .entry(shard.clone())
                                .or_default()
                                .record(request_id.clone(), fingerprint, recovery, expires_at);
                        }
                    }
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
            // Seed mint counters past every item id materialised by recovery so a post-reopen
            // push/upsert never re-mints a live id (would corrupt the eligibility index via
            // insert_pending overwrite — fireweed-6e38e2b4).
            self.projection
                .with_store(|p| ProjectionStore::restore_counters(p, &shard, &self.counters))?;
        }
        Ok(self)
    }

    /// No-op group-commit flush (memory / non-buffered log axes have no buffered segments).
    pub async fn flush_tick_async(&self, _now_ms: i64) -> EngineResult<()> {
        Ok(())
    }

    /// Drain deferred projection work when the projection axis supports it (async checkpoint).
    pub fn flush_deferred_projection(&self) -> EngineResult<()> {
        self.projection
            .with_store_mut(|p| ProjectionStore::flush_deferred(p))
    }

    /// Async entrypoint for deferred projection drain.
    pub async fn flush_deferred_projection_async(&self) -> EngineResult<()> {
        self.flush_deferred_projection()
    }

    /// No-op object-log retention trim (no segment store).
    pub async fn trim_reclaimable_segments_async(
        &self,
        _shard: QueueKey,
        _request_id_retention_ms: u64,
        _now: UtcTimestamp,
    ) -> EngineResult<crate::MaintenanceSummary> {
        Ok(crate::MaintenanceSummary::default())
    }

    /// No-op recover (in-process memory has no durable reopen path).
    pub async fn recover_async(&self) -> EngineResult<()> {
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

    fn map_lifecycle(error: AsyncLifecycleError) -> EngineError {
        match error {
            AsyncLifecycleError::BeforeCommit(error) | AsyncLifecycleError::Commit(error) => error,
            AsyncLifecycleError::AfterCommit { source, .. } => source,
            AsyncLifecycleError::Submit(error) => {
                EngineError::Storage(format!("async lifecycle submission failed: {error:?}"))
            }
        }
    }

    fn make_envelope(
        &self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
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

    async fn commit_envelope(
        &self,
        shard: &QueueKey,
        envelope: CommandEnvelope,
        expected_epoch: Option<u64>,
    ) -> EngineResult<RawCommitOutcome> {
        self.commit_envelopes(shard, vec![envelope], expected_epoch)
            .await
    }

    async fn commit_envelopes(
        &self,
        shard: &QueueKey,
        envelopes: Vec<CommandEnvelope>,
        expected_epoch: Option<u64>,
    ) -> EngineResult<RawCommitOutcome> {
        let epoch = self.resolve_epoch(shard, expected_epoch).await?;
        self.engine
            .submit_commit(RawCommitRequest::new(shard.clone(), envelopes, epoch))
            .await
            .map_err(|error| {
                EngineError::Storage(format!("async commit submission failed: {error:?}"))
            })?
    }
}

impl<L, P> Backend for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::Atomic
    }

    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }

    fn commit_capabilities(&self) -> crate::CommitCapabilities {
        // P advertises full commit-transition support (Snorri StateStore parity).
        crate::CommitCapabilities {
            atomic_transition_commit: true,
            vectorized_commit: true,
            lease_validation: true,
            retained_commit_idempotency: true,
            non_work_side_records: true,
            authoritative_recovery_reads: true,
            delayed_awaits_timers: true,
            durability_class: DurabilityClass::Atomic,
            consistency: "atomic durable log batch with synchronous projection apply (async log-replay composition)",
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

impl<L, P> ControlPlaneStore for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        async move {
            use crate::{ControlPlane, LogStore};

            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let mut outcome = ControlPlane::create_queue(self.control.as_ref(), definition)?;
            AsyncLogStore::ensure_shard(self.log.as_ref(), shard.clone()).await?;
            // Durable catalog so reopen / second handle can recover without re-create_queue.
            if let Some(durable) = self
                .log
                .run_with_store_mut({
                    let definition = outcome.definition.clone();
                    move |log| LogStore::create_or_read_definition(log, &definition)
                })
                .await?
            {
                let matches = durable.definition == outcome.definition;
                ControlPlane::cache_authoritative_definition(
                    self.control.as_ref(),
                    durable.definition.clone(),
                )?;
                outcome = durable;
                if !matches {
                    return Err(EngineError::QueueDefinitionConflict);
                }
            }
            // Ensure projection exists; if this handle is late to an already-created durable
            // queue (no local serving image yet), replay the log tail so the serving image
            // catches commands committed before create.
            //
            // Replay ONLY when the projection shard is missing (`needs_replay`). An
            // idempotent re-create (`!outcome.created`) against a live image must not re-apply
            // the durable log: `apply_recovery` defaults to plain `apply`, so replaying Push
            // (including commit_transition lifecycle items) would insert a second eligibility
            // index row per item_id (stale created_seq) and make ClaimPort::claim fail with
            // Invalid("invalid async claim plan") after select_item_claim returns duplicates
            // (fireweed-6e38e2b4 / snorri v0.24 regression).
            let needs_replay = self
                .projection
                .run_with_store({
                    let shard = shard.clone();
                    move |p| Ok(ProjectionStore::metrics(p, &shard).is_err())
                })
                .await?;
            AsyncProjectionStore::ensure_shard(
                self.projection.as_ref(),
                outcome.definition.clone(),
            )
            .await?;
            if needs_replay {
                use crate::{RequestOutcome, request_expires_at};
                let retention_ms = outcome.definition.request_id_retention_ms;
                let mut from = None;
                loop {
                    let page = AsyncLogStore::read_from(
                        self.log.as_ref(),
                        shard.clone(),
                        from.clone(),
                        256,
                    )
                    .await?;
                    if page.entries.is_empty() {
                        break;
                    }
                    let positions: Vec<_> = page.entries.iter().map(|(p, _)| p.clone()).collect();
                    let commands: Vec<_> = page.entries.iter().map(|(_, e)| e.clone()).collect();
                    AsyncProjectionStore::apply_recovery(
                        self.projection.as_ref(),
                        positions,
                        commands,
                    )
                    .await?;
                    // Rebuild push request-id ledger from durable envelopes (parity with
                    // recover()). Without this, late-join create_queue materializes items but
                    // leaves retained push idempotency empty — same-request_id conflict/replay
                    // after a cold open that only joins via create_queue would Proceed fresh.
                    {
                        let mut push_cache = self
                            .push_idempotency
                            .lock()
                            .expect("push idempotency poisoned");
                        for (_, env) in &page.entries {
                            let Some(request_id) = &env.request_id else {
                                continue;
                            };
                            if let QueueCommand::Push(_) = &env.command {
                                let fingerprint = push_envelope_body_hash(env)?;
                                let expires_at = request_expires_at(env.created_at, retention_ms);
                                let ids = match &env.request_outcome {
                                    Some(RequestOutcome::Push { item_ids }) => item_ids.clone(),
                                    _ => env.item_ids.clone(),
                                };
                                push_cache.entry(shard.clone()).or_default().record(
                                    request_id.clone(),
                                    fingerprint,
                                    ids,
                                    expires_at,
                                );
                            }
                        }
                    }
                    match page.next {
                        Some(next) => from = Some(next),
                        None => break,
                    }
                }
                // Late-join create_queue replayed into an empty image — seed mint counters
                // the same way recover() does after full-log rebuild.
                self.projection
                    .run_with_store({
                        let shard = shard.clone();
                        let counters = Arc::clone(&self.counters);
                        move |p| ProjectionStore::restore_counters(p, &shard, &counters)
                    })
                    .await?;
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
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
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

impl<L, P> PushPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
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
    ) -> impl std::future::Future<Output = EngineResult<crate::PushBatchOutcome>> + Send {
        async move {
            // Unified relational products persist request outcomes in their projection transaction
            // and have no command stream from which `recover()` can rebuild the process-local cache.
            // Resolve that durable authority before epoch validation or any new write. Canonicalizing
            // gate keys matches the async push planner's fingerprint contract.
            let mut replay_items = items.clone();
            crate::async_composed::canonicalize_push_gate_keys(&mut replay_items);
            let durable_replay = self
                .projection
                .run_with_store_mut({
                    let shard = shard.clone();
                    let request_id = request_id.clone();
                    move |projection| {
                        projection.replay_durable_push(&shard, &request_id, &replay_items, now)
                    }
                })
                .await?;
            if let Some(item_ids) = durable_replay {
                return Ok(crate::PushBatchOutcome::replayed(item_ids));
            }
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

impl<L, P> ClaimPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn claim(
        &self,
        request: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        async move { self.engine.claim(request).await.map_err(Self::map_claim) }
    }
}

impl<L, P> FinalizePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // fireweed-2be744bd / fireweed-9cec8b02 residual (log-replay product): resolve leases under
        // the same queue permit as plan+commit. The prior path validated outside the gate then
        // submitted a raw Finalize envelope, so concurrent claim/reclaim/commit_transition could
        // invalidate the observed lease and append a command apply_transition rejects
        // (snorri worker-pool/sqlite + campaign-scale/sqlite).
        async move {
            self.engine
                .finalize_outcomes(shard.clone(), outcomes, now, expected_epoch)
                .await
                .map_err(Self::map_lifecycle)
        }
    }
}

impl<L, P> RenewLeasePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        // Same TOCTOU family as finalize: renew under one queue permit (fireweed-c8e0a7a5).
        async move {
            self.engine
                .renew_item_ids(
                    shard.clone(),
                    item_ids.clone(),
                    new_lease_expires_at,
                    now,
                    expected_epoch,
                )
                .await
                .map_err(Self::map_lifecycle)?;
            // Keep claim_by_query / claim_by_item_ids idempotency replay alive through lease renewals.
            let renewed: HashSet<ItemId> = item_ids.into_iter().collect();
            self.claim_by_query_idempotency
                .lock()
                .expect("claim_by_query idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .extend_expiry_matching(new_lease_expires_at, |(claimed, _)| {
                    !claimed.is_empty() && claimed.iter().all(|item_id| renewed.contains(item_id))
                });
            self.claim_by_item_ids_idempotency
                .lock()
                .expect("claim_by_item_ids idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .extend_expiry_matching(new_lease_expires_at, |(claimed, _, _)| {
                    !claimed.is_empty() && claimed.iter().all(|item_id| renewed.contains(item_id))
                });
            Ok(())
        }
    }
}

impl<L, P> ReassignLeasePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
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
            self.projection
                .run_with_store({
                    let shard = shard.clone();
                    let item_ids = item_ids.clone();
                    move |projection| {
                        ProjectionStore::reassign_validate(projection, &shard, &item_ids)
                    }
                })
                .await?;
            let envelope = self.make_envelope(
                QueueCommand::ReassignLease(ReassignLeaseCommand {
                    item_ids: item_ids.clone(),
                    lease_token: new_lease_token,
                    lease_expires_at: new_lease_expires_at,
                }),
                item_ids,
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch)
                .await?;
            Ok(())
        }
    }
}

impl<L, P> PurgePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
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

impl<L, P> UpsertPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
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
        let client_item_key = client_item_key.clone();
        async move {
            let def =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            let max_attempts = def.retry_policy.max_attempts;
            let epoch = self.resolve_epoch(shard, expected_epoch).await?;
            let counter_base = self.counters.reserve(shard, epoch, 1);
            let new_item_id = ItemId::mint(epoch, self.node_id, counter_base);
            let item = PushItem {
                client_item_key: client_item_key.clone(),
                item_id: new_item_id,
                priority,
                not_before,
                group_key,
                max_attempts,
                payload,
                fields,
                metadata,
                cohort_size: None,
                gate_keys: Vec::new(),
                entity_document: entity,
            };

            let plan = self
                .projection
                .with_store(|projection| -> EngineResult<_> {
                    let existing =
                        ProjectionStore::lookup_by_key(projection, shard, &client_item_key)?;
                    match existing {
                        None => {
                            ProjectionStore::index_validate(
                                projection,
                                shard,
                                &item.item_id,
                                &item.fields,
                                item.entity_document.as_ref(),
                                None,
                            )?;
                            Ok(UpsertPlan::Insert(item))
                        }
                        Some(existing_id) => {
                            let state =
                                ProjectionStore::item_state(projection, shard, &existing_id)?
                                    .ok_or(EngineError::NotFound)?;
                            match state {
                                ItemState::Pending => {
                                    ProjectionStore::index_validate_replace(
                                        projection,
                                        shard,
                                        &existing_id,
                                        &item,
                                    )?;
                                    Ok(UpsertPlan::Replace { existing_id, item })
                                }
                                ItemState::Leased => {
                                    Err(EngineError::Invalid("collision with claimed item"))
                                }
                                ItemState::Complete | ItemState::Failed => {
                                    Err(EngineError::Terminal)
                                }
                            }
                        }
                    }
                })?;

            match plan {
                UpsertPlan::Insert(item) => {
                    let envelope = self.make_envelope(
                        QueueCommand::Push(PushCommand { items: vec![item] }),
                        vec![new_item_id],
                        now,
                    );
                    self.commit_envelope(shard, envelope, Some(epoch)).await?;
                    Ok(UpsertOutcome::Inserted {
                        item_id: new_item_id,
                    })
                }
                UpsertPlan::Replace { existing_id, item } => {
                    let envelope = self.make_envelope(
                        QueueCommand::ReplacePending(ReplacePendingCommand {
                            client_item_key,
                            superseded_item_id: existing_id,
                            replacement: item,
                        }),
                        vec![new_item_id],
                        now,
                    );
                    self.commit_envelope(shard, envelope, Some(epoch)).await?;
                    Ok(UpsertOutcome::Replaced {
                        new_item_id,
                        superseded_item_id: existing_id,
                    })
                }
            }
        }
    }
}

enum UpsertPlan {
    Insert(PushItem),
    Replace { existing_id: ItemId, item: PushItem },
}

impl<L, P> UpdateFieldsPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn update_fields(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        field_ops: BTreeMap<String, Option<Bytes>>,
        payload: PayloadUpdate,
        entity: Option<serde_json::Value>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            validate_api001_reserved_write_fields(&field_ops)?;
            let def =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            let schema = def
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;
            validate_entity(schema.as_ref(), entity.as_ref())?;
            self.projection.with_store(|projection| {
                ProjectionStore::update_fields_validate(
                    projection,
                    shard,
                    &item_id,
                    expected_item_version,
                )?;
                ProjectionStore::index_validate_update(
                    projection,
                    shard,
                    &item_id,
                    &field_ops,
                    entity.as_ref(),
                )
            })?;
            let envelope = self.make_envelope(
                QueueCommand::UpdateFields(UpdateFieldsCommand {
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
                }),
                vec![item_id],
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch)
                .await?;
            self.projection.with_store(|projection| {
                ProjectionStore::item_version(projection, shard, &item_id)?
                    .ok_or(EngineError::NotFound)
            })
        }
    }
}

impl<L, P> ReclaimPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
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
                .map_err(Self::map_lifecycle)
        }
    }
}

impl<L, P> ReclaimDriver for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        async move {
            // Scan every projection shard for expired leases (parity with sync composition's
            // expired_leases_page sweep — definitions are not required).
            let expired = self
                .projection
                .run_with_store(move |projection| {
                    Ok(ProjectionStore::all_expired_leases(projection, now))
                })
                .await?;
            let mut leases_reclaimed = 0u64;
            for (shard, _ids) in expired {
                let reclaimed = ReclaimPort::reclaim_expired(self, &shard, None, now, None)
                    .await?
                    .len() as u64;
                leases_reclaimed += reclaimed;
            }

            // Terminal retention is part of the same maintenance driver as lease reclamation.
            // A Class A log owns the queue catalog; Class B memory logs recover it from a durable
            // projection. For emit-enabled queues the durable emission cursor remains a hard
            // deletion barrier, matching the relational products.
            let mut definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
            if definitions.is_empty() {
                definitions =
                    AsyncProjectionStore::recover_definitions(self.projection.as_ref()).await?;
            }
            for definition in definitions {
                let shard =
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
                let may_advance = self
                    .projection
                    .run_with_store({
                        let shard = shard.clone();
                        move |projection| {
                            Ok(ProjectionStore::retention_may_advance(projection, &shard))
                        }
                    })
                    .await?;
                if !may_advance {
                    continue;
                }
                let emission_cursor = if definition.emit_change_records {
                    self.log
                        .run_with_store({
                            let shard = shard.clone();
                            move |log| LogStore::emission_cursor(log, &shard)
                        })
                        .await?
                } else {
                    None
                };
                if definition.emit_change_records && emission_cursor.is_none() {
                    continue;
                }
                self.projection
                    .run_with_store_mut(move |projection| {
                        ProjectionStore::reap_terminal_items(
                            projection,
                            &shard,
                            now,
                            definition.terminal_retention_ms,
                            definition.emit_change_records,
                            emission_cursor.as_ref(),
                        )
                        .map(|_| ())
                    })
                    .await?;
            }
            Ok(TickReport {
                leases_reclaimed,
                ..TickReport::default()
            })
        }
    }
}

impl<L, P> ProjectionRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::select_eligible(projection, &shard, now, limit)
                })
                .await
        }
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| ProjectionStore::peek(projection, &shard, limit))
                .await
        }
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| ProjectionStore::pending(projection, &shard))
                .await
        }
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::pending_summary(projection, &shard)
                })
                .await
        }
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::pending_page(projection, &shard, start, limit)
                })
                .await
        }
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let consumer = consumer.cloned();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::pending_range(
                        projection,
                        &shard,
                        start,
                        end,
                        consumer.as_ref(),
                        limit,
                    )
                })
                .await
        }
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let ids = ids.to_vec();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::pending_by_ids(projection, &shard, &ids)
                })
                .await
        }
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ClaimedItem>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let ids = ids.to_vec();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::render_claimed(projection, &shard, &ids)
                })
                .await
        }
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let keys = keys.to_vec();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::live_items(projection, &shard, &keys)
                })
                .await
        }
    }

    fn metrics(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            projection
                .run_with_store(move |projection| ProjectionStore::metrics(projection, &shard))
                .await
        }
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let emission_cursor = emission_cursor.cloned();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::terminal_emission_metrics(
                        projection,
                        &shard,
                        now,
                        emit_change_records,
                        emission_cursor.as_ref(),
                    )
                })
                .await
        }
    }
}

impl<L, P> LogRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        AsyncLogStore::read_from(self.log.as_ref(), shard.clone(), from, limit)
    }
}

/// Harness-only probe so AC-TXN-3 can strike the append→apply window with a real request_id.
/// Mirrors the retired sync `ComposedBackend` probe against the async log-replay product.
impl<L, P> RequestIdReplayProbe for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn build_request_id_push_envelope(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(CommandEnvelope, Vec<ItemId>)> {
        let supports_gates = self.projection.with_store(|p| p.supports_gates());
        validate_gate_push(supports_gates, &items)?;
        let fingerprint = crate::compose::push_body_hash(&items)?;
        let def = ControlPlane::queue_definition(self.control.as_ref(), shard)?;
        crate::async_composed::validate_push_shape(&def, &items)?;
        let schema = def
            .entity_schema
            .as_ref()
            .and_then(|esd| esd.entity_schema.as_ref())
            .map(compile_entity_schema)
            .transpose()?;
        for item in &items {
            validate_entity(schema.as_ref(), item.entity.as_ref())?;
        }
        let max_attempts = def.retry_policy.max_attempts;
        let epoch = match expected_epoch {
            Some(epoch) => epoch,
            None => self
                .log
                .with_store(|log| LogStore::current_epoch(log, shard))?,
        };
        let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
        let (push_items, ids) =
            build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
        self.projection
            .with_store(|p| p.index_validate_push(shard, &push_items))?;
        let env = CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: Some(RequestOutcome::Push {
                item_ids: ids.clone(),
            }),
            item_ids: ids.clone(),
            command: QueueCommand::Push(PushCommand { items: push_items }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, ids))
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
        let entry = CommitTransitionEntry {
            claim_ref: claim_ref.clone(),
            additional_claim_refs: Vec::new(),
            finalize,
            side_records: Vec::new(),
            lifecycle_items: Vec::new(),
            instance_fence: None,
        };
        let fingerprint = commit_body_hash(std::slice::from_ref(&entry))?;
        let item_id = claim_ref.item_id;
        let _ = expected_epoch;
        let supports = self
            .projection
            .with_store(|p| p.supports_commit_transition());
        if !supports {
            return Err(EngineError::Unavailable);
        }
        self.projection
            .with_store(|p| p.commit_validate(shard, std::slice::from_ref(&claim_ref), now))?;
        let env = CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(item_id, finalize)],
            }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        Ok((env, fingerprint))
    }

    fn build_request_id_commit_envelopes(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        entries: Vec<CommitTransitionEntry>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<(Vec<CommandEnvelope>, BodyHash)> {
        let fingerprint = commit_body_hash(&entries)?;
        let _ = expected_epoch;
        let supports = self
            .projection
            .with_store(|p| p.supports_commit_transition());
        if !supports {
            return Err(EngineError::Unavailable);
        }
        let commit_fingerprint = fingerprint.0;
        let mut envelopes: Vec<CommandEnvelope> = Vec::new();
        let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
        for entry in entries {
            if !entry.side_records.is_empty()
                || !entry.lifecycle_items.is_empty()
                || entry.instance_fence.is_some()
            {
                return Err(EngineError::Invalid(
                    "build_request_id_commit_envelopes: finalize-only entries",
                ));
            }
            let claim_ref = entry.claim_ref;
            let consumed_input_id = claim_ref.item_id;
            let additional_claim_refs = entry.additional_claim_refs;
            let additional_consumed_input_ids = additional_claim_refs
                .iter()
                .map(|claim| claim.item_id)
                .collect::<Vec<_>>();
            let mut claim_refs = Vec::with_capacity(1 + additional_claim_refs.len());
            claim_refs.push(claim_ref);
            claim_refs.extend(additional_claim_refs);
            if let Err(error) =
                crate::port::validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..])
            {
                recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: crate::CommitEntryStatus::Rejected(error),
                });
                continue;
            }
            match self
                .projection
                .with_store(|p| p.commit_validate(shard, &claim_refs, now))
            {
                Ok(()) => {
                    envelopes.push(CommandEnvelope {
                        command_id: self.ids.next_command_id(),
                        request_id: Some(request_id.clone()),
                        request_fingerprint: Some(commit_fingerprint),
                        request_outcome: None,
                        item_ids: claim_refs.iter().map(|claim| claim.item_id).collect(),
                        command: QueueCommand::Finalize(FinalizeCommand {
                            outcomes: claim_refs
                                .iter()
                                .map(|claim| FinalizeOutcome::new(claim.item_id, entry.finalize))
                                .collect(),
                        }),
                        checksum: CommandChecksum(0),
                        created_at: now,
                    });
                    recovery.push(EntryRecovery {
                        consumed_input_id,
                        additional_consumed_input_ids,
                        instance: None,
                        side_record_keys: Vec::new(),
                        lifecycle_item_ids: Vec::new(),
                        status: CommitEntryStatus::Committed,
                    });
                }
                Err(error) => recovery.push(EntryRecovery {
                    consumed_input_id,
                    additional_consumed_input_ids,
                    instance: None,
                    side_record_keys: Vec::new(),
                    lifecycle_item_ids: Vec::new(),
                    status: CommitEntryStatus::Rejected(error),
                }),
            }
        }
        // Always stamp a CommitTransition marker when request_id is present. Without the marker,
        // a success-only batch killed after append but before apply cannot rebuild commit
        // idempotency on reopen, and its retry is misclassified as a fresh terminal rejection.
        let outcome_entries: Vec<CommitOutcomeEntry> =
            recovery.iter().map(outcome_entry_from_recovery).collect();
        envelopes.push(CommandEnvelope {
            command_id: self.ids.next_command_id(),
            request_id: Some(request_id),
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
        Ok((envelopes, fingerprint))
    }
}

impl<L, P> SnapshotStore for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        AsyncLogStore::write_snapshot(self.log.as_ref(), shard.clone(), position, snapshot)
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

impl<L, P> IndexQueryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let index = index.to_string();
        let key = key.to_vec();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::index_get_unique(projection, &shard, &index, &key)
                })
                .await
        }
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        let index = index.to_string();
        let key = key.to_vec();
        async move {
            projection
                .run_with_store(move |projection| {
                    ProjectionStore::index_lookup(projection, &shard, &index, &key)
                })
                .await
        }
    }
}

impl<L, P> HotProjectionQueryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
        self.projection
            .with_store(ProjectionStore::hot_projection_capabilities)
    }

    fn range_scan(
        &self,
        shard: &QueueKey,
        request: fireweed_core::RangeScanRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::RangeScanResponse>> + Send
    {
        {
            let projection = Arc::clone(&self.projection);
            let shard = shard.clone();
            async move {
                projection
                    .run_with_store(move |projection| {
                        ProjectionStore::range_scan(projection, &shard, request)
                    })
                    .await
            }
        }
    }

    fn grouped_aggregate(
        &self,
        shard: &QueueKey,
        request: fireweed_core::GroupedAggregateRequest,
    ) -> impl std::future::Future<Output = EngineResult<fireweed_core::GroupedAggregateResponse>> + Send
    {
        {
            let projection = Arc::clone(&self.projection);
            let shard = shard.clone();
            async move {
                projection
                    .run_with_store(move |projection| {
                        ProjectionStore::grouped_aggregate(projection, &shard, request)
                    })
                    .await
            }
        }
    }

    fn metrics_by_query(
        &self,
        shard: &QueueKey,
        request: MetricsByQueryRequest,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        {
            let projection = Arc::clone(&self.projection);
            let shard = shard.clone();
            async move {
                projection
                    .run_with_store(move |projection| {
                        ProjectionStore::metrics_by_query(projection, &shard, request)
                    })
                    .await
            }
        }
    }

    fn declared_bucket_segment(
        &self,
        shard: &QueueKey,
        request: fireweed_core::DeclaredBucketSegmentRequest,
    ) -> impl std::future::Future<
        Output = EngineResult<fireweed_core::DeclaredBucketSegmentResponse>,
    > + Send {
        {
            let projection = Arc::clone(&self.projection);
            let shard = shard.clone();
            async move {
                projection
                    .run_with_store(move |projection| {
                        ProjectionStore::declared_bucket_segment(projection, &shard, request)
                    })
                    .await
            }
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
            let plan = self.projection.with_store(|projection| {
                ProjectionStore::plan_bounded_mutation(projection, &shard, request)
            })?;
            for update in plan.updates {
                let item_id = update.command.item_id;
                self.projection.with_store(|projection| {
                    ProjectionStore::update_fields_validate(
                        projection,
                        &shard,
                        &item_id,
                        Some(update.expected_item_version),
                    )?;
                    ProjectionStore::index_validate_update(
                        projection,
                        &shard,
                        &item_id,
                        &update.command.field_ops,
                        update.command.set_entity_document.as_ref(),
                    )
                })?;
                let envelope = self.make_envelope(
                    QueueCommand::UpdateFields(update.command),
                    vec![item_id],
                    context.now,
                );
                self.commit_envelope(&shard, envelope, context.expected_epoch)
                    .await?;
            }
            Ok(plan.response)
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
            use crate::{RequestOutcome, request_expires_at};

            let definition =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
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
            let expires_at = request_expires_at(context.now, definition.request_id_retention_ms);

            match self
                .claim_by_query_idempotency
                .lock()
                .expect("claim_by_query idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .check_conflict_first(&request_id, fingerprint, context.now)
            {
                IdempotencyDecision::Replay((item_ids, lease_token)) => {
                    let items = self.projection.with_store(|projection| {
                        ProjectionStore::render_claimed(projection, &shard, &item_ids)
                    })?;
                    if items.len() != item_ids.len()
                        || items
                            .iter()
                            .any(|item| item.lease_expires_at <= context.now)
                    {
                        return Err(EngineError::RequestExpired);
                    }
                    for item in &items {
                        if item.lease_token.as_ref() != Some(&lease_token) {
                            return Err(EngineError::RequestExpired);
                        }
                    }
                    return Ok(Claimed {
                        items,
                        ..Default::default()
                    });
                }
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Expired => return Err(EngineError::RequestExpired),
                IdempotencyDecision::Proceed => {}
            }

            // fireweed-9cec8b02: select eligible candidates and append Claim under one queue
            // permit. Selecting outside the gate let concurrent claim_by_query workers both
            // observe the same Pending ids and append two Claim commands (Leased + Claim →
            // illegal lifecycle) — snorri worker-pool/sqlite + campaign-scale/sqlite.
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let strategy = self.engine.commit_strategy();
            let projection = Arc::clone(&self.projection);
            let ids = Arc::clone(&self.ids);
            let claim_by_query_idempotency = Arc::clone(&self.claim_by_query_idempotency);
            self.engine
                .submit_operation(shard.clone(), move || {
                    Box::pin(async move {
                        let eligible: HashSet<ItemId> = projection
                            .with_store(|projection| {
                                ProjectionStore::eligible_candidates(
                                    projection,
                                    &shard,
                                    context.eligibility_at(),
                                    usize::MAX,
                                )
                            })?
                            .into_iter()
                            .collect();
                        let page_size = request.max_items.clamp(1, 1_000);
                        let mut cursor = None;
                        let mut item_ids = Vec::new();
                        while item_ids.len() < request.max_items as usize {
                            let page = projection.with_store(|projection| {
                                ProjectionStore::range_scan(
                                    projection,
                                    &shard,
                                    RangeScanRequest {
                                        index: request.index.clone(),
                                        filters: request.filters.clone(),
                                        order_by: vec![request.order_by.clone()],
                                        page_size,
                                        cursor,
                                    },
                                )
                            })?;
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
                            (generate_query_lease_token()?, item_ids.clone())
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
                        // Under the held permit — do not re-enter submit_commit.
                        strategy
                            .commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                            .await?;

                        let items = if claim_item_ids.is_empty() {
                            Vec::new()
                        } else {
                            projection.with_store(|projection| {
                                ProjectionStore::render_claimed(projection, &shard, &claim_item_ids)
                            })?
                        };
                        debug_assert_eq!(
                            items.len(),
                            claim_item_ids.len(),
                            "every queried claim candidate must render"
                        );
                        let replay_expires_at = if claim_item_ids.is_empty() {
                            expires_at
                        } else {
                            expires_at.max(lease_expires_at)
                        };
                        claim_by_query_idempotency
                            .lock()
                            .expect("claim_by_query idempotency poisoned")
                            .entry(shard)
                            .or_default()
                            .record(
                                request_id,
                                fingerprint,
                                (claim_item_ids, lease_token),
                                replay_expires_at,
                            );
                        Ok(Claimed {
                            items,
                            ..Default::default()
                        })
                    })
                })
                .await
                .map_err(|e| EngineError::Storage(format!("async claim_by_query failed: {e:?}")))?
        }
    }

    fn claim_by_item_ids(
        &self,
        shard: &QueueKey,
        request: ClaimByItemIdsRequest,
        context: ClaimByQueryContext,
    ) -> impl std::future::Future<Output = EngineResult<ClaimByItemIdsResponse>> + Send {
        let shard = shard.clone();
        async move {
            use crate::{RequestOutcome, request_expires_at};

            let definition =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            if request.item_ids.is_empty() {
                return Err(EngineError::Invalid("claim_by_item_ids item_ids required"));
            }
            // Collapse duplicates (first-occurrence order) before validating batch size.
            let mut seen = HashSet::new();
            let distinct: Vec<ItemId> = request
                .item_ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id))
                .collect();
            if u64::try_from(distinct.len()).unwrap_or(u64::MAX) > definition.max_claim_batch_size {
                return Err(EngineError::Invalid(
                    "claim_by_item_ids exceeds max_claim_batch_size",
                ));
            }
            if request.lease_duration_ms == 0
                || request.lease_duration_ms > definition.max_lease_duration_ms
            {
                return Err(EngineError::Invalid(
                    "invalid claim_by_item_ids lease_duration_ms",
                ));
            }
            let request_id = request.request_id.clone();
            let fingerprint = claim_by_item_ids_body_hash(&request)?;
            let expires_at = request_expires_at(context.now, definition.request_id_retention_ms);

            match self
                .claim_by_item_ids_idempotency
                .lock()
                .expect("claim_by_item_ids idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .check_conflict_first(&request_id, fingerprint, context.now)
            {
                IdempotencyDecision::Replay((claimed_ids, lease_token, outcomes)) => {
                    let items = if claimed_ids.is_empty() {
                        Vec::new()
                    } else {
                        self.projection.with_store(|projection| {
                            ProjectionStore::render_claimed(projection, &shard, &claimed_ids)
                        })?
                    };
                    if items.len() != claimed_ids.len()
                        || items
                            .iter()
                            .any(|item| item.lease_expires_at <= context.now)
                    {
                        return Err(EngineError::RequestExpired);
                    }
                    for item in &items {
                        if item.lease_token.as_ref() != Some(&lease_token) {
                            return Err(EngineError::RequestExpired);
                        }
                    }
                    return Ok(ClaimByItemIdsResponse { items, outcomes });
                }
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Expired => return Err(EngineError::RequestExpired),
                IdempotencyDecision::Proceed => {}
            }

            // fireweed-9cec8b02: classify + append Claim under one queue permit (same race as
            // claim_by_query when concurrent workers target overlapping ids).
            let epoch = self.resolve_epoch(&shard, context.expected_epoch).await?;
            let strategy = self.engine.commit_strategy();
            let projection = Arc::clone(&self.projection);
            let ids = Arc::clone(&self.ids);
            let claim_by_item_ids_idempotency = Arc::clone(&self.claim_by_item_ids_idempotency);
            self.engine
                .submit_operation(shard.clone(), move || {
                    Box::pin(async move {
                        let eligibility_at = context.eligibility_at();
                        let mut outcomes = Vec::with_capacity(distinct.len());
                        let mut claimable: Vec<ItemId> = Vec::new();
                        for item_id in &distinct {
                            let class = projection.with_store(|projection| {
                                ProjectionStore::classify_claim_by_item_id(
                                    projection,
                                    &shard,
                                    item_id,
                                    eligibility_at,
                                )
                            })?;
                            match class {
                                ClaimByItemIdClass::Claimable => {
                                    claimable.push(*item_id);
                                    outcomes.push(ClaimByItemIdsOutcome {
                                        item_id: *item_id,
                                        disposition: ClaimByItemIdsDisposition::Claimed,
                                    });
                                }
                                other => {
                                    outcomes.push(ClaimByItemIdsOutcome {
                                        item_id: *item_id,
                                        disposition: other.into(),
                                    });
                                }
                            }
                        }

                        let lease_expires_at = context.lease_expires_at(request.lease_duration_ms);
                        let (lease_token, claim_item_ids) = if claimable.is_empty() {
                            (
                                request.lease_token.clone().unwrap_or_else(|| {
                                    LeaseToken::new("empty-claim-by-item-ids").expect("valid token")
                                }),
                                Vec::new(),
                            )
                        } else if let Some(token) = request.lease_token.clone() {
                            (token, claimable)
                        } else {
                            (generate_query_lease_token()?, claimable)
                        };

                        let envelope = CommandEnvelope {
                            command_id: ids.next_command_id(),
                            request_id: Some(request_id.clone()),
                            request_fingerprint: Some(fingerprint.0),
                            request_outcome: Some(RequestOutcome::ClaimByItemIds {
                                claimed_item_ids: claim_item_ids.clone(),
                                lease_token: lease_token.clone(),
                                outcomes: outcomes.clone(),
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
                            .commit(RawCommitRequest::new(shard.clone(), vec![envelope], epoch))
                            .await?;

                        let items = if claim_item_ids.is_empty() {
                            Vec::new()
                        } else {
                            projection.with_store(|projection| {
                                ProjectionStore::render_claimed(projection, &shard, &claim_item_ids)
                            })?
                        };
                        debug_assert_eq!(
                            items.len(),
                            claim_item_ids.len(),
                            "every claim_by_item_ids candidate must render"
                        );
                        let replay_expires_at = if claim_item_ids.is_empty() {
                            expires_at
                        } else {
                            expires_at.max(lease_expires_at)
                        };
                        claim_by_item_ids_idempotency
                            .lock()
                            .expect("claim_by_item_ids idempotency poisoned")
                            .entry(shard)
                            .or_default()
                            .record(
                                request_id,
                                fingerprint,
                                (claim_item_ids, lease_token, outcomes.clone()),
                                replay_expires_at,
                            );
                        Ok(ClaimByItemIdsResponse { items, outcomes })
                    })
                })
                .await
                .map_err(|e| {
                    EngineError::Storage(format!("async claim_by_item_ids failed: {e:?}"))
                })?
        }
    }
}

impl<L, P> crate::SetGatesPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn set_gates(
        &self,
        shard: &QueueKey,
        command: crate::SetGatesCommand,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        async move {
            let envelope = self.make_envelope(QueueCommand::SetGates(command), Vec::new(), now);
            self.commit_envelope(shard, envelope, expected_epoch)
                .await?;
            Ok(())
        }
    }
}

impl<L, P> crate::ReschedulePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn reschedule(
        &self,
        shard: &QueueKey,
        item_id: ItemId,
        set_priority: crate::ScheduleUpdate<PriorityValue>,
        set_not_before: crate::ScheduleUpdate<UtcTimestamp>,
        expected_item_version: Option<u64>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        async move {
            self.projection.with_store(|projection| {
                ProjectionStore::update_fields_validate(
                    projection,
                    shard,
                    &item_id,
                    expected_item_version,
                )
            })?;
            let envelope = self.make_envelope(
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id,
                    field_ops: BTreeMap::new(),
                    payload: PayloadUpdate::Keep,
                    set_priority,
                    set_not_before,
                    set_entity_document: None,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                }),
                vec![item_id],
                now,
            );
            self.commit_envelope(shard, envelope, expected_epoch)
                .await?;
            self.projection.with_store(|projection| {
                ProjectionStore::item_version(projection, shard, &item_id)?
                    .ok_or(EngineError::NotFound)
            })
        }
    }
}

impl<L, P> crate::DiscoveryPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn discover_active_scopes(
        &self,
        shard: &QueueKey,
        granularity: crate::DiscoveryGranularity,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::ActiveScope>>> + Send {
        std::future::ready(self.projection.with_store(|projection| {
            ProjectionStore::discover_active_scopes(projection, shard, granularity, now)
        }))
    }
}

impl<L, P> crate::RecoveryReadPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn explain_commit(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
    ) -> impl std::future::Future<Output = EngineResult<Option<crate::CommitRecovery>>> + Send {
        let cache = Arc::clone(&self.commit_idempotency);
        let projection = Arc::clone(&self.projection);
        let shard = shard.clone();
        async move {
            if let Some(recovery) = cache
                .lock()
                .expect("commit idempotency poisoned")
                .get(&shard)
                .and_then(|c| c.peek(&request_id))
            {
                return Ok(Some(crate::CommitRecovery {
                    request_id,
                    entries: recovery,
                }));
            }
            let durable = projection
                .with_store(|p| ProjectionStore::read_durable_commit(p, &shard, &request_id))?;
            Ok(durable.map(|entries| crate::CommitRecovery {
                request_id,
                entries: entries
                    .into_iter()
                    .map(crate::recovery_from_outcome_entry)
                    .collect(),
            }))
        }
    }

    fn side_record(
        &self,
        shard: &QueueKey,
        key: &[u8],
    ) -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
        let key = key.to_vec();
        std::future::ready(
            self.projection
                .with_store(|projection| ProjectionStore::side_record(projection, shard, &key)),
        )
    }
}

impl<L, P> crate::HistoricalProjectionRead for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + AsOfProjectionStore + Send + 'static,
    P::AsOfProjection: ProjectionStore + Send + 'static,
{
    type AsOfProjection = P::AsOfProjection;

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
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> impl std::future::Future<Output = EngineResult<T>> + Send
    where
        T: Send + 'static,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send + 'static,
    {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let shard = shard.clone();
        async move {
            use crate::AsOfProjectionStore;
            if !projection.with_store(|p| p.supports_as_of()) {
                return Err(EngineError::Unavailable);
            }
            let definition =
                AsyncControlPlane::queue_definition(control.as_ref(), shard.clone()).await?;
            let snapshot_ref =
                AsyncLogStore::snapshot_at_or_before(log.as_ref(), shard.clone(), position.clone())
                    .await?;
            let snapshot = match snapshot_ref.as_ref() {
                Some(r) => Some(AsyncLogStore::read_snapshot(log.as_ref(), r.clone()).await?),
                None => None,
            };
            let mut as_of = projection
                .with_store(|p| AsOfProjectionStore::reconstruct_as_of(p, &definition, snapshot))?;
            let mut from = snapshot_ref.map(|s| s.position);
            loop {
                let page = AsyncLogStore::read_from(log.as_ref(), shard.clone(), from.clone(), 256)
                    .await?;
                if page.entries.is_empty() {
                    break;
                }
                let mut positions = Vec::new();
                let mut envelopes = Vec::new();
                let mut reached_target = false;
                for (entry_position, env) in page.entries {
                    if entry_position == position || entry_position.precedes(&position) {
                        positions.push(entry_position.clone());
                        envelopes.push(env);
                    } else {
                        reached_target = true;
                        break;
                    }
                }
                if !positions.is_empty() {
                    ProjectionStore::apply_recovery(&mut as_of, &positions, &envelopes)?;
                }
                if reached_target || page.next.is_none() {
                    break;
                }
                from = page.next;
            }
            query(&as_of)
        }
    }
}

impl<L, P> BatchUpdatePort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn batch_update(
        &self,
        shard: &QueueKey,
        request: BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<BatchUpdateResponse>> + Send {
        let shard = shard.clone();
        async move {
            use crate::{BatchUpdateOutcome, RequestOutcome, request_expires_at};

            if request.updates.is_empty() {
                return Err(EngineError::Invalid("empty batch update"));
            }
            if request.updates.len() > 1_000 {
                return Err(EngineError::BatchTooLarge);
            }

            let fingerprint = batch_update_body_hash(&request)?;
            let definition =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            let expires_at = request_expires_at(now, definition.request_id_retention_ms);
            let refs = request
                .updates
                .iter()
                .map(|update| update.item_ref.clone())
                .collect::<Vec<_>>();
            let request_id = request.request_id.clone();

            match self
                .batch_update_idempotency
                .lock()
                .expect("batch_update idempotency poisoned")
                .entry(shard.clone())
                .or_default()
                .check(&request_id, fingerprint, now)
            {
                IdempotencyDecision::Replay(response) => return Ok(response),
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
            if let Some(response) = self.projection.with_store_mut(|p| {
                ProjectionStore::replay_durable_batch_update(
                    p,
                    &shard,
                    &request_id,
                    fingerprint.0,
                    now,
                )
            })? {
                self.batch_update_idempotency
                    .lock()
                    .expect("batch_update idempotency poisoned")
                    .entry(shard.clone())
                    .or_default()
                    .record(request_id, fingerprint, response.clone(), expires_at);
                return Ok(response);
            }

            let snapshot = self.projection.with_store(|projection| {
                ProjectionStore::batch_update_snapshot(projection, &shard, &refs)
            })?;
            let mut plan = plan_batch_update(
                &definition,
                self.supports_gates(),
                request.updates,
                snapshot,
            );
            let candidate_commands = plan
                .commands
                .iter()
                .map(|(_, command)| command.clone())
                .collect::<Vec<_>>();
            let accepted = self.projection.with_store(|projection| {
                ProjectionStore::batch_update_preflight(projection, &shard, &candidate_commands)
            })?;
            if accepted.len() != candidate_commands.len() {
                return Err(EngineError::Storage(
                    "batch update preflight returned a mismatched result count".into(),
                ));
            }
            plan.commands = plan
                .commands
                .into_iter()
                .zip(accepted)
                .filter_map(|((outcome_index, command), accepted)| {
                    if accepted {
                        Some((outcome_index, command))
                    } else {
                        plan.outcomes[outcome_index] = BatchUpdateOutcome::Invalid;
                        None
                    }
                })
                .collect();

            let response = BatchUpdateResponse {
                request_id: request_id.clone(),
                results: plan.outcomes,
            };
            let response_payload = serde_json::to_string(&response)
                .map_err(|error| EngineError::Storage(error.to_string()))?;
            let mut envelopes = plan
                .commands
                .into_iter()
                .map(|(_, command)| {
                    let item_id = command.item_id;
                    self.make_envelope(QueueCommand::UpdateFields(command), vec![item_id], now)
                })
                .collect::<Vec<_>>();
            if envelopes.is_empty() {
                envelopes.push(self.make_envelope(
                    QueueCommand::WriteSideRecords(WriteSideRecordsCommand::default()),
                    Vec::new(),
                    now,
                ));
            }
            let marker = envelopes
                .first_mut()
                .expect("batch update always emits a command or marker");
            marker.request_id = Some(request_id.clone());
            marker.request_fingerprint = Some(fingerprint.0);
            marker.request_outcome = Some(RequestOutcome::BatchUpdate { response_payload });

            self.commit_envelopes(&shard, envelopes, expected_epoch)
                .await?;
            self.batch_update_idempotency
                .lock()
                .expect("batch_update idempotency poisoned")
                .entry(shard)
                .or_default()
                .record(request_id, fingerprint, response.clone(), expires_at);
            Ok(response)
        }
    }
}

impl<L, P> crate::ItemMutationPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn mutate_items(
        &self,
        shard: &QueueKey,
        request: crate::ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<crate::ItemMutationResponse>> + Send {
        let shard = shard.clone();
        async move {
            use crate::{RequestOutcome, item_mutation_fingerprint};

            let fingerprint = BodyHash(item_mutation_fingerprint(&request)?);
            let request_id = request.request_id.clone();
            let evaluated_at = request.evaluated_at;

            if let Some(response) = self.projection.with_store_mut(|p| {
                ProjectionStore::replay_durable_item_mutation(
                    p,
                    &shard,
                    &request_id,
                    fingerprint.0,
                    evaluated_at,
                )
            })? {
                return Ok(response);
            }

            // Scan log for retained request-id outcome.
            let mut from = self
                .log
                .with_store(|l| crate::LogStore::retention_floor(l, &shard))?;
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
                    let mut response: crate::ItemMutationResponse =
                        serde_json::from_str(response_payload)
                            .map_err(|e| EngineError::Storage(e.to_string()))?;
                    response.position = Some(position.clone());
                    return Ok(response);
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }

            let mut plan = self
                .projection
                .with_store_mut(|p| ProjectionStore::plan_item_mutation(p, &shard, &request))?;
            if request.dry_run {
                return Ok(plan.response);
            }
            let response_payload = serde_json::to_string(&plan.response)
                .map_err(|e| EngineError::Storage(e.to_string()))?;
            let item_ids = plan.command.items.iter().map(|i| i.item_id).collect();
            let mut envelope = self.make_envelope(
                QueueCommand::MutateItems(plan.command),
                item_ids,
                evaluated_at,
            );
            envelope.request_id = Some(request_id);
            envelope.request_fingerprint = Some(fingerprint.0);
            envelope.request_outcome = Some(RequestOutcome::ItemMutation { response_payload });
            self.commit_envelope(&shard, envelope, expected_epoch)
                .await?;
            plan.response.position = Some(
                AsyncLogStore::high_water(self.log.as_ref(), shard.clone())
                    .await?
                    .ok_or(EngineError::NotFound)?,
            );
            Ok(plan.response)
        }
    }
}

impl<L, P> crate::CommitTransitionPort for AsyncLogReplayBackend<L, P>
where
    L: LogStore + Send + 'static,
    P: ProjectionStore + Send + 'static,
{
    fn commit_transition(
        &self,
        shard: &QueueKey,
        transition: crate::CommitTransition,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<crate::CommitEntryOutcome>>> + Send
    {
        let shard = shard.clone();
        async move {
            use crate::{
                AdvanceInstanceFenceCommand, CommitEntryStatus, CommitOutcomeEntry,
                CommitTransitionEntry, EntryRecovery, FinalizeOutcome, RequestOutcome,
                WriteSideRecordsCommand, build_push_items, commit_body_hash,
                outcome_entry_from_recovery, outcomes_from_recovery, request_expires_at,
                validate_distinct_commit_claims, validate_entity, validate_instance_fence,
            };
            use std::collections::HashSet;

            let crate::CommitTransition {
                request_id,
                entries,
            } = transition;
            let fingerprint = commit_body_hash(&entries)?;
            let definition =
                AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
            let max_attempts = definition.retry_policy.max_attempts;
            let retention = definition.request_id_retention_ms;
            let schema = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?;

            if let Some(rid) = &request_id {
                let cached = {
                    let cache = self
                        .commit_idempotency
                        .lock()
                        .expect("commit idempotency poisoned");
                    cache.get(&shard).map(|c| c.check(rid, fingerprint, now))
                };
                if let Some(decision) = cached {
                    match decision {
                        IdempotencyDecision::Replay(recovery)
                            if recovery.len() == entries.len() =>
                        {
                            return Ok(outcomes_from_recovery(&recovery));
                        }
                        IdempotencyDecision::Conflict => {
                            return Err(EngineError::RequestIdConflict);
                        }
                        IdempotencyDecision::Replay(_)
                        | IdempotencyDecision::Proceed
                        | IdempotencyDecision::Expired => {}
                    }
                }
                if let Some(entries) = self
                    .projection
                    .run_with_store_mut({
                        let shard = shard.clone();
                        let rid = rid.clone();
                        let fp = fingerprint.0;
                        move |p| ProjectionStore::replay_durable_commit(p, &shard, &rid, fp, now)
                    })
                    .await?
                {
                    let recovery = entries
                        .into_iter()
                        .map(crate::recovery_from_outcome_entry)
                        .collect::<Vec<_>>();
                    self.commit_idempotency
                        .lock()
                        .expect("commit idempotency poisoned")
                        .entry(shard.clone())
                        .or_default()
                        .record(
                            rid.clone(),
                            fingerprint,
                            recovery.clone(),
                            request_expires_at(now, retention),
                        );
                    return Ok(outcomes_from_recovery(&recovery));
                }
            }

            // fireweed-5497780d: fence validation + append/apply under one queue-local permit so
            // concurrent fenced side-record commits cannot last-writer-wins overwrite each other.
            let commit_fingerprint = fingerprint.0;
            let epoch = self.resolve_epoch(&shard, expected_epoch).await?;
            let strategy = self.engine.commit_strategy();
            let projection = Arc::clone(&self.projection);
            let ids = Arc::clone(&self.ids);
            let counters = Arc::clone(&self.counters);
            let commit_idempotency = Arc::clone(&self.commit_idempotency);
            let node_id = self.node_id;
            // fireweed-a355d82b / fireweed-60ca4bfd: always validate only this entry's push delta
            // against durable state. Unique-index queues track staged keys incrementally so
            // within-commit cross-entry uniqueness stays O(1) per key (not O(N²) full re-scan).
            let requires_cross_entry_push_validation =
                definition.requires_cross_entry_push_validation();
            let definition_for_unique = definition.clone();
            self.engine
                .submit_operation(shard.clone(), move || {
                    Box::pin(async move {
                        let mut recovery: Vec<EntryRecovery> = Vec::with_capacity(entries.len());
                        let mut committed_envelopes: Vec<CommandEnvelope> = Vec::new();
                        let mut finalized_in_commit: HashSet<ItemId> = HashSet::new();
                        let mut staged_fences: HashMap<Vec<u8>, u64> = HashMap::new();
                        let mut staged_unique_keys: HashMap<(String, Vec<u8>), ItemId> =
                            HashMap::new();

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
                            let mut claim_refs =
                                Vec::with_capacity(1 + additional_claim_refs.len());
                            claim_refs.push(claim_ref);
                            claim_refs.extend(additional_claim_refs);
                            let reject = |e: EngineError| EntryRecovery {
                                consumed_input_id,
                                additional_consumed_input_ids: additional_consumed_input_ids
                                    .clone(),
                                instance: None,
                                side_record_keys: Vec::new(),
                                lifecycle_item_ids: Vec::new(),
                                status: CommitEntryStatus::Rejected(e),
                            };

                            if let Err(error) =
                                validate_distinct_commit_claims(&claim_refs[0], &claim_refs[1..])
                            {
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
                            if let Err(e) = projection
                                .run_with_store({
                                    let shard = shard.clone();
                                    let claim_refs = claim_refs.clone();
                                    move |p| {
                                        ProjectionStore::commit_validate(
                                            p,
                                            &shard,
                                            &claim_refs,
                                            now,
                                        )
                                    }
                                })
                                .await
                            {
                                recovery.push(reject(e));
                                continue;
                            }
                            if let Some(fence) = &instance_fence {
                                let stored = match staged_fences.get(&fence.instance_key) {
                                    Some(v) => *v,
                                    None => projection
                                        .run_with_store({
                                            let shard = shard.clone();
                                            let key = fence.instance_key.clone();
                                            move |p| {
                                                ProjectionStore::instance_fence(p, &shard, &key)
                                            }
                                        })
                                        .await?
                                        .unwrap_or(0),
                                };
                                if let Err(e) = validate_instance_fence(stored, fence) {
                                    recovery.push(reject(e));
                                    continue;
                                }
                            }

                            let side_record_keys: Vec<Vec<u8>> =
                                side_records.iter().map(|r| r.key.clone()).collect();
                            let instance = instance_fence
                                .as_ref()
                                .map(|f| (f.instance_key.clone(), f.next));
                            let mut envelopes: Vec<CommandEnvelope> = Vec::new();
                            let mk_env =
                                |command: QueueCommand, item_ids: Vec<ItemId>| CommandEnvelope {
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
                                    QueueCommand::AdvanceInstanceFence(
                                        AdvanceInstanceFenceCommand {
                                            instance_key: fence.instance_key,
                                            expected: fence.expected,
                                            next: fence.next,
                                        },
                                    ),
                                    Vec::new(),
                                ));
                            }

                            let mut lifecycle_item_ids = Vec::new();
                            if !lifecycle_items.is_empty() {
                                if let Some(e) = lifecycle_items.iter().find_map(|item| {
                                    validate_entity(schema.as_ref(), item.entity.as_ref()).err()
                                }) {
                                    recovery.push(reject(e));
                                    continue;
                                }
                                let counter_base =
                                    counters.reserve(&shard, epoch, lifecycle_items.len() as u32);
                                let (push_items, push_ids) = build_push_items(
                                    lifecycle_items,
                                    epoch,
                                    node_id,
                                    counter_base,
                                    max_attempts,
                                );
                                // fireweed-a355d82b / fireweed-60ca4bfd: validate only this entry's
                                // push delta against durable projection (plus within-delta uniqueness).
                                // Cross-entry uniqueness for unique-index queues uses staged_unique_keys.
                                if let Err(e) = projection
                                    .run_with_store({
                                        let shard = shard.clone();
                                        let candidate = push_items.clone();
                                        move |p| {
                                            ProjectionStore::index_validate_push(
                                                p, &shard, &candidate,
                                            )
                                        }
                                    })
                                    .await
                                {
                                    recovery.push(reject(e));
                                    continue;
                                }
                                if requires_cross_entry_push_validation
                                    && let Err(e) = stage_unique_push_keys(
                                        &definition_for_unique,
                                        &push_items,
                                        &mut staged_unique_keys,
                                    )
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

                        let mut batch = committed_envelopes;
                        if let Some(rid) = &request_id {
                            let outcome_entries: Vec<CommitOutcomeEntry> =
                                recovery.iter().map(outcome_entry_from_recovery).collect();
                            batch.push(CommandEnvelope {
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
                        if !batch.is_empty() {
                            // Under the held permit — do not re-enter submit_commit.
                            strategy
                                .commit(RawCommitRequest::new(shard.clone(), batch, epoch))
                                .await?;
                        }

                        let outcomes = outcomes_from_recovery(&recovery);
                        if let Some(rid) = request_id {
                            commit_idempotency
                                .lock()
                                .expect("commit idempotency poisoned")
                                .entry(shard)
                                .or_default()
                                .record(
                                    rid,
                                    fingerprint,
                                    recovery,
                                    request_expires_at(now, retention),
                                );
                        }
                        Ok(outcomes)
                    })
                })
                .await
                .map_err(|e| {
                    EngineError::Storage(format!("async commit_transition failed: {e:?}"))
                })?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    struct FailAtHook {
        fail_at: ComposeFaultPoint,
        seen: Arc<Mutex<Vec<ComposeFaultPoint>>>,
    }

    impl ComposeFaultHook for FailAtHook {
        fn fault_point(&self, cut: ComposeFaultPoint) -> EngineResult<()> {
            self.seen.lock().expect("seen points poisoned").push(cut);
            if cut == self.fail_at {
                return Err(EngineError::Invalid("test composed apply fault"));
            }
            Ok(())
        }
    }

    #[test]
    fn during_projection_apply_fault_stops_before_projection_advance() {
        futures::executor::block_on(async {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let hook: Arc<dyn ComposeFaultHook> = Arc::new(FailAtHook {
                fail_at: ComposeFaultPoint::DuringProjectionApply,
                seen: Arc::clone(&seen),
            });
            let applied = AtomicBool::new(false);

            assert!(
                apply_with_compose_fault_hook(Some(&hook), || async {
                    applied.store(true, AtomicOrdering::SeqCst);
                    Ok(())
                })
                .await
                .is_err(),
                "the injected pre-apply fault must fail the caller"
            );
            assert_eq!(
                *seen.lock().expect("seen points poisoned"),
                vec![ComposeFaultPoint::DuringProjectionApply]
            );
            assert!(
                !applied.load(AtomicOrdering::SeqCst),
                "the projection must not advance at the during-apply cut"
            );
        });
    }

    #[test]
    fn after_apply_fault_advances_projection_before_failing_response() {
        futures::executor::block_on(async {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let hook: Arc<dyn ComposeFaultHook> = Arc::new(FailAtHook {
                fail_at: ComposeFaultPoint::AfterApplyBeforeResponse,
                seen: Arc::clone(&seen),
            });
            let applied = AtomicBool::new(false);

            assert!(
                apply_with_compose_fault_hook(Some(&hook), || async {
                    applied.store(true, AtomicOrdering::SeqCst);
                    Ok(())
                })
                .await
                .is_err(),
                "the injected post-apply fault must fail the caller"
            );
            assert_eq!(
                *seen.lock().expect("seen points poisoned"),
                vec![
                    ComposeFaultPoint::DuringProjectionApply,
                    ComposeFaultPoint::AfterApplyBeforeResponse,
                ],
                "the post-apply cut must follow the during-apply cut"
            );
            assert!(
                applied.load(AtomicOrdering::SeqCst),
                "the projection must already be advanced at the post-apply cut"
            );
        });
    }
}
