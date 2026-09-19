//! Turso projection composition for the public 4×3 storage matrix.
//!
//! Composes each log axis with [`fireweed_turso::TursoRelational`] through the same
//! engine planners / commit strategies used by other derived projections:
//! - Atomic logs (memory / postgres): [`UnifiedAtomicCommit`] (log-replay product shape)
//! - Object logs (filesystem / s3): [`fireweed_engine::SeparateReplayCommit`] (provider-neutral LogEngine constructors)
//!
//! This module deliberately avoids an `ObjectLogTursoBackend` public alias.

#![allow(clippy::manual_async_fn)]

#[path = "turso_parity.rs"]
mod parity;

#[cfg(feature = "objectlog")]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "objectlog")]
use std::sync::Mutex;
#[cfg(feature = "objectlog")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "objectlog")]
use std::time::Duration;
#[cfg(feature = "objectlog")]
use std::time::Instant;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueryCapabilityFlags, QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::commit_surface::{CommitIdempotency, new_commit_idempotency};
use fireweed_engine::{
    AppendAdmissionClass, AsyncClaimError, AsyncCommitStrategy, AsyncCommitSubmitError,
    AsyncComposedBackend, AsyncControlPlane, AsyncFinalizeRequest, AsyncLifecycleError,
    AsyncLogStore, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest,
    AsyncReclaimRequest, AsyncRenewRequest, Backend, BatchUpdatePort, ClaimCommand, ClaimPort,
    ClaimRequest, Claimed, CommandChecksum, CommandEnvelope, CommandPosition, ControlPlaneStore,
    CoordinationError, CreateQueueOutcome, DurabilityClass, EngineError, EngineResult,
    FinalizeOutcome, FinalizePort, FinalizeTarget, HistoricalProjectionRead,
    HotProjectionQueryPort, IdGen, InProcessControlPlane, InProcessLogStore, IndexQueryPort,
    InlineOwnedTaskDispatcher, ItemMutationPort, ItemMutationRequest, ItemMutationResponse,
    ItemView, LeaseView, LiveItemView, LogStore, OutcomeReadAdmission, OwnedTask, PendingPage,
    PendingSummary, ProjectionClaimPlanner, ProjectionLifecyclePlanner, ProjectionPushPlanner,
    ProjectionRead, ProjectionReclaimPlanner, ProjectionSnapshot, PurgePort, PushCommand, PushItem,
    PushPort, PushSpec, QueueCommand, QueueCounters, QueueGateError, QueueKey, QueueMetrics,
    RawCommitFault, RawCommitOutcome, RawCommitRequest, ReassignLeaseCommand, ReassignLeasePort,
    ReclaimDriver, ReclaimPort, RenewLeasePort, RenewTarget, ReplacePendingCommand, RequestOutcome,
    SeqIdGen, SetGatesPort, SnapshotRef, SnapshotStore, TerminalEmissionMetrics, TickReport,
    UnifiedAtomicCommit, UnifiedAtomicCommitter, UpdateFieldsBatchCommand, UpdateFieldsPort,
    UpsertOutcome, UpsertPort,
};
#[cfg(feature = "objectlog")]
use fireweed_engine::{
    AsyncProjectionSpec, CLAIM_GENERATION_MAX_REQUESTS, ClaimDriverReadAdmission, ClaimQueueTurn,
    ControlPlane, DispatchError, ExpiredLeaseCursor, ExpiredLeasePage, FinalizeKind,
    IdempotencyDecision, MUTATION_SEQUENCER_DEFAULT_MAX_WAIT, MutationGenerationMemberOutcome,
    MutationGenerationWork, MutationIngress, MutationSequencer, MutationSequencerKey,
    OwnedTaskDispatcher, OwnedTaskFactory, PreparedClaim, PreparedClaimedResult, PreparedFinalize,
    PushFingerprint, S3S_DERIVED_COVERAGE_OR_WORK_WAIT, SelectionFence, SelectionFenceAdmission,
    SelectionFenceDisposition, SeparateReplayCommit, SeparateReplayCommitter,
    SharedDriverReadAdmission, TaskOutcome, TaskOutcomeError, TaskOutcomeSender,
    allocate_push_epoch_blob_and_counters, retain_sequencer_after_slot_release,
    selection_fence_disposition_for_commands, task_outcome_channel,
    validate_inert_mutation_generation_folding,
};
#[cfg(any(feature = "objectlog", test))]
use fireweed_engine::{
    ClaimCompatibility, GENERATION_MAX_ITEMS, MutationDriverSnapshot, PreparedMutationGeneration,
    PreparedPush,
};
use fireweed_projection::InMemoryProjection;
#[cfg(feature = "objectlog")]
use fireweed_turso::materialize_grouped_cohort_claimed_on;
use fireweed_turso::{TursoConfig, TursoRelational};

#[cfg(feature = "objectlog")]
use fireweed_objectlog::{
    AsyncProjectionApplyCoordinator, ObjectLogEngineStore, ObjectLogTaskDispatcher,
    PackedAppendError, PackedAppendOutcome, claim_only_tail, flush_config_from_segment,
    map_submit_error,
};

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
    TursoRelational::open(TursoConfig::local(path).with_log_backed_projection())
        .await
        .map_err(|e| EngineError::Storage(e.to_string()))
}

pub fn open_turso_projection(path: &Path) -> EngineResult<TursoRelational> {
    let path = path.to_path_buf();
    block_on_turso(async move { open_turso_projection_async(&path).await })
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

fn map_coord(error: CoordinationError) -> EngineError {
    EngineError::Backpressure {
        resource: error.resource(),
    }
}

#[derive(Clone, Default)]
#[cfg(feature = "objectlog")]
struct QueueFrontiers {
    last_claim: Option<CommandPosition>,
    last_candidate_mutation: Option<CommandPosition>,
}

#[cfg(feature = "objectlog")]
struct GenerationJoin {
    requests: Mutex<Vec<Arc<MutationGenerationWork>>>,
    notify: tokio::sync::Notify,
    outcome: Mutex<Option<EngineResult<Vec<fireweed_engine::MutationGenerationMember>>>>,
}

#[cfg(feature = "objectlog")]
impl GenerationJoin {
    fn member(
        &self,
        work: &MutationGenerationWork,
    ) -> Option<EngineResult<MutationGenerationMemberOutcome>> {
        self.outcome
            .lock()
            .expect("generation outcome")
            .as_ref()
            .map(|outcome| match outcome {
                Ok(members) => {
                    let requests = self.requests.lock().expect("generation request order");
                    requests
                        .iter()
                        .position(|request| std::ptr::eq(request.as_ref(), work))
                        .and_then(|index| members.get(index))
                        .map(|member| member.outcome.clone())
                        .ok_or_else(|| {
                            EngineError::Storage("mutation generation lost member outcome".into())
                        })
                }
                Err(error) => Err(error.clone()),
            })
    }
}

/// Post-apply send of a pre-materialized grouped/cohort envelope.
#[cfg(any(feature = "objectlog", test))]
fn finish_retained_grouped_cohort_response(claimed: Claimed) -> EngineResult<Claimed> {
    Ok(claimed)
}

/// Co-seal after the shared slot/connection is released. Sequencer remains held by the caller.
#[cfg(any(feature = "objectlog", test))]
fn finish_inert_mutation_generation_append(
    generation: &mut PreparedMutationGeneration<
        QueueKey,
        fireweed_engine::MutationSequencerKey,
        fireweed_engine::MutationGenerationWork,
    >,
) -> EngineResult<Vec<RawCommitRequest>> {
    debug_assert!(generation.slot_and_connection_released);
    Ok(std::mem::take(&mut generation.members)
        .into_iter()
        .filter_map(|member| match member.outcome {
            fireweed_engine::MutationGenerationMemberOutcome::Push(PreparedPush::Commit {
                request,
                ..
            }) => Some(request),
            fireweed_engine::MutationGenerationMemberOutcome::BatchUpdate { request, .. }
            | fireweed_engine::MutationGenerationMemberOutcome::Finalize { request }
            | fireweed_engine::MutationGenerationMemberOutcome::Singleton { request } => {
                Some(request)
            }
            fireweed_engine::MutationGenerationMemberOutcome::Claim { request, .. } => request,
            fireweed_engine::MutationGenerationMemberOutcome::Push(PreparedPush::Replay(_))
            | fireweed_engine::MutationGenerationMemberOutcome::PushAccepted
            | fireweed_engine::MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | fireweed_engine::MutationGenerationMemberOutcome::ItemMutation { .. }
            | fireweed_engine::MutationGenerationMemberOutcome::Rejected(_) => None,
        })
        .collect())
}

/// A short aggregation window lets compatible concurrent work share a generation.
/// Already collected local generations seal their append without another linger.
/// The former fixed 20 ms capped 100-row sequential batches at 5k records/s.
// Peer handlers may need several milliseconds to materialize a full request.
// Full generations start immediately; a bounded linger also amortizes log syncs
// for partially filled batches without changing FIFO admission.
#[cfg(feature = "objectlog")]
const MICROBATCH_LINGER: Duration = Duration::from_millis(10);

/// Reference size for the configured generation budget. Live reservations
/// must never be truncated merely to fit this estimate.
#[cfg(any(feature = "objectlog", test))]
fn claim_exclude_ids(snapshot: &MutationDriverSnapshot) -> Vec<ItemId> {
    let mut ids: Vec<ItemId> = snapshot
        .leased_ids
        .iter()
        .chain(snapshot.terminal_ids.iter())
        .copied()
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[cfg(feature = "objectlog")]
fn identity_base(definition: QueueDefinition) -> MutationDriverSnapshot {
    MutationDriverSnapshot {
        definition,
        paused_drain_intake: false,
        client_keys: HashSet::new(),
        request_fingerprints: HashMap::new(),
        unique_index_values: HashSet::new(),
        group_counts: HashMap::new(),
        batch_items: Vec::new(),
        leased_ids: HashSet::new(),
        terminal_ids: HashSet::new(),
    }
}

/// Copy only identity facts a generation can consult. The authoritative cache may
/// contain millions of keys; per-request validation must remain proportional to
/// this generation's input, not the retained queue history.
#[cfg(feature = "objectlog")]
fn identity_for_generation(
    existing: &MutationDriverSnapshot,
    definition: QueueDefinition,
    works: &[MutationGenerationWork],
) -> MutationDriverSnapshot {
    let mut snapshot = identity_base(definition);
    snapshot.paused_drain_intake = existing.paused_drain_intake;
    for work in works {
        let request_id = match work {
            MutationGenerationWork::Push { request, .. } => {
                for item in &request.items {
                    if let Some(key) = &item.client_item_key
                        && existing.client_keys.contains(key.as_str())
                    {
                        snapshot.client_keys.insert(key.as_str().to_string());
                    }
                    for (name, value) in &item.index_fields {
                        let encoded = format!("{name}={value:?}");
                        if existing.unique_index_values.contains(&encoded) {
                            snapshot.unique_index_values.insert(encoded);
                        }
                    }
                    if let Some(group) = &item.group_key
                        && let Some(count) = existing.group_counts.get(group.as_str())
                    {
                        snapshot
                            .group_counts
                            .insert(group.as_str().to_string(), *count);
                    }
                }
                request.request_id.as_ref()
            }
            MutationGenerationWork::BatchUpdate { request, .. } => Some(&request.request_id),
            _ => None,
        };
        if let Some(id) = request_id
            && let Some(fingerprint) = existing.request_fingerprints.get(id)
        {
            snapshot
                .request_fingerprints
                .insert(id.clone(), *fingerprint);
        }
    }
    snapshot
}

#[cfg(feature = "objectlog")]
fn command_kind(command: &QueueCommand) -> &'static str {
    match command {
        QueueCommand::CreateQueue(_) => "CreateQueue",
        QueueCommand::Push(_) => "Push",
        QueueCommand::Claim(_) => "Claim",
        QueueCommand::Finalize(_) => "Finalize",
        QueueCommand::UpdateFields(_) => "UpdateFields",
        QueueCommand::UpdateFieldsBatch(_) => "UpdateFieldsBatch",
        QueueCommand::MutateItems(_) => "MutateItems",
        QueueCommand::ReplacePending(_) => "ReplacePending",
        QueueCommand::PurgeItems(_) => "PurgeItems",
        QueueCommand::SetGates(_) => "SetGates",
        QueueCommand::WriteSideRecords(_) => "WriteSideRecords",
        QueueCommand::AdvanceInstanceFence(_) => "AdvanceInstanceFence",
        _ => "Other",
    }
}

fn unpublished_has_identity(snapshot: &MutationDriverSnapshot) -> bool {
    !snapshot.client_keys.is_empty()
        || !snapshot.request_fingerprints.is_empty()
        || !snapshot.unique_index_values.is_empty()
        || !snapshot.group_counts.is_empty()
        || !snapshot.batch_items.is_empty()
        || !snapshot.leased_ids.is_empty()
        || !snapshot.terminal_ids.is_empty()
}

#[cfg(feature = "objectlog")]
fn coalesce_generation_commits(
    commits: Vec<RawCommitRequest>,
) -> EngineResult<Vec<RawCommitRequest>> {
    if commits.len() <= 1 {
        return Ok(commits);
    }
    let mut groups: HashMap<
        (QueueKey, u64),
        (Vec<CommandEnvelope>, RawCommitFault, AppendAdmissionClass),
    > = HashMap::new();
    let mut order: Vec<(QueueKey, u64)> = Vec::new();
    for commit in commits {
        let (shard, commands, epoch, fault, admission) = commit.into_parts_with_append_admission();
        let key = (shard, epoch);
        match groups.get_mut(&key) {
            Some((existing, existing_fault, existing_admission)) => {
                if *existing_fault != fault || *existing_admission != admission {
                    return Err(EngineError::Storage(
                        "generation commits mixed fault or admission class".into(),
                    ));
                }
                existing.extend(commands);
            }
            None => {
                order.push(key.clone());
                groups.insert(key, (commands, fault, admission));
            }
        }
    }
    Ok(order
        .into_iter()
        .map(|key| {
            let (commands, fault, admission) = groups.remove(&key).expect("coalesced commit");
            RawCommitRequest::new(key.0, commands, key.1)
                .with_fault(fault)
                .with_append_admission(admission)
        })
        .collect())
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
            compose.contains("tick_turso_expired_leases") && compose.contains("tick_owned_reclaim"),
            "Turso reclaim ticks must own per-queue retry through the shared object-log driver"
        );
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

        let derived_generation = between(
            compose,
            "async fn drive_started_generation(",
            "async fn dispatch_claim(",
        );
        assert!(derived_generation.contains("AppendAdmissionClass::SharedSelectionLive"));

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
        assert!(push.contains("AppendAdmissionClass::SharedSelectionLive"));

        let class_s = between(
            derived,
            "async fn append_class_s_claim",
            "async fn dispatch_claim_legacy",
        );
        assert!(class_s.contains("AppendAdmissionClass::ClaimCoordinatorLive"));
        assert!(class_s.contains(".packed_append_owned("));

        let finalize = between(
            derived,
            "async fn dispatch_finalize",
            "fn create_queue_impl",
        );
        assert!(
            finalize.contains("drive_candidate_mutation"),
            "Complete/Fail must join the packed Update generation"
        );
        assert!(finalize.contains("AppendAdmissionClass::SelectionRequired"));
        assert!(
            !finalize.contains("AppendAdmissionClass::Bypass"),
            "Complete/Fail must not skip the selection fence via Bypass"
        );
    }

    #[test]
    fn grouped_cohort_claim_materializes_before_append() {
        use std::collections::BTreeMap;

        use fireweed_core::{ClientItemKey, CohortId, GroupKey, MetadataValue, TenantId, WorkerId};
        use fireweed_engine::{
            ClaimedItem, PreparedClaimedResult, QueueKey, finish_retained_grouped_cohort_claim,
        };

        let request = ClaimRequest {
            shard: QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap()),
            worker_id: WorkerId::new("worker").unwrap(),
            max_items: 2,
            lease_token: LeaseToken::new("token-grouped").unwrap(),
            lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
            now: UtcTimestamp::new(10, 0).unwrap(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(1),
        };
        let first = ItemId::mint(1, 1, 1);
        let second = ItemId::mint(1, 1, 2);
        let item = |id: ItemId, seq: u8| ClaimedItem {
            item_id: id,
            client_item_key: ClientItemKey::new(format!("key-{id}")).unwrap(),
            item_version: 2,
            priority: Some(PriorityValue::Int64(7)),
            group_key: Some(GroupKey::new("group-a").unwrap()),
            not_before: Some(UtcTimestamp::new(7, 0).unwrap()),
            lease_token: Some(request.lease_token.clone()),
            lease_expires_at: request.lease_expires_at,
            attempt_count: 3,
            max_attempts: 9,
            payload: Some(Bytes::from_static(b"\xCA\xFE")),
            fields: BTreeMap::from([("blob".to_string(), Bytes::from(vec![seq, 2, 3]))]),
            metadata: Metadata::from_entries(BTreeMap::from([(
                "source".to_string(),
                MetadataValue::String("grouped".to_string()),
            )])),
            gate_keys: vec!["gate-satisfied".to_string()],
            entity: Some(serde_json::json!({"kind": "stored", "rank": 7})),
        };
        let items = vec![item(first, 1), item(second, 2)];
        let grouped =
            PreparedClaimedResult::from_rendered(&request, &[first, second], items.clone(), None)
                .expect("grouped retained");
        let claimed = finish_retained_grouped_cohort_response(grouped.into_claimed())
            .expect("post-apply continuation");
        assert_eq!(claimed.items.len(), 2);
        assert_eq!(claimed.items[0].item_id, first);
        assert_eq!(claimed.items[1].item_id, second);
        assert_eq!(claimed.items[0].payload, items[0].payload);
        assert_eq!(claimed.items[0].fields, items[0].fields);
        assert_eq!(claimed.items[0].metadata, items[0].metadata);
        assert_eq!(claimed.items[0].entity, items[0].entity);
        assert_eq!(claimed.items[0].gate_keys, items[0].gate_keys);
        assert_eq!(claimed.items[0].not_before, items[0].not_before);
        assert_eq!(
            claimed.items[0].lease_token.as_ref(),
            Some(&request.lease_token)
        );
        assert!(claimed.cohort_id.is_none());

        let mut cohort_request = request.clone();
        cohort_request.compatibility.whole_cohort = true;
        let cohort_id = CohortId::new("coh:group-a:10000000000").unwrap();
        let shaped = finish_retained_grouped_cohort_claim(
            &cohort_request,
            &[first, second],
            items,
            Some(cohort_id.clone()),
        )
        .expect("cohort shape");
        let continued = finish_retained_grouped_cohort_response(
            PreparedClaimedResult::Retained(shaped).into_claimed(),
        )
        .expect("cohort continuation");
        assert!(
            continued
                .items
                .iter()
                .all(|item| item.lease_token.is_none())
        );
        assert_eq!(
            continued.cohort_lease_token.as_ref(),
            Some(&cohort_request.lease_token)
        );
        assert_eq!(continued.cohort_id.as_ref(), Some(&cohort_id));

        let compose_file = include_str!("turso_compose.rs");
        let (preamble, production) = compose_file
            .rsplit_once("// Atomic log-replay × Turso")
            .expect("production Turso composition boundary");
        let grouped = between(
            production,
            "async fn dispatch_grouped_cohort_claim(",
            "async fn append_class_s_claim(",
        );
        assert!(
            grouped.contains("claim_turns"),
            "grouped/cohort planning must take ClaimQueueTurn"
        );
        assert!(
            grouped.contains("claim_slots"),
            "grouped/cohort planning must take ClaimDriverReadAdmission"
        );
        assert!(
            grouped.contains("materialize_grouped_cohort_committed")
                || grouped.contains("PreparedClaimedResult"),
            "grouped/cohort must use the S3g retained carrier"
        );
        assert!(grouped.contains("finish_retained_grouped_cohort_response"));
        assert!(
            !grouped.contains("render_prepared_claim"),
            "grouped/cohort must not post-append render_prepared_claim"
        );
        assert!(
            grouped.contains("acquire_exclusive"),
            "grouped/cohort must take the exclusive selection fence"
        );
        let derived_impl = production
            .split("impl DerivedObjectLogTursoBackend {")
            .nth(1)
            .expect("derived Turso implementation");
        let item_claim = between(
            derived_impl,
            "async fn dispatch_claim(",
            "async fn dispatch_grouped_cohort_claim(",
        );
        assert!(
            item_claim.contains("drive_candidate_mutation"),
            "ordinary item Claim must admit onto the packed Update generation"
        );
        assert!(
            !item_claim.contains("claim_coordinator"),
            "ordinary item Claim must not use ClaimCoordinator"
        );
        assert!(
            !item_claim.contains("acquire_exclusive"),
            "ordinary item Claim shares the Update generation fence"
        );
        let realized = between(
            derived_impl,
            "async fn realize_accepted_claims(",
            "async fn dispatch_claim(",
        );
        assert!(
            realized.contains("authority_first: true"),
            "new item Claim envelopes must be authority-first"
        );
        assert!(
            realized.contains("item_claim_microbatch_on_serving_reader"),
            "ordinary item Claim selects applied rows from Turso"
        );
        assert!(
            !realized.contains("take_unpublished_pending"),
            "Claim must not fill a process-local unpublished row cache"
        );
        assert!(
            !realized.contains("QueueServingSet") && !realized.contains("take_eligible"),
            "ordinary item Claim must not keep a serving replica of every pending row"
        );
        let drive = between(
            derived_impl,
            "async fn drive_started_generation(",
            "async fn allocate_accepted_pushes(",
        );
        assert!(
            drive.contains("wait_selected_frontiers(&queue, claimed, claimed)"),
            "Claim SELECT waits for previous claim and push to apply"
        );
        assert!(
            drive.contains("claim_select.lock()"),
            "this process owns the Turso writer and sequences Claim SELECT in memory"
        );
        let continuation = between(
            preamble,
            "fn finish_retained_grouped_cohort_response(",
            "fn finish_inert_mutation_generation_append(",
        );
        assert!(continuation.contains("Ok(claimed)"));
        for needle in ["render_claimed", "render_prepared_claim", "self.projection"] {
            assert!(
                !continuation.contains(needle),
                "post-apply continuation must need no projection handle ({needle})"
            );
        }
    }

    #[test]
    fn mutation_generations_are_live_for_compatible_push_and_batch_update() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use fireweed_engine::{
            MutationDriverSnapshot, MutationGenerationKind, MutationGenerationMemberOutcome,
            MutationGenerationWork, MutationIngress, MutationSequencer, MutationSequencerKey,
            PushFingerprint, PushSpec, abort_unplanned_generation_on_deadline,
            retain_sequencer_after_slot_release, validate_inert_mutation_generation,
        };

        let compose_file = include_str!("turso_compose.rs");
        let (_, production) = compose_file
            .rsplit_once("// Atomic log-replay × Turso")
            .expect("production Turso composition boundary");
        assert!(
            production.contains("self.engine.push(request).await.map_err(map_push)"),
            "atomic products keep native push"
        );
        let derived_add = between(
            production,
            "async fn dispatch_push(",
            "async fn dispatch_batch_update(",
        );
        assert!(
            derived_add.contains("drive_candidate_mutation"),
            "add must admit then drive like modify"
        );
        assert!(
            !derived_add.contains("prepare_push("),
            "derived add must not prepare under the per-queue submit permit"
        );
        let atomic_push = between(
            production,
            "self.engine.push(request).await.map_err(map_push)",
            "async fn dispatch_claim(",
        );
        assert!(!atomic_push.contains("validate_inert_mutation_generation"));
        assert!(!atomic_push.contains("SharedDriverReadAdmission"));
        assert!(!atomic_push.contains("SelectionFence"));
        let derived_push = between(
            production,
            "async fn drive_candidate_mutation(",
            "async fn dispatch_claim(",
        );
        assert!(derived_push.contains("validate_inert_mutation_generation"));
        assert!(derived_push.contains("shared_slots"));
        assert!(derived_push.contains("sequencer"));
        assert!(
            derived_push.contains("allocate_accepted_pushes"),
            "add must allocate durable debt in the elected driver after accept"
        );
        assert!(
            derived_push.contains("applied_identity"),
            "produce identity must survive prune without a Turso snapshot"
        );
        assert!(
            derived_push.contains("needs_serving_snapshot"),
            "serving-reader snapshot is only for versioned BatchUpdate, not Push/pipelined Update"
        );
        assert!(
            derived_push.contains("MICROBATCH_LINGER"),
            "compatible inflight work must linger before start_generation so fill can reach 8"
        );
        assert!(
            derived_push.contains("coalesce_generation_commits"),
            "one generation must be one packed append, not N serial commits"
        );
        assert!(
            !derived_push.contains("unpublished_mutations"),
            "packed generations must not keep an unpublished identity overlay"
        );
        assert!(
            derived_push.contains("realize_accepted_claims"),
            "ordinary item Claim must realize on the same packed Update generation as field-update"
        );
        assert!(
            derived_push.contains("wait_selected_frontiers"),
            "the next generation waits for previous apply instead of an unpublished overlay"
        );
        assert!(
            derived_push.contains("if needs_serving_snapshot"),
            "keyed identity and addressed rewrites require projection coverage before snapshot validation"
        );
        assert!(
            derived_push.contains("acquire_shared"),
            "candidate mutations must take the shared selection fence"
        );

        let helper = between(
            compose_file,
            "fn finish_inert_mutation_generation_append(",
            "#[cfg(test)]",
        );
        assert!(helper.contains("slot_and_connection_released"));
        for needle in ["self.projection", "self.engine.push", "borrow_outcome"] {
            assert!(
                !helper.contains(needle),
                "inert generation append must not take serving I/O ({needle})"
            );
        }

        let shard = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let spec = PushSpec {
            client_item_key: Some(ClientItemKey::new("k").unwrap()),
            ..PushSpec::default()
        };
        let request = AsyncPushRequest {
            shard: shard.clone(),
            request_id: Some(RequestId::new("r1").unwrap()),
            items: vec![spec.clone()],
            now: UtcTimestamp::new(1, 0).unwrap(),
            expected_epoch: Some(1),
        };
        let work = MutationGenerationWork::Push {
            fingerprint: Some(PushFingerprint {
                canonical_sha256: [1; 32],
                legacy_body_hash: fireweed_core::BodyHash(1),
            }),
            request,
        };
        let snapshot = MutationDriverSnapshot {
            definition: QueueDefinition {
                tenant_id: TenantId::new("t").unwrap(),
                queue_id: QueueId::new("q").unwrap(),
                priority_model: fireweed_core::PriorityModel::timestamp_ascending(),
                ordering_mode: fireweed_core::OrderingMode::Strict,
                max_rank_error: 0,
                progress_bound_ms: 60_000,
                eligibility_policy: fireweed_core::EligibilityPolicy::default(),
                cohort_policy: None,
                recurrence: fireweed_core::RecurrencePolicy::default(),
                request_id_retention_ms: 60_000,
                client_item_key_retention_ms: 60_000,
                terminal_retention_ms: 60_000,
                max_lease_duration_ms: 60_000,
                retry_policy: fireweed_core::RetryPolicy { max_attempts: 3 },
                max_push_batch_size: 100,
                max_claim_batch_size: 100,
                max_eligible_group_size: None,
                secondary_indexes: Vec::new(),
                entity_schema: None,
                typed_indexes: Vec::new(),
                emit_change_records: false,
            },
            paused_drain_intake: false,
            client_keys: HashSet::new(),
            request_fingerprints: std::collections::HashMap::new(),
            unique_index_values: HashSet::new(),
            group_counts: std::collections::HashMap::new(),
            batch_items: Vec::new(),
            leased_ids: HashSet::new(),
            terminal_ids: HashSet::new(),
        };
        let sequencer =
            MutationSequencer::<QueueKey, MutationSequencerKey, MutationGenerationWork>::new();
        let ticket = sequencer
            .admit(
                shard.clone(),
                MutationSequencerKey::Compatible(MutationGenerationKind::Push),
                MutationIngress::Direct,
                Arc::new(work.clone()),
                1,
                1,
            )
            .expect("admit");
        let generation = sequencer.start_generation(&shard).expect("start");
        let members = validate_inert_mutation_generation(&snapshot, std::slice::from_ref(&work))
            .expect("validate");
        assert!(matches!(
            members[0].outcome,
            MutationGenerationMemberOutcome::PushAccepted
        ));
        drop(ticket);
        let mut prepared = retain_sequencer_after_slot_release(members, generation);
        let commits =
            finish_inert_mutation_generation_append(&mut prepared).expect("co-seal carrier");
        assert!(
            commits.is_empty(),
            "unprepared add must not append; the driver allocates after accept"
        );
        let expired = abort_unplanned_generation_on_deadline(
            Vec::<
                fireweed_engine::MutationTicket<
                    QueueKey,
                    MutationSequencerKey,
                    MutationGenerationWork,
                >,
            >::new(),
            Instant::now() - Duration::from_secs(1),
            Duration::ZERO,
        );
        let Err(error) = expired else {
            panic!("queued generation deadline must reject");
        };
        assert_eq!(
            error,
            EngineError::Backpressure {
                resource: "mutation sequencer wait",
            }
        );
    }

    #[test]
    fn public_reads_wait_then_take_outcome_admission_before_pool() {
        let compose_file = include_str!("turso_compose.rs");
        let (_, production) = compose_file
            .rsplit_once("// Atomic log-replay × Turso")
            .expect("production Turso composition boundary");
        let derived = between(
            production,
            "async fn acquire_outcome_read(",
            "async fn dispatch_push(",
        );
        assert!(derived.contains("wait_request_entry_coverage"));
        assert!(derived.contains("outcome_slots.acquire"));
        assert!(derived.contains("server_peek_committed"));
        assert!(derived.contains("server_pending_committed"));
        assert!(derived.contains("server_metrics_committed"));
        assert!(
            !derived.contains("self.projection.server_peek("),
            "public reads must not use the shared serving reader"
        );
        let derived_impl = production
            .split("impl DerivedObjectLogTursoBackend {")
            .nth(1)
            .expect("derived Turso implementation");
        let item_claim = between(
            derived_impl,
            "async fn dispatch_claim(",
            "async fn dispatch_grouped_cohort_claim(",
        );
        assert!(!item_claim.contains("class_s_claim_for_queue"));
        assert!(
            item_claim.contains("drive_candidate_mutation"),
            "ordinary item Claim must linger and pack via the mutation sequencer"
        );
        assert!(
            !item_claim.contains("acquire_exclusive"),
            "ordinary item Claim must not take the exclusive selection fence"
        );
        assert!(
            !item_claim.contains("wait_request_entry_coverage"),
            "item Claim must not wait apply before return; Complete uses remembered leases"
        );
        assert!(
            derived_impl.contains("remembered_claim_targets"),
            "Complete/renew must use process-owned lease tokens before waiting Turso apply"
        );
        let realized = between(
            derived_impl,
            "async fn realize_accepted_claims(",
            "async fn dispatch_claim(",
        );
        assert!(
            realized.contains("claim_exclude_ids"),
            "this generation's already-selected ids are excluded from later Claims in the same batch"
        );
        assert!(
            !realized.contains("take_unpublished_pending"),
            "Claim must not fill a process-local unpublished row cache"
        );
        assert!(
            !realized.contains("remembered_lease_ids"),
            "Claim SELECT must not bind the cumulative remembered lease set"
        );
        assert!(
            !realized.contains("item_id NOT IN"),
            "Claim SELECT must filter this-generation ids in memory, not SQL NOT IN"
        );
        assert!(
            !realized.contains("borrow_committed_driver_connection"),
            "ordinary item Claim must not borrow the 4 MiB driver pool"
        );
        assert!(
            !realized.contains("render_claimed") && !realized.contains("render_prepared_claim"),
            "item Claim continuation must not post-publication render"
        );
        let grouped = between(
            derived_impl,
            "async fn dispatch_grouped_cohort_claim(",
            "async fn append_class_s_claim(",
        );
        assert!(
            grouped.contains("self.claim_slots.acquire()"),
            "grouped/cohort Claim still takes ClaimDriverReadAdmission"
        );
    }
}

// ---------------------------------------------------------------------------
// Atomic log-replay × Turso (memory / postgres logs)
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
                | AppendAdmissionClass::SharedSelectionLive
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
    outcome_slots: OutcomeReadAdmission,
    commit_idempotency: CommitIdempotency,
}

impl<L> AtomicTursoBackend<L>
where
    L: AsyncLogStore + 'static,
{
    /// Shared authoritative log handle for log-derived delivery and diagnostics.
    #[doc(hidden)]
    pub fn log_store(&self) -> Arc<L> {
        Arc::clone(&self.log)
    }

    async fn wait_request_entry_coverage(&self, _shard: &QueueKey) -> EngineResult<()> {
        Ok(())
    }

    async fn committed_peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection.server_peek_committed(shard, limit).await
    }

    async fn committed_pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection.server_pending_committed(shard).await
    }

    async fn committed_pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection
            .server_pending_page_committed(shard, start, limit)
            .await
    }

    async fn committed_pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection
            .server_pending_range_committed(shard, start, end, consumer, limit)
            .await
    }

    async fn committed_pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection
            .server_pending_by_ids_committed(shard, ids)
            .await
    }

    async fn committed_retained_items(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<fireweed_engine::RetainedItemView>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection
            .server_retained_items_committed(shard, after, limit)
            .await
    }

    async fn committed_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection
            .server_live_items_committed(shard, keys)
            .await
    }

    async fn committed_metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        self.projection.server_metrics_committed(shard).await
    }

    async fn dispatch_batch_update(
        &self,
        shard: QueueKey,
        request: fireweed_engine::BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<fireweed_engine::BatchUpdateResponse> {
        let definition =
            AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
        if !definition.secondary_indexes.is_empty() {
            return self
                .parity_operation(&shard, expected_epoch, move |operation| {
                    Box::pin(operation.batch_update(request, now))
                })
                .await;
        }
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let id_generator = Arc::clone(&self.ids);
        let strategy = self.engine.commit_strategy();
        self.engine
            .submit_operation(shard.clone(), move || {
                Box::pin(async move {
                    use fireweed_engine::{
                        AsyncCommitStrategy, BatchUpdateItemRef, batch_update_body_hash,
                        plan_batch_update,
                    };

                    if request.updates.is_empty() {
                        return Err(EngineError::Invalid("empty batch update"));
                    }
                    if request.updates.len() > 1_000 {
                        return Err(EngineError::BatchTooLarge);
                    }

                    let definition =
                        AsyncControlPlane::queue_definition(control.as_ref(), shard.clone())
                            .await?;
                    let request_id = request.request_id.clone();
                    let fingerprint = batch_update_body_hash(&request)?;
                    if let Some(response) = projection
                        .batch_update_replay(&shard, &request_id, fingerprint, now)
                        .await?
                    {
                        return Ok(response);
                    }

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
                    let mut snapshot = projection.server_update_snapshot(&shard, &keys).await?;
                    snapshot.extend(
                        projection
                            .server_update_snapshot_by_ids(&shard, &ids)
                            .await?,
                    );
                    snapshot.sort_unstable_by_key(|item| item.item_id);
                    snapshot.dedup_by_key(|item| item.item_id);
                    let plan = plan_batch_update(&definition, true, request.updates, snapshot);
                    let updates: Vec<_> = plan
                        .commands
                        .into_iter()
                        .map(|(_idx, update)| update)
                        .collect();
                    let response = fireweed_engine::BatchUpdateResponse {
                        request_id: request_id.clone(),
                        results: plan.outcomes,
                    };
                    {
                        let item_ids: Vec<_> = updates
                            .iter()
                            .map(|u| u.item_id)
                            .filter(|id| id.as_u64() != 0)
                            .collect();
                        let envelope = CommandEnvelope {
                            command_id: id_generator.next_command_id(),
                            request_id: Some(request_id),
                            request_fingerprint: Some(fingerprint.0),
                            request_outcome: Some(RequestOutcome::BatchUpdate {
                                response_payload: serde_json::to_string(&response)
                                    .map_err(|error| EngineError::Storage(error.to_string()))?,
                            }),
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
                                AsyncLogStore::current_epoch(log.as_ref(), shard.clone()).await?
                            }
                        };
                        let committed = strategy
                            .commit(
                                RawCommitRequest::new(shard, vec![envelope], epoch)
                                    .with_append_admission(AppendAdmissionClass::AtomicNative),
                            )
                            .await;
                        committed?;
                    }
                    Ok(response)
                })
            })
            .await
            .map_err(|error| map_submit("batch update", error))?
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
            outcome_slots: OutcomeReadAdmission::default(),
            commit_idempotency: new_commit_idempotency(),
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
            let repair_push_receipts = self.projection.has_legacy_push_fingerprints(&shard).await?;
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
                if repair_push_receipts {
                    self.projection
                        .repair_legacy_push_fingerprints(&page.entries)
                        .await?;
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
        // Atomic lifecycle validation needs the actual row version. The bearer
        // cache does not carry versions and cannot stand in for a claimed row.
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

#[cfg(feature = "objectlog")]
struct TursoExpiredScan {
    shards: Vec<QueueKey>,
    index: usize,
}

#[cfg(feature = "objectlog")]
async fn next_turso_expired_page(
    projection: &TursoRelational,
    scan: &tokio::sync::Mutex<TursoExpiredScan>,
    now: UtcTimestamp,
) -> EngineResult<ExpiredLeasePage> {
    let limit = fireweed_objectlog::EXPIRED_LEASE_SCAN_LIMIT;
    let mut leases = Vec::new();
    let mut rows = 0usize;
    loop {
        let shard = {
            let mut scan = scan.lock().await;
            if scan.index >= scan.shards.len() || rows >= limit {
                let next = (scan.index < scan.shards.len()).then(|| {
                    ExpiredLeaseCursor::from_row(0, &scan.shards[scan.index], &ItemId::from_u64(0))
                });
                return Ok(ExpiredLeasePage { leases, next });
            }
            let shard = scan.shards[scan.index].clone();
            scan.index += 1;
            shard
        };
        let remaining = limit.saturating_sub(rows);
        let ids =
            AsyncProjectionStore::expired_leases(projection, shard.clone(), now, remaining).await?;
        if ids.is_empty() {
            continue;
        }
        rows = rows.saturating_add(ids.len());
        leases.push((shard, ids));
    }
}

async fn tick_turso_expired_leases<B>(
    projection: Arc<TursoRelational>,
    control: Arc<InProcessControlPlane>,
    backend: &B,
    now: UtcTimestamp,
) -> EngineResult<TickReport>
where
    B: ReclaimPort + Sync,
{
    #[cfg(feature = "objectlog")]
    {
        let definitions = AsyncProjectionStore::recover_definitions(projection.as_ref()).await?;
        let mut shards = definitions
            .into_iter()
            .map(|definition| QueueKey::new(definition.tenant_id, definition.queue_id))
            .collect::<Vec<_>>();
        shards.sort();
        let scan = Arc::new(tokio::sync::Mutex::new(TursoExpiredScan {
            shards,
            index: 0,
        }));
        let outcome =
            fireweed_objectlog::tick_owned_reclaim(
                backend,
                now,
                {
                    let scan = Arc::clone(&scan);
                    let projection = Arc::clone(&projection);
                    move |_cursor| {
                        let scan = Arc::clone(&scan);
                        let projection = Arc::clone(&projection);
                        async move {
                            next_turso_expired_page(projection.as_ref(), scan.as_ref(), now).await
                        }
                    }
                },
                move |shard| {
                    let definition = ControlPlane::queue_definition(control.as_ref(), shard)?;
                    Ok(usize::try_from(definition.max_claim_batch_size).unwrap_or(usize::MAX))
                },
                1,
            )
            .await?;
        Ok(outcome.report)
    }
    #[cfg(not(feature = "objectlog"))]
    {
        let _ = (projection, control, backend, now);
        Ok(TickReport::default())
    }
}

macro_rules! impl_turso_product_ports {
    ($ty:ty, $durability:expr, $consistency:expr, $full_transition:expr) => {
        impl Backend for $ty {
            fn durability_class(&self) -> DurabilityClass {
                $durability
            }
            fn supports_gates(&self) -> bool {
                // The projection stores gate membership and enforces logged gate changes.
                true
            }
            fn commit_capabilities(&self) -> fireweed_engine::CommitCapabilities {
                fireweed_engine::CommitCapabilities {
                    atomic_transition_commit: $full_transition,
                    vectorized_commit: $full_transition,
                    lease_validation: true,
                    retained_commit_idempotency: $full_transition,
                    non_work_side_records: $full_transition,
                    authoritative_recovery_reads: true,
                    delayed_awaits_timers: $full_transition,
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

        impl fireweed_engine::RecoveryReadPort for $ty {
            fn explain_commit(&self, shard: &QueueKey, request_id: RequestId)
                -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::CommitRecovery>>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    fireweed_engine::commit_surface::explain_commit_if_authoritative(
                        true, self.projection.as_ref(), &self.commit_idempotency, shard, request_id,
                    ).await
                }
            }
            fn side_records_by_prefix(&self, shard: &QueueKey, prefix: &[u8], page_size: usize, cursor: Option<Vec<u8>>)
                -> impl std::future::Future<Output = EngineResult<fireweed_engine::SideRecordPage>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    AsyncProjectionStore::side_records_by_prefix(self.projection.as_ref(), shard.clone(), prefix.to_vec(), page_size, cursor).await
                }
            }
            fn side_record(&self, shard: &QueueKey, key: &[u8])
                -> impl std::future::Future<Output = EngineResult<Option<Bytes>>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    AsyncProjectionStore::side_record(self.projection.as_ref(), shard.clone(), key.to_vec()).await
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
                    self.dispatch_batch_update(shard, request, now, expected_epoch)
                        .await
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
                    // Renew's engine validator reads the projection. A remembered bearer
                    // can precede its async claim apply, so establish coverage first.
                    self.wait_request_entry_coverage(shard).await?;
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
                    self.wait_request_entry_coverage(shard).await?;
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
                let item = PushItem {
                    client_item_key: client_item_key.clone(), item_id: ItemId::from_u64(0),
                    priority, group_key, not_before, payload, fields, metadata, entity_document: entity,
                    max_attempts: 0, cohort_size: None, gate_keys: vec![], index_fields: Default::default(),
                };
                self.parity_operation(shard, expected_epoch, move |operation| Box::pin(operation.upsert(item, now)))
            }
        }

        impl UpdateFieldsPort for $ty {
            fn update_fields(
                &self, shard: &QueueKey, item_id: ItemId,
                field_ops: BTreeMap<String, Option<Bytes>>, payload: fireweed_engine::PayloadUpdate,
                entity: Option<serde_json::Value>, expected_item_version: Option<u64>,
                now: UtcTimestamp, expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
                let command = fireweed_engine::UpdateFieldsCommand {
                    item_id, field_ops, payload, set_entity_document: entity, ..Default::default()
                };
                self.parity_operation(shard, expected_epoch, move |operation| {
                    Box::pin(operation.update_fields(command, expected_item_version, now))
                })
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
                    self.wait_request_entry_coverage(shard).await?;
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
                now: UtcTimestamp,
            ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
                async move {
                    tick_turso_expired_leases(
                        Arc::clone(&self.projection),
                        Arc::clone(&self.control),
                        self,
                        now,
                    )
                    .await
                }
            }
        }

        impl SetGatesPort for $ty {
            fn set_gates(&self, shard: &QueueKey, command: fireweed_engine::SetGatesCommand,
                now: UtcTimestamp, expected_epoch: Option<u64>)
                -> impl std::future::Future<Output = EngineResult<()>> + Send {
                self.parity_operation(shard, expected_epoch, move |operation| Box::pin(operation.set_gates(command, now)))
            }
        }
        impl fireweed_engine::ReschedulePort for $ty {
            fn reschedule(&self, shard: &QueueKey, item_id: ItemId,
                set_priority: fireweed_engine::ScheduleUpdate<PriorityValue>,
                set_not_before: fireweed_engine::ScheduleUpdate<UtcTimestamp>,
                expected_item_version: Option<u64>, now: UtcTimestamp, expected_epoch: Option<u64>)
                -> impl std::future::Future<Output = EngineResult<u64>> + Send {
                let command = fireweed_engine::UpdateFieldsCommand { item_id, set_priority, set_not_before, ..Default::default() };
                self.parity_operation(shard, expected_epoch, move |operation| Box::pin(operation.update_fields(command, expected_item_version, now)))
            }
        }
        impl fireweed_engine::DiscoveryPort for $ty {
            fn discover_active_scopes(&self, shard: &QueueKey, granularity: fireweed_engine::DiscoveryGranularity, now: UtcTimestamp)
                -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::ActiveScope>>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_discover_active_scopes(shard, granularity, now).await
                }
            }
        }
        impl HotProjectionQueryPort for $ty {
            fn hot_projection_capabilities(&self, _shard: &QueueKey) -> QueryCapabilityFlags {
                QueryCapabilityFlags { range_scan: true, grouped_aggregate: true, declared_bucket_segment: true,
                    bounded_mutation: true, claim_by_query: true, claim_by_item_ids: true, side_record_query: false }
            }
            fn range_scan(&self, shard: &QueueKey, request: fireweed_core::RangeScanRequest)
                -> impl std::future::Future<Output = EngineResult<fireweed_core::RangeScanResponse>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_range_scan(shard, request).await
                }
            }
            fn grouped_aggregate(&self, shard: &QueueKey, request: fireweed_core::GroupedAggregateRequest)
                -> impl std::future::Future<Output = EngineResult<fireweed_core::GroupedAggregateResponse>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_grouped_aggregate(shard, request).await
                }
            }
            fn metrics_by_query(&self, shard: &QueueKey, request: fireweed_core::MetricsByQueryRequest)
                -> impl std::future::Future<Output = EngineResult<fireweed_engine::QueueMetrics>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_metrics_by_query(shard, request).await
                }
            }
            fn declared_bucket_segment(&self, shard: &QueueKey, request: fireweed_core::DeclaredBucketSegmentRequest)
                -> impl std::future::Future<Output = EngineResult<fireweed_core::DeclaredBucketSegmentResponse>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_declared_bucket_segment(shard, request).await
                }
            }
            fn bounded_mutation(&self, shard: &QueueKey, request: fireweed_core::BoundedMutationRequest, context: fireweed_engine::BoundedMutationContext)
                -> impl std::future::Future<Output = EngineResult<fireweed_core::BoundedMutationResponse>> + Send {
                self.parity_operation(shard, context.expected_epoch, move |operation| Box::pin(operation.bounded_mutation(request, context.now)))
            }
            fn claim_by_query(&self, shard: &QueueKey, request: fireweed_core::ClaimByQueryRequest, context: fireweed_engine::ClaimByQueryContext)
                -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
                self.parity_operation(shard, context.expected_epoch, move |operation| Box::pin(operation.claim_by_query(request, context)))
            }
            fn claim_by_item_ids(&self, shard: &QueueKey, request: fireweed_core::ClaimByItemIdsRequest, context: fireweed_engine::ClaimByQueryContext)
                -> impl std::future::Future<Output = EngineResult<fireweed_engine::ClaimByItemIdsResponse>> + Send {
                self.parity_operation(shard, context.expected_epoch, move |operation| Box::pin(operation.claim_by_item_ids(request, context)))
            }
        }
        impl IndexQueryPort for $ty {
            fn index_get_unique(&self, shard: &QueueKey, index: &str, key: &[Vec<u8>])
                -> impl std::future::Future<Output = EngineResult<Option<fireweed_engine::IndexHit>>> + Send {
                async move { self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_index_get_unique(shard, index, key).await }
            }
            fn index_lookup(&self, shard: &QueueKey, index: &str, key: &[Vec<u8>])
                -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::IndexHit>>> + Send {
                async move { self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_index_lookup(shard, index, key).await }
            }
        }

        impl ItemMutationPort for $ty {
            fn mutate_items(
                &self,
                shard: &QueueKey,
                request: ItemMutationRequest,
                expected_epoch: Option<u64>,
            ) -> impl std::future::Future<Output = EngineResult<ItemMutationResponse>> + Send {
                self.dispatch_item_mutation(shard, request, expected_epoch)
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
                self.committed_peek(shard, limit)
            }
            fn pending(
                &self,
                shard: &QueueKey,
            ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
                self.committed_pending(shard)
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
                self.committed_pending_page(shard, start, limit)
            }
            fn pending_range(
                &self,
                shard: &QueueKey,
                start: Option<ItemId>,
                end: Option<ItemId>,
                consumer: Option<&LeaseToken>,
                limit: usize,
            ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
                self.committed_pending_range(shard, start, end, consumer, limit)
            }
            fn pending_by_ids(
                &self,
                shard: &QueueKey,
                ids: &[ItemId],
            ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
                self.committed_pending_by_ids(shard, ids)
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
            fn retained_items(
                &self, shard: &QueueKey, after: Option<ItemId>, limit: usize,
            ) -> impl std::future::Future<Output = EngineResult<Vec<fireweed_engine::RetainedItemView>>> + Send {
                self.committed_retained_items(shard, after, limit)
            }
            fn live_items(
                &self,
                shard: &QueueKey,
                keys: &[ClientItemKey],
            ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send
            {
                self.committed_live_items(shard, keys)
            }
            fn metrics(
                &self,
                shard: &QueueKey,
            ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
                self.committed_metrics(shard)
            }
            fn terminal_emission_metrics(
                &self, shard: &QueueKey, now: UtcTimestamp, emit_change_records: bool,
                emission_cursor: Option<&CommandPosition>,
            ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
                async move {
                    self.wait_request_entry_coverage(shard).await?;
                    self.projection.server_terminal_emission_metrics_at(shard, now, emit_change_records, emission_cursor).await
                }
            }
        }
    };
}

macro_rules! impl_turso_commit_transition {
    ($ty:ty) => {
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
                self.parity_operation(shard, expected_epoch, move |operation| {
                    Box::pin(operation.transition(transition, now))
                })
            }
        }
    };
}

impl_turso_product_ports!(
    AtomicTursoBackend<InProcessLogStore<fireweed_projection::MemoryLog>>,
    DurabilityClass::Atomic,
    "atomic log batch with synchronous Turso apply",
    true
);
impl_turso_commit_transition!(
    AtomicTursoBackend<InProcessLogStore<fireweed_projection::MemoryLog>>
);

#[cfg(feature = "postgres")]
impl_turso_product_ports!(
    AtomicTursoBackend<InProcessLogStore<fireweed_postgres::PostgresLog>>,
    DurabilityClass::Atomic,
    "atomic log batch with synchronous Turso apply",
    true
);
#[cfg(feature = "postgres")]
impl_turso_commit_transition!(
    AtomicTursoBackend<InProcessLogStore<fireweed_postgres::PostgresLog>>
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
    selection_fence: SelectionFence<QueueKey>,
    fence_admission: SelectionFenceAdmission,
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
        let selection_fence = self.selection_fence.clone();
        let fence_admission = self.fence_admission.clone();
        Box::pin(async move {
            let (shard, commands, expected_epoch, fault, append_admission) =
                request.into_parts_with_append_admission();
            match append_admission {
                AppendAdmissionClass::NonDerived
                | AppendAdmissionClass::KeyedPermitLive
                | AppendAdmissionClass::SelectionRequired
                | AppendAdmissionClass::SharedSelectionLive
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::RecoveryOnly
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
            let _shared_fence = match (
                append_admission,
                selection_fence_disposition_for_commands(
                    commands.iter().map(|envelope| &envelope.command),
                ),
            ) {
                (
                    AppendAdmissionClass::KeyedPermitLive | AppendAdmissionClass::SelectionRequired,
                    SelectionFenceDisposition::Shared,
                ) => {
                    let _waiter = fence_admission.admit_waiter().map_err(map_coord)?;
                    Some(
                        selection_fence
                            .acquire_shared(shard.clone())
                            .await
                            .map_err(map_coord)?,
                    )
                }
                _ => None,
            };
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
            let force_seal = (log.uses_local_filesystem()
                && matches!(
                    append_admission,
                    AppendAdmissionClass::SharedSelectionLive
                        | AppendAdmissionClass::KeyedPermitLive
                ))
                || commands.len() >= CLAIM_GENERATION_MAX_REQUESTS
                || commands
                    .iter()
                    .map(|envelope| {
                        envelope
                            .item_ids
                            .iter()
                            .filter(|id| id.as_u64() != 0)
                            .count()
                    })
                    .sum::<usize>()
                    >= GENERATION_MAX_ITEMS;
            let outcome = match log
                .packed_append_owned(
                    shard.clone(),
                    commands.clone(),
                    expected_epoch,
                    reservation.as_ref().map(|reserved| reserved.id()),
                    force_seal,
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(dispose_packed_append_error(
                        async_apply.as_ref(),
                        reservation,
                        error,
                    )
                    .await);
                }
            };
            note_produce_positions(&last_produce, &outcome.positions, &commands).await;
            if matches!(fault, RawCommitFault::AfterAppendBeforeApply) {
                // Successful append allocated a position. Skip apply, latch poison,
                // and leave the reservation outstanding — never cancel durable work.
                // Read-side poison visibility stays deferred to S3c.
                if let Some(coordinator) = async_apply.as_ref() {
                    coordinator
                        .latch_poison(
                            shard,
                            "fault-injection: AfterAppendBeforeApply after durable append".into(),
                        )
                        .await;
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

#[cfg(any(feature = "objectlog", test))]
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
                .transfer_followers_and_recharge(
                    &reservation,
                    &batch.transferred_reservation_ids,
                    &batch.commands,
                )
                .await?;
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
        if coordinator.is_none()
            && let Some(apply_turn) = apply_turn
        {
            wait_turso_apply_turn(projection, shard, &positions, apply_turn).await?;
        }
    }
    Ok(positions)
}

#[cfg(feature = "objectlog")]
async fn dispose_packed_append_error(
    coordinator: Option<&AsyncProjectionApplyCoordinator<TursoRelational>>,
    reservation: Option<fireweed_objectlog::AsyncProjectionApplyReservation>,
    error: PackedAppendError,
) -> EngineError {
    match error {
        PackedAppendError::BeforePosition(inner) => {
            if let (Some(coordinator), Some(reservation)) = (coordinator, reservation) {
                coordinator.cancel(reservation).await;
            }
            inner
        }
        PackedAppendError::PostPositionAmbiguous { shard, reason } => {
            if let Some(coordinator) = coordinator {
                coordinator
                    .latch_poison(shard.clone(), reason.clone())
                    .await;
            }
            PackedAppendError::PostPositionAmbiguous { shard, reason }.into_engine()
        }
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

/// Coordinator-owned active-driver registry.
///
/// `ObjectLogTaskDispatcher::drain` resolves immediately, so the Turso product tracks dispatched
/// append/apply work here and waits for those registrations through publication.
#[cfg(feature = "objectlog")]
#[derive(Clone)]
struct CoordinatorDriverRegistry {
    inner: Arc<Mutex<CoordinatorDriverState>>,
}

#[cfg(feature = "objectlog")]
struct CoordinatorDriverState {
    closed: bool,
    next_id: u64,
    drivers: HashMap<u64, ()>,
    drainers: Vec<TaskOutcomeSender<()>>,
}

/// RAII registration held through append/apply publication.
#[cfg(feature = "objectlog")]
pub(crate) struct RegisteredDriver {
    inner: Arc<Mutex<CoordinatorDriverState>>,
    id: u64,
}

#[cfg(feature = "objectlog")]
impl CoordinatorDriverRegistry {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CoordinatorDriverState {
                closed: false,
                next_id: 0,
                drivers: HashMap::new(),
                drainers: Vec::new(),
            })),
        }
    }

    fn register(&self) -> Result<RegisteredDriver, DispatchError> {
        let mut state = self
            .inner
            .lock()
            .expect("coordinator driver registry poisoned");
        if state.closed {
            return Err(DispatchError::Closed);
        }
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.drivers.insert(id, ());
        Ok(RegisteredDriver {
            inner: Arc::clone(&self.inner),
            id,
        })
    }

    fn close(&self) {
        self.inner
            .lock()
            .expect("coordinator driver registry poisoned")
            .closed = true;
    }

    fn drain(&self) -> TaskOutcome<()> {
        let (sender, outcome) = task_outcome_channel();
        let mut state = self
            .inner
            .lock()
            .expect("coordinator driver registry poisoned");
        if state.drivers.is_empty() {
            sender.send(());
        } else {
            state.drainers.push(sender);
        }
        outcome
    }
}

#[cfg(feature = "objectlog")]
impl Drop for RegisteredDriver {
    fn drop(&mut self) {
        let drainers = {
            let mut state = self
                .inner
                .lock()
                .expect("coordinator driver registry poisoned");
            state.drivers.remove(&self.id);
            if state.drivers.is_empty() {
                std::mem::take(&mut state.drainers)
            } else {
                Vec::new()
            }
        };
        for drainer in drainers {
            drainer.send(());
        }
    }
}

/// Dispatched object-log work is registered independently of the best-effort dispatcher drain.
#[cfg(feature = "objectlog")]
struct CoordinatorOwnedDispatcher {
    inner: ObjectLogTaskDispatcher,
    registry: CoordinatorDriverRegistry,
}

#[cfg(feature = "objectlog")]
impl CoordinatorOwnedDispatcher {
    fn new() -> Self {
        Self {
            inner: ObjectLogTaskDispatcher::new(),
            registry: CoordinatorDriverRegistry::new(),
        }
    }

    fn registry(&self) -> CoordinatorDriverRegistry {
        self.registry.clone()
    }
}

#[cfg(feature = "objectlog")]
impl OwnedTaskDispatcher for CoordinatorOwnedDispatcher {
    fn submit<T: Send + 'static>(
        &self,
        factory: OwnedTaskFactory<T>,
    ) -> Result<TaskOutcome<T>, DispatchError> {
        let driver = self.registry.register()?;
        self.inner.submit(Box::new(move || {
            let work = factory();
            Box::pin(async move {
                let _driver = driver;
                work.await
            })
        }))
    }

    fn close(&self) {
        self.registry.close();
        self.inner.close();
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    fn drain(&self) -> TaskOutcome<()> {
        self.registry.drain()
    }
}

#[cfg(feature = "objectlog")]
type ObjectLogEngine = AsyncComposedBackend<
    SeparateReplayCommit<ObjectLogTursoCommitter>,
    CoordinatorOwnedDispatcher,
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

#[cfg(feature = "objectlog")]
type GenerationJoins = Arc<Mutex<HashMap<(QueueKey, u64), Arc<GenerationJoin>>>>;
#[cfg(all(feature = "objectlog", test))]
type MetricsSnapshotHook = Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>;

/// Provider-neutral object-log × Turso product (not a public `ObjectLogTursoBackend` alias).
#[cfg(feature = "objectlog")]
#[derive(Clone)]
pub struct DerivedObjectLogTursoBackend {
    engine: Arc<ObjectLogEngine>,
    log: Arc<ObjectLogEngineStore>,
    projection: Arc<TursoRelational>,
    #[allow(dead_code)]
    projection_path: PathBuf,
    control: Arc<InProcessControlPlane>,
    ids: Arc<SeqIdGen>,
    /// Shared with push planners; recovery observes recovered item ids into this map.
    counters: Arc<QueueCounters>,
    node_id: u8,
    commit_idempotency: CommitIdempotency,
    async_apply: Option<AsyncProjectionApplyCoordinator<TursoRelational>>,
    last_produce: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
    produce_caught_up: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
    frontiers: Arc<tokio::sync::Mutex<HashMap<QueueKey, QueueFrontiers>>>,
    applied_identity: Arc<tokio::sync::Mutex<HashMap<QueueKey, MutationDriverSnapshot>>>,
    sequencer: MutationSequencer<QueueKey, MutationSequencerKey, MutationGenerationWork>,
    claim_turns: ClaimQueueTurn<QueueKey>,
    claim_slots: ClaimDriverReadAdmission,
    shared_slots: SharedDriverReadAdmission,
    outcome_slots: OutcomeReadAdmission,
    #[cfg(test)]
    metrics_snapshot_hook: MetricsSnapshotHook,
    selection_fence: SelectionFence<QueueKey>,
    fence_admission: SelectionFenceAdmission,
    generation_joins: GenerationJoins,
    claim_work_ids: Arc<AtomicU64>,
    /// This process owns the Turso writer. Item Claim SELECT and the FIFO
    /// rowid floor are sequenced here so the next generation can read the
    /// following slice without waiting for apply.
    claim_select: Arc<tokio::sync::Mutex<()>>,
    #[allow(dead_code)] // S4b test hook: dropping_objectlog_turso_drains_registered_driver
    drivers: CoordinatorDriverRegistry,
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
        let frontiers = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let applied_identity = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let commit_idempotency = new_commit_idempotency();
        let selection_fence = SelectionFence::default();
        let fence_admission = SelectionFenceAdmission::new(1_024);
        let committer = ObjectLogTursoCommitter {
            log: Arc::clone(&log),
            projection: Arc::clone(&projection),
            apply_turn: Arc::new(tokio::sync::Notify::new()),
            async_apply: async_apply.clone(),
            last_produce: Arc::clone(&last_produce),
            selection_fence: selection_fence.clone(),
            fence_admission: fence_admission.clone(),
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
        let dispatcher = CoordinatorOwnedDispatcher::new();
        let drivers = dispatcher.registry();
        let engine =
            AsyncComposedBackend::new_with_planners(strategy, dispatcher, claim, push, 1024)
                .with_lifecycle_planner(lifecycle)
                .with_reclaim_planner(reclaim)
                .with_append_admission(AppendAdmissionClass::KeyedPermitLive);

        let backend = Self {
            engine: Arc::new(engine),
            log,
            projection,
            projection_path,
            control,
            ids,
            counters,
            node_id,
            commit_idempotency,
            async_apply,
            last_produce,
            produce_caught_up,
            frontiers,
            applied_identity,
            sequencer: MutationSequencer::new(),
            claim_turns: ClaimQueueTurn::default(),
            claim_slots: ClaimDriverReadAdmission::default(),
            shared_slots: SharedDriverReadAdmission::default(),
            outcome_slots: OutcomeReadAdmission::default(),
            #[cfg(test)]
            metrics_snapshot_hook: Arc::new(Mutex::new(None)),
            selection_fence,
            fence_admission,
            generation_joins: Arc::new(Mutex::new(HashMap::new())),
            claim_work_ids: Arc::new(AtomicU64::new(1)),
            claim_select: Arc::new(tokio::sync::Mutex::new(())),
            drivers,
        };
        backend.recover_async().await?;
        Ok(backend)
    }

    /// Register in-flight coordinator work that must complete through append/apply publication.
    #[allow(dead_code)] // S4b test hook: dropping_objectlog_turso_drains_registered_driver
    pub(crate) fn register_driver(&self) -> Result<RegisteredDriver, DispatchError> {
        self.drivers.register()
    }

    /// Close admission and wait registered drivers through append/apply publication.
    pub async fn close_and_drain(&self) -> EngineResult<()> {
        self.sequencer.close();
        self.engine
            .close_and_drain()
            .await
            .map_err(|error: TaskOutcomeError| {
                EngineError::Storage(format!(
                    "object-log turso coordinator drain failed: {error:?}"
                ))
            })
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
            coordinator
                .wait_until_covers(shard, target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
                .await?;
        }
    }

    async fn recover_async(&self) -> EngineResult<()> {
        let mut definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        if definitions.is_empty() {
            definitions =
                AsyncProjectionStore::recover_definitions(self.projection.as_ref()).await?;
        }
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let _ =
                AsyncControlPlane::create_queue(self.control.as_ref(), definition.clone()).await;
            AsyncProjectionStore::ensure_shard(self.projection.as_ref(), definition.clone())
                .await?;
            let high_water = self.projection.writer_recovery_high_water(&shard).await?;
            let repair_push_receipts = self.projection.has_legacy_push_fingerprints(&shard).await?;
            let mut from = None;
            // Full rebuild on a live Turso connection cannot apply mixed
            // Push/UpdateFields/MutateItems in one recovery transaction.
            let page_size = if high_water.is_none() { 1 } else { 256 };
            loop {
                let page = AsyncLogStore::read_from(
                    self.log.as_ref(),
                    shard.clone(),
                    from.clone(),
                    page_size,
                )
                .await?;
                if page.entries.is_empty() {
                    break;
                }
                if repair_push_receipts {
                    self.projection
                        .repair_legacy_push_fingerprints(&page.entries)
                        .await?;
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
                    .await
                    .map_err(|error| {
                        let kinds: Vec<_> = tail
                            .iter()
                            .map(|(position, envelope)| {
                                format!(
                                    "{}:{:?}",
                                    position.sequence,
                                    command_kind(&envelope.command)
                                )
                            })
                            .collect();
                        EngineError::Storage(format!(
                            "projection recover apply {}/{} commands [{}]: {error}",
                            shard.tenant_id.as_str(),
                            shard.queue_id.as_str(),
                            kinds.join(", ")
                        ))
                    })?;
                }
                match page.next {
                    Some(next) => from = Some(next),
                    None => break,
                }
            }
            if let Some(item_id) = self.projection.recovery_counter_high_water(&shard).await? {
                self.counters.observe(&shard, item_id);
            }
            let now = UtcTimestamp::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(1),
                0,
            )
            .unwrap_or_else(|_| UtcTimestamp::new(1, 0).expect("epoch"));
            let hydrated = self
                .projection
                .load_applied_identity(&shard, definition.clone(), now)
                .await?;
            if unpublished_has_identity(&hydrated) || hydrated.paused_drain_intake {
                self.applied_identity
                    .lock()
                    .await
                    .insert(shard.clone(), hydrated);
            }
            // Migration window: pre-upgrade SQL-first leases still publish here.
            self.drain_claim_outbox(&shard).await?;
            if let Some(coordinator) = &self.async_apply {
                let recovered = AsyncProjectionStore::recovery_high_water(
                    self.projection.as_ref(),
                    shard.clone(),
                )
                .await?;
                let log_hw = AsyncLogStore::high_water(self.log.as_ref(), shard.clone()).await?;
                if match (recovered.as_ref(), log_hw.as_ref()) {
                    (None, None) => true,
                    (Some(recovered), Some(log_hw)) => position_covers(Some(recovered), log_hw),
                    _ => false,
                } {
                    coordinator.seed_high_water(shard, recovered).await;
                }
            }
        }
        Ok(())
    }

    /// Publish pre-upgrade outbox leases. New serving writes no `fireweed_claim_outbox` rows.
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
                    authority_first: false,
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
                | AppendAdmissionClass::SharedSelectionLive
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
            debug_assert_eq!(fault, RawCommitFault::None);
            let outcome = self
                .log
                .packed_append(append_shard, commands, append_epoch)
                .await
                .map_err(PackedAppendError::into_engine)?;
            // Publish the newly logged migration claim before deleting its
            // outbox entry. Startup must seed a cursor covering this append;
            // otherwise every later dependent read waits for an unqueued apply.
            publish_packed_apply(None, None, outcome, self.projection.as_ref(), shard, None)
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
        if let Some(claimed) = self.projection.remembered_claim_targets(shard, ids).await {
            return Ok(claimed);
        }
        self.wait_request_entry_coverage(shard).await?;
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

    /// Observe provider-neutral selected-projection debt and watermark state.
    pub async fn async_projection_snapshot(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<fireweed_objectlog::AsyncProjectionApplySnapshot> {
        let coordinator = self.async_apply.as_ref().ok_or(EngineError::Invalid(
            "async-projection-control-requires-async-barrier",
        ))?;
        Ok(coordinator.snapshot(shard).await)
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

    fn ensure_generation_join(&self, queue: &QueueKey, generation_id: u64) -> Arc<GenerationJoin> {
        self.generation_joins
            .lock()
            .expect("generation join map")
            .entry((queue.clone(), generation_id))
            .or_insert_with(|| {
                Arc::new(GenerationJoin {
                    requests: Mutex::new(Vec::new()),
                    notify: tokio::sync::Notify::new(),
                    outcome: Mutex::new(None),
                })
            })
            .clone()
    }

    fn publish_generation_outcome(
        &self,
        queue: &QueueKey,
        generation_id: u64,
        join: &GenerationJoin,
        driven: EngineResult<Vec<fireweed_engine::MutationGenerationMember>>,
    ) {
        *join.outcome.lock().expect("generation outcome") = Some(driven);
        // Every admitted caller owns this cell. Completed payloads need no
        // global cache, and cannot be evicted out from under a delayed caller.
        self.generation_joins
            .lock()
            .expect("generation join map")
            .remove(&(queue.clone(), generation_id));
        join.notify.notify_waiters();
    }

    async fn wait_request_entry_coverage(&self, shard: &QueueKey) -> EngineResult<()> {
        let Some(coordinator) = &self.async_apply else {
            return Ok(());
        };
        coordinator.ensure_healthy(shard)?;
        let Some(target) = AsyncLogStore::high_water(self.log.as_ref(), shard.clone()).await?
        else {
            return Ok(());
        };
        coordinator
            .wait_until_covers(shard, &target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
            .await
    }

    async fn wait_queue_frontiers(&self, shard: &QueueKey) -> EngineResult<()> {
        self.wait_selected_frontiers(shard, true, true).await
    }

    async fn wait_selected_frontiers(
        &self,
        shard: &QueueKey,
        last_claim: bool,
        last_mutation: bool,
    ) -> EngineResult<()> {
        let Some(coordinator) = &self.async_apply else {
            return Ok(());
        };
        let frontiers = self
            .frontiers
            .lock()
            .await
            .get(shard)
            .cloned()
            .unwrap_or_default();
        let targets = [
            last_claim.then_some(frontiers.last_claim).flatten(),
            last_mutation
                .then_some(frontiers.last_candidate_mutation)
                .flatten(),
        ];
        for target in targets.into_iter().flatten() {
            coordinator
                .wait_until_covers(shard, &target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
                .await?;
        }
        Ok(())
    }

    async fn record_frontier(&self, shard: &QueueKey, claim: bool) -> EngineResult<()> {
        let Some(position) = AsyncLogStore::high_water(self.log.as_ref(), shard.clone()).await?
        else {
            return Ok(());
        };
        let mut frontiers = self.frontiers.lock().await;
        let entry = frontiers.entry(shard.clone()).or_default();
        if claim {
            entry.last_claim = Some(position);
        } else {
            entry.last_candidate_mutation = Some(position);
        }
        Ok(())
    }

    async fn acquire_outcome_read(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<fireweed_engine::SlotPermit> {
        self.wait_request_entry_coverage(shard).await?;
        self.outcome_slots.acquire().await.map_err(map_coord)
    }

    async fn committed_peek(&self, shard: &QueueKey, limit: usize) -> EngineResult<Vec<ItemView>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection.server_peek_committed(shard, limit).await
    }

    async fn committed_pending(&self, shard: &QueueKey) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection.server_pending_committed(shard).await
    }

    async fn committed_pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<PendingPage> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection
            .server_pending_page_committed(shard, start, limit)
            .await
    }

    async fn committed_pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection
            .server_pending_range_committed(shard, start, end, consumer, limit)
            .await
    }

    async fn committed_pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> EngineResult<Vec<LeaseView>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection
            .server_pending_by_ids_committed(shard, ids)
            .await
    }

    async fn committed_retained_items(
        &self,
        shard: &QueueKey,
        after: Option<ItemId>,
        limit: usize,
    ) -> EngineResult<Vec<fireweed_engine::RetainedItemView>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection
            .server_retained_items_committed(shard, after, limit)
            .await
    }

    async fn committed_live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> EngineResult<Vec<Option<LiveItemView>>> {
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection
            .server_live_items_committed(shard, keys)
            .await
    }

    async fn committed_metrics(&self, shard: &QueueKey) -> EngineResult<QueueMetrics> {
        // Opt-in diagnostic only: successful reads report exclusive phase times.
        // Keep the coverage contract and admission order unchanged.
        let trace = std::env::var_os("FIREWEED_METRICS_TRACE").is_some();
        let mut previous = trace.then(Instant::now);
        let mut phases = [0_u128; 11];
        let mut mark = |phase: usize| {
            if let Some(previous) = &mut previous {
                let now = Instant::now();
                phases[phase] = now.duration_since(*previous).as_micros();
                *previous = now;
            }
        };
        if let Some(coordinator) = &self.async_apply {
            coordinator.ensure_healthy(shard)?;
            let target = AsyncLogStore::high_water(self.log.as_ref(), shard.clone()).await?;
            mark(0);
            // Release the snapshot and read slot before a fallback coverage wait.
            // Counts and SQL frontier must never be read separately.
            {
                let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
                mark(1);
                let (mut metrics, applied) = self
                    .projection
                    .server_metrics_with_position_committed(shard)
                    .await?;
                mark(2);
                #[cfg(test)]
                {
                    let hook = self.metrics_snapshot_hook.lock().unwrap().take();
                    if let Some((entered, release)) = hook {
                        entered.notify_one();
                        release.notified().await;
                    }
                }
                if target
                    .as_ref()
                    .is_none_or(|target| position_covers(applied.as_ref(), target))
                {
                    coordinator.ensure_healthy(shard)?;
                    if trace {
                        eprintln!("metrics_read path=covered phases_us={phases:?}");
                    }
                    return Ok(metrics);
                }
                if let (Some(applied), Some(target)) = (&applied, &target) {
                    let claims = coordinator.retained_claim_tail(applied, target).await?;
                    mark(3);
                    if let Some(claims) = claims {
                        // Authoritative claims require every distinct named row to
                        // transition Pending -> Leased. The validator rejects gaps,
                        // duplicate IDs, historical claims and every other mutation.
                        // Pruning during the snapshot read safely falls back.
                        let count = claims.iter().try_fold(0_u64, |n, claim| {
                            n.checked_add(u64::try_from(claim.item_ids.len()).ok()?)
                        });
                        if let Some((pending, leased)) = count.and_then(|n| {
                            Some((
                                metrics.pending.checked_sub(n)?,
                                metrics.leased.checked_add(n)?,
                            ))
                        }) {
                            metrics.pending = pending;
                            metrics.leased = leased;
                            coordinator.ensure_healthy(shard)?;
                            if trace {
                                eprintln!("metrics_read path=claims phases_us={phases:?}");
                            }
                            return Ok(metrics);
                        }
                    }
                }
                if let Some(target) = &target {
                    let membership = coordinator
                        .retained_membership_tail(applied.as_ref(), target)
                        .await?;
                    mark(7);
                    if let Some(tail) = membership {
                        use fireweed_objectlog::RetainedMembershipChange;
                        let identities = tail
                            .iter()
                            .flat_map(|(_, change)| match change {
                                RetainedMembershipChange::Push(items) => items
                                    .iter()
                                    .map(|(id, key)| (*id, Some(key.clone())))
                                    .collect::<Vec<_>>(),
                                RetainedMembershipChange::Purge(ids) => {
                                    ids.iter().map(|id| (*id, None)).collect::<Vec<_>>()
                                }
                            })
                            .collect::<Vec<_>>();
                        let snapshot = self
                            .projection
                            .server_metrics_with_membership_committed(shard, &identities)
                            .await?;
                        mark(8);
                        if let Some(metrics) = snapshot.and_then(|snapshot| {
                            fold_membership_metrics(snapshot, applied.as_ref(), target, &tail)
                        }) {
                            coordinator.ensure_healthy(shard)?;
                            if trace {
                                eprintln!("metrics_read path=membership phases_us={phases:?}");
                            }
                            return Ok(metrics);
                        }
                    }
                    if let Some(applied) = applied.as_ref() {
                        let lifecycle =
                            coordinator.retained_lifecycle_tail(applied, target).await?;
                        mark(9);
                        if let Some(tail) = lifecycle {
                            use fireweed_objectlog::RetainedLifecycleChange;
                            let mut ids = HashSet::new();
                            for (_, change) in &tail {
                                match change {
                                    RetainedLifecycleChange::Claim(items) => {
                                        ids.extend(items.iter().copied())
                                    }
                                    RetainedLifecycleChange::Replace(items) => {
                                        ids.extend(items.iter().map(|(id, _, _)| *id))
                                    }
                                }
                            }
                            let mut identities =
                                ids.into_iter().map(|id| (id, None)).collect::<Vec<_>>();
                            identities.sort_unstable_by_key(|(id, _)| *id);
                            let snapshot = self
                                .projection
                                .server_metrics_with_membership_committed(shard, &identities)
                                .await?;
                            mark(10);
                            if let Some(metrics) = snapshot.and_then(|snapshot| {
                                fold_lifecycle_metrics(snapshot, applied, target, &tail)
                            }) {
                                coordinator.ensure_healthy(shard)?;
                                if trace {
                                    eprintln!("metrics_read path=lifecycle phases_us={phases:?}");
                                }
                                return Ok(metrics);
                            }
                        }
                    }
                }
            }
            // Preserve this read's entry frontier. Capturing high-water again
            // would add later writes to the wait after the snapshot/fast paths.
            if let Some(target) = target {
                coordinator
                    .wait_until_covers(shard, &target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
                    .await?;
            }
        }
        mark(4);
        let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
        mark(5);
        let result = self.projection.server_metrics_committed(shard).await;
        mark(6);
        if trace {
            eprintln!("metrics_read path=fallback phases_us={phases:?}");
        }
        result
    }

    async fn dispatch_push(
        &self,
        request: AsyncPushRequest,
    ) -> EngineResult<fireweed_engine::PushBatchOutcome> {
        if let (Some(request_id), items) = (request.request_id.clone(), &request.items) {
            let fingerprint = PushFingerprint {
                canonical_sha256: fireweed_engine::push_specs_fingerprint_sha256(items)?,
                legacy_body_hash: fireweed_engine::push_body_hash(items)?,
            };
            self.wait_request_entry_coverage(&request.shard).await?;
            let _permit = self.outcome_slots.acquire().await.map_err(map_coord)?;
            match self
                .projection
                .push_idempotency_committed(&request.shard, &request_id, &fingerprint, request.now)
                .await?
            {
                IdempotencyDecision::Replay(item_ids) => {
                    return Ok(fireweed_engine::PushBatchOutcome::replayed(item_ids));
                }
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
        }
        let fingerprint = request.request_id.as_ref().map(|_| PushFingerprint {
            canonical_sha256: fireweed_engine::push_specs_fingerprint_sha256(&request.items)
                .unwrap_or([0; 32]),
            legacy_body_hash: fireweed_engine::push_body_hash(&request.items)
                .unwrap_or(fireweed_core::BodyHash(0)),
        });
        let replay = request.request_id.clone().map(|id| {
            (
                request.shard.clone(),
                id,
                fingerprint.expect("request fingerprint"),
                request.now,
            )
        });
        let work = MutationGenerationWork::Push {
            fingerprint,
            request,
        };
        match self.drive_candidate_mutation(work).await? {
            MutationGenerationMemberOutcome::Push(PreparedPush::Replay(ids)) => {
                Ok(fireweed_engine::PushBatchOutcome::replayed(ids))
            }
            MutationGenerationMemberOutcome::Push(PreparedPush::Commit { item_ids, .. }) => {
                Ok(fireweed_engine::PushBatchOutcome::fresh(item_ids))
            }
            MutationGenerationMemberOutcome::Rejected(EngineError::RequestIdConflict) => {
                if let Some((shard, id, fingerprint, now)) = replay {
                    self.wait_request_entry_coverage(&shard).await?;
                    if let IdempotencyDecision::Replay(ids) = self
                        .projection
                        .push_idempotency_committed(&shard, &id, &fingerprint, now)
                        .await?
                    {
                        return Ok(fireweed_engine::PushBatchOutcome::replayed(ids));
                    }
                }
                Err(EngineError::RequestIdConflict)
            }
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            MutationGenerationMemberOutcome::PushAccepted
            | MutationGenerationMemberOutcome::BatchUpdate { .. }
            | MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | MutationGenerationMemberOutcome::Claim { .. }
            | MutationGenerationMemberOutcome::Finalize { .. }
            | MutationGenerationMemberOutcome::ItemMutation { .. }
            | MutationGenerationMemberOutcome::Singleton { .. } => Err(EngineError::Storage(
                "push generation produced a non-push outcome".into(),
            )),
        }
    }

    async fn dispatch_batch_update(
        &self,
        shard: QueueKey,
        request: fireweed_engine::BatchUpdateRequest,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> EngineResult<fireweed_engine::BatchUpdateResponse> {
        let definition =
            AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
        if !definition.secondary_indexes.is_empty() {
            return self
                .parity_operation(&shard, expected_epoch, move |operation| {
                    Box::pin(operation.batch_update(request, now))
                })
                .await;
        }
        if request.updates.is_empty() {
            return Err(EngineError::Invalid("empty batch update"));
        }
        if request.updates.len() > 1_000 {
            return Err(EngineError::BatchTooLarge);
        }
        let fingerprint = fireweed_engine::batch_update_body_hash(&request)?;
        let request_id = request.request_id.clone();
        if let Some(response) = self
            .projection
            .batch_update_replay(&shard, &request_id, fingerprint, now)
            .await?
        {
            return Ok(response);
        }
        let replay_shard = shard.clone();

        let epoch = match expected_epoch {
            Some(epoch) => epoch,
            None => AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?,
        };
        let work = MutationGenerationWork::BatchUpdate {
            shard,
            request,
            now,
            expected_epoch: epoch,
            fingerprint,
            command_id: self.ids.next_command_id(),
        };
        match self.drive_candidate_mutation(work).await? {
            MutationGenerationMemberOutcome::BatchUpdate { response, .. } => Ok(response),
            MutationGenerationMemberOutcome::Rejected(EngineError::NotFound) => {
                self.wait_request_entry_coverage(&replay_shard).await?;
                self.projection
                    .batch_update_replay(&replay_shard, &request_id, fingerprint, now)
                    .await?
                    .ok_or(EngineError::NotFound)
            }
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            MutationGenerationMemberOutcome::Push(_)
            | MutationGenerationMemberOutcome::PushAccepted
            | MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | MutationGenerationMemberOutcome::Claim { .. }
            | MutationGenerationMemberOutcome::Finalize { .. }
            | MutationGenerationMemberOutcome::ItemMutation { .. }
            | MutationGenerationMemberOutcome::Singleton { .. } => Err(EngineError::Storage(
                "batch-update generation produced a non-batch outcome".into(),
            )),
        }
    }

    async fn drive_candidate_mutation(
        &self,
        work: MutationGenerationWork,
    ) -> EngineResult<MutationGenerationMemberOutcome> {
        if self.engine.is_closed() {
            return Err(EngineError::Storage("closed product admission".into()));
        }
        let queue = work.queue();
        let items = work.items();
        let response_bytes = work.response_bytes();
        if items > GENERATION_MAX_ITEMS
            || response_bytes > fireweed_engine::GENERATION_MAX_RESPONSE_BYTES
        {
            return Err(EngineError::BatchTooLarge);
        }
        let key = work.sequencer_key();
        let work = Arc::new(work);
        // Retain the response cell at admission. A caller may drive another
        // generation or be descheduled for arbitrarily many later generations. Its
        // acknowledged result must remain available either way.
        // Holding the join map across admission also prevents publication from
        // racing the installation of this cell.
        let (ticket, own_join) = {
            let mut joins = self.generation_joins.lock().expect("generation join map");
            // Remove cells whose queued callers all cancelled before a driver
            // started. Active drivers and live callers retain their own Arc.
            joins.retain(|_, join| Arc::strong_count(join) > 1);
            let ticket = self
                .sequencer
                .admit(
                    queue.clone(),
                    key,
                    MutationIngress::Direct,
                    Arc::clone(&work),
                    items,
                    response_bytes,
                )
                .map_err(map_coord)?;
            let join = joins
                .entry((queue.clone(), ticket.generation_id()))
                .or_insert_with(|| {
                    Arc::new(GenerationJoin {
                        requests: Mutex::new(Vec::new()),
                        notify: tokio::sync::Notify::new(),
                        outcome: Mutex::new(None),
                    })
                })
                .clone();
            (ticket, join)
        };
        let generation_id = ticket.generation_id();
        let started = Instant::now();
        loop {
            if let Some(outcome) = own_join.member(&work) {
                drop(ticket);
                return outcome;
            }
            if let Some(generation) = self
                .sequencer
                .start_generation_after(&queue, MICROBATCH_LINGER)
            {
                let driven_id = generation.generation_id();
                let join = self.ensure_generation_join(&queue, driven_id);
                // Match responses by the admitted request identity and driver order.
                // Client keys and optional request IDs are not unique caller IDs.
                *join.requests.lock().expect("generation request order") =
                    generation.requests().to_vec();
                // Publication belongs to an owned, drainable driver. Cancelling
                // the caller that started a generation must not strand its peers.
                let runner = self.clone();
                let publish_queue = queue.clone();
                let driver = CoordinatorOwnedDispatcher {
                    inner: ObjectLogTaskDispatcher::new(),
                    registry: self.drivers.clone(),
                }
                .submit(Box::new(move || {
                    Box::pin(async move {
                        let driven = runner.drive_started_generation(generation).await;
                        runner.publish_generation_outcome(&publish_queue, driven_id, &join, driven);
                    })
                }))
                .map_err(|error| EngineError::Storage(format!("generation dispatch: {error:?}")))?;
                driver.await.map_err(|error| {
                    EngineError::Storage(format!("generation driver: {error:?}"))
                })?;
                continue;
            }
            let notified = own_join.notify.notified();
            tokio::pin!(notified);
            if let Some(outcome) = own_join.member(&work) {
                drop(ticket);
                return outcome;
            }
            if started.elapsed() >= MUTATION_SEQUENCER_DEFAULT_MAX_WAIT
                && !self
                    .sequencer
                    .generation_queued_or_active(&queue, generation_id)
            {
                drop(ticket);
                return Err(EngineError::Backpressure {
                    resource: "mutation sequencer wait",
                });
            }
            // Queued generations still need a caller to start them after the
            // linger interval. Publication wakes active-generation waiters.
            tokio::select! {
                _ = &mut notified => {},
                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
            }
        }
    }

    async fn drive_started_generation(
        &self,
        generation: fireweed_engine::MutationGenerationBatch<
            QueueKey,
            MutationSequencerKey,
            MutationGenerationWork,
        >,
    ) -> EngineResult<Vec<fireweed_engine::MutationGenerationMember>> {
        if matches!(
            generation.requests()[0].as_ref(),
            MutationGenerationWork::ItemMutation { .. }
        ) {
            return self.drive_addressed_generation(generation).await;
        }
        let queue = generation.requests()[0].queue();
        debug_assert!(generation.requests().len() <= CLAIM_GENERATION_MAX_REQUESTS);
        let works: Vec<MutationGenerationWork> = generation
            .requests()
            .iter()
            .map(|work| work.as_ref().clone())
            .collect();
        let has_claim = works
            .iter()
            .any(|work| matches!(work, MutationGenerationWork::Claim { .. }));
        if has_claim {
            self.catch_up_produce(&queue).await?;
        }
        let mut keys = Vec::new();
        let mut batch_keys = Vec::new();
        let mut batch_ids = Vec::new();
        let mut claimed = false;
        let mut records_mutation = false;
        let mut has_request_id = false;
        for work in &works {
            match work {
                MutationGenerationWork::Push { request, .. } => {
                    records_mutation = true;
                    has_request_id |= request.request_id.is_some();
                    keys.extend(
                        request
                            .items
                            .iter()
                            .filter_map(|item| item.client_item_key.clone()),
                    );
                }
                MutationGenerationWork::BatchUpdate { request, .. } => {
                    records_mutation = true;
                    has_request_id = true;
                    for update in &request.updates {
                        if let fireweed_engine::BatchUpdateItemRef::ItemId(id) = &update.item_ref {
                            batch_ids.push(*id);
                        }

                        if let fireweed_engine::BatchUpdateItemRef::ClientItemKey(key)
                        | fireweed_engine::BatchUpdateItemRef::Both {
                            client_item_key: key,
                            ..
                        } = &update.item_ref
                        {
                            batch_keys.push(key.clone());
                        }
                    }
                }
                MutationGenerationWork::Claim { .. } => claimed = true,
                MutationGenerationWork::ItemMutation { .. } => {
                    return Err(EngineError::Invalid(
                        "addressed request in identity generation",
                    ));
                }
                MutationGenerationWork::Finalize { .. } => records_mutation = true,
                MutationGenerationWork::Singleton { .. } => {}
            }
        }
        // Claim SELECT reads Turso. Wait for this process's previous claim and
        // push to apply so those rows exist. Mutate acks the log without that wait.
        self.wait_selected_frontiers(&queue, claimed, claimed)
            .await?;
        let definition =
            AsyncControlPlane::queue_definition(self.control.as_ref(), queue.clone()).await?;
        let _waiter = self.fence_admission.admit_waiter().map_err(map_coord)?;
        let _fence = self
            .selection_fence
            .acquire_shared(queue.clone())
            .await
            .map_err(map_coord)?;
        let now = match &works[0] {
            MutationGenerationWork::Push { request, .. } => request.now,
            MutationGenerationWork::BatchUpdate { now, .. }
            | MutationGenerationWork::Finalize { now, .. } => *now,
            MutationGenerationWork::Claim { request, .. } => request.now,
            MutationGenerationWork::ItemMutation { request, .. } => request.evaluated_at,
            MutationGenerationWork::Singleton { commit, .. } => commit
                .commands()
                .first()
                .map(|envelope| envelope.created_at)
                .unwrap_or_else(|| UtcTimestamp::new(1, 0).expect("epoch")),
        };
        let snapshot_batch_keys = batch_keys;
        let needs_serving_snapshot =
            !keys.is_empty() || !snapshot_batch_keys.is_empty() || !batch_ids.is_empty();
        if needs_serving_snapshot || has_request_id {
            self.wait_request_entry_coverage(&queue).await?;
        }
        let _slot = if needs_serving_snapshot || claimed {
            Some(self.shared_slots.acquire().await.map_err(map_coord)?)
        } else {
            None
        };
        let mut snapshot = if records_mutation {
            let applied = self.applied_identity.lock().await;
            match applied.get(&queue) {
                Some(existing) => identity_for_generation(existing, definition.clone(), &works),
                None => identity_base(definition.clone()),
            }
        } else {
            identity_base(definition.clone())
        };
        if needs_serving_snapshot {
            let loaded = self
                .projection
                .mutation_driver_snapshot(
                    &queue,
                    definition.clone(),
                    &keys,
                    &snapshot_batch_keys,
                    now,
                )
                .await?;
            snapshot.client_keys = loaded.client_keys;
            snapshot.batch_items = loaded.batch_items;
            snapshot.batch_items.extend(
                self.projection
                    .server_update_snapshot_by_ids(&queue, &batch_ids)
                    .await?,
            );
            snapshot.paused_drain_intake |= loaded.paused_drain_intake;
        }
        // Admission may have waited behind an earlier generation after the
        // facade's replay check. Recheck retained identities here, under the
        // generation turn, without accumulating an unbounded historical cache.
        for work in &works {
            match work {
                MutationGenerationWork::Push {
                    request,
                    fingerprint: Some(fingerprint),
                } => {
                    if let Some(id) = &request.request_id {
                        let decision = self
                            .projection
                            .push_idempotency_committed(&queue, id, fingerprint, request.now)
                            .await?;
                        if matches!(
                            decision,
                            IdempotencyDecision::Replay(_) | IdempotencyDecision::Conflict
                        ) {
                            snapshot
                                .request_fingerprints
                                .insert(id.clone(), fingerprint.legacy_body_hash);
                        }
                    }
                }
                MutationGenerationWork::BatchUpdate {
                    request,
                    fingerprint,
                    now,
                    ..
                } if self
                    .projection
                    .batch_update_replay(&queue, &request.request_id, *fingerprint, *now)
                    .await?
                    .is_some() =>
                {
                    snapshot
                        .request_fingerprints
                        .insert(request.request_id.clone(), *fingerprint);
                }
                _ => {}
            }
        }
        let (mut members, mut folded) =
            validate_inert_mutation_generation_folding(&snapshot, &works)?;
        let _claim_select = if claimed {
            Some(self.claim_select.lock().await)
        } else {
            None
        };
        let saved_claim_hint = claimed.then(|| self.projection.claim_scan_hint(&queue));
        self.realize_accepted_claims(&queue, &works, &mut members, &mut folded)
            .await?;
        self.allocate_accepted_pushes(&definition, &works, &mut members, &mut folded)
            .await?;
        drop(_slot);
        let mut prepared = retain_sequencer_after_slot_release(members.clone(), generation);
        let commits =
            coalesce_generation_commits(finish_inert_mutation_generation_append(&mut prepared)?)?;
        if !commits.is_empty() {
            for commit in commits {
                if let Err(error) = self
                    .commit_prepared(commit, AppendAdmissionClass::SharedSelectionLive)
                    .await
                {
                    if let Some(hint) = saved_claim_hint {
                        self.projection.replace_claim_scan_hint(&queue, hint);
                    }
                    return Err(error);
                }
            }
            for (work, member) in works.iter().zip(&members) {
                if let (
                    MutationGenerationWork::Claim { request, .. },
                    MutationGenerationMemberOutcome::Claim {
                        claimed: claimed_items,
                        ..
                    },
                ) = (work, &member.outcome)
                {
                    let ids: Vec<_> = claimed_items
                        .items
                        .iter()
                        .map(|item| item.item_id)
                        .collect();
                    if !ids.is_empty() {
                        self.projection
                            .remember_leases(&queue, &ids, request.lease_token.clone())
                            .await;
                    }
                }
            }
            if claimed {
                self.record_frontier(&queue, true).await?;
            }
            if records_mutation {
                self.record_frontier(&queue, false).await?;
            }
        }
        Ok(members)
    }

    async fn allocate_accepted_pushes(
        &self,
        definition: &QueueDefinition,
        works: &[MutationGenerationWork],
        members: &mut [fireweed_engine::MutationGenerationMember],
        folded: &mut MutationDriverSnapshot,
    ) -> EngineResult<()> {
        for (work, member) in works.iter().zip(members.iter_mut()) {
            if !matches!(
                member.outcome,
                MutationGenerationMemberOutcome::PushAccepted
            ) {
                continue;
            }
            let MutationGenerationWork::Push {
                request,
                fingerprint,
            } = work
            else {
                continue;
            };
            let (epoch, items, item_ids) = allocate_push_epoch_blob_and_counters(
                self.log.as_ref(),
                self.counters.as_ref(),
                definition,
                request,
                self.node_id,
            )
            .await?;
            for item in &items {
                folded
                    .client_keys
                    .insert(item.client_item_key.as_str().to_string());
            }
            let envelope = CommandEnvelope {
                command_id: self.ids.next_command_id(),
                request_id: request.request_id.clone(),
                request_fingerprint: fingerprint.map(|hash| hash.legacy_body_hash.0),
                request_outcome: request.request_id.as_ref().map(|_| RequestOutcome::Push {
                    item_ids: item_ids.clone(),
                }),
                item_ids: item_ids.clone(),
                command: QueueCommand::Push(PushCommand { items }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            member.outcome = MutationGenerationMemberOutcome::Push(PreparedPush::Commit {
                request: RawCommitRequest::new(request.shard.clone(), vec![envelope], epoch),
                item_ids,
            });
        }
        Ok(())
    }

    async fn realize_accepted_claims(
        &self,
        queue: &QueueKey,
        works: &[MutationGenerationWork],
        members: &mut [fireweed_engine::MutationGenerationMember],
        folded: &mut MutationDriverSnapshot,
    ) -> EngineResult<()> {
        let mut claim_indices = Vec::new();
        let mut claim_members = Vec::new();
        for (index, (work, member)) in works.iter().zip(members.iter()).enumerate() {
            if !matches!(
                member.outcome,
                MutationGenerationMemberOutcome::ClaimAccepted { .. }
            ) {
                continue;
            }
            let MutationGenerationWork::Claim { request, .. } = work else {
                continue;
            };
            claim_indices.push(index);
            claim_members.push((
                request.eligibility_at(),
                request.max_items,
                request.lease_token.clone(),
                request.lease_expires_at,
            ));
        }
        if claim_members.is_empty() {
            return Ok(());
        }
        let exclude = claim_exclude_ids(folded);
        let selected = self
            .projection
            .item_claim_microbatch_on_serving_reader_with_gate_policy(
                queue,
                &claim_members,
                &exclude,
                Some(folded.definition.eligibility_policy.gate_keys),
            )
            .await?;
        let epoch = match claim_indices
            .first()
            .and_then(|&index| match &works[index] {
                MutationGenerationWork::Claim { request, .. } => request.expected_epoch,
                _ => None,
            }) {
            Some(epoch) => epoch,
            None => AsyncLogStore::current_epoch(self.log.as_ref(), queue.clone()).await?,
        };
        for (index, (ids, items)) in claim_indices.into_iter().zip(selected) {
            let MutationGenerationWork::Claim { id, request } = &works[index] else {
                continue;
            };
            if ids.is_empty() {
                members[index].outcome = MutationGenerationMemberOutcome::Claim {
                    id: *id,
                    request: None,
                    claimed: Claimed::default(),
                };
                continue;
            }
            let claimed =
                PreparedClaimedResult::from_rendered(request, &ids, items, None)?.into_claimed();
            folded
                .leased_ids
                .extend(claimed.items.iter().map(|item| item.item_id));
            let envelope = CommandEnvelope {
                command_id: self.ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: claimed.items.iter().map(|item| item.item_id).collect(),
                command: QueueCommand::Claim(ClaimCommand {
                    item_ids: claimed.items.iter().map(|item| item.item_id).collect(),
                    lease_token: request.lease_token.clone(),
                    lease_expires_at: request.lease_expires_at,
                    worker_id: Some(request.worker_id.clone()),
                    authority_first: true,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            members[index].outcome = MutationGenerationMemberOutcome::Claim {
                id: *id,
                request: Some(RawCommitRequest::new(
                    request.shard.clone(),
                    vec![envelope],
                    epoch,
                )),
                claimed,
            };
        }
        Ok(())
    }

    async fn dispatch_claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        // WholeGroup / SameGroupKey / WholeCohort stay exclusive. A lone
        // `group_key` filter is still exclusive too: packed realize does not
        // yet honor that filter, so sending it down `drive_candidate_mutation`
        // would claim FIFO neighbors. Do not widen this until realize filters.
        if request.compatibility != ClaimCompatibility::default() {
            return self.dispatch_grouped_cohort_claim(request).await;
        }
        let work = MutationGenerationWork::Claim {
            id: self.claim_work_ids.fetch_add(1, Ordering::Relaxed),
            request,
        };
        match self.drive_candidate_mutation(work).await? {
            MutationGenerationMemberOutcome::Claim { claimed, .. } => Ok(claimed),
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            MutationGenerationMemberOutcome::Push(_)
            | MutationGenerationMemberOutcome::PushAccepted
            | MutationGenerationMemberOutcome::BatchUpdate { .. }
            | MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | MutationGenerationMemberOutcome::Finalize { .. }
            | MutationGenerationMemberOutcome::ItemMutation { .. }
            | MutationGenerationMemberOutcome::Singleton { .. } => Err(EngineError::Storage(
                "claim generation produced a non-claim outcome".into(),
            )),
        }
    }

    async fn dispatch_grouped_cohort_claim(&self, request: ClaimRequest) -> EngineResult<Claimed> {
        let _turn = self
            .claim_turns
            .acquire(request.shard.clone())
            .await
            .map_err(map_coord)?;
        self.catch_up_produce(&request.shard).await?;
        self.wait_queue_frontiers(&request.shard).await?;
        let slot = self.claim_slots.acquire().await.map_err(map_coord)?;
        let driver = self.projection.borrow_committed_driver_connection().await?;
        let _waiter = self.fence_admission.admit_waiter().map_err(map_coord)?;
        let fence = self
            .selection_fence
            .acquire_exclusive(request.shard.clone())
            .await
            .map_err(map_coord)?;
        self.wait_queue_frontiers(&request.shard).await?;
        let prepared = self
            .engine
            .plan_exclusive_claim(request.clone())
            .await
            .map_err(map_claim)?;
        match prepared {
            PreparedClaim::Empty => Ok(Claimed::default()),
            PreparedClaim::Commit {
                request: commit,
                item_ids,
                cohort_id,
            } => {
                let items = match driver.as_ref() {
                    Some(connection) => {
                        materialize_grouped_cohort_claimed_on(
                            connection,
                            &request.shard,
                            &item_ids,
                            &request.lease_token,
                            request.lease_expires_at,
                        )
                        .await?
                    }
                    None => {
                        self.projection
                            .materialize_grouped_cohort_committed(
                                &request.shard,
                                &item_ids,
                                &request.lease_token,
                                request.lease_expires_at,
                            )
                            .await?
                    }
                };
                drop(driver);
                drop(slot);
                let retained =
                    PreparedClaimedResult::from_rendered(&request, &item_ids, items, cohort_id)?;
                let reservation = match &self.async_apply {
                    Some(coordinator) => Some(
                        coordinator
                            .reserve(request.shard.clone(), commit.commands())
                            .await?,
                    ),
                    None => None,
                };
                self.append_class_s_claim(
                    commit.with_append_admission(AppendAdmissionClass::ClaimCoordinatorLive),
                    reservation,
                )
                .await?;
                drop(fence);
                self.projection
                    .remember_leases(&request.shard, &item_ids, request.lease_token.clone())
                    .await;
                self.record_frontier(&request.shard, true).await?;
                finish_retained_grouped_cohort_response(retained.into_claimed())
            }
        }
    }

    async fn append_class_s_claim(
        &self,
        request: RawCommitRequest,
        reservation: Option<fireweed_objectlog::AsyncProjectionApplyReservation>,
    ) -> EngineResult<()> {
        let (shard, commands, epoch, fault, append_admission) =
            request.into_parts_with_append_admission();
        match append_admission {
            AppendAdmissionClass::ClaimCoordinatorLive
            | AppendAdmissionClass::NonDerived
            | AppendAdmissionClass::KeyedPermitLive
            | AppendAdmissionClass::SelectionRequired
            | AppendAdmissionClass::SharedSelectionLive
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
            .packed_append_owned(
                shard.clone(),
                commands.clone(),
                epoch,
                reservation.as_ref().map(|reserved| reserved.id()),
                true,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(dispose_packed_append_error(
                    self.async_apply.as_ref(),
                    reservation,
                    error,
                )
                .await);
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

    #[allow(dead_code)]
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
                self.commit_prepared(commit, AppendAdmissionClass::SharedSelectionLive)
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
        let packed_update = outcomes
            .iter()
            .all(|outcome| matches!(outcome.kind, FinalizeKind::Complete | FinalizeKind::Fail));
        if packed_update {
            let epoch = match expected_epoch {
                Some(epoch) => epoch,
                None => AsyncLogStore::current_epoch(self.log.as_ref(), shard.clone()).await?,
            };
            let work = MutationGenerationWork::Finalize {
                shard: shard.clone(),
                outcomes,
                now,
                expected_epoch: epoch,
                command_id: self.ids.next_command_id(),
            };
            return match self.drive_candidate_mutation(work).await? {
                MutationGenerationMemberOutcome::Finalize { .. } => Ok(()),
                MutationGenerationMemberOutcome::Rejected(error) => Err(error),
                MutationGenerationMemberOutcome::Push(_)
                | MutationGenerationMemberOutcome::PushAccepted
                | MutationGenerationMemberOutcome::BatchUpdate { .. }
                | MutationGenerationMemberOutcome::ClaimAccepted { .. }
                | MutationGenerationMemberOutcome::Claim { .. }
                | MutationGenerationMemberOutcome::ItemMutation { .. }
                | MutationGenerationMemberOutcome::Singleton { .. } => Err(EngineError::Storage(
                    "finalize generation produced a non-finalize outcome".into(),
                )),
            };
        }
        // Retry/release planning reads lease versions and attempt counts from
        // the projection. A remembered bearer alone cannot substitute for the
        // acknowledged Claim's state: it may still be Pending while apply runs.
        self.wait_request_entry_coverage(shard).await?;
        let PreparedFinalize { request, .. } = self
            .engine
            .prepare_finalize(shard.clone(), outcomes, now, expected_epoch)
            .await
            .map_err(map_lifecycle)?;
        self.commit_prepared(request, AppendAdmissionClass::SelectionRequired)
            .await?;
        self.record_frontier(shard, false).await?;
        Ok(())
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

    async fn validate_projection_catalog(&self) -> EngineResult<Vec<QueueDefinition>> {
        let definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        let log_by_key: HashMap<QueueKey, QueueDefinition> = definitions
            .iter()
            .cloned()
            .map(|definition| {
                (
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
                    definition,
                )
            })
            .collect();
        for projected in AsyncProjectionStore::recover_definitions(self.projection.as_ref()).await?
        {
            let key = QueueKey::new(projected.tenant_id.clone(), projected.queue_id.clone());
            let Some(authoritative) = log_by_key.get(&key) else {
                return Err(EngineError::Storage(
                    "projection contains a queue absent from the authoritative object log".into(),
                ));
            };
            if authoritative != &projected {
                return Err(EngineError::Storage(
                    "projection queue definition conflicts with the authoritative object log"
                        .into(),
                ));
            }
            let projected_high_water =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), key.clone())
                    .await?;
            let authoritative_high_water =
                AsyncLogStore::high_water(self.log.as_ref(), key).await?;
            match (projected_high_water, authoritative_high_water) {
                (Some(_), None) => {
                    return Err(EngineError::Storage(
                        "projection is non-empty but the authoritative object log is empty".into(),
                    ));
                }
                (Some(projected), Some(authoritative))
                    if projected.backend_epoch > authoritative.backend_epoch
                        || (projected.backend_epoch == authoritative.backend_epoch
                            && projected.sequence > authoritative.sequence) =>
                {
                    return Err(EngineError::Storage(
                        "projection is ahead of the authoritative object log".into(),
                    ));
                }
                _ => {}
            }
        }
        Ok(definitions)
    }

    fn pause_async_apply(&self) {
        if let Some(coordinator) = &self.async_apply {
            coordinator.pause();
        }
    }

    async fn drop_queued_async_apply(&self, definitions: &[QueueDefinition]) {
        let Some(coordinator) = &self.async_apply else {
            return;
        };
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            coordinator.reset_after_rebuild(shard, None).await;
        }
    }

    pub(crate) async fn verify_projection(
        &self,
    ) -> EngineResult<crate::ProjectionVerificationState> {
        let definitions = self.validate_projection_catalog().await?;
        if let Some(coordinator) = &self.async_apply
            && !coordinator.is_paused()
        {
            for definition in &definitions {
                let shard =
                    QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
                let _ = self.wait_request_entry_coverage(&shard).await;
            }
        }
        let mut projection_sequence = 0;
        let mut authoritative_sequence = 0;
        let mut compatible = true;
        for definition in definitions {
            let key = QueueKey::new(definition.tenant_id, definition.queue_id);
            let projected_position =
                AsyncProjectionStore::recovery_high_water(self.projection.as_ref(), key.clone())
                    .await?;
            let authoritative_position = AsyncLogStore::high_water(self.log.as_ref(), key).await?;
            compatible &= projected_position == authoritative_position;
            projection_sequence = projection_sequence.max(
                projected_position
                    .as_ref()
                    .map_or(0, |position| position.sequence),
            );
            authoritative_sequence = authoritative_sequence.max(
                authoritative_position
                    .as_ref()
                    .map_or(0, |position| position.sequence),
            );
        }
        Ok(crate::ProjectionVerificationState {
            compatible,
            projection_sequence,
            authoritative_sequence,
        })
    }

    pub(crate) async fn delete_projection(&self) -> EngineResult<()> {
        let definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        self.pause_async_apply();
        self.drop_queued_async_apply(&definitions).await;
        self.projection.delete_projection().await?;
        self.applied_identity.lock().await.clear();
        self.frontiers.lock().await.clear();
        self.last_produce.lock().await.clear();
        self.produce_caught_up.lock().await.clear();
        Ok(())
    }

    pub(crate) async fn rebuild_projection(
        &self,
        max_tail_commands: u64,
    ) -> EngineResult<crate::ProjectionRebuildState> {
        self.pause_async_apply();
        let definitions = AsyncLogStore::recover_definitions(self.log.as_ref()).await?;
        self.drop_queued_async_apply(&definitions).await;
        let mut estimated_tail = 0_u64;
        for definition in &definitions {
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let page = AsyncLogStore::read_from(self.log.as_ref(), key, None, 1).await?;
            estimated_tail = estimated_tail.saturating_add(page.entries.len() as u64);
        }
        if estimated_tail > max_tail_commands {
            return Err(EngineError::Storage(format!(
                "projection rebuild exceeds configured tail bound {max_tail_commands}"
            )));
        }
        self.recover_async().await?;
        self.projection.refresh_serving_reader().await?;
        for definition in &definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let recovered = self.projection.writer_recovery_high_water(&shard).await?;
            if let Some(coordinator) = &self.async_apply {
                coordinator.reset_after_rebuild(shard, recovered).await;
            }
        }
        if let Some(coordinator) = &self.async_apply {
            coordinator.resume();
        }
        let verification = self.verify_projection().await?;
        Ok(crate::ProjectionRebuildState {
            snapshot_used: false,
            tail_commands_replayed: verification.projection_sequence.max(estimated_tail),
            projection_sequence: verification.projection_sequence,
        })
    }
}

#[cfg(feature = "objectlog")]
impl_turso_product_ports!(
    DerivedObjectLogTursoBackend,
    DurabilityClass::EventualApply,
    "object-log append then Turso apply (SeparateReplayCommit)",
    true
);

impl<L: AsyncLogStore + 'static> AtomicTursoBackend<L> {
    async fn dispatch_item_mutation(
        &self,
        shard: &QueueKey,
        request: ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> EngineResult<ItemMutationResponse> {
        self.parity_operation(shard, expected_epoch, move |operation| {
            Box::pin(operation.mutate(request))
        })
        .await
    }
}

#[cfg(feature = "objectlog")]
impl DerivedObjectLogTursoBackend {
    async fn dispatch_item_mutation(
        &self,
        shard: &QueueKey,
        request: ItemMutationRequest,
        expected_epoch: Option<u64>,
    ) -> EngineResult<ItemMutationResponse> {
        let definition =
            AsyncControlPlane::queue_definition(self.control.as_ref(), shard.clone()).await?;
        let fast = matches!(
            &request.operation,
            fireweed_engine::ItemMutationOperation::Addressed { .. }
        ) && request.gate_changes.is_empty()
            && definition.secondary_indexes.is_empty()
            && definition.typed_indexes.is_empty()
            && definition.entity_schema.is_none()
            && definition.cohort_policy.is_none();
        if !fast {
            return self
                .parity_operation(shard, expected_epoch, move |operation| {
                    Box::pin(operation.mutate(request))
                })
                .await;
        }
        if let fireweed_engine::ItemMutationOperation::Addressed { entries } = &request.operation
            && entries.len() > 1000
        {
            return Err(EngineError::Invalid(
                "addressed mutation batch exceeds 1000 items",
            ));
        }
        let work = MutationGenerationWork::ItemMutation {
            id: self.claim_work_ids.fetch_add(1, Ordering::Relaxed),
            shard: shard.clone(),
            request,
            expected_epoch,
        };
        match self.drive_candidate_mutation(work).await? {
            MutationGenerationMemberOutcome::ItemMutation { result, .. } => result,
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            _ => Err(EngineError::Storage(
                "addressed generation returned another operation's outcome".into(),
            )),
        }
    }

    async fn drive_addressed_generation(
        &self,
        generation: fireweed_engine::MutationGenerationBatch<
            QueueKey,
            MutationSequencerKey,
            MutationGenerationWork,
        >,
    ) -> EngineResult<Vec<fireweed_engine::MutationGenerationMember>> {
        let shard = generation.requests()[0].queue();
        // Only disjoint requests share an append. Overlaps and repeated request
        // IDs start a new group, after the previous group has applied, so every
        // lease/version check and replay decision observes its FIFO predecessor.
        let mut groups = Vec::new();
        let mut group = Vec::new();
        let mut addressed = HashSet::new();
        let mut request_ids = HashSet::new();
        for work in generation.requests() {
            let MutationGenerationWork::ItemMutation { request, .. } = work.as_ref() else {
                return Err(EngineError::Invalid("mixed addressed generation"));
            };
            let fireweed_engine::ItemMutationOperation::Addressed { entries } = &request.operation
            else {
                return Err(EngineError::Unavailable);
            };
            let intersects = request_ids.contains(&request.request_id)
                || entries
                    .iter()
                    .any(|entry| addressed.contains(&entry.item_id));
            if !group.is_empty() && (intersects || !request.gate_changes.is_empty()) {
                groups.push(std::mem::take(&mut group));
                addressed.clear();
                request_ids.clear();
            }
            addressed.extend(entries.iter().map(|entry| entry.item_id));
            request_ids.insert(request.request_id.clone());
            group.push(Arc::clone(work));
            if !request.gate_changes.is_empty() {
                groups.push(std::mem::take(&mut group));
                addressed.clear();
                request_ids.clear();
            }
        }
        if !group.is_empty() {
            groups.push(group);
        }
        // The generation owner survives caller cancellation and retains the
        // sequencer through SQL publication. Legacy operations share the keyed
        // gate; all native generations share the selection fence.
        let _generation = generation;
        let coordinator = self.async_apply.clone();
        let log = Arc::clone(&self.log);
        let strategy = self.engine.commit_strategy();
        let projection = Arc::clone(&self.projection);
        let control = Arc::clone(&self.control);
        let ids = Arc::clone(&self.ids);
        let fence = self.selection_fence.clone();
        let admission = self.fence_admission.clone();
        self.engine
            .submit_operation(shard.clone(), move || {
                Box::pin(async move {
                    let _waiter = admission.admit_waiter().map_err(map_coord)?;
                    let _fence = fence
                        .acquire_exclusive(shard.clone())
                        .await
                        .map_err(map_coord)?;
                    let epoch = AsyncLogStore::current_epoch(log.as_ref(), shard.clone()).await?;
                    let mut pending_claims = Vec::new();
                    if let Some(coordinator) = &coordinator
                        && let Some(target) =
                            AsyncLogStore::high_water(log.as_ref(), shard.clone()).await?
                    {
                        let applied = coordinator.snapshot(&shard).await.applied_high_water;
                        if !position_covers(applied.as_ref(), &target) {
                            // The exclusive fence freezes the log tail. A bounded tail
                            // containing only disjoint authoritative claims can validate
                            // the next mutation without publishing intermediate leases.
                            if let Some(applied) = applied.as_ref().filter(|p| {
                                p.backend_epoch == target.backend_epoch
                                    && target.sequence.saturating_sub(p.sequence) <= 16
                            }) {
                                let tail_started = std::time::Instant::now();
                                let retained =
                                    coordinator.retained_claim_tail(applied, &target).await?;
                                let retained_hit = retained.is_some();
                                pending_claims = if let Some(claims) = retained {
                                    claims
                                } else {
                                    let page = AsyncLogStore::read_from(
                                        log.as_ref(),
                                        shard.clone(),
                                        Some(applied.clone()),
                                        16,
                                    )
                                    .await?;
                                    claim_only_tail(applied, &target, page.entries)
                                        .unwrap_or_default()
                                };
                                if std::env::var_os("FIREWEED_APPLY_TRACE").is_some() {
                                    eprintln!(
                                        "claim_tail retained={retained_hit} commands={} us={}",
                                        pending_claims.len(),
                                        tail_started.elapsed().as_micros()
                                    );
                                }
                            }
                            if pending_claims.is_empty() {
                                coordinator
                                    .wait_until_covers(
                                        &shard,
                                        &target,
                                        S3S_DERIVED_COVERAGE_OR_WORK_WAIT,
                                    )
                                    .await?;
                            }
                        }
                    }
                    let definition =
                        AsyncControlPlane::queue_definition(control.as_ref(), shard.clone())
                            .await?;
                    let mut members = Vec::new();
                    let mut response_bytes = 0usize;
                    for group in groups {
                        let mut commands = Vec::new();
                        let mut command_members = Vec::new();
                        for work in group {
                            let MutationGenerationWork::ItemMutation {
                                id,
                                request,
                                expected_epoch,
                                ..
                            } = work.as_ref()
                            else {
                                unreachable!("addressed generation checked before submission")
                            };
                            let result = async {
                                if expected_epoch.is_some_and(|expected| expected != epoch) {
                                    return Err(EngineError::EpochFenced);
                                }
                                let fingerprint =
                                    fireweed_engine::item_mutation_fingerprint(request)?;
                                let replay = projection
                                    .item_mutation_replay(&shard, request, fingerprint)
                                    .await?;
                                let (response, command) = if let Some(response) = replay {
                                    (response, None)
                                } else {
                                    let plan = projection
                                        .plan_item_mutation(
                                            &shard,
                                            &definition,
                                            request,
                                            &pending_claims,
                                        )
                                        .await?;
                                    (plan.response, (!request.dry_run).then_some(plan.command))
                                };
                                let response_payload = serde_json::to_string(&response)
                                    .map_err(|e| EngineError::Storage(e.to_string()))?;
                                let next_bytes =
                                    response_bytes.saturating_add(response_payload.len());
                                if next_bytes > fireweed_engine::GENERATION_MAX_RESPONSE_BYTES {
                                    return Err(EngineError::BatchTooLarge);
                                }
                                response_bytes = next_bytes;
                                if let Some(command) = command {
                                    commands.push(CommandEnvelope {
                                        command_id: ids.next_command_id(),
                                        request_id: Some(request.request_id.clone()),
                                        request_fingerprint: Some(fingerprint),
                                        request_outcome: Some(RequestOutcome::ItemMutation {
                                            response_payload,
                                        }),
                                        item_ids: command
                                            .items
                                            .iter()
                                            .map(|item| item.item_id)
                                            .collect(),
                                        command: QueueCommand::MutateItems(command),
                                        checksum: CommandChecksum(0),
                                        created_at: request.evaluated_at,
                                    });
                                    command_members.push(members.len());
                                }
                                Ok(response)
                            }
                            .await;
                            members.push(fireweed_engine::MutationGenerationMember {
                                outcome: MutationGenerationMemberOutcome::ItemMutation {
                                    id: *id,
                                    result,
                                },
                            });
                        }
                        if !commands.is_empty() {
                            let outcome = strategy
                                .commit(
                                    RawCommitRequest::new(shard.clone(), commands, epoch)
                                        .with_append_admission(
                                            AppendAdmissionClass::SharedSelectionLive,
                                        ),
                                )
                                .await?;
                            if outcome.positions().len() != command_members.len() {
                                return Err(EngineError::Storage(
                                    "addressed append position count mismatch".into(),
                                ));
                            }
                            for (member, position) in
                                command_members.into_iter().zip(outcome.positions())
                            {
                                let MutationGenerationMemberOutcome::ItemMutation {
                                    result: Ok(response),
                                    ..
                                } = &mut members[member].outcome
                                else {
                                    return Err(EngineError::Storage(
                                        "addressed append lost response".into(),
                                    ));
                                };
                                response.position = Some(position.clone());
                            }
                            // Make replacements visible before another group or a
                            // claim validates against the SQL projection.
                            if let (Some(coordinator), Some(position)) =
                                (&coordinator, outcome.positions().last())
                            {
                                coordinator
                                    .wait_until_covers(
                                        &shard,
                                        position,
                                        S3S_DERIVED_COVERAGE_OR_WORK_WAIT,
                                    )
                                    .await?;
                            }
                            pending_claims.clear();
                        }
                    }
                    Ok(members)
                })
            })
            .await
            .map_err(map_submit_error)?
    }
}

#[cfg(feature = "objectlog")]
impl_turso_commit_transition!(DerivedObjectLogTursoBackend);

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
    let log = InProcessLogStore::new_with_blocking_offload(
        log,
        fireweed_engine::DEFAULT_BLOCKING_AXIS_IN_FLIGHT,
    )?;
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

/// Rebuild a local filesystem object-log × Turso projection through ProjectionLifecycle.
///
/// Tests and operators must call this (or `Fireweed::projection_control`) rather
/// than unlinking projection files and reopening.
#[cfg(feature = "objectlog")]
pub async fn rebuild_filesystem_turso_projection(
    log_root: PathBuf,
    projection_path: PathBuf,
    target_bytes: usize,
    max_latency_ms: u64,
    async_spec: Option<AsyncProjectionSpec>,
    max_tail_commands: u64,
) -> EngineResult<crate::ProjectionRebuild> {
    let flush = flush_config_from_segment(target_bytes, max_latency_ms);
    let log = ObjectLogEngineStore::open_local(log_root, flush).await?;
    let backend = assemble_objectlog_turso(log, projection_path, async_spec)?;
    backend.delete_projection().await?;
    let rebuilt = backend.rebuild_projection(max_tail_commands).await?;
    Ok(crate::ProjectionRebuild {
        snapshot_used: rebuilt.snapshot_used,
        tail_commands_replayed: rebuilt.tail_commands_replayed,
        projection_sequence: rebuilt.projection_sequence,
    })
}

#[cfg(all(test, feature = "objectlog"))]
mod s3v_after_append {
    use std::sync::atomic::{AtomicU64, Ordering};

    use fireweed_core::{
        EligibilityPolicy, ItemId, Metadata, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId, UtcTimestamp,
    };
    use fireweed_engine::{
        AsyncLogStore, AsyncProjectionSpec, Backend, CommandChecksum, CommandEnvelope, CommandId,
        ControlPlaneStore, EngineError, ProjectionRead, PushCommand, PushItem, PushPort, PushSpec,
        QueueCommand, RawCommitFault, RawCommitRequest,
    };
    use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
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

    fn spec() -> AsyncProjectionSpec {
        AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()
    }

    fn push_envelope() -> CommandEnvelope {
        let item_id = ItemId::mint(1, 0, 1);
        CommandEnvelope {
            command_id: CommandId::new("s3v-after-append"),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids: vec![item_id],
            command: QueueCommand::Push(PushCommand {
                items: vec![PushItem {
                    client_item_key: ClientItemKey::new("s3v-item").unwrap(),
                    item_id,
                    priority: Some(PriorityValue::Int64(1)),
                    not_before: None,
                    group_key: None,
                    max_attempts: 3,
                    payload: None,
                    fields: Default::default(),
                    metadata: Metadata::default(),
                    cohort_size: None,
                    gate_keys: Vec::new(),
                    index_fields: Default::default(),
                    entity_document: None,
                }],
            }),
            checksum: CommandChecksum(0),
            created_at: UtcTimestamp::new(1, 0).unwrap(),
        }
    }

    async fn open(root: &std::path::Path) -> DerivedObjectLogTursoBackend {
        let log_root = root.join("log");
        let projection_path = root.join("projection.db");
        std::fs::create_dir_all(&log_root).unwrap();
        let log =
            ObjectLogEngineStore::open_local(&log_root, flush_config_from_segment(256 * 1_024, 50))
                .await
                .unwrap();
        let projection = open_turso_projection_async(&projection_path).await.unwrap();
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            projection_path,
            0,
            Some(spec()),
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn after_append_before_apply_poisons_then_recovers_authoritatively() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3v-turso-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let epoch = backend.current_epoch(&shard).await.unwrap();
        let outcome = backend
            .commit_raw(
                RawCommitRequest::new(shard.clone(), vec![push_envelope()], epoch)
                    .with_fault(RawCommitFault::AfterAppendBeforeApply),
            )
            .await
            .expect("AfterAppendBeforeApply must withhold applied success, not fail the append");
        assert!(
            !outcome.projection_applied(),
            "withheld-success must remain unapplied"
        );

        let snap = backend.async_projection_snapshot(&shard).await.unwrap();
        assert!(
            snap.poison_reason.is_some(),
            "AfterAppendBeforeApply must latch coordinator poison"
        );
        assert_eq!(
            snap.apply_queue_depth, 1,
            "post-position AfterAppendBeforeApply must not cancel the reservation"
        );
        let page = AsyncLogStore::read_from(backend.log.as_ref(), shard.clone(), None, 16)
            .await
            .unwrap();
        assert_eq!(
            page.entries.len(),
            1,
            "produce allocated a durable position that must remain occupied"
        );
        assert!(
            matches!(
                backend
                    .push(
                        &shard,
                        vec![PushSpec::default()],
                        UtcTimestamp::new(2, 0).unwrap(),
                        None,
                    )
                    .await
                    .unwrap_err(),
                EngineError::Storage(message)
                    if message.contains("async projection poisoned")
            ),
            "poisoned shard must reject new reservations"
        );
        drop(backend);

        let reopened = open(&root).await;
        assert_eq!(
            reopened.metrics(&shard).await.unwrap().pending,
            1,
            "reopen must rebuild the durable reservation authoritatively"
        );
        drop(reopened);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, feature = "objectlog"))]
mod s4b_lifecycle {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use fireweed_engine::{DispatchError, PushPort};
    use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    async fn open(root: &std::path::Path) -> DerivedObjectLogTursoBackend {
        let log_root = root.join("log");
        let projection_path = root.join("projection.db");
        std::fs::create_dir_all(&log_root).unwrap();
        let log =
            ObjectLogEngineStore::open_local(&log_root, flush_config_from_segment(256 * 1_024, 50))
                .await
                .unwrap();
        let projection = open_turso_projection_async(&projection_path).await.unwrap();
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            projection_path,
            0,
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_objectlog_turso_drains_registered_driver() {
        let lib = include_str!("lib.rs");
        assert!(
            lib.contains("struct ObjectLogTursoLifecycle"),
            "S4b installs ObjectLogTursoLifecycle on the object-log × Turso product"
        );
        let shutdown = lib
            .split("impl ProjectionLifecycle for ObjectLogTursoLifecycle")
            .nth(1)
            .and_then(|rest| rest.split("fn shutdown(&mut self)").nth(1))
            .expect("ObjectLogTursoLifecycle::shutdown");
        let shutdown_body = shutdown.split("fn ").next().expect("shutdown body");
        assert!(
            shutdown_body.contains("block_on_objectlog"),
            "sync shutdown must use the flavor-safe object-log runtime bridge"
        );
        assert!(
            !shutdown_body.contains("futures::executor::block_on")
                && !shutdown_body.contains("block_in_place"),
            "shutdown must never nest block_on on the caller's runtime"
        );
        assert!(
            shutdown_body.contains("close_and_drain"),
            "shutdown must close admission and await the coordinator registry"
        );

        let root = std::env::temp_dir().join(format!(
            "fireweed-s4b-turso-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = Arc::new(open(&root).await);
        let driver = backend
            .register_driver()
            .expect("open coordinator registry accepts a driver");
        let published = Arc::new(AtomicBool::new(false));
        let published_flag = Arc::clone(&published);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(80));
            published_flag.store(true, Ordering::Release);
            drop(driver);
        });

        let lifecycle = crate::ObjectLogTursoLifecycle {
            backend: Some(Arc::clone(&backend)),
            max_tail_commands: 1_000_000,
        };
        drop(lifecycle);

        assert!(
            published.load(Ordering::Acquire),
            "dropping ObjectLogTursoLifecycle must wait registered drivers through publication"
        );
        assert!(
            matches!(backend.register_driver(), Err(DispatchError::Closed)),
            "close admission must reject queued/unsubmitted drivers"
        );
        assert!(
            matches!(
                backend
                    .push(
                        &fireweed_engine::QueueKey::new(
                            fireweed_core::TenantId::new("t").unwrap(),
                            fireweed_core::QueueId::new("closed").unwrap(),
                        ),
                        vec![PushSpec::default()],
                        fireweed_core::UtcTimestamp::new(1, 0).unwrap(),
                        None,
                    )
                    .await
                    .unwrap_err(),
                EngineError::Storage(_)
            ),
            "closed product admission must reject new appends"
        );
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(feature = "objectlog")]
fn fold_lifecycle_metrics(
    mut snapshot: fireweed_turso::MetricsMembershipSnapshot,
    base: &CommandPosition,
    target: &CommandPosition,
    tail: &[(CommandPosition, fireweed_objectlog::RetainedLifecycleChange)],
) -> Option<QueueMetrics> {
    use fireweed_objectlog::RetainedLifecycleChange;
    let applied = snapshot.position.as_ref()?;
    if applied.queue != base.queue
        || applied.backend_epoch != base.backend_epoch
        || applied.sequence < base.sequence
        || target.queue != base.queue
        || target.backend_epoch != base.backend_epoch
        || snapshot.cursor_epoch != Some(base.backend_epoch)
    {
        return None;
    }
    if position_covers(Some(applied), target) {
        return Some(snapshot.metrics);
    }
    let mut next = applied.sequence.checked_add(1)?;
    fn count(metrics: &mut QueueMetrics, state: ItemState) -> &mut u64 {
        match state {
            ItemState::Pending => &mut metrics.pending,
            ItemState::Leased => &mut metrics.leased,
            ItemState::Complete => &mut metrics.complete,
            ItemState::Failed => &mut metrics.failed,
        }
    }
    for (position, change) in tail {
        if position.sequence <= applied.sequence {
            continue;
        }
        if position.queue != base.queue
            || position.backend_epoch != base.backend_epoch
            || position.sequence != next
            || position.sequence > target.sequence
        {
            return None;
        }
        next = next.checked_add(1)?;
        let mut seen = HashSet::new();
        let mut apply = |id: ItemId, replacement: Option<(ItemState, u64)>| -> Option<()> {
            if !seen.insert(id) {
                return None;
            }
            let row = snapshot.rows.get_mut(&id)?;
            if row.superseded {
                return None;
            }
            let before = row.state?;
            let version = row.item_version?;
            let next_version = version.checked_add(1).filter(|n| *n <= i64::MAX as u64)?;
            let after = match replacement {
                None if before == ItemState::Pending => ItemState::Leased,
                Some((state, version)) if state != ItemState::Leased && version == next_version => {
                    state
                }
                _ => return None,
            };
            let old_count = count(&mut snapshot.metrics, before);
            *old_count = old_count.checked_sub(1)?;
            let new_count = count(&mut snapshot.metrics, after);
            *new_count = new_count.checked_add(1)?;
            row.state = Some(after);
            row.item_version = Some(next_version);
            Some(())
        };
        match change {
            RetainedLifecycleChange::Claim(ids) => {
                for id in ids {
                    apply(*id, None)?;
                }
            }
            RetainedLifecycleChange::Replace(items) => {
                for (id, state, version) in items {
                    apply(*id, Some((*state, *version)))?;
                }
            }
        }
    }
    if next != target.sequence.checked_add(1)? {
        return None;
    }
    snapshot.metrics.resident_terminal_count = snapshot
        .metrics
        .complete
        .checked_add(snapshot.metrics.failed)?;
    Some(snapshot.metrics)
}

#[cfg(feature = "objectlog")]
fn fold_membership_metrics(
    mut snapshot: fireweed_turso::MetricsMembershipSnapshot,
    base: Option<&CommandPosition>,
    target: &CommandPosition,
    tail: &[(
        CommandPosition,
        fireweed_objectlog::RetainedMembershipChange,
    )],
) -> Option<QueueMetrics> {
    use fireweed_objectlog::RetainedMembershipChange;
    let base_next = match base {
        Some(base) if base.queue == target.queue && base.backend_epoch == target.backend_epoch => {
            base.sequence.checked_add(1)?
        }
        None if target.backend_epoch == 0 => 0,
        _ => return None,
    };
    if snapshot.cursor_epoch != Some(target.backend_epoch) {
        return None;
    }
    let applied = snapshot.position.as_ref();
    let mut next = match applied {
        Some(applied)
            if applied.queue == target.queue && applied.backend_epoch == target.backend_epoch =>
        {
            applied.sequence.checked_add(1)?
        }
        None => 0,
        _ => return None,
    };
    if next < base_next {
        return None;
    }
    if position_covers(applied, target) {
        return Some(snapshot.metrics);
    }
    let snapshot_next = next;
    for (position, change) in tail {
        if position.sequence < snapshot_next {
            continue;
        }
        if position.queue != target.queue
            || position.backend_epoch != target.backend_epoch
            || position.sequence != next
            || position.sequence > target.sequence
        {
            return None;
        }
        next = next.checked_add(1)?;
        match change {
            RetainedMembershipChange::Push(items) => {
                for (id, _) in items {
                    let row = snapshot.rows.remove(id)?;
                    if row.state.is_some() || row.active_key_exists {
                        return None;
                    }
                    snapshot.metrics.pending = snapshot.metrics.pending.checked_add(1)?;
                }
            }
            RetainedMembershipChange::Purge(ids) => {
                for id in ids {
                    let row = snapshot.rows.remove(id)?;
                    if row.superseded {
                        continue;
                    }
                    let count = match row.state {
                        None => continue,
                        Some(ItemState::Pending) => &mut snapshot.metrics.pending,
                        Some(ItemState::Leased) => &mut snapshot.metrics.leased,
                        Some(ItemState::Complete) => &mut snapshot.metrics.complete,
                        Some(ItemState::Failed) => &mut snapshot.metrics.failed,
                    };
                    *count = count.checked_sub(1)?;
                }
            }
        }
    }
    if next != target.sequence.checked_add(1)? {
        return None;
    }
    snapshot.metrics.resident_terminal_count = snapshot
        .metrics
        .complete
        .checked_add(snapshot.metrics.failed)?;
    Some(snapshot.metrics)
}

#[cfg(all(test, feature = "objectlog"))]
mod s3c_activation {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use fireweed_core::{
        ClientItemKey, EligibilityPolicy, GroupKey, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        ClaimCompatibility, ClaimPort, EngineError, ProjectionRead, PushPort, PushSpec,
    };
    use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    fn qdef(queue: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new(queue).unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
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

    fn spec() -> AsyncProjectionSpec {
        AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()
    }

    async fn open(root: &std::path::Path) -> DerivedObjectLogTursoBackend {
        let log_root = root.join("log");
        let projection_path = root.join("projection.db");
        std::fs::create_dir_all(&log_root).unwrap();
        let log =
            ObjectLogEngineStore::open_local(&log_root, flush_config_from_segment(256 * 1_024, 50))
                .await
                .unwrap();
        let projection = open_turso_projection_async(&projection_path).await.unwrap();
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            projection_path,
            0,
            Some(spec()),
        )
        .await
        .unwrap()
    }

    async fn push_until_admitted(
        backend: &DerivedObjectLogTursoBackend,
        shard: &fireweed_engine::QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
    ) -> Vec<fireweed_core::ItemId> {
        let started = std::time::Instant::now();
        loop {
            match backend.push(shard, items.clone(), now, None).await {
                Ok(ids) => return ids,
                Err(EngineError::Backpressure { .. })
                    if started.elapsed() < std::time::Duration::from_secs(5) =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("concurrent push: {error:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn metrics_fallback_waits_only_for_its_captured_log_target() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let root = std::env::temp_dir().join(format!(
                "fireweed-metrics-fixed-target-{}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            let backend = Arc::new(open(&root).await);
            let definition = qdef("metrics-fixed-target");
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            backend.create_queue(definition).await.unwrap();
            backend
                .push(
                    &shard,
                    vec![PushSpec::default()],
                    UtcTimestamp::new(1, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            backend.peek(&shard, 1).await.unwrap();
            let coordinator = backend.async_apply.as_ref().unwrap().clone();
            coordinator.pause();
            let base = coordinator
                .snapshot(&shard)
                .await
                .applied_high_water
                .unwrap();
            // Exceed the bounded membership shortcut so this read must use its
            // physical coverage fallback, even when retained commands are present.
            for _ in 0..9 {
                backend
                    .push(
                        &shard,
                        vec![PushSpec::default(); 1000],
                        UtcTimestamp::new(2, 0).unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
            }
            let target = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                .await
                .unwrap()
                .unwrap();
            let page =
                AsyncLogStore::read_from(backend.log.as_ref(), shard.clone(), Some(base), 128)
                    .await
                    .unwrap();
            assert_eq!(page.entries.last().unwrap().0, target);
            let (positions, commands): (Vec<_>, Vec<_>) = page.entries.into_iter().unzip();
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            *backend.metrics_snapshot_hook.lock().unwrap() =
                Some((entered.clone(), release.clone()));
            let mut read = {
                let backend = backend.clone();
                let shard = shard.clone();
                tokio::spawn(async move { backend.metrics(&shard).await })
            };
            entered.notified().await;
            // This write arrived after the read's entry target and SQL snapshot.
            backend
                .push(
                    &shard,
                    vec![PushSpec::default()],
                    UtcTimestamp::new(3, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let later = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                .await
                .unwrap()
                .unwrap();
            assert!(later.sequence > target.sequence);
            // Deterministically advance only the captured prefix, leaving the
            // later durable write unapplied. Background apply stays paused.
            AsyncProjectionStore::apply_live(backend.projection.as_ref(), positions, commands)
                .await
                .unwrap();
            coordinator
                .seed_high_water(shard.clone(), Some(target.clone()))
                .await;
            release.notify_one();
            let result = tokio::time::timeout(Duration::from_millis(250), &mut read).await;
            if result.is_err() {
                read.abort();
            }
            assert_eq!(
                result
                    .expect("metrics must not retarget to the later write")
                    .unwrap()
                    .unwrap()
                    .pending,
                9001
            );
            assert_eq!(
                coordinator.snapshot(&shard).await.applied_high_water,
                Some(target)
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), backend.peek(&shard, 1))
                    .await
                    .is_err(),
                "a new physical read must still wait for the later write"
            );
            coordinator.resume();
            backend.peek(&shard, 1).await.unwrap();
            assert_eq!(backend.metrics(&shard).await.unwrap().pending, 9002);
            drop(coordinator);
            drop(backend);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn metrics_read_durable_push_and_purge_without_advancing_projection() {
        tokio::time::timeout(Duration::from_secs(20), async {
            let root = std::env::temp_dir().join(format!(
                "fireweed-membership-tail-{}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            let backend = open(&root).await;
            let definition = qdef("membership-tail");
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            backend.create_queue(definition).await.unwrap();
            let coordinator = backend.async_apply.as_ref().unwrap().clone();
            coordinator.pause();
            backend
                .push(
                    &shard,
                    vec![PushSpec::default(); 2],
                    UtcTimestamp::new(1, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("genesis push counts do not wait for paused apply")
                .unwrap();
            assert_eq!(metrics.pending, 2);
            let (sql, cursor) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!(sql.pending, 0);
            assert_eq!(cursor, None);
            assert_eq!(coordinator.snapshot(&shard).await.applied_high_water, None);
            // A metrics read is not a physical apply barrier. Row reads still are.
            assert!(
                tokio::time::timeout(Duration::from_millis(50), backend.peek(&shard, 10))
                    .await
                    .is_err()
            );
            coordinator.resume();
            let initial = backend.peek(&shard, 10).await.unwrap();
            assert_eq!(initial.len(), 2);
            coordinator.pause();
            let before = coordinator.snapshot(&shard).await.applied_high_water;
            backend
                .push(
                    &shard,
                    vec![PushSpec::default(); 2],
                    UtcTimestamp::new(2, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("durable push counts do not wait for paused apply")
                .unwrap();
            assert_eq!(metrics.pending, 4);
            let (sql, cursor) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!(sql.pending, 2);
            assert_eq!(cursor, before);
            assert_eq!(
                coordinator.snapshot(&shard).await.applied_high_water,
                before
            );
            coordinator.resume();
            let all = backend.peek(&shard, 10).await.unwrap();
            assert_eq!(all.len(), 4);
            coordinator.pause();
            let before = coordinator.snapshot(&shard).await.applied_high_water;
            backend
                .purge(
                    &shard,
                    all[..3].iter().map(|i| i.item_id).collect(),
                    true,
                    UtcTimestamp::new(3, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("durable purge counts do not wait for paused apply")
                .unwrap();
            assert_eq!(metrics.pending, 1);
            let (sql, cursor) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!(sql.pending, 4);
            assert_eq!(cursor, before);
            assert_eq!(
                coordinator.snapshot(&shard).await.applied_high_water,
                before
            );
            coordinator.resume();
            assert_eq!(backend.peek(&shard, 10).await.unwrap().len(), 1);
            drop(coordinator);
            drop(backend);
            let reopened = open(&root).await;
            assert_eq!(reopened.metrics(&shard).await.unwrap().pending, 1);
            drop(reopened);
            std::fs::remove_dir_all(root).unwrap();
        })
        .await
        .unwrap();
    }

    #[test]
    fn lifecycle_metrics_rebase_and_validate_row_versions() {
        use fireweed_objectlog::RetainedLifecycleChange as Change;
        use fireweed_turso::{MetricsMembershipRow, MetricsMembershipSnapshot};
        let shard = QueueKey::new(
            TenantId::new("t").unwrap(),
            QueueId::new("lifecycle").unwrap(),
        );
        let pos = |n| CommandPosition::new(shard.clone(), 0, n);
        let id = |n| ItemId::mint(1, 0, n);
        let snapshot = |sequence, rows: Vec<(u32, ItemState, u64, bool)>| {
            let mut metrics = QueueMetrics::default();
            let rows = rows
                .into_iter()
                .map(|(n, state, version, superseded)| {
                    if !superseded {
                        match state {
                            ItemState::Pending => metrics.pending += 1,
                            ItemState::Leased => metrics.leased += 1,
                            ItemState::Complete => metrics.complete += 1,
                            ItemState::Failed => metrics.failed += 1,
                        }
                    }
                    (
                        id(n),
                        MetricsMembershipRow {
                            state: Some(state),
                            item_version: Some(version),
                            superseded,
                            active_key_exists: false,
                        },
                    )
                })
                .collect();
            metrics.resident_terminal_count = metrics.complete + metrics.failed;
            MetricsMembershipSnapshot {
                cursor_epoch: Some(0),
                position: Some(pos(sequence)),
                metrics,
                rows,
            }
        };
        let tail = vec![
            (pos(1), Change::Claim(vec![id(1), id(2), id(3)])),
            (
                pos(2),
                Change::Replace(vec![
                    (id(1), ItemState::Pending, 3),
                    (id(2), ItemState::Complete, 3),
                    (id(3), ItemState::Failed, 3),
                ]),
            ),
        ];
        for (sequence, state, version) in [(0, ItemState::Pending, 1), (1, ItemState::Leased, 2)] {
            let s = snapshot(
                sequence,
                (1..=3).map(|n| (n, state, version, false)).collect(),
            );
            let m = fold_lifecycle_metrics(s, &pos(0), &pos(2), &tail).unwrap();
            assert_eq!(
                (
                    m.pending,
                    m.leased,
                    m.complete,
                    m.failed,
                    m.resident_terminal_count
                ),
                (1, 0, 1, 1, 2)
            );
        }
        // Valid repeated IDs across commands represent another claim/release cycle.
        let mut repeated = tail.clone();
        repeated.extend([
            (pos(3), Change::Claim(vec![id(1)])),
            (pos(4), Change::Replace(vec![(id(1), ItemState::Failed, 5)])),
        ]);
        let m = fold_lifecycle_metrics(
            snapshot(
                0,
                (1..=3).map(|n| (n, ItemState::Pending, 1, false)).collect(),
            ),
            &pos(0),
            &pos(4),
            &repeated,
        )
        .unwrap();
        assert_eq!((m.pending, m.leased, m.complete, m.failed), (0, 0, 1, 2));
        // Missing row, incorrect old state/version, superseded row and overflow.
        for rows in [
            vec![],
            vec![(1, ItemState::Leased, 1, false)],
            vec![(1, ItemState::Pending, 9, false)],
            vec![(1, ItemState::Pending, 1, true)],
            vec![(1, ItemState::Pending, i64::MAX as u64, false)],
        ] {
            let t = [
                (pos(1), Change::Claim(vec![id(1)])),
                (
                    pos(2),
                    Change::Replace(vec![(id(1), ItemState::Pending, 3)]),
                ),
            ];
            assert!(fold_lifecycle_metrics(snapshot(0, rows), &pos(0), &pos(2), &t).is_none());
        }
        let mut missing_version = snapshot(0, vec![(1, ItemState::Pending, 1, false)]);
        missing_version.rows.get_mut(&id(1)).unwrap().item_version = None;
        assert!(fold_lifecycle_metrics(missing_version, &pos(0), &pos(1), &tail[..1]).is_none());
        let mut underflow = snapshot(0, vec![(1, ItemState::Pending, 1, false)]);
        underflow.metrics.pending = 0;
        assert!(
            fold_lifecycle_metrics(
                underflow,
                &pos(0),
                &pos(1),
                &[(pos(1), Change::Claim(vec![id(1)]))]
            )
            .is_none()
        );
        for changes in [
            vec![(pos(2), Change::Claim(vec![id(1)]))],
            vec![(pos(1), Change::Claim(vec![id(1), id(1)]))],
            vec![(pos(1), Change::Replace(vec![(id(1), ItemState::Leased, 2)]))],
            vec![(
                pos(1),
                Change::Replace(vec![
                    (id(1), ItemState::Pending, 2),
                    (id(1), ItemState::Complete, 3),
                ]),
            )],
        ] {
            let target = changes.last().unwrap().0.clone();
            assert!(
                fold_lifecycle_metrics(
                    snapshot(0, vec![(1, ItemState::Pending, 1, false)]),
                    &pos(0),
                    &target,
                    &changes
                )
                .is_none()
            );
        }
        let mut foreign = snapshot(0, vec![]);
        foreign.cursor_epoch = Some(1);
        assert!(fold_lifecycle_metrics(foreign, &pos(0), &pos(1), &tail[..1]).is_none());
        assert!(fold_lifecycle_metrics(snapshot(0, vec![]), &pos(1), &pos(2), &tail).is_none());
        let mut covered = snapshot(3, vec![]);
        covered.metrics.pending = 7;
        assert_eq!(
            fold_lifecycle_metrics(covered, &pos(0), &pos(2), &tail)
                .unwrap()
                .pending,
            7
        );
        // A mutation-only tail is validated against the actual leased SQL version.
        let m = fold_lifecycle_metrics(
            snapshot(0, vec![(1, ItemState::Leased, 7, false)]),
            &pos(0),
            &pos(1),
            &[(
                pos(1),
                Change::Replace(vec![(id(1), ItemState::Complete, 8)]),
            )],
        )
        .unwrap();
        assert_eq!((m.leased, m.complete), (0, 1));
    }

    #[test]
    fn membership_metrics_rebases_and_rejects_conflicts_gaps_and_underflow() {
        use fireweed_objectlog::RetainedMembershipChange as Change;
        use fireweed_turso::{MetricsMembershipRow, MetricsMembershipSnapshot};
        let shard = QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap());
        let pos = |sequence| CommandPosition::new(shard.clone(), 0, sequence);
        let id = |n| ItemId::mint(1, 0, n);
        let push = |n| Change::Push(vec![(id(n), ClientItemKey::new(format!("k{n}")).unwrap())]);
        let snapshot = |sequence, pending, rows: Vec<(ItemId, Option<ItemState>, bool, bool)>| {
            MetricsMembershipSnapshot {
                cursor_epoch: Some(0),
                metrics: QueueMetrics {
                    pending,
                    ..Default::default()
                },
                position: Some(pos(sequence)),
                rows: rows
                    .into_iter()
                    .map(|(id, state, superseded, active_key_exists)| {
                        (
                            id,
                            MetricsMembershipRow {
                                item_version: state.map(|_| 1),
                                state,
                                superseded,
                                active_key_exists,
                            },
                        )
                    })
                    .collect(),
            }
        };
        let genesis_tail = vec![(pos(0), push(0)), (pos(1), push(1))];
        for epoch in [None, Some(1), Some(0)] {
            let mut s = snapshot(
                0,
                0,
                vec![(id(0), None, false, false), (id(1), None, false, false)],
            );
            s.position = None;
            s.cursor_epoch = epoch;
            let result = fold_membership_metrics(s, None, &pos(1), &genesis_tail);
            if epoch == Some(0) {
                assert_eq!(result.unwrap().pending, 2);
            } else {
                assert!(result.is_none());
            }
        }
        let s = snapshot(
            0,
            1,
            vec![
                (id(0), Some(ItemState::Pending), false, true),
                (id(1), None, false, false),
            ],
        );
        assert_eq!(
            fold_membership_metrics(s, None, &pos(1), &genesis_tail)
                .unwrap()
                .pending,
            2
        );
        let tail = vec![(pos(1), push(1)), (pos(2), push(2))];
        // Apply advanced while row presence was being read. The first push is
        // already represented in both the SQL counter and the existing row.
        let s = snapshot(
            1,
            1,
            vec![
                (id(1), Some(ItemState::Pending), false, true),
                (id(2), None, false, false),
            ],
        );
        assert_eq!(
            fold_membership_metrics(s, Some(&pos(0)), &pos(2), &tail)
                .unwrap()
                .pending,
            2
        );
        for (state, key_exists) in [(Some(ItemState::Pending), false), (None, true)] {
            let s = snapshot(
                0,
                0,
                vec![
                    (id(1), state, false, key_exists),
                    (id(2), None, false, false),
                ],
            );
            assert!(fold_membership_metrics(s, Some(&pos(0)), &pos(2), &tail).is_none());
        }
        assert!(
            fold_membership_metrics(
                snapshot(0, 0, vec![(id(2), None, false, false)]),
                Some(&pos(0)),
                &pos(2),
                &tail[1..]
            )
            .is_none()
        );
        assert!(
            fold_membership_metrics(snapshot(0, 0, vec![]), Some(&pos(1)), &pos(2), &tail)
                .is_none()
        );
        let purge = vec![(pos(1), Change::Purge(vec![id(1), id(2), id(3)]))];
        let s = snapshot(
            0,
            1,
            vec![
                (id(1), Some(ItemState::Pending), false, false),
                (id(2), Some(ItemState::Pending), true, false),
                (id(3), None, false, false),
            ],
        );
        assert_eq!(
            fold_membership_metrics(s, Some(&pos(0)), &pos(1), &purge)
                .unwrap()
                .pending,
            0
        );
        let s = snapshot(0, 0, vec![(id(1), Some(ItemState::Pending), false, false)]);
        assert!(fold_membership_metrics(s, Some(&pos(0)), &pos(1), &purge).is_none());
        let mut all_states = snapshot(
            0,
            1,
            vec![
                (id(1), Some(ItemState::Pending), false, false),
                (id(2), Some(ItemState::Leased), false, false),
                (id(3), Some(ItemState::Complete), false, false),
                (id(4), Some(ItemState::Failed), false, false),
            ],
        );
        all_states.metrics.leased = 1;
        all_states.metrics.complete = 1;
        all_states.metrics.failed = 1;
        all_states.metrics.resident_terminal_count = 2;
        let all_purged = fold_membership_metrics(
            all_states,
            Some(&pos(0)),
            &pos(1),
            &[(pos(1), Change::Purge((1..=4).map(id).collect()))],
        )
        .unwrap();
        assert_eq!(
            (
                all_purged.pending,
                all_purged.leased,
                all_purged.complete,
                all_purged.failed,
                all_purged.resident_terminal_count
            ),
            (0, 0, 0, 0, 0)
        );
        // A snapshot that already covers the target needs no retained rows.
        assert_eq!(
            fold_membership_metrics(snapshot(3, 7, vec![]), Some(&pos(0)), &pos(2), &tail)
                .unwrap()
                .pending,
            7
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn addressed_mutation_appends_behind_unapplied_claim_with_lease_guards() {
        use fireweed_engine::{
            AddressedMutation, ItemMutationOperation, ItemMutationOutcome, ItemMutationReturning,
            ItemPatch, LeaseGuard, LifecyclePatch,
        };
        tokio::time::timeout(Duration::from_secs(20), async {
            let root = std::env::temp_dir().join(format!(
                "fireweed-claim-tail-{}-{}",
                std::process::id(),
                NEXT_DIR.fetch_add(1, Ordering::Relaxed)
            ));
            let backend = Arc::new(open(&root).await);
            let definition = qdef("claim-tail");
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            backend.create_queue(definition.clone()).await.unwrap();
            backend
                .push(
                    &shard,
                    vec![PushSpec::default(), PushSpec::default()],
                    UtcTimestamp::new(1, 0).unwrap(),
                    None,
                )
                .await
                .unwrap();
            backend.peek(&shard, 1).await.unwrap(); // cover the push before pausing
            let coordinator = backend.async_apply.as_ref().unwrap().clone();
            coordinator.pause();
            let applied = coordinator
                .snapshot(&shard)
                .await
                .applied_high_water
                .unwrap();
            let token = LeaseToken::new("claim-tail-token").unwrap();
            let claimed = backend
                .claim(ClaimRequest {
                    shard: shard.clone(),
                    worker_id: WorkerId::new("w").unwrap(),
                    max_items: 2,
                    lease_token: token.clone(),
                    lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
                    now: UtcTimestamp::new(2, 0).unwrap(),
                    eligibility_time: None,
                    compatibility: ClaimCompatibility::default(),
                    expected_epoch: None,
                })
                .await
                .unwrap();
            assert_eq!(claimed.items.len(), 2);
            let target = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                .await
                .unwrap()
                .unwrap();
            let page = AsyncLogStore::read_from(
                backend.log.as_ref(),
                shard.clone(),
                Some(applied.clone()),
                16,
            )
            .await
            .unwrap();
            let tail =
                claim_only_tail(&applied, &target, page.entries.clone()).expect("claim-only tail");
            let retained = coordinator
                .retained_claim_tail(&applied, &target)
                .await
                .unwrap()
                .expect("acknowledged claims are retained before apply");
            assert_eq!(
                serde_json::to_value(&retained).unwrap(),
                serde_json::to_value(&tail).unwrap()
            );
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("durable claim counts must not wait for paused SQL apply")
                .unwrap();
            assert_eq!(
                (
                    metrics.pending,
                    metrics.leased,
                    metrics.complete,
                    metrics.failed
                ),
                (0, 2, 0, 0)
            );
            let (sql_metrics, sql_position) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!((sql_metrics.pending, sql_metrics.leased), (2, 0));
            assert_eq!(sql_position.as_ref(), Some(&applied));
            assert_eq!(
                coordinator
                    .snapshot(&shard)
                    .await
                    .applied_high_water
                    .as_ref(),
                Some(&applied)
            );
            let mut invalid = page.entries.clone();
            invalid[0].0.sequence += 1;
            assert!(
                claim_only_tail(&applied, &target, invalid).is_none(),
                "gaps must wait for coverage"
            );
            let mut invalid = page.entries.clone();
            let QueueCommand::Claim(claim) = &mut invalid[0].1.command else {
                unreachable!()
            };
            claim.authority_first = false;
            assert!(
                claim_only_tail(&applied, &target, invalid).is_none(),
                "historical claims must wait"
            );
            let mut invalid = page.entries.clone();
            let QueueCommand::Claim(claim) = &mut invalid[0].1.command else {
                unreachable!()
            };
            claim.item_ids.push(claim.item_ids[0]);
            assert!(
                claim_only_tail(&applied, &target, invalid).is_none(),
                "repeated IDs must wait"
            );
            let request = ItemMutationRequest {
                request_id: RequestId::new("tail-update").unwrap(),
                evaluated_at: UtcTimestamp::new(3, 0).unwrap(),
                dry_run: false,
                returning: ItemMutationReturning::BeforeSnapshot,
                gate_changes: vec![],
                operation: ItemMutationOperation::Addressed {
                    entries: vec![AddressedMutation {
                        item_id: claimed.items[0].item_id,
                        expected_item_version: Some(claimed.items[0].item_version),
                        predicates: vec![],
                        lease_guard: LeaseGuard::Match(token),
                        patch: ItemPatch {
                            lifecycle: LifecyclePatch::SetComplete,
                            ..Default::default()
                        },
                    }],
                },
            };
            let valid = backend
                .projection
                .plan_addressed_item_mutation(&shard, &definition, &request, &tail)
                .await
                .unwrap();
            assert_eq!(valid.response.summary.changed, 1, "{:?}", valid.response);
            let mut stale = request.clone();
            let ItemMutationOperation::Addressed { entries } = &mut stale.operation else {
                unreachable!()
            };
            entries[0].lease_guard = LeaseGuard::Match(LeaseToken::new("wrong-token").unwrap());
            let rejected = backend
                .projection
                .plan_addressed_item_mutation(&shard, &definition, &stale, &tail)
                .await
                .unwrap();
            assert!(matches!(
                rejected.response.results[0].outcome,
                ItemMutationOutcome::StaleLease
            ));
            let mut expired = request.clone();
            expired.evaluated_at = UtcTimestamp::new(31, 0).unwrap();
            let rejected = backend
                .projection
                .plan_addressed_item_mutation(&shard, &definition, &expired, &tail)
                .await
                .unwrap();
            assert!(matches!(
                rejected.response.results[0].outcome,
                ItemMutationOutcome::StaleLease
            ));
            let mut caught_up_request = request.clone();
            caught_up_request.request_id = RequestId::new("tail-update-second").unwrap();
            let ItemMutationOperation::Addressed { entries } = &mut caught_up_request.operation
            else {
                unreachable!()
            };
            entries[0].item_id = claimed.items[1].item_id;
            entries[0].expected_item_version = Some(claimed.items[1].item_version);
            let worker = {
                let backend = Arc::clone(&backend);
                let shard = shard.clone();
                tokio::spawn(
                    async move { backend.dispatch_item_mutation(&shard, request, None).await },
                )
            };
            loop {
                let high = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                    .await
                    .unwrap()
                    .unwrap();
                if high.sequence > target.sequence {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            assert_eq!(
                coordinator.snapshot(&shard).await.applied_high_water,
                Some(applied)
            );
            assert!(
                !worker.is_finished(),
                "response must still cover the mutation before releasing fence"
            );
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("resolved lifecycle counts do not wait for paused apply")
                .unwrap();
            assert_eq!(
                (
                    metrics.pending,
                    metrics.leased,
                    metrics.complete,
                    metrics.failed
                ),
                (0, 1, 1, 0)
            );
            let (sql_metrics, _) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!(
                (
                    sql_metrics.pending,
                    sql_metrics.leased,
                    sql_metrics.complete
                ),
                (2, 0, 0)
            );
            assert!(
                !worker.is_finished(),
                "mutation response must still cover projection apply"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), backend.peek(&shard, 1))
                    .await
                    .is_err(),
                "physical reads still require coverage"
            );
            coordinator.resume();
            let response = worker.await.unwrap().unwrap();
            assert_eq!(response.summary.changed, 1);
            let metrics = backend.metrics(&shard).await.unwrap();
            assert_eq!(
                (metrics.pending, metrics.leased, metrics.complete),
                (0, 1, 1)
            );
            let caught_up = backend
                .projection
                .plan_addressed_item_mutation(&shard, &definition, &caught_up_request, &tail)
                .await
                .unwrap();
            assert_eq!(
                caught_up.response.summary.changed, 1,
                "already-applied claim must not bump version twice"
            );
            coordinator.pause();
            let before = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                .await
                .unwrap()
                .unwrap();
            let second_worker = {
                let backend = backend.clone();
                let shard = shard.clone();
                tokio::spawn(async move {
                    backend
                        .dispatch_item_mutation(&shard, caught_up_request, None)
                        .await
                })
            };
            loop {
                let high = AsyncLogStore::high_water(backend.log.as_ref(), shard.clone())
                    .await
                    .unwrap()
                    .unwrap();
                if high.sequence > before.sequence {
                    break;
                }
                assert!(
                    !second_worker.is_finished(),
                    "second mutation ended before appending"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let metrics = tokio::time::timeout(Duration::from_millis(250), backend.metrics(&shard))
                .await
                .expect("mutation-only lifecycle tail must not wait for paused apply")
                .unwrap();
            assert_eq!(
                (metrics.pending, metrics.leased, metrics.complete),
                (0, 0, 2)
            );
            let (sql, _) = backend
                .projection
                .server_metrics_with_position_committed(&shard)
                .await
                .unwrap();
            assert_eq!((sql.leased, sql.complete), (1, 1));
            assert!(!second_worker.is_finished());
            coordinator.resume();
            assert_eq!(second_worker.await.unwrap().unwrap().summary.changed, 1);
            assert_eq!(backend.metrics(&shard).await.unwrap().complete, 2);
            drop(coordinator);
            drop(backend);
            let _ = std::fs::remove_dir_all(root);
        })
        .await
        .expect("unapplied-claim mutation must append without waiting for SQL");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_reads_use_outcome_pool_after_request_entry_high_water() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-reads-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef("q-s3c-reads");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let before = backend
            .projection()
            .committed_pools()
            .expect("file-backed pools")
            .outcome_borrow_count();
        backend
            .push(
                &shard,
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new("s3c-read").unwrap()),
                    payload: None,
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let peeked = backend.peek(&shard, 8).await.unwrap();
        assert_eq!(peeked.len(), 1);
        let metrics = backend.metrics(&shard).await.unwrap();
        assert_eq!(metrics.pending, 1);
        let after = backend
            .projection()
            .committed_pools()
            .expect("file-backed pools")
            .outcome_borrow_count();
        assert!(
            after > before,
            "public reads must borrow the outcome pool, before={before} after={after}"
        );
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grouped_and_item_claim_use_retained_carrier_without_sql_first_lease() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-claim-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef("q-s3c-claim");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new("s3c-item").unwrap()),
                    group_key: Some(GroupKey::new("g1").unwrap()),
                    payload: Some(Bytes::from_static(b"body")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let item_claimed = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("tok-item").unwrap(),
                lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
                now: UtcTimestamp::new(2, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(item_claimed.items.len(), 1);
        assert_eq!(
            item_claimed.items[0].payload.as_ref().map(Bytes::as_ref),
            Some(&b"body"[..])
        );
        backend
            .push(
                &shard,
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new("s3c-grouped").unwrap()),
                    group_key: Some(GroupKey::new("g2").unwrap()),
                    payload: Some(Bytes::from_static(b"grouped")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(3, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let grouped = backend
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w2").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("tok-group").unwrap(),
                lease_expires_at: UtcTimestamp::new(40, 0).unwrap(),
                now: UtcTimestamp::new(4, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility {
                    group_key: Some(GroupKey::new("g2").unwrap()),
                    ..ClaimCompatibility::default()
                },
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(grouped.items.len(), 1);
        assert_eq!(
            grouped.items[0].payload.as_ref().map(Bytes::as_ref),
            Some(&b"grouped"[..])
        );
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn delayed_generation_waiter_retains_result_without_global_cache() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-delayed-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let shard = QueueKey::new(
            TenantId::new("t").unwrap(),
            QueueId::new("delayed").unwrap(),
        );
        let delayed = backend.ensure_generation_join(&shard, 0);
        let notified = delayed.notify.notified();
        backend.publish_generation_outcome(&shard, 0, &delayed, Err(EngineError::Conflict));
        // Do not poll the original waiter until well beyond the old cache's
        // capacity. Completion ownership must survive unrelated publication.
        for id in 1..256 {
            let join = backend.ensure_generation_join(&shard, id);
            backend.publish_generation_outcome(&shard, id, &join, Ok(Vec::new()));
        }
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .unwrap();
        assert!(matches!(
            *delayed.outcome.lock().unwrap(),
            Some(Err(EngineError::Conflict))
        ));
        assert!(backend.generation_joins.lock().unwrap().is_empty());
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn eight_inflight_pushes_all_land_exactly_once() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-fill-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = Arc::new(open(&root).await);
        let definition = qdef("q-s3c-fill");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let futs = (0..CLAIM_GENERATION_MAX_REQUESTS).map(|i| {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            async move {
                push_until_admitted(
                    &backend,
                    &shard,
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("fill-{i}")).unwrap()),
                        payload: Some(Bytes::from(format!("p{i}"))),
                        ..PushSpec::default()
                    }],
                    now,
                )
                .await
            }
        });
        let results = futures::future::join_all(futs).await;
        assert_eq!(results.len(), CLAIM_GENERATION_MAX_REQUESTS);
        for ids in &results {
            assert_eq!(ids.len(), 1);
        }
        let mut unique = results.into_iter().flatten().collect::<Vec<_>>();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), CLAIM_GENERATION_MAX_REQUESTS);
        assert_eq!(
            backend.metrics(&shard).await.unwrap().pending as usize,
            CLAIM_GENERATION_MAX_REQUESTS
        );
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn sixteen_inflight_pushes_overlay_across_generations_exactly_once() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-overlay-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = Arc::new(open(&root).await);
        let definition = qdef("q-s3c-overlay");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let n = CLAIM_GENERATION_MAX_REQUESTS * 2;
        let futs = (0..n).map(|i| {
            let backend = Arc::clone(&backend);
            let shard = shard.clone();
            async move {
                push_until_admitted(
                    &backend,
                    &shard,
                    vec![PushSpec {
                        client_item_key: Some(ClientItemKey::new(format!("overlay-{i}")).unwrap()),
                        payload: Some(Bytes::from(format!("p{i}"))),
                        ..PushSpec::default()
                    }],
                    now,
                )
                .await
            }
        });
        let results = futures::future::join_all(futs).await;
        assert_eq!(results.len(), n);
        let mut unique = results.into_iter().flatten().collect::<Vec<_>>();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), n);
        assert_eq!(backend.metrics(&shard).await.unwrap().pending as usize, n);
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn duplicate_client_key_conflicts_after_apply_covers() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-dup-applied-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef("q-s3c-dup-applied");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        let now = UtcTimestamp::new(1, 0).unwrap();
        let spec = PushSpec {
            client_item_key: Some(ClientItemKey::new("same").unwrap()),
            payload: Some(Bytes::from_static(b"a")),
            ..PushSpec::default()
        };
        backend
            .push(&shard, vec![spec.clone()], now, None)
            .await
            .expect("first push");
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
        let duplicate = backend.push(&shard, vec![spec], now, None).await;
        assert!(
            matches!(duplicate, Err(EngineError::Conflict)),
            "applied identity must reject the duplicate key after prune, got {duplicate:?}"
        );
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn duplicate_client_key_conflicts_after_reopen() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-dup-reopen-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let definition = qdef("q-s3c-dup-reopen");
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let spec = PushSpec {
            client_item_key: Some(ClientItemKey::new("same").unwrap()),
            payload: Some(Bytes::from_static(b"a")),
            ..PushSpec::default()
        };
        let now = UtcTimestamp::new(1, 0).unwrap();
        {
            let backend = open(&root).await;
            backend.create_queue(definition).await.unwrap();
            backend
                .push(&shard, vec![spec.clone()], now, None)
                .await
                .expect("first push");
            assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
            drop(backend);
        }
        let reopened = open(&root).await;
        let duplicate = reopened.push(&shard, vec![spec], now, None).await;
        assert!(
            matches!(duplicate, Err(EngineError::Conflict)),
            "reopen hydrate must reject the duplicate key, got {duplicate:?}"
        );
        assert_eq!(reopened.metrics(&shard).await.unwrap().pending, 1);
        drop(reopened);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, feature = "objectlog"))]
mod s8c_outbox_migration {
    use std::sync::atomic::{AtomicU64, Ordering};

    use fireweed_core::{
        EligibilityPolicy, LeaseToken, OrderingMode, PriorityDirection, PriorityModel,
        PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy,
        RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };
    use fireweed_engine::{
        AsyncLogStore, AsyncProjectionSpec, ClaimCompatibility, ClaimPort, FinalizeKind,
        FinalizeOutcome, FinalizePort, ProjectionRead, PushPort, PushSpec, QueueCommand,
    };
    use fireweed_objectlog::{ObjectLogEngineStore, flush_config_from_segment};
    use sha2::{Digest, Sha256};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

    fn qdef() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("t").unwrap(),
            queue_id: QueueId::new("q-s8c-outbox").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
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

    fn spec() -> AsyncProjectionSpec {
        AsyncProjectionSpec::new(32, 1024 * 1024, 16, 30_000, 3).unwrap()
    }

    async fn open(root: &std::path::Path) -> DerivedObjectLogTursoBackend {
        let log_root = root.join("log");
        let projection_path = root.join("projection.db");
        std::fs::create_dir_all(&log_root).unwrap();
        let log =
            ObjectLogEngineStore::open_local(&log_root, flush_config_from_segment(256 * 1_024, 50))
                .await
                .unwrap();
        let projection = open_turso_projection_async(&projection_path).await.unwrap();
        DerivedObjectLogTursoBackend::from_log_and_projection(
            log,
            projection,
            projection_path,
            0,
            Some(spec()),
        )
        .await
        .unwrap()
    }

    fn assert_sql_first_serving_path_removed() {
        let local = include_str!("../../fireweed-turso/src/local.rs");
        let production_local = local
            .split("#[cfg(test)]")
            .next()
            .expect("production local");
        for needle in [
            "class_s_claim_for_queue",
            "pub async fn class_s_claim(",
            "lease_pack",
            "seal_lease_pack",
            "abort_class_s_claim",
            "INSERT_CLAIM_OUTBOX",
        ] {
            assert!(
                !production_local.contains(needle),
                "SQL-first serving must not remain live ({needle})"
            );
        }

        let compose = include_str!("turso_compose.rs");
        let item_claim = compose
            .split("async fn realize_accepted_claims(")
            .nth(1)
            .and_then(|rest| rest.split("async fn dispatch_grouped_cohort_claim(").next())
            .expect("derived claim realize");
        assert!(!item_claim.contains("class_s_claim_for_queue"));
        assert!(!item_claim.contains("pending_claim_outbox"));
        assert!(!item_claim.contains("INSERT INTO fireweed_claim_outbox"));
        assert!(item_claim.contains("drive_candidate_mutation"));
        let drain = compose
            .split("    async fn drain_claim_outbox(")
            .nth(1)
            .and_then(|rest| rest.split("    async fn claimed_targets(").next())
            .expect("drain_claim_outbox");
        assert!(drain.contains("AppendAdmissionClass::RecoveryOnly"));
        assert!(drain.contains(".packed_append("));
        assert!(drain.contains("delete_claim_outbox_row"));
        assert!(!drain.contains("INSERT INTO fireweed_claim_outbox"));

        let schema = include_str!("../../fireweed-relational/src/schema.rs");
        assert!(
            schema.contains("CREATE TABLE IF NOT EXISTS fireweed_claim_outbox"),
            "legacy outbox schema stays for the migration window"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preupgrade_claim_outbox_reopens_after_log_first_cutover() {
        assert_sql_first_serving_path_removed();

        let root = std::env::temp_dir().join(format!(
            "fireweed-s8c-outbox-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        backend.create_queue(definition).await.unwrap();
        backend
            .push(
                &shard,
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new("s8c-preupgrade").unwrap()),
                    payload: Some(Bytes::from_static(b"preupgrade")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(1, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let peeked = backend.peek(&shard, 8).await.unwrap();
        assert_eq!(peeked.len(), 1);
        let item_id = peeked[0].item_id;
        assert!(
            backend
                .projection()
                .pending_claim_outbox("t", "q-s8c-outbox")
                .await
                .unwrap()
                .is_empty(),
            "log-first Push must not write a Claim outbox row"
        );

        let token = LeaseToken::new("preupgrade-lease").unwrap();
        let hash = Sha256::digest(token.as_str().as_bytes());
        let hash_hex: String = hash.iter().map(|byte| format!("{byte:02X}")).collect();
        let ids_json = format!("[\"{item_id}\"]");
        backend
            .projection()
            .execute(
                format!(
                    "INSERT INTO fireweed_claim_outbox (\
                     tenant_id, queue_id, outbox_id, item_ids, lease_token, lease_expires_at, \
                     request_id, request_fingerprint, worker_id, claim_unit, cohort_id, created_at) \
                     VALUES ('t','q-s8c-outbox','preupgrade-outbox-1','{ids_json}',\
                     'preupgrade-lease',30000000000,NULL,NULL,'w','item',NULL,2000000000)"
                ),
                vec![],
            )
            .await
            .unwrap();
        backend
            .projection()
            .execute(
                format!(
                    "UPDATE fireweed_items SET lifecycle_state='Leased', \
                     lease_token_hash=X'{hash_hex}', lease_expires_at=30000000000, \
                     worker_id='w', retry_count=retry_count+1, item_version=item_version+1, \
                     updated_at=2000000000 \
                     WHERE tenant_id='t' AND queue_id='q-s8c-outbox' AND item_id='{item_id}'"
                ),
                vec![],
            )
            .await
            .unwrap();
        backend
            .projection()
            .execute(
                format!(
                    "INSERT INTO fireweed_lease_bearers(tenant_id,queue_id,item_id,lease_token) \
                     VALUES ('t','q-s8c-outbox','{item_id}','preupgrade-lease')"
                ),
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .projection()
                .pending_claim_outbox("t", "q-s8c-outbox")
                .await
                .unwrap()
                .len(),
            1,
            "fixture must plant one pre-upgrade committed lease"
        );
        // This fixture represents a database from before resident counters
        // existed. Raw SQL above bypasses modern counter maintenance; mark the
        // counters uninitialized so reopen exercises the real migration backfill.
        backend.projection().execute(
            "UPDATE queues SET resident_pending=0,resident_leased=0,resident_complete=0,resident_failed=0,resident_counts_version=0 WHERE tenant='t' AND queue='q-s8c-outbox'",
            vec![],
        ).await.unwrap();
        drop(backend);

        let drained = open(&root).await;
        assert!(
            drained
                .projection()
                .pending_claim_outbox("t", "q-s8c-outbox")
                .await
                .unwrap()
                .is_empty(),
            "reopen must drain the pre-upgrade outbox row"
        );
        let migrated_counts = drained.metrics(&shard).await.unwrap();
        assert_eq!(
            (migrated_counts.pending, migrated_counts.leased),
            (0, 1),
            "pre-counter schema must backfill the already committed lease exactly once"
        );
        let page = AsyncLogStore::read_from(drained.log.as_ref(), shard.clone(), None, 16)
            .await
            .unwrap();
        let claims: Vec<_> = page
            .entries
            .iter()
            .filter_map(|(_, envelope)| match &envelope.command {
                QueueCommand::Claim(claim) => Some((envelope.command_id.0.as_str(), claim)),
                _ => None,
            })
            .collect();
        assert_eq!(claims.len(), 1, "drain must append the pre-upgrade lease");
        assert_eq!(claims[0].0, "preupgrade-outbox-1");
        assert_eq!(claims[0].1.item_ids, [item_id]);
        assert_eq!(claims[0].1.lease_token, token);
        assert!(
            !claims[0].1.authority_first,
            "legacy outbox drain must retain migration Claim semantics"
        );
        drop(drained);

        let recovered = open(&root).await;
        recovered
            .finalize(
                &shard,
                vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
                UtcTimestamp::new(3, 0).unwrap(),
                None,
            )
            .await
            .expect("drained pre-upgrade lease must be finalizable after reopen apply");
        recovered
            .push(
                &shard,
                vec![PushSpec {
                    client_item_key: Some(ClientItemKey::new("s8c-live").unwrap()),
                    payload: Some(Bytes::from_static(b"live")),
                    ..PushSpec::default()
                }],
                UtcTimestamp::new(4, 0).unwrap(),
                None,
            )
            .await
            .unwrap();
        let live = recovered
            .claim(ClaimRequest {
                shard: shard.clone(),
                worker_id: WorkerId::new("w2").unwrap(),
                max_items: 1,
                lease_token: LeaseToken::new("log-first-lease").unwrap(),
                lease_expires_at: UtcTimestamp::new(40, 0).unwrap(),
                now: UtcTimestamp::new(5, 0).unwrap(),
                eligibility_time: None,
                compatibility: ClaimCompatibility::default(),
                expected_epoch: None,
            })
            .await
            .unwrap();
        assert_eq!(live.items.len(), 1);
        assert_eq!(
            live.items[0].payload.as_ref().map(Bytes::as_ref),
            Some(&b"live"[..])
        );
        assert!(
            recovered
                .projection()
                .pending_claim_outbox("t", "q-s8c-outbox")
                .await
                .unwrap()
                .is_empty(),
            "log-first Claim must not write a new outbox row"
        );
        assert_eq!(recovered.metrics(&shard).await.unwrap().complete, 1);
        drop(recovered);
        let _ = std::fs::remove_dir_all(root);
    }
}
