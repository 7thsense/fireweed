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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, PriorityValue, QueryCapabilityFlags,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AppendAdmissionClass, AsyncClaimError, AsyncCommitSubmitError, AsyncComposedBackend,
    AsyncControlPlane, AsyncFinalizeRequest, AsyncLifecycleError, AsyncLogStore,
    AsyncProjectionSpec, AsyncProjectionStore, AsyncPurgeRequest, AsyncPushError, AsyncPushRequest,
    AsyncReclaimRequest, AsyncRenewRequest, Backend, BatchUpdatePort,
    CLAIM_GENERATION_MAX_REQUESTS, ClaimCommand, ClaimCompatibility, ClaimDriverReadAdmission,
    ClaimPort, ClaimQueueTurn, ClaimRequest, Claimed, CommandChecksum, CommandEnvelope,
    CommandPosition, ControlPlane, ControlPlaneStore, CoordinationError, CreateQueueOutcome,
    DEFAULT_BLOCKING_AXIS_IN_FLIGHT, DispatchError, DurabilityClass, EngineError, EngineResult,
    ExpiredLeaseCursor, ExpiredLeasePage, FinalizeKind, FinalizeOutcome, FinalizePort,
    FinalizeTarget, HistoricalProjectionRead, HotProjectionQueryPort, IdGen, IdempotencyDecision,
    InProcessControlPlane, InProcessLogStore, IndexQueryPort, InlineOwnedTaskDispatcher,
    ItemMutationPort, ItemMutationRequest, ItemMutationResponse, ItemView, LeaseView, LiveItemView,
    LogStore, MUTATION_SEQUENCER_DEFAULT_MAX_WAIT, MutationDriverSnapshot,
    MutationGenerationMemberOutcome, MutationGenerationWork, MutationIngress, MutationSequencer,
    MutationSequencerKey, OutcomeReadAdmission, OwnedTask, OwnedTaskDispatcher, OwnedTaskFactory,
    PendingPage, PendingSummary, PreparedClaim, PreparedClaimedResult, PreparedFinalize,
    PreparedMutationGeneration, PreparedPush, ProjectionClaimPlanner, ProjectionLifecyclePlanner,
    ProjectionPushPlanner, ProjectionRead, ProjectionReclaimPlanner, ProjectionSnapshot, PurgePort,
    PushCommand, PushFingerprint, PushPort, PushSpec, QueueCommand, QueueCounters, QueueGateError,
    QueueKey, QueueMetrics, RawCommitFault, RawCommitOutcome, RawCommitRequest,
    ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeasePort,
    RenewTarget, RequestOutcome, S3S_DERIVED_COVERAGE_OR_WORK_WAIT, SelectionFence,
    SelectionFenceAdmission, SelectionFenceDisposition, SeparateReplayCommit,
    SeparateReplayCommitter, SeqIdGen, SetGatesPort, SharedDriverReadAdmission, SnapshotRef,
    SnapshotStore, TaskOutcome, TaskOutcomeError, TaskOutcomeSender, TerminalEmissionMetrics,
    TickReport, UnifiedAtomicCommit, UnifiedAtomicCommitter, UpdateFieldsBatchCommand,
    UpdateFieldsPort, UpsertOutcome, UpsertPort, allocate_push_epoch_blob_and_counters,
    retain_sequencer_after_slot_release, selection_fence_disposition_for_commands,
    task_outcome_channel, validate_inert_mutation_generation_folding,
};
use fireweed_projection::InMemoryProjection;
use fireweed_turso::{TursoConfig, TursoRelational, materialize_grouped_cohort_claimed_on};

#[cfg(feature = "objectlog")]
use fireweed_objectlog::{
    AsyncProjectionApplyCoordinator, ObjectLogEngineStore, ObjectLogTaskDispatcher,
    PackedAppendError, PackedAppendOutcome,
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

fn map_coord(error: CoordinationError) -> EngineError {
    EngineError::Backpressure {
        resource: error.resource(),
    }
}

#[derive(Clone, Default)]
struct QueueFrontiers {
    last_claim: Option<CommandPosition>,
    last_candidate_mutation: Option<CommandPosition>,
}

struct GenerationJoin {
    notify: tokio::sync::Notify,
    outcome: Mutex<Option<EngineResult<Vec<fireweed_engine::MutationGenerationMember>>>>,
}

/// Post-apply send of a pre-materialized grouped/cohort envelope.
fn finish_retained_grouped_cohort_response(claimed: Claimed) -> EngineResult<Claimed> {
    Ok(claimed)
}

/// Co-seal after the shared slot/connection is released. Sequencer remains held by the caller.
fn finish_inert_mutation_generation_append(
    generation: PreparedMutationGeneration<
        QueueKey,
        fireweed_engine::MutationSequencerKey,
        fireweed_engine::MutationGenerationWork,
    >,
) -> EngineResult<Vec<RawCommitRequest>> {
    debug_assert!(generation.slot_and_connection_released);
    Ok(generation
        .members
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
            | fireweed_engine::MutationGenerationMemberOutcome::Rejected(_) => None,
        })
        .collect())
}

/// Same 20 ms linger as the object-log packer. Without it, `start_generation` /
/// `start_driver` fire on the first waiter and compatible inflight=8 work
/// becomes eight serial apply rounds.
const MICROBATCH_LINGER: Duration = Duration::from_millis(20);

fn merge_unpublished_into_snapshot(
    snapshot: &mut MutationDriverSnapshot,
    unpublished: &MutationDriverSnapshot,
) {
    snapshot
        .client_keys
        .extend(unpublished.client_keys.iter().cloned());
    for (request_id, fingerprint) in &unpublished.request_fingerprints {
        snapshot
            .request_fingerprints
            .insert(request_id.clone(), *fingerprint);
    }
    snapshot
        .unique_index_values
        .extend(unpublished.unique_index_values.iter().cloned());
    for (group, count) in &unpublished.group_counts {
        let current = snapshot.group_counts.get(group).copied().unwrap_or(0);
        snapshot
            .group_counts
            .insert(group.clone(), current.max(*count));
    }
    for item in &unpublished.batch_items {
        match snapshot
            .batch_items
            .iter_mut()
            .find(|existing| existing.item_id == item.item_id)
        {
            Some(existing) => {
                if existing.item_version < item.item_version {
                    *existing = item.clone();
                }
            }
            None => snapshot.batch_items.push(item.clone()),
        }
    }
    snapshot
        .leased_ids
        .extend(unpublished.leased_ids.iter().copied());
    snapshot
        .leased_ids
        .retain(|id| !unpublished.terminal_ids.contains(id));
    snapshot
        .terminal_ids
        .extend(unpublished.terminal_ids.iter().copied());
}

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

fn push_spec_keys(request: &AsyncPushRequest) -> Vec<String> {
    request
        .items
        .iter()
        .filter_map(|item| {
            item.client_item_key
                .as_ref()
                .map(|key| key.as_str().to_string())
        })
        .collect()
}

fn push_commit_keys(commit: &RawCommitRequest) -> Vec<String> {
    commit
        .commands()
        .iter()
        .flat_map(|envelope| match &envelope.command {
            QueueCommand::Push(command) => command
                .items
                .iter()
                .map(|item| item.client_item_key.as_str().to_string())
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

fn push_member_matches(
    request: &AsyncPushRequest,
    outcome: &MutationGenerationMemberOutcome,
) -> bool {
    match outcome {
        MutationGenerationMemberOutcome::Push(PreparedPush::Commit {
            request: commit, ..
        }) => {
            let id_match = request.request_id.is_some()
                && request.request_id
                    == commit
                        .commands()
                        .first()
                        .and_then(|envelope| envelope.request_id.clone());
            let spec_keys = push_spec_keys(request);
            let key_match = !spec_keys.is_empty() && spec_keys == push_commit_keys(commit);
            id_match || key_match
        }
        MutationGenerationMemberOutcome::Push(PreparedPush::Replay(_)) => {
            request.request_id.is_some()
        }
        _ => false,
    }
}

fn member_for_work(
    work: &MutationGenerationWork,
    members: Vec<fireweed_engine::MutationGenerationMember>,
) -> EngineResult<MutationGenerationMemberOutcome> {
    let mut rejected = None;
    for member in members {
        match (work, &member.outcome) {
            (MutationGenerationWork::Push { request, .. }, outcome)
                if push_member_matches(request, outcome) =>
            {
                return Ok(member.outcome);
            }
            (
                MutationGenerationWork::BatchUpdate { request, .. },
                MutationGenerationMemberOutcome::BatchUpdate { response, .. },
            ) if response.request_id == request.request_id => return Ok(member.outcome),
            (
                MutationGenerationWork::Claim { id, .. },
                MutationGenerationMemberOutcome::Claim { id: claimed_id, .. },
            ) if id == claimed_id => return Ok(member.outcome),
            (
                MutationGenerationWork::Finalize { command_id, .. },
                MutationGenerationMemberOutcome::Finalize { request },
            ) if request
                .commands()
                .first()
                .map(|envelope| &envelope.command_id)
                == Some(command_id) =>
            {
                return Ok(member.outcome);
            }
            (MutationGenerationWork::Singleton { .. }, _) => return Ok(member.outcome),
            (
                _,
                MutationGenerationMemberOutcome::Rejected(_)
                | MutationGenerationMemberOutcome::Push(PreparedPush::Replay(_)),
            ) => rejected = Some(member.outcome),
            _ => {}
        }
    }
    rejected.ok_or_else(|| EngineError::Storage("mutation generation lost member outcome".into()))
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

        let atomic_batch = between(
            compose,
            "async fn atomic_batch_update<",
            "macro_rules! impl_turso_product_ports",
        );
        assert!(atomic_batch.contains("AppendAdmissionClass::AtomicNative"));
        let derived_generation = between(
            compose,
            "async fn drive_started_generation(",
            "async fn dispatch_claim(",
        );
        assert!(derived_generation.contains("AppendAdmissionClass::SelectionRequired"));

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
            "ordinary item Claim must select on the serving reader, not the 4 MiB driver pool"
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
            "add must allocate durable debt in the elected driver after overlay"
        );
        assert!(
            derived_push.contains("mutation_driver_snapshot_on_serving_reader"),
            "mutation overlay snapshots must use the warm serving reader, not the 4 MiB driver pool"
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
            derived_push.contains("unpublished_mutations"),
            "next generation must overlay unpublished identities instead of waiting apply"
        );
        assert!(
            derived_push.contains("realize_accepted_claims"),
            "ordinary item Claim must realize on the same packed Update generation as field-update"
        );
        assert!(
            derived_push.contains("wait_selected_frontiers"),
            "candidate mutations keep the frontier helper; overlay replaces apply waits"
        );
        assert!(
            !derived_push.contains("wait_request_entry_coverage"),
            "candidate mutations must not serialize on projection apply"
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
            .expect("overlay");
        assert!(matches!(
            members[0].outcome,
            MutationGenerationMemberOutcome::PushAccepted
        ));
        drop(ticket);
        let prepared = retain_sequencer_after_slot_release(members, generation);
        let commits = finish_inert_mutation_generation_append(prepared).expect("co-seal carrier");
        assert!(
            commits.is_empty(),
            "unprepared add overlay must not append; the driver allocates after accept"
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
        let realized = between(
            derived_impl,
            "async fn realize_accepted_claims(",
            "async fn dispatch_claim(",
        );
        assert!(
            realized.contains("remembered_lease_ids"),
            "next Claim must exclude in-process leases instead of waiting last_claim apply"
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
    outcome_slots: OutcomeReadAdmission,
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

    #[allow(dead_code)]
    async fn catch_up_projection(&self, _shard: &QueueKey) -> EngineResult<()> {
        Ok(())
    }

    #[allow(dead_code)]
    async fn catch_up_produce(&self, _shard: &QueueKey) -> EngineResult<()> {
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
        atomic_batch_update(self, shard, request, now, expected_epoch).await
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

async fn atomic_batch_update<L>(
    backend: &AtomicTursoBackend<L>,
    shard: QueueKey,
    request: fireweed_engine::BatchUpdateRequest,
    now: UtcTimestamp,
    expected_epoch: Option<u64>,
) -> EngineResult<fireweed_engine::BatchUpdateResponse>
where
    L: AsyncLogStore + 'static,
{
    use fireweed_engine::{
        AsyncCommitStrategy, BatchUpdateItemRef, batch_update_body_hash, plan_batch_update,
        plan_batch_update_pipelined,
    };

    if request.updates.is_empty() {
        return Err(EngineError::Invalid("empty batch update"));
    }
    if request.updates.len() > 1_000 {
        return Err(EngineError::BatchTooLarge);
    }

    let definition =
        AsyncControlPlane::queue_definition(backend.control.as_ref(), shard.clone()).await?;
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
    let snapshot = if backend.pipeline_unresolved_updates() && !needs_version_peek {
        Vec::new()
    } else {
        backend.planner_update_snapshot(&shard, &keys, &ids).await?
    };

    let plan = if backend.pipeline_unresolved_updates() {
        plan_batch_update_pipelined(&definition, true, request.updates, snapshot)
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
        let planned = backend.reserve_planned_updates(&shard, &updates)?;
        let item_ids: Vec<_> = updates
            .iter()
            .map(|u| u.item_id)
            .filter(|id| id.as_u64() != 0)
            .collect();
        let envelope = CommandEnvelope {
            command_id: backend.ids.next_command_id(),
            request_id: Some(request_id),
            request_fingerprint: Some(fingerprint.0),
            request_outcome: None,
            item_ids,
            command: QueueCommand::UpdateFieldsBatch(UpdateFieldsBatchCommand { updates }),
            checksum: CommandChecksum(0),
            created_at: now,
        };
        let epoch = match expected_epoch {
            Some(e) => e,
            None => AsyncLogStore::current_epoch(backend.log.as_ref(), shard.clone()).await?,
        };
        let committed = backend
            .engine
            .commit_strategy()
            .commit(
                RawCommitRequest::new(shard, vec![envelope], epoch)
                    .with_append_admission(AppendAdmissionClass::AtomicNative),
            )
            .await;
        backend.finish_planned(planned, committed.is_ok());
        committed?;
    }
    Ok(response)
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
            let outcome = match log
                .packed_append_owned(
                    shard.clone(),
                    commands.clone(),
                    expected_epoch,
                    reservation.as_ref().map(|reserved| reserved.id()),
                    false,
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
        if coordinator.is_none() {
            if let Some(apply_turn) = apply_turn {
                wait_turso_apply_turn(projection, shard, &positions, apply_turn).await?;
            }
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
    node_id: u8,
    async_apply: Option<AsyncProjectionApplyCoordinator<TursoRelational>>,
    last_produce: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
    produce_caught_up: Arc<tokio::sync::Mutex<HashMap<QueueKey, CommandPosition>>>,
    frontiers: Arc<tokio::sync::Mutex<HashMap<QueueKey, QueueFrontiers>>>,
    unpublished_mutations: Arc<tokio::sync::Mutex<HashMap<QueueKey, MutationDriverSnapshot>>>,
    sequencer: MutationSequencer<QueueKey, MutationSequencerKey, MutationGenerationWork>,
    claim_turns: ClaimQueueTurn<QueueKey>,
    claim_slots: ClaimDriverReadAdmission,
    shared_slots: SharedDriverReadAdmission,
    outcome_slots: OutcomeReadAdmission,
    selection_fence: SelectionFence<QueueKey>,
    fence_admission: SelectionFenceAdmission,
    generation_joins: Arc<Mutex<HashMap<(QueueKey, u64), Arc<GenerationJoin>>>>,
    generation_outcomes: Arc<
        Mutex<
            HashMap<(QueueKey, u64), EngineResult<Vec<fireweed_engine::MutationGenerationMember>>>,
        >,
    >,
    claim_work_ids: AtomicU64,
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
        let unpublished_mutations = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
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
            frontiers,
            unpublished_mutations,
            sequencer: MutationSequencer::new(),
            claim_turns: ClaimQueueTurn::default(),
            claim_slots: ClaimDriverReadAdmission::default(),
            shared_slots: SharedDriverReadAdmission::default(),
            outcome_slots: OutcomeReadAdmission::default(),
            selection_fence,
            fence_admission,
            generation_joins: Arc::new(Mutex::new(HashMap::new())),
            generation_outcomes: Arc::new(Mutex::new(HashMap::new())),
            claim_work_ids: AtomicU64::new(1),
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

    #[allow(dead_code)]
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
                coordinator
                    .wait_until_covers(shard, &target, S3S_DERIVED_COVERAGE_OR_WORK_WAIT)
                    .await?;
            } else {
                return Ok(views);
            }
        }
    }

    #[allow(dead_code)]
    async fn planner_update_snapshot(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
        _ids: &[ItemId],
    ) -> EngineResult<Vec<fireweed_engine::BatchUpdateSnapshotItem>> {
        self.projection.server_update_snapshot(shard, keys).await
    }

    #[allow(dead_code)]
    fn pipeline_unresolved_updates(&self) -> bool {
        self.async_apply.is_some()
    }

    #[allow(dead_code)]
    fn reserve_planned_updates(
        &self,
        _shard: &QueueKey,
        _updates: &[fireweed_engine::UpdateFieldsCommand],
    ) -> EngineResult<Option<PlannedReservation>> {
        Ok(None)
    }

    #[allow(dead_code)]
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
                | AppendAdmissionClass::Bypass
                | AppendAdmissionClass::AtomicNative
                | AppendAdmissionClass::ClaimCoordinatorLive => {}
            }
            debug_assert_eq!(fault, RawCommitFault::None);
            self.log
                .packed_append(append_shard, commands, append_epoch)
                .await
                .map_err(PackedAppendError::into_engine)?;
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

    fn generation_outcome(
        &self,
        queue: &QueueKey,
        generation_id: u64,
    ) -> Option<EngineResult<Vec<fireweed_engine::MutationGenerationMember>>> {
        self.generation_outcomes
            .lock()
            .expect("generation outcome map")
            .get(&(queue.clone(), generation_id))
            .cloned()
    }

    fn ensure_generation_join(&self, queue: &QueueKey, generation_id: u64) -> Arc<GenerationJoin> {
        self.generation_joins
            .lock()
            .expect("generation join map")
            .entry((queue.clone(), generation_id))
            .or_insert_with(|| {
                Arc::new(GenerationJoin {
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
        *join.outcome.lock().expect("generation outcome") = Some(driven.clone());
        let stale = {
            let mut outcomes = self
                .generation_outcomes
                .lock()
                .expect("generation outcome map");
            outcomes.insert((queue.clone(), generation_id), driven);
            if outcomes.len() > 64 {
                let stale: Vec<_> = outcomes
                    .keys()
                    .filter(|(key, id)| key != queue || *id != generation_id)
                    .take(outcomes.len().saturating_sub(32))
                    .cloned()
                    .collect();
                for key in &stale {
                    outcomes.remove(key);
                }
                stale
            } else {
                Vec::new()
            }
        };
        join.notify.notify_waiters();
        if !stale.is_empty() {
            let mut joins = self.generation_joins.lock().expect("generation join map");
            for key in stale {
                joins.remove(&key);
            }
        }
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
        let _permit = self.acquire_outcome_read(shard).await?;
        self.projection.server_metrics_committed(shard).await
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
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            MutationGenerationMemberOutcome::PushAccepted
            | MutationGenerationMemberOutcome::BatchUpdate { .. }
            | MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | MutationGenerationMemberOutcome::Claim { .. }
            | MutationGenerationMemberOutcome::Finalize { .. }
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
        if request.updates.is_empty() {
            return Err(EngineError::Invalid("empty batch update"));
        }
        if request.updates.len() > 1_000 {
            return Err(EngineError::BatchTooLarge);
        }
        let fingerprint = fireweed_engine::batch_update_body_hash(&request)?;
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
            MutationGenerationMemberOutcome::Rejected(error) => Err(error),
            MutationGenerationMemberOutcome::Push(_)
            | MutationGenerationMemberOutcome::PushAccepted
            | MutationGenerationMemberOutcome::ClaimAccepted { .. }
            | MutationGenerationMemberOutcome::Claim { .. }
            | MutationGenerationMemberOutcome::Finalize { .. }
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
        let key = work.sequencer_key();
        let work = Arc::new(work);
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
        let generation_id = ticket.generation_id();
        let started = Instant::now();
        loop {
            if let Some(outcome) = self.generation_outcome(&queue, generation_id) {
                drop(ticket);
                return member_for_work(&work, outcome?);
            }
            if let Some(generation) = self
                .sequencer
                .start_generation_after(&queue, MICROBATCH_LINGER)
            {
                let driven_id = generation.generation_id();
                let join = self.ensure_generation_join(&queue, driven_id);
                let driven = self.drive_started_generation(generation).await;
                self.publish_generation_outcome(&queue, driven_id, &join, driven.clone());
                if driven_id != generation_id {
                    continue;
                }
                drop(ticket);
                return member_for_work(&work, driven?);
            }
            let join = self
                .generation_joins
                .lock()
                .expect("generation join map")
                .get(&(queue.clone(), generation_id))
                .cloned();
            if let Some(join) = join {
                let notified = join.notify.notified();
                tokio::pin!(notified);
                if let Some(outcome) = join.outcome.lock().expect("generation outcome").clone() {
                    drop(ticket);
                    return member_for_work(&work, outcome?);
                }
                if let Some(outcome) = self.generation_outcome(&queue, generation_id) {
                    drop(ticket);
                    return member_for_work(&work, outcome?);
                }
                notified.await;
                continue;
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
            tokio::time::sleep(Duration::from_millis(1)).await;
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
        let queue = generation.requests()[0].queue();
        debug_assert!(generation.requests().len() <= CLAIM_GENERATION_MAX_REQUESTS);
        let works: Vec<MutationGenerationWork> = generation
            .requests()
            .iter()
            .map(|work| work.as_ref().clone())
            .collect();
        let wait_push_apply = works
            .iter()
            .any(|work| matches!(work, MutationGenerationWork::Claim { .. }));
        self.wait_selected_frontiers(&queue, false, wait_push_apply)
            .await?;
        let _slot = self.shared_slots.acquire().await.map_err(map_coord)?;
        let mut keys = Vec::new();
        let mut batch_keys = Vec::new();
        let mut claimed = false;
        let mut records_mutation = false;
        for work in &works {
            match work {
                MutationGenerationWork::Push { request, .. } => {
                    records_mutation = true;
                    keys.extend(
                        request
                            .items
                            .iter()
                            .filter_map(|item| item.client_item_key.clone()),
                    );
                }
                MutationGenerationWork::BatchUpdate { request, .. } => {
                    records_mutation = true;
                    for update in &request.updates {
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
                MutationGenerationWork::Finalize { .. }
                | MutationGenerationWork::Singleton { .. } => {}
            }
        }
        let definition =
            AsyncControlPlane::queue_definition(self.control.as_ref(), queue.clone()).await?;
        let _waiter = self.fence_admission.admit_waiter().map_err(map_coord)?;
        let _fence = self
            .selection_fence
            .acquire_shared(queue.clone())
            .await
            .map_err(map_coord)?;
        self.wait_selected_frontiers(&queue, false, wait_push_apply)
            .await?;
        let now = match &works[0] {
            MutationGenerationWork::Push { request, .. } => request.now,
            MutationGenerationWork::BatchUpdate { now, .. }
            | MutationGenerationWork::Finalize { now, .. } => *now,
            MutationGenerationWork::Claim { request, .. } => request.now,
            MutationGenerationWork::Singleton { commit, .. } => commit
                .commands()
                .first()
                .map(|envelope| envelope.created_at)
                .unwrap_or_else(|| UtcTimestamp::new(1, 0).expect("epoch")),
        };
        let mut snapshot = self
            .projection
            .mutation_driver_snapshot_on_serving_reader(
                &queue,
                definition.clone(),
                &keys,
                &batch_keys,
                now,
            )
            .await?;
        if let Some(unpublished) = self.unpublished_mutations.lock().await.get(&queue) {
            merge_unpublished_into_snapshot(&mut snapshot, unpublished);
        }
        let (mut members, mut folded) =
            validate_inert_mutation_generation_folding(&snapshot, &works)?;
        self.realize_accepted_claims(&queue, &works, &mut members, &mut folded)
            .await?;
        self.allocate_accepted_pushes(&definition, &works, &mut members, &mut folded)
            .await?;
        drop(_slot);
        let prepared = retain_sequencer_after_slot_release(members.clone(), generation);
        let commits =
            coalesce_generation_commits(finish_inert_mutation_generation_append(prepared)?)?;
        if !commits.is_empty() {
            for commit in commits {
                if let Err(error) = self
                    .commit_prepared(commit, AppendAdmissionClass::SelectionRequired)
                    .await
                {
                    return Err(error);
                }
            }
            for (work, member) in works.iter().zip(&members) {
                match (work, &member.outcome) {
                    (
                        MutationGenerationWork::Claim { request, .. },
                        MutationGenerationMemberOutcome::Claim {
                            claimed: claimed_items,
                            ..
                        },
                    ) => {
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
                    _ => {}
                }
            }
            let mut unpublished = self.unpublished_mutations.lock().await;
            match unpublished.get_mut(&queue) {
                Some(existing) => merge_unpublished_into_snapshot(existing, &folded),
                None => {
                    unpublished.insert(queue.clone(), folded);
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
        let mut exclude: Vec<ItemId> = folded.leased_ids.iter().copied().collect();
        exclude.extend(folded.terminal_ids.iter().copied());
        exclude.extend(self.projection.remembered_lease_ids(queue).await);
        exclude.sort_unstable();
        exclude.dedup();
        let selected = self
            .projection
            .item_claim_microbatch_on_serving_reader(queue, &claim_members, &exclude)
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
            folded.leased_ids.extend(ids.iter().copied());
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
            let envelope = CommandEnvelope {
                command_id: self.ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: ids.clone(),
                command: QueueCommand::Claim(ClaimCommand {
                    item_ids: ids,
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
                self.wait_request_entry_coverage(&request.shard).await?;
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
                | MutationGenerationMemberOutcome::Singleton { .. } => Err(EngineError::Storage(
                    "finalize generation produced a non-finalize outcome".into(),
                )),
            };
        }
        let PreparedFinalize { request, .. } = self
            .engine
            .prepare_finalize(shard.clone(), outcomes, now, expected_epoch)
            .await
            .map_err(map_lifecycle)?;
        self.commit_prepared(request, AppendAdmissionClass::SelectionRequired)
            .await
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
    async fn duplicate_client_key_conflicts_before_apply_covers() {
        let root = std::env::temp_dir().join(format!(
            "fireweed-s3c-dup-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        let backend = open(&root).await;
        let definition = qdef("q-s3c-dup");
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
        let duplicate = backend.push(&shard, vec![spec], now, None).await;
        assert!(
            matches!(duplicate, Err(EngineError::Conflict)),
            "unpublished overlay must reject the duplicate key, got {duplicate:?}"
        );
        assert_eq!(backend.metrics(&shard).await.unwrap().pending, 1);
        drop(backend);
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
