#![forbid(unsafe_code)]
//! # fireweed-engine
//!
//! The domain hexagon: the engine's ports (driven + driving-support) and, in later chunks, the
//! execution layer. Depends only on `fireweed-core`; no I/O. See `docs/helix/04-build/
//! hexagonal-migration-plan.md` (v4) and TD-007.

mod active_scope;
mod async_claim_planner;
mod async_cohort_lifecycle;
mod async_commit;
mod async_composed;
mod async_lifecycle_planner;
mod async_log_replay_product;
mod async_projection_spec;
mod async_push_planner;
mod async_reclaim_planner;
mod async_store;
mod auth;
mod byte_admission;
mod claim_validation;
mod command;
mod commit;
mod compose;
mod control_plane;
mod density;
mod epoch;
mod error;
mod finalize_validation;
mod idempotency;
mod maintenance;
mod object_log_segment_safety;
mod operator;
mod ownership;
mod port;
pub mod schema_validation;
pub mod sequenced_metadata;
mod types;
/// Compact Base64 wire encoding for durable envelope byte fields (fireweed-659490cc).
pub mod wire_bytes;

pub use active_scope::{
    ActiveScope, DiscoveryGranularity, project_scopes, resolve_granularity, roll_up_queue_scopes,
    validate_discovery_request,
};
pub use async_claim_planner::ProjectionClaimPlanner;
pub use async_cohort_lifecycle::{
    AsyncCohortFinalizeRequest, AsyncCohortLifecyclePlan, AsyncCohortLifecyclePlanner,
    AsyncCohortRenewRequest, CohortLeaseMember, NoAsyncCohortLifecyclePlanner,
    ProjectionCohortLifecyclePlanner,
};
pub use async_commit::{
    AsyncCommitStrategy, CommitStrategy, CommitStrategyKind, DispatchError,
    InlineOwnedTaskDispatcher, InvalidCommitStrategy, KeyedQueueGate, OwnedTask,
    OwnedTaskDispatcher, OwnedTaskFactory, PreparedAsyncCommitStrategy, QueueGateAcquire,
    QueueGateError, QueueGatePermit, SeparateReplayCommit, SeparateReplayCommitter, TaskOutcome,
    TaskOutcomeError, TaskOutcomeSender, UnifiedAtomicCommit, UnifiedAtomicCommitter,
    task_outcome_channel,
};
pub use async_composed::{
    AsyncClaimError, AsyncClaimPlan, AsyncClaimPlanner, AsyncClaimPostCommitStage,
    AsyncCommitSubmitError, AsyncComposedBackend, AsyncFinalizeRequest, AsyncLifecycleError,
    AsyncLifecyclePlan, AsyncLifecyclePlanner, AsyncLifecyclePostCommitStage, AsyncPurgeRequest,
    AsyncPushError, AsyncPushPlan, AsyncPushPlanner, AsyncPushPostCommitStage, AsyncPushRequest,
    AsyncReassignRequest, AsyncRenewRequest, FinalizeTarget, NoAsyncClaimPlanner,
    NoAsyncLifecyclePlanner, NoAsyncPushPlanner, NoAsyncReclaimPlanner, PushFingerprint,
    RenewTarget,
};
pub use async_lifecycle_planner::ProjectionLifecyclePlanner;
pub use async_log_replay_product::{
    AsyncLogReplayBackend, AtomicLogReplayCommitter, SeqIdGen, assemble_async_log_replay,
    assemble_async_log_replay_from_parts, assemble_async_log_replay_with_axis_offload,
};
pub use async_projection_spec::AsyncProjectionSpec;
pub use async_push_planner::ProjectionPushPlanner;
pub use async_reclaim_planner::{
    AsyncReclaimPlan, AsyncReclaimPlanner, AsyncReclaimRequest, ProjectionReclaimPlanner,
};
pub use async_store::{
    AsyncControlPlane, AsyncLogStore, AsyncProjectionStore, BlockingControlPlane, BlockingLogStore,
    BlockingProjectionStore, BlockingStoreOperation, BoundedBlockingExecutor,
    DEFAULT_BLOCKING_AXIS_IN_FLIGHT, FinalizeLeaseMember, InProcessLogStore,
    InProcessProjectionStore,
};
pub use auth::{AuthContext, RedactedLeaseToken, hash_lease_token};
pub use byte_admission::{
    BufferedByteBudget, BufferedByteBudgetConfig, BufferedByteBudgetStats, ByteAdmissionError,
    ByteBudgetAcquire, ByteBudgetScope, OwnedBytePermit, retained_records_plus_frame_bytes,
};
pub use claim_validation::{
    ClaimCompatibility, ClaimUnit, GroupBatching, require_item_level_claim,
    validate_claim_compatibility,
};
// Product composition is `AsyncLogReplayBackend` / `assemble_async_log_replay`
// (and async family factories). Sync dual-stack `ComposedBackend` was deleted.
pub use compose::{
    BatchUpdateSnapshotItem, BoundedMutationPlan, BoundedMutationUpdate, ComposeFaultHook,
    ComposeFaultPoint, ControlPlane, DefinitionCursor, DefinitionPage, DetachedLogMaintenance,
    DetachedRetentionOutcome, DetachedRetentionRequest, DetachedTrimWatermark, ExpiredLeaseCursor,
    ExpiredLeasePage, InProcessControlPlane, ItemMutationPlan, LogLineageIdentity, LogStore,
    PlannedBatchUpdate, ProjectionStore, RecoveryStart, RichClaimSelection, batch_update_body_hash,
    claim_by_item_ids_body_hash, claim_by_query_body_hash, commit_body_hash,
    definition_page_from_sorted_rows, definition_page_from_storage_rows, item_mutation_fingerprint,
    max_position, outcome_entry_from_recovery, outcomes_from_recovery, plan_batch_update,
    push_body_hash, push_items_fingerprint_sha256, push_specs_fingerprint_sha256,
    queue_worker_partition, recovery_from_outcome_entry, request_expires_at,
    resolve_recovery_start, validate_gate_command_definition, validate_gate_key_sets,
};
pub use control_plane::{
    AcquireOutcome, ControlPlaneConfig, InMemoryControlPlane, LeaseRenewal, LeaseRenewalOutcome,
    LeaseState, OwnerEndpointAdvertisement, OwnerResolution, QueueControlPlane, QueueLease,
    add_millis, lease_decide_acquire, lease_decide_begin_drain, lease_decide_confirm_fence,
    lease_decide_release, lease_decide_renew, lease_resolution, owner_heartbeat_live,
    resolve_target,
};
pub use density::{RenewSweep, ResidentQueues, renew_all_resident};
pub use epoch::{
    MIN_ASSIGNMENT_EPOCH, resolve_bounded_epoch, resolve_bounded_epoch_sync, resolve_epoch_fence,
};
pub use idempotency::{IdempotencyDecision, QueueIdempotencyCache};
pub use maintenance::{
    FrontierRequirement, MaintenanceAuthoritySnapshot, MaintenanceCandidate, MaintenanceDecision,
    MaintenanceDisposition, MaintenanceFilter, MaintenanceObjectClass, MaintenancePolicy,
    MaintenanceReason,
};
pub use object_log_segment_safety::{
    PRODUCTION_OBJECT_LOG_MAX_BATCHES, PRODUCTION_ONE_OBJECT_PER_COMMAND_ERROR,
    validate_production_object_log_segment_shape,
};
pub use operator::{
    OperationHandle, OperationId, OperatorAsyncAccept, OperatorAuditRecord, OperatorItemView,
    OperatorOpKind, OperatorOpPayload, OperatorOperationState, OperatorOperationStore,
    OperatorProgress, QueueAdminState, RepairAction, RetryCountMode, deterministic_operation_id,
    operator_body_fingerprint,
};
pub use ownership::{OwnedSession, OwnershipOutcome, acquire_and_fence, owner_liveness_violation};

pub use axon_esf::CompiledSchema;
pub use command::{
    AdvanceInstanceFenceCommand, ChangeRecord, ChangeRecordKind, ChangeRecordPosition,
    ChangeRecordState, ClaimCommand, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortRenewLeaseCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommitOutcomeEntry, CreateQueueCommand, FenceLeaseCommand, FinalizeCommand, FinalizeKind,
    FinalizeOutcome, LeaseExpiredCommand, MutateItemsCommand, PauseQueueCommand, PayloadUpdate,
    PurgeItemsCommand, PushCommand, PushItem, QueueCommand, QueueCounters, ReassignLeaseCommand,
    RenewLeaseCommand, ReplacePendingCommand, RequestOutcome, ResolvedItemMutation,
    ResolvedItemMutationAction, ResolvedItemValues, ScheduleUpdate, SetGatesCommand, SideRecord,
    UnfenceLeaseCommand, UpdateFieldsCommand, WriteSideRecordsCommand, build_push_items,
    command_envelope_change_records, stage_unique_push_keys, unique_index_keys_for_push_item,
    validate_gate_command, validate_gate_push, validate_request_replay_metadata,
};
pub use commit::{RawCommitFault, RawCommitOutcome, RawCommitRequest};
pub use error::{CommitRejection, DurableIntegrityStage, EngineError, EngineResult};
pub use finalize_validation::{
    FinalizeTargeting, validate_finalize_targeting, validate_purge_force, validate_purge_targeting,
    validate_rearm,
};
pub use port::{
    AddressedMutation, AsOfProjectionStore, Backend, BatchUpdateEntry, BatchUpdateItemRef,
    BatchUpdateOutcome, BatchUpdatePort, BatchUpdateRequest, BatchUpdateResponse, BatchUpdateValue,
    BoundedMutationContext, ChangeRecordSink, ClaimByItemIdsResponse, ClaimByQueryContext,
    ClaimPort, ClaimRef, ClaimRequest, Claimed, ClaimedItem, Clock, CohortFinalizePort,
    CohortLeaseTarget, CohortRenewLeasePort, CommandPage, CommitCapabilities, CommitEntryOutcome,
    CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome, DiscoveryPort, EntityEdit,
    EntityEditOperation, EntityPredicateValue, EntryRecovery, FinalizePort, GateChange,
    GateKeyDelta, HistoricalProjectionRead, HotProjectionQueryPort, IdGen, IndexHit,
    IndexQueryPort, InstanceFence, ItemMutationOperation, ItemMutationOutcome, ItemMutationPort,
    ItemMutationPrecondition, ItemMutationRequest, ItemMutationResponse, ItemMutationResult,
    ItemMutationReturning, ItemMutationSelectorAggregate, ItemMutationSnapshot,
    ItemMutationSummary, ItemPatch, ItemPredicate, ItemSelector, ItemSelectorScope, ItemView,
    LeaseGuard, LeaseView, LifecyclePatch, LiveItemView, LogRead,
    MAX_ORDERED_INDEPENDENT_PUSH_ITEMS, MaintenanceStopReason, MaintenanceSummary, PendingPage,
    PendingSummary, ProjectionRead, ProjectionSnapshot, PurgePort, PushBatchOutcome,
    PushDisposition, PushPort, PushSpec, QueueMetrics, ReassignLeasePort, ReclaimDriver,
    ReclaimPort, RecoveryReadPort, RenewLeasePort, RequestIdReplayProbe, ReschedulePort,
    SelectedMutation, SetGatesPort, SnapshotRef, SnapshotStore, TerminalEmissionMetrics,
    TickReport, TimestampComparison, UpdateFieldsPort, UpsertOutcome, UpsertPort,
    generate_query_lease_token, is_api001_reserved_write_field,
    validate_api001_reserved_write_fields, validate_distinct_commit_claims,
    validate_instance_fence,
};
pub use schema_validation::{compile_entity_schema, validate_entity};
pub use types::{CommandPosition, DurabilityClass, QueueKey};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_resp_tokens_match_td006() {
        // The wire vocabulary is pinned in TD-006 section 7 and asserted verbatim by conformance.
        assert_eq!(
            EngineError::Invalid("x").resp_token(),
            Some("-ERR fireweed invalid")
        );
        assert_eq!(
            EngineError::Terminal.resp_token(),
            Some("-ERR fireweed terminal")
        );
        assert_eq!(
            EngineError::StaleLease.resp_token(),
            Some("-ERR fireweed stale_lease")
        );
        assert_eq!(
            EngineError::Superseded.resp_token(),
            Some("-ERR fireweed superseded")
        );
        assert_eq!(
            EngineError::Unavailable.resp_token(),
            Some("-ERR fireweed unavailable")
        );
        assert_eq!(
            EngineError::Conflict.resp_token(),
            Some("-ERR fireweed conflict")
        );
        assert_eq!(
            EngineError::BatchTooLarge.resp_token(),
            Some("-ERR fireweed batch_too_large")
        );
        assert_eq!(
            EngineError::RequestIdConflict.resp_token(),
            Some("-ERR fireweed request_id_conflict")
        );
        assert_eq!(
            EngineError::RequestExpired.resp_token(),
            Some("-ERR fireweed request_expired")
        );
        assert_eq!(
            EngineError::EpochFenced.resp_token(),
            Some("-ERR fireweed epoch_stale")
        );
        // NotFound (to nil) and Forbidden (to -NOPERM) have non-`-ERR` mappings; no token here.
        assert_eq!(EngineError::NotFound.resp_token(), None);
        assert_eq!(EngineError::Forbidden("x").resp_token(), None);
    }

    #[test]
    fn durability_class_gates_upsert() {
        // Invariant 2 / TD-007 section 2.3: upsert is offered on atomic backends only.
        assert!(DurabilityClass::Atomic.supports_upsert());
        assert!(!DurabilityClass::EventualApply.supports_upsert());
    }

    #[test]
    fn gate_validation_rejects_when_capability_absent() {
        let gated = PushSpec {
            gate_keys: vec!["hold".to_string()],
            ..Default::default()
        };
        assert_eq!(
            validate_gate_push(false, &[gated]).unwrap_err(),
            EngineError::Unavailable
        );
        assert!(validate_gate_push(true, &[PushSpec::default()]).is_ok());
        assert_eq!(
            validate_gate_command(
                false,
                &QueueCommand::SetGates(SetGatesCommand {
                    gate_keys: vec!["hold".to_string()],
                    blocked: true,
                })
            )
            .unwrap_err(),
            EngineError::Unavailable
        );
    }

    #[test]
    fn pause_error_is_structured() {
        assert_eq!(
            EngineError::Paused { drain_intake: true }.resp_token(),
            Some("-ERR fireweed paused")
        );
    }

    #[test]
    fn command_position_is_shard_local_monotonic() {
        let tenant = fireweed_core::TenantId::new("t").unwrap();
        let queue = fireweed_core::QueueId::new("q").unwrap();
        let shard = QueueKey::new(tenant, queue);
        let p1 = CommandPosition::new(shard.clone(), 0, 1);
        let p2 = CommandPosition::new(shard.clone(), 0, 2);
        let p3 = CommandPosition::new(shard, 1, 0);
        assert!(p1.precedes(&p2));
        assert!(p2.precedes(&p3)); // higher epoch wins
        assert!(!p2.precedes(&p1));
    }
}
