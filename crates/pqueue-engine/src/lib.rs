#![forbid(unsafe_code)]
//! # pqueue-engine
//!
//! The domain hexagon: the engine's ports (driven + driving-support) and, in later chunks, the
//! execution layer. Depends only on `pqueue-core`; no I/O. See `docs/helix/04-build/
//! hexagonal-migration-plan.md` (v4) and TD-007.

mod active_scope;
mod auth;
mod claim_validation;
mod command;
mod compose;
mod control_plane;
mod density;
mod error;
mod finalize_validation;
mod idempotency;
mod operator;
mod ownership;
mod port;
pub mod schema_validation;
mod types;

pub use active_scope::{
    ActiveScope, DiscoveryGranularity, project_scopes, resolve_granularity, roll_up_queue_scopes,
    validate_discovery_request,
};
pub use auth::{AuthContext, RedactedLeaseToken, hash_lease_token};
pub use claim_validation::{
    ClaimCompatibility, ClaimUnit, GroupBatching, require_item_level_claim,
    validate_claim_compatibility,
};
pub use compose::{
    ComposedBackend, ControlPlane, InProcessControlPlane, LogLineageIdentity, LogStore,
    ProjectionStore, RecoveryStart, resolve_recovery_start,
};
pub use control_plane::{
    AcquireOutcome, ControlPlaneConfig, InMemoryControlPlane, LeaseState, OwnerResolution,
    QueueControlPlane, QueueLease, lease_decide_acquire, lease_decide_begin_drain,
    lease_decide_release, lease_decide_renew, lease_resolution, owner_heartbeat_live,
    resolve_target,
};
pub use density::{RenewSweep, ResidentQueues, renew_all_resident};
pub use idempotency::{IdempotencyDecision, QueueIdempotencyCache};
pub use operator::{OperationHandle, OperationId, OperatorOperationState, OperatorOperationStore};
pub use ownership::{OwnedSession, OwnershipOutcome, acquire_and_fence, owner_liveness_violation};

pub use axon_esf::CompiledSchema;
pub use command::{
    AdvanceInstanceFenceCommand, ChangeRecord, ChangeRecordKind, ChangeRecordPosition,
    ChangeRecordState, ClaimCommand, CohortClaimCommand, CohortExpiredCommand,
    CohortFinalizeCommand, CohortRenewLeaseCommand, CommandChecksum, CommandEnvelope, CommandId,
    CreateQueueCommand, FenceLeaseCommand, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, PayloadUpdate, PurgeItemsCommand, PushCommand, PushItem, QueueCommand,
    QueueCounters, ReassignLeaseCommand, RenewLeaseCommand, ReplacePendingCommand, RequestOutcome,
    ScheduleUpdate, SetGatesCommand, SideRecord, UnfenceLeaseCommand, UpdateFieldsCommand,
    WriteSideRecordsCommand, build_push_items, command_envelope_change_records,
    validate_gate_command, validate_gate_push, validate_request_replay_metadata,
};
pub use error::{EngineError, EngineResult};
pub use finalize_validation::{
    FinalizeTargeting, validate_finalize_targeting, validate_purge_force, validate_purge_targeting,
    validate_rearm,
};
pub use port::{
    Backend, ChangeRecordSink, ClaimPort, ClaimRef, ClaimRequest, Claimed, ClaimedItem, Clock,
    CohortFinalizePort, CohortLeaseTarget, CohortRenewLeasePort, CommandPage, CommitCapabilities,
    CommitEntryOutcome, CommitEntryStatus, CommitRecovery, CommitTransition, CommitTransitionEntry,
    CommitTransitionPort, ControlPlaneStore, CreateQueueOutcome, DiscoveryPort, EntryRecovery,
    FinalizePort, HotProjectionQueryPort, IdGen, IndexHit, IndexQueryPort, InstanceFence, ItemView,
    LeaseView, LiveItemView, LogRead, LogWriter, ProjectionRead, ProjectionSnapshot,
    ProjectionWriter, PurgePort, PushPort, PushSpec, QueueMetrics, ReassignLeasePort,
    ReclaimDriver, ReclaimPort, RecoveryReadPort, RenewLeasePort, ReschedulePort, SetGatesPort,
    SnapshotRef, SnapshotStore, TickReport, UpdateFieldsPort, UpsertOutcome, UpsertPort,
    AsOfProjectionStore, HistoricalProjectionRead, validate_instance_fence,
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
            Some("-ERR pqueue invalid")
        );
        assert_eq!(
            EngineError::Terminal.resp_token(),
            Some("-ERR pqueue terminal")
        );
        assert_eq!(
            EngineError::StaleLease.resp_token(),
            Some("-ERR pqueue stale_lease")
        );
        assert_eq!(
            EngineError::Superseded.resp_token(),
            Some("-ERR pqueue superseded")
        );
        assert_eq!(
            EngineError::Unavailable.resp_token(),
            Some("-ERR pqueue unavailable")
        );
        assert_eq!(
            EngineError::Conflict.resp_token(),
            Some("-ERR pqueue conflict")
        );
        assert_eq!(
            EngineError::BatchTooLarge.resp_token(),
            Some("-ERR pqueue batch_too_large")
        );
        assert_eq!(
            EngineError::RequestIdConflict.resp_token(),
            Some("-ERR pqueue request_id_conflict")
        );
        assert_eq!(
            EngineError::RequestExpired.resp_token(),
            Some("-ERR pqueue request_expired")
        );
        assert_eq!(
            EngineError::EpochFenced.resp_token(),
            Some("-ERR pqueue epoch_stale")
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
    fn command_position_is_shard_local_monotonic() {
        let tenant = pqueue_core::TenantId::new("t").unwrap();
        let queue = pqueue_core::QueueId::new("q").unwrap();
        let shard = QueueKey::new(tenant, queue);
        let p1 = CommandPosition::new(shard.clone(), 0, 1);
        let p2 = CommandPosition::new(shard.clone(), 0, 2);
        let p3 = CommandPosition::new(shard, 1, 0);
        assert!(p1.precedes(&p2));
        assert!(p2.precedes(&p3)); // higher epoch wins
        assert!(!p2.precedes(&p1));
    }
}
