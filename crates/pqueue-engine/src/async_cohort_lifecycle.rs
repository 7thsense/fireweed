//! Typed cohort lifecycle preparation for native-async composed backends.

use std::sync::Arc;

use pqueue_core::{CohortId, ItemId, ItemState, LeaseToken, UtcTimestamp, is_retry_exhausted};

use crate::{
    AsyncControlPlane, AsyncLogStore, AsyncProjectionStore, CohortFinalizeCommand,
    CohortLeaseTarget, CohortRenewLeaseCommand, CommandChecksum, CommandEnvelope, EngineError,
    EngineResult, FinalizeKind, IdGen, OwnedTask, QueueCommand, QueueKey, RawCommitRequest,
};

#[derive(Debug, Clone)]
pub struct AsyncCohortRenewRequest {
    pub shard: QueueKey,
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
    pub new_lease_expires_at: UtcTimestamp,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AsyncCohortFinalizeRequest {
    pub shard: QueueKey,
    pub cohort_id: CohortId,
    pub cohort_lease_token: LeaseToken,
    pub kind: FinalizeKind,
    pub not_before: Option<UtcTimestamp>,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

/// Engine-private prepared cohort mutation. The composed backend retains commit authority.
pub struct AsyncCohortLifecyclePlan {
    pub(crate) request: RawCommitRequest,
    pub(crate) item_ids: Vec<ItemId>,
    pub(crate) outcomes: Option<Vec<crate::FinalizeOutcome>>,
}

impl AsyncCohortLifecyclePlan {
    pub(crate) fn renew(request: RawCommitRequest, item_ids: Vec<ItemId>) -> Self {
        Self {
            request,
            item_ids,
            outcomes: None,
        }
    }

    pub(crate) fn finalize(
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        outcomes: Vec<crate::FinalizeOutcome>,
    ) -> Self {
        Self {
            request,
            item_ids,
            outcomes: Some(outcomes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CohortLeaseMember {
    pub item_id: ItemId,
    pub attempt_count: u32,
    pub max_attempts: u32,
}

pub trait AsyncCohortLifecyclePlanner: Send + Sync + 'static {
    fn plan_cohort_renew(
        &self,
        request: AsyncCohortRenewRequest,
    ) -> OwnedTask<EngineResult<AsyncCohortLifecyclePlan>>;

    fn plan_cohort_finalize(
        &self,
        request: AsyncCohortFinalizeRequest,
    ) -> OwnedTask<EngineResult<AsyncCohortLifecyclePlan>>;
}

/// Marker used when cohort lifecycle support was not injected.
pub struct NoAsyncCohortLifecyclePlanner;

/// Cohort lifecycle planner over the same native-async control, log, and projection axes used by
/// ordinary lifecycle planning. It can prepare commands but cannot commit them.
pub struct ProjectionCohortLifecyclePlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
}

impl<C, L, P, I> ProjectionCohortLifecyclePlanner<C, L, P, I> {
    pub fn from_shared(control: Arc<C>, log: Arc<L>, projection: Arc<P>, ids: Arc<I>) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
        }
    }
}

fn validate_renew_duration(
    now: UtcTimestamp,
    expiry: UtcTimestamp,
    max_duration_ms: u64,
) -> EngineResult<()> {
    let now_ns = i128::from(now.seconds) * 1_000_000_000 + i128::from(now.nanoseconds);
    let expiry_ns = i128::from(expiry.seconds) * 1_000_000_000 + i128::from(expiry.nanoseconds);
    let max_ns = i128::from(max_duration_ms) * 1_000_000;
    if expiry_ns <= now_ns || expiry_ns - now_ns > max_ns {
        return Err(EngineError::Invalid(
            "invalid cohort lease renewal duration",
        ));
    }
    Ok(())
}

fn validate_definition(
    request_shard: &QueueKey,
    definition: &pqueue_core::QueueDefinition,
) -> EngineResult<()> {
    if definition.tenant_id != request_shard.tenant_id
        || definition.queue_id != request_shard.queue_id
    {
        return Err(EngineError::Storage(
            "async cohort lifecycle planner returned the wrong queue definition".to_string(),
        ));
    }
    Ok(())
}

fn validate_finalize_disposition(
    kind: FinalizeKind,
    not_before: Option<UtcTimestamp>,
) -> EngineResult<()> {
    match kind {
        FinalizeKind::Rearm => Err(EngineError::Invalid("cohort rearm is invalid")),
        FinalizeKind::Retry if not_before.is_none() => {
            Err(EngineError::Invalid("retry.not_before is required"))
        }
        FinalizeKind::Complete | FinalizeKind::Fail | FinalizeKind::Release
            if not_before.is_some() =>
        {
            Err(EngineError::Invalid(
                "not_before is invalid for cohort finalize disposition",
            ))
        }
        _ => Ok(()),
    }
}

fn seal_finalize_outcomes(
    kind: FinalizeKind,
    not_before: Option<UtcTimestamp>,
    members: Vec<CohortLeaseMember>,
) -> (
    FinalizeKind,
    Option<UtcTimestamp>,
    Vec<crate::FinalizeOutcome>,
) {
    // API-001 makes a cohort one lifecycle unit. Retry exhaustion therefore cannot split a
    // cohort into Failed and Pending members: one exhausted member makes the effective durable
    // disposition Fail for the whole cohort. Otherwise every member is retried together.
    let effective_kind = if matches!(kind, FinalizeKind::Retry)
        && members
            .iter()
            .any(|member| is_retry_exhausted(member.attempt_count, member.max_attempts))
    {
        FinalizeKind::Fail
    } else {
        kind
    };
    let effective_not_before = matches!(effective_kind, FinalizeKind::Retry)
        .then_some(not_before)
        .flatten();
    let outcomes = members
        .into_iter()
        .map(|member| {
            let applied_state = match effective_kind {
                FinalizeKind::Complete => ItemState::Complete,
                FinalizeKind::Fail => ItemState::Failed,
                FinalizeKind::Retry | FinalizeKind::Release => ItemState::Pending,
                FinalizeKind::Rearm => unreachable!("rearm rejected before outcome sealing"),
            };
            crate::FinalizeOutcome {
                item_id: member.item_id,
                kind: effective_kind,
                applied_state: Some(applied_state),
                not_before: effective_not_before,
            }
        })
        .collect::<Vec<_>>();
    (effective_kind, effective_not_before, outcomes)
}

async fn validate_epoch<L: AsyncLogStore>(
    log: &L,
    shard: QueueKey,
    expected: Option<u64>,
) -> EngineResult<u64> {
    let epoch = log.current_epoch(shard).await?;
    if expected.is_some_and(|expected| expected != epoch) {
        return Err(EngineError::EpochFenced);
    }
    Ok(epoch)
}

impl<C, L, P, I> AsyncCohortLifecyclePlanner for ProjectionCohortLifecyclePlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn plan_cohort_renew(
        &self,
        request: AsyncCohortRenewRequest,
    ) -> OwnedTask<EngineResult<AsyncCohortLifecyclePlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            let definition = control.queue_definition(request.shard.clone()).await?;
            validate_definition(&request.shard, &definition)?;
            validate_renew_duration(
                request.now,
                request.new_lease_expires_at,
                definition.max_lease_duration_ms,
            )?;
            projection.admit_mutation(request.shard.clone()).await?;
            let target = CohortLeaseTarget {
                cohort_id: request.cohort_id.clone(),
                cohort_lease_token: request.cohort_lease_token.clone(),
            };
            let members = projection
                .cohort_lease_validate(request.shard.clone(), target, request.now)
                .await?;
            let item_ids = members
                .into_iter()
                .map(|member| member.item_id)
                .collect::<Vec<_>>();
            if item_ids.is_empty() {
                return Err(EngineError::Invalid("cohort lease has no members"));
            }
            let epoch =
                validate_epoch(log.as_ref(), request.shard.clone(), request.expected_epoch).await?;
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::CohortRenewLease(CohortRenewLeaseCommand {
                    cohort_id: request.cohort_id,
                    lease_expires_at: request.new_lease_expires_at,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncCohortLifecyclePlan::renew(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
            ))
        })
    }

    fn plan_cohort_finalize(
        &self,
        request: AsyncCohortFinalizeRequest,
    ) -> OwnedTask<EngineResult<AsyncCohortLifecyclePlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            let definition = control.queue_definition(request.shard.clone()).await?;
            validate_definition(&request.shard, &definition)?;
            validate_finalize_disposition(request.kind, request.not_before)?;
            projection.admit_mutation(request.shard.clone()).await?;
            let target = CohortLeaseTarget {
                cohort_id: request.cohort_id.clone(),
                cohort_lease_token: request.cohort_lease_token.clone(),
            };
            let members = projection
                .cohort_lease_validate(request.shard.clone(), target, request.now)
                .await?;
            let (effective_kind, effective_not_before, outcomes) =
                seal_finalize_outcomes(request.kind, request.not_before, members);
            let item_ids = outcomes
                .iter()
                .map(|outcome| outcome.item_id)
                .collect::<Vec<_>>();
            if item_ids.is_empty() {
                return Err(EngineError::Invalid("cohort lease has no members"));
            }
            let epoch =
                validate_epoch(log.as_ref(), request.shard.clone(), request.expected_epoch).await?;
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::CohortFinalize(CohortFinalizeCommand {
                    cohort_id: request.cohort_id,
                    kind: effective_kind,
                    not_before: effective_not_before,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncCohortLifecyclePlan::finalize(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
                outcomes,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_duration_is_strictly_future_and_capped() {
        let now = UtcTimestamp::new(10, 0).unwrap();
        assert!(validate_renew_duration(now, UtcTimestamp::new(10, 1).unwrap(), 1).is_ok());
        assert!(validate_renew_duration(now, now, 1).is_err());
        assert!(
            validate_renew_duration(now, UtcTimestamp::new(10, 1_000_001).unwrap(), 1).is_err()
        );
    }

    #[test]
    fn cohort_finalize_disposition_controls_not_before() {
        let later = Some(UtcTimestamp::new(20, 0).unwrap());
        assert!(validate_finalize_disposition(FinalizeKind::Retry, later).is_ok());
        assert!(validate_finalize_disposition(FinalizeKind::Retry, None).is_err());
        assert!(validate_finalize_disposition(FinalizeKind::Rearm, later).is_err());
        for kind in [
            FinalizeKind::Complete,
            FinalizeKind::Fail,
            FinalizeKind::Release,
        ] {
            assert!(validate_finalize_disposition(kind, None).is_ok());
            assert!(validate_finalize_disposition(kind, later).is_err());
        }
    }

    #[test]
    fn retry_exhaustion_seals_one_fail_disposition_for_the_whole_cohort() {
        let ids = [
            ItemId::from_u64(1),
            ItemId::from_u64(2),
            ItemId::from_u64(3),
        ];
        let (kind, not_before, outcomes) = seal_finalize_outcomes(
            FinalizeKind::Retry,
            Some(UtcTimestamp::new(20, 0).unwrap()),
            vec![
                CohortLeaseMember {
                    item_id: ids[0],
                    attempt_count: 1,
                    max_attempts: 2,
                },
                CohortLeaseMember {
                    item_id: ids[1],
                    attempt_count: 2,
                    max_attempts: 2,
                },
                CohortLeaseMember {
                    item_id: ids[2],
                    attempt_count: 3,
                    max_attempts: 2,
                },
            ],
        );
        assert_eq!(kind, FinalizeKind::Fail);
        assert_eq!(not_before, None);
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.item_id)
                .collect::<Vec<_>>(),
            ids
        );
        assert_eq!(outcomes[0].applied_state, Some(ItemState::Failed));
        assert_eq!(outcomes[1].applied_state, Some(ItemState::Failed));
        assert_eq!(outcomes[2].applied_state, Some(ItemState::Failed));
    }

    #[test]
    fn retry_without_exhaustion_keeps_the_whole_cohort_pending() {
        let retry_at = UtcTimestamp::new(20, 0).unwrap();
        let (kind, not_before, outcomes) = seal_finalize_outcomes(
            FinalizeKind::Retry,
            Some(retry_at),
            vec![
                CohortLeaseMember {
                    item_id: ItemId::from_u64(1),
                    attempt_count: 1,
                    max_attempts: 3,
                },
                CohortLeaseMember {
                    item_id: ItemId::from_u64(2),
                    attempt_count: 2,
                    max_attempts: 3,
                },
            ],
        );
        assert_eq!(kind, FinalizeKind::Retry);
        assert_eq!(not_before, Some(retry_at));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.applied_state == Some(ItemState::Pending))
        );
    }
}
