//! Reusable typed lifecycle preparation over native-async storage axes.

use std::sync::Arc;

use crate::{
    AsyncControlPlane, AsyncFinalizeRequest, AsyncLifecyclePlan, AsyncLifecyclePlanner,
    AsyncLogStore, AsyncProjectionStore, AsyncPurgeRequest, AsyncRenewRequest, CommandChecksum,
    CommandEnvelope, EngineError, EngineResult, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    IdGen, OwnedTask, PurgeItemsCommand, QueueCommand, RawCommitRequest, RenewLeaseCommand,
    validate_rearm,
};

fn validate_renew_duration(
    now: fireweed_core::UtcTimestamp,
    expiry: fireweed_core::UtcTimestamp,
    max_duration_ms: u64,
) -> EngineResult<()> {
    let now_ns = i128::from(now.seconds) * 1_000_000_000 + i128::from(now.nanoseconds);
    let expiry_ns = i128::from(expiry.seconds) * 1_000_000_000 + i128::from(expiry.nanoseconds);
    let max_ns = i128::from(max_duration_ms) * 1_000_000;
    if expiry_ns <= now_ns || expiry_ns - now_ns > max_ns {
        return Err(EngineError::Invalid("invalid lease renewal duration"));
    }
    Ok(())
}

/// Ordinary-item lifecycle planner. It validates and constructs typed commands but owns no commit
/// capability; the composed backend retains the only durable mutation authority.
pub struct ProjectionLifecyclePlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
}

impl<C, L, P, I> ProjectionLifecyclePlanner<C, L, P, I> {
    pub fn from_shared(control: Arc<C>, log: Arc<L>, projection: Arc<P>, ids: Arc<I>) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
        }
    }
}

impl<C, L, P, I> AsyncLifecyclePlanner for ProjectionLifecyclePlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn plan_renew(
        &self,
        request: AsyncRenewRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            if request.targets.is_empty() {
                return Err(EngineError::Invalid("renew item batch must not be empty"));
            }
            let definition = control.queue_definition(request.shard.clone()).await?;
            if definition.tenant_id != request.shard.tenant_id
                || definition.queue_id != request.shard.queue_id
            {
                return Err(EngineError::Storage(
                    "async lifecycle planner returned the wrong queue definition".to_string(),
                ));
            }
            validate_renew_duration(
                request.now,
                request.new_lease_expires_at,
                definition.max_lease_duration_ms,
            )?;
            projection.admit_mutation(request.shard.clone()).await?;
            projection
                .renew_validate(request.shard.clone(), request.targets.clone(), request.now)
                .await?;
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request
                .expected_epoch
                .is_some_and(|expected| expected != epoch)
            {
                return Err(EngineError::EpochFenced);
            }
            let item_ids = request
                .targets
                .iter()
                .map(|target| target.item_id)
                .collect::<Vec<_>>();
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids,
                    lease_expires_at: request.new_lease_expires_at,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncLifecyclePlan::renew(RawCommitRequest::new(
                request.shard,
                vec![envelope],
                epoch,
            )))
        })
    }

    fn plan_finalize(
        &self,
        request: AsyncFinalizeRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            if request.targets.is_empty() {
                return Err(EngineError::Invalid(
                    "finalize item batch must not be empty",
                ));
            }
            let definition = control.queue_definition(request.shard.clone()).await?;
            for target in &request.targets {
                match target.kind {
                    FinalizeKind::Rearm => validate_rearm(target.not_before, &definition)?,
                    FinalizeKind::Complete | FinalizeKind::Fail | FinalizeKind::Release
                        if target.not_before.is_some() =>
                    {
                        return Err(EngineError::Invalid(
                            "not_before is invalid for finalize disposition",
                        ));
                    }
                    _ => {}
                }
            }
            projection.admit_mutation(request.shard.clone()).await?;
            let attempts = projection
                .finalize_validate(
                    request.shard.clone(),
                    request.targets.clone(),
                    request.now,
                    definition.retry_policy.max_attempts,
                )
                .await?;
            if attempts.len() != request.targets.len()
                || attempts
                    .iter()
                    .zip(&request.targets)
                    .any(|(attempt, target)| attempt.item_id != target.item_id)
            {
                return Err(EngineError::Storage(
                    "async finalize validation returned the wrong item footprint".into(),
                ));
            }
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request.expected_epoch.is_some_and(|e| e != epoch) {
                return Err(EngineError::EpochFenced);
            }
            let mut outcomes = Vec::with_capacity(request.targets.len());
            for (target, attempt) in request.targets.into_iter().zip(attempts) {
                let applied_state = match target.kind {
                    FinalizeKind::Complete => fireweed_core::ItemState::Complete,
                    FinalizeKind::Fail => fireweed_core::ItemState::Failed,
                    FinalizeKind::Retry
                        if fireweed_core::is_retry_exhausted(
                            attempt.attempt_count,
                            attempt.max_attempts,
                        ) =>
                    {
                        fireweed_core::ItemState::Failed
                    }
                    FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm => {
                        fireweed_core::ItemState::Pending
                    }
                };
                outcomes.push(FinalizeOutcome {
                    item_id: target.item_id,
                    kind: target.kind,
                    applied_state: Some(applied_state),
                    not_before: target.not_before,
                });
            }
            let item_ids = outcomes.iter().map(|o| o.item_id).collect::<Vec<_>>();
            let env = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::Finalize(FinalizeCommand {
                    outcomes: outcomes.clone(),
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncLifecyclePlan::finalize(
                RawCommitRequest::new(request.shard, vec![env], epoch),
                outcomes,
            ))
        })
    }

    fn plan_purge(
        &self,
        request: AsyncPurgeRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            projection.admit_mutation(request.shard.clone()).await?;
            let present = projection
                .purge_validate(request.shard.clone(), request.item_ids, request.force)
                .await?;
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request.expected_epoch.is_some_and(|e| e != epoch) {
                return Err(EngineError::EpochFenced);
            }
            let env = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: present.clone(),
                command: QueueCommand::PurgeItems(PurgeItemsCommand {
                    item_ids: present,
                    force: request.force,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncLifecyclePlan::purge(RawCommitRequest::new(
                request.shard,
                vec![env],
                epoch,
            )))
        })
    }
}

#[cfg(test)]
#[allow(refining_impl_trait)]
mod tests {
    use std::future::{Ready, ready};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use fireweed_core::{
        EligibilityPolicy, ItemId, ItemState, LeaseToken, OrderingMode, PriorityDirection,
        PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
        RecurrenceMode, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
    };

    use super::*;
    use crate::{
        ClaimedItem, CommandId, CommandPage, CommandPosition, CreateQueueOutcome, DurabilityClass,
        QueueKey,
    };

    #[test]
    fn renewal_duration_is_strictly_future_and_capped() {
        let now = UtcTimestamp::new(10, 0).unwrap();
        assert!(validate_renew_duration(now, UtcTimestamp::new(10, 1).unwrap(), 1).is_ok());
        assert!(validate_renew_duration(now, now, 1).is_err());
        assert!(
            validate_renew_duration(now, UtcTimestamp::new(10, 1_000_001).unwrap(), 1).is_err()
        );
    }

    struct TestAxes {
        definition: QueueDefinition,
        epoch: u64,
        attempts: Mutex<Vec<u32>>,
        attempt_bounds: Mutex<Option<Vec<u32>>>,
        validation_ids: Mutex<Option<Vec<ItemId>>>,
        ids: AtomicU64,
    }

    impl IdGen for TestAxes {
        fn next_item_id(&self) -> ItemId {
            ItemId::from_u64(999)
        }
        fn next_command_id(&self) -> CommandId {
            CommandId::new(format!(
                "finalize-{}",
                self.ids.fetch_add(1, Ordering::SeqCst)
            ))
        }
    }

    impl AsyncControlPlane for TestAxes {
        fn create_queue(
            &self,
            _definition: QueueDefinition,
        ) -> Ready<EngineResult<CreateQueueOutcome>> {
            ready(Err(EngineError::Unavailable))
        }
        fn queue_definition(&self, _key: QueueKey) -> Ready<EngineResult<QueueDefinition>> {
            ready(Ok(self.definition.clone()))
        }
        fn list_queues(&self, _tenant: TenantId) -> Ready<EngineResult<Vec<QueueId>>> {
            ready(Err(EngineError::Unavailable))
        }
    }

    impl AsyncLogStore for TestAxes {
        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::Atomic
        }
        fn ensure_shard(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn current_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(self.epoch))
        }
        fn acquire_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            ready(Err(EngineError::Unavailable))
        }
        fn append(
            &self,
            _shard: QueueKey,
            _commands: Vec<CommandEnvelope>,
            _expected_epoch: u64,
        ) -> Ready<EngineResult<Vec<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn read_from(
            &self,
            _shard: QueueKey,
            _from: Option<CommandPosition>,
            _limit: usize,
        ) -> Ready<EngineResult<CommandPage>> {
            ready(Err(EngineError::Unavailable))
        }
        fn high_water(&self, _shard: QueueKey) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn set_high_water(
            &self,
            _shard: QueueKey,
            _position: CommandPosition,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
    }

    impl AsyncProjectionStore for TestAxes {
        fn ensure_shard(&self, _definition: QueueDefinition) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn admit_mutation(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }
        fn finalize_validate(
            &self,
            _shard: QueueKey,
            targets: Vec<crate::FinalizeTarget>,
            _now: UtcTimestamp,
            default_max_attempts: u32,
        ) -> Ready<EngineResult<Vec<crate::FinalizeLeaseMember>>> {
            let attempts = self.attempts.lock().unwrap().clone();
            let bounds = self
                .attempt_bounds
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| vec![default_max_attempts; attempts.len()]);
            let validation_ids = self.validation_ids.lock().unwrap().clone();
            assert_eq!(attempts.len(), targets.len());
            assert_eq!(bounds.len(), targets.len());
            ready(Ok(attempts
                .into_iter()
                .zip(bounds)
                .zip(targets.into_iter().enumerate())
                .map(|((attempt_count, max_attempts), (index, target))| {
                    crate::FinalizeLeaseMember {
                        item_id: validation_ids
                            .as_ref()
                            .map_or(target.item_id, |ids| ids[index]),
                        attempt_count,
                        max_attempts,
                    }
                })
                .collect()))
        }
        fn purge_validate(
            &self,
            _shard: QueueKey,
            ids: Vec<ItemId>,
            force: bool,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            assert!(force);
            let mut present = Vec::new();
            for id in ids {
                if id != ItemId::from_u64(2) && !present.contains(&id) {
                    present.push(id);
                }
            }
            ready(Ok(present))
        }
        fn apply_live(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn apply_recovery(
            &self,
            _positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn eligible_candidates(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn render_claimed(
            &self,
            _shard: QueueKey,
            _ids: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<ClaimedItem>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<ItemState>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_version(&self, _shard: QueueKey, _id: ItemId) -> Ready<EngineResult<Option<u64>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recovery_high_water(
            &self,
            _shard: QueueKey,
        ) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recover_definitions(&self) -> Ready<EngineResult<Vec<QueueDefinition>>> {
            ready(Err(EngineError::Unavailable))
        }
    }

    fn definition() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("finalize").unwrap(),
            priority_model: PriorityModel {
                kind: PriorityModelKind::Int64,
                direction: PriorityDirection::Ascending,
                tie_breaker: PriorityTieBreaker::CreatedSequence,
            },
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 1,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: None,
            recurrence: RecurrencePolicy {
                mode: RecurrenceMode::Recurring,
                until: Some(UtcTimestamp::new(100, 0).unwrap()),
            },
            request_id_retention_ms: 1,
            client_item_key_retention_ms: 1,
            terminal_retention_ms: 1,
            max_lease_duration_ms: 1,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 10,
            max_claim_batch_size: 10,
            max_eligible_group_size: None,
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn target(
        counter: u64,
        kind: FinalizeKind,
        not_before: Option<UtcTimestamp>,
    ) -> crate::FinalizeTarget {
        crate::FinalizeTarget {
            item_id: ItemId::from_u64(counter),
            lease_token: LeaseToken::new(format!("token-{counter}")).unwrap(),
            item_version: 2,
            kind,
            not_before,
        }
    }

    fn request(
        definition: &QueueDefinition,
        targets: Vec<crate::FinalizeTarget>,
        epoch: u64,
    ) -> AsyncFinalizeRequest {
        AsyncFinalizeRequest {
            shard: QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
            targets,
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(epoch),
        }
    }

    #[test]
    fn finalize_planner_derives_retry_state_and_enforces_retry_rearm_and_epoch_rules() {
        let definition = definition();
        let axes = Arc::new(TestAxes {
            definition: definition.clone(),
            epoch: 7,
            attempts: Mutex::new(vec![2, 3]),
            attempt_bounds: Mutex::new(Some(vec![2, 4])),
            validation_ids: Mutex::new(None),
            ids: AtomicU64::new(0),
        });
        let planner = ProjectionLifecyclePlanner::from_shared(
            axes.clone(),
            axes.clone(),
            axes.clone(),
            axes.clone(),
        );
        let plan = futures::executor::block_on(planner.plan_finalize(request(
            &definition,
            vec![
                target(
                    1,
                    FinalizeKind::Retry,
                    Some(UtcTimestamp::new(20, 0).unwrap()),
                ),
                target(
                    2,
                    FinalizeKind::Retry,
                    Some(UtcTimestamp::new(20, 0).unwrap()),
                ),
            ],
            7,
        )))
        .unwrap();
        let QueueCommand::Finalize(command) = &plan.request().commands()[0].command else {
            panic!("expected finalize")
        };
        assert_eq!(command.outcomes[0].applied_state, Some(ItemState::Failed));
        assert_eq!(command.outcomes[1].applied_state, Some(ItemState::Pending));

        *axes.attempts.lock().unwrap() = vec![1, 1];
        *axes.attempt_bounds.lock().unwrap() = None;
        *axes.validation_ids.lock().unwrap() = Some(vec![ItemId::from_u64(2), ItemId::from_u64(1)]);
        assert!(matches!(
            futures::executor::block_on(planner.plan_finalize(request(
                &definition,
                vec![
                    target(1, FinalizeKind::Complete, None),
                    target(2, FinalizeKind::Complete, None),
                ],
                7,
            ))),
            Err(EngineError::Storage(message))
                if message == "async finalize validation returned the wrong item footprint"
        ));
        *axes.validation_ids.lock().unwrap() = None;

        *axes.attempts.lock().unwrap() = vec![3];
        let fallback = futures::executor::block_on(planner.plan_finalize(request(
            &definition,
            vec![target(
                3,
                FinalizeKind::Retry,
                Some(UtcTimestamp::new(20, 0).unwrap()),
            )],
            7,
        )))
        .unwrap();
        let QueueCommand::Finalize(command) = &fallback.request().commands()[0].command else {
            panic!("expected finalize")
        };
        assert_eq!(command.outcomes[0].applied_state, Some(ItemState::Failed));

        *axes.attempts.lock().unwrap() = vec![1];
        let immediate_retry = futures::executor::block_on(planner.plan_finalize(request(
            &definition,
            vec![target(3, FinalizeKind::Retry, None)],
            7,
        )))
        .unwrap();
        let QueueCommand::Finalize(command) = &immediate_retry.request().commands()[0].command
        else {
            panic!("expected finalize")
        };
        assert_eq!(command.outcomes[0].applied_state, Some(ItemState::Pending));
        assert_eq!(command.outcomes[0].not_before, None);

        for (kind, not_before, expected) in [
            (
                FinalizeKind::Rearm,
                Some(UtcTimestamp::new(100, 0).unwrap()),
                EngineError::Unavailable,
            ),
            (
                FinalizeKind::Rearm,
                Some(UtcTimestamp::new(100, 1).unwrap()),
                EngineError::Terminal,
            ),
        ] {
            *axes.attempts.lock().unwrap() = vec![1];
            *axes.attempt_bounds.lock().unwrap() = None;
            let result = futures::executor::block_on(planner.plan_finalize(request(
                &definition,
                vec![target(3, kind, not_before)],
                7,
            )));
            if matches!(expected, EngineError::Unavailable) {
                assert!(result.is_ok());
            } else {
                assert!(matches!(result, Err(error) if error == expected));
            }
        }
        *axes.attempts.lock().unwrap() = vec![1];
        assert!(matches!(
            futures::executor::block_on(planner.plan_finalize(request(
                &definition,
                vec![target(4, FinalizeKind::Complete, None)],
                8,
            ))),
            Err(EngineError::EpochFenced)
        ));
    }

    #[test]
    fn purge_planner_preserves_present_order_dedups_and_fences_epoch() {
        let definition = definition();
        let axes = Arc::new(TestAxes {
            definition: definition.clone(),
            epoch: 7,
            attempts: Mutex::new(Vec::new()),
            attempt_bounds: Mutex::new(None),
            validation_ids: Mutex::new(None),
            ids: AtomicU64::new(0),
        });
        let planner = ProjectionLifecyclePlanner::from_shared(
            axes.clone(),
            axes.clone(),
            axes.clone(),
            axes.clone(),
        );
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let plan = futures::executor::block_on(planner.plan_purge(AsyncPurgeRequest {
            shard: shard.clone(),
            item_ids: vec![
                ItemId::from_u64(3),
                ItemId::from_u64(2),
                ItemId::from_u64(3),
                ItemId::from_u64(1),
            ],
            force: true,
            now: UtcTimestamp::new(10, 0).unwrap(),
            expected_epoch: Some(7),
        }))
        .unwrap();
        let QueueCommand::PurgeItems(command) = &plan.request().commands()[0].command else {
            panic!()
        };
        assert_eq!(
            command.item_ids,
            vec![ItemId::from_u64(3), ItemId::from_u64(1)]
        );
        assert!(command.force);
        assert!(matches!(
            futures::executor::block_on(planner.plan_purge(AsyncPurgeRequest {
                shard,
                item_ids: vec![ItemId::from_u64(1)],
                force: true,
                now: UtcTimestamp::new(10, 0).unwrap(),
                expected_epoch: Some(6),
            })),
            Err(EngineError::EpochFenced)
        ));
    }
}
