//! Reusable native-async claim planning over the storage axes.
//!
//! This component owns no commit authority. It selects against an [`AsyncProjectionStore`], resolves the
//! authoritative log epoch, and builds exactly one typed [`RawCommitRequest`]. [`AsyncComposedBackend`]
//! keeps the queue permit across these calls and routes the resulting request through its configured commit
//! strategy.

use std::sync::Arc;

use fireweed_core::{ItemId, QueueDefinition};

use crate::{
    AsyncClaimPlan, AsyncClaimPlanner, AsyncControlPlane, AsyncLogStore, AsyncProjectionStore,
    ClaimCommand, ClaimRequest, ClaimUnit, CohortClaimCommand, CommandChecksum, CommandEnvelope,
    EngineError, EngineResult, IdGen, OwnedTask, QueueCommand, QueueKey, RawCommitRequest,
};

/// Claim planner shared by native-async object-log/relational projection compositions.
///
/// The four capabilities are explicit so a profile cannot silently obtain definitions, epochs, selection,
/// or command ids from a fallback store. Callers that share these capabilities with a commit strategy can
/// construct the planner with [`Self::from_shared`].
pub struct ProjectionClaimPlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
}

impl<C, L, P, I> ProjectionClaimPlanner<C, L, P, I> {
    pub fn new(control: C, log: L, projection: P, ids: I) -> Self {
        Self::from_shared(
            Arc::new(control),
            Arc::new(log),
            Arc::new(projection),
            Arc::new(ids),
        )
    }

    pub fn from_shared(control: Arc<C>, log: Arc<L>, projection: Arc<P>, ids: Arc<I>) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
        }
    }
}

impl<C, L, P, I> AsyncClaimPlanner for ProjectionClaimPlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
        let control = Arc::clone(&self.control);
        Box::pin(async move { control.queue_definition(shard).await })
    }

    fn plan_claim(
        &self,
        request: ClaimRequest,
        unit: ClaimUnit,
    ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            let selection = match unit {
                ClaimUnit::Item => crate::RichClaimSelection {
                    item_ids: projection
                        .select_item_claim(
                            request.shard.clone(),
                            request.compatibility.clone(),
                            request.eligibility_at(),
                            request.max_items,
                        )
                        .await?,
                    cohort_id: None,
                },
                ClaimUnit::SameGroupKey | ClaimUnit::WholeGroup | ClaimUnit::WholeCohort => {
                    projection
                        .select_rich_claim(
                            request.shard.clone(),
                            unit,
                            request.compatibility.clone(),
                            request.eligibility_at(),
                            request.max_items,
                        )
                        .await?
                }
            };
            if selection.item_ids.is_empty() {
                return Ok(AsyncClaimPlan::empty());
            }

            projection.admit_mutation(request.shard.clone()).await?;

            // Read the durable epoch only after selection, immediately before constructing the commit. A
            // supplied acquire-time epoch is never replaced with this fresh value: disagreement fails closed,
            // and the commit strategy checks the same resolved epoch again at its mutation boundary.
            // P14: async pre-resolution + pure write fence (no reactor block_on).
            let epoch = crate::resolve_write_epoch_async(request.expected_epoch, || {
                log.current_epoch(request.shard.clone())
            })
            .await?;

            let item_ids = selection.item_ids;
            let cohort_id = selection.cohort_id;
            let command = match &cohort_id {
                Some(cohort_id) => QueueCommand::CohortClaim(CohortClaimCommand {
                    cohort_id: cohort_id.clone(),
                    item_ids: item_ids.clone(),
                    lease_token: request.lease_token.clone(),
                    lease_expires_at: request.lease_expires_at,
                }),
                None => QueueCommand::Claim(ClaimCommand {
                    item_ids: item_ids.clone(),
                    lease_token: request.lease_token.clone(),
                    lease_expires_at: request.lease_expires_at,
                    worker_id: Some(request.worker_id.clone()),
                }),
            };
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command,
                // Command payload CRC calculation remains the log codec's responsibility. Every existing
                // engine-created envelope uses this sentinel before serialization.
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncClaimPlan::commit(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
                cohort_id,
            ))
        })
    }

    fn render_claimed(
        &self,
        shard: QueueKey,
        item_ids: Vec<ItemId>,
    ) -> OwnedTask<EngineResult<Vec<crate::ClaimedItem>>> {
        let projection = Arc::clone(&self.projection);
        Box::pin(async move { projection.render_claimed(shard, item_ids).await })
    }
}

#[cfg(test)]
mod tests {
    #![allow(refining_impl_trait)]

    use std::collections::BTreeMap;
    use std::future::{Ready, ready};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use fireweed_core::{
        ClientItemKey, CohortId, CohortPolicy, EligibilityPolicy, GroupKey, LeaseToken, Metadata,
        MetadataValue, OrderingMode, PriorityModel, QueueId, RecurrencePolicy, RetryPolicy,
        TenantId, UtcTimestamp, WorkerId,
    };

    use super::*;
    use crate::{
        AsyncCommitStrategy, AsyncComposedBackend, ClaimCompatibility, ClaimedItem, CommandId,
        CommandPage, CommandPosition, CommitStrategy, CommitStrategyKind, CreateQueueOutcome,
        DispatchError, DurabilityClass, OwnedTaskFactory, RawCommitOutcome, RichClaimSelection,
        TaskOutcome, task_outcome_channel,
    };

    struct TestAxes {
        definition: QueueDefinition,
        epoch: AtomicU64,
        next_id: AtomicU64,
        eligible: Mutex<Vec<ItemId>>,
        rich: Mutex<RichClaimSelection>,
        selection_calls: Mutex<Vec<(ClaimUnit, UtcTimestamp, usize)>>,
        item_compatibility: Mutex<Vec<ClaimCompatibility>>,
        admission_calls: AtomicU64,
        reject_admission: AtomicBool,
    }

    impl TestAxes {
        fn new(definition: QueueDefinition, epoch: u64) -> Self {
            Self {
                definition,
                epoch: AtomicU64::new(epoch),
                next_id: AtomicU64::new(0),
                eligible: Mutex::new(Vec::new()),
                rich: Mutex::new(RichClaimSelection::default()),
                selection_calls: Mutex::new(Vec::new()),
                item_compatibility: Mutex::new(Vec::new()),
                admission_calls: AtomicU64::new(0),
                reject_admission: AtomicBool::new(false),
            }
        }
    }

    impl IdGen for TestAxes {
        fn next_item_id(&self) -> ItemId {
            ItemId::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst))
        }

        fn next_command_id(&self) -> CommandId {
            CommandId::new(format!(
                "async-claim-{}",
                self.next_id.fetch_add(1, Ordering::SeqCst)
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
            ready(Ok(self.epoch.load(Ordering::SeqCst)))
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
            self.admission_calls.fetch_add(1, Ordering::SeqCst);
            ready(if self.reject_admission.load(Ordering::SeqCst) {
                Err(EngineError::Unavailable)
            } else {
                Ok(())
            })
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
            now: UtcTimestamp,
            max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            self.selection_calls
                .lock()
                .unwrap()
                .push((ClaimUnit::Item, now, max));
            ready(Ok(self
                .eligible
                .lock()
                .unwrap()
                .iter()
                .copied()
                .take(max)
                .collect()))
        }

        fn select_item_claim(
            &self,
            shard: QueueKey,
            compatibility: ClaimCompatibility,
            now: UtcTimestamp,
            max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            self.item_compatibility.lock().unwrap().push(compatibility);
            self.eligible_candidates(shard, now, max)
        }

        fn select_rich_claim(
            &self,
            _shard: QueueKey,
            unit: ClaimUnit,
            _compatibility: ClaimCompatibility,
            now: UtcTimestamp,
            max_items: usize,
        ) -> Ready<EngineResult<RichClaimSelection>> {
            self.selection_calls
                .lock()
                .unwrap()
                .push((unit, now, max_items));
            ready(Ok(self.rich.lock().unwrap().clone()))
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            ids: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<ClaimedItem>>> {
            ready(Ok(ids
                .into_iter()
                .map(|item_id| ClaimedItem {
                    item_id,
                    client_item_key: ClientItemKey::new(format!("item-{item_id}")).unwrap(),
                    item_version: 1,
                    priority: None,
                    group_key: None,
                    not_before: None,
                    lease_token: Some(LeaseToken::new("lease").unwrap()),
                    lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
                    attempt_count: 1,
                    payload: None,
                    fields: BTreeMap::new(),
                    metadata: Metadata::default(),
                    gate_keys: Vec::new(),
                })
                .collect()))
        }

        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<fireweed_core::ItemState>>> {
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
            ready(Ok(vec![self.definition.clone()]))
        }
    }

    struct RecordingStrategy {
        commits: Arc<Mutex<Vec<RawCommitRequest>>>,
    }

    impl CommitStrategy for RecordingStrategy {
        fn kind(&self) -> CommitStrategyKind {
            CommitStrategyKind::UnifiedAtomic
        }

        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::Atomic
        }
    }

    impl AsyncCommitStrategy for RecordingStrategy {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit(&self, request: RawCommitRequest) -> OwnedTask<Self::Output> {
            let position = CommandPosition::new(
                request.shard().clone(),
                request.expected_epoch(),
                self.commits.lock().unwrap().len() as u64,
            );
            self.commits.lock().unwrap().push(request);
            Box::pin(ready(Ok(RawCommitOutcome::applied(vec![position]))))
        }
    }

    #[derive(Default)]
    struct InlineDispatcher {
        closed: AtomicBool,
    }

    impl crate::OwnedTaskDispatcher for InlineDispatcher {
        fn submit<T: Send + 'static>(
            &self,
            factory: OwnedTaskFactory<T>,
        ) -> Result<TaskOutcome<T>, DispatchError> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(DispatchError::Closed);
            }
            let (sender, outcome) = task_outcome_channel();
            std::thread::spawn(move || {
                sender.send(futures::executor::block_on(factory()));
            });
            Ok(outcome)
        }

        fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome) = task_outcome_channel();
            sender.send(());
            outcome
        }
    }

    fn definition(cohort: bool) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
            ordering_mode: OrderingMode::Strict,
            max_rank_error: 0,
            progress_bound_ms: 60_000,
            eligibility_policy: EligibilityPolicy::default(),
            cohort_policy: cohort.then_some(CohortPolicy {
                enabled: true,
                completion_bound_ms: Some(60_000),
                on_incomplete: None,
                max_cohort_size: Some(10),
            }),
            recurrence: RecurrencePolicy::default(),
            request_id_retention_ms: 60_000,
            client_item_key_retention_ms: 60_000,
            terminal_retention_ms: 60_000,
            max_lease_duration_ms: 60_000,
            retry_policy: RetryPolicy { max_attempts: 3 },
            max_push_batch_size: 100,
            max_claim_batch_size: 100,
            max_eligible_group_size: Some(10),
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn request(expected_epoch: Option<u64>, whole_cohort: bool) -> ClaimRequest {
        ClaimRequest {
            shard: QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            worker_id: WorkerId::new("worker").unwrap(),
            max_items: 4,
            lease_token: LeaseToken::new("lease").unwrap(),
            lease_expires_at: UtcTimestamp::new(30, 0).unwrap(),
            now: UtcTimestamp::new(20, 0).unwrap(),
            eligibility_time: Some(UtcTimestamp::new(10, 0).unwrap()),
            compatibility: ClaimCompatibility {
                whole_cohort,
                ..ClaimCompatibility::default()
            },
            expected_epoch,
        }
    }

    fn backend(
        axes: Arc<TestAxes>,
        commits: Arc<Mutex<Vec<RawCommitRequest>>>,
    ) -> AsyncComposedBackend<
        RecordingStrategy,
        InlineDispatcher,
        ProjectionClaimPlanner<TestAxes, TestAxes, TestAxes, TestAxes>,
    > {
        let planner = ProjectionClaimPlanner::from_shared(
            Arc::clone(&axes),
            Arc::clone(&axes),
            Arc::clone(&axes),
            axes,
        );
        AsyncComposedBackend::new_with_claim_planner(
            RecordingStrategy { commits },
            InlineDispatcher::default(),
            planner,
            1,
        )
    }

    #[test]
    fn item_claim_uses_eligibility_epoch_and_mints_distinct_envelopes() {
        let axes = Arc::new(TestAxes::new(definition(false), 7));
        *axes.eligible.lock().unwrap() = vec![ItemId::from_u64(1), ItemId::from_u64(2)];
        let commits = Arc::new(Mutex::new(Vec::new()));
        let backend = backend(Arc::clone(&axes), Arc::clone(&commits));

        let first = futures::executor::block_on(backend.claim(request(Some(7), false))).unwrap();
        let second = futures::executor::block_on(backend.claim(request(None, false))).unwrap();
        assert_eq!(first.items.len(), 2);
        assert_eq!(second.items.len(), 2);
        assert_eq!(
            axes.selection_calls.lock().unwrap()[0],
            (ClaimUnit::Item, UtcTimestamp::new(10, 0).unwrap(), 4,)
        );
        let commits = commits.lock().unwrap();
        assert_eq!(commits[0].expected_epoch(), 7);
        assert_ne!(
            commits[0].commands()[0].command_id,
            commits[1].commands()[0].command_id
        );
        assert_eq!(
            commits[0].commands()[0].created_at,
            UtcTimestamp::new(20, 0).unwrap()
        );
        assert!(matches!(
            commits[0].commands()[0].command,
            QueueCommand::Claim(_)
        ));
    }

    #[test]
    fn stale_acquire_epoch_fails_before_id_mint_or_commit() {
        let axes = Arc::new(TestAxes::new(definition(false), 8));
        *axes.eligible.lock().unwrap() = vec![ItemId::from_u64(1)];
        let commits = Arc::new(Mutex::new(Vec::new()));
        let backend = backend(Arc::clone(&axes), Arc::clone(&commits));

        let error =
            futures::executor::block_on(backend.claim(request(Some(7), false))).unwrap_err();
        assert_eq!(
            error,
            crate::AsyncClaimError::BeforeCommit(EngineError::EpochFenced)
        );
        assert!(commits.lock().unwrap().is_empty());
        assert_eq!(axes.next_id.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn item_claim_preserves_group_and_metadata_filters() {
        let axes = Arc::new(TestAxes::new(definition(false), 2));
        *axes.eligible.lock().unwrap() = vec![ItemId::from_u64(9)];
        let commits = Arc::new(Mutex::new(Vec::new()));
        let backend = backend(Arc::clone(&axes), commits);
        let mut filtered = request(Some(2), false);
        filtered.compatibility.group_key = Some(GroupKey::new("group-a").unwrap());
        filtered.compatibility.metadata_equals.insert(
            "region".to_string(),
            MetadataValue::String("east".to_string()),
        );

        let claimed = futures::executor::block_on(backend.claim(filtered.clone())).unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(
            axes.item_compatibility.lock().unwrap().as_slice(),
            &[filtered.compatibility]
        );
    }

    #[test]
    fn admission_rejection_precedes_epoch_read_id_mint_and_commit() {
        let axes = Arc::new(TestAxes::new(definition(false), 5));
        *axes.eligible.lock().unwrap() = vec![ItemId::from_u64(1)];
        axes.reject_admission.store(true, Ordering::SeqCst);
        let commits = Arc::new(Mutex::new(Vec::new()));
        let backend = backend(Arc::clone(&axes), Arc::clone(&commits));

        let error =
            futures::executor::block_on(backend.claim(request(Some(5), false))).unwrap_err();
        assert_eq!(
            error,
            crate::AsyncClaimError::BeforeCommit(EngineError::Unavailable)
        );
        assert_eq!(axes.admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(axes.next_id.load(Ordering::SeqCst), 0);
        assert!(commits.lock().unwrap().is_empty());
    }

    #[test]
    fn whole_cohort_preserves_selection_and_top_level_lease_shape() {
        let axes = Arc::new(TestAxes::new(definition(true), 3));
        let cohort_id = CohortId::new("cohort").unwrap();
        *axes.rich.lock().unwrap() = RichClaimSelection {
            item_ids: vec![ItemId::from_u64(4), ItemId::from_u64(5)],
            cohort_id: Some(cohort_id.clone()),
        };
        let commits = Arc::new(Mutex::new(Vec::new()));
        let backend = backend(Arc::clone(&axes), Arc::clone(&commits));

        let claimed = futures::executor::block_on(backend.claim(request(Some(3), true))).unwrap();
        assert_eq!(claimed.cohort_id, Some(cohort_id));
        assert_eq!(
            claimed.cohort_lease_token,
            Some(LeaseToken::new("lease").unwrap())
        );
        assert!(claimed.items.iter().all(|item| item.lease_token.is_none()));
        assert!(matches!(
            commits.lock().unwrap()[0].commands()[0].command,
            QueueCommand::CohortClaim(_)
        ));
        assert_eq!(
            axes.selection_calls.lock().unwrap()[0].0,
            ClaimUnit::WholeCohort
        );
    }
}
