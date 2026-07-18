//! Reusable, fail-closed async push preparation over the storage axes.

use std::sync::Arc;

use pqueue_core::QueueDefinition;

use crate::{
    AsyncControlPlane, AsyncLogStore, AsyncProjectionStore, AsyncPushPlan, AsyncPushPlanner,
    AsyncPushRequest, CommandChecksum, CommandEnvelope, EngineError, EngineResult, IdGen,
    IdempotencyDecision, OwnedTask, PushCommand, PushFingerprint, QueueCommand, QueueCounters,
    RawCommitRequest, RequestOutcome, build_push_items,
};

/// Push preparation shared by native-async compositions. It owns no commit capability.
///
/// Projection admission, pause, constraint, and retained-idempotency checks are mandatory methods on the
/// async projection axis and default to `Unavailable`, so an incomplete axis cannot append optimistically.
pub struct ProjectionPushPlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
    counters: Arc<QueueCounters>,
    node_id: u8,
}

impl<C, L, P, I> ProjectionPushPlanner<C, L, P, I> {
    pub fn from_shared(
        control: Arc<C>,
        log: Arc<L>,
        projection: Arc<P>,
        ids: Arc<I>,
        counters: Arc<QueueCounters>,
        node_id: u8,
    ) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
            counters,
            node_id,
        }
    }
}

impl<C, L, P, I> AsyncPushPlanner for ProjectionPushPlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn supports_gates(&self) -> bool {
        self.projection.supports_gates()
    }

    fn queue_definition(&self, shard: crate::QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
        let control = Arc::clone(&self.control);
        Box::pin(async move { control.queue_definition(shard).await })
    }

    fn plan_push(
        &self,
        request: AsyncPushRequest,
        definition: QueueDefinition,
        fingerprint: Option<PushFingerprint>,
    ) -> OwnedTask<EngineResult<AsyncPushPlan>> {
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        let counters = Arc::clone(&self.counters);
        let node_id = self.node_id;
        Box::pin(async move {
            if let (Some(request_id), Some(fingerprint)) = (request.request_id.clone(), fingerprint)
            {
                match projection
                    .push_idempotency(request.shard.clone(), request_id, fingerprint, request.now)
                    .await?
                {
                    IdempotencyDecision::Replay(item_ids) => {
                        return Ok(AsyncPushPlan::replay(item_ids));
                    }
                    IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                    IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
                }
            }

            projection.admit_mutation(request.shard.clone()).await?;
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request
                .expected_epoch
                .is_some_and(|expected| expected != epoch)
            {
                return Err(EngineError::EpochFenced);
            }
            let base = counters.reserve(&request.shard, epoch, request.items.len() as u32);
            let (items, item_ids) = build_push_items(
                request.items,
                epoch,
                node_id,
                base,
                definition.retry_policy.max_attempts,
            );
            projection
                .validate_push(request.shard.clone(), items.clone(), request.now)
                .await?;
            if projection
                .pause_blocks_intake(request.shard.clone())
                .await?
            {
                return Err(EngineError::Paused { drain_intake: true });
            }
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
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
            Ok(AsyncPushPlan::commit(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(refining_impl_trait)]

    use std::collections::HashMap;
    use std::future::{Ready, ready};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use pqueue_core::{
        BodyHash, EligibilityPolicy, GateKeyPolicy, ItemId, ItemState, OrderingMode, PriorityModel,
        QueueId, RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp,
    };

    use super::*;
    use crate::{
        AsyncCommitStrategy, AsyncComposedBackend, CommandId, CommandPage, CommandPosition,
        CommitStrategy, CommitStrategyKind, CreateQueueOutcome, DispatchError, DurabilityClass,
        OwnedTaskFactory, PushSpec, RawCommitOutcome, TaskOutcome, task_outcome_channel,
    };

    struct TestAxes {
        definition: QueueDefinition,
        epoch: u64,
        next_command: AtomicU64,
        replays: Mutex<HashMap<RequestId, (BodyHash, Vec<ItemId>)>>,
        validated: AtomicU64,
    }

    impl IdGen for TestAxes {
        fn next_item_id(&self) -> ItemId {
            ItemId::from_u64(0)
        }
        fn next_command_id(&self) -> CommandId {
            CommandId::new(format!(
                "push-{}",
                self.next_command.fetch_add(1, Ordering::SeqCst)
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
        fn queue_definition(&self, _key: crate::QueueKey) -> Ready<EngineResult<QueueDefinition>> {
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
        fn ensure_shard(&self, _shard: crate::QueueKey) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn current_epoch(&self, _shard: crate::QueueKey) -> Ready<EngineResult<u64>> {
            ready(Ok(self.epoch))
        }
        fn acquire_epoch(&self, _shard: crate::QueueKey) -> Ready<EngineResult<u64>> {
            ready(Err(EngineError::Unavailable))
        }
        fn append(
            &self,
            _shard: crate::QueueKey,
            _commands: Vec<CommandEnvelope>,
            _expected_epoch: u64,
        ) -> Ready<EngineResult<Vec<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn read_from(
            &self,
            _shard: crate::QueueKey,
            _from: Option<CommandPosition>,
            _limit: usize,
        ) -> Ready<EngineResult<CommandPage>> {
            ready(Err(EngineError::Unavailable))
        }
        fn high_water(
            &self,
            _shard: crate::QueueKey,
        ) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn set_high_water(
            &self,
            _shard: crate::QueueKey,
            _position: CommandPosition,
        ) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
    }

    impl AsyncProjectionStore for TestAxes {
        fn supports_gates(&self) -> bool {
            true
        }

        fn ensure_shard(&self, _definition: QueueDefinition) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn admit_mutation(&self, _shard: crate::QueueKey) -> Ready<EngineResult<()>> {
            ready(Ok(()))
        }
        fn validate_push(
            &self,
            _shard: crate::QueueKey,
            _items: Vec<crate::PushItem>,
            _now: UtcTimestamp,
        ) -> Ready<EngineResult<()>> {
            self.validated.fetch_add(1, Ordering::SeqCst);
            ready(Ok(()))
        }
        fn pause_blocks_intake(&self, _shard: crate::QueueKey) -> Ready<EngineResult<bool>> {
            ready(Ok(false))
        }
        fn push_idempotency(
            &self,
            _shard: crate::QueueKey,
            request_id: RequestId,
            fingerprint: PushFingerprint,
            _now: UtcTimestamp,
        ) -> Ready<EngineResult<IdempotencyDecision<Vec<ItemId>>>> {
            let decision = match self.replays.lock().unwrap().get(&request_id) {
                None => IdempotencyDecision::Proceed,
                Some((stored, ids)) if *stored == fingerprint.legacy_body_hash => {
                    IdempotencyDecision::Replay(ids.clone())
                }
                Some(_) => IdempotencyDecision::Conflict,
            };
            ready(Ok(decision))
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
            _shard: crate::QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn render_claimed(
            &self,
            _shard: crate::QueueKey,
            _ids: Vec<ItemId>,
        ) -> Ready<EngineResult<Vec<crate::ClaimedItem>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_state(
            &self,
            _shard: crate::QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<ItemState>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn item_version(
            &self,
            _shard: crate::QueueKey,
            _id: ItemId,
        ) -> Ready<EngineResult<Option<u64>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recovery_high_water(
            &self,
            _shard: crate::QueueKey,
        ) -> Ready<EngineResult<Option<CommandPosition>>> {
            ready(Err(EngineError::Unavailable))
        }
        fn recover_definitions(&self) -> Ready<EngineResult<Vec<QueueDefinition>>> {
            ready(Ok(vec![self.definition.clone()]))
        }
    }

    struct ApplyingStrategy {
        axes: Arc<TestAxes>,
        commits: Arc<AtomicU64>,
    }
    impl CommitStrategy for ApplyingStrategy {
        fn kind(&self) -> CommitStrategyKind {
            CommitStrategyKind::UnifiedAtomic
        }
        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::Atomic
        }
    }
    impl AsyncCommitStrategy for ApplyingStrategy {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;
        fn commit(&self, request: RawCommitRequest) -> OwnedTask<Self::Output> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            let envelope = &request.commands()[0];
            if let (Some(id), Some(hash), Some(RequestOutcome::Push { item_ids })) = (
                &envelope.request_id,
                envelope.request_fingerprint,
                &envelope.request_outcome,
            ) {
                self.axes
                    .replays
                    .lock()
                    .unwrap()
                    .insert(id.clone(), (BodyHash(hash), item_ids.clone()));
            }
            let position =
                CommandPosition::new(request.shard().clone(), request.expected_epoch(), 1);
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
            std::thread::spawn(move || sender.send(futures::executor::block_on(factory())));
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

    fn definition() -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new("queue").unwrap(),
            priority_model: PriorityModel::timestamp_ascending(),
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

    #[test]
    fn committed_request_replays_without_second_commit_and_conflicts_on_body_change() {
        let axes = Arc::new(TestAxes {
            definition: definition(),
            epoch: 7,
            next_command: AtomicU64::new(0),
            replays: Mutex::new(HashMap::new()),
            validated: AtomicU64::new(0),
        });
        let commits = Arc::new(AtomicU64::new(0));
        let planner = ProjectionPushPlanner::from_shared(
            axes.clone(),
            axes.clone(),
            axes.clone(),
            axes.clone(),
            Arc::new(QueueCounters::default()),
            1,
        );
        let backend = AsyncComposedBackend::new_with_planners(
            ApplyingStrategy {
                axes: axes.clone(),
                commits: commits.clone(),
            },
            InlineDispatcher::default(),
            crate::NoAsyncClaimPlanner,
            planner,
            4,
        );
        let request_id = RequestId::new("request").unwrap();
        let make = |payload: &'static [u8]| AsyncPushRequest {
            shard: crate::QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            request_id: Some(request_id.clone()),
            items: vec![PushSpec {
                payload: Some(bytes::Bytes::from_static(payload)),
                ..PushSpec::default()
            }],
            now: UtcTimestamp::new(1, 0).unwrap(),
            expected_epoch: Some(7),
        };

        let first = futures::executor::block_on(backend.push(make(b"one"))).unwrap();
        let replay = futures::executor::block_on(backend.push(make(b"one"))).unwrap();
        assert_eq!(replay, first);
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(axes.validated.load(Ordering::SeqCst), 1);
        assert_eq!(
            futures::executor::block_on(backend.push(make(b"two"))).unwrap_err(),
            crate::AsyncPushError::BeforeCommit(EngineError::RequestIdConflict)
        );
        assert_eq!(commits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn legacy_gate_order_fingerprint_replays_after_canonical_normalization() {
        let request_id = RequestId::new("legacy-gate-order").unwrap();
        let original = vec![PushSpec {
            gate_keys: vec!["z".to_string(), "a".to_string()],
            ..PushSpec::default()
        }];
        let legacy = crate::compose::push_body_hash(&original).unwrap();
        let replayed_id = ItemId::mint(7, 1, 99);
        let mut definition = definition();
        definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
        let axes = Arc::new(TestAxes {
            definition,
            epoch: 7,
            next_command: AtomicU64::new(0),
            replays: Mutex::new(HashMap::from([(
                request_id.clone(),
                (legacy, vec![replayed_id]),
            )])),
            validated: AtomicU64::new(0),
        });
        let commits = Arc::new(AtomicU64::new(0));
        let planner = ProjectionPushPlanner::from_shared(
            axes.clone(),
            axes.clone(),
            axes.clone(),
            axes.clone(),
            Arc::new(QueueCounters::default()),
            1,
        );
        let backend = AsyncComposedBackend::new_with_planners(
            ApplyingStrategy {
                axes: axes.clone(),
                commits: commits.clone(),
            },
            InlineDispatcher::default(),
            crate::NoAsyncClaimPlanner,
            planner,
            4,
        );

        let replay = futures::executor::block_on(backend.push(AsyncPushRequest {
            shard: crate::QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new("queue").unwrap(),
            ),
            request_id: Some(request_id),
            items: original,
            now: UtcTimestamp::new(2, 0).unwrap(),
            expected_epoch: Some(7),
        }))
        .unwrap();

        assert_eq!(replay, vec![replayed_id]);
        assert_eq!(commits.load(Ordering::SeqCst), 0);
        assert_eq!(axes.validated.load(Ordering::SeqCst), 0);
    }
}
