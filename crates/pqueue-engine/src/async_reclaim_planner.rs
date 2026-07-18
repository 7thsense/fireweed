//! Typed expired-lease preparation over native-async storage axes.

use std::sync::Arc;

use pqueue_core::{ItemId, QueueDefinition, UtcTimestamp};

use crate::{
    AsyncControlPlane, AsyncLogStore, AsyncProjectionStore, CommandChecksum, CommandEnvelope,
    EngineError, EngineResult, IdGen, LeaseExpiredCommand, OwnedTask, QueueCommand, QueueKey,
    RawCommitRequest,
};

#[derive(Debug, Clone)]
pub struct AsyncReclaimRequest {
    pub shard: QueueKey,
    pub limit: Option<usize>,
    pub now: UtcTimestamp,
    pub expected_epoch: Option<u64>,
}

/// Read-only planning result. The composed lifecycle seam retains commit authority and the queue permit.
pub struct AsyncReclaimPlan {
    request: Option<RawCommitRequest>,
    item_ids: Vec<ItemId>,
}

impl AsyncReclaimPlan {
    pub(crate) fn empty() -> Self {
        Self {
            request: None,
            item_ids: Vec::new(),
        }
    }

    pub(crate) fn commit(request: RawCommitRequest, item_ids: Vec<ItemId>) -> Self {
        Self {
            request: Some(request),
            item_ids,
        }
    }

    pub fn item_ids(&self) -> &[ItemId] {
        &self.item_ids
    }

    pub fn into_parts(self) -> (Option<RawCommitRequest>, Vec<ItemId>) {
        (self.request, self.item_ids)
    }
}

pub trait AsyncReclaimPlanner: Send + Sync + 'static {
    fn plan_reclaim(
        &self,
        request: AsyncReclaimRequest,
    ) -> OwnedTask<EngineResult<AsyncReclaimPlan>>;
}

pub struct ProjectionReclaimPlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
}

impl<C, L, P, I> ProjectionReclaimPlanner<C, L, P, I> {
    pub fn from_shared(control: Arc<C>, log: Arc<L>, projection: Arc<P>, ids: Arc<I>) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
        }
    }
}

fn reclaim_limit(definition: &QueueDefinition, requested: Option<usize>) -> EngineResult<usize> {
    let maximum = definition.max_claim_batch_size as usize;
    let limit = requested.unwrap_or(maximum);
    if limit > maximum {
        return Err(EngineError::Invalid(
            "reclaim batch exceeds queue claim batch limit",
        ));
    }
    Ok(limit)
}

impl<C, L, P, I> AsyncReclaimPlanner for ProjectionReclaimPlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn plan_reclaim(
        &self,
        request: AsyncReclaimRequest,
    ) -> OwnedTask<EngineResult<AsyncReclaimPlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            let definition = control.queue_definition(request.shard.clone()).await?;
            if definition.tenant_id != request.shard.tenant_id
                || definition.queue_id != request.shard.queue_id
            {
                return Err(EngineError::Storage(
                    "async reclaim planner returned the wrong queue definition".to_string(),
                ));
            }
            let limit = reclaim_limit(&definition, request.limit)?;
            if limit == 0 {
                return Ok(AsyncReclaimPlan::empty());
            }
            projection.admit_mutation(request.shard.clone()).await?;
            let item_ids = projection
                .expired_leases(request.shard.clone(), request.now, limit)
                .await?;
            if item_ids.is_empty() {
                return Ok(AsyncReclaimPlan::empty());
            }
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request
                .expected_epoch
                .is_some_and(|expected| expected != epoch)
            {
                return Err(EngineError::EpochFenced);
            }
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: item_ids.clone(),
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncReclaimPlan::commit(
                RawCommitRequest::new(request.shard, vec![envelope], epoch),
                item_ids,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(refining_impl_trait)]

    use std::future::{Ready, ready};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use pqueue_conformance::{qdef, ts};
    use pqueue_core::{ItemState, QueueId, TenantId};

    use crate::{
        ClaimedItem, CommandId, CommandPage, CommandPosition, CreateQueueOutcome, DurabilityClass,
    };

    use super::*;

    #[test]
    fn reclaim_batch_is_capped_by_queue_claim_limit() {
        let definition = qdef();
        assert_eq!(reclaim_limit(&definition, None).unwrap(), 100);
        assert_eq!(reclaim_limit(&definition, Some(0)).unwrap(), 0);
        assert!(reclaim_limit(&definition, Some(101)).is_err());
    }

    struct TestAxes {
        definition: QueueDefinition,
        epoch: u64,
        expired: Mutex<Vec<ItemId>>,
        events: Mutex<Vec<&'static str>>,
        reject_admission: bool,
        ids: AtomicU64,
    }

    impl TestAxes {
        fn new(definition: QueueDefinition, epoch: u64, expired: Vec<ItemId>) -> Self {
            Self {
                definition,
                epoch,
                expired: Mutex::new(expired),
                events: Mutex::new(Vec::new()),
                reject_admission: false,
                ids: AtomicU64::new(0),
            }
        }

        fn event(&self, event: &'static str) {
            self.events.lock().unwrap().push(event);
        }
    }

    impl IdGen for TestAxes {
        fn next_item_id(&self) -> ItemId {
            ItemId::from_u64(999)
        }

        fn next_command_id(&self) -> CommandId {
            self.event("id");
            CommandId::new(format!(
                "reclaim-{}",
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
            self.event("definition");
            ready(Ok(self.definition.clone()))
        }

        fn list_queues(&self, _tenant: TenantId) -> Ready<EngineResult<Vec<QueueId>>> {
            ready(Err(EngineError::Unavailable))
        }
    }

    impl AsyncLogStore for TestAxes {
        fn durability_class(&self) -> DurabilityClass {
            DurabilityClass::EventualApply
        }
        fn ensure_shard(&self, _shard: QueueKey) -> Ready<EngineResult<()>> {
            ready(Err(EngineError::Unavailable))
        }
        fn current_epoch(&self, _shard: QueueKey) -> Ready<EngineResult<u64>> {
            self.event("epoch");
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
            self.event("admit");
            ready(if self.reject_admission {
                Err(EngineError::Unavailable)
            } else {
                Ok(())
            })
        }
        fn expired_leases(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            max: usize,
        ) -> Ready<EngineResult<Vec<ItemId>>> {
            self.event("select");
            ready(Ok(self
                .expired
                .lock()
                .unwrap()
                .iter()
                .copied()
                .take(max)
                .collect()))
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

    fn planner(
        axes: Arc<TestAxes>,
    ) -> ProjectionReclaimPlanner<TestAxes, TestAxes, TestAxes, TestAxes> {
        ProjectionReclaimPlanner::from_shared(
            Arc::clone(&axes),
            Arc::clone(&axes),
            Arc::clone(&axes),
            axes,
        )
    }

    fn request(definition: &QueueDefinition) -> AsyncReclaimRequest {
        AsyncReclaimRequest {
            shard: QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone()),
            limit: Some(2),
            now: ts(20),
            expected_epoch: Some(7),
        }
    }

    #[test]
    fn planner_orders_admission_selection_epoch_and_exact_envelope() {
        let definition = qdef();
        let selected = vec![ItemId::from_u64(1), ItemId::from_u64(2)];
        let axes = Arc::new(TestAxes::new(definition.clone(), 7, selected.clone()));
        let plan = futures::executor::block_on(
            planner(Arc::clone(&axes)).plan_reclaim(request(&definition)),
        )
        .unwrap();
        assert_eq!(
            axes.events.lock().unwrap().as_slice(),
            ["definition", "admit", "select", "epoch", "id"]
        );
        let (commit, ids) = plan.into_parts();
        assert_eq!(ids, selected);
        let commit = commit.unwrap();
        assert_eq!(commit.expected_epoch(), 7);
        assert_eq!(commit.commands().len(), 1);
        assert_eq!(commit.commands()[0].item_ids, ids);
        assert_eq!(commit.commands()[0].created_at, ts(20));
        assert!(matches!(
            &commit.commands()[0].command,
            QueueCommand::LeaseExpired(command) if command.item_ids == ids
        ));
    }

    #[test]
    fn empty_selection_skips_epoch_and_id_and_fence_precedes_id() {
        let definition = qdef();
        let empty = Arc::new(TestAxes::new(definition.clone(), 99, Vec::new()));
        let plan = futures::executor::block_on(
            planner(Arc::clone(&empty)).plan_reclaim(request(&definition)),
        )
        .unwrap();
        assert_eq!(
            empty.events.lock().unwrap().as_slice(),
            ["definition", "admit", "select"]
        );
        assert!(plan.into_parts().0.is_none());

        let fenced = Arc::new(TestAxes::new(
            definition.clone(),
            8,
            vec![ItemId::from_u64(1)],
        ));
        assert!(matches!(
            futures::executor::block_on(
                planner(Arc::clone(&fenced)).plan_reclaim(request(&definition))
            ),
            Err(EngineError::EpochFenced)
        ));
        assert_eq!(
            fenced.events.lock().unwrap().as_slice(),
            ["definition", "admit", "select", "epoch"]
        );
    }

    #[test]
    fn wrong_definition_and_admission_failure_stop_before_selection() {
        let definition = qdef();
        let mut wrong = definition.clone();
        wrong.queue_id = QueueId::new("wrong").unwrap();
        let axes = Arc::new(TestAxes::new(wrong, 7, vec![ItemId::from_u64(1)]));
        assert!(
            futures::executor::block_on(
                planner(Arc::clone(&axes)).plan_reclaim(request(&definition))
            )
            .is_err()
        );
        assert_eq!(axes.events.lock().unwrap().as_slice(), ["definition"]);

        let mut rejected = TestAxes::new(definition.clone(), 7, vec![ItemId::from_u64(1)]);
        rejected.reject_admission = true;
        let rejected = Arc::new(rejected);
        assert!(
            futures::executor::block_on(
                planner(Arc::clone(&rejected)).plan_reclaim(request(&definition))
            )
            .is_err()
        );
        assert_eq!(
            rejected.events.lock().unwrap().as_slice(),
            ["definition", "admit"]
        );
    }
}
