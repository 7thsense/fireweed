//! Narrow async mutation-path scaffolding from ADR-017.

use std::collections::HashSet;
use std::sync::Arc;

use pqueue_core::{CohortId, ItemId, QueueDefinition};

use crate::{
    AsyncCommitStrategy, ClaimCommand, ClaimRequest, ClaimUnit, Claimed, ClaimedItem,
    CohortClaimCommand, CommandChecksum, DispatchError, DurabilityClass, EngineError, EngineResult,
    KeyedQueueGate, OwnedTask, OwnedTaskDispatcher, QueueCommand, QueueGateError, QueueKey,
    RawCommitFault, RawCommitOutcome, RawCommitRequest, TaskOutcomeError,
    validate_claim_compatibility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncCommitSubmitError {
    Admission(QueueGateError),
    Dispatch(DispatchError),
    Outcome(TaskOutcomeError),
}

/// Failure of a typed async claim operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AsyncClaimError {
    Submit(AsyncCommitSubmitError),
    /// Definition lookup, compatibility validation, or planning failed before strategy invocation.
    BeforeCommit(EngineError),
    /// The strategy reported failure while owning the commit. This does not itself prove rollback: a
    /// substrate error at the commit boundary may be an unknown outcome and must be resolved by replay.
    Commit(EngineError),
    /// The strategy returned durable success, but its footprint or the response barrier failed validation.
    AfterCommit {
        stage: AsyncClaimPostCommitStage,
        source: EngineError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncClaimPostCommitStage {
    CommitOutcome,
    Render,
    RenderValidation,
}

/// The owned result of claim selection and command construction.
///
/// A planner may select nothing or produce one typed claim commit. The composed backend validates that
/// the commit is a claim for the requested queue, candidates, lease, and fence before routing it through
/// its injected [`AsyncCommitStrategy`].
pub struct AsyncClaimPlan {
    kind: AsyncClaimPlanKind,
}

enum AsyncClaimPlanKind {
    Empty,
    Commit {
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    },
}

impl AsyncClaimPlan {
    pub fn empty() -> Self {
        Self {
            kind: AsyncClaimPlanKind::Empty,
        }
    }

    pub fn commit(
        request: RawCommitRequest,
        item_ids: Vec<ItemId>,
        cohort_id: Option<CohortId>,
    ) -> Self {
        Self {
            kind: AsyncClaimPlanKind::Commit {
                request,
                item_ids,
                cohort_id,
            },
        }
    }
}

/// Backend-owned claim planning and rendering capability.
///
/// Implementations perform selection and construct a claim envelope, but never commit it. This split keeps
/// the durable mutation authority in [`AsyncCommitStrategy`] while allowing native-async projections to
/// perform selection and response rendering without a blocking bridge.
pub trait AsyncClaimPlanner: Send + Sync + 'static {
    /// Load the authoritative definition used by engine-owned compatibility validation.
    fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>>;

    fn plan_claim(
        &self,
        request: ClaimRequest,
        unit: ClaimUnit,
    ) -> OwnedTask<EngineResult<AsyncClaimPlan>>;

    fn render_claimed(
        &self,
        shard: QueueKey,
        item_ids: Vec<ItemId>,
    ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>>;
}

/// Marker used by a raw-commit-only composed backend.
pub struct NoAsyncClaimPlanner;

/// An ADR-017 mutation-path scaffold, not a full backend implementation.
pub struct AsyncComposedBackend<S, D, P = NoAsyncClaimPlanner> {
    strategy: Arc<S>,
    dispatcher: D,
    claim_planner: Arc<P>,
    admission: KeyedQueueGate<crate::QueueKey>,
    durability: DurabilityClass,
}

impl<S, D> AsyncComposedBackend<S, D, NoAsyncClaimPlanner>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn new(strategy: S, dispatcher: D, max_queued_commits: usize) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy: Arc::new(strategy),
            dispatcher,
            claim_planner: Arc::new(NoAsyncClaimPlanner),
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
        }
    }
}

impl<S, D, P> AsyncComposedBackend<S, D, P>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest>,
    D: OwnedTaskDispatcher,
{
    pub fn new_with_claim_planner(
        strategy: S,
        dispatcher: D,
        claim_planner: P,
        max_queued_commits: usize,
    ) -> Self {
        let durability = strategy.durability_class();
        Self {
            strategy: Arc::new(strategy),
            dispatcher,
            claim_planner: Arc::new(claim_planner),
            admission: KeyedQueueGate::new(max_queued_commits),
            durability,
        }
    }

    pub fn durability_class(&self) -> DurabilityClass {
        self.durability
    }

    /// Serialize and dispatch one complete queue-local operation.
    ///
    /// The factory is invoked only after dispatcher acceptance. Its owned task retains the queue permit
    /// across every phase it contains, such as validation, idempotency, claim planning, commit, and render.
    /// This remains a narrow composition primitive and does not imply operation-port parity.
    pub(crate) async fn submit_operation<T, F>(
        &self,
        queue: QueueKey,
        operation: F,
    ) -> Result<T, AsyncCommitSubmitError>
    where
        T: Send + 'static,
        F: FnOnce() -> OwnedTask<T> + Send + 'static,
    {
        let permit = self
            .admission
            .acquire(queue)
            .await
            .map_err(AsyncCommitSubmitError::Admission)?;
        let outcome = self
            .dispatcher
            .submit(Box::new(move || {
                let operation = operation();
                Box::pin(async move {
                    let _permit = permit;
                    operation.await
                })
            }))
            .map_err(AsyncCommitSubmitError::Dispatch)?;
        outcome.await.map_err(AsyncCommitSubmitError::Outcome)
    }

    /// Typed raw-commit wrapper over [`Self::submit_operation`].
    pub async fn submit_commit(
        &self,
        request: RawCommitRequest,
    ) -> Result<S::Output, AsyncCommitSubmitError> {
        let queue = request.shard().clone();
        let strategy = Arc::clone(&self.strategy);
        self.submit_operation(queue, move || strategy.commit(request))
            .await
    }

    pub fn close(&self) {
        self.admission.close();
        self.dispatcher.close();
    }

    pub fn is_closed(&self) -> bool {
        self.admission.is_closed() || self.dispatcher.is_closed()
    }

    pub async fn drain(&self) -> Result<(), TaskOutcomeError> {
        self.dispatcher.drain().await
    }

    pub async fn close_and_drain(&self) -> Result<(), TaskOutcomeError> {
        self.close();
        self.drain().await
    }
}

impl<S, D, P> AsyncComposedBackend<S, D, P>
where
    S: AsyncCommitStrategy<Request = RawCommitRequest, Output = EngineResult<RawCommitOutcome>>,
    D: OwnedTaskDispatcher,
    P: AsyncClaimPlanner,
{
    /// Plan, durably commit, and render one typed claim under a single queue-local permit.
    pub async fn claim(&self, request: ClaimRequest) -> Result<Claimed, AsyncClaimError> {
        let queue = request.shard.clone();
        let strategy = Arc::clone(&self.strategy);
        let planner = Arc::clone(&self.claim_planner);
        self.submit_operation(queue.clone(), move || {
            Box::pin(async move {
                let definition = planner
                    .queue_definition(queue.clone())
                    .await
                    .map_err(ClaimExecutionError::BeforeCommit)?;
                if definition.tenant_id != queue.tenant_id || definition.queue_id != queue.queue_id
                {
                    return Err(ClaimExecutionError::BeforeCommit(EngineError::Storage(
                        "async claim planner returned the wrong queue definition".to_string(),
                    )));
                }
                let unit = validate_claim_compatibility(
                    &request.compatibility,
                    request.max_items as u64,
                    &definition,
                )
                .map_err(ClaimExecutionError::BeforeCommit)?;
                let plan = planner
                    .plan_claim(request.clone(), unit)
                    .await
                    .map_err(ClaimExecutionError::BeforeCommit)?;
                let AsyncClaimPlanKind::Commit {
                    request: commit,
                    item_ids,
                    cohort_id,
                } = plan.kind
                else {
                    return Ok::<Claimed, ClaimExecutionError>(Claimed::default());
                };
                validate_claim_plan(&request, &queue, &commit, &item_ids, cohort_id.as_ref())
                    .map_err(ClaimExecutionError::BeforeCommit)?;
                let expected_epoch = commit.expected_epoch();
                let outcome = strategy
                    .commit(commit)
                    .await
                    .map_err(ClaimExecutionError::Commit)?;
                validate_claim_commit_outcome(&queue, expected_epoch, &outcome).map_err(
                    |source| ClaimExecutionError::AfterCommit {
                        stage: AsyncClaimPostCommitStage::CommitOutcome,
                        source,
                    },
                )?;
                let mut items = planner
                    .render_claimed(queue.clone(), item_ids.clone())
                    .await
                    .map_err(|source| ClaimExecutionError::AfterCommit {
                        stage: AsyncClaimPostCommitStage::Render,
                        source,
                    })?;
                validate_rendered_claim(&request, &item_ids, &items).map_err(|source| {
                    ClaimExecutionError::AfterCommit {
                        stage: AsyncClaimPostCommitStage::RenderValidation,
                        source,
                    }
                })?;
                let mut claimed = Claimed {
                    items: Vec::new(),
                    cohort_lease_token: None,
                    cohort_id: None,
                };
                if let Some(cohort_id) = cohort_id {
                    for item in &mut items {
                        item.lease_token = None;
                    }
                    claimed.cohort_lease_token = Some(request.lease_token);
                    claimed.cohort_id = Some(cohort_id);
                }
                claimed.items = items;
                Ok(claimed)
            })
        })
        .await
        .map_err(AsyncClaimError::Submit)?
        .map_err(AsyncClaimError::from)
    }
}

enum ClaimExecutionError {
    BeforeCommit(EngineError),
    Commit(EngineError),
    AfterCommit {
        stage: AsyncClaimPostCommitStage,
        source: EngineError,
    },
}

impl From<ClaimExecutionError> for AsyncClaimError {
    fn from(error: ClaimExecutionError) -> Self {
        match error {
            ClaimExecutionError::BeforeCommit(error) => Self::BeforeCommit(error),
            ClaimExecutionError::Commit(error) => Self::Commit(error),
            ClaimExecutionError::AfterCommit { stage, source } => {
                Self::AfterCommit { stage, source }
            }
        }
    }
}

fn validate_claim_plan(
    requested: &ClaimRequest,
    queue: &QueueKey,
    commit: &RawCommitRequest,
    item_ids: &[ItemId],
    cohort_id: Option<&CohortId>,
) -> EngineResult<()> {
    let unique_ids: HashSet<_> = item_ids.iter().copied().collect();
    if commit.shard() != queue
        || commit.fault() != RawCommitFault::None
        || item_ids.is_empty()
        || item_ids.len() > requested.max_items
        || unique_ids.len() != item_ids.len()
        || cohort_id.is_some() != requested.compatibility.whole_cohort
        || (requested.compatibility.whole_cohort
            && (requested.compatibility.same_group_key
                || requested.compatibility.group_key.is_some()
                || requested.compatibility.group_batching.is_some()))
        || requested
            .expected_epoch
            .is_some_and(|epoch| epoch != commit.expected_epoch())
        || commit.commands().len() != 1
    {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    let envelope = &commit.commands()[0];
    if envelope.item_ids != item_ids
        || envelope.command_id.0.is_empty()
        || envelope.request_id.is_some()
        || envelope.request_fingerprint.is_some()
        || envelope.request_outcome.is_some()
        || envelope.checksum != CommandChecksum(0)
        || envelope.created_at != requested.now
    {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    let valid = match (&envelope.command, cohort_id) {
        (QueueCommand::Claim(command), None) => claim_command_matches(command, requested, item_ids),
        (QueueCommand::CohortClaim(command), Some(cohort_id)) => {
            cohort_claim_command_matches(command, requested, item_ids, cohort_id)
        }
        _ => false,
    };
    if !valid {
        return Err(EngineError::Invalid("invalid async claim plan"));
    }
    Ok(())
}

fn validate_claim_commit_outcome(
    queue: &QueueKey,
    expected_epoch: u64,
    outcome: &RawCommitOutcome,
) -> EngineResult<()> {
    let positions = outcome.positions();
    if !outcome.projection_applied()
        || positions.len() != 1
        || positions[0].queue != *queue
        || positions[0].backend_epoch != expected_epoch
    {
        return Err(EngineError::Storage(
            "invalid async claim commit outcome".to_string(),
        ));
    }
    Ok(())
}

fn validate_rendered_claim(
    requested: &ClaimRequest,
    item_ids: &[ItemId],
    items: &[ClaimedItem],
) -> EngineResult<()> {
    if items.len() != item_ids.len()
        || items.iter().zip(item_ids).any(|(item, expected_id)| {
            item.item_id != *expected_id
                || item.lease_token.as_ref() != Some(&requested.lease_token)
                || item.lease_expires_at != requested.lease_expires_at
        })
    {
        return Err(EngineError::Storage(
            "invalid async claim render".to_string(),
        ));
    }
    Ok(())
}

fn claim_command_matches(
    command: &ClaimCommand,
    requested: &ClaimRequest,
    item_ids: &[ItemId],
) -> bool {
    command.item_ids == item_ids
        && command.lease_token == requested.lease_token
        && command.lease_expires_at == requested.lease_expires_at
        && command.worker_id.as_ref() == Some(&requested.worker_id)
}

fn cohort_claim_command_matches(
    command: &CohortClaimCommand,
    requested: &ClaimRequest,
    item_ids: &[ItemId],
    cohort_id: &CohortId,
) -> bool {
    command.cohort_id == *cohort_id
        && command.item_ids == item_ids
        && command.lease_token == requested.lease_token
        && command.lease_expires_at == requested.lease_expires_at
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::future::{Future, poll_fn};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, Wake, Waker};

    use std::collections::BTreeMap;

    use pqueue_core::{
        ClientItemKey, EligibilityPolicy, ItemId, LeaseToken, Metadata, OrderingMode,
        PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition,
        QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp, WorkerId,
    };

    use super::*;
    use crate::{
        ClaimCommand, ClaimCompatibility, CommandChecksum, CommandEnvelope, CommandId,
        CommandPosition, OwnedTask, OwnedTaskFactory, QueueCommand, QueueKey, RawCommitOutcome,
        RequestOutcome, TaskOutcome, TaskOutcomeSender, UnifiedAtomicCommit,
        UnifiedAtomicCommitter, task_outcome_channel,
    };

    fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    trait ErasedTask: Send {
        fn poll_erased(&mut self, context: &mut Context<'_>) -> Poll<()>;
    }

    struct TypedTask<T> {
        task: OwnedTask<T>,
        outcome: Option<TaskOutcomeSender<T>>,
    }

    impl<T: Send + 'static> ErasedTask for TypedTask<T> {
        fn poll_erased(&mut self, context: &mut Context<'_>) -> Poll<()> {
            match self.task.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(value) => {
                    self.outcome
                        .take()
                        .expect("typed task outcome sender missing")
                        .send(value);
                    Poll::Ready(())
                }
            }
        }
    }

    struct DispatchState {
        closed: bool,
        capacity: usize,
        next_id: u64,
        accepted: usize,
        live: HashSet<u64>,
        ready: VecDeque<u64>,
        tasks: HashMap<u64, Box<dyn ErasedTask>>,
        drainers: Vec<TaskOutcomeSender<()>>,
    }

    struct DispatchInner {
        state: Mutex<DispatchState>,
    }

    #[derive(Clone)]
    struct ControlledDispatcher {
        inner: Arc<DispatchInner>,
    }

    struct TaskWake {
        id: u64,
        dispatcher: Weak<DispatchInner>,
    }

    impl Wake for TaskWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            if let Some(dispatcher) = self.dispatcher.upgrade() {
                let mut state = dispatcher.state.lock().unwrap();
                if state.live.contains(&self.id) {
                    state.ready.push_back(self.id);
                }
            }
        }
    }

    impl ControlledDispatcher {
        fn new(capacity: usize) -> Self {
            Self {
                inner: Arc::new(DispatchInner {
                    state: Mutex::new(DispatchState {
                        closed: false,
                        capacity,
                        next_id: 0,
                        accepted: 0,
                        live: HashSet::new(),
                        ready: VecDeque::new(),
                        tasks: HashMap::new(),
                        drainers: Vec::new(),
                    }),
                }),
            }
        }

        fn accepted(&self) -> usize {
            self.inner.state.lock().unwrap().accepted
        }

        fn drive_next(&self) -> bool {
            let (id, mut slot) = loop {
                let candidate = {
                    let mut state = self.inner.state.lock().unwrap();
                    state
                        .ready
                        .pop_front()
                        .and_then(|id| state.tasks.remove(&id).map(|slot| (id, slot)))
                };
                if let Some(candidate) = candidate {
                    break candidate;
                }
                if self.inner.state.lock().unwrap().ready.is_empty() {
                    return false;
                }
            };
            let waker = Waker::from(Arc::new(TaskWake {
                id,
                dispatcher: Arc::downgrade(&self.inner),
            }));
            match slot.poll_erased(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    self.inner.state.lock().unwrap().tasks.insert(id, slot);
                }
                Poll::Ready(()) => {
                    let drainers = {
                        let mut state = self.inner.state.lock().unwrap();
                        state.live.remove(&id);
                        state.accepted -= 1;
                        if state.closed && state.accepted == 0 {
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
            true
        }
    }

    impl OwnedTaskDispatcher for ControlledDispatcher {
        fn submit<T: Send + 'static>(
            &self,
            factory: OwnedTaskFactory<T>,
        ) -> Result<TaskOutcome<T>, DispatchError> {
            let id = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed {
                    return Err(DispatchError::Closed);
                }
                if state.accepted >= state.capacity {
                    return Err(DispatchError::AtCapacity);
                }
                let id = state.next_id;
                state.next_id = state.next_id.wrapping_add(1);
                state.accepted += 1;
                state.live.insert(id);
                id
            };
            let task = factory();
            let (outcome_sender, outcome) = task_outcome_channel();
            let mut state = self.inner.state.lock().unwrap();
            state.tasks.insert(
                id,
                Box::new(TypedTask {
                    task,
                    outcome: Some(outcome_sender),
                }),
            );
            state.ready.push_back(id);
            Ok(outcome)
        }

        fn close(&self) {
            let drainers = {
                let mut state = self.inner.state.lock().unwrap();
                state.closed = true;
                if state.accepted == 0 {
                    std::mem::take(&mut state.drainers)
                } else {
                    Vec::new()
                }
            };
            for drainer in drainers {
                drainer.send(());
            }
        }

        fn is_closed(&self) -> bool {
            self.inner.state.lock().unwrap().closed
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome) = task_outcome_channel();
            let immediate = {
                let mut state = self.inner.state.lock().unwrap();
                if state.closed && state.accepted == 0 {
                    Some(sender)
                } else {
                    state.drainers.push(sender);
                    None
                }
            };
            if let Some(sender) = immediate {
                sender.send(());
            }
            outcome
        }
    }

    struct Phase {
        started: AtomicBool,
        released: AtomicBool,
        waker: Mutex<Option<Waker>>,
    }

    impl Phase {
        fn new(released: bool) -> Arc<Self> {
            Arc::new(Self {
                started: AtomicBool::new(false),
                released: AtomicBool::new(released),
                waker: Mutex::new(None),
            })
        }

        fn release(&self) {
            self.released.store(true, Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    #[derive(Clone)]
    struct ControlledCommitter {
        constructed: Arc<AtomicUsize>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
    }

    impl UnifiedAtomicCommitter for ControlledCommitter {
        type Request = RawCommitRequest;
        type Output = QueueKey;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            self.constructed.fetch_add(1, Ordering::AcqRel);
            let key = request.shard().clone();
            let phase = Arc::clone(self.phases.lock().unwrap().get(&key).unwrap());
            let completed = Arc::clone(&self.completed);
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    completed.lock().unwrap().push(key.clone());
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    fn queue(name: &str) -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new(name).unwrap(),
        )
    }

    fn request(name: &str) -> RawCommitRequest {
        RawCommitRequest::new(queue(name), Vec::new(), 1)
    }

    fn claim_request(name: &str) -> ClaimRequest {
        ClaimRequest {
            shard: queue(name),
            worker_id: WorkerId::new("worker").unwrap(),
            max_items: 1,
            lease_token: LeaseToken::new(format!("lease-{name}")).unwrap(),
            lease_expires_at: UtcTimestamp::new(20, 0).unwrap(),
            now: UtcTimestamp::new(10, 0).unwrap(),
            eligibility_time: None,
            compatibility: ClaimCompatibility::default(),
            expected_epoch: Some(1),
        }
    }

    fn definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            tenant_id: TenantId::new("tenant").unwrap(),
            queue_id: QueueId::new(name).unwrap(),
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
            max_eligible_group_size: Some(10),
            secondary_indexes: Vec::new(),
            entity_schema: None,
            typed_indexes: Vec::new(),
            emit_change_records: false,
        }
    }

    fn claim_envelope(request: &ClaimRequest, item_id: ItemId) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::new(format!("claim-{item_id}")),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None::<RequestOutcome>,
            item_ids: vec![item_id],
            command: QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token: request.lease_token.clone(),
                lease_expires_at: request.lease_expires_at,
                worker_id: Some(request.worker_id.clone()),
            }),
            checksum: CommandChecksum(0),
            created_at: request.now,
        }
    }

    fn claimed_item(request: &ClaimRequest, item_id: ItemId) -> ClaimedItem {
        ClaimedItem {
            item_id,
            client_item_key: ClientItemKey::new(format!("key-{item_id}")).unwrap(),
            item_version: 1,
            priority: None,
            group_key: None,
            not_before: None,
            lease_token: Some(request.lease_token.clone()),
            lease_expires_at: request.lease_expires_at,
            attempt_count: 1,
            payload: None,
            fields: BTreeMap::new(),
            metadata: Metadata::default(),
            gate_keys: Vec::new(),
        }
    }

    type TestBackend =
        AsyncComposedBackend<UnifiedAtomicCommit<ControlledCommitter>, ControlledDispatcher>;

    struct Fixture {
        backend: TestBackend,
        dispatcher: ControlledDispatcher,
        constructed: Arc<AtomicUsize>,
        completed: Arc<Mutex<Vec<QueueKey>>>,
        phases: Arc<Mutex<HashMap<QueueKey, Arc<Phase>>>>,
    }

    fn fixture(capacity: usize) -> Fixture {
        let constructed = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(Mutex::new(Vec::new()));
        let phases = Arc::new(Mutex::new(HashMap::new()));
        let committer = ControlledCommitter {
            constructed: Arc::clone(&constructed),
            phases: Arc::clone(&phases),
            completed: Arc::clone(&completed),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        Fixture {
            backend: AsyncComposedBackend::new(strategy, dispatcher.clone(), 8),
            dispatcher,
            constructed,
            completed,
            phases,
        }
    }

    #[derive(Clone)]
    struct ClaimCommitter {
        calls: Arc<AtomicUsize>,
        completed: Arc<AtomicBool>,
        phase: Arc<Phase>,
    }

    impl UnifiedAtomicCommitter for ClaimCommitter {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit_atomic(&self, request: Self::Request) -> OwnedTask<Self::Output> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let completed = Arc::clone(&self.completed);
            let phase = Arc::clone(&self.phase);
            let queue = request.shard().clone();
            let epoch = request.expected_epoch();
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if !phase.released.load(Ordering::Acquire) {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    return Poll::Pending;
                }
                completed.store(true, Ordering::Release);
                Poll::Ready(Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                    queue.clone(),
                    epoch,
                    1,
                )])))
            }))
        }
    }

    #[derive(Clone)]
    struct ControlledClaimPlanner {
        item_id: ItemId,
        plan_calls: Arc<AtomicUsize>,
        render_calls: Arc<AtomicUsize>,
        commit_completed: Arc<AtomicBool>,
        render_phase: Arc<Phase>,
    }

    impl AsyncClaimPlanner for ControlledClaimPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            self.plan_calls.fetch_add(1, Ordering::AcqRel);
            let item_id = self.item_id;
            Box::pin(async move {
                let envelope = claim_envelope(&request, item_id);
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(request.shard.clone(), vec![envelope], 1),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            shard: QueueKey,
            item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            assert!(
                self.commit_completed.load(Ordering::Acquire),
                "claim rendering must follow the strategy commit"
            );
            self.render_calls.fetch_add(1, Ordering::AcqRel);
            let phase = Arc::clone(&self.render_phase);
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if !phase.released.load(Ordering::Acquire) {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    return Poll::Pending;
                }
                let request = claim_request(shard.queue_id.as_str());
                Poll::Ready(Ok(item_ids
                    .iter()
                    .copied()
                    .map(|id| claimed_item(&request, id))
                    .collect()))
            }))
        }
    }

    type ClaimBackend = AsyncComposedBackend<
        UnifiedAtomicCommit<ClaimCommitter>,
        ControlledDispatcher,
        ControlledClaimPlanner,
    >;

    struct ClaimFixture {
        backend: ClaimBackend,
        dispatcher: ControlledDispatcher,
        phase: Arc<Phase>,
        render_phase: Arc<Phase>,
        commit_calls: Arc<AtomicUsize>,
        commit_completed: Arc<AtomicBool>,
        plan_calls: Arc<AtomicUsize>,
        render_calls: Arc<AtomicUsize>,
        item_id: ItemId,
    }

    fn claim_fixture(capacity: usize, released: bool, render_released: bool) -> ClaimFixture {
        let phase = Phase::new(released);
        let render_phase = Phase::new(render_released);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let commit_completed = Arc::new(AtomicBool::new(false));
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let render_calls = Arc::new(AtomicUsize::new(0));
        let item_id = ItemId::mint(1, 1, 1);
        let committer = ClaimCommitter {
            calls: Arc::clone(&commit_calls),
            completed: Arc::clone(&commit_completed),
            phase: Arc::clone(&phase),
        };
        let planner = ControlledClaimPlanner {
            item_id,
            plan_calls: Arc::clone(&plan_calls),
            render_calls: Arc::clone(&render_calls),
            commit_completed: Arc::clone(&commit_completed),
            render_phase: Arc::clone(&render_phase),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(capacity);
        let backend =
            AsyncComposedBackend::new_with_claim_planner(strategy, dispatcher.clone(), planner, 8);
        ClaimFixture {
            backend,
            dispatcher,
            phase,
            render_phase,
            commit_calls,
            commit_completed,
            plan_calls,
            render_calls,
            item_id,
        }
    }

    fn planned_operation(
        key: QueueKey,
        phase: Arc<Phase>,
        planned: Arc<Mutex<Vec<QueueKey>>>,
        active_planners: Arc<AtomicUsize>,
        max_active_planners: Arc<AtomicUsize>,
        finished: Arc<AtomicUsize>,
    ) -> impl FnOnce() -> OwnedTask<QueueKey> + Send + 'static {
        move || {
            planned.lock().unwrap().push(key.clone());
            let active = active_planners.fetch_add(1, Ordering::AcqRel) + 1;
            max_active_planners.fetch_max(active, Ordering::AcqRel);
            let mut done = false;
            Box::pin(poll_fn(move |context| {
                phase.started.store(true, Ordering::Release);
                if phase.released.load(Ordering::Acquire) {
                    if !done {
                        done = true;
                        active_planners.fetch_sub(1, Ordering::AcqRel);
                        finished.fetch_add(1, Ordering::AcqRel);
                    }
                    Poll::Ready(key.clone())
                } else {
                    *phase.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }
    }

    #[test]
    fn same_queue_operation_planning_starts_only_after_predecessor_releases() {
        let fixture = fixture(2);
        let first_phase = Phase::new(false);
        let second_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut first = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&first_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut second = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                second_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().as_slice(), &[queue("q")]);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(first_phase.started.load(Ordering::Acquire));
        assert_eq!(planned.lock().unwrap().len(), 1);

        first_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(active.load(Ordering::Acquire), 1);
        assert_eq!(max_active.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn different_queue_operation_planning_progresses_while_first_is_pending() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        let ready_phase = Phase::new(true);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut stalled = Box::pin(fixture.backend.submit_operation(
            queue("a"),
            planned_operation(
                queue("a"),
                Arc::clone(&stalled_phase),
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));
        let mut unrelated = Box::pin(fixture.backend.submit_operation(
            queue("b"),
            planned_operation(
                queue("b"),
                ready_phase,
                Arc::clone(&planned),
                Arc::clone(&active),
                Arc::clone(&max_active),
                Arc::clone(&finished),
            ),
        ));

        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert_eq!(planned.lock().unwrap().len(), 2);
        assert_eq!(max_active.load(Ordering::Acquire), 2);
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(unrelated.as_mut()),
            Poll::Ready(Ok(key)) if key == queue("b")
        ));
        assert_eq!(finished.load(Ordering::Acquire), 1);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn one_backend_dispatches_commit_and_distinct_operation_response_types() {
        let fixture = fixture(2);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("commit"), Phase::new(true));
        let mut commit = Box::pin(fixture.backend.submit_commit(request("commit")));
        let mut rendered = Box::pin(
            fixture
                .backend
                .submit_operation::<String, _>(queue("render"), || {
                    Box::pin(async { "rendered-response".to_string() })
                }),
        );

        assert!(matches!(poll_once(commit.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(rendered.as_mut()), Poll::Pending));
        assert_eq!(fixture.dispatcher.accepted(), 2);
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(commit.as_mut()),
            Poll::Ready(Ok(key)) if key == queue("commit")
        ));
        assert!(matches!(
            poll_once(rendered.as_mut()),
            Poll::Ready(Ok(value)) if value == "rendered-response"
        ));
        assert_eq!(fixture.constructed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_claim_routes_through_strategy_before_rendering() {
        let fixture = claim_fixture(2, true, true);
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 0);
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 0);
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Ok(Claimed { items, .. }))
                if items.len() == 1 && items[0].item_id == fixture.item_id
        ));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        assert!(fixture.commit_completed.load(Ordering::Acquire));
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_claim_keeps_queue_permit_across_plan_commit_and_render() {
        let fixture = claim_fixture(2, false, false);
        let mut first = Box::pin(fixture.backend.claim(claim_request("claim")));
        let mut second = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 0);
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.phase.started.load(Ordering::Acquire));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);

        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.render_phase.started.load(Ordering::Acquire));
        assert!(matches!(poll_once(first.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);

        fixture.render_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(first.as_mut()), Poll::Ready(Ok(_))));
        assert!(matches!(poll_once(second.as_mut()), Poll::Pending));
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 1);
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.plan_calls.load(Ordering::Acquire), 2);
        assert!(matches!(poll_once(second.as_mut()), Poll::Ready(Ok(_))));
        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 2);
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 2);
    }

    #[test]
    fn cancelling_typed_claim_caller_does_not_cancel_accepted_commit() {
        let fixture = claim_fixture(1, false, true);
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.phase.started.load(Ordering::Acquire));
        drop(claim);
        fixture.phase.release();
        assert!(fixture.dispatcher.drive_next());

        assert_eq!(fixture.commit_calls.load(Ordering::Acquire), 1);
        assert!(fixture.commit_completed.load(Ordering::Acquire));
        assert_eq!(fixture.render_calls.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn one_claim_backend_dispatches_raw_commit_and_claim_response_types() {
        let fixture = claim_fixture(2, true, true);
        let mut raw = Box::pin(fixture.backend.submit_commit(request("raw")));
        let mut claim = Box::pin(fixture.backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(raw.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(raw.as_mut()), Poll::Ready(Ok(Ok(_)))));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Ready(Ok(_))));
    }

    #[derive(Clone)]
    struct InvalidClaimPlanner {
        item_id: ItemId,
    }

    impl AsyncClaimPlanner for InvalidClaimPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            let item_id = self.item_id;
            Box::pin(async move {
                let mut envelope = claim_envelope(&request, item_id);
                envelope.command = QueueCommand::ResumeQueue;
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(request.shard, vec![envelope], 1),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            panic!("an invalid plan must not render")
        }
    }

    #[test]
    fn typed_claim_rejects_non_claim_mutation_before_strategy() {
        let phase = Phase::new(true);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = ClaimCommitter {
            calls: Arc::clone(&commit_calls),
            completed: Arc::new(AtomicBool::new(false)),
            phase,
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            InvalidClaimPlanner {
                item_id: ItemId::mint(1, 1, 1),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(claim_request("claim")));

        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::BeforeCommit(EngineError::Invalid(
                "invalid async claim plan"
            ))))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn claim_plan_rejects_empty_duplicate_oversized_and_mismatched_cohort_candidates() {
        let request = claim_request("claim");
        let first = ItemId::mint(1, 1, 1);
        let second = ItemId::mint(1, 1, 2);
        let valid = |ids: Vec<ItemId>| {
            RawCommitRequest::new(
                request.shard.clone(),
                vec![claim_envelope(&request, ids[0])],
                1,
            )
        };

        let empty_commit = RawCommitRequest::new(request.shard.clone(), Vec::new(), 1);
        assert!(validate_claim_plan(&request, &request.shard, &empty_commit, &[], None).is_err());

        let mut duplicate_envelope = claim_envelope(&request, first);
        duplicate_envelope.item_ids = vec![first, first];
        if let QueueCommand::Claim(command) = &mut duplicate_envelope.command {
            command.item_ids = vec![first, first];
        }
        let duplicate_commit =
            RawCommitRequest::new(request.shard.clone(), vec![duplicate_envelope], 1);
        assert!(
            validate_claim_plan(
                &request,
                &request.shard,
                &duplicate_commit,
                &[first, first],
                None
            )
            .is_err()
        );

        let mut oversized_request = request.clone();
        oversized_request.max_items = 1;
        let mut oversized_envelope = claim_envelope(&oversized_request, first);
        oversized_envelope.item_ids = vec![first, second];
        if let QueueCommand::Claim(command) = &mut oversized_envelope.command {
            command.item_ids = vec![first, second];
        }
        let oversized_commit =
            RawCommitRequest::new(oversized_request.shard.clone(), vec![oversized_envelope], 1);
        assert!(
            validate_claim_plan(
                &oversized_request,
                &oversized_request.shard,
                &oversized_commit,
                &[first, second],
                None
            )
            .is_err()
        );

        let ordinary_commit = valid(vec![first]);
        assert!(
            validate_claim_plan(
                &request,
                &request.shard,
                &ordinary_commit,
                &[first],
                Some(&CohortId::new("cohort").unwrap())
            )
            .is_err()
        );

        let mut cohort_request = request.clone();
        cohort_request.compatibility.whole_cohort = true;
        let cohort_shape_mismatch = RawCommitRequest::new(
            cohort_request.shard.clone(),
            vec![claim_envelope(&cohort_request, first)],
            1,
        );
        assert!(
            validate_claim_plan(
                &cohort_request,
                &cohort_request.shard,
                &cohort_shape_mismatch,
                &[first],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn claim_plan_rejects_smuggled_envelope_metadata_time_and_checksum() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let mut envelope = claim_envelope(&request, item_id);
        envelope.request_fingerprint = Some(7);
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());

        let mut envelope = claim_envelope(&request, item_id);
        envelope.created_at = UtcTimestamp::new(11, 0).unwrap();
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());

        let mut envelope = claim_envelope(&request, item_id);
        envelope.checksum = CommandChecksum(9);
        let commit = RawCommitRequest::new(request.shard.clone(), vec![envelope], 1);
        assert!(validate_claim_plan(&request, &request.shard, &commit, &[item_id], None).is_err());
    }

    #[test]
    fn claim_commit_outcome_and_render_must_match_the_committed_footprint() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let position = CommandPosition::new(request.shard.clone(), 1, 1);
        assert!(
            validate_claim_commit_outcome(
                &request.shard,
                1,
                &RawCommitOutcome::appended(vec![position.clone()])
            )
            .is_err()
        );
        assert!(
            validate_claim_commit_outcome(
                &request.shard,
                2,
                &RawCommitOutcome::applied(vec![position])
            )
            .is_err()
        );

        let mut wrong = claimed_item(&request, item_id);
        wrong.lease_token = Some(LeaseToken::new("wrong").unwrap());
        assert!(validate_rendered_claim(&request, &[item_id], &[wrong]).is_err());
        assert!(validate_rendered_claim(&request, &[item_id], &[]).is_err());
    }

    #[derive(Clone)]
    struct FixedOutcomeCommitter {
        calls: Arc<AtomicUsize>,
        outcome: EngineResult<RawCommitOutcome>,
    }

    impl UnifiedAtomicCommitter for FixedOutcomeCommitter {
        type Request = RawCommitRequest;
        type Output = EngineResult<RawCommitOutcome>;

        fn commit_atomic(&self, _request: Self::Request) -> OwnedTask<Self::Output> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let outcome = self.outcome.clone();
            Box::pin(async move { outcome })
        }
    }

    #[derive(Clone)]
    struct FixedRenderPlanner {
        item_id: ItemId,
        rendered: EngineResult<Vec<ClaimedItem>>,
        render_calls: Arc<AtomicUsize>,
    }

    impl AsyncClaimPlanner for FixedRenderPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            let item_id = self.item_id;
            Box::pin(async move {
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(
                        request.shard.clone(),
                        vec![claim_envelope(&request, item_id)],
                        1,
                    ),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            self.render_calls.fetch_add(1, Ordering::AcqRel);
            let rendered = self.rendered.clone();
            Box::pin(async move { rendered })
        }
    }

    #[test]
    fn durable_commit_outcome_and_render_failures_are_phase_aware() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let render_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::appended(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Ok(vec![claimed_item(&request, item_id)]),
                render_calls: Arc::clone(&render_calls),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request.clone()));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::CommitOutcome,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
        assert_eq!(render_calls.load(Ordering::Acquire), 0);

        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let mut wrong_item = claimed_item(&request, item_id);
        wrong_item.lease_expires_at = UtcTimestamp::new(99, 0).unwrap();
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Ok(vec![wrong_item]),
                render_calls: Arc::new(AtomicUsize::new(0)),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::RenderValidation,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn render_storage_error_is_reported_after_commit() {
        let request = claim_request("claim");
        let item_id = ItemId::mint(1, 1, 1);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            FixedRenderPlanner {
                item_id,
                rendered: Err(EngineError::Storage("render unavailable".to_string())),
                render_calls: Arc::new(AtomicUsize::new(0)),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::AfterCommit {
                stage: AsyncClaimPostCommitStage::Render,
                ..
            }))
        ));
        assert_eq!(commit_calls.load(Ordering::Acquire), 1);
    }

    #[derive(Clone)]
    struct CompatibilityBypassPlanner {
        empty: bool,
        plan_calls: Arc<AtomicUsize>,
        item_id: ItemId,
    }

    impl AsyncClaimPlanner for CompatibilityBypassPlanner {
        fn queue_definition(&self, shard: QueueKey) -> OwnedTask<EngineResult<QueueDefinition>> {
            Box::pin(async move { Ok(definition(shard.queue_id.as_str())) })
        }

        fn plan_claim(
            &self,
            request: ClaimRequest,
            _unit: ClaimUnit,
        ) -> OwnedTask<EngineResult<AsyncClaimPlan>> {
            self.plan_calls.fetch_add(1, Ordering::AcqRel);
            let empty = self.empty;
            let item_id = self.item_id;
            Box::pin(async move {
                if empty {
                    return Ok(AsyncClaimPlan::empty());
                }
                Ok(AsyncClaimPlan::commit(
                    RawCommitRequest::new(
                        request.shard.clone(),
                        vec![claim_envelope(&request, item_id)],
                        1,
                    ),
                    vec![item_id],
                    None,
                ))
            })
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _item_ids: Vec<ItemId>,
        ) -> OwnedTask<EngineResult<Vec<ClaimedItem>>> {
            panic!("invalid compatibility must fail before render")
        }
    }

    fn assert_invalid_compatibility_precedes_plan(compatibility: ClaimCompatibility, empty: bool) {
        let request = ClaimRequest {
            compatibility,
            ..claim_request("claim")
        };
        let plan_calls = Arc::new(AtomicUsize::new(0));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let committer = FixedOutcomeCommitter {
            calls: Arc::clone(&commit_calls),
            outcome: Ok(RawCommitOutcome::applied(vec![CommandPosition::new(
                request.shard.clone(),
                1,
                1,
            )])),
        };
        let strategy =
            UnifiedAtomicCommit::for_profile(DurabilityClass::Atomic, committer).unwrap();
        let dispatcher = ControlledDispatcher::new(1);
        let backend = AsyncComposedBackend::new_with_claim_planner(
            strategy,
            dispatcher.clone(),
            CompatibilityBypassPlanner {
                empty,
                plan_calls: Arc::clone(&plan_calls),
                item_id: ItemId::mint(1, 1, 1),
            },
            1,
        );
        let mut claim = Box::pin(backend.claim(request));
        assert!(matches!(poll_once(claim.as_mut()), Poll::Pending));
        assert!(dispatcher.drive_next());
        assert!(matches!(
            poll_once(claim.as_mut()),
            Poll::Ready(Err(AsyncClaimError::BeforeCommit(EngineError::Invalid(_))))
        ));
        assert_eq!(plan_calls.load(Ordering::Acquire), 0);
        assert_eq!(commit_calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn invalid_compatibility_cannot_return_empty_success() {
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                same_group_key: true,
                group_batching: Some(crate::GroupBatching { max_groups: 1 }),
                ..Default::default()
            },
            true,
        );
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                group_key: Some(pqueue_core::GroupKey::new("bad key!").unwrap()),
                ..Default::default()
            },
            true,
        );
    }

    #[test]
    fn invalid_compatibility_cannot_reach_commit_plan() {
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                group_batching: Some(crate::GroupBatching { max_groups: 0 }),
                ..Default::default()
            },
            false,
        );
        assert_invalid_compatibility_precedes_plan(
            ClaimCompatibility {
                same_group_key: true,
                group_key: Some(pqueue_core::GroupKey::new("group").unwrap()),
                whole_cohort: true,
                ..Default::default()
            },
            false,
        );
    }

    #[test]
    fn rejected_and_closed_operation_submission_never_invokes_planner() {
        let rejected = fixture(0);
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        let mut full = Box::pin(rejected.backend.submit_operation(queue("q"), move || {
            rejected_effects.fetch_add(1, Ordering::AcqRel);
            Box::pin(async { queue("q") })
        }));
        assert!(matches!(
            poll_once(full.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));

        let closed = fixture(1);
        closed.dispatcher.close();
        let closed_effects = Arc::clone(&effects);
        let mut closed_submission =
            Box::pin(closed.backend.submit_operation(queue("q"), move || {
                closed_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("q") })
            }));
        assert!(matches!(
            poll_once(closed_submission.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(DispatchError::Closed)))
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        assert_eq!(rejected.backend.admission.entry_count(), 0);
        assert_eq!(closed.backend.admission.entry_count(), 0);
    }

    #[test]
    fn caller_cancellation_after_operation_acceptance_does_not_cancel_work() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        let planned = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut caller = Box::pin(fixture.backend.submit_operation(
            queue("q"),
            planned_operation(
                queue("q"),
                Arc::clone(&phase),
                planned,
                active,
                max_active,
                Arc::clone(&finished),
            ),
        ));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(finished.load(Ordering::Acquire), 1);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn rejected_lazy_factory_has_no_construction_effects() {
        let fixture = fixture(0);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Phase::new(true));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(
            poll_once(caller.as_mut()),
            Poll::Ready(Err(AsyncCommitSubmitError::Dispatch(
                DispatchError::AtCapacity
            )))
        ));
        assert_eq!(fixture.constructed.load(Ordering::Acquire), 0);
        assert_eq!(fixture.backend.admission.entry_count(), 0);
    }

    #[test]
    fn started_caller_cancellation_does_not_cancel_accepted_task() {
        let fixture = fixture(1);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(phase.started.load(Ordering::Acquire));
        drop(caller);
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("q")]);
    }

    #[test]
    fn stalled_queue_does_not_block_actual_unrelated_queue_progress() {
        let fixture = fixture(2);
        let stalled_phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("a"), Arc::clone(&stalled_phase));
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("b"), Phase::new(true));
        let mut stalled = Box::pin(fixture.backend.submit_commit(request("a")));
        let mut unrelated = Box::pin(fixture.backend.submit_commit(request("b")));
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Pending));
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        assert!(stalled_phase.started.load(Ordering::Acquire));
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(unrelated.as_mut()), Poll::Ready(Ok(key)) if key == queue("b")));
        assert_eq!(fixture.completed.lock().unwrap().as_slice(), &[queue("b")]);
        stalled_phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(stalled.as_mut()), Poll::Ready(Ok(_))));
    }

    #[test]
    fn submit_close_is_linearizable_and_drain_waits_for_pending_task() {
        let fixture = fixture(2);
        let phase = Phase::new(false);
        fixture
            .phases
            .lock()
            .unwrap()
            .insert(queue("q"), Arc::clone(&phase));
        let mut caller = Box::pin(fixture.backend.submit_commit(request("q")));
        assert!(matches!(poll_once(caller.as_mut()), Poll::Pending));
        assert!(fixture.dispatcher.drive_next());
        fixture.backend.close();
        let effects = Arc::new(AtomicUsize::new(0));
        let rejected_effects = Arc::clone(&effects);
        assert!(matches!(
            fixture.dispatcher.submit(Box::new(move || {
                rejected_effects.fetch_add(1, Ordering::AcqRel);
                Box::pin(async { queue("never") })
            })),
            Err(DispatchError::Closed)
        ));
        assert_eq!(effects.load(Ordering::Acquire), 0);
        let mut drain = Box::pin(fixture.backend.drain());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Pending));
        phase.release();
        assert!(fixture.dispatcher.drive_next());
        assert!(matches!(poll_once(drain.as_mut()), Poll::Ready(Ok(()))));
        assert_eq!(fixture.dispatcher.accepted(), 0);
    }
}
