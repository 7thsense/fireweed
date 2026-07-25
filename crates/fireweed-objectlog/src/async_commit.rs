//! Native-async object-log then derived-projection commit capability.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures::channel::oneshot;

use fireweed_engine::{
    AsyncLogStore, AsyncProjectionStore, BufferedByteBudget, BufferedByteBudgetConfig,
    BufferedByteBudgetStats, ByteAdmissionError, EngineError, EngineResult, KeyedQueueGate,
    OwnedBytePermit, OwnedTask, QueueKey, RawCommitFault, RawCommitOutcome, RawCommitRequest,
    SeparateReplayCommitter, retained_records_plus_frame_bytes,
};

use crate::AsyncObjectLog;
use crate::segmented::SerializedCommandEnvelope;

/// Owns the two eventual-apply axes used by [`fireweed_engine::SeparateReplayCommit`].
///
/// The object log is the durable and fencing authority. A successful commit is returned only after the
/// projection has applied the exact positions minted by append, which is the ADR-013 response barrier.
pub struct ObjectLogProjectionCommitter<P> {
    log: AsyncObjectLog,
    projection: Arc<P>,
    gate: KeyedQueueGate<QueueKey>,
    recovery_page_size: usize,
}

impl<P> Clone for ObjectLogProjectionCommitter<P> {
    fn clone(&self) -> Self {
        Self {
            log: self.log.clone(),
            projection: Arc::clone(&self.projection),
            gate: self.gate.clone(),
            recovery_page_size: self.recovery_page_size,
        }
    }
}

pub const MAX_RECOVERY_PAGE_SIZE: usize = 4096;

impl<P> ObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub async fn open(
        log: AsyncObjectLog,
        projection: P,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        Self::open_shared(
            log,
            Arc::new(projection),
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await
    }

    pub async fn open_shared(
        log: AsyncObjectLog,
        projection: Arc<P>,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        validate_page_size(recovery_page_size)?;
        if max_queued_commits == 0 {
            return Err(EngineError::Invalid(
                "separate replay commit queue capacity must be positive",
            ));
        }
        let committer = Self {
            log,
            projection,
            gate: KeyedQueueGate::new(max_queued_commits),
            recovery_page_size,
        };
        for definition in definitions {
            let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            committer.log.ensure_shard(shard.clone()).await?;
            committer.projection.ensure_shard(definition).await?;
            committer.recover_projection(shard).await?;
        }
        Ok(committer)
    }

    /// Convenience for migrations where a surviving projection is still available.
    /// Production composition must prefer [`Self::open`] with control-plane-owned definitions because a
    /// projection is a disposable cache and may be empty or lost.
    pub async fn open_from_surviving_projection(
        log: AsyncObjectLog,
        projection: P,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        let definitions = projection.recover_definitions().await?;
        Self::open(
            log,
            projection,
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await
    }

    pub fn log(&self) -> &AsyncObjectLog {
        &self.log
    }

    pub fn projection(&self) -> &Arc<P> {
        &self.projection
    }

    /// Replay every durable command after the projection's persisted frontier.
    ///
    /// This is the repair path for an append whose live apply failed or whose response was lost after the
    /// append-only fault boundary. Recovery apply, rather than live apply, intentionally accepts historical
    /// epochs while preserving ordered, page-at-a-time frontier advancement.
    pub async fn recover_projection(&self, shard: QueueKey) -> EngineResult<()> {
        let _permit = self
            .gate
            .acquire(shard.clone())
            .await
            .map_err(|_| EngineError::Unavailable)?;
        repair_tail(
            &self.log,
            self.projection.as_ref(),
            shard,
            self.recovery_page_size,
        )
        .await
    }
}

impl<P> SeparateReplayCommitter for ObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
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
        let log = self.log.clone();
        let projection = Arc::clone(&self.projection);
        let gate = self.gate.clone();
        let page_size = self.recovery_page_size;
        Box::pin(async move {
            let shard = request.shard().clone();
            let commands = request.commands().to_vec();
            let expected_epoch = request.expected_epoch();
            match request.fault() {
                RawCommitFault::BeforeAppend => {
                    return Err(EngineError::Invalid("fault-injection: kill before append"));
                }
                RawCommitFault::None | RawCommitFault::AfterAppendBeforeApply => {}
            }

            let _permit = gate
                .acquire(shard.clone())
                .await
                .map_err(|_| EngineError::Unavailable)?;
            repair_tail(&log, projection.as_ref(), shard.clone(), page_size).await?;

            let positions = log
                .append(shard.clone(), commands.clone(), expected_epoch)
                .await?;
            validate_append_footprint(&shard, &positions, commands.len(), Some(expected_epoch))?;
            if request.fault() == RawCommitFault::AfterAppendBeforeApply {
                return Ok(RawCommitOutcome::appended(positions));
            }

            projection.apply_live(positions.clone(), commands).await?;
            Ok(RawCommitOutcome::applied(positions))
        })
    }
}

/// Explicit group-commit variant. It never probes mode or calls ordinary append.
pub struct GroupCommitObjectLogProjectionCommitter<P> {
    coordinator: Arc<GroupCommitCoordinator<P>>,
}

impl<P> Clone for GroupCommitObjectLogProjectionCommitter<P> {
    fn clone(&self) -> Self {
        Self {
            coordinator: Arc::clone(&self.coordinator),
        }
    }
}

/// Runtime-neutral exhaustion behavior. `Reject` is the finite production default; `Wait` preserves
/// cancellation-safe async queuing for services that race the future with their own runtime deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteAdmissionWaitPolicy {
    Reject,
    Wait,
}

#[derive(Clone)]
pub struct ObjectLogByteAdmissionConfig {
    budget: BufferedByteBudget,
    max_queue_waiting_bytes: usize,
    wait_policy: ByteAdmissionWaitPolicy,
}

/// Low-cardinality operator snapshot. It intentionally contains no tenant, queue, request, or object IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLogByteAdmissionSnapshot {
    pub configured_global_bytes: usize,
    pub configured_tenant_bytes: Option<usize>,
    pub configured_queue_waiting_bytes: usize,
    pub current_bytes: usize,
    pub peak_bytes: usize,
    pub waiters: usize,
    pub waits: u64,
    pub rejects: u64,
    pub total_wait_nanos: u128,
    pub max_wait_nanos: u128,
}

/// A raw object-log commit whose canonical bytes and finite-cap permit are already owned. This is the
/// transfer object used to cross a queue gate or dispatcher boundary without reserialization or recharging.
pub struct PreparedObjectLogCommit {
    shard: QueueKey,
    expected_epoch: u64,
    fault: RawCommitFault,
    serialized: Vec<SerializedCommandEnvelope>,
    charged_bytes: usize,
    permit: OwnedBytePermit,
}

impl ObjectLogByteAdmissionConfig {
    pub fn new(
        budget: BufferedByteBudget,
        max_queue_waiting_bytes: usize,
        wait_policy: ByteAdmissionWaitPolicy,
    ) -> Self {
        Self {
            budget,
            max_queue_waiting_bytes,
            wait_policy,
        }
    }
}

struct CoordinatedRequest {
    id: u64,
    command_count: usize,
    serialized: Vec<SerializedCommandEnvelope>,
    charged_bytes: usize,
    permit: Option<OwnedBytePermit>,
    expected_epoch: u64,
    fault: RawCommitFault,
    response: oneshot::Sender<EngineResult<RawCommitOutcome>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FlushPhase {
    PreRepair,
    SealPending,
    ActorSubmitted,
    ApplyPending,
}

struct InFlightBatch {
    requests: Vec<CoordinatedRequest>,
    phase: FlushPhase,
    positions: Option<Vec<Vec<fireweed_engine::CommandPosition>>>,
}

#[derive(Default)]
struct CoordinatedQueue {
    pending: VecDeque<CoordinatedRequest>,
    driver: Option<u64>,
    in_flight: Option<InFlightBatch>,
    outstanding: usize,
    pending_bytes: usize,
    driver_wakers: Vec<Waker>,
}

struct CoordinatorState {
    closed: bool,
    next_request_id: u64,
    outstanding: usize,
    queues: HashMap<QueueKey, CoordinatedQueue>,
    drain_wakers: Vec<Waker>,
}

struct CoordinatedTaskGuard<P: AsyncProjectionStore + 'static> {
    coordinator: Arc<GroupCommitCoordinator<P>>,
    shard: QueueKey,
    request_id: u64,
    completed: bool,
}

impl<P: AsyncProjectionStore + 'static> Drop for CoordinatedTaskGuard<P> {
    fn drop(&mut self) {
        if !self.completed {
            self.coordinator.abandon(&self.shard, self.request_id);
        }
    }
}

struct GroupCommitCoordinator<P> {
    inner: ObjectLogProjectionCommitter<P>,
    max_outstanding: usize,
    byte_budget: BufferedByteBudget,
    max_queue_waiting_bytes: usize,
    wait_policy: ByteAdmissionWaitPolicy,
    state: Mutex<CoordinatorState>,
    #[cfg(test)]
    phase_hook: Mutex<Option<PhaseHook>>,
}

#[cfg(test)]
struct PhaseHook {
    phase: FlushPhase,
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct CoordinatorDrain<P> {
    coordinator: Arc<GroupCommitCoordinator<P>>,
}

impl<P> Future for CoordinatorDrain<P> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self
            .coordinator
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        if state.outstanding == 0 {
            Poll::Ready(())
        } else {
            if !state
                .drain_wakers
                .iter()
                .any(|registered| registered.will_wake(context.waker()))
            {
                state.drain_wakers.push(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

impl<P> GroupCommitObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    pub async fn open(
        log: AsyncObjectLog,
        projection: P,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        Self::open_shared(
            log,
            Arc::new(projection),
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await
    }

    /// Open over a projection shared with server read/planning adapters.
    pub async fn open_shared(
        log: AsyncObjectLog,
        projection: Arc<P>,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
    ) -> EngineResult<Self> {
        let budget = BufferedByteBudget::new(
            BufferedByteBudgetConfig::new(64 * 1024 * 1024).expect("constant byte budget is valid"),
        );
        Self::open_shared_with_byte_admission(
            log,
            projection,
            definitions,
            recovery_page_size,
            max_queued_commits,
            ObjectLogByteAdmissionConfig::new(
                budget,
                16 * 1024 * 1024,
                ByteAdmissionWaitPolicy::Reject,
            ),
        )
        .await
    }

    /// Open with a node-shared byte budget and a per-queue cap for admitted requests waiting to drive.
    pub async fn open_with_byte_admission(
        log: AsyncObjectLog,
        projection: P,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
        admission: ObjectLogByteAdmissionConfig,
    ) -> EngineResult<Self> {
        Self::open_shared_with_byte_admission(
            log,
            Arc::new(projection),
            definitions,
            recovery_page_size,
            max_queued_commits,
            admission,
        )
        .await
    }

    pub async fn open_shared_with_byte_admission(
        log: AsyncObjectLog,
        projection: Arc<P>,
        definitions: Vec<fireweed_core::QueueDefinition>,
        recovery_page_size: usize,
        max_queued_commits: usize,
        admission: ObjectLogByteAdmissionConfig,
    ) -> EngineResult<Self> {
        let ObjectLogByteAdmissionConfig {
            budget: byte_budget,
            max_queue_waiting_bytes,
            wait_policy,
        } = admission;
        if max_queue_waiting_bytes == 0
            || max_queue_waiting_bytes > byte_budget.config().global_limit()
        {
            return Err(EngineError::Invalid(
                "queue waiting-byte limit must be positive and no larger than global byte limit",
            ));
        }
        if byte_budget.config().global_limit() < log.segment_target_bytes() {
            return Err(EngineError::Invalid(
                "global buffered-byte limit must be at least segment_target_bytes",
            ));
        }
        let inner = ObjectLogProjectionCommitter::open_shared(
            log,
            projection,
            definitions,
            recovery_page_size,
            max_queued_commits,
        )
        .await?;
        Ok(Self {
            coordinator: Arc::new(GroupCommitCoordinator {
                inner,
                max_outstanding: max_queued_commits,
                byte_budget,
                max_queue_waiting_bytes,
                wait_policy,
                state: Mutex::new(CoordinatorState {
                    closed: false,
                    next_request_id: 0,
                    outstanding: 0,
                    queues: HashMap::new(),
                    drain_wakers: Vec::new(),
                }),
                #[cfg(test)]
                phase_hook: Mutex::new(None),
            }),
        })
    }

    pub async fn recover_projection(&self, shard: QueueKey) -> EngineResult<()> {
        self.coordinator.inner.recover_projection(shard).await
    }

    /// Stop accepting new commit requests. Already accepted requests remain coordinator-owned.
    pub fn close(&self) {
        // Lifecycle order is deliberate: revoke byte admission first, then close queue admission. Accepted
        // permits remain charged until their coordinator-owned seal/apply work resolves.
        self.coordinator.byte_budget.close();
        self.coordinator
            .state
            .lock()
            .expect("group commit coordinator poisoned")
            .closed = true;
    }

    /// Stop admission and wait until every accepted request has resolved.
    pub async fn close_and_drain(&self) {
        self.close();
        CoordinatorDrain {
            coordinator: Arc::clone(&self.coordinator),
        }
        .await;
        debug_assert_eq!(
            self.coordinator.byte_budget.stats().charged_bytes,
            0,
            "object-log byte charge remained after accepted work drained"
        );
    }

    pub fn byte_admission_stats(&self) -> BufferedByteBudgetStats {
        self.coordinator.byte_budget.stats()
    }

    pub fn byte_admission_snapshot(&self) -> ObjectLogByteAdmissionSnapshot {
        let stats = self.coordinator.byte_budget.stats();
        ObjectLogByteAdmissionSnapshot {
            configured_global_bytes: self.coordinator.byte_budget.config().global_limit(),
            configured_tenant_bytes: self.coordinator.byte_budget.config().tenant_limit(),
            configured_queue_waiting_bytes: self.coordinator.max_queue_waiting_bytes,
            current_bytes: stats.charged_bytes,
            peak_bytes: stats.peak_charged_bytes,
            waiters: stats.waiting_requests,
            waits: stats.wait_count,
            rejects: stats.rejection_count,
            total_wait_nanos: stats.total_wait_nanos,
            max_wait_nanos: stats.max_wait_nanos,
        }
    }

    /// Serialize and use non-waiting admission now, before the caller enters a queue gate or dispatcher.
    /// This is intentionally the only preparation API suitable while an authoritative gate is held.
    pub fn prepare_reject(
        &self,
        request: RawCommitRequest,
    ) -> EngineResult<PreparedObjectLogCommit> {
        let (shard, expected_epoch, fault, serialized, charged_bytes) = prepare_serialized_request(
            request,
            self.coordinator.byte_budget.config().global_limit(),
        )?;
        let permit = self
            .coordinator
            .byte_budget
            .try_acquire(shard.tenant_id.clone(), charged_bytes)
            .map_err(map_byte_admission_error)?;
        Ok(PreparedObjectLogCommit {
            shard,
            expected_epoch,
            fault,
            serialized,
            charged_bytes,
            permit,
        })
    }

    /// Explicit pre-dispatch preparation using the configured wait policy. A caller selecting `Wait` MUST
    /// race this future with its service-owned deadline before submitting [`Self::commit_prepared`]; the
    /// direct [`SeparateReplayCommitter::commit_replayable`] fallback is always finite `Reject` after
    /// dispatcher acceptance. Generic composed submission invokes this configured preparation boundary.
    pub async fn prepare_configured(
        &self,
        request: RawCommitRequest,
    ) -> EngineResult<PreparedObjectLogCommit> {
        prepare_with_configured_policy(Arc::clone(&self.coordinator), request).await
    }

    /// Submit already-admitted work. Dispatcher ownership can begin with this task without any admission wait.
    pub fn commit_prepared(
        &self,
        prepared: PreparedObjectLogCommit,
    ) -> OwnedTask<EngineResult<RawCommitOutcome>> {
        run_prepared_commit(Arc::clone(&self.coordinator), prepared)
    }

    #[cfg(test)]
    fn gate_phase(&self, phase: FlushPhase) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (started_sender, started) = oneshot::channel();
        let (release, release_receiver) = oneshot::channel();
        *self
            .coordinator
            .phase_hook
            .lock()
            .expect("group commit phase hook poisoned") = Some(PhaseHook {
            phase,
            started: started_sender,
            release: release_receiver,
        });
        (started, release)
    }
}

impl<P> GroupCommitCoordinator<P>
where
    P: AsyncProjectionStore + 'static,
{
    #[cfg(test)]
    async fn await_phase_hook(&self, phase: FlushPhase) {
        let hook = {
            let mut hook = self
                .phase_hook
                .lock()
                .expect("group commit phase hook poisoned");
            if hook.as_ref().is_some_and(|hook| hook.phase == phase) {
                hook.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            let _ = hook.started.send(());
            let _ = hook.release.await;
        }
    }

    fn enqueue(
        self: &Arc<Self>,
        shard: QueueKey,
        expected_epoch: u64,
        fault: RawCommitFault,
        serialized: Vec<SerializedCommandEnvelope>,
        charged_bytes: usize,
        permit: OwnedBytePermit,
    ) -> EngineResult<(
        QueueKey,
        u64,
        oneshot::Receiver<EngineResult<RawCommitOutcome>>,
    )> {
        let command_count = serialized.len();
        let (response, receiver) = oneshot::channel();
        let mut state = self
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        if state.closed {
            return Err(EngineError::Unavailable);
        }
        let request_id = state.next_request_id;
        state.next_request_id = state.next_request_id.wrapping_add(1);
        if state.outstanding >= self.max_outstanding {
            return Err(EngineError::Unavailable);
        }
        if state.queues.get(&shard).is_some_and(|queue| {
            let must_park =
                queue.driver.is_some() || queue.in_flight.is_some() || !queue.pending.is_empty();
            must_park
                && queue.pending_bytes.saturating_add(charged_bytes) > self.max_queue_waiting_bytes
        }) {
            return Err(EngineError::Backpressure {
                resource: "queue buffered bytes",
            });
        }
        state.outstanding += 1;
        let queue = state.queues.entry(shard.clone()).or_default();
        queue.outstanding += 1;
        queue.pending_bytes += charged_bytes;
        queue.pending.push_back(CoordinatedRequest {
            id: request_id,
            command_count,
            serialized,
            charged_bytes,
            permit: Some(permit),
            expected_epoch,
            fault,
            response,
        });
        Ok((shard, request_id, receiver))
    }

    fn poll_driver(&self, shard: &QueueKey, request_id: u64, context: &mut Context<'_>) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        let queue = state
            .queues
            .get_mut(shard)
            .expect("accepted coordinated queue disappeared");
        if queue.driver.is_none() {
            queue.driver = Some(request_id);
            true
        } else {
            if !queue
                .driver_wakers
                .iter()
                .any(|registered| registered.will_wake(context.waker()))
            {
                queue.driver_wakers.push(context.waker().clone());
            }
            false
        }
    }

    fn abandon(&self, shard: &QueueKey, request_id: u64) {
        enum Resolution {
            Error(CoordinatedRequest, EngineError),
            Appended(CoordinatedRequest, Vec<fireweed_engine::CommandPosition>),
        }
        let (driver_wakers, drain_wakers, resolutions) = {
            let mut state = self
                .state
                .lock()
                .expect("group commit coordinator poisoned");
            let Some(queue) = state.queues.get_mut(shard) else {
                return;
            };
            let mut removed = 0;
            if let Some(index) = queue
                .pending
                .iter()
                .position(|request| request.id == request_id)
            {
                let request = queue
                    .pending
                    .remove(index)
                    .expect("pending request disappeared");
                queue.pending_bytes = queue
                    .pending_bytes
                    .checked_sub(request.charged_bytes)
                    .expect("group commit pending-byte accounting underflow");
                queue.outstanding -= 1;
                removed += 1;
            }
            let mut resolutions = Vec::new();
            let driver_wakers = if queue.driver == Some(request_id) {
                if let Some(in_flight) = queue.in_flight.take() {
                    match in_flight.phase {
                        FlushPhase::PreRepair => {
                            for request in in_flight.requests.into_iter().rev() {
                                if request.id == request_id {
                                    queue.outstanding -= 1;
                                    removed += 1;
                                } else {
                                    queue.pending_bytes += request.charged_bytes;
                                    queue.pending.push_front(request);
                                }
                            }
                        }
                        FlushPhase::SealPending | FlushPhase::ActorSubmitted => {
                            for request in in_flight.requests {
                                queue.outstanding -= 1;
                                removed += 1;
                                resolutions.push(Resolution::Error(
                                    request,
                                    EngineError::Storage(
                                        "group commit outcome unknown after seal cancellation; recovery required before retry"
                                            .to_string(),
                                    ),
                                ));
                            }
                        }
                        FlushPhase::ApplyPending => {
                            let positions = in_flight
                                .positions
                                .expect("apply-pending group batch missing positions");
                            for (request, positions) in
                                in_flight.requests.into_iter().zip(positions)
                            {
                                queue.outstanding -= 1;
                                removed += 1;
                                resolutions.push(Resolution::Appended(request, positions));
                            }
                        }
                    }
                }
                queue.driver = None;
                std::mem::take(&mut queue.driver_wakers)
            } else {
                Vec::new()
            };
            let reclaim = queue.outstanding == 0;
            state.outstanding -= removed;
            if reclaim {
                state.queues.remove(shard);
            }
            let drain_wakers = if state.closed && state.outstanding == 0 {
                std::mem::take(&mut state.drain_wakers)
            } else {
                Vec::new()
            };
            (driver_wakers, drain_wakers, resolutions)
        };
        for resolution in resolutions {
            match resolution {
                Resolution::Error(request, error) => {
                    let _ = request.response.send(Err(error));
                }
                Resolution::Appended(request, positions) => {
                    let _ = request
                        .response
                        .send(Ok(RawCommitOutcome::appended(positions)));
                }
            }
        }
        for waker in driver_wakers.into_iter().chain(drain_wakers) {
            waker.wake();
        }
    }

    async fn drive(self: Arc<Self>, shard: QueueKey) {
        // Give other already-runnable requests one executor turn to join this queue's first batch. This is
        // deliberately runtime-neutral: no clock, timer, or executor-specific spawn API is required.
        let mut yielded = false;
        futures::future::poll_fn(|context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;

        loop {
            let prepared = {
                let mut state = self
                    .state
                    .lock()
                    .expect("group commit coordinator poisoned");
                let Some(queue) = state.queues.get_mut(&shard) else {
                    return;
                };
                let Some(expected_epoch) =
                    queue.pending.front().map(|request| request.expected_epoch)
                else {
                    queue.driver = None;
                    let wakers = std::mem::take(&mut queue.driver_wakers);
                    if queue.outstanding == 0 {
                        state.queues.remove(&shard);
                    }
                    drop(state);
                    for waker in wakers {
                        waker.wake();
                    }
                    return;
                };
                let count = queue
                    .pending
                    .iter()
                    .take_while(|request| request.expected_epoch == expected_epoch)
                    .count();
                let requests = queue.pending.drain(..count).collect::<Vec<_>>();
                let drained_bytes = requests
                    .iter()
                    .map(|request| request.charged_bytes)
                    .sum::<usize>();
                queue.pending_bytes = queue
                    .pending_bytes
                    .checked_sub(drained_bytes)
                    .expect("group commit pending-byte accounting underflow");
                let needs_apply = requests
                    .iter()
                    .any(|request| request.fault != RawCommitFault::AfterAppendBeforeApply);
                queue.in_flight = Some(InFlightBatch {
                    requests,
                    phase: FlushPhase::PreRepair,
                    positions: None,
                });
                (expected_epoch, needs_apply)
            };

            let (expected_epoch, needs_apply) = prepared;
            #[cfg(test)]
            self.await_phase_hook(FlushPhase::PreRepair).await;
            if let Err(error) = repair_tail(
                &self.inner.log,
                self.inner.projection.as_ref(),
                shard.clone(),
                self.inner.recovery_page_size,
            )
            .await
            {
                self.finish_in_flight(&shard, Err(error));
                continue;
            }
            self.set_phase(&shard, FlushPhase::SealPending, None);
            let (serialized, permits) = self.take_in_flight_storage(&shard);
            #[cfg(test)]
            self.await_phase_hook(FlushPhase::SealPending).await;
            let command_count = serialized.len();
            let submitted = match self.inner.log.submit_group_commit_serialized_and_seal(
                shard.clone(),
                serialized,
                permits,
                expected_epoch,
                0,
            ) {
                Ok(submitted) => submitted,
                Err(error) => {
                    self.finish_in_flight(&shard, Err(error));
                    continue;
                }
            };
            self.set_phase(&shard, FlushPhase::ActorSubmitted, None);
            #[cfg(test)]
            self.await_phase_hook(FlushPhase::ActorSubmitted).await;
            let (sealed, permits) = match submitted.await {
                Ok(sealed) => sealed,
                Err(error) => {
                    self.finish_in_flight(&shard, Err(error));
                    continue;
                }
            };
            if sealed.len() < command_count {
                self.finish_in_flight(
                    &shard,
                    Err(EngineError::Storage(
                        "group commit seal omitted accepted commands".to_string(),
                    )),
                );
                continue;
            }
            if let Err(error) =
                validate_append_footprint(&shard, &sealed, sealed.len(), Some(expected_epoch))
            {
                self.finish_in_flight(&shard, Err(error));
                continue;
            }
            let own = &sealed[sealed.len() - command_count..];
            let positions = self.partition_positions(&shard, own);
            if needs_apply {
                self.set_phase(&shard, FlushPhase::ApplyPending, Some(positions));
                #[cfg(test)]
                self.await_phase_hook(FlushPhase::ApplyPending).await;
                let apply_result = repair_tail(
                    &self.inner.log,
                    self.inner.projection.as_ref(),
                    shard.clone(),
                    self.inner.recovery_page_size,
                )
                .await;
                self.finish_in_flight(&shard, apply_result);
            } else {
                self.set_phase(&shard, FlushPhase::ApplyPending, Some(positions));
                self.finish_in_flight(&shard, Ok(()));
            }
            // Durable segment/frame bytes and their ownership token remain charged through the complete
            // projection/apply response barrier, including repair failures and caller cancellation.
            drop(permits);
        }
    }

    fn set_phase(
        &self,
        shard: &QueueKey,
        phase: FlushPhase,
        positions: Option<Vec<Vec<fireweed_engine::CommandPosition>>>,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        let in_flight = state
            .queues
            .get_mut(shard)
            .and_then(|queue| queue.in_flight.as_mut())
            .expect("group commit in-flight batch disappeared");
        in_flight.phase = phase;
        if positions.is_some() {
            in_flight.positions = positions;
        }
    }

    fn take_in_flight_storage(
        &self,
        shard: &QueueKey,
    ) -> (Vec<SerializedCommandEnvelope>, Vec<OwnedBytePermit>) {
        let mut state = self
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        let requests = &mut state
            .queues
            .get_mut(shard)
            .and_then(|queue| queue.in_flight.as_mut())
            .expect("group commit in-flight batch disappeared")
            .requests;
        let serialized = requests
            .iter_mut()
            .flat_map(|request| std::mem::take(&mut request.serialized))
            .collect();
        let permits = requests
            .iter_mut()
            .map(|request| {
                request
                    .permit
                    .take()
                    .expect("group commit request permit already transferred")
            })
            .collect();
        (serialized, permits)
    }

    fn partition_positions(
        &self,
        shard: &QueueKey,
        own: &[fireweed_engine::CommandPosition],
    ) -> Vec<Vec<fireweed_engine::CommandPosition>> {
        let state = self
            .state
            .lock()
            .expect("group commit coordinator poisoned");
        let requests = &state
            .queues
            .get(shard)
            .and_then(|queue| queue.in_flight.as_ref())
            .expect("group commit in-flight batch disappeared")
            .requests;
        let mut offset = 0;
        requests
            .iter()
            .map(|request| {
                let end = offset + request.command_count;
                let positions = own[offset..end].to_vec();
                offset = end;
                positions
            })
            .collect()
    }

    fn finish_in_flight(&self, shard: &QueueKey, apply_result: EngineResult<()>) {
        let (requests, positions, drain_wakers) = {
            let mut state = self
                .state
                .lock()
                .expect("group commit coordinator poisoned");
            let queue = state
                .queues
                .get_mut(shard)
                .expect("group commit queue disappeared");
            let in_flight = queue
                .in_flight
                .take()
                .expect("group commit in-flight batch disappeared");
            let count = in_flight.requests.len();
            queue.outstanding -= count;
            state.outstanding -= count;
            let drain_wakers = if state.closed && state.outstanding == 0 {
                std::mem::take(&mut state.drain_wakers)
            } else {
                Vec::new()
            };
            (in_flight.requests, in_flight.positions, drain_wakers)
        };
        if let Some(positions) = positions {
            for (request, positions) in requests.into_iter().zip(positions) {
                let outcome = if request.fault == RawCommitFault::AfterAppendBeforeApply {
                    Ok(RawCommitOutcome::appended(positions))
                } else {
                    apply_result
                        .clone()
                        .map(|()| RawCommitOutcome::applied(positions))
                };
                let _ = request.response.send(outcome);
            }
        } else {
            debug_assert!(apply_result.is_err());
            for request in requests {
                let _ =
                    request.response.send(apply_result.clone().map(|()| {
                        unreachable!("pre-append recovery success must carry positions")
                    }));
            }
        }
        for waker in drain_wakers {
            waker.wake();
        }
    }
}

impl<P> SeparateReplayCommitter for GroupCommitObjectLogProjectionCommitter<P>
where
    P: AsyncProjectionStore + 'static,
{
    type Request = RawCommitRequest;
    type PreparedRequest = PreparedObjectLogCommit;
    type Output = EngineResult<RawCommitOutcome>;

    fn prepare_replayable(
        &self,
        request: Self::Request,
    ) -> OwnedTask<EngineResult<Self::PreparedRequest>> {
        let coordinator = Arc::clone(&self.coordinator);
        Box::pin(prepare_with_configured_policy(coordinator, request))
    }

    fn commit_prepared_replayable(
        &self,
        request: Self::PreparedRequest,
    ) -> OwnedTask<Self::Output> {
        run_prepared_commit(Arc::clone(&self.coordinator), request)
    }

    fn commit_replayable(&self, request: Self::Request) -> OwnedTask<Self::Output> {
        let coordinator = Arc::clone(&self.coordinator);
        Box::pin(async move {
            let (shard, expected_epoch, fault, serialized, charged_bytes) =
                prepare_serialized_request(
                    request,
                    coordinator.byte_budget.config().global_limit(),
                )?;
            // Dispatcher acceptance has already happened when this owned future runs. It must therefore be
            // finite: an ordinary trait caller can never park a dispatcher slot waiting for byte capacity.
            let permit = coordinator
                .byte_budget
                .try_acquire(shard.tenant_id.clone(), charged_bytes)
                .map_err(map_byte_admission_error)?;
            run_prepared_commit(
                coordinator,
                PreparedObjectLogCommit {
                    shard,
                    expected_epoch,
                    fault,
                    serialized,
                    charged_bytes,
                    permit,
                },
            )
            .await
        })
    }
}

async fn prepare_with_configured_policy<P>(
    coordinator: Arc<GroupCommitCoordinator<P>>,
    request: RawCommitRequest,
) -> EngineResult<PreparedObjectLogCommit> {
    let (shard, expected_epoch, fault, serialized, charged_bytes) = prepare_serialized_request(
        request,
        coordinator.byte_budget.config().global_limit(),
    )?;
    let tenant = shard.tenant_id.clone();
    let permit = match coordinator.wait_policy {
        ByteAdmissionWaitPolicy::Reject => {
            coordinator.byte_budget.try_acquire(tenant, charged_bytes)
        }
        ByteAdmissionWaitPolicy::Wait => {
            coordinator.byte_budget.acquire(tenant, charged_bytes).await
        }
    }
    .map_err(map_byte_admission_error)?;
    Ok(PreparedObjectLogCommit {
        shard,
        expected_epoch,
        fault,
        serialized,
        charged_bytes,
        permit,
    })
}

fn prepare_serialized_request(
    request: RawCommitRequest,
    global_limit: usize,
) -> EngineResult<(
    QueueKey,
    u64,
    RawCommitFault,
    Vec<SerializedCommandEnvelope>,
    usize,
)> {
    if request.fault() == RawCommitFault::BeforeAppend {
        return Err(EngineError::Invalid("fault-injection: kill before append"));
    }
    let (shard, commands, expected_epoch, fault) = request.into_parts();
    let serialized = commands
        .into_iter()
        .map(SerializedCommandEnvelope::new)
        .collect::<EngineResult<Vec<_>>>()?;
    let charged_bytes = serialized_peak_charge(&serialized, global_limit)?;
    Ok((shard, expected_epoch, fault, serialized, charged_bytes))
}

fn run_prepared_commit<P>(
    coordinator: Arc<GroupCommitCoordinator<P>>,
    prepared: PreparedObjectLogCommit,
) -> OwnedTask<EngineResult<RawCommitOutcome>>
where
    P: AsyncProjectionStore + 'static,
{
    Box::pin(async move {
        let (shard, request_id, mut response) = coordinator.enqueue(
            prepared.shard,
            prepared.expected_epoch,
            prepared.fault,
            prepared.serialized,
            prepared.charged_bytes,
            prepared.permit,
        )?;
        let mut guard = CoordinatedTaskGuard {
            coordinator: Arc::clone(&coordinator),
            shard: shard.clone(),
            request_id,
            completed: false,
        };
        loop {
            enum Turn<T> {
                Response(T),
                Drive,
            }
            let turn = futures::future::poll_fn(|context| {
                if let Poll::Ready(result) = Pin::new(&mut response).poll(context) {
                    return Poll::Ready(Turn::Response(result));
                }
                if coordinator.poll_driver(&shard, request_id, context) {
                    Poll::Ready(Turn::Drive)
                } else {
                    Poll::Pending
                }
            })
            .await;
            match turn {
                Turn::Response(result) => {
                    guard.completed = true;
                    return result.map_err(|_| {
                        EngineError::Storage(
                            "group commit coordinator dropped a response".to_string(),
                        )
                    })?;
                }
                Turn::Drive => {
                    Arc::clone(&coordinator).drive(shard.clone()).await;
                }
            }
        }
    })
}

fn map_byte_admission_error(error: ByteAdmissionError) -> EngineError {
    match error {
        ByteAdmissionError::Closed => EngineError::Unavailable,
        ByteAdmissionError::Backpressure => EngineError::Backpressure {
            resource: "buffered bytes",
        },
        ByteAdmissionError::Oversize {
            requested, limit, ..
        } => EngineError::RequestTooLarge { requested, limit },
    }
}

/// Canonically encode an owned command batch once and compute its conservative resident peak: retained
/// records plus a temporary sealed frame. Independently admitted batches each reserve the fixed frame
/// overhead, so later co-batching safely overcharges rather than attempting a racy merge-time adjustment.
pub fn prepare_serialized_commands(
    commands: Vec<fireweed_engine::CommandEnvelope>,
    limit: usize,
) -> EngineResult<(Vec<SerializedCommandEnvelope>, usize)> {
    let serialized = commands
        .into_iter()
        .map(SerializedCommandEnvelope::new)
        .collect::<EngineResult<Vec<_>>>()?;
    crate::segment_integrity::validate_write_lengths(
        serialized.iter().map(|record| record.record_len()),
    )?;
    let charged = serialized_peak_charge(&serialized, limit)?;
    Ok((serialized, charged))
}

pub fn serialized_peak_charge(
    records: &[SerializedCommandEnvelope],
    limit: usize,
) -> EngineResult<usize> {
    serialized_peak_charge_for_lengths(
        records.iter().map(|record| record.record.len()),
        limit,
    )
}

fn serialized_peak_charge_for_lengths(
    lengths: impl IntoIterator<Item = usize>,
    limit: usize,
) -> EngineResult<usize> {
    let overflow = || EngineError::RequestTooLarge {
        requested: usize::MAX,
        limit,
    };
    retained_records_plus_frame_bytes(lengths, 29, 8).ok_or_else(overflow)
}

fn validate_page_size(page_size: usize) -> EngineResult<()> {
    if page_size == 0 || page_size > MAX_RECOVERY_PAGE_SIZE || page_size.checked_add(1).is_none() {
        return Err(EngineError::Invalid("invalid recovery page size"));
    }
    Ok(())
}

async fn repair_tail<P: AsyncProjectionStore + ?Sized>(
    log: &AsyncObjectLog,
    projection: &P,
    shard: QueueKey,
    page_size: usize,
) -> EngineResult<()> {
    validate_page_size(page_size)?;
    let mut cursor = projection.recovery_high_water(shard.clone()).await?;
    loop {
        let page = log
            .read_from(shard.clone(), cursor.clone(), page_size)
            .await?;
        if page.entries.is_empty() {
            return Ok(());
        }
        let next = page.next;
        let (positions, commands): (Vec<_>, Vec<_>) = page.entries.into_iter().unzip();
        validate_append_footprint(&shard, &positions, positions.len(), None)?;
        let last = positions.last().cloned();
        projection.apply_recovery(positions, commands).await?;
        cursor = next.clone().or(last);
        if next.is_none() {
            return Ok(());
        }
    }
}

fn validate_append_footprint(
    shard: &QueueKey,
    positions: &[fireweed_engine::CommandPosition],
    expected_count: usize,
    expected_epoch: Option<u64>,
) -> EngineResult<()> {
    if positions.len() != expected_count
        || positions.iter().any(|position| {
            position.queue != *shard
                || expected_epoch.is_some_and(|epoch| position.backend_epoch != epoch)
        })
        || positions.windows(2).any(|pair| !pair[0].precedes(&pair[1]))
    {
        return Err(EngineError::Storage(
            "object log returned an invalid append footprint".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use fireweed_conformance::{envelope, item};
    use fireweed_core::{ItemId, ItemState, QueueDefinition, QueueId, TenantId, UtcTimestamp};
    use fireweed_engine::{
        AsyncCommitStrategy, AsyncComposedBackend, ClaimCompatibility, ClaimUnit, ClaimedItem,
        CommandEnvelope, CommandPosition, DispatchError, DurabilityClass, IdempotencyDecision,
        OwnedTaskDispatcher, OwnedTaskFactory, PushCommand, PushFingerprint, PushItem,
        QueueCommand, RawCommitFault, RichClaimSelection, SeparateReplayCommit, TaskOutcome,
        TaskOutcomeSender, task_outcome_channel,
    };
    use futures::channel::oneshot;

    use super::*;
    use crate::segmented::{InMemoryBlobStore, SegmentConfig};

    static NEXT_ROOT: AtomicUsize = AtomicUsize::new(0);

    fn root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "fireweed-async-replay-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn shard() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    fn command(id: &str) -> CommandEnvelope {
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(id, id, 1)],
            }),
            vec![ItemId::new(id).unwrap()],
        )
    }

    #[derive(Default)]
    struct ProjectionState {
        frontier: Option<CommandPosition>,
        live: Vec<Vec<CommandPosition>>,
        recovery: Vec<Vec<CommandPosition>>,
        fail_live: bool,
        gate: Option<oneshot::Receiver<()>>,
        started: Option<oneshot::Sender<()>>,
    }

    #[derive(Clone)]
    struct RecordingProjection {
        log: AsyncObjectLog,
        state: Arc<Mutex<ProjectionState>>,
    }

    impl RecordingProjection {
        fn new(log: AsyncObjectLog) -> Self {
            Self {
                log,
                state: Arc::new(Mutex::new(ProjectionState::default())),
            }
        }

        fn fail_next_live(&self) {
            self.state.lock().unwrap().fail_live = true;
        }

        fn gate_next_live(&self, started: oneshot::Sender<()>, release: oneshot::Receiver<()>) {
            let mut state = self.state.lock().unwrap();
            state.started = Some(started);
            state.gate = Some(release);
        }
    }

    impl AsyncProjectionStore for RecordingProjection {
        fn ensure_shard(
            &self,
            _definition: QueueDefinition,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn admit_mutation(
            &self,
            _shard: QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn validate_push(
            &self,
            _shard: QueueKey,
            _items: Vec<PushItem>,
            _now: UtcTimestamp,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            std::future::ready(Ok(()))
        }

        fn push_idempotency(
            &self,
            _shard: QueueKey,
            _request_id: fireweed_core::RequestId,
            _fingerprint: PushFingerprint,
            _now: UtcTimestamp,
        ) -> impl std::future::Future<Output = EngineResult<IdempotencyDecision<Vec<ItemId>>>> + Send
        {
            std::future::ready(Ok(IdempotencyDecision::Proceed))
        }

        fn apply_live(
            &self,
            positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            let this = self.clone();
            async move {
                let expected = positions.last().cloned();
                assert_eq!(
                    this.log
                        .high_water(positions[0].queue.clone())
                        .await
                        .unwrap(),
                    expected,
                    "the durable append must precede projection apply"
                );
                let (fail, started, gate) = {
                    let mut state = this.state.lock().unwrap();
                    (
                        std::mem::take(&mut state.fail_live),
                        state.started.take(),
                        state.gate.take(),
                    )
                };
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                if fail {
                    return Err(EngineError::Storage(
                        "injected projection failure".to_string(),
                    ));
                }
                let mut state = this.state.lock().unwrap();
                state.frontier = expected;
                state.live.push(positions);
                Ok(())
            }
        }

        fn apply_recovery(
            &self,
            positions: Vec<CommandPosition>,
            _commands: Vec<CommandEnvelope>,
        ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
            let state = Arc::clone(&self.state);
            async move {
                let mut state = state.lock().unwrap();
                state.frontier = positions.last().cloned();
                state.recovery.push(positions);
                Ok(())
            }
        }

        fn eligible_candidates(
            &self,
            _shard: QueueKey,
            _now: UtcTimestamp,
            _max: usize,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn select_rich_claim(
            &self,
            _shard: QueueKey,
            _unit: ClaimUnit,
            _compatibility: ClaimCompatibility,
            _now: UtcTimestamp,
            _max_items: usize,
        ) -> impl std::future::Future<Output = EngineResult<RichClaimSelection>> + Send {
            std::future::ready(Err(EngineError::Unavailable))
        }

        fn render_claimed(
            &self,
            _shard: QueueKey,
            _ids: Vec<ItemId>,
        ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn item_state(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> impl std::future::Future<Output = EngineResult<Option<ItemState>>> + Send {
            std::future::ready(Ok(None))
        }

        fn item_version(
            &self,
            _shard: QueueKey,
            _id: ItemId,
        ) -> impl std::future::Future<Output = EngineResult<Option<u64>>> + Send {
            std::future::ready(Ok(None))
        }

        fn recovery_high_water(
            &self,
            _shard: QueueKey,
        ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send
        {
            let frontier = self.state.lock().unwrap().frontier.clone();
            std::future::ready(Ok(frontier))
        }

        fn recover_definitions(
            &self,
        ) -> impl std::future::Future<Output = EngineResult<Vec<QueueDefinition>>> + Send {
            std::future::ready(Ok(Vec::new()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn append_precedes_apply_and_outcome_crosses_response_barrier() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer).unwrap();
        let outcome = strategy
            .commit(RawCommitRequest::new(
                shard(),
                vec![command("1"), command("2")],
                0,
            ))
            .await
            .unwrap();
        assert!(outcome.projection_applied());
        assert_eq!(outcome.positions().len(), 2);
        assert_eq!(projection.state.lock().unwrap().live.len(), 1);
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn typed_faults_and_epoch_fence_have_exact_durable_footprints() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        assert!(
            committer
                .commit_replayable(
                    RawCommitRequest::new(shard(), vec![command("1")], 0)
                        .with_fault(RawCommitFault::BeforeAppend)
                )
                .await
                .is_err()
        );
        assert_eq!(log.high_water(shard()).await.unwrap(), None);
        let appended = committer
            .commit_replayable(
                RawCommitRequest::new(shard(), vec![command("2")], 0)
                    .with_fault(RawCommitFault::AfterAppendBeforeApply),
            )
            .await
            .unwrap();
        assert!(!appended.projection_applied());
        assert_eq!(projection.state.lock().unwrap().live.len(), 0);
        log.acquire_epoch(shard()).await.unwrap();
        assert!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), vec![command("3")], 0))
                .await
                .is_err()
        );
        assert_eq!(
            log.high_water(shard()).await.unwrap(),
            Some(appended.positions()[0].clone())
        );
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_live_apply_is_repaired_before_the_next_live_commit() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        projection.fail_next_live();
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 1, 8)
                .await
                .unwrap();
        assert!(
            committer
                .commit_replayable(RawCommitRequest::new(
                    shard(),
                    vec![command("1"), command("2")],
                    0
                ))
                .await
                .is_err()
        );
        assert!(log.high_water(shard()).await.unwrap().is_some());
        assert_eq!(projection.recovery_high_water(shard()).await.unwrap(), None);
        let later = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("3")], 0))
            .await
            .unwrap();
        assert!(later.projection_applied());
        {
            let state = projection.state.lock().unwrap();
            assert_eq!(state.recovery.len(), 2, "page-size one replays in order");
            assert!(state.frontier.is_some());
            assert_eq!(state.live.len(), 1, "later live apply follows tail repair");
        }
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn constructors_reject_zero_and_oversized_recovery_pages() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        assert!(
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 0, 8)
                .await
                .is_err()
        );
        assert!(
            ObjectLogProjectionCommitter::open(
                log.clone(),
                projection,
                Vec::new(),
                MAX_RECOVERY_PAGE_SIZE + 1,
                8,
            )
            .await
            .is_err()
        );
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_group_committer_seals_and_maps_each_owned_request() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection.clone(),
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();
        let first = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0))
            .await
            .unwrap();
        let second = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("2")], 0))
            .await
            .unwrap();
        assert!(first.projection_applied() && second.projection_applied());
        assert!(first.positions()[0].precedes(&second.positions()[0]));
        assert_eq!(projection.state.lock().unwrap().recovery.len(), 2);
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn atomic_group_enqueue_seal_maps_around_external_enqueues() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection,
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();

        // External enqueue wins the first ordering race. The atomic committer operation seals both and
        // maps only its own suffix.
        assert!(
            log.group_commit_enqueue(shard(), vec![command("10")], 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        let own = committer
            .commit_replayable(RawCommitRequest::new(shard(), vec![command("11")], 0))
            .await
            .unwrap();
        assert_eq!(own.positions().len(), 1);
        let page = log.read_from(shard(), None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(own.positions()[0], page.entries[1].0);

        // The committer wins the second race. A following external enqueue remains unsealed and cannot
        // alter the already returned request mapping.
        assert!(
            log.group_commit_enqueue(shard(), vec![command("12")], 0, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            log.high_water(shard()).await.unwrap(),
            Some(own.positions()[0].clone())
        );
        let sealed_after = log.group_commit_seal(shard(), 0, 0).await.unwrap();
        assert_eq!(sealed_after.len(), 1);
        assert!(own.positions()[0].precedes(&sealed_after[0]));
        log.close_and_drain().await.unwrap();
    }

    #[derive(Clone)]
    struct TokioTestDispatcher {
        closed: Arc<AtomicBool>,
        accepted: Arc<AtomicUsize>,
        outstanding: Arc<AtomicUsize>,
    }

    impl TokioTestDispatcher {
        fn new() -> Self {
            Self {
                closed: Arc::new(AtomicBool::new(false)),
                accepted: Arc::new(AtomicUsize::new(0)),
                outstanding: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn accepted(&self) -> usize {
            self.accepted.load(Ordering::Acquire)
        }

        fn outstanding(&self) -> usize {
            self.outstanding.load(Ordering::Acquire)
        }
    }

    impl OwnedTaskDispatcher for TokioTestDispatcher {
        fn submit<T: Send + 'static>(
            &self,
            factory: OwnedTaskFactory<T>,
        ) -> Result<TaskOutcome<T>, DispatchError> {
            if self.closed.load(Ordering::Acquire) {
                return Err(DispatchError::Closed);
            }
            self.accepted.fetch_add(1, Ordering::AcqRel);
            self.outstanding.fetch_add(1, Ordering::AcqRel);
            let outstanding = Arc::clone(&self.outstanding);
            let (sender, outcome) = task_outcome_channel();
            tokio::spawn(async move {
                sender.send(factory().await);
                outstanding.fetch_sub(1, Ordering::AcqRel);
            });
            Ok(outcome)
        }

        fn close(&self) {
            self.closed.store(true, Ordering::Release);
        }

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }

        fn drain(&self) -> TaskOutcome<()> {
            let (sender, outcome): (TaskOutcomeSender<()>, _) = task_outcome_channel();
            sender.send(());
            outcome
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_accepted_response_does_not_cancel_commit() {
        let root = root();
        let log = AsyncObjectLog::open(&root).await.unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        projection.gate_next_live(started_tx, release_rx);
        let committer =
            ObjectLogProjectionCommitter::open(log.clone(), projection.clone(), Vec::new(), 16, 8)
                .await
                .unwrap();
        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer).unwrap();
        let backend =
            fireweed_engine::AsyncComposedBackend::new(strategy, TokioTestDispatcher::new(), 2);
        let response = tokio::spawn({
            let request = RawCommitRequest::new(shard(), vec![command("1")], 0);
            async move { backend.submit_commit(request).await }
        });
        started_rx.await.unwrap();
        response.abort();
        release_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(projection.state.lock().unwrap().live.len(), 1);
        log.close_and_drain().await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coincident_group_requests_share_one_projection_recovery_batch() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection.clone(),
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();

        let first =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0));
        let second =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("2")], 0));
        let (first, second) = futures::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();

        assert!(first.projection_applied() && second.projection_applied());
        assert!(first.positions()[0].precedes(&second.positions()[0]));
        {
            let state = projection.state.lock().unwrap();
            assert_eq!(state.recovery.len(), 1);
            assert_eq!(state.recovery[0].len(), 2);
        }
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn group_coordinator_enforces_global_capacity_and_drains_accepted_requests() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let second_shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue-2").unwrap(),
        );
        let third_shard = QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue-3").unwrap(),
        );
        log.ensure_shard(second_shard.clone()).await.unwrap();
        log.ensure_shard(third_shard.clone()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection,
            Vec::new(),
            16,
            2,
        )
        .await
        .unwrap();

        let mut first =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0));
        let mut second =
            committer.commit_replayable(RawCommitRequest::new(second_shard, vec![command("2")], 0));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(
            committer
                .commit_replayable(RawCommitRequest::new(third_shard, vec![command("3")], 0))
                .await,
            Err(EngineError::Unavailable)
        ));
        committer.close();
        assert!(matches!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), vec![command("4")], 0))
                .await,
            Err(EngineError::Unavailable)
        ));
        let (first, second) = futures::join!(first, second);
        assert!(first.is_ok() && second.is_ok());
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_pre_durable_driver_hands_progress_to_later_task() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection,
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();

        let mut dropped =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0));
        let survivor =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("2")], 0));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(dropped.as_mut().poll(&mut context), Poll::Pending));
        drop(dropped);

        let survivor = survivor.await.unwrap();
        assert_eq!(survivor.positions().len(), 1);
        let page = log.read_from(shard(), None, 10).await.unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].0, survivor.positions()[0]);
        committer.close_and_drain().await;
        let snapshot = committer.byte_admission_snapshot();
        assert_eq!(snapshot.configured_global_bytes, 64 * 1024 * 1024);
        assert_eq!(snapshot.configured_tenant_bytes, None);
        assert_eq!(snapshot.configured_queue_waiting_bytes, 16 * 1024 * 1024);
        assert_eq!(snapshot.current_bytes, 0);
        assert!(snapshot.peak_bytes > 0);
        log.close_and_drain().await.unwrap();
    }

    async fn assert_driver_drop_at_phase(phase: FlushPhase) {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let projection = RecordingProjection::new(log.clone());
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            projection,
            Vec::new(),
            16,
            8,
        )
        .await
        .unwrap();
        let (started, release) = committer.gate_phase(phase);
        let first =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0));
        let second =
            committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("2")], 0));
        let first = tokio::spawn(first);
        let second = tokio::spawn(second);
        started.await.unwrap();
        first.abort();
        let _ = first.await;
        drop(release);

        let second = second.await.unwrap();
        match phase {
            FlushPhase::PreRepair => {
                let outcome = second.unwrap();
                assert!(outcome.projection_applied());
                assert_eq!(outcome.positions().len(), 1);
            }
            FlushPhase::SealPending | FlushPhase::ActorSubmitted => assert!(matches!(
                second,
                Err(EngineError::Storage(message)) if message.contains("outcome unknown")
            )),
            FlushPhase::ApplyPending => {
                let outcome = second.unwrap();
                assert!(!outcome.projection_applied());
                assert_eq!(outcome.positions().len(), 1);
            }
        }
        committer.close_and_drain().await;
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        assert_eq!(committer.coordinator.state.lock().unwrap().outstanding, 0);
        assert!(
            committer
                .coordinator
                .state
                .lock()
                .unwrap()
                .queues
                .is_empty()
        );
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn driver_drop_during_pre_repair_requeues_other_requests_and_drains() {
        assert_driver_drop_at_phase(FlushPhase::PreRepair).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn driver_drop_during_seal_reports_unknown_outcome_and_drains() {
        assert_driver_drop_at_phase(FlushPhase::SealPending).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn driver_drop_after_actor_submission_keeps_job_owned_until_completion() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            1,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("401")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Wait),
        )
        .await
        .unwrap();
        let (job_started, release_job) = log.gate_serialized_job();
        let (submitted, release_coordinator) = committer.gate_phase(FlushPhase::ActorSubmitted);
        let task =
            tokio::spawn(committer.commit_replayable(RawCommitRequest::new(shard(), commands, 0)));
        tokio::task::spawn_blocking(move || job_started.recv())
            .await
            .unwrap()
            .unwrap();
        submitted.await.unwrap();
        task.abort();
        let _ = task.await;
        drop(release_coordinator);

        // The coordinator request is gone, but the accepted blocking actor job still owns the records and
        // therefore the byte permit.
        assert_eq!(committer.byte_admission_stats().charged_bytes, charge);
        release_job.send(()).unwrap();
        log.close_and_drain().await.unwrap();
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn driver_drop_during_apply_reports_durable_append_and_drains() {
        assert_driver_drop_at_phase(FlushPhase::ApplyPending).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpolled_drop_has_no_state_and_global_capacity_reclaims_queue_entries() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1024 * 1024, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        let committer = GroupCommitObjectLogProjectionCommitter::open(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            2,
        )
        .await
        .unwrap();

        drop(committer.commit_replayable(RawCommitRequest::new(shard(), vec![command("1")], 0)));
        {
            let state = committer.coordinator.state.lock().unwrap();
            assert_eq!(state.outstanding, 0);
            assert!(state.queues.is_empty());
        }

        for index in 0..16_u64 {
            let key = QueueKey::new(
                TenantId::new("tenant").unwrap(),
                QueueId::new(format!("queue-{index}")).unwrap(),
            );
            log.ensure_shard(key.clone()).await.unwrap();
            committer
                .commit_replayable(RawCommitRequest::new(
                    key,
                    vec![command(&(index + 10).to_string())],
                    0,
                ))
                .await
                .unwrap();
        }
        {
            let state = committer.coordinator.state.lock().unwrap();
            assert_eq!(state.outstanding, 0);
            assert!(state.queues.is_empty());
        }
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    fn peak_charge(commands: &[CommandEnvelope]) -> usize {
        let records = commands
            .iter()
            .map(|command| SerializedCommandEnvelope {
                envelope: command.clone(),
                record: serde_json::to_vec(command).unwrap(),
            })
            .collect::<Vec<_>>();
        serialized_peak_charge(&records, usize::MAX).unwrap()
    }

    #[test]
    fn serialized_bundle_is_golden_byte_identical_and_charge_matches_frame_peak() {
        let commands = [command("501"), command("502")];
        let serialized = commands
            .iter()
            .map(|command| SerializedCommandEnvelope {
                envelope: command.clone(),
                record: serde_json::to_vec(command).unwrap(),
            })
            .collect::<Vec<_>>();
        for (command, bundled) in commands.iter().zip(&serialized) {
            assert_eq!(bundled.record, serde_json::to_vec(command).unwrap());
            assert_eq!(
                serde_json::to_vec(&bundled.envelope).unwrap(),
                bundled.record
            );
        }
        let records = serialized
            .iter()
            .map(|bundled| bundled.record.clone())
            .collect::<Vec<_>>();
        let frame = crate::segment_integrity::encode(7, 11, &records)
            .unwrap()
            .bytes;
        let resident_records = records.iter().map(Vec::len).sum::<usize>();
        assert_eq!(
            serialized_peak_charge(&serialized, usize::MAX).unwrap(),
            resident_records + frame.len()
        );
        assert_eq!(
            frame.len(),
            crate::segment_integrity::encoded_len(records.iter().map(Vec::len)).unwrap()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn byte_admission_is_oversize_typed_and_releases_after_seal_apply() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("100")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Wait),
        )
        .await
        .unwrap();

        let outcome = committer
            .commit_replayable(RawCommitRequest::new(shard(), commands, 0))
            .await
            .unwrap();
        assert!(outcome.projection_applied());
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        assert_eq!(committer.byte_admission_stats().peak_charged_bytes, charge);

        let too_large = vec![command("1000000000000000000")];
        assert!(matches!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), too_large, 0))
                .await,
            Err(EngineError::RequestTooLarge { limit, .. }) if limit == charge
        ));
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_commit_bounds_bytes_and_waiter_cancellation_is_conservative() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("200")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Wait),
        )
        .await
        .unwrap();
        let (started, release) = committer.gate_phase(FlushPhase::PreRepair);
        let first = tokio::spawn(committer.commit_replayable(RawCommitRequest::new(
            shard(),
            commands.clone(),
            0,
        )));
        started.await.unwrap();

        let mut waiting = Box::pin(committer.prepare_configured(RawCommitRequest::new(
            shard(),
            commands.clone(),
            0,
        )));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        let stats = committer.byte_admission_stats();
        assert_eq!(stats.charged_bytes, charge);
        assert_eq!(stats.waiting_requests, 1);
        assert!(matches!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), commands, 0,))
                .await,
            Err(EngineError::Backpressure {
                resource: "buffered bytes"
            })
        ));
        drop(waiting);
        assert_eq!(committer.byte_admission_stats().waiting_requests, 0);
        assert_eq!(committer.byte_admission_stats().charged_bytes, charge);

        drop(release);
        first.await.unwrap().unwrap();
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finite_admission_policy_maps_exhaustion_to_typed_backpressure() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            1,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("250")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Reject),
        )
        .await
        .unwrap();
        let (started, release) = committer.gate_phase(FlushPhase::PreRepair);
        let first = tokio::spawn(committer.commit_replayable(RawCommitRequest::new(
            shard(),
            commands.clone(),
            0,
        )));
        started.await.unwrap();
        assert!(matches!(
            committer.prepare_reject(RawCommitRequest::new(shard(), commands, 0)),
            Err(EngineError::Backpressure {
                resource: "buffered bytes"
            })
        ));
        assert_eq!(
            committer
                .coordinator
                .state
                .lock()
                .expect("coordinator state")
                .outstanding,
            1,
            "rejected preparation must not reach queue admission or dispatcher ownership"
        );
        drop(release);
        first.await.unwrap().unwrap();
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn composed_raw_commit_prepares_before_queue_gate_and_dispatch() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            1,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("275")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Wait),
        )
        .await
        .unwrap();
        let (started, release) = committer.gate_phase(FlushPhase::PreRepair);
        let first = tokio::spawn(committer.commit_replayable(RawCommitRequest::new(
            shard(),
            commands.clone(),
            0,
        )));
        started.await.unwrap();

        let strategy =
            SeparateReplayCommit::for_profile(DurabilityClass::EventualApply, committer.clone())
                .unwrap();
        let dispatcher = TokioTestDispatcher::new();
        let backend = AsyncComposedBackend::new(strategy, dispatcher.clone(), 1);
        backend.close();

        let mut waiting =
            Box::pin(backend.submit_commit(RawCommitRequest::new(shard(), commands, 0)));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(dispatcher.accepted(), 0);
        assert_eq!(dispatcher.outstanding(), 0);
        assert_eq!(committer.byte_admission_stats().waiting_requests, 1);
        drop(waiting);
        assert_eq!(committer.byte_admission_stats().waiting_requests, 0);

        drop(release);
        first.await.unwrap().unwrap();
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_queue_waiting_cap_rejects_parked_bytes_without_blocking_driver() {
        let store: Arc<dyn crate::segmented::BlobStore> = Arc::new(InMemoryBlobStore::default());
        let log = AsyncObjectLog::open_group_commit_with_blob_store_and_limits(
            store,
            SegmentConfig::new(1, 100).unwrap(),
            8,
            2,
        )
        .await
        .unwrap();
        log.ensure_shard(shard()).await.unwrap();
        let commands = vec![command("300")];
        let charge = peak_charge(&commands);
        let budget = BufferedByteBudget::new(BufferedByteBudgetConfig::new(charge * 3).unwrap());
        let committer = GroupCommitObjectLogProjectionCommitter::open_with_byte_admission(
            log.clone(),
            RecordingProjection::new(log.clone()),
            Vec::new(),
            16,
            8,
            ObjectLogByteAdmissionConfig::new(budget, charge, ByteAdmissionWaitPolicy::Wait),
        )
        .await
        .unwrap();
        let (started, release) = committer.gate_phase(FlushPhase::PreRepair);
        let first = tokio::spawn(committer.commit_replayable(RawCommitRequest::new(
            shard(),
            commands.clone(),
            0,
        )));
        started.await.unwrap();
        let mut parked =
            committer.commit_replayable(RawCommitRequest::new(shard(), commands.clone(), 0));
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(parked.as_mut().poll(&mut context), Poll::Pending));
        assert!(matches!(
            committer
                .commit_replayable(RawCommitRequest::new(shard(), commands, 0))
                .await,
            Err(EngineError::Backpressure {
                resource: "queue buffered bytes"
            })
        ));
        assert_eq!(committer.byte_admission_stats().charged_bytes, charge * 2);
        drop(parked);
        assert_eq!(committer.byte_admission_stats().charged_bytes, charge);
        drop(release);
        first.await.unwrap().unwrap();
        assert_eq!(committer.byte_admission_stats().charged_bytes, 0);
        committer.close_and_drain().await;
        log.close_and_drain().await.unwrap();
    }

    #[test]
    #[ignore = "manual SP-01 serialization/admission microbenchmark"]
    fn byte_admission_serialization_microbenchmark() {
        use std::hint::black_box;
        use std::time::Instant;

        let commands = (1..=100)
            .map(|id| command(&id.to_string()))
            .collect::<Vec<_>>();
        let iterations = 2_000;
        let started = Instant::now();
        let mut baseline_bytes = 0usize;
        for _ in 0..iterations {
            for command in &commands {
                baseline_bytes += black_box(serde_json::to_vec(command).unwrap()).len();
            }
        }
        let baseline = started.elapsed();

        let started = Instant::now();
        let mut admitted_bytes = 0usize;
        for _ in 0..iterations {
            let records = commands
                .iter()
                .map(|command| serde_json::to_vec(command).unwrap())
                .collect::<Vec<_>>();
            admitted_bytes += black_box(
                serialized_peak_charge_for_lengths(
                    records.iter().map(Vec::len),
                    usize::MAX,
                )
                .unwrap(),
            );
            black_box(records);
        }
        let admitted = started.elapsed();
        assert!(baseline_bytes > 0 && admitted_bytes > baseline_bytes);
        eprintln!(
            "SP-01 serialization microbenchmark: commands={}, iterations={}, baseline={baseline:?}, admitted={admitted:?}, ratio={:.3}",
            commands.len(),
            iterations,
            admitted.as_secs_f64() / baseline.as_secs_f64()
        );
    }
}
