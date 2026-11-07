//! One loop per queue; every operation, read or write, goes through it.
//!
//! The loop owns the queue's position, its storage fence, its lease deadline
//! and its use of the local projection, and keeps `P = fold(log[0, p))`.
//! A healthy iteration makes no object-store read: writes make one conditional
//! publish and apply the same record to the projection; reads are served from
//! the projection. Exit is the only failure path. Entry always rebuilds from
//! the log under a strictly newer fence.
use super::*;
use fireweed_core::OwnerId;
use fireweed_engine::{
    AcquireOutcome, AsyncQueueControlPlane, Clock, LeaseState, QueueKey,
    queue_log_record::{QueueLogPayload, QueueLogRecordView},
};
use fireweed_objectlog::Api007NamespaceIdentity;
use futures::future::BoxFuture;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// A handle serves at most this many queues. Further queues receive `Busy`.
const MAX_QUEUES: usize = 4096;
/// An operation whose own execution leaves owner state this many times fails.
const MAX_ATTEMPTS: u32 = 3;
/// Records read between lease checks during entry.
const ENTRY_CHUNK: u64 = 256;

/// Everything a pipeline needs to own queues by lease. Every store has one.
#[derive(Clone)]
pub struct QueueOwnership {
    control: Arc<dyn AsyncQueueControlPlane>,
    owner: OwnerId,
    clock: Arc<dyn Clock>,
    lease_clock: Arc<dyn LeaseClock>,
    ttl: Duration,
    skew: Duration,
}
impl QueueOwnership {
    /// `clock` stamps control-plane calls. `lease_clock` measures deadlines and
    /// must count all elapsed time. `ttl` is the control plane's lease duration;
    /// the skew allowance is `ttl / 5`.
    pub fn new(
        control: Arc<dyn AsyncQueueControlPlane>,
        owner: OwnerId,
        clock: Arc<dyn Clock>,
        lease_clock: Arc<dyn LeaseClock>,
        ttl: Duration,
    ) -> QueueResult<Self> {
        let skew = ttl / 5;
        if skew.is_zero() {
            return Err(QueueError::Invalid("lease ttl is too short".into()));
        }
        Ok(Self {
            control,
            owner,
            clock,
            lease_clock,
            ttl,
            skew,
        })
    }
    /// Zero is permitted only here, for tests that drive a manual lease clock.
    #[cfg(test)]
    pub(super) fn with_skew(mut self, skew: Duration) -> Self {
        self.skew = skew;
        self
    }
    pub(super) fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }
    fn stamp(&self) -> QueueResult<fireweed_core::UtcTimestamp> {
        Ok(self.clock.now())
    }
}

/// Stall points tests use to advance their manual lease clock.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Stage {
    BatchTaken,
    BeforeAppend,
    AfterRead,
}

/// Leave owner state: drop the projection, forget the position and fence, and
/// run every unacknowledged operation again after re-entry.
pub(super) struct Exit;

/// Map a store or projection error that cannot be classified to `Exit`;
/// everything else is a result for the caller.
pub(super) fn settle<T>(result: QueueResult<T>) -> Result<QueueResult<T>, Exit> {
    match result {
        Err(QueueError::OutcomeUnknown(_)) => Err(Exit),
        other => Ok(other),
    }
}

pub(super) struct Genesis {
    pub epoch: u64,
    pub definition: Vec<u8>,
    pub cursor_secret: [u8; 32],
}
/// Position, fence and genesis authority of one owned queue.
pub(super) struct Tenure {
    lease_epoch: u64,
    identity: Api007NamespaceIdentity,
    /// Writer epoch: the storage fence taken at entry.
    pub epoch: u64,
    pub next_offset: u64,
    pub insertion_sequence: u64,
    pub historical_epoch: u64,
    pub genesis: Option<Genesis>,
    deadline: Duration,
    renew_at: Duration,
}
impl Tenure {
    pub(super) fn namespace(
        &self,
        p: &QueueRecoveryPipeline,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<fireweed_objectlog::VerifiedApi007Namespace> {
        p.store.rebind_api007_namespace(&self.identity, job)
    }
    pub(super) fn point(&self) -> (u64, u64, u64) {
        (
            self.next_offset,
            self.insertion_sequence,
            self.historical_epoch,
        )
    }
    pub(super) fn genesis_record(
        &self,
        p: &QueueRecoveryPipeline,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<fireweed_engine::queue_log_record::OwnedQueueLogRecord> {
        let genesis = self.genesis.as_ref().ok_or(QueueError::QueueNotFound)?;
        let definition = AdmittedQueueDefinition::from_raw(&genesis.definition, job)?;
        encode_checked_genesis(
            genesis.epoch,
            address,
            &definition,
            &genesis.cursor_secret,
            p.hard_record_bytes,
            job,
        )
    }
    /// Advance the position past one record that is already in the projection.
    fn observe(&mut self, view: &QueueLogRecordView<'_>, offset: u64) -> QueueResult<()> {
        if offset != self.next_offset {
            return Err(unknown("record offset differs from owned position"));
        }
        match view.payload() {
            QueueLogPayload::Genesis(genesis) => {
                if offset != 0 {
                    return Err(unknown("genesis is not the first record"));
                }
                self.genesis = Some(Genesis {
                    epoch: view.epoch(),
                    definition: genesis.definition_bytes().to_vec(),
                    cursor_secret: *genesis.cursor_secret(),
                });
                self.insertion_sequence = 0;
            }
            QueueLogPayload::Publication(frame) => {
                self.insertion_sequence = frame.summary().final_sequence;
            }
            QueueLogPayload::Claim(claim) => {
                self.insertion_sequence = claim.commands().summary().final_sequence;
            }
        }
        self.historical_epoch = view.epoch();
        self.next_offset = offset.checked_add(1).ok_or(QueueError::TooLarge)?;
        Ok(())
    }
    /// Publish conditionally on the fence and the exact expected offset, then
    /// apply the same bytes to the projection. Anything but `Stored` is `Exit`.
    pub(super) async fn publish(
        &mut self,
        p: &QueueRecoveryPipeline,
        record: fireweed_engine::queue_log_record::OwnedQueueLogRecord,
        expected_head: u64,
        job: &Arc<QueueReservation>,
    ) -> Result<(), Exit> {
        if expected_head != self.next_offset {
            return Err(Exit);
        }
        let epoch = record.view().map_err(|_| Exit)?.epoch();
        if epoch != self.epoch {
            return Err(Exit);
        }
        let frozen = fireweed_engine::queue_log_record::OwnedQueueLogRecord::from_admitted_bytes(
            bytes::Bytes::copy_from_slice(record.bytes()),
            job.owned_buffer((record.bytes().len() as u64) * 4 + 4096)
                .map_err(|_| Exit)?,
            job,
        )
        .map_err(|_| Exit)?;
        let namespace = p
            .store
            .rebind_api007_namespace(&self.identity, job)
            .map_err(|_| Exit)?;
        #[cfg(test)]
        p.stage(Stage::BeforeAppend);
        let appended = p
            .store
            .append_api007_record(namespace, record, expected_head, job, &p.workers)
            .await;
        #[cfg(test)]
        let appended = if p
            .lose_append_reply
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            assert!(
                appended
                    .as_ref()
                    .is_ok_and(|result| result.outcome().is_ok())
            );
            // The real append completed; suppress only its returned classification.
            Err(unknown("injected lost durable append reply"))
        } else {
            appended
        };
        let stored = matches!(
            appended.as_ref().map(|result| result.outcome()),
            Ok(Ok(position)) if position.backend_epoch == epoch
                && position.sequence == expected_head
        );
        drop(appended);
        if !stored {
            return Err(Exit);
        }
        self.apply(p, frozen, expected_head, job)
            .await
            .map_err(|_| Exit)
    }
    pub(super) async fn apply(
        &mut self,
        p: &QueueRecoveryPipeline,
        record: fireweed_engine::queue_log_record::OwnedQueueLogRecord,
        offset: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        self.check(&record, offset)?;
        p.projection.apply(record, offset, job, &p.workers).await?;
        Ok(())
    }
    /// Verify the record and advance the position past it.
    pub(super) fn check(
        &mut self,
        record: &fireweed_engine::queue_log_record::OwnedQueueLogRecord,
        offset: u64,
    ) -> QueueResult<()> {
        let checked = record.checked()?;
        let view = checked.view();
        if view.format() != QueueLogFormat::ChecksummedV2 || view.epoch() > self.epoch {
            return Err(unknown("record checksum policy or epoch mismatch"));
        }
        self.observe(view, offset)
    }
}

/// One operation as the loop sees it.
pub(super) trait QueueOp: Send {
    fn mutation(&self) -> bool;
    fn job(&self) -> &Arc<QueueReservation>;
    fn attempts(&mut self) -> &mut u32;
    fn run<'a>(
        &'a mut self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
    ) -> BoxFuture<'a, Result<(), Exit>>;
    /// Deliver the stored result.
    fn ack(self: Box<Self>);
    fn fail(self: Box<Self>, error: QueueError);
}

/// The request-specific part of an operation.
pub(super) trait Plan: Send + Sync + 'static {
    type Output: Send + 'static;
    const MUTATION: bool;
    fn address(&self) -> &QueueAddress;
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>>;
}
struct Op<P: Plan> {
    plan: P,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    result: Option<QueueResult<P::Output>>,
    reply: Option<tokio::sync::oneshot::Sender<QueueResult<P::Output>>>,
    attempts: u32,
    _input: OwnedQueueBufferCharge,
    // Release the operation slot before the final reservation owner can
    // notify shutdown waiters.
    _completion: OperationCompletion,
}
impl<P: Plan> QueueOp for Op<P> {
    fn mutation(&self) -> bool {
        P::MUTATION
    }
    fn job(&self) -> &Arc<QueueReservation> {
        &self.job
    }
    fn attempts(&mut self) -> &mut u32 {
        &mut self.attempts
    }
    fn run<'a>(
        &'a mut self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
    ) -> BoxFuture<'a, Result<(), Exit>> {
        Box::pin(async move {
            self.result = None;
            let result = self
                .plan
                .run(p, t, self.limits, &self.clock, &self.job)
                .await?;
            self.result = Some(result);
            Ok(())
        })
    }
    fn ack(mut self: Box<Self>) {
        let result = self
            .result
            .take()
            .unwrap_or_else(|| Err(unknown("operation acknowledged without a result")));
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }
    fn fail(mut self: Box<Self>, error: QueueError) {
        self.result = None;
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(error));
        }
    }
}

struct QueueSlot {
    sender: UnboundedSender<Box<dyn QueueOp>>,
    task: tokio::task::JoinHandle<()>,
}
pub(super) struct QueueTable {
    open: bool,
    slots: std::collections::BTreeMap<QueueAddress, QueueSlot>,
}
impl Default for QueueTable {
    fn default() -> Self {
        Self {
            open: true,
            slots: Default::default(),
        }
    }
}

impl QueueRecoveryPipeline {
    /// Enqueue one operation on the queue's loop and wait for its acknowledgement.
    pub(super) async fn submit<P: Plan>(
        self: &Arc<Self>,
        plan: P,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
        input: OwnedQueueBufferCharge,
    ) -> QueueResult<P::Output> {
        plan.address().validate()?;
        let slot = Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let sender = self.sender(plan.address())?;
        let op: Box<dyn QueueOp> = Box::new(Op {
            plan,
            limits,
            clock,
            job: job.clone(),
            result: None,
            reply: Some(reply),
            attempts: 0,
            _input: input,
            _completion: OperationCompletion {
                _slot: slot,
                _job: job,
            },
        });
        sender.send(op).map_err(|_| QueueError::Busy)?;
        receiver
            .await
            .map_err(|_| unknown("queue loop dropped an operation"))?
    }
    fn sender(
        self: &Arc<Self>,
        address: &QueueAddress,
    ) -> QueueResult<UnboundedSender<Box<dyn QueueOp>>> {
        let mut table = self
            .queues
            .lock()
            .map_err(|_| unknown("queue table poisoned"))?;
        if !table.open {
            return Err(QueueError::Busy);
        }
        if let Some(slot) = table.slots.get(address)
            && !slot.task.is_finished()
        {
            return Ok(slot.sender.clone());
        }
        table.slots.retain(|_, slot| !slot.task.is_finished());
        if table.slots.len() >= MAX_QUEUES {
            return Err(QueueError::Busy);
        }
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let key = queue_key(address)?;
        let task = tokio::spawn(
            QueueLoop {
                pipeline: Arc::downgrade(self),
                address: address.clone(),
                key,
                receiver,
                pending: VecDeque::new(),
                tenure: None,
                closing: false,
            }
            .run(),
        );
        table.slots.insert(
            address.clone(),
            QueueSlot {
                sender: sender.clone(),
                task,
            },
        );
        Ok(sender)
    }
    /// Stop accepting operations, let every loop finish what it holds, and
    /// release the leases. The projection is not touched: it dies with the process.
    pub(super) async fn close_queues(&self) {
        let slots = {
            let mut table = match self.queues.lock() {
                Ok(table) => table,
                Err(poisoned) => poisoned.into_inner(),
            };
            table.open = false;
            std::mem::take(&mut table.slots)
        };
        let mut tasks = Vec::new();
        for (_, slot) in slots {
            drop(slot.sender);
            tasks.push(slot.task);
        }
        for task in tasks {
            let _ = task.await;
        }
    }
    #[cfg(test)]
    pub(super) fn stage(&self, stage: Stage) {
        if let Some(hook) = self.on_stage.lock().unwrap().clone() {
            hook(stage);
        }
    }
}

fn queue_key(address: &QueueAddress) -> QueueResult<QueueKey> {
    use sha2::Digest;
    let digest = Sha256::digest(fireweed_engine::queue_namespace::encode_address(address)?);
    let mut name = String::from("q7-");
    for byte in digest {
        name.push_str(&format!("{byte:02x}"));
    }
    let id = |value: String, what: &str| -> QueueResult<String> {
        if value.trim().is_empty() {
            return Err(QueueError::Invalid(format!("empty {what}")));
        }
        Ok(value)
    };
    Ok(QueueKey::new(
        fireweed_core::TenantId::new(id("api007".into(), "tenant")?)
            .map_err(|e| QueueError::Invalid(e.to_string()))?,
        fireweed_core::QueueId::new(id(name, "queue")?)
            .map_err(|e| QueueError::Invalid(e.to_string()))?,
    ))
}

struct QueueLoop {
    pipeline: std::sync::Weak<QueueRecoveryPipeline>,
    address: QueueAddress,
    key: QueueKey,
    receiver: UnboundedReceiver<Box<dyn QueueOp>>,
    pending: VecDeque<Box<dyn QueueOp>>,
    tenure: Option<Tenure>,
    closing: bool,
}
impl QueueLoop {
    fn take_queued(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(op) => self.pending.push_back(op),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.closing = true;
                    return;
                }
            }
        }
    }
    /// Wait for an operation, or for `renew_at` when we own the queue.
    async fn wait(&mut self, p: &QueueRecoveryPipeline) {
        loop {
            self.take_queued();
            if !self.pending.is_empty() || self.closing {
                return;
            }
            let wait = match &self.tenure {
                None => None,
                Some(t) => {
                    let now = p.ownership.lease_clock.now();
                    if now >= t.renew_at {
                        return;
                    }
                    Some((t.renew_at - now).min(Duration::from_millis(200)))
                }
            };
            let received = match wait {
                None => self.receiver.recv().await.map(Some),
                Some(wait) => match tokio::time::timeout(wait, self.receiver.recv()).await {
                    Ok(received) => received.map(Some),
                    Err(_) => Some(None),
                },
            };
            match received {
                Some(Some(op)) => self.pending.push_back(op),
                Some(None) => {}
                None => self.closing = true,
            }
        }
    }
    async fn run(mut self) {
        loop {
            let Some(p) = self.pipeline.upgrade() else {
                return;
            };
            self.wait(&p).await;
            if self.pending.is_empty() && self.closing {
                break;
            }
            self.iterate(&p).await;
        }
        if let Some(p) = self.pipeline.upgrade() {
            self.release(&p).await;
        }
    }
    async fn iterate(&mut self, p: &Arc<QueueRecoveryPipeline>) {
        if self.tenure.is_none() {
            if self.pending.is_empty() {
                return;
            }
            let job = self.pending[0].job().clone();
            match self.enter(p, &job).await {
                Ok(tenure) => self.tenure = Some(tenure),
                Err(error) => {
                    for op in self.pending.drain(..) {
                        op.fail(error.clone());
                    }
                    return;
                }
            }
        }
        let mut batch: Vec<Box<dyn QueueOp>> = self.pending.drain(..).collect();
        #[cfg(test)]
        p.stage(Stage::BatchTaken);
        let tenure = self.tenure.as_mut().expect("owner");
        let mut leave = false;
        // Mutations in order, then the batch's reads from the projection.
        for want_mutation in [true, false] {
            for op in batch.iter_mut().filter(|op| op.mutation() == want_mutation) {
                if op.run(p, tenure).await.is_err() {
                    *op.attempts() += 1;
                    leave = true;
                    break;
                }
            }
            if leave {
                break;
            }
        }
        #[cfg(test)]
        p.stage(Stage::AfterRead);
        // Every result passes this check after it was read and before it is acknowledged.
        if leave || p.ownership.lease_clock.now() >= tenure.deadline {
            self.exit(p, batch.first().map(|op| op.job().clone())).await;
            self.requeue(batch);
            return;
        }
        for op in batch {
            op.ack();
        }
        let renew_at = self.tenure.as_ref().expect("owner").renew_at;
        if p.ownership.lease_clock.now() >= renew_at {
            self.renew(p).await;
        }
    }
    fn requeue(&mut self, batch: Vec<Box<dyn QueueOp>>) {
        // Unacknowledged operations run again first, ahead of anything queued since.
        let mut again: VecDeque<Box<dyn QueueOp>> = VecDeque::new();
        for mut op in batch {
            if *op.attempts() > MAX_ATTEMPTS {
                op.fail(unknown("operation left owner state repeatedly"));
            } else {
                again.push_back(op);
            }
        }
        again.append(&mut self.pending);
        self.pending = again;
    }
    /// Renew between iterations. Rejected leaves owner state; a failed or
    /// unknown renewal leaves the deadline alone, so the owner stops at it.
    async fn renew(&mut self, p: &Arc<QueueRecoveryPipeline>) {
        let own = &p.ownership;
        let Some(t) = self.tenure.as_mut() else {
            return;
        };
        let started = own.lease_clock.now();
        let renewed = match own.stamp() {
            Ok(now) => {
                own.control
                    .renew_queue_lease(&self.key, &own.owner, t.lease_epoch, now)
                    .await
            }
            Err(_) => Err(fireweed_engine::EngineError::Unavailable),
        };
        match renewed {
            Ok(_) => {
                t.deadline = started + own.ttl;
                // A renewal that took longer than half a lease is not retried at once.
                t.renew_at = (started + own.ttl / 2).max(own.lease_clock.now() + own.ttl / 10);
            }
            Err(fireweed_engine::EngineError::EpochFenced) => {
                self.tenure = None;
            }
            Err(_) => {
                t.renew_at = own.lease_clock.now() + own.ttl / 10;
            }
        }
    }
    /// Drop this queue's projection and leave owner state. With no operation
    /// to charge, the rows are dropped by the next entry.
    async fn exit(&mut self, p: &Arc<QueueRecoveryPipeline>, job: Option<Arc<QueueReservation>>) {
        self.tenure = None;
        if let Some(job) = job {
            let _ = p
                .projection
                .drop_queue(self.address.clone(), &job, &p.workers)
                .await;
        }
    }
    async fn release(&mut self, p: &Arc<QueueRecoveryPipeline>) {
        if let Some(t) = self.tenure.take() {
            let own = &p.ownership;
            if let Ok(now) = own.stamp() {
                let _ = own
                    .control
                    .release_queue_lease(&self.key, &own.owner, t.lease_epoch, now)
                    .await;
            }
        }
    }
    async fn wait_lease_clock(p: &QueueRecoveryPipeline, until: Duration) {
        loop {
            let now = p.ownership.lease_clock.now();
            if now >= until {
                return;
            }
            tokio::time::sleep((until - now).min(Duration::from_millis(20))).await;
        }
    }
    /// Acquire the lease, wait out the skew allowance, take a strictly newer
    /// fence and rebuild the projection from the log.
    async fn enter(
        &mut self,
        p: &Arc<QueueRecoveryPipeline>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Tenure> {
        let mut span = p.diagnostics.begin();
        let result = self.enter_scanned(p, job, &mut span).await;
        span.finish(result.is_ok());
        result
    }
    async fn enter_scanned(
        &mut self,
        p: &Arc<QueueRecoveryPipeline>,
        job: &Arc<QueueReservation>,
        span: &mut diagnostics::RecoverySpan,
    ) -> QueueResult<Tenure> {
        let own = &p.ownership;
        let busy = |_: fireweed_engine::EngineError| QueueError::Busy;
        // A previous tenure's rows are never reused.
        p.projection
            .drop_queue(self.address.clone(), job, &p.workers)
            .await?;
        let mut requested = own.lease_clock.now();
        own.control
            .register_owner(&own.owner, own.stamp()?)
            .await
            .map_err(busy)?;
        let lease = match own
            .control
            .acquire_queue_lease(&self.key, &own.owner, own.stamp()?)
            .await
            .map_err(busy)?
        {
            AcquireOutcome::Acquired(lease) => lease,
            AcquireOutcome::Rejected(_) => return Err(QueueError::Busy),
        };
        // Always wait, with no check of whether there was a previous owner.
        Self::wait_lease_clock(p, own.lease_clock.now() + own.skew).await;
        let namespace = p.store.bind_api007_namespace(&self.address, job).await?;
        let identity = namespace.identity();
        p.diagnostics.head_read();
        let head = p
            .store
            .read_api007_state(namespace, job, &p.workers)
            .await?;
        let mut tenure = Tenure {
            lease_epoch: lease.assignment_epoch,
            identity,
            epoch: 0,
            next_offset: 0,
            insertion_sequence: 0,
            historical_epoch: 0,
            genesis: None,
            deadline: Duration::ZERO,
            renew_at: Duration::ZERO,
        };
        let target = if head.state().next_offset == 0 && head.state().log_start == 0 {
            // No queue yet: nothing to fence or read. Provisioning fences.
            0
        } else {
            p.diagnostics.fence_acquire();
            let acquired = p
                .store
                .acquire_api007_state(head.into_namespace(), job, &p.workers)
                .await?;
            let state = acquired.state();
            if state.log_start != 0 || state.next_offset < 1 {
                return Err(unknown("complete genesis history unavailable"));
            }
            tenure.epoch = state.epoch;
            let target =
                u64::try_from(state.next_offset).map_err(|_| unknown("invalid acquired head"))?;
            if lease.state == LeaseState::PendingFence {
                own.control
                    .confirm_queue_lease_fence(
                        &self.key,
                        &own.owner,
                        lease.assignment_epoch,
                        own.stamp()?,
                    )
                    .await
                    .map_err(busy)?;
            }
            let mut renewed_at = requested;
            let mut chunk = 0u64;
            while tenure.next_offset < target {
                span.phase(diagnostics::RecoveryPhase::PlannedRead);
                let offset = tenure.next_offset;
                let plan = p.store.plan_api007_read(
                    acquired.namespace(),
                    offset,
                    p.metadata_bytes,
                    job,
                )?;
                let read = plan
                    .read(&p.workers, p.hard_record_bytes)
                    .await?
                    .ok_or_else(|| unknown("early EOF before acquired target"))?;
                let (actual, record) = read.into_record(job)?;
                span.record(record.bytes().len());
                if actual != offset {
                    return Err(unknown("record offset mismatch"));
                }
                span.phase(diagnostics::RecoveryPhase::CheckedFold);
                tenure.check(&record, offset)?;
                span.phase(diagnostics::RecoveryPhase::NativeApply);
                p.projection.apply(record, offset, job, &p.workers).await?;
                chunk += 1;
                if chunk.is_multiple_of(ENTRY_CHUNK)
                    && own.lease_clock.now() >= renewed_at + own.ttl / 2
                {
                    // A long rebuild keeps the lease it is building under.
                    let started = own.lease_clock.now();
                    if own
                        .control
                        .renew_queue_lease(
                            &self.key,
                            &own.owner,
                            lease.assignment_epoch,
                            own.stamp()?,
                        )
                        .await
                        .is_ok()
                    {
                        requested = started;
                        renewed_at = started;
                    }
                }
            }
            target
        };
        let _ = target;
        if lease.state == LeaseState::PendingFence && tenure.epoch == 0 {
            // Absent queue: the lease row is confirmed with nothing to fence.
            own.control
                .confirm_queue_lease_fence(
                    &self.key,
                    &own.owner,
                    lease.assignment_epoch,
                    own.stamp()?,
                )
                .await
                .map_err(busy)?;
        }
        // The deadline is measured from before the latest request that extended
        // the lease, on the lease clock.
        tenure.deadline = requested + own.ttl;
        tenure.renew_at = requested + own.ttl / 2;
        if own.lease_clock.now() >= tenure.deadline {
            return Err(QueueError::Busy);
        }
        Ok(tenure)
    }
}
