//! Internal composition of owned native log stages and the stock-Turso projector.
//! Coverage is operation-scoped; this is not a consumer transaction or readiness cache.
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
mod claim;
mod diagnostics;
pub use diagnostics::{RecoveryCounters, RecoverySnapshot};
mod commit;
mod commit_batch;
pub use commit_batch::AdmittedQueueCommitBatch;
mod insert;
mod mutation;
mod preview;
mod publication;
mod query;
mod read;
mod receipts;
mod state;
mod stats;
#[cfg(target_os = "linux")]
mod store;
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{QueueLogFormat, encode_checked_genesis},
    queue_original_authority::{AdmittedOriginalAuthority, ColdOriginalTracker},
    queue_recovery::ColdQueueHistory,
};
use fireweed_objectlog::ObjectLogEngineStore;
use fireweed_queue_spool::ScratchWorkers;
use fireweed_turso::queue_projection::{QueueProjection, QueueProjectionCheckpoint};
use std::sync::{Arc, Weak};
#[cfg(target_os = "linux")]
pub use store::NativeQueueStore;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 recovery: {message}"))
}
#[cfg(test)]
type StagePause = std::sync::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>;
/// Process-local pipeline capacities. Validate before a factory constructs its
/// store/projection resources; these are not durable queue policy.
#[derive(Debug, Clone, Copy)]
pub struct QueuePipelineOptions {
    pub operations: usize,
    pub metadata_bytes: usize,
    pub hard_record_bytes: u64,
}
impl QueuePipelineOptions {
    pub fn validate(&self) -> QueueResult<()> {
        if self.operations == 0
            || self.operations > Semaphore::MAX_PERMITS
            || self.metadata_bytes == 0
            || self.hard_record_bytes == 0
        {
            return Err(QueueError::Invalid(
                "invalid queue pipeline capacities".into(),
            ));
        }
        Ok(())
    }
}
/// An owned orchestrator has no blocking worker while awaiting individual stages.
pub struct QueueRecoveryPipeline {
    store: Arc<ObjectLogEngineStore>,
    projection: Arc<QueueProjection>,
    workers: ScratchWorkers,
    scratch_root: Arc<fireweed_queue_spool::ScratchRoot>,
    scratch_quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
    slots: Arc<Semaphore>,
    operation_slots: Arc<Semaphore>,
    metadata_bytes: usize,
    hard_record_bytes: u64,
    diagnostics: Arc<RecoveryCounters>,
    #[cfg(test)]
    before_drop: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    before_acquire: std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>,
    #[cfg(test)]
    before_claim_append: StagePause,
    #[cfg(test)]
    before_claim_replay: StagePause,
    #[cfg(test)]
    before_insert_append: StagePause,
    #[cfg(test)]
    before_insert_replay: StagePause,
    #[cfg(test)]
    after_receipt_read: StagePause,
    #[cfg(test)]
    after_state_read: StagePause,
    #[cfg(test)]
    after_query_read: StagePause,
    #[cfg(test)]
    after_stats_read: StagePause,
    #[cfg(test)]
    after_preview_read: StagePause,
    #[cfg(test)]
    before_commit_append: StagePause,
    #[cfg(test)]
    after_commit_replay: StagePause,
    #[cfg(test)]
    after_batch_read: StagePause,
    #[cfg(test)]
    before_handoff_check: StagePause,
    #[cfg(test)]
    after_handoff_check: StagePause,
}
#[cfg(test)]
impl Drop for QueueRecoveryPipeline {
    fn drop(&mut self) {
        if let Some(hook) = self.before_drop.get_mut().unwrap().take() {
            hook();
        }
    }
}
enum RecoveryStateTracker {
    Read(fireweed_engine::queue_state_authority::ColdStateTracker),
    Groups(fireweed_engine::queue_group_state_authority::ColdGroupStateTracker),
}
impl RecoveryStateTracker {
    fn observe(
        &mut self,
        offset: u64,
        record: &fireweed_engine::queue_log_record::CheckedQueueLogRecord<'_>,
    ) -> QueueResult<()> {
        match self {
            Self::Read(s) => s.observe_record(offset, record),
            Self::Groups(s) => s.observe_record(offset, record),
        }
    }
}
/// Point-in-time coverage only; later consumer reads/writes must establish
/// current projection and fence authority again.
pub struct RecoveredQueue {
    source: Weak<ObjectLogEngineStore>,
    pub history: ColdQueueHistory,
    /// Original genesis header epoch, distinct from the acquired fence and last record.
    pub original_genesis_epoch: u64,
    pub acquired_epoch: u64,
    pub target_next_offset: u64,
    original_identity_present: Option<bool>,
    original_authority: Option<Arc<AdmittedOriginalAuthority>>,
    state_authority: Option<Arc<fireweed_engine::queue_state_authority::AdmittedStateAuthority>>,
    group_state_authority:
        Option<Arc<fireweed_engine::queue_group_state_authority::AdmittedGroupStateSet>>,
}
/// On return, cancellation or unwind, release the operation permit before the
/// final task reservation owner can notify shutdown waiters.
struct OperationCompletion {
    _slot: OwnedSemaphorePermit,
    _job: Arc<QueueReservation>,
}
struct BatchContinuation {
    previous: RecoveredQueue,
    request: Arc<fireweed_core::queue_contract::CommitBatch>,
}
struct RecoveryTask {
    continuation: Option<BatchContinuation>,
    // Release the bounded task slot before releasing the operation reservation.
    _slot: OwnedSemaphorePermit,
    pipeline: Arc<QueueRecoveryPipeline>,
    address: QueueAddress,
    identity: Option<(u8, String)>,
    originals: Option<ColdOriginalTracker>,
    states: Option<RecoveryStateTracker>,
    _headers: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    pub fn new(
        store: Arc<ObjectLogEngineStore>,
        projection: Arc<QueueProjection>,
        workers: ScratchWorkers,
        options: QueuePipelineOptions,
        scratch_root: Arc<fireweed_queue_spool::ScratchRoot>,
        scratch_quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
    ) -> QueueResult<Arc<Self>> {
        options.validate()?;
        Ok(Arc::new(Self {
            store,
            projection,
            workers,
            scratch_root,
            scratch_quota,
            slots: Arc::new(Semaphore::new(options.operations)),
            operation_slots: Arc::new(Semaphore::new(options.operations)),
            metadata_bytes: options.metadata_bytes,
            hard_record_bytes: options.hard_record_bytes,
            diagnostics: Arc::new(RecoveryCounters::default()),
            #[cfg(test)]
            before_drop: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_acquire: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_claim_append: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_claim_replay: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_insert_append: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_insert_replay: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_receipt_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_state_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_query_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_stats_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_preview_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_commit_append: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_commit_replay: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_batch_read: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_handoff_check: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_handoff_check: std::sync::Mutex::new(None),
        }))
    }
    pub(super) fn original_genesis(
        &self,
        address: &QueueAddress,
        recovered: &RecoveredQueue,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<fireweed_engine::queue_log_record::OwnedQueueLogRecord> {
        encode_checked_genesis(
            recovered.original_genesis_epoch,
            address,
            recovered.history.definition()?,
            recovered.history.cursor_secret()?,
            self.hard_record_bytes,
            job,
        )
    }
    async fn head_matches(
        &self,
        address: &QueueAddress,
        recovered: &RecoveredQueue,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<bool> {
        let namespace = self.store.bind_api007_namespace(address, job).await?;
        let current = self
            .store
            .read_api007_state(namespace, job, &self.workers)
            .await?;
        let state = current.state();
        Ok(state.log_start == 0
            && state.epoch >= recovered.acquired_epoch
            && u64::try_from(state.next_offset).ok() == Some(recovered.target_next_offset))
    }
    /// Once spawned, caller cancellation cannot abandon fencing or suffix apply.
    pub async fn recover(
        self: &Arc<Self>,
        address: QueueAddress,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        self.recover_checked(address, None, None, None, job).await
    }
    pub(super) async fn recover_request(
        self: &Arc<Self>,
        address: QueueAddress,
        domain: u8,
        identity: String,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        let originals = ColdOriginalTracker::new_request(&address, domain, &identity, &job)?;
        self.recover_checked(
            address,
            Some((domain, identity)),
            Some(originals),
            None,
            job,
        )
        .await
    }
    async fn recover_batch(
        self: &Arc<Self>,
        request: Arc<fireweed_core::queue_contract::CommitBatch>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        let originals = ColdOriginalTracker::new(&request, &job)?;
        self.recover_checked(
            request.queue.clone(),
            None,
            Some(originals),
            Some(RecoveryStateTracker::Groups(
                fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::with_work(
                    &request.queue,
                    fireweed_engine::queue_group_state_authority::GroupDeclarations::Batch(
                        request.clone(),
                    ),
                    &job,
                )?,
            )),
            job,
        )
        .await
    }
    async fn recover_batch_after(
        self: &Arc<Self>,
        request: Arc<fireweed_core::queue_contract::CommitBatch>,
        previous: RecoveredQueue,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        self.recover_scoped(
            request.queue.clone(),
            None,
            None,
            None,
            Some(BatchContinuation { previous, request }),
            job,
        )
        .await
    }
    async fn recover_checked(
        self: &Arc<Self>,
        address: QueueAddress,
        identity: Option<(u8, String)>,
        originals: Option<ColdOriginalTracker>,
        states: Option<RecoveryStateTracker>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        self.recover_scoped(address, identity, originals, states, None, job)
            .await
    }
    async fn recover_scoped(
        self: &Arc<Self>,
        address: QueueAddress,
        identity: Option<(u8, String)>,
        originals: Option<ColdOriginalTracker>,
        states: Option<RecoveryStateTracker>,
        continuation: Option<BatchContinuation>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<RecoveredQueue> {
        address.validate()?;
        let headers = job.owned_buffer(
            u64::try_from(address.tenant.len())
                .map_err(|_| QueueError::TooLarge)?
                .checked_add(u64::try_from(address.queue.len()).map_err(|_| QueueError::TooLarge)?)
                .and_then(|n| n.checked_add(identity.as_ref().map_or(0, |(_, id)| id.len() as u64)))
                .and_then(|n| n.checked_mul(16))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let slot = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = RecoveryTask {
            continuation,
            _slot: slot,
            pipeline: Arc::clone(self),
            address,
            identity,
            originals,
            states,
            _headers: headers,
            job,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = task.execute().await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| unknown("owned recovery task failed"))?
    }
}
fn verify_prefix(
    history: &ColdQueueHistory,
    checkpoint: &QueueProjectionCheckpoint,
) -> QueueResult<()> {
    if history.next_offset() != checkpoint.next_offset
        || history.insertion_sequence() != checkpoint.insertion_sequence
        || history.historical_epoch() != checkpoint.last_record_epoch
    {
        return Err(unknown(
            "projection checkpoint disagrees with authoritative prefix",
        ));
    }
    Ok(())
}
impl RecoveryTask {
    async fn execute(mut self) -> QueueResult<RecoveredQueue> {
        let mut span = self.pipeline.diagnostics.begin();
        let result = self.execute_scanned(&mut span).await;
        span.finish(result.is_ok());
        // Merge before self drops the final operation reservation.
        drop(span);
        result
    }
    async fn execute_scanned(
        &mut self,
        span: &mut diagnostics::RecoverySpan,
    ) -> QueueResult<RecoveredQueue> {
        let pipeline = &self.pipeline;
        // Reject the complete old bundle before consulting projection state.
        if let Some(seed) = &self.continuation {
            let old = &seed.previous;
            if !old
                .source
                .upgrade()
                .is_some_and(|store| Arc::ptr_eq(&store, &pipeline.store))
                || seed.request.queue != self.address
                || old.target_next_offset != old.history.next_offset()
                || old.acquired_epoch < old.history.historical_epoch()
            {
                return Err(unknown("continuation source/declaration/cut mismatch"));
            }
            old.history.definition()?;
            let address = old.history.address()?;
            if !old.history.belongs_to(&self.job)
                || address.tenant != self.address.tenant
                || address.queue != self.address.queue
            {
                return Err(unknown("continuation history operation/address mismatch"));
            }
            let genesis = encode_checked_genesis(
                old.original_genesis_epoch,
                &self.address,
                old.history.definition()?,
                old.history.cursor_secret()?,
                pipeline.hard_record_bytes,
                &self.job,
            )?;
            let progress = (
                old.history.next_offset(),
                old.history.insertion_sequence(),
                old.history.historical_epoch(),
            );
            old.original_authority
                .as_ref()
                .ok_or_else(|| unknown("missing continuation originals"))?
                .validate(&seed.request, &genesis, progress, &self.job)?;
            old.group_state_authority
                .as_ref()
                .ok_or_else(|| unknown("missing continuation states"))?
                .validate_batch(&seed.request, &genesis, progress, &self.job)?;
        }
        let namespace = pipeline
            .store
            .bind_api007_namespace(&self.address, &self.job)
            .await?;
        #[cfg(test)]
        if let Some(notify) = pipeline.before_acquire.lock().unwrap().clone() {
            notify.notify_one();
        }
        let observed = pipeline
            .store
            .read_api007_state(namespace, &self.job, &pipeline.workers)
            .await?;
        let before = observed.state();
        if let Some(seed) = &self.continuation
            && (before.log_start != 0
                || u64::try_from(before.next_offset)
                    .ok()
                    .is_none_or(|head| head < seed.previous.target_next_offset)
                || before.epoch < seed.previous.acquired_epoch)
        {
            return Err(unknown(
                "known continuation authority disappeared or regressed",
            ));
        }
        if before.log_start == 0 && before.next_offset == 0 {
            if pipeline
                .projection
                .has_exact_queue(self.address.clone(), &self.job, &pipeline.workers)
                .await?
            {
                return Err(unknown("known projection has no genesis authority"));
            }
            let confirmed = pipeline
                .store
                .read_api007_state(observed.into_namespace(), &self.job, &pipeline.workers)
                .await?;
            if confirmed.state() == before {
                return Err(QueueError::QueueNotFound);
            }
            return Err(unknown("authority changed during absence classification"));
        }
        let acquired = pipeline
            .store
            .acquire_api007_state(observed.into_namespace(), &self.job, &pipeline.workers)
            .await?;
        let target = acquired.state();
        if target.log_start != 0 || target.next_offset < 1 {
            return Err(unknown("complete genesis history unavailable"));
        }
        let target_next_offset =
            u64::try_from(target.next_offset).map_err(|_| unknown("invalid acquired head"))?;
        let mut history = ColdQueueHistory::new(&self.address, &self.job)?;
        let mut checkpoint: Option<QueueProjectionCheckpoint> = None;
        let mut genesis_epoch = 0;
        let mut original_identity_present = self.identity.as_ref().map(|_| false);
        if let Some(seed) = self.continuation.take() {
            let old = seed.previous;
            if target_next_offset < old.target_next_offset || target.epoch <= old.acquired_epoch {
                return Err(unknown(
                    "continuation requires nonregressing head and fresh fence",
                ));
            }
            let genesis = encode_checked_genesis(
                old.original_genesis_epoch,
                &self.address,
                old.history.definition()?,
                old.history.cursor_secret()?,
                pipeline.hard_record_bytes,
                &self.job,
            )?;
            span.phase(diagnostics::RecoveryPhase::NativeApply);
            let point = pipeline
                .projection
                .checkpoint(genesis, &self.job, &pipeline.workers)
                .await?;
            if point
                .as_ref()
                .is_some_and(|point| point.next_offset > target_next_offset)
            {
                return Err(unknown("projection ahead of continuation target"));
            }
            // Missing/behind projections rebuild cold under this same acquired
            // target and task slot, without adding a resolution attempt.
            if let Some(point) = point.filter(|point| point.next_offset >= old.target_next_offset) {
                if point.next_offset == old.target_next_offset {
                    verify_prefix(&old.history, &point)?;
                }
                let genesis = encode_checked_genesis(
                    old.original_genesis_epoch,
                    &self.address,
                    old.history.definition()?,
                    old.history.cursor_secret()?,
                    pipeline.hard_record_bytes,
                    &self.job,
                )?;
                let progress = (
                    old.history.next_offset(),
                    old.history.insertion_sequence(),
                    old.history.historical_epoch(),
                );
                self.originals = Some(ColdOriginalTracker::resume(
                    old.original_authority.as_ref().unwrap(),
                    &seed.request,
                    &genesis,
                    progress,
                    &self.job,
                )?);
                self.states = Some(RecoveryStateTracker::Groups(
                    fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::resume(
                        old.group_state_authority.as_ref().unwrap(),
                        &seed.request,
                        &genesis,
                        progress,
                        &self.job,
                    )?,
                ));
                genesis_epoch = old.original_genesis_epoch;
                history = old.history;
                checkpoint = Some(point);
            } else {
                self.originals = Some(ColdOriginalTracker::new(&seed.request, &self.job)?);
                self.states = Some(RecoveryStateTracker::Groups(
                    fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::with_work(
                        &seed.request.queue,
                        fireweed_engine::queue_group_state_authority::GroupDeclarations::Batch(
                            seed.request.clone(),
                        ),
                        &self.job,
                    )?,
                ));
            }
        }
        while history.next_offset() < target_next_offset {
            span.phase(diagnostics::RecoveryPhase::PlannedRead);
            let offset = history.next_offset();
            let plan = pipeline.store.plan_api007_read(
                acquired.namespace(),
                offset,
                pipeline.metadata_bytes,
                &self.job,
            )?;
            let read = plan
                .read(&pipeline.workers, pipeline.hard_record_bytes)
                .await?
                .ok_or_else(|| unknown("early EOF before acquired target"))?;
            let (actual, record) = read.into_record(&self.job)?;
            span.record(record.bytes().len());
            span.phase(diagnostics::RecoveryPhase::CheckedFold);
            let checked = record.checked()?;
            let view = checked.view();
            if actual != offset
                || view.format() != QueueLogFormat::ChecksummedV2
                || view.epoch() > target.epoch
            {
                return Err(unknown(
                    "record offset, checksum policy or acquired epoch mismatch",
                ));
            }
            history.observe_record(offset, &checked)?;
            if offset == 0
                && let Some(RecoveryStateTracker::Groups(groups)) = &mut self.states
            {
                groups.initialize_work(&history, &checked, &self.job)?;
            }
            if let Some(originals) = &mut self.originals {
                originals.observe_record(offset, &checked)?;
            }
            if let Some(states) = &mut self.states {
                states.observe(offset, &checked)?;
            }
            if let Some((domain, id)) = &self.identity {
                let present = match view.payload() {
                    fireweed_engine::queue_log_record::QueueLogPayload::Genesis(_) => false,
                    fireweed_engine::queue_log_record::QueueLogPayload::Claim(claim) => {
                        *domain == 1 && claim.request_id() == id
                    }
                    fireweed_engine::queue_log_record::QueueLogPayload::Publication(
                        publication,
                    ) => {
                        let mut found = false;
                        for receipt in publication.receipts() {
                            let receipt =
                                receipt.map_err(|e| unknown(&format!("history receipt: {e:?}")))?;
                            if receipt.domain == *domain && receipt.request_id == id {
                                if found {
                                    return Err(unknown(
                                        "duplicate authoritative original identity",
                                    ));
                                }
                                found = true;
                            }
                        }
                        found
                    }
                };
                if present {
                    if original_identity_present == Some(true) {
                        return Err(unknown("duplicate authoritative original identity"));
                    }
                    original_identity_present = Some(true);
                }
            }
            if offset == 0 {
                genesis_epoch = history.historical_epoch();
                let authority = encode_checked_genesis(
                    genesis_epoch,
                    &self.address,
                    history.definition()?,
                    history.cursor_secret()?,
                    pipeline.hard_record_bytes,
                    &self.job,
                )?;
                span.phase(diagnostics::RecoveryPhase::NativeApply);
                checkpoint = pipeline
                    .projection
                    .checkpoint(authority, &self.job, &pipeline.workers)
                    .await?;
                if checkpoint
                    .as_ref()
                    .is_some_and(|point| point.next_offset > target_next_offset)
                {
                    return Err(unknown("projection ahead of acquired history"));
                }
                if checkpoint.is_none() {
                    pipeline
                        .projection
                        .apply(record, 0, &self.job, &pipeline.workers)
                        .await?;
                }
            } else if checkpoint
                .as_ref()
                .is_none_or(|point| offset >= point.next_offset)
            {
                span.phase(diagnostics::RecoveryPhase::NativeApply);
                pipeline
                    .projection
                    .apply(record, offset, &self.job, &pipeline.workers)
                    .await?;
            }
            span.phase(diagnostics::RecoveryPhase::CheckedFold);
            if let Some(point) = &checkpoint
                && history.next_offset() == point.next_offset
            {
                verify_prefix(&history, point)?;
            }
        }
        let genesis = encode_checked_genesis(
            genesis_epoch,
            &self.address,
            history.definition()?,
            history.cursor_secret()?,
            pipeline.hard_record_bytes,
            &self.job,
        )?;
        span.phase(diagnostics::RecoveryPhase::NativeApply);
        let final_checkpoint = pipeline
            .projection
            .checkpoint(genesis, &self.job, &pipeline.workers)
            .await?
            .ok_or_else(|| unknown("projection disappeared after apply"))?;
        span.phase(diagnostics::RecoveryPhase::CheckedFold);
        verify_prefix(&history, &final_checkpoint)?;
        let original_authority = self
            .originals
            .take()
            .map(|originals| {
                let genesis = encode_checked_genesis(
                    genesis_epoch,
                    &self.address,
                    history.definition()?,
                    history.cursor_secret()?,
                    pipeline.hard_record_bytes,
                    &self.job,
                )?;
                originals
                    .finish(
                        &history,
                        &genesis,
                        (
                            target_next_offset,
                            history.insertion_sequence(),
                            history.historical_epoch(),
                        ),
                    )
                    .map(Arc::new)
            })
            .transpose()?;
        let (state_authority, group_state_authority) = if let Some(states) = self.states.take() {
            let genesis = encode_checked_genesis(
                genesis_epoch,
                &self.address,
                history.definition()?,
                history.cursor_secret()?,
                pipeline.hard_record_bytes,
                &self.job,
            )?;
            let progress = (
                target_next_offset,
                history.insertion_sequence(),
                history.historical_epoch(),
            );
            match states {
                RecoveryStateTracker::Read(states) => (
                    Some(Arc::new(states.finish(&history, &genesis, progress)?)),
                    None,
                ),
                RecoveryStateTracker::Groups(states) => (
                    None,
                    Some(Arc::new(states.finish(&history, &genesis, progress)?)),
                ),
            }
        } else {
            (None, None)
        };
        if let Some((_, id)) = &self.identity {
            let present = original_authority
                .as_ref()
                .ok_or_else(|| unknown("request original proof missing"))?
                .descriptor(0, id)?
                .is_some();
            if original_identity_present != Some(present) {
                return Err(unknown(
                    "original presence fold disagrees with sealed source proof",
                ));
            }
            original_identity_present = Some(present);
        }
        Ok(RecoveredQueue {
            source: Arc::downgrade(&pipeline.store),
            history,
            original_genesis_epoch: genesis_epoch,
            acquired_epoch: target.epoch,
            target_next_offset,
            original_identity_present,
            original_authority,
            state_authority,
            group_state_authority,
        })
    }
}

#[cfg(test)]
mod tests;
