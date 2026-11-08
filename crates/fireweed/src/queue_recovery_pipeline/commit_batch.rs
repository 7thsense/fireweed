//! Commit batches: classify every group against the iteration's batch, stage the
//! first unresolved group (or ordinary pack) as an entry, and repeat until every
//! group has its stored outcome or the record is full. A request id that is
//! already stored is its outcome. One group is one entry; N groups are N entries
//! of the same record, unless the record fills, in which case the rest go in the
//! next iteration's record.
use super::owner::{Batch, Exit, Plan, Staged, StagedEntry, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::{
    CommitBatch, CommitGroup, CommitReceipt, QueueLimits, ReceiptLookup, RequestPreflight,
    encoded_len,
};
use fireweed_engine::{
    Clock,
    queue_memory::{administrative_envelope_preflight, incoming_headers},
};
use fireweed_turso::queue_projection::{
    AdministrativePreparationInput, AdmittedCommitBatchReplay, AdmittedCommitReceipts,
    CommitBatchReplayInput, CommitPackPreparationInput, CommitPreparationInput,
    OrdinaryPackPreparation,
};
use futures::future::BoxFuture;

/// DTOs drop before the classification owner that backs them.
pub struct AdmittedQueueCommitBatch {
    receipts: Vec<CommitReceipt>,
    classification: AdmittedCommitBatchReplay,
    _containers: OwnedQueueBufferCharge,
}
impl AdmittedQueueCommitBatch {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<Vec<CommitReceipt>, impl Send> {
        fireweed_engine::queue_contract::OwnedQueueValue::new(
            self.receipts,
            (self.classification, self._containers),
        )
    }

    pub fn receipts(&self) -> &[CommitReceipt] {
        &self.receipts
    }
}

struct BatchPlan {
    request: Arc<CommitBatch>,
    /// Some entry of this batch was staged, in this iteration or an earlier
    /// one, so a prefix of its groups may be durable whatever happens next.
    started: std::sync::atomic::AtomicBool,
}
struct GroupPlan {
    queue: QueueAddress,
    group: CommitGroup,
}
impl QueueRecoveryPipeline {
    pub async fn commit_batch(
        self: &Arc<Self>,
        request: CommitBatch,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueueCommitBatch> {
        request.preflight(&limits)?;
        administrative_envelope_preflight(&request, &limits)?;
        let headers = incoming_headers(&request)?;
        let input = job.owned_buffer(
            encoded_len(&request)?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers.checked_mul(2)?))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        self.submit(
            BatchPlan {
                request: Arc::new(request),
                started: std::sync::atomic::AtomicBool::new(false),
            },
            limits,
            clock,
            job,
            input,
        )
        .await
    }
    /// One group is a batch of one; its stored receipt is the reply.
    pub async fn commit(
        self: &Arc<Self>,
        queue: QueueAddress,
        group: CommitGroup,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedCommitReceipts> {
        group.preflight_for(&queue, &limits)?;
        fireweed_engine::queue_memory::administrative_groups_preflight(
            &queue,
            std::slice::from_ref(&group),
            &limits,
        )?;
        let headers = incoming_headers(&group)?
            .checked_mul(2)
            .ok_or(QueueError::TooLarge)?;
        let input = job.owned_buffer(
            encoded_len(&(&queue, &group))?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        self.submit(GroupPlan { queue, group }, limits, clock, job, input)
            .await
    }
}

fn preparation(
    p: &QueueRecoveryPipeline,
    t: &Tenure,
    point: (u64, u64, u64),
    queue: &QueueAddress,
    group: &CommitGroup,
    limits: QueueLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<CommitPreparationInput> {
    let (next, sequence, epoch) = point;
    Ok(CommitPreparationInput {
        genesis: t.genesis_record(p, queue, job)?,
        queue: queue.clone(),
        group: group.clone(),
        limits,
        required_next_offset: next,
        required_insertion_sequence: sequence,
        required_last_record_epoch: epoch,
    })
}
/// What staging one group came to.
enum GroupStage {
    /// The group's request id is already stored.
    Known,
    Staged,
    Full,
}
fn staged_group(entry: StagedEntry) -> GroupStage {
    match entry {
        StagedEntry::Staged => GroupStage::Staged,
        StagedEntry::Full => GroupStage::Full,
        StagedEntry::TooLarge => unreachable!("handled by the caller"),
    }
}
/// Prepare one group against the batch and stage its entry.
#[allow(clippy::too_many_arguments)]
async fn stage_group(
    p: &QueueRecoveryPipeline,
    t: &Tenure,
    batch: &mut Batch,
    queue: &QueueAddress,
    group: &CommitGroup,
    limits: QueueLimits,
    clock: &Arc<dyn Clock>,
    job: &Arc<QueueReservation>,
) -> Result<QueueResult<GroupStage>, Exit> {
    let input = match preparation(p, t, batch.point(t), queue, group, limits, job) {
        Ok(input) => input,
        Err(error) => return Ok(Err(error)),
    };
    let prepared = settle(if group.administration.is_some() {
        batch
            .host(p, t)
            .await?
            .prepare_administration(
                AdministrativePreparationInput {
                    commit: input,
                    scratch_root: p.scratch_root.clone(),
                    scratch_quota: Arc::clone(&p.scratch_quota),
                },
                t.epoch,
                p.hard_record_bytes,
                clock.clone(),
                job,
                &p.workers,
            )
            .await
    } else {
        batch
            .host(p, t)
            .await?
            .prepare_ordinary_commit(
                input,
                t.epoch,
                p.hard_record_bytes,
                clock.clone(),
                job,
                &p.workers,
            )
            .await
    })?;
    match prepared {
        Err(error) => Ok(Err(error)),
        Ok(None) => Ok(Ok(GroupStage::Known)),
        Ok(Some(prepared)) => {
            let (record, provisional, expected_head) = prepared.into_parts();
            drop(provisional);
            match batch.stage(p, t, record, expected_head, job).await? {
                StagedEntry::TooLarge => Ok(Err(QueueError::TooLarge)),
                entry => Ok(Ok(staged_group(entry))),
            }
        }
    }
}

impl Plan for GroupPlan {
    type Output = AdmittedCommitReceipts;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.queue
    }
    fn stage<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a Tenure,
        batch: &'a mut Batch,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<Staged, Exit>> {
        Box::pin(async move {
            Ok(
                match stage_group(p, t, batch, &self.queue, &self.group, limits, clock, job).await?
                {
                    Err(error) => Staged::Rejected(error),
                    Ok(GroupStage::Known | GroupStage::Staged) => Staged::Pending,
                    Ok(GroupStage::Full) => Staged::Full { progressed: false },
                },
            )
        })
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        _clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            let input = match preparation(p, t, t.point(), &self.queue, &self.group, limits, job) {
                Ok(input) => input,
                Err(error) => return Ok(Err(error)),
            };
            match settle(t.projection().replay_commit(input, job, &p.workers).await)? {
                Err(error) => Ok(Err(error)),
                Ok(response) => match response.receipts() {
                    [ReceiptLookup::Found(_)] => Ok(Ok(response)),
                    [ReceiptLookup::Expired] => Ok(Err(QueueError::RequestExpired)),
                    _ => Err(Exit),
                },
            }
        })
    }
}

/// What staging one ordinary pack came to.
enum PackStage {
    Staged,
    Full,
    /// The first group exceeds the physical record limit and must go alone.
    SingleGroup,
}
impl BatchPlan {
    fn request_input(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        point: (u64, u64, u64),
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<CommitBatchReplayInput> {
        let (next, sequence, epoch) = point;
        let genesis = t.genesis_record(p, &self.request.queue, job)?;
        Ok(CommitBatchReplayInput {
            genesis,
            request: self.request.as_ref().clone(),
            limits,
            response_max_bytes: limits.max_response_bytes,
            required_next_offset: next,
            required_insertion_sequence: sequence,
            required_last_record_epoch: epoch,
        })
    }
    /// Classify every group against the batch's staged state.
    async fn classify_staged(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        batch: &mut Batch,
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> Result<QueueResult<AdmittedCommitBatchReplay>, Exit> {
        let input = match self.request_input(p, t, batch.point(t), limits, job) {
            Ok(input) => input,
            Err(error) => return Ok(Err(error)),
        };
        settle(
            batch
                .host(p, t)
                .await?
                .replay_commit_batch(input, job, &p.workers)
                .await,
        )
    }
    /// Classify every group against the committed projection.
    async fn classify(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> Result<QueueResult<AdmittedCommitBatchReplay>, Exit> {
        let input = match self.request_input(p, t, t.point(), limits, job) {
            Ok(input) => input,
            Err(error) => return Ok(Err(error)),
        };
        settle(
            t.projection()
                .replay_commit_batch(input, job, &p.workers)
                .await,
        )
    }
    /// Stage one ordinary pack of the groups at `positions`.
    #[allow(clippy::too_many_arguments)]
    async fn pack(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        batch: &mut Batch,
        positions: Vec<usize>,
        limits: QueueLimits,
        clock: &Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
    ) -> Result<QueueResult<PackStage>, Exit> {
        let position = positions[0];
        let end = position + positions.len();
        let genesis = match t.genesis_record(p, &self.request.queue, job) {
            Ok(genesis) => genesis,
            Err(error) => return Ok(Err(error)),
        };
        let progress = batch.point(t);
        let prepared = settle(
            batch
                .host(p, t)
                .await?
                .prepare_ordinary_pack(
                    CommitPackPreparationInput {
                        request: self.request.clone(),
                        positions: positions.clone(),
                        genesis,
                        limits,
                        required_progress: progress,
                    },
                    t.epoch,
                    p.hard_record_bytes,
                    clock.clone(),
                    job,
                    &p.workers,
                )
                .await,
        )?;
        match prepared {
            Err(error) => Ok(Err(error)),
            Ok(OrdinaryPackPreparation::SingleGroup) => Ok(Ok(PackStage::SingleGroup)),
            // The classification was taken from this same projection.
            Ok(OrdinaryPackPreparation::Reclassify) => Err(Exit),
            Ok(OrdinaryPackPreparation::Prepared(prepared)) => {
                // The physical limit may accept only a strict prefix of the input.
                let submitted = prepared.response().positions();
                if submitted.first() != Some(&position)
                    || submitted.last().is_none_or(|&q| q >= end)
                    || submitted
                        .windows(2)
                        .any(|w| w[0].checked_add(1) != Some(w[1]))
                {
                    return Err(Exit);
                }
                let (record, provisional, expected_head) = prepared.into_parts();
                drop(provisional);
                match batch.stage(p, t, record, expected_head, job).await? {
                    // Even alone the pack does not fit: let its first group try alone.
                    StagedEntry::TooLarge => Ok(Ok(PackStage::SingleGroup)),
                    StagedEntry::Staged => Ok(Ok(PackStage::Staged)),
                    StagedEntry::Full => Ok(Ok(PackStage::Full)),
                }
            }
        }
    }
    /// After a possible durable prefix, a plain error would hide it.
    fn after(&self, error: QueueError) -> QueueError {
        match error {
            error @ QueueError::OutcomeUnknown(_) => error,
            _ if self.started.load(std::sync::atomic::Ordering::SeqCst) => {
                unknown("batch failed after possible durable prefix; resolve original identities")
            }
            error => error,
        }
    }
}
impl Plan for BatchPlan {
    type Output = AdmittedQueueCommitBatch;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn stage<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a Tenure,
        batch: &'a mut Batch,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<Staged, Exit>> {
        Box::pin(async move {
            use std::sync::atomic::Ordering::SeqCst;
            let count = self.request.groups.len();
            let full = || Staged::Full {
                progressed: self.started.load(SeqCst),
            };
            loop {
                let replay = match self.classify_staged(p, t, batch, limits, job).await? {
                    Ok(replay) => replay,
                    Err(error) => return Ok(Staged::Rejected(self.after(error))),
                };
                if replay.known().len() != count {
                    return Err(Exit);
                }
                let Some(position) = replay.known().iter().position(Option::is_none) else {
                    return Ok(Staged::Pending);
                };
                let group = &self.request.groups[position];
                let mut staged = false;
                if group.administration.is_none() {
                    let known = replay.known();
                    let end = (position..count)
                        .find(|&q| {
                            known[q].is_some() || self.request.groups[q].administration.is_some()
                        })
                        .unwrap_or(count);
                    drop(replay);
                    match self
                        .pack(p, t, batch, (position..end).collect(), limits, clock, job)
                        .await?
                    {
                        Ok(PackStage::Staged) => staged = true,
                        Ok(PackStage::Full) => return Ok(full()),
                        Ok(PackStage::SingleGroup) => {}
                        Err(error) => return Ok(Staged::Rejected(self.after(error))),
                    }
                }
                if !staged {
                    match stage_group(p, t, batch, &self.request.queue, group, limits, clock, job)
                        .await?
                    {
                        // Classified unresolved, yet stored: the projection disagrees with itself.
                        Ok(GroupStage::Known) => return Err(Exit),
                        Ok(GroupStage::Staged) => {}
                        Ok(GroupStage::Full) => return Ok(full()),
                        Err(error) => return Ok(Staged::Rejected(self.after(error))),
                    }
                }
                self.started.store(true, SeqCst);
            }
        })
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        _clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            let count = self.request.groups.len();
            let started = self.started.load(std::sync::atomic::Ordering::SeqCst);
            let mut replay = match self.classify(p, t, limits, job).await? {
                Ok(replay) => replay,
                Err(error) => return Ok(Err(self.after(error))),
            };
            if replay.known().len() != count || replay.known().iter().any(Option::is_none) {
                return Err(Exit);
            }
            let containers = match job.owned_buffer(
                (count as u64)
                    .saturating_mul(
                        (std::mem::size_of::<CommitReceipt>()
                            + std::mem::size_of::<Option<CommitReceipt>>())
                            as u64,
                    )
                    .saturating_mul(3),
            ) {
                Ok(containers) => containers,
                Err(error) => return Ok(Err(self.after(error))),
            };
            let mut receipts = Vec::with_capacity(count);
            for position in 0..count {
                match replay.take_known(position) {
                    Ok(Some(receipt)) => receipts.push(receipt),
                    _ => return Err(Exit),
                }
            }
            match encoded_len(&receipts) {
                Ok(size) if size <= limits.max_response_bytes => {}
                Ok(_) if started => {
                    return Ok(Err(unknown(
                        "batch response exceeded capacity after possible durable prefix; resolve original identities",
                    )));
                }
                Ok(_) => return Ok(Err(QueueError::TooLarge)),
                Err(error) => return Ok(Err(error)),
            }
            Ok(Ok(AdmittedQueueCommitBatch {
                receipts,
                classification: replay,
                _containers: containers,
            }))
        })
    }
}
