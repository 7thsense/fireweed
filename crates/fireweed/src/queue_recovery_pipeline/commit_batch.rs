//! Commit batches: classify every group from the projection, publish the first
//! unresolved group (or ordinary pack), apply it, and repeat until every group
//! has its stored outcome. A request id that is already stored is its outcome.
use super::owner::{Exit, Plan, Tenure, settle};
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
    queue: &QueueAddress,
    group: &CommitGroup,
    limits: QueueLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<CommitPreparationInput> {
    let (next, sequence, epoch) = t.point();
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
/// Prepare one group against the projection and publish it. `Ok(false)` means
/// the group's request id is already stored.
async fn publish_group(
    p: &QueueRecoveryPipeline,
    t: &mut Tenure,
    queue: &QueueAddress,
    group: &CommitGroup,
    limits: QueueLimits,
    clock: &Arc<dyn Clock>,
    job: &Arc<QueueReservation>,
) -> Result<QueueResult<bool>, Exit> {
    let input = match preparation(p, t, queue, group, limits, job) {
        Ok(input) => input,
        Err(error) => return Ok(Err(error)),
    };
    let prepared = settle(if group.administration.is_some() {
        p.projection
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
        p.projection
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
        Ok(None) => Ok(Ok(false)),
        Ok(Some(prepared)) => {
            let (record, provisional, expected_head) = prepared.into_parts();
            drop(provisional);
            t.publish(p, record, expected_head, job).await?;
            Ok(Ok(true))
        }
    }
}

impl Plan for GroupPlan {
    type Output = AdmittedCommitReceipts;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.queue
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            if let Err(error) = publish_group(p, t, &self.queue, &self.group, limits, clock, job)
                .await?
            {
                return Ok(Err(error));
            }
            let input = match preparation(p, t, &self.queue, &self.group, limits, job) {
                Ok(input) => input,
                Err(error) => return Ok(Err(error)),
            };
            match settle(p.projection.replay_commit(input, job, &p.workers).await)? {
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

impl BatchPlan {
    async fn classify(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> Result<QueueResult<AdmittedCommitBatchReplay>, Exit> {
        let (next, sequence, epoch) = t.point();
        let genesis = match t.genesis_record(p, &self.request.queue, job) {
            Ok(genesis) => genesis,
            Err(error) => return Ok(Err(error)),
        };
        settle(
            p.projection
                .replay_commit_batch(
                    CommitBatchReplayInput {
                        genesis,
                        request: self.request.as_ref().clone(),
                        limits,
                        response_max_bytes: limits.max_response_bytes,
                        required_next_offset: next,
                        required_insertion_sequence: sequence,
                        required_last_record_epoch: epoch,
                    },
                    job,
                    &p.workers,
                )
                .await,
        )
    }
    /// Publish one ordinary pack starting at `position`. `Ok(false)` means the
    /// first group exceeds the physical record limit and must go alone.
    async fn pack(
        &self,
        p: &QueueRecoveryPipeline,
        t: &mut Tenure,
        position: usize,
        known: &[Option<CommitReceipt>],
        limits: QueueLimits,
        clock: &Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
    ) -> Result<QueueResult<bool>, Exit> {
        let end = (position..self.request.groups.len())
            .find(|&q| known[q].is_some() || self.request.groups[q].administration.is_some())
            .unwrap_or(self.request.groups.len());
        let positions: Vec<usize> = (position..end).collect();
        let genesis = match t.genesis_record(p, &self.request.queue, job) {
            Ok(genesis) => genesis,
            Err(error) => return Ok(Err(error)),
        };
        let prepared = settle(
            p.projection
                .prepare_ordinary_pack(
                    CommitPackPreparationInput {
                        request: self.request.clone(),
                        positions: positions.clone(),
                        genesis,
                        limits,
                        required_progress: t.point(),
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
            Ok(OrdinaryPackPreparation::SingleGroup) => Ok(Ok(false)),
            // The classification was taken from this same projection.
            Ok(OrdinaryPackPreparation::Reclassify) => Err(Exit),
            Ok(OrdinaryPackPreparation::Prepared(prepared)) => {
                // The physical limit may accept only a strict prefix of the input.
                let submitted = prepared.response().positions();
                if submitted.first() != Some(&position)
                    || submitted.last().is_none_or(|&q| q >= end)
                    || submitted.windows(2).any(|w| w[0].checked_add(1) != Some(w[1]))
                {
                    return Err(Exit);
                }
                let (record, provisional, expected_head) = prepared.into_parts();
                drop(provisional);
                t.publish(p, record, expected_head, job).await?;
                Ok(Ok(true))
            }
        }
    }
}
impl Plan for BatchPlan {
    type Output = AdmittedQueueCommitBatch;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            let count = self.request.groups.len();
            let mut started = false;
            // After a possible durable prefix, a plain error would hide it.
            let after = |error: QueueError, started: bool| match error {
                error @ QueueError::OutcomeUnknown(_) => error,
                _ if started => unknown(
                    "batch failed after possible durable prefix; resolve original identities",
                ),
                error => error,
            };
            let mut replay = loop {
                let replay = match self.classify(p, t, limits, job).await? {
                    Ok(replay) => replay,
                    Err(error) => return Ok(Err(after(error, started))),
                };
                if replay.known().len() != count {
                    return Err(Exit);
                }
                let Some(position) = replay.known().iter().position(Option::is_none) else {
                    break replay;
                };
                let group = &self.request.groups[position];
                let mut published = false;
                if group.administration.is_none() {
                    match self
                        .pack(p, t, position, replay.known(), limits, clock, job)
                        .await?
                    {
                        Ok(done) => published = done,
                        Err(error) => return Ok(Err(after(error, started))),
                    }
                }
                if !published {
                    match publish_group(p, t, &self.request.queue, group, limits, clock, job)
                        .await?
                    {
                        // Classified unresolved, yet stored: the projection disagrees with itself.
                        Ok(false) => return Err(Exit),
                        Ok(true) => {}
                        Err(error) => return Ok(Err(after(error, started))),
                    }
                }
                started = true;
            };
            let containers = match job.owned_buffer(
                (count as u64)
                    .saturating_mul(
                        (std::mem::size_of::<CommitReceipt>()
                            + std::mem::size_of::<Option<CommitReceipt>>()) as u64,
                    )
                    .saturating_mul(3),
            ) {
                Ok(containers) => containers,
                Err(error) => return Ok(Err(after(error, started))),
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
