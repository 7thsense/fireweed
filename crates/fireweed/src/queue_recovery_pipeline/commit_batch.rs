//! One owned batch permit: validate all siblings and resolve all originals before
//! new execution. Private execution cores reuse that permit at capacity one.
use super::*;
use super::{
    commit::CommitExecution,
    read::{CoveredRead, ReadExecution},
};
use fireweed_core::queue_contract::{
    CommitBatch, CommitReceipt, GroupOutcome, QueueLimits, RequestPreflight, encoded_len,
};
use fireweed_engine::{
    Clock,
    queue_contract::QueueFuture,
    queue_memory::{IncomingHeaders, administrative_envelope_preflight, incoming_headers},
};
use fireweed_turso::queue_projection::{
    AdmittedCommitBatchReplay, AdmittedCommitReceipts, CommitBatchReplayInput,
    CommitPackPreparationInput, OrdinaryPackPreparation,
};

/// DTOs drop before every retained classification/fresh-output owner and guard.
pub struct AdmittedQueueCommitBatch {
    receipts: Vec<CommitReceipt>,
    // Its full-batch input/header charge covers every position, including
    // initially absent identities copied into racing conflict/expiry statuses.
    classification: AdmittedCommitBatchReplay,
    fresh: Vec<FreshOwner>,
    _containers: OwnedQueueBufferCharge,
}
impl AdmittedQueueCommitBatch {
    pub fn into_owned(
        self,
    ) -> fireweed_engine::queue_contract::OwnedQueueValue<Vec<CommitReceipt>, impl Send> {
        fireweed_engine::queue_contract::OwnedQueueValue::new(
            self.receipts,
            (self.classification, self.fresh, self._containers),
        )
    }

    pub fn receipts(&self) -> &[CommitReceipt] {
        &self.receipts
    }
}
// Every moved DTO retains its producing admitted owner until output destruction.
enum FreshOwner {
    Single(AdmittedCommitReceipts),
    Batch(AdmittedCommitBatchReplay),
}
// This status describes the actual submitted prefix, never one arbitrary original.
enum PackResolution {
    AllKnown,
    FencedAbsent,
}
enum PackResult {
    Single(RecoveredQueue),
    Classified {
        replay: AdmittedCommitBatchReplay,
        coverage: RecoveredQueue,
        resolution: Option<PackResolution>,
    },
}
struct BatchRead {
    request: Arc<CommitBatch>,
}
impl IncomingHeaders for BatchRead {
    fn headers(&self) -> QueueResult<u64> {
        self.request.headers()
    }
}
impl CoveredRead for BatchRead {
    type Output = AdmittedCommitBatchReplay;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn encoded_size(&self) -> QueueResult<u64> {
        encoded_len(self.request.as_ref())
    }
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.request.preflight(limits)?;
        administrative_envelope_preflight(&self.request, limits)
    }
    fn recover<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        job: Arc<QueueReservation>,
    ) -> QueueFuture<'a, RecoveredQueue> {
        Box::pin(async move { pipeline.recover_batch(self.request.clone(), job).await })
    }
    fn read<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        recovered: &'a RecoveredQueue,
        limits: QueueLimits,
        job: &'a Arc<QueueReservation>,
    ) -> QueueFuture<'a, Self::Output> {
        Box::pin(async move {
            pipeline
                .projection
                .replay_commit_batch(
                    CommitBatchReplayInput {
                        originals: recovered
                            .original_authority
                            .clone()
                            .ok_or_else(|| unknown("batch original authority missing"))?,
                        genesis: pipeline.original_genesis(&self.request.queue, recovered, job)?,
                        request: self.request.as_ref().clone(),
                        limits,
                        response_max_bytes: limits.max_response_bytes,
                        required_next_offset: recovered.target_next_offset,
                        required_insertion_sequence: recovered.history.insertion_sequence(),
                        required_last_record_epoch: recovered.history.historical_epoch(),
                    },
                    job,
                    &pipeline.workers,
                )
                .await
        })
    }
    #[cfg(test)]
    fn pause(
        &self,
        pipeline: &QueueRecoveryPipeline,
    ) -> Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)> {
        pipeline.after_batch_read.lock().unwrap().take()
    }
}
struct BatchTask {
    _slot: OwnedSemaphorePermit,
    pipeline: Arc<QueueRecoveryPipeline>,
    request: Arc<CommitBatch>,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
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
        #[cfg(not(target_os = "linux"))]
        if request.groups.iter().any(|group| {
            matches!(
                group.administration.as_ref().map(|a| &a.intent.operation),
                Some(fireweed_core::queue_administration::AdministrativeOperation::SelectFirst(_))
            )
        }) {
            return Err(QueueError::Invalid(
                "broad administrative commit not assembled".into(),
            ));
        }
        let headers = incoming_headers(&request)?;
        let input = job.owned_buffer(
            encoded_len(&request)?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers.checked_mul(2)?))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let slot = self
            .operation_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = BatchTask {
            _slot: slot,
            pipeline: self.clone(),
            request: Arc::new(request),
            limits,
            clock,
            _input: input,
            job,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = task.execute().await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| unknown("owned commit batch task failed"))?
    }
}
impl BatchTask {
    async fn execute(self) -> QueueResult<AdmittedQueueCommitBatch> {
        let (classification, coverage) = ReadExecution::new(
            self.pipeline.clone(),
            BatchRead {
                request: self.request.clone(),
            },
            self.limits,
            self.job.clone(),
        )?
        .execute_with_coverage()
        .await?;
        let mut coverage = Some(coverage);
        let count = self.request.groups.len();
        if classification.known().len() != count {
            return Err(unknown("batch classification count changed"));
        }
        let containers = self.job.owned_buffer(
            (count as u64)
                .checked_mul(
                    (std::mem::size_of::<CommitReceipt>()
                        + std::mem::size_of::<FreshOwner>()
                        + std::mem::size_of::<Option<CommitReceipt>>()) as u64,
                )
                .and_then(|n| n.checked_mul(3))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let mut output = AdmittedQueueCommitBatch {
            receipts: Vec::with_capacity(count),
            classification,
            fresh: Vec::with_capacity(count),
            _containers: containers,
        };
        // Finalized positions are distinct from replay slots consumed by take_known.
        let mut finalized: Vec<Option<CommitReceipt>> = (0..count)
            .map(|position| output.classification.take_known(position))
            .collect::<QueueResult<_>>()?;
        let mut unresolved_started = false;
        let mut contention = 0usize;
        while let Some(position) = finalized.iter().position(Option::is_none) {
            let group = &self.request.groups[position];
            unresolved_started = true;
            let result = async {
                if group.administration.is_none() {
                    let end = (position..count)
                        .find(|&p| {
                            finalized[p].is_some()
                                || self.request.groups[p].administration.is_some()
                        })
                        .unwrap_or(count);
                    // The preadmitted container charge includes this bounded usize vector.
                    let positions = (position..end).collect();
                    match self
                        .pack(
                            positions,
                            coverage
                                .take()
                                .ok_or_else(|| unknown("batch coverage missing"))?,
                        )
                        .await?
                    {
                        PackResult::Single(next) => coverage = Some(next),
                        PackResult::Classified {
                            replay,
                            coverage: next,
                            resolution,
                        } => {
                            let before = finalized.iter().filter(|r| r.is_some()).count();
                            // Retain the owner before transferring any newly finalized DTO.
                            output.fresh.push(FreshOwner::Batch(replay));
                            let FreshOwner::Batch(owner) = output.fresh.last_mut().unwrap() else {
                                unreachable!()
                            };
                            for (p, receipt) in finalized.iter_mut().enumerate() {
                                if receipt.is_none() {
                                    *receipt = owner.take_known(p)?;
                                }
                            }
                            let after = finalized.iter().filter(|r| r.is_some()).count();
                            if after == before {
                                output.fresh.pop();
                            }
                            if matches!(resolution, Some(PackResolution::AllKnown))
                                && after == before
                            {
                                return Err(unknown("resolved pack finalized no positions"));
                            }
                            coverage = Some(next);
                            contention = if after > before { 0 } else { contention + 1 };
                            if contention >= 32 {
                                return Err(unknown("batch pack contention exhausted"));
                            }
                            return Ok(());
                        }
                    }
                }
                let result = CommitExecution::new(
                    self.pipeline.clone(),
                    self.request.queue.clone(),
                    group.clone(),
                    self.limits,
                    self.clock.clone(),
                    self.job.clone(),
                )?
                .with_batch(self.request.clone())
                .execute_batch_covered(coverage.take())
                .await?;
                match result {
                    super::commit::BatchCommitResult::Found(fresh, next) => {
                        output.fresh.push(FreshOwner::Single(fresh));
                        let FreshOwner::Single(owner) = output.fresh.last_mut().unwrap() else {
                            unreachable!()
                        };
                        finalized[position] = Some(owner.take_original()?);
                        coverage = Some(next);
                    }
                    super::commit::BatchCommitResult::Classified(error, next) => {
                        let outcome = match error {
                            QueueError::RequestConflict => GroupOutcome::RequestConflict,
                            QueueError::RequestExpired => GroupOutcome::RequestExpired,
                            _ => return Err(unknown("invalid single batch classification")),
                        };
                        finalized[position] = Some(CommitReceipt {
                            request_id: group.request_id.clone(),
                            outcome,
                        });
                        coverage = Some(next);
                    }
                }
                contention = 0;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => {}
                Err(_) => {
                    return Err(unknown(
                        "batch failed after possible durable prefix; resolve original identities",
                    ));
                }
            }
        }
        output.receipts = finalized
            .into_iter()
            .map(|r| r.ok_or_else(|| unknown("unresolved batch output")))
            .collect::<QueueResult<_>>()?;
        if encoded_len(&output.receipts)? > self.limits.max_response_bytes {
            return Err(if unresolved_started {
                unknown(
                    "batch response exceeded capacity after possible durable prefix; resolve original identities",
                )
            } else {
                QueueError::TooLarge
            });
        }
        Ok(output)
    }
}

impl BatchTask {
    async fn classify_after(
        &self,
        mut previous: RecoveredQueue,
        submitted: Option<(&[usize], u64)>,
    ) -> QueueResult<(AdmittedCommitBatchReplay, RecoveredQueue)> {
        let read = BatchRead {
            request: self.request.clone(),
        };
        for _ in 0..3 {
            let recovered = self
                .pipeline
                .recover_batch_after(self.request.clone(), previous, self.job.clone())
                .await?;
            let result = read
                .read(&self.pipeline, &recovered, self.limits, &self.job)
                .await;
            #[cfg(test)]
            {
                let pause = self.pipeline.after_commit_replay.lock().unwrap().take();
                if let Some((entered, release)) = pause {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            if self
                .pipeline
                .head_matches(&self.request.queue, &recovered, &self.job)
                .await?
                && let Ok(replay) = result
                && submitted.is_none_or(|(positions, epoch)| {
                    positions
                        .iter()
                        .all(|&p| replay.known().get(p).is_some_and(Option::is_some))
                        || recovered.acquired_epoch > epoch
                })
            {
                return Ok((replay, recovered));
            }
            previous = recovered;
        }
        Err(unknown("pack classification has no current complete cut"))
    }
    async fn pack(
        &self,
        positions: Vec<usize>,
        coverage: RecoveredQueue,
    ) -> QueueResult<PackResult> {
        #[cfg(test)]
        {
            let pause = self.pipeline.before_handoff_check.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        if !self
            .pipeline
            .head_matches(&self.request.queue, &coverage, &self.job)
            .await?
        {
            let (replay, coverage) = self.classify_after(coverage, None).await?;
            return Ok(PackResult::Classified {
                replay,
                coverage,
                resolution: None,
            });
        }
        #[cfg(test)]
        {
            let pause = self.pipeline.after_handoff_check.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        let requested_start = positions[0];
        let requested_end = positions
            .len()
            .checked_add(requested_start)
            .ok_or(QueueError::TooLarge)?;
        let input = CommitPackPreparationInput {
            request: self.request.clone(),
            positions,
            originals: coverage
                .original_authority
                .clone()
                .ok_or_else(|| unknown("pack original authority missing"))?,
            states: coverage
                .group_state_authority
                .clone()
                .ok_or_else(|| unknown("pack state authority missing"))?,
            genesis: self
                .pipeline
                .original_genesis(&self.request.queue, &coverage, &self.job)?,
            limits: self.limits,
            required_progress: (
                coverage.target_next_offset,
                coverage.history.insertion_sequence(),
                coverage.history.historical_epoch(),
            ),
        };
        let prepared = match self
            .pipeline
            .projection
            .prepare_ordinary_pack(
                input,
                coverage.acquired_epoch,
                self.pipeline.hard_record_bytes,
                self.clock.clone(),
                &self.job,
                &self.pipeline.workers,
            )
            .await?
        {
            OrdinaryPackPreparation::SingleGroup => return Ok(PackResult::Single(coverage)),
            OrdinaryPackPreparation::Reclassify => {
                let (replay, coverage) = self.classify_after(coverage, None).await?;
                return Ok(PackResult::Classified {
                    replay,
                    coverage,
                    resolution: None,
                });
            }
            OrdinaryPackPreparation::Prepared(prepared) => prepared,
        };
        // The physical byte limit may accept only a strict prefix of the input.
        let submitted = prepared.response().positions();
        if submitted.is_empty()
            || submitted[0] != requested_start
            || submitted.last().is_none_or(|&p| p >= requested_end)
            || submitted
                .windows(2)
                .any(|p| p[0].checked_add(1) != Some(p[1]))
        {
            return Err(unknown("invalid prepared pack prefix"));
        }
        let _submitted_charge = self.job.owned_buffer(
            (submitted.len() as u64)
                .checked_mul(std::mem::size_of::<usize>() as u64)
                .ok_or(QueueError::TooLarge)?,
        )?;
        let submitted = submitted.to_vec();
        let epoch = prepared.writer_epoch();
        let (record, provisional, expected_head) = prepared.into_parts();
        drop(provisional);
        let namespace = self
            .pipeline
            .store
            .bind_api007_namespace(&self.request.queue, &self.job)
            .await?;
        #[cfg(test)]
        {
            let pause = self.pipeline.before_commit_append.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        self.pipeline
            .publish_prepared(namespace, record, expected_head, epoch, &self.job)
            .await?;
        let (replay, next) = self
            .classify_after(coverage, Some((&submitted, epoch)))
            .await?;
        let all_known = submitted
            .iter()
            .all(|&p| replay.known().get(p).is_some_and(Option::is_some));
        let resolution = if all_known {
            PackResolution::AllKnown
        } else if next.acquired_epoch > epoch {
            PackResolution::FencedAbsent
        } else {
            return Err(unknown(
                "pack has no resolved or fenced-absent submitted prefix",
            ));
        };
        Ok(PackResult::Classified {
            replay,
            coverage: next,
            resolution: Some(resolution),
        })
    }
}
