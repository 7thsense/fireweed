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
};

/// DTOs drop before every retained classification/fresh-output owner and guard.
pub struct AdmittedQueueCommitBatch {
    receipts: Vec<CommitReceipt>,
    // Its full-batch input/header charge covers every position, including
    // initially absent identities copied into racing conflict/expiry statuses.
    classification: AdmittedCommitBatchReplay,
    fresh: Vec<AdmittedCommitReceipts>,
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
        Box::pin(async move { pipeline.recover_batch(&self.request, job).await })
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
                        + std::mem::size_of::<AdmittedCommitReceipts>()) as u64,
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
        let mut unresolved_started = false;
        for (position, group) in self.request.groups.iter().enumerate() {
            let receipt = if let Some(known) = output.classification.take_known(position)? {
                known
            } else {
                // An attempt may submit and then resolve as expired. Do not infer
                // absence of a durable prefix from the final positional status.
                unresolved_started = true;
                let result = match CommitExecution::new(
                    self.pipeline.clone(),
                    self.request.queue.clone(),
                    group.clone(),
                    self.limits,
                    self.clock.clone(),
                    self.job.clone(),
                ) {
                    Ok(execution) => {
                        execution
                            .with_batch(self.request.clone())
                            .execute_covered(coverage.take())
                            .await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok((fresh, next_coverage)) => {
                        coverage = Some(next_coverage);
                        // Keep the whole owner before moving any receipt DTO.
                        output.fresh.push(fresh);
                        output
                            .fresh
                            .last_mut()
                            .ok_or_else(|| unknown("fresh batch owner disappeared"))?
                            .take_original()?
                    }
                    Err(QueueError::RequestConflict) => CommitReceipt {
                        request_id: group.request_id.clone(),
                        outcome: GroupOutcome::RequestConflict,
                    },
                    Err(QueueError::RequestExpired) => CommitReceipt {
                        request_id: group.request_id.clone(),
                        outcome: GroupOutcome::RequestExpired,
                    },
                    Err(_) if unresolved_started => {
                        return Err(unknown(
                            "batch failed after possible durable prefix; resolve original identities",
                        ));
                    }
                    Err(error) => return Err(error),
                }
            };
            output.receipts.push(receipt);
        }
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
