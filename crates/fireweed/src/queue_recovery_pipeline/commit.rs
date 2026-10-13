//! Owned commit publication. Only covered immutable original-receipt replay
//! returns an outcome; provisional planning never crosses the completion edge.
use super::*;
#[cfg(not(target_os = "linux"))]
use fireweed_core::queue_administration::AdministrativeOperation;
use fireweed_core::queue_contract::{CommitGroup, QueueLimits, ReceiptLookup, encoded_len};
use fireweed_engine::{
    Clock,
    queue_memory::{administrative_groups_preflight, incoming_headers},
};
use fireweed_turso::queue_projection::{
    AdministrativePreparationInput, AdmittedCommitReceipts, CommitPreparationInput,
};

struct CommitTask {
    _slot: OwnedSemaphorePermit,
    execution: CommitExecution,
}
pub(super) struct CommitExecution {
    pipeline: Arc<QueueRecoveryPipeline>,
    queue: QueueAddress,
    group: CommitGroup,
    limits: QueueLimits,
    batch: Option<Arc<fireweed_core::queue_contract::CommitBatch>>,
    clock: Arc<dyn Clock>,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    /// Internal complete commit operation, not a public QueueStore activation.
    /// Once dispatched, cancellation cannot abandon publication or resolution.
    pub async fn commit(
        self: &Arc<Self>,
        queue: QueueAddress,
        group: CommitGroup,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedCommitReceipts> {
        let execution = CommitExecution::new(self.clone(), queue, group, limits, clock, job)?;
        let slot = Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = CommitTask {
            _slot: slot,
            execution,
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = task.execute().await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| unknown("owned commit task failed"))?
    }
}
impl CommitTask {
    async fn execute(self) -> QueueResult<AdmittedCommitReceipts> {
        let completion = OperationCompletion {
            _slot: self._slot,
            _job: self.execution.job.clone(),
        };
        let result = self.execution.execute().await;
        drop(completion);
        result
    }
}
impl CommitExecution {
    pub(super) fn new(
        pipeline: Arc<QueueRecoveryPipeline>,
        queue: QueueAddress,
        group: CommitGroup,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        group.preflight_for(&queue, &limits)?;
        administrative_groups_preflight(&queue, std::slice::from_ref(&group), &limits)?;
        #[cfg(not(target_os = "linux"))]
        if matches!(
            group.administration.as_ref().map(|a| &a.intent.operation),
            Some(AdministrativeOperation::SelectFirst(_))
        ) {
            return Err(QueueError::Invalid(
                "broad administrative commit not assembled".into(),
            ));
        }
        // Admit both input owners before native-stage cloning or any I/O.
        let headers = incoming_headers(&group)?
            .checked_mul(2)
            .and_then(|n| {
                n.checked_add(
                    (pipeline.scratch_root.path().as_os_str().len() as u64).checked_mul(4)?,
                )
            })
            .ok_or(QueueError::TooLarge)?;
        let input = job.owned_buffer(
            encoded_len(&(&queue, &group))?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        Ok(Self {
            pipeline,
            queue,
            group,
            limits,
            batch: None,
            clock,
            _input: input,
            job,
        })
    }
    pub(super) fn with_batch(
        mut self,
        batch: Arc<fireweed_core::queue_contract::CommitBatch>,
    ) -> Self {
        self.batch = Some(batch);
        self
    }
    fn batch_position(
        &self,
        recovered: &RecoveredQueue,
    ) -> QueueResult<Option<fireweed_engine::queue_original_authority::AdmittedOriginalPosition>>
    {
        let Some(batch) = &self.batch else {
            return Ok(None);
        };
        let authority = recovered
            .original_authority
            .as_ref()
            .ok_or_else(|| unknown("batch recovery proof missing"))?;
        let genesis = self
            .pipeline
            .original_genesis(&self.queue, recovered, &self.job)?;
        authority.validate(
            batch,
            &genesis,
            (
                recovered.target_next_offset,
                recovered.history.insertion_sequence(),
                recovered.history.historical_epoch(),
            ),
            &self.job,
        )?;
        let position = batch
            .groups
            .iter()
            .position(|group| group.request_id == self.group.request_id)
            .ok_or_else(|| unknown("original outside declared batch"))?;
        authority
            .position(position, &self.group.request_id)
            .map(Some)
    }
    fn bind_batch_original(&self, recovered: &mut RecoveredQueue) -> QueueResult<()> {
        if let Some(original) = self.batch_position(recovered)? {
            let genesis = self
                .pipeline
                .original_genesis(&self.queue, recovered, &self.job)?;
            recovered.original_identity_present = Some(
                original
                    .validate(
                        &self.queue,
                        &self.group.request_id,
                        &genesis,
                        (
                            recovered.target_next_offset,
                            recovered.history.insertion_sequence(),
                            recovered.history.historical_epoch(),
                        ),
                        &self.job,
                    )?
                    .is_some(),
            );
        }
        Ok(())
    }
    fn input(&self, recovered: &RecoveredQueue) -> QueueResult<CommitPreparationInput> {
        Ok(CommitPreparationInput {
            original: self.batch_position(recovered)?,
            genesis: self
                .pipeline
                .original_genesis(&self.queue, recovered, &self.job)?,
            queue: self.queue.clone(),
            group: self.group.clone(),
            limits: self.limits,
            required_next_offset: recovered.target_next_offset,
            required_insertion_sequence: recovered.history.insertion_sequence(),
            required_last_record_epoch: recovered.history.historical_epoch(),
        })
    }
    async fn replay(&self, recovered: &RecoveredQueue) -> QueueResult<AdmittedCommitReceipts> {
        self.pipeline
            .projection
            .replay_commit(self.input(recovered)?, &self.job, &self.pipeline.workers)
            .await
    }
    async fn replay_covered(
        &self,
        recovered: &RecoveredQueue,
    ) -> QueueResult<AdmittedCommitReceipts> {
        let result = self.replay(recovered).await;
        #[cfg(test)]
        {
            let pause = self.pipeline.after_commit_replay.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        // Confirm every result against a fresh authoritative cut. A concurrent
        // publication can change original-identity visibility after recovery.
        if !self
            .pipeline
            .head_matches(&self.queue, recovered, &self.job)
            .await?
        {
            return Err(unknown("commit replay authority advanced"));
        }
        result
    }
    pub(super) async fn execute(self) -> QueueResult<AdmittedCommitReceipts> {
        self.execute_covered(None)
            .await
            .map(|(response, _)| response)
    }
    pub(super) async fn execute_covered(
        self,
        mut covered: Option<RecoveredQueue>,
    ) -> QueueResult<(AdmittedCommitReceipts, RecoveredQueue)> {
        for _ in 0..32 {
            let mut recovered = match covered.take() {
                Some(value) => {
                    #[cfg(test)]
                    {
                        let pause = self.pipeline.before_handoff_check.lock().unwrap().take();
                        if let Some((entered, release)) = pause {
                            entered.notify_one();
                            release.notified().await;
                        }
                    }
                    if self
                        .pipeline
                        .head_matches(&self.queue, &value, &self.job)
                        .await
                        .unwrap_or(false)
                    {
                        #[cfg(test)]
                        {
                            let pause = self.pipeline.after_handoff_check.lock().unwrap().take();
                            if let Some((entered, release)) = pause {
                                entered.notify_one();
                                release.notified().await;
                            }
                        }
                        value
                    } else {
                        drop(value);
                        super::mutation::recover_original(&self).await?
                    }
                }
                None => super::mutation::recover_original(&self).await?,
            };
            self.bind_batch_original(&mut recovered)?;
            let input = self.input(&recovered)?;
            let prepared = if input.group.administration.is_some() {
                self.pipeline
                    .projection
                    .prepare_administration(
                        AdministrativePreparationInput {
                            commit: input,
                            scratch_root: self.pipeline.scratch_root.clone(),
                            scratch_quota: Arc::clone(&self.pipeline.scratch_quota),
                        },
                        recovered.acquired_epoch,
                        self.pipeline.hard_record_bytes,
                        self.clock.clone(),
                        &self.job,
                        &self.pipeline.workers,
                    )
                    .await
            } else {
                self.pipeline
                    .projection
                    .prepare_ordinary_commit(
                        input,
                        recovered.acquired_epoch,
                        self.pipeline.hard_record_bytes,
                        self.clock.clone(),
                        &self.job,
                        &self.pipeline.workers,
                    )
                    .await
            };
            let prepared = super::mutation::check_preparation(&recovered, prepared)?;
            let Some(prepared) = prepared else {
                return match super::mutation::resolve_submission(&self, None, Some(recovered))
                    .await?
                {
                    super::mutation::Submission::Resolved(response, coverage) => {
                        Ok((response, coverage))
                    }
                    super::mutation::Submission::Reprepare(_) => {
                        Err(unknown("known original cannot be reprepared"))
                    }
                };
            };
            let epoch = prepared.writer_epoch();
            let (record, provisional, expected_head) = prepared.into_parts();
            drop(provisional);
            drop(recovered);
            let namespace = self
                .pipeline
                .store
                .bind_api007_namespace(&self.queue, &self.job)
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
            match super::mutation::resolve_submission(&self, Some(epoch), None).await? {
                super::mutation::Submission::Resolved(response, coverage) => {
                    return Ok((response, coverage));
                }
                super::mutation::Submission::Reprepare(next) => covered = Some(next),
            }
        }
        Err(unknown("mutation contention retry exhausted"))
    }
}

impl super::mutation::OriginalMutation for CommitExecution {
    type Output = AdmittedCommitReceipts;
    fn pipeline(&self) -> &Arc<QueueRecoveryPipeline> {
        &self.pipeline
    }
    fn address(&self) -> &QueueAddress {
        &self.queue
    }
    fn identity(&self) -> (u8, &str) {
        (2, &self.group.request_id)
    }
    fn job(&self) -> &Arc<QueueReservation> {
        &self.job
    }
    fn recover<'a>(&'a self) -> fireweed_engine::queue_contract::QueueFuture<'a, RecoveredQueue> {
        Box::pin(async move {
            let mut recovered = if let Some(batch) = &self.batch {
                self.pipeline.recover_batch(batch, self.job.clone()).await?
            } else {
                self.pipeline
                    .recover_request(
                        self.queue.clone(),
                        2,
                        self.group.request_id.clone(),
                        self.job.clone(),
                    )
                    .await?
            };
            self.bind_batch_original(&mut recovered)?;
            Ok(recovered)
        })
    }
    fn replay<'a>(
        &'a self,
        recovered: &'a RecoveredQueue,
    ) -> fireweed_engine::queue_contract::QueueFuture<'a, Option<Self::Output>> {
        Box::pin(async move {
            let response = self.replay_covered(recovered).await?;
            match response.receipts() {
                [ReceiptLookup::Found(_)] => Ok(Some(response)),
                [ReceiptLookup::Absent] => Ok(None),
                [ReceiptLookup::Expired] => Err(QueueError::RequestExpired),
                _ => Err(unknown("invalid original commit resolution")),
            }
        })
    }
}
