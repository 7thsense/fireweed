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
    pipeline: Arc<QueueRecoveryPipeline>,
    queue: QueueAddress,
    group: CommitGroup,
    limits: QueueLimits,
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
                n.checked_add((self.scratch_root.path().as_os_str().len() as u64).checked_mul(4)?)
            })
            .ok_or(QueueError::TooLarge)?;
        let input = job.owned_buffer(
            encoded_len(&(&queue, &group))?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let slot = Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = CommitTask {
            _slot: slot,
            pipeline: Arc::clone(self),
            queue,
            group,
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
            .map_err(|_| unknown("owned commit task failed"))?
    }
}
impl CommitTask {
    fn input(&self, recovered: &RecoveredQueue) -> QueueResult<CommitPreparationInput> {
        Ok(CommitPreparationInput {
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
    async fn resolve(
        &self,
        mut covered: Option<RecoveredQueue>,
    ) -> QueueResult<AdmittedCommitReceipts> {
        // Retry original-identity resolution only; never replan after submission.
        for _ in 0..3 {
            let recovered = match covered.take() {
                Some(recovered) => recovered,
                None => match self
                    .pipeline
                    .recover(self.queue.clone(), self.job.clone())
                    .await
                {
                    Ok(recovered) => recovered,
                    Err(_) => continue,
                },
            };
            match self.replay_covered(&recovered).await {
                Ok(response) => match response.receipts().first() {
                    Some(ReceiptLookup::Found(_)) => return Ok(response),
                    Some(ReceiptLookup::Expired) => return Err(QueueError::RequestExpired),
                    _ => {}
                },
                Err(error @ (QueueError::RequestConflict | QueueError::RequestExpired)) => {
                    return Err(error);
                }
                _ => {}
            }
        }
        Err(unknown(
            "commit publication has no resolved original response",
        ))
    }
    async fn execute(self) -> QueueResult<AdmittedCommitReceipts> {
        let recovered = self
            .pipeline
            .recover(self.queue.clone(), self.job.clone())
            .await?;
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
                .await?
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
                .await?
        };
        let Some(prepared) = prepared else {
            return self.resolve(Some(recovered)).await;
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
        self.resolve(None).await
    }
}
