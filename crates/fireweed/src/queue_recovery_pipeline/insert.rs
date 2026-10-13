//! Owned insert publication. Only covered immutable original-receipt replay
//! returns an outcome; provisional planning never crosses the completion edge.
use super::*;
use fireweed_core::queue_contract::{InsertRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_memory::incoming_headers};
use fireweed_turso::queue_projection::{AdmittedInsertReplay, InsertReplayInput};

struct InsertTask {
    _slot: OwnedSemaphorePermit,
    pipeline: Arc<QueueRecoveryPipeline>,
    request: InsertRequest,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    /// Internal complete insert operation, not a public QueueStore activation.
    /// Once dispatched, cancellation cannot abandon publication or resolution.
    pub async fn insert(
        self: &Arc<Self>,
        request: InsertRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedInsertReplay> {
        request.preflight(&limits)?;
        // Admit both input owners before native-stage cloning or any I/O.
        let headers = incoming_headers(&request)?
            .checked_mul(2)
            .ok_or(QueueError::TooLarge)?;
        let input = job.owned_buffer(
            encoded_len(&request)?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let slot = Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = InsertTask {
            _slot: slot,
            pipeline: Arc::clone(self),
            request,
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
            .map_err(|_| unknown("owned insert task failed"))?
    }
}
impl InsertTask {
    fn input(&self, recovered: &RecoveredQueue) -> QueueResult<InsertReplayInput> {
        Ok(InsertReplayInput {
            genesis: self
                .pipeline
                .original_genesis(&self.request.queue, recovered, &self.job)?,
            request: self.request.clone(),
            limits: self.limits,
            required_next_offset: recovered.target_next_offset,
            required_insertion_sequence: recovered.history.insertion_sequence(),
            required_last_record_epoch: recovered.history.historical_epoch(),
        })
    }
    async fn replay(
        &self,
        recovered: &RecoveredQueue,
    ) -> QueueResult<Option<AdmittedInsertReplay>> {
        self.pipeline
            .projection
            .replay_insert(self.input(recovered)?, &self.job, &self.pipeline.workers)
            .await
    }
    async fn replay_covered(
        &self,
        recovered: &RecoveredQueue,
    ) -> QueueResult<Option<AdmittedInsertReplay>> {
        #[cfg(test)]
        {
            let pause = self.pipeline.before_insert_replay.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        let result = self.replay(recovered).await;
        // Confirm every result against a fresh authoritative cut. A concurrent
        // publication can change original-identity visibility after recovery.
        if !self
            .pipeline
            .head_matches(&self.request.queue, recovered, &self.job)
            .await?
        {
            return Err(unknown("insert replay authority advanced"));
        }
        result
    }
    async fn execute(self) -> QueueResult<AdmittedInsertReplay> {
        let mut covered = None;
        for _ in 0..32 {
            let recovered = match covered.take() {
                Some(value) => value,
                None => super::mutation::recover_original(&self).await?,
            };
            let prepared = self
                .pipeline
                .projection
                .prepare_insert(
                    self.input(&recovered)?,
                    recovered.acquired_epoch,
                    self.pipeline.hard_record_bytes,
                    self.clock.clone(),
                    &self.job,
                    &self.pipeline.workers,
                )
                .await;
            let prepared = super::mutation::check_preparation(&recovered, prepared)?;
            let Some(prepared) = prepared else {
                return match super::mutation::resolve_submission(&self, None, Some(recovered))
                    .await?
                {
                    super::mutation::Submission::Resolved(response, _) => Ok(response),
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
                .bind_api007_namespace(&self.request.queue, &self.job)
                .await?;
            #[cfg(test)]
            {
                let pause = self.pipeline.before_insert_append.lock().unwrap().clone();
                if let Some((entered, release)) = pause {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            self.pipeline
                .publish_prepared(namespace, record, expected_head, epoch, &self.job)
                .await?;
            match super::mutation::resolve_submission(&self, Some(epoch), None).await? {
                super::mutation::Submission::Resolved(response, _) => return Ok(response),
                super::mutation::Submission::Reprepare(next) => covered = Some(next),
            }
        }
        Err(unknown("mutation contention retry exhausted"))
    }
}

impl super::mutation::OriginalMutation for InsertTask {
    type Output = AdmittedInsertReplay;
    fn pipeline(&self) -> &Arc<QueueRecoveryPipeline> {
        &self.pipeline
    }
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn identity(&self) -> (u8, &str) {
        (0, &self.request.request_id)
    }
    fn job(&self) -> &Arc<QueueReservation> {
        &self.job
    }
    fn replay<'a>(
        &'a self,
        recovered: &'a RecoveredQueue,
    ) -> fireweed_engine::queue_contract::QueueFuture<'a, Option<Self::Output>> {
        Box::pin(self.replay_covered(recovered))
    }
}
