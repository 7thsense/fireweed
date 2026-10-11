//! Internal claim orchestration. Only authoritative current-record replay may
//! return a response; a prepared token vector never crosses the completion edge.
use super::*;
use fireweed_core::queue_contract::{ClaimRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_memory::incoming_headers};
use fireweed_turso::queue_projection::{AdmittedClaimReplay, ClaimReplayInput};

struct ClaimTask {
    _slot: OwnedSemaphorePermit,
    pipeline: Arc<QueueRecoveryPipeline>,
    request: ClaimRequest,
    limits: QueueLimits,
    clock: Arc<dyn Clock>,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    /// Internal complete claim operation, not a public QueueStore activation.
    /// Once dispatched, cancellation cannot abandon publication or resolution.
    pub async fn claim(
        self: &Arc<Self>,
        request: ClaimRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedClaimReplay> {
        request.preflight(&limits)?;
        // Original input plus one simultaneous native-stage clone. Compact
        // wire values still own full Vec/BTree headers before native admission.
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
        let task = ClaimTask {
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
            .map_err(|_| unknown("owned claim task failed"))?
    }
}
impl ClaimTask {
    fn input(&self, recovered: &RecoveredQueue) -> QueueResult<ClaimReplayInput> {
        Ok(ClaimReplayInput {
            genesis: encode_checked_genesis(
                recovered.original_genesis_epoch,
                &self.request.queue,
                recovered.history.definition()?,
                recovered.history.cursor_secret()?,
                self.pipeline.hard_record_bytes,
                &self.job,
            )?,
            request: self.request.clone(),
            now: 0,
            limits: self.limits,
            required_next_offset: recovered.target_next_offset,
            required_insertion_sequence: recovered.history.insertion_sequence(),
            required_last_record_epoch: recovered.history.historical_epoch(),
        })
    }
    async fn replay(&self, recovered: &RecoveredQueue) -> QueueResult<Option<AdmittedClaimReplay>> {
        self.pipeline
            .projection
            .replay_claim_with_clock(
                self.input(recovered)?,
                self.clock.clone(),
                &self.job,
                &self.pipeline.workers,
            )
            .await
    }
    async fn replay_covered(
        &self,
        recovered: &RecoveredQueue,
    ) -> QueueResult<Option<AdmittedClaimReplay>> {
        #[cfg(test)]
        {
            let pause = self.pipeline.before_claim_replay.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
        }
        let result = self.replay(recovered).await;
        // Fresh time alone is insufficient: a renewal can publish after recovery
        // but before replay. Validate an authoritative cut after EVERY outcome,
        // including expiry, and discard stale classifications as well as responses.
        if !self
            .pipeline
            .head_matches(&self.request.queue, recovered, &self.job)
            .await?
        {
            return Err(unknown("claim replay authority advanced"));
        }
        result
    }
    async fn resolve(
        &self,
        mut covered: Option<RecoveredQueue>,
    ) -> QueueResult<AdmittedClaimReplay> {
        // Retry resolution only; never prepare another token set after submission.
        for _ in 0..3 {
            let recovered = match covered.take() {
                Some(recovered) => recovered,
                None => match self
                    .pipeline
                    .recover(self.request.queue.clone(), self.job.clone())
                    .await
                {
                    Ok(recovered) => recovered,
                    Err(_) => continue,
                },
            };
            match self.replay_covered(&recovered).await {
                Ok(Some(response)) => return Ok(response),
                Err(
                    error @ (QueueError::RequestConflict
                    | QueueError::RequestExpired
                    | QueueError::ClaimExpired),
                ) => return Err(error),
                _ => {}
            }
        }
        Err(unknown(
            "claim publication has no resolved original response",
        ))
    }
    async fn execute(self) -> QueueResult<AdmittedClaimReplay> {
        let recovered = self
            .pipeline
            .recover(self.request.queue.clone(), self.job.clone())
            .await?;
        let prepared = self
            .pipeline
            .projection
            .prepare_claim(
                self.input(&recovered)?,
                recovered.acquired_epoch,
                self.pipeline.hard_record_bytes,
                self.clock.clone(),
                &self.job,
                &self.pipeline.workers,
            )
            .await?;
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
            .bind_api007_namespace(&self.request.queue, &self.job)
            .await?;
        #[cfg(test)]
        {
            let pause = self.pipeline.before_claim_append.lock().unwrap().clone();
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
