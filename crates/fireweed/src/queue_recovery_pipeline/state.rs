//! Owned workflow state lookup with a subsequent authoritative head cut.
use super::*;
use fireweed_core::queue_contract::{QueueLimits, RequestPreflight, StateReadRequest, encoded_len};
use fireweed_engine::queue_memory::incoming_headers;
use fireweed_turso::queue_projection::{AdmittedStateRecords, StateReadInput};
struct StateTask {
    _slot: OwnedSemaphorePermit,
    pipeline: Arc<QueueRecoveryPipeline>,
    request: StateReadRequest,
    limits: QueueLimits,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    /// Internal owned operation, not public QueueStore construction/activation.
    pub async fn read_state(
        self: &Arc<Self>,
        request: StateReadRequest,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedStateRecords> {
        request.preflight(&limits)?;
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
        let slot = self
            .operation_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = StateTask {
            _slot: slot,
            pipeline: self.clone(),
            request,
            limits,
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
            .map_err(|_| unknown("owned state task failed"))?
    }
}
impl StateTask {
    async fn execute(self) -> QueueResult<AdmittedStateRecords> {
        for _ in 0..3 {
            let Ok(recovered) = self
                .pipeline
                .recover(self.request.queue.clone(), self.job.clone())
                .await
            else {
                continue;
            };
            let genesis = encode_checked_genesis(
                recovered.original_genesis_epoch,
                &self.request.queue,
                recovered.history.definition()?,
                recovered.history.cursor_secret()?,
                self.pipeline.hard_record_bytes,
                &self.job,
            )?;
            let result = self
                .pipeline
                .projection
                .read_state_records(
                    StateReadInput {
                        genesis,
                        request: self.request.clone(),
                        limits: self.limits,
                        required_next_offset: recovered.target_next_offset,
                        required_insertion_sequence: recovered.history.insertion_sequence(),
                        required_last_record_epoch: recovered.history.historical_epoch(),
                    },
                    &self.job,
                    &self.pipeline.workers,
                )
                .await;
            #[cfg(test)]
            {
                let pause = self.pipeline.after_state_read.lock().unwrap().take();
                if let Some((entered, release)) = pause {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            // An authoritative matching cut validates every classification,
            // including a complete absent vector or a response TooLarge.
            // Stale guarded output is dropped before the next recovery attempt.
            match self
                .pipeline
                .head_matches(&self.request.queue, &recovered, &self.job)
                .await
            {
                Ok(true) => return result,
                _ => drop(result),
            }
        }
        Err(unknown("state read has no resolved current coverage"))
    }
}
