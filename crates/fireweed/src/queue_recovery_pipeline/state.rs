//! Complete workflow-state read adapter; owned completion and freshness are shared.
use super::read::CoveredRead;
use super::*;
use fireweed_core::queue_contract::{QueueLimits, RequestPreflight, StateReadRequest};
use fireweed_engine::queue_contract::QueueFuture;
use fireweed_turso::queue_projection::{AdmittedStateRecords, StateReadInput};
impl QueueRecoveryPipeline {
    /// Internal owned operation, not public QueueStore construction/activation.
    pub async fn read_state(
        self: &Arc<Self>,
        request: StateReadRequest,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedStateRecords> {
        self.covered_read(request, limits, job).await
    }
}
impl CoveredRead for StateReadRequest {
    type Output = AdmittedStateRecords;
    fn encoded_size(&self) -> QueueResult<u64> {
        fireweed_core::queue_contract::encoded_len(self)
    }
    fn address(&self) -> &QueueAddress {
        &self.queue
    }
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        RequestPreflight::preflight(self, limits)
    }
    fn recover<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        job: Arc<QueueReservation>,
    ) -> QueueFuture<'a, RecoveredQueue> {
        Box::pin(async move {
            let states = fireweed_engine::queue_state_authority::ColdStateTracker::new(self, &job)?;
            pipeline
                .recover_checked(self.queue.clone(), None, None, Some(states), job)
                .await
        })
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
                .read_state_records(
                    StateReadInput {
                        authority: Some(
                            recovered
                                .state_authority
                                .as_ref()
                                .ok_or_else(|| unknown("state source proof missing"))?
                                .clone(),
                        ),
                        genesis: pipeline.original_genesis(&self.queue, recovered, job)?,
                        request: self.clone(),
                        limits,
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
        pipeline.after_state_read.lock().unwrap().take()
    }
}
