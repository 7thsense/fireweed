//! Queue stats adapter for the shared covered-read completion protocol.
use super::read::CoveredRead;
use super::*;
use fireweed_core::queue_contract::{QueueLimits, RequestPreflight, StatsRequest, encoded_len};
use fireweed_engine::{Clock, queue_contract::QueueFuture, queue_memory::IncomingHeaders};
use fireweed_turso::queue_projection::{AdmittedQueueStats, StatsReadInput};
struct StatsRead {
    request: StatsRequest,
    clock: Arc<dyn Clock>,
}
impl IncomingHeaders for StatsRead {
    fn headers(&self) -> QueueResult<u64> {
        self.request.headers()
    }
}
impl QueueRecoveryPipeline {
    pub async fn stats(
        self: &Arc<Self>,
        request: StatsRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueueStats> {
        self.covered_read(StatsRead { request, clock }, limits, job)
            .await
    }
}
impl CoveredRead for StatsRead {
    type Output = AdmittedQueueStats;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn encoded_size(&self) -> QueueResult<u64> {
        encoded_len(&self.request)
    }
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.request.preflight(limits)
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
                .read_queue_stats(
                    StatsReadInput {
                        genesis: pipeline.original_genesis(&self.request.queue, recovered, job)?,
                        request: self.request.clone(),
                        limits,
                        required_next_offset: recovered.target_next_offset,
                        required_insertion_sequence: recovered.history.insertion_sequence(),
                        required_last_record_epoch: recovered.history.historical_epoch(),
                    },
                    self.clock.clone(),
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
        pipeline.after_stats_read.lock().unwrap().take()
    }
}
