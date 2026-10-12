//! Indexed query adapter for the shared covered-read completion protocol.
use super::read::CoveredRead;
use super::*;
use fireweed_core::queue_contract::{QueryRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_contract::QueueFuture, queue_memory::IncomingHeaders};
use fireweed_turso::queue_projection::{AdmittedQueryPage, QueryReadInput};
struct QueryRead {
    request: QueryRequest,
    clock: Arc<dyn Clock>,
}
impl IncomingHeaders for QueryRead {
    fn headers(&self) -> QueueResult<u64> {
        self.request.headers()
    }
}
impl QueueRecoveryPipeline {
    pub async fn query(
        self: &Arc<Self>,
        request: QueryRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueryPage> {
        self.covered_read(QueryRead { request, clock }, limits, job)
            .await
    }
}
impl CoveredRead for QueryRead {
    type Output = AdmittedQueryPage;
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
                .read_query_page(
                    QueryReadInput {
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
        pipeline.after_query_read.lock().unwrap().take()
    }
}
