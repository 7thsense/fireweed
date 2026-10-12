//! Immutable receipt read adapter; owned completion and freshness are shared.
use super::read::CoveredRead;
use super::*;
use fireweed_core::queue_contract::{QueueLimits, ReceiptReadRequest};
use fireweed_engine::queue_contract::QueueFuture;
use fireweed_turso::queue_projection::{AdmittedCommitReceipts, ReceiptReadInput};
impl QueueRecoveryPipeline {
    /// Internal owned operation, not public QueueStore construction/activation.
    pub async fn read_commit(
        self: &Arc<Self>,
        request: ReceiptReadRequest,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedCommitReceipts> {
        self.covered_read(request, limits, job).await
    }
}
impl CoveredRead for ReceiptReadRequest {
    type Output = AdmittedCommitReceipts;
    fn encoded_size(&self) -> QueueResult<u64> {
        fireweed_core::queue_contract::encoded_len(self)
    }
    fn address(&self) -> &QueueAddress {
        &self.queue
    }
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.validate(limits)
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
                .read_commit_receipts(
                    ReceiptReadInput {
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
        pipeline.after_receipt_read.lock().unwrap().take()
    }
}
