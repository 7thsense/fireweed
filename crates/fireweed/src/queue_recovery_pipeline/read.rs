//! One owned completion protocol for covered reads. Request adapters supply only
//! schema preflight and native snapshot work; cancellation and freshness live here.
use super::*;
use fireweed_core::queue_contract::QueueLimits;
use fireweed_engine::{
    queue_contract::QueueFuture,
    queue_memory::{IncomingHeaders, incoming_headers},
};
pub(super) trait CoveredRead: IncomingHeaders + Send + Sync + 'static {
    type Output: Send + 'static;
    fn address(&self) -> &QueueAddress;
    fn encoded_size(&self) -> QueueResult<u64>;
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()>;
    fn read<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        recovered: &'a RecoveredQueue,
        limits: QueueLimits,
        job: &'a Arc<QueueReservation>,
    ) -> QueueFuture<'a, Self::Output>;
    #[cfg(test)]
    fn pause(
        &self,
        pipeline: &QueueRecoveryPipeline,
    ) -> Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>;
}
struct ReadTask<R: CoveredRead> {
    _slot: OwnedSemaphorePermit,
    execution: ReadExecution<R>,
}
pub(super) struct ReadExecution<R: CoveredRead> {
    pipeline: Arc<QueueRecoveryPipeline>,
    request: R,
    limits: QueueLimits,
    _input: OwnedQueueBufferCharge,
    job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    pub(super) async fn covered_read<R: CoveredRead>(
        self: &Arc<Self>,
        request: R,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
    ) -> QueueResult<R::Output> {
        let execution = ReadExecution::new(self.clone(), request, limits, job)?;
        let slot = self
            .operation_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueueError::Busy)?;
        let task = ReadTask {
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
            .map_err(|_| unknown("owned covered read task failed"))?
    }
}
impl<R: CoveredRead> ReadTask<R> {
    async fn execute(self) -> QueueResult<R::Output> {
        let completion = OperationCompletion {
            _slot: self._slot,
            _job: self.execution.job.clone(),
        };
        let result = self.execution.execute().await;
        drop(completion);
        result
    }
}
impl<R: CoveredRead> ReadExecution<R> {
    pub(super) fn new(
        pipeline: Arc<QueueRecoveryPipeline>,
        request: R,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        request.preflight(&limits)?;
        let headers = incoming_headers(&request)?
            .checked_mul(2)
            .ok_or(QueueError::TooLarge)?;
        let input = job.owned_buffer(
            request
                .encoded_size()?
                .checked_mul(16)
                .and_then(|n| n.checked_add(headers))
                .and_then(|n| n.checked_add(65536))
                .ok_or(QueueError::TooLarge)?,
        )?;
        Ok(Self {
            pipeline,
            request,
            limits,
            _input: input,
            job,
        })
    }
    pub(super) async fn execute(self) -> QueueResult<R::Output> {
        for _ in 0..3 {
            let Ok(recovered) = self
                .pipeline
                .recover(self.request.address().clone(), self.job.clone())
                .await
            else {
                continue;
            };
            let result = self
                .request
                .read(&self.pipeline, &recovered, self.limits, &self.job)
                .await;
            #[cfg(test)]
            {
                let pause = self.request.pause(&self.pipeline);
                if let Some((entered, release)) = pause {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            // Every classification, including absence and TooLarge, requires a
            // fresh matching authoritative cut. Drop stale guarded output before
            // another recovery; never return it merely because decoding succeeded.
            match self
                .pipeline
                .head_matches(self.request.address(), &recovered, &self.job)
                .await
            {
                Ok(true) => return result,
                _ => drop(result),
            }
        }
        Err(unknown("covered read has no resolved current coverage"))
    }
}
