//! Queue stats served from the projection.
use super::owner::{Exit, Plan, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::{QueueLimits, RequestPreflight, StatsRequest, encoded_len};
use fireweed_engine::{Clock, queue_memory::IncomingHeaders};
use fireweed_turso::queue_projection::{AdmittedQueueStats, StatsReadInput};
use futures::future::BoxFuture;

struct StatsPlan {
    request: StatsRequest,
}
impl QueueRecoveryPipeline {
    pub async fn stats(
        self: &Arc<Self>,
        request: StatsRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueueStats> {
        request.preflight(&limits)?;
        let input = read_input_charge(encoded_len(&request)?, request.headers()?, &job)?;
        self.submit(StatsPlan { request }, limits, clock, job, input)
            .await
    }
}
/// Original input plus one native-stage clone, as every read admits it.
pub(super) fn read_input_charge(
    encoded: u64,
    headers: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedQueueBufferCharge> {
    job.owned_buffer(
        encoded
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers.checked_mul(2)?))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?,
    )
}
impl Plan for StatsPlan {
    type Output = AdmittedQueueStats;
    const MUTATION: bool = false;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            let (next, sequence, epoch) = t.point();
            let genesis = match t.genesis_record(p, &self.request.queue, job) {
                Ok(genesis) => genesis,
                Err(error) => return Ok(Err(error)),
            };
            settle(
                t.projection()
                    .read_queue_stats(
                        StatsReadInput {
                            genesis,
                            request: self.request.clone(),
                            limits,
                            required_next_offset: next,
                            required_insertion_sequence: sequence,
                            required_last_record_epoch: epoch,
                        },
                        clock.clone(),
                        job,
                        &p.workers,
                    )
                    .await,
            )
        })
    }
}
