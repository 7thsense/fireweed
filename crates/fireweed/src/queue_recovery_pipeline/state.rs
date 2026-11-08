//! Complete workflow-state read served from the projection.
use super::owner::{Exit, Plan, Tenure, settle};
use super::stats::read_input_charge;
use super::*;
use fireweed_core::queue_contract::{QueueLimits, RequestPreflight, StateReadRequest};
use fireweed_engine::{Clock, queue_memory::IncomingHeaders};
use fireweed_turso::queue_projection::{AdmittedStateRecords, StateReadInput};
use futures::future::BoxFuture;

struct StatePlan {
    request: StateReadRequest,
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
        let input = read_input_charge(
            fireweed_core::queue_contract::encoded_len(&request)?,
            request.headers()?,
            &job,
        )?;
        self.submit(
            StatePlan { request },
            limits,
            self.ownership.clock(),
            job,
            input,
        )
        .await
    }
}
impl Plan for StatePlan {
    type Output = AdmittedStateRecords;
    const MUTATION: bool = false;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        limits: QueueLimits,
        _clock: &'a Arc<dyn Clock>,
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
                    .read_state_records(
                        StateReadInput {
                            genesis,
                            request: self.request.clone(),
                            limits,
                            required_next_offset: next,
                            required_insertion_sequence: sequence,
                            required_last_record_epoch: epoch,
                        },
                        job,
                        &p.workers,
                    )
                    .await,
            )
        })
    }
}
