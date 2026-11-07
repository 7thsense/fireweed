//! Indexed query served from the projection.
use super::owner::{Exit, Plan, Tenure, settle};
use super::stats::read_input_charge;
use super::*;
use fireweed_core::queue_contract::{QueryRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_memory::IncomingHeaders};
use fireweed_turso::queue_projection::{AdmittedQueryPage, QueryReadInput};
use futures::future::BoxFuture;

struct QueryPlan {
    request: QueryRequest,
}
impl QueueRecoveryPipeline {
    pub async fn query(
        self: &Arc<Self>,
        request: QueryRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueryPage> {
        request.preflight(&limits)?;
        let input = read_input_charge(encoded_len(&request)?, request.headers()?, &job)?;
        self.submit(QueryPlan { request }, limits, clock, job, input)
            .await
    }
}
impl Plan for QueryPlan {
    type Output = AdmittedQueryPage;
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
                p.projection
                    .read_query_page(
                        QueryReadInput {
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
