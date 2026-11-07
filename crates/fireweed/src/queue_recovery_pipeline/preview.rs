//! Administrative and ordinary group preview served from the projection.
use super::owner::{Exit, Plan, Tenure, settle};
use super::stats::read_input_charge;
use super::*;
use fireweed_core::queue_contract::{PreviewRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{
    Clock,
    queue_memory::{IncomingHeaders, administrative_groups_preflight},
};
use fireweed_turso::queue_projection::{
    AdministrativePreparationInput, AdmittedQueuePreview, CommitPreparationInput,
};
use futures::future::BoxFuture;

struct PreviewPlan {
    request: PreviewRequest,
}
impl QueueRecoveryPipeline {
    pub async fn preview(
        self: &Arc<Self>,
        request: PreviewRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueuePreview> {
        request.preflight(&limits)?;
        administrative_groups_preflight(
            &request.queue,
            std::slice::from_ref(&request.group),
            &limits,
        )?;
        let headers = request
            .headers()?
            .checked_add(
                (self.scratch_root.path().as_os_str().len() as u64)
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)?;
        let input = read_input_charge(encoded_len(&request)?, headers, &job)?;
        self.submit(PreviewPlan { request }, limits, clock, job, input)
            .await
    }
}
impl Plan for PreviewPlan {
    type Output = AdmittedQueuePreview;
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
            let input = CommitPreparationInput {
                genesis,
                queue: self.request.queue.clone(),
                group: self.request.group.clone(),
                limits,
                required_next_offset: next,
                required_insertion_sequence: sequence,
                required_last_record_epoch: epoch,
            };
            settle(if input.group.administration.is_none() {
                p.projection
                    .preview_ordinary_commit(input, clock.clone(), job, &p.workers)
                    .await
            } else {
                p.projection
                    .preview_administration(
                        AdministrativePreparationInput {
                            commit: input,
                            scratch_root: p.scratch_root.clone(),
                            scratch_quota: p.scratch_quota.clone(),
                        },
                        clock.clone(),
                        job,
                        &p.workers,
                    )
                    .await
            })
        })
    }
}
