//! Claim: plan against the batch, stage the entry, reply from the projection
//! once the batch is published and committed.
use super::owner::{Batch, Exit, Plan, Staged, StagedEntry, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::{ClaimRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_memory::incoming_headers};
use fireweed_turso::queue_projection::{AdmittedClaimReplay, ClaimReplayInput};
use futures::future::BoxFuture;

struct ClaimPlan {
    request: ClaimRequest,
}
impl QueueRecoveryPipeline {
    /// Internal complete claim operation, not a public QueueStore activation.
    pub async fn claim(
        self: &Arc<Self>,
        request: ClaimRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedClaimReplay> {
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
        self.submit(ClaimPlan { request }, limits, clock, job, input)
            .await
    }
}
impl ClaimPlan {
    fn input(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        point: (u64, u64, u64),
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<ClaimReplayInput> {
        let (next, sequence, epoch) = point;
        Ok(ClaimReplayInput {
            genesis: t.genesis_record(p, &self.request.queue, job)?,
            request: self.request.clone(),
            now: 0,
            limits,
            required_next_offset: next,
            required_insertion_sequence: sequence,
            required_last_record_epoch: epoch,
        })
    }
}
impl Plan for ClaimPlan {
    type Output = AdmittedClaimReplay;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn stage<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a Tenure,
        batch: &'a mut Batch,
        limits: QueueLimits,
        clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<Staged, Exit>> {
        Box::pin(async move {
            let input = match self.input(p, t, batch.point(t), limits, job) {
                Ok(input) => input,
                Err(error) => return Ok(Staged::Rejected(error)),
            };
            let prepared = settle(
                batch
                    .host(p, t)
                    .await?
                    .prepare_claim(
                        input,
                        t.epoch,
                        p.hard_record_bytes,
                        clock.clone(),
                        job,
                        &p.workers,
                    )
                    .await,
            )?;
            match prepared {
                Err(error) => Ok(Staged::Rejected(error)),
                // A known request id is just its stored outcome.
                Ok(None) => Ok(Staged::Pending),
                Ok(Some(prepared)) => {
                    let (record, provisional, expected_head) = prepared.into_parts();
                    drop(provisional);
                    Ok(match batch.stage(p, t, record, expected_head, job).await? {
                        StagedEntry::Staged => Staged::Pending,
                        StagedEntry::TooLarge => Staged::Rejected(QueueError::TooLarge),
                        StagedEntry::Full => Staged::Full { progressed: false },
                    })
                }
            }
        })
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
            let input = match self.input(p, t, t.point(), limits, job) {
                Ok(input) => input,
                Err(error) => return Ok(Err(error)),
            };
            match settle(
                t.projection()
                    .replay_claim_with_clock(input, clock.clone(), job, &p.workers)
                    .await,
            )? {
                Ok(Some(replay)) => Ok(Ok(replay)),
                Ok(None) => Err(Exit),
                Err(error) => Ok(Err(error)),
            }
        })
    }
}
