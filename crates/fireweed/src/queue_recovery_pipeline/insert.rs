//! Insert: plan against the batch, stage the entry, reply from the projection
//! once the batch is published and committed.
use super::owner::{Batch, Exit, Plan, Staged, StagedEntry, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::{InsertRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{Clock, queue_memory::incoming_headers};
use fireweed_turso::queue_projection::{AdmittedInsertReplay, InsertReplayInput};
use futures::future::BoxFuture;

struct InsertPlan {
    request: InsertRequest,
}
impl QueueRecoveryPipeline {
    /// Internal complete insert operation, not a public QueueStore activation.
    pub async fn insert(
        self: &Arc<Self>,
        request: InsertRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedInsertReplay> {
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
        self.submit(InsertPlan { request }, limits, clock, job, input)
            .await
    }
}
impl InsertPlan {
    fn input(
        &self,
        p: &QueueRecoveryPipeline,
        t: &Tenure,
        point: (u64, u64, u64),
        limits: QueueLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<InsertReplayInput> {
        let (next, sequence, epoch) = point;
        Ok(InsertReplayInput {
            genesis: t.genesis_record(p, &self.request.queue, job)?,
            request: self.request.clone(),
            limits,
            required_next_offset: next,
            required_insertion_sequence: sequence,
            required_last_record_epoch: epoch,
        })
    }
}
impl Plan for InsertPlan {
    type Output = AdmittedInsertReplay;
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
                    .host(p)
                    .await?
                    .prepare_insert(
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
        _clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            let input = match self.input(p, t, t.point(), limits, job) {
                Ok(input) => input,
                Err(error) => return Ok(Err(error)),
            };
            match settle(p.projection.replay_insert(input, job, &p.workers).await)? {
                Ok(Some(replay)) => Ok(Ok(replay)),
                // The request is published or known; the projection must hold it.
                Ok(None) => Err(Exit),
                Err(error) => Ok(Err(error)),
            }
        })
    }
}
