//! Validate the complete batch and classify all original identities in one
//! native snapshot. Fresh execution begins only after covered orchestration.
use super::*;
use fireweed_engine::{
    queue_memory::incoming_headers, queue_publication::decode_original_commit_outcome,
};

pub struct CommitBatchReplayInput {
    pub genesis: OwnedQueueLogRecord,
    pub request: CommitBatch,
    pub limits: QueueLimits,
    /// Public receipt-vector budget; independent of internal classification tags.
    pub response_max_bytes: u64,
    pub required_next_offset: u64,
    pub required_insertion_sequence: u64,
    pub required_last_record_epoch: u64,
}
pub struct AdmittedCommitBatchReplay {
    known: Vec<Option<CommitReceipt>>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedCommitBatchReplay {
    pub fn known(&self) -> &[Option<CommitReceipt>] {
        &self.known
    }
    pub fn take_known(&mut self, position: usize) -> QueueResult<Option<CommitReceipt>> {
        self.known
            .get_mut(position)
            .map(Option::take)
            .ok_or_else(|| unknown("invalid batch replay position"))
    }
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: CommitBatchReplayInput,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    pub async fn replay_commit_batch(
        self: &Arc<Self>,
        input: CommitBatchReplayInput,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<AdmittedCommitBatchReplay> {
        if !input.genesis.belongs_to(job) {
            return Err(unknown("batch genesis belongs to another operation"));
        }
        input.request.preflight(&input.limits)?;
        if input.response_max_bytes == 0
            || input.response_max_bytes > input.limits.max_response_bytes
        {
            return Err(QueueError::TooLarge);
        }
        fireweed_engine::queue_memory::administrative_envelope_preflight(
            &input.request,
            &input.limits,
        )?;
        let containers = (input.request.groups.len() as u64)
            .checked_mul(
                (std::mem::size_of::<Option<CommitReceipt>>()
                    + std::mem::size_of::<OwnedQueueBufferCharge>() * 4) as u64,
            )
            .and_then(|n| n.checked_mul(3))
            .ok_or(QueueError::TooLarge)?;
        let headers = incoming_headers(&input.request)?;
        let capacity = encoded_len(&input.request)?
            .checked_mul(16)
            .and_then(|n| n.checked_add(headers))
            .and_then(|n| n.checked_add(containers))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let known = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(AdmittedCommitBatchReplay {
                        known,
                        _charges: std::mem::take(&mut worker.charges),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Vec<Option<CommitReceipt>>;
    async fn before_commit(&mut self) {}
    async fn run(&mut self, connection: &Connection) -> QueueResult<Self::Output> {
        let native = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("batch projection missing"))?;
        native.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            (
                self.input.required_next_offset,
                self.input.required_insertion_sequence,
                self.input.required_last_record_epoch,
            ),
        )?;
        classify_originals(
            connection,
            &native,
            BatchRead {
                request: &self.input.request,
                limits: &self.input.limits,
                response_max_bytes: self.input.response_max_bytes,
            },
            &self.job,
            &mut self.charges,
        )
        .await
    }
}
pub(super) struct BatchRead<'a> {
    pub request: &'a CommitBatch,
    pub limits: &'a QueueLimits,
    pub response_max_bytes: u64,
}
pub(super) async fn classify_originals(
    connection: &Connection,
    native: &super::snapshot::NativeQueueSnapshot,
    input: BatchRead<'_>,
    job: &Arc<QueueReservation>,
    charges: &mut Vec<OwnedQueueBufferCharge>,
) -> QueueResult<Vec<Option<CommitReceipt>>> {
    // This includes every later sibling's schema/type/credential/target checks.
    input
        .request
        .validate(native.definition.schema(), input.limits)?;
    let mut known = Vec::new();
    for group in &input.request.groups {
        let format = if group.administration.is_some() { 4 } else { 3 };
        let raw = super::receipt_authority::load_raw(
            connection,
            native.queue_id,
            &group.request_id,
            Some((format, group.fingerprint(&input.request.queue)?)),
            job,
            charges,
        )
        .await;
        let receipt = match raw {
            Ok(None) => None,
            Ok(Some((_, None))) | Err(QueueError::RequestExpired) => Some(CommitReceipt {
                request_id: group.request_id.clone(),
                outcome: GroupOutcome::RequestExpired,
            }),
            Err(QueueError::RequestConflict) => Some(CommitReceipt {
                request_id: group.request_id.clone(),
                outcome: GroupOutcome::RequestConflict,
            }),
            Err(error) => return Err(error),
            Ok(Some((format, Some(raw)))) => {
                let (receipt, owner) =
                    decode_original_commit_outcome(&input.request.queue, group, format, &raw, job)?;
                charges.push(owner);
                Some(receipt)
            }
        };
        known.push(receipt);
    }
    // The public sequence is Vec<CommitReceipt>. Internal Option/Lookup tags
    // must not turn an exact-boundary valid public response into TooLarge.
    let mut bytes = encoded_len(&(known.len() as u64))?;
    for receipt in known.iter().flatten() {
        bytes = bytes
            .checked_add(encoded_len(receipt)?)
            .ok_or(QueueError::TooLarge)?;
    }
    if bytes > input.response_max_bytes {
        return Err(QueueError::TooLarge);
    }
    Ok(known)
}
