//! Provisional ordinary-group packing. Each logical result remains independent;
//! only the orchestrator's fresh complete original resolution may acknowledge it.
use super::*;
use fireweed_engine::{
    Clock,
    queue_group_state_authority::AdmittedGroupStateSet,
    queue_log_record::{checked_publication_record_len, encode_checked_publication_record},
    queue_memory::{group_expansion_budget, incoming_headers},
    queue_original_authority::AdmittedOriginalAuthority,
    queue_planner::{CommitDelta, combine_deltas, plan_commit_group, prospective_unique_probes},
    queue_publication::{PublicationError, PublicationReceipt, encode_publication_pinned},
};

pub struct CommitPackPreparationInput {
    pub request: Arc<CommitBatch>,
    /// Strict caller-order positions; administration is an external barrier.
    pub positions: Vec<usize>,
    pub originals: Arc<AdmittedOriginalAuthority>,
    pub states: Arc<AdmittedGroupStateSet>,
    pub genesis: OwnedQueueLogRecord,
    pub limits: QueueLimits,
    pub required_progress: (u64, u64, u64),
}
pub struct AdmittedPackReceipts {
    positions: Vec<usize>,
    receipts: Vec<CommitReceipt>,
    _charges: Vec<OwnedQueueBufferCharge>,
}
impl AdmittedPackReceipts {
    pub fn positions(&self) -> &[usize] {
        &self.positions
    }
    pub fn receipts(&self) -> &[CommitReceipt] {
        &self.receipts
    }
}
pub type PreparedOrdinaryPack = super::preparation::PreparedPublication<AdmittedPackReceipts>;
#[allow(clippy::large_enum_variant)]
pub enum OrdinaryPackPreparation {
    Prepared(PreparedOrdinaryPack),
    /// Whole-batch classification changed; repeat covered classification first.
    Reclassify,
    /// The first logical group exceeds the physical record byte limit; use its existing path.
    SingleGroup,
}
#[allow(clippy::large_enum_variant)]
enum Parts {
    Prepared {
        record: OwnedQueueLogRecord,
        receipts: Vec<CommitReceipt>,
        positions: Vec<usize>,
        head: u64,
        epoch: u64,
    },
    Reclassify,
    SingleGroup,
}
struct Worker {
    projection: Arc<QueueProjection>,
    input: CommitPackPreparationInput,
    epoch: u64,
    max_record_bytes: u64,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    charges: Vec<OwnedQueueBufferCharge>,
}
impl QueueProjection {
    pub async fn prepare_ordinary_pack(
        self: &Arc<Self>,
        input: CommitPackPreparationInput,
        epoch: u64,
        max_record_bytes: u64,
        clock: Arc<dyn Clock>,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<OrdinaryPackPreparation> {
        input.request.preflight(&input.limits)?;
        fireweed_engine::queue_memory::administrative_envelope_preflight(
            &input.request,
            &input.limits,
        )?;
        input
            .originals
            .validate(&input.request, &input.genesis, input.required_progress, job)?;
        input.states.validate_batch(
            &input.request,
            &input.genesis,
            input.required_progress,
            job,
        )?;
        if epoch == 0
            || max_record_bytes == 0
            || input.positions.is_empty()
            || input
                .positions
                .windows(2)
                .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
            || input.positions.iter().any(|&position| {
                input
                    .request
                    .groups
                    .get(position)
                    .is_none_or(|group| group.administration.is_some())
            })
        {
            return Err(QueueError::Invalid(
                "invalid ordinary pack positions/authority/capacity".into(),
            ));
        }
        let capacity = encoded_len(input.request.as_ref())?
            .checked_mul(24)
            .and_then(|n| n.checked_add(incoming_headers(input.request.as_ref()).ok()?))
            .and_then(|n| n.checked_add((input.positions.len() as u64).checked_mul(4096)?))
            .and_then(|n| n.checked_add(65536))
            .ok_or(QueueError::TooLarge)?;
        let mut worker = Worker {
            projection: self.clone(),
            input,
            epoch,
            max_record_bytes,
            clock,
            job: job.clone(),
            charges: vec![job.owned_buffer(capacity)?],
        };
        workers
            .run(move || {
                Ok(crate::tx::block_on_owned_apply(async {
                    let projection = worker.projection.clone();
                    let result = owned_native_transaction(
                        &projection,
                        TransactionBehavior::Deferred,
                        &mut worker,
                    )
                    .await?;
                    Ok(match result {
                        Parts::Reclassify => OrdinaryPackPreparation::Reclassify,
                        Parts::SingleGroup => OrdinaryPackPreparation::SingleGroup,
                        Parts::Prepared {
                            record,
                            receipts,
                            positions,
                            head,
                            epoch,
                        } => OrdinaryPackPreparation::Prepared(PreparedOrdinaryPack::new(
                            record,
                            AdmittedPackReceipts {
                                positions,
                                receipts,
                                _charges: std::mem::take(&mut worker.charges),
                            },
                            head,
                            epoch,
                        )),
                    })
                }))
            })
            .await?
    }
}
impl NativeTransactionWork for Worker {
    type Output = Parts;
    async fn before_commit(&mut self) {
        #[cfg(test)]
        {
            self.projection.pause_apply(false).await;
            assert!(!self.charges.is_empty());
            if self
                .projection
                .panic_after_command
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("injected pack preparation precommit panic");
            }
        }
    }
    async fn run(&mut self, connection: &Connection) -> QueueResult<Parts> {
        let native = super::snapshot::read_snapshot(connection, &self.input.genesis, &self.job)
            .await?
            .ok_or_else(|| unknown("pack preparation projection missing"))?;
        native.validate_point(
            &self.input.genesis,
            &self.input.request.queue,
            self.input.required_progress,
        )?;
        if self.epoch < native.last_record_epoch {
            return Err(unknown("pack epoch precedes source history"));
        }
        let known = super::commit_batch_replay::classify_originals(
            connection,
            &native,
            super::commit_batch_replay::BatchRead {
                request: &self.input.request,
                originals: &self.input.originals,
                limits: &self.input.limits,
                response_max_bytes: self.input.limits.max_response_bytes,
            },
            &self.job,
            &mut self.charges,
        )
        .await?;
        if known[..self.input.positions[0]].iter().any(Option::is_none) {
            return Err(QueueError::Invalid(
                "ordinary pack cannot skip an unexecuted caller predecessor".into(),
            ));
        }
        if self
            .input
            .positions
            .iter()
            .any(|&position| known[position].is_some())
        {
            return Ok(Parts::Reclassify);
        }
        drop(known);
        self.charges.push(
            self.job.owned_buffer(
                (native.definition.raw().len() as u64)
                    .checked_mul(16)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )?,
        );
        let states = self
            .input
            .positions
            .iter()
            .map(|&position| self.input.states.group(position))
            .collect::<QueueResult<Vec<_>>>()?;
        let reads = self
            .input
            .positions
            .iter()
            .zip(&states)
            .map(|(&position, states)| super::group_authority::GroupRead {
                group: &self.input.request.groups[position],
                states: Some(states),
                sequence: native.insertion_sequence,
            })
            .collect::<Vec<_>>();
        let schema = native.definition.schema();
        let mut snapshot = super::group_authority::load_groups(
            connection,
            native.queue_id,
            schema,
            &reads,
            &self.job,
            &mut self.charges,
            false,
        )
        .await?;
        // Initial metadata, fields, body and container clones overlap the overlay
        // and every retained delta. Admit them before making the before image.
        let base_bytes = encoded_len(&(
            &snapshot.work,
            &snapshot.state,
            &snapshot.versions,
            snapshot.next_sequence,
        ))?
        .checked_mul(24)
        .and_then(|n| n.checked_add(65536))
        .ok_or(QueueError::TooLarge)?;
        self.charges.push(self.job.owned_buffer(base_bytes)?);
        let before = snapshot.clone();
        let mut deltas: Vec<CommitDelta> = Vec::new();
        let mut accepted: Option<OwnedQueueLogRecord> = None;
        for &position in &self.input.positions {
            let group = &self.input.request.groups[position];
            self.charges.push(
                self.job.owned_buffer(
                    group_expansion_budget(group, &snapshot, schema)?
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(incoming_headers(group).ok()?))
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?,
            );
            let probes = prospective_unique_probes(group, &snapshot, schema)?
                .into_iter()
                .filter(|probe| !snapshot.unique_probes_loaded.contains(probe))
                .collect();
            super::unique_authority::load_unique_authority(
                connection,
                native.queue_id,
                schema,
                &probes,
                &mut snapshot,
                &self.job,
                &mut self.charges,
            )
            .await?;
            // Retained deltas freeze their independently sampled lease decisions.
            let now = timestamp_nanos(self.clock.now())?;
            let delta = plan_commit_group(group, &mut snapshot, now, schema)?;
            if encoded_len(&delta.receipt)? > self.input.limits.max_response_bytes {
                return Err(QueueError::TooLarge);
            }
            deltas.push(delta);
            let combined = combine_deltas(&deltas, before.next_sequence)?;
            let mut receipts = self
                .input
                .positions
                .iter()
                .take(deltas.len())
                .zip(&deltas)
                .map(|(&position, delta)| {
                    PublicationReceipt::commit(
                        &self.input.request.queue,
                        &self.input.request.groups[position],
                        &delta.receipt,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(publication_error)?;
            receipts.sort_unstable_by(|left, right| left.identity().cmp(&right.identity()));
            let frame = encode_publication_pinned(
                &self.input.request.queue,
                native.definition.pin(),
                &before,
                &combined,
                &receipts,
                self.max_record_bytes,
                &self.job,
            );
            let candidate = match frame {
                Ok(frame)
                    if checked_publication_record_len(frame.as_bytes().len() as u64)?
                        <= self.max_record_bytes =>
                {
                    Some(encode_checked_publication_record(
                        self.epoch,
                        frame.as_bytes(),
                        self.max_record_bytes,
                        &self.job,
                    )?)
                }
                Ok(_) | Err(PublicationError::FrameLimit) => None,
                Err(error) => return Err(publication_error(error)),
            };
            if let Some(record) = candidate {
                accepted = Some(record);
            } else {
                deltas.pop();
                if accepted.is_none() {
                    return Ok(Parts::SingleGroup);
                }
                break; // deterministic first overflow; only the unpublished suffix replans
            }
        }
        let count = deltas.len();
        let receipts = deltas.into_iter().map(|delta| delta.receipt).collect();
        Ok(Parts::Prepared {
            record: accepted.ok_or_else(|| unknown("pack has no accepted prefix"))?,
            receipts,
            positions: self.input.positions[..count].to_vec(),
            head: native.next_offset,
            epoch: self.epoch,
        })
    }
}

#[cfg(test)]
mod tests;
