//! One shared assembly path; no public method promotes an arbitrary frame.
use super::*;
use crate::ResolvedAdministrativePublicationInput;
use fireweed_engine::{
    queue_log_record::encode_checked_publication_record,
    queue_planner::CommitDelta,
    queue_publication::{
        AdministrativePublicationBuilder, AdministrativePublicationFrame,
        AdministrativePublicationReceipt, AdministrativeReceiptOutcome,
    },
};
fn publication_error(error: fireweed_engine::queue_publication::PublicationError) -> QueueError {
    use fireweed_engine::queue_publication::PublicationError;
    match error {
        PublicationError::Admission(error) => error,
        PublicationError::FrameLimit | PublicationError::Overflow => QueueError::TooLarge,
        PublicationError::Invalid(reason) => unknown(reason),
    }
}
impl SourceAdministrativeResolution {
    fn verify_declarations(
        &self,
        queue: &QueueAddress,
        group: &CommitGroup,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        if group.fingerprint(queue)? != self.request
            || !self.initial.ordinary().cut().belongs_to(job)
        {
            return Err(unknown("source assembly request or operation differs"));
        }
        Ok(())
    }
    fn frame(
        &mut self,
        queue: &QueueAddress,
        group: &CommitGroup,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<AdministrativePublicationFrame> {
        self.verify_declarations(queue, group, job)?;
        let admin = group
            .administration
            .as_ref()
            .ok_or_else(|| unknown("source assembly intent missing"))?;
        match &mut self.result {
            Ok(value) => {
                let (bytes, commands) = self
                    .fit
                    .ok_or_else(|| unknown("source publication fit missing"))?;
                let administration = value
                    .summary
                    .result()
                    .ok_or_else(|| unknown("source publication summary incomplete"))?;
                value.scratch.prepare_resolved_publication(
                    &ResolvedAdministrativePublicationInput {
                        queue,
                        pin: self.pin,
                        group,
                        intent: &admin.intent,
                        before: value.initial.snapshot(),
                        ordinary: value.ordinary.delta(),
                        administration,
                        max_result_bytes: admin.max_result_bytes,
                        max_publication_bytes: self.publication_bytes,
                        expected_bytes: bytes,
                        expected_commands: commands,
                    },
                    job,
                )
            }
            Err(reason) => {
                // Rejections have exactly one FP4 receipt, no commands and no
                // sequence movement. Fund temporary receipt clones independently.
                let _charge = job.owned_buffer(
                    encoded_len(reason)?
                        .checked_mul(16)
                        .and_then(|n| {
                            n.checked_add((group.request_id.len() as u64).checked_mul(4)?)
                        })
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                let outcome = AdministrativeReceiptOutcome::Rejected(reason);
                let receipt = AdministrativePublicationReceipt::new(
                    queue,
                    group,
                    &admin.intent,
                    admin.max_result_bytes,
                    &outcome,
                    job,
                )
                .map_err(publication_error)?;
                let delta = CommitDelta {
                    work: vec![],
                    work_fields: Default::default(),
                    work_bodies: Default::default(),
                    deleted_work: vec![],
                    state: vec![],
                    deleted_state: vec![],
                    versions: vec![],
                    next_sequence: self.initial.snapshot().next_sequence,
                    receipt: CommitReceipt {
                        request_id: group.request_id.clone(),
                        outcome: GroupOutcome::Rejected(reason.clone()),
                    },
                };
                AdministrativePublicationBuilder::new(
                    &receipt,
                    self.pin,
                    self.initial.snapshot(),
                    &delta,
                    0,
                    0,
                    self.publication_bytes,
                    job,
                )
                .map_err(publication_error)?
                .finish()
                .map_err(publication_error)
            }
        }
    }
    /// Run under the owned blocking worker. The private resolution supplies all
    /// commands and immutable source authority; callers cannot supply a frame.
    pub fn publication(
        mut self,
        queue: &QueueAddress,
        group: &CommitGroup,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<SourceAdministrativePublication> {
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.initial.probe {
            probe.checkpoint(FinalizationStage::AssembleFrame)?;
        }
        let frame = self.frame(queue, group, job)?;
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.initial.probe {
            probe.checkpoint(FinalizationStage::EncodeRecord)?;
        }
        let record = encode_checked_publication_record(
            self.initial.ordinary().cut().epoch(),
            frame.as_bytes(),
            self.record_bytes,
            job,
        )?;
        Ok(SourceAdministrativePublication {
            record,
            source: self.source,
            initial: self.initial,
        })
    }
    /// Preview validates the same actual frame assembler, then retains only a
    /// separately funded DTO and source provenance for the outer freshness check.
    pub fn preview(
        mut self,
        queue: &QueueAddress,
        group: &CommitGroup,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<SourceAdministrativePreview> {
        let frame = self.frame(queue, group, job)?;
        let (outcome, charge) = match &self.result {
            Err(reason) => {
                let charge = job.owned_buffer(
                    encoded_len(reason)?
                        .checked_mul(16)
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                (GroupOutcome::Rejected(reason.clone()), charge)
            }
            Ok(value) => {
                let administration = value
                    .summary
                    .result()
                    .ok_or_else(|| unknown("source preview result incomplete"))?;
                let GroupOutcome::Committed { versions } = &value.ordinary.delta().receipt.outcome
                else {
                    return Err(unknown("source preview ordinary proposal not committed"));
                };
                let headers =
                    fireweed_engine::queue_memory::administrative_result_headers(administration)?
                        .checked_add(
                            (versions.len() as u64)
                                .checked_mul(256)
                                .ok_or(QueueError::TooLarge)?,
                        )
                        .ok_or(QueueError::TooLarge)?;
                let charge = job.owned_buffer(
                    encoded_len(&(versions, administration))?
                        .checked_mul(16)
                        .and_then(|n| headers.checked_mul(4).and_then(|h| n.checked_add(h)))
                        .and_then(|n| n.checked_add(65536))
                        .ok_or(QueueError::TooLarge)?,
                )?;
                (
                    GroupOutcome::AdministrativeCommitted {
                        versions: versions.clone(),
                        administration: administration.clone(),
                    },
                    charge,
                )
            }
        };
        let result = PreviewResult::try_from(outcome)?;
        if encoded_len(&result)? > self.limits.max_response_bytes {
            return Err(QueueError::TooLarge);
        }
        drop(frame);
        Ok(SourceAdministrativePreview {
            value: OwnedQueueValue::new(result, vec![charge]),
            source: self.source,
            initial: self.initial,
        })
    }
}
