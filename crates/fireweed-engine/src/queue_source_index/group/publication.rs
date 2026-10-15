//! One-way encoding of a sealed group; caller declarations cannot replace it.
use super::*;
use crate::{
    queue_log_record::{OwnedQueueLogRecord, encode_checked_publication_record},
    queue_publication::{PublicationError, PublicationReceipt, encode_publication_pinned},
    queue_schema_codec::DefinitionPin,
};
#[derive(Clone, Copy)]
pub struct QueueSourceGroupEncodingLimits {
    pub frame_bytes: u64,
    pub record_bytes: u64,
}
/// A speculative checked record bound to its ORIGINAL opaque cut, persisted
/// definition and operation. No mutable record, detach or rebind is exposed.
/// Only conditional durable publication and original resolution can acknowledge it.
pub struct QueueSourceGroupPublication<'a> {
    record: OwnedQueueLogRecord,
    cut: QueueSourceCut<'a>,
    definition: DefinitionPin,
    job: &'a QueueReservation,
}
impl QueueSourceGroupPublication<'_> {
    /// Storage must verify this binding before any compilation read or work debit.
    pub fn record_for(
        &self,
        cut: &QueueSourceCut<'_>,
        definition: &AdmittedQueueDefinition,
        job: &QueueReservation,
    ) -> QueueResult<&OwnedQueueLogRecord> {
        if !self.cut.same_cut(cut)
            || !std::ptr::eq(self.job, job)
            || !definition.belongs_to(job)
            || definition.pin() != self.definition
            || !self.record.belongs_to(job)
        {
            return Err(unknown(
                "planned publication cut, definition or operation differs",
            ));
        }
        let checked = self.record.checked()?;
        cut.validate_publication(&checked, job)?;
        if checked.view().definition_pin_bytes() != *self.definition.as_bytes() {
            return Err(unknown("planned publication definition changed"));
        }
        Ok(&self.record)
    }
}
fn publication_error(error: PublicationError) -> QueueError {
    match error {
        PublicationError::Admission(error) => error,
        PublicationError::FrameLimit | PublicationError::Overflow => QueueError::TooLarge,
        PublicationError::Invalid(reason) => unknown(reason),
    }
}
impl<'a> QueueSourceGroupPlan<'a> {
    pub fn encode(
        self,
        limits: QueueSourceGroupEncodingLimits,
    ) -> QueueResult<QueueSourceGroupPublication<'a>> {
        if limits.frame_bytes == 0 || limits.record_bytes == 0 {
            return Err(QueueError::Invalid(
                "group encoding limits must be positive".into(),
            ));
        }
        let input = &self.preimage;
        // combine_deltas allocates sorted maps, key/value clones and output
        // vectors. Admit that additional peak before it runs; the original plan
        // retains its own request-ID/field guards through receipt/frame borrowing.
        let bytes = crate::queue_memory::group_expansion_budget(
            input.group,
            &input.snapshot,
            input.definition.schema(),
        )?
        .checked_add(4096)
        .ok_or(QueueError::TooLarge)?;
        let _canonical_charge = input.job.owned_buffer(bytes)?;
        let canonical = queue_planner::combine_deltas(
            std::slice::from_ref(&self.delta),
            input.snapshot.next_sequence,
        )?;
        let receipt = PublicationReceipt::commit(input.queue, input.group, &self.delta.receipt)
            .map_err(publication_error)?;
        // The inner frame and outer checked record each admit their own live
        // buffer, including their simultaneous peak; neither borrows naked bytes.
        let frame = encode_publication_pinned(
            input.queue,
            input.definition.pin(),
            &input.snapshot,
            &canonical,
            &[receipt],
            limits.frame_bytes,
            input.job,
        )
        .map_err(publication_error)?;
        let record = encode_checked_publication_record(
            input.cut.epoch(),
            frame.as_bytes(),
            limits.record_bytes,
            input.job,
        )?;
        Ok(QueueSourceGroupPublication {
            record,
            cut: input.cut,
            definition: input.definition.pin(),
            job: input.job,
        })
    }
}
