//! Common resolved publication assembly for storage backends. Selection, business
//! approval, append authority and atomic application remain with the fenced job.
use super::*;
use fireweed_core::{queue_administration::*, queue_contract::*};
use fireweed_engine::{
    queue_planner::{CommitDelta, RecordSnapshot},
    queue_publication::*,
    queue_schema_codec::DefinitionPin,
};

pub struct ResolvedAdministrativePublicationInput<'a> {
    pub queue: &'a QueueAddress,
    pub pin: DefinitionPin,
    pub group: &'a CommitGroup,
    pub intent: &'a AdministrativeIntent,
    pub before: &'a RecordSnapshot,
    pub ordinary: &'a CommitDelta,
    pub administration: &'a AdministrativeResult,
    pub max_result_bytes: u64,
    pub max_publication_bytes: u64,
    pub expected_bytes: u64,
    pub expected_commands: u64,
}
impl SealedAdministrativeScratch {
    /// Run under the owning blocking worker after complete business approval.
    /// Every source command and the final frame validate before bytes escape.
    pub fn prepare_resolved_publication(
        &mut self,
        input: &ResolvedAdministrativePublicationInput<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<AdministrativePublicationFrame> {
        let result = self.prepare_resolved_publication_checked(input, job);
        if result.is_err() {
            self.poison_publication();
        }
        result
    }
    fn prepare_resolved_publication_checked(
        &mut self,
        input: &ResolvedAdministrativePublicationInput<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<AdministrativePublicationFrame> {
        self.validate_publication_context(input.queue, input.pin, input.before.next_sequence, job)?;
        if self.publication_overflowed() {
            return Err(unknown("overflowed publication source cannot be assembled"));
        }
        let GroupOutcome::Committed { versions } = &input.ordinary.receipt.outcome else {
            return Err(unknown("publication lacks ordinary success"));
        };
        let outcome = AdministrativeReceiptOutcome::Committed {
            versions,
            administration: input.administration,
        };
        let receipt = AdministrativePublicationReceipt::new(
            input.queue,
            input.group,
            input.intent,
            input.max_result_bytes,
            &outcome,
            job,
        )
        .map_err(|_| unknown("publication receipt binding failed"))?;
        let ordinary = encode_publication_pinned(
            input.queue,
            input.pin,
            input.before,
            input.ordinary,
            &[],
            input.max_publication_bytes,
            job,
        )
        .map_err(|_| unknown("ordinary publication encoding failed"))?;
        let ordinary_view = view_publication(ordinary.as_bytes())
            .map_err(|_| unknown("ordinary publication validation failed"))?;
        let mut ordinary_commands = ordinary_view.command_fragments();
        let mut next_ordinary = ordinary_commands
            .next()
            .transpose()
            .map_err(|_| unknown("ordinary command source failed"))?;
        let measured = measure_resolved_commands(input.before, input.ordinary)
            .map_err(|_| unknown("ordinary publication measurement failed"))?;
        let command_bytes = self
            .command_bytes()
            .checked_add(measured.bytes)
            .ok_or_else(|| unknown("publication bytes overflow"))?;
        if self.command_count().checked_add(measured.count) != Some(input.expected_commands) {
            return Err(unknown("publication command count changed"));
        }
        let mut builder = AdministrativePublicationBuilder::new(
            &receipt,
            input.pin,
            input.before,
            input.ordinary,
            input.expected_commands,
            command_bytes,
            input.max_publication_bytes,
            job,
        )
        .map_err(|_| unknown("publication builder failed"))?;
        self.rewind_commands()?;
        let mut next_administrative = self.next_command()?;
        while next_administrative.is_some() || next_ordinary.is_some() {
            let administrative_first = match (&next_administrative, &next_ordinary) {
                (Some(admin), Some(ordinary)) => {
                    let admin = admin.command()?;
                    (admin.identity.collection, admin.identity.key)
                        <= (
                            ordinary.command.identity.collection,
                            ordinary.command.identity.key,
                        )
                }
                (Some(_), None) => true,
                _ => false,
            };
            if administrative_first {
                let packet = next_administrative.take().expect("administrative head");
                builder
                    .append_administrative(packet.resolved_bytes())
                    .map_err(|_| unknown("administrative publication command binding failed"))?;
                drop(packet);
                next_administrative = self.next_command()?;
            } else {
                let packet = next_ordinary.take().expect("ordinary head");
                builder
                    .append_ordinary(packet.as_bytes())
                    .map_err(|_| unknown("ordinary publication command binding failed"))?;
                next_ordinary = ordinary_commands
                    .next()
                    .transpose()
                    .map_err(|_| unknown("ordinary command source failed"))?;
            }
        }
        let frame = builder
            .finish()
            .map_err(|_| unknown("publication frame validation failed"))?;
        if frame.as_bytes().len() as u64 != input.expected_bytes {
            return Err(unknown("prepared publication size changed"));
        }
        Ok(frame)
    }
}
