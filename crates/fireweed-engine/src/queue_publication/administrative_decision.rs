//! Backend-independent complete administrative publication decision.
use super::*;
use crate::queue_administration::{AdmittedAdministrativeScan, ProvisionalOrdinaryCandidate};

#[derive(Debug, PartialEq, Eq)]
pub enum AdministrativePublicationDecision {
    Fits { bytes: u64, commands: u64 },
    TooLarge,
}
/// Measurements from an adapter's context-validated sealed command spool.
/// This summary establishes neither spool identity nor publication authority.
pub struct AdministrativeCommandMeasurement {
    pub command_limit: u64,
    pub command_bytes: u64,
    pub command_count: u64,
    pub overflowed: bool,
}
fn storage(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
/// The adapter must validate its sealed spool's queue, schema and sequence first.
#[allow(clippy::too_many_arguments)]
pub fn assess_administrative_publication(
    address: &QueueAddress,
    pin: DefinitionPin,
    group: &CommitGroup,
    intent: &fireweed_core::queue_administration::AdministrativeIntent,
    ordinary: &ProvisionalOrdinaryCandidate,
    summary: &AdmittedAdministrativeScan,
    scratch: AdministrativeCommandMeasurement,
    request_fingerprint: [u8; 32],
    max_result_bytes: u64,
    max_publication_bytes: u64,
    reservation: &std::sync::Arc<QueueReservation>,
) -> QueueResult<AdministrativePublicationDecision> {
    if group.administrative_fingerprint(address, intent, max_result_bytes)? != request_fingerprint {
        return Err(storage(
            "publication decision belongs to a different request",
        ));
    }
    let floor = measure_administrative_success_floor(address, pin, group)
        .map_err(|_| storage("publication floor measurement failed"))?;
    if scratch.command_limit != max_publication_bytes.saturating_sub(floor) {
        return Err(storage("publication command allowance context mismatch"));
    }
    let measured = measure_resolved_commands(ordinary.initial_authority(), ordinary.delta())
        .map_err(|_| storage("ordinary command measurement failed"))?;
    let GroupOutcome::Committed { versions } = &ordinary.delta().receipt.outcome else {
        return Err(storage("provisional ordinary result is not committed"));
    };
    if matches!(
        assess_administrative_result_limit(group, summary, versions, max_result_bytes)
            .map_err(|_| storage("exact administrative result measurement failed"))?,
        AdministrativeResultLimit::TooLarge
    ) || scratch.overflowed
    {
        return Ok(AdministrativePublicationDecision::TooLarge);
    }
    let administration = summary
        .result()
        .ok_or_else(|| storage("complete publication lacks complete result"))?;
    let outcome = AdministrativeReceiptOutcome::Committed {
        versions,
        administration,
    };
    let receipt = AdministrativePublicationReceipt::new(
        address,
        group,
        intent,
        max_result_bytes,
        &outcome,
        reservation,
    )
    .map_err(|_| storage("publication receipt binding failed"))?;
    let command_bytes = scratch
        .command_bytes
        .checked_add(measured.bytes)
        .ok_or_else(|| storage("publication command bytes overflow"))?;
    let commands = scratch
        .command_count
        .checked_add(measured.count)
        .ok_or_else(|| storage("publication command count overflow"))?;
    let bytes = measure_administrative_publication(address, pin, command_bytes, &receipt)
        .map_err(|_| storage("complete publication measurement failed"))?;
    Ok(if bytes > max_publication_bytes {
        AdministrativePublicationDecision::TooLarge
    } else {
        AdministrativePublicationDecision::Fits { bytes, commands }
    })
}
