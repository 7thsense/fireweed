//! One physical checked view and one receipt-stream walk for requested members.
use super::*;
use crate::queue_publication::view_receipt_outcome;
use std::collections::BTreeMap;

pub struct QueueSourceVerifiedOriginals<'r> {
    outcomes: Vec<&'r [u8]>,
    _charge: OwnedQueueBufferCharge,
}
impl<'r> QueueSourceVerifiedOriginals<'r> {
    pub fn outcome(&self, position: usize) -> QueueResult<&'r [u8]> {
        self.outcomes
            .get(position)
            .copied()
            .ok_or_else(|| unknown("verified member position differs"))
    }
}
/// The adapter authenticates this physical record at observed_offset in the
/// exact original cut. Every supplied member must be matching-present or Lookup
/// present; conflicts and absence are classified without calling this verifier.
pub fn verify_source_originals<'r>(
    members: &[&QueueSourceOriginalEvidence<'_>],
    record: &CheckedQueueLogRecord<'r>,
    observed_offset: u64,
    definition_pin: DefinitionPin,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceVerifiedOriginals<'r>> {
    let first = members
        .first()
        .ok_or_else(|| unknown("empty original member set"))?;
    let charge = job.owned_buffer(
        (members.len() as u64)
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut identities: BTreeMap<(u8, &str), Vec<usize>> = BTreeMap::new();
    for (position, member) in members.iter().enumerate() {
        let fact = member
            .fact
            .ok_or_else(|| unknown("absent original has no outcome"))?;
        if !member.cut.same_cut(&first.cut)
            || !member.cut.belongs_to(job)
            || !record.owner().belongs_to(job)
            || record.view().address() != member.cut.address()
            || record.view().epoch() > member.cut.epoch()
            || record.view().definition_pin_bytes() != *definition_pin.as_bytes()
            || observed_offset >= member.cut.through()
        {
            return Err(unknown(
                "historical record owner, address, epoch or stored pin differs",
            ));
        }
        // Exact physical offset is checked again by the rederived span below.
        if fact.outcome.span().log_offset() != observed_offset {
            return Err(unknown("outcome is not the exact matching receipt span"));
        }
        identities
            .entry((member.domain, member.request_id.as_str()))
            .or_default()
            .push(position);
    }
    let mut found = vec![None; members.len()];
    let mut accept = |domain: u8,
                      id: &str,
                      format: u8,
                      fingerprint: &[u8; 32],
                      raw: &'r [u8]|
     -> QueueResult<()> {
        let Some(positions) = identities.get(&(domain, id)) else {
            return Ok(());
        };
        // Validate a shared receipt once; duplicate logical requests cannot
        // bypass per-member fact/span equality below.
        let representative = members[positions[0]].fact.expect("presence checked");
        let span = QueueSourceSpan::from_checked(record, observed_offset, raw)?;
        for &position in positions {
            let fact = members[position].fact.expect("presence checked");
            if found[position].is_some()
                || fact.fingerprint_format != format
                || fact.fingerprint != *fingerprint
            {
                return Err(unknown("stored receipt identity or fingerprint differs"));
            }
            if fact.outcome.span() != span {
                return Err(unknown("outcome is not the exact matching receipt span"));
            }
            if fact != representative {
                return Err(unknown("duplicate original facts differ"));
            }
            found[position] = Some(raw);
        }
        representative
            .outcome
            .span()
            .resolve(record, observed_offset)?;
        representative.outcome.verify_cached(raw)?;
        if domain != 1 {
            view_receipt_outcome(domain, format, raw)
                .map_err(|_| unknown("invalid stored outcome encoding"))?;
        }
        Ok(())
    };
    match record.view().payload() {
        QueueLogPayload::Publication(publication) => {
            if members.iter().any(|m| m.domain == 1) {
                return Err(unknown("original points at another record domain"));
            }
            for receipt in publication.receipts() {
                let r = receipt.map_err(|_| unknown("invalid stored receipt"))?;
                accept(
                    r.domain,
                    r.request_id,
                    r.fingerprint_format,
                    r.fingerprint,
                    r.encoded_outcome(),
                )?;
            }
        }
        QueueLogPayload::Claim(claim) => {
            if members.iter().any(|m| m.domain != 1) {
                return Err(unknown("original points at another record domain"));
            }
            accept(
                1,
                claim.request_id(),
                claim.fingerprint_format(),
                claim.fingerprint(),
                claim.receipt_bytes(),
            )?;
            claim.validate_binding(job)?;
        }
        _ => return Err(unknown("original points at another record domain")),
    }
    let outcomes = found
        .into_iter()
        .map(|o| o.ok_or_else(|| unknown("literal receipt missing from original record")))
        .collect::<QueueResult<_>>()?;
    Ok(QueueSourceVerifiedOriginals {
        outcomes,
        _charge: charge,
    })
}
