//! Bounded original-request evidence under one original authenticated cut.
use super::{
    QueueSourceCut, QueueSourceKey, QueueSourceOriginal, QueueSourceReader, QueueSourceSpan,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{CheckedQueueLogRecord, QueueLogPayload},
    queue_publication::view_receipt_outcome,
    queue_schema_codec::DefinitionPin,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::Arc;

fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source original: {message}"))
}

/// Presence and absence retain the full literal identity and original cut.
/// This borrows the driven reader; it never becomes an unbound absence proof.
pub struct QueueSourceOriginalEvidence<'a> {
    cut: QueueSourceCut<'a>,
    domain: u8,
    request_id: String,
    full_key: Vec<u8>,
    fact: Option<QueueSourceOriginal>,
    _charge: OwnedQueueBufferCharge,
}

pub async fn load_source_original<'a, R: QueueSourceReader>(
    reader: &'a R,
    domain: u8,
    request_id: &str,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceOriginalEvidence<'a>> {
    let cut = reader.cut()?;
    if !cut.belongs_to(job) || cut.through() == 0 {
        return Err(unknown("operation or nonempty cut differs"));
    }
    if domain > 2 || request_id.trim().is_empty() {
        return Err(QueueError::Invalid(
            "invalid original domain or literal ID".into(),
        ));
    }
    let key = QueueSourceKey::Original { domain, request_id };
    let bytes = (key.encoded_len()? as u64)
        .checked_add(request_id.len() as u64)
        .and_then(|n| n.checked_add(std::mem::size_of::<QueueSourceOriginalEvidence<'_>>() as u64))
        .ok_or(QueueError::TooLarge)?;
    let charge = job.owned_buffer(bytes)?;
    let full_key = key.encode()?;
    let owner = reader.lookup(&full_key).await?;
    let point = reader.point(&owner)?;
    let fact = point
        .value_for(&cut, &full_key)?
        .map(crate::queue_storage_layout::decode::<QueueSourceOriginal>)
        .transpose()?;
    if let Some(fact) = fact {
        fact.validate(domain)?;
        if fact.outcome.span().log_offset() == 0
            || fact.outcome.span().log_offset() >= cut.through()
            || fact.outcome.span().byte_length() == 0
        {
            return Err(unknown("outcome lies outside original source cut"));
        }
    }
    Ok(QueueSourceOriginalEvidence {
        cut,
        domain,
        request_id: request_id.to_owned(),
        full_key,
        fact,
        _charge: charge,
    })
}

impl QueueSourceOriginalEvidence<'_> {
    pub fn domain(&self) -> u8 {
        self.domain
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn full_key(&self) -> &[u8] {
        &self.full_key
    }
    pub fn source_offset(&self) -> Option<u64> {
        self.fact.map(|fact| fact.outcome.span().log_offset())
    }
    pub fn fingerprint(&self) -> Option<(u8, [u8; 32])> {
        self.fact
            .map(|fact| (fact.fingerprint_format, fact.fingerprint))
    }
    /// Fixed authenticated fingerprint conflict precedes body hydration.
    pub fn check_fingerprint(&self, format: u8, fingerprint: &[u8; 32]) -> QueueResult<()> {
        if !(format == 3 || (self.domain == 2 && format == 4)) {
            return Err(QueueError::Invalid(
                "invalid expected fingerprint format".into(),
            ));
        }
        if let Some(fact) = self.fact
            && (fact.fingerprint_format != format || fact.fingerprint != *fingerprint)
        {
            return Err(QueueError::RequestConflict);
        }
        Ok(())
    }
    /// Physical membership at observed_offset under this SAME cut is the
    /// adapter's duty. Verify the actual receipt identity and rederive its exact
    /// span before accepting isolated bytes, even if another outcome is equal.
    pub fn verify_record<'r>(
        &self,
        record: &CheckedQueueLogRecord<'r>,
        observed_offset: u64,
        definition_pin: DefinitionPin,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<&'r [u8]> {
        let fact = self
            .fact
            .ok_or_else(|| unknown("absent original has no outcome"))?;
        // The record accessor projects the actual embedded Publication/Claim
        // command pin; FWQL does not carry a second independent definition pin.
        if !self.cut.belongs_to(job)
            || !record.owner().belongs_to(job)
            || record.view().address() != self.cut.address()
            || record.view().epoch() > self.cut.epoch()
            || record.view().definition_pin_bytes() != *definition_pin.as_bytes()
            || observed_offset >= self.cut.through()
        {
            return Err(unknown(
                "historical record owner, address, epoch or stored pin differs",
            ));
        }
        let mut outcome = None;
        match record.view().payload() {
            QueueLogPayload::Publication(publication) if self.domain != 1 => {
                for receipt in publication.receipts() {
                    let receipt = receipt.map_err(|_| unknown("invalid stored receipt"))?;
                    if receipt.domain == self.domain && receipt.request_id == self.request_id {
                        if outcome.is_some()
                            || receipt.fingerprint_format != fact.fingerprint_format
                            || *receipt.fingerprint != fact.fingerprint
                        {
                            return Err(unknown("stored receipt identity or fingerprint differs"));
                        }
                        outcome = Some(receipt.encoded_outcome());
                    }
                }
            }
            QueueLogPayload::Claim(claim) if self.domain == 1 => {
                if claim.request_id() != self.request_id
                    || claim.fingerprint_format() != fact.fingerprint_format
                    || *claim.fingerprint() != fact.fingerprint
                {
                    return Err(unknown("stored claim identity or fingerprint differs"));
                }
                outcome = Some(claim.receipt_bytes());
            }
            _ => return Err(unknown("original points at another record domain")),
        }
        let outcome =
            outcome.ok_or_else(|| unknown("literal receipt missing from original record"))?;
        if fact.outcome.span() != QueueSourceSpan::from_checked(record, observed_offset, outcome)? {
            return Err(unknown("outcome is not the exact matching receipt span"));
        }
        // resolve verifies full FWQL2 checksum and physical offset; the isolated
        // digest remains required even after exact receipt membership is known.
        fact.outcome.span().resolve(record, observed_offset)?;
        fact.outcome.verify_cached(outcome)?;
        match record.view().payload() {
            QueueLogPayload::Claim(claim) => claim.validate_binding(job)?,
            _ => {
                view_receipt_outcome(self.domain, fact.fingerprint_format, outcome)
                    .map_err(|_| unknown("invalid stored outcome encoding"))?;
            }
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests;
