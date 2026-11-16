//! Bounded original-request evidence under one original authenticated cut.
use super::{
    QueueSourceCut, QueueSourceKey, QueueSourceOriginal, QueueSourceReader, QueueSourceSpan,
};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{CheckedQueueLogRecord, QueueLogPayload},
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
    /// Complete typed fixed-fact equality for continuity across recaptures.
    /// This copied value grants no source, absence or publication authority.
    pub fn observation(&self) -> Option<QueueSourceOriginalObservation> {
        self.fact.map(QueueSourceOriginalObservation)
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
        let verified =
            verify_source_originals(&[self], record, observed_offset, definition_pin, job)?;
        verified.outcome(0)
    }
}

#[cfg(test)]
mod tests;

/// Domain and full literal identity must be compared separately. Unlike the
/// projection descriptor this includes the complete physical span and checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueSourceOriginalObservation(QueueSourceOriginal);
impl QueueSourceOriginalObservation {
    pub fn fingerprint(&self) -> (u8, [u8; 32]) {
        (self.0.fingerprint_format, self.0.fingerprint)
    }
    pub fn source_offset(&self) -> u64 {
        self.0.outcome.span().log_offset()
    }
}
mod batch;
pub use batch::{QueueSourceVerifiedOriginals, verify_source_originals};
