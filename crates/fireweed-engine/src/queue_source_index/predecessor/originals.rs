//! Immutable original outcomes derived from every checked receipt. No receipt
//! payload is copied into the semantic map; durable source retention remains
//! an adapter obligation.
use super::*;
use crate::queue_source_index::{
    QueueSourceCompileLimits, QueueSourceOriginal, QueueSourcePayload, edits::SourceEdits,
};

pub struct QueueSourceOriginalEdits<'a> {
    pub(super) output: SourceEdits<'a>,
}
impl QueueSourceOriginalEdits<'_> {
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> {
        self.output.edits()
    }
    pub fn verify(
        &self,
        record: &crate::queue_log_record::OwnedQueueLogRecord,
        cut: &QueueSourceCut<'_>,
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.output.verify(record, cut, job)
    }
}
impl<'a, R: QueueSourceReader> QueueSourcePublicationContext<'a, R> {
    /// Internally traverses every original. An error discards the entire local
    /// result, including originals already checked before a later source error.
    pub async fn originals(
        &self,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<QueueSourceOriginalEdits<'a>> {
        let expected = match self.checked.view().payload() {
            QueueLogPayload::Publication(publication) => publication.summary().receipts,
            QueueLogPayload::Claim(_) => 1,
            QueueLogPayload::Genesis(_) => unreachable!("context rejects genesis"),
        };
        if expected > limits.edits {
            return Err(unknown("original count exceeds admitted limits"));
        }
        let mut output = self.output(limits, "original")?;
        let mut processed = 0u64;
        match self.checked.view().payload() {
            QueueLogPayload::Publication(publication) => {
                for receipt in publication.receipts() {
                    let receipt =
                        receipt.map_err(|_| unknown("invalid checked original receipt"))?;
                    self.original(
                        &mut output,
                        receipt.domain,
                        receipt.request_id,
                        receipt.fingerprint_format,
                        receipt.fingerprint,
                        receipt.encoded_outcome(),
                    )
                    .await?;
                    processed = processed.checked_add(1).ok_or(QueueError::TooLarge)?;
                }
            }
            QueueLogPayload::Claim(claim) => {
                self.original(
                    &mut output,
                    1,
                    claim.request_id(),
                    claim.fingerprint_format(),
                    claim.fingerprint(),
                    claim.receipt_bytes(),
                )
                .await?;
                processed = 1;
            }
            QueueLogPayload::Genesis(_) => unreachable!("context rejects genesis"),
        }
        if processed != expected {
            return Err(unknown("incomplete original receipt coverage"));
        }
        Ok(QueueSourceOriginalEdits { output })
    }
    async fn original(
        &self,
        output: &mut SourceEdits<'a>,
        domain: u8,
        request_id: &str,
        fingerprint_format: u8,
        fingerprint: &[u8; 32],
        outcome: &[u8],
    ) -> QueueResult<()> {
        if request_id.trim().is_empty() {
            return Err(unknown("blank original request identity"));
        }
        let fact = QueueSourceOriginal {
            fingerprint_format,
            fingerprint: *fingerprint,
            outcome: QueueSourcePayload::from_checked(self.checked, self.cut.through(), outcome)?,
        };
        fact.validate(domain)?;
        let key = QueueSourceKey::Original { domain, request_id };
        let _key_charge = self.job.owned_buffer(key.encoded_len()? as u64)?;
        let key = key.encode()?;
        let lookup = self.reader.lookup(&key).await?;
        let point = self.reader.point(&lookup)?;
        // Even identical originals cannot be published twice. Replay resolution
        // returns the retained outcome before a new publication is constructed.
        if point.value_for(&self.cut, &key)?.is_some() {
            return Err(unknown("duplicate source original identity"));
        }
        output.insert_encoded(&key, Some(&fact))
    }
}

#[cfg(test)]
mod tests;
