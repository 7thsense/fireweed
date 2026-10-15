//! Payload references bind the full checksummed queue record and exact byte span.
//! They avoid copying unchanged body/state/result bytes into every index update.
use crate::queue_log_record::{CheckedQueueLogRecord, QueueLogFormat};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use serde::{Deserialize, Serialize};

fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue source reference: {message}"))
}

/// The record checksum is the already-verified FWQL2 trailer, covering its full
/// framing and payload. A reference is not itself proof that a record came from
/// a particular storage cut: the adapter must authenticate log_offset under that
/// same cut before passing its checked record to resolve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSourceSpan {
    log_offset: u64,
    record_checksum: [u8; 32],
    byte_offset: u64,
    byte_length: u64,
}
impl QueueSourceSpan {
    fn checksum(record: &CheckedQueueLogRecord<'_>) -> QueueResult<[u8; 32]> {
        if record.view().format() != QueueLogFormat::ChecksummedV2 {
            return Err(unknown("payload references require checked FWQL2"));
        }
        let bytes = record.owner().bytes();
        bytes
            .get(
                bytes
                    .len()
                    .checked_sub(32)
                    .ok_or_else(|| unknown("checksum length"))?..,
            )
            .ok_or_else(|| unknown("checksum missing"))?
            .try_into()
            .map_err(|_| unknown("checksum width"))
    }
    /// Derive only from an actual borrowed span of this immutable checked owner.
    /// No payload bytes are copied or hashed again. The physical source offset
    /// must be supplied by the exact-cut adapter, never by a projection row.
    pub fn from_checked(
        record: &CheckedQueueLogRecord<'_>,
        log_offset: u64,
        value: &[u8],
    ) -> QueueResult<Self> {
        if log_offset > i64::MAX as u64 {
            return Err(unknown("log offset outside native domain"));
        }
        let bytes = record.owner().bytes();
        let offset = value
            .as_ptr()
            .addr()
            .checked_sub(bytes.as_ptr().addr())
            .ok_or_else(|| unknown("span is not borrowed from record"))?;
        let end = offset
            .checked_add(value.len())
            .ok_or_else(|| unknown("span overflow"))?;
        let payload = record.payload_interval()?;
        if offset < payload.start || end > payload.end {
            return Err(unknown("span is outside record payload"));
        }
        Ok(Self {
            log_offset,
            record_checksum: Self::checksum(record)?,
            byte_offset: u64::try_from(offset).map_err(|_| unknown("span offset width"))?,
            byte_length: u64::try_from(value.len()).map_err(|_| unknown("span length width"))?,
        })
    }
    pub fn log_offset(&self) -> u64 {
        self.log_offset
    }
    pub fn byte_length(&self) -> u64 {
        self.byte_length
    }
    /// Return a protected borrow only after full record identity, physical
    /// offset, and checked arithmetic/range validation. Never truncate a span.
    pub fn resolve<'a>(
        &self,
        record: &CheckedQueueLogRecord<'a>,
        observed_log_offset: u64,
    ) -> QueueResult<&'a [u8]> {
        if self.log_offset > i64::MAX as u64
            || observed_log_offset != self.log_offset
            || Self::checksum(record)? != self.record_checksum
        {
            return Err(unknown("record identity or source offset differs"));
        }
        let start = usize::try_from(self.byte_offset).map_err(|_| unknown("span offset width"))?;
        let length = usize::try_from(self.byte_length).map_err(|_| unknown("span length width"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| unknown("span overflow"))?;
        let bytes = record.owner().bytes();
        let payload = record.payload_interval()?;
        if start < payload.start || end > payload.end {
            return Err(unknown("span outside checked payload"));
        }
        bytes.get(start..end).ok_or_else(|| unknown("span missing"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_log_record::encode_checked_genesis,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use std::sync::Arc;
    #[test]
    fn references_bind_full_record_and_exact_owner_span_without_copying() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let address = QueueAddress {
            tenant: "tenant\0é".into(),
            queue: "queue".into(),
        };
        let record =
            encode_checked_genesis(7, &address, &definition, &[4; 32], 4096, &job).unwrap();
        let other = encode_checked_genesis(7, &address, &definition, &[5; 32], 4096, &job).unwrap();
        let checked = record.checked().unwrap();
        let crate::queue_log_record::QueueLogPayload::Genesis(genesis) = checked.view().payload()
        else {
            panic!()
        };
        let payload = genesis.definition_bytes();
        let span = QueueSourceSpan::from_checked(&checked, 42, payload).unwrap();
        assert_eq!(
            span.resolve(&checked, 42).unwrap().as_ptr(),
            payload.as_ptr()
        );
        assert_eq!(span.byte_length(), payload.len() as u64);
        assert!(span.resolve(&checked, 41).is_err());
        // Equal field bytes in another valid record cannot substitute for the
        // full original record, even at the same claimed source offset.
        assert!(span.resolve(&other.checked().unwrap(), 42).is_err());
        // Identical bytes in a different allocation are not a protected slice
        // of this checked record.
        let foreign_payload = payload.to_vec();
        assert_ne!(foreign_payload.as_ptr(), payload.as_ptr());
        assert!(QueueSourceSpan::from_checked(&checked, 42, &foreign_payload).is_err());
        let empty = &payload[payload.len()..];
        let empty_ref = QueueSourceSpan::from_checked(&checked, 42, empty).unwrap();
        assert!(empty_ref.resolve(&checked, 42).unwrap().is_empty());
        assert!(QueueSourceSpan::from_checked(&checked, u64::MAX, payload).is_err());
        let mut malformed = span;
        malformed.byte_offset = u64::MAX;
        assert!(malformed.resolve(&checked, 42).is_err());
        malformed = span;
        malformed.byte_length = u64::MAX;
        assert!(malformed.resolve(&checked, 42).is_err());
        let interval = checked.payload_interval().unwrap();
        for range in [0..1, 0..0, (interval.start - 1)..(interval.start + 1)] {
            assert!(QueueSourceSpan::from_checked(&checked, 42, &record.bytes()[range]).is_err());
        }
        for index in [interval.start, interval.end] {
            let empty =
                QueueSourceSpan::from_checked(&checked, 42, &record.bytes()[index..index]).unwrap();
            assert!(empty.resolve(&checked, 42).unwrap().is_empty());
        }
        malformed = span;
        malformed.byte_offset = 0;
        malformed.byte_length = 1;
        assert!(malformed.resolve(&checked, 42).is_err());
        malformed.byte_offset = (interval.start - 1) as u64;
        malformed.byte_length = 2;
        assert!(malformed.resolve(&checked, 42).is_err());
        let trailer = &record.bytes()[record.bytes().len() - 32..];
        assert!(QueueSourceSpan::from_checked(&checked, 42, trailer).is_err());
    }
}
