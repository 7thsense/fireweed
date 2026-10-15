//! Construction bounds for records emitted by the admitted API-007 producers.
//! These are logical wire ceilings, independent of an operation's byte credit.
use super::checked_publication_record_len;
use fireweed_core::queue_contract::{QueueError, QueueLimits, QueueResult};

/// Conservative complete FWQL2 record bounds. Arbitrary inputs to lower-level
/// writers are outside this domain: requests/results must pass the shared
/// interface limits, and administrative producers enforce the whole-frame cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueLogRecordBounds {
    pub genesis: u64,
    pub ordinary: u64,
    pub administrative: u64,
    pub claim: u64,
}
impl QueueLogRecordBounds {
    /// Pure checked arithmetic; no encoding, allocation, runtime or storage I/O.
    pub fn for_limits(limits: &QueueLimits) -> QueueResult<Self> {
        limits.validate()?;
        let overflow = || QueueError::TooLarge;
        let request = limits.max_request_bytes;
        let count = u64::from(limits.max_records)
            .checked_mul(u64::from(limits.max_groups))
            .and_then(|n| n.checked_add(u64::from(limits.max_groups)))
            .and_then(|n| n.checked_add(1))
            .ok_or_else(overflow)?;
        // Ordinary fixed-width values can expand compact Postcard integers by
        // eight. This also covers duplicated command/receipt identities and
        // per-record metadata, including insert and homogeneous commit packs.
        let inner = request
            .checked_mul(8)
            .and_then(|n| n.checked_add(count.checked_mul(512)?))
            .ok_or_else(overflow)?;
        let ordinary = checked_publication_record_len(inner)?;
        // Administrative success and fallback producers cap the entire binary
        // publication, including broad commands and projected before fields.
        let administrative = checked_publication_record_len(limits.max_group_bytes)?;
        // Claim commands preserve body/fields. Each selected key appears twice
        // and each token once, covered by two complete response allowances.
        // 140 = 116 fixed lease-command bytes + 24 FWCR1 entry bytes; this bound
        // does not depend on a particular lease-token generator's byte length.
        let claim = request
            .checked_add(
                limits
                    .max_response_bytes
                    .checked_mul(2)
                    .ok_or_else(overflow)?,
            )
            .and_then(|n| n.checked_add(u64::from(limits.max_records).checked_mul(140)?))
            .and_then(|n| n.checked_add(237))
            .ok_or_else(overflow)?;
        let genesis = request.checked_add(141).ok_or_else(overflow)?;
        Ok(Self {
            genesis,
            ordinary,
            administrative,
            claim,
        })
    }
    pub fn maximum(self) -> u64 {
        self.genesis
            .max(self.ordinary)
            .max(self.administrative)
            .max(self.claim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn construction_bounds_reject_arithmetic_overflow_without_encoding() {
        let default = QueueLogRecordBounds::for_limits(&QueueLimits::default()).unwrap();
        assert!(default.maximum() > u64::from(u32::MAX) / 2);
        assert!(default.maximum() < u64::from(u32::MAX));
        for limits in [
            QueueLimits {
                max_request_bytes: u64::MAX,
                ..QueueLimits::default()
            },
            QueueLimits {
                max_response_bytes: u64::MAX,
                ..QueueLimits::default()
            },
            QueueLimits {
                max_records: u32::MAX,
                max_groups: u32::MAX,
                ..QueueLimits::default()
            },
        ] {
            assert!(matches!(
                QueueLogRecordBounds::for_limits(&limits),
                Err(QueueError::TooLarge)
            ));
        }
    }
}
