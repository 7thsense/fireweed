//! Borrowed operation/cut identity supplied by an authenticated source adapter.
//! Numeric epoch/offset equality never substitutes for that identity.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{CheckedQueueLogRecord, QueueLogFormat, QueueLogPayload},
    queue_namespace::AddressRef,
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::Arc;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue source cut: {message}"))
}

/// Borrowing retains the adapter's admitted exact-cut capability. Construction
/// is a trusted port assertion, not storage qualification performed by engine
/// code. The adapter must use one acquired bundle identity and opaque SDK cut.
#[derive(Clone, Copy)]
pub struct QueueSourceCut<'a> {
    identity: &'a Arc<OwnedQueueBufferCharge>,
    address: AddressRef<'a>,
    epoch: u64,
    through: u64,
}
impl<'a> QueueSourceCut<'a> {
    pub fn from_captured(
        identity: &'a Arc<OwnedQueueBufferCharge>,
        address: AddressRef<'a>,
        epoch: u64,
        through: u64,
        job: &QueueReservation,
    ) -> QueueResult<Self> {
        address.validate()?;
        if !std::ptr::eq(identity.reservation(), job) || through > i64::MAX as u64 {
            return Err(unknown("captured owner or logical bound differs"));
        }
        Ok(Self {
            identity,
            address,
            epoch,
            through,
        })
    }
    pub fn address(&self) -> AddressRef<'a> {
        self.address
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn through(&self) -> u64 {
        self.through
    }
    pub fn same_cut(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.identity, other.identity)
            && self.address == other.address
            && self.epoch == other.epoch
            && self.through == other.through
    }
    pub fn belongs_to(&self, job: &QueueReservation) -> bool {
        std::ptr::eq(self.identity.reservation(), job)
    }
    /// Bind a newly resolved publication to the exact captured predecessor.
    /// This checks domain framing/ownership; the SDK still verifies its opaque
    /// storage token under the writer lock at preparation/publication.
    pub fn validate_publication(
        &self,
        record: &CheckedQueueLogRecord<'_>,
        job: &QueueReservation,
    ) -> QueueResult<()> {
        if !self.belongs_to(job)
            || !record.owner().belongs_to(job)
            || record.view().address() != self.address
            || record.view().epoch() != self.epoch
            || record.view().format() != QueueLogFormat::ChecksummedV2
            || matches!(record.view().payload(), QueueLogPayload::Genesis(_)) != (self.through == 0)
        {
            return Err(unknown(
                "publication owner, queue, epoch or genesis differs",
            ));
        }
        Ok(())
    }
    /// Trusted adapter boundary: value must be the complete lookup result for
    /// this full key under this exact cut, with retained source owners. None is
    /// authenticated absence; source errors must never invoke this constructor.
    pub fn point_from_source(self, key: &'a [u8], value: Option<&'a [u8]>) -> QueueSourcePoint<'a> {
        QueueSourcePoint {
            cut: self,
            key,
            value,
        }
    }
}
/// Borrowed exact-key source evidence. The producing adapter retains payload
/// and admission owners for the entire borrow, including authoritative absence.
pub struct QueueSourcePoint<'a> {
    cut: QueueSourceCut<'a>,
    key: &'a [u8],
    value: Option<&'a [u8]>,
}
impl<'a> QueueSourcePoint<'a> {
    pub fn key(&self) -> &'a [u8] {
        self.key
    }
    pub fn value_for(&self, cut: &QueueSourceCut<'_>, key: &[u8]) -> QueueResult<Option<&'a [u8]>> {
        if !self.cut.same_cut(cut) || key != self.key {
            return Err(unknown(
                "lookup evidence belongs to another cut or full key",
            ));
        }
        Ok(self.value)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    #[test]
    fn equal_numeric_heads_and_prefix_keys_cannot_substitute_for_source_evidence() {
        let admission = QueueAdmission::new(2, 4096).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let other = Arc::new(admission.reserve(0).unwrap());
        let first = Arc::new(job.owned_buffer(128).unwrap());
        let second = Arc::new(job.owned_buffer(128).unwrap());
        let address = AddressRef {
            tenant: "tenant",
            queue: "queue",
        };
        let cut = QueueSourceCut::from_captured(&first, address, 7, 11, &job).unwrap();
        let alias = cut;
        let unrelated = QueueSourceCut::from_captured(&second, address, 7, 11, &job).unwrap();
        let different_offset = QueueSourceCut::from_captured(&first, address, 7, 12, &job).unwrap();
        let different_epoch = QueueSourceCut::from_captured(&first, address, 8, 11, &job).unwrap();
        let other_address = QueueSourceCut::from_captured(
            &first,
            AddressRef {
                tenant: "tenant",
                queue: "other",
            },
            7,
            11,
            &job,
        )
        .unwrap();
        assert!(QueueSourceCut::from_captured(&first, address, 7, 11, &other).is_err());
        let key = b"full\0literal\xff";
        let present = cut.point_from_source(key, Some(b""));
        assert_eq!(
            present.value_for(&alias, key).unwrap(),
            Some(b"".as_slice())
        );
        assert!(present.value_for(&cut, b"full").is_err());
        for wrong in [unrelated, different_offset, different_epoch, other_address] {
            assert!(present.value_for(&wrong, key).is_err());
        }
        assert_eq!(
            cut.point_from_source(key, None)
                .value_for(&cut, key)
                .unwrap(),
            None
        );
    }
}
