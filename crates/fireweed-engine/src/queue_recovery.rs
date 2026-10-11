//! Operation-local cold-history validation. This is neither a projection cursor
//! nor an acknowledgement frontier; callers apply only after their own checks.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{QueueLogPayload, view_queue_log_record},
    queue_namespace::{AddressRef, encode_address, framed_address_len, view_address},
    queue_schema_codec::AdmittedQueueDefinition,
};
use bytes::Bytes;
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use std::sync::Arc;
fn invalid(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue history: {message}"))
}
/// One cold recovery operation retains its admitted genesis authority. Do not
/// cache this across operations or confuse validation progress with apply.
pub struct ColdQueueHistory {
    address: Bytes,
    definition: Option<AdmittedQueueDefinition>,
    cursor_secret: Option<[u8; 32]>,
    next_offset: u64,
    insertion_sequence: u64,
    epoch: u64,
    poisoned: bool,
    job: Arc<QueueReservation>,
    _charge: OwnedQueueBufferCharge,
}
impl ColdQueueHistory {
    pub fn new(address: &QueueAddress, job: &Arc<QueueReservation>) -> QueueResult<Self> {
        address.validate()?;
        let bytes =
            u64::try_from(framed_address_len(address)?).map_err(|_| QueueError::TooLarge)?;
        let charge = job.owned_buffer(
            bytes
                .checked_mul(4)
                .and_then(|n| n.checked_add(1024))
                .ok_or(QueueError::TooLarge)?,
        )?;
        Ok(Self {
            address: Bytes::from(encode_address(address)?),
            definition: None,
            cursor_secret: None,
            next_offset: 0,
            insertion_sequence: 0,
            epoch: 0,
            poisoned: false,
            job: Arc::clone(job),
            _charge: charge,
        })
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn insertion_sequence(&self) -> u64 {
        self.insertion_sequence
    }
    pub fn address(&self) -> QueueResult<AddressRef<'_>> {
        view_address(&self.address)
    }
    pub fn definition(&self) -> QueueResult<&AdmittedQueueDefinition> {
        if self.poisoned {
            return Err(invalid("poisoned recovery"));
        }
        self.definition
            .as_ref()
            .ok_or_else(|| invalid("genesis not observed"))
    }
    pub fn cursor_secret(&self) -> QueueResult<&[u8; 32]> {
        self.definition()?;
        self.cursor_secret
            .as_ref()
            .ok_or_else(|| invalid("genesis not observed"))
    }
    /// Observe sequential records only. Any invalid record poisons the operation
    /// so callers cannot skip corruption and continue toward an apparent EOF.
    pub fn observe(&mut self, offset: u64, raw: &[u8]) -> QueueResult<()> {
        if self.poisoned {
            return Err(invalid("poisoned recovery"));
        }
        let result = self.observe_checked(offset, raw);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn observe_checked(&mut self, offset: u64, raw: &[u8]) -> QueueResult<()> {
        if offset != self.next_offset {
            return Err(invalid("noncontiguous log offset"));
        }
        let next = offset
            .checked_add(1)
            .ok_or_else(|| invalid("offset overflow"))?;
        let record = view_queue_log_record(raw)?;
        if record.address() != self.address()? {
            return Err(invalid("full address mismatch"));
        }
        if record.epoch() < self.epoch {
            return Err(invalid("writer epoch regressed"));
        }
        match record.payload() {
            QueueLogPayload::Genesis(genesis) => {
                if offset != 0 || self.definition.is_some() {
                    return Err(invalid("genesis must occur exactly once at offset zero"));
                }
                let definition = genesis.validate_definition(&self.job)?;
                self.definition = Some(definition);
                self.cursor_secret = Some(*genesis.cursor_secret());
            }
            QueueLogPayload::Publication(frame) => {
                let definition = self.definition()?;
                let summary = frame.summary();
                if summary.definition_pin != definition.pin().as_bytes() {
                    return Err(invalid("definition identity mismatch"));
                }
                if summary.initial_sequence != self.insertion_sequence {
                    return Err(invalid("insertion sequence discontinuity"));
                }
                self.insertion_sequence = summary.final_sequence;
            }
        }
        self.epoch = record.epoch();
        self.next_offset = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_log_record::{encode_genesis, encode_publication_record},
        queue_planner::{RecordSnapshot, combine_deltas},
        queue_publication::encode_publication_pinned,
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema};
    fn schema() -> QueueSchema {
        QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    #[test]
    fn exact_genesis_and_both_publication_formats_preserve_authority() {
        let job = Arc::new(QueueAdmission::new(1, 1 << 20).unwrap().reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "t\0β".into(),
            queue: "q/λ".into(),
        };
        let legacy = [0, 0, 0, 0, 1, 3];
        let definition = AdmittedQueueDefinition::from_raw(&legacy, &job).unwrap();
        let genesis = encode_genesis(7, &address, &definition, &[19; 32], 4096, &job).unwrap();
        let before = RecordSnapshot::default();
        let delta = combine_deltas(&[], 0).unwrap();
        let frame =
            encode_publication_pinned(&address, definition.pin(), &before, &delta, &[], 4096, &job)
                .unwrap();
        let mut history = ColdQueueHistory::new(&address, &job).unwrap();
        assert!(history.definition().is_err());
        history.observe(0, genesis.bytes()).unwrap();
        assert_eq!(history.definition().unwrap().raw(), legacy);
        assert_eq!(history.cursor_secret().unwrap(), &[19; 32]);
        for (offset, version) in [(1, b'1'), (2, b'2')] {
            let mut raw = frame.as_bytes().to_vec();
            raw[4] = version;
            let record = encode_publication_record(8, &raw, 4096, &job).unwrap();
            history.observe(offset, record.bytes()).unwrap();
        }
        assert_eq!(history.next_offset(), 3);
        assert_eq!(history.insertion_sequence(), 0);
        assert_eq!(history.definition().unwrap().pin(), definition.pin());
    }
    #[test]
    fn invalid_histories_fail_closed_and_cannot_skip_to_eof() {
        let job = Arc::new(QueueAdmission::new(1, 1 << 20).unwrap().reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "t".into(),
            queue: "q".into(),
        };
        let raw = encode_definition(&schema()).unwrap();
        let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
        let genesis = encode_genesis(7, &address, &definition, &[3; 32], 4096, &job).unwrap();
        let before = RecordSnapshot::default();
        let delta = combine_deltas(&[], 0).unwrap();
        let frame =
            encode_publication_pinned(&address, definition.pin(), &before, &delta, &[], 4096, &job)
                .unwrap();
        let publication = encode_publication_record(8, frame.as_bytes(), 4096, &job).unwrap();
        let regressed = encode_publication_record(6, frame.as_bytes(), 4096, &job).unwrap();
        for (offset, bytes) in [(1, genesis.bytes()), (0, publication.bytes())] {
            let mut history = ColdQueueHistory::new(&address, &job).unwrap();
            assert!(history.observe(offset, bytes).is_err());
            assert!(history.observe(0, genesis.bytes()).is_err());
            assert_eq!(history.next_offset(), 0);
        }
        for (offset, bytes) in [
            (1, genesis.bytes()),
            (2, publication.bytes()),
            (1, regressed.bytes()),
            (1, &publication.bytes()[..publication.bytes().len() - 1]),
        ] {
            let mut history = ColdQueueHistory::new(&address, &job).unwrap();
            history.observe(0, genesis.bytes()).unwrap();
            assert!(history.observe(offset, bytes).is_err());
            assert!(history.definition().is_err());
            assert!(history.observe(1, publication.bytes()).is_err());
            assert_eq!(history.next_offset(), 1);
        }
        // Semantically equivalent modern definition still has a different raw pin.
        let legacy = AdmittedQueueDefinition::from_raw(&[0, 0, 0, 0, 1, 3], &job).unwrap();
        let wrong =
            encode_publication_pinned(&address, legacy.pin(), &before, &delta, &[], 4096, &job)
                .unwrap();
        let wrong = encode_publication_record(8, wrong.as_bytes(), 4096, &job).unwrap();
        let mut history = ColdQueueHistory::new(&address, &job).unwrap();
        history.observe(0, genesis.bytes()).unwrap();
        assert!(history.observe(1, wrong.bytes()).is_err());

        let mut invalid_schema = genesis.bytes().to_vec();
        let schema_start = invalid_schema
            .windows(raw.len())
            .position(|candidate| candidate == raw)
            .unwrap();
        invalid_schema[schema_start + raw.len() - 1] = 0;
        let mut history = ColdQueueHistory::new(&address, &job).unwrap();
        assert!(history.observe(0, &invalid_schema).is_err());
        assert_eq!(history.next_offset(), 0);
        // Derive the fixed header location from exact full address lengths.
        let initial_offset = 5 + 8 + address.tenant.len() + 8 + address.queue.len() + 32;
        let mut discontinuous = frame.as_bytes().to_vec();
        discontinuous[initial_offset..initial_offset + 8].copy_from_slice(&1u64.to_le_bytes());
        discontinuous[initial_offset + 8..initial_offset + 16].copy_from_slice(&1u64.to_le_bytes());
        let discontinuous = encode_publication_record(8, &discontinuous, 4096, &job).unwrap();
        let mut history = ColdQueueHistory::new(&address, &job).unwrap();
        history.observe(0, genesis.bytes()).unwrap();
        assert!(history.observe(1, discontinuous.bytes()).is_err());
        assert_eq!(history.insertion_sequence(), 0);
    }
}
