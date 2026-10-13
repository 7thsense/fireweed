//! Bounded original descriptors folded from a complete, independently checked
//! prefix. Source qualification and final head freshness remain adapter duties.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{OwnedQueueLogRecord, QueueLogPayload},
    queue_namespace::{encode_address, framed_address_len, view_address},
    queue_recovery::ColdQueueHistory,
};
use fireweed_core::queue_contract::{CommitBatch, QueueError, QueueResult};
use sha2::{Digest, Sha256};
use std::sync::Arc;
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("original authority: {reason}"))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginalDescriptor {
    pub fingerprint_format: u8,
    pub fingerprint: [u8; 32],
    pub outcome_bytes: u64,
    pub outcome_sha256: [u8; 32],
}
impl OriginalDescriptor {
    pub fn verify_outcome(&self, raw: &[u8]) -> QueueResult<()> {
        if raw.len() as u64 != self.outcome_bytes
            || <[u8; 32]>::from(Sha256::digest(raw)) != self.outcome_sha256
        {
            return Err(unknown("cached original outcome differs from source"));
        }
        Ok(())
    }
}
struct Entry {
    id: String,
    descriptor: Option<OriginalDescriptor>,
}
/// One operation's ordered original-ID subset. No queue-sized map is retained.
pub struct ColdOriginalTracker {
    address: Vec<u8>,
    entries: Vec<Entry>,
    genesis_sha256: Option<[u8; 32]>,
    next_offset: u64,
    insertion_sequence: u64,
    historical_epoch: u64,
    poisoned: bool,
    _charge: OwnedQueueBufferCharge,
}
/// Immutable descriptors for the declared complete checked cut, not permission
/// to trust a projection or to reuse a prefix across operations.
pub struct AdmittedOriginalAuthority {
    address: Vec<u8>,
    entries: Vec<Entry>,
    genesis_sha256: [u8; 32],
    progress: (u64, u64, u64),
    _charge: OwnedQueueBufferCharge,
}
impl ColdOriginalTracker {
    pub fn new(batch: &CommitBatch, job: &Arc<QueueReservation>) -> QueueResult<Self> {
        batch.queue.validate()?;
        let mut capacity = u64::try_from(framed_address_len(&batch.queue)?)
            .map_err(|_| QueueError::TooLarge)?
            .checked_mul(8)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        for group in &batch.groups {
            capacity = capacity
                .checked_add(
                    (group.request_id.len() as u64)
                        .checked_mul(8)
                        .ok_or(QueueError::TooLarge)?,
                )
                .and_then(|n| n.checked_add((std::mem::size_of::<Entry>() as u64) * 4))
                .ok_or(QueueError::TooLarge)?;
        }
        let charge = job.owned_buffer(capacity)?;
        let mut entries = Vec::with_capacity(batch.groups.len());
        for group in &batch.groups {
            if group.request_id.is_empty()
                || entries
                    .iter()
                    .any(|entry: &Entry| entry.id == group.request_id)
            {
                return Err(QueueError::Invalid(
                    "batch original IDs must be nonempty and unique".into(),
                ));
            }
            entries.push(Entry {
                id: group.request_id.clone(),
                descriptor: None,
            });
        }
        Ok(Self {
            address: encode_address(&batch.queue)?,
            entries,
            genesis_sha256: None,
            next_offset: 0,
            insertion_sequence: 0,
            historical_epoch: 0,
            poisoned: false,
            _charge: charge,
        })
    }
    pub fn observe(&mut self, offset: u64, record: &OwnedQueueLogRecord) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned prefix"));
        }
        let result = self.observe_checked(offset, record);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn observe_checked(&mut self, offset: u64, record: &OwnedQueueLogRecord) -> QueueResult<()> {
        if !record.belongs_to(self._charge.reservation()) || offset != self.next_offset {
            return Err(unknown("record operation or contiguous offset mismatch"));
        }
        let view = record.view()?;
        if view.address() != view_address(&self.address)? || view.epoch() < self.historical_epoch {
            return Err(unknown("record address or epoch mismatch"));
        }
        match view.payload() {
            QueueLogPayload::Genesis(_) => {
                if offset != 0 || self.genesis_sha256.is_some() {
                    return Err(unknown("genesis uniqueness"));
                }
                self.genesis_sha256 = Some(Sha256::digest(record.bytes()).into());
            }
            QueueLogPayload::Publication(frame) => {
                let summary = frame.summary();
                if self.genesis_sha256.is_none()
                    || summary.initial_sequence != self.insertion_sequence
                {
                    return Err(unknown("publication predecessor mismatch"));
                }
                for receipt in frame.receipts() {
                    let receipt = receipt.map_err(|_| unknown("invalid source receipt"))?;
                    if receipt.domain != 2 {
                        continue;
                    }
                    if let Some(entry) = self
                        .entries
                        .iter_mut()
                        .find(|entry| entry.id == receipt.request_id)
                    {
                        if entry.descriptor.is_some() {
                            return Err(unknown("duplicate source original identity"));
                        }
                        entry.descriptor = Some(OriginalDescriptor {
                            fingerprint_format: receipt.fingerprint_format,
                            fingerprint: *receipt.fingerprint,
                            outcome_bytes: receipt.encoded_outcome().len() as u64,
                            outcome_sha256: Sha256::digest(receipt.encoded_outcome()).into(),
                        });
                    }
                }
                self.insertion_sequence = summary.final_sequence;
            }
            QueueLogPayload::Claim(_) => {
                if self.genesis_sha256.is_none() {
                    return Err(unknown("claim before genesis"));
                }
            }
        }
        self.historical_epoch = view.epoch();
        self.next_offset = offset
            .checked_add(1)
            .ok_or_else(|| unknown("offset exhausted"))?;
        Ok(())
    }
    /// Call only after full ColdQueueHistory validation and final checkpoint
    /// agreement with the acquired target; this method verifies their binding.
    pub fn finish(
        self,
        history: &ColdQueueHistory,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
    ) -> QueueResult<AdmittedOriginalAuthority> {
        let hash = self
            .genesis_sha256
            .ok_or_else(|| unknown("missing genesis"))?;
        history.definition()?;
        if self.poisoned
            || !history.belongs_to(self._charge.reservation())
            || history.address()? != view_address(&self.address)?
            || progress
                != (
                    history.next_offset(),
                    history.insertion_sequence(),
                    history.historical_epoch(),
                )
            || progress
                != (
                    self.next_offset,
                    self.insertion_sequence,
                    self.historical_epoch,
                )
            || !genesis.belongs_to(self._charge.reservation())
            || hash != <[u8; 32]>::from(Sha256::digest(genesis.bytes()))
        {
            return Err(unknown("incomplete or mismatched checked prefix"));
        }
        Ok(AdmittedOriginalAuthority {
            address: self.address,
            entries: self.entries,
            genesis_sha256: hash,
            progress,
            _charge: self._charge,
        })
    }
}
impl AdmittedOriginalAuthority {
    pub fn validate(
        &self,
        batch: &CommitBatch,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        if !std::ptr::eq(self._charge.reservation(), job)
            || !genesis.belongs_to(job)
            || view_address(&self.address)?.tenant != batch.queue.tenant
            || view_address(&self.address)?.queue != batch.queue.queue
            || self.progress != progress
            || self.genesis_sha256 != <[u8; 32]>::from(Sha256::digest(genesis.bytes()))
            || self.entries.len() != batch.groups.len()
            || self
                .entries
                .iter()
                .zip(&batch.groups)
                .any(|(entry, group)| entry.id != group.request_id)
        {
            return Err(unknown(
                "snapshot operation/address/genesis/progress/position mismatch",
            ));
        }
        Ok(())
    }
    pub fn descriptor(&self, position: usize, id: &str) -> QueueResult<Option<OriginalDescriptor>> {
        let entry = self
            .entries
            .get(position)
            .filter(|entry| entry.id == id)
            .ok_or_else(|| unknown("original position mismatch"))?;
        Ok(entry.descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_log_record::{encode_checked_genesis, encode_checked_publication_record},
        queue_planner::{RecordSnapshot, combine_deltas},
        queue_publication::{PublicationReceipt, encode_publication_pinned},
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    use fireweed_core::queue_contract::*;
    fn batch() -> CommitBatch {
        let group = |id: &str| CommitGroup {
            request_id: id.into(),
            administration: None,
            claimed_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![UnclaimedUpdate {
                key: "missing".into(),
                expect: StateExpect::Version(1),
                change: UnclaimedChange::Patch(WorkPatch {
                    priority: Some(1),
                    ..Default::default()
                }),
            }],
        };
        CommitBatch {
            queue: QueueAddress {
                tenant: "tenant\0β".into(),
                queue: "work/λ".into(),
            },
            groups: vec![group("known"), group("fresh")],
        }
    }
    fn records(batch: &CommitBatch, job: &Arc<QueueReservation>) -> Vec<OwnedQueueLogRecord> {
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
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), job).unwrap();
        let genesis =
            encode_checked_genesis(1, &batch.queue, &definition, &[41; 32], 1 << 20, job).unwrap();
        let receipt = CommitReceipt {
            request_id: batch.groups[0].request_id.clone(),
            outcome: GroupOutcome::Rejected(Rejection::NotFound),
        };
        let original =
            PublicationReceipt::commit(&batch.queue, &batch.groups[0], &receipt).unwrap();
        let frame = encode_publication_pinned(
            &batch.queue,
            definition.pin(),
            &RecordSnapshot::default(),
            &combine_deltas(&[], 0).unwrap(),
            &[original],
            1 << 20,
            job,
        )
        .unwrap();
        vec![
            genesis,
            encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, job).unwrap(),
        ]
    }
    fn fold(
        batch: &CommitBatch,
        records: &[OwnedQueueLogRecord],
        job: &Arc<QueueReservation>,
    ) -> (ColdOriginalTracker, ColdQueueHistory) {
        let mut tracker = ColdOriginalTracker::new(batch, job).unwrap();
        let mut history = ColdQueueHistory::new(&batch.queue, job).unwrap();
        for (offset, record) in records.iter().enumerate() {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        (tracker, history)
    }
    #[test]
    fn exact_source_descriptors_bind_position_scope_genesis_job_and_cut() {
        let job = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        let batch = batch();
        let records = records(&batch, &job);
        let (tracker, history) = fold(&batch, &records, &job);
        let proof = tracker.finish(&history, &records[0], (2, 0, 1)).unwrap();
        proof
            .validate(&batch, &records[0], (2, 0, 1), &job)
            .unwrap();
        let descriptor = proof.descriptor(0, "known").unwrap().unwrap();
        let view = records[1].view().unwrap();
        let QueueLogPayload::Publication(frame) = view.payload() else {
            panic!("publication")
        };
        let receipt = frame.receipts().next().unwrap().unwrap();
        assert_eq!(descriptor.fingerprint_format, 3);
        assert_eq!(descriptor.fingerprint, *receipt.fingerprint);
        descriptor
            .verify_outcome(receipt.encoded_outcome())
            .unwrap();
        assert!(descriptor.verify_outcome(&[0xff; 2]).is_err());
        assert_eq!(proof.descriptor(1, "fresh").unwrap(), None);
        assert!(proof.descriptor(0, "fresh").is_err());
        assert!(
            proof
                .validate(&batch, &records[0], (1, 0, 1), &job)
                .is_err()
        );
        assert!(
            proof
                .validate(&batch, &records[0], (2, 1, 1), &job)
                .is_err()
        );
        assert!(
            proof
                .validate(&batch, &records[0], (2, 0, 2), &job)
                .is_err()
        );
        let other = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        assert!(
            proof
                .validate(&batch, &records[0], (2, 0, 1), &other)
                .is_err()
        );
        let mut changed = batch.clone();
        changed.queue.tenant.push('x');
        assert!(
            proof
                .validate(&changed, &records[0], (2, 0, 1), &job)
                .is_err()
        );
        changed = batch.clone();
        changed.groups.reverse();
        assert!(
            proof
                .validate(&changed, &records[0], (2, 0, 1), &job)
                .is_err()
        );
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
        let changed_genesis =
            encode_checked_genesis(1, &batch.queue, &definition, &[42; 32], 1 << 20, &job).unwrap();
        assert!(
            proof
                .validate(&batch, &changed_genesis, (2, 0, 1), &job)
                .is_err()
        );
    }
    #[test]
    fn late_duplicate_source_poison_and_incomplete_prefix_cannot_finish() {
        let job = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        let batch = batch();
        let records = records(&batch, &job);
        let (tracker, history) = fold(&batch, &records, &job);
        assert!(tracker.finish(&history, &records[0], (1, 0, 1)).is_err());
        let (mut tracker, mut history) = fold(&batch, &records, &job);
        history.observe(2, records[1].bytes()).unwrap();
        assert!(tracker.observe(2, &records[1]).is_err());
        assert!(tracker.observe(2, &records[1]).is_err());
        assert!(tracker.finish(&history, &records[0], (3, 0, 1)).is_err());
    }
    #[test]
    fn original_header_admission_and_unique_positions_precede_allocation() {
        let admission = QueueAdmission::new(1, 512).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        assert!(matches!(
            ColdOriginalTracker::new(&batch(), &job),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap(), (1, 0));
        let job = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        let mut duplicate = batch();
        duplicate.groups.push(duplicate.groups[0].clone());
        assert!(matches!(
            ColdOriginalTracker::new(&duplicate, &job),
            Err(QueueError::Invalid(_))
        ));
    }
}
