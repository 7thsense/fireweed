//! Bounded original descriptors folded from a complete, independently checked
//! prefix. Source qualification and final head freshness remain adapter duties.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{OwnedQueueLogRecord, QueueLogPayload},
    queue_namespace::{encode_address, framed_address_len, view_address},
    queue_recovery::ColdQueueHistory,
};
use fireweed_core::queue_contract::{
    CommitBatch, QueueAddress, QueueError, QueueResult, ReceiptReadRequest,
};
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
    domain: u8,
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
    domain: u8,
    address: Vec<u8>,
    entries: Vec<Entry>,
    genesis_sha256: [u8; 32],
    progress: (u64, u64, u64),
    _charge: OwnedQueueBufferCharge,
}
impl ColdOriginalTracker {
    pub fn new(batch: &CommitBatch, job: &Arc<QueueReservation>) -> QueueResult<Self> {
        Self::new_ids(
            &batch.queue,
            2,
            batch.groups.iter().map(|group| group.request_id.as_str()),
            false,
            job,
        )
    }
    /// One literal original in its immutable insert, claim or commit domain.
    pub fn new_request(
        queue: &QueueAddress,
        domain: u8,
        id: &str,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        Self::new_ids(queue, domain, std::iter::once(id), false, job)
    }
    /// Ordered receipt lookups retain duplicate positions and valid empty lists.
    pub fn new_lookup(
        request: &ReceiptReadRequest,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        Self::new_ids(
            &request.queue,
            2,
            request.request_ids.iter().map(String::as_str),
            true,
            job,
        )
    }
    fn new_ids<'a>(
        queue: &QueueAddress,
        domain: u8,
        ids: impl ExactSizeIterator<Item = &'a str> + Clone,
        allow_duplicates: bool,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        queue.validate()?;
        if domain > 2 {
            return Err(QueueError::Invalid("unsupported original domain".into()));
        }
        let mut capacity = u64::try_from(framed_address_len(queue)?)
            .map_err(|_| QueueError::TooLarge)?
            .checked_mul(8)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        for id in ids.clone() {
            capacity = capacity
                .checked_add(
                    (id.len() as u64)
                        .checked_mul(8)
                        .ok_or(QueueError::TooLarge)?,
                )
                .and_then(|n| n.checked_add((std::mem::size_of::<Entry>() as u64) * 4))
                .ok_or(QueueError::TooLarge)?;
        }
        let charge = job.owned_buffer(capacity)?;
        let mut entries = Vec::with_capacity(ids.len());
        for id in ids {
            if id.is_empty()
                || (!allow_duplicates && entries.iter().any(|entry: &Entry| entry.id == id))
            {
                return Err(QueueError::Invalid(
                    "original IDs must be nonempty and unique".into(),
                ));
            }
            entries.push(Entry {
                id: id.to_owned(),
                descriptor: None,
            });
        }
        Ok(Self {
            domain,
            address: encode_address(queue)?,
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
                    if receipt.domain != self.domain {
                        continue;
                    }
                    let mut positions = self
                        .entries
                        .iter_mut()
                        .filter(|entry| entry.id == receipt.request_id)
                        .peekable();
                    if positions.peek().is_some() {
                        let descriptor = OriginalDescriptor {
                            fingerprint_format: receipt.fingerprint_format,
                            fingerprint: *receipt.fingerprint,
                            outcome_bytes: receipt.encoded_outcome().len() as u64,
                            outcome_sha256: Sha256::digest(receipt.encoded_outcome()).into(),
                        };
                        for entry in positions {
                            if entry.descriptor.is_some() {
                                return Err(unknown("duplicate source original identity"));
                            }
                            entry.descriptor = Some(descriptor);
                        }
                    }
                }
                self.insertion_sequence = summary.final_sequence;
            }
            QueueLogPayload::Claim(claim) => {
                if self.genesis_sha256.is_none() {
                    return Err(unknown("claim before genesis"));
                }
                if self.domain == 1
                    && let Some(entry) = self
                        .entries
                        .iter_mut()
                        .find(|entry| entry.id == claim.request_id())
                {
                    if entry.descriptor.is_some() {
                        return Err(unknown("duplicate source original identity"));
                    }
                    entry.descriptor = Some(OriginalDescriptor {
                        fingerprint_format: claim.fingerprint_format(),
                        fingerprint: *claim.fingerprint(),
                        outcome_bytes: claim.receipt_bytes().len() as u64,
                        outcome_sha256: Sha256::digest(claim.receipt_bytes()).into(),
                    });
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
            domain: self.domain,
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
        self.validate_cut(&batch.queue, genesis, progress, job)?;
        if self.domain != 2 {
            return Err(unknown("batch original domain mismatch"));
        }
        if self.entries.len() != batch.groups.len()
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
    /// Bind the complete ordered lookup, including multiplicity and empty vectors.
    pub fn validate_lookup(
        &self,
        request: &ReceiptReadRequest,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.validate_cut(&request.queue, genesis, progress, job)?;
        if self.domain != 2
            || self.entries.len() != request.request_ids.len()
            || self
                .entries
                .iter()
                .zip(&request.request_ids)
                .any(|(entry, id)| &entry.id != id)
        {
            return Err(unknown(
                "receipt lookup domain or ordered positions mismatch",
            ));
        }
        Ok(())
    }
    fn validate_cut(
        &self,
        queue: &QueueAddress,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        if !std::ptr::eq(self._charge.reservation(), job)
            || !genesis.belongs_to(job)
            || view_address(&self.address)?.tenant != queue.tenant
            || view_address(&self.address)?.queue != queue.queue
            || self.progress != progress
            || self.genesis_sha256 != <[u8; 32]>::from(Sha256::digest(genesis.bytes()))
        {
            return Err(unknown(
                "original operation/address/genesis/progress mismatch",
            ));
        }
        Ok(())
    }
    pub fn position(
        self: &Arc<Self>,
        position: usize,
        id: &str,
    ) -> QueueResult<AdmittedOriginalPosition> {
        self.descriptor(position, id)?;
        Ok(AdmittedOriginalPosition {
            authority: self.clone(),
            position,
        })
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

/// A declared literal-ID position retaining the complete admitted proof owner.
/// Some(handle) with an absent descriptor never means an unproved lookup.
#[derive(Clone)]
pub struct AdmittedOriginalPosition {
    authority: Arc<AdmittedOriginalAuthority>,
    position: usize,
}
impl AdmittedOriginalPosition {
    pub fn validate(
        &self,
        queue: &QueueAddress,
        id: &str,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<Option<OriginalDescriptor>> {
        self.validate_domain(2, queue, id, genesis, progress, job)
    }
    /// Domain binding is required even when the declared original is absent.
    pub fn validate_domain(
        &self,
        domain: u8,
        queue: &QueueAddress,
        id: &str,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<Option<OriginalDescriptor>> {
        if self.authority.domain != domain {
            return Err(unknown("original domain mismatch"));
        }
        self.authority.validate_cut(queue, genesis, progress, job)?;
        self.authority.descriptor(self.position, id)
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
    pub(super) fn batch() -> CommitBatch {
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
    pub(super) fn records(
        batch: &CommitBatch,
        job: &Arc<QueueReservation>,
    ) -> Vec<OwnedQueueLogRecord> {
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
        let admission = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let batch = batch();
        let records = records(&batch, &job);
        let (tracker, history) = fold(&batch, &records, &job);
        let proof = Arc::new(tracker.finish(&history, &records[0], (2, 0, 1)).unwrap());
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
        let present = proof.position(0, "known").unwrap();
        let absent = proof.position(1, "fresh").unwrap();
        assert_eq!(
            present
                .validate(&batch.queue, "known", &records[0], (2, 0, 1), &job)
                .unwrap(),
            Some(descriptor)
        );
        assert_eq!(
            absent
                .validate(&batch.queue, "fresh", &records[0], (2, 0, 1), &job)
                .unwrap(),
            None
        );
        assert!(
            absent
                .validate(&batch.queue, "known", &records[0], (2, 0, 1), &job)
                .is_err()
        );
        assert!(
            absent
                .validate(&batch.queue, "fresh", &records[0], (1, 0, 1), &job)
                .is_err()
        );
        assert!(proof.position(0, "fresh").is_err());

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
        drop(changed_genesis);
        drop(definition);
        drop(other);
        drop(records);
        drop(history);
        drop(proof);
        drop(present);
        assert!(
            admission.usage().unwrap().1 > 0,
            "absent handle retains whole admitted proof"
        );
        drop(absent);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
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
    fn sealed_original_header_exact_capacity_and_one_byte_short() {
        let request = batch();
        let admitted = QueueAdmission::new(1, 4 << 20).unwrap();
        let job = Arc::new(admitted.reserve(0).unwrap());
        let tracker = ColdOriginalTracker::new(&request, &job).unwrap();
        let exact = admitted.usage().unwrap().1;
        drop(tracker);
        assert_eq!(admitted.usage().unwrap(), (1, 0));
        let fit = QueueAdmission::new(1, exact).unwrap();
        let fit_job = Arc::new(fit.reserve(0).unwrap());
        drop(ColdOriginalTracker::new(&request, &fit_job).unwrap());
        assert_eq!(fit.usage().unwrap(), (1, 0));
        let short = QueueAdmission::new(1, exact - 1).unwrap();
        let short_job = Arc::new(short.reserve(0).unwrap());
        assert!(matches!(
            ColdOriginalTracker::new(&request, &short_job),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(short.usage().unwrap(), (1, 0));
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

#[cfg(test)]
mod domain_tests {
    use super::tests::{batch, records};
    use super::*;
    use crate::queue_admission::QueueAdmission;
    #[test]
    fn lookup_aliases_bind_whole_order_empty_scope_and_duplicate_source_poison() {
        let batch = batch();
        let job = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        let source = records(&batch, &job);
        let request = ReceiptReadRequest {
            queue: batch.queue.clone(),
            request_ids: vec![
                "known".into(),
                "fresh".into(),
                "known".into(),
                "fresh".into(),
            ],
            max_bytes: 1 << 20,
        };
        let fold = |request: &ReceiptReadRequest| {
            let mut tracker = ColdOriginalTracker::new_lookup(request, &job).unwrap();
            let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
            for (offset, record) in source.iter().enumerate() {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            (tracker, history)
        };
        let (tracker, history) = fold(&request);
        let proof = tracker.finish(&history, &source[0], (2, 0, 1)).unwrap();
        proof
            .validate_lookup(&request, &source[0], (2, 0, 1), &job)
            .unwrap();
        assert_eq!(
            proof.descriptor(0, "known").unwrap(),
            proof.descriptor(2, "known").unwrap()
        );
        assert!(proof.descriptor(0, "known").unwrap().is_some());
        assert_eq!(proof.descriptor(1, "fresh").unwrap(), None);
        assert_eq!(proof.descriptor(3, "fresh").unwrap(), None);
        for ids in [
            vec!["fresh", "known", "known", "fresh"],
            vec!["known", "fresh", "known"],
            vec!["known", "fresh", "fresh", "fresh"],
        ] {
            let mut changed = request.clone();
            changed.request_ids = ids.into_iter().map(str::to_owned).collect();
            assert!(
                proof
                    .validate_lookup(&changed, &source[0], (2, 0, 1), &job)
                    .is_err()
            );
        }
        let (mut tracker, mut history) = fold(&request);
        history.observe(2, source[1].bytes()).unwrap();
        assert!(tracker.observe(2, &source[1]).is_err());
        assert!(tracker.finish(&history, &source[0], (3, 0, 1)).is_err());
        let empty = ReceiptReadRequest {
            request_ids: vec![],
            ..request
        };
        let (tracker, history) = fold(&empty);
        let mut empty_proof = tracker.finish(&history, &source[0], (2, 0, 1)).unwrap();
        empty_proof
            .validate_lookup(&empty, &source[0], (2, 0, 1), &job)
            .unwrap();
        assert!(
            empty_proof
                .validate_lookup(&empty, &source[0], (1, 0, 1), &job)
                .is_err()
        );
        let other = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        assert!(
            empty_proof
                .validate_lookup(&empty, &source[0], (2, 0, 1), &other)
                .is_err()
        );
        empty_proof.domain = 1;
        assert!(
            empty_proof
                .validate_lookup(&empty, &source[0], (2, 0, 1), &job)
                .is_err()
        );
    }

    #[test]
    fn lookup_alias_and_empty_header_admission_is_exact_and_position_retains_owner() {
        let batch = batch();
        for ids in [
            vec![],
            vec![
                "known".to_string(),
                "known".to_string(),
                "absent".to_string(),
            ],
        ] {
            let request = ReceiptReadRequest {
                queue: batch.queue.clone(),
                request_ids: ids,
                max_bytes: 1 << 20,
            };
            let admission = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let tracker = ColdOriginalTracker::new_lookup(&request, &job).unwrap();
            let exact = admission.usage().unwrap().1;
            drop(tracker);
            let fit = QueueAdmission::new(1, exact).unwrap();
            let fit_job = Arc::new(fit.reserve(0).unwrap());
            drop(ColdOriginalTracker::new_lookup(&request, &fit_job).unwrap());
            let short = QueueAdmission::new(1, exact - 1).unwrap();
            let short_job = Arc::new(short.reserve(0).unwrap());
            assert!(matches!(
                ColdOriginalTracker::new_lookup(&request, &short_job),
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert_eq!(short.usage().unwrap().1, 0);
            let source = records(&batch, &job);
            let mut tracker = ColdOriginalTracker::new_lookup(&request, &job).unwrap();
            let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
            for (offset, record) in source.iter().enumerate() {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            let proof = Arc::new(tracker.finish(&history, &source[0], (2, 0, 1)).unwrap());
            let position =
                (!request.request_ids.is_empty()).then(|| proof.position(1, "known").unwrap());
            drop(source);
            drop(history);
            drop(proof);
            assert_eq!(
                admission.usage().unwrap().1,
                if position.is_some() { exact } else { 0 }
            );
            drop(position);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }

    #[test]
    fn same_literal_has_three_independent_originals_and_scoped_duplicate_poison() {
        use crate::{
            queue_claim_publication::{ClaimPublicationInput, encode_claim_publication},
            queue_claim_receipt::encode_claim_receipt,
            queue_log_record::{encode_checked_claim_record, encode_checked_publication_record},
            queue_planner::{RecordSnapshot, combine_deltas},
            queue_publication::{PublicationReceipt, encode_publication_pinned},
        };
        use fireweed_core::queue_contract::*;
        let batch = batch();
        let job = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
        let source = records(&batch, &job);
        let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
        history.observe(0, source[0].bytes()).unwrap();
        let definition = history.definition().unwrap();
        let insert = InsertRequest {
            queue: batch.queue.clone(),
            request_id: "known".into(),
            mode: InsertMode::InsertOnly,
            items: vec![NewWork {
                key: "absent".into(),
                priority: 0,
                available_at: 0,
                fields: Default::default(),
                body: Default::default(),
            }],
        };
        let outcomes = [InsertOutcome::Rejected {
            key: "absent".into(),
            reason: Rejection::UniqueConflict,
        }];
        let frame = encode_publication_pinned(
            &batch.queue,
            definition.pin(),
            &RecordSnapshot::default(),
            &combine_deltas(&[], 0).unwrap(),
            &[PublicationReceipt::insert(&insert, &outcomes).unwrap()],
            1 << 20,
            &job,
        )
        .unwrap();
        let insert_record =
            encode_checked_publication_record(1, frame.as_bytes(), 1 << 20, &job).unwrap();
        let commands = encode_publication_pinned(
            &batch.queue,
            definition.pin(),
            &RecordSnapshot::default(),
            &combine_deltas(&[], 0).unwrap(),
            &[],
            1 << 20,
            &job,
        )
        .unwrap();
        let claim = ClaimRequest {
            queue: batch.queue.clone(),
            request_id: "known".into(),
            filter: Filter::default(),
            order: ClaimOrder::PriorityAscending,
            eligible_at: 0,
            lease_nanos: 1_000_000,
            max_records: 2,
            max_bytes: 1024,
            fields: None,
            include_body: true,
        };
        let receipt = encode_claim_receipt(&[], 1 << 20, &job).unwrap();
        let wrapper = encode_claim_publication(
            ClaimPublicationInput {
                request: &claim,
                definition,
                commands: commands.as_bytes(),
                receipt: &receipt,
                claimed_at: 0,
                limits: &QueueLimits::default(),
            },
            1 << 20,
            &job,
        )
        .unwrap();
        let claim_record = encode_checked_claim_record(1, &wrapper, 1 << 20, &job).unwrap();
        let prefix = [&source[0], &insert_record, &claim_record, &source[1]];
        for domain in 0..=2 {
            let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
            let mut tracker =
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &job).unwrap();
            for (offset, record) in prefix.iter().enumerate() {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            let proof = Arc::new(tracker.finish(&history, &source[0], (4, 0, 1)).unwrap());
            let position = proof.position(0, "known").unwrap();
            let descriptor = position
                .validate_domain(domain, &batch.queue, "known", &source[0], (4, 0, 1), &job)
                .unwrap()
                .unwrap();
            let raw = match prefix[domain as usize + 1].view().unwrap().payload() {
                QueueLogPayload::Publication(frame) => {
                    frame.receipts().next().unwrap().unwrap().encoded_outcome()
                }
                QueueLogPayload::Claim(claim) => claim.receipt_bytes(),
                _ => panic!("original source"),
            };
            descriptor.verify_outcome(raw).unwrap();
            // The duplicate is in domain0. Only that requested domain is poisoned.
            let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
            let mut tracker =
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &job).unwrap();
            for (offset, record) in prefix.iter().enumerate() {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            history.observe(4, insert_record.bytes()).unwrap();
            let observed = tracker.observe(4, &insert_record);
            if domain == 0 {
                assert!(observed.is_err());
                assert!(tracker.finish(&history, &source[0], (5, 0, 1)).is_err());
            } else {
                observed.unwrap();
                tracker.finish(&history, &source[0], (5, 0, 1)).unwrap();
            }
        }
    }

    #[test]
    fn single_original_domains_bind_presence_absence_and_exact_admission() {
        let batch = batch();
        for domain in 0..=2 {
            let admitted = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admitted.reserve(0).unwrap());
            let tracker =
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &job).unwrap();
            let exact = admitted.usage().unwrap().1;
            drop(tracker);
            let fit = QueueAdmission::new(1, exact).unwrap();
            let fit_job = Arc::new(fit.reserve(0).unwrap());
            drop(
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &fit_job).unwrap(),
            );
            let short = QueueAdmission::new(1, exact - 1).unwrap();
            let short_job = Arc::new(short.reserve(0).unwrap());
            assert!(matches!(
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &short_job),
                Err(QueueError::OutcomeUnknown(_))
            ));
            let source = records(&batch, &job);
            let mut history = ColdQueueHistory::new(&batch.queue, &job).unwrap();
            let mut tracker =
                ColdOriginalTracker::new_request(&batch.queue, domain, "known", &job).unwrap();
            for (offset, record) in source.iter().enumerate() {
                history.observe(offset as u64, record.bytes()).unwrap();
                tracker.observe(offset as u64, record).unwrap();
            }
            let proof = Arc::new(tracker.finish(&history, &source[0], (2, 0, 1)).unwrap());
            let position = proof.position(0, "known").unwrap();
            assert_eq!(
                position
                    .validate_domain(domain, &batch.queue, "known", &source[0], (2, 0, 1), &job)
                    .unwrap()
                    .is_some(),
                domain == 2
            );
            assert!(
                position
                    .validate_domain(
                        (domain + 1) % 3,
                        &batch.queue,
                        "known",
                        &source[0],
                        (2, 0, 1),
                        &job
                    )
                    .is_err()
            );
            drop(source);
            drop(history);
            drop(proof);
            let retained = admitted.usage().unwrap().1;
            assert_eq!(
                retained, exact,
                "position must retain the complete admitted proof even for absence"
            );
            drop(position);
            assert_eq!(admitted.usage().unwrap().1, 0);
            assert!(matches!(
                ColdOriginalTracker::new_request(&batch.queue, 3, "known", &job),
                Err(QueueError::Invalid(_))
            ));
            assert_eq!(admitted.usage().unwrap().1, 0);
        }
    }
}
