//! Operation-local authority for explicitly addressed state reads. Full checked
//! history and fresh-head confirmation remain mandatory adapter responsibilities.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{OwnedQueueLogRecord, QueueLogPayload},
    queue_namespace::{encode_address, framed_address_len, view_address},
    queue_publication::{
        EffectRef, FieldsFingerprint, borrowed_fields_fingerprint, owned_fields_fingerprint,
        state_record_encoded_len,
    },
    queue_recovery::ColdQueueHistory,
};
use fireweed_core::queue_contract::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("state authority: {reason}"))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateDescriptor {
    NeverSeen,
    Deleted {
        highwater: u64,
    },
    Present {
        highwater: u64,
        field_count: u64,
        fields: FieldsFingerprint,
        record_encoded_bytes: u64,
        value_bytes: u64,
        value_sha256: [u8; 32],
    },
}
impl StateDescriptor {
    fn highwater(self) -> u64 {
        match self {
            Self::NeverSeen => 0,
            Self::Deleted { highwater } | Self::Present { highwater, .. } => highwater,
        }
    }
    fn expected_version(self) -> Option<u64> {
        match self {
            Self::Present { highwater, .. } => Some(highwater),
            _ => None,
        }
    }
    pub fn payload_bound(self) -> QueueResult<u64> {
        match self {
            Self::Present {
                fields,
                value_bytes,
                ..
            } => fields
                .encoded_bytes
                .checked_add(value_bytes)
                .ok_or(QueueError::TooLarge),
            _ => Ok(0),
        }
    }
    pub fn verify_fields(self, version: u64, actual: &Fields) -> QueueResult<()> {
        let Self::Present {
            highwater,
            field_count,
            fields,
            ..
        } = self
        else {
            return Err(unknown("cached fields lack live source authority"));
        };
        if version != highwater
            || actual.len() as u64 != field_count
            || owned_fields_fingerprint(actual)
                .map_err(|_| unknown("cached fields encoding invalid"))?
                != fields
        {
            return Err(unknown("cached state fields differ from source"));
        }
        Ok(())
    }
    pub fn verify_record(self, record: &StateRecord) -> QueueResult<()> {
        let Self::Present {
            highwater,
            field_count,
            fields,
            value_bytes,
            value_sha256,
            ..
        } = self
        else {
            return Err(unknown("cached live state lacks source authority"));
        };
        if record.version != highwater
            || record.fields.len() as u64 != field_count
            || record.value.len() as u64 != value_bytes
            || owned_fields_fingerprint(&record.fields)
                .map_err(|_| unknown("cached fields encoding invalid"))?
                != fields
            || <[u8; 32]>::from(Sha256::digest(&record.value)) != value_sha256
        {
            return Err(unknown("cached state payload differs from source"));
        }
        Ok(())
    }
}
struct Entry {
    key: String,
    descriptor: StateDescriptor,
}
pub struct ColdStateTracker {
    address: Vec<u8>,
    collection: String,
    entries: Vec<Entry>,
    genesis_sha256: Option<[u8; 32]>,
    next_offset: u64,
    insertion_sequence: u64,
    historical_epoch: u64,
    poisoned: bool,
    charge: OwnedQueueBufferCharge,
}
pub struct AdmittedStateAuthority {
    address: Vec<u8>,
    collection: String,
    entries: Vec<Entry>,
    genesis_sha256: [u8; 32],
    progress: (u64, u64, u64),
    charge: OwnedQueueBufferCharge,
}
impl ColdStateTracker {
    pub fn new(request: &StateReadRequest, job: &Arc<QueueReservation>) -> QueueResult<Self> {
        Self::new_keys(
            &request.queue,
            &request.collection,
            request.keys.iter().map(String::as_str),
            job,
        )
    }
    pub(super) fn new_keys<'a>(
        queue: &QueueAddress,
        collection: &str,
        keys: impl Iterator<Item = &'a str> + Clone,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        queue.validate()?;
        if collection.is_empty() {
            return Err(QueueError::Invalid("empty state collection".into()));
        }
        let mut capacity = (framed_address_len(queue)? as u64)
            .checked_mul(8)
            .and_then(|n| n.checked_add((collection.len() as u64).checked_mul(8)?))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        for key in keys.clone() {
            if key.is_empty() {
                return Err(QueueError::Invalid("empty state key".into()));
            }
            capacity = capacity
                .checked_add(
                    (key.len() as u64)
                        .checked_mul(8)
                        .ok_or(QueueError::TooLarge)?,
                )
                .and_then(|n| n.checked_add((std::mem::size_of::<Entry>() as u64) * 4))
                .ok_or(QueueError::TooLarge)?;
        }
        let charge = job.owned_buffer(capacity)?;
        Ok(Self {
            address: encode_address(queue)?,
            collection: collection.to_owned(),
            entries: keys
                .map(|key| Entry {
                    key: key.to_owned(),
                    descriptor: StateDescriptor::NeverSeen,
                })
                .collect(),
            genesis_sha256: None,
            next_offset: 0,
            insertion_sequence: 0,
            historical_epoch: 0,
            poisoned: false,
            charge,
        })
    }
    pub fn observe(&mut self, offset: u64, record: &OwnedQueueLogRecord) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned state history"));
        }
        let view = match record.view() {
            Ok(view) => view,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        self.observe_view(offset, record, &view)
    }
    pub(super) fn observe_view(
        &mut self,
        offset: u64,
        record: &OwnedQueueLogRecord,
        view: &crate::queue_log_record::QueueLogRecordView<'_>,
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned state history"));
        }
        let result = self.observe_checked(offset, record, view);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn observe_checked(
        &mut self,
        offset: u64,
        record: &OwnedQueueLogRecord,
        view: &crate::queue_log_record::QueueLogRecordView<'_>,
    ) -> QueueResult<()> {
        if !record.belongs_to(self.charge.reservation()) || offset != self.next_offset {
            return Err(unknown("record owner or contiguous offset mismatch"));
        }
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
                if self.genesis_sha256.is_none()
                    || frame.summary().initial_sequence != self.insertion_sequence
                {
                    return Err(unknown("publication predecessor mismatch"));
                }
                for command in frame.commands() {
                    let command = command.map_err(|_| unknown("invalid source state command"))?;
                    if command.identity.collection != Some(self.collection.as_str()) {
                        continue;
                    }
                    let mut positions = self
                        .entries
                        .iter_mut()
                        .filter(|entry| entry.key == command.identity.key)
                        .peekable();
                    if positions.peek().is_none() {
                        continue;
                    }
                    let descriptor = match command.effect {
                        EffectRef::Delete => StateDescriptor::Deleted {
                            highwater: command.output_highwater,
                        },
                        EffectRef::StatePut { fields, value } => StateDescriptor::Present {
                            highwater: command.output_highwater,
                            field_count: fields.len(),
                            fields: borrowed_fields_fingerprint(fields)
                                .map_err(|_| unknown("invalid source fields"))?,
                            record_encoded_bytes: state_record_encoded_len(
                                &self.collection,
                                command.identity.key,
                                command.output_highwater,
                                fields,
                                value,
                            )
                            .map_err(|_| unknown("source state record size invalid"))?,
                            value_bytes: value.len() as u64,
                            value_sha256: Sha256::digest(value).into(),
                        },
                        EffectRef::WorkPut { .. } => {
                            return Err(unknown("work mutation in state collection"));
                        }
                    };
                    for entry in positions {
                        if command.prior_highwater != entry.descriptor.highwater()
                            || command.expected_version != entry.descriptor.expected_version()
                            || command.output_highwater <= command.prior_highwater
                        {
                            return Err(unknown("state predecessor disagreement"));
                        }
                        entry.descriptor = descriptor;
                    }
                }
                self.insertion_sequence = frame.summary().final_sequence;
            }
            QueueLogPayload::Claim(_) => {
                if self.genesis_sha256.is_none() {
                    return Err(unknown("claim before genesis"));
                }
            }
        }
        self.historical_epoch = view.epoch();
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .ok_or(QueueError::TooLarge)?;
        Ok(())
    }
    pub fn finish(
        self,
        history: &ColdQueueHistory,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
    ) -> QueueResult<AdmittedStateAuthority> {
        let hash = self
            .genesis_sha256
            .ok_or_else(|| unknown("missing genesis"))?;
        history.definition()?;
        if self.poisoned
            || !history.belongs_to(self.charge.reservation())
            || !genesis.belongs_to(self.charge.reservation())
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
            || hash != <[u8; 32]>::from(Sha256::digest(genesis.bytes()))
        {
            return Err(unknown("incomplete or foreign checked cut"));
        }
        Ok(AdmittedStateAuthority {
            address: self.address,
            collection: self.collection,
            entries: self.entries,
            genesis_sha256: hash,
            progress,
            charge: self.charge,
        })
    }
}
impl AdmittedStateAuthority {
    pub fn validate(
        &self,
        request: &StateReadRequest,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.validate_keys(
            &request.queue,
            &request.collection,
            request.keys.iter().map(String::as_str),
            genesis,
            progress,
            job,
        )
    }
    pub(super) fn validate_keys<'a>(
        &self,
        queue: &QueueAddress,
        collection: &str,
        keys: impl Iterator<Item = &'a str> + Clone,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        let address = view_address(&self.address)?;
        if !std::ptr::eq(self.charge.reservation(), job)
            || !genesis.belongs_to(job)
            || address.tenant != queue.tenant
            || address.queue != queue.queue
            || self.collection != collection
            || self.progress != progress
            || self.genesis_sha256 != <[u8; 32]>::from(Sha256::digest(genesis.bytes()))
            || self.entries.len() != keys.clone().count()
            || self
                .entries
                .iter()
                .zip(keys)
                .any(|(entry, key)| entry.key != key)
        {
            return Err(unknown(
                "state operation/address/collection/genesis/cut/vector mismatch",
            ));
        }
        Ok(())
    }
    pub(super) fn descriptor_identity(
        &self,
        position: usize,
        collection: &str,
        key: &str,
    ) -> QueueResult<StateDescriptor> {
        if self.collection != collection {
            return Err(unknown("state collection mismatch"));
        }
        self.descriptor(position, key)
    }
    pub fn descriptor(&self, position: usize, key: &str) -> QueueResult<StateDescriptor> {
        self.entries
            .get(position)
            .filter(|entry| entry.key == key)
            .map(|entry| entry.descriptor)
            .ok_or_else(|| unknown("state position mismatch"))
    }
    /// Postcard Vec prefix and Option tags retain every requested alias position.
    pub fn response_bytes(&self) -> QueueResult<u64> {
        let prefix = encoded_len(&(self.entries.len() as u64))?;
        self.entries.iter().try_fold(prefix, |total, entry| {
            let record = match entry.descriptor {
                StateDescriptor::Present {
                    record_encoded_bytes,
                    ..
                } => record_encoded_bytes,
                _ => 0,
            };
            total
                .checked_add(1)
                .and_then(|n| n.checked_add(record))
                .ok_or(QueueError::TooLarge)
        })
    }
    pub fn payload_bound(&self) -> QueueResult<u64> {
        self.entries.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.descriptor.payload_bound()?)
                .ok_or(QueueError::TooLarge)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::QueueAdmission,
        queue_log_record::{encode_checked_genesis, encode_checked_publication_record},
        queue_planner::{RecordSnapshot, plan_commit_group},
        queue_publication::{PublicationReceipt, encode_publication_pinned},
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
    };
    fn request(keys: &[&str]) -> StateReadRequest {
        StateReadRequest {
            queue: QueueAddress {
                tenant: "tenant\0β".into(),
                queue: "states/λ".into(),
            },
            collection: "instances".into(),
            keys: keys.iter().map(|key| (*key).into()).collect(),
            max_bytes: 1 << 20,
        }
    }
    fn schema() -> QueueSchema {
        let collection = CollectionDefinition {
            fields: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: ["meta/".into()].into(),
        };
        QueueSchema {
            work: collection.clone(),
            state: [
                ("instances".into(), collection.clone()),
                ("other".into(), collection),
            ]
            .into(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        }
    }
    fn genesis(job: &Arc<QueueReservation>) -> (AdmittedQueueDefinition, OwnedQueueLogRecord) {
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), job).unwrap();
        let record =
            encode_checked_genesis(1, &request(&[]).queue, &definition, &[31; 32], 4 << 20, job)
                .unwrap();
        (definition, record)
    }
    fn put(fields: Fields, value: &[u8]) -> StateChange {
        StateChange::Put {
            fields,
            value: bytes::Bytes::copy_from_slice(value),
        }
    }
    fn mutation(
        job: &Arc<QueueReservation>,
        definition: &AdmittedQueueDefinition,
        snapshot: &mut RecordSnapshot,
        id: &str,
        key: &str,
        expect: StateExpect,
        change: StateChange,
    ) -> OwnedQueueLogRecord {
        mutation_inner(job, definition, snapshot, (id, None), key, expect, change)
    }
    fn mutation_inner(
        job: &Arc<QueueReservation>,
        definition: &AdmittedQueueDefinition,
        snapshot: &mut RecordSnapshot,
        identity: (&str, Option<u64>),
        key: &str,
        expect: StateExpect,
        change: StateChange,
    ) -> OwnedQueueLogRecord {
        let (id, output) = identity;
        let group = CommitGroup {
            request_id: id.into(),
            administration: None,
            claimed_updates: vec![],
            work_inserts: vec![],
            work_updates: vec![],
            state_updates: vec![StateUpdate {
                collection: "instances".into(),
                key: key.into(),
                expect,
                change,
            }],
        };
        let before = snapshot.clone();
        let mut delta = plan_commit_group(&group, snapshot, 0, &schema()).unwrap();
        if let Some(output) = output {
            delta.state[0].version = output;
            delta.versions[0].version = output;
            let GroupOutcome::Committed { versions } = &mut delta.receipt.outcome else {
                panic!()
            };
            versions[0].version = output;
            snapshot
                .state
                .get_mut(&("instances".into(), key.into()))
                .unwrap()
                .version = output;
            snapshot.versions.insert(
                crate::queue_planner::RecordIdentity {
                    collection: Some("instances".into()),
                    key: key.into(),
                },
                output,
            );
        }
        assert!(matches!(
            delta.receipt.outcome,
            GroupOutcome::Committed { .. }
        ));
        let queue = request(&[]).queue;
        let receipt = PublicationReceipt::commit(&queue, &group, &delta.receipt).unwrap();
        let frame = encode_publication_pinned(
            &request(&[]).queue,
            definition.pin(),
            &before,
            &delta,
            &[receipt],
            4 << 20,
            job,
        )
        .unwrap();
        encode_checked_publication_record(1, frame.as_bytes(), 4 << 20, job).unwrap()
    }
    fn fold(
        request: &StateReadRequest,
        records: &[OwnedQueueLogRecord],
        job: &Arc<QueueReservation>,
    ) -> AdmittedStateAuthority {
        let mut tracker = ColdStateTracker::new(request, job).unwrap();
        let mut history = ColdQueueHistory::new(&request.queue, job).unwrap();
        for (offset, record) in records.iter().enumerate() {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        tracker
            .finish(&history, &records[0], (records.len() as u64, 0, 1))
            .unwrap()
    }
    #[test]
    fn state_history_binds_hwm_payload_aliases_and_exact_public_size() {
        let job = Arc::new(
            QueueAdmission::new(1, 32 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        let (definition, first) = genesis(&job);
        let mut records = vec![first];
        let mut snapshot = RecordSnapshot::default();
        let fields: Fields = [
            ("meta/int".into(), FieldValue::Integer(i64::MIN)),
            ("meta/float".into(), FieldValue::Float(-0.0)),
            ("meta/text\0雪".into(), FieldValue::Text("\0β\n".into())),
            ("meta/bool".into(), FieldValue::Boolean(true)),
            ("meta/bytes".into(), FieldValue::Bytes(b"\0\xff".to_vec())),
        ]
        .into();
        for (id, key, expect, change) in [
            (
                "create",
                "a",
                StateExpect::Absent,
                put(fields.clone(), b"first"),
            ),
            ("check", "a", StateExpect::Version(1), StateChange::Check),
            ("delete", "a", StateExpect::Version(1), StateChange::Delete),
            (
                "delete-absent",
                "a",
                StateExpect::Absent,
                StateChange::Delete,
            ),
            (
                "recreate",
                "a",
                StateExpect::Absent,
                put(fields.clone(), b"\0\xfffinal"),
            ),
            ("empty", "b", StateExpect::Absent, put(Fields::new(), b"")),
            (
                "tombstone",
                "tomb",
                StateExpect::Absent,
                StateChange::Delete,
            ),
        ] {
            records.push(mutation(
                &job,
                &definition,
                &mut snapshot,
                id,
                key,
                expect,
                change,
            ));
        }
        let request = request(&["a", "missing", "b", "a", "tomb"]);
        let proof = fold(&request, &records, &job);
        proof
            .validate(&request, &records[0], (8, 0, 1), &job)
            .unwrap();
        assert_eq!(
            proof.descriptor(0, "a").unwrap(),
            proof.descriptor(3, "a").unwrap()
        );
        assert_eq!(
            proof.descriptor(1, "missing").unwrap(),
            StateDescriptor::NeverSeen
        );
        assert_eq!(
            proof.descriptor(4, "tomb").unwrap(),
            StateDescriptor::Deleted { highwater: 1 }
        );
        let a = snapshot.state[&("instances".into(), "a".into())].clone();
        let b = snapshot.state[&("instances".into(), "b".into())].clone();
        assert_eq!(a.version, 4);
        proof.descriptor(0, "a").unwrap().verify_record(&a).unwrap();
        let expected = vec![Some(a.clone()), None, Some(b), Some(a.clone()), None];
        assert_eq!(
            proof.response_bytes().unwrap(),
            encoded_len(&expected).unwrap()
        );
        let StateDescriptor::Present {
            fields: fingerprint,
            ..
        } = proof.descriptor(0, "a").unwrap()
        else {
            panic!()
        };
        let raw = postcard::to_stdvec(&fields).unwrap();
        assert_eq!(fingerprint.encoded_bytes, raw.len() as u64);
        assert_eq!(fingerprint.sha256, <[u8; 32]>::from(Sha256::digest(raw)));
        let mut corrupted = a;
        corrupted
            .fields
            .insert("meta/float".into(), FieldValue::Float(0.0));
        assert!(
            proof
                .descriptor(0, "a")
                .unwrap()
                .verify_record(&corrupted)
                .is_err()
        );
        for keys in [
            vec![],
            vec!["a".into()],
            vec![
                "a".into(),
                "a".into(),
                "b".into(),
                "missing".into(),
                "tomb".into(),
            ],
        ] {
            let mut changed = request.clone();
            changed.keys = keys;
            assert!(
                proof
                    .validate(&changed, &records[0], (8, 0, 1), &job)
                    .is_err()
            );
        }
        let mut changed = request.clone();
        changed.collection = "other".into();
        assert!(
            proof
                .validate(&changed, &records[0], (8, 0, 1), &job)
                .is_err()
        );
        assert!(
            proof
                .validate(&request, &records[0], (7, 0, 1), &job)
                .is_err()
        );
    }
    #[test]
    fn state_predecessor_disagreement_poison_and_version_jumps() {
        let job = Arc::new(
            QueueAdmission::new(1, 32 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        let (definition, first) = genesis(&job);
        let mut snapshot = RecordSnapshot::default();
        let create = mutation(
            &job,
            &definition,
            &mut snapshot,
            "create",
            "a",
            StateExpect::Absent,
            put(Fields::new(), b"a"),
        );
        let request = request(&["a", "a"]);
        let mut tracker = ColdStateTracker::new(&request, &job).unwrap();
        let mut history = ColdQueueHistory::new(&request.queue, &job).unwrap();
        for (offset, record) in [&first, &create].into_iter().enumerate() {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        history.observe(2, create.bytes()).unwrap();
        assert!(tracker.observe(2, &create).is_err());
        assert!(tracker.observe(2, &create).is_err());
        assert!(tracker.finish(&history, &first, (3, 0, 1)).is_err());
        let mut wrong_existence = snapshot.clone();
        wrong_existence.state.clear();
        let wrong_expected = mutation(
            &job,
            &definition,
            &mut wrong_existence,
            "wrong-expected",
            "a",
            StateExpect::Absent,
            put(Fields::new(), b"different"),
        );
        let mut tracker = ColdStateTracker::new(&request, &job).unwrap();
        tracker.observe(0, &first).unwrap();
        tracker.observe(1, &create).unwrap();
        assert!(
            tracker.observe(2, &wrong_expected).is_err(),
            "matching prior HWM cannot excuse wrong existence expectation"
        );
        let mut advanced = snapshot.clone();
        let valid_jump = mutation_inner(
            &job,
            &definition,
            &mut advanced,
            ("valid-jump", Some(7)),
            "a",
            StateExpect::Version(1),
            put(Fields::new(), b"jumped"),
        );
        let mut tracker = ColdStateTracker::new(&request, &job).unwrap();
        let mut history = ColdQueueHistory::new(&request.queue, &job).unwrap();
        for (offset, record) in [&first, &create, &valid_jump].into_iter().enumerate() {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        let proof = tracker.finish(&history, &first, (3, 0, 1)).unwrap();
        assert!(matches!(
            proof.descriptor(0, "a").unwrap(),
            StateDescriptor::Present { highwater: 7, .. }
        ));
        // Jumping output versions is wire-valid; predecessor existence is separate.
        let mut jumped = snapshot.clone();
        jumped
            .state
            .get_mut(&("instances".into(), "a".into()))
            .unwrap()
            .version = 7;
        jumped.versions.insert(
            crate::queue_planner::RecordIdentity {
                collection: Some("instances".into()),
                key: "a".into(),
            },
            7,
        );
        let jump = mutation(
            &job,
            &definition,
            &mut jumped,
            "jump",
            "a",
            StateExpect::Version(7),
            put(Fields::new(), b"new"),
        );
        let mut tracker = ColdStateTracker::new(&request, &job).unwrap();
        tracker.observe(0, &first).unwrap();
        tracker.observe(1, &create).unwrap();
        assert!(
            tracker.observe(2, &jump).is_err(),
            "wrong prior HWM must not be accepted"
        );
    }
    #[test]
    fn state_public_size_varint_boundaries_match_owned_oracle() {
        let job = Arc::new(
            QueueAdmission::new(1, 32 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        for version in [127, 128] {
            for value_len in [127, 128] {
                let (definition, first) = genesis(&job);
                let mut snapshot = RecordSnapshot::default();
                let value = vec![7; value_len];
                let record = mutation_inner(
                    &job,
                    &definition,
                    &mut snapshot,
                    ("boundary", Some(version)),
                    "a",
                    StateExpect::Absent,
                    put(Fields::new(), &value),
                );
                let records = vec![first, record];
                for positions in [0, 1, 127, 128] {
                    let mut request = request(&[]);
                    request.keys = (0..positions)
                        .map(|n| {
                            if n % 3 == 0 {
                                "missing".into()
                            } else {
                                "a".into()
                            }
                        })
                        .collect();
                    let expected: Vec<Option<StateRecord>> = request
                        .keys
                        .iter()
                        .map(|key| {
                            snapshot
                                .state
                                .get(&(request.collection.clone(), key.clone()))
                                .cloned()
                        })
                        .collect();
                    let proof = fold(&request, &records, &job);
                    assert_eq!(
                        proof.response_bytes().unwrap(),
                        encoded_len(&expected).unwrap(),
                        "positions={positions} version={version} value_len={value_len}"
                    );
                }
            }
        }
    }
    #[test]
    fn state_header_empty_alias_exact_admission_and_owner_scope() {
        for keys in [&[][..], &["a", "a", "b"][..]] {
            let request = request(keys);
            let admission = QueueAdmission::new(1, 4 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let tracker = ColdStateTracker::new(&request, &job).unwrap();
            let exact = admission.usage().unwrap().1;
            drop(tracker);
            let fit = Arc::new(QueueAdmission::new(1, exact).unwrap().reserve(0).unwrap());
            drop(ColdStateTracker::new(&request, &fit).unwrap());
            let short = Arc::new(
                QueueAdmission::new(1, exact - 1)
                    .unwrap()
                    .reserve(0)
                    .unwrap(),
            );
            assert!(matches!(
                ColdStateTracker::new(&request, &short),
                Err(QueueError::OutcomeUnknown(_))
            ));
            let (definition, first) = genesis(&job);
            let proof = fold(&request, std::slice::from_ref(&first), &job);
            assert_eq!(
                proof.response_bytes().unwrap(),
                encoded_len(&vec![None::<StateRecord>; keys.len()]).unwrap()
            );
            let other = Arc::new(QueueAdmission::new(1, 4 << 20).unwrap().reserve(0).unwrap());
            assert!(proof.validate(&request, &first, (1, 0, 1), &other).is_err());
            drop(first);
            drop(definition);
            drop(proof);
            assert_eq!(admission.usage().unwrap(), (1, 0));
        }
    }
    #[test]
    fn group_state_bindings_cover_siblings_collections_empty_groups_and_owner() {
        use crate::queue_group_state_authority::ColdGroupStateTracker;
        let job = Arc::new(
            QueueAdmission::new(1, 32 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        let (definition, genesis) = genesis(&job);
        let mut snapshot = RecordSnapshot::default();
        let mut records = vec![genesis];
        for (id, expect, change) in [
            (
                "put",
                StateExpect::Absent,
                put(Fields::new(), b"large-old-body"),
            ),
            ("delete", StateExpect::Version(1), StateChange::Delete),
            ("recreate", StateExpect::Absent, put(Fields::new(), b"new")),
        ] {
            records.push(mutation(
                &job,
                &definition,
                &mut snapshot,
                id,
                "same",
                expect,
                change,
            ));
        }
        let queue = request(&[]).queue;
        let make_group = |id: &str, collection: Option<&str>| CommitGroup {
            request_id: id.into(),
            administration: None,
            claimed_updates: vec![],
            work_updates: vec![],
            work_inserts: vec![],
            state_updates: collection
                .into_iter()
                .map(|collection| StateUpdate {
                    collection: collection.into(),
                    key: "same".into(),
                    expect: StateExpect::Absent,
                    change: StateChange::Check,
                })
                .collect(),
        };
        let mut multi = make_group("multi", Some("instances"));
        let mut other_key = multi.state_updates[0].clone();
        other_key.key = "second".into();
        multi.state_updates.push(other_key);
        let groups = [
            make_group("one", Some("instances")),
            make_group("two", Some("other")),
            make_group("empty", None),
            multi,
            make_group("shared", Some("instances")),
        ];
        let mut tracker = ColdGroupStateTracker::new(&queue, &groups, &job).unwrap();
        let mut history = ColdQueueHistory::new(&queue, &job).unwrap();
        for (offset, record) in records.iter().enumerate() {
            history.observe(offset as u64, record.bytes()).unwrap();
            tracker.observe(offset as u64, record).unwrap();
        }
        let cut = (4, 0, 1);
        let proof = Arc::new(tracker.finish(&history, &records[0], cut).unwrap());
        let handles: Vec<_> = (0..5).map(|n| proof.group(n).unwrap()).collect();
        for (handle, group) in handles.iter().zip(&groups) {
            handle
                .validate(&queue, group, &records[0], cut, &job)
                .unwrap();
        }
        assert!(matches!(
            handles[0].descriptor(0, "same").unwrap(),
            StateDescriptor::Present { highwater: 3, .. }
        ));
        assert_eq!(
            handles[1].descriptor(0, "same").unwrap(),
            StateDescriptor::NeverSeen
        );
        assert!(
            handles[0]
                .validate(&queue, &groups[1], &records[0], cut, &job)
                .is_err()
        );
        assert!(
            handles[2]
                .validate(&queue, &groups[2], &records[0], (3, 0, 1), &job)
                .is_err()
        );
        let foreign = Arc::new(
            QueueAdmission::new(1, 32 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        assert!(
            handles[2]
                .validate(&queue, &groups[2], &records[0], cut, &foreign)
                .is_err()
        );
        assert_eq!(
            handles[3].descriptor(0, "same").unwrap(),
            handles[4].descriptor(0, "same").unwrap()
        );
        assert_eq!(
            handles[3].descriptor(1, "second").unwrap(),
            StateDescriptor::NeverSeen
        );
        let mut reordered = groups[3].clone();
        reordered.state_updates.reverse();
        assert!(
            handles[3]
                .validate(&queue, &reordered, &records[0], cut, &job)
                .is_err()
        );
        assert!(handles[3].descriptor(1, "same").is_err());
        assert!(proof.group(5).is_err());
        let mut poisoned = ColdGroupStateTracker::new(&queue, &groups, &job).unwrap();
        assert!(poisoned.observe(1, &records[0]).is_err());
        assert!(poisoned.observe(0, &records[0]).is_err());
        assert!(poisoned.finish(&history, &records[0], cut).is_err());
        let weak = Arc::downgrade(&proof);
        drop(proof);
        assert!(weak.upgrade().is_some());
        drop(handles);
        assert!(weak.upgrade().is_none());
    }
}
