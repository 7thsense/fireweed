//! Bounded physical bridge to the shared per-tuple domain reducer.
use super::*;
use fireweed_engine::queue_unique_stream::{
    ScratchUniqueEffect, ScratchUniqueEvidence, UniqueClaimOrigin, UniqueTupleDecision,
    UniqueTupleReducer,
};
use sha2::{Digest, Sha256};
const MAGIC: &[u8] = b"FWUE1";
const TRAILER: usize = 32;
fn memory_bound(bytes: u64) -> QueueResult<u64> {
    bytes
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| unknown("evidence allocation overflow"))
}
fn checksum(key: &UniqueIdentity, ordinal: u64, body: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"fireweed-scratch-unique-evidence-v1\0");
    hash.update(ordinal.to_be_bytes());
    hash.update([u8::from(key.collection.is_some())]);
    for segment in [
        key.collection.as_deref().unwrap_or("").as_bytes(),
        key.index.as_bytes(),
        key.full_sort_key.as_slice(),
        body,
    ] {
        hash.update((segment.len() as u64).to_be_bytes());
        hash.update(segment);
    }
    hash.finalize().into()
}
impl QueueSpool {
    /// Producer must supply already accepted per-row frozen initial evidence.
    /// Encoding buffers are admitted before allocation, then immediately dropped.
    pub fn append_unique_evidence(
        &mut self,
        key: &UniqueIdentity,
        record: &ScratchUniqueEvidence,
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("append evidence after poison"));
        }
        let result = self.append_evidence_checked(key, record);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn append_evidence_checked(
        &mut self,
        key: &UniqueIdentity,
        record: &ScratchUniqueEvidence,
    ) -> QueueResult<()> {
        let bytes = fireweed_core::queue_contract::encoded_len(record)
            .map_err(|_| unknown("evidence canonical measurement failed"))?;
        let bytes = add(bytes, (MAGIC.len() + TRAILER) as u64)?;
        let _encoding = self._memory.child_buffer(memory_bound(bytes)?)?;
        let mut payload = Vec::with_capacity(usize::try_from(bytes).map_err(unknown)?);
        payload.extend_from_slice(MAGIC);
        // to_extend writes into the pre-admitted buffer; no second payload Vec.
        let mut payload = postcard::to_extend(record, payload).map_err(unknown)?;
        let digest = checksum(key, self.count, &payload[MAGIC.len()..]);
        payload.extend_from_slice(&digest);
        if payload.len() as u64 != bytes {
            return Err(unknown("evidence encoding size mismatch"));
        }
        self.append(key, &payload)
    }
}
struct AdmittedKey {
    key: UniqueIdentity,
    _charge: OwnedQueueBufferCharge,
}
struct AdmittedRecord {
    record: ScratchUniqueEvidence,
    _raw: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
impl SortedSpool {
    fn component_bytes(
        &mut self,
        entry: &SpoolEntry,
        part: SpoolComponent,
        bytes: u64,
    ) -> QueueResult<Vec<u8>> {
        let mut buffer = vec![0; usize::try_from(bytes).map_err(unknown)?];
        if self.read_component(entry, part, 0, &mut buffer)? as u64 != bytes {
            return Err(unknown("short semantic component"));
        }
        Ok(buffer)
    }
    fn hydrate_key(&mut self, entry: &SpoolEntry) -> QueueResult<AdmittedKey> {
        let (collection, index, tuple, _) = entry.lengths();
        let charge = self._memory.child_buffer(memory_bound(add(
            add(collection.unwrap_or(0), index)?,
            tuple,
        )?)?)?;
        let collection = match collection {
            None => None,
            Some(bytes) => Some(
                String::from_utf8(self.component_bytes(
                    entry,
                    SpoolComponent::Collection,
                    bytes,
                )?)
                .map_err(unknown)?,
            ),
        };
        let index = String::from_utf8(self.component_bytes(entry, SpoolComponent::Index, index)?)
            .map_err(unknown)?;
        let full_sort_key = self.component_bytes(entry, SpoolComponent::Tuple, tuple)?;
        Ok(AdmittedKey {
            key: UniqueIdentity {
                collection,
                index,
                full_sort_key,
            },
            _charge: charge,
        })
    }
    fn hydrate_record(
        &mut self,
        entry: &SpoolEntry,
        key: &UniqueIdentity,
    ) -> QueueResult<AdmittedRecord> {
        let bytes = entry.lengths().3;
        if bytes < (MAGIC.len() + TRAILER) as u64 {
            return Err(unknown("short evidence envelope"));
        }
        let charge = self._memory.child_buffer(memory_bound(bytes)?)?;
        let raw = self.component_bytes(entry, SpoolComponent::Payload, bytes)?;
        if !raw.starts_with(MAGIC) {
            return Err(unknown("unknown evidence envelope"));
        }
        let body = &raw[MAGIC.len()..raw.len() - TRAILER];
        if checksum(key, entry.descriptor.0[9], body).as_slice() != &raw[raw.len() - TRAILER..] {
            return Err(unknown("evidence checksum mismatch"));
        }
        let (record, tail) =
            postcard::take_from_bytes::<ScratchUniqueEvidence>(body).map_err(unknown)?;
        if !tail.is_empty() {
            return Err(unknown("trailing evidence bytes"));
        }
        // Administrative claims are accepted against immutable initial owners,
        // so a different initial owner cannot be smuggled into final validation.
        if let ScratchUniqueEffect::Claim {
            owner,
            origin: UniqueClaimOrigin::Administrative,
        } = &record.effect
            && record
                .initial_owner
                .as_ref()
                .is_some_and(|initial| initial != owner)
        {
            return Err(unknown("administrative claim lacks per-row acceptance"));
        }
        Ok(AdmittedRecord {
            record,
            _raw: raw,
            _charge: charge,
        })
    }
    fn entry_matches(&mut self, entry: &SpoolEntry, key: &UniqueIdentity) -> QueueResult<bool> {
        let (collection, index, tuple, _) = entry.lengths();
        if collection.is_some() != key.collection.is_some()
            || collection.unwrap_or(0)
                != key.collection.as_ref().map_or(0, |name| name.len() as u64)
            || index != key.index.len() as u64
            || tuple != key.full_sort_key.len() as u64
        {
            return Ok(false);
        }
        let mut window = [0; WINDOW];
        for (part, expected) in [
            (
                SpoolComponent::Collection,
                key.collection.as_deref().unwrap_or("").as_bytes(),
            ),
            (SpoolComponent::Index, key.index.as_bytes()),
            (SpoolComponent::Tuple, key.full_sort_key.as_slice()),
        ] {
            let mut position = 0;
            while position < expected.len() {
                let size = (expected.len() - position).min(WINDOW);
                if self.read_component(entry, part, position as u64, &mut window[..size])? != size {
                    return Err(unknown("short grouped key"));
                }
                if window[..size] != expected[position..position + size] {
                    return Ok(false);
                }
                position += size;
            }
        }
        Ok(true)
    }
    /// Completes this evidence spool, not the original selected source. Every
    /// tuple is consumed even after a provisional business conflict. No ACK.
    pub fn reduce_unique_evidence(&mut self) -> QueueResult<UniqueTupleDecision> {
        if self.poisoned {
            return Err(unknown("reduce poisoned spool"));
        }
        let result = self.rewind().and_then(|()| self.reduce_evidence_checked());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn reduce_evidence_checked(&mut self) -> QueueResult<UniqueTupleDecision> {
        let driver = self._memory.child_buffer((WINDOW + 8192) as u64)?;
        let mut pending = self.next_entry()?;
        let mut overall = UniqueTupleDecision {
            administrative_collision: false,
            final_collision: false,
        };
        while let Some(first) = pending.take() {
            let key = self.hydrate_key(&first)?;
            let header = self.hydrate_record(&first, &key.key)?;
            let mut reducer = UniqueTupleReducer::new(
                &key.key,
                header.record.initial_loaded,
                header.record.initial_owner.as_ref(),
                driver.reservation(),
            )?;
            reducer.observe(header.record.borrowed(&key.key))?;
            while let Some(entry) = self.next_entry()? {
                if !self.entry_matches(&entry, &key.key)? {
                    pending = Some(entry);
                    break;
                }
                let row = self.hydrate_record(&entry, &key.key)?;
                reducer.observe(row.record.borrowed(&key.key))?;
            }
            let result = reducer.finish()?;
            overall.administrative_collision |= result.administrative_collision;
            overall.final_collision |= result.final_collision;
        }
        Ok(overall)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::{FieldValue, Rejection};
    use fireweed_engine::queue_admission::QueueAdmission;
    use fireweed_engine::queue_fields::UniqueFieldEffects;
    use fireweed_engine::queue_planner::{RecordIdentity, RecordSnapshot};
    use std::collections::{BTreeMap, BTreeSet};
    fn owner(name: &str) -> RecordIdentity {
        RecordIdentity {
            collection: None,
            key: name.into(),
        }
    }
    fn key(value: u8) -> UniqueIdentity {
        UniqueIdentity {
            collection: None,
            index: "unique".into(),
            full_sort_key: vec![value],
        }
    }
    fn claim(
        initial: Option<&RecordIdentity>,
        claimant: &RecordIdentity,
        origin: UniqueClaimOrigin,
    ) -> ScratchUniqueEvidence {
        ScratchUniqueEvidence {
            initial_loaded: true,
            initial_owner: initial.cloned(),
            effect: ScratchUniqueEffect::Claim {
                owner: claimant.clone(),
                origin,
            },
        }
    }
    #[test]
    fn actual_spilled_combined_reduction_matches_complete_oracle_including_late_release() {
        let directory = tempfile::tempdir().unwrap();
        let tuple = key(7);
        let a = owner("a");
        let b = owner("b");
        let c = owner("c");
        for initial in [None, Some(&a)] {
            for release in [false, true] {
                if release && initial.is_none() {
                    continue;
                }
                for claims in [vec![], vec![&a], vec![&b], vec![&b, &b], vec![&b, &c]] {
                    let admission = QueueAdmission::new(1, 65536).unwrap();
                    let job = Arc::new(admission.reserve(0).unwrap());
                    let quota = ScratchDiskQuota::new(65536).unwrap();
                    let mut spool =
                        QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
                    for claimant in &claims {
                        spool
                            .append_unique_evidence(
                                &tuple,
                                &claim(initial, claimant, UniqueClaimOrigin::Ordinary),
                            )
                            .unwrap();
                    }
                    if release {
                        spool
                            .append_unique_evidence(
                                &tuple,
                                &ScratchUniqueEvidence {
                                    initial_loaded: true,
                                    initial_owner: Some(a.clone()),
                                    effect: ScratchUniqueEffect::Release(a.clone()),
                                },
                            )
                            .unwrap();
                    }
                    let mut sorted = spool.seal().unwrap();
                    let result = sorted.reduce_unique_evidence().unwrap();
                    let effects = UniqueFieldEffects {
                        releases: if release {
                            BTreeMap::from([(tuple.clone(), a.clone())])
                        } else {
                            BTreeMap::new()
                        },
                        claims: claims
                            .iter()
                            .map(|owner| (tuple.clone(), (*owner).clone()))
                            .collect(),
                    };
                    let image = RecordSnapshot {
                        unique_probes_loaded: BTreeSet::from([tuple.clone()]),
                        unique_owners: initial
                            .map(|owner| BTreeMap::from([(tuple.clone(), owner.clone())]))
                            .unwrap_or_default(),
                        ..Default::default()
                    };
                    assert_eq!(
                        result.final_collision,
                        effects.validate(&image).unwrap() == Err(Rejection::UniqueConflict)
                    );
                    assert!(!result.administrative_collision);
                    sorted.rewind().unwrap();
                    assert_eq!(sorted.reduce_unique_evidence().unwrap(), result);
                    drop(sorted);
                    assert_eq!(quota.usage().unwrap(), 0);
                    assert_eq!(admission.usage().unwrap().1, 0);
                }
            }
        }
    }
    #[test]
    fn composite_edit_authority_exceeds_ram_but_spilled_reduction_succeeds() {
        let directory = tempfile::tempdir().unwrap();
        let memory = 2 << 20;
        let admission = QueueAdmission::new(1, memory).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(64 << 20).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        for index in 0..48 {
            let _row = job.buffer(6 * (256 << 10) + 8192).unwrap();
            let b = FieldValue::Text(format!("{}-{index}", "b".repeat(256 << 10)));
            let record_owner = owner(&format!("work-{index}"));
            for a in [0, 1] {
                let a_value = FieldValue::Integer(a);
                let tuple = UniqueIdentity {
                    collection: None,
                    index: "unique_ab".into(),
                    full_sort_key: fireweed_engine::queue_index::ordered_key(&[
                        Some(&a_value),
                        Some(&b),
                    ])
                    .unwrap(),
                };
                let record = if a == 0 {
                    ScratchUniqueEvidence {
                        initial_loaded: true,
                        initial_owner: Some(record_owner.clone()),
                        effect: ScratchUniqueEffect::Release(record_owner.clone()),
                    }
                } else {
                    claim(None, &record_owner, UniqueClaimOrigin::Administrative)
                };
                spool.append_unique_evidence(&tuple, &record).unwrap();
            }
        }
        let mut sorted = spool.seal().unwrap();
        assert!(quota.usage().unwrap() > 12 * memory);
        let fixed = admission.usage().unwrap().1;
        assert_eq!(
            sorted.reduce_unique_evidence().unwrap(),
            UniqueTupleDecision {
                administrative_collision: false,
                final_collision: false
            }
        );
        assert_eq!(admission.usage().unwrap().1, fixed);
        drop(sorted);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    fn append_bad(spool: &mut QueueSpool, tuple: &UniqueIdentity, body: &[u8], valid_digest: bool) {
        let mut payload = MAGIC.to_vec();
        payload.extend_from_slice(body);
        payload.extend_from_slice(&if valid_digest {
            checksum(tuple, spool.count, body)
        } else {
            [0; 32]
        });
        spool.append(tuple, &payload).unwrap();
    }
    #[test]
    fn late_checksum_serde_and_read_failures_override_provisional_business_conflict() {
        for failure in 0..4 {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 65536).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(65536).unwrap();
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            for claimant in [owner("a"), owner("b")] {
                spool
                    .append_unique_evidence(
                        &key(0),
                        &claim(None, &claimant, UniqueClaimOrigin::Administrative),
                    )
                    .unwrap();
            }
            let valid = claim(None, &owner("late"), UniqueClaimOrigin::Ordinary);
            let mut body = postcard::to_allocvec(&valid).unwrap();
            match failure {
                0 => append_bad(&mut spool, &key(1), &body, false),
                1 => append_bad(&mut spool, &key(1), &[255], true),
                2 => {
                    body.push(0);
                    append_bad(&mut spool, &key(1), &body, true);
                }
                _ => spool.append_unique_evidence(&key(1), &valid).unwrap(),
            }
            let mut sorted = spool.seal().unwrap();
            if failure == 3 {
                sorted.data.set_len(sorted.data_bytes - 1).unwrap();
            }
            assert!(matches!(
                sorted.reduce_unique_evidence(),
                Err(QueueError::OutcomeUnknown(_))
            ));
            assert!(sorted.rewind().is_err());
            assert!(sorted.next_entry().is_err());
            assert!(quota.usage().unwrap() > 0);
            drop(sorted);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }
    #[test]
    fn late_contradictory_initial_authority_and_unaccepted_admin_claim_fail_closed() {
        for case in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 65536).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(65536).unwrap();
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            let a = owner("a");
            let b = owner("b");
            match case {
                0 => {
                    spool
                        .append_unique_evidence(
                            &key(0),
                            &claim(None, &a, UniqueClaimOrigin::Administrative),
                        )
                        .unwrap();
                    spool
                        .append_unique_evidence(
                            &key(0),
                            &ScratchUniqueEvidence {
                                initial_loaded: true,
                                initial_owner: Some(a.clone()),
                                effect: ScratchUniqueEffect::Release(a.clone()),
                            },
                        )
                        .unwrap();
                }
                1 => spool
                    .append_unique_evidence(
                        &key(0),
                        &claim(Some(&a), &b, UniqueClaimOrigin::Administrative),
                    )
                    .unwrap(),
                _ => {
                    let mut record = claim(None, &a, UniqueClaimOrigin::Ordinary);
                    record.initial_loaded = false;
                    spool.append_unique_evidence(&key(0), &record).unwrap();
                }
            }
            let mut sorted = spool.seal().unwrap();
            assert!(sorted.reduce_unique_evidence().is_err());
            assert!(sorted.rewind().is_err());
            drop(sorted);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap().1, 0);
        }
    }
    #[test]
    fn complete_reduction_rewinds_partial_cursor_and_replays_without_explicit_rewind() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(65536).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        for claimant in [owner("a"), owner("b")] {
            spool
                .append_unique_evidence(
                    &key(0),
                    &claim(None, &claimant, UniqueClaimOrigin::Administrative),
                )
                .unwrap();
        }
        spool
            .append_unique_evidence(
                &key(1),
                &claim(None, &owner("later"), UniqueClaimOrigin::Administrative),
            )
            .unwrap();
        let mut sorted = spool.seal().unwrap();
        let _partial = sorted.next_entry().unwrap().unwrap();
        let expected = UniqueTupleDecision {
            administrative_collision: true,
            final_collision: true,
        };
        assert_eq!(sorted.reduce_unique_evidence().unwrap(), expected);
        assert!(sorted.next_entry().unwrap().is_none());
        assert_eq!(sorted.reduce_unique_evidence().unwrap(), expected);
        drop(sorted);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn decoder_capacity_failure_keeps_spool_ownership_and_discards_temporaries() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        let tuple = UniqueIdentity {
            collection: None,
            index: "u".into(),
            full_sort_key: vec![8; 256 << 10],
        };
        spool
            .append_unique_evidence(
                &tuple,
                &claim(None, &owner("a"), UniqueClaimOrigin::Ordinary),
            )
            .unwrap();
        let mut sorted = spool.seal().unwrap();
        let baseline = admission.usage().unwrap().1;
        assert!(matches!(
            sorted.reduce_unique_evidence(),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap().1, baseline);
        assert!(quota.usage().unwrap() > 0);
        drop(sorted);
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
    #[test]
    fn evidence_ordinal_binding_and_strict_order_reject_valid_record_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(65536).unwrap();
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        let tuple = key(0);
        let record = claim(None, &owner("a"), UniqueClaimOrigin::Ordinary);
        let body = postcard::to_allocvec(&record).unwrap();
        let mut payload = MAGIC.to_vec();
        payload.extend_from_slice(&body);
        payload.extend_from_slice(&checksum(&tuple, 0, &body));
        spool.append(&tuple, &payload).unwrap();
        spool.append(&tuple, &payload).unwrap();
        let mut sorted = spool.seal().unwrap();
        assert!(sorted.reduce_unique_evidence().is_err());
        assert!(sorted.rewind().is_err());
        drop(sorted);
        let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
        spool.append_unique_evidence(&tuple, &record).unwrap();
        spool.append_unique_evidence(&tuple, &record).unwrap();
        let mut sorted = spool.seal().unwrap();
        let original = descriptor(&mut sorted.index, 0, sorted.data_bytes, sorted.count).unwrap();
        sorted.index.seek(SeekFrom::Start(INDEX_BYTES)).unwrap();
        sorted.index.write_all(&original.encode()).unwrap();
        assert!(sorted.reduce_unique_evidence().is_err());
        assert!(sorted.rewind().is_err());
        drop(sorted);
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn unknown_evidence_version_and_invalid_key_utf8_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 65536).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(65536).unwrap();
        for invalid_key in [false, true] {
            let mut spool = QueueSpool::new(directory.path(), Arc::clone(&quota), &job).unwrap();
            let tuple = key(0);
            let record = claim(None, &owner("a"), UniqueClaimOrigin::Ordinary);
            if invalid_key {
                spool.append_unique_evidence(&tuple, &record).unwrap();
                spool.data.seek(SeekFrom::Start(0)).unwrap();
                spool.data.write_all(&[255]).unwrap();
            } else {
                let mut payload = b"FWUE2".to_vec();
                payload.extend_from_slice(&[0; 32]);
                spool.append(&tuple, &payload).unwrap();
            }
            let mut sorted = spool.seal().unwrap();
            assert!(sorted.reduce_unique_evidence().is_err());
            assert!(sorted.next_entry().is_err());
            drop(sorted);
        }
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap().1, 0);
    }
}
