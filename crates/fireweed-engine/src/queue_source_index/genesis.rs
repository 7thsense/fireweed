//! Engine policy for persisted Genesis facts and historical record correspondence.
use super::{
    QueueSourceCut, QueueSourceGenesis, QueueSourceKey, QueueSourceReader, QueueSourceSpan,
};
use crate::{
    queue_admission::QueueReservation,
    queue_log_record::{CheckedQueueLogRecord, QueueLogPayload},
};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::Arc;

fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source genesis: {message}"))
}

/// Capture the reader's ORIGINAL cut before awaiting. The adapter must retain
/// that same source when hydrating the returned fact's offset-zero record.
pub async fn load_source_genesis<R: QueueSourceReader>(
    reader: &R,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceGenesis> {
    let cut = reader.cut()?;
    load_source_genesis_at(reader, cut, job).await
}

pub(super) async fn load_source_genesis_at<R: QueueSourceReader>(
    reader: &R,
    cut: QueueSourceCut<'_>,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceGenesis> {
    if !cut.belongs_to(job) || cut.through() == 0 {
        return Err(unknown("operation or nonempty cut differs"));
    }
    let key = QueueSourceKey::Genesis;
    let _charge = job.owned_buffer(key.encoded_len()? as u64)?;
    let key = key.encode()?;
    let owner = reader.lookup(&key).await?;
    let point = reader.point(&owner)?;
    let bytes = point
        .value_for(&cut, &key)?
        .ok_or_else(|| unknown("covered source has no genesis fact"))?;
    let fact: QueueSourceGenesis = crate::queue_storage_layout::decode(bytes)?;
    if fact.source.log_offset() != 0 || fact.source.byte_length() == 0 {
        return Err(unknown("invalid genesis source span"));
    }
    Ok(fact)
}
impl QueueSourceGenesis {
    /// This verifies a historical record, not a proposed append. Its epoch may
    /// precede the captured head, and later records may already exist. Physical
    /// membership of observed_offset in the original cut is the adapter's duty.
    pub fn verify_record(
        &self,
        record: &CheckedQueueLogRecord<'_>,
        observed_offset: u64,
    ) -> QueueResult<()> {
        let QueueLogPayload::Genesis(genesis) = record.view().payload() else {
            return Err(unknown("offset zero is not genesis"));
        };
        let full = &record.owner().bytes()[record.payload_interval()?];
        if observed_offset != 0
            || self.source != QueueSourceSpan::from_checked(record, 0, full)?
            || record.view().definition_pin_bytes() != *self.definition_pin.as_bytes()
        {
            return Err(unknown(
                "genesis record, full payload span or stored pin differs",
            ));
        }
        self.definition_pin
            .verify(genesis.definition_bytes())
            .map_err(|_| unknown("genesis definition bytes differ from stored pin"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        queue_admission::{OwnedQueueBufferCharge, QueueAdmission},
        queue_log_record::{OwnedQueueLogRecord, encode_checked_genesis},
        queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
        queue_source_index::{QueueSourcePoint, predecessor::tests::Reader},
        queue_storage_layout::encode,
    };
    use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    fn fixture() -> (
        Arc<QueueReservation>,
        OwnedQueueLogRecord,
        QueueSourceGenesis,
        Reader,
    ) {
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
        let record = encode_checked_genesis(
            1,
            &QueueAddress {
                tenant: "t".into(),
                queue: "q".into(),
            },
            &definition,
            &[7; 32],
            4096,
            &job,
        )
        .unwrap();
        let checked = record.checked().unwrap();
        let fact = QueueSourceGenesis {
            source: QueueSourceSpan::from_checked(
                &checked,
                0,
                &record.bytes()[checked.payload_interval().unwrap()],
            )
            .unwrap(),
            definition_pin: definition.pin(),
        };
        let reader = Reader {
            through: 9,
            identity: Arc::new(job.owned_buffer(128).unwrap()),
            facts: BTreeMap::from([(
                QueueSourceKey::Genesis.encode().unwrap(),
                encode(&fact).unwrap(),
            )]),
        };
        (job, record, fact, reader)
    }
    #[test]
    fn historical_genesis_requires_the_full_payload_span_and_stored_pin() {
        let (_job, record, fact, _reader) = fixture();
        let checked = record.checked().unwrap();
        fact.verify_record(&checked, 0).unwrap();
        assert!(fact.verify_record(&checked, 1).is_err());
        let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
            panic!()
        };
        let subspan = QueueSourceGenesis {
            source: QueueSourceSpan::from_checked(&checked, 0, genesis.definition_bytes()).unwrap(),
            ..fact
        };
        // This is a VALID internal span with the correct full-record checksum.
        subspan.source.resolve(&checked, 0).unwrap();
        assert!(subspan.verify_record(&checked, 0).is_err());
        let wrong_offset = QueueSourceGenesis {
            source: QueueSourceSpan::from_checked(
                &checked,
                1,
                &record.bytes()[checked.payload_interval().unwrap()],
            )
            .unwrap(),
            ..fact
        };
        assert!(wrong_offset.verify_record(&checked, 0).is_err());
        let (_job, other_record, other_fact, _reader) = fixture();
        let mut raw = encode(&other_fact).unwrap();
        *raw.last_mut().unwrap() ^= 1;
        let wrong_pin: QueueSourceGenesis = crate::queue_storage_layout::decode(&raw).unwrap();
        assert!(
            wrong_pin
                .verify_record(&other_record.checked().unwrap(), 0)
                .is_err()
        );
    }
    struct BoundReader {
        inner: Reader,
        foreign: Arc<OwnedQueueBufferCharge>,
        cuts: AtomicUsize,
        lookups: AtomicUsize,
        fault: &'static str,
    }
    impl QueueSourceReader for BoundReader {
        type Point = Vec<u8>;
        type Range = ();
        fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
            self.cuts.fetch_add(1, Ordering::SeqCst);
            self.inner.cut()
        }
        async fn lookup(&self, key: &[u8]) -> QueueResult<Vec<u8>> {
            assert_eq!(
                self.cuts.load(Ordering::SeqCst),
                1,
                "capture must precede lookup"
            );
            self.lookups.fetch_add(1, Ordering::SeqCst);
            self.inner.lookup(key).await
        }
        fn point<'a>(&'a self, key: &'a Vec<u8>) -> QueueResult<QueueSourcePoint<'a>> {
            let cut = self.inner.cut()?;
            let cut = if self.fault == "foreign" {
                QueueSourceCut::from_captured(
                    &self.foreign,
                    cut.address(),
                    cut.epoch(),
                    cut.through(),
                    self.foreign.reservation(),
                )?
            } else {
                cut
            };
            Ok(cut.point_from_source(
                if self.fault == "key" { b"wrong" } else { key },
                self.inner.facts.get(key).map(Vec::as_slice),
            ))
        }
        async fn complete_range(&self, _lower: &[u8], _upper: &[u8]) -> QueueResult<()> {
            panic!()
        }
        fn visit_range(
            &self,
            _owner: &(),
            _lower: &[u8],
            _upper: &[u8],
            _visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
        ) -> QueueResult<()> {
            panic!()
        }
    }
    #[test]
    fn source_genesis_loader_retains_original_cut_and_full_key_evidence() {
        futures::executor::block_on(source_genesis_loader_contract());
    }
    async fn source_genesis_loader_contract() {
        for fault in [
            "none", "foreign", "key", "missing", "trailing", "empty", "job",
        ] {
            let (job, record, _fact, mut inner) = fixture();
            if fault == "empty" {
                inner.through = 0;
            }
            if fault == "missing" {
                inner.facts.clear();
            }
            if fault == "trailing" {
                inner.facts.values_mut().next().unwrap().push(0);
            }
            let reader = BoundReader {
                inner,
                foreign: Arc::new(job.owned_buffer(128).unwrap()),
                cuts: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
                fault,
            };
            let other_admission = QueueAdmission::new(1, 1 << 20).unwrap();
            let other_job = Arc::new(other_admission.reserve(0).unwrap());
            let result =
                load_source_genesis(&reader, if fault == "job" { &other_job } else { &job }).await;
            if fault == "none" {
                result
                    .unwrap()
                    .verify_record(&record.checked().unwrap(), 0)
                    .unwrap();
            } else {
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(_))),
                    "{fault}"
                );
            }
            assert_eq!(reader.cuts.load(Ordering::SeqCst), 1);
            assert_eq!(
                reader.lookups.load(Ordering::SeqCst),
                usize::from(fault != "empty" && fault != "job")
            );
        }
    }
}
