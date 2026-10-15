use super::*;
use crate::{
    queue_admission::QueueAdmission,
    queue_log_record::encode_checked_genesis,
    queue_namespace::AddressRef,
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
};
use fireweed_core::queue_contract::{CollectionDefinition, QueueAddress, QueueSchema};

struct Fixture {
    record: OwnedQueueLogRecord,
    identity: Arc<OwnedQueueBufferCharge>,
    job: Arc<QueueReservation>,
    admission: Arc<QueueAdmission>,
}
impl Fixture {
    fn new() -> Self {
        let admission = QueueAdmission::new(2, 1 << 20).unwrap();
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
            1 << 20,
            &job,
        )
        .unwrap();
        let identity = Arc::new(job.owned_buffer(128).unwrap());
        Self {
            record,
            identity,
            job,
            admission,
        }
    }
    fn cut(&self) -> QueueSourceCut<'_> {
        QueueSourceCut::from_captured(
            &self.identity,
            AddressRef {
                tenant: "t",
                queue: "q",
            },
            1,
            0,
            &self.job,
        )
        .unwrap()
    }
}

#[test]
fn combined_components_obey_exact_edit_and_wire_boundaries() {
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    // Includes a deletion, an empty-unit posting, and a nonempty value. Unit
    // serializes to zero bytes but still consumes one full key and one edit.
    let bytes = 1 + 2 + 3 + encoded_len(&127u64).unwrap();
    for limits in [
        QueueSourceCompileLimits {
            edits: 3,
            input_bytes: bytes,
        },
        QueueSourceCompileLimits {
            edits: 2,
            input_bytes: bytes,
        },
        QueueSourceCompileLimits {
            edits: 3,
            input_bytes: bytes - 1,
        },
    ] {
        let quota = OutputQuota::new(&f.record, f.cut(), &f.job, limits).unwrap();
        let mut a =
            SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "a").unwrap();
        let mut b =
            SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "b").unwrap();
        a.insert_encoded::<()>(b"a", None).unwrap();
        b.insert_encoded(b"bb", Some(&())).unwrap();
        let result = a.insert_encoded(b"ccc", Some(&127u64));
        if limits.edits == 3 && limits.input_bytes == bytes {
            result.unwrap();
            a.verify(&f.record, &f.cut(), &f.job).unwrap();
            b.verify(&f.record, &f.cut(), &f.job).unwrap();
            let state = quota.state.lock().unwrap();
            assert_eq!((state.edits, state.bytes), (3, bytes));
        } else {
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("exceeds admitted limits"))
            );
            assert!(a.verify(&f.record, &f.cut(), &f.job).is_err());
            assert!(b.verify(&f.record, &f.cut(), &f.job).is_err());
            assert!(b.insert_encoded(b"d", Some(&())).is_err());
        }
        drop((a, b, quota));
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
}

#[test]
fn a_late_root_failure_poisoning_is_shared_and_never_refunded() {
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    let quota = OutputQuota::new(
        &f.record,
        f.cut(),
        &f.job,
        QueueSourceCompileLimits {
            edits: 10,
            input_bytes: 100,
        },
    )
    .unwrap();
    let mut a = SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "a").unwrap();
    let mut b = SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "b").unwrap();
    a.insert_encoded(b"a", Some(&())).unwrap();
    let occupied = f.admission.usage().unwrap().1;
    let blocker = f.job.owned_buffer((1 << 20) - occupied).unwrap();
    let error = b.insert_encoded(b"b", Some(&())).unwrap_err();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(ref message) if message.contains("b output admission"))
    );
    drop(blocker);
    assert!(a.verify(&f.record, &f.cut(), &f.job).is_err());
    assert!(a.insert_encoded(b"c", Some(&())).is_err());
    let state = quota.state.lock().unwrap();
    assert_eq!((state.edits, state.bytes), (2, 2));
    drop(state);
    drop((a, b, quota));
    assert_eq!(f.admission.usage().unwrap(), baseline);
}

#[test]
fn equal_numeric_cuts_cannot_share_a_quota_with_another_identity_or_job() {
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    for foreign_job in [false, true] {
        let quota = OutputQuota::new(
            &f.record,
            f.cut(),
            &f.job,
            QueueSourceCompileLimits {
                edits: 10,
                input_bytes: 100,
            },
        )
        .unwrap();
        let sibling =
            SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "sibling").unwrap();
        let identity = Arc::new(f.job.owned_buffer(128).unwrap());
        let cut = QueueSourceCut::from_captured(
            &identity,
            AddressRef {
                tenant: "t",
                queue: "q",
            },
            1,
            0,
            &f.job,
        )
        .unwrap();
        let other_job = Arc::new(f.admission.reserve(0).unwrap());
        let result = SourceEdits::with_quota(
            &f.record,
            if foreign_job { f.cut() } else { cut },
            if foreign_job { &other_job } else { &f.job },
            quota.clone(),
            "foreign",
        );
        assert!(
            matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("scope differs"))
        );
        assert!(sibling.verify(&f.record, &f.cut(), &f.job).is_err());
        drop(result);
        drop((sibling, quota));
        drop((identity, other_job));
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
}

#[test]
fn duplicate_and_serialization_failures_invalidate_prior_output() {
    struct Invalid;
    impl serde::Serialize for Invalid {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("injected length failure"))
        }
    }
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    for duplicate in [false, true] {
        let quota = OutputQuota::new(
            &f.record,
            f.cut(),
            &f.job,
            QueueSourceCompileLimits {
                edits: 10,
                input_bytes: 100,
            },
        )
        .unwrap();
        let mut a =
            SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "a").unwrap();
        let mut b =
            SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "b").unwrap();
        a.insert_encoded(b"a", Some(&())).unwrap();
        if duplicate {
            let error = a.insert_encoded(b"a", Some(&())).unwrap_err();
            assert!(
                matches!(error, QueueError::OutcomeUnknown(ref message) if message.contains("duplicate provisional"))
            );
        } else {
            assert!(b.insert_encoded(b"b", Some(&Invalid)).is_err());
        }
        assert_eq!(a.edits().len(), 1);
        assert!(a.verify(&f.record, &f.cut(), &f.job).is_err());
        assert!(b.verify(&f.record, &f.cut(), &f.job).is_err());
        drop((a, b, quota));
        assert_eq!(f.admission.usage().unwrap(), baseline);
    }
}

#[test]
fn caught_serializer_unwinds_poison_siblings_at_length_and_encoding() {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct Panics {
        calls: AtomicUsize,
        at: usize,
    }
    impl serde::Serialize for Panics {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == self.at {
                std::panic::panic_any("exact injected serialization panic");
            }
            serializer.serialize_u64(1)
        }
    }
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    for encoded_key in [false, true] {
        for at in [0, 1] {
            let quota = OutputQuota::new(
                &f.record,
                f.cut(),
                &f.job,
                QueueSourceCompileLimits {
                    edits: 10,
                    input_bytes: 100,
                },
            )
            .unwrap();
            let mut a =
                SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "a").unwrap();
            let mut b =
                SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "b").unwrap();
            a.insert_encoded(b"a", Some(&())).unwrap();
            let value = Panics {
                calls: AtomicUsize::new(0),
                at,
            };
            let panic = catch_unwind(AssertUnwindSafe(|| {
                if encoded_key {
                    b.insert_encoded(b"b", Some(&value))
                } else {
                    b.insert(QueueSourceKey::Sequence, Some(&value))
                }
            }))
            .unwrap_err();
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"exact injected serialization panic")
            );
            assert_eq!(value.calls.load(Ordering::SeqCst), at + 1);
            assert_eq!(a.edits().len(), 1);
            assert_eq!(b.edits().len(), 0);
            assert!(a.verify(&f.record, &f.cut(), &f.job).is_err());
            assert!(b.verify(&f.record, &f.cut(), &f.job).is_err());
            assert!(a.insert_encoded(b"c", Some(&())).is_err());
            drop((a, b, quota));
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    }
}

#[test]
fn dropping_a_component_never_refunds_its_logical_debits() {
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    let quota = OutputQuota::new(
        &f.record,
        f.cut(),
        &f.job,
        QueueSourceCompileLimits {
            edits: 1,
            input_bytes: 1,
        },
    )
    .unwrap();
    let mut a = SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "a").unwrap();
    let mut b = SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "b").unwrap();
    a.insert_encoded(b"a", Some(&())).unwrap();
    drop(a);
    let state = quota.state.lock().unwrap();
    assert_eq!((state.edits, state.bytes), (1, 1));
    drop(state);
    assert!(b.insert_encoded(b"b", Some(&())).is_err());
    assert!(b.verify(&f.record, &f.cut(), &f.job).is_err());
    drop((b, quota));
    assert_eq!(f.admission.usage().unwrap(), baseline);
}

#[test]
fn equal_record_bytes_do_not_substitute_for_the_attempt_owner() {
    let f = Fixture::new();
    let baseline = f.admission.usage().unwrap();
    let charge = f
        .job
        .owned_buffer(f.record.bytes().len() as u64 * 4 + 4096)
        .unwrap();
    let equal =
        OwnedQueueLogRecord::from_admitted_bytes(f.record.bytes().to_vec().into(), charge, &f.job)
            .unwrap();
    assert_eq!(equal.bytes(), f.record.bytes());
    let quota = OutputQuota::new(
        &f.record,
        f.cut(),
        &f.job,
        QueueSourceCompileLimits {
            edits: 1,
            input_bytes: 1,
        },
    )
    .unwrap();
    let sibling =
        SourceEdits::with_quota(&f.record, f.cut(), &f.job, quota.clone(), "sibling").unwrap();
    let result = SourceEdits::with_quota(&equal, f.cut(), &f.job, quota.clone(), "equal");
    assert!(
        matches!(result, Err(QueueError::OutcomeUnknown(ref message)) if message.contains("scope differs"))
    );
    assert!(sibling.verify(&f.record, &f.cut(), &f.job).is_err());
    drop(result);
    drop((sibling, quota));
    drop(equal);
    assert_eq!(f.admission.usage().unwrap(), baseline);
}
