use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueAdmission, QueueReservation},
    queue_namespace::AddressRef,
    queue_source_index::QueueSourceKey,
};
use fireweed_core::queue_contract::Lifecycle;
use std::sync::Arc;
struct Fixture {
    job: Arc<QueueReservation>,
    owner: Arc<OwnedQueueBufferCharge>,
}
impl Fixture {
    fn new() -> Self {
        let root = QueueAdmission::new(2, 8192).unwrap();
        let job = Arc::new(root.reserve(0).unwrap());
        let owner = Arc::new(job.owned_buffer(128).unwrap());
        Self { job, owner }
    }
    fn cut(&self) -> QueueSourceCut<'_> {
        QueueSourceCut::from_captured(
            &self.owner,
            AddressRef {
                tenant: "t",
                queue: "q",
            },
            2,
            3,
            &self.job,
        )
        .unwrap()
    }
}
fn row() -> WorkMetadata {
    WorkMetadata {
        version: 1,
        priority: -1,
        available_at: 17,
        insertion_sequence: 2,
        lifecycle: Lifecycle::Leased,
        eligible_since: 9,
        attempts: 3,
        lease_expires_at: Some(-1),
        lease_hash: Some([1; 32]),
    }
}
#[test]
fn live_key_memberships_preserve_full_domain_and_refuse_blank_or_foreign_evidence() {
    let f = Fixture::new();
    let cut = f.cut();
    let other_owner = Arc::new(f.job.owned_buffer(128).unwrap());
    let other = QueueSourceCut::from_captured(&other_owner, cut.address(), 2, 3, &f.job).unwrap();
    for collection in [None, Some("state\0é")] {
        for key in [" k ", "\0", "é\0🦊"] {
            let raw = QueueSourceKey::LiveKey { collection, key }
                .encode()
                .unwrap();
            let value =
                decode_live_key(&cut, cut.point_from_source(&raw, Some(&[])), collection).unwrap();
            assert_eq!(value.collection(), collection);
            assert_eq!(value.key_len(), key.len());
            assert!(value.key_bytes().eq(key.bytes()));
            assert!(
                decode_live_key(&cut, other.point_from_source(&raw, Some(&[])), collection)
                    .is_err()
            );
            for marker in [None, Some(&[0][..]), Some(&[1][..])] {
                assert!(
                    decode_live_key(&cut, cut.point_from_source(&raw, marker), collection).is_err()
                );
            }
            for wrong in [Some(""), Some("state"), Some("state\0éx")] {
                assert!(
                    decode_live_key(&cut, cut.point_from_source(&raw, Some(&[])), wrong).is_err()
                );
            }
            for end in 0..raw.len() {
                assert!(
                    decode_live_key(
                        &cut,
                        cut.point_from_source(&raw[..end], Some(&[])),
                        collection
                    )
                    .is_err()
                );
            }
            let mut extra = raw;
            extra.push(0);
            assert!(
                decode_live_key(&cut, cut.point_from_source(&extra, Some(&[])), collection)
                    .is_err()
            );
        }
    }
    for key in ["", " ", "\t\r\n", "\u{2003}\u{a0}"] {
        let raw = QueueSourceKey::LiveKey {
            collection: None,
            key,
        }
        .encode()
        .unwrap();
        assert!(decode_live_key(&cut, cut.point_from_source(&raw, Some(&[])), None).is_err());
        let family = super::super::posting::QueueSourcePostingFamily::Scalar {
            collection: None,
            field: "n",
        };
        let sort = crate::queue_index::ordered_key(&[Some(
            &fireweed_core::queue_contract::FieldValue::Integer(1),
        )])
        .unwrap();
        let raw = QueueSourceKey::ScalarPosting {
            collection: None,
            field: "n",
            sort: &sort,
            key,
        }
        .encode()
        .unwrap();
        assert!(
            family
                .decode(
                    &cut,
                    cut.point_from_source(&raw, Some(&[])),
                    &[fireweed_core::queue_contract::FieldType::Integer]
                )
                .is_err()
        );
    }
}
#[test]
fn intrinsic_memberships_preserve_all_fixed_tuples_and_require_metadata_correspondence() {
    let f = Fixture::new();
    let cut = f.cut();
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        let row = WorkMetadata {
            priority: value,
            available_at: value,
            eligible_since: value,
            lease_expires_at: Some(value),
            ..row()
        };
        for index in QueueSourceWorkIndex::ALL {
            let posting = index.posting(row).unwrap().unwrap();
            let raw = QueueSourceKey::WorkIntrinsic {
                posting,
                key: "k\0é",
            }
            .encode()
            .unwrap();
            let decoded = index
                .decode(&cut, cut.point_from_source(&raw, Some(&[])))
                .unwrap();
            assert_eq!(decoded.index(), index);
            assert_eq!(decoded.sort(), posting.sort());
            assert!(decoded.identity().key_bytes().eq("k\0é".bytes()));
            assert_eq!(decoded.identity().collection(), None);
            assert!(decoded.matches_metadata(row).unwrap());
            let different = match index {
                QueueSourceWorkIndex::RawLifecycle => WorkMetadata {
                    lifecycle: Lifecycle::Pending,
                    lease_expires_at: None,
                    lease_hash: None,
                    ..row
                },
                QueueSourceWorkIndex::Attempts => WorkMetadata {
                    attempts: row.attempts + 1,
                    ..row
                },
                QueueSourceWorkIndex::LeaseExpiry => WorkMetadata {
                    lease_expires_at: Some(!value),
                    ..row
                },
                QueueSourceWorkIndex::Schedule | QueueSourceWorkIndex::OverdueSchedule => {
                    WorkMetadata {
                        available_at: !value,
                        ..row
                    }
                }
                _ => WorkMetadata {
                    priority: !value,
                    ..row
                },
            };
            assert!(!decoded.matches_metadata(different).unwrap());
            if matches!(
                index,
                QueueSourceWorkIndex::PriorityAscending
                    | QueueSourceWorkIndex::PriorityDescending
                    | QueueSourceWorkIndex::Schedule
                    | QueueSourceWorkIndex::OverduePriorityAscending
                    | QueueSourceWorkIndex::OverduePriorityDescending
                    | QueueSourceWorkIndex::OverdueSchedule
            ) {
                assert!(
                    !decoded
                        .matches_metadata(WorkMetadata {
                            insertion_sequence: 3,
                            ..row
                        })
                        .unwrap()
                );
            }
            if matches!(
                index,
                QueueSourceWorkIndex::OverduePriorityAscending
                    | QueueSourceWorkIndex::OverduePriorityDescending
                    | QueueSourceWorkIndex::OverdueSchedule
            ) {
                assert!(
                    !decoded
                        .matches_metadata(WorkMetadata {
                            eligible_since: !value,
                            ..row
                        })
                        .unwrap()
                );
            }
            assert!(
                decoded
                    .matches_metadata(WorkMetadata { version: 0, ..row })
                    .is_err()
            );
            // A static posting deliberately excludes record version. The reader
            // must separately establish same-cut live record/HWM correspondence.
            assert!(
                decoded
                    .matches_metadata(WorkMetadata { version: 2, ..row })
                    .unwrap()
            );
        }
    }
}
#[test]
fn intrinsic_invalid_width_index_lifecycle_sequence_identity_and_markers_are_refused() {
    let f = Fixture::new();
    let cut = f.cut();
    let other = Fixture::new();
    for index in QueueSourceWorkIndex::ALL {
        let posting = index.posting(row()).unwrap().unwrap();
        let raw = QueueSourceKey::WorkIntrinsic { posting, key: "k" }
            .encode()
            .unwrap();
        for end in 0..raw.len() {
            assert!(
                index
                    .decode(&cut, cut.point_from_source(&raw[..end], Some(&[])))
                    .is_err(),
                "{index:?} at {end}"
            );
        }
        for marker in [None, Some(&[0][..]), Some(&[1][..])] {
            assert!(
                index
                    .decode(&cut, cut.point_from_source(&raw, marker))
                    .is_err()
            );
        }
        assert!(
            index
                .decode(&cut, other.cut().point_from_source(&raw, Some(&[])))
                .is_err()
        );
        let mut wrong = raw.clone();
        wrong[2] = 255;
        assert!(
            index
                .decode(&cut, cut.point_from_source(&wrong, Some(&[])))
                .is_err()
        );
        let mut extra = raw;
        extra.push(0);
        assert!(
            index
                .decode(&cut, cut.point_from_source(&extra, Some(&[])))
                .is_err()
        );
        for key in ["", "\u{2003}"] {
            let raw = QueueSourceKey::WorkIntrinsic { posting, key }
                .encode()
                .unwrap();
            assert!(
                index
                    .decode(&cut, cut.point_from_source(&raw, Some(&[])))
                    .is_err()
            );
        }
        let mut bad = index.prefix().to_vec();
        let mut sort = posting.sort().to_vec();
        match index {
            QueueSourceWorkIndex::RawLifecycle => sort[0] = 4,
            QueueSourceWorkIndex::Attempts | QueueSourceWorkIndex::LeaseExpiry => continue,
            _ => {
                let length = sort.len();
                sort[length - 8..].fill(0);
            }
        }
        bad.extend_from_slice(&sort);
        crate::queue_index::escaped(&mut bad, b"k");
        assert!(
            index
                .decode(&cut, cut.point_from_source(&bad, Some(&[])))
                .is_err()
        );
    }
}
