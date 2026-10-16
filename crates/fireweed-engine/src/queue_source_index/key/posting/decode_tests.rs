use super::*;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueAdmission, QueueReservation},
    queue_index::ordered_key,
    queue_namespace::AddressRef,
    queue_source_index::QueueSourceKey,
};
use fireweed_core::queue_contract::FieldValue;
use std::sync::Arc;

struct Fixture {
    job: Arc<QueueReservation>,
    owner: Arc<OwnedQueueBufferCharge>,
}
impl Fixture {
    fn new() -> Self {
        let admission = QueueAdmission::new(2, 8192).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
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
fn key(family: QueueSourcePostingFamily<'_>, sort: &[u8], key: &str) -> Vec<u8> {
    match family {
        QueueSourcePostingFamily::Scalar { collection, field } => QueueSourceKey::ScalarPosting {
            collection,
            field,
            sort,
            key,
        },
        QueueSourcePostingFamily::Named { collection, index } => QueueSourceKey::NamedPosting {
            collection,
            index,
            sort,
            key,
        },
    }
    .encode()
    .unwrap()
}
#[test]
fn typed_scalar_postings_borrow_complete_literals_and_all_canonical_scalar_shapes() {
    let f = Fixture::new();
    let cut = f.cut();
    let values = [
        FieldValue::Integer(i64::MIN),
        FieldValue::Integer(0),
        FieldValue::Integer(i64::MAX),
        FieldValue::Float(-f64::MAX),
        FieldValue::Float(-f64::MIN_POSITIVE),
        FieldValue::Float(-0.0),
        FieldValue::Float(f64::from_bits(1)),
        FieldValue::Float(f64::MAX),
        FieldValue::Boolean(false),
        FieldValue::Boolean(true),
        FieldValue::Text("é\0🦊".into()),
        FieldValue::Text(String::new()),
        FieldValue::Bytes(vec![0, 255, 0, 128]),
        FieldValue::Bytes(vec![]),
    ];
    for collection in [None, Some("state\0é")] {
        let family = QueueSourcePostingFamily::Scalar {
            collection,
            field: "bag.\0é",
        };
        for value in &values {
            let sort = ordered_key(&[Some(value)]).unwrap();
            for name in [" k ", "\0", "key\0é🦊"] {
                let raw = key(family, &sort, name);
                let decoded = family
                    .decode(
                        &cut,
                        cut.point_from_source(&raw, Some(&[])),
                        &[value.field_type()],
                    )
                    .unwrap();
                assert_eq!(decoded.identity().collection(), collection);
                assert_eq!(decoded.identity().key_len(), name.len());
                assert_eq!(decoded.sort_len(), sort.len());
                assert!(decoded.identity().key_bytes().eq(name.bytes()));
                assert!(decoded.sort_bytes().eq(sort.iter().copied()));
            }
        }
    }
}
#[test]
fn named_postings_include_missing_components_but_enforce_complete_arity_and_types() {
    let f = Fixture::new();
    let cut = f.cut();
    let family = QueueSourcePostingFamily::Named {
        collection: Some("s"),
        index: "i\0",
    };
    let value = FieldValue::Text("é\0🦊".into());
    let zero = FieldValue::Float(-0.0);
    for (values, types) in [
        (
            vec![None, Some(&zero), Some(&value)],
            vec![FieldType::Integer, FieldType::Float, FieldType::Text],
        ),
        (vec![None; 8], vec![FieldType::Bytes; 8]),
    ] {
        let sort = ordered_key(&values).unwrap();
        let raw = key(family, &sort, "k\0");
        let decoded = family
            .decode(&cut, cut.point_from_source(&raw, Some(&[])), &types)
            .unwrap();
        assert!(decoded.sort_bytes().eq(sort.iter().copied()));
        for bad in [
            types[..types.len() - 1].to_vec(),
            vec![],
            vec![FieldType::Bytes; 9],
        ] {
            assert!(
                family
                    .decode(&cut, cut.point_from_source(&raw, Some(&[])), &bad)
                    .is_err()
            );
        }
    }
    let sort = ordered_key(&[Some(&value)]).unwrap();
    let raw = key(family, &sort, "k");
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&raw, Some(&[])),
                &[FieldType::Integer]
            )
            .is_err()
    );
}
#[test]
fn posting_markers_scope_and_full_namespace_must_match_before_candidate_use() {
    let f = Fixture::new();
    let cut = f.cut();
    let other = Fixture::new();
    let independent_owner = Arc::new(f.job.owned_buffer(128).unwrap());
    let independent =
        QueueSourceCut::from_captured(&independent_owner, cut.address(), 2, 3, &f.job).unwrap();
    let family = QueueSourcePostingFamily::Scalar {
        collection: Some("s"),
        field: "n",
    };
    let sort = ordered_key(&[Some(&FieldValue::Integer(1))]).unwrap();
    let raw = key(family, &sort, "k");
    for marker in [None, Some(&[0][..]), Some(&[1][..]), Some(&[0, 0][..])] {
        assert!(
            family
                .decode(
                    &cut,
                    cut.point_from_source(&raw, marker),
                    &[FieldType::Integer]
                )
                .is_err()
        );
    }
    for foreign in [independent, other.cut()] {
        assert!(
            family
                .decode(
                    &cut,
                    foreign.point_from_source(&raw, Some(&[])),
                    &[FieldType::Integer]
                )
                .is_err()
        );
    }
    for wrong in [
        QueueSourcePostingFamily::Scalar {
            collection: None,
            field: "n",
        },
        QueueSourcePostingFamily::Scalar {
            collection: Some("s\0"),
            field: "n",
        },
        QueueSourcePostingFamily::Scalar {
            collection: Some("s"),
            field: "n\0",
        },
        QueueSourcePostingFamily::Named {
            collection: Some("s"),
            index: "n",
        },
    ] {
        assert!(
            wrong
                .decode(
                    &cut,
                    cut.point_from_source(&raw, Some(&[])),
                    &[FieldType::Integer]
                )
                .is_err()
        );
    }
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&raw, Some(&[])),
                &[FieldType::Integer; 2]
            )
            .is_err()
    );
    for end in 0..raw.len() {
        assert!(
            family
                .decode(
                    &cut,
                    cut.point_from_source(&raw[..end], Some(&[])),
                    &[FieldType::Integer]
                )
                .is_err(),
            "at {end}"
        );
    }
    let mut trailing = raw.clone();
    trailing.extend_from_slice(&[0, 0]);
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&trailing, Some(&[])),
                &[FieldType::Integer]
            )
            .is_err()
    );
    let mut bad_key = family.value_prefix(&sort).unwrap();
    bad_key.extend_from_slice(&[255, 0, 0]);
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&bad_key, Some(&[])),
                &[FieldType::Integer]
            )
            .is_err()
    );
}
#[test]
fn noncanonical_nested_tuples_numbers_and_utf8_never_become_candidates() {
    let f = Fixture::new();
    let cut = f.cut();
    let family = QueueSourcePostingFamily::Scalar {
        collection: None,
        field: "v",
    };
    let mut cases = vec![
        (vec![], FieldType::Integer),
        (vec![2], FieldType::Integer),
        (vec![1, 0], FieldType::Integer),
        (vec![1, 2], FieldType::Integer),
        (vec![1, 1, 0], FieldType::Integer),
        (vec![1, 1, 2, 2], FieldType::Boolean),
        (vec![1, 1, 2], FieldType::Boolean),
        (vec![1, 1, 3, 0, 1], FieldType::Text),
        (vec![1, 1, 3, 0, 255], FieldType::Text),
        (vec![1, 1, 3, 0xc0, 0xaf, 0, 0], FieldType::Text),
        (vec![1, 1, 3, 0xed, 0xa0, 0x80, 0, 0], FieldType::Text),
        (vec![1, 1, 3, 0xc3, 0, 255, 0xa9, 0, 0], FieldType::Text),
        (vec![1, 1, 3, 0xf0, 0x9f, 0, 0], FieldType::Text),
        (vec![1, 1, 4, 0, 0, 1], FieldType::Bytes),
    ];
    for value in [-0.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let bits = value.to_bits();
        let ordered = if bits >> 63 == 1 {
            !bits
        } else {
            bits ^ (1 << 63)
        };
        let mut bytes = vec![1, 1, 1];
        bytes.extend_from_slice(&ordered.to_be_bytes());
        cases.push((bytes, FieldType::Float));
    }
    for (sort, kind) in cases {
        let raw = key(family, &sort, "k");
        assert!(
            family
                .decode(&cut, cut.point_from_source(&raw, Some(&[])), &[kind])
                .is_err(),
            "{kind:?} {sort:?}"
        );
    }
    // Arbitrary byte payloads are legal, while identical text payloads are not.
    let raw = key(family, &[1, 1, 4, 255, 0, 0], "k");
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&raw, Some(&[])),
                &[FieldType::Bytes]
            )
            .is_ok()
    );
}

#[test]
fn family_arity_version_presence_and_type_controls_are_individually_causal() {
    let f = Fixture::new();
    let cut = f.cut();
    for family in [
        QueueSourcePostingFamily::Scalar {
            collection: None,
            field: "v",
        },
        QueueSourcePostingFamily::Named {
            collection: None,
            index: "v",
        },
    ] {
        let counts: &[usize] = match family {
            QueueSourcePostingFamily::Scalar { .. } => &[0, 2],
            _ => &[0, 9],
        };
        for &count in counts {
            let value = FieldValue::Integer(0);
            let values = vec![Some(&value); count];
            let sort = ordered_key(&values).unwrap();
            let raw = key(family, &sort, "k");
            assert!(
                family
                    .decode(
                        &cut,
                        cut.point_from_source(&raw, Some(&[])),
                        &vec![FieldType::Integer; count]
                    )
                    .is_err()
            );
        }
    }
    let family = QueueSourcePostingFamily::Scalar {
        collection: None,
        field: "v",
    };
    let value = FieldValue::Text("é\0🦊".into());
    let sort = ordered_key(&[Some(&value)]).unwrap();
    let raw = key(family, &sort, "k");
    assert!(
        family
            .decode(
                &cut,
                cut.point_from_source(&raw, Some(&[])),
                &[FieldType::Bytes]
            )
            .is_err()
    );
    let valid = ordered_key(&[Some(&FieldValue::Integer(0))]).unwrap();
    for (position, byte) in [(0, 2), (1, 2)] {
        let mut sort = valid.clone();
        sort[position] = byte;
        let raw = key(family, &sort, "k");
        assert!(
            family
                .decode(
                    &cut,
                    cut.point_from_source(&raw, Some(&[])),
                    &[FieldType::Integer]
                )
                .is_err()
        );
    }
}

#[test]
fn matching_blank_names_and_collections_are_not_valid_candidate_identifiers() {
    let f = Fixture::new();
    let cut = f.cut();
    let sort = ordered_key(&[Some(&FieldValue::Integer(1))]).unwrap();
    for blank in ["", " \t\r\n", "\u{2003}\u{a0}"] {
        for family in [
            QueueSourcePostingFamily::Scalar {
                collection: Some(blank),
                field: "n",
            },
            QueueSourcePostingFamily::Named {
                collection: Some(blank),
                index: "i",
            },
            QueueSourcePostingFamily::Scalar {
                collection: None,
                field: blank,
            },
            QueueSourcePostingFamily::Named {
                collection: None,
                index: blank,
            },
        ] {
            let raw = key(family, &sort, "k");
            assert!(
                family
                    .decode(
                        &cut,
                        cut.point_from_source(&raw, Some(&[])),
                        &[FieldType::Integer]
                    )
                    .is_err()
            );
        }
        let raw = QueueSourceKey::LiveKey {
            collection: Some(blank),
            key: "k",
        }
        .encode()
        .unwrap();
        assert!(
            crate::queue_source_index::decode_live_key(
                &cut,
                cut.point_from_source(&raw, Some(&[])),
                Some(blank)
            )
            .is_err()
        );
    }
}
