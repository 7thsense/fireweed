use super::*;
use crate::queue_source_index::{
    QueueSourceKey, live_keys_prefix, live_keys_prefix_len, prefix_successor,
};
fn metadata() -> WorkMetadata {
    WorkMetadata {
        version: 1,
        priority: 0,
        available_at: 0,
        insertion_sequence: 1,
        lifecycle: Lifecycle::Pending,
        eligible_since: 0,
        attempts: 0,
        lease_expires_at: None,
        lease_hash: None,
    }
}
fn key(index: QueueSourceWorkIndex, metadata: WorkMetadata, literal: &str) -> Option<Vec<u8>> {
    index.posting(metadata).unwrap().map(|posting| {
        QueueSourceKey::WorkIntrinsic {
            posting,
            key: literal,
        }
        .encode()
        .unwrap()
    })
}
#[test]
fn static_memberships_include_terminal_schedule_and_attempts_but_not_candidate_orders() {
    for lifecycle in [
        Lifecycle::Pending,
        Lifecycle::Leased,
        Lifecycle::Completed,
        Lifecycle::Failed,
    ] {
        let row = WorkMetadata {
            lifecycle,
            lease_expires_at: (lifecycle == Lifecycle::Leased).then_some(i64::MIN),
            lease_hash: (lifecycle == Lifecycle::Leased).then_some([1; 32]),
            ..metadata()
        };
        for index in QueueSourceWorkIndex::ALL {
            let expected = match index {
                QueueSourceWorkIndex::RawLifecycle
                | QueueSourceWorkIndex::Attempts
                | QueueSourceWorkIndex::Schedule => true,
                QueueSourceWorkIndex::LeaseExpiry => lifecycle == Lifecycle::Leased,
                _ => matches!(lifecycle, Lifecycle::Pending | Lifecycle::Leased),
            };
            assert_eq!(
                key(index, row, "k").is_some(),
                expected,
                "{index:?} {lifecycle:?}"
            );
        }
    }
}
#[test]
fn ordering_matches_independent_metadata_comparison_at_all_numeric_edges() {
    use std::cmp::Ordering;
    let numbers = [i64::MIN, -1, 0, 1, i64::MAX];
    let mut rows = Vec::new();
    for (n, priority) in numbers.into_iter().enumerate() {
        for available_at in numbers {
            for eligible_since in numbers {
                for sequence in [1, i64::MAX as u64 + 1, u64::MAX] {
                    rows.push((
                        format!("k\0α{n}-{available_at}-{eligible_since}-{sequence}"),
                        WorkMetadata {
                            priority,
                            available_at,
                            eligible_since,
                            insertion_sequence: sequence,
                            attempts: [0, 1, u32::MAX][n % 3],
                            ..metadata()
                        },
                    ));
                }
            }
        }
    }
    for index in QueueSourceWorkIndex::ALL {
        if index == QueueSourceWorkIndex::LeaseExpiry {
            continue;
        }
        let compare = |a: &(String, WorkMetadata), b: &(String, WorkMetadata)| {
            let (a_key, a) = a;
            let (b_key, b) = b;
            let primary = match index {
                QueueSourceWorkIndex::RawLifecycle => Ordering::Equal,
                QueueSourceWorkIndex::Attempts => a.attempts.cmp(&b.attempts),
                QueueSourceWorkIndex::PriorityAscending
                | QueueSourceWorkIndex::OverduePriorityAscending => a.priority.cmp(&b.priority),
                QueueSourceWorkIndex::PriorityDescending
                | QueueSourceWorkIndex::OverduePriorityDescending => b.priority.cmp(&a.priority),
                _ => a.available_at.cmp(&b.available_at),
            };
            let overdue = matches!(
                index,
                QueueSourceWorkIndex::OverduePriorityAscending
                    | QueueSourceWorkIndex::OverduePriorityDescending
                    | QueueSourceWorkIndex::OverdueSchedule
            );
            let age = if overdue {
                a.eligible_since.cmp(&b.eligible_since)
            } else {
                Ordering::Equal
            };
            let sequence = if matches!(
                index,
                QueueSourceWorkIndex::RawLifecycle | QueueSourceWorkIndex::Attempts
            ) {
                Ordering::Equal
            } else {
                a.insertion_sequence.cmp(&b.insertion_sequence)
            };
            age.then(primary).then(sequence).then(a_key.cmp(b_key))
        };
        let mut expected = rows.clone();
        expected.sort_by(compare);
        let mut actual = rows.clone();
        actual.sort_by_cached_key(|(literal, row)| key(index, *row, literal).unwrap());
        assert_eq!(actual, expected, "{index:?}");
    }
}
#[test]
fn numeric_prefixes_cover_exact_tie_groups_and_do_not_truncate_unsigned_sequence() {
    for index in QueueSourceWorkIndex::ALL {
        for number in [i64::MIN, -1, 0, 1, i64::MAX] {
            let row = WorkMetadata {
                priority: number,
                available_at: number,
                eligible_since: number,
                lifecycle: Lifecycle::Leased,
                lease_expires_at: Some(number),
                lease_hash: Some([1; 32]),
                ..metadata()
            };
            let value = match index {
                QueueSourceWorkIndex::RawLifecycle => {
                    QueueSourceWorkValue::Lifecycle(Lifecycle::Leased)
                }
                QueueSourceWorkIndex::Attempts => QueueSourceWorkValue::Attempts(0),
                _ => QueueSourceWorkValue::Signed(number),
            };
            let prefix = index.value_prefix(value).unwrap();
            let mut upper = prefix.as_bytes().to_vec();
            assert!(prefix_successor(&mut upper));
            for sequence in [1, i64::MAX as u64 + 1, u64::MAX] {
                for literal in ["a", "a\0", "a\0α", "α", "β"] {
                    let row = WorkMetadata {
                        insertion_sequence: sequence,
                        ..row
                    };
                    let posting = index.posting(row).unwrap().unwrap();
                    let fact = QueueSourceKey::WorkIntrinsic {
                        posting,
                        key: literal,
                    };
                    let encoded = fact.encode().unwrap();
                    assert_eq!(encoded.len(), fact.encoded_len().unwrap());
                    assert!(encoded.starts_with(prefix.as_bytes()));
                    assert!(encoded.as_slice() >= prefix.as_bytes() && encoded < upper);
                }
            }
        }
    }
    assert!(
        QueueSourceWorkIndex::Attempts
            .value_prefix(QueueSourceWorkValue::Signed(0))
            .is_err()
    );
    assert!(
        QueueSourceWorkIndex::Schedule
            .value_prefix(QueueSourceWorkValue::Attempts(0))
            .is_err()
    );
}
#[test]
fn live_namespaces_preserve_full_collection_and_literal_identity() {
    let mut distinct = std::collections::BTreeSet::new();
    for collection in [
        None,
        Some(""),
        Some("a"),
        Some("a\0"),
        Some("ab"),
        Some("α"),
    ] {
        let prefix = live_keys_prefix(collection).unwrap();
        assert_eq!(prefix.len(), live_keys_prefix_len(collection).unwrap());
        let mut upper = prefix.clone();
        assert!(prefix_successor(&mut upper));
        for other in [
            None,
            Some(""),
            Some("a"),
            Some("a\0"),
            Some("ab"),
            Some("α"),
        ] {
            for literal in ["", "a", "a\0", "α"] {
                let key = QueueSourceKey::LiveKey {
                    collection: other,
                    key: literal,
                }
                .encode()
                .unwrap();
                assert_eq!(key >= prefix && key < upper, collection == other);
            }
        }
        for literal in ["a", "a\0", "a\0α", "α"] {
            let encoded = QueueSourceKey::LiveKey {
                collection,
                key: literal,
            }
            .encode()
            .unwrap();
            assert!(encoded.starts_with(&prefix));
            assert!(distinct.insert(encoded));
        }
    }
}

#[test]
fn first_component_prefix_bounds_include_the_whole_equal_group_only() {
    use std::cmp::Ordering;
    for index in QueueSourceWorkIndex::ALL {
        for wanted in 0..5 {
            let numbers = [i64::MIN, -1, 0, 1, i64::MAX];
            let lifecycle = [
                Lifecycle::Pending,
                Lifecycle::Leased,
                Lifecycle::Completed,
                Lifecycle::Failed,
                Lifecycle::Failed,
            ];
            let attempts = [0, 1, 2, u32::MAX - 1, u32::MAX];
            let value = match index {
                QueueSourceWorkIndex::RawLifecycle => {
                    QueueSourceWorkValue::Lifecycle(lifecycle[wanted])
                }
                QueueSourceWorkIndex::Attempts => QueueSourceWorkValue::Attempts(attempts[wanted]),
                _ => QueueSourceWorkValue::Signed(numbers[wanted]),
            };
            let lower = index.value_prefix(value).unwrap();
            let mut upper = lower.as_bytes().to_vec();
            assert!(prefix_successor(&mut upper));
            for actual in 0..5 {
                let raw = if index == QueueSourceWorkIndex::RawLifecycle {
                    lifecycle[actual]
                } else {
                    Lifecycle::Leased
                };
                let row = WorkMetadata {
                    priority: numbers[actual],
                    available_at: numbers[actual],
                    eligible_since: numbers[actual],
                    attempts: attempts[actual],
                    lifecycle: raw,
                    lease_expires_at: (raw == Lifecycle::Leased).then_some(numbers[actual]),
                    lease_hash: (raw == Lifecycle::Leased).then_some([1; 32]),
                    ..metadata()
                };
                let mut order = match index {
                    QueueSourceWorkIndex::RawLifecycle => {
                        lifecycle_tag(lifecycle[actual]).cmp(&lifecycle_tag(lifecycle[wanted]))
                    }
                    QueueSourceWorkIndex::Attempts => attempts[actual].cmp(&attempts[wanted]),
                    _ => numbers[actual].cmp(&numbers[wanted]),
                };
                if index == QueueSourceWorkIndex::PriorityDescending {
                    order = order.reverse();
                }
                for literal in ["a\0α", "β"] {
                    let encoded = key(index, row, literal).unwrap();
                    assert_eq!(
                        encoded.as_slice() < lower.as_bytes(),
                        order == Ordering::Less
                    );
                    assert_eq!(encoded >= upper, order == Ordering::Greater);
                    assert_eq!(
                        encoded.starts_with(lower.as_bytes()),
                        order == Ordering::Equal
                    );
                }
            }
        }
    }
}
#[test]
fn metadata_changes_rewrite_only_their_actual_ordering_components() {
    use QueueSourceWorkIndex::*;
    let old = WorkMetadata {
        lifecycle: Lifecycle::Leased,
        lease_expires_at: Some(7),
        lease_hash: Some([1; 32]),
        ..metadata()
    };
    for (new, changed) in [
        (
            WorkMetadata {
                version: 2,
                lease_hash: Some([2; 32]),
                ..old
            },
            vec![],
        ),
        (
            WorkMetadata {
                lease_expires_at: Some(8),
                ..old
            },
            vec![LeaseExpiry],
        ),
        (WorkMetadata { attempts: 1, ..old }, vec![Attempts]),
        (
            WorkMetadata { priority: 1, ..old },
            vec![
                PriorityAscending,
                PriorityDescending,
                OverduePriorityAscending,
                OverduePriorityDescending,
            ],
        ),
        (
            WorkMetadata {
                available_at: 1,
                ..old
            },
            vec![Schedule, OverdueSchedule],
        ),
        (
            WorkMetadata {
                eligible_since: 1,
                ..old
            },
            vec![
                OverduePriorityAscending,
                OverduePriorityDescending,
                OverdueSchedule,
            ],
        ),
    ] {
        for index in QueueSourceWorkIndex::ALL {
            assert_eq!(
                index.posting(old).unwrap() != index.posting(new).unwrap(),
                changed.contains(&index),
                "{index:?}"
            );
        }
    }
    for invalid in [
        WorkMetadata {
            lifecycle: Lifecycle::Leased,
            lease_expires_at: None,
            lease_hash: None,
            ..old
        },
        WorkMetadata {
            lifecycle: Lifecycle::Pending,
            lease_expires_at: Some(1),
            lease_hash: Some([1; 32]),
            ..old
        },
        WorkMetadata { version: 0, ..old },
        WorkMetadata {
            insertion_sequence: 0,
            ..old
        },
        WorkMetadata {
            lease_expires_at: None,
            ..old
        },
        WorkMetadata {
            lease_hash: None,
            ..old
        },
    ] {
        for index in QueueSourceWorkIndex::ALL {
            assert!(index.posting(invalid).is_err());
        }
    }
}
