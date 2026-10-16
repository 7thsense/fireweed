use super::*;
use crate::{
    queue_admission::QueueAdmission, queue_schema_codec::encode_definition,
    queue_source_index::QueueSourceKey,
};
use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema, compare_fields};
use std::collections::{BTreeMap, BTreeSet};
fn fixture(
    capacity: u64,
) -> (
    Arc<QueueAdmission>,
    Arc<QueueReservation>,
    AdmittedQueueDefinition,
) {
    let root = QueueAdmission::new(2, capacity).unwrap();
    let job = Arc::new(root.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: BTreeMap::from([("n".into(), FieldType::Integer)]),
        indexes: BTreeMap::new(),
        unique_indexes: BTreeSet::new(),
        additional_field_prefixes: BTreeSet::from(["bag/".into()]),
    };
    let schema = QueueSchema {
        work: collection.clone(),
        state: BTreeMap::from([("state\0é".into(), collection)]),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
    (root, job, definition)
}
fn values() -> Vec<FieldValue> {
    vec![
        FieldValue::Integer(i64::MIN),
        FieldValue::Integer(0),
        FieldValue::Integer(i64::MAX),
        FieldValue::Float(-1.0),
        FieldValue::Float(-0.0),
        FieldValue::Float(0.0),
        FieldValue::Float(1.0),
        FieldValue::Boolean(false),
        FieldValue::Boolean(true),
        FieldValue::Text("".into()),
        FieldValue::Text("a\0".into()),
        FieldValue::Text("é🦊".into()),
        FieldValue::Bytes(vec![]),
        FieldValue::Bytes(vec![0, 255]),
        FieldValue::Bytes(vec![255]),
    ]
}
fn contains(plan: &QueueSourceScalarRanges<'_>, raw: &[u8]) -> bool {
    plan.ranges()
        .iter()
        .any(|range| range.lower() <= raw && raw < range.upper())
}
#[test]
fn compiled_scalar_ranges_match_common_predicates_for_all_types_ties_and_open_bounds() {
    let (root, job, definition) = fixture(8 << 20);
    let baseline = root.usage().unwrap();
    let values = values();
    let mut predicates: Vec<_> = values.iter().cloned().map(Predicate::Equal).collect();
    predicates.push(Predicate::In(values.clone()));
    for a in &values {
        for b in &values {
            if matches!(
                compare_fields(a, b),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) {
                for lower_included in [false, true] {
                    for upper_included in [false, true] {
                        predicates.push(Predicate::Range {
                            lower: Some(if lower_included {
                                Bound::Included(a.clone())
                            } else {
                                Bound::Excluded(a.clone())
                            }),
                            upper: Some(if upper_included {
                                Bound::Included(b.clone())
                            } else {
                                Bound::Excluded(b.clone())
                            }),
                        });
                    }
                }
            }
        }
        for included in [false, true] {
            let bound = if included {
                Bound::Included(a.clone())
            } else {
                Bound::Excluded(a.clone())
            };
            predicates.push(Predicate::Range {
                lower: Some(bound.clone()),
                upper: None,
            });
            predicates.push(Predicate::Range {
                lower: None,
                upper: Some(bound),
            });
        }
    }
    for collection in [None, Some("state\0é")] {
        for predicate in &predicates {
            let plan = plan_source_scalar_ranges(&definition, collection, "bag/v", predicate, &job)
                .unwrap();
            assert_eq!(plan.definition_pin(), definition.pin());
            assert!(plan.belongs_to(&job));
            for value in &values {
                let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
                for key in ["k", "k\0", "é🦊"] {
                    let raw = QueueSourceKey::ScalarPosting {
                        collection,
                        field: "bag/v",
                        sort: &sort,
                        key,
                    }
                    .encode()
                    .unwrap();
                    for range in plan.ranges() {
                        if range.lower() <= raw.as_slice() && raw.as_slice() < range.upper() {
                            assert_eq!(range.field_type(), value.field_type());
                        }
                    }
                    assert_eq!(
                        contains(&plan, &raw),
                        predicate.matches(Some(value)),
                        "{collection:?} {predicate:?} {value:?}"
                    );
                    for field in ["bag/", "bag/v\0", "bag/vx"] {
                        let foreign = QueueSourceKey::ScalarPosting {
                            collection,
                            field,
                            sort: &sort,
                            key,
                        }
                        .encode()
                        .unwrap();
                        assert!(!contains(&plan, &foreign));
                    }
                }
            }
            let missing_sort = crate::queue_index::ordered_key(&[None]).unwrap();
            let missing = QueueSourceKey::ScalarPosting {
                collection,
                field: "bag/v",
                sort: &missing_sort,
                key: "k",
            }
            .encode()
            .unwrap();
            assert!(!contains(&plan, &missing));
            assert!(!predicate.matches(None));
        }
    }
    assert_eq!(root.usage().unwrap(), baseline);
}
#[test]
fn typed_scalar_prefixes_use_partial_outer_literals_and_measured_lengths() {
    let values = values();
    for kind in [
        FieldType::Integer,
        FieldType::Float,
        FieldType::Boolean,
        FieldType::Text,
        FieldType::Bytes,
    ] {
        let family = QueueSourcePostingFamily::Scalar {
            collection: Some("s\0"),
            field: "f\0é",
        };
        let lower = family.scalar_type_prefix(kind).unwrap();
        assert_eq!(lower.len(), family.scalar_type_prefix_len(kind).unwrap());
        let mut upper = lower.clone();
        assert!(prefix_successor(&mut upper));
        for value in &values {
            let sort = crate::queue_index::ordered_key(&[Some(value)]).unwrap();
            let raw = QueueSourceKey::ScalarPosting {
                collection: Some("s\0"),
                field: "f\0é",
                sort: &sort,
                key: "k\0",
            }
            .encode()
            .unwrap();
            assert_eq!(lower <= raw && raw < upper, kind == value.field_type());
        }
        let named = QueueSourcePostingFamily::Named {
            collection: None,
            index: "i",
        };
        assert!(named.scalar_type_prefix(kind).is_err());
        assert!(named.scalar_type_prefix_len(kind).is_err());
    }
}
#[test]
fn membership_deduplicates_canonical_values_and_equal_exclusive_bounds_are_empty() {
    let (_, job, definition) = fixture(1 << 20);
    let predicate = Predicate::In(vec![
        FieldValue::Float(-0.0),
        FieldValue::Integer(0),
        FieldValue::Text("0".into()),
        FieldValue::Float(0.0),
        FieldValue::Integer(0),
        FieldValue::Text("0".into()),
    ]);
    let plan = plan_source_scalar_ranges(&definition, None, "bag/v", &predicate, &job).unwrap();
    assert_eq!(plan.ranges().len(), 3);
    for (lower, upper) in [
        (
            Bound::Excluded(FieldValue::Integer(1)),
            Bound::Included(FieldValue::Integer(1)),
        ),
        (
            Bound::Included(FieldValue::Integer(1)),
            Bound::Excluded(FieldValue::Integer(1)),
        ),
    ] {
        let plan = plan_source_scalar_ranges(
            &definition,
            None,
            "n",
            &Predicate::Range {
                lower: Some(lower),
                upper: Some(upper),
            },
            &job,
        )
        .unwrap();
        assert!(plan.ranges().is_empty());
        assert!(plan.belongs_to(&job));
    }
}
#[test]
fn invalid_declarations_and_foreign_admission_fail_without_retaining_plan_memory() {
    let (root, job, definition) = fixture(1 << 20);
    let other = Arc::new(root.reserve(0).unwrap());
    let baseline = root.usage().unwrap();
    for predicate in [
        Predicate::In(vec![]),
        Predicate::Equal(FieldValue::Float(f64::NAN)),
        Predicate::Equal(FieldValue::Text("wrong".into())),
        Predicate::Range {
            lower: None,
            upper: None,
        },
        Predicate::Range {
            lower: Some(Bound::Included(FieldValue::Integer(2))),
            upper: Some(Bound::Included(FieldValue::Integer(1))),
        },
        Predicate::Range {
            lower: Some(Bound::Included(FieldValue::Integer(0))),
            upper: Some(Bound::Included(FieldValue::Float(1.0))),
        },
    ] {
        assert!(plan_source_scalar_ranges(&definition, None, "n", &predicate, &job).is_err());
        assert_eq!(root.usage().unwrap(), baseline);
    }
    assert!(
        plan_source_scalar_ranges(
            &definition,
            None,
            "bag/v",
            &Predicate::Range {
                lower: Some(Bound::Included(FieldValue::Integer(0))),
                upper: Some(Bound::Included(FieldValue::Float(1.0))),
            },
            &job
        )
        .is_err()
    );
    let predicate = Predicate::Equal(FieldValue::Integer(1));
    assert!(plan_source_scalar_ranges(&definition, None, "n", &predicate, &other).is_err());
    assert!(
        plan_source_scalar_ranges(&definition, Some("foreign"), "n", &predicate, &job).is_err()
    );
    assert!(plan_source_scalar_ranges(&definition, None, "foreign", &predicate, &job).is_err());
    assert_eq!(root.usage().unwrap(), baseline);
}
#[test]
fn plan_owns_exact_root_admission_until_drop_and_one_less_fails_cleanly() {
    let predicate = Predicate::Equal(FieldValue::Integer(1));
    let (root, job, definition) = fixture(1 << 20);
    let baseline = root.usage().unwrap();
    let plan = plan_source_scalar_ranges(&definition, None, "n", &predicate, &job).unwrap();
    let needed = root.usage().unwrap().1;
    drop(plan);
    assert_eq!(root.usage().unwrap(), baseline);
    for (capacity, success) in [(needed, true), (needed - 1, false)] {
        let (root, job, definition) = fixture(capacity);
        let baseline = root.usage().unwrap();
        let result = plan_source_scalar_ranges(&definition, None, "n", &predicate, &job);
        assert_eq!(result.is_ok(), success);
        if success {
            assert_eq!(root.usage().unwrap().1, capacity);
            assert!(job.owned_buffer(1).is_err());
        }
        drop(result);
        assert_eq!(root.usage().unwrap(), baseline);
        drop(definition);
        drop(job);
        assert_eq!(root.usage().unwrap(), (0, 0));
    }
}

#[test]
fn long_namespace_two_sided_bounds_fit_owned_admission_and_fail_one_less_cleanly() {
    let field = format!("bag/{}", "\0é".repeat(8192));
    let predicate = Predicate::Range {
        lower: Some(Bound::Excluded(FieldValue::Integer(-1))),
        upper: Some(Bound::Included(FieldValue::Integer(1))),
    };
    let (root, job, definition) = fixture(8 << 20);
    let baseline = root.usage().unwrap();
    let plan =
        plan_source_scalar_ranges(&definition, Some("state\0é"), &field, &predicate, &job).unwrap();
    assert_eq!(plan.ranges().len(), 1);
    let range = &plan.ranges()[0];
    // Actual retained Vec capacities, plus the temporary complete value codec.
    // No allocator/RSS theorem is inferred from logical reservation accounting.
    let temporary = crate::queue_index::ordered_key(&[Some(&FieldValue::Integer(1))]).unwrap();
    assert!(
        plan._charge.admitted_bytes()
            >= (range.lower.capacity() + range.upper.capacity() + temporary.capacity()) as u64
    );
    let needed = root.usage().unwrap().1;
    for value in [-1, 0, 1, 2] {
        let value = FieldValue::Integer(value);
        let sort = crate::queue_index::ordered_key(&[Some(&value)]).unwrap();
        let raw = QueueSourceKey::ScalarPosting {
            collection: Some("state\0é"),
            field: &field,
            sort: &sort,
            key: "k",
        }
        .encode()
        .unwrap();
        assert_eq!(contains(&plan, &raw), predicate.matches(Some(&value)));
    }
    drop(plan);
    assert_eq!(root.usage().unwrap(), baseline);
    for (capacity, success) in [(needed, true), (needed - 1, false)] {
        let (root, job, definition) = fixture(capacity);
        let baseline = root.usage().unwrap();
        let result =
            plan_source_scalar_ranges(&definition, Some("state\0é"), &field, &predicate, &job);
        assert_eq!(result.is_ok(), success);
        if success {
            assert_eq!(root.usage().unwrap().1, capacity);
            assert!(job.owned_buffer(1).is_err());
        }
        drop(result);
        assert_eq!(root.usage().unwrap(), baseline);
    }
}
