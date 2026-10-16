//! Administrative hydration reuses exact original-cut row/body authority.
use super::*;
use fireweed_core::queue_administration::{
    AdministrativeAction, AdministrativeEntry, SelectorNode, WorkSelector,
};
fn action(body: Option<Vec<u8>>) -> AdministrativeAction {
    AdministrativeAction::Update {
        patch: WorkPatch {
            body: body.map(Into::into),
            ..Default::default()
        },
        lifecycle: AdministrativeLifecycle::Keep,
        lease_guard: AdministrativeLeaseGuard::RejectActive,
        remove_field_prefixes: BTreeSet::new(),
    }
}
#[test]
fn administrative_rows_preserve_positions_absence_tombstones_and_one_header() {
    futures::executor::block_on(async {
        let mut f = fixture();
        let mut delete = group("delete-b");
        delete.work_updates.push(UnclaimedUpdate {
            key: "b".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Delete,
        });
        append(&mut f, delete);
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let baseline = f.admission.usage().unwrap();
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let action = action(None);
        let mut inputs = Vec::new();
        for (position, key) in ["a", "a", "b", "missing"].into_iter().enumerate() {
            let entry = AdministrativeEntry {
                key: key.into(),
                expected_version: None,
                precondition: WorkSelector::leaf(SelectorNode::StoredLifecycleIn(vec![
                    Lifecycle::Pending,
                ])),
                action: action.clone(),
            };
            inputs.push(
                source
                    .load_addressed_row(position, &entry, None)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(reader.header_reads.load(Ordering::SeqCst), 2);
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        drop(source);
        for (position, input) in inputs.iter().enumerate() {
            let crate::queue_administration::AddressedAdministrativeInput::OwnedAdmittedOneRow {
                position: actual,
                key,
                image,
                hydration,
            } = input
            else {
                panic!()
            };
            assert_eq!(*actual, position);
            assert!(!hydration.is_empty());
            let identity = RecordIdentity {
                collection: None,
                key: key.clone(),
            };
            if position < 2 {
                assert_eq!(key, "a");
                assert!(image.work.contains_key("a"));
                assert!(!image.work["a"].body_loaded);
                assert!(image.work["a"].record.work.fields.is_empty());
                assert_eq!(image.versions.get(&identity), Some(&1));
            } else {
                assert!(image.work.is_empty());
                assert_eq!(
                    image.versions.get(&identity),
                    if position == 2 { Some(&2) } else { None }
                );
            }
        }
        drop(inputs);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn administrative_body_reads_are_conditional_and_independently_funded() {
    futures::executor::block_on(async {
        for (old, replacement) in [
            (vec![1, 2], None),
            (vec![1, 2], Some(vec![])),
            (vec![1, 2], Some(vec![1, 2])),
            (vec![1, 2], Some(vec![9])),
            (vec![], None),
            (vec![], Some(vec![])),
            (vec![], Some(vec![9])),
        ] {
            let mut item = work("a", "a");
            item.body = old.clone().into();
            let f = fixture_with_work(vec![item]);
            let reader = BodyReader {
                fixture: &f,
                reads: AtomicUsize::new(0),
                header_reads: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
                fail_at: usize::MAX,
            };
            let baseline = f.admission.usage().unwrap();
            let source = open_administrative_source(
                &reader,
                reader.cut().unwrap(),
                &f.queue,
                &f.definition,
                &f.job,
            )
            .await
            .unwrap();
            let needs_body = replacement.is_some();
            let action = action(replacement);
            let before = BTreeSet::from(["u".into()]);
            let predicate = WorkSelector::leaf(SelectorNode::Present {
                name: "n".into(),
                present: true,
            });
            let row = source
                .load_row(QueueSourceAdministrativeRead {
                    key: "a",
                    action: &action,
                    before_fields: Some(&before),
                    selectors: &[&predicate],
                })
                .await
                .unwrap();
            let work = &row.image().work["a"];
            assert_eq!(work.body_loaded, needs_body);
            assert_eq!(reader.reads.load(Ordering::SeqCst), usize::from(needs_body));
            assert_eq!(
                work.record.work.fields,
                f.snapshot.work["a"].record.work.fields
            );
            if needs_body {
                assert_eq!(work.record.work.body.as_ref(), old.as_slice());
            }
            let input = row.into_addressed(0);
            drop(source);
            assert!(f.admission.usage().unwrap().1 > baseline.1);
            drop(input);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn prefix_only_edits_load_composite_peers_and_release_owner_authority() {
    futures::executor::block_on(async {
        let mut item = work("a", "a");
        item.fields
            .insert("/payload/x".into(), FieldValue::Integer(7));
        item.fields
            .insert("/payload/drop".into(), FieldValue::Bytes(vec![9; 1024]));
        let f = fixture_with_work_schema(vec![item], |schema| {
            schema
                .fields
                .insert("/payload/x".into(), FieldType::Integer);
            schema.additional_field_prefixes.insert("/payload/".into());
            schema
                .indexes
                .insert("payload".into(), vec!["/payload/x".into(), "n".into()]);
            schema.unique_indexes.insert("payload".into());
        });
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 1,
        };
        let baseline = f.admission.usage().unwrap();
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let action = AdministrativeAction::Update {
            patch: WorkPatch::default(),
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
            remove_field_prefixes: BTreeSet::from(["/payload/".into()]),
        };
        let row = source
            .load_row(QueueSourceAdministrativeRead {
                key: "a",
                action: &action,
                before_fields: None,
                selectors: &[],
            })
            .await
            .unwrap();
        let image = row.image();
        let fields = crate::queue_fields::CheckedFields::new(
            &image.work["a"].record.work.fields,
            &image.work["a"].field_coverage,
        )
        .unwrap();
        assert!(fields.get("n").unwrap().is_some());
        assert!(fields.get("u").is_err());
        assert_eq!(fields.prefix("/payload/").unwrap().count(), 2);
        assert_eq!(image.unique_probes_loaded.len(), 1);
        assert_eq!(image.unique_owners.len(), 1);
        assert!(image.unique_owners.values().all(|owner| owner.key == "a"));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
        drop(row);
        drop(source);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}

#[test]
fn later_row_body_failure_does_not_invalidate_prior_owned_input() {
    futures::executor::block_on(async {
        let f = fixture();
        let reader = BodyReader {
            fixture: &f,
            reads: AtomicUsize::new(0),
            header_reads: AtomicUsize::new(0),
            lookups: AtomicUsize::new(0),
            fail_at: 2,
        };
        let baseline = f.admission.usage().unwrap();
        let source = open_administrative_source(
            &reader,
            reader.cut().unwrap(),
            &f.queue,
            &f.definition,
            &f.job,
        )
        .await
        .unwrap();
        let action = action(Some(vec![9]));
        let first = source
            .load_row(QueueSourceAdministrativeRead {
                key: "a",
                action: &action,
                before_fields: None,
                selectors: &[],
            })
            .await
            .unwrap()
            .into_addressed(0);
        let before_failure = f.admission.usage().unwrap();
        let error = source
            .load_row(QueueSourceAdministrativeRead {
                key: "b",
                action: &action,
                before_fields: None,
                selectors: &[],
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(error, QueueError::TooLarge));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
        assert_eq!(f.admission.usage().unwrap(), before_failure);
        let crate::queue_administration::AddressedAdministrativeInput::OwnedAdmittedOneRow {
            image,
            position,
            key,
            ..
        } = &first
        else {
            panic!()
        };
        assert_eq!(*position, 0);
        assert_eq!(key, "a");
        assert!(image.work["a"].body_loaded);
        assert_eq!(image.work["a"].record.work.body.as_ref(), &[1, 2]);
        drop(source);
        drop(first);
        assert_eq!(f.admission.usage().unwrap(), baseline);
    });
}
#[test]
fn claim_and_release_owner_evidence_precedes_rejectable_predicate() {
    futures::executor::block_on(async {
        for corrupt in [false, true] {
            let mut f = fixture();
            if corrupt {
                let value = FieldValue::Text("new".into());
                let sort = crate::queue_index::ordered_key(&[Some(&value)]).unwrap();
                f.reader.facts.insert(
                    QueueSourceKey::UniqueOwner {
                        collection: None,
                        index: "u",
                        sort: &sort,
                    }
                    .encode()
                    .unwrap(),
                    vec![255],
                );
            }
            let reader = BodyReader {
                fixture: &f,
                reads: AtomicUsize::new(0),
                header_reads: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
                fail_at: 1,
            };
            let baseline = f.admission.usage().unwrap();
            let source = open_administrative_source(
                &reader,
                reader.cut().unwrap(),
                &f.queue,
                &f.definition,
                &f.job,
            )
            .await
            .unwrap();
            let action = AdministrativeAction::Update {
                patch: WorkPatch {
                    fields: BTreeMap::from([("u".into(), Some(FieldValue::Text("new".into())))]),
                    ..Default::default()
                },
                lifecycle: AdministrativeLifecycle::Keep,
                lease_guard: AdministrativeLeaseGuard::RejectActive,
                remove_field_prefixes: BTreeSet::new(),
            };
            let predicate = WorkSelector::leaf(SelectorNode::Present {
                name: "n".into(),
                present: false,
            });
            let result = source
                .load_row(QueueSourceAdministrativeRead {
                    key: "a",
                    action: &action,
                    before_fields: None,
                    selectors: &[&predicate],
                })
                .await;
            if corrupt {
                assert!(
                    matches!(result, Err(QueueError::OutcomeUnknown(message)) if message == "invalid source unique owner")
                );
            } else {
                let row = result.unwrap();
                assert_eq!(row.image().unique_probes_loaded.len(), 2);
                assert_eq!(row.image().unique_owners.len(), 1);
                assert!(row.image().work["a"].record.work.fields.contains_key("n"));
                assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
                drop(row);
            }
            drop(source);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}

#[test]
fn original_header_rejects_noncanonical_and_genesis_only_sequence() {
    futures::executor::block_on(async {
        for genesis_only in [false, true] {
            let mut f = fixture();
            if genesis_only {
                f.reader.through = 1;
            } else {
                f.reader
                    .facts
                    .insert(QueueSourceKey::Sequence.encode().unwrap(), vec![0x80, 0]);
            }
            let reader = BodyReader {
                fixture: &f,
                reads: AtomicUsize::new(0),
                header_reads: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
                fail_at: 1,
            };
            let baseline = f.admission.usage().unwrap();
            let result = open_administrative_source(
                &reader,
                reader.cut().unwrap(),
                &f.queue,
                &f.definition,
                &f.job,
            )
            .await;
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(message)) if message == "source header: invalid canonical sequence or genesis-only sequence")
            );
            assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
#[test]
fn scanner_handoff_preserves_conflict_precedence_over_missing_release_owner() {
    use fireweed_core::queue_administration::{
        AdministrativeIntent, AdministrativeOperation, AdministrativeOutcome,
    };
    futures::executor::block_on(async {
        for stale in [true, false] {
            let mut f = fixture();
            let value = FieldValue::Text("a".into());
            let sort = crate::queue_index::ordered_key(&[Some(&value)]).unwrap();
            assert!(
                f.reader
                    .facts
                    .remove(
                        &QueueSourceKey::UniqueOwner {
                            collection: None,
                            index: "u",
                            sort: &sort
                        }
                        .encode()
                        .unwrap()
                    )
                    .is_some()
            );
            let reader = BodyReader {
                fixture: &f,
                reads: AtomicUsize::new(0),
                header_reads: AtomicUsize::new(0),
                lookups: AtomicUsize::new(0),
                fail_at: 1,
            };
            let baseline = f.admission.usage().unwrap();
            let source = open_administrative_source(
                &reader,
                reader.cut().unwrap(),
                &f.queue,
                &f.definition,
                &f.job,
            )
            .await
            .unwrap();
            let entry = AdministrativeEntry {
                key: "a".into(),
                expected_version: Some(if stale { 99 } else { 1 }),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Delete {
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            };
            let intent = AdministrativeIntent {
                evaluated_at: 0,
                before_fields: None,
                operation: AdministrativeOperation::Addressed(vec![entry]),
            };
            let AdministrativeOperation::Addressed(entries) = &intent.operation else {
                panic!()
            };
            let input = source
                .load_addressed_row(0, &entries[0], intent.before_fields.as_ref())
                .await
                .unwrap();
            let ordinary = group("handoff");
            let ordinary_image = RecordSnapshot {
                next_sequence: f.snapshot.next_sequence,
                ..Default::default()
            };
            let scanned = crate::queue_administration::scan_addressed_group_to_sink_bounded(
                &ordinary,
                &intent,
                futures::stream::iter(vec![Ok(input)]),
                &ordinary_image,
                f.definition.schema(),
                &QueueLimits::default(),
                Arc::clone(&f.job),
                u64::MAX,
                |_| std::future::ready(Ok(())),
            )
            .await;
            if stale {
                let scan = scanned.unwrap();
                assert!(matches!(
                    scan.result().unwrap().items[0].outcome,
                    AdministrativeOutcome::Conflict { actual_version: 1 }
                ));
                drop(scan);
            } else {
                assert!(
                    matches!(scanned, Err(QueueError::OutcomeUnknown(message)) if message == "released unique tuple lacks initial owner authority")
                );
            }
            drop(source);
            assert_eq!(f.admission.usage().unwrap(), baseline);
        }
    });
}
