use super::super::tests::{bootstrap, execute_for_test, group, inspect, seed};
use super::tests::input;
use super::*;
use fireweed_conformance::queue_contract::TestClock;
use fireweed_core::queue_administration::{
    AdministrativeEntry, AdministrativeIntent, AdministrativeOutcome, WorkSelector,
};
use fireweed_engine::queue_admission::QueueAdmission;
fn request() -> CommitGroup {
    let mut request = group("preview-original");
    request.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 20,
            before_fields: Some(["u".into()].into()),
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: None,
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(5),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        },
    });
    request
}
#[tokio::test]
async fn preview_bypasses_original_conflicting_expired_and_malformed_receipts_without_effects() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("preview.db"), &job, &workers).await;
    seed(&projection, &definition, &job, &workers, true).await;
    let clock = Arc::new(TestClock::default());
    let original = request();
    let preview = projection
        .preview_administration(
            input(
                &definition,
                &job,
                original.clone(),
                directory.path(),
                2,
                2,
                &workers,
            )
            .await,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    let PreviewOutcome::AdministrativeCommitted { administration, .. } = &preview.result().outcome
    else {
        panic!("not administrative")
    };
    assert_eq!(
        administration.items[0].outcome,
        AdministrativeOutcome::Changed { version: 2 }
    );
    drop(preview);
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_requests WHERE domain=2",
            vec![],
            1
        )
        .await[0],
        Value::Integer(0)
    );
    let prepared = projection
        .prepare_administration(
            input(
                &definition,
                &job,
                original.clone(),
                directory.path(),
                2,
                2,
                &workers,
            )
            .await,
            1,
            16 << 20,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap()
        .unwrap();
    let (record, response, head) = prepared.into_parts();
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    drop(response);
    let preview = projection
        .preview_administration(
            input(
                &definition,
                &job,
                original.clone(),
                directory.path(),
                3,
                2,
                &workers,
            )
            .await,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    let PreviewOutcome::AdministrativeCommitted { administration, .. } = &preview.result().outcome
    else {
        panic!("not administrative")
    };
    assert_eq!(
        administration.items[0].outcome,
        AdministrativeOutcome::NoChange
    );
    assert_eq!(administration.items[0].before.as_ref().unwrap().version, 2);
    drop(preview);
    let mut changed = original.clone();
    if let AdministrativeOperation::Addressed(entries) =
        &mut changed.administration.as_mut().unwrap().intent.operation
        && let AdministrativeAction::Update { patch, .. } = &mut entries[0].action
    {
        patch.priority = Some(6);
    }
    assert!(matches!(
        projection
            .prepare_administration(
                input(
                    &definition,
                    &job,
                    changed.clone(),
                    directory.path(),
                    3,
                    2,
                    &workers
                )
                .await,
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers
            )
            .await,
        Err(QueueError::RequestConflict)
    ));
    for state in 0..3 {
        if state == 1 {
            execute_for_test(
                &projection,
                "UPDATE q7_requests SET outcome=x'ff',fingerprint=x'' WHERE domain=2",
                vec![],
            )
            .await;
        } else if state == 2 {
            execute_for_test(
                &projection,
                "UPDATE q7_requests SET expired=1 WHERE domain=2",
                vec![],
            )
            .await;
        }
        let preview = projection
            .preview_administration(
                input(
                    &definition,
                    &job,
                    changed.clone(),
                    directory.path(),
                    3,
                    2,
                    &workers,
                )
                .await,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        let PreviewOutcome::AdministrativeCommitted { administration, .. } =
            &preview.result().outcome
        else {
            panic!("not administrative")
        };
        assert_eq!(
            administration.items[0].outcome,
            AdministrativeOutcome::Changed { version: 3 }
        );
        drop(preview);
    }
    let mut large = original;
    large.request_id = "preview-overflow".into();
    large.administration.as_mut().unwrap().max_result_bytes = 1024;
    large.administration.as_mut().unwrap().intent.before_fields = Some(["opaque".into()].into());
    let preview = projection
        .preview_administration(
            input(
                &definition,
                &job,
                large.clone(),
                directory.path(),
                3,
                2,
                &workers,
            )
            .await,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    assert_eq!(
        preview.result().outcome,
        PreviewOutcome::Rejected(Rejection::TooLarge)
    );
    drop(preview);
    let producer = projection
        .prepare_administration(
            input(&definition, &job, large, directory.path(), 3, 2, &workers).await,
            1,
            16 << 20,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        producer.receipt().outcome,
        GroupOutcome::Rejected(Rejection::TooLarge)
    );
    drop(producer);
    assert_eq!(inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=2),(SELECT version FROM q7_records WHERE key=?1),(SELECT priority FROM q7_records WHERE key=?1) FROM q7_queues",vec![blob(b"a")],4).await,
        vec![Value::Integer(3),Value::Integer(1),blob(&2u64.to_be_bytes()),Value::Integer(5)]);
    let mut invalid = changed;
    invalid.administration.as_mut().unwrap().max_result_bytes = 1;
    let invalid = input(&definition, &job, invalid, directory.path(), 3, 2, &workers).await;
    workers.close_and_drain().await.unwrap();
    assert!(matches!(
        projection
            .preview_administration(invalid, clock, &job, &workers)
            .await,
        Err(QueueError::TooLarge)
    ));
    drop(definition);
    drop(projection);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn broad_native_leaf_and_boolean_plans_preserve_first_ownership_without_effects() {
    use fireweed_core::queue_administration::{AdministrativeClause, Comparison, SelectorNode};
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("broad.db"), &job, &workers).await;
    seed(&projection, &definition, &job, &workers, true).await;
    let clock = Arc::new(TestClock::default());
    clock.set(1000);
    let action = match request().administration.unwrap().intent.operation {
        AdministrativeOperation::Addressed(mut entries) => entries.remove(0).action,
        _ => unreachable!(),
    };
    let mut cases = vec![
        (WorkSelector::all(vec![]), 2),
        (WorkSelector::any(vec![]), 0),
        (
            WorkSelector::leaf(SelectorNode::Keys(["a".into()].into())),
            1,
        ),
        (
            WorkSelector::leaf(SelectorNode::Field {
                name: "u".into(),
                predicate: Predicate::Equal(FieldValue::Integer(1)),
            }),
            1,
        ),
        (
            WorkSelector::leaf(SelectorNode::Field {
                name: "u".into(),
                predicate: Predicate::In(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
            }),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::Field {
                name: "u".into(),
                predicate: Predicate::Range {
                    lower: Some(Bound::Included(FieldValue::Integer(1))),
                    upper: Some(Bound::Excluded(FieldValue::Integer(2))),
                },
            }),
            1,
        ),
        (
            WorkSelector::leaf(SelectorNode::Present {
                name: "opaque".into(),
                present: true,
            }),
            1,
        ),
        (
            WorkSelector::leaf(SelectorNode::Present {
                name: "opaque".into(),
                present: false,
            }),
            1,
        ),
        (
            WorkSelector::leaf(SelectorNode::StoredLifecycleIn(vec![Lifecycle::Pending])),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::EffectiveLifecycleIn(vec![Lifecycle::Pending])),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::Attempts {
                comparison: Comparison::Equal,
                value: 0,
            }),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::AvailableAt {
                comparison: Comparison::BeforeOrEqual,
                value: 0,
            }),
            2,
        ),
        (WorkSelector::leaf(SelectorNode::LeaseActive(false)), 2),
        (WorkSelector::leaf(SelectorNode::LeaseActive(true)), 0),
        (
            WorkSelector::negate(WorkSelector::leaf(SelectorNode::Field {
                name: "opaque".into(),
                predicate: Predicate::Equal(FieldValue::Bytes(vec![1])),
            })),
            2,
        ),
        (
            WorkSelector::all(vec![
                WorkSelector::leaf(SelectorNode::Keys(["a".into()].into())),
                WorkSelector::leaf(SelectorNode::LeaseActive(false)),
            ]),
            1,
        ),
        (
            WorkSelector::any(vec![
                WorkSelector::leaf(SelectorNode::Keys(["a".into()].into())),
                WorkSelector::leaf(SelectorNode::Keys(["b".into()].into())),
            ]),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::Present {
                name: "meta/missing".into(),
                present: false,
            }),
            2,
        ),
        (
            WorkSelector::leaf(SelectorNode::Field {
                name: "meta/missing".into(),
                predicate: Predicate::Equal(FieldValue::Text("none".into())),
            }),
            0,
        ),
    ];
    cases.push((
        WorkSelector::any(
            (0..5001)
                .map(|_| WorkSelector::leaf(SelectorNode::Keys(["a".into()].into())))
                .collect(),
        ),
        1,
    ));
    let mut deep = WorkSelector::leaf(SelectorNode::Keys(["a".into()].into()));
    for _ in 0..62 {
        deep = WorkSelector::negate(deep);
    }
    cases.push((deep, 1));
    for (i, (selector, count)) in cases.into_iter().enumerate() {
        let mut proposal = group(&format!("broad-{i}"));
        proposal.administration = Some(AdministrativeCommit {
            max_result_bytes: 65536,
            intent: AdministrativeIntent {
                evaluated_at: 20,
                before_fields: None,
                operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                    id: "first".into(),
                    selector,
                    precondition: WorkSelector::all(vec![]),
                    action: action.clone(),
                }]),
            },
        });
        let preview = projection
            .preview_administration(
                input(
                    &definition,
                    &job,
                    proposal,
                    directory.path(),
                    2,
                    2,
                    &workers,
                )
                .await,
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap_or_else(|e| panic!("case {i}: {e:?}"));
        let PreviewOutcome::AdministrativeCommitted { administration, .. } =
            &preview.result().outcome
        else {
            panic!("case {i}: {:?}", preview.result());
        };
        assert_eq!(
            administration
                .histogram
                .outcomes
                .get(&fireweed_core::queue_administration::AdministrativeOutcomeKind::Changed)
                .copied()
                .unwrap_or(0),
            count,
            "case {i}"
        );
    }
    let mut proposal = group("first-owner-rejects");
    proposal.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 20,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![
                AdministrativeClause {
                    id: "first".into(),
                    selector: WorkSelector::all(vec![]),
                    precondition: WorkSelector::any(vec![]),
                    action: action.clone(),
                },
                AdministrativeClause {
                    id: "later".into(),
                    selector: WorkSelector::all(vec![]),
                    precondition: WorkSelector::all(vec![]),
                    action,
                },
            ]),
        },
    });
    let preview = projection
        .preview_administration(
            input(
                &definition,
                &job,
                proposal,
                directory.path(),
                2,
                2,
                &workers,
            )
            .await,
            clock,
            &job,
            &workers,
        )
        .await
        .unwrap();
    let PreviewOutcome::AdministrativeCommitted { administration, .. } = &preview.result().outcome
    else {
        panic!("{:?}", preview.result());
    };
    assert_eq!(
        administration
            .histogram
            .outcomes
            .get(&fireweed_core::queue_administration::AdministrativeOutcomeKind::Changed)
            .copied()
            .unwrap_or(0),
        0
    );
    assert_eq!(administration.clauses[1].histogram, Default::default());
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_requests WHERE domain=2",
            vec![],
            1
        )
        .await[0],
        Value::Integer(0)
    );
    assert_eq!(
        inspect(
            &projection,
            "SELECT SUM(priority) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(0)
    );
    execute_for_test(&projection, "UPDATE q7_fields SET value=x'00' WHERE record_id=(SELECT id FROM q7_records WHERE key=?1) AND field=1", vec![blob(b"b")]).await;
    let mut late = group("overlap-before-late-corruption");
    late.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(777),
            ..Default::default()
        }),
    });
    late.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 20,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![
                fireweed_core::queue_administration::AdministrativeClause {
                    id: "all".into(),
                    selector: WorkSelector::all(vec![]),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                },
            ]),
        },
    });
    assert!(matches!(
        projection
            .preview_administration(
                input(&definition, &job, late, directory.path(), 2, 2, &workers).await,
                Arc::new(TestClock::default()),
                &job,
                &workers
            )
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn suspended_cursor_retains_key_admission_until_future_is_destroyed() {
    let admission = QueueAdmission::new(1, 1 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let baseline = admission.usage().unwrap().1;
    let key = CursorKey::copy(&vec![7; 65536], &job).unwrap();
    let mut pending = Box::pin(async move {
        let bytes = key.as_slice();
        std::future::pending::<()>().await;
        std::hint::black_box(bytes);
        drop(key);
    });
    // A one-shot poll leaves the native next-read equivalent suspended.
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        std::future::Future::poll(pending.as_mut(), &mut cx),
        Poll::Pending
    ));
    assert!(admission.usage().unwrap().1 > baseline);
    drop(pending);
    assert_eq!(admission.usage().unwrap().1, baseline);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn broad_more_than_5000_targets_stream_complete_and_fail_closed_on_unselected_corruption() {
    use super::super::tests::{address, publication, schema, work};
    use fireweed_core::queue_administration::AdministrativeClause;
    use fireweed_engine::{
        queue_planner::{RecordSnapshot, combine_deltas, insert_unique_probes, plan_insert_vector},
        queue_publication::PublicationReceipt,
    };
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("many.db"), &job, &workers).await;
    let mut snapshot = RecordSnapshot::default();
    let request = InsertRequest {
        queue: address(),
        request_id: "many-seed".into(),
        mode: InsertMode::InsertOnly,
        items: (0..5001)
            .map(|i| work(&format!("target-{i:05}-λ"), i, false))
            .collect(),
    };
    snapshot.unique_probes_loaded =
        insert_unique_probes(&request.items, &snapshot, &schema()).unwrap();
    let before = snapshot.clone();
    let (deltas, outcomes) =
        plan_insert_vector(&request, &mut snapshot, &schema(), || Ok(10)).unwrap();
    let delta = combine_deltas(&deltas, before.next_sequence).unwrap();
    let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
    let record = publication(&before, &delta, &[receipt], &definition, &job);
    projection.apply(record, 1, &job, &workers).await.unwrap();
    drop(snapshot);
    drop(request);
    drop(deltas);
    drop(outcomes);
    drop(delta);
    let clock = Arc::new(TestClock::default());
    clock.set(1000);
    let mut proposal = group("many-preview");
    proposal.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 20,
            before_fields: None,
            operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                id: "every-active-work-row".into(),
                selector: WorkSelector::all(vec![]),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Delete {
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                },
            }]),
        },
    });
    let preview = projection
        .preview_administration(
            input(
                &definition,
                &job,
                proposal.clone(),
                directory.path(),
                2,
                5001,
                &workers,
            )
            .await,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    let PreviewOutcome::AdministrativeCommitted { administration, .. } = &preview.result().outcome
    else {
        panic!("{:?}", preview.result());
    };
    assert_eq!(administration.histogram.matched, 5001);
    assert_eq!(
        administration
            .histogram
            .outcomes
            .get(&fireweed_core::queue_administration::AdministrativeOutcomeKind::Deleted),
        Some(&5001)
    );
    drop(preview);
    // A malformed row outside this selector must not disappear through SQL NULL semantics.
    execute_for_test(
        &projection,
        "UPDATE q7_records SET lifecycle=x'' WHERE key=?1",
        vec![blob("target-05000-λ".as_bytes())],
    )
    .await;
    let AdministrativeOperation::SelectFirst(clauses) =
        &mut proposal.administration.as_mut().unwrap().intent.operation
    else {
        unreachable!()
    };
    clauses[0].selector = WorkSelector::leaf(
        fireweed_core::queue_administration::SelectorNode::Keys(["target-00000-λ".into()].into()),
    );
    assert!(matches!(
        projection
            .preview_administration(
                input(
                    &definition,
                    &job,
                    proposal,
                    directory.path(),
                    2,
                    5001,
                    &workers
                )
                .await,
                clock,
                &job,
                &workers
            )
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(5001)
    );
}
