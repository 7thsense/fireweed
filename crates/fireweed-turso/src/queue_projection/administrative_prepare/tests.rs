use super::super::tests::{address, bootstrap, group, inspect, seed, work};
use super::*;
use fireweed_conformance::queue_contract::TestClock;
use fireweed_core::queue_administration::{
    AdministrativeEntry, AdministrativeIntent, WorkSelector,
};
use fireweed_engine::{queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis};
pub(super) async fn input(
    definition: &AdmittedQueueDefinition,
    job: &Arc<QueueReservation>,
    group: CommitGroup,
    directory: &Path,
    head: u64,
    sequence: u64,
    workers: &ScratchWorkers,
) -> AdministrativePreparationInput {
    let definition = AdmittedQueueDefinition::from_raw(definition.raw(), job).unwrap();
    AdministrativePreparationInput {
        commit: CommitPreparationInput {
            genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, job)
                .unwrap(),
            queue: address(),
            group,
            limits: QueueLimits::default(),
            required_next_offset: head,
            required_insertion_sequence: sequence,
            required_last_record_epoch: 1,
        },
        scratch_root: ScratchRoot::open(directory.into(), workers).await.unwrap(),
        scratch_quota: ScratchDiskQuota::new(128 << 20).unwrap(),
    }
}
fn transition(id: &str) -> CommitGroup {
    let mut request = group(id);
    let change = AdministrativeAction::Update {
        patch: WorkPatch {
            fields: [("u".into(), Some(FieldValue::Integer(3)))].into(),
            ..Default::default()
        },
        lifecycle: AdministrativeLifecycle::Keep,
        lease_guard: AdministrativeLeaseGuard::RejectActive,
        remove_field_prefixes: Default::default(),
    };
    let entry = |key: &str, action| AdministrativeEntry {
        key: key.into(),
        expected_version: None,
        precondition: WorkSelector::all(vec![]),
        action,
    };
    request.administration = Some(AdministrativeCommit {
        intent: AdministrativeIntent {
            evaluated_at: 20,
            before_fields: Some(["u".into()].into()),
            operation: AdministrativeOperation::Addressed(vec![
                entry("a", change.clone()),
                entry("a", change.clone()),
                entry("missing", change),
                entry(
                    "b",
                    AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                    },
                ),
            ]),
        },
        max_result_bytes: 1 << 20,
    });
    request.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("ready".into()))].into(),
            value: vec![7, 8].into(),
        },
    });
    request.work_inserts.push(work("c", 4, false));
    request
}
#[tokio::test]
async fn addressed_preparation_one_worker_publishes_atomic_state_and_original_positional_results() {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("addressed.db"), &job, &workers).await;
    seed(&projection, &definition, &job, &workers, true).await;
    let clock = Arc::new(TestClock::default());
    clock.set(1000);
    let original = transition("addressed-original");
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
            "SELECT COUNT(*) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(2)
    );
    let expected = prepared.receipt().clone();
    let GroupOutcome::AdministrativeCommitted {
        versions,
        administration,
    } = &expected.outcome
    else {
        panic!("not committed: {expected:?}")
    };
    assert_eq!(versions.len(), 2);
    use fireweed_core::queue_administration::AdministrativeOutcome;
    assert_eq!(
        administration
            .items
            .iter()
            .map(|i| i.outcome.clone())
            .collect::<Vec<_>>(),
        vec![
            AdministrativeOutcome::Changed { version: 2 },
            AdministrativeOutcome::Invalid,
            AdministrativeOutcome::NotFound,
            AdministrativeOutcome::Deleted { version: 2 }
        ]
    );
    assert_eq!(
        administration.items[0].before,
        administration.items[1].before
    );
    let (record, response, head) = prepared.into_parts();
    assert_eq!(head, 2);
    drop(response);
    projection.apply(record, 2, &job, &workers).await.unwrap();
    assert_eq!(inspect(&projection,"SELECT length(value) FROM q7_bodies WHERE record_id=(SELECT id FROM q7_records WHERE key=?1)",vec![blob(b"a")],1).await[0],Value::Integer(1<<20));
    let replay = projection
        .replay_commit(
            input(
                &definition,
                &job,
                original.clone(),
                directory.path(),
                3,
                3,
                &workers,
            )
            .await
            .commit,
            &job,
            &workers,
        )
        .await
        .unwrap();
    assert_eq!(replay.receipts(), &[ReceiptLookup::Found(expected)]);
    drop(replay);
    assert!(
        projection
            .prepare_administration(
                input(
                    &definition,
                    &job,
                    original,
                    directory.path(),
                    3,
                    3,
                    &workers
                )
                .await,
                1,
                16 << 20,
                clock,
                &job,
                &workers
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(3)
    );
    drop(definition);
    drop(projection);
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn exact_result_logical_and_transport_bounds_keep_zero_effect_overflow_and_late_corruption_distinct()
 {
    use super::super::tests::execute_for_test;
    use fireweed_engine::queue_publication::{
        AdministrativeReceiptOutcome, measure_complete_administrative_receipt,
    };
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) =
        bootstrap(&directory.path().join("bounds.db"), &job, &workers).await;
    seed(&projection, &definition, &job, &workers, true).await;
    let clock = Arc::new(TestClock::default());
    let mut original = transition("bound-original");
    let admin = original.administration.as_mut().unwrap();
    admin.intent.before_fields = Some(["u".into(), "opaque".into()].into());
    admin.max_result_bytes = 8 << 20;
    let prepare = |request: AdministrativePreparationInput, transport| {
        projection.prepare_administration(request, 1, transport, clock.clone(), &job, &workers)
    };
    let prepared = prepare(
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
        16 << 20,
    )
    .await
    .unwrap()
    .unwrap();
    let expected = prepared.receipt().clone();
    let GroupOutcome::AdministrativeCommitted {
        versions,
        administration,
    } = &expected.outcome
    else {
        panic!("not committed")
    };
    let result_bytes = measure_complete_administrative_receipt(
        &original.request_id,
        &AdministrativeReceiptOutcome::Committed {
            versions,
            administration,
        },
    )
    .unwrap();
    let (record, response, _) = prepared.into_parts();
    let transport_bytes = record.bytes().len() as u64;
    let logical_bytes = transport_bytes - 54;
    drop(record);
    drop(response);
    original.administration.as_mut().unwrap().max_result_bytes = result_bytes;
    let exact = prepare(
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
        transport_bytes,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(exact.receipt(), &expected);
    drop(exact);
    original.administration.as_mut().unwrap().max_result_bytes = result_bytes - 1;
    let overflow = prepare(
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
        16 << 20,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        overflow.receipt().outcome,
        GroupOutcome::Rejected(Rejection::TooLarge)
    );
    drop(overflow);
    original.administration.as_mut().unwrap().max_result_bytes = result_bytes;
    let mut exact_input = input(
        &definition,
        &job,
        original.clone(),
        directory.path(),
        2,
        2,
        &workers,
    )
    .await;
    exact_input.commit.limits.max_group_bytes = logical_bytes;
    exact_input.commit.limits.max_state_bytes = logical_bytes;
    let exact = prepare(exact_input, transport_bytes)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.receipt(), &expected);
    drop(exact);
    let mut under = input(
        &definition,
        &job,
        original.clone(),
        directory.path(),
        2,
        2,
        &workers,
    )
    .await;
    under.commit.limits.max_group_bytes = logical_bytes - 1;
    under.commit.limits.max_state_bytes = logical_bytes - 1;
    let overflow = prepare(under, 16 << 20).await.unwrap().unwrap();
    assert_eq!(
        overflow.receipt().outcome,
        GroupOutcome::Rejected(Rejection::TooLarge)
    );
    let (record, response, head) = overflow.into_parts();
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    drop(response);
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(2)
    );
    assert_eq!(
        inspect(
            &projection,
            "SELECT next_sequence FROM q7_queues",
            vec![],
            1
        )
        .await[0],
        blob(&2u64.to_be_bytes())
    );
    original.request_id = "other-original".into();
    assert!(matches!(
        prepare(
            input(
                &definition,
                &job,
                original.clone(),
                directory.path(),
                3,
                2,
                &workers
            )
            .await,
            transport_bytes - 1
        )
        .await,
        Err(QueueError::TooLarge)
    ));
    original.administration.as_mut().unwrap().max_result_bytes = 1024;
    execute_for_test(
        &projection,
        "UPDATE q7_records SET lease_hash=x'' WHERE key=?1",
        vec![blob(b"b")],
    )
    .await;
    assert!(matches!(
        prepare(
            input(
                &definition,
                &job,
                original,
                directory.path(),
                3,
                2,
                &workers
            )
            .await,
            16 << 20
        )
        .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    drop(expected);
    drop(definition);
    drop(projection);
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
async fn run_mode(
    preview: bool,
    projection: &Arc<QueueProjection>,
    input: AdministrativePreparationInput,
    clock: Arc<dyn Clock>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
) -> QueueResult<()> {
    if preview {
        projection
            .preview_administration(input, clock, job, workers)
            .await
            .map(drop)
    } else {
        projection
            .prepare_administration(input, 1, 16 << 20, clock, job, workers)
            .await
            .map(drop)
    }
}
#[tokio::test]
async fn precommit_panic_and_caller_cancellation_release_only_after_physical_completion() {
    for preview in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 128 << 20).unwrap();
        let seed_job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) =
            bootstrap(&directory.path().join("cancel.db"), &seed_job, &workers).await;
        seed(&projection, &definition, &seed_job, &workers, false).await;
        let terminal = Arc::new(tokio::sync::Notify::new());
        let notification = terminal.clone();
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(move || notification.notify_one())
                .unwrap(),
        );
        let clock = Arc::new(TestClock::default());
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            run_mode(
                preview,
                &projection,
                input(
                    &definition,
                    &job,
                    transition("panic-original"),
                    directory.path(),
                    2,
                    2,
                    &workers
                )
                .await,
                clock.clone(),
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let call_input = input(
            &definition,
            &job,
            transition("cancel-original"),
            directory.path(),
            2,
            2,
            &workers,
        )
        .await;
        drop(definition);
        drop(seed_job);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(pause.clone());
        let call = tokio::spawn({
            let projection = projection.clone();
            let workers = workers.clone();
            async move { run_mode(preview, &projection, call_input, clock, &job, &workers).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let retained = admission.usage().unwrap();
        assert!(retained.1 > 1000);
        call.abort();
        assert!(matches!(call.await, Err(error) if error.is_cancelled()));
        assert_eq!(admission.usage().unwrap(), retained);
        let mut drain = Box::pin(workers.close_and_drain());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut drain)
                .await
                .is_err()
        );
        pause.release.send(true).unwrap();
        drain.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), terminal.notified())
            .await
            .unwrap();
        assert_eq!(admission.usage().unwrap(), (0, 0));
        *projection.pause.lock().await = None;
        assert_eq!(inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=2),(SELECT COUNT(*) FROM q7_records WHERE active=1) FROM q7_queues",vec![],3).await,
        vec![Value::Integer(2),Value::Integer(0),Value::Integer(2)]);
    }
}
#[tokio::test]
async fn prefix_composite_unique_active_delete_and_final_clock_expiry_preserve_authority() {
    use super::super::tests::schema;
    use fireweed_engine::queue_schema_codec::encode_definition;
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let projection = QueueProjection::open(&directory.path().join("prefix.db"), &job, &workers)
        .await
        .unwrap();
    let mut schema = schema();
    schema
        .work
        .indexes
        .insert("pair".into(), vec!["u".into(), "opaque".into()]);
    schema.work.unique_indexes.insert("pair".into());
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
    projection
        .apply(
            encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job).unwrap(),
            0,
            &job,
            &workers,
        )
        .await
        .unwrap();
    seed(&projection, &definition, &job, &workers, false).await;
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let mut metadata = group("seed-prefixes");
    for key in ["a", "b"] {
        metadata.work_updates.push(UnclaimedUpdate {
            key: key.into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                fields: [
                    ("opaque".into(), Some(FieldValue::Bytes(vec![5]))),
                    ("meta/key".into(), Some(FieldValue::Text(key.into()))),
                    ("meta/remove".into(), Some(FieldValue::Text("old".into()))),
                ]
                .into(),
                ..Default::default()
            }),
        });
    }
    let prepared = projection
        .prepare_ordinary_commit(
            input(
                &definition,
                &job,
                metadata,
                directory.path(),
                2,
                2,
                &workers,
            )
            .await
            .commit,
            1,
            16 << 20,
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        prepared.receipt().outcome,
        GroupOutcome::Committed { .. }
    ));
    let (record, response, head) = prepared.into_parts();
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    drop(response);
    let claim_input = |head| ClaimReplayInput {
        genesis: encode_checked_genesis(1, &address(), &definition, &[31; 32], 16 << 20, &job)
            .unwrap(),
        request: ClaimRequest {
            queue: address(),
            request_id: "lease-b".into(),
            filter: Filter {
                keys: Some(["b".into()].into()),
                ..Default::default()
            },
            order: ClaimOrder::PriorityAscending,
            eligible_at: 100,
            lease_nanos: 1_000_000,
            max_records: 1,
            max_bytes: 65536,
            fields: Some(["u".into()].into()),
            include_body: false,
        },
        now: 100,
        limits: QueueLimits::default(),
        required_next_offset: head,
        required_insertion_sequence: 2,
        required_last_record_epoch: 1,
    };
    let claim = projection
        .prepare_claim(claim_input(3), 1, 16 << 20, clock.clone(), &job, &workers)
        .await
        .unwrap()
        .unwrap();
    let (record, response, head) = claim.into_parts();
    projection
        .apply(record, head, &job, &workers)
        .await
        .unwrap();
    drop(response);
    let claim = projection
        .replay_claim(claim_input(4), &job, &workers)
        .await
        .unwrap()
        .unwrap();
    let claimed = claim.records()[0].clone();
    drop(claim);
    let mut request = group("release-active-owner");
    request.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 100,
            before_fields: Some(["u".into(), "meta/key".into(), "meta/remove".into()].into()),
            operation: AdministrativeOperation::Addressed(vec![
                AdministrativeEntry {
                    key: "a".into(),
                    expected_version: Some(2),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Update {
                        patch: WorkPatch {
                            fields: [
                                ("u".into(), Some(FieldValue::Integer(3))),
                                ("meta/key".into(), Some(FieldValue::Text("new".into()))),
                            ]
                            .into(),
                            ..Default::default()
                        },
                        lifecycle: AdministrativeLifecycle::Keep,
                        lease_guard: AdministrativeLeaseGuard::RejectActive,
                        remove_field_prefixes: ["meta/".into()].into(),
                    },
                },
                AdministrativeEntry {
                    key: "b".into(),
                    expected_version: Some(3),
                    precondition: WorkSelector::all(vec![]),
                    action: AdministrativeAction::Delete {
                        lease_guard: AdministrativeLeaseGuard::InvalidateActive,
                    },
                },
            ]),
        },
    });
    #[cfg(target_os = "linux")]
    {
        use fireweed_core::queue_administration::{AdministrativeClause, SelectorNode};
        let leaf = WorkSelector::leaf;
        for (selector, evaluated, matched) in [
            (
                leaf(SelectorNode::Field {
                    name: "meta/key".into(),
                    predicate: Predicate::Equal(FieldValue::Text("a".into())),
                }),
                100,
                1,
            ),
            (
                leaf(SelectorNode::Field {
                    name: "meta/key".into(),
                    predicate: Predicate::Range {
                        lower: Some(Bound::Included(FieldValue::Text("a".into()))),
                        upper: Some(Bound::Excluded(FieldValue::Text("b".into()))),
                    },
                }),
                100,
                1,
            ),
            (
                leaf(SelectorNode::Present {
                    name: "meta/remove".into(),
                    present: true,
                }),
                100,
                2,
            ),
            (leaf(SelectorNode::LeaseActive(true)), 100, 1),
            (leaf(SelectorNode::LeaseActive(false)), 100, 1),
            (leaf(SelectorNode::LeaseActive(false)), 1_000_100, 2),
            (
                leaf(SelectorNode::StoredLifecycleIn(vec![Lifecycle::Leased])),
                1_000_100,
                1,
            ),
            (
                leaf(SelectorNode::EffectiveLifecycleIn(vec![Lifecycle::Pending])),
                1_000_100,
                2,
            ),
            (
                leaf(SelectorNode::Attempts {
                    comparison: fireweed_core::queue_administration::Comparison::Equal,
                    value: 1,
                }),
                100,
                1,
            ),
        ] {
            let mut broad = group("broad-metadata-preview");
            broad.administration = Some(AdministrativeCommit {
                max_result_bytes: 65536,
                intent: AdministrativeIntent {
                    evaluated_at: evaluated,
                    before_fields: None,
                    operation: AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                        id: "metadata".into(),
                        selector,
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Delete {
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                        },
                    }]),
                },
            });
            let preview = projection
                .preview_administration(
                    input(&definition, &job, broad, directory.path(), 4, 2, &workers).await,
                    clock.clone(),
                    &job,
                    &workers,
                )
                .await
                .unwrap();
            let PreviewOutcome::AdministrativeCommitted { administration, .. } =
                &preview.result().outcome
            else {
                panic!("{:?}", preview.result());
            };
            assert_eq!(administration.histogram.matched, matched);
        }
    }
    let prepared = projection
        .prepare_administration(
            input(
                &definition,
                &job,
                request.clone(),
                directory.path(),
                4,
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
    let GroupOutcome::AdministrativeCommitted { administration, .. } = &prepared.receipt().outcome
    else {
        panic!("expected administrative commit");
    };
    assert!(matches!(
        administration.items[0].outcome,
        fireweed_core::queue_administration::AdministrativeOutcome::Changed { version: 3 }
    ));
    assert!(matches!(
        administration.items[1].outcome,
        fireweed_core::queue_administration::AdministrativeOutcome::Deleted { version: 4 }
    ));
    drop(prepared);
    // An ordinary claimed update is evaluated at the independent final
    // operational clock, even though administration was evaluated at 100.
    let mut expired = group("expired-final-clock");
    expired.administration = request.administration.clone();
    if let AdministrativeOperation::Addressed(entries) =
        &mut expired.administration.as_mut().unwrap().intent.operation
    {
        entries.pop();
        if let AdministrativeAction::Update { patch, .. } = &mut entries[0].action {
            patch.fields.remove("u");
        }
    }
    expired.claimed_updates.push(ClaimedUpdate {
        key: "b".into(),
        lease: claimed.lease,
        expected_version: claimed.record.version,
        patch: WorkPatch::default(),
        disposition: Disposition::Complete,
    });
    clock.set(2_000_000);
    let rejected = projection
        .prepare_administration(
            input(&definition, &job, expired, directory.path(), 4, 2, &workers).await,
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
        rejected.receipt().outcome,
        GroupOutcome::Rejected(Rejection::StaleLease)
    );
    drop(rejected);
    clock.set(100);
    let prepared = projection
        .prepare_administration(
            input(&definition, &job, request, directory.path(), 4, 2, &workers).await,
            1,
            16 << 20,
            clock,
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
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_records WHERE active=1",
            vec![],
            1
        )
        .await[0],
        Value::Integer(1)
    );
    assert_eq!(
        inspect(
            &projection,
            "SELECT COUNT(*) FROM q7_additional WHERE name=?1",
            vec![blob(b"meta/remove")],
            1
        )
        .await[0],
        Value::Integer(0)
    );
    let pair=inspect(&projection,"SELECT r.key,i.full_key FROM q7_indexes i JOIN q7_records r ON r.id=i.record_id WHERE i.index_no=0",vec![],2).await;
    let number = FieldValue::Integer(3);
    let opaque = FieldValue::Bytes(vec![5]);
    assert_eq!(
        pair,
        vec![
            blob(b"a"),
            blob(&ordered_key(&[Some(&number), Some(&opaque)]).unwrap())
        ]
    );
    assert_eq!(
        inspect(&projection, "SELECT COUNT(*) FROM q7_indexes", vec![], 1).await[0],
        Value::Integer(2)
    );
    drop(definition);
    drop(projection);
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
