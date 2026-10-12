use super::*;

#[tokio::test]
async fn addressed_commit_publishes_atomic_groups_and_resolves_originals_after_fencing_and_rebuild()
{
    administrative_commit_case(false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn broad_commit_publishes_atomic_groups_and_resolves_originals_after_fencing_and_rebuild() {
    administrative_commit_case(true).await;
}
async fn administrative_commit_case(broad_mode: bool) {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-admin-commit-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = Arc::new(
        ObjectLogEngineStore::open_local(root.join("log"), FlushConfig::default())
            .await
            .unwrap(),
    );
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let mut definition = schema();
    definition
        .state
        .insert("workflow".into(), definition.work.clone());
    store
        .provision_api007_genesis(
            Api007GenesisRequest {
                namespace: store.bind_api007_namespace(&address(), &job).await.unwrap(),
                definition: AdmittedQueueDefinition::from_raw(
                    &encode_definition(&definition).unwrap(),
                    &job,
                )
                .unwrap(),
                candidate_secret: [17; 32],
                max_record_bytes: 1 << 20,
                metadata_bytes: 65536,
            },
            &job,
            &workers,
        )
        .await
        .unwrap();
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline = test_pipeline(
        store.clone(),
        projection,
        workers.clone(),
        65536,
        1 << 20,
        &root,
    )
    .await
    .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let work = |key: &str| NewWork {
        key: key.into(),
        priority: 1,
        available_at: 0,
        fields: Default::default(),
        body: bytes::Bytes::new(),
    };
    drop(
        pipeline
            .insert(
                InsertRequest {
                    queue: address(),
                    request_id: "seed-commit".into(),
                    mode: InsertMode::InsertOnly,
                    items: vec![work("a")],
                },
                QueueLimits::default(),
                clock.clone(),
                job.clone(),
            )
            .await
            .unwrap(),
    );
    use fireweed_core::queue_administration::*;
    let make = |id: &str, key: &str, version: u64, priority: i64| {
        let mut group = CommitGroup {
            request_id: id.into(),
            claimed_updates: vec![],
            work_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![],
            administration: Some(AdministrativeCommit {
                max_result_bytes: 65536,
                intent: AdministrativeIntent {
                    evaluated_at: 0,
                    before_fields: Some(Default::default()),
                    operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                        key: key.into(),
                        expected_version: Some(version),
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Update {
                            patch: WorkPatch {
                                priority: Some(priority),
                                ..Default::default()
                            },
                            lifecycle: AdministrativeLifecycle::Keep,
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                            remove_field_prefixes: Default::default(),
                        },
                    }]),
                },
            }),
        };
        if broad_mode {
            let admin = group.administration.as_mut().unwrap();
            admin.intent.before_fields = None;
            let AdministrativeOperation::Addressed(entries) = &admin.intent.operation else {
                unreachable!()
            };
            admin.intent.operation =
                AdministrativeOperation::SelectFirst(vec![AdministrativeClause {
                    id: "selected".into(),
                    selector: WorkSelector::leaf(SelectorNode::Keys([key.into()].into())),
                    precondition: WorkSelector::all(vec![]),
                    action: entries[0].action.clone(),
                }]);
        }
        group
    };
    let mut group = make("atomic-admin", "a", 1, 17);
    group.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Default::default(),
            value: vec![7, 8].into(),
        },
    });
    group.work_inserts.push(work("next"));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_commit_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let first = tokio::spawn({
        let pipeline = pipeline.clone();
        let group = group.clone();
        let clock = clock.clone();
        let job = job.clone();
        async move {
            pipeline
                .commit(address(), group, QueueLimits::default(), clock, job)
                .await
        }
    });
    if tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .is_err()
    {
        assert!(
            first.is_finished(),
            "commit did not reach append or terminate"
        );
        panic!(
            "commit terminated before append: {:?}",
            first.await.unwrap().err()
        );
    }
    let winner = pipeline
        .commit(
            address(),
            group.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let original = winner.receipts().to_vec();
    drop(winner);
    assert!(matches!(
        &original[..],
        [ReceiptLookup::Found(CommitReceipt {
            outcome: GroupOutcome::AdministrativeCommitted { .. },
            ..
        })]
    ));
    release.notify_one();
    let loser = first.await.unwrap().unwrap();
    assert_eq!(loser.receipts(), original);
    drop(loser);
    let preview = pipeline
        .preview(
            PreviewRequest {
                queue: address(),
                group: make("preview-only", "a", 2, 23),
            },
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        preview.result().outcome,
        PreviewOutcome::AdministrativeCommitted { .. }
    ));
    drop(preview);
    assert_eq!(
        pipeline
            .read_commit(
                ReceiptReadRequest {
                    queue: address(),
                    request_ids: vec!["preview-only".into()],
                    max_bytes: 65536,
                },
                QueueLimits::default(),
                job.clone()
            )
            .await
            .unwrap()
            .receipts(),
        &[ReceiptLookup::Absent]
    );
    let mut changed = group.clone();
    changed.administration.as_mut().unwrap().max_result_bytes -= 1;
    assert!(matches!(
        pipeline
            .commit(
                address(),
                changed,
                QueueLimits::default(),
                clock.clone(),
                job.clone()
            )
            .await,
        Err(QueueError::RequestConflict)
    ));
    let state = pipeline
        .read_state(
            StateReadRequest {
                queue: address(),
                collection: "workflow".into(),
                keys: vec!["run".into()],
                max_bytes: 65536,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(state.records()[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    drop(state);
    let mut rejected = make("rejected-admin", "a", 2, 29);
    rejected.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Version(999),
        change: StateChange::Put {
            fields: Default::default(),
            value: vec![99].into(),
        },
    });
    rejected.work_inserts.push(work("must-not-exist"));
    let result = pipeline
        .commit(
            address(),
            rejected.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let original_rejection = result.receipts().to_vec();
    drop(result);
    assert!(matches!(
        &original_rejection[..],
        [ReceiptLookup::Found(CommitReceipt {
            outcome: GroupOutcome::Rejected(Rejection::VersionConflict),
            ..
        })]
    ));
    let replay = pipeline
        .commit(
            address(),
            rejected.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.receipts(), original_rejection);
    drop(replay);
    let mut oversized = make("logical-admin-too-large", "missing", 1, 43);
    let administration = oversized.administration.as_mut().unwrap();
    administration.max_result_bytes = 512;
    match &mut administration.intent.operation {
        AdministrativeOperation::Addressed(entries) => {
            let entry = entries[0].clone();
            *entries = (0..8)
                .map(|i| AdministrativeEntry {
                    key: format!("missing-{i}-{}", "x".repeat(1024)),
                    ..entry.clone()
                })
                .collect();
        }
        AdministrativeOperation::SelectFirst(clauses) => {
            let clause = clauses[0].clone();
            *clauses = (0..8)
                .map(|i| AdministrativeClause {
                    id: format!("empty-{i}-{}", "x".repeat(1024)),
                    ..clause.clone()
                })
                .collect();
        }
    }
    oversized
        .work_inserts
        .push(work("too-large-must-not-exist"));
    let result = pipeline
        .commit(
            address(),
            oversized.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let too_large_receipt = result.receipts().to_vec();
    drop(result);
    assert!(matches!(
        &too_large_receipt[..],
        [ReceiptLookup::Found(CommitReceipt {
            outcome: GroupOutcome::Rejected(Rejection::TooLarge),
            ..
        })]
    ));
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    let (terminal_tx, mut terminal_rx) = tokio::sync::oneshot::channel();
    let job = Arc::new(
        admission
            .reserve(0)
            .unwrap()
            .with_terminal_callback(move || {
                let _ = terminal_tx.send(());
            })
            .unwrap(),
    );
    let cancelled = make("cancelled-admin", "a", 2, 31);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_commit_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let group = cancelled.clone();
        let clock = clock.clone();
        let job = job.clone();
        async move {
            pipeline
                .commit(address(), group, QueueLimits::default(), clock, job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    drop(job);
    assert_eq!(admission.usage().unwrap().0, 1);
    assert!(matches!(
        terminal_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
    let job = Arc::new(admission.reserve(0).unwrap());
    let lookup = pipeline
        .read_commit(
            ReceiptReadRequest {
                queue: address(),
                request_ids: vec![cancelled.request_id.clone()],
                max_bytes: 65536,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        lookup.receipts(),
        [ReceiptLookup::Found(CommitReceipt {
            outcome: GroupOutcome::AdministrativeCommitted { .. },
            ..
        })]
    ));
    let before_retry = lookup.receipts().to_vec();
    drop(lookup);
    let response = pipeline
        .commit(
            address(),
            cancelled.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(response.receipts(), before_retry);
    let cancellation_receipt = response.receipts().to_vec();
    drop(response);
    drop(pipeline);
    let rebuilt = QueueProjection::open(&root.join("rebuilt.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline = test_pipeline(
        store.clone(),
        rebuilt,
        workers.clone(),
        65536,
        1 << 20,
        &root,
    )
    .await
    .unwrap();
    for (request, expected) in [
        (group, original),
        (rejected, original_rejection),
        (cancelled, cancellation_receipt),
        (oversized, too_large_receipt),
    ] {
        let replay = pipeline
            .commit(
                address(),
                request,
                QueueLimits::default(),
                clock.clone(),
                job.clone(),
            )
            .await
            .unwrap();
        assert_eq!(replay.receipts(), expected);
        drop(replay);
    }
    let response = pipeline
        .query(
            QueryRequest {
                queue: address(),
                source: RecordSource::Work,
                index: "$key".into(),
                descending: false,
                filter: Filter::default(),
                max_records: 10,
                max_bytes: 65536,
                cursor: None,
            },
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(response.page().records.len(), 2);
    assert!(
        matches!(&response.page().records[0], QueryRecord::Work(row) if row.work.key == "a" && row.version == 3 && row.work.priority == 31)
    );
    assert!(
        matches!(&response.page().records[1], QueryRecord::Work(row) if row.work.key == "next")
    );
    drop(response);
    let state = pipeline
        .read_state(
            StateReadRequest {
                queue: address(),
                collection: "workflow".into(),
                keys: vec!["run".into()],
                max_bytes: 65536,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(state.records()[0].as_ref().unwrap().value.as_ref(), &[7, 8]);
    drop(state);
    workers.close_and_drain().await.unwrap();
    let mut invalid = make("invalid-before-closed-workers", "a", 3, 47);
    invalid.administration.as_mut().unwrap().max_result_bytes = 1;
    assert!(matches!(
        pipeline
            .commit(
                address(),
                invalid,
                QueueLimits::default(),
                clock.clone(),
                job.clone()
            )
            .await,
        Err(QueueError::TooLarge)
    ));
    drop(pipeline);
    drop(job);
    drop(store);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}
