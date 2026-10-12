use super::super::tests::{bootstrap, execute_for_test, group, inspect, seed, work};
use super::tests::input;
use super::*;
use fireweed_conformance::queue_contract::TestClock;
use fireweed_engine::queue_admission::QueueAdmission;
#[tokio::test]
async fn ordinary_preview_has_identical_response_admission_atomic_proposals_and_no_execution_identity_or_transport_gate()
 {
    let directory = tempfile::tempdir().unwrap();
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let (projection, definition) = bootstrap(
        &directory.path().join("ordinary-preview.db"),
        &job,
        &workers,
    )
    .await;
    seed(&projection, &definition, &job, &workers, true).await;
    let clock = Arc::new(TestClock::default());
    let mut original = group("ordinary-preview-original");
    original.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            priority: Some(5),
            ..Default::default()
        }),
    });
    original.work_inserts.push(work("c", 3, false));
    original.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: [("phase".into(), FieldValue::Text("ready".into()))].into(),
            value: vec![1, 2].into(),
        },
    });
    let preview = projection
        .preview_ordinary_commit(
            input(&definition, &job, original.clone(), 2, 2),
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    let expected = preview.result().clone();
    assert!(matches!(&expected.outcome,PreviewOutcome::Committed {versions} if versions.len()==3));
    // Preview obeys the same complete execution-response reservation as commit,
    // including the fallback floor and maximum version widths. Find its actual
    // common-contract threshold without duplicating the serializer arithmetic.
    let mut low = 1u64;
    let mut high = 4096u64;
    while low < high {
        let middle = low + (high - low) / 2;
        let limits = QueueLimits {
            max_response_bytes: middle,
            ..Default::default()
        };
        if original
            .validate_for(
                &super::super::tests::address(),
                definition.schema(),
                &limits,
            )
            .is_ok()
        {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let bytes = low;
    drop(preview);
    for limit in [bytes, bytes - 1] {
        let mut request = input(&definition, &job, original.clone(), 2, 2);
        request.limits.max_response_bytes = limit;
        let result = projection
            .preview_ordinary_commit(request, clock.clone(), &job, &workers)
            .await;
        if limit == bytes {
            assert_eq!(result.unwrap().result(), &expected);
        } else {
            assert!(matches!(result, Err(QueueError::TooLarge)));
        }
    }
    assert!(
        projection
            .prepare_ordinary_commit(
                input(&definition, &job, original.clone(), 2, 2),
                1,
                1,
                clock.clone(),
                &job,
                &workers
            )
            .await
            .is_err()
    );
    assert_eq!(
        projection
            .preview_ordinary_commit(
                input(&definition, &job, original.clone(), 2, 2),
                clock.clone(),
                &job,
                &workers
            )
            .await
            .unwrap()
            .result(),
        &expected
    );
    assert_eq!(inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=2),(SELECT COUNT(*) FROM q7_records WHERE active=1) FROM q7_queues",vec![],3).await,
        vec![Value::Integer(2),Value::Integer(0),Value::Integer(2)]);
    let prepared = projection
        .prepare_ordinary_commit(
            input(&definition, &job, original.clone(), 2, 2),
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
        .preview_ordinary_commit(
            input(&definition, &job, original.clone(), 3, 3),
            clock.clone(),
            &job,
            &workers,
        )
        .await
        .unwrap();
    assert_eq!(
        preview.result().outcome,
        PreviewOutcome::Rejected(Rejection::AlreadyExists)
    );
    drop(preview);
    let mut changed = original;
    changed.work_inserts.clear();
    changed.state_updates.clear();
    changed.work_updates[0].expect = StateExpect::Version(2);
    if let UnclaimedChange::Patch(patch) = &mut changed.work_updates[0].change {
        patch.priority = Some(6);
    }
    assert!(matches!(
        projection
            .prepare_ordinary_commit(
                input(&definition, &job, changed.clone(), 3, 3),
                1,
                16 << 20,
                clock.clone(),
                &job,
                &workers
            )
            .await,
        Err(QueueError::RequestConflict)
    ));
    for stage in 0..3 {
        if stage == 1 {
            execute_for_test(
                &projection,
                "UPDATE q7_requests SET fingerprint=x'',outcome=x'ff' WHERE domain=2",
                vec![],
            )
            .await;
        } else if stage == 2 {
            execute_for_test(
                &projection,
                "UPDATE q7_requests SET expired=1 WHERE domain=2",
                vec![],
            )
            .await;
        }
        let result = projection
            .preview_ordinary_commit(
                input(&definition, &job, changed.clone(), 3, 3),
                clock.clone(),
                &job,
                &workers,
            )
            .await
            .unwrap();
        assert_eq!(
            result.result().outcome,
            PreviewOutcome::Committed {
                versions: vec![RecordVersion {
                    collection: None,
                    key: "a".into(),
                    version: 3
                }]
            }
        );
        drop(result);
    }
    assert_eq!(
        inspect(
            &projection,
            "SELECT priority,version FROM q7_records WHERE key=?1",
            vec![blob(b"a")],
            2
        )
        .await,
        vec![Value::Integer(5), blob(&2u64.to_be_bytes())]
    );
    drop(expected);
    drop(definition);
    drop(projection);
    workers.close_and_drain().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn ordinary_preview_and_publication_retain_reservations_through_precommit_panic_and_cancellation()
 {
    for preview in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(2, 64 << 20).unwrap();
        let seed_job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let (projection, definition) = bootstrap(
            &directory.path().join("ordinary-cancel.db"),
            &seed_job,
            &workers,
        )
        .await;
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
        let mut update = group("ordinary-cancel");
        update.work_updates.push(UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(1),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(5),
                ..Default::default()
            }),
        });
        projection
            .panic_after_command
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            run(
                preview,
                &projection,
                input(&definition, &job, update.clone(), 2, 2),
                clock.clone(),
                &job,
                &workers
            )
            .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let call_input = input(&definition, &job, update, 2, 2);
        drop(definition);
        drop(seed_job);
        let pause = Arc::new(ApplyPause {
            after_commit: false,
            fail_reply: false,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::watch::channel(false).0,
        });
        *projection.pause.lock().await = Some(pause.clone());
        let caller = tokio::spawn({
            let projection = projection.clone();
            let workers = workers.clone();
            async move { run(preview, &projection, call_input, clock, &job, &workers).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered.notified())
            .await
            .unwrap();
        let retained = admission.usage().unwrap();
        assert!(retained.1 > 1000);
        caller.abort();
        assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
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
        assert_eq!(inspect(&projection,"SELECT applied_next_offset,(SELECT COUNT(*) FROM q7_requests WHERE domain=2),(SELECT priority FROM q7_records WHERE key=?1) FROM q7_queues",vec![blob(b"a")],3).await,
            vec![Value::Integer(2),Value::Integer(0),Value::Integer(0)]);
    }
}
async fn run(
    preview: bool,
    projection: &Arc<QueueProjection>,
    input: CommitPreparationInput,
    clock: Arc<dyn Clock>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
) -> QueueResult<()> {
    if preview {
        projection
            .preview_ordinary_commit(input, clock, job, workers)
            .await
            .map(drop)
    } else {
        projection
            .prepare_ordinary_commit(input, 1, 16 << 20, clock, job, workers)
            .await
            .map(drop)
    }
}
