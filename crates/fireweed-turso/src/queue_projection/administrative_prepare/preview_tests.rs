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
        .preview_addressed_administration(
            input(&definition, &job, original.clone(), directory.path(), 2, 2),
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
        .prepare_addressed_administration(
            input(&definition, &job, original.clone(), directory.path(), 2, 2),
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
        .preview_addressed_administration(
            input(&definition, &job, original.clone(), directory.path(), 3, 2),
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
            .prepare_addressed_administration(
                input(&definition, &job, changed.clone(), directory.path(), 3, 2),
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
            .preview_addressed_administration(
                input(&definition, &job, changed.clone(), directory.path(), 3, 2),
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
        .preview_addressed_administration(
            input(&definition, &job, large.clone(), directory.path(), 3, 2),
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
        .prepare_addressed_administration(
            input(&definition, &job, large, directory.path(), 3, 2),
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
    let invalid = input(&definition, &job, invalid, directory.path(), 3, 2);
    workers.close_and_drain().await.unwrap();
    assert!(matches!(
        projection
            .preview_addressed_administration(invalid, clock, &job, &workers)
            .await,
        Err(QueueError::TooLarge)
    ));
    drop(definition);
    drop(projection);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
