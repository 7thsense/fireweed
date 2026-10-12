use super::*;
use fireweed_conformance::queue_contract::TestClock;
use fireweed_engine::Clock;

fn patch_group(id: &str, version: u64, priority: i64) -> CommitGroup {
    CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![UnclaimedUpdate {
            key: "a".into(),
            expect: StateExpect::Version(version),
            change: UnclaimedChange::Patch(WorkPatch {
                priority: Some(priority),
                ..Default::default()
            }),
        }],
    }
}
fn batch(groups: Vec<CommitGroup>) -> CommitBatch {
    CommitBatch {
        queue: address(),
        groups,
    }
}
struct FailSecondClock {
    inner: TestClock,
    calls: std::sync::atomic::AtomicUsize,
}
impl Clock for FailSecondClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            panic!("injected second-group clock failure");
        }
        self.inner.now()
    }
}
#[tokio::test]
async fn one_permit_batch_validates_all_siblings_replays_before_fresh_and_finishes_after_caller_drop()
 {
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-batch-{}-{}",
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
    let admission = QueueAdmission::new(2, 256 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    provision(&store, &job, &workers).await;
    append_insert(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let options = QueuePipelineOptions {
        operations: 1,
        metadata_bytes: 65536,
        hard_record_bytes: 1 << 20,
    };
    let pipeline =
        test_pipeline_options(store.clone(), projection, workers.clone(), options, &root)
            .await
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let first = patch_group("first", 1, 7);
    let stale = patch_group("stale", 1, 8);
    let final_group = patch_group("final", 2, 9);
    let output = pipeline
        .commit_batch(
            batch(vec![first.clone(), stale.clone(), final_group.clone()]),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        output.receipts()[0].outcome,
        GroupOutcome::Committed { .. }
    ));
    assert_eq!(
        output.receipts()[1].outcome,
        GroupOutcome::Rejected(Rejection::VersionConflict)
    );
    assert!(matches!(
        output.receipts()[2].outcome,
        GroupOutcome::Committed { .. }
    ));
    let expected = output.receipts().to_vec();
    drop(output);
    let output = pipeline
        .commit_batch(
            batch(vec![final_group.clone(), stale.clone(), first.clone()]),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        output.receipts(),
        [
            expected[2].clone(),
            expected[1].clone(),
            expected[0].clone()
        ]
    );
    drop(output);
    let before = pipeline
        .recover(address(), job.clone())
        .await
        .unwrap()
        .target_next_offset;
    let new = patch_group("before-invalid-sibling", 3, 31);
    let mut invalid = patch_group("invalid-later-sibling", 3, 32);
    invalid.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
        fields: [("undeclared".into(), Some(FieldValue::Integer(1)))].into(),
        ..Default::default()
    });
    assert!(matches!(
        pipeline
            .commit_batch(
                batch(vec![new, invalid]),
                QueueLimits::default(),
                clock.clone(),
                job.clone()
            )
            .await,
        Err(QueueError::Invalid(_))
    ));
    assert_eq!(
        pipeline
            .recover(address(), job.clone())
            .await
            .unwrap()
            .target_next_offset,
        before
    );
    let mut conflict = first;
    conflict.work_updates[0].change = UnclaimedChange::Patch(WorkPatch {
        priority: Some(11),
        ..Default::default()
    });
    let output = pipeline
        .commit_batch(
            batch(vec![
                patch_group("new-before-known-conflict", 3, 31),
                conflict,
                patch_group("tail-after-known-conflict", 4, 37),
            ]),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        output.receipts()[0].outcome,
        GroupOutcome::Committed { .. }
    ));
    assert_eq!(output.receipts()[1].outcome, GroupOutcome::RequestConflict);
    assert!(matches!(
        output.receipts()[2].outcome,
        GroupOutcome::Committed { .. }
    ));
    drop(output);

    // An independent writer changes originals after the provisional batch read.
    let other_projection = QueueProjection::open(&root.join("other.db"), &job, &workers)
        .await
        .unwrap();
    let other = test_pipeline_options(
        store.clone(),
        other_projection,
        workers.clone(),
        options,
        &root,
    )
    .await
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.after_batch_read.lock().unwrap() = Some((entered.clone(), release.clone()));
    let reader = tokio::spawn({
        let pipeline = pipeline.clone();
        let clock = clock.clone();
        let job = job.clone();
        async move {
            pipeline
                .commit_batch(
                    batch(vec![
                        patch_group("race-known", 5, 41),
                        patch_group("race-tail", 5, 43),
                    ]),
                    QueueLimits::default(),
                    clock,
                    job,
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let winner = patch_group("race-known", 1, 42);
    drop(
        other
            .commit(
                address(),
                winner,
                QueueLimits::default(),
                clock.clone(),
                job.clone(),
            )
            .await
            .unwrap(),
    );
    release.notify_one();
    let output = reader.await.unwrap().unwrap();
    assert_eq!(output.receipts()[0].outcome, GroupOutcome::RequestConflict);
    assert!(matches!(
        output.receipts()[1].outcome,
        GroupOutcome::Committed { .. }
    ));
    drop(output);
    drop(other);

    // Abandon the caller while the first publication is paused: both groups finish.
    let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
    let cancelled_job = Arc::new(
        admission
            .reserve(0)
            .unwrap()
            .with_terminal_callback(move || {
                let _ = terminal_tx.send(());
            })
            .unwrap(),
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_commit_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let cancelled = batch(vec![
        patch_group("cancel-first", 6, 47),
        patch_group("cancel-second", 7, 53),
    ]);
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let clock = clock.clone();
        let job = cancelled_job.clone();
        let request = cancelled.clone();
        async move {
            pipeline
                .commit_batch(request, QueueLimits::default(), clock, job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    drop(cancelled_job);
    assert_eq!(admission.usage().unwrap().0, 2);
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.usage().unwrap().0, 1);
    let originals = pipeline
        .read_commit(
            ReceiptReadRequest {
                queue: address(),
                request_ids: vec!["cancel-first".into(), "cancel-second".into()],
                max_bytes: 65536,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(
        originals
            .receipts()
            .iter()
            .all(|x| matches!(x, ReceiptLookup::Found(_)))
    );
    let expected_cancellation = originals.receipts().to_vec();
    drop(originals);

    let failing = Arc::new(FailSecondClock {
        inner: TestClock::default(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let prefix = batch(vec![
        patch_group("durable-prefix", 8, 57),
        patch_group("failed-suffix", 9, 59),
    ]);
    assert!(matches!(
        pipeline
            .commit_batch(prefix, QueueLimits::default(), failing, job.clone())
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    let lookup = pipeline
        .read_commit(
            ReceiptReadRequest {
                queue: address(),
                request_ids: vec!["durable-prefix".into(), "failed-suffix".into()],
                max_bytes: 65536,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        lookup.receipts(),
        [ReceiptLookup::Found(_), ReceiptLookup::Absent]
    ));
    drop(lookup);
    drop(pipeline);
    let rebuilt = QueueProjection::open(&root.join("rebuilt.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline = test_pipeline_options(store.clone(), rebuilt, workers.clone(), options, &root)
        .await
        .unwrap();
    let output = pipeline
        .commit_batch(cancelled, QueueLimits::default(), clock, job.clone())
        .await
        .unwrap();
    assert_eq!(
        output
            .receipts()
            .iter()
            .cloned()
            .map(ReceiptLookup::Found)
            .collect::<Vec<_>>(),
        expected_cancellation
    );
    drop(output);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}
