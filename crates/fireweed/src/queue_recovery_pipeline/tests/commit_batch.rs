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
struct FailThirdClock {
    inner: TestClock,
    calls: std::sync::atomic::AtomicUsize,
}
impl Clock for FailThirdClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
            panic!("injected suffix clock failure after physical prefix");
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
    let mut pipeline =
        test_pipeline_options(store.clone(), projection, workers.clone(), options, &root)
            .await
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let before_batch = pipeline.diagnostics.snapshot();
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
    let measured = pipeline
        .diagnostics
        .snapshot()
        .checked_delta(before_batch)
        .unwrap();
    assert_eq!(
        (
            measured.attempts,
            measured.succeeded,
            measured.admitted_raw_records
        ),
        (2, 2, 3)
    );
    let replay_before = pipeline.diagnostics.snapshot();
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
    let replay = pipeline
        .diagnostics
        .snapshot()
        .checked_delta(replay_before)
        .unwrap();
    assert_eq!((replay.attempts, replay.admitted_raw_records), (1, 3));
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

    let failing = Arc::new(FailThirdClock {
        inner: TestClock::default(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let prefix = batch(vec![
        split_group("durable-prefix", 8, 57),
        split_group("failed-suffix", 9, 59),
    ]);
    let cap = first_group_record_bytes(&pipeline, &prefix, clock.clone(), job.clone()).await;
    Arc::get_mut(&mut pipeline).unwrap().hard_record_bytes = cap;
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

#[cfg(target_os = "linux")]
async fn confirmed_handoff_recovers_after_head_and_fence_changes(s3: bool) {
    for kind in ["head", "fence", "cancel"] {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("fireweed-handoff-{s3}-{kind}-{nonce}"));
        std::fs::create_dir_all(&root).unwrap();
        let prefix = s3.then(|| format!("api007-handoff-{kind}-{nonce}"));
        let store = Arc::new(native_log_fixture(&root, prefix.as_deref()).await);
        let admission = QueueAdmission::new(2, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(2, 2).unwrap();
        provision(&store, &job, &workers).await;
        append_insert(&store, &job, &workers).await;
        let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
            .await
            .unwrap();
        let mut pipeline = test_pipeline(
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
        let split_request = batch(vec![
            split_group("first", 1, 7),
            split_group("next", 2, 8),
            split_group("tail", 3, 9),
        ]);
        let cap =
            first_group_record_bytes(&pipeline, &split_request, clock.clone(), job.clone()).await;
        Arc::get_mut(&mut pipeline).unwrap().hard_record_bytes = cap;
        let recovery_before = pipeline.diagnostics.snapshot();
        let initial_entered = Arc::new(tokio::sync::Notify::new());
        let initial_release = Arc::new(tokio::sync::Notify::new());
        let second_entered = Arc::new(tokio::sync::Notify::new());
        let second_release = Arc::new(tokio::sync::Notify::new());
        let pause = if kind != "fence" {
            &pipeline.before_handoff_check
        } else {
            &pipeline.after_handoff_check
        };
        *pause.lock().unwrap() = Some((initial_entered.clone(), initial_release.clone()));
        let caller = tokio::spawn({
            let pipeline = pipeline.clone();
            let job = job.clone();
            let clock = clock.clone();
            async move {
                pipeline
                    .commit_batch(
                        batch(vec![
                            split_group("first", 1, 7),
                            split_group("next", 2, 8),
                            split_group("tail", 3, 9),
                        ]),
                        QueueLimits::default(),
                        clock,
                        job,
                    )
                    .await
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            initial_entered.notified(),
        )
        .await
        .unwrap();
        *pause.lock().unwrap() = Some((second_entered.clone(), second_release.clone()));
        initial_release.notify_one();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            second_entered.notified(),
        )
        .await
        .unwrap();
        assert_eq!(
            pipeline
                .diagnostics
                .snapshot()
                .checked_delta(recovery_before)
                .unwrap()
                .succeeded,
            2,
            "initial cut and first post-publication cut completed"
        );
        if kind == "head" {
            let other_projection = QueueProjection::open(&root.join("other.db"), &job, &workers)
                .await
                .unwrap();
            let other = test_pipeline(
                store.clone(),
                other_projection,
                workers.clone(),
                65536,
                1 << 20,
                &root,
            )
            .await
            .unwrap();
            drop(
                other
                    .commit(
                        address(),
                        patch_group("next", 2, 88),
                        QueueLimits::default(),
                        clock.clone(),
                        job.clone(),
                    )
                    .await
                    .unwrap(),
            );
            drop(other);
        } else if kind == "fence" {
            let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
            drop(
                store
                    .acquire_api007_state(namespace, &job, &workers)
                    .await
                    .unwrap(),
            );
        }
        if kind == "cancel" {
            caller.abort();
            assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
            second_release.notify_one();
            tokio::time::timeout(std::time::Duration::from_secs(60), async {
                while pipeline.operation_slots.available_permits() != 64 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                pipeline
                    .diagnostics
                    .snapshot()
                    .checked_delta(recovery_before)
                    .unwrap()
                    .succeeded,
                4
            );
        } else {
            second_release.notify_one();
            let result = tokio::time::timeout(std::time::Duration::from_secs(60), caller)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(
                result.receipts()[0].outcome,
                GroupOutcome::Committed { .. }
            ));
            if kind == "head" {
                assert_eq!(result.receipts()[1].outcome, GroupOutcome::RequestConflict);
            } else {
                assert!(matches!(
                    result.receipts()[1].outcome,
                    GroupOutcome::Committed { .. }
                ));
            }
            assert!(matches!(
                result.receipts()[2].outcome,
                GroupOutcome::Committed { .. }
            ));
        }
        let measured = pipeline
            .diagnostics
            .snapshot()
            .checked_delta(recovery_before)
            .unwrap();
        if kind == "fence" {
            assert_eq!(
                (
                    measured.attempts,
                    measured.succeeded,
                    measured.admitted_raw_records
                ),
                (5, 5, 5)
            );
        }
        let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
        assert_eq!(
            recovered.target_next_offset, 5,
            "exactly one source original per declared ID"
        );
        drop(recovered);
        let raw = pipeline
            .read_commit(
                ReceiptReadRequest {
                    queue: address(),
                    request_ids: vec!["first".into(), "next".into(), "tail".into()],
                    max_bytes: 1 << 20,
                },
                QueueLimits::default(),
                job.clone(),
            )
            .await
            .unwrap();
        assert!(
            raw.receipts()
                .iter()
                .all(|receipt| matches!(receipt, ReceiptLookup::Found(_)))
        );
        drop(raw);
        drop(pipeline);
        workers.close_and_drain().await.unwrap();
        drop(workers);
        drop(job);
        drop(store);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn confirmed_batch_handoff_recovers_head_and_fence_races_local() {
    confirmed_handoff_recovers_after_head_and_fence_changes(false).await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn confirmed_batch_handoff_recovers_head_and_fence_races_real_s3() {
    confirmed_handoff_recovers_after_head_and_fence_changes(true).await;
}

#[tokio::test]
async fn batch_continuation_empty_suffix_preserves_historical_epoch_and_rebuilds_missing_projection()
 {
    let root = std::env::temp_dir().join(format!(
        "fireweed-continuation-{}-{}",
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
    let behind_projection = QueueProjection::open(&root.join("behind.db"), &job, &workers)
        .await
        .unwrap();
    let behind = test_pipeline(
        store.clone(),
        behind_projection,
        workers.clone(),
        65536,
        1 << 20,
        &root,
    )
    .await
    .unwrap();
    drop(behind.recover(address(), job.clone()).await.unwrap());
    append_insert(&store, &job, &workers).await;
    let options = QueuePipelineOptions {
        operations: 1,
        metadata_bytes: 65536,
        hard_record_bytes: 1 << 20,
    };
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        test_pipeline_options(store.clone(), projection, workers.clone(), options, &root)
            .await
            .unwrap();
    let request = Arc::new(batch(vec![
        patch_group("one", 1, 7),
        patch_group("two", 2, 8),
    ]));
    let old = pipeline
        .recover_batch(request.clone(), job.clone())
        .await
        .unwrap();
    let historical_epoch = old.history.historical_epoch();
    let previous_fence = old.acquired_epoch;
    let before = pipeline.diagnostics.snapshot();
    let continued = pipeline
        .recover_batch_after(request.clone(), old, job.clone())
        .await
        .unwrap();
    assert_eq!(continued.history.historical_epoch(), historical_epoch);
    assert!(continued.acquired_epoch > previous_fence);
    assert_eq!(
        pipeline
            .diagnostics
            .snapshot()
            .checked_delta(before)
            .unwrap()
            .admitted_raw_records,
        0
    );
    // The source affinity is the store, so an empty replacement projection can
    // rebuild under the same fresh acquire even with a one-slot pipeline.
    let fresh_projection = QueueProjection::open(&root.join("fresh.db"), &job, &workers)
        .await
        .unwrap();
    let fresh = test_pipeline_options(
        store.clone(),
        fresh_projection,
        workers.clone(),
        options,
        &root,
    )
    .await
    .unwrap();
    let rebuilt = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        fresh.recover_batch_after(request.clone(), continued, job.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rebuilt.history.next_offset(), 2);
    assert_eq!(rebuilt.history.historical_epoch(), historical_epoch);
    assert_eq!(fresh.diagnostics.snapshot().admitted_raw_records, 2);
    let rebuilt = behind
        .recover_batch_after(request.clone(), rebuilt, job.clone())
        .await
        .unwrap();
    assert_eq!(rebuilt.history.next_offset(), 2);
    assert_eq!(rebuilt.history.historical_epoch(), historical_epoch);
    // One initial genesis read plus the two-record fallback rebuild.
    assert_eq!(behind.diagnostics.snapshot().admitted_raw_records, 3);
    let mut changed = (*request).clone();
    changed.groups[1].work_updates[0].key = "changed".into();
    assert!(matches!(
        fresh
            .recover_batch_after(Arc::new(changed), rebuilt, job.clone())
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    drop(behind);
    drop(fresh);
    drop(pipeline);
    drop(store);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn submission_resolution_does_not_retry_failed_continuation_into_fenced_absence() {
    struct RewoundMutation {
        pipeline: Arc<QueueRecoveryPipeline>,
        job: Arc<QueueReservation>,
        queue: QueueAddress,
        calls: std::sync::atomic::AtomicUsize,
        replay_retry: bool,
    }
    impl super::super::mutation::OriginalMutation for RewoundMutation {
        type Output = ();
        fn pipeline(&self) -> &Arc<QueueRecoveryPipeline> {
            &self.pipeline
        }
        fn address(&self) -> &QueueAddress {
            &self.queue
        }
        fn identity(&self) -> (u8, &str) {
            (2, "one")
        }
        fn job(&self) -> &Arc<QueueReservation> {
            &self.job
        }
        fn continuation_enabled(&self) -> bool {
            true
        }
        fn recover_from<'a>(
            &'a self,
            previous: Option<RecoveredQueue>,
        ) -> fireweed_engine::queue_contract::QueueFuture<'a, RecoveredQueue> {
            Box::pin(async move {
                let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert!(
                    previous.is_some(),
                    "must not forget the known authority floor"
                );
                if self.replay_retry && call == 0 {
                    return Ok(previous.unwrap());
                }
                Err(unknown(
                    "known continuation authority disappeared or regressed",
                ))
            })
        }
        fn replay<'a>(
            &'a self,
            _: &'a RecoveredQueue,
        ) -> fireweed_engine::queue_contract::QueueFuture<'a, Option<()>> {
            Box::pin(async move {
                assert!(self.replay_retry);
                assert_eq!(self.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
                Err(unknown("injected replay head advance"))
            })
        }
    }
    let root = std::env::temp_dir().join(format!(
        "fireweed-continuation-resolution-{}-{}",
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
    let job = Arc::new(
        QueueAdmission::new(1, 256 << 20)
            .unwrap()
            .reserve(0)
            .unwrap(),
    );
    let workers = ScratchWorkers::new(1, 1).unwrap();
    provision(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline = test_pipeline(store.clone(), projection, workers, 65536, 1 << 20, &root)
        .await
        .unwrap();
    for replay_retry in [false, true] {
        let previous = pipeline
            .recover_batch(Arc::new(batch(vec![patch_group("one", 1, 7)])), job.clone())
            .await
            .unwrap();
        let mutation = RewoundMutation {
            pipeline: pipeline.clone(),
            job: job.clone(),
            queue: address(),
            calls: Default::default(),
            replay_retry,
        };
        let result = super::super::mutation::resolve_submission_after(
            &mutation,
            Some(previous.acquired_epoch),
            None,
            Some(previous),
        )
        .await;
        assert!(matches!(result, Err(QueueError::OutcomeUnknown(_))));
        assert_eq!(
            mutation.calls.load(std::sync::atomic::Ordering::SeqCst),
            if replay_retry { 2 } else { 1 }
        );
        drop(mutation);
    }
    drop(pipeline);
    drop(store);
    drop(job);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn batch_continuation_rejects_real_valid_rewound_and_vanished_authority() {
    for vanished in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "fireweed-continuation-rewind-{vanished}-{}-{}",
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
        let job = Arc::new(
            QueueAdmission::new(1, 256 << 20)
                .unwrap()
                .reserve(0)
                .unwrap(),
        );
        let workers = ScratchWorkers::new(1, 1).unwrap();
        provision(&store, &job, &workers).await;
        let index_dir = root.join("log/fwmeta/manifest/index");
        let paths = std::fs::read_dir(index_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_none())
            .collect::<Vec<_>>();
        let saved_indexes = paths
            .iter()
            .map(|path| {
                let value: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                (path.clone(), value)
            })
            .collect::<Vec<_>>();
        append_insert(&store, &job, &workers).await;
        let (path, mut saved) = saved_indexes
            .into_iter()
            .find(|(path, _)| {
                let now: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                now["next_offset"] == 2
            })
            .unwrap();
        assert_eq!(saved["next_offset"], 1);
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
        let request = Arc::new(batch(vec![patch_group("one", 1, 7)]));
        let previous = pipeline
            .recover_batch(request.clone(), job.clone())
            .await
            .unwrap();
        assert_eq!(previous.target_next_offset, 2);
        saved["epoch"] = (previous.acquired_epoch + 10).into();
        if vanished {
            saved["next_offset"] = 0.into();
            saved["entries"] = serde_json::json!([]);
        }
        // Atomic replacement of an idle test-only index presents a genuinely
        // valid older prefix with a HIGHER fence to the SAME live store Arc.
        let temporary = path.with_extension("rewind");
        std::fs::write(&temporary, serde_json::to_vec(&saved).unwrap()).unwrap();
        std::fs::rename(temporary, &path).unwrap();
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let actual = store
            .read_api007_state(namespace, &job, &workers)
            .await
            .unwrap();
        assert_eq!(actual.state().next_offset, if vanished { 0 } else { 1 });
        assert!(actual.state().epoch > previous.acquired_epoch);
        drop(actual);
        // An empty cache cannot mask the known-source guard by being ahead.
        let fresh_projection = QueueProjection::open(&root.join("fresh.db"), &job, &workers)
            .await
            .unwrap();
        let fresh = test_pipeline(
            store.clone(),
            fresh_projection,
            workers.clone(),
            65536,
            1 << 20,
            &root,
        )
        .await
        .unwrap();
        assert!(matches!(
            fresh
                .recover_batch_after(request, previous, job.clone())
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(fresh.diagnostics.snapshot().admitted_raw_records, 0);
        let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let unchanged = store
            .read_api007_state(namespace, &job, &workers)
            .await
            .unwrap();
        assert_eq!(unchanged.state().next_offset, if vanished { 0 } else { 1 });
        assert_eq!(unchanged.state().epoch, saved["epoch"].as_u64().unwrap());
        drop(unchanged);
        drop(fresh);
        drop(pipeline);
        drop(store);
        drop(job);
        std::fs::remove_dir_all(root).unwrap();
    }
}

// Derive a real physical boundary from the admitted wire encoder, not a test-only
// group-count limit. The helper publishes nothing and uses a normal clock.
async fn first_group_record_bytes(
    pipeline: &Arc<QueueRecoveryPipeline>,
    request: &CommitBatch,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
) -> u64 {
    let prepared = sized_pack(pipeline, request, clock, job, vec![0]).await;
    let (record, provisional, _) = prepared.into_parts();
    let cap = record.bytes().len() as u64;
    drop(provisional);
    drop(record);
    cap
}

async fn sized_pack(
    pipeline: &Arc<QueueRecoveryPipeline>,
    request: &CommitBatch,
    clock: Arc<dyn Clock>,
    job: Arc<QueueReservation>,
    positions: Vec<usize>,
) -> fireweed_turso::queue_projection::PreparedOrdinaryPack {
    let request = Arc::new(request.clone());
    let covered = pipeline
        .recover_batch(request.clone(), job.clone())
        .await
        .unwrap();
    let prepared = pipeline
        .projection
        .prepare_ordinary_pack(
            fireweed_turso::queue_projection::CommitPackPreparationInput {
                request: request.clone(),
                positions,
                originals: covered.original_authority.clone().unwrap(),
                states: covered.group_state_authority.clone().unwrap(),
                genesis: pipeline
                    .original_genesis(&request.queue, &covered, &job)
                    .unwrap(),
                limits: QueueLimits::default(),
                required_progress: (
                    covered.target_next_offset,
                    covered.history.insertion_sequence(),
                    covered.history.historical_epoch(),
                ),
            },
            covered.acquired_epoch,
            pipeline.hard_record_bytes,
            clock,
            &job,
            &pipeline.workers,
        )
        .await
        .unwrap();
    let fireweed_turso::queue_projection::OrdinaryPackPreparation::Prepared(prepared) = prepared
    else {
        panic!("first group must fit");
    };
    prepared
}

fn split_group(id: &str, version: u64, priority: i64) -> CommitGroup {
    let mut group = patch_group(id, version, priority);
    group.work_inserts.push(NewWork {
        key: format!("split-{id}"),
        priority: 0,
        available_at: 0,
        fields: Default::default(),
        body: vec![23; 16 << 10].into(),
    });
    group
}

async fn packed_original_races(s3: bool, cases: &[&str]) {
    use fireweed_core::queue_administration::*;
    for &kind in cases {
        let root = std::env::temp_dir().join(format!(
            "fireweed-pack-race-{kind}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let prefix = s3.then(|| {
            format!(
                "api007-packed-race-{kind}-{}",
                root.file_name().unwrap().to_string_lossy()
            )
        });
        let store = Arc::new(native_log_fixture(&root, prefix.as_deref()).await);
        let admission = QueueAdmission::new(2, 256 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let workers = ScratchWorkers::new(2, 2).unwrap();
        provision(&store, &job, &workers).await;
        append_insert(&store, &job, &workers).await;
        let projection = QueueProjection::open(&root.join("main.db"), &job, &workers)
            .await
            .unwrap();
        let mut pipeline = test_pipeline(
            store.clone(),
            projection,
            workers.clone(),
            65536,
            1 << 20,
            &root,
        )
        .await
        .unwrap();
        let projection = QueueProjection::open(&root.join("other.db"), &job, &workers)
            .await
            .unwrap();
        let other = test_pipeline(
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
        let insert = |id: &str, priority| CommitGroup {
            request_id: id.into(),
            administration: None,
            claimed_updates: vec![],
            work_updates: vec![],
            state_updates: vec![],
            work_inserts: vec![NewWork {
                key: format!("work-{id}"),
                priority,
                available_at: 0,
                fields: Default::default(),
                body: bytes::Bytes::from_static(b"opaque"),
            }],
        };
        let mut a = insert("z-first", 1);
        let mut middle = insert("a-middle", 2);
        let mut c = insert("β-tail", 3);
        if kind == "outside-match" {
            for group in [&mut a, &mut middle, &mut c] {
                group.work_inserts[0].body = vec![23; 16 << 10].into();
            }
            let mut cap = 0;
            for group in [&a, &middle, &c] {
                cap = cap.max(
                    first_group_record_bytes(
                        &pipeline,
                        &batch(vec![group.clone()]),
                        clock.clone(),
                        job.clone(),
                    )
                    .await,
                );
            }
            Arc::get_mut(&mut pipeline).unwrap().hard_record_bytes = cap;
            let prepared = sized_pack(
                &pipeline,
                &batch(vec![a.clone(), middle.clone(), c.clone()]),
                clock.clone(),
                job.clone(),
                vec![0, 1, 2],
            )
            .await;
            assert_eq!(
                prepared.response().positions(),
                &[0],
                "winner C must lie outside the actual submitted prefix"
            );
            drop(prepared);
        }
        let groups = if kind == "fp4-conflict" {
            let mut admin = middle.clone();
            admin.work_inserts.clear();
            admin.administration = Some(AdministrativeCommit {
                max_result_bytes: 65536,
                intent: AdministrativeIntent {
                    evaluated_at: 0,
                    before_fields: None,
                    operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                        key: "missing".into(),
                        expected_version: None,
                        precondition: WorkSelector::all(vec![]),
                        action: AdministrativeAction::Delete {
                            lease_guard: AdministrativeLeaseGuard::RejectActive,
                        },
                    }]),
                },
            });
            vec![admin, a.clone(), c.clone()]
        } else {
            vec![a.clone(), middle.clone(), c.clone()]
        };
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let recovery_before = pipeline.diagnostics.snapshot();
        let hook = if kind == "post-replay-head" {
            &pipeline.after_commit_replay
        } else {
            &pipeline.before_commit_append
        };
        *hook.lock().unwrap() = Some((entered.clone(), release.clone()));
        let request = batch(groups);
        let caller = tokio::spawn({
            let pipeline = pipeline.clone();
            let request = request.clone();
            let job = job.clone();
            let clock = clock.clone();
            async move {
                pipeline
                    .commit_batch(request, QueueLimits::default(), clock, job)
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
            .await
            .unwrap();
        let winner = if kind == "post-replay-head" {
            insert("external-unrelated", 99)
        } else if kind == "outside-match" {
            c.clone()
        } else if kind == "matching-middle" {
            middle.clone()
        } else {
            insert("a-middle", 99)
        };
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
        let output = tokio::time::timeout(std::time::Duration::from_secs(30), caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            output
                .receipts()
                .iter()
                .map(|r| &r.request_id)
                .collect::<Vec<_>>(),
            request
                .groups
                .iter()
                .map(|g| &g.request_id)
                .collect::<Vec<_>>()
        );
        let middle_position = if kind == "fp4-conflict" {
            0
        } else if kind == "outside-match" {
            2
        } else {
            1
        };
        for (p, receipt) in output.receipts().iter().enumerate() {
            if p == middle_position && matches!(kind, "conflicting-middle" | "fp4-conflict") {
                assert_eq!(receipt.outcome, GroupOutcome::RequestConflict);
            } else {
                assert!(matches!(receipt.outcome, GroupOutcome::Committed { .. }));
            }
        }
        if kind == "post-replay-head" {
            assert_eq!(
                pipeline
                    .diagnostics
                    .snapshot()
                    .checked_delta(recovery_before)
                    .unwrap()
                    .succeeded,
                3,
                "stale replay must continue from its checked cut before ACK"
            );
        }
        let expected = output.receipts().to_vec();
        drop(output);
        let replay = pipeline
            .commit_batch(request, QueueLimits::default(), clock, job.clone())
            .await
            .unwrap();
        assert_eq!(replay.receipts(), expected);
        drop(replay);
        let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
        assert_eq!(
            recovered.target_next_offset,
            if matches!(kind, "fp4-conflict" | "post-replay-head") {
                4
            } else {
                5
            },
            "failed speculative pack must publish no absent sibling effect"
        );
        drop(recovered);
        drop(other);
        drop(pipeline);
        workers.close_and_drain().await.unwrap();
        drop(workers);
        drop(job);
        drop(store);
        assert_eq!(admission.usage().unwrap(), (0, 0));
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn packed_submission_refreshes_middle_outside_and_fp4_originals_local() {
    packed_original_races(
        false,
        &[
            "matching-middle",
            "conflicting-middle",
            "outside-match",
            "fp4-conflict",
        ],
    )
    .await;
}
#[tokio::test]
async fn packed_submission_refreshes_middle_outside_and_fp4_originals_real_s3() {
    packed_original_races(
        true,
        &[
            "matching-middle",
            "conflicting-middle",
            "outside-match",
            "fp4-conflict",
        ],
    )
    .await;
}

#[tokio::test]
async fn packed_replay_head_advancement_retains_checked_floor_local() {
    packed_original_races(false, &["post-replay-head"]).await;
}
#[tokio::test]
async fn packed_replay_head_advancement_retains_checked_floor_real_s3() {
    packed_original_races(true, &["post-replay-head"]).await;
}
