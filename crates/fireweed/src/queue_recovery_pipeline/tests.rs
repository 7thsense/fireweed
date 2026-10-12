use super::*;
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    queue_admission::QueueAdmission,
    queue_log_record::encode_checked_publication_record,
    queue_planner::{RecordSnapshot, combine_deltas, plan_insert_vector},
    queue_publication::{PublicationReceipt, encode_publication_pinned},
    queue_schema_codec::{AdmittedQueueDefinition, encode_definition},
};
use fireweed_objectlog::{Api007GenesisRequest, FlushConfig};
fn schema() -> QueueSchema {
    QueueSchema {
        work: CollectionDefinition {
            fields: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        },
        state: Default::default(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    }
}
fn address() -> QueueAddress {
    QueueAddress {
        tenant: "recovery\0β".into(),
        queue: "work/λ".into(),
    }
}
async fn provision(
    store: &Arc<ObjectLogEngineStore>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
) {
    let request = Api007GenesisRequest {
        namespace: store.bind_api007_namespace(&address(), job).await.unwrap(),
        definition: AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), job)
            .unwrap(),
        candidate_secret: [17; 32],
        max_record_bytes: 1 << 20,
        metadata_bytes: 65536,
    };
    store
        .provision_api007_genesis(request, job, workers)
        .await
        .unwrap();
}
async fn append_insert(
    store: &Arc<ObjectLogEngineStore>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
) {
    let namespace = store.bind_api007_namespace(&address(), job).await.unwrap();
    let acquired = store
        .acquire_api007_state(namespace, job, workers)
        .await
        .unwrap();
    let record = insert_record(acquired.state().epoch, job);
    let namespace = acquired.into_namespace();
    assert!(
        store
            .append_api007_record(namespace, record, 1, job, workers)
            .await
            .unwrap()
            .outcome()
            .is_ok()
    );
}
fn insert_record(
    epoch: u64,
    job: &Arc<QueueReservation>,
) -> fireweed_engine::queue_log_record::OwnedQueueLogRecord {
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), job).unwrap();
    let request = InsertRequest {
        queue: address(),
        request_id: "insert".into(),
        items: vec![NewWork {
            key: "a".into(),
            priority: 9,
            available_at: 0,
            fields: Default::default(),
            body: bytes::Bytes::new(),
        }],
        mode: InsertMode::InsertOnly,
    };
    let before = RecordSnapshot::default();
    let mut after = before.clone();
    let (deltas, outcomes) =
        plan_insert_vector(&request, &mut after, &schema(), || Ok(10)).unwrap();
    let delta = combine_deltas(&deltas, 0).unwrap();
    let receipt = PublicationReceipt::insert(&request, &outcomes).unwrap();
    let frame = encode_publication_pinned(
        &address(),
        definition.pin(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        job,
    )
    .unwrap();
    encode_checked_publication_record(epoch, frame.as_bytes(), 1 << 20, job).unwrap()
}
fn empty_record(
    epoch: u64,
    sequence: u64,
    job: &Arc<QueueReservation>,
) -> fireweed_engine::queue_log_record::OwnedQueueLogRecord {
    use fireweed_engine::queue_planner::plan_commit_group;
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), job).unwrap();
    let before = RecordSnapshot {
        next_sequence: sequence,
        ..Default::default()
    };
    let mut after = before.clone();
    let group = CommitGroup {
        request_id: "empty-suffix".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
        administration: None,
    };
    let delta = plan_commit_group(&group, &mut after, 10, &schema()).unwrap();
    let queue = address();
    let receipt = PublicationReceipt::commit(&queue, &group, &delta.receipt).unwrap();
    let frame = encode_publication_pinned(
        &queue,
        definition.pin(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        job,
    )
    .unwrap();
    encode_checked_publication_record(epoch, frame.as_bytes(), 1 << 20, job).unwrap()
}
async fn append_empty(
    store: &Arc<ObjectLogEngineStore>,
    job: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
) {
    let namespace = store.bind_api007_namespace(&address(), job).await.unwrap();
    let acquired = store
        .acquire_api007_state(namespace, job, workers)
        .await
        .unwrap();
    let record = empty_record(acquired.state().epoch, 1, job);
    assert!(
        store
            .append_api007_record(acquired.into_namespace(), record, 2, job, workers)
            .await
            .unwrap()
            .outcome()
            .is_ok()
    );
}
#[tokio::test]
async fn competing_same_identity_claims_never_acknowledge_losing_tokens() {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-claim-race-{}-{}",
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
    let admission = QueueAdmission::new(4, 128 << 20).unwrap();
    let workers = ScratchWorkers::new(2, 4).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    provision(&store, &job, &workers).await;
    append_insert(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let request = ClaimRequest {
        queue: address(),
        request_id: "renew-race".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 1,
        max_bytes: 65536,
        fields: None,
        include_body: true,
    };
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_claim_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let spawn = || {
        let pipeline = pipeline.clone();
        let request = request.clone();
        let clock = clock.clone();
        let job = job.clone();
        tokio::spawn(async move {
            pipeline
                .claim(request, QueueLimits::default(), clock, job)
                .await
        })
    };
    let first = spawn();
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let second_entered = Arc::new(tokio::sync::Notify::new());
    let second_release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_claim_append.lock().unwrap() =
        Some((second_entered.clone(), second_release.clone()));
    let second = spawn();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        second_entered.notified(),
    )
    .await
    .unwrap();
    // The newer fence publishes first. The losing original preparation then
    // attempts its stale conditional append and resolves the winner's identity.
    second_release.notify_one();
    let b = second.await.unwrap();
    assert!(
        b.is_ok(),
        "the controlled newer-fence publication must resolve"
    );
    release.notify_one();
    let a = first.await.unwrap();
    *pipeline.before_claim_append.lock().unwrap() = None;
    let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let current = store.read_api007_state(ns, &job, &workers).await.unwrap();
    assert_eq!(
        current.state().next_offset,
        3,
        "race winner exists before final resolving claim"
    );
    drop(current);
    let resolved = pipeline
        .claim(request, QueueLimits::default(), clock, job.clone())
        .await
        .unwrap();
    assert_eq!(resolved.records().len(), 1);
    for result in [a, b] {
        match result {
            Ok(response) => assert_eq!(response.records(), resolved.records()),
            Err(QueueError::OutcomeUnknown(_)) => {}
            Err(other) => panic!("unexpected claim classification: {other:?}"),
        }
    }
    drop(resolved);
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        recovered.target_next_offset, 3,
        "exactly one original identity publication"
    );
    drop(recovered);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn replay_discards_stale_expiry_after_concurrent_durable_renewal() {
    use fireweed_conformance::queue_contract::TestClock;
    use fireweed_engine::{
        queue_fields::FieldCoverage,
        queue_planner::{RecordIdentity, StoredWork, lease_hash, plan_commit_group},
    };
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-renew-{}-{}",
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
    let admission = QueueAdmission::new(4, 128 << 20).unwrap();
    let workers = ScratchWorkers::new(2, 4).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    provision(&store, &job, &workers).await;
    append_insert(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let request = ClaimRequest {
        queue: address(),
        request_id: "renew-race".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 1,
        max_bytes: 65536,
        fields: None,
        include_body: true,
    };
    let original = pipeline
        .claim(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let work = original.records()[0].clone();
    drop(original);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_claim_replay.lock().unwrap() = Some((entered.clone(), release.clone()));
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = request.clone();
        let job = job.clone();
        let clock = clock.clone();
        async move {
            pipeline
                .claim(request, QueueLimits::default(), clock, job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let acquired = store
        .acquire_api007_state(ns, &job, &workers)
        .await
        .unwrap();
    let head = acquired.state().next_offset as u64;
    let mut before = RecordSnapshot {
        next_sequence: 1,
        ..Default::default()
    };
    before.work.insert(
        "a".into(),
        StoredWork {
            record: work.record.clone(),
            lease_hash: Some(lease_hash(&work.lease.token)),
            field_coverage: FieldCoverage::Complete,
            body_loaded: true,
        },
    );
    before.versions.insert(
        RecordIdentity {
            collection: None,
            key: "a".into(),
        },
        work.record.version,
    );
    let group = CommitGroup {
        request_id: "renew-between-coverage-and-replay".into(),
        claimed_updates: vec![ClaimedUpdate {
            key: "a".into(),
            lease: work.lease.clone(),
            expected_version: work.record.version,
            patch: WorkPatch::default(),
            disposition: Disposition::Retain {
                renew_nanos: Some(2_000_000),
            },
        }],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![],
        administration: None,
    };
    let mut after = before.clone();
    let delta = plan_commit_group(&group, &mut after, 900_000, &schema()).unwrap();
    let queue = address();
    let receipt = PublicationReceipt::commit(&queue, &group, &delta.receipt).unwrap();
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job).unwrap();
    let frame = encode_publication_pinned(
        &address(),
        definition.pin(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        &job,
    )
    .unwrap();
    let record =
        encode_checked_publication_record(acquired.state().epoch, frame.as_bytes(), 1 << 20, &job)
            .unwrap();
    drop(frame);
    assert!(
        store
            .append_api007_record(acquired.into_namespace(), record, head, &job, &workers)
            .await
            .unwrap()
            .outcome()
            .is_ok()
    );
    // Renewal is authoritative but deliberately absent from the projection.
    clock.set(1_100_000);
    release.notify_one();
    let renewed = caller.await.unwrap().unwrap();
    assert_eq!(renewed.records()[0].lease, work.lease);
    assert_eq!(renewed.records()[0].record.version, work.record.version + 1);
    assert_eq!(
        renewed.records()[0].record.lease_expires_at,
        Some(2_900_000)
    );
    drop(renewed);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn owned_claim_publishes_then_replays_original_identity_and_expiry() {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-claim-{}-{}",
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
    let admission = QueueAdmission::new(4, 128 << 20).unwrap();
    let workers = ScratchWorkers::new(2, 4).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    provision(&store, &job, &workers).await;
    append_insert(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let request = ClaimRequest {
        queue: address(),
        request_id: "owned-claim".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 2,
        max_bytes: 65536,
        fields: None,
        include_body: true,
    };
    // Many tiny encoded values have substantial resident vector headers.
    // Reject before even acquiring a durable fence or cloning the request.
    let mut tiny_values = request.clone();
    tiny_values.request_id = "admission-only".into();
    tiny_values.filter.fields.insert(
        "many".into(),
        Predicate::In(vec![FieldValue::Boolean(true); 20_000]),
    );
    let tight = QueueAdmission::new(1, 1 << 20).unwrap();
    let tight_job = Arc::new(tight.reserve(0).unwrap());
    let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let before = store
        .read_api007_state(ns, &job, &workers)
        .await
        .unwrap()
        .state();
    assert!(matches!(
        pipeline
            .claim(
                tiny_values,
                QueueLimits::default(),
                clock.clone(),
                tight_job
            )
            .await,
        Err(QueueError::OutcomeUnknown(_))
    ));
    let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let after = store
        .read_api007_state(ns, &job, &workers)
        .await
        .unwrap()
        .state();
    assert_eq!(
        (before.epoch, before.next_offset),
        (after.epoch, after.next_offset)
    );
    let response = pipeline
        .claim(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(response.records().len(), 1);
    assert_eq!(response.records()[0].record.work.key, "a");
    let expected = response.records().to_vec();
    drop(response);
    let replay = pipeline
        .claim(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.records(), expected);
    drop(replay);
    let mut changed = request.clone();
    changed.include_body = false;
    assert!(matches!(
        pipeline
            .claim(changed, QueueLimits::default(), clock.clone(), job.clone())
            .await,
        Err(QueueError::RequestConflict)
    ));
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 3);
    assert!(recovered.original_genesis_epoch < recovered.acquired_epoch);
    drop(recovered);
    drop(pipeline);
    let projection = QueueProjection::open(&root.join("rebuild.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store, projection, workers.clone(), 65536, 1 << 20).unwrap();
    let replay = pipeline
        .claim(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.records(), expected);
    drop(replay);
    clock.set(1_000_100);
    assert!(matches!(
        pipeline
            .claim(request, QueueLimits::default(), clock, job.clone())
            .await,
        Err(QueueError::ClaimExpired)
    ));
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        recovered.target_next_offset, 3,
        "expired identity must not append another token set"
    );
    drop(recovered);
    // Cancellation after preparation cannot abandon the original durable
    // publication. Its delayed completion must not acknowledge expired tokens.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_claim_append.lock().unwrap() = Some((entered.clone(), release.clone()));
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
    let cancelled_request = ClaimRequest {
        queue: address(),
        request_id: "cancelled-original".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000,
        max_records: 2,
        max_bytes: 65536,
        fields: None,
        include_body: true,
    };
    let delayed_clock = Arc::new(TestClock::default());
    delayed_clock.set(1_000_100);
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = cancelled_request.clone();
        let clock = delayed_clock.clone();
        async move {
            pipeline
                .claim(request, QueueLimits::default(), clock, cancelled_job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    delayed_clock.set(2_000_100);
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    *pipeline.before_claim_append.lock().unwrap() = None;
    assert!(matches!(
        pipeline
            .claim(
                cancelled_request,
                QueueLimits::default(),
                delayed_clock,
                job.clone()
            )
            .await,
        Err(QueueError::ClaimExpired)
    ));
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 4);
    drop(recovered);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn cold_prefix_cached_prefix_and_new_suffix_recover_to_exact_native_target() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-recovery-{}-{}",
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
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let workers = ScratchWorkers::new(1, 2).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    provision(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let first = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        (first.target_next_offset, first.history.insertion_sequence()),
        (1, 0)
    );
    let epoch = first.acquired_epoch;
    drop(first);
    append_insert(&store, &job, &workers).await;
    let second = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        (
            second.target_next_offset,
            second.history.insertion_sequence()
        ),
        (2, 1)
    );
    assert!(second.acquired_epoch > epoch);
    assert_eq!(second.history.cursor_secret().unwrap(), &[17; 32]);
    drop(second);
    let third = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        (third.target_next_offset, third.history.insertion_sequence()),
        (2, 1)
    );
    drop(third);
    append_empty(&store, &job, &workers).await;
    // A different but well-framed cache at the same cursor must reject its
    // prefix before touching the legitimate suffix.
    for kind in 0..2 {
        let bad_projection =
            QueueProjection::open(&root.join(format!("bad-prefix-{kind}.db")), &job, &workers)
                .await
                .unwrap();
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job)
                .unwrap();
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[17; 32], 1 << 20, &job).unwrap();
        bad_projection
            .apply(genesis, 0, &job, &workers)
            .await
            .unwrap();
        bad_projection
            .apply(
                if kind == 0 {
                    empty_record(1, 0, &job)
                } else {
                    insert_record(1, &job)
                },
                1,
                &job,
                &workers,
            )
            .await
            .unwrap();
        let bad_pipeline = QueueRecoveryPipeline::new(
            store.clone(),
            bad_projection.clone(),
            workers.clone(),
            65536,
            1 << 20,
        )
        .unwrap();
        assert!(
            matches!(bad_pipeline.recover(address(),job.clone()).await,Err(QueueError::OutcomeUnknown(message)) if message.contains("disagrees with authoritative prefix"))
        );
        let genesis =
            encode_checked_genesis(1, &address(), &definition, &[17; 32], 1 << 20, &job).unwrap();
        assert_eq!(
            bad_projection
                .checkpoint(genesis, &job, &workers)
                .await
                .unwrap()
                .unwrap()
                .next_offset,
            2
        );
        drop(definition);
    }
    let limited = QueueRecoveryPipeline::new(
        store.clone(),
        pipeline.projection.clone(),
        workers.clone(),
        65536,
        1,
    )
    .unwrap();
    assert!(matches!(
        limited.recover(address(), job.clone()).await,
        Err(QueueError::TooLarge)
    ));
    drop(limited);
    // Occupy the only native worker; the owned orchestration must survive
    // its caller disappearing while a subsequent stage is queued.
    let (entered_sender, entered_receiver) = tokio::sync::oneshot::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::channel();
    let blocking_workers = workers.clone();
    let blocker = tokio::spawn(async move {
        blocking_workers
            .run(move || {
                let _ = entered_sender.send(());
                release_receiver.recv().unwrap();
                Ok(())
            })
            .await
    });
    entered_receiver.await.unwrap();
    let acquire_entered = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_acquire.lock().unwrap() = Some(acquire_entered.clone());
    let recovery_pipeline = pipeline.clone();
    let recovery_job = job.clone();
    let caller =
        tokio::spawn(async move { recovery_pipeline.recover(address(), recovery_job).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        acquire_entered.notified(),
    )
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    drop(job);
    assert_eq!(pipeline.slots.available_permits(), 63);
    assert_eq!(admission.usage().unwrap().0, 1);
    assert!(admission.usage().unwrap().1 > 0);
    release_sender.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while admission.usage().unwrap() != (0, 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(pipeline.slots.available_permits(), 64);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    let job = Arc::new(admission.reserve(0).unwrap());
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job).unwrap();
    let genesis =
        encode_checked_genesis(1, &address(), &definition, &[17; 32], 1 << 20, &job).unwrap();
    assert_eq!(
        pipeline
            .projection
            .checkpoint(genesis, &job, &workers)
            .await
            .unwrap()
            .unwrap()
            .next_offset,
        3
    );
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    drop(pipeline);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn legacy_record_below_cached_cursor_never_establishes_coverage() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-legacy-prefix-{}-{}",
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
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema()).unwrap(), &job).unwrap();
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let acquired = store
        .acquire_api007_state(namespace, &job, &workers)
        .await
        .unwrap();
    let legacy = encode_checked_genesis(
        acquired.state().epoch,
        &address(),
        &definition,
        &[17; 32],
        1 << 20,
        &job,
    )
    .unwrap();
    assert!(
        store
            .append_api007_record(acquired.into_namespace(), legacy, 0, &job, &workers)
            .await
            .unwrap()
            .outcome()
            .is_ok()
    );
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let acquired = store
        .acquire_api007_state(namespace, &job, &workers)
        .await
        .unwrap();
    let checked_publication = empty_record(acquired.state().epoch, 0, &job);
    // FWQL2 has a 22-byte fixed header and 32-byte complete checksum.
    let legacy_publication = fireweed_engine::queue_log_record::encode_publication_record(
        acquired.state().epoch,
        &checked_publication.bytes()[22..checked_publication.bytes().len() - 32],
        1 << 20,
        &job,
    )
    .unwrap();
    assert!(
        store
            .append_api007_record(
                acquired.into_namespace(),
                legacy_publication,
                1,
                &job,
                &workers
            )
            .await
            .unwrap()
            .outcome()
            .is_ok()
    );
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let checked =
        encode_checked_genesis(1, &address(), &definition, &[17; 32], 1 << 20, &job).unwrap();
    projection.apply(checked, 0, &job, &workers).await.unwrap();
    projection
        .apply(checked_publication, 1, &job, &workers)
        .await
        .unwrap();
    let pipeline = QueueRecoveryPipeline::new(
        store.clone(),
        projection.clone(),
        workers.clone(),
        65536,
        1 << 20,
    )
    .unwrap();
    assert!(
        matches!(pipeline.recover(address(),job.clone()).await,Err(QueueError::OutcomeUnknown(message)) if message.contains("checksum policy"))
    );
    let checked =
        encode_checked_genesis(1, &address(), &definition, &[17; 32], 1 << 20, &job).unwrap();
    assert_eq!(
        projection
            .checkpoint(checked, &job, &workers)
            .await
            .unwrap()
            .unwrap()
            .next_offset,
        2
    );
    drop(definition);
    drop(job);
    workers.close_and_drain().await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
    drop(pipeline);
    drop(projection);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn receipt_read_discards_stale_absence_and_confirms_current_errors() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-receipt-cut-{}-{}",
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
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 2).unwrap();
    provision(&store, &job, &workers).await;
    append_insert(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.after_receipt_read.lock().unwrap() = Some((entered.clone(), release.clone()));
    let request = ReceiptReadRequest {
        queue: address(),
        request_ids: vec![
            "empty-suffix".into(),
            "absent".into(),
            "empty-suffix".into(),
        ],
        max_bytes: 65536,
    };
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = request.clone();
        let job = job.clone();
        async move {
            pipeline
                .read_commit(request, QueueLimits::default(), job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    append_empty(&store, &job, &workers).await;
    release.notify_one();
    let found = ReceiptLookup::Found(CommitReceipt {
        request_id: "empty-suffix".into(),
        outcome: GroupOutcome::Committed { versions: vec![] },
    });
    let expected = vec![found.clone(), ReceiptLookup::Absent, found];
    let result = caller.await.unwrap().unwrap();
    assert_eq!(result.receipts(), expected);
    drop(result);
    let mut too_small = request.clone();
    too_small.max_bytes = encoded_len(&expected).unwrap() - 1;
    assert!(matches!(
        pipeline
            .read_commit(too_small, QueueLimits::default(), job.clone())
            .await,
        Err(QueueError::TooLarge)
    ));
    // A confirmed absence is a successful immutable result, not a retry signal.
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let before = store
        .read_api007_state(namespace, &job, &workers)
        .await
        .unwrap()
        .state();
    let result = pipeline
        .read_commit(
            ReceiptReadRequest {
                queue: address(),
                request_ids: vec!["absent".into()],
                max_bytes: 2,
            },
            QueueLimits::default(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(result.receipts(), &[ReceiptLookup::Absent]);
    drop(result);
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let after = store
        .read_api007_state(namespace, &job, &workers)
        .await
        .unwrap()
        .state();
    assert_eq!(
        after.epoch,
        before.epoch + 1,
        "one recovery, no retry for confirmed absence"
    );
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn owned_insert_publishes_original_results_rebuilds_and_survives_caller_drop() {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-insert-{}-{}",
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
    let admission = QueueAdmission::new(4, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(2, 4).unwrap();
    provision(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let item = |key: &str, priority: i64| NewWork {
        key: key.into(),
        priority,
        available_at: 0,
        fields: Default::default(),
        body: bytes::Bytes::new(),
    };
    let request = InsertRequest {
        queue: address(),
        request_id: "owned-original".into(),
        mode: InsertMode::InsertOnly,
        items: vec![item("a", 1), item("b", 2)],
    };
    let response = pipeline
        .insert(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let original = response.outcomes().to_vec();
    drop(response);
    assert_eq!(
        original,
        vec![
            InsertOutcome::Inserted {
                key: "a".into(),
                version: 1
            },
            InsertOutcome::Inserted {
                key: "b".into(),
                version: 1
            }
        ]
    );
    let upsert = InsertRequest {
        queue: address(),
        request_id: "owned-upsert".into(),
        mode: InsertMode::Upsert,
        items: vec![item("a", 10), item("c", 3)],
    };
    let response = pipeline
        .insert(upsert, QueueLimits::default(), clock.clone(), job.clone())
        .await
        .unwrap();
    assert!(matches!(
        response.outcomes(),
        [
            InsertOutcome::Replaced { version: 2, .. },
            InsertOutcome::Inserted { version: 1, .. }
        ]
    ));
    drop(response);
    let replay = pipeline
        .insert(
            request.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.outcomes(), original);
    drop(replay);
    let mut changed = request.clone();
    changed.items[0].priority += 1;
    assert!(matches!(
        pipeline
            .insert(changed, QueueLimits::default(), clock.clone(), job.clone())
            .await,
        Err(QueueError::RequestConflict)
    ));
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 3);
    drop(recovered);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_insert_append.lock().unwrap() = Some((entered.clone(), release.clone()));
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
    let cancelled = InsertRequest {
        queue: address(),
        request_id: "cancelled-insert-original".into(),
        mode: InsertMode::InsertOnly,
        items: vec![item("d", 4)],
    };
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = cancelled.clone();
        let clock = clock.clone();
        async move {
            pipeline
                .insert(request, QueueLimits::default(), clock, cancelled_job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    *pipeline.before_insert_append.lock().unwrap() = None;
    let replay = pipeline
        .insert(
            cancelled,
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        replay.outcomes(),
        [InsertOutcome::Inserted { version: 1, .. }]
    ));
    drop(replay);
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 4);
    drop(recovered);
    drop(pipeline);
    // A fresh derived database must replay exact original authority, including
    // the original insert result whose current row has since been replaced.
    let projection = QueueProjection::open(&root.join("rebuilt.db"), &job, &workers)
        .await
        .unwrap();
    let rebuilt =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let replay = rebuilt
        .insert(request, QueueLimits::default(), clock, job.clone())
        .await
        .unwrap();
    assert_eq!(replay.outcomes(), original);
    drop(replay);
    drop(rebuilt);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn losing_insert_fence_resolves_winner_without_replanning_or_extra_publication() {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-insert-loser-{}-{}",
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
    let admission = QueueAdmission::new(2, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(1, 2).unwrap();
    provision(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_insert_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let clock = Arc::new(TestClock::default());
    clock.set(100);
    let request = InsertRequest {
        queue: address(),
        request_id: "insert".into(),
        mode: InsertMode::InsertOnly,
        items: vec![NewWork {
            key: "a".into(),
            priority: 9,
            available_at: 0,
            fields: Default::default(),
            body: bytes::Bytes::new(),
        }],
    };
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let job = job.clone();
        async move {
            pipeline
                .insert(request, QueueLimits::default(), clock, job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    // A newer authority publishes the same original identity while the first
    // producer's fully prepared record is paused. It cannot win that old fence.
    append_insert(&store, &job, &workers).await;
    release.notify_one();
    let resolved = caller.await.unwrap().unwrap();
    assert_eq!(
        resolved.outcomes(),
        [InsertOutcome::Inserted {
            key: "a".into(),
            version: 1
        }]
    );
    drop(resolved);
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(
        recovered.target_next_offset, 2,
        "losing producer must not publish or replan another insert"
    );
    drop(recovered);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn owned_commit_resolves_atomic_original_group_and_rejects_regrouped_retry() {
    use fireweed_conformance::queue_contract::TestClock;
    let root = std::env::temp_dir().join(format!(
        "fireweed-api007-commit-{}-{}",
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
    let admission = QueueAdmission::new(4, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let workers = ScratchWorkers::new(2, 4).unwrap();
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
    let pipeline =
        QueueRecoveryPipeline::new(store.clone(), projection, workers.clone(), 65536, 1 << 20)
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
    let claimed = pipeline
        .claim(
            ClaimRequest {
                queue: address(),
                request_id: "claim-commit".into(),
                filter: Filter::default(),
                order: ClaimOrder::PriorityAscending,
                eligible_at: 0,
                lease_nanos: 1_000_000,
                max_records: 1,
                max_bytes: 65536,
                fields: None,
                include_body: true,
            },
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let row = claimed.records()[0].clone();
    drop(claimed);
    let group = CommitGroup {
        request_id: "atomic-original".into(),
        claimed_updates: vec![ClaimedUpdate {
            key: "a".into(),
            lease: row.lease,
            expected_version: row.record.version,
            patch: WorkPatch::default(),
            disposition: Disposition::Complete,
        }],
        state_updates: vec![StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Absent,
            change: StateChange::Put {
                fields: Default::default(),
                value: vec![7, 8].into(),
            },
        }],
        work_inserts: vec![work("next")],
        work_updates: vec![],
        administration: None,
    };
    let result = pipeline
        .commit(
            address(),
            group.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let original = result.receipts().to_vec();
    drop(result);
    assert!(
        matches!(&original[..], [ReceiptLookup::Found(CommitReceipt {outcome:GroupOutcome::Committed {versions}, ..})] if versions.len()==3)
    );
    // Later expiry cannot turn this original immutable success into a new plan.
    clock.set(2_000_000);
    let result = pipeline
        .commit(
            address(),
            group.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(result.receipts(), original);
    drop(result);
    let mut changed = group.clone();
    changed.work_inserts[0].priority += 1;
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
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 4);
    assert_eq!(recovered.history.insertion_sequence(), 2);
    drop(recovered);
    // Caller cancellation after preparation cannot abandon atomic publication.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_commit_append.lock().unwrap() = Some((entered.clone(), release.clone()));
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
    let cancelled = CommitGroup {
        request_id: "cancelled-commit".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![work("cancel-next")],
        work_updates: vec![],
        administration: None,
    };
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let group = cancelled.clone();
        let clock = clock.clone();
        async move {
            pipeline
                .commit(
                    address(),
                    group,
                    QueueLimits::default(),
                    clock,
                    cancelled_job,
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    let response = pipeline
        .commit(
            address(),
            cancelled,
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        response.receipts(),
        [ReceiptLookup::Found(CommitReceipt {
            outcome: GroupOutcome::Committed { .. },
            ..
        })]
    ));
    drop(response);
    // A stale prepared publisher loses its fence and resolves the winner's
    // original receipt, without replanning or inserting an extra log record.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.before_commit_append.lock().unwrap() = Some((entered.clone(), release.clone()));
    let competing = CommitGroup {
        request_id: "competing-commit".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![work("winner-next")],
        work_updates: vec![],
        administration: None,
    };
    let stale = tokio::spawn({
        let pipeline = pipeline.clone();
        let group = competing.clone();
        let clock = clock.clone();
        let job = job.clone();
        async move {
            pipeline
                .commit(address(), group, QueueLimits::default(), clock, job.clone())
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let winner = pipeline
        .commit(
            address(),
            competing,
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let winner_receipts = winner.receipts().to_vec();
    drop(winner);
    release.notify_one();
    let stale = tokio::time::timeout(std::time::Duration::from_secs(30), stale)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stale.receipts(), winner_receipts);
    drop(stale);
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 6);
    assert_eq!(recovered.history.insertion_sequence(), 4);
    drop(recovered);
    // Discard an already-decoded original response if its authoritative cut
    // advances before completion, then resolve the same original identity.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.after_commit_replay.lock().unwrap() = Some((entered.clone(), release.clone()));
    let retry = tokio::spawn({
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
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let advance = CommitGroup {
        request_id: "advance-replay-cut".into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![work("cut-next")],
        work_updates: vec![],
        administration: None,
    };
    drop(
        pipeline
            .commit(
                address(),
                advance,
                QueueLimits::default(),
                clock.clone(),
                job.clone(),
            )
            .await
            .unwrap(),
    );
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let state = store
        .read_api007_state(namespace, &job, &workers)
        .await
        .unwrap();
    let advanced_epoch = state.state().epoch;
    drop(state);
    release.notify_one();
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), retry)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.receipts(), original);
    drop(response);
    let namespace = store.bind_api007_namespace(&address(), &job).await.unwrap();
    let state = store
        .read_api007_state(namespace, &job, &workers)
        .await
        .unwrap();
    assert!(
        state.state().epoch > advanced_epoch,
        "stale receipt must trigger original-identity recovery before returning"
    );
    drop(state);
    let recovered = pipeline.recover(address(), job.clone()).await.unwrap();
    assert_eq!(recovered.target_next_offset, 7);
    assert_eq!(recovered.history.insertion_sequence(), 5);
    drop(recovered);
    // A complete state read discards the old value when an atomic workflow
    // transition becomes authoritative after decoding and before completion.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *pipeline.after_state_read.lock().unwrap() = Some((entered.clone(), release.clone()));
    let state_request = StateReadRequest {
        queue: address(),
        collection: "workflow".into(),
        keys: vec!["run".into(), "absent".into(), "run".into()],
        max_bytes: 65536,
    };
    let read = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = state_request.clone();
        let job = job.clone();
        async move {
            pipeline
                .read_state(request, QueueLimits::default(), job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    let update = CommitGroup {
        request_id: "workflow-state-advance".into(),
        claimed_updates: vec![],
        state_updates: vec![StateUpdate {
            collection: "workflow".into(),
            key: "run".into(),
            expect: StateExpect::Version(1),
            change: StateChange::Put {
                fields: Default::default(),
                value: vec![9, 10].into(),
            },
        }],
        work_inserts: vec![],
        work_updates: vec![],
        administration: None,
    };
    drop(
        pipeline
            .commit(
                address(),
                update,
                QueueLimits::default(),
                clock.clone(),
                job.clone(),
            )
            .await
            .unwrap(),
    );
    release.notify_one();
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response.records()[0], response.records()[2]);
    assert_eq!(response.records()[1], None);
    let state = response.records()[0].as_ref().unwrap();
    assert_eq!(state.version, 2);
    assert_eq!(state.value.as_ref(), [9, 10]);
    drop(response);
    let (terminal_tx, mut terminal_rx) = tokio::sync::oneshot::channel();
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
    *pipeline.after_state_read.lock().unwrap() = Some((entered.clone(), release.clone()));
    let caller = tokio::spawn({
        let pipeline = pipeline.clone();
        let request = state_request.clone();
        async move {
            pipeline
                .read_state(request, QueueLimits::default(), cancelled_job)
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    caller.abort();
    assert!(matches!(caller.await,Err(error) if error.is_cancelled()));
    assert!(matches!(
        terminal_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(
        admission.usage().unwrap().0,
        2,
        "cancelled state read retains its operation until completion"
    );
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(30), terminal_rx)
        .await
        .unwrap()
        .unwrap();
    // Rebuild into a fresh derived projection through original genesis/history.
    let rebuilt = QueueProjection::open(&root.join("rebuilt.db"), &job, &workers)
        .await
        .unwrap();
    let rebuilt =
        QueueRecoveryPipeline::new(store.clone(), rebuilt, workers.clone(), 65536, 1 << 20)
            .unwrap();
    let state = rebuilt
        .read_state(state_request, QueueLimits::default(), job.clone())
        .await
        .unwrap();
    assert_eq!(state.records()[0].as_ref().unwrap().value.as_ref(), [9, 10]);
    assert_eq!(state.records()[0].as_ref().unwrap().version, 2);
    drop(state);
    let result = rebuilt
        .commit(address(), group, QueueLimits::default(), clock, job.clone())
        .await
        .unwrap();
    assert_eq!(result.receipts(), original);
    drop(result);
    drop(rebuilt);
    drop(pipeline);
    workers.close_and_drain().await.unwrap();
    drop(job);
    drop(store);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}
