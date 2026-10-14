//! Actual insert/claim same-operation suffix and ownership controls.
use super::*;
#[tokio::test]
async fn single_original_continuation_preserves_presence_and_reads_only_suffix() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-single-continuation-{}-{}",
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
    let workers = ScratchWorkers::new(1, 1).unwrap();
    provision(&store, &job, &workers).await;
    let projection = QueueProjection::open(&root.join("projection.db"), &job, &workers)
        .await
        .unwrap();
    let pipeline = test_pipeline_options(
        store.clone(),
        projection,
        workers.clone(),
        QueuePipelineOptions {
            operations: 1,
            metadata_bytes: 65536,
            hard_record_bytes: 1 << 20,
        },
        &root,
    )
    .await
    .unwrap();
    let clock = Arc::new(fireweed_conformance::queue_contract::TestClock::default());
    let inserted = InsertRequest {
        queue: address(),
        request_id: "single-insert".into(),
        mode: InsertMode::InsertOnly,
        items: vec![NewWork {
            key: "a".into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: bytes::Bytes::new(),
        }],
    };
    let before = pipeline.diagnostics.snapshot();
    pipeline
        .lose_append_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let response = pipeline
        .insert(
            inserted.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let insert_outcomes = response.outcomes().to_vec();
    drop(response);
    assert!(
        !pipeline
            .lose_append_reply
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    let delta = pipeline
        .diagnostics
        .snapshot()
        .checked_delta(before)
        .unwrap();
    assert_eq!((delta.attempts, delta.admitted_raw_records), (2, 2)); // H=1: H+1, rather than 2H+1.
    let claim = ClaimRequest {
        queue: address(),
        request_id: "single-claim".into(),
        filter: Filter::default(),
        order: ClaimOrder::PriorityAscending,
        eligible_at: 0,
        lease_nanos: 1_000_000_000,
        max_records: 1,
        max_bytes: 65536,
        fields: None,
        include_body: false,
    };
    let before = pipeline.diagnostics.snapshot();
    pipeline
        .lose_append_reply
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let response = pipeline
        .claim(
            claim.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    let claimed_records = response.records().to_vec();
    assert_eq!(claimed_records.len(), 1);
    drop(response);
    assert!(
        !pipeline
            .lose_append_reply
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    let delta = pipeline
        .diagnostics
        .snapshot()
        .checked_delta(before)
        .unwrap();
    assert_eq!((delta.attempts, delta.admitted_raw_records), (2, 3)); // H=2.
    let replay = pipeline
        .insert(
            inserted.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.outcomes(), insert_outcomes);
    drop(replay);
    let replay = pipeline
        .claim(
            claim.clone(),
            QueueLimits::default(),
            clock.clone(),
            job.clone(),
        )
        .await
        .unwrap();
    assert_eq!(replay.records(), claimed_records);
    drop(replay);
    for (domain, id) in [
        (0, "single-insert"),
        (1, "single-claim"),
        (0, "absent"),
        (1, "absent"),
    ] {
        let old = pipeline
            .recover_request(address(), domain, id.into(), job.clone())
            .await
            .unwrap();
        let descriptor = old
            .original_authority
            .as_ref()
            .unwrap()
            .descriptor(0, id)
            .unwrap();
        assert_eq!(descriptor.is_some(), id != "absent");
        let historical_epoch = old.history.historical_epoch();
        let before = pipeline.diagnostics.snapshot();
        let continued = pipeline
            .recover_request_after(address(), domain, id.into(), old, job.clone())
            .await
            .unwrap();
        assert_eq!(
            pipeline
                .diagnostics
                .snapshot()
                .checked_delta(before)
                .unwrap()
                .admitted_raw_records,
            0
        );
        assert_eq!(
            continued.original_identity_present,
            Some(descriptor.is_some())
        );
        assert_eq!(continued.history.historical_epoch(), historical_epoch);
        assert_eq!(
            continued
                .original_authority
                .as_ref()
                .unwrap()
                .descriptor(0, id)
                .unwrap(),
            descriptor
        );
        let cold = pipeline
            .recover_request(address(), domain, id.into(), job.clone())
            .await
            .unwrap();
        assert_eq!(
            cold.original_authority
                .as_ref()
                .unwrap()
                .descriptor(0, id)
                .unwrap(),
            descriptor
        );
        drop(cold);
        let fresh_projection = QueueProjection::open(
            &root.join(format!("fresh-{domain}-{id}.db")),
            &job,
            &workers,
        )
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
        let rebuilt = fresh
            .recover_request_after(address(), domain, id.into(), continued, job.clone())
            .await
            .unwrap();
        assert_eq!(fresh.diagnostics.snapshot().admitted_raw_records, 3);
        assert_eq!(
            rebuilt
                .original_authority
                .as_ref()
                .unwrap()
                .descriptor(0, id)
                .unwrap(),
            descriptor
        );
        drop(rebuilt);
        drop(fresh);
    }
    for (domain, id) in [(1, "single-insert"), (0, "different")] {
        let old = pipeline
            .recover_request(address(), 0, "single-insert".into(), job.clone())
            .await
            .unwrap();
        let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
        let authoritative_before = store
            .read_api007_state(ns, &job, &workers)
            .await
            .unwrap()
            .state();
        let before = pipeline.diagnostics.snapshot();
        assert!(matches!(
            pipeline
                .recover_request_after(address(), domain, id.into(), old, job.clone())
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(
            pipeline
                .diagnostics
                .snapshot()
                .checked_delta(before)
                .unwrap()
                .admitted_raw_records,
            0
        );
        let ns = store.bind_api007_namespace(&address(), &job).await.unwrap();
        assert_eq!(
            store
                .read_api007_state(ns, &job, &workers)
                .await
                .unwrap()
                .state(),
            authoritative_before
        );
    }
    drop(pipeline);
    drop(store);
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    std::fs::remove_dir_all(root).unwrap();
}
