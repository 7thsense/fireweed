//! Addressed source state hydration agrees with the independent complete image.
use super::*;
use fireweed_engine::queue_source_index::{QueueSourceReader, load_source_state_records};
async fn contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: BTreeMap::from([("n".into(), FieldType::Integer)]),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: ["meta/".into()].into(),
    };
    let schema = QueueSchema {
        work: collection.clone(),
        state: BTreeMap::from([("s".into(), collection)]),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let (acquired, genesis) = bundle_for_schema(&log, "producer", &job, &schema).await;
    let definition = {
        let checked = genesis.checked().unwrap();
        let QueueLogPayload::Genesis(view) = checked.view().payload() else {
            panic!()
        };
        view.validate_definition(&job).unwrap()
    };
    let ledger = QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: QueueNativeSemanticWork {
                pages: 1_000_000,
                hydrated_bytes: 1 << 30,
            },
            record_attempts: 1000,
            hydration_reads: 1000,
            hydration_body_bytes: 1 << 30,
        },
        &job,
    )
    .unwrap();
    let mut history = Vec::new();
    let before = RecordSnapshot::default();
    let acquired = accept(
        &log,
        acquired,
        genesis,
        &definition,
        (&mut history, &before),
        ledger.clone(),
        &job,
    )
    .await;
    let state = |key: &str, n: i64, value: Vec<u8>| StateUpdate {
        collection: "s".into(),
        key: key.into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: BTreeMap::from([
                ("n".into(), FieldValue::Integer(n)),
                ("meta/\0雪".into(), FieldValue::Bytes(vec![0, 255])),
            ]),
            value: value.into(),
        },
    };
    let mut seed = group("seed-state");
    seed.state_updates = vec![
        state("a", 1, vec![1, 2]),
        state("b", 2, vec![1, 2]),
        state("empty", 0, vec![]),
    ];
    let (record, before) = ordinary(&acquired, &definition, &before, &[seed], &job);
    let acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &before),
        ledger.clone(),
        &job,
    )
    .await;
    let mut next = group("second-state-C");
    next.state_updates.push(state("c", 3, vec![9, 9]));
    let (record, before) = ordinary(&acquired, &definition, &before, &[next], &job);
    let acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &before),
        ledger.clone(),
        &job,
    )
    .await;
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let keys = ["c", "b", "never", "a", "c", "empty"];
    let expected: Vec<_> = keys
        .iter()
        .map(|key| before.state.get(&("s".into(), (*key).into())).cloned())
        .collect();
    let mut request = StateReadRequest {
        queue: queue.clone(),
        collection: "s".into(),
        keys: keys.map(String::from).to_vec(),
        max_bytes: encoded_len(&expected).unwrap(),
    };
    let reader = QueueNativeSourceReader::new(
        acquired.as_source(),
        &log,
        &job,
        ledger.clone(),
        limits().read,
    )
    .unwrap()
    .with_records(super::super::record::record_limits())
    .unwrap();
    let cut = reader.cut().unwrap();
    let baseline = admission.usage().unwrap();
    let before_hydration = ledger.remaining_work().record_attempts;
    let owned = load_source_state_records(
        &reader,
        cut,
        &request,
        &definition,
        &QueueLimits::default(),
        &job,
    )
    .await
    .unwrap();
    assert_eq!(
        before_hydration - ledger.remaining_work().record_attempts,
        2,
        "one record attempt per distinct historical C"
    );
    assert!(admission.usage().unwrap().1 > baseline.1);
    assert_eq!(owned.into_value(), expected);
    assert_eq!(admission.usage().unwrap(), baseline);
    request.max_bytes -= 1;
    assert!(matches!(
        load_source_state_records(
            &reader,
            cut,
            &request,
            &definition,
            &QueueLimits::default(),
            &job
        )
        .await,
        Err(QueueError::TooLarge)
    ));
    assert_eq!(admission.usage().unwrap(), baseline);
    let before_empty = remaining(&ledger);
    request.keys.clear();
    request.max_bytes = 1;
    assert!(
        load_source_state_records(
            &reader,
            cut,
            &request,
            &definition,
            &QueueLimits::default(),
            &job
        )
        .await
        .unwrap()
        .into_value()
        .is_empty()
    );
    assert_eq!(admission.usage().unwrap(), baseline);
    let after_empty = remaining(&ledger);
    assert_eq!(
        (after_empty.2, after_empty.3, after_empty.4),
        (before_empty.2, before_empty.3, before_empty.4),
        "empty reads spend no record or payload hydration work"
    );
    let namespace = acquired
        .as_source()
        .retained_namespace(&queue, &job)
        .unwrap();
    let foreign = log.capture_source(namespace, &job).await.unwrap();
    assert_eq!(foreign.head(), acquired.as_source().head());
    let foreign_cut = foreign.source_cut(&log, &job).unwrap();
    let before_refusal = remaining(&ledger);
    assert!(
        load_source_state_records(
            &reader,
            foreign_cut,
            &request,
            &definition,
            &QueueLimits::default(),
            &job
        )
        .await
        .is_err()
    );
    assert_eq!(
        remaining(&ledger),
        before_refusal,
        "foreign empty scope refuses before any dispatch"
    );
    assert!(acquired.as_source().confirms(&log, &job).await.unwrap());
    drop(foreign);
    drop(reader);
    drop(acquired);
    drop((definition, ledger, history));
    log.shutdown().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
fn remaining(ledger: &QueueNativeSourceBudget) -> (u64, u64, u64, u64, u64) {
    let value = ledger.remaining_work();
    (
        value.semantic.pages,
        value.semantic.hydrated_bytes,
        value.record_attempts,
        value.hydration_reads,
        value.hydration_body_bytes,
    )
}
#[tokio::test]
async fn source_state_hydration_matches_complete_image_local() {
    let root = tempfile::tempdir().unwrap();
    contract(QueueNativeLog::local(root.path(), "source-state".into(), options(1 << 20)).unwrap())
        .await;
}
#[tokio::test]
async fn source_state_hydration_matches_complete_image_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-source-state/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let log = QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: &env.endpoint,
                region: &env.region,
                bucket: &env.bucket,
                access_key: &env.access_key,
                secret_key: &env.secret_key,
            },
            prefix.clone(),
            options(1 << 20),
            QueueNativeS3TransportOptions {
                disable_payload_signing: disabled,
                ..Default::default()
            },
        )
        .unwrap();
        contract(log).await;
        let store = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        for key in store.list(&prefix).await.unwrap() {
            store.delete(&key).await.unwrap();
        }
        assert!(store.list(&prefix).await.unwrap().is_empty());
    }
}
