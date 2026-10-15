//! Batch producer handoff: caller-order effects, exact bytes and sealed binding.
use super::*;
use fireweed_engine::queue_source_index::{
    QueueSourceGroupEncodingLimits, QueueSourceReader, load_batch_preimage,
};

struct Clock;
impl fireweed_engine::Clock for Clock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        fireweed_core::UtcTimestamp {
            seconds: 0,
            nanoseconds: 10,
        }
    }
}
async fn batch_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let mut work_schema = collection.clone();
    work_schema.fields.insert("tag".into(), FieldType::Integer);
    work_schema
        .indexes
        .insert("by_tag".into(), vec!["tag".into()]);
    work_schema.unique_indexes.insert("by_tag".into());
    let schema = QueueSchema {
        work: work_schema,
        state: BTreeMap::from([("workflow".into(), collection)]),
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
    let ledger = budget(&job, 10_000);
    let mut history = Vec::new();
    let snapshot = RecordSnapshot::default();
    let acquired = accept(
        &log,
        acquired,
        genesis,
        &definition,
        (&mut history, &snapshot),
        ledger.clone(),
        &job,
    )
    .await;
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let insert = |key: &str| NewWork {
        key: key.into(),
        priority: 1,
        available_at: 0,
        fields: Fields::from([("tag".into(), FieldValue::Integer(7))]),
        body: vec![1, 2, 3].into(),
    };
    let mut seed = group("z-seed");
    seed.work_inserts.push(insert("a"));
    seed.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Fields::new(),
            value: vec![9].into(),
        },
    });
    let mut transfer = group("a-transfer");
    transfer.work_updates.push(UnclaimedUpdate {
        key: "a".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Delete,
    });
    transfer.work_inserts.push(insert("b"));
    transfer.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Version(1),
        change: StateChange::Delete,
    });
    let mut rejected = group("rejected");
    rejected.work_inserts.push(insert("c"));
    let batch = CommitBatch {
        queue: queue.clone(),
        groups: vec![seed, transfer, rejected],
    };
    let (expected, after) = ordinary(&acquired, &definition, &snapshot, &batch.groups, &job);
    assert!(!after.work.contains_key("a"));
    assert!(!after.work.contains_key("c"));
    assert_eq!(after.work["b"].record.version, 1);
    assert!(!after.state.contains_key(&("workflow".into(), "run".into())));
    let reader = QueueNativeSourceReader::new(
        acquired.as_source(),
        &log,
        &job,
        ledger.clone(),
        limits().read,
    )
    .unwrap();
    let publication = load_batch_preimage(
        &reader,
        &batch,
        &[0, 1, 2],
        &definition,
        &QueueLimits::default(),
        &job,
    )
    .await
    .unwrap()
    .plan(&reader, &Clock)
    .await
    .unwrap()
    .encode_prefix(QueueSourceGroupEncodingLimits {
        frame_bytes: 1 << 20,
        record_bytes: 1 << 20,
    })
    .unwrap();
    assert_eq!(publication.positions(), &[0, 1, 2]);
    assert_eq!(
        publication
            .record_for(&reader.cut().unwrap(), &definition, &batch, &job)
            .unwrap()
            .bytes(),
        expected.bytes()
    );
    let before = acquired.as_source().head();
    let namespace = acquired
        .as_source()
        .retained_namespace(&queue, &job)
        .unwrap();
    let cut = log.capture(&namespace).await.unwrap();
    let foreign = QueueNativeAcquired {
        writer: QueueNativeWriter {
            writer: acquired.writer.writer.clone(),
            namespace: acquired.writer.namespace.clone(),
            epoch: acquired.writer.epoch,
            next_offset: acquired.writer.next_offset,
        },
        source: QueueNativeCaptured {
            log_identity: acquired.source.log_identity.clone(),
            namespace,
            cut,
            semantic_identity: QueueNativeCaptured::identity_charge(&job).unwrap(),
        },
    };
    assert_eq!(foreign.as_source().head(), before);
    let cloned_batch = batch.clone();
    let mut changed_schema = schema.clone();
    changed_schema.max_attempts += 1;
    let changed_definition = AdmittedQueueDefinition::from_raw(
        &fireweed_engine::queue_schema_codec::encode_definition(&changed_schema).unwrap(),
        &job,
    )
    .unwrap();
    let foreign_job = Arc::new(admission.reserve(0).unwrap());
    for (target, declaration, definition, target_job) in [
        (&foreign, &batch, &definition, &job),
        (&acquired, &cloned_batch, &definition, &job),
        (&acquired, &batch, &changed_definition, &job),
        (&acquired, &batch, &definition, &foreign_job),
    ] {
        let probe = budget(&job, 1);
        let error = target
            .prepare_batch_publication(
                &log,
                &publication,
                QueueNativeBatchPreparation {
                    batch: declaration,
                    definition,
                    budget: probe.clone(),
                    limits: limits(),
                },
                target_job,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(error, QueueError::OutcomeUnknown(_)), "{error:?}");
        // A full grant remains: binding failure cannot spend compiler work.
        probe.grant(work()).unwrap();
        assert!(
            probe
                .grant(QueueNativeSemanticWork {
                    pages: 1,
                    hydrated_bytes: 1
                })
                .is_err()
        );
        assert_eq!(
            log.capture(&acquired.source.namespace)
                .await
                .unwrap()
                .head(),
            before
        );
    }
    drop((foreign, changed_definition, foreign_job, cloned_batch));
    let prepared = acquired
        .prepare_batch_publication(
            &log,
            &publication,
            QueueNativeBatchPreparation {
                batch: &batch,
                definition: &definition,
                budget: ledger.clone(),
                limits: limits(),
            },
            &job,
        )
        .await
        .unwrap();
    assert!(matches!(
        prepared.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    assert_eq!(
        prepared.resolve(&log, 1).await.unwrap(),
        QueueNativeResolution::DurableContent
    );
    drop(prepared);
    drop(publication);
    drop(reader);
    let reopened = verify_published(
        &log,
        &acquired,
        expected,
        &definition,
        (&mut history, &after),
        &job,
    )
    .await;
    drop((reopened, acquired, definition, ledger, history));
    log.shutdown().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn source_batch_handoff_matches_cold_oracle_local() {
    let dir = tempfile::tempdir().unwrap();
    batch_contract(
        QueueNativeLog::local(dir.path(), "batch-producer".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn source_batch_handoff_matches_cold_oracle_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-batch-producer/{}/{disabled}",
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
        batch_contract(log).await;
        let cleanup = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}
