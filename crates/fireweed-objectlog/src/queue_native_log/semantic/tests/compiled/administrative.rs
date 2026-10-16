//! Typed source FP4 handoff agrees with the independent full-history fact oracle.
use super::*;
use fireweed_core::queue_administration::*;
use fireweed_engine::{
    queue_administration::propose_group,
    queue_publication::measure_administrative_success_floor,
    queue_source_index::{
        QueueSourceReader, admit_administrative_group, open_administrative_source,
    },
};
use fireweed_queue_spool::{
    OwnedAdministrativeSink, ScratchDiskQuota, ScratchWorkers, SourceAdministrativeAuthority,
    SourceAdministrativeFinalization, finalize_source_administration,
    source_administrative_publication_limit,
};
async fn administrative_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: collection.clone(),
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
    let work = |key: &str| NewWork {
        key: key.into(),
        priority: 1,
        available_at: 0,
        fields: Default::default(),
        body: vec![1, 2, 3].into(),
    };
    let mut seed = group("seed");
    seed.work_inserts.push(work("a"));
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
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let mut request = group("source-administration");
    request.work_inserts.push(work("next"));
    request.state_updates.push(StateUpdate {
        collection: "workflow".into(),
        key: "run".into(),
        expect: StateExpect::Absent,
        change: StateChange::Put {
            fields: Default::default(),
            value: vec![7].into(),
        },
    });
    request.administration = Some(AdministrativeCommit {
        max_result_bytes: 65536,
        intent: AdministrativeIntent {
            evaluated_at: 10,
            before_fields: None,
            operation: AdministrativeOperation::Addressed(vec![AdministrativeEntry {
                key: "a".into(),
                expected_version: Some(1),
                precondition: WorkSelector::all(vec![]),
                action: AdministrativeAction::Update {
                    patch: WorkPatch {
                        priority: Some(9),
                        ..Default::default()
                    },
                    lifecycle: AdministrativeLifecycle::Keep,
                    lease_guard: AdministrativeLeaseGuard::RejectActive,
                    remove_field_prefixes: Default::default(),
                },
            }]),
        },
    });
    let reference = propose_group(
        &request,
        &request.administration.as_ref().unwrap().intent,
        &[],
        &before,
        || Ok(10),
        &schema,
        &QueueLimits::default(),
        &job,
    )
    .unwrap();
    let mut after = before.clone();
    let delta = &reference.proposal().delta;
    for row in &delta.work {
        after.work.insert(row.record.work.key.clone(), row.clone());
    }
    for row in &delta.state {
        after
            .state
            .insert((row.collection.clone(), row.key.clone()), row.clone());
    }
    for version in &delta.versions {
        after.versions.insert(
            fireweed_engine::queue_planner::RecordIdentity {
                collection: version.collection.clone(),
                key: version.key.clone(),
            },
            version.version,
        );
    }
    after.next_sequence = delta.next_sequence;
    let workers = ScratchWorkers::new(2, 4).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let quota = ScratchDiskQuota::new(1 << 20).unwrap();
    let context = setup::Context {
        source: acquired.as_source(),
        log: &log,
        definition: &definition,
        queue: &queue,
        job: &job,
        ledger: &ledger,
        workers: &workers,
        directory: scratch.path(),
        quota: &quota,
    };
    let original = acquired.as_source().source_cut(&log, &job).unwrap();
    let resolution = context.resolution(&request, 1 << 20).await;
    let output_job = job.clone();
    let output_queue = queue.clone();
    let output_request = request.clone();
    let publication = workers
        .run(move || resolution.publication(&output_queue, &output_request, &output_job))
        .await
        .unwrap();
    let record = publication
        .record_for(&original, &queue, &request, &definition, &job)
        .unwrap();
    // Predict semantics with the complete-image planner, independently of the
    // source reader, scratch merge, typed handoff and semantic compiler.
    let expected_frame = encode_publication_pinned(
        &queue,
        definition.pin(),
        reference.initial_authority(),
        &reference.proposal().delta,
        &[],
        1 << 20,
        &job,
    )
    .unwrap();
    {
        use fireweed_engine::queue_publication::{
            decode_original_commit_outcome, view_publication,
        };
        let checked = record.checked().unwrap();
        let QueueLogPayload::Publication(actual) = checked.view().payload() else {
            panic!("administrative publication expected")
        };
        let expected = view_publication(expected_frame.as_bytes()).unwrap();
        let fragments = |view: &fireweed_engine::queue_publication::PublicationView<'_>| {
            view.command_fragments()
                .map(|command| command.unwrap().as_bytes().to_vec())
                .collect::<Vec<_>>()
        };
        assert_eq!(fragments(actual), fragments(&expected));
        assert_eq!(
            actual.summary().initial_sequence,
            expected.summary().initial_sequence
        );
        assert_eq!(
            actual.summary().final_sequence,
            expected.summary().final_sequence
        );
        let mut receipts = actual.receipts();
        let receipt = receipts.next().unwrap().unwrap();
        assert!(receipts.next().is_none());
        assert_eq!(receipt.fingerprint_format, 4);
        assert_eq!(receipt.domain, 2);
        assert_eq!(receipt.request_id, request.request_id);
        assert_eq!(receipt.fingerprint, &request.fingerprint(&queue).unwrap());
        let (dto, _owner) =
            decode_original_commit_outcome(&queue, &request, 4, receipt.encoded_outcome(), &job)
                .unwrap();
        let GroupOutcome::Committed { versions } = &reference.proposal().delta.receipt.outcome
        else {
            panic!("independent reference must commit")
        };
        assert_eq!(
            dto.outcome,
            GroupOutcome::AdministrativeCommitted {
                versions: versions.clone(),
                administration: reference
                    .proposal()
                    .administration
                    .as_ref()
                    .unwrap()
                    .clone(),
            }
        );
    }
    drop(expected_frame);
    let charge = job
        .owned_buffer(record.bytes().len() as u64 * 4 + 4096)
        .unwrap();
    let expected = OwnedQueueLogRecord::from_admitted_bytes(
        bytes::Bytes::copy_from_slice(record.bytes()),
        charge,
        &job,
    )
    .unwrap();
    // Binding failures must precede compiler reads and nonrefunding debits.
    let spent = || {
        let value = ledger.remaining_work();
        (
            value.semantic.pages,
            value.semantic.hydrated_bytes,
            value.record_attempts,
            value.hydration_reads,
            value.hydration_body_bytes,
        )
    };
    let before_rejections = spent();
    let mut changed_id = request.clone();
    changed_id.request_id.push_str("-foreign");
    let mut changed_action = request.clone();
    let AdministrativeOperation::Addressed(entries) = &mut changed_action
        .administration
        .as_mut()
        .unwrap()
        .intent
        .operation
    else {
        panic!()
    };
    let AdministrativeAction::Update { patch, .. } = &mut entries[0].action else {
        panic!()
    };
    patch.priority = Some(3);
    let mut changed_schema = schema.clone();
    changed_schema.max_attempts += 1;
    let foreign_definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&changed_schema).unwrap(), &job)
            .unwrap();
    let foreign_job = Arc::new(admission.reserve(0).unwrap());
    let foreign_queue = QueueAddress {
        tenant: "foreign".into(),
        queue: queue.queue.clone(),
    };
    for (actual_queue, actual_request, actual_definition, actual_job) in [
        (&queue, &changed_id, &definition, &job),
        (&queue, &changed_action, &definition, &job),
        (&queue, &request, &foreign_definition, &job),
        (&queue, &request, &definition, &foreign_job),
        (&foreign_queue, &request, &definition, &job),
    ] {
        assert!(
            acquired
                .prepare_administrative_publication(
                    &log,
                    &publication,
                    QueueNativeAdministrativePreparation {
                        queue: actual_queue,
                        group: actual_request,
                        definition: actual_definition,
                        budget: ledger.clone(),
                        limits: limits(),
                    },
                    actual_job,
                )
                .await
                .is_err()
        );
        assert_eq!(
            spent(),
            before_rejections,
            "binding refusal must not spend compiler work"
        );
    }
    drop(foreign_job);
    drop(foreign_definition);
    let namespace = acquired
        .as_source()
        .retained_namespace(&queue, &job)
        .unwrap();
    let foreign_source = log.capture_source(namespace, &job).await.unwrap();
    assert_eq!(foreign_source.head(), acquired.as_source().head());
    let foreign_context = setup::Context {
        source: &foreign_source,
        ..context
    };
    let foreign_resolution = foreign_context.resolution(&request, 1 << 20).await;
    let foreign_publication = foreign_resolution
        .publication(&queue, &request, &job)
        .unwrap();
    let foreign_cut = foreign_source.source_cut(&log, &job).unwrap();
    assert!(
        foreign_publication
            .record_for(&foreign_cut, &queue, &request, &definition, &job)
            .is_ok()
    );
    assert!(
        publication
            .record_for(&foreign_cut, &queue, &request, &definition, &job)
            .is_err()
    );
    let before_foreign = spent();
    assert!(
        acquired
            .prepare_administrative_publication(
                &log,
                &foreign_publication,
                QueueNativeAdministrativePreparation {
                    queue: &queue,
                    group: &request,
                    definition: &definition,
                    budget: ledger.clone(),
                    limits: limits(),
                },
                &job,
            )
            .await
            .is_err()
    );
    assert_eq!(
        spent(),
        before_foreign,
        "equal-numbered foreign source refuses before compilation"
    );
    drop(foreign_publication);
    drop(foreign_source);
    boundaries::check(&context, &request, &publication, &admission).await;
    let prepared = acquired
        .prepare_administrative_publication(
            &log,
            &publication,
            QueueNativeAdministrativePreparation {
                queue: &queue,
                group: &request,
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
    drop(reference);
    let reopened = verify_published(
        &log,
        &acquired,
        expected,
        &definition,
        (&mut history, &after),
        &job,
    )
    .await;
    assert_eq!(quota.usage().unwrap(), 0);
    workers.close_and_drain().await.unwrap();
    drop((reopened, acquired, definition, ledger, history));
    log.shutdown().await.unwrap();
    drop(job);
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn source_administrative_handoff_matches_cold_oracle_local() {
    let dir = tempfile::tempdir().unwrap();
    administrative_contract(
        QueueNativeLog::local(dir.path(), "admin-producer".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn source_administrative_handoff_matches_cold_oracle_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-admin-producer/{}/{disabled}",
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
        administrative_contract(log).await;
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

mod boundaries;
mod setup;
