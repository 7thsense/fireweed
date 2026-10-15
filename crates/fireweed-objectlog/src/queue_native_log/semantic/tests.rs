use super::*;
use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema};
use fireweed_engine::{
    queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
    queue_schema_codec::encode_definition,
};

fn options(frame: usize) -> QueueNativeLogOptions {
    QueueNativeLogOptions {
        workers: 2,
        blocking_workers: 2,
        operations: 4,
        bytes: 4 << 20,
        version_bytes: 4096,
        binding_bytes: 4096,
        group_entries: 1,
        group_bytes: frame,
    }
}
fn work() -> QueueNativeSemanticWork {
    QueueNativeSemanticWork {
        pages: 1000,
        hydrated_bytes: 1 << 16,
    }
}
fn edits_limit() -> QueueNativeEditLimits {
    QueueNativeEditLimits {
        edits: 100,
        input_bytes: 1 << 16,
        work: work(),
    }
}
fn scan_limit() -> QueueNativeScanLimits {
    QueueNativeScanLimits {
        rows: 1,
        output_bytes: 4096,
        work: work(),
    }
}
async fn bundle(
    log: &QueueNativeLog,
    name: &str,
    job: &Arc<QueueReservation>,
) -> (QueueNativeAcquired, OwnedQueueLogRecord) {
    let schema = QueueSchema {
        work: CollectionDefinition {
            fields: Default::default(),
            indexes: Default::default(),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        },
        state: Default::default(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    bundle_for_schema(log, name, job, &schema).await
}
async fn bundle_for_schema(
    log: &QueueNativeLog,
    name: &str,
    job: &Arc<QueueReservation>,
    schema: &QueueSchema,
) -> (QueueNativeAcquired, OwnedQueueLogRecord) {
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: name.into(),
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(schema).unwrap(), job).unwrap();
    let ns = log.provision_namespace(&address, job).await.unwrap();
    let acquired = log.acquire_bundle(ns, job).await.unwrap();
    let record = encode_checked_genesis(
        acquired.head().epoch(),
        &address,
        &definition,
        &[7; 32],
        4096,
        job,
    )
    .unwrap();
    (acquired, record)
}
async fn fresh(
    log: &QueueNativeLog,
    prior: &QueueNativeAcquired,
    name: &str,
    job: &Arc<QueueReservation>,
) -> QueueNativeAcquired {
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: name.into(),
    };
    let ns = prior.retained_namespace(&address, job).unwrap();
    log.acquire_bundle(ns, job).await.unwrap()
}
async fn storage_image(
    provider: &object_log::LocalBlobStore,
    prefix: &str,
) -> Vec<(String, [u8; 32])> {
    use object_log::BlobStore;
    use sha2::{Digest, Sha256};
    let mut image = Vec::new();
    for key in provider.list(prefix).await.unwrap() {
        let bytes = provider.get(&key).await.unwrap().unwrap();
        image.push((key, Sha256::digest(&bytes).into()));
    }
    image.sort();
    image
}

#[tokio::test]
async fn coupled_source_pages_are_fixed_scoped_and_keep_alias_credit() {
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(dir.path(), "semantic".into(), options(1 << 20)).unwrap();
    source_pages_contract(log).await;
}

async fn source_pages_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let foreign = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    assert!(
        first
            .semantic_lookup(&log, b"a", work(), &job)
            .await
            .is_err()
    );
    let large = vec![8; 5000];
    let edits = [
        QueueNativeSemanticEdit {
            key: b"a",
            value: Some(b""),
        },
        QueueNativeSemanticEdit {
            key: b"b\0full",
            value: Some(&large),
        },
        QueueNativeSemanticEdit {
            key: b"c",
            value: Some(b"last"),
        },
    ];
    assert!(
        first
            .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &foreign)
            .await
            .is_err()
    );
    let publication = first
        .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &job)
        .await
        .unwrap();
    assert!(matches!(
        publication.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    assert!(
        first
            .prepare_with_edits(&log, &record, "stale", &edits, edits_limit(), &job)
            .await
            .is_err()
    );
    assert!(
        first
            .semantic_lookup(&log, b"a", work(), &job)
            .await
            .is_err()
    );
    let current = fresh(&log, &first, "queue", &job).await;
    assert_eq!(
        current
            .semantic_lookup(&log, b"a", work(), &job)
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        b""
    );
    assert!(
        current
            .semantic_lookup(&log, b"absent", work(), &job)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        current
            .semantic_lookup(&log, b"b\0full", work(), &job)
            .await
            .unwrap()
            .unwrap()
            .bytes(),
        large
    );
    assert!(
        current
            .semantic_lookup(&log, b"a", work(), &foreign)
            .await
            .is_err()
    );
    {
        let cut = current.source_cut(&log, &job).unwrap();
        let present = current
            .source_lookup(&log, b"a", work(), &job)
            .await
            .unwrap();
        let absent = current
            .source_lookup(&log, b"absent", work(), &job)
            .await
            .unwrap();
        let present_proof = present.evidence(&current, &log, &job).unwrap();
        assert_eq!(
            present_proof.value_for(&cut, b"a").unwrap(),
            Some(b"".as_slice())
        );
        assert!(present_proof.value_for(&cut, b"other").is_err());
        assert_eq!(
            absent
                .evidence(&current, &log, &job)
                .unwrap()
                .value_for(&cut, b"absent")
                .unwrap(),
            None
        );
        assert!(present.evidence(&current, &log, &foreign).is_err());
        let other = fresh(&log, &current, "queue", &job).await;
        assert!(present.evidence(&other, &log, &job).is_err());
        super::reader::reader_contract(&current, &log, &job, &foreign, &other, &admission).await;
    }
    let baseline = admission.usage().unwrap();
    let first_page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
        .await
        .unwrap();
    assert_eq!(first_page.status(), QueueNativeScanStatus::LimitReached);
    assert_eq!(first_page.rows().next().unwrap().key(), b"a");
    let token = first_page.continuation().unwrap();
    let alias = token.clone();
    let retained = admission.usage().unwrap();
    drop(first_page);
    drop(token);
    assert_eq!(admission.usage().unwrap(), retained);
    let mut big_row = scan_limit();
    big_row.output_bytes = 8192;
    let page = current
        .semantic_scan_next(&log, &alias, big_row, &job)
        .await
        .unwrap();
    assert_eq!(page.rows().next().unwrap().key(), b"b\0full");
    assert_eq!(page.rows().next().unwrap().value(), large);
    let repeated = current
        .semantic_scan_next(&log, &alias, big_row, &job)
        .await
        .unwrap();
    assert_eq!(repeated.rows().next().unwrap().key(), b"b\0full");
    assert!(
        current
            .semantic_scan_next(&log, &alias, big_row, &foreign)
            .await
            .is_err()
    );
    let other_bundle = fresh(&log, &current, "queue", &job).await;
    assert!(
        other_bundle
            .semantic_scan_next(&log, &alias, big_row, &job)
            .await
            .is_err()
    );
    let second_token = page.continuation().unwrap();
    let last = current
        .semantic_scan_next(&log, &second_token, scan_limit(), &job)
        .await
        .unwrap();
    assert_eq!(last.rows().next().unwrap().key(), b"c");
    assert_eq!(last.status(), QueueNativeScanStatus::LimitReached);
    let end = current
        .semantic_scan_next(&log, &last.continuation().unwrap(), scan_limit(), &job)
        .await
        .unwrap();
    assert_eq!(end.rows().len(), 0);
    assert_eq!(end.status(), QueueNativeScanStatus::Complete);
    assert!(end.continuation().is_none());
    drop(end);
    drop(last);
    drop(second_token);
    drop(repeated);
    drop(page);
    drop(alias);
    drop(other_bundle);
    assert_eq!(admission.usage().unwrap(), baseline);
    drop(publication);
    drop(record);
    drop(first);
    drop(current);
    drop(job);
    drop(foreign);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    log.shutdown().await.unwrap();
}
#[tokio::test]
async fn coupled_framing_checks_exact_boundary() {
    for deficit in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(dir.path(), "framing".into(), options(1 << 20)).unwrap();
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (acquired, record) = bundle(&log, "queue", &job).await;
        // The serialized provider/binding can be reopened with the same logical
        // namespace but a tighter frame limit before any publication.
        drop(acquired);
        log.shutdown().await.unwrap();
        let n = record.bytes().len() + 111 + 24 + 224;
        let log =
            QueueNativeLog::local(dir.path(), "framing".into(), options(n - deficit)).unwrap();
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let ns = log.open(&address, &job).await.unwrap().unwrap();
        let acquired = log.acquire_bundle(ns, &job).await.unwrap();
        // Re-encode the genesis under this freshly fenced epoch.
        let view = record.view().unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!()
        };
        let definition = genesis.validate_definition(&job).unwrap();
        let record = encode_checked_genesis(
            acquired.head().epoch(),
            &address,
            &definition,
            &[7; 32],
            4096,
            &job,
        )
        .unwrap();
        let outcome = acquired
            .prepare_with_edits(&log, &record, &"i".repeat(111), &[], edits_limit(), &job)
            .await;
        assert_eq!(outcome.is_ok(), deficit == 0);
        if let Ok(publication) = outcome {
            assert!(matches!(
                publication.submit(&log).await.unwrap(),
                QueueNativeSubmit::Stored(_)
            ));
        }
        assert_eq!(
            log.capture(&acquired.namespace)
                .await
                .unwrap()
                .head()
                .bounds()
                .1,
            u64::from(deficit == 0)
        );
        log.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn nonempty_legacy_cut_cannot_be_silently_promoted() {
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(dir.path(), "legacy".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let original = first.prepare_record(&log, &record, &job).await.unwrap();
    assert!(matches!(
        original.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    let current = fresh(&log, &first, "queue", &job).await;
    assert!(
        current
            .semantic_lookup(&log, b"missing", work(), &job)
            .await
            .is_err()
    );
    // The SDK refuses direct promotion, independently of Root's unique-genesis guard.
    let before = log.capture(&current.namespace).await.unwrap().head();
    let provider = object_log::LocalBlobStore::new(dir.path());
    let before_storage = storage_image(&provider, "legacy").await;
    let result = log
        .log
        .prepare_with_edits(
            &current.writer.writer,
            &current.cut.cut,
            &[NativeAppend {
                identity: "legacy-promotion",
                payload: b"later",
                records: 1,
            }],
            &[],
            object_log::NativeEditLimits {
                edits: 0,
                input_bytes: 0,
                work: work().sdk(),
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(
        log.capture(&current.namespace).await.unwrap().head(),
        before
    );
    assert_eq!(storage_image(&provider, "legacy").await, before_storage);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn root_output_admission_is_checked_before_source_and_released_by_last_alias() {
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(dir.path(), "credit".into(), options(1 << 20)).unwrap();
    let cap = 1 << 20;
    let admission = QueueAdmission::new(1, cap).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let edits = [QueueNativeSemanticEdit {
        key: b"a",
        value: Some(b""),
    }];
    let original = first
        .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &job)
        .await
        .unwrap();
    original.submit(&log).await.unwrap();
    let current = fresh(&log, &first, "queue", &job).await;
    use object_log::BlobStore;
    let provider = object_log::LocalBlobStore::new(dir.path());
    let mut removed = None;
    for key in provider.list("credit").await.unwrap() {
        let bytes = provider.get(&key).await.unwrap().unwrap();
        if bytes.starts_with(b"ONM2") {
            removed = Some((key, bytes));
            break;
        }
    }
    let (map_key, map_bytes) = removed.unwrap();
    provider.delete(&map_key).await.unwrap();
    let limits = scan_limit();
    let probe = QueueNativeAcquired::page_charge(&job, limits, 0).unwrap();
    let retained = probe.admitted_bytes();
    let scratch = ownership::physical_owner_bytes::<2>();
    let needed = retained + scratch;
    drop(probe);
    let available = cap - admission.usage().unwrap().1;
    let blocker = job.owned_buffer(available - needed + 1).unwrap();
    let err = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, QueueError::OutcomeUnknown(message) if message.contains("admitted buffer capacity exhausted"))
    );
    drop(blocker);
    let blocker = job.owned_buffer(available - needed).unwrap();
    let error = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .err()
        .unwrap();
    assert!(matches!(error, QueueError::OutcomeUnknown(message)
        if !message.contains("admitted buffer capacity exhausted")));
    provider.put(&map_key, map_bytes).await.unwrap();
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .unwrap();
    assert_eq!(admission.usage().unwrap().1, cap - scratch);
    let token = page.continuation().unwrap();
    let alias = token.clone();
    drop(page);
    drop(token);
    assert_eq!(admission.usage().unwrap().1, cap - scratch);
    drop(alias);
    assert_eq!(admission.usage().unwrap().1, cap - needed);
    drop(blocker);
    // Closing the SDK proves subsequent errors are surfaced, never empty pages.
    log.shutdown().await.unwrap();
    assert!(
        current
            .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn caller_cancellation_retains_completed_output_in_owned_task() {
    let dir = tempfile::tempdir().unwrap();
    let log =
        Arc::new(QueueNativeLog::local(dir.path(), "cancel".into(), options(1 << 20)).unwrap());
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let edits = [
        QueueNativeSemanticEdit {
            key: b"a",
            value: Some(b"first"),
        },
        QueueNativeSemanticEdit {
            key: b"b",
            value: Some(b"next"),
        },
    ];
    let publication = first
        .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &job)
        .await
        .unwrap();
    publication.submit(&log).await.unwrap();
    let current = Arc::new(fresh(&log, &first, "queue", &job).await);
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
        .await
        .unwrap();
    let token = page.continuation().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let owned = {
        let log = log.clone();
        let current = current.clone();
        let job = job.clone();
        tokio::spawn(async move {
            let page = current
                .semantic_scan_next(&log, &token, scan_limit(), &job)
                .await
                .unwrap();
            assert_eq!(page.rows().next().unwrap().key(), b"b");
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            drop(page);
            drop(token);
            drop(current);
            drop(job);
            drop(log);
            done_tx.send(()).unwrap();
        })
    };
    let caller = tokio::spawn(async move { owned.await.unwrap() });
    ready_rx.await.unwrap();
    caller.abort();
    let _ = caller.await;
    drop(page);
    drop(current);
    drop(first);
    drop(record);
    drop(publication);
    drop(job);
    assert_eq!(
        admission.usage().unwrap().0,
        1,
        "owned storage task retains Root operation slot"
    );
    log.shutdown().await.unwrap(); // SDK physical drain is distinct from Root retention.
    assert_eq!(admission.usage().unwrap().0, 1);
    release_tx.send(()).unwrap();
    done_rx.await.unwrap();
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn missing_authenticated_map_is_never_absence_or_complete() {
    use object_log::BlobStore;
    let dir = tempfile::tempdir().unwrap();
    let provider = object_log::LocalBlobStore::new(dir.path());
    let log = QueueNativeLog::local(dir.path(), "missing".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let edits = [QueueNativeSemanticEdit {
        key: b"a",
        value: Some(b"stored"),
    }];
    let publication = first
        .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &job)
        .await
        .unwrap();
    publication.submit(&log).await.unwrap();
    let current = fresh(&log, &first, "queue", &job).await;
    let mut map_pages = Vec::new();
    for key in provider.list("missing").await.unwrap() {
        let bytes = provider.get(&key).await.unwrap().unwrap();
        if bytes.starts_with(b"ONM2") {
            map_pages.push((key, bytes));
        }
    }
    assert_eq!(map_pages.len(), 1);
    let (key, bytes) = map_pages.pop().unwrap();
    provider.delete(&key).await.unwrap();
    assert!(
        current
            .semantic_lookup(&log, b"absent", work(), &job)
            .await
            .is_err()
    );
    assert!(
        current
            .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
            .await
            .is_err()
    );
    provider.put(&key, bytes).await.unwrap();
    assert!(
        current
            .semantic_lookup(&log, b"absent", work(), &job)
            .await
            .unwrap()
            .is_none()
    );
    let mut limits = scan_limit();
    limits.rows = 2;
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .unwrap();
    assert_eq!(page.rows().len(), 1);
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    let other_log = QueueNativeLog::local(dir.path(), "other".into(), options(1 << 20)).unwrap();
    other_log.shutdown().await.unwrap();
    let err = current
        .semantic_lookup(&other_log, b"absent", work(), &job)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, QueueError::OutcomeUnknown(message) if message.contains("semantic namespace, cut or operation differs"))
    );
    // Foreign ownership must win over a closed SDK error, before SDK admission.
    let (foreign, _) = bundle(&log, "foreign", &job).await;
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, scan_limit(), &job)
        .await
        .unwrap();
    let token = page.continuation().unwrap();
    assert!(
        foreign
            .semantic_scan_next(&log, &token, scan_limit(), &job)
            .await
            .is_err()
    );
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn bundle_identity_admission_precedes_authoritative_fence() {
    let dir = tempfile::tempdir().unwrap();
    let provider = object_log::LocalBlobStore::new(dir.path());
    let log = QueueNativeLog::local(dir.path(), "preflight".into(), options(1 << 20)).unwrap();
    let cap = 1 << 20;
    let admission = QueueAdmission::new(1, cap).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "queue".into(),
    };
    let namespace = log.provision_namespace(&address, &job).await.unwrap();
    let retained = log.open(&address, &job).await.unwrap().unwrap();
    let before = log.capture(&retained).await.unwrap().head();
    let image = storage_image(&provider, "preflight").await;
    let probe = QueueNativeAcquired::identity_charge(&job).unwrap();
    let scratch = ownership::physical_owner_bytes::<1>();
    let needed = probe.admitted_bytes() + scratch;
    drop(probe);
    let available = cap - admission.usage().unwrap().1;
    let blocker = job.owned_buffer(available - needed + 1).unwrap();
    assert!(log.acquire_bundle(namespace, &job).await.is_err());
    assert_eq!(log.capture(&retained).await.unwrap().head(), before);
    assert_eq!(storage_image(&provider, "preflight").await, image);
    drop(blocker);
    let namespace = log.open(&address, &job).await.unwrap().unwrap();
    let available = cap - admission.usage().unwrap().1;
    let blocker = job.owned_buffer(available - needed).unwrap();
    let acquired = log.acquire_bundle(namespace, &job).await.unwrap();
    assert_eq!(admission.usage().unwrap().1, cap - scratch);
    assert_eq!(acquired.head().epoch(), before.epoch() + 1);
    drop(acquired);
    drop(blocker);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn long_empty_value_key_charges_page_and_retained_continuation_copy() {
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(dir.path(), "long".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let key = vec![b'x'; 6000];
    let edits = [QueueNativeSemanticEdit {
        key: &key,
        value: Some(b""),
    }];
    let publication = first
        .prepare_with_edits(&log, &record, "genesis", &edits, edits_limit(), &job)
        .await
        .unwrap();
    publication.submit(&log).await.unwrap();
    let current = fresh(&log, &first, "queue", &job).await;
    let baseline = admission.usage().unwrap();
    let limits = QueueNativeScanLimits {
        rows: 1,
        output_bytes: key.len(),
        work: work(),
    };
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .unwrap();
    assert_eq!(page.rows().next().unwrap().key(), key);
    assert!(page.rows().next().unwrap().value().is_empty());
    assert!(
        page.charge.admitted_bytes() >= 2 * key.len() as u64,
        "both the row key and continuation's copied key require Root credit"
    );
    let token = page.continuation().unwrap();
    let alias = token.clone();
    let held = admission.usage().unwrap();
    drop(page);
    drop(token);
    assert_eq!(admission.usage().unwrap(), held);
    let end = current
        .semantic_scan_next(&log, &alias, limits, &job)
        .await
        .unwrap();
    assert_eq!(end.status(), QueueNativeScanStatus::Complete);
    assert_eq!(end.rows().len(), 0);
    drop(end);
    drop(alias);
    assert_eq!(admission.usage().unwrap(), baseline);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn s3_coupled_source_pages_run_the_same_contract_both_signing_modes() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-semantic-wrapper/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let transport = QueueNativeS3TransportOptions {
            disable_payload_signing: disabled,
            ..Default::default()
        };
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
            transport,
        )
        .unwrap();
        source_pages_contract(log).await;
        let cleanup = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        let keys = cleanup.list(&prefix).await.unwrap();
        assert!(
            !keys.is_empty(),
            "S3 source contract must publish real objects"
        );
        for key in keys {
            cleanup.delete(&key).await.unwrap();
        }
    }
}

async fn compiled_genesis_contract(log: QueueNativeLog) {
    use fireweed_engine::queue_source_index::{
        QueueSourceCompileLimits, QueueSourceGenesis, QueueSourceKey, compile_genesis,
    };
    use fireweed_engine::queue_storage_layout::decode;
    let cap = 32 << 20;
    let admission = QueueAdmission::new(2, cap).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let foreign_job = Arc::new(admission.reserve(0).unwrap());
    let (first, record) = bundle(&log, "queue", &job).await;
    let cut = first.source_cut(&log, &job).unwrap();
    let checked = record.checked().unwrap();
    let QueueLogPayload::Genesis(genesis_view) = checked.view().payload() else {
        panic!()
    };
    let definition = genesis_view.validate_definition(&job).unwrap();
    let foreign_definition = genesis_view.validate_definition(&foreign_job).unwrap();
    let mut different_schema = definition.schema().clone();
    different_schema.max_attempts += 1;
    let different_definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&different_schema).unwrap(), &job)
            .unwrap();
    let before_compile = admission.usage().unwrap().1;
    let generous = QueueSourceCompileLimits {
        edits: 100,
        input_bytes: 1 << 20,
    };
    let error = compile_genesis(&checked, cut, &foreign_definition, generous, &job)
        .err()
        .unwrap();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(message) if message.contains("genesis definition belongs to another operation"))
    );
    let error = compile_genesis(&checked, cut, &different_definition, generous, &job)
        .err()
        .unwrap();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(message) if message.contains("definition pin mismatch"))
    );
    let compiled = compile_genesis(&checked, cut, &definition, generous, &job).unwrap();
    let required = compiled
        .edits()
        .map(|(key, value)| key.len() as u64 + value.unwrap().len() as u64)
        .sum::<u64>();
    assert_eq!(compiled.edits().len(), 6);
    let output_capacity = admission.usage().unwrap().1 - before_compile;
    assert!(output_capacity > required);
    drop(compiled);
    let available = cap - admission.usage().unwrap().1;
    let blocker = job.owned_buffer(available - output_capacity + 1).unwrap();
    let error = compile_genesis(&checked, cut, &definition, generous, &job)
        .err()
        .unwrap();
    assert!(
        matches!(error, QueueError::OutcomeUnknown(message) if message.contains("admitted buffer capacity exhausted"))
    );
    drop(blocker);
    let blocker = job.owned_buffer(available - output_capacity).unwrap();
    let admitted = compile_genesis(&checked, cut, &definition, generous, &job).unwrap();
    assert_eq!(admission.usage().unwrap().1, cap);
    drop(admitted);
    assert_eq!(admission.usage().unwrap().1, cap - output_capacity);
    drop(blocker);

    assert!(
        compile_genesis(
            &checked,
            cut,
            &definition,
            QueueSourceCompileLimits {
                edits: 5,
                ..generous
            },
            &job
        )
        .is_err()
    );
    assert!(
        compile_genesis(
            &checked,
            cut,
            &definition,
            QueueSourceCompileLimits {
                input_bytes: required - 1,
                ..generous
            },
            &job
        )
        .is_err()
    );
    let exact = compile_genesis(
        &checked,
        cut,
        &definition,
        QueueSourceCompileLimits {
            edits: 6,
            input_bytes: required,
        },
        &job,
    )
    .unwrap();
    assert!(compile_genesis(&checked, cut, &definition, generous, &foreign_job).is_err());
    assert!(
        first
            .prepare_compiled(&log, &exact, "foreign", edits_limit(), &foreign_job)
            .await
            .is_err()
    );
    let publication = first
        .prepare_compiled(&log, &exact, "compiled-genesis", edits_limit(), &job)
        .await
        .unwrap();
    assert!(matches!(
        publication.submit(&log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    let current = fresh(&log, &first, "queue", &job).await;
    assert!(
        current
            .prepare_compiled(&log, &exact, "wrong-cut", edits_limit(), &job)
            .await
            .is_err()
    );
    let key = QueueSourceKey::Genesis.encode().unwrap();
    let point = current
        .source_lookup(&log, &key, work(), &job)
        .await
        .unwrap();
    let cut = current.source_cut(&log, &job).unwrap();
    let evidence = point.evidence(&current, &log, &job).unwrap();
    let genesis: QueueSourceGenesis =
        decode(evidence.value_for(&cut, &key).unwrap().unwrap()).unwrap();
    let loaded = current.read(&log, 0, 4096, &job).await.unwrap().unwrap();
    assert_eq!(
        genesis
            .source
            .resolve(&loaded.checked().unwrap(), 0)
            .unwrap(),
        &record.bytes()[checked.payload_interval().unwrap()]
    );
    assert_eq!(
        *genesis.definition_pin.as_bytes(),
        checked.view().definition_pin_bytes()
    );
    for (key, value) in exact.edits() {
        assert_eq!(
            current
                .semantic_lookup(&log, key, work(), &job)
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            value.unwrap()
        );
    }
    let mut limits = scan_limit();
    limits.rows = 7;
    let page = current
        .semantic_scan(&log, Bound::Unbounded, Bound::Unbounded, limits, &job)
        .await
        .unwrap();
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    assert_eq!(page.rows().len(), 6);
    let expected_keys: std::collections::BTreeSet<_> = [
        QueueSourceKey::Genesis,
        QueueSourceKey::Sequence,
        QueueSourceKey::LifecycleCount(fireweed_core::queue_contract::Lifecycle::Pending),
        QueueSourceKey::LifecycleCount(fireweed_core::queue_contract::Lifecycle::Leased),
        QueueSourceKey::LifecycleCount(fireweed_core::queue_contract::Lifecycle::Completed),
        QueueSourceKey::LifecycleCount(fireweed_core::queue_contract::Lifecycle::Failed),
    ]
    .into_iter()
    .map(|key| key.encode().unwrap())
    .collect();
    assert_eq!(
        page.rows()
            .map(|row| row.key().to_vec())
            .collect::<std::collections::BTreeSet<_>>(),
        expected_keys
    );
    for key in expected_keys
        .iter()
        .filter(|key| **key != QueueSourceKey::Genesis.encode().unwrap())
    {
        let value = current
            .semantic_lookup(&log, key, work(), &job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decode::<u64>(value.bytes()).unwrap(),
            0,
            "initial sequence/count must be zero"
        );
    }

    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn compiled_genesis_and_all_initial_facts_publish_together_local() {
    let dir = tempfile::tempdir().unwrap();
    compiled_genesis_contract(
        QueueNativeLog::local(dir.path(), "compiled-genesis".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn s3_compiled_genesis_runs_identical_contract_both_signing_modes() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-compiled-genesis/{}/{disabled}",
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
        compiled_genesis_contract(log).await;
        let cleanup = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        let keys = cleanup.list(&prefix).await.unwrap();
        assert!(!keys.is_empty());
        for key in keys {
            cleanup.delete(&key).await.unwrap();
        }
    }
}

async fn field_preimages_contract(log: QueueNativeLog) {
    use fireweed_core::queue_contract::*;
    use fireweed_engine::{
        queue_planner::{RecordSnapshot, plan_commit_group},
        queue_publication::{EffectRef, PublicationReceipt, encode_publication, view_publication},
        queue_source_index::load_command_fields,
        queue_storage_layout,
    };
    use std::collections::BTreeMap;
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let schema = QueueSchema {
        work: CollectionDefinition {
            fields: BTreeMap::from([
                ("a\0α".into(), FieldType::Integer),
                ("b".into(), FieldType::Text),
                ("untouched".into(), FieldType::Bytes),
            ]),
            indexes: BTreeMap::from([("ab".into(), vec!["a\0α".into(), "b".into()])]),
            unique_indexes: Default::default(),
            additional_field_prefixes: Default::default(),
        },
        state: Default::default(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    };
    let (first, genesis) = bundle_for_schema(&log, "fields", &job, &schema).await;
    let fields = Fields::from([
        ("a\0α".into(), FieldValue::Integer(7)),
        ("b".into(), FieldValue::Text("value\0β".into())),
        ("untouched".into(), FieldValue::Bytes(vec![9; 8192])),
    ]);
    let mut facts: Vec<_> = fields
        .iter()
        .map(|(name, value)| {
            (
                fireweed_engine::queue_source_index::QueueSourceKey::Field {
                    collection: None,
                    key: "work\0α",
                    field: name,
                }
                .encode()
                .unwrap(),
                queue_storage_layout::encode(value).unwrap(),
            )
        })
        .collect();
    let group = |id: &str| CommitGroup {
        request_id: id.into(),
        administration: None,
        claimed_updates: vec![],
        state_updates: vec![],
        work_updates: vec![],
        work_inserts: vec![],
    };
    let mut seed = group("seed");
    seed.work_inserts.push(NewWork {
        key: "work\0α".into(),
        priority: 0,
        available_at: 0,
        fields: fields.clone(),
        body: Default::default(),
    });
    let mut before = RecordSnapshot::default();
    let seed_delta = plan_commit_group(&seed, &mut before, 0, &schema).unwrap();
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "fields".into(),
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
    {
        let checked = genesis.checked().unwrap();
        let compiled = fireweed_engine::queue_source_index::compile_genesis(
            &checked,
            first.source_cut(&log, &job).unwrap(),
            &definition,
            fireweed_engine::queue_source_index::QueueSourceCompileLimits {
                edits: 6,
                input_bytes: 1 << 16,
            },
            &job,
        )
        .unwrap();
        first
            .prepare_compiled(&log, &compiled, "genesis", edits_limit(), &job)
            .await
            .unwrap()
            .submit(&log)
            .await
            .unwrap();
    }
    let seeded = fresh(&log, &first, "fields", &job).await;
    let seed_receipt = PublicationReceipt::commit(&address, &seed, &seed_delta.receipt).unwrap();
    let seed_frame = encode_publication(
        &address,
        definition.raw(),
        &RecordSnapshot::default(),
        &seed_delta,
        &[seed_receipt],
        1 << 20,
        &job,
    )
    .unwrap();
    let seed_record = fireweed_engine::queue_log_record::encode_checked_publication_record(
        seeded.head().epoch(),
        seed_frame.as_bytes(),
        1 << 20,
        &job,
    )
    .unwrap();
    let checked_seed = seed_record.checked().unwrap();
    let fireweed_engine::queue_log_record::QueueLogPayload::Publication(seed_view) =
        checked_seed.view().payload()
    else {
        panic!()
    };
    let seed_command = seed_view.commands().next().unwrap().unwrap();
    let EffectRef::WorkPut {
        metadata,
        body: fireweed_engine::queue_publication::BodyCommandRef::Replace(body),
        ..
    } = seed_command.effect
    else {
        panic!()
    };
    let fact = fireweed_engine::queue_source_index::QueueSourceRecord {
        highwater: 1,
        field_count: 3,
        live: Some(fireweed_engine::queue_source_index::QueueSourceLive::Work {
            metadata: fireweed_engine::queue_group_state_authority::WorkMetadata {
                version: 1,
                priority: metadata.priority,
                available_at: metadata.available_at,
                insertion_sequence: metadata.insertion_sequence,
                lifecycle: metadata.lifecycle,
                eligible_since: metadata.eligible_since,
                attempts: metadata.attempts,
                lease_expires_at: metadata.lease_expires_at,
                lease_hash: metadata.lease_hash.copied(),
            },
            body: fireweed_engine::queue_source_index::QueueSourcePayload::from_checked(
                &checked_seed,
                1,
                body,
            )
            .unwrap(),
        }),
    };
    facts.push((
        fireweed_engine::queue_source_index::QueueSourceKey::Sequence
            .encode()
            .unwrap(),
        queue_storage_layout::encode(&1u64).unwrap(),
    ));
    facts.push((
        fireweed_engine::queue_source_index::QueueSourceKey::Record {
            collection: None,
            key: "work\0α",
        }
        .encode()
        .unwrap(),
        queue_storage_layout::encode(&fact).unwrap(),
    ));
    let mut edits: Vec<_> = facts
        .iter()
        .map(|(key, value)| QueueNativeSemanticEdit {
            key,
            value: Some(value),
        })
        .collect();
    edits.sort_by(|a, b| a.key.cmp(b.key));
    seeded
        .prepare_with_edits(&log, &seed_record, "seed", &edits, edits_limit(), &job)
        .await
        .unwrap()
        .submit(&log)
        .await
        .unwrap();
    let current = fresh(&log, &seeded, "fields", &job).await;
    let budget = QueueNativeSourceBudget::new(
        QueueNativeSemanticWork {
            pages: 16000,
            hydrated_bytes: 1 << 20,
        },
        &job,
    )
    .unwrap();
    let reader = QueueNativeSourceReader::new(
        &current,
        &log,
        &job,
        budget,
        QueueNativeSourceReadLimits {
            page: QueueNativeScanLimits {
                rows: 1,
                output_bytes: 16384,
                work: work(),
            },
            range_pages: 4,
        },
    )
    .unwrap();
    let mut patch = group("patch");
    patch.work_updates.push(UnclaimedUpdate {
        key: "work\0α".into(),
        expect: StateExpect::Version(1),
        change: UnclaimedChange::Patch(WorkPatch {
            fields: BTreeMap::from([("a\0α".into(), Some(FieldValue::Integer(8)))]),
            ..Default::default()
        }),
    });
    let mut after = before.clone();
    let delta = plan_commit_group(&patch, &mut after, 0, &schema).unwrap();
    let receipt = PublicationReceipt::commit(&address, &patch, &delta.receipt).unwrap();
    let frame = encode_publication(
        &address,
        &encode_definition(&schema).unwrap(),
        &before,
        &delta,
        &[receipt],
        1 << 20,
        &job,
    )
    .unwrap();
    let view = view_publication(frame.as_bytes()).unwrap();
    let command = view.commands().next().unwrap().unwrap();
    let baseline = admission.usage().unwrap();
    let selected = load_command_fields(&reader, command, 3, &schema.work, &job)
        .await
        .unwrap();
    assert_eq!(selected.present_count(), 2);
    assert_eq!(
        selected.checked().unwrap().get("a\0α").unwrap(),
        fields.get("a\0α")
    );
    assert_eq!(
        selected.checked().unwrap().get("b").unwrap(),
        fields.get("b")
    );
    assert!(selected.checked().unwrap().get("untouched").is_err());
    drop(selected);
    assert_eq!(admission.usage().unwrap(), baseline);
    let deleted = fireweed_engine::queue_publication::CommandRef {
        effect: EffectRef::Delete,
        ..command
    };
    let complete = load_command_fields(&reader, deleted, 3, &schema.work, &job)
        .await
        .unwrap();
    assert_eq!(complete.present_count(), 3);
    assert_eq!(
        complete.checked().unwrap().get("untouched").unwrap(),
        fields.get("untouched")
    );
    drop(complete);
    assert_eq!(admission.usage().unwrap(), baseline);
    assert!(
        load_command_fields(&reader, deleted, 2, &schema.work, &job)
            .await
            .is_err()
    );
    let target = fireweed_engine::queue_log_record::encode_checked_publication_record(
        current.head().epoch(),
        frame.as_bytes(),
        1 << 20,
        &job,
    )
    .unwrap();
    let checked_target = target.checked().unwrap();
    let context = fireweed_engine::queue_source_index::QueueSourcePublicationContext::begin(
        &reader,
        &checked_target,
        &definition,
        &job,
    )
    .await
    .unwrap();
    let token = context.commands().next().unwrap().unwrap();
    let predecessor = context.predecessor(&token).await.unwrap();
    assert_eq!(predecessor.fact().unwrap().field_count, 3);
    let transition = context.record_transition(&predecessor).await.unwrap();
    assert_eq!(
        (transition.fact().highwater, transition.fact().field_count),
        (2, 3)
    );
    assert_eq!(
        transition.before_fields().unwrap().get("b").unwrap(),
        fields.get("b")
    );
    assert!(
        transition
            .before_fields()
            .unwrap()
            .get("untouched")
            .is_err()
    );
    let mut changes = Vec::new();
    transition
        .visit_field_changes(&mut |name, old, new| {
            changes.push((
                name.to_string(),
                fireweed_engine::queue_index::ordered_key_ref(&[old])?,
                fireweed_engine::queue_index::ordered_key_ref(&[new])?,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        changes,
        vec![(
            "a\0α".into(),
            fireweed_engine::queue_index::ordered_key(&[fields.get("a\0α")]).unwrap(),
            fireweed_engine::queue_index::ordered_key(&[Some(&FieldValue::Integer(8))]).unwrap(),
        )]
    );
    let posting_baseline = admission.usage().unwrap();
    let mut postings = Vec::new();
    transition
        .visit_scalar_postings(&job, &mut |key, present| {
            postings.push((key.encode()?, present));
            Ok(())
        })
        .unwrap();
    assert_eq!(admission.usage().unwrap(), posting_baseline);
    let expected: Vec<_> = [(7, false), (8, true)]
        .into_iter()
        .map(|(value, present)| {
            let sort =
                fireweed_engine::queue_index::ordered_key(&[Some(&FieldValue::Integer(value))])
                    .unwrap();
            (
                fireweed_engine::queue_source_index::QueueSourceKey::ScalarPosting {
                    collection: None,
                    field: "a\0α",
                    sort: &sort,
                    key: "work\0α",
                }
                .encode()
                .unwrap(),
                present,
            )
        })
        .collect();
    assert_eq!(postings, expected);
    let named_baseline = admission.usage().unwrap();
    let mut named = Vec::new();
    transition
        .visit_named_postings(&job, &mut |effect| {
            named.push((
                effect.index.to_string(),
                effect.before.map(<[u8]>::to_vec),
                effect.after.map(<[u8]>::to_vec),
                effect.before_unique,
                effect.after_unique,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(admission.usage().unwrap(), named_baseline);
    assert_eq!(
        named,
        vec![(
            "ab".into(),
            Some(
                fireweed_engine::queue_index::ordered_key(&[fields.get("a\0α"), fields.get("b")])
                    .unwrap()
            ),
            Some(
                fireweed_engine::queue_index::ordered_key(&[
                    Some(&FieldValue::Integer(8)),
                    fields.get("b")
                ])
                .unwrap()
            ),
            false,
            false,
        )]
    );
    let mut dependencies = Vec::new();
    transition
        .visit_after_fields(&["a\0α", "b"], &mut |name, value| {
            dependencies.push((
                name.to_string(),
                fireweed_engine::queue_index::ordered_key_ref(&[value])?,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        dependencies,
        vec![
            (
                "a\0α".into(),
                fireweed_engine::queue_index::ordered_key(&[Some(&FieldValue::Integer(8))])
                    .unwrap()
            ),
            (
                "b".into(),
                fireweed_engine::queue_index::ordered_key(&[fields.get("b")]).unwrap()
            ),
        ]
    );
    assert!(
        transition
            .visit_after_fields(&["untouched"], &mut |_, _| Ok(()))
            .is_err()
    );
    let Some(fireweed_engine::queue_source_index::QueueSourceLive::Work { body, .. }) =
        transition.fact().live
    else {
        panic!()
    };
    assert_eq!(body.span().log_offset(), 1);
    assert_eq!(body.span().resolve(&checked_seed, 1).unwrap(), b"");
    log.shutdown().await.unwrap();
}
#[tokio::test]
async fn selective_field_preimages_match_engine_contract_local() {
    let dir = tempfile::tempdir().unwrap();
    field_preimages_contract(
        QueueNativeLog::local(dir.path(), "fields".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn s3_selective_field_preimages_run_identical_contract_both_signing_modes() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disable in [false, true] {
        let prefix = format!(
            "source-fields-{}-{disable}",
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
                disable_payload_signing: disable,
                ..Default::default()
            },
        )
        .unwrap();
        field_preimages_contract(log).await;
        let cleanup = object_log::S3BlobStore::new_owned_with_options(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
            object_log::S3ClientOptions::default(),
        )
        .unwrap();
        let keys = cleanup.list(&prefix).await.unwrap();
        assert!(!keys.is_empty());
        for key in keys {
            cleanup.delete(&key).await.unwrap();
        }
    }
}

mod s3_cancellation;
