use super::*;
use crate::queue_native_log::tests::{provision_limits, source_budget};

fn recovery_limits() -> QueueNativeRecoveryLimits {
    let limits = provision_limits();
    QueueNativeRecoveryLimits {
        read: limits.preparation.read,
        record: limits.record,
    }
}
async fn exact_budget(
    log: &QueueNativeLog,
    source: &QueueNativeCaptured,
    job: &Arc<QueueReservation>,
) -> Arc<QueueNativeSourceBudget> {
    let req = log
        .log
        .plan_read(&source.cut.cut, 0)
        .await
        .unwrap()
        .unwrap()
        .requirements()
        .unwrap();
    let work = recovery_limits().read.page.work;
    QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: QueueNativeSemanticWork {
                pages: 2 * work.pages,
                hydrated_bytes: 2 * work.hydrated_bytes,
            },
            record_attempts: 1,
            hydration_reads: req.hydration_reads(),
            hydration_body_bytes: req.hydration_bytes(),
        },
        job,
    )
    .unwrap()
}
fn exhausted(budget: &QueueNativeSourceBudget) {
    let left = budget.remaining_work();
    assert_eq!(
        (
            left.semantic.pages,
            left.semantic.hydrated_bytes,
            left.record_attempts,
            left.hydration_reads,
            left.hydration_body_bytes
        ),
        (0, 0, 0, 0, 0)
    );
}
async fn recovery_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let raw = super::provision::legacy_definition_bytes();
    let definition = AdmittedQueueDefinition::from_raw(&raw, &job).unwrap();
    let modern =
        AdmittedQueueDefinition::from_raw(&encode_definition(definition.schema()).unwrap(), &job)
            .unwrap();
    assert_ne!(definition.pin(), modern.pin());
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "queue\0α".into(),
    };
    let genesis = log
        .provision_record(
            &address,
            QueueNativeGenesisIntent {
                definition: &definition,
                candidate_secret: &[7; 32],
                known_queue: false,
            },
            source_budget(&job),
            provision_limits(),
            &job,
        )
        .await
        .unwrap();
    let source = log
        .capture_source(log.open(&address, &job).await.unwrap().unwrap(), &job)
        .await
        .unwrap();
    let head = source.head();
    let identity = source.semantic_identity.clone();
    let budget = exact_budget(&log, &source, &job).await;
    let recovered = log
        .recover_captured(source, budget.clone(), recovery_limits(), &job)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(recovered.budget(), &budget));
    assert!(Arc::ptr_eq(
        &recovered.as_source().semantic_identity,
        &identity
    ));
    assert!(recovered.as_acquired().is_none());
    assert_eq!(recovered.sequence(), 0);
    assert_eq!(recovered.genesis_record().bytes(), genesis.bytes());
    assert_eq!(recovered.definition().raw(), raw);
    assert_eq!(recovered.definition().pin(), definition.pin());
    assert_eq!(
        log.capture(&recovered.as_source().namespace)
            .await
            .unwrap()
            .head(),
        head
    );
    exhausted(&budget);
    drop((recovered, identity, budget));
    let source = log
        .capture_source(log.open(&address, &job).await.unwrap().unwrap(), &job)
        .await
        .unwrap();
    let advanced =
        super::provision::append_work_group(&log, &source, &address, &definition, "later", &job)
            .await;
    drop(source);
    let advanced_head = advanced.head();
    let genesis_epoch = genesis.view().unwrap().epoch();
    assert_eq!(advanced_head.bounds(), (0, 2));
    assert!(advanced_head.epoch() > genesis_epoch);
    let budget = exact_budget(&log, &advanced, &job).await;
    let historical = log
        .recover_captured(advanced, budget.clone(), recovery_limits(), &job)
        .await
        .unwrap();
    assert_eq!(historical.sequence(), 1);
    assert!(historical.as_acquired().is_none());
    assert_eq!(historical.genesis_record().bytes(), genesis.bytes());
    assert_eq!(
        historical.genesis_record().view().unwrap().epoch(),
        genesis_epoch
    );
    assert_eq!(historical.as_source().head(), advanced_head);
    exhausted(&budget);
    let acquired = log
        .acquire_bundle(
            historical
                .as_source()
                .retained_namespace(&address, &job)
                .unwrap(),
            &job,
        )
        .await
        .unwrap();
    let acquired_head = acquired.as_source().head();
    assert_eq!(acquired_head.bounds(), advanced_head.bounds());
    assert!(
        acquired_head.epoch() > advanced_head.epoch(),
        "a fence need not append"
    );
    let acquired_budget = exact_budget(&log, acquired.as_source(), &job).await;
    let writer = log
        .recover_acquired(acquired, acquired_budget.clone(), recovery_limits(), &job)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(writer.budget(), &acquired_budget));
    assert_eq!(
        writer.as_acquired().unwrap().as_source().head(),
        acquired_head
    );
    assert_eq!(writer.as_source().head(), acquired_head);
    assert_eq!(writer.sequence(), 1);
    assert_eq!(writer.genesis_record().bytes(), genesis.bytes());
    assert_eq!(writer.definition().raw(), raw);
    assert_eq!(
        log.capture(&writer.as_source().namespace)
            .await
            .unwrap()
            .head(),
        acquired_head
    );
    assert_eq!(
        historical.as_source().head(),
        advanced_head,
        "old source remains immutable"
    );
    exhausted(&acquired_budget);
    drop((
        writer,
        historical,
        budget,
        acquired_budget,
        genesis,
        definition,
        modern,
    ));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn captured_recovery_retains_persisted_definition_cut_and_ledger_local() {
    let directory = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(directory.path(), "recovery".into(), options(1 << 20)).unwrap();
    recovery_contract(log).await;
}
#[tokio::test]
async fn captured_recovery_retains_persisted_definition_cut_and_ledger_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "recovery/{}/{disabled}",
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
        recovery_contract(log).await;
        let provider = object_log::S3BlobStore::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
        );
        for key in provider.list(&prefix).await.unwrap() {
            provider.delete(&key).await.unwrap();
        }
    }
}

async fn malformed_source(
    log: &QueueNativeLog,
    job: &Arc<QueueReservation>,
    fault: &str,
) -> (
    QueueNativeCaptured,
    OwnedQueueLogRecord,
    object_log::NativeReadRequirements,
) {
    use fireweed_engine::queue_source_index::{
        QueueSourceGenesis, QueueSourceKey, QueueSourceSpan,
    };
    use sha2::{Digest, Sha256};
    let (acquired, record) = bundle(log, "queue", job).await;
    let mut facts = {
        let checked = record.checked().unwrap();
        let QueueLogPayload::Genesis(value) = checked.view().payload() else {
            panic!();
        };
        let definition = value.validate_definition(job).unwrap();
        let compiled = fireweed_engine::queue_source_index::compile_genesis(
            &checked,
            acquired.as_source().source_cut(log, job).unwrap(),
            &definition,
            provision_limits().preparation.compile,
            job,
        )
        .unwrap();
        compiled
            .edits()
            .map(|(key, value)| (key.to_vec(), value.unwrap().to_vec()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let record = if fault == "definition" {
        let mut raw = record.bytes().to_vec();
        let (offset, length) = {
            let checked = record.checked().unwrap();
            let QueueLogPayload::Genesis(value) = checked.view().payload() else {
                panic!();
            };
            let definition = value.definition_bytes();
            (
                definition.as_ptr() as usize - record.bytes().as_ptr() as usize,
                definition.len(),
            )
        };
        // Same-length, structurally valid schema with forbidden max_attempts=0.
        assert_eq!(raw[offset + length - 1], 3);
        raw[offset + length - 1] = 0;
        assert!(
            fireweed_engine::queue_schema_codec::decode_definition(&raw[offset..offset + length])
                .is_err()
        );
        let end = raw.len() - 32;
        let checksum = Sha256::digest(&raw[..end]);
        raw[end..].copy_from_slice(&checksum);
        let charge = job.owned_buffer(4 * raw.len() as u64 + 4096).unwrap();
        let changed = OwnedQueueLogRecord::from_admitted_bytes(raw.into(), charge, job).unwrap();
        let checked = changed.checked().unwrap();
        let fact = QueueSourceGenesis {
            source: QueueSourceSpan::from_checked(
                &checked,
                0,
                &changed.bytes()[checked.payload_interval().unwrap()],
            )
            .unwrap(),
            definition_pin:
                fireweed_engine::queue_schema_codec::DefinitionPin::from_persisted_bytes(
                    &changed.bytes()[offset..offset + length],
                ),
        };
        fact.verify_record(&checked, 0).unwrap();
        facts.insert(
            QueueSourceKey::Genesis.encode().unwrap(),
            fireweed_engine::queue_storage_layout::encode(&fact).unwrap(),
        );
        changed
    } else {
        record
    };
    if matches!(fault, "subspan" | "pin") {
        let key = QueueSourceKey::Genesis.encode().unwrap();
        let mut fact: QueueSourceGenesis =
            fireweed_engine::queue_storage_layout::decode(facts.get(&key).unwrap()).unwrap();
        let checked = record.checked().unwrap();
        let QueueLogPayload::Genesis(value) = checked.view().payload() else {
            panic!();
        };
        if fault == "subspan" {
            fact.source =
                QueueSourceSpan::from_checked(&checked, 0, value.definition_bytes()).unwrap();
            fact.source.resolve(&checked, 0).unwrap();
        } else {
            fact.definition_pin =
                fireweed_engine::queue_schema_codec::DefinitionPin::from_persisted_bytes(
                    b"different persisted pin",
                );
        }
        assert!(fact.verify_record(&checked, 0).is_err());
        facts.insert(
            key,
            fireweed_engine::queue_storage_layout::encode(&fact).unwrap(),
        );
    }
    let sequence = QueueSourceKey::Sequence.encode().unwrap();
    match fault {
        "missing-sequence" => {
            facts.remove(&sequence);
        }
        "nonminimal-sequence" => {
            facts.insert(sequence, vec![0x80, 0]);
        }
        "trailing-sequence" => {
            facts.insert(sequence, vec![0, 0]);
        }
        "nonzero-genesis-sequence" => {
            facts.insert(sequence, vec![1]);
        }
        "checksum" | "definition" | "subspan" | "pin" => {}
        _ => panic!("unknown fault"),
    }
    let mut payload = record.bytes().to_vec();
    if fault == "checksum" {
        *payload.last_mut().unwrap() ^= 1;
    }
    let edits: Vec<_> = facts
        .iter()
        .map(|(key, value)| object_log::NativeSemanticEdit {
            key,
            value: Some(value),
        })
        .collect();
    let limits = provision_limits().preparation.prepare;
    // Independent SDK fixture deliberately bypasses the engine compiler for
    // malformed authority; production publication has no such input path.
    let publication = log
        .log
        .prepare_with_edits(
            &acquired.writer.writer,
            &acquired.source.cut.cut,
            &[object_log::NativeAppend {
                identity: "malformed-recovery-fixture",
                payload: &payload,
                records: 1,
            }],
            &edits,
            object_log::NativeEditLimits {
                edits: limits.edits,
                input_bytes: limits.input_bytes,
                work: limits.work.sdk(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        log.log.submit(&publication).await.unwrap(),
        object_log::NativePublishOutcome::Stored(_)
    ));
    let source = log
        .capture_source(
            acquired
                .as_source()
                .retained_namespace(
                    &QueueAddress {
                        tenant: "tenant".into(),
                        queue: "queue".into(),
                    },
                    job,
                )
                .unwrap(),
            job,
        )
        .await
        .unwrap();
    let req = log
        .log
        .plan_read(&source.cut.cut, 0)
        .await
        .unwrap()
        .unwrap()
        .requirements()
        .unwrap();
    (source, record, req)
}
#[tokio::test]
async fn recovery_rejects_malformed_authority_and_releases_memory_without_refunding_work() {
    for fault in [
        "missing-sequence",
        "nonminimal-sequence",
        "trailing-sequence",
        "nonzero-genesis-sequence",
        "checksum",
        "definition",
        "subspan",
        "pin",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
        let provider = object_log::LocalBlobStore::new(directory.path());
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (source, record, req) = malformed_source(&log, &job, fault).await;
        let namespace_bytes =
            source.namespace._charge.admitted_bytes() + source.semantic_identity.admitted_bytes();
        let budget = exact_budget(&log, &source, &job).await;
        let baseline = admission.usage().unwrap();
        let before = storage_image(&provider, fault).await;
        let result = log
            .recover_captured(source, budget.clone(), recovery_limits(), &job)
            .await;
        let expected = match fault {
            "missing-sequence" => "covered source has no sequence fact",
            "nonminimal-sequence" | "nonzero-genesis-sequence" => "invalid canonical sequence",
            "trailing-sequence" => "trailing persisted bytes",
            "checksum" => "invalid stored source record",
            "definition" => "invalid recovery definition",
            "subspan" | "pin" => "genesis record, full payload span or stored pin differs",
            _ => unreachable!(),
        };
        assert!(
            matches!(result,Err(QueueError::OutcomeUnknown(ref message)) if message.contains(expected)),
            "{fault}"
        );
        assert_eq!(
            admission.usage().unwrap(),
            (baseline.0, baseline.1 - namespace_bytes),
            "{fault}"
        );
        assert_eq!(storage_image(&provider, fault).await, before);
        let left = budget.remaining_work();
        assert_eq!((left.semantic.pages, left.semantic.hydrated_bytes), (0, 0));
        if matches!(fault, "checksum" | "definition" | "subspan" | "pin") {
            exhausted(&budget);
        } else {
            assert_eq!(
                (
                    left.record_attempts,
                    left.hydration_reads,
                    left.hydration_body_bytes
                ),
                (1, req.hydration_reads(), req.hydration_bytes())
            );
        }
        drop((record, budget));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}

#[tokio::test]
async fn recovery_two_lookup_budget_is_atomic_and_late_record_limit_does_not_refund_work() {
    for fault in ["pages", "bytes", "record"] {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (source, record, req) = super::record::seeded(&log, &job, "queue").await;
        let source_bytes =
            source.namespace._charge.admitted_bytes() + source.semantic_identity.admitted_bytes();
        let work = recovery_limits().read.page.work;
        let mut initial = QueueNativeSourceWork {
            semantic: QueueNativeSemanticWork {
                pages: 2 * work.pages,
                hydrated_bytes: 2 * work.hydrated_bytes,
            },
            record_attempts: 1,
            hydration_reads: req.hydration_reads(),
            hydration_body_bytes: req.hydration_bytes(),
        };
        let mut limits = recovery_limits();
        match fault {
            "pages" => initial.semantic.pages -= 1,
            "bytes" => initial.semantic.hydrated_bytes -= 1,
            "record" => limits.record.payload_bytes = record.bytes().len() as u64 - 1,
            _ => unreachable!(),
        }
        let budget = QueueNativeSourceBudget::new(initial, &job).unwrap();
        let baseline = admission.usage().unwrap();
        let result = log
            .recover_captured(source, budget.clone(), limits, &job)
            .await;
        let left = budget.remaining_work();
        if fault == "record" {
            assert!(matches!(result, Err(QueueError::TooLarge)));
            assert_eq!(
                (
                    left.semantic.pages,
                    left.semantic.hydrated_bytes,
                    left.record_attempts
                ),
                (0, 0, 0)
            );
        } else {
            assert!(matches!(result,Err(QueueError::OutcomeUnknown(ref message))
                if message.contains("request source work admission exhausted")));
            assert_eq!(
                (
                    left.semantic.pages,
                    left.semantic.hydrated_bytes,
                    left.record_attempts
                ),
                (
                    initial.semantic.pages - work.pages,
                    initial.semantic.hydrated_bytes - work.hydrated_bytes,
                    1
                )
            );
        }
        assert_eq!(
            (left.hydration_reads, left.hydration_body_bytes),
            (req.hydration_reads(), req.hydration_bytes())
        );
        assert_eq!(
            admission.usage().unwrap(),
            (baseline.0, baseline.1 - source_bytes)
        );
        drop((budget, record));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}

#[tokio::test]
async fn recovery_constructors_reject_foreign_scope_invalid_limits_and_empty_sources_before_reads()
{
    for acquired in [false, true] {
        for fault in [
            "log",
            "job",
            "ledger",
            "read-range",
            "read-rows",
            "read-output",
            "read-pages",
            "read-bytes",
            "record-payload",
            "record-logical",
            "record-physical",
            "record-frame",
            "empty",
            "valid-closed",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let log =
                QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
            let other =
                QueueNativeLog::local(directory.path(), "other".into(), options(1 << 20)).unwrap();
            let provider = object_log::LocalBlobStore::new(directory.path());
            let admission = QueueAdmission::new(2, 32 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let foreign = Arc::new(admission.reserve(0).unwrap());
            let address = QueueAddress {
                tenant: "tenant".into(),
                queue: "queue".into(),
            };
            let (writer, record) = if fault == "empty" {
                bundle(&log, "queue", &job).await
            } else {
                let (source, record, _) = super::record::seeded(&log, &job, "queue").await;
                let writer = log
                    .acquire_bundle(source.retained_namespace(&address, &job).unwrap(), &job)
                    .await
                    .unwrap();
                drop(source);
                (writer, record)
            };
            let (writer, captured) = if acquired {
                (Some(writer), None)
            } else {
                let source = log
                    .capture_source(
                        writer
                            .as_source()
                            .retained_namespace(&address, &job)
                            .unwrap(),
                        &job,
                    )
                    .await
                    .unwrap();
                drop(writer);
                (None, Some(source))
            };
            let source = writer
                .as_ref()
                .map(QueueNativeAcquired::as_source)
                .or(captured.as_ref())
                .unwrap();
            let source_bytes = source.namespace._charge.admitted_bytes()
                + source.semantic_identity.admitted_bytes();
            let mut limits = recovery_limits();
            match fault {
                "read-range" => limits.read.range_pages = 0,
                "read-rows" => limits.read.page.rows = 0,
                "read-output" => limits.read.page.output_bytes = 0,
                "read-pages" => limits.read.page.work.pages = 0,
                "read-bytes" => limits.read.page.work.hydrated_bytes = 0,
                "record-payload" => limits.record.payload_bytes = 0,
                "record-logical" => limits.record.logical_identity_bytes = 0,
                "record-physical" => limits.record.physical_identity_bytes = 0,
                "record-frame" => limits.record.frame_bytes = 0,
                _ => {}
            }
            let caller = if fault == "job" { &foreign } else { &job };
            let budget = source_budget(if fault == "ledger" { &foreign } else { caller });
            let initial = budget.remaining_work();
            log.shutdown().await.unwrap();
            other.shutdown().await.unwrap();
            let baseline = admission.usage().unwrap();
            let before = storage_image(&provider, fault).await;
            let target = if fault == "log" { &other } else { &log };
            let result = if let Some(writer) = writer {
                target
                    .recover_acquired(writer, budget.clone(), limits, caller)
                    .await
            } else {
                target
                    .recover_captured(captured.unwrap(), budget.clone(), limits, caller)
                    .await
            };
            let expected = match fault {
                "log" | "job" => "semantic namespace, cut or operation differs",
                "ledger" => "source work ledger belongs to another operation",
                "read-range" | "read-rows" | "read-output" | "read-pages" | "read-bytes" => {
                    "source read limits invalid"
                }
                "record-payload" | "record-logical" | "record-physical" | "record-frame" => {
                    "source record limits must be positive"
                }
                "empty" => "recovery source does not contain genesis offset zero",
                "valid-closed" => "native execution closed",
                _ => unreachable!(),
            };
            assert!(
                matches!(result,Err(QueueError::OutcomeUnknown(ref message)) if message.contains(expected)),
                "{fault}/{acquired}"
            );
            let spent = if fault == "valid-closed" {
                recovery_limits().read.page.work
            } else {
                QueueNativeSemanticWork {
                    pages: 0,
                    hydrated_bytes: 0,
                }
            };
            let left = budget.remaining_work();
            assert_eq!(
                (
                    left.semantic.pages,
                    left.semantic.hydrated_bytes,
                    left.record_attempts,
                    left.hydration_reads,
                    left.hydration_body_bytes
                ),
                (
                    initial.semantic.pages - spent.pages,
                    initial.semantic.hydrated_bytes - spent.hydrated_bytes,
                    initial.record_attempts,
                    initial.hydration_reads,
                    initial.hydration_body_bytes
                ),
                "{fault}/{acquired}"
            );
            assert_eq!(
                admission.usage().unwrap(),
                (baseline.0, baseline.1 - source_bytes),
                "{fault}/{acquired}"
            );
            assert_eq!(storage_image(&provider, fault).await, before);
            drop((budget, record, job, foreign));
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
    }
}
