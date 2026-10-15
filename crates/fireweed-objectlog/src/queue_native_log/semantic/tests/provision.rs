use super::*;
use crate::queue_native_log::tests::{provision_limits, source_budget};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize)]
struct LegacyDefinition {
    fields: BTreeMap<String, fireweed_core::queue_contract::FieldType>,
    indexes: BTreeMap<String, Vec<String>>,
    unique_indexes: BTreeSet<String>,
}
#[derive(Serialize)]
struct LegacySchema {
    work: LegacyDefinition,
    state: BTreeMap<String, LegacyDefinition>,
    progress_bound_nanos: u64,
    max_attempts: u32,
}
async fn provision_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let legacy = fireweed_engine::queue_storage_layout::encode(&LegacySchema {
        work: LegacyDefinition {
            fields: BTreeMap::new(),
            indexes: BTreeMap::new(),
            unique_indexes: BTreeSet::new(),
        },
        state: BTreeMap::new(),
        progress_bound_nanos: 1,
        max_attempts: 3,
    })
    .unwrap();
    let persisted = AdmittedQueueDefinition::from_raw(&legacy, &job).unwrap();
    let modern =
        AdmittedQueueDefinition::from_raw(&encode_definition(persisted.schema()).unwrap(), &job)
            .unwrap();
    assert_ne!(persisted.pin(), modern.pin());
    assert_eq!(persisted.schema(), modern.schema());
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "queue\0α".into(),
    };
    let budget = source_budget(&job);
    let first = log
        .provision_record(
            &address,
            QueueNativeGenesisIntent {
                definition: &persisted,
                candidate_secret: &[7; 32],
                known_queue: false,
            },
            budget.clone(),
            provision_limits(),
            &job,
        )
        .await
        .unwrap();
    let namespace = log.open(&address, &job).await.unwrap().unwrap();
    let source = log.capture_source(namespace, &job).await.unwrap();
    let before = source.head();
    assert_eq!(before.bounds(), (0, 1));
    let reader = QueueNativeSourceReader::new(
        &source,
        &log,
        &job,
        budget.clone(),
        provision_limits().preparation.read,
    )
    .unwrap();
    let fact = fireweed_engine::queue_source_index::load_source_genesis(&reader, &job)
        .await
        .unwrap();
    fact.verify_record(&first.checked().unwrap(), 0).unwrap();
    assert_eq!(fact.definition_pin, persisted.pin());
    let page = source
        .semantic_scan(
            &log,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            QueueNativeScanLimits {
                rows: 8,
                output_bytes: 4096,
                work: provision_limits().preparation.read.page.work,
            },
            &job,
        )
        .await
        .unwrap();
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    let actual: BTreeMap<_, _> = page
        .rows()
        .map(|row| (row.key().to_vec(), row.value().to_vec()))
        .collect();
    let mut expected_keys = BTreeSet::from([
        fireweed_engine::queue_source_index::QueueSourceKey::Genesis
            .encode()
            .unwrap(),
        fireweed_engine::queue_source_index::QueueSourceKey::Sequence
            .encode()
            .unwrap(),
    ]);
    for lifecycle in [
        fireweed_core::queue_contract::Lifecycle::Pending,
        fireweed_core::queue_contract::Lifecycle::Leased,
        fireweed_core::queue_contract::Lifecycle::Completed,
        fireweed_core::queue_contract::Lifecycle::Failed,
    ] {
        expected_keys.insert(
            fireweed_engine::queue_source_index::QueueSourceKey::LifecycleCount(lifecycle)
                .encode()
                .unwrap(),
        );
    }
    assert_eq!(
        actual.keys().cloned().collect::<BTreeSet<_>>(),
        expected_keys
    );
    assert_eq!(actual.len(), 6);
    for (key, value) in &actual {
        if key
            != &fireweed_engine::queue_source_index::QueueSourceKey::Genesis
                .encode()
                .unwrap()
        {
            assert_eq!(
                value,
                &fireweed_engine::queue_storage_layout::encode(&0u64).unwrap()
            );
        }
    }
    drop(page);
    let requirements = log
        .log
        .plan_read(&source.cut.cut, 0)
        .await
        .unwrap()
        .unwrap()
        .requirements()
        .unwrap();
    let mut adoption_limits = provision_limits();
    let read_work = adoption_limits.preparation.read.page.work;
    // Existing-winner adoption needs ONE lookup plus record hydration, not a
    // genesis write/preparation grant. An accidental write preflight must fail.
    adoption_limits.preparation.prepare.work.pages = read_work.pages + 1;
    adoption_limits.preparation.prepare.work.hydrated_bytes = read_work.hydrated_bytes + 1;
    let adoption_budget = QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: read_work,
            record_attempts: 1,
            hydration_reads: requirements.hydration_reads(),
            hydration_body_bytes: requirements.hydration_bytes(),
        },
        &job,
    )
    .unwrap();
    let adopted = log
        .provision_record(
            &address,
            QueueNativeGenesisIntent {
                definition: &modern,
                candidate_secret: &[99; 32],
                known_queue: true,
            },
            adoption_budget.clone(),
            adoption_limits,
            &job,
        )
        .await
        .unwrap();
    let remaining = adoption_budget.remaining_work();
    assert_eq!(
        (
            remaining.semantic.pages,
            remaining.semantic.hydrated_bytes,
            remaining.record_attempts,
            remaining.hydration_reads,
            remaining.hydration_body_bytes
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(adopted.bytes(), first.bytes());
    let checked = adopted.checked().unwrap();
    let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
        panic!()
    };
    assert_eq!(genesis.cursor_secret(), &[7; 32]);
    assert_eq!(genesis.definition_bytes(), legacy);
    let observed = log
        .capture_source(source.retained_namespace(&address, &job).unwrap(), &job)
        .await
        .unwrap();
    assert_eq!(
        observed.head(),
        before,
        "adoption must not acquire another fence"
    );
    if let Some(probe) = &log.submit_probe {
        assert_eq!(probe.acquisition_count(), 1, "exactly one fence attempt");
        let (calls, preparations, accepted, injected) = probe.snapshot();
        assert_eq!((calls, preparations, injected), (1, 1, 1));
        assert_eq!(
            accepted,
            vec![observed.head()],
            "actual SDK Stored head remains authoritative after lost Root reply and adoption"
        );
    }
    drop(reader);
    drop((
        first,
        adopted,
        observed,
        source,
        modern,
        persisted,
        budget,
        adoption_budget,
    ));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn coupled_provision_and_equivalent_schema_adoption_local() {
    let directory = tempfile::tempdir().unwrap();
    provision_contract(
        QueueNativeLog::local(
            directory.path(),
            "coupled-provision".into(),
            options(1 << 20),
        )
        .unwrap(),
    )
    .await;
}
#[tokio::test]
async fn coupled_provision_and_equivalent_schema_adoption_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "coupled-provision/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        provision_contract(
            QueueNativeLog::s3(
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
            .unwrap(),
        )
        .await;
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

#[tokio::test]
async fn provision_exhausted_adoption_capacity_precedes_namespace_io() {
    for closed in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let prefix = "preflight";
        let log = QueueNativeLog::local(directory.path(), prefix.into(), options(1 << 20)).unwrap();
        let provider = object_log::LocalBlobStore::new(directory.path());
        if closed {
            log.shutdown().await.unwrap();
        }
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
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
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "unbound".into(),
        };
        let limits = provision_limits();
        let initial = QueueNativeSourceWork {
            semantic: limits.preparation.read.page.work,
            record_attempts: 1,
            hydration_reads: 1,
            hydration_body_bytes: 1,
        };
        let before = storage_image(&provider, prefix).await;
        for dimension in ["pages", "semantic-bytes", "attempts", "reads", "body-bytes"] {
            let mut work = initial;
            match dimension {
                "pages" => work.semantic.pages -= 1,
                "semantic-bytes" => work.semantic.hydrated_bytes -= 1,
                "attempts" => work.record_attempts = 0,
                "reads" => work.hydration_reads = 0,
                "body-bytes" => work.hydration_body_bytes = 0,
                _ => unreachable!(),
            }
            let budget = QueueNativeSourceBudget::new(work, &job).unwrap();
            let result = log
                .provision_record(
                    &address,
                    QueueNativeGenesisIntent {
                        definition: &definition,
                        candidate_secret: &[7; 32],
                        known_queue: false,
                    },
                    budget.clone(),
                    limits,
                    &job,
                )
                .await;
            assert!(
                matches!(result,Err(QueueError::OutcomeUnknown(ref message))
                if message.contains("request source adoption admission exhausted")),
                "{dimension}"
            );
            let remaining = budget.remaining_work();
            assert_eq!(
                (
                    remaining.semantic.pages,
                    remaining.semantic.hydrated_bytes,
                    remaining.record_attempts,
                    remaining.hydration_reads,
                    remaining.hydration_body_bytes
                ),
                (
                    work.semantic.pages,
                    work.semantic.hydrated_bytes,
                    work.record_attempts,
                    work.hydration_reads,
                    work.hydration_body_bytes
                )
            );
            assert_eq!(storage_image(&provider, prefix).await, before);
        }
        // Invalid configuration and foreign operation owners must fail before
        // even namespace lookup, including when the SDK is already closed.
        for dimension in 0..15 {
            let mut invalid = limits;
            match dimension {
                0 => invalid.preparation.read.range_pages = 0,
                1 => invalid.preparation.read.page.output_bytes = 0,
                2 => invalid.preparation.read.page.rows = 0,
                3 => invalid.preparation.read.page.work.pages = 0,
                4 => invalid.preparation.read.page.work.hydrated_bytes = 0,
                5 => invalid.preparation.compile.edits = 0,
                6 => invalid.preparation.compile.input_bytes = 0,
                7 => invalid.preparation.prepare.edits = 0,
                8 => invalid.preparation.prepare.input_bytes = 0,
                9 => invalid.preparation.prepare.work.pages = 0,
                10 => invalid.preparation.prepare.work.hydrated_bytes = 0,
                11 => invalid.record.payload_bytes = 0,
                12 => invalid.record.logical_identity_bytes = 0,
                13 => invalid.record.physical_identity_bytes = 0,
                14 => invalid.record.frame_bytes = 0,
                _ => unreachable!(),
            }
            let budget = QueueNativeSourceBudget::new(initial, &job).unwrap();
            let result = log
                .provision_record(
                    &address,
                    QueueNativeGenesisIntent {
                        definition: &definition,
                        candidate_secret: &[7; 32],
                        known_queue: false,
                    },
                    budget.clone(),
                    invalid,
                    &job,
                )
                .await;
            let expected = match dimension {
                0..=4 => "source read limits invalid",
                5..=10 => "source preparation limits invalid",
                11..=14 => "source record limits must be positive",
                _ => unreachable!(),
            };
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message))
                if message.contains(expected)),
                "invalid dimension {dimension}"
            );
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
                    initial.semantic.pages,
                    initial.semantic.hydrated_bytes,
                    initial.record_attempts,
                    initial.hydration_reads,
                    initial.hydration_body_bytes
                )
            );
            assert_eq!(storage_image(&provider, prefix).await, before);
        }
        let other_admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let other_job = Arc::new(other_admission.reserve(0).unwrap());
        let other_definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &other_job)
                .unwrap();
        for foreign_ledger in [false, true] {
            let budget = QueueNativeSourceBudget::new(
                initial,
                if foreign_ledger { &other_job } else { &job },
            )
            .unwrap();
            let result = log
                .provision_record(
                    &address,
                    QueueNativeGenesisIntent {
                        definition: if foreign_ledger {
                            &definition
                        } else {
                            &other_definition
                        },
                        candidate_secret: &[7; 32],
                        known_queue: false,
                    },
                    budget.clone(),
                    limits,
                    &job,
                )
                .await;
            let expected = if foreign_ledger {
                "source work ledger belongs to another operation"
            } else {
                "provision definition belongs to another operation"
            };
            assert!(
                matches!(result, Err(QueueError::OutcomeUnknown(ref message))
                if message.contains(expected))
            );
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
                    initial.semantic.pages,
                    initial.semantic.hydrated_bytes,
                    initial.record_attempts,
                    initial.hydration_reads,
                    initial.hydration_body_bytes
                )
            );
            assert_eq!(storage_image(&provider, prefix).await, before);
        }
        drop((other_definition, other_job));
        assert_eq!(other_admission.usage().unwrap(), (0, 0));
        if closed {
            let budget = QueueNativeSourceBudget::new(initial, &job).unwrap();
            let result = log
                .provision_record(
                    &address,
                    QueueNativeGenesisIntent {
                        definition: &definition,
                        candidate_secret: &[7; 32],
                        known_queue: false,
                    },
                    budget,
                    limits,
                    &job,
                )
                .await;
            assert!(matches!(result,Err(QueueError::OutcomeUnknown(ref message))
                if message.contains("native execution closed")));
        } else {
            log.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn provision_validates_covered_genesis_authority_before_schema_conflict_without_mutation() {
    use fireweed_engine::queue_source_index::{
        QueueSourceGenesis, QueueSourceKey, QueueSourceSpan,
    };
    for fault in [
        "valid",
        "missing",
        "subspan",
        "pin",
        "offset",
        "uninitialized",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
        let provider = object_log::LocalBlobStore::new(directory.path());
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (acquired, record) = bundle(&log, "queue", &job).await;
        let checked = record.checked().unwrap();
        let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
            panic!()
        };
        let definition = genesis.validate_definition(&job).unwrap();
        let publication = if fault == "uninitialized" {
            acquired.prepare_record(&log, &record, &job).await.unwrap()
        } else {
            let compiled = fireweed_engine::queue_source_index::compile_genesis(
                &checked,
                acquired.as_source().source_cut(&log, &job).unwrap(),
                &definition,
                provision_limits().preparation.compile,
                &job,
            )
            .unwrap();
            let key = QueueSourceKey::Genesis.encode().unwrap();
            let mut facts: BTreeMap<Vec<u8>, Vec<u8>> = compiled
                .edits()
                .map(|(key, value)| (key.to_vec(), value.unwrap().to_vec()))
                .collect();
            if fault == "missing" {
                facts.remove(&key);
            } else if fault != "valid" {
                let mut fact: QueueSourceGenesis =
                    fireweed_engine::queue_storage_layout::decode(facts.get(&key).unwrap())
                        .unwrap();
                match fault {
                    "subspan" => {
                        fact.source =
                            QueueSourceSpan::from_checked(&checked, 0, genesis.definition_bytes())
                                .unwrap();
                        fact.source.resolve(&checked, 0).unwrap();
                    }
                    "pin" => {
                        fact.definition_pin =
                            fireweed_engine::queue_schema_codec::DefinitionPin::from_persisted_bytes(
                                b"wrong stored pin",
                            )
                    }
                    "offset" => {
                        fact.source = QueueSourceSpan::from_checked(
                            &checked,
                            1,
                            &record.bytes()[checked.payload_interval().unwrap()],
                        )
                        .unwrap()
                    }
                    _ => unreachable!(),
                }
                facts.insert(
                    key,
                    fireweed_engine::queue_storage_layout::encode(&fact).unwrap(),
                );
            }
            let edits: Vec<_> = facts
                .iter()
                .map(|(key, value)| QueueNativeSemanticEdit {
                    key,
                    value: Some(value),
                })
                .collect();
            acquired
                .prepare_with_edits(
                    &log,
                    &record,
                    "fixture",
                    &edits,
                    provision_limits().preparation.prepare,
                    &job,
                )
                .await
                .unwrap()
        };
        assert!(matches!(
            publication.submit(&log).await.unwrap(),
            QueueNativeSubmit::Stored(_)
        ));
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let source = log
            .capture_source(
                acquired
                    .as_source()
                    .retained_namespace(&address, &job)
                    .unwrap(),
                &job,
            )
            .await
            .unwrap();
        let before = source.head();
        let image = storage_image(&provider, fault).await;
        let mut schema = definition.schema().clone();
        schema.max_attempts += 1;
        let different =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let result = log
            .provision_record(
                &address,
                QueueNativeGenesisIntent {
                    definition: &different,
                    candidate_secret: &[99; 32],
                    known_queue: true,
                },
                source_budget(&job),
                provision_limits(),
                &job,
            )
            .await;
        if fault == "valid" {
            assert!(matches!(result, Err(QueueError::DefinitionConflict)));
        } else {
            let expected = match fault {
                "missing" => "covered source has no genesis fact",
                "subspan" | "pin" => "full payload span or stored pin differs",
                "offset" => "invalid genesis source span",
                "uninitialized" => "captured head has no semantic coverage",
                _ => unreachable!(),
            };
            assert!(
                matches!(result,Err(QueueError::OutcomeUnknown(ref message))
                if message.contains(expected)),
                "{fault}: expected {expected}"
            );
        }
        let after = log
            .capture_source(source.retained_namespace(&address, &job).unwrap(), &job)
            .await
            .unwrap();
        assert_eq!(after.head(), before, "{fault}");
        assert_eq!(storage_image(&provider, fault).await, image, "{fault}");
        log.shutdown().await.unwrap();
    }
}

struct ProvisionClock;
impl fireweed_engine::Clock for ProvisionClock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        fireweed_core::UtcTimestamp {
            seconds: 0,
            nanoseconds: 1,
        }
    }
}
async fn append_work_group(
    log: &QueueNativeLog,
    source: &QueueNativeCaptured,
    address: &QueueAddress,
    definition: &AdmittedQueueDefinition,
    request_id: &str,
    job: &Arc<QueueReservation>,
) -> QueueNativeCaptured {
    let acquired = log
        .acquire_bundle(source.retained_namespace(address, job).unwrap(), job)
        .await
        .unwrap();
    let budget = source_budget(job);
    let limits = provision_limits().preparation;
    let reader =
        QueueNativeSourceReader::new(acquired.as_source(), log, job, budget.clone(), limits.read)
            .unwrap();
    let group = fireweed_core::queue_contract::CommitGroup {
        request_id: request_id.into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![fireweed_core::queue_contract::NewWork {
            key: request_id.into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: Default::default(),
        }],
        work_updates: vec![],
        administration: None,
    };
    let queue_limits = fireweed_core::queue_contract::QueueLimits::default();
    let plan = fireweed_engine::queue_source_index::load_group_preimage(
        &reader,
        address,
        &group,
        definition,
        &queue_limits,
        job,
    )
    .await
    .unwrap()
    .plan(&ProvisionClock)
    .unwrap();
    assert!(matches!(
        plan.receipt().outcome,
        fireweed_core::queue_contract::GroupOutcome::Committed { .. }
    ));
    let publication = plan
        .encode(
            fireweed_engine::queue_source_index::QueueSourceGroupEncodingLimits {
                frame_bytes: 1 << 20,
                record_bytes: 1 << 20,
            },
        )
        .unwrap();
    let original = acquired
        .prepare_group_publication(log, &publication, definition, budget, limits, job)
        .await
        .unwrap();
    assert!(matches!(
        original.submit(log).await.unwrap(),
        QueueNativeSubmit::Stored(_)
    ));
    assert_eq!(
        original.resolve(log, 1).await.unwrap(),
        QueueNativeResolution::DurableContent
    );
    log.capture_source(
        acquired
            .as_source()
            .retained_namespace(address, job)
            .unwrap(),
        job,
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn historical_genesis_adoption_survives_a_later_append_during_frame_read_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let proxy = super::s3_cancellation::Proxy::new(&env.endpoint);
        let prefix = format!(
            "advanced-genesis/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let log = Arc::new(
            QueueNativeLog::s3(
                QueueNativeS3Connection {
                    endpoint: proxy.endpoint(),
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
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
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
        let definition = Arc::new(
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap(),
        );
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
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
        let original_epoch = genesis.view().unwrap().epoch();
        let initial_source = log
            .capture_source(log.open(&address, &job).await.unwrap().unwrap(), &job)
            .await
            .unwrap();
        let source = Arc::new(
            append_work_group(
                &log,
                &initial_source,
                &address,
                &definition,
                "later-1",
                &job,
            )
            .await,
        );
        drop(initial_source);
        assert_eq!(source.head().bounds(), (0, 2));
        assert!(source.head().epoch() > original_epoch);
        let (entered, release) = proxy.arm_matching(Some(b"/coupled-data/"));
        let caller = {
            let (log, source, definition, job) =
                (log.clone(), source.clone(), definition.clone(), job.clone());
            tokio::spawn(async move {
                log.adopt_genesis(
                    &source,
                    &definition,
                    source_budget(&job),
                    provision_limits(),
                    &job,
                )
                .await
            })
        };
        let request = tokio::time::timeout(std::time::Duration::from_secs(10), entered)
            .await
            .unwrap()
            .unwrap();
        let line = std::str::from_utf8(&request)
            .unwrap()
            .lines()
            .next()
            .unwrap();
        assert!(line.starts_with("GET "), "{line}");
        assert!(
            line.contains(&format!("/{}/{prefix}/namespaces/candidates/", env.bucket)),
            "{line}"
        );
        assert!(line.contains("/coupled-data/"), "{line}");
        let advanced =
            append_work_group(&log, &source, &address, &definition, "later-2", &job).await;
        assert_eq!(advanced.head().bounds(), (0, 3));
        assert!(advanced.head().epoch() > source.head().epoch());
        assert!(
            !caller.is_finished(),
            "later commit completed while historical frame read remains gated"
        );
        release.send(()).unwrap();
        let adopted = caller.await.unwrap().unwrap();
        assert_eq!(adopted.record.bytes(), genesis.bytes());
        let after = log
            .capture_source(advanced.retained_namespace(&address, &job).unwrap(), &job)
            .await
            .unwrap();
        assert_eq!(
            after.head(),
            advanced.head(),
            "adoption does not refence or require head quiescence"
        );
        drop((adopted, genesis, source, advanced, after, definition));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(proxy);
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

#[tokio::test]
async fn lost_root_submit_reply_after_actual_stored_adopts_original_local() {
    let directory = tempfile::tempdir().unwrap();
    let mut log =
        QueueNativeLog::local(directory.path(), "lost-root-reply".into(), options(1 << 20))
            .unwrap();
    log.submit_probe =
        Some(crate::queue_native_log::submit_test_probe::SubmitProbe::lose_one_stored_reply());
    provision_contract(log).await;
}
#[tokio::test]
async fn lost_root_submit_reply_after_actual_stored_adopts_original_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "lost-root-reply/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut log = QueueNativeLog::s3(
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
        log.submit_probe =
            Some(crate::queue_native_log::submit_test_probe::SubmitProbe::lose_one_stored_reply());
        provision_contract(log).await;
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

#[tokio::test]
async fn cancelled_public_provision_adoption_keeps_read_owners_and_reuses_ledger_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let proxy = super::s3_cancellation::Proxy::new(&env.endpoint);
        let prefix = format!(
            "cancel-provision/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut options = options(1 << 20);
        options.operations = 1;
        let mut native = QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: proxy.endpoint(),
                region: &env.region,
                bucket: &env.bucket,
                access_key: &env.access_key,
                secret_key: &env.secret_key,
            },
            prefix.clone(),
            options,
            QueueNativeS3TransportOptions {
                disable_payload_signing: disabled,
                ..Default::default()
            },
        )
        .unwrap();
        let (probe, stored, release_stored) =
            crate::queue_native_log::submit_test_probe::SubmitProbe::pause_one_stored_reply();
        native.submit_probe = Some(probe.clone());
        let log = Arc::new(native);
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
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
        let definition = Arc::new(
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap(),
        );
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let budget = source_budget(&job);
        let initial = budget.remaining_work();
        let baseline = admission.usage().unwrap();
        let caller = {
            let (log, job, definition, budget, address) = (
                log.clone(),
                job.clone(),
                definition.clone(),
                budget.clone(),
                address.clone(),
            );
            tokio::spawn(async move {
                log.provision_record(
                    &address,
                    QueueNativeGenesisIntent {
                        definition: &definition,
                        candidate_secret: &[7; 32],
                        known_queue: false,
                    },
                    budget,
                    provision_limits(),
                    &job,
                )
                .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), stored)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            probe.snapshot().2.len(),
            1,
            "actual Stored precedes arming the adoption gate"
        );
        let (entered, release) = proxy.arm_matching(Some(b"/coupled-data/"));
        release_stored.send(()).unwrap();
        let request = tokio::time::timeout(std::time::Duration::from_secs(10), entered)
            .await
            .unwrap()
            .unwrap();
        let line = std::str::from_utf8(&request).unwrap();
        assert!(
            line.starts_with("GET ") && line.contains("/coupled-data/"),
            "{line}"
        );
        assert!(
            line.contains(&format!("/{}/{prefix}/namespaces/candidates/", env.bucket)),
            "{line}"
        );
        let (calls, preparations, accepted, injected) = probe.snapshot();
        assert_eq!(
            (calls, preparations, injected, probe.acquisition_count()),
            (1, 1, 1, 1)
        );
        assert_eq!(accepted.len(), 1, "gate follows actual SDK Stored");
        let spent = budget.remaining_work();
        assert_eq!(spent.record_attempts, initial.record_attempts - 1);
        let semantic_prepare = provision_limits().preparation.prepare.work;
        let semantic_adopt = provision_limits().preparation.read.page.work;
        assert_eq!(
            spent.semantic.pages,
            initial.semantic.pages - semantic_prepare.pages - semantic_adopt.pages
        );
        assert_eq!(
            spent.semantic.hydrated_bytes,
            initial.semantic.hydrated_bytes
                - semantic_prepare.hydrated_bytes
                - semantic_adopt.hydrated_bytes
        );
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        // The retired publication may drop here; only the blocked read's owners
        // must remain. Compute their exact charge from the persisted plan below.
        let after_abort = admission.usage().unwrap();
        release.send(()).unwrap();
        let namespace = log.open(&address, &job).await.unwrap().unwrap();
        let source = log.capture_source(namespace, &job).await.unwrap();
        assert_eq!(source.head(), accepted[0]); // one operation slot drains the read
        let req = log
            .log
            .plan_read(&source.cut.cut, 0)
            .await
            .unwrap()
            .unwrap()
            .requirements()
            .unwrap();
        let guard = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        assert_eq!(
            after_abort,
            (
                baseline.0,
                baseline.1
                    + source.namespace._charge.admitted_bytes()
                    + source.semantic_identity.admitted_bytes()
                    + 65536
                    + guard
                    + 4 * req.frame_bytes()
                    + 4096
                    + guard
                    + ownership::physical_owner_bytes::<4>()
            ),
            "pending adoption owns namespace, cut identity, metadata and whole native frame"
        );
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
                spent.semantic.pages,
                spent.semantic.hydrated_bytes,
                spent.record_attempts,
                spent.hydration_reads,
                spent.hydration_body_bytes
            ),
            "abort never refunds work"
        );
        assert_eq!(
            initial.hydration_reads - spent.hydration_reads,
            req.hydration_reads()
        );
        assert_eq!(
            initial.hydration_body_bytes - spent.hydration_body_bytes,
            req.hydration_bytes()
        );
        drop(source);
        assert_eq!(admission.usage().unwrap(), baseline);
        let winner = log
            .provision_record(
                &address,
                QueueNativeGenesisIntent {
                    definition: &definition,
                    candidate_secret: &[99; 32],
                    known_queue: true,
                },
                budget.clone(),
                provision_limits(),
                &job,
            )
            .await
            .unwrap();
        let checked = winner.checked().unwrap();
        let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
            panic!("winner is genesis");
        };
        assert_eq!(genesis.cursor_secret(), &[7; 32]);
        let final_work = budget.remaining_work();
        assert_eq!(
            (
                final_work.semantic.pages,
                final_work.semantic.hydrated_bytes,
                final_work.record_attempts,
                final_work.hydration_reads,
                final_work.hydration_body_bytes
            ),
            (
                initial.semantic.pages - semantic_prepare.pages - 2 * semantic_adopt.pages,
                initial.semantic.hydrated_bytes
                    - semantic_prepare.hydrated_bytes
                    - 2 * semantic_adopt.hydrated_bytes,
                initial.record_attempts - 2,
                initial.hydration_reads - 2 * req.hydration_reads(),
                initial.hydration_body_bytes - 2 * req.hydration_bytes()
            )
        );
        let source = log
            .capture_source(log.open(&address, &job).await.unwrap().unwrap(), &job)
            .await
            .unwrap();
        assert_eq!(source.head(), accepted[0]);
        assert_eq!(probe.snapshot(), (1, 1, accepted, 1));
        assert_eq!(probe.acquisition_count(), 1);
        drop((source, winner, definition, budget));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(proxy);
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

async fn winner_during_acquisition_recovery_contract(
    mut contender: QueueNativeLog,
    winner: QueueNativeLog,
) {
    let (probe, entered, release) =
        crate::queue_native_log::submit_test_probe::SubmitProbe::pause_one_acquisition();
    contender.submit_probe = Some(probe.clone());
    let contender = Arc::new(contender);
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
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
    let definition = Arc::new(
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap(),
    );
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "queue".into(),
    };
    let budget = source_budget(&job);
    let initial = budget.remaining_work();
    let caller = {
        let (log, job, definition, budget, address) = (
            contender.clone(),
            job.clone(),
            definition.clone(),
            budget.clone(),
            address.clone(),
        );
        tokio::spawn(async move {
            log.provision_record(
                &address,
                QueueNativeGenesisIntent {
                    definition: &definition,
                    candidate_secret: &[7; 32],
                    known_queue: false,
                },
                budget,
                provision_limits(),
                &job,
            )
            .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), entered)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.acquisition_count(), 1);
    assert_eq!(probe.snapshot(), (0, 0, vec![], 0));
    let committed = winner
        .provision_record(
            &address,
            QueueNativeGenesisIntent {
                definition: &definition,
                candidate_secret: &[99; 32],
                known_queue: false,
            },
            source_budget(&job),
            provision_limits(),
            &job,
        )
        .await
        .unwrap();
    let source = winner
        .capture_source(winner.open(&address, &job).await.unwrap().unwrap(), &job)
        .await
        .unwrap();
    let accepted = source.head();
    let requirements = winner
        .log
        .plan_read(&source.cut.cut, 0)
        .await
        .unwrap()
        .unwrap()
        .requirements()
        .unwrap();
    assert_eq!(accepted.bounds(), (0, 1));
    assert!(
        !caller.is_finished(),
        "winner is committed before acquisition error is delivered"
    );
    release.send(()).unwrap();
    let adopted = caller.await.unwrap().unwrap();
    assert_eq!(adopted.bytes(), committed.bytes());
    let checked = adopted.checked().unwrap();
    let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
        panic!("genesis");
    };
    assert_eq!(genesis.cursor_secret(), &[99; 32]);
    assert_eq!(
        probe.acquisition_count(),
        1,
        "fresh nonempty capture forbids another fence"
    );
    assert_eq!(
        probe.snapshot(),
        (0, 0, vec![], 0),
        "contender never prepares or submits"
    );
    let left = budget.remaining_work();
    assert_eq!(left.record_attempts, initial.record_attempts - 1);
    assert_eq!(
        left.hydration_reads,
        initial.hydration_reads - requirements.hydration_reads()
    );
    assert_eq!(
        left.hydration_body_bytes,
        initial.hydration_body_bytes - requirements.hydration_bytes()
    );
    assert_eq!(
        left.semantic.pages,
        initial.semantic.pages - provision_limits().preparation.read.page.work.pages
    );
    assert_eq!(
        left.semantic.hydrated_bytes,
        initial.semantic.hydrated_bytes
            - provision_limits().preparation.read.page.work.hydrated_bytes
    );
    let after = contender
        .capture_source(contender.open(&address, &job).await.unwrap().unwrap(), &job)
        .await
        .unwrap();
    assert_eq!(after.head(), accepted);
    let reader = QueueNativeSourceReader::new(
        &after,
        &contender,
        &job,
        source_budget(&job),
        provision_limits().preparation.read,
    )
    .unwrap();
    let fact = fireweed_engine::queue_source_index::load_source_genesis(&reader, &job)
        .await
        .unwrap();
    fact.verify_record(&checked, 0).unwrap();
    let page = after
        .semantic_scan(
            &contender,
            std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded,
            QueueNativeScanLimits {
                rows: 8,
                output_bytes: 4096,
                work: provision_limits().preparation.read.page.work,
            },
            &job,
        )
        .await
        .unwrap();
    assert_eq!(page.status(), QueueNativeScanStatus::Complete);
    use fireweed_engine::queue_source_index::QueueSourceKey;
    let mut expected = BTreeMap::from([
        (
            QueueSourceKey::Genesis.encode().unwrap(),
            fireweed_engine::queue_storage_layout::encode(&fact).unwrap(),
        ),
        (
            QueueSourceKey::Sequence.encode().unwrap(),
            fireweed_engine::queue_storage_layout::encode(&0u64).unwrap(),
        ),
    ]);
    for lifecycle in [
        fireweed_core::queue_contract::Lifecycle::Pending,
        fireweed_core::queue_contract::Lifecycle::Leased,
        fireweed_core::queue_contract::Lifecycle::Completed,
        fireweed_core::queue_contract::Lifecycle::Failed,
    ] {
        expected.insert(
            QueueSourceKey::LifecycleCount(lifecycle).encode().unwrap(),
            fireweed_engine::queue_storage_layout::encode(&0u64).unwrap(),
        );
    }
    assert_eq!(
        page.rows()
            .map(|row| (row.key().to_vec(), row.value().to_vec()))
            .collect::<BTreeMap<_, _>>(),
        expected
    );
    drop(page);
    drop(reader);
    drop((after, source, adopted, committed, budget, definition));
    contender.shutdown().await.unwrap();
    winner.shutdown().await.unwrap();
    drop((contender, winner, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn winner_during_root_acquisition_error_is_adopted_without_refencing_local() {
    let directory = tempfile::tempdir().unwrap();
    let make = || {
        QueueNativeLog::local(directory.path(), "acquire-winner".into(), options(1 << 20)).unwrap()
    };
    winner_during_acquisition_recovery_contract(make(), make()).await;
}

#[tokio::test]
async fn winner_during_root_acquisition_error_is_adopted_without_refencing_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "acquire-winner/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let make = || {
            QueueNativeLog::s3(
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
            .unwrap()
        };
        winner_during_acquisition_recovery_contract(make(), make()).await;
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
