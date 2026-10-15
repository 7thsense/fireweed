use super::*;
use object_log::{BlobStore, NativeReadRequirements};

pub(super) fn record_limits() -> QueueNativeSourceRecordLimits {
    QueueNativeSourceRecordLimits {
        payload_bytes: 1 << 20,
        logical_identity_bytes: 4096,
        physical_identity_bytes: 4096,
        frame_bytes: 2 << 20,
    }
}
pub(super) fn ledger(
    job: &Arc<QueueReservation>,
    attempts: u64,
    reads: u64,
    bytes: u64,
) -> Arc<QueueNativeSourceBudget> {
    QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: work(),
            record_attempts: attempts,
            hydration_reads: reads,
            hydration_body_bytes: bytes,
        },
        job,
    )
    .unwrap()
}
pub(super) async fn seeded(
    log: &QueueNativeLog,
    job: &Arc<QueueReservation>,
    name: &str,
) -> (
    QueueNativeCaptured,
    OwnedQueueLogRecord,
    NativeReadRequirements,
) {
    let (acquired, record) = bundle(log, name, job).await;
    let checked = record.checked().unwrap();
    let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
        panic!()
    };
    let definition = genesis.validate_definition(job).unwrap();
    let budget = QueueNativeSourceBudget::new(
        QueueNativeSourceWork {
            semantic: QueueNativeSemanticWork {
                pages: work().pages * 100,
                hydrated_bytes: work().hydrated_bytes * 100,
            },
            record_attempts: 0,
            hydration_reads: 0,
            hydration_body_bytes: 0,
        },
        job,
    )
    .unwrap();
    let original = acquired
        .compile_and_prepare(
            log,
            &record,
            &definition,
            budget,
            QueueNativeSourcePreparationLimits {
                compile: fireweed_engine::queue_source_index::QueueSourceCompileLimits {
                    edits: 100,
                    input_bytes: 1 << 16,
                },
                read: QueueNativeSourceReadLimits {
                    page: scan_limit(),
                    range_pages: 16,
                },
                prepare: edits_limit(),
            },
            job,
        )
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
    let namespace = acquired
        .as_source()
        .retained_namespace(
            &QueueAddress {
                tenant: "tenant".into(),
                queue: name.into(),
            },
            job,
        )
        .unwrap();
    let captured = log.capture_source(namespace, job).await.unwrap();
    let plan = log
        .log
        .plan_read(&captured.cut.cut, 0)
        .await
        .unwrap()
        .unwrap();
    let requirements = plan.requirements().unwrap();
    assert_eq!(plan.payload_bytes(), record.bytes().len());
    // Independent native coupled framing, not the wrapper's funding formula.
    assert_eq!(
        requirements.frame_bytes(),
        24 + 224 + requirements.logical_identity_bytes() + record.bytes().len() as u64
    );
    drop(plan);
    (captured, record, requirements)
}
pub(super) async fn identity_objects(
    local: &object_log::LocalBlobStore,
) -> Vec<(String, bytes::Bytes)> {
    let mut found = Vec::new();
    for key in local.list("").await.unwrap() {
        let raw = local.get(&key).await.unwrap().unwrap();
        if raw.len() > 112 && raw.starts_with(b"ONP2") && raw[72] == 3 {
            found.push((key, raw));
        }
    }
    assert!(found.len() >= 2);
    found
}
fn exhausted(result: QueueResult<OwnedQueueLogRecord>, message: &str) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("expected bounded failure"),
    };
    assert!(
        matches!(error, QueueError::OutcomeUnknown(ref text) if text.contains(message)),
        "{error:?}"
    );
}

#[tokio::test]
async fn source_record_whole_frame_root_grant_exact_and_one_short_precedes_hydration() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(directory.path(), "whole-frame".into(), options(1 << 20)).unwrap();
    let local = object_log::LocalBlobStore::new(directory.path());
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (source, expected, requirements) = seeded(&log, &job, "queue").await;
    let identities = identity_objects(&local).await;
    let guard =
        (std::mem::size_of::<OwnedQueueBufferCharge>() + 2 * std::mem::size_of::<usize>()) as u64;
    let output_credit = 4 * requirements.frame_bytes() + 4096 + guard;
    let exact = 65536 + guard + output_credit + ownership::physical_owner_bytes::<4>();
    for deficit in [1, 0] {
        let budget = ledger(
            &job,
            1,
            requirements.hydration_reads(),
            requirements.hydration_bytes(),
        );
        let free = (32 << 20) - admission.usage().unwrap().1;
        let filler = job.owned_buffer(free - exact + deficit).unwrap();
        let before = admission.usage().unwrap();
        if deficit == 1 {
            // Offset lookup does not hydrate identities. If ANY hydration runs,
            // it fails on a missing identity, rather than Root admission below.
            for (key, _) in &identities {
                local.delete(key).await.unwrap();
            }
            exhausted(
                source
                    .read_source_record(&log, 0, record_limits(), &budget, &job)
                    .await,
                "admitted buffer capacity exhausted",
            );
            assert_eq!(admission.usage().unwrap(), before);
            for (key, raw) in &identities {
                local.put(key, raw.clone()).await.unwrap();
            }
        } else {
            let read = source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await
                .unwrap();
            assert_eq!(read.bytes(), expected.bytes());
            assert_eq!(admission.usage().unwrap().1, before.1 + output_credit);
            drop(read);
            assert_eq!(admission.usage().unwrap(), before);
        }
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, 0, 0)
        );
        exhausted(
            source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await,
            "request record attempts exhausted",
        );
        drop(filler);
    }
    drop((source, expected));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn source_record_limits_and_atomic_work_debits_never_refund_attempts() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(directory.path(), "record-limits".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (source, expected, req) = seeded(&log, &job, "queue").await;
    let local = object_log::LocalBlobStore::new(directory.path());
    let identities = identity_objects(&local).await;
    for short in ["reads", "bytes"] {
        for (key, _) in &identities {
            local.delete(key).await.unwrap();
        }
        let reads = req.hydration_reads() - u64::from(short == "reads");
        let bytes = req.hydration_bytes() - u64::from(short == "bytes");
        let budget = ledger(&job, 1, reads, bytes);
        exhausted(
            source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await,
            "request record hydration exhausted",
        );
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, reads, bytes)
        );
        for (key, raw) in &identities {
            local.put(key, raw.clone()).await.unwrap();
        }
    }
    let exact_work = ledger(&job, 1, req.hydration_reads(), req.hydration_bytes());
    let read = source
        .read_source_record(&log, 0, record_limits(), &exact_work, &job)
        .await
        .unwrap();
    assert_eq!(read.bytes(), expected.bytes());
    drop((read, exact_work));
    for field in ["payload", "logical", "physical", "frame"] {
        for (key, _) in &identities {
            local.delete(key).await.unwrap();
        }
        let budget = ledger(&job, 2, req.hydration_reads(), req.hydration_bytes());
        let mut limits = record_limits();
        match field {
            "payload" => limits.payload_bytes = expected.bytes().len() as u64 - 1,
            "logical" => limits.logical_identity_bytes = req.logical_identity_bytes() - 1,
            "physical" => limits.physical_identity_bytes = req.physical_identity_bytes() - 1,
            "frame" => limits.frame_bytes = req.frame_bytes() - 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            source
                .read_source_record(&log, 0, limits, &budget, &job)
                .await,
            Err(QueueError::TooLarge)
        ));
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (1, req.hydration_reads(), req.hydration_bytes())
        );
        for (key, raw) in &identities {
            local.put(key, raw.clone()).await.unwrap();
        }
        let exact = QueueNativeSourceRecordLimits {
            payload_bytes: expected.bytes().len() as u64,
            logical_identity_bytes: req.logical_identity_bytes(),
            physical_identity_bytes: req.physical_identity_bytes(),
            frame_bytes: req.frame_bytes(),
        };
        let read = source
            .read_source_record(&log, 0, exact, &budget, &job)
            .await
            .unwrap();
        assert_eq!(read.bytes(), expected.bytes());
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, 0, 0)
        );
        drop(read);
    }
    drop((source, expected));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn source_record_scope_and_configuration_fail_before_attempt_or_closed_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(directory.path(), "record-scope".into(), options(1 << 20)).unwrap();
    let foreign_directory = tempfile::tempdir().unwrap();
    let foreign =
        QueueNativeLog::local(foreign_directory.path(), "foreign".into(), options(1 << 20))
            .unwrap();
    let admission = QueueAdmission::new(2, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let other = Arc::new(admission.reserve(0).unwrap());
    let (source, expected, req) = seeded(&log, &job, "queue").await;
    let budget = ledger(&job, 1, req.hydration_reads(), req.hydration_bytes());
    let foreign_budget = ledger(&other, 1, req.hydration_reads(), req.hydration_bytes());
    log.shutdown().await.unwrap();
    exhausted(
        source
            .read_source_record(&foreign, 0, record_limits(), &budget, &job)
            .await,
        "semantic namespace, cut or operation differs",
    );
    exhausted(
        source
            .read_source_record(&log, 0, record_limits(), &budget, &other)
            .await,
        "semantic namespace, cut or operation differs",
    );
    exhausted(
        source
            .read_source_record(&log, 0, record_limits(), &foreign_budget, &job)
            .await,
        "source work ledger belongs to another operation",
    );
    exhausted(
        source
            .read_source_record(
                &log,
                source.head().bounds().1,
                record_limits(),
                &budget,
                &job,
            )
            .await,
        "source record offset outside captured range",
    );
    let mut invalid = record_limits();
    invalid.frame_bytes = 0;
    exhausted(
        source
            .read_source_record(&log, 0, invalid, &budget, &job)
            .await,
        "source record limits must be positive",
    );
    assert_eq!(budget.remaining_work().record_attempts, 1);
    assert_eq!(foreign_budget.remaining_work().record_attempts, 1);
    exhausted(
        source
            .read_source_record(&log, 0, record_limits(), &budget, &job)
            .await,
        "native execution closed",
    );
    let left = budget.remaining_work();
    assert_eq!(
        (
            left.record_attempts,
            left.hydration_reads,
            left.hydration_body_bytes
        ),
        (0, req.hydration_reads(), req.hydration_bytes())
    );
    drop((source, expected, budget, foreign_budget));
    foreign.shutdown().await.unwrap();
    drop((log, foreign, job, other));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn source_record_missing_and_corrupt_storage_fail_closed_and_retry_spends_same_ledger() {
    for fault in ["missing-index", "corrupt-frame"] {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
        let local = object_log::LocalBlobStore::new(directory.path());
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (source, expected, req) = seeded(&log, &job, "queue").await;
        let mut targets = Vec::new();
        for key in local.list("").await.unwrap() {
            let raw = local.get(&key).await.unwrap().unwrap();
            let selected = if fault == "missing-index" {
                raw.len() > 112 && raw.starts_with(b"ONP2") && raw[72] == 1
            } else {
                key.contains("/coupled-data/")
            };
            if selected {
                targets.push((key, raw));
            }
        }
        assert_eq!(targets.len(), 1);
        let (key, original) = targets.pop().unwrap();
        let multiplier = if fault == "missing-index" { 1 } else { 2 };
        let budget = ledger(
            &job,
            2,
            multiplier * req.hydration_reads(),
            multiplier * req.hydration_bytes(),
        );
        if fault == "missing-index" {
            local.delete(&key).await.unwrap();
        } else {
            let mut corrupt = original.to_vec();
            corrupt[0] ^= 1;
            local.put(&key, corrupt.into()).await.unwrap();
        }
        let before = admission.usage().unwrap();
        exhausted(
            source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await,
            if fault == "missing-index" {
                "native immutable page"
            } else {
                "native data framing"
            },
        );
        assert_eq!(admission.usage().unwrap(), before);
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (1, req.hydration_reads(), req.hydration_bytes())
        );
        local.put(&key, original).await.unwrap();
        let read = source
            .read_source_record(&log, 0, record_limits(), &budget, &job)
            .await
            .unwrap();
        assert_eq!(read.bytes(), expected.bytes());
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, 0, 0)
        );
        drop((read, source, expected, budget));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}

async fn raw_native_payload(
    log: &QueueNativeLog,
    acquired: &QueueNativeAcquired,
    payload: &[u8],
    records: u32,
    job: &Arc<QueueReservation>,
) -> (QueueNativeCaptured, NativeReadRequirements) {
    // Test-only direct SDK publication: authenticate opaque bytes independently
    // of the consumer's queue format and one-record storage policy.
    let original = log
        .log
        .prepare(
            &acquired.writer.writer,
            &[object_log::NativeAppend {
                identity: "independent-consumer-record",
                payload,
                records,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(
        log.log.submit(&original).await.unwrap(),
        object_log::NativePublishOutcome::Stored(_)
    ));
    let namespace = acquired
        .as_source()
        .retained_namespace(
            &QueueAddress {
                tenant: "tenant".into(),
                queue: "queue".into(),
            },
            job,
        )
        .unwrap();
    let source = log.capture_source(namespace, job).await.unwrap();
    let plan = log
        .log
        .plan_read(&source.cut.cut, 0)
        .await
        .unwrap()
        .unwrap();
    let req = plan.requirements().unwrap();
    drop(plan);
    (source, req)
}

#[tokio::test]
async fn source_record_valid_native_frames_reject_invalid_checksum_address_and_epoch() {
    for fault in ["checksum", "address", "epoch"] {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::local(directory.path(), fault.into(), options(1 << 20)).unwrap();
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (acquired, genesis) = bundle(&log, "queue", &job).await;
        let checked = genesis.checked().unwrap();
        let QueueLogPayload::Genesis(view) = checked.view().payload() else {
            panic!()
        };
        let definition = view.validate_definition(&job).unwrap();
        let payload = if fault == "checksum" {
            let mut raw = genesis.bytes().to_vec();
            *raw.last_mut().unwrap() ^= 1;
            assert!(
                format!("{:?}", view_queue_log_record(&raw).err().unwrap())
                    .contains("queue-log checksum mismatch")
            );
            raw
        } else {
            encode_checked_genesis(
                acquired.as_source().head().epoch() + u64::from(fault == "epoch"),
                &QueueAddress {
                    tenant: "tenant".into(),
                    queue: if fault == "address" { "wrong" } else { "queue" }.into(),
                },
                &definition,
                &[7; 32],
                4096,
                &job,
            )
            .unwrap()
            .bytes()
            .to_vec()
        };
        let (source, req) = raw_native_payload(&log, &acquired, &payload, 1, &job).await;
        let budget = ledger(&job, 1, req.hydration_reads(), req.hydration_bytes());
        let before = admission.usage().unwrap();
        exhausted(
            source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await,
            if fault == "checksum" {
                "queue-log checksum mismatch"
            } else {
                "source record scope, format, range or epoch differs"
            },
        );
        assert_eq!(admission.usage().unwrap(), before);
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, 0, 0)
        );
        drop((source, acquired, genesis, definition, budget));
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}

#[tokio::test]
async fn source_record_grouped_sdk_entry_rejects_before_identity_hydration() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(directory.path(), "grouped-entry".into(), options(1 << 20)).unwrap();
    let local = object_log::LocalBlobStore::new(directory.path());
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (acquired, genesis) = bundle(&log, "queue", &job).await;
    let (source, req) = raw_native_payload(&log, &acquired, genesis.bytes(), 2, &job).await;
    assert_eq!(source.head().bounds(), (0, 2));
    for (key, _) in identity_objects(&local).await {
        local.delete(&key).await.unwrap();
    }
    for offset in [0, 1] {
        let budget = ledger(&job, 1, req.hydration_reads(), req.hydration_bytes());
        exhausted(
            source
                .read_source_record(&log, offset, record_limits(), &budget, &job)
                .await,
            "source record planned logical range differs",
        );
        let left = budget.remaining_work();
        assert_eq!(
            (
                left.record_attempts,
                left.hydration_reads,
                left.hydration_body_bytes
            ),
            (0, req.hydration_reads(), req.hydration_bytes())
        );
    }
    drop((source, acquired, genesis));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

#[tokio::test]
async fn source_record_and_semantic_reads_share_one_nonrefunding_request_ledger() {
    use fireweed_engine::queue_source_index::{QueueSourceKey, QueueSourceReader};
    let directory = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(directory.path(), "mixed-work".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 32 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (source, expected, req) = seeded(&log, &job, "queue").await;
    let key = QueueSourceKey::Genesis.encode().unwrap();
    for semantic_first in [true, false] {
        let budget = ledger(&job, 1, req.hydration_reads(), req.hydration_bytes());
        let reader = QueueNativeSourceReader::new(
            &source,
            &log,
            &job,
            budget.clone(),
            QueueNativeSourceReadLimits {
                page: scan_limit(),
                range_pages: 16,
            },
        )
        .unwrap();
        if semantic_first {
            let point = reader.lookup(&key).await.unwrap();
            assert!(
                reader
                    .point(&point)
                    .unwrap()
                    .value_for(&reader.cut().unwrap(), &key)
                    .unwrap()
                    .is_some()
            );
        }
        let record = source
            .read_source_record(&log, 0, record_limits(), &budget, &job)
            .await
            .unwrap();
        assert_eq!(record.bytes(), expected.bytes());
        if !semantic_first {
            let point = reader.lookup(&key).await.unwrap();
            assert!(
                reader
                    .point(&point)
                    .unwrap()
                    .value_for(&reader.cut().unwrap(), &key)
                    .unwrap()
                    .is_some()
            );
        }
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
        assert!(reader.lookup(&key).await.is_err());
        exhausted(
            source
                .read_source_record(&log, 0, record_limits(), &budget, &job)
                .await,
            "request record attempts exhausted",
        );
    }
    drop((source, expected));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
