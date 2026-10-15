//! Real shared-frame resolution, zero-payload conflicts and bounded conversion.
use super::*;
use crate::queue_native_log::tests::{provision_limits, source_budget};
use std::sync::atomic::{AtomicUsize, Ordering};
const ROOT_BYTES: u64 = 128 << 20;
fn recovery_limits() -> QueueNativeRecoveryLimits {
    let mut record = provision_limits().record;
    record.payload_bytes = 1 << 20;
    record.frame_bytes = 2 << 20;
    QueueNativeRecoveryLimits {
        read: limits().read,
        record,
    }
}
struct Converted {
    id: String,
    raw: Vec<u8>,
    dropped: Arc<AtomicUsize>,
    _charge: OwnedQueueBufferCharge,
}
impl Drop for Converted {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
fn convert(
    view: QueueNativeOriginalView<'_>,
    job: &Arc<QueueReservation>,
    dropped: &Arc<AtomicUsize>,
) -> QueueResult<Converted> {
    let raw = view
        .outcome_bytes()?
        .ok_or_else(|| unknown("test requires present outcome"))?;
    let charge = job.owned_buffer(
        (std::mem::size_of::<Converted>() + view.request_id().len() + raw.len()) as u64,
    )?;
    Ok(Converted {
        id: view.request_id().to_owned(),
        raw: raw.to_vec(),
        dropped: dropped.clone(),
        _charge: charge,
    })
}
async fn contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(2, ROOT_BYTES).unwrap();
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
    let (acquired, genesis) = bundle_for_schema(&log, "producer", &job, &schema).await;
    let definition = {
        let checked = genesis.checked().unwrap();
        let QueueLogPayload::Genesis(view) = checked.view().payload() else {
            panic!()
        };
        view.validate_definition(&job).unwrap()
    };
    let compile_budget = budget(&job, 10000);
    let mut history = Vec::new();
    let snapshot = RecordSnapshot::default();
    let acquired = accept(
        &log,
        acquired,
        genesis,
        &definition,
        (&mut history, &snapshot),
        compile_budget.clone(),
        &job,
    )
    .await;
    let mut a = group(" a\0α ");
    a.work_inserts.push(NewWork {
        key: "a".into(),
        priority: 0,
        available_at: 0,
        fields: Fields::new(),
        body: vec![7; 256 << 10].into(),
    });
    let b = group("b");
    let c = group("c");
    let groups = vec![a, b, c];
    let (record, snapshot) = ordinary(&acquired, &definition, &snapshot, &groups, &job);
    let acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &snapshot),
        compile_budget.clone(),
        &job,
    )
    .await;
    let mut d = group("d");
    d.work_inserts.push(NewWork {
        key: "d".into(),
        priority: 0,
        available_at: 0,
        fields: Fields::new(),
        body: vec![9; 256 << 10].into(),
    });
    let (record, snapshot) = ordinary(
        &acquired,
        &definition,
        &snapshot,
        std::slice::from_ref(&d),
        &job,
    );
    let acquired = accept(
        &log,
        acquired,
        record,
        &definition,
        (&mut history, &snapshot),
        compile_budget.clone(),
        &job,
    )
    .await;
    let ledger = source_budget(&job);
    let recovered = log
        .recover_acquired(acquired, ledger.clone(), recovery_limits(), &job)
        .await
        .unwrap();
    let queue = QueueAddress {
        tenant: "tenant".into(),
        queue: "producer".into(),
    };
    let fp = groups[0].fingerprint(&queue).unwrap();
    let baseline = admission.usage().unwrap();
    let work_before = ledger.remaining_work();
    let batch = recovered
        .resolve_original_batch(
            &log,
            &[
                QueueNativeOriginalRequest::Lookup {
                    domain: 2,
                    request_id: "b",
                },
                QueueNativeOriginalRequest::Replay {
                    domain: 2,
                    request_id: &groups[0].request_id,
                    fingerprint_format: 3,
                    fingerprint: &fp,
                },
                QueueNativeOriginalRequest::Replay {
                    domain: 2,
                    request_id: "c",
                    fingerprint_format: 3,
                    fingerprint: &[0; 32],
                },
                QueueNativeOriginalRequest::Lookup {
                    domain: 2,
                    request_id: "absent",
                },
            ],
            recovery_limits(),
            &job,
        )
        .await
        .unwrap();
    assert_eq!(batch.len(), 4);
    assert!(!batch.is_empty());
    assert!(batch.original(4).is_err());
    assert_eq!(
        work_before.record_attempts - ledger.remaining_work().record_attempts,
        1,
        "two matching members share ONE physical read"
    );
    for position in [0, 1] {
        let member = batch.original(position).unwrap();
        assert!(std::ptr::eq(member.recovered(), &recovered));
        assert_eq!(member.status(), QueueNativeOriginalStatus::Present);
        assert_eq!(member.observation().unwrap().source_offset(), 1);
        assert!(member.outcome_bytes().unwrap().is_some());
    }
    let conflict = batch.original(2).unwrap();
    assert_eq!(conflict.status(), QueueNativeOriginalStatus::Conflict);
    assert!(conflict.observation().is_some());
    assert_eq!(conflict.outcome_bytes(), Err(QueueError::RequestConflict));
    let absent = batch.original(3).unwrap();
    assert_eq!(absent.status(), QueueNativeOriginalStatus::Absent);
    assert_eq!(absent.observation(), None);
    assert_eq!(absent.outcome_bytes(), Ok(None));
    drop(batch);
    assert_eq!(admission.usage().unwrap(), baseline);
    let before = ledger.remaining_work();
    let batch = recovered
        .resolve_original_batch(
            &log,
            &[
                QueueNativeOriginalRequest::Replay {
                    domain: 2,
                    request_id: &groups[0].request_id,
                    fingerprint_format: 3,
                    fingerprint: &[0; 32],
                },
                QueueNativeOriginalRequest::Replay {
                    domain: 2,
                    request_id: "d",
                    fingerprint_format: 3,
                    fingerprint: &[0; 32],
                },
            ],
            recovery_limits(),
            &job,
        )
        .await
        .unwrap();
    for p in [0, 1] {
        assert_eq!(
            batch.original(p).unwrap().status(),
            QueueNativeOriginalStatus::Conflict
        );
    }
    let after = ledger.remaining_work();
    assert_eq!(
        (
            before.record_attempts,
            before.hydration_reads,
            before.hydration_body_bytes
        ),
        (
            after.record_attempts,
            after.hydration_reads,
            after.hydration_body_bytes
        )
    );
    drop(batch);
    assert_eq!(admission.usage().unwrap(), baseline);
    let requests = [
        QueueNativeOriginalRequest::Lookup {
            domain: 2,
            request_id: "d",
        },
        QueueNativeOriginalRequest::Lookup {
            domain: 2,
            request_id: &groups[0].request_id,
        },
    ];
    let before = ledger.remaining_work();
    let batch = recovered
        .resolve_original_batch(&log, &requests, recovery_limits(), &job)
        .await
        .unwrap();
    assert_eq!(
        before.record_attempts - ledger.remaining_work().record_attempts,
        2,
        "distinct offsets need distinct grants"
    );
    let expected: Vec<_> = (0..2)
        .map(|p| {
            batch
                .original(p)
                .unwrap()
                .outcome_bytes()
                .unwrap()
                .unwrap()
                .to_vec()
        })
        .collect();
    drop(batch);
    assert_eq!(admission.usage().unwrap(), baseline);
    let dropped = Arc::new(AtomicUsize::new(0));
    let before = ledger.remaining_work();
    let failure = recovered
        .convert_original_batch(&log, &requests, recovery_limits(), &job, |view| {
            if view.observation().unwrap().source_offset() == 2 {
                return Err(unknown("injected late conversion"));
            }
            convert(view, &job, &dropped)
        })
        .await
        .err()
        .unwrap();
    assert!(
        matches!(failure,QueueError::OutcomeUnknown(m) if m.contains("injected late conversion"))
    );
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "earlier provisional DTO must be discarded"
    );
    assert_eq!(
        before.record_attempts - ledger.remaining_work().record_attempts,
        2
    );
    assert_eq!(admission.usage().unwrap(), baseline);
    // Fit a complete physical frame plus all planning/verification scratch,
    // while refusing two simultaneously retained full frames.
    let mut max_frame = 0;
    for offset in [1, 2] {
        let plan = log
            .log
            .plan_read(&recovered.as_source().cut.cut, offset)
            .await
            .unwrap()
            .unwrap();
        max_frame = max_frame.max(plan.requirements().unwrap().frame_bytes());
    }
    let allowance = 4 * max_frame + 4096 + (256 << 10);
    let starver = job
        .owned_buffer(ROOT_BYTES - baseline.1 - allowance)
        .unwrap();
    let occupied = admission.usage().unwrap();
    let before = ledger.remaining_work();
    assert!(matches!(
        recovered
            .resolve_original_batch(&log, &requests, recovery_limits(), &job)
            .await,
        Err(QueueError::OutcomeUnknown(m)) if m.contains("admitted buffer capacity exhausted")
    ));
    assert_eq!(
        before.record_attempts - ledger.remaining_work().record_attempts,
        2
    );
    assert_eq!(
        admission.usage().unwrap(),
        occupied,
        "late read failure releases retained frames without refunding work"
    );
    let before = ledger.remaining_work();
    let converted = recovered
        .convert_original_batch(&log, &requests, recovery_limits(), &job, |view| {
            convert(view, &job, &dropped)
        })
        .await
        .unwrap();
    assert_eq!(
        before.record_attempts - ledger.remaining_work().record_attempts,
        2
    );
    assert_eq!(converted.values()[0].id, "d");
    assert_eq!(converted.values()[1].id, groups[0].request_id);
    for (p, expected) in expected.iter().enumerate() {
        assert_eq!(&converted.values()[p].raw, expected);
        assert!(std::ptr::eq(
            converted.identity(p).unwrap().recovered(),
            &recovered
        ));
    }
    let (values, container_charge) = converted.into_parts();
    drop(starver);
    drop((
        recovered,
        ledger,
        compile_budget,
        history,
        definition,
        snapshot,
    ));
    log.shutdown().await.unwrap();
    drop(job);
    assert_eq!(
        admission.usage().unwrap().0,
        1,
        "independent DTO funding keeps its admission alive"
    );
    assert_eq!(values[0].id, "d");
    assert_eq!(values[1].id, " a\0α ");
    drop((values, container_charge));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn original_batch_reads_shared_frames_once_and_streams_distinct_offsets_local() {
    let dir = tempfile::tempdir().unwrap();
    contract(QueueNativeLog::local(dir.path(), "original-batch".into(), options(1 << 20)).unwrap())
        .await;
}
#[tokio::test]
async fn original_batch_reads_shared_frames_once_and_streams_distinct_offsets_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "original-batch/{}/{disabled}",
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
