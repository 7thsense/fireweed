use super::*;
use crate::queue_native_log::tests::{provision_limits, source_budget};
const ID: &str = " original\0α ";
const SECOND_ID: &str = " second original\0β ";
fn limits() -> QueueNativeRecoveryLimits {
    let p = provision_limits();
    QueueNativeRecoveryLimits {
        read: p.preparation.read,
        record: p.record,
    }
}
async fn seeded(
    log: &QueueNativeLog,
    job: &Arc<QueueReservation>,
    tight: bool,
) -> (QueueNativeRecovered, [u8; 32]) {
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "original\0queue".into(),
    };
    let definition =
        AdmittedQueueDefinition::from_raw(&super::provision::legacy_definition_bytes(), job)
            .unwrap();
    let genesis = log
        .provision_record(
            &address,
            QueueNativeGenesisIntent {
                definition: &definition,
                candidate_secret: &[7; 32],
                known_queue: false,
            },
            source_budget(job),
            provision_limits(),
            job,
        )
        .await
        .unwrap();
    let source = log
        .capture_source(log.open(&address, job).await.unwrap().unwrap(), job)
        .await
        .unwrap();
    let advanced =
        super::provision::append_work_group(log, &source, &address, &definition, ID, job).await;
    drop(source);
    let group = fireweed_core::queue_contract::CommitGroup {
        request_id: ID.into(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_updates: vec![],
        administration: None,
        work_inserts: vec![fireweed_core::queue_contract::NewWork {
            key: ID.into(),
            priority: 0,
            available_at: 0,
            fields: Default::default(),
            body: Default::default(),
        }],
    };
    let fp = group.fingerprint(&address).unwrap();
    let budget = if tight {
        let plan = log
            .log
            .plan_read(&advanced.cut.cut, 0)
            .await
            .unwrap()
            .unwrap();
        let req = plan.requirements().unwrap();
        QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: QueueNativeSemanticWork {
                    pages: 12 * limits().read.page.work.pages,
                    hydrated_bytes: 12 * limits().read.page.work.hydrated_bytes,
                },
                record_attempts: 1,
                hydration_reads: req.hydration_reads(),
                hydration_body_bytes: req.hydration_bytes(),
            },
            job,
        )
        .unwrap()
    } else {
        source_budget(job)
    };
    let recovered = log
        .recover_captured(advanced, budget, limits(), job)
        .await
        .unwrap();
    assert_eq!(recovered.definition().raw(), definition.raw());
    assert_eq!(recovered.genesis_record().bytes(), genesis.bytes());
    (recovered, fp)
}
fn semantic_spent(before: QueueNativeSourceWork, after: QueueNativeSourceWork) {
    assert_eq!(
        before.semantic.pages - after.semantic.pages,
        limits().read.page.work.pages
    );
    assert_eq!(
        before.semantic.hydrated_bytes - after.semantic.hydrated_bytes,
        limits().read.page.work.hydrated_bytes
    );
}
fn no_record_spent(before: QueueNativeSourceWork, after: QueueNativeSourceWork) {
    semantic_spent(before, after);
    assert_eq!(
        (
            after.record_attempts,
            after.hydration_reads,
            after.hydration_body_bytes
        ),
        (
            before.record_attempts,
            before.hydration_reads,
            before.hydration_body_bytes
        )
    );
}
async fn retained_contract(log: QueueNativeLog) {
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (recovered, fp) = seeded(&log, &job, false).await;
    let source = recovered.as_source();
    let head = source.head();
    let plan = log
        .log
        .plan_read(&source.cut.cut, 1)
        .await
        .unwrap()
        .unwrap();
    let req = plan.requirements().unwrap();
    drop(plan);
    let before = admission.usage().unwrap();
    let work = recovered.budget().remaining_work();
    let input_id = ID.to_owned();
    let output = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: &input_id,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await
        .unwrap();
    drop(input_id);
    assert!(std::ptr::eq(output.recovered(), &recovered));
    assert_eq!(
        (
            output.domain(),
            output.request_id(),
            output.source_offset(),
            output.fingerprint()
        ),
        (2, ID, Some(1), Some((3, fp)))
    );
    let raw = output.outcome_bytes().unwrap().unwrap();
    assert!(fireweed_engine::queue_publication::view_receipt_outcome(2, 3, raw).is_ok());
    let identity = (std::mem::size_of::<QueueNativeResolvedOriginal<'_>>() + ID.len()) as u64;
    let physical = 4 * req.frame_bytes()
        + 4096
        + (std::mem::size_of::<OwnedQueueBufferCharge>() + 2 * std::mem::size_of::<usize>()) as u64;
    assert_eq!(admission.usage().unwrap().1, before.1 + identity + physical);
    let after = recovered.budget().remaining_work();
    semantic_spent(work, after);
    assert_eq!(
        (
            work.record_attempts - after.record_attempts,
            work.hydration_reads - after.hydration_reads,
            work.hydration_body_bytes - after.hydration_body_bytes
        ),
        (1, req.hydration_reads(), req.hydration_bytes())
    );
    let expected_outcome = raw.to_vec();
    drop(output);
    assert_eq!(admission.usage().unwrap(), before);
    let work = recovered.budget().remaining_work();
    let lookup = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Lookup {
                domain: 2,
                request_id: ID,
            },
            limits(),
            &job,
        )
        .await
        .unwrap();
    assert!(std::ptr::eq(lookup.recovered(), &recovered));
    assert_eq!(
        (
            lookup.domain(),
            lookup.request_id(),
            lookup.source_offset(),
            lookup.fingerprint()
        ),
        (2, ID, Some(1), Some((3, fp)))
    );
    assert_eq!(lookup.outcome_bytes().unwrap().unwrap(), expected_outcome);
    assert_eq!(admission.usage().unwrap().1, before.1 + identity + physical);
    let after = recovered.budget().remaining_work();
    semantic_spent(work, after);
    assert_eq!(
        (
            work.record_attempts - after.record_attempts,
            work.hydration_reads - after.hydration_reads,
            work.hydration_body_bytes - after.hydration_body_bytes
        ),
        (1, req.hydration_reads(), req.hydration_bytes())
    );
    drop(lookup);
    assert_eq!(admission.usage().unwrap(), before);
    for (domain, id) in [(0, ID), (1, ID), (2, ID.trim())] {
        let work = recovered.budget().remaining_work();
        let output = recovered
            .resolve_original(
                &log,
                QueueNativeOriginalRequest::Lookup {
                    domain,
                    request_id: id,
                },
                limits(),
                &job,
            )
            .await
            .unwrap();
        assert!(std::ptr::eq(output.recovered(), &recovered));
        assert_eq!(
            (
                output.domain(),
                output.request_id(),
                output.source_offset(),
                output.fingerprint()
            ),
            (domain, id, None, None)
        );
        assert!(output.outcome_bytes().unwrap().is_none());
        assert_eq!(
            admission.usage().unwrap().1,
            before.1 + (std::mem::size_of::<QueueNativeResolvedOriginal<'_>>() + id.len()) as u64
        );
        no_record_spent(work, recovered.budget().remaining_work());
        drop(output);
        assert_eq!(admission.usage().unwrap(), before);
    }
    assert_eq!(log.capture(&source.namespace).await.unwrap().head(), head);
    drop(recovered);
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn resolved_original_retains_exact_capsule_record_literal_and_credits_local() {
    let dir = tempfile::tempdir().unwrap();
    retained_contract(
        QueueNativeLog::local(dir.path(), "original-retained".into(), options(1 << 20)).unwrap(),
    )
    .await;
}
#[tokio::test]
async fn original_conflict_and_absence_need_no_record_capacity_but_matching_replay_does() {
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(dir.path(), "original-tight".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (recovered, fp) = seeded(&log, &job, true).await;
    let before = admission.usage().unwrap();
    let work = recovered.budget().remaining_work();
    assert_eq!(
        (
            work.record_attempts,
            work.hydration_reads,
            work.hydration_body_bytes
        ),
        (0, 0, 0)
    );
    let conflict = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &[0; 32],
            },
            limits(),
            &job,
        )
        .await;
    assert!(matches!(conflict, Err(QueueError::RequestConflict)));
    no_record_spent(work, recovered.budget().remaining_work());
    assert_eq!(admission.usage().unwrap(), before);
    let work = recovered.budget().remaining_work();
    let absent = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 0,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await
        .unwrap();
    assert!(absent.outcome_bytes().unwrap().is_none());
    no_record_spent(work, recovered.budget().remaining_work());
    drop(absent);
    assert_eq!(admission.usage().unwrap(), before);
    let work = recovered.budget().remaining_work();
    let matching = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await;
    assert!(
        matches!(matching,Err(QueueError::OutcomeUnknown(ref m)) if m.contains("record attempts exhausted"))
    );
    no_record_spent(work, recovered.budget().remaining_work());
    assert_eq!(admission.usage().unwrap(), before);
    drop(recovered);
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn original_conflict_precedes_missing_payload_and_matching_retry_uses_same_ledger() {
    use object_log::BlobStore;
    let dir = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(dir.path(), "original-missing".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (recovered, fp) = seeded(&log, &job, false).await;
    let local = object_log::LocalBlobStore::new(dir.path());
    let identities = super::record::identity_objects(&local).await;
    for (key, _) in &identities {
        local.delete(key).await.unwrap();
    }
    let before = admission.usage().unwrap();
    let work = recovered.budget().remaining_work();
    let conflict = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &[0; 32],
            },
            limits(),
            &job,
        )
        .await;
    assert!(matches!(conflict, Err(QueueError::RequestConflict)));
    no_record_spent(work, recovered.budget().remaining_work());
    assert_eq!(admission.usage().unwrap(), before);
    let work = recovered.budget().remaining_work();
    let missing = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await;
    assert!(
        matches!(missing,Err(QueueError::OutcomeUnknown(ref m)) if m.contains("missing object"))
    );
    let failed = recovered.budget().remaining_work();
    semantic_spent(work, failed);
    assert_eq!(failed.record_attempts + 1, work.record_attempts);
    assert!(
        failed.hydration_reads < work.hydration_reads
            && failed.hydration_body_bytes < work.hydration_body_bytes
    );
    assert_eq!(admission.usage().unwrap(), before);
    for (key, raw) in identities {
        local.put(&key, raw).await.unwrap();
    }
    let output = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await
        .unwrap();
    assert!(output.outcome_bytes().unwrap().is_some());
    let retried = recovered.budget().remaining_work();
    semantic_spent(failed, retried);
    assert_eq!(retried.record_attempts + 1, failed.record_attempts);
    drop(output);
    assert_eq!(admission.usage().unwrap(), before);
    drop(recovered);
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}

async fn cleanup(env: &crate::S3TestEnv, prefix: &str) {
    use object_log::BlobStore;
    let provider = object_log::S3BlobStore::new(
        &env.endpoint,
        &env.region,
        &env.bucket,
        &env.access_key,
        &env.secret_key,
    );
    for key in provider.list(prefix).await.unwrap() {
        provider.delete(&key).await.unwrap();
    }
}
#[tokio::test]
async fn resolved_original_retains_exact_capsule_record_literal_and_credits_real_s3() {
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "original-retained/{}/{disabled}",
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
        retained_contract(log).await;
        cleanup(env, &prefix).await;
    }
}
#[derive(Clone, Copy)]
enum OriginalResolutionMode {
    Single,
    RetainedBatch,
    ConvertedBatch,
}
async fn resolve_cancelled_mode(
    mode: OriginalResolutionMode,
    log: &QueueNativeLog,
    recovered: &QueueNativeRecovered,
    fp: &[u8; 32],
    job: &Arc<QueueReservation>,
) -> QueueResult<usize> {
    let request = QueueNativeOriginalRequest::Replay {
        domain: 2,
        request_id: ID,
        fingerprint_format: 3,
        fingerprint: fp,
    };
    match mode {
        OriginalResolutionMode::Single => {
            let value = recovered
                .resolve_original(log, request, limits(), job)
                .await?;
            Ok(value.outcome_bytes()?.unwrap().len())
        }
        OriginalResolutionMode::RetainedBatch => {
            let value = recovered
                .resolve_original_batch(log, &[request], limits(), job)
                .await?;
            Ok(value.original(0)?.outcome_bytes()?.unwrap().len())
        }
        OriginalResolutionMode::ConvertedBatch => {
            let value = recovered
                .convert_original_batch(
                    log,
                    &[
                        request,
                        QueueNativeOriginalRequest::Lookup {
                            domain: 2,
                            request_id: SECOND_ID,
                        },
                    ],
                    limits(),
                    job,
                    |v| Ok(v.outcome_bytes()?.unwrap().len()),
                )
                .await?;
            Ok(value.values().iter().sum())
        }
    }
}
#[tokio::test]
async fn cancelled_original_resolution_retires_temporary_identity_but_retains_physical_owners_real_s3()
 {
    cancelled_original_mode(OriginalResolutionMode::Single).await;
}
#[tokio::test]
async fn cancelled_original_batch_retires_containers_but_retains_physical_owners_real_s3() {
    cancelled_original_mode(OriginalResolutionMode::RetainedBatch).await;
}
#[tokio::test]
async fn cancelled_converted_original_batch_retires_containers_but_retains_physical_owners_real_s3()
{
    cancelled_original_mode(OriginalResolutionMode::ConvertedBatch).await;
}
async fn cancelled_original_mode(mode: OriginalResolutionMode) {
    use std::time::Duration;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let proxy = Arc::new(super::s3_cancellation::Proxy::new(&env.endpoint));
        let prefix = format!(
            "original-cancel/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut opts = options(1 << 20);
        opts.operations = 1;
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
                opts,
                QueueNativeS3TransportOptions {
                    disable_payload_signing: disabled,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 64 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let (mut recovered, fp) = seeded(&log, &job, false).await;
        let converted = matches!(mode, OriginalResolutionMode::ConvertedBatch);
        if converted {
            let address = QueueAddress {
                tenant: "tenant".into(),
                queue: "original\0queue".into(),
            };
            let advanced = super::provision::append_work_group(
                &log,
                recovered.as_source(),
                &address,
                recovered.definition(),
                SECOND_ID,
                &job,
            )
            .await;
            let budget = recovered.budget().clone();
            drop(recovered);
            recovered = log
                .recover_captured(advanced, budget, limits(), &job)
                .await
                .unwrap();
        }
        let recovered = Arc::new(recovered);
        let head = recovered.as_source().head();
        let plan = log
            .log
            .plan_read(&recovered.as_source().cut.cut, 1)
            .await
            .unwrap()
            .unwrap();
        let first_req = plan.requirements().unwrap();
        drop(plan);
        let second_plan = if converted {
            Some(
                log.log
                    .plan_read(&recovered.as_source().cut.cut, 2)
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        let req = second_plan
            .as_ref()
            .map(|p| p.requirements().unwrap())
            .unwrap_or(first_req);
        let attempts = if converted { 2 } else { 1 };
        let hydration_reads =
            first_req.hydration_reads() + if converted { req.hydration_reads() } else { 0 };
        let hydration_bytes =
            first_req.hydration_bytes() + if converted { req.hydration_bytes() } else { 0 };
        drop(second_plan);
        let baseline = admission.usage().unwrap();
        let work = recovered.budget().remaining_work();
        let guard = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let pending = (
            baseline.0,
            baseline.1
                + 65536
                + guard
                + 4 * req.frame_bytes()
                + 4096
                + guard
                + ownership::physical_owner_bytes::<4>(),
        );
        let identity = (std::mem::size_of::<QueueNativeResolvedOriginal<'_>>() + ID.len()) as u64;
        let evidence = (std::mem::size_of::<
            fireweed_engine::queue_source_index::QueueSourceOriginalEvidence<'_>,
        >() + ID.len()
            + fireweed_engine::queue_source_index::QueueSourceKey::Original {
                domain: 2,
                request_id: ID,
            }
            .encoded_len()
            .unwrap()) as u64;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Converted {
            _raw: Vec<u8>,
            dropped: Arc<AtomicUsize>,
            _charge: OwnedQueueBufferCharge,
        }
        impl Drop for Converted {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let produced = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let initial_gate = if converted {
            None
        } else {
            Some(proxy.arm_matching(Some(b"/coupled-data/")))
        };
        let (armed_tx, armed_rx) = tokio::sync::oneshot::channel();
        let caller = {
            let (log, recovered, job) = (log.clone(), recovered.clone(), job.clone());
            let (proxy, produced, dropped) = (proxy.clone(), produced.clone(), dropped.clone());
            tokio::spawn(async move {
                if converted {
                    let mut armed_tx = Some(armed_tx);
                    let requests = [
                        QueueNativeOriginalRequest::Replay {
                            domain: 2,
                            request_id: ID,
                            fingerprint_format: 3,
                            fingerprint: &fp,
                        },
                        QueueNativeOriginalRequest::Lookup {
                            domain: 2,
                            request_id: SECOND_ID,
                        },
                    ];
                    let _values = recovered
                        .convert_original_batch(&log, &requests, limits(), &job, |view| {
                            let raw = view.outcome_bytes()?.unwrap();
                            let charge = job.owned_buffer(raw.len() as u64)?;
                            let value = Converted {
                                _raw: raw.to_vec(),
                                dropped: dropped.clone(),
                                _charge: charge,
                            };
                            if produced.fetch_add(1, Ordering::SeqCst) == 0 {
                                // Test instrumentation gates the NEXT physical read, after
                                // an independently funded provisional value exists.
                                armed_tx
                                    .take()
                                    .unwrap()
                                    .send(proxy.arm_matching(Some(b"/coupled-data/")))
                                    .unwrap();
                            }
                            Ok(value)
                        })
                        .await
                        .unwrap();
                } else {
                    assert!(
                        resolve_cancelled_mode(mode, &log, &recovered, &fp, &job)
                            .await
                            .unwrap()
                            > 0
                    );
                }
            })
        };
        let (started, release) = if let Some(gate) = initial_gate {
            gate
        } else {
            tokio::time::timeout(Duration::from_secs(10), armed_rx)
                .await
                .unwrap()
                .unwrap()
        };
        let request = tokio::time::timeout(Duration::from_secs(10), started)
            .await
            .unwrap()
            .unwrap();
        assert!(request.starts_with(b"GET ") || request.starts_with(b"HEAD "));
        if matches!(mode, OriginalResolutionMode::Single) {
            assert_eq!(
                admission.usage().unwrap(),
                (pending.0, pending.1 + identity + evidence)
            );
        } else {
            let live = admission.usage().unwrap();
            assert_eq!(live.0, pending.0);
            assert!(
                live.1 > pending.1 + evidence,
                "batch identities and containers are funded before C completes"
            );
        }
        if converted {
            assert_eq!(produced.load(Ordering::SeqCst), 1);
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
        }
        caller.abort();
        assert!(matches!(caller.await,Err(e)if e.is_cancelled()));
        if converted {
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                1,
                "prior provisional DTO retires on second-read cancellation"
            );
        }
        assert_eq!(
            admission.usage().unwrap(),
            pending,
            "only C physical owners survive the cancelled F future"
        );
        let spent = recovered.budget().remaining_work();
        assert_eq!(
            work.semantic.pages - spent.semantic.pages,
            attempts * limits().read.page.work.pages
        );
        assert_eq!(
            work.semantic.hydrated_bytes - spent.semantic.hydrated_bytes,
            attempts * limits().read.page.work.hydrated_bytes
        );
        assert_eq!(
            (
                work.record_attempts - spent.record_attempts,
                work.hydration_reads - spent.hydration_reads,
                work.hydration_body_bytes - spent.hydration_body_bytes
            ),
            (attempts, hydration_reads, hydration_bytes)
        );
        release.send(()).unwrap();
        assert_eq!(
            log.capture(&recovered.as_source().namespace)
                .await
                .unwrap()
                .head(),
            head
        );
        assert_eq!(admission.usage().unwrap(), baseline);
        let settled = recovered.budget().remaining_work();
        assert_eq!(
            (
                spent.semantic.pages,
                spent.semantic.hydrated_bytes,
                spent.record_attempts,
                spent.hydration_reads,
                spent.hydration_body_bytes
            ),
            (
                settled.semantic.pages,
                settled.semantic.hydrated_bytes,
                settled.record_attempts,
                settled.hydration_reads,
                settled.hydration_body_bytes
            )
        );
        assert!(
            resolve_cancelled_mode(mode, &log, &recovered, &fp, &job)
                .await
                .unwrap()
                > 0
        );
        let retried = recovered.budget().remaining_work();
        assert_eq!(
            spent.semantic.pages - retried.semantic.pages,
            attempts * limits().read.page.work.pages
        );
        assert_eq!(
            spent.semantic.hydrated_bytes - retried.semantic.hydrated_bytes,
            attempts * limits().read.page.work.hydrated_bytes
        );
        assert_eq!(
            (
                spent.record_attempts - retried.record_attempts,
                spent.hydration_reads - retried.hydration_reads,
                spent.hydration_body_bytes - retried.hydration_body_bytes
            ),
            (attempts, hydration_reads, hydration_bytes)
        );
        assert_eq!(admission.usage().unwrap(), baseline);
        drop(recovered);
        log.shutdown().await.unwrap();
        drop((log, job));
        assert_eq!(admission.usage().unwrap(), (0, 0));
        drop(proxy);
        cleanup(env, &prefix).await;
    }
}

fn same_work(a: QueueNativeSourceWork, b: QueueNativeSourceWork) {
    assert_eq!(
        (
            a.semantic.pages,
            a.semantic.hydrated_bytes,
            a.record_attempts,
            a.hydration_reads,
            a.hydration_body_bytes
        ),
        (
            b.semantic.pages,
            b.semantic.hydrated_bytes,
            b.record_attempts,
            b.hydration_reads,
            b.hydration_body_bytes
        )
    );
}
#[tokio::test]
async fn original_preflight_and_identity_grant_fail_before_closed_dispatch_or_work() {
    let dir = tempfile::tempdir().unwrap();
    let log =
        QueueNativeLog::local(dir.path(), "original-preflight".into(), options(1 << 20)).unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (recovered, fp) = seeded(&log, &job, false).await;
    let other_log =
        QueueNativeLog::local(dir.path(), "other-original".into(), options(1 << 20)).unwrap();
    let other_admission = QueueAdmission::new(1, 1 << 20).unwrap();
    let other_job = Arc::new(other_admission.reserve(0).unwrap());
    log.shutdown().await.unwrap();
    let baseline = admission.usage().unwrap();
    let work = recovered.budget().remaining_work();
    for fault in [
        "rows", "output", "pages", "bytes", "range", "payload", "logical", "physical", "frame",
        "domain", "id", "format", "job", "log",
    ] {
        let mut caps = limits();
        match fault {
            "rows" => caps.read.page.rows = 0,
            "output" => caps.read.page.output_bytes = 0,
            "pages" => caps.read.page.work.pages = 0,
            "bytes" => caps.read.page.work.hydrated_bytes = 0,
            "range" => caps.read.range_pages = 0,
            "payload" => caps.record.payload_bytes = 0,
            "logical" => caps.record.logical_identity_bytes = 0,
            "physical" => caps.record.physical_identity_bytes = 0,
            "frame" => caps.record.frame_bytes = 0,
            _ => (),
        }
        let result = recovered
            .resolve_original(
                if fault == "log" { &other_log } else { &log },
                QueueNativeOriginalRequest::Replay {
                    domain: if fault == "domain" { 3 } else { 2 },
                    request_id: if fault == "id" { " \t" } else { ID },
                    fingerprint_format: if fault == "format" { 0 } else { 3 },
                    fingerprint: &fp,
                },
                caps,
                if fault == "job" { &other_job } else { &job },
            )
            .await;
        let error = match result {
            Err(e) => e,
            Ok(_) => panic!("preflight {fault} unexpectedly succeeded"),
        };
        assert!(
            !format!("{error:?}").contains("native execution closed"),
            "{fault} reached closed SDK"
        );
        if matches!(fault, "domain" | "id" | "format") {
            assert!(matches!(error, QueueError::Invalid(_)));
        } else {
            assert!(matches!(error, QueueError::OutcomeUnknown(_)));
        }
        same_work(work, recovered.budget().remaining_work());
        assert_eq!(admission.usage().unwrap(), baseline);
    }
    let identity = (std::mem::size_of::<QueueNativeResolvedOriginal<'_>>() + ID.len()) as u64;
    let filler = job
        .owned_buffer((64 << 20) - baseline.1 - (identity - 1))
        .unwrap();
    let before = admission.usage().unwrap();
    let short = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Lookup {
                domain: 2,
                request_id: ID,
            },
            limits(),
            &job,
        )
        .await;
    assert!(
        matches!(short, Err(QueueError::OutcomeUnknown(ref m)) if m.contains("admitted buffer capacity exhausted"))
    );
    same_work(work, recovered.budget().remaining_work());
    assert_eq!(admission.usage().unwrap(), before);
    drop(filler);
    assert_eq!(admission.usage().unwrap(), baseline);
    let control = recovered
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Lookup {
                domain: 2,
                request_id: ID,
            },
            limits(),
            &job,
        )
        .await;
    assert!(
        matches!(control,Err(QueueError::OutcomeUnknown(ref m))if m.contains("native execution closed"))
    );
    no_record_spent(work, recovered.budget().remaining_work());
    assert_eq!(admission.usage().unwrap(), baseline);
    other_log.shutdown().await.unwrap();
    drop((recovered, log, job, other_log, other_job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(other_admission.usage().unwrap(), (0, 0));
}

// The SDK fixture deliberately bypasses the production compiler to publish an
// authenticated semantic fact. It never modifies an existing captured cut.
// Re-appending a checked record only installs the fault; this is neither
// compiler-valid history nor qualification of a full cold history fold.
async fn replace_original_fact(
    log: &QueueNativeLog,
    prior: &QueueNativeRecovered,
    fact: &fireweed_engine::queue_source_index::QueueSourceOriginal,
    payload: &[u8],
    identity: &str,
    job: &Arc<QueueReservation>,
) -> QueueNativeRecovered {
    use fireweed_engine::queue_source_index::QueueSourceKey;
    let address = QueueAddress {
        tenant: "tenant".into(),
        queue: "original\0queue".into(),
    };
    let acquired = log
        .acquire_bundle(
            prior.as_source().retained_namespace(&address, job).unwrap(),
            job,
        )
        .await
        .unwrap();
    let key = QueueSourceKey::Original {
        domain: 2,
        request_id: ID,
    }
    .encode()
    .unwrap();
    let value = fireweed_engine::queue_storage_layout::encode(fact).unwrap();
    let caps = provision_limits().preparation.prepare;
    let publication = log
        .log
        .prepare_with_edits(
            &acquired.writer.writer,
            &acquired.source.cut.cut,
            &[object_log::NativeAppend {
                identity,
                payload,
                records: 1,
            }],
            &[object_log::NativeSemanticEdit {
                key: &key,
                value: Some(&value),
            }],
            object_log::NativeEditLimits {
                edits: caps.edits,
                input_bytes: caps.input_bytes,
                work: caps.work.sdk(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        log.log.submit(&publication).await.unwrap(),
        object_log::NativePublishOutcome::Stored(_)
    ));
    let captured = log
        .capture_source(
            acquired
                .as_source()
                .retained_namespace(&address, job)
                .unwrap(),
            job,
        )
        .await
        .unwrap();
    log.recover_captured(captured, prior.budget().clone(), limits(), job)
        .await
        .unwrap()
}

#[tokio::test]
async fn original_adapter_rejects_authenticated_valid_subspan_after_hydration_then_retries_same_ledger()
 {
    use fireweed_engine::queue_log_record::QueueLogPayload;
    use fireweed_engine::queue_source_index::{
        QueueSourceKey, QueueSourceOriginal, QueueSourcePayload,
    };
    let dir = tempfile::tempdir().unwrap();
    let log = QueueNativeLog::local(
        dir.path(),
        "original-correspondence".into(),
        options(1 << 20),
    )
    .unwrap();
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let (original, fp) = seeded(&log, &job, false).await;
    let key = QueueSourceKey::Original {
        domain: 2,
        request_id: ID,
    }
    .encode()
    .unwrap();
    let value = original
        .as_source()
        .semantic_lookup(&log, &key, work(), &job)
        .await
        .unwrap()
        .unwrap();
    let good: QueueSourceOriginal =
        fireweed_engine::queue_storage_layout::decode(value.bytes()).unwrap();
    drop(value);
    let record = original
        .as_source()
        .read_source_record(&log, 1, limits().record, original.budget(), &job)
        .await
        .unwrap();
    let checked = record.checked().unwrap();
    let QueueLogPayload::Publication(publication) = checked.view().payload() else {
        panic!()
    };
    let receipt = publication.receipts().next().unwrap().unwrap();
    let raw = receipt.encoded_outcome();
    assert!(raw.len() > 1);
    let wrong_payload = QueueSourcePayload::from_checked(&checked, 1, &raw[1..]).unwrap();
    wrong_payload.span().resolve(&checked, 1).unwrap();
    wrong_payload.verify_cached(&raw[1..]).unwrap();
    let bad = QueueSourceOriginal {
        outcome: wrong_payload,
        ..good
    };
    bad.validate(2).unwrap();
    let malformed = replace_original_fact(
        &log,
        &original,
        &bad,
        record.bytes(),
        "wrong-original-span",
        &job,
    )
    .await;
    assert!(Arc::ptr_eq(original.budget(), malformed.budget()));
    let before = admission.usage().unwrap();
    let work_before = malformed.budget().remaining_work();
    let plan = log
        .log
        .plan_read(&malformed.as_source().cut.cut, 1)
        .await
        .unwrap()
        .unwrap();
    let req = plan.requirements().unwrap();
    drop(plan);
    let result = malformed
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await;
    assert_eq!(
        result.err().unwrap(),
        QueueError::OutcomeUnknown(
            "source original: outcome is not the exact matching receipt span".into()
        )
    );
    assert_eq!(admission.usage().unwrap(), before);
    let work_after = malformed.budget().remaining_work();
    semantic_spent(work_before, work_after);
    assert_eq!(
        (
            work_before.record_attempts - work_after.record_attempts,
            work_before.hydration_reads - work_after.hydration_reads,
            work_before.hydration_body_bytes - work_after.hydration_body_bytes
        ),
        (1, req.hydration_reads(), req.hydration_bytes())
    );
    let restored = replace_original_fact(
        &log,
        &malformed,
        &good,
        record.bytes(),
        "restored-original-span",
        &job,
    )
    .await;
    assert!(Arc::ptr_eq(malformed.budget(), restored.budget()));
    // A replacement cut requires fresh evidence; spent work remains on the same ledger.
    let work_before = restored.budget().remaining_work();
    assert!(work_before.record_attempts < work_after.record_attempts);
    let before = admission.usage().unwrap();
    let result = restored
        .resolve_original(
            &log,
            QueueNativeOriginalRequest::Replay {
                domain: 2,
                request_id: ID,
                fingerprint_format: 3,
                fingerprint: &fp,
            },
            limits(),
            &job,
        )
        .await
        .unwrap();
    assert_eq!(result.outcome_bytes().unwrap().unwrap(), raw);
    let work_after = restored.budget().remaining_work();
    semantic_spent(work_before, work_after);
    assert_eq!(
        (
            work_before.record_attempts - work_after.record_attempts,
            work_before.hydration_reads - work_after.hydration_reads,
            work_before.hydration_body_bytes - work_after.hydration_body_bytes
        ),
        (1, req.hydration_reads(), req.hydration_bytes())
    );
    drop(result);
    assert_eq!(admission.usage().unwrap(), before);
    drop((restored, malformed, record, original));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
