//! Actual native C streaming: shared-frame copies, late caps and cancellation.
use super::*;
use fireweed_engine::queue_source_index::{
    QueueSourceCut, QueueSourcePoint, QueueSourceReader, QueueSourceRecordReader,
    load_batch_preimage,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

type Armed = (
    tokio::sync::oneshot::Receiver<Vec<u8>>,
    std::sync::mpsc::Sender<()>,
);
struct Reader<'a> {
    inner: QueueNativeRecordReader<'a>,
    admission: &'a Arc<QueueAdmission>,
    reads: AtomicUsize,
    window: Option<(u64, &'a Arc<QueueReservation>)>,
    ballast: Mutex<Option<OwnedQueueBufferCharge>>,
    usage: Mutex<Vec<u64>>,
    gate: Option<(
        &'a super::super::s3_cancellation::Proxy,
        Mutex<Option<tokio::sync::oneshot::Sender<Armed>>>,
    )>,
}
impl QueueSourceReader for Reader<'_> {
    type Point = QueueNativeSourcePoint;
    type Range = QueueNativeSourceRange;
    fn cut(&self) -> QueueResult<QueueSourceCut<'_>> {
        self.inner.cut()
    }
    async fn lookup(&self, key: &[u8]) -> QueueResult<Self::Point> {
        self.inner.lookup(key).await
    }
    fn point<'a>(&'a self, owner: &'a Self::Point) -> QueueResult<QueueSourcePoint<'a>> {
        self.inner.point(owner)
    }
    async fn complete_range(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Range> {
        self.inner.complete_range(lower, upper).await
    }
    fn visit_range(
        &self,
        owner: &Self::Range,
        lower: &[u8],
        upper: &[u8],
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        self.inner.visit_range(owner, lower, upper, visitor)
    }
}
impl QueueSourceRecordReader for Reader<'_> {
    type Record = QueueNativeBoundSourceRecord;
    async fn read_record(
        &self,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<Self::Record> {
        let ordinal = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == 1
            && let Some((window, job)) = self.window
        {
            let bytes = (64 << 20) - self.admission.usage()?.1 - window;
            *self.ballast.lock().unwrap() = Some(job.owned_buffer(bytes)?);
        }
        self.usage.lock().unwrap().push(self.admission.usage()?.1);
        assert_eq!(offset, ordinal as u64, "C1 then C2, never one C per alias");
        if ordinal == 2
            && let Some((proxy, sender)) = &self.gate
        {
            let pair = proxy.arm_matching(Some(b"/coupled-data/"));
            sender.lock().unwrap().take().unwrap().send(pair).unwrap();
        }
        self.inner.read_record(cut, offset).await
    }
    fn checked_record<'a>(
        &'a self,
        owner: &'a Self::Record,
        cut: &QueueSourceCut<'_>,
        offset: u64,
    ) -> QueueResult<fireweed_engine::queue_log_record::CheckedQueueLogRecord<'a>> {
        self.inner.checked_record(owner, cut, offset)
    }
}
struct Clock;
impl fireweed_engine::Clock for Clock {
    fn now(&self) -> fireweed_core::UtcTimestamp {
        fireweed_core::UtcTimestamp {
            seconds: 0,
            nanoseconds: 10,
        }
    }
}
fn change(id: &str, key: &str, version: u64) -> CommitGroup {
    let mut g = group(id);
    g.work_updates.push(UnclaimedUpdate {
        key: key.into(),
        expect: StateExpect::Version(version),
        change: UnclaimedChange::Administrative {
            patch: WorkPatch {
                body: Some(vec![9].into()),
                ..Default::default()
            },
            lifecycle: AdministrativeLifecycle::Keep,
            lease_guard: AdministrativeLeaseGuard::RejectActive,
        },
    });
    g
}
async fn contract(log: QueueNativeLog, proxy: Option<&super::super::s3_cancellation::Proxy>) {
    let admission = QueueAdmission::new(1, 64 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let collection = CollectionDefinition {
        fields: Default::default(),
        indexes: Default::default(),
        unique_indexes: Default::default(),
        additional_field_prefixes: Default::default(),
    };
    let schema = QueueSchema {
        work: collection,
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
    let mut history = Vec::new();
    let mut snapshot = RecordSnapshot::default();
    let mut acquired = accept(
        &log,
        acquired,
        genesis,
        &definition,
        (&mut history, &snapshot),
        budget(&job, 10_000),
        &job,
    )
    .await;
    let insert = |key: &str, bytes: usize| NewWork {
        key: key.into(),
        priority: 0,
        available_at: 0,
        fields: Fields::new(),
        body: vec![7; bytes].into(),
    };
    for items in [
        vec![insert("a", 64 << 10), insert("b", 64 << 10)],
        vec![insert("c", 192 << 10)],
    ] {
        let mut seed = group(if history.len() == 1 {
            "seed-ab"
        } else {
            "seed-c"
        });
        seed.work_inserts = items;
        let (record, after) = ordinary(&acquired, &definition, &snapshot, &[seed], &job);
        snapshot = after;
        acquired = accept(
            &log,
            acquired,
            record,
            &definition,
            (&mut history, &snapshot),
            budget(&job, 10_000),
            &job,
        )
        .await;
    }
    let before = acquired.as_source().head();
    let batch = CommitBatch {
        queue: QueueAddress {
            tenant: "tenant".into(),
            queue: "producer".into(),
        },
        groups: vec![
            change("a-change", "a", 1),
            change("a-equal", "a", 2),
            change("b-change", "b", 1),
            change("c-change", "c", 1),
        ],
    };
    let payload = history[2].bytes().len() as u64;
    assert!(payload > history[1].bytes().len() as u64);
    let plan = log
        .log
        .plan_read(&acquired.source.cut.cut, 2)
        .await
        .unwrap()
        .unwrap();
    let req = plan.requirements().unwrap();
    drop(plan);
    let plan = log
        .log
        .plan_read(&acquired.source.cut.cut, 1)
        .await
        .unwrap()
        .unwrap();
    let first_req = plan.requirements().unwrap();
    drop(plan);
    let guard =
        (std::mem::size_of::<OwnedQueueBufferCharge>() + 2 * std::mem::size_of::<usize>()) as u64;
    let physical =
        65536 + 2 * guard + 4 * req.frame_bytes() + 4096 + ownership::physical_owner_bytes::<4>();
    let window = physical + (320 << 10) + (32 << 10);
    assert!(
        window < physical + 4 * first_req.frame_bytes(),
        "bounded Root cannot retain C1 alongside C2 and all output copies"
    );
    for cap in [payload - 1, payload] {
        let ledger = QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: QueueNativeSemanticWork {
                    pages: 100_000,
                    hydrated_bytes: 64 << 20,
                },
                record_attempts: 4,
                hydration_reads: 100,
                hydration_body_bytes: 64 << 20,
            },
            &job,
        )
        .unwrap();
        let initial = ledger.remaining_work();
        let baseline = admission.usage().unwrap();
        let mut record_limits = super::super::record::record_limits();
        record_limits.payload_bytes = cap;
        let reader = Reader {
            inner: QueueNativeSourceReader::new(
                acquired.as_source(),
                &log,
                &job,
                ledger.clone(),
                limits().read,
            )
            .unwrap()
            .with_records(record_limits)
            .unwrap(),
            admission: &admission,
            reads: AtomicUsize::new(0),
            window: if cap == payload {
                Some((window, &job))
            } else {
                None
            },
            ballast: Mutex::new(None),
            usage: Mutex::new(Vec::new()),
            gate: None,
        };
        let loaded = load_batch_preimage(
            &reader,
            &batch,
            &[0, 1, 2, 3],
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .await;
        if cap < payload {
            assert!(matches!(loaded, Err(QueueError::TooLarge)));
        } else {
            let plan = loaded.unwrap().plan(&reader, &Clock).await.unwrap();
            assert_eq!(
                plan.receipts()
                    .map(|r| r.request_id.as_str())
                    .collect::<Vec<_>>(),
                ["a-change", "a-equal", "b-change", "c-change"]
            );
            for receipt in plan.receipts() {
                let GroupOutcome::Committed { versions } = &receipt.outcome else {
                    panic!()
                };
                assert_eq!(versions.len(), 1);
                assert_eq!(versions[0].version, 2);
            }
            drop(plan);
        }
        assert_eq!(reader.reads.load(Ordering::SeqCst), 2);
        {
            let usage = reader.usage.lock().unwrap();
            assert_eq!(
                usage[1] - usage[0],
                128 << 10,
                "only independently funded a/b copies remain between C1 and C2"
            );
        }
        drop(reader.ballast.lock().unwrap().take());
        assert_eq!(admission.usage().unwrap(), baseline);
        let left = ledger.remaining_work();
        assert_eq!(left.record_attempts, 2);
        let expected_reads = first_req.hydration_reads()
            + if cap == payload {
                req.hydration_reads()
            } else {
                0
            };
        let expected_bytes = first_req.hydration_bytes()
            + if cap == payload {
                req.hydration_bytes()
            } else {
                0
            };
        assert_eq!(
            initial.hydration_reads - left.hydration_reads,
            expected_reads
        );
        assert_eq!(
            initial.hydration_body_bytes - left.hydration_body_bytes,
            expected_bytes
        );
        assert_eq!(
            log.capture(&acquired.source.namespace)
                .await
                .unwrap()
                .head(),
            before
        );
    }
    if let Some(proxy) = proxy {
        let ledger = QueueNativeSourceBudget::new(
            QueueNativeSourceWork {
                semantic: QueueNativeSemanticWork {
                    pages: 100_000,
                    hydrated_bytes: 64 << 20,
                },
                record_attempts: 4,
                hydration_reads: 100,
                hydration_body_bytes: 64 << 20,
            },
            &job,
        )
        .unwrap();
        let initial = ledger.remaining_work();
        let baseline = admission.usage().unwrap();
        let (armed, receiver) = tokio::sync::oneshot::channel();
        let reader = Reader {
            inner: QueueNativeSourceReader::new(
                acquired.as_source(),
                &log,
                &job,
                ledger.clone(),
                limits().read,
            )
            .unwrap()
            .with_records(super::super::record::record_limits())
            .unwrap(),
            admission: &admission,
            reads: AtomicUsize::new(0),
            window: None,
            ballast: Mutex::new(None),
            usage: Mutex::new(Vec::new()),
            gate: Some((proxy, Mutex::new(Some(armed)))),
        };
        let queue_limits = QueueLimits::default();
        let mut future = Box::pin(load_batch_preimage(
            &reader,
            &batch,
            &[0, 1, 2, 3],
            &definition,
            &queue_limits,
            &job,
        ));
        let (started, release) = tokio::select! { pair = receiver => pair.unwrap(), _ = &mut future => panic!("C2 gate never armed"), _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("C2 arm timed out") };
        let request = tokio::select! { r = started => r.unwrap(), _ = &mut future => panic!("C2 frame never gated"), _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("C2 gate timed out") };
        assert!(
            String::from_utf8(request)
                .unwrap()
                .contains("/coupled-data/")
        );
        let spent = ledger.remaining_work();
        assert_eq!(spent.record_attempts, 2);
        assert_eq!(
            initial.hydration_reads - spent.hydration_reads,
            first_req.hydration_reads() + req.hydration_reads()
        );
        assert_eq!(
            initial.hydration_body_bytes - spent.hydration_body_bytes,
            first_req.hydration_bytes() + req.hydration_bytes()
        );
        drop(future);
        let guard = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let physical = 65536
            + 2 * guard
            + 4 * req.frame_bytes()
            + 4096
            + ownership::physical_owner_bytes::<4>();
        assert_eq!(
            admission.usage().unwrap(),
            (baseline.0, baseline.1 + physical),
            "only pending C2 physical owners survive driven-future cancellation"
        );
        release.send(()).unwrap();
        assert_eq!(
            log.capture(&acquired.source.namespace)
                .await
                .unwrap()
                .head(),
            before
        );
        assert_eq!(admission.usage().unwrap(), baseline);
        let after = ledger.remaining_work();
        assert_eq!(
            (
                after.semantic.pages,
                after.semantic.hydrated_bytes,
                after.record_attempts,
                after.hydration_reads,
                after.hydration_body_bytes
            ),
            (
                spent.semantic.pages,
                spent.semantic.hydrated_bytes,
                spent.record_attempts,
                spent.hydration_reads,
                spent.hydration_body_bytes
            )
        );
        let retry = Reader {
            inner: QueueNativeSourceReader::new(
                acquired.as_source(),
                &log,
                &job,
                ledger.clone(),
                limits().read,
            )
            .unwrap()
            .with_records(super::super::record::record_limits())
            .unwrap(),
            admission: &admission,
            reads: AtomicUsize::new(0),
            window: None,
            ballast: Mutex::new(None),
            usage: Mutex::new(Vec::new()),
            gate: None,
        };
        let preimage = load_batch_preimage(
            &retry,
            &batch,
            &[0, 1, 2, 3],
            &definition,
            &QueueLimits::default(),
            &job,
        )
        .await
        .unwrap();
        drop(preimage);
        assert_eq!(admission.usage().unwrap(), baseline);
        let retried = ledger.remaining_work();
        assert_eq!(retried.record_attempts, 0);
        let counts = |w: QueueNativeSourceWork| {
            [
                w.semantic.pages,
                w.semantic.hydrated_bytes,
                w.record_attempts,
                w.hydration_reads,
                w.hydration_body_bytes,
            ]
        };
        let initial = counts(initial);
        let spent = counts(spent);
        let after = counts(after);
        let retried = counts(retried);
        for i in 0..5 {
            assert!(initial[i] > spent[i]);
            assert_eq!(
                initial[i] - spent[i],
                after[i] - retried[i],
                "same-ledger retry repeats exact work dimension {i}"
            );
        }
    }
    drop((history, snapshot, acquired, definition));
    log.shutdown().await.unwrap();
    drop((log, job));
    assert_eq!(admission.usage().unwrap(), (0, 0));
}
#[tokio::test]
async fn native_body_preimage_streams_two_frames_and_enforces_late_exact_cap_local() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = options(1 << 20);
    config.operations = 1;
    contract(
        QueueNativeLog::local(directory.path(), "body-stream".into(), config).unwrap(),
        None,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_body_preimage_second_frame_cancellation_real_s3() {
    use object_log::BlobStore;
    for disabled in [false, true] {
        let env = crate::shared_s3_test_env();
        let proxy = super::super::s3_cancellation::Proxy::new(&env.endpoint);
        let prefix = format!(
            "body-stream/{}/{disabled}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut config = options(1 << 20);
        config.operations = 1;
        let log = QueueNativeLog::s3(
            QueueNativeS3Connection {
                endpoint: proxy.endpoint(),
                region: &env.region,
                bucket: &env.bucket,
                access_key: &env.access_key,
                secret_key: &env.secret_key,
            },
            prefix.clone(),
            config,
            QueueNativeS3TransportOptions {
                disable_payload_signing: disabled,
                ..Default::default()
            },
        )
        .unwrap();
        contract(log, Some(&proxy)).await;
        drop(proxy);
        let cleanup = object_log::S3BlobStore::new(
            &env.endpoint,
            &env.region,
            &env.bucket,
            &env.access_key,
            &env.secret_key,
        );
        for key in cleanup.list(&prefix).await.unwrap() {
            cleanup.delete(&key).await.unwrap();
        }
    }
}
