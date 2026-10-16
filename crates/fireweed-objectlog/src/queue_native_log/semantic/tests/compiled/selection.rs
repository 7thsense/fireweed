//! Real source capture + one-row SDK pages + physical Turso candidate storage.
//! This component check does not qualify complete QueueStore selection or RSS.
use super::*;
use fireweed_engine::queue_source_index::{
    QueueSourceCandidateProducer, QueueSourceSelectionBudget, QueueSourceSelectionRequest,
    QueueSourceSelectionWork,
};
use fireweed_queue_spool::{ScratchDiskQuota, ScratchRoot, ScratchWorkers};
use fireweed_turso::queue_scratch::SourceCandidateScratch;

use fireweed_engine::queue_source_index::{
    QueueSourceCut, QueueSourcePoint, QueueSourceReader, QueueSourceStreamingReader,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[derive(Default)]
struct Trace {
    pages: AtomicUsize,
    empty: AtomicUsize,
    rows: AtomicUsize,
    finishes: AtomicUsize,
}
struct Observed<R> {
    inner: R,
    trace: Arc<Trace>,
}
impl<R: QueueSourceReader> QueueSourceReader for Observed<R> {
    type Point = R::Point;
    type Range = R::Range;
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
impl<R: QueueSourceStreamingReader> QueueSourceStreamingReader for Observed<R> {
    type Cursor = R::Cursor;
    type Page = R::Page;
    type Completion = R::Completion;
    fn open_stream(&self, lower: &[u8], upper: &[u8]) -> QueueResult<Self::Cursor> {
        self.inner.open_stream(lower, upper)
    }
    async fn next_stream_page(&self, cursor: &mut Self::Cursor) -> QueueResult<Option<Self::Page>> {
        self.inner.next_stream_page(cursor).await
    }
    fn visit_stream_page(
        &self,
        cursor: &mut Self::Cursor,
        page: Self::Page,
        visitor: &mut dyn for<'a> FnMut(QueueSourcePoint<'a>) -> QueueResult<()>,
    ) -> QueueResult<()> {
        let mut rows = 0;
        self.inner.visit_stream_page(cursor, page, &mut |point| {
            rows += 1;
            visitor(point)
        })?;
        self.trace.pages.fetch_add(1, Ordering::SeqCst);
        self.trace.rows.fetch_add(rows, Ordering::SeqCst);
        if rows == 0 {
            self.trace.empty.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
    fn finish_stream(&self, cursor: Self::Cursor) -> QueueResult<Self::Completion> {
        let proof = self.inner.finish_stream(cursor)?;
        self.trace.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(proof)
    }
    fn validate_stream_completion(
        &self,
        proof: &Self::Completion,
        lower: &[u8],
        upper: &[u8],
    ) -> QueueResult<()> {
        self.inner.validate_stream_completion(proof, lower, upper)
    }
}

// Faults occur only AFTER the underlying physical insertion succeeds.
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    None,
    FailSecondWrite,
    PauseSecondWrite,
    CorruptCorrespondence,
    DenyTerminalPage,
    HoldSecondWrite,
}
struct PhysicalScratch {
    inner: SourceCandidateScratch,
    writes: Arc<AtomicUsize>,
    fault: Fault,
    quota: Arc<ScratchDiskQuota>,
    entered: Arc<tokio::sync::Notify>,
    release: Option<Arc<tokio::sync::Notify>>,
    released: Option<Arc<AtomicBool>>,
}
impl fireweed_engine::queue_source_index::QueueSourceCandidateScratch for PhysicalScratch {
    type Builder = fireweed_turso::queue_scratch::SourceSetBuilder;
    type Set = fireweed_turso::queue_scratch::SourceCandidateSet;
    type Row = fireweed_turso::queue_scratch::SourceCandidateIdentity;
    fn validate_operation(
        &self,
        job: &QueueReservation,
        budget: &Arc<QueueSourceSelectionBudget>,
    ) -> QueueResult<()> {
        fireweed_engine::queue_source_index::QueueSourceCandidateScratch::validate_operation(
            &self.inner,
            job,
            budget,
        )
    }
    fn begin_set(&mut self) -> QueueResult<Self::Builder> {
        self.inner.begin_set()
    }
    async fn insert(
        &mut self,
        builder: &mut Self::Builder,
        collection: Option<&str>,
        key: &str,
    ) -> QueueResult<()> {
        self.inner.insert(builder, collection, key).await?;
        if self.writes.fetch_add(1, Ordering::SeqCst) == 1 {
            assert!(self.quota.usage()? > 0);
            match self.fault {
                Fault::None | Fault::CorruptCorrespondence | Fault::DenyTerminalPage => {}
                Fault::FailSecondWrite => {
                    return Err(QueueError::OutcomeUnknown(
                        "injected after second physical write".into(),
                    ));
                }
                Fault::HoldSecondWrite => {
                    self.entered.notify_one();
                    self.release.as_ref().unwrap().notified().await;
                    self.released
                        .as_ref()
                        .unwrap()
                        .store(true, Ordering::SeqCst);
                    return Err(QueueError::OutcomeUnknown(
                        "released abandoned physical operation".into(),
                    ));
                }
                Fault::PauseSecondWrite => {
                    self.entered.notify_one();
                    std::future::pending::<()>().await;
                }
            }
        }
        Ok(())
    }
    fn seal(&self, builder: Self::Builder) -> QueueResult<Self::Set> {
        self.inner.seal(builder)
    }
    async fn union(&mut self, left: &Self::Set, right: &Self::Set) -> QueueResult<Self::Set> {
        self.inner.union(left, right).await
    }
    async fn intersection(
        &mut self,
        left: &Self::Set,
        right: &Self::Set,
    ) -> QueueResult<Self::Set> {
        self.inner.intersection(left, right).await
    }
    async fn difference(&mut self, left: &Self::Set, right: &Self::Set) -> QueueResult<Self::Set> {
        self.inner.difference(left, right).await
    }
    async fn next(
        &mut self,
        set: &Self::Set,
        after: Option<&Self::Row>,
    ) -> QueueResult<Option<Self::Row>> {
        self.inner.next(set, after).await
    }
    fn encoded_identity<'a>(&self, row: &'a Self::Row) -> QueueResult<&'a [u8]> {
        fireweed_engine::queue_source_index::QueueSourceCandidateScratch::encoded_identity(
            &self.inner,
            row,
        )
    }
    fn count(&self, set: &Self::Set) -> QueueResult<u64> {
        fireweed_engine::queue_source_index::QueueSourceCandidateScratch::count(&self.inner, set)
    }
}

async fn contract(log: QueueNativeLog) {
    let log = Arc::new(log);
    let admission = QueueAdmission::new(1, 128 << 20).unwrap();
    let job = Arc::new(admission.reserve(0).unwrap());
    let schema = QueueSchema {
        work: CollectionDefinition {
            fields: BTreeMap::from([("n".into(), FieldType::Integer)]),
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
    let mut seed = group("selection-seed");
    for (key, n) in [("a", 1), ("b", 1), ("c", 2)] {
        seed.work_inserts.push(NewWork {
            key: key.into(),
            priority: 1,
            available_at: 0,
            fields: BTreeMap::from([("n".into(), FieldValue::Integer(n))]),
            body: vec![1, 2, 3].into(),
        });
    }
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
    drop(history);
    let abandoned = fresh(&log, &acquired, "producer", &job).await;
    caller_abandonment(
        log.clone(),
        abandoned,
        AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap(),
        job.clone(),
        admission.clone(),
    )
    .await;
    // The isolated trial acquired its own writer epoch; capture a fresh source
    // rather than treating the prior capsule as current.
    let refreshed = fresh(&log, &acquired, "producer", &job).await;
    drop(acquired);
    let acquired = refreshed;
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let delivery_quota = quota.clone();
    // This observes the old cut's identity allocation only; the isolated
    // caller-abandonment trial above observes the actual funded capsule.
    let capsule = Arc::downgrade(&acquired.source.semantic_identity);
    let observing_capsule = capsule.clone();
    workers
        .run(move || {
            // One owned runtime drives both actual SDKs; never dispatch another
            // operation to this same single-worker pool from inside the operation.
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut acquired = acquired;
                    let queue = QueueAddress {
                        tenant: "tenant".into(),
                        queue: "producer".into(),
                    };
                    let mut read = limits().read;
                    read.page.rows = 1;
                    read.range_pages = 1;
                    for (predicate, expected, fault) in [
                        (Predicate::Equal(FieldValue::Integer(1)), 2, Fault::None),
                        (Predicate::Equal(FieldValue::Integer(99)), 0, Fault::None),
                        (Predicate::Equal(FieldValue::Integer(1)), 2, Fault::FailSecondWrite),
                        (Predicate::Equal(FieldValue::Integer(1)), 2, Fault::PauseSecondWrite),
                        (Predicate::Equal(FieldValue::Integer(1)), 2, Fault::DenyTerminalPage),
                        (Predicate::Equal(FieldValue::Integer(1)), 2, Fault::CorruptCorrespondence),
                    ] {
                        if fault == Fault::CorruptCorrespondence {
                            // Deliberately reauthenticate inconsistent semantic facts;
                            // this is not damaged unauthenticated cache data.
                            let (record, _) = ordinary(&acquired, &definition, &before,
                                &[group("inconsistent-field")], &job);
                            let key = fireweed_engine::queue_source_index::QueueSourceKey::Field {
                                collection: None, key: "b", field: "n",
                            }.encode().unwrap();
                            let value = fireweed_engine::queue_storage_layout::encode(&FieldValue::Integer(9)).unwrap();
                            let prepared = acquired.prepare_with_edits(&log, &record,
                                "test-only-inconsistent-field", &[QueueNativeSemanticEdit { key: &key, value: Some(&value) }],
                                limits().prepare, &job).await.unwrap();
                            assert!(matches!(prepared.submit(&log).await.unwrap(), QueueNativeSubmit::Stored(_)));
                            acquired = fresh(&log, &acquired, "producer", &job).await;
                        }
                        let read_ledger = if fault == Fault::DenyTerminalPage { budget(&job, 8) } else { ledger.clone() };
                        // The actual acquired capsule remains in this coordinator until
                        // the reader, producer, leaves and physical scratch have retired.
                        let reader = QueueNativeSourceReader::new(
                            acquired.as_source(),
                            &log,
                            &job,
                            read_ledger.clone(),
                            read,
                        )
                        .unwrap();
                        let trace = Arc::new(Trace::default());
                        let reader = Observed {
                            inner: reader,
                            trace: trace.clone(),
                        };
                        let selection = QueueSourceSelectionBudget::new(
                            QueueSourceSelectionWork {
                                rows: 100_000,
                                copied_bytes: 64 << 20,
                                scratch_steps: 100_000,
                            },
                            &job,
                        )
                        .unwrap();
                        let scratch = SourceCandidateScratch::open(
                            root.clone(),
                            delivery_quota.clone(),
                            job.clone(),
                            selection.clone(),
                        )
                        .await
                        .unwrap();
                        let writes = Arc::new(AtomicUsize::new(0));
                        let entered = Arc::new(tokio::sync::Notify::new());
                        let scratch = PhysicalScratch { inner: scratch, writes: writes.clone(), fault,
                            quota: delivery_quota.clone(), entered: entered.clone(), release: None, released: None };
                        let request = ClaimRequest {
                            queue: queue.clone(),
                            request_id: "selection".into(),
                            filter: Filter {
                                fields: BTreeMap::from([("n".into(), predicate)]),
                                ..Default::default()
                            },
                            order: ClaimOrder::PriorityAscending,
                            eligible_at: 0,
                            lease_nanos: 1_000_000,
                            max_records: 3,
                            max_bytes: 1 << 20,
                            fields: None,
                            include_body: false,
                        };
                        let producer = QueueSourceCandidateProducer::begin(
                            reader,
                            scratch,
                            &definition,
                            QueueSourceSelectionRequest::Claim(&request),
                            0,
                            &QueueLimits::default(),
                            job.clone(),
                            selection.clone(),
                        )
                        .await
                        .unwrap();
                        let before = selection.remaining(&job).unwrap();
                        let source_before = read_ledger.remaining_work();
                        if fault == Fault::PauseSecondWrite {
                            let mut pending = Box::pin(producer.scalar(0));
                            tokio::select! {
                                _ = entered.notified() => {},
                                _ = &mut pending => panic!("physical pause did not retain producer"),
                            }
                            let spent = selection.remaining(&job).unwrap();
                            assert!(spent.rows < before.rows && spent.scratch_steps < before.scratch_steps);
                            assert_eq!(writes.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.finishes.load(Ordering::SeqCst), 0);
                            assert!(delivery_quota.usage().unwrap() > 0);
                            assert!(observing_capsule.upgrade().is_some());
                            let source_spent = read_ledger.remaining_work();
                            assert!(source_spent.semantic.pages < source_before.semantic.pages);
                            drop(pending);
                            assert_eq!(delivery_quota.usage().unwrap(), 0);
                            assert_eq!(selection.remaining(&job).unwrap(), spent);
                            let after = read_ledger.remaining_work();
                            assert_eq!((after.semantic.pages, after.semantic.hydrated_bytes, after.record_attempts, after.hydration_reads, after.hydration_body_bytes),
                                (source_spent.semantic.pages, source_spent.semantic.hydrated_bytes, source_spent.record_attempts, source_spent.hydration_reads, source_spent.hydration_body_bytes));
                            continue;
                        }
                        let result = producer.scalar(0).await;
                        if fault == Fault::DenyTerminalPage {
                            assert!(matches!(result, Err(QueueError::OutcomeUnknown(ref reason)) if reason == "native queue log: request source work admission exhausted"));
                            assert_eq!(writes.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.rows.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.pages.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.empty.load(Ordering::SeqCst), 0);
                            assert_eq!(trace.finishes.load(Ordering::SeqCst), 0);
                            assert_eq!(read_ledger.remaining_work().semantic.pages, 0);
                            assert_eq!(delivery_quota.usage().unwrap(), 0);
                            continue;
                        }
                        if fault == Fault::CorruptCorrespondence {
                            assert!(matches!(result, Err(QueueError::OutcomeUnknown(ref reason)) if reason == "source candidate producer: posting tuple differs from field fact"));
                            assert_eq!(writes.load(Ordering::SeqCst), 1);
                            assert_eq!(trace.rows.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.finishes.load(Ordering::SeqCst), 0);
                            assert_eq!(delivery_quota.usage().unwrap(), 0);
                            assert!(ledger.remaining_work().semantic.pages < source_before.semantic.pages);
                            continue;
                        }
                        if fault == Fault::FailSecondWrite {
                            assert!(matches!(result, Err(QueueError::OutcomeUnknown(ref reason)) if reason == "injected after second physical write"));
                            assert_eq!(writes.load(Ordering::SeqCst), 2);
                            assert_eq!(trace.finishes.load(Ordering::SeqCst), 0);
                            assert_eq!(delivery_quota.usage().unwrap(), 0);
                            let spent = selection.remaining(&job).unwrap();
                            assert!(spent.rows < before.rows && spent.scratch_steps < before.scratch_steps);
                            continue;
                        }
                        let (producer, leaf) = result.unwrap();
                        assert_eq!(producer.scalar_len(&leaf, 0).unwrap(), expected);
                        assert_eq!(trace.rows.load(Ordering::SeqCst), expected as usize);
                        assert_eq!(trace.finishes.load(Ordering::SeqCst), 1);
                        assert_eq!(trace.empty.load(Ordering::SeqCst), 1);
                        assert_eq!(trace.pages.load(Ordering::SeqCst), expected as usize + 1);
                        drop(leaf);
                        drop(producer);
                        assert_eq!(delivery_quota.usage().unwrap(), 0);
                        assert!(acquired.as_source().confirms(&log, &job).await.unwrap());
                    }
                    Ok(())
                })
        })
        .await
        .unwrap();
    workers.close_and_drain().await.unwrap();
    assert!(capsule.upgrade().is_none());
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(admission.usage().unwrap(), (0, 0));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
#[tokio::test]
async fn source_scalar_candidates_compose_with_actual_turso_local() {
    let root = tempfile::tempdir().unwrap();
    contract(QueueNativeLog::local(root.path(), "source-scalar".into(), options(1 << 20)).unwrap())
        .await;
}

#[tokio::test]
async fn source_scalar_candidates_compose_with_actual_turso_real_s3() {
    use object_log::BlobStore;
    let env = crate::shared_s3_test_env();
    for disabled in [false, true] {
        let prefix = format!(
            "native-source-scalar/{}/{disabled}",
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

struct OwnedCapsule {
    acquired: QueueNativeAcquired,
    _charge: OwnedQueueBufferCharge,
}
async fn caller_abandonment(
    log: Arc<QueueNativeLog>,
    acquired: QueueNativeAcquired,
    definition: AdmittedQueueDefinition,
    job: Arc<QueueReservation>,
    admission: Arc<QueueAdmission>,
) {
    use std::future::Future;
    let directory = tempfile::tempdir().unwrap();
    let workers = ScratchWorkers::new(1, 1).unwrap();
    let root = ScratchRoot::open(directory.path().to_path_buf(), &workers)
        .await
        .unwrap();
    let quota = ScratchDiskQuota::new(64 << 20).unwrap();
    let capsule = Arc::new(OwnedCapsule {
        acquired,
        _charge: job
            .owned_buffer(
                (std::mem::size_of::<OwnedCapsule>() + 2 * std::mem::size_of::<usize>()) as u64,
            )
            .unwrap(),
    });
    let weak = Arc::downgrade(&capsule);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    struct ReleaseOnDrop(Arc<tokio::sync::Notify>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let release_guard = ReleaseOnDrop(release.clone());
    let released = Arc::new(AtomicBool::new(false));
    let terminal = Arc::new(AtomicBool::new(false));
    let sent_released = released.clone();
    let sent_terminal = terminal.clone();
    let selection = QueueSourceSelectionBudget::new(
        QueueSourceSelectionWork {
            rows: 100_000,
            copied_bytes: 64 << 20,
            scratch_steps: 100_000,
        },
        &job,
    )
    .unwrap();
    let ledger = budget(&job, 1000);
    let observer_job = job.clone();
    let running = workers.clone();
    let sent_quota = quota.clone();
    let sent_selection = selection.clone();
    let sent_ledger = ledger.clone();
    let sent_entered = entered.clone();
    let sent_release = release.clone();
    let caller = tokio::spawn(async move {
        running.run(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
                let mut read = limits().read;
                read.page.rows = 1;
                read.range_pages = 1;
                let reader = QueueNativeSourceReader::new(capsule.acquired.as_source(), &log, &job, sent_ledger.clone(), read).unwrap();
                let trace = Arc::new(Trace::default());
                let reader = Observed { inner: reader, trace: trace.clone() };
                let writes = Arc::new(AtomicUsize::new(0));
                let scratch = PhysicalScratch {
                    inner: SourceCandidateScratch::open(root, sent_quota.clone(), job.clone(), sent_selection.clone()).await.unwrap(),
                    writes: writes.clone(), fault: Fault::HoldSecondWrite,
                    quota: sent_quota.clone(), entered: sent_entered, release: Some(sent_release), released: Some(sent_released),
                };
                let request = ClaimRequest {
                    queue: QueueAddress { tenant: "tenant".into(), queue: "producer".into() }, request_id: "abandoned-selection".into(),
                    filter: Filter { fields: BTreeMap::from([("n".into(), Predicate::Equal(FieldValue::Integer(1)))]), ..Default::default() },
                    order: ClaimOrder::PriorityAscending, eligible_at: 0, lease_nanos: 1_000_000,
                    max_records: 3, max_bytes: 1 << 20, fields: None, include_body: false,
                };
                let producer = QueueSourceCandidateProducer::begin(reader, scratch, &definition,
                    QueueSourceSelectionRequest::Claim(&request), 0, &QueueLimits::default(), job.clone(), sent_selection).await.unwrap();
                let result = producer.scalar(0).await;
                assert!(matches!(result, Err(QueueError::OutcomeUnknown(ref reason)) if reason == "released abandoned physical operation"));
                assert_eq!(writes.load(Ordering::SeqCst), 2);
                assert_eq!(trace.finishes.load(Ordering::SeqCst), 0);
                assert_eq!(sent_quota.usage().unwrap(), 0);
                drop(capsule);
                sent_terminal.store(true, Ordering::SeqCst);
                Ok(())
            })
        }).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), entered.notified())
        .await
        .expect("owned worker did not reach the physical write gate");
    assert!(weak.upgrade().is_some());
    assert!(quota.usage().unwrap() > 0);
    let held_root = admission.usage().unwrap();
    let spent = selection.remaining(&observer_job).unwrap();
    let source_spent = ledger.remaining_work();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(admission.usage().unwrap(), held_root);
    let mut drain = Box::pin(workers.close_and_drain());
    assert!(matches!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(drain.as_mut().poll(cx))).await,
        std::task::Poll::Pending
    ));
    assert!(quota.usage().unwrap() > 0);
    assert_eq!(admission.usage().unwrap(), held_root);
    drop(release_guard);
    tokio::time::timeout(std::time::Duration::from_secs(30), drain)
        .await
        .expect("released physical worker did not drain")
        .unwrap();
    assert!(released.load(Ordering::SeqCst));
    assert!(terminal.load(Ordering::SeqCst));
    assert!(admission.usage().unwrap().1 < held_root.1);
    assert!(weak.upgrade().is_none());
    assert_eq!(quota.usage().unwrap(), 0);
    assert_eq!(selection.remaining(&observer_job).unwrap(), spent);
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
            source_spent.semantic.pages,
            source_spent.semantic.hydrated_bytes,
            source_spent.record_attempts,
            source_spent.hydration_reads,
            source_spent.hydration_body_bytes
        )
    );
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
