//! Engine invariant tests: durability, group-commit cost, ordering, atomicity,
//! idempotency, fault-on-commit, and retention.

use async_trait::async_trait;
use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, BudgetMode, CommitBatch, CommitOutcome, Durability, FlushConfig,
    InMemorySequencer, IndexEntry, LogEngine, MemoryBlobStore, ObjectLogError, PartitionKey,
    Sequencer,
};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

/// Flush only when `n` batches have accumulated — makes multiplexing deterministic.
fn coalesce_after(n: usize) -> FlushConfig {
    FlushConfig {
        max_bytes: usize::MAX,
        max_batches: n,
        linger: Duration::from_secs(3600),
        max_inflight_flushes: 1,
        max_buffered_bytes: usize::MAX,
        budget: object_log::BudgetConfig {
            enabled: false,
            ..object_log::BudgetConfig::default()
        },
    }
}

#[tokio::test]
async fn produce_fetch_round_trip() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let a = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"aa"),
            2,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let b = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"bbb"),
            3,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(a.base_offset, Some(0));
    assert_eq!(a.last_offset, Some(1));
    assert_eq!(b.base_offset, Some(2));
    assert_eq!(b.last_offset, Some(4));

    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].payload, "aa");
    assert_eq!(all[1].base_offset, 2);
    assert_eq!(all[1].payload, "bbb");

    // Mid-offset fetch returns only the covering batch.
    let tail = engine.fetch(&p, 2, 1 << 20).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].payload, "bbb");
}

#[tokio::test]
async fn put_count_independent_of_partition_count() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        coalesce_after(100),
        "log/",
    );
    // One flush spanning 100 partitions -> ONE object.
    for i in 0..99 {
        engine
            .produce(
                pk(&format!("t-{i}")),
                Bytes::from_static(b"x"),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine
        .produce(
            pk("t-99"),
            Bytes::from_static(b"x"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(
        blob.object_count(),
        1,
        "100 partitions multiplexed into one object"
    );
}

#[tokio::test]
async fn sequenced_implies_durable() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    assert_eq!(blob.object_count(), 0);
    let out = engine
        .produce(
            pk("t-0"),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    // The object exists by the time produce returns: PUT happened before the ack.
    assert!(out.durable && out.sequenced);
    assert_eq!(blob.object_count(), 1);
}

#[tokio::test]
async fn concurrent_producers_get_dense_contiguous_offsets() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    ));
    let p = pk("t-0");
    let mut handles = Vec::new();
    for _ in 0..50 {
        let engine = Arc::clone(&engine);
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            engine
                .produce(p, Bytes::from_static(b"r"), 1, (), Durability::Sequenced)
                .await
                .unwrap()
        }));
    }
    let mut bases = Vec::new();
    for h in handles {
        bases.push(h.await.unwrap().base_offset.unwrap());
    }
    bases.sort();
    // Dense, contiguous, no gaps or dupes.
    assert_eq!(bases, (0..50).collect::<Vec<i64>>());

    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 50);
    for (i, b) in all.iter().enumerate() {
        assert_eq!(b.base_offset, i as i64);
    }
}

/// Per-producer send-order contiguity (ADR-002 invariant residual).
///
/// Many producers share one partition; each keeps in-flight=1 and stamps
/// `(producer_id, seq)` into Meta. The engine must present batches to `commit`
/// in arrival order without splitting a partition across concurrent commits
/// (default single flush worker). Recording the Meta stream proves each
/// producer's sequences are observed contiguously 0..N-1 in send order.
#[derive(Default)]
struct RecordingSeq {
    inner: InMemorySequencer,
    /// Global commit presentation order: (producer_id, seq).
    order: Mutex<Vec<(u32, u32)>>,
}

impl Sequencer for RecordingSeq {
    type Meta = (u32, u32);

    fn commit(
        &self,
        batches: &[CommitBatch<'_, Self::Meta>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut order = self.order.lock().expect("poisoned");
        let mut out = Vec::with_capacity(batches.len());
        for b in batches {
            order.push(*b.meta);
            let clean = [CommitBatch {
                partition: b.partition.clone(),
                record_count: b.record_count,
                location: b.location.clone(),
                meta: &(),
            }];
            out.push(self.inner.commit(&clean)?.into_iter().next().unwrap());
        }
        Ok(out)
    }

    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_producer_send_order_is_contiguous_on_shared_partition() {
    const PRODUCERS: u32 = 8;
    const PER_PRODUCER: u32 = 25;

    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(RecordingSeq::default());
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        // Single-flight flush (default): partition never split across commits.
        FlushConfig {
            max_inflight_flushes: 1,
            budget: object_log::BudgetConfig {
                enabled: false,
                ..object_log::BudgetConfig::default()
            },
            ..FlushConfig::default()
        },
        "log/",
    ));
    let p = pk("shared");

    let mut handles = Vec::new();
    for producer_id in 0..PRODUCERS {
        let engine = Arc::clone(&engine);
        let p = p.clone();
        handles.push(tokio::spawn(async move {
            for seq_no in 0..PER_PRODUCER {
                // in-flight=1 for this producer: await before next produce.
                let out = engine
                    .produce(
                        p.clone(),
                        Bytes::from(format!("p{producer_id}-s{seq_no}").into_bytes()),
                        1,
                        (producer_id, seq_no),
                        Durability::Sequenced,
                    )
                    .await
                    .unwrap();
                assert!(out.sequenced);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let order = seq.order.lock().expect("poisoned").clone();
    assert_eq!(
        order.len() as u32,
        PRODUCERS * PER_PRODUCER,
        "every produce must reach commit"
    );

    // For each producer, filter the global commit stream → sequences 0..N-1 in order.
    for producer_id in 0..PRODUCERS {
        let seqs: Vec<u32> = order
            .iter()
            .filter(|(pid, _)| *pid == producer_id)
            .map(|(_, s)| *s)
            .collect();
        assert_eq!(
            seqs,
            (0..PER_PRODUCER).collect::<Vec<_>>(),
            "producer {producer_id} must see send-order contiguity in commit presentation"
        );
    }

    // Global offsets remain dense.
    assert_eq!(
        seq.high_watermark(&p).unwrap(),
        (PRODUCERS * PER_PRODUCER) as i64
    );
}

// ---- A blob store whose put always fails. ----
struct FailingPut;
#[async_trait]
impl BlobStore for FailingPut {
    async fn put(&self, _: &str, _: Bytes) -> Result<(), ObjectLogError> {
        Err(ObjectLogError::StorageUnavailable("disk on fire".into()))
    }
    async fn get(&self, _: &str) -> Result<Option<Bytes>, ObjectLogError> {
        Ok(None)
    }
    async fn get_range(&self, _: &str, _: Range<u64>) -> Result<Option<Bytes>, ObjectLogError> {
        Ok(None)
    }
    async fn list(&self, _: &str) -> Result<Vec<String>, ObjectLogError> {
        Ok(Vec::new())
    }
    async fn delete(&self, _: &str) -> Result<(), ObjectLogError> {
        Ok(())
    }
}

#[tokio::test]
async fn put_failure_yields_no_ack_no_offset() {
    let seq = Arc::new(InMemorySequencer::new());
    let engine = LogEngine::new(
        Arc::new(FailingPut),
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let err = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectLogError::StorageUnavailable(_)));
    // Nothing sequenced.
    assert_eq!(seq.high_watermark(&p).unwrap(), 0);
}

struct FailsPutOnce {
    inner: MemoryBlobStore,
    failures: AtomicUsize,
}

#[async_trait]
impl BlobStore for FailsPutOnce {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        if self.failures.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::StorageUnavailable("transient put".into()));
        }
        self.inner.put(key, value).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn transient_put_failure_is_retried_before_ack() {
    let blob = Arc::new(FailsPutOnce {
        inner: MemoryBlobStore::new(),
        failures: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let out = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(0));
    assert_eq!(blob.failures.load(Ordering::SeqCst), 2);
    assert_eq!(engine.fetch(&p, 0, 1024).await.unwrap().len(), 1);
}

struct FailsRangeOnce {
    inner: MemoryBlobStore,
    failures: AtomicUsize,
}

#[async_trait]
impl BlobStore for FailsRangeOnce {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        self.inner.put(key, value).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ObjectLogError> {
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Option<Bytes>, ObjectLogError> {
        if self.failures.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::StorageUnavailable(
                "transient get_range".into(),
            ));
        }
        self.inner.get_range(key, range).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ObjectLogError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectLogError> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn transient_range_failure_is_retried_on_fetch() {
    let blob = Arc::new(FailsRangeOnce {
        inner: MemoryBlobStore::new(),
        failures: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let out = engine.fetch(&p, 0, 1024).await.unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(blob.failures.load(Ordering::SeqCst), 2);
}

// ---- A sequencer that fails its first commit, then works (fault BETWEEN put and commit). ----
struct FlakyCommit {
    inner: InMemorySequencer,
    failed_once: AtomicUsize,
}
impl Sequencer for FlakyCommit {
    type Meta = ();
    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if self.failed_once.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ObjectLogError::Sequencer("transient".into()));
        }
        self.inner.commit(batches)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn commit_failure_orphans_object_and_retry_is_exactly_once() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(FlakyCommit {
        inner: InMemorySequencer::new(),
        failed_once: AtomicUsize::new(0),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    // First attempt: PUT succeeds (orphan object), commit fails -> Err, no offset.
    let err = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"once"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectLogError::Sequencer(_)));
    assert_eq!(seq.high_watermark(&p).unwrap(), 0, "nothing committed");
    assert_eq!(blob.object_count(), 1, "the PUT object is orphaned");

    // Retry: a fresh object id, commit succeeds -> exactly once.
    let out = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"once"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(out.base_offset, Some(0));
    assert_eq!(seq.high_watermark(&p).unwrap(), 1);
    assert_eq!(blob.object_count(), 2, "orphan + the committed object");
    let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
    assert_eq!(all.len(), 1, "exactly one record visible");
    assert_eq!(all[0].payload, "once");
}

// ---- A sequencer that rejects the whole object if any batch is poison. ----
#[derive(Default)]
struct PoisonIfAny {
    inner: InMemorySequencer,
}
impl Sequencer for PoisonIfAny {
    type Meta = bool; // true = poison
    fn commit(
        &self,
        batches: &[CommitBatch<'_, bool>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if batches.iter().any(|b| *b.meta) {
            return Err(ObjectLogError::Sequencer("poison batch".into()));
        }
        // Delegate the assignment via () batches.
        let clean: Vec<CommitBatch<'_, ()>> = batches
            .iter()
            .map(|b| CommitBatch {
                partition: b.partition.clone(),
                record_count: b.record_count,
                location: b.location.clone(),
                meta: &(),
            })
            .collect();
        self.inner.commit(&clean)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn multiplexed_commit_is_all_or_nothing() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(PoisonIfAny::default());
    let engine = Arc::new(LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        coalesce_after(2),
        "log/",
    ));
    // One object with a healthy batch (p-a) and a poison batch (p-b): the flush
    // fires only once both have accumulated (coalesce_after(2)).
    let e2 = Arc::clone(&engine);
    let healthy = tokio::spawn(async move {
        e2.produce(
            pk("p-a"),
            Bytes::from_static(b"ok"),
            1,
            false,
            Durability::Sequenced,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await; // let the healthy batch enqueue first
    let poison = engine
        .produce(
            pk("p-b"),
            Bytes::from_static(b"bad"),
            1,
            true,
            Durability::Sequenced,
        )
        .await;
    assert!(matches!(poison, Err(ObjectLogError::Sequencer(_))));
    assert!(matches!(
        healthy.await.unwrap(),
        Err(ObjectLogError::Sequencer(_))
    ));
    // Neither partition advanced — all-or-nothing.
    assert_eq!(seq.high_watermark(&pk("p-a")).unwrap(), 0);
    assert_eq!(seq.high_watermark(&pk("p-b")).unwrap(), 0);
}

// ---- A sequencer that dedups on a token in Meta. ----
#[derive(Default)]
struct DedupSeq {
    inner: InMemorySequencer,
    seen: Mutex<HashMap<u64, i64>>, // token -> base_offset
}
impl Sequencer for DedupSeq {
    type Meta = u64;
    fn commit(
        &self,
        batches: &[CommitBatch<'_, u64>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        let mut seen = self.seen.lock().unwrap();
        let mut out = Vec::with_capacity(batches.len());
        for b in batches {
            if let Some(base) = seen.get(b.meta) {
                out.push(CommitOutcome::Duplicate { base_offset: *base });
                continue;
            }
            let clean = [CommitBatch {
                partition: b.partition.clone(),
                record_count: b.record_count,
                location: b.location.clone(),
                meta: &(),
            }];
            let r = self.inner.commit(&clean)?;
            if let CommitOutcome::Assigned { base_offset, .. } = r[0] {
                seen.insert(*b.meta, base_offset);
            }
            out.push(r.into_iter().next().unwrap());
        }
        Ok(out)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn idempotent_retry_does_not_duplicate() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(DedupSeq::default());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    let first = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            7u64,
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let retry = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"v"),
            1,
            7u64,
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(first.base_offset, Some(0));
    assert_eq!(retry.base_offset, Some(0)); // duplicate -> original offset
    assert_eq!(
        seq.high_watermark(&p).unwrap(),
        1,
        "only one record committed"
    );
}

#[tokio::test]
async fn truncate_before_deletes_dead_objects() {
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("t-0");
    for _ in 0..3 {
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"r"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    }
    assert_eq!(blob.object_count(), 3);
    // Drop everything below offset 2 -> the first two single-record objects die.
    engine.truncate_before(&p, 2).await.unwrap();
    assert_eq!(blob.object_count(), 1, "two covered objects reaped");
    assert_eq!(engine.fetch(&p, 2, 1 << 20).await.unwrap().len(), 1);
}

#[tokio::test]
async fn pipeline_snapshot_exposes_budget_defaults() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let snap = engine.pipeline_snapshot();
    assert!(snap.budget_enabled);
    assert_eq!(snap.budget_mode, BudgetMode::LatencyPriority);
    assert!(snap.budget_per_sec.effective > 0.0);
    assert_eq!(snap.max_linger_ms, 50);
}

#[tokio::test]
async fn fail_closed_rejects_when_budget_starved() {
    let budget = BudgetConfig {
        mode: BudgetMode::FailClosed,
        default_capacity_per_sec: 0.0,
        budget_fraction: 0.0,
        budget_per_sec_cap: Some(0.0),
        ..BudgetConfig::default()
    };
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            budget,
            ..FlushConfig::default()
        },
        "log/",
    );
    // Force empty tokens: predicted cost > 0 and capacity 0.
    {
        // Produce once may still work if tokens bootstrapped; drain tokens by
        // setting predicted high via a produce then starve — simpler: capacity 0
        // still starts with tokens = max(1.0) in BudgetRuntime::new. So burn them.
        for _ in 0..5 {
            let _ = engine
                .produce(
                    pk("burn"),
                    Bytes::from_static(b"x"),
                    1,
                    (),
                    Durability::Sequenced,
                )
                .await;
        }
    }
    // With zero refill rate, further fail_closed admissions should eventually fail.
    let mut saw_budget = false;
    for _ in 0..20 {
        match engine
            .produce(
                pk("starve"),
                Bytes::from_static(b"y"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
        {
            Err(ObjectLogError::BudgetExceeded(_)) => {
                saw_budget = true;
                break;
            }
            Ok(_) => continue,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }
    assert!(
        saw_budget,
        "expected BudgetExceeded under fail_closed starvation"
    );
}

#[tokio::test]
async fn headroom_allows_fast_single_produce() {
    // Isolate headroom+small-queue early-flush (idle gap tested in perf_throughput).
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            budget: object_log::BudgetConfig {
                early_flush_idle: Duration::ZERO,
                ..object_log::BudgetConfig::default()
            },
            ..FlushConfig::default()
        },
        "log/",
    );
    let start = std::time::Instant::now();
    engine
        .produce(
            pk("fast"),
            Bytes::from_static(b"hi"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(40),
        "headroom early-flush should beat full linger, elapsed={:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn fetch_stream_visits_batches_in_order() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("stream-0");
    for (i, payload) in [b"a" as &[u8], b"bb", b"ccc"].into_iter().enumerate() {
        engine
            .produce(
                p.clone(),
                Bytes::copy_from_slice(payload),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        let _ = i;
    }

    let mut seen = Vec::new();
    engine
        .fetch_stream(&p, 0, |b| {
            seen.push((b.base_offset, b.payload.to_vec()));
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        seen,
        vec![
            (0, b"a".to_vec()),
            (1, b"bb".to_vec()),
            (2, b"ccc".to_vec()),
        ]
    );

    // Mid-stream start matches fetch mid-offset behavior.
    let mut mid = Vec::new();
    engine
        .fetch_stream(&p, 1, |b| {
            mid.push(b.base_offset);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(mid, vec![1, 2]);
}

#[tokio::test]
async fn fetch_stream_stops_on_visitor_error() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("stream-err");
    for _ in 0..3 {
        engine
            .produce(
                p.clone(),
                Bytes::from_static(b"x"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
    }
    let n = AtomicUsize::new(0);
    let err = engine
        .fetch_stream(&p, 0, |_| {
            if n.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(ObjectLogError::InvalidBatch("stop".into()));
            }
            Ok(())
        })
        .await;
    assert!(matches!(err, Err(ObjectLogError::InvalidBatch(_))));
    assert_eq!(n.load(Ordering::SeqCst), 2);
}

// ---- Sequencer that fails the first commit (orphan after successful PUT). ----
struct FailCommitOnce {
    inner: InMemorySequencer,
    failed: AtomicBool,
}
impl Sequencer for FailCommitOnce {
    type Meta = ();
    fn commit(
        &self,
        batches: &[CommitBatch<'_, ()>],
    ) -> Result<Vec<CommitOutcome>, ObjectLogError> {
        if !self.failed.swap(true, Ordering::SeqCst) {
            return Err(ObjectLogError::Sequencer("first commit fails".into()));
        }
        self.inner.commit(batches)
    }
    fn lookup(&self, p: &PartitionKey, o: i64) -> Result<Vec<IndexEntry>, ObjectLogError> {
        self.inner.lookup(p, o)
    }
    fn high_watermark(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.high_watermark(p)
    }
    fn log_start_offset(&self, p: &PartitionKey) -> Result<i64, ObjectLogError> {
        self.inner.log_start_offset(p)
    }
    fn truncate_before(&self, p: &PartitionKey, o: i64) -> Result<Vec<String>, ObjectLogError> {
        self.inner.truncate_before(p, o)
    }
}

#[tokio::test]
async fn reap_orphans_deletes_unreferenced_data_objects() {
    let blob = Arc::new(MemoryBlobStore::new());
    let seq = Arc::new(FailCommitOnce {
        inner: InMemorySequencer::new(),
        failed: AtomicBool::new(false),
    });
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::clone(&seq),
        FlushConfig::default(),
        "log/",
    );
    let p = pk("orphan-0");

    // First produce: PUT succeeds, commit fails → orphan object, no index entry.
    let first = engine
        .produce(
            p.clone(),
            Bytes::from_static(b"orphan"),
            1,
            (),
            Durability::Sequenced,
        )
        .await;
    assert!(first.is_err());
    assert_eq!(blob.object_count(), 1);

    // Second produce succeeds.
    engine
        .produce(
            p.clone(),
            Bytes::from_static(b"live"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    assert_eq!(blob.object_count(), 2);
    assert_eq!(engine.fetch(&p, 0, 1 << 20).await.unwrap().len(), 1);

    let live = seq.inner.live_object_ids();
    assert_eq!(live.len(), 1);
    let deleted = engine.reap_orphans(&live).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(blob.object_count(), 1);
    // Live payload still fetchable.
    assert_eq!(
        engine.fetch(&p, 0, 1 << 20).await.unwrap()[0].payload,
        "live"
    );
}

/// fireweed-481d3e43: reopening LogEngine must not reissue data-object keys under
/// the same prefix. Overwriting sealed objects while manifests still reference them
/// yields RangeOutOfBounds / mid-JSON EOF on fetch.
#[tokio::test]
async fn reopen_does_not_overwrite_sealed_data_objects() {
    use object_log::{LocalBlobStore, ManifestSequencer};

    let root = std::env::temp_dir().join(format!(
        "object-log-reopen-counter-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let payload_a = Bytes::from(vec![b'A'; 24_000]);
    let payload_b = Bytes::from(vec![b'B'; 1_000]);

    // Process 1: seal a large batch.
    {
        let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(&root));
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(
            Arc::clone(&blob),
            seq,
            FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            },
            "data/",
        );
        let p = pk("t-0");
        engine
            .produce(p.clone(), payload_a.clone(), 1, (), Durability::Sequenced)
            .await
            .unwrap();
        let got = engine.fetch(&p, 0, 1 << 20).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].payload, payload_a);
        drop(engine);
    }

    // Process 2: reopen, produce more, fetch full history including process-1 payload.
    {
        let blob: Arc<dyn BlobStore> = Arc::new(LocalBlobStore::new(&root));
        let seq = Arc::new(
            ManifestSequencer::open(Arc::clone(&blob), "manifest/")
                .await
                .unwrap(),
        );
        let engine = LogEngine::new(
            Arc::clone(&blob),
            seq,
            FlushConfig {
                linger: Duration::ZERO,
                ..FlushConfig::default()
            },
            "data/",
        );
        let p = pk("t-0");
        engine
            .produce(p.clone(), payload_b.clone(), 1, (), Durability::Sequenced)
            .await
            .unwrap();
        let all = engine.fetch(&p, 0, 1 << 20).await.unwrap();
        assert_eq!(all.len(), 2, "both generations must be readable");
        assert_eq!(all[0].payload, payload_a, "first sealed object must not be overwritten");
        assert_eq!(all[1].payload, payload_b);
        // Distinct data keys under data/ (counter advanced past recovered max).
        let data_keys: Vec<_> = blob
            .list("data/")
            .await
            .unwrap()
            .into_iter()
            .filter(|k| k.starts_with("data/"))
            .collect();
        assert!(
            data_keys.len() >= 2,
            "reopen must allocate a new object id, got {data_keys:?}"
        );
        drop(engine);
    }

    let _ = std::fs::remove_dir_all(&root);
}
