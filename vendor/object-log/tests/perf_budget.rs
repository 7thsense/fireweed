//! Performance and rate-control tests for TD-004 (group-commit + durable-ops budget).
//!
//! These are **correctness-of-performance-policy** tests: they assert floors and
//! relative improvements that must hold on a quiet CI machine, not absolute
//! hardware benches. Heavy S3 saturation lives in `examples/*_saturate.rs`.

use async_trait::async_trait;
use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, Durability, FlushConfig, InMemorySequencer, LogEngine, MediaOpStats,
    MemoryBlobStore, ObjectLogError, PartitionKey,
};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn pk(s: &str) -> PartitionKey {
    PartitionKey(s.to_string())
}

/// Memory store that reports 1 media_op per successful put (for budget tests).
struct CostlyMemory {
    inner: MemoryBlobStore,
    puts: AtomicU64,
    media_ops: AtomicU64,
    bytes: AtomicU64,
}

impl CostlyMemory {
    fn new() -> Self {
        Self {
            inner: MemoryBlobStore::new(),
            puts: AtomicU64::new(0),
            media_ops: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn put_count(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl BlobStore for CostlyMemory {
    async fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectLogError> {
        let n = value.len() as u64;
        self.inner.put(key, value).await?;
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.media_ops.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(n, Ordering::Relaxed);
        Ok(())
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

    fn take_media_op_stats(&self) -> Option<MediaOpStats> {
        Some(MediaOpStats {
            media_ops: self.media_ops.swap(0, Ordering::Relaxed),
            bytes: self.bytes.swap(0, Ordering::Relaxed),
        })
    }
}

/// Many produces must collapse to far fewer durable puts when linger allows co-buffer.
#[tokio::test]
async fn group_commit_reduces_put_count_under_linger() {
    let blob = Arc::new(CostlyMemory::new());
    let n = 100usize;
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: usize::MAX,
            max_batches: n, // one flush after all buffered if we get there first
            linger: Duration::from_millis(200),
            max_inflight_flushes: 1,
            max_buffered_bytes: usize::MAX,
            budget: BudgetConfig {
                enabled: false, // isolate group-commit sizing
                ..BudgetConfig::default()
            },
        },
        "gc/",
    );

    let start = Instant::now();
    for i in 0..n - 1 {
        engine
            .produce(
                pk("p0"),
                Bytes::from(format!("b{i}")),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine
        .produce(
            pk("p0"),
            Bytes::from_static(b"last"),
            1,
            (),
            Durability::Sequenced,
        )
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(
        blob.put_count(),
        1,
        "100 produces should be one object when max_batches=100"
    );
    // Floor: should be far faster than 100 serial 1ms sleeps; keep loose for CI.
    assert!(
        elapsed < Duration::from_secs(2),
        "group-commit path too slow: {elapsed:?}"
    );
}

/// Pipelined Buffered produces + flush: non-zero linger packs many chunks per put;
/// linger=0 ASAP path puts once per chunk.
#[tokio::test]
async fn linger_coalesces_pipelined_buffered_produces() {
    let n = 40u64;

    let asap_puts = {
        let blob = Arc::new(CostlyMemory::new());
        let engine = LogEngine::new(
            Arc::clone(&blob) as Arc<dyn BlobStore>,
            Arc::new(InMemorySequencer::new()),
            FlushConfig {
                max_bytes: usize::MAX,
                max_batches: 1, // force one object per produce
                linger: Duration::ZERO,
                max_inflight_flushes: 4,
                max_buffered_bytes: usize::MAX,
                budget: BudgetConfig {
                    enabled: false,
                    ..BudgetConfig::default()
                },
            },
            "asap/",
        );
        for i in 0..n {
            // Sequenced + max_batches=1: one durable put per produce (no pipeline).
            engine
                .produce(
                    pk("p"),
                    Bytes::from(format!("x{i}")),
                    1,
                    (),
                    Durability::Sequenced,
                )
                .await
                .unwrap();
        }
        drop(engine);
        blob.put_count()
    };

    let linger_puts = {
        let blob = Arc::new(CostlyMemory::new());
        let engine = LogEngine::new(
            Arc::clone(&blob) as Arc<dyn BlobStore>,
            Arc::new(InMemorySequencer::new()),
            FlushConfig {
                max_bytes: usize::MAX,
                max_batches: usize::MAX,
                linger: Duration::from_millis(100),
                max_inflight_flushes: 1,
                max_buffered_bytes: usize::MAX,
                budget: BudgetConfig {
                    enabled: true,
                    default_capacity_per_sec: 2.0,
                    budget_fraction: 1.0,
                    budget_per_sec_cap: Some(2.0),
                    early_flush_fill_ratio: 2.0, // never early-flush
                    ..BudgetConfig::default()
                },
            },
            "budg/",
        );
        for i in 0..n {
            engine
                .produce(
                    pk("p"),
                    Bytes::from(format!("y{i}")),
                    1,
                    (),
                    Durability::Buffered,
                )
                .await
                .unwrap();
        }
        // Single barrier: one (or few) seals for all buffered work.
        engine.flush().await.unwrap();
        drop(engine);
        blob.put_count()
    };

    assert_eq!(asap_puts, n, "max_batches=1 ASAP path: one put per produce");
    assert!(
        linger_puts <= 2,
        "linger+flush should pack into ~1 object: linger_puts={linger_puts} asap_puts={asap_puts}"
    );
}

/// Memory engine throughput floor for sequenced produces with packing.
#[tokio::test]
async fn memory_sequenced_throughput_floor() {
    let n = 2_000usize;
    let pack = 100usize;
    let blob = Arc::new(MemoryBlobStore::new());
    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: usize::MAX,
            max_batches: pack,
            linger: Duration::from_secs(5),
            max_inflight_flushes: 4,
            max_buffered_bytes: usize::MAX,
            budget: BudgetConfig {
                enabled: false,
                ..BudgetConfig::default()
            },
        },
        "thr/",
    );

    let start = Instant::now();
    for i in 0..n {
        let durability = if (i + 1) % pack == 0 {
            Durability::Sequenced
        } else {
            Durability::Buffered
        };
        engine
            .produce(pk("hot"), Bytes::from(vec![0u8; 64]), 1, (), durability)
            .await
            .unwrap();
    }
    // Drain any non-full tail without waiting the long linger.
    engine.flush().await.unwrap();
    let elapsed = start.elapsed().as_secs_f64().max(1e-6);
    let rate = n as f64 / elapsed;

    // Loose CI floor: pure memory group-commit should crush this.
    assert!(
        rate > 5_000.0,
        "sequenced throughput too low: {rate:.0} ops/s (elapsed {elapsed:.3}s)"
    );
    assert!(
        blob.object_count() <= (n / pack) + 3,
        "unexpected object amplification: {}",
        blob.object_count()
    );
}

/// Default config: idle single produce stays snappy (headroom early-flush).
#[tokio::test]
async fn default_config_idle_latency_budget() {
    let engine = LogEngine::new(
        Arc::new(MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "lat/",
    );
    let mut samples = Vec::new();
    for i in 0..10 {
        // Pause so budget tokens refill / early-flush allowed.
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let t0 = Instant::now();
        engine
            .produce(
                pk("idle"),
                Bytes::from_static(b"x"),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        samples.push(t0.elapsed());
    }
    let p50 = {
        let mut s = samples.clone();
        s.sort();
        s[s.len() / 2]
    };
    assert!(
        p50 < Duration::from_millis(30),
        "idle p50 latency {p50:?} should be << max linger (50ms) via early-flush"
    );
}
