//! Honest Local throughput harness (Rust-only, `--release`).
//!
//! | Label | What is timed |
//! |-------|----------------|
//! | **dd**   | Shell `dd conv=fdatasync` (host reference) |
//! | **B0**   | Rust stream zeros → `sync_data` |
//! | **B0d**  | B0 + parent dir `fsync` |
//! | **B1**   | Warm `LocalBlobStore::put` flat key (median of N runs; alloc outside) |
//! | **B2**   | Prebuilt zero chunks → produce + flush (split: enqueue vs flush) |
//!
//! ```text
//! OBJECT_LOG_PERF_BYTES=$((256*1024*1024)) \
//!   cargo test --release --test perf_throughput honest -- --nocapture
//! ```

use bytes::Bytes;
use object_log::{
    BlobStore, BudgetConfig, Durability, FlushConfig, InMemorySequencer, LocalBlobStore, LogEngine,
    PartitionKey,
};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn total_bytes() -> usize {
    std::env::var("OBJECT_LOG_PERF_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024)
}

fn b1_warm_runs() -> usize {
    std::env::var("OBJECT_LOG_PERF_B1_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        .max(2)
}

fn perf_root() -> (PathBuf, Option<tempfile::TempDir>) {
    if let Ok(p) = std::env::var("OBJECT_LOG_PERF_DIR") {
        let p = PathBuf::from(p);
        std::fs::create_dir_all(&p).ok();
        return (p, None);
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_path_buf();
    (path, Some(tmp))
}

fn mib_s(bytes: usize, elapsed: Duration) -> f64 {
    (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(1e-9)
}

fn zeros(n: usize) -> Vec<u8> {
    vec![0u8; n]
}

fn print_row(name: &str, total: usize, elapsed: Duration, extra: &str) {
    eprintln!(
        "  {name:<14} {:>8.1} MiB/s   {:>7.3}s   {extra}",
        mib_s(total, elapsed),
        elapsed.as_secs_f64(),
    );
}

fn baseline_dd(dir: &Path, total: usize) -> Option<(Duration, f64)> {
    let path = dir.join("dd_ref.bin");
    let bs = 8 * 1024 * 1024;
    if !total.is_multiple_of(bs) {
        return None;
    }
    let count = total / bs;
    let t0 = Instant::now();
    let status = Command::new("dd")
        .args([
            "if=/dev/zero",
            &format!("of={}", path.display()),
            &format!("bs={bs}"),
            &format!("count={count}"),
            "conv=fdatasync",
            "status=none",
        ])
        .status()
        .ok()?;
    if !status.success() {
        return None;
    }
    let elapsed = t0.elapsed();
    let _ = std::fs::remove_file(&path);
    Some((elapsed, mib_s(total, elapsed)))
}

fn baseline_b0(dir: &Path, total: usize) -> Duration {
    let path = dir.join("b0.bin");
    let chunk = 8 * 1024 * 1024;
    let buf = zeros(chunk);
    let t0 = Instant::now();
    {
        let mut f = File::create(&path).unwrap();
        let mut left = total;
        while left > 0 {
            let n = left.min(chunk);
            f.write_all(&buf[..n]).unwrap();
            left -= n;
        }
        f.flush().unwrap();
        f.sync_data().unwrap();
    }
    t0.elapsed()
}

fn baseline_b0_dir(dir: &Path, total: usize) -> Duration {
    let path = dir.join("b0d.bin");
    let chunk = 8 * 1024 * 1024;
    let buf = zeros(chunk);
    let t0 = Instant::now();
    {
        let mut f = File::create(&path).unwrap();
        let mut left = total;
        while left > 0 {
            let n = left.min(chunk);
            f.write_all(&buf[..n]).unwrap();
            left -= n;
        }
        f.flush().unwrap();
        f.sync_data().unwrap();
    }
    File::open(dir).unwrap().sync_all().unwrap();
    t0.elapsed()
}

/// Warm flat-key B1: median of `runs` puts (drop cold first). Alloc outside each timed put.
async fn baseline_b1_warm(dir: &Path, total: usize, runs: usize) -> (Duration, Vec<Duration>) {
    let store = LocalBlobStore::new(dir);
    // Warm mkdir / first put (not scored).
    let warm_payload = Bytes::from(zeros(total.min(1024 * 1024)));
    store.put("obj", warm_payload).await.unwrap();
    let _ = store.delete("obj").await;

    let payload = Bytes::from(zeros(total));
    let mut samples = Vec::with_capacity(runs);
    for i in 0..runs {
        let key = format!("obj{i}");
        // Ensure parent exists (flat keys under root).
        let t0 = Instant::now();
        store.put(&key, payload.clone()).await.unwrap();
        samples.push(t0.elapsed());
        let _ = store.delete(&key).await;
    }
    // Median
    let mut sorted = samples.clone();
    sorted.sort();
    let median = sorted[sorted.len() / 2];
    (median, samples)
}

fn next_chunk_len(seed: &mut u64, min: usize, max: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    min + (*seed as usize % (max - min + 1))
}

fn prebuild_chunks(total: usize) -> Vec<Bytes> {
    let mut seed = 0xC0FFEE_u64;
    let mut out = Vec::new();
    let mut produced = 0usize;
    while produced < total {
        let n = next_chunk_len(&mut seed, 1024, 256 * 1024).min(total - produced);
        out.push(Bytes::from(zeros(n)));
        produced += n;
    }
    out
}

fn engine_config() -> FlushConfig {
    FlushConfig {
        max_bytes: FlushConfig::default().max_bytes,
        max_batches: usize::MAX,
        linger: Duration::from_millis(50),
        // Single-flight Local put: flush thread block_on + block_in_place (no extra hops).
        max_inflight_flushes: 1,
        max_buffered_bytes: FlushConfig::default().max_buffered_bytes,
        budget: BudgetConfig {
            enabled: true,
            default_capacity_per_sec: 10_000.0,
            budget_fraction: 1.0,
            early_flush_max_queued_bytes: 4 * 1024 * 1024,
            early_flush_idle: Duration::from_millis(10),
            early_flush_fill_ratio: 0.5,
            ..BudgetConfig::default()
        },
    }
}

struct B2Timing {
    enqueue: Duration,
    flush: Duration,
    total: Duration,
    objects: u64,
    flushes: u64,
    chunks: usize,
}

async fn baseline_b2_split(dir: &Path, chunks: &[Bytes]) -> B2Timing {
    let blob = Arc::new(LocalBlobStore::new(dir));
    // Warm store root.
    blob.put("warm", Bytes::from(zeros(4096))).await.unwrap();
    let _ = blob.delete("warm").await;

    let engine = LogEngine::new(
        Arc::clone(&blob) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        engine_config(),
        "b2/",
    );
    let partition = PartitionKey("p0".into());

    let t_enq0 = Instant::now();
    for c in chunks {
        engine
            .produce(partition.clone(), c.clone(), 1, (), Durability::Buffered)
            .await
            .unwrap();
    }
    let enqueue = t_enq0.elapsed();

    let t_fl0 = Instant::now();
    engine.flush().await.unwrap();
    let flush = t_fl0.elapsed();

    let objects = blob.list("b2/").await.unwrap().len() as u64;
    let flushes = engine.pipeline_snapshot().flushes_total;
    B2Timing {
        enqueue,
        flush,
        total: enqueue + flush,
        objects,
        flushes,
        chunks: chunks.len(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn honest_local_throughput_table() {
    let total = total_bytes();
    assert!(total >= 1024 * 1024, "need at least 1 MiB");
    let (root, _guard) = perf_root();
    let run = root.join(format!("run-{}", std::process::id()));
    std::fs::create_dir_all(&run).unwrap();

    eprintln!("=== object-log local throughput (Rust --release, zeros, fair timers) ===");
    eprintln!("total={} MiB  dir={}", total / (1024 * 1024), run.display());

    if let Some((elapsed, rate)) = baseline_dd(&run, total) {
        eprintln!(
            "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (shell dd conv=fdatasync)",
            "dd",
            rate,
            elapsed.as_secs_f64()
        );
    } else {
        eprintln!("  dd             (skipped)");
    }

    let b0 = baseline_b0(&run, total);
    print_row("B0", total, b0, "(Rust stream + sync_data)");

    let b0d = baseline_b0_dir(&run, total);
    print_row("B0d", total, b0d, "(B0 + dir fsync)");

    let b1_dir = run.join("b1");
    std::fs::create_dir_all(&b1_dir).unwrap();
    let runs = b1_warm_runs();
    let (b1_med, b1_samples) = baseline_b1_warm(&b1_dir, total, runs).await;
    let b1_rates: Vec<f64> = b1_samples.iter().map(|d| mib_s(total, *d)).collect();
    eprintln!(
        "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (Local put warm median n={runs}; samples_mib/s={b1_rates:?})",
        "B1",
        mib_s(total, b1_med),
        b1_med.as_secs_f64(),
    );

    let chunks = prebuild_chunks(total);
    let b2_dir = run.join("b2");
    std::fs::create_dir_all(&b2_dir).unwrap();
    let b2 = baseline_b2_split(&b2_dir, &chunks).await;
    eprintln!(
        "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (engine chunks={} objects={} flushes={})",
        "B2",
        mib_s(total, b2.total),
        b2.total.as_secs_f64(),
        b2.chunks,
        b2.objects,
        b2.flushes,
    );
    eprintln!(
        "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (enqueue only)",
        "B2.enqueue",
        mib_s(total, b2.enqueue),
        b2.enqueue.as_secs_f64(),
    );
    eprintln!(
        "  {:<14} {:>8.1} MiB/s   {:>7.3}s   (flush only ≈ durable put path)",
        "B2.flush",
        mib_s(total, b2.flush),
        b2.flush.as_secs_f64(),
    );

    let r_b1_b0 = mib_s(total, b1_med) / mib_s(total, b0).max(0.1);
    let r_b2_b1 = mib_s(total, b2.total) / mib_s(total, b1_med).max(0.1);
    let r_b2_b0 = mib_s(total, b2.total) / mib_s(total, b0).max(0.1);
    let r_flush_b0 = mib_s(total, b2.flush) / mib_s(total, b0).max(0.1);
    // B0d includes parent-dir fsync — the fair baseline for LocalBlobStore puts.
    let r_flush_b0d = mib_s(total, b2.flush) / mib_s(total, b0d).max(0.1);
    eprintln!(
        "  ratios         B1/B0={r_b1_b0:.2}  B2/B1={r_b2_b1:.2}  B2/B0={r_b2_b0:.2}  B2.flush/B0={r_flush_b0:.2}  B2.flush/B0d={r_flush_b0d:.2}"
    );

    assert!(
        b2.objects <= 2,
        "B2 bulk should pack into ≤2 objects, got {}",
        b2.objects
    );

    // Ratio floor is a release-mode / explicit-assert gate. Debug builds often
    // miss parity because the engine path is not optimized (see TD-004).
    // Fair check: B2.flush vs B0d (both pay dir fsync). Loose B0 floor absorbs
    // noisy GHA disks when B0 (no dir fsync) spikes.
    //   cargo test --release --test perf_throughput honest -- --nocapture
    //   OBJECT_LOG_PERF_ASSERT=1 cargo test --test perf_throughput honest
    let assert_ratios = cfg!(not(debug_assertions))
        || std::env::var("OBJECT_LOG_PERF_ASSERT").ok().as_deref() == Some("1");
    if assert_ratios {
        assert!(
            r_flush_b0d >= 0.70 || r_flush_b0 >= 0.60,
            "B2.flush should be near durable baseline: B2.flush/B0d={r_flush_b0d:.2} B2.flush/B0={r_flush_b0:.2}"
        );
    } else {
        eprintln!(
            "  (skipping B2.flush ratio assert in debug; use --release or OBJECT_LOG_PERF_ASSERT=1)"
        );
    }

    let _ = std::fs::remove_dir_all(&run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_drains_buffered_produces() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LogEngine::new(
        Arc::new(LocalBlobStore::new(dir.path())) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig {
            max_bytes: usize::MAX,
            max_batches: usize::MAX,
            linger: Duration::from_secs(3600),
            max_inflight_flushes: 1,
            max_buffered_bytes: usize::MAX,
            budget: BudgetConfig {
                enabled: false,
                ..BudgetConfig::default()
            },
        },
        "f/",
    );
    for _ in 0..10 {
        engine
            .produce(
                PartitionKey("p".into()),
                Bytes::from(zeros(16)),
                1,
                (),
                Durability::Buffered,
            )
            .await
            .unwrap();
    }
    engine.flush().await.unwrap();
    assert_eq!(
        engine
            .fetch(&PartitionKey("p".into()), 0, 1 << 20)
            .await
            .unwrap()
            .len(),
        10
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_chunks_matches_put_without_premerge() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::new(dir.path());
    store
        .put_chunks(
            "k/c",
            vec![
                Bytes::from(zeros(3)),
                Bytes::from(zeros(5)),
                Bytes::from(zeros(2)),
            ],
        )
        .await
        .unwrap();
    store.put("k/s", Bytes::from(zeros(10))).await.unwrap();
    assert_eq!(
        store.get("k/c").await.unwrap().unwrap().len(),
        store.get("k/s").await.unwrap().unwrap().len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_single_produce_still_snappy() {
    // Memory: measure early-flush policy, not Local fsync latency.
    let engine = LogEngine::new(
        Arc::new(object_log::MemoryBlobStore::new()) as Arc<dyn BlobStore>,
        Arc::new(InMemorySequencer::new()),
        FlushConfig::default(),
        "idle/",
    );
    let mut samples = Vec::new();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let t0 = Instant::now();
        engine
            .produce(
                PartitionKey("i".into()),
                Bytes::from(zeros(64)),
                1,
                (),
                Durability::Sequenced,
            )
            .await
            .unwrap();
        samples.push(t0.elapsed());
    }
    samples.sort();
    let p50 = samples[samples.len() / 2];
    assert!(
        p50 < Duration::from_millis(40),
        "idle early-flush should beat full linger, p50={p50:?}"
    );
}
