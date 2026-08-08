//! fireweed-310f7a64 — push cost should not grow superlinearly with store size.
//! Measures wall time for push batches of 2_000 items near 10k and 30k corpus sizes.
//!
//! Host-physics: under concurrent CI load a single sample can noise the ratio; we take the
//! median of three independent backends so the growth bar reflects the storage path, not
//! momentary scheduler contention.

use std::time::Instant;

use fireweed_conformance::qdef;
use fireweed_core::{ClientItemKey, UtcTimestamp};
use fireweed_engine::{ControlPlaneStore, ProjectionRead, PushPort, PushSpec};
use fireweed_sqlite::SqliteRelationalBackend;

fn bo<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

fn push_batch(backend: &SqliteRelationalBackend, n: usize, offset: usize) {
    let shard = fireweed_conformance::shard();
    let specs: Vec<PushSpec> = (0..n)
        .map(|i| PushSpec {
            client_item_key: Some(ClientItemKey::new(format!("cost-{offset}-{i}")).unwrap()),
            ..Default::default()
        })
        .collect();
    bo(backend.push(&shard, specs, UtcTimestamp::new(0, 0).unwrap(), None)).expect("push");
}

/// One full corpus grow + timed 2k batches at 10k and 30k. Returns (per10_ms, per30_ms).
fn sample_push_costs(key_namespace: usize) -> (f64, f64) {
    let backend = SqliteRelationalBackend::in_memory().expect("sqlite");
    let shard = fireweed_conformance::shard();
    bo(backend.create_queue(qdef())).expect("create_queue");
    let base = key_namespace * 100_000;

    // Seed to ~10k pending items (5 × 2k).
    for wave in 0..5 {
        push_batch(&backend, 2_000, base + wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 10_000);

    let t10 = Instant::now();
    push_batch(&backend, 2_000, base + 10_000);
    let ms10 = t10.elapsed().as_secs_f64() * 1000.0;
    let per10 = ms10 / 2000.0;

    // Grow to ~30k, then time another 2k batch.
    for wave in 0..9 {
        push_batch(&backend, 2_000, base + 12_000 + wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 30_000);

    let t30 = Instant::now();
    push_batch(&backend, 2_000, base + 30_000);
    let ms30 = t30.elapsed().as_secs_f64() * 1000.0;
    let per30 = ms30 / 2000.0;

    eprintln!(
        "push_cost_scale sample[{key_namespace}]: @10k 2k-batch {ms10:.1} ms ({per10:.4} ms/item); \
         @30k 2k-batch {ms30:.1} ms ({per30:.4} ms/item); ratio {:.2}",
        per30 / per10.max(1e-9)
    );
    (per10, per30)
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

#[test]
fn push_cost_is_flat_across_corpus_sizes() {
    let mut ratios = Vec::with_capacity(3);
    let mut per10s = Vec::with_capacity(3);
    let mut per30s = Vec::with_capacity(3);
    for trial in 0..3 {
        let (per10, per30) = sample_push_costs(trial);
        per10s.push(per10);
        per30s.push(per30);
        ratios.push(per30 / per10.max(1e-9));
    }
    let med_ratio = median(ratios.clone());
    let med10 = median(per10s);
    let med30 = median(per30s);
    eprintln!(
        "push_cost_scale median: @10k {med10:.4} ms/item; @30k {med30:.4} ms/item; \
         ratio {med_ratio:.2} (samples {ratios:?})"
    );
    // Growth bar: median per-item at 30k within 1.25× of 10k (snorri saw +38% before the batch fix).
    // Median-of-3 resists concurrent-host noise that can inflate a single sample's ratio.
    assert!(
        med_ratio <= 1.25,
        "median push per-item grew {med_ratio:.2}× from {med10:.4} to {med30:.4} ms (allowed 1.25×); samples {ratios:?}"
    );
}
