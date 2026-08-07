//! fireweed-310f7a64 — push cost should not grow superlinearly with store size.
//! Measures wall time for push batches of 2_000 items near 10k and 30k corpus sizes.

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

#[test]
fn push_cost_is_flat_across_corpus_sizes() {
    let backend = SqliteRelationalBackend::in_memory().expect("sqlite");
    let shard = fireweed_conformance::shard();
    bo(backend.create_queue(qdef())).expect("create_queue");

    // Seed to ~10k pending items (5 × 2k).
    for wave in 0..5 {
        push_batch(&backend, 2_000, wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 10_000);

    let t10 = Instant::now();
    push_batch(&backend, 2_000, 10_000);
    let ms10 = t10.elapsed().as_secs_f64() * 1000.0;
    let per10 = ms10 / 2000.0;

    // Grow to ~30k, then time another 2k batch.
    for wave in 0..9 {
        push_batch(&backend, 2_000, 12_000 + wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 30_000);

    let t30 = Instant::now();
    push_batch(&backend, 2_000, 30_000);
    let ms30 = t30.elapsed().as_secs_f64() * 1000.0;
    let per30 = ms30 / 2000.0;

    eprintln!(
        "push_cost_scale: @10k corpus 2k-batch {ms10:.1} ms ({per10:.4} ms/item); \
         @30k corpus 2k-batch {ms30:.1} ms ({per30:.4} ms/item); ratio {:.2}",
        per30 / per10.max(1e-9)
    );
    // Growth bar: per-item at 30k within 1.25× of 10k (snorri saw +38% before the batch fix).
    assert!(
        per30 <= per10 * 1.25,
        "push per-item grew from {per10:.4} to {per30:.4} ms (allowed {:.4})",
        per10 * 1.25
    );
}
