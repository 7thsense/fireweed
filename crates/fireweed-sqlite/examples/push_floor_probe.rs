//! ad-hoc probe: file-backed vs in-memory SqliteRelationalBackend push cost
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

fn run(backend: SqliteRelationalBackend, label: &str) {
    let shard = fireweed_conformance::shard();
    bo(backend.create_queue(qdef())).expect("create_queue");

    for wave in 0..5 {
        push_batch(&backend, 2_000, wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 10_000);

    let t10 = Instant::now();
    push_batch(&backend, 2_000, 10_000);
    let ms10 = t10.elapsed().as_secs_f64() * 1000.0;

    for wave in 0..9 {
        push_batch(&backend, 2_000, 12_000 + wave * 2_000);
    }
    assert_eq!(bo(backend.metrics(&shard)).unwrap().pending, 30_000);

    let t30 = Instant::now();
    push_batch(&backend, 2_000, 30_000);
    let ms30 = t30.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "{label}: @10k {ms10:.1} ms ({:.4} ms/item); @30k {ms30:.1} ms ({:.4} ms/item)",
        ms10 / 2000.0,
        ms30 / 2000.0,
    );
}

fn main() {
    run(SqliteRelationalBackend::in_memory().expect("mem"), "in_memory");

    let path = std::env::temp_dir().join(format!("push-floor-probe-{}.sqlite", std::process::id()));
    let path_str = path.to_str().unwrap().to_owned();
    let _ = std::fs::remove_file(&path);
    run(SqliteRelationalBackend::open(&path_str).expect("file"), "file");
    let _ = std::fs::remove_file(&path);
}
