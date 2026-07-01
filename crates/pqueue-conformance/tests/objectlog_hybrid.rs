use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_engine::{ComposedBackend, InProcessControlPlane};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-conformance-objectlog-hybrid-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn make() -> ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane> {
    let root = tmp_root();
    let projection = root.join("projection.sqlite");
    ComposedBackend::new(
        ObjectLog::open_group_commit(&root, SegmentConfig::new(1, 1).unwrap())
            .expect("open object log"),
        HybridProjectionStore::open(projection.to_str().expect("utf8 projection path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover objectlog/hybrid")
}

pqueue_conformance::eventual_apply_suite!(make);
