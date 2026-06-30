//! The shared backend-conformance suite, **eventual-apply variant** (the atomic scenarios minus the
//! upsert ones, plus the upsert-is-unavailable assertion), run against the object-log backend. Each
//! scenario gets a fresh temp-directory object store.

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("pqueue-objlog-conf-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

pqueue_conformance::eventual_apply_suite!(
    || pqueue_objectlog::ObjectLogBackend::open(tmp_root()).expect("open object log")
);

/// ADR-012 Phase 1b-i: the SAME shared eventual-apply suite against the COMPOSED object-log backend
/// (`ComposedBackend<ObjectLog, InMemoryProjection, InProcessControlPlane>` over the production segmented
/// group-commit substrate). Passing identically to the monolith above proves the orthogonal composition is
/// faithful before the monolith is removed (Phase 2).
mod composed {
    use super::tmp_root;
    pqueue_conformance::eventual_apply_suite!(|| pqueue_objectlog::composed_objectlog_backend(
        tmp_root()
    )
    .expect("compose object log"));
}
