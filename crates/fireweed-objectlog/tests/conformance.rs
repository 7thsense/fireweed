//! The shared backend-conformance suite, **eventual-apply variant** (the atomic scenarios minus the
//! upsert ones, plus the upsert-is-unavailable assertion), run against the object-log backend. Each
//! scenario gets a fresh temp-directory object store.

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("fireweed-objlog-conf-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fireweed_conformance::eventual_apply_suite!(|| fireweed_objectlog::ObjectLogBackend::open(
    tmp_root()
)
.expect("open object log"));

/// Shared commit-transition scenario (governing bead pqueue-c42136f3) against the object-log backend.
/// `ObjectLogBackend` keeps the all-false default `CommitCapabilities` (NON-SCOPE: no real
/// `CommitTransitionPort` behavior for this eventual-apply class), so the shared scenario's capability
/// check routes it through the decline path — proving objectlog's decline behavior against the SAME
/// conformance contract every other backend runs, not just the bespoke coverage in
/// `hot_projection_queries.rs`. Each invocation opens a fresh temp-directory root (no shared durable
/// state between scenario runs).
#[tokio::test]
async fn commit_transition_shared_scenario_declines_for_objectlog() {
    use fireweed_conformance::scenarios::commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen;

    commit_transition_writes_side_records_enqueues_lifecycle_finalizes_and_survives_reopen(|| {
        fireweed_objectlog::ObjectLogBackend::open(tmp_root()).expect("open object log")
    })
    .await;
}

/// ADR-012: the complete shared suite against the composed object-log backend
/// (`ComposedBackend<ObjectLog, InMemoryProjection, InProcessControlPlane>` over the production segmented
/// group-commit substrate). Its prevalidated command boundary provides the complete mutation contract even
/// though projection application follows the durable append.
mod composed {
    use super::tmp_root;
    fireweed_conformance::conformance_suite!(|| fireweed_objectlog::composed_objectlog_backend(
        tmp_root()
    )
    .expect("compose object log"));
}
