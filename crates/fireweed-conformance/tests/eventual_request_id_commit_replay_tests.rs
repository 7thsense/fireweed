use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::fault::{TxnCaps, ac_txn_3_commit_transition_request_id};
use fireweed_engine::{ComposedBackend, InProcessControlPlane};
use fireweed_objectlog::{ObjectLog, SegmentConfig};
use fireweed_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

static RUN: AtomicU64 = AtomicU64::new(0);

const DURABLE: TxnCaps = TxnCaps {
    durable_reopen: true,
};

fn root(profile: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fireweed-eventual-request-id-{profile}-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ))
}

fn assert_eventual_commit_cut_points(assertions: &[String]) {
    assert!(
        assertions
            .iter()
            .all(|assertion| !assertion.contains("capability-N/A")),
        "eventual projection application must not disable an authoritative request-id commit: {assertions:#?}"
    );
    assert!(
        assertions.iter().any(|assertion| {
            assertion.contains("commit_transition AfterAppendBeforeApply")
                && assertion.contains("request_id replay PROVEN")
        }),
        "the all-committed durable-but-unapplied cut must replay exactly: {assertions:#?}"
    );
    assert!(
        assertions.iter().any(|assertion| {
            assertion.contains("MIXED committed+rejected AfterAppendBeforeApply")
                && assertion.contains("BYTE-IDENTICAL")
        }),
        "the mixed durable-but-unapplied cut must replay its exact outcome vector: {assertions:#?}"
    );
}

#[tokio::test]
async fn objectlog_authority_replays_request_id_commit_before_projection_apply() {
    let base = root("objectlog");
    let make = move |tag: &str| {
        fireweed_objectlog::composed_objectlog_backend(base.join(tag))
            .expect("open composed object log")
    };

    let assertions = ac_txn_3_commit_transition_request_id(make, DURABLE)
        .await
        .expect("object-log request-id commit cut points");
    assert_eventual_commit_cut_points(&assertions);
}

type HybridAsyncBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

#[tokio::test]
async fn objectlog_hybrid_async_replays_request_id_commit_before_projection_checkpoint() {
    let base = root("hybrid-async");
    let make = move |tag: &str| -> HybridAsyncBackend {
        let profile = base.join(tag);
        std::fs::create_dir_all(&profile).expect("create hybrid profile root");
        let log = ObjectLog::open_group_commit(
            &profile,
            SegmentConfig::new(1, 1).expect("segment config"),
        )
        .expect("open object log");
        let projection = HybridProjectionStore::open(
            profile
                .join("projection.sqlite")
                .to_str()
                .expect("UTF-8 path"),
        )
        .expect("open hybrid projection")
        .with_deferred_flush_chunk(1_024)
        .with_async_monitor(
            HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
                .expect("hybrid async thresholds"),
        );
        ComposedBackend::new(log, projection, InProcessControlPlane::new())
            .with_group_commit(true)
            .recover()
            .expect("recover object-log/hybrid-async")
    };

    let assertions = ac_txn_3_commit_transition_request_id(make, DURABLE)
        .await
        .expect("hybrid-async request-id commit cut points");
    assert_eventual_commit_cut_points(&assertions);
}
