//! Local filesystem-log transaction recovery checks.
//!
//! These tests exercise actual append-before-apply faults and push/claim recovery. They do not
//! emit E3 evidence, vary ineffective latency labels, or claim remote S3 qualification. Historical
//! E3 schema-v1 artifacts continue to be checked by the fireweed-release contract tests.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::{claim_req, envelope, item, qdef, qkey, shard, ts};
use fireweed_core::{ClientItemKey, ItemId, ItemState};
use fireweed_engine::{
    Backend, ClaimPort, ControlPlaneStore, LogRead, ProjectionRead, PushCommand, PushPort,
    PushSpec, QueueCommand, RawCommitFault, RawCommitRequest,
};
use fireweed_objectlog::{AsyncObjectLogMemoryBackend, flush_config_from_segment};

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let ordinal = NEXT.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fireweed-local-recovery-{label}-{}-{ordinal}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn open_memory_product(root: &Path) -> AsyncObjectLogMemoryBackend {
    AsyncObjectLogMemoryBackend::open_local(root, flush_config_from_segment(256 * 1024, 50))
        .await
        .expect("open local filesystem-log memory projection")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_before_apply_fault_retains_log_and_recovers_exact_item() {
    let root = TestRoot::new("append-before-apply");
    let backend = open_memory_product(&root.0).await;
    backend.create_queue(qdef()).await.unwrap();
    let epoch = backend.current_epoch(&shard()).await.unwrap();
    let expected = item("700000004", "fault-recovery", 1);
    let command = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![expected.clone()],
        }),
        vec![expected.item_id],
    );
    let result = backend
        .commit_raw(
            RawCommitRequest::new(shard(), vec![command], epoch)
                .with_fault(RawCommitFault::AfterAppendBeforeApply),
        )
        .await;
    assert!(
        result.is_err() || result.is_ok_and(|outcome| !outcome.projection_applied()),
        "fault must withhold fully applied success"
    );
    let durable = backend.read_from(&shard(), None, 16).await.unwrap();
    assert_eq!(durable.entries.len(), 1, "the append remains authoritative");
    drop(backend);

    let recovered = open_memory_product(&root.0).await;
    let metrics = recovered.metrics(&qkey()).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [1, 0, 0, 0],
        "recovery must rebuild exactly the committed pending item"
    );
    let views = recovered
        .live_items(&qkey(), &[ClientItemKey::new("fault-recovery").unwrap()])
        .await
        .unwrap();
    let view = views[0].as_ref().expect("recovered named item");
    assert_eq!(view.item_id, ItemId::from_u64(700000004));
    assert_eq!(view.priority, expected.priority);
    assert_eq!(view.lifecycle_state, ItemState::Pending);
    assert_eq!(view.item_version, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_push_and_claim_survive_reopen() {
    let root = TestRoot::new("push-claim");
    let backend = open_memory_product(&root.0).await;
    backend.create_queue(qdef()).await.unwrap();
    let ids = backend
        .push(
            &shard(),
            vec![PushSpec {
                client_item_key: Some(ClientItemKey::new("claimed-work").unwrap()),
                payload: Some(bytes::Bytes::from_static(b"x")),
                ..PushSpec::default()
            }],
            ts(1),
            None,
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);
    let claimed = backend.claim(claim_req(1, 500, 10)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].item_id, ids[0]);
    drop(backend);

    let reopened = open_memory_product(&root.0).await;
    let metrics = reopened.metrics(&qkey()).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 1, 0, 0]
    );
    let views = reopened
        .live_items(&qkey(), &[ClientItemKey::new("claimed-work").unwrap()])
        .await
        .unwrap();
    let view = views[0].as_ref().expect("recovered claimed item");
    assert_eq!(view.item_id, ids[0]);
    assert_eq!(view.lifecycle_state, ItemState::Leased);
    assert_eq!(view.payload.as_deref(), Some(b"x".as_slice()));
}
