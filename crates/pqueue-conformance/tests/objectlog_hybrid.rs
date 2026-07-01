use std::sync::atomic::{AtomicU64, Ordering};

use pqueue_conformance::{qdef, shard, ts};
use pqueue_core::RequestId;
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, ProjectionRead, PushPort, PushSpec,
};
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

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

/// Open (and run recovery-on-open for) a hybrid backend rooted at `root` with its durable SQLite projection
/// at `sqlite`. Reopening the SAME paths exercises the recovery path: lineage validation against the log
/// identity, hydration from the durable SQLite image, and object-log tail replay beyond `sqlite_high_water`.
fn open_hybrid(root: &std::path::Path, sqlite: &std::path::Path) -> HybridBackend {
    ComposedBackend::new(
        ObjectLog::open_group_commit(root, SegmentConfig::new(1 << 20, 20).unwrap())
            .expect("open object log"),
        HybridProjectionStore::open(sqlite.to_str().expect("utf8 projection path"))
            .expect("open hybrid projection"),
        InProcessControlPlane::new(),
    )
    .with_group_commit(true)
    .recover()
    .expect("recover objectlog/hybrid")
}

pqueue_conformance::eventual_apply_suite!(make);

/// End-to-end recovery over the REAL object-log substrate (the bead's whole recovery contract on one
/// reopen): the durable SQLite lineage validates against the object log, hot memory hydrates from the
/// validated image, the retained object-log tail is replayed, and the request-id outcome is rebuilt so a
/// committed-but-unreturned push converges before serving — all WITHOUT a re-`create_queue`.
#[tokio::test]
async fn objectlog_hybrid_recovery_hydrates_replays_tail_and_rebuilds_request_id() {
    let root = tmp_root();
    let sqlite = root.join("projection.sqlite");
    let queue = shard();
    let request = RequestId::new("push-request-1").unwrap();
    let body = vec![PushSpec::default()];

    let first_ids = {
        let backend = open_hybrid(&root, &sqlite);
        backend.create_queue(qdef()).await.unwrap();
        let ids = backend
            .push_with_request_id(&queue, request.clone(), body.clone(), ts(1), None)
            .await
            .unwrap();
        assert_eq!(backend.metrics(&queue).await.unwrap().pending, 1);
        ids
    };

    // Reopen: recovery validates lineage, hydrates memory from the SQLite image, and replays the retained
    // object-log tail — the resident set is rebuilt with no re-create_queue.
    let reopened = open_hybrid(&root, &sqlite);
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "recovery rebuilt the resident set from the durable image + object-log tail"
    );

    // The request-id outcome was rebuilt before serving: the same request/body replays the original ids
    // rather than appending a second item.
    let replayed = reopened
        .push_with_request_id(&queue, request, body, ts(2), None)
        .await
        .unwrap();
    assert_eq!(
        replayed, first_ids,
        "committed request-id converges after restart"
    );
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "the replay did not append a duplicate command"
    );

    let _ = std::fs::remove_dir_all(&root);
}
