use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_conformance::{qdef, shard, ts};
use fireweed_core::{PriorityValue, RequestId};
use fireweed_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, ProjectionRead, PushPort, PushSpec,
};
use fireweed_objectlog::{ObjectLog, SegmentConfig};
use fireweed_sqlite::HybridProjectionStore;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root() -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "fireweed-conformance-objectlog-hybrid-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn public_config(
    root: &std::path::Path,
    max_tail_commands: u64,
) -> fireweed::ObjectLogRuntimeConfig {
    fireweed::ObjectLogRuntimeConfig {
        object_log: fireweed::ObjectLogStorage::Local {
            root: root.join("objects"),
        },
        projection: fireweed::ProjectionConfig::Sqlite {
            path: root.join("projection.sqlite"),
        },
        response_barrier: fireweed::ResponseBarrier::Strict,
        segments: fireweed::SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: "objectlog-hybrid-conformance".into(),
        recovery: fireweed::RecoveryPolicy {
            max_tail_commands,
            ..fireweed::RecoveryPolicy::default()
        },
    }
}

fn public_item(priority: i64) -> fireweed::NewItem {
    fireweed::NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("public-{priority}").into()),
        ..fireweed::NewItem::default()
    }
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

fireweed_conformance::eventual_apply_suite!(make);

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

#[tokio::test]
async fn objectlog_hybrid_public_strict_failure_replay_and_request_id_conflict() {
    let root = tmp_root();
    let config = public_config(&root, 1_000);
    let pq =
        fireweed::open_objectlog_sqlite(config, Arc::new(fireweed_memory::ManualClock::at(1_000)))
            .unwrap();
    let queue = shard();
    pq.create_queue(qdef()).await.unwrap();
    let request = RequestId::new("public-strict-request").unwrap();

    let first = pq
        .push_with_request_id(&queue, request.clone(), public_item(10))
        .await
        .unwrap();
    let verification = pq.projection_control().unwrap().verify().await.unwrap();
    assert_eq!(
        verification.projection_sequence, verification.authoritative_sequence,
        "strict returned success is manifest-committed and SQLite-visible"
    );
    assert_eq!(
        pq.push_with_request_id(&queue, request.clone(), public_item(10))
            .await
            .unwrap(),
        first,
        "same request and body replay the original result"
    );
    assert!(matches!(
        pq.push_with_request_id(&queue, request, public_item(11))
            .await,
        Err(fireweed::EngineError::RequestIdConflict)
    ));
    assert_eq!(pq.metrics(&queue).await.unwrap().pending, 1);
    drop(pq);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn objectlog_hybrid_public_projection_behind_recovers_without_duplicates() {
    let root = tmp_root();
    let config = public_config(&root, 1_000);
    let pq =
        fireweed::open_objectlog_sqlite(config, Arc::new(fireweed_memory::ManualClock::at(1_000)))
            .unwrap();
    let queue = shard();
    pq.create_queue(qdef()).await.unwrap();
    let first = pq.push(&queue, public_item(10)).await.unwrap();
    let second = pq.push(&queue, public_item(20)).await.unwrap();
    let expected = pq.metrics(&queue).await.unwrap();

    pq.projection_control().unwrap().delete().await.unwrap();
    let rebuilt = pq.projection_control().unwrap().rebuild().await.unwrap();
    assert_eq!(rebuilt.tail_commands_replayed, 2);
    assert_eq!(pq.metrics(&queue).await.unwrap(), expected);
    assert_eq!(
        pq.peek(&queue, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    drop(pq);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn objectlog_hybrid_public_interrupted_rebuild_recovers_on_reopen() {
    let root = tmp_root();
    let limited = public_config(&root, 1);
    let pq =
        fireweed::open_objectlog_sqlite(limited, Arc::new(fireweed_memory::ManualClock::at(1_000)))
            .unwrap();
    let queue = shard();
    pq.create_queue(qdef()).await.unwrap();
    let first = pq.push(&queue, public_item(10)).await.unwrap();
    let second = pq.push(&queue, public_item(20)).await.unwrap();
    pq.projection_control().unwrap().delete().await.unwrap();
    assert!(pq.projection_control().unwrap().rebuild().await.is_err());
    drop(pq);

    let reopened = fireweed::open_objectlog_sqlite(
        public_config(&root, 1_000),
        Arc::new(fireweed_memory::ManualClock::at(2_000)),
    )
    .unwrap();
    assert_eq!(reopened.metrics(&queue).await.unwrap().pending, 2);
    assert_eq!(
        reopened
            .peek(&queue, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|view| view.item_id)
            .collect::<Vec<_>>(),
        vec![first, second],
        "restart finishes the interrupted rebuild from authoritative history exactly once"
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}
