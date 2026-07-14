use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::executor::block_on;
use pqueue_conformance::{qdef, qkey, ts};
use pqueue_engine::{
    ComposedBackend, ControlPlaneStore, InProcessControlPlane, LogRead, LogStore, PushPort,
    PushSpec, QueueKey, ReclaimDriver,
};
use pqueue_objectlog::{ObjectLog, SegmentConfig};
use pqueue_sqlite::{HybridAsyncThresholds, HybridProjectionStore};

static COUNTER: AtomicU64 = AtomicU64::new(0);

type HybridBackend = ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>;

#[derive(Clone, Copy)]
enum ProjectionMode {
    HybridStrict,
    HybridAsync,
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "pqueue-engine-deleted-manifest-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn shard() -> QueueKey {
    qkey()
}

fn clear_thresholds() -> HybridAsyncThresholds {
    HybridAsyncThresholds::new(10_000, 1_000_000_000, 1_000_000_000, 3_600_000_000, 3)
        .expect("thresholds")
}

fn make_mode(root: &Path, mode: ProjectionMode) -> HybridBackend {
    std::fs::create_dir_all(root).ok();
    let sqlite = root.join("projection.sqlite");
    let log = ObjectLog::open_group_commit(root, SegmentConfig::new(1, 1).unwrap()).expect("log");
    let hybrid = match mode {
        ProjectionMode::HybridStrict => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_strict_apply(true),
        ProjectionMode::HybridAsync => HybridProjectionStore::open(sqlite.to_str().unwrap())
            .expect("hybrid")
            .with_deferred_flush_chunk(1)
            .with_async_monitor(clear_thresholds()),
    };
    ComposedBackend::new(log, hybrid, InProcessControlPlane::new()).with_group_commit(true)
}

fn drain_projection(backend: &HybridBackend) {
    while backend.with_projection(|p| p.deferred_command_count()) > 0 {
        backend.flush_deferred_projection().expect("flush");
    }
}

async fn push(backend: &HybridBackend, key: &str, at_s: i64) {
    backend
        .push(&shard(), vec![PushSpec::default()], ts(at_s), None)
        .await
        .unwrap_or_else(|e| panic!("push {key}: {e:?}"));
}

async fn create_trimmed_backend(mode: ProjectionMode, root: &Path) -> HybridBackend {
    let backend = make_mode(root, mode).recover().expect("recover");
    backend.create_queue(qdef()).await.unwrap();
    for i in 0..3 {
        push(&backend, &format!("old-{i}"), 10).await;
    }
    drain_projection(&backend);
    push(&backend, "fresh", 10_000).await;
    drain_projection(&backend);
    backend.tick(ts(10_000)).await.unwrap();
    backend
}

#[test]
#[allow(non_snake_case)]
fn TestEngineObjectlogDeletedManifestRecovery() {
    for mode in [ProjectionMode::HybridStrict, ProjectionMode::HybridAsync] {
        let tag = match mode {
            ProjectionMode::HybridStrict => "strict",
            ProjectionMode::HybridAsync => "async",
        };
        let root = tmp_root(tag);
        let backend = block_on(create_trimmed_backend(mode, &root));
        let floor = backend
            .with_log(|log| log.retention_floor(&shard()))
            .expect("retention floor")
            .expect("trimmed floor");
        drop(backend);

        // A healthy reopen still resumes from the retained floor/head.
        let reopened = make_mode(&root, mode).recover().unwrap();
        let replay = block_on(reopened.read_from(&shard(), Some(floor), 100))
            .unwrap_or_else(|e| panic!("{tag}: read_from retained floor errored: {e:?}"));
        assert_eq!(
            replay
                .entries
                .iter()
                .map(|(p, _)| p.sequence)
                .collect::<Vec<_>>(),
            vec![3],
            "{tag}: recovery resumes at the retained floor/head without data loss"
        );
        drop(reopened);

        // A projection image behind the deleted prefix must fail closed on reopen.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(root.join(format!("projection.sqlite{suffix}")));
        }
        let err = match make_mode(&root, mode).recover() {
            Ok(_) => panic!("{tag}: recovery over a deleted manifest prefix must fail closed"),
            Err(err) => err,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("retention floor") && msg.contains("behind"),
            "{tag}: deleted manifest prefixes must fail closed, got {msg}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
#[allow(non_snake_case)]
fn TestEngineObjectlogFloorHeadReplayRecovery() {
    let root = tmp_root("floor-head");
    let backend = block_on(create_trimmed_backend(ProjectionMode::HybridStrict, &root));
    let floor = backend
        .with_log(|log| log.retention_floor(&shard()))
        .expect("retention floor")
        .expect("trimmed floor");
    drop(backend);

    let reopened = make_mode(&root, ProjectionMode::HybridStrict)
        .recover()
        .unwrap();
    let replay = block_on(reopened.read_from(&shard(), Some(floor), 100))
        .unwrap_or_else(|e| panic!("read_from retained floor errored: {e:?}"));
    assert_eq!(
        replay
            .entries
            .iter()
            .map(|(p, _)| p.sequence)
            .collect::<Vec<_>>(),
        vec![3],
        "recovery resumes at the retained floor/head and preserves the live tail"
    );
    let _ = std::fs::remove_dir_all(&root);
}
