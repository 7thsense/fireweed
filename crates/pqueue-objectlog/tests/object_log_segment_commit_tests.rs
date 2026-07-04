use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use pqueue_conformance::{envelope, item};
use pqueue_core::{
    EligibilityPolicy, ItemId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::{ControlPlaneStore, EngineError, ProjectionRead, PushCommand, QueueCommand};
use pqueue_objectlog::{LocalObjectLog, ObjectLogBackend, ObjectLogSegmentConfig};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!(
        "pqueue-objlog-seg-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn shard() -> pqueue_engine::QueueKey {
    pqueue_engine::QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant").unwrap(),
        queue_id: QueueId::new("queue").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: pqueue_core::OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 10 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn push_env(id: &str) -> pqueue_engine::CommandEnvelope {
    envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item(id, &format!("k{id}"), 1)],
        }),
        vec![ItemId::new(id).unwrap()],
    )
}

fn log_dir(root: &std::path::Path) -> std::path::PathBuf {
    std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .join("log")
}

#[tokio::test]
async fn segmented_commands_wait_for_manifest_commit() {
    let root = tmp_root("commit");
    let store = LocalObjectLog::open_with_config(
        &root,
        ObjectLogSegmentConfig {
            segment_max_commands: 2,
            segment_max_bytes: 0,
            segment_max_latency_ms: 10,
        },
    )
    .expect("open");
    store.create_queue(qdef()).unwrap();
    let shard = shard();

    let positions = store
        .append(&shard, &[push_env("1"), push_env("2"), push_env("3")], 0)
        .expect("append");
    assert_eq!(
        positions.iter().map(|p| p.sequence).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    let files = std::fs::read_dir(log_dir(&root)).unwrap().count();
    assert_eq!(
        files, 2,
        "two durable segment objects for three commands at max=2"
    );

    let reopened = ObjectLogBackend::open(&root).expect("reopen");
    assert_eq!(
        reopened.metrics(&shard).await.unwrap().pending,
        3,
        "reopen must rebuild the committed segment contents"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn segment_manifest_cas_fences_concurrent_writers() {
    let root = tmp_root("fence");
    let store = Arc::new(
        LocalObjectLog::open_with_config(&root, ObjectLogSegmentConfig::default()).expect("open"),
    );
    store.create_queue(qdef()).unwrap();
    let shard = shard();

    assert_eq!(store.acquire_epoch(&shard).unwrap(), 1);

    let barrier = Arc::new(Barrier::new(2));
    let stale_store = Arc::clone(&store);
    let stale_barrier = Arc::clone(&barrier);
    let stale_shard = shard.clone();
    let stale = thread::spawn(move || {
        stale_barrier.wait();
        stale_store
            .append(&stale_shard, &[push_env("1")], 0)
            .map(|_| ())
    });

    let current_store = Arc::clone(&store);
    let current_barrier = Arc::clone(&barrier);
    let current_shard = shard.clone();
    let current = thread::spawn(move || {
        current_barrier.wait();
        current_store
            .append(&current_shard, &[push_env("2")], 1)
            .map(|_| ())
    });

    let stale_res = stale.join().expect("stale writer thread");
    let current_res = current.join().expect("current writer thread");

    assert_eq!(stale_res.unwrap_err(), EngineError::EpochFenced);
    current_res.expect("current writer should commit");

    let files = std::fs::read_dir(log_dir(&root)).unwrap().count();
    assert_eq!(
        files, 1,
        "only the fenced-in writer should commit a segment"
    );

    let reopened = ObjectLogBackend::open(&root).expect("reopen");
    assert_eq!(reopened.current_epoch(&shard).await.unwrap(), 1);
    assert_eq!(
        reopened.metrics(&shard).await.unwrap().pending,
        1,
        "only one committed command should be recovered"
    );

    let _ = std::fs::remove_dir_all(&root);
}
