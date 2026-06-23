//! Tests for the unified single-transaction `SqliteBackend` (TD-005):
//! atomic append+apply (read-after-write), reopen recovery, epoch bootstrap,
//! single-writer ownership, and the atomic `claim` path.

use pqueue_core::{
    ClientItemKey, CohortPolicy, CreateQueue, EligibilityPolicy, ItemId, OrderingMode,
    PriorityModel, QueueCreationPolicy, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp,
};
use pqueue_sqlite::{SqliteBackend, SqliteBackendError, SqliteSynchronous};
use pqueue_storage::commands::{BatchPushCommand, FinalizeKind, FinalizeOutcome, PushItem};
use pqueue_storage::types::{CommandChecksum, QueueKey, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};

fn tenant() -> TenantId {
    TenantId::new("t").unwrap()
}

fn queue() -> QueueId {
    QueueId::new("q").unwrap()
}

fn shard() -> ShardKey {
    ShardKey {
        tenant_id: tenant(),
        queue_id: queue(),
        shard_id: ShardId::new(0),
    }
}

fn qk() -> QueueKey {
    QueueKey {
        tenant_id: tenant(),
        queue_id: queue(),
    }
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn queue_def() -> QueueDefinition {
    CreateQueue {
        tenant_id: tenant(),
        queue_id: queue(),
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn push_item(id: &str) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(format!("k-{id}")).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: None,
        not_before: None,
        max_attempts: 3,
        payload: None,
    }
}

fn db_path(tag: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("backend-{tag}-{}.db", std::process::id()))
}

fn clean(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn append_and_apply_is_read_after_write() {
    let backend = SqliteBackend::open_in_memory().unwrap();
    backend.create_queue(queue_def()).await.unwrap();

    backend
        .push(&shard(), vec![push_item("a"), push_item("b")])
        .await
        .unwrap();

    // The projection already reflects the just-committed append (no apply lag).
    let m = backend.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 2);
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn claim_leases_once_and_is_single_active() {
    let backend = SqliteBackend::open_in_memory().unwrap();
    backend.create_queue(queue_def()).await.unwrap();
    backend
        .push(&shard(), vec![push_item("a"), push_item("b")])
        .await
        .unwrap();

    let claimed = backend
        .claim(&shard(), 1, "tok-1", ts(100), ts(200))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].as_str(), "a"); // FIFO

    let m = backend.metrics(&qk()).await.unwrap();
    assert_eq!(m.leased_count, 1);
    assert_eq!(m.pending_count, 1);

    // The claim wrote exactly ONE BatchClaim command (the atomic claim path does
    // not also lease via batch_claim — so attempts is incremented exactly once).
    let page = backend.read_from(&shard(), None, 100).await.unwrap();
    assert_eq!(page.commands.len(), 2, "1 push + 1 claim");

    // A second claim of the same item finds nothing (single active lease).
    let again = backend
        .claim(&shard(), 10, "tok-2", ts(100), ts(300))
        .await
        .unwrap();
    assert_eq!(again, vec![ItemId::new("b").unwrap()]);
}

#[tokio::test]
async fn epoch_bootstrap_allows_fenced_append() {
    let backend = SqliteBackend::open_in_memory().unwrap();
    backend.create_queue(queue_def()).await.unwrap();

    // Control plane assigned epoch 1; create_queue bootstrapped the log shard to
    // the same epoch, so a fenced append with expected_epoch=Some(1) succeeds
    // rather than hitting a stale-epoch (log would otherwise default to 0).
    let assignments = backend.shard_assignments(&qk()).await.unwrap();
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].epoch, 1);

    backend
        .append_and_apply(&shard(), Some(1), Vec::new())
        .await
        .expect("fenced append at the bootstrapped epoch succeeds");
}

#[tokio::test]
async fn stale_epoch_append_rolls_back_atomically() {
    let backend = SqliteBackend::open_in_memory().unwrap();
    backend.create_queue(queue_def()).await.unwrap();
    backend.push(&shard(), vec![push_item("a")]).await.unwrap();

    let before = backend.read_from(&shard(), None, 100).await.unwrap();
    assert_eq!(before.commands.len(), 1);

    // A fenced append at the wrong epoch must commit nothing (append+apply are
    // one transaction; the epoch check fails before any row is written).
    let fenced = CommandEnvelope {
        command_id: CommandId::new("fenced"),
        request_id: None,
        tenant_id: tenant(),
        queue_id: queue(),
        shard_id: ShardId::new(0),
        item_ids: vec![ItemId::new("b").unwrap()],
        command: QueueCommand::BatchPush(BatchPushCommand {
            items: vec![push_item("b")],
        }),
        checksum: CommandChecksum(0),
        created_at: ts(0),
    };
    let err = backend
        .append_and_apply(&shard(), Some(99), vec![fenced])
        .await
        .unwrap_err();
    assert!(
        matches!(err, SqliteBackendError::Log(_)),
        "stale epoch: {err}"
    );

    let after = backend.read_from(&shard(), None, 100).await.unwrap();
    assert_eq!(after.commands.len(), 1, "rolled back; no orphan log row");
    let m = backend.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 1);
}

#[tokio::test]
async fn reopen_preserves_committed_state() {
    let path = db_path("reopen");
    clean(&path);
    {
        let backend = SqliteBackend::open_durable(&path).unwrap();
        backend.create_queue(queue_def()).await.unwrap();
        backend
            .push(
                &shard(),
                vec![push_item("a"), push_item("b"), push_item("c")],
            )
            .await
            .unwrap();
        let claimed = backend
            .claim(&shard(), 2, "tok", ts(100), ts(200))
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        backend
            .finalize(
                &shard(),
                vec![FinalizeOutcome {
                    item_id: ItemId::new("a").unwrap(),
                    kind: FinalizeKind::Complete,
                }],
            )
            .await
            .unwrap();
        // backend dropped here -> file lock released
    }

    // Reopen the same file: committed state is read directly from the persisted
    // projection (no replay needed; append+apply committed atomically).
    let reopened = SqliteBackend::open_durable(&path).unwrap();
    let m = reopened.metrics(&qk()).await.unwrap();
    assert_eq!(m.completed_count, 1, "a completed");
    assert_eq!(m.leased_count, 1, "b still leased");
    assert_eq!(m.pending_count, 1, "c still pending");
    drop(reopened);
    clean(&path);
}

#[tokio::test]
async fn second_open_of_same_file_is_rejected() {
    let path = db_path("ownership");
    clean(&path);
    let _owner = SqliteBackend::open(&path, SqliteSynchronous::Full).unwrap();

    match SqliteBackend::open(&path, SqliteSynchronous::Full) {
        Err(SqliteBackendError::AlreadyOpen) => {}
        Err(other) => panic!("expected AlreadyOpen, got: {other}"),
        Ok(_) => panic!("second opener must be rejected (single-writer)"),
    }
    drop(_owner);
    clean(&path);
}
