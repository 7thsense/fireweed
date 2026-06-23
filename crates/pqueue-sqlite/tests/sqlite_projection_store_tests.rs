//! Conformance-style tests for the SQLite `ProjectionStore`, mirroring the
//! in-memory reference backend's lifecycle semantics (TD-005 / TD-001).

use pqueue_core::{ClientItemKey, ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_sqlite::projection::SqliteProjectionStore;
use pqueue_storage::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, PushItem,
};
use pqueue_storage::traits::{ClaimRequest, ProjectionError, ProjectionStore};
use pqueue_storage::types::{CommandChecksum, CommandPosition, QueueKey, ShardId, ShardKey};
use pqueue_storage::{CommandEnvelope, CommandId, QueueCommand};

fn shard() -> ShardKey {
    ShardKey {
        tenant_id: TenantId::new("t").unwrap(),
        queue_id: QueueId::new("q").unwrap(),
        shard_id: ShardId::new(0),
    }
}

fn qk() -> QueueKey {
    QueueKey {
        tenant_id: TenantId::new("t").unwrap(),
        queue_id: QueueId::new("q").unwrap(),
    }
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn pos(seq: u64) -> CommandPosition {
    CommandPosition {
        shard_key: shard(),
        sequence: seq,
        backend_epoch: 0,
    }
}

fn envelope(command: QueueCommand) -> CommandEnvelope {
    let s = shard();
    CommandEnvelope {
        command_id: CommandId::new("c"),
        request_id: None,
        tenant_id: s.tenant_id.clone(),
        queue_id: s.queue_id.clone(),
        shard_id: s.shard_id.clone(),
        item_ids: vec![],
        command,
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

fn push(id: &str, not_before: Option<i64>) -> CommandEnvelope {
    envelope(QueueCommand::BatchPush(BatchPushCommand {
        items: vec![PushItem {
            client_item_key: ClientItemKey::new(format!("k-{id}")).unwrap(),
            item_id: ItemId::new(id).unwrap(),
            priority: None,
            not_before: not_before.map(ts),
            max_attempts: 3,
            payload: None,
        }],
    }))
}

fn claim_cmd(ids: &[&str], token: &str, expires: i64) -> CommandEnvelope {
    envelope(QueueCommand::BatchClaim(BatchClaimCommand {
        item_ids: ids.iter().map(|i| ItemId::new(*i).unwrap()).collect(),
        lease_token: token.into(),
        lease_expires_at: ts(expires),
    }))
}

fn finalize(id: &str, kind: FinalizeKind) -> CommandEnvelope {
    envelope(QueueCommand::BatchFinalize(BatchFinalizeCommand {
        outcomes: vec![FinalizeOutcome {
            item_id: ItemId::new(id).unwrap(),
            kind,
        }],
    }))
}

fn lease_expired(ids: &[&str]) -> CommandEnvelope {
    envelope(QueueCommand::LeaseExpired(LeaseExpiredCommand {
        item_ids: ids.iter().map(|i| ItemId::new(*i).unwrap()).collect(),
    }))
}

fn claim_req(max: usize, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard_key: shard(),
        max_items: max,
        now: ts(now),
        lease_token: "lease".into(),
        lease_expires_at: ts(now + 60),
    }
}

async fn apply(store: &SqliteProjectionStore, seq: u64, env: CommandEnvelope) {
    store
        .apply_committed(pos(seq), std::slice::from_ref(&env))
        .await
        .unwrap();
}

#[tokio::test]
async fn push_makes_items_pending_and_claim_leases_in_order() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", None)).await;
    apply(&store, 1, push("b", None)).await;

    let m = store.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 2);
    assert_eq!(m.leased_count, 0);

    let claimed = store.batch_claim(claim_req(1, 100)).await.unwrap();
    assert_eq!(claimed.claimed_item_ids.len(), 1);
    assert_eq!(claimed.claimed_item_ids[0].as_str(), "a"); // FIFO by insertion order

    let m = store.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 1);
    assert_eq!(m.leased_count, 1);
}

#[tokio::test]
async fn claim_skips_already_leased_items_single_active_lease() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", None)).await;
    apply(&store, 1, push("b", None)).await;

    let first = store.batch_claim(claim_req(10, 100)).await.unwrap();
    assert_eq!(first.claimed_item_ids.len(), 2);
    // A second claim finds nothing — both are leased (no double-lease).
    let second = store.batch_claim(claim_req(10, 100)).await.unwrap();
    assert!(second.claimed_item_ids.is_empty());
}

#[tokio::test]
async fn finalize_complete_and_fail_reach_terminal_states() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", None)).await;
    apply(&store, 1, push("b", None)).await;
    store.batch_claim(claim_req(10, 100)).await.unwrap();

    apply(&store, 2, finalize("a", FinalizeKind::Complete)).await;
    apply(&store, 3, finalize("b", FinalizeKind::Fail)).await;
    let m = store.metrics(&qk()).await.unwrap();
    assert_eq!(m.completed_count, 1);
    assert_eq!(m.failed_count, 1);
    assert_eq!(m.leased_count, 0);
    assert_eq!(m.pending_count, 0);
}

#[tokio::test]
async fn retry_and_release_return_items_to_pending() {
    for kind in [FinalizeKind::Retry, FinalizeKind::Release] {
        let store = SqliteProjectionStore::open_in_memory().unwrap();
        apply(&store, 0, push("a", None)).await;
        store.batch_claim(claim_req(10, 100)).await.unwrap();
        apply(&store, 1, finalize("a", kind)).await;

        let m = store.metrics(&qk()).await.unwrap();
        assert_eq!(m.pending_count, 1, "kind {kind:?} should re-enqueue");
        assert_eq!(m.leased_count, 0);
        // Re-claimable.
        let claimed = store.batch_claim(claim_req(10, 200)).await.unwrap();
        assert_eq!(claimed.claimed_item_ids.len(), 1);
    }
}

#[tokio::test]
async fn lease_expiry_returns_item_to_pending() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", None)).await;
    store.batch_claim(claim_req(10, 100)).await.unwrap();
    apply(&store, 1, lease_expired(&["a"])).await;
    let m = store.metrics(&qk()).await.unwrap();
    assert_eq!(m.pending_count, 1);
    assert_eq!(m.leased_count, 0);
}

#[tokio::test]
async fn not_before_gates_claim_until_due() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", Some(500))).await;
    // Before the not_before instant: not claimable.
    let early = store.batch_claim(claim_req(10, 100)).await.unwrap();
    assert!(early.claimed_item_ids.is_empty());
    // At/after: claimable.
    let due = store.batch_claim(claim_req(10, 500)).await.unwrap();
    assert_eq!(due.claimed_item_ids.len(), 1);
}

#[tokio::test]
async fn max_items_caps_the_claim() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        apply(&store, i as u64, push(id, None)).await;
    }
    let claimed = store.batch_claim(claim_req(2, 100)).await.unwrap();
    assert_eq!(claimed.claimed_item_ids.len(), 2);
}

#[tokio::test]
async fn unknown_queue_and_shard_report_not_found() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    assert!(matches!(
        store.metrics(&qk()).await.unwrap_err(),
        ProjectionError::QueueNotFound
    ));
    assert!(matches!(
        store.batch_claim(claim_req(1, 100)).await.unwrap_err(),
        ProjectionError::QueueNotFound
    ));
}

#[tokio::test]
async fn apply_committed_claim_command_leases_items() {
    let store = SqliteProjectionStore::open_in_memory().unwrap();
    apply(&store, 0, push("a", None)).await;
    apply(&store, 1, push("b", None)).await;
    // Replaying a BatchClaim *command* from the log leases the item — this is the
    // recovery path (lease state is reconstructed by replaying committed commands,
    // distinct from the live batch_claim call).
    apply(&store, 2, claim_cmd(&["a"], "tok", 200)).await;
    let m = store.metrics(&qk()).await.unwrap();
    assert_eq!(m.leased_count, 1);
    assert_eq!(m.pending_count, 1);
}

#[tokio::test]
async fn projection_survives_reopen() {
    let path = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("proj-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let store = SqliteProjectionStore::open(&path).unwrap();
        apply(&store, 0, push("a", None)).await;
        apply(&store, 1, push("b", None)).await;
        store.batch_claim(claim_req(1, 100)).await.unwrap();
    }
    let reopened = SqliteProjectionStore::open(&path).unwrap();
    let m = reopened.metrics(&qk()).await.unwrap();
    assert_eq!(m.leased_count, 1);
    assert_eq!(m.pending_count, 1);
    let _ = std::fs::remove_file(&path);
}
