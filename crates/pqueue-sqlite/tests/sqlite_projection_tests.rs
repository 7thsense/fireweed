//! Projection-only replay/apply seam for object_log_sqlite_projection.

use pqueue_conformance::{item, qdef, shard, ts};
use pqueue_core::{ItemId, LeaseToken};
use pqueue_engine::{
    ClaimCommand, CommandChecksum, CommandEnvelope, CommandId, CommandPosition, EngineError,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionRead, PushCommand, QueueCommand,
};
use pqueue_sqlite::SqliteProjectionStore;

fn envelope(
    id: &str,
    command: QueueCommand,
    item_ids: Vec<ItemId>,
    created_at: i64,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id),
        request_id: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(created_at),
    }
}

fn pos(sequence: u64) -> CommandPosition {
    CommandPosition::new(shard(), 0, sequence)
}

#[tokio::test]
async fn projection_replays_committed_push_claim_finalize() {
    let store = SqliteProjectionStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let lease_token = LeaseToken::new("lease-1").unwrap();

    let push = envelope(
        "push-1",
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "k1", 10)],
        }),
        vec![item_id],
        0,
    );
    store.apply_committed(&pos(0), &push).unwrap();
    assert_eq!(store.metrics(&shard()).await.unwrap().pending, 1);
    assert_eq!(
        store.select_eligible(&shard(), ts(0), 10).await.unwrap(),
        vec![item_id]
    );

    let claim = envelope(
        "claim-1",
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![item_id],
            lease_token: lease_token.clone(),
            lease_expires_at: ts(60),
        }),
        vec![item_id],
        1,
    );
    store.apply_committed(&pos(1), &claim).unwrap();
    let claimed = store
        .claimed_view(&shard(), &[item_id])
        .await
        .expect("claimed view");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease_token.as_ref(), Some(&lease_token));
    assert_eq!(store.metrics(&shard()).await.unwrap().leased, 1);

    // Duplicate replay of an already-applied position is idempotently skipped.
    store.apply_committed(&pos(1), &claim).unwrap();
    assert_eq!(store.metrics(&shard()).await.unwrap().leased, 1);

    let finalize = envelope(
        "finalize-1",
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
        }),
        vec![item_id],
        2,
    );
    store.apply_committed(&pos(2), &finalize).unwrap();
    let metrics = store.metrics(&shard()).await.unwrap();
    assert_eq!(metrics.pending, 0);
    assert_eq!(metrics.leased, 0);
    assert_eq!(metrics.complete, 1);
}

#[test]
fn projection_rejects_replay_gaps() {
    let store = SqliteProjectionStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let push = envelope(
        "push-1",
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "k1", 10)],
        }),
        vec![item_id],
        0,
    );

    let err = store.apply_committed(&pos(1), &push).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("replay gap")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn apply_committed_batch_applies_segment_in_one_transaction() {
    // The group-commit apply path: a whole sealed segment (push + claim + finalize) lands in one call.
    let store = SqliteProjectionStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let lease_token = LeaseToken::new("lease-1").unwrap();

    let envelopes = vec![
        envelope(
            "push-1",
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "k1", 10)],
            }),
            vec![item_id],
            0,
        ),
        envelope(
            "claim-1",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token: lease_token.clone(),
                lease_expires_at: ts(60),
            }),
            vec![item_id],
            1,
        ),
    ];
    let positions = vec![pos(0), pos(1)];
    store.apply_committed_batch(&positions, &envelopes).unwrap();
    assert_eq!(store.metrics(&shard()).await.unwrap().leased, 1);

    // Re-applying a batch whose positions are already absorbed is an idempotent no-op (recovery replay).
    store.apply_committed_batch(&positions, &envelopes).unwrap();
    assert_eq!(store.metrics(&shard()).await.unwrap().leased, 1);

    // A batch that starts past the cursor (a gap) is rejected, and length mismatch is a usage error.
    let finalize = envelope(
        "finalize-1",
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
        }),
        vec![item_id],
        2,
    );
    let gap = store
        .apply_committed_batch(&[pos(5)], std::slice::from_ref(&finalize))
        .unwrap_err();
    assert!(
        matches!(gap, EngineError::Storage(ref msg) if msg.contains("replay gap")),
        "unexpected gap error: {gap:?}"
    );
    let mismatch = store.apply_committed_batch(&[pos(2)], &[]).unwrap_err();
    assert!(matches!(mismatch, EngineError::Storage(_)));

    // The next contiguous batch (finalize at seq 2) applies and completes the item.
    store
        .apply_committed_batch(&[pos(2)], std::slice::from_ref(&finalize))
        .unwrap();
    let metrics = store.metrics(&shard()).await.unwrap();
    assert_eq!(metrics.leased, 0);
    assert_eq!(metrics.complete, 1);
}
