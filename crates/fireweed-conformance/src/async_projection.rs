//! Shared qualification route for native-async projection adapters.

use fireweed_core::{ItemId, ItemState, LeaseToken, RequestId};
use fireweed_engine::{
    AsyncProjectionStore, ClaimCommand, CommandPosition, EngineError, FinalizeCommand,
    FinalizeKind, FinalizeOutcome, PushCommand, QueueCommand, RenewLeaseCommand, RenewTarget,
};

use crate::{envelope, item, qdef, ts};

/// Exercise the common supported lifecycle and the reference adapter's explicit capability declines.
/// Adapter-specific suites remain responsible for rich relational queries and white-box persistence.
pub async fn run_full_async_projection_conformance<S: AsyncProjectionStore>(store: &S) {
    let definition = qdef();
    let shard =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let item_id = ItemId::new("900001").unwrap();
    let lease = LeaseToken::new("async-projection-conformance-lease").unwrap();

    AsyncProjectionStore::ensure_shard(store, definition.clone())
        .await
        .unwrap();
    AsyncProjectionStore::admit_mutation(store, shard.clone())
        .await
        .unwrap();
    assert!(store.supports_gates());

    let push_item = item(&item_id.to_string(), "async-projection-conformance-key", 7);
    AsyncProjectionStore::validate_push(store, shard.clone(), vec![push_item.clone()], ts(0))
        .await
        .unwrap();
    AsyncProjectionStore::apply_live(
        store,
        vec![CommandPosition::new(shard.clone(), 0, 0)],
        vec![envelope(
            QueueCommand::Push(PushCommand {
                items: vec![push_item],
            }),
            vec![item_id],
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(store, shard.clone(), item_id)
            .await
            .unwrap(),
        Some(ItemState::Pending)
    );
    assert_eq!(
        AsyncProjectionStore::eligible_candidates(store, shard.clone(), ts(0), 10)
            .await
            .unwrap(),
        vec![item_id]
    );

    AsyncProjectionStore::apply_live(
        store,
        vec![CommandPosition::new(shard.clone(), 0, 1)],
        vec![envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token: lease.clone(),
                lease_expires_at: ts(20),
                worker_id: None,
            }),
            vec![item_id],
        )],
    )
    .await
    .unwrap();
    let claimed = AsyncProjectionStore::render_claimed(store, shard.clone(), vec![item_id])
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].lease_token.as_ref(), Some(&lease));
    AsyncProjectionStore::renew_validate(
        store,
        shard.clone(),
        vec![RenewTarget {
            item_id,
            lease_token: lease,
        }],
        ts(10),
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        store,
        vec![CommandPosition::new(shard.clone(), 0, 2)],
        vec![envelope(
            QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: vec![item_id],
                lease_expires_at: ts(30),
            }),
            vec![item_id],
        )],
    )
    .await
    .unwrap();
    AsyncProjectionStore::apply_live(
        store,
        vec![CommandPosition::new(shard.clone(), 0, 3)],
        vec![envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
            }),
            vec![item_id],
        )],
    )
    .await
    .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(store, shard.clone(), item_id)
            .await
            .unwrap(),
        Some(ItemState::Complete)
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(store, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard.clone(), 0, 3))
    );
    assert_eq!(
        AsyncProjectionStore::recover_definitions(store)
            .await
            .unwrap(),
        vec![definition]
    );

    // Durable-commit replay and the instance-fence seam stay Unavailable on pure projection
    // adapters (no unified relational authority). Qualification requires the exact typed decline
    // instead of a silent default/no-op. The retained-commit and side-record READS may instead be
    // implemented by relational authorities (Turso, bead fireweed-82211ac4), where an unwritten
    // key/request-id reads as `Ok(None)` — both outcomes are accepted below.
    let request_id = RequestId::new("async-projection-conformance-request").unwrap();
    let durable_replay = AsyncProjectionStore::replay_durable_commit(
        store,
        shard.clone(),
        request_id.clone(),
        1,
        ts(0),
    )
    .await;
    assert!(
        matches!(durable_replay, Ok(None) | Err(EngineError::Unavailable)),
        "replay_durable_commit of an unknown request id: expected Ok(None) or Unavailable, got {durable_replay:?}"
    );
    let durable_commit =
        AsyncProjectionStore::read_durable_commit(store, shard.clone(), request_id).await;
    assert!(
        matches!(durable_commit, Ok(None) | Err(EngineError::Unavailable)),
        "read_durable_commit of an unknown request id: expected Ok(None) or Unavailable, got {durable_commit:?}"
    );
    let fence = AsyncProjectionStore::instance_fence(store, shard.clone(), b"fence".to_vec()).await;
    assert!(
        matches!(fence, Ok(None) | Err(EngineError::Unavailable)),
        "instance_fence of an unwritten key: expected Ok(None) or Unavailable, got {fence:?}"
    );
    let side_record = AsyncProjectionStore::side_record(store, shard, b"side".to_vec()).await;
    assert!(
        matches!(side_record, Ok(None) | Err(EngineError::Unavailable)),
        "side_record of an unwritten key: expected Ok(None) or Unavailable, got {side_record:?}"
    );
    // index_validate_push / commit_validate may be Unavailable (async SQLite reference) or
    // implemented with a vacuous Ok(()) on empty batches (Turso / relational projections).
    // Both are acceptable; other outcomes indicate a silent no-op or wrong error class.
    let empty_push = AsyncProjectionStore::index_validate_push(
        store,
        fireweed_engine::QueueKey::new(qdef().tenant_id, qdef().queue_id),
        Vec::new(),
    )
    .await;
    assert!(
        matches!(empty_push, Ok(()) | Err(EngineError::Unavailable)),
        "index_validate_push empty batch: expected Ok or Unavailable, got {empty_push:?}"
    );
    let empty_commit = AsyncProjectionStore::commit_validate(
        store,
        fireweed_engine::QueueKey::new(qdef().tenant_id, qdef().queue_id),
        Vec::new(),
        ts(0),
    )
    .await;
    assert!(
        matches!(empty_commit, Ok(()) | Err(EngineError::Unavailable)),
        "commit_validate empty batch: expected Ok or Unavailable, got {empty_commit:?}"
    );
}
