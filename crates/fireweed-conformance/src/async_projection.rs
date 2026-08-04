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

    // These seams are also unavailable on the async SQLite reference. Qualification requires the
    // exact typed decline instead of a silent default/no-op.
    let request_id = RequestId::new("async-projection-conformance-request").unwrap();
    assert_eq!(
        AsyncProjectionStore::replay_durable_commit(
            store,
            shard.clone(),
            request_id.clone(),
            1,
            ts(0),
        )
        .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        AsyncProjectionStore::read_durable_commit(store, shard.clone(), request_id).await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        AsyncProjectionStore::instance_fence(store, shard.clone(), b"fence".to_vec()).await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        AsyncProjectionStore::side_record(store, shard, b"side".to_vec()).await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        AsyncProjectionStore::index_validate_push(
            store,
            fireweed_engine::QueueKey::new(qdef().tenant_id, qdef().queue_id),
            Vec::new(),
        )
        .await,
        Err(EngineError::Unavailable)
    );
    assert_eq!(
        AsyncProjectionStore::commit_validate(
            store,
            fireweed_engine::QueueKey::new(qdef().tenant_id, qdef().queue_id),
            Vec::new(),
            ts(0),
        )
        .await,
        Err(EngineError::Unavailable)
    );
}
