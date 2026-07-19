use std::sync::Arc;

use pqueue_conformance::{envelope, item, qdef};
use pqueue_core::{ItemId, ItemState};
use pqueue_engine::{AsyncProjectionStore, CommandPosition, PushCommand, QueueCommand, QueueKey};
use pqueue_turso::TursoRelational;

fn push(
    shard: &QueueKey,
    id: ItemId,
    sequence: u64,
) -> (CommandPosition, pqueue_engine::CommandEnvelope) {
    (
        CommandPosition::new(shard.clone(), 0, sequence),
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item(&id.to_string(), &format!("key-{id}"), 0)],
            }),
            vec![id],
        ),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_cuts_leave_zero_or_one_replayable_outcome_and_no_waiter_loss() {
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let store = Arc::new(TursoRelational::in_memory().await.unwrap());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();

    let unpolled_id = ItemId::new("301").unwrap();
    let (position, command) = push(&shard, unpolled_id, 0);
    let unpolled = AsyncProjectionStore::apply_live(store.as_ref(), vec![position], vec![command]);
    drop(unpolled);
    assert_eq!(
        AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), unpolled_id)
            .await
            .unwrap(),
        None
    );

    let raced_id = ItemId::new("302").unwrap();
    let (position, command) = push(&shard, raced_id, 0);
    let raced_store = store.clone();
    let task = tokio::spawn(async move {
        AsyncProjectionStore::apply_live(raced_store.as_ref(), vec![position], vec![command]).await
    });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;

    let raced_state = AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), raced_id)
        .await
        .unwrap();
    assert!(matches!(raced_state, None | Some(ItemState::Pending)));
    let next_sequence = AsyncProjectionStore::recovery_high_water(store.as_ref(), shard.clone())
        .await
        .unwrap()
        .map_or(0, |position| position.sequence + 1);
    let survivor_id = ItemId::new("303").unwrap();
    let (position, command) = push(&shard, survivor_id, next_sequence);
    AsyncProjectionStore::apply_live(store.as_ref(), vec![position], vec![command])
        .await
        .unwrap();
    assert_eq!(
        AsyncProjectionStore::item_state(store.as_ref(), shard, survivor_id)
            .await
            .unwrap(),
        Some(ItemState::Pending)
    );
}
