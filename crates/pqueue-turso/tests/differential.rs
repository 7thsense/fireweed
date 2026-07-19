mod support;

use pqueue_conformance::{envelope, item};
use pqueue_core::{ItemId, ItemState};
use pqueue_engine::{AsyncProjectionStore, CommandPosition, PushCommand, QueueCommand};

use support::{Pair, lifecycle};

#[tokio::test]
async fn sqlite_and_turso_lifecycle_have_zero_observable_mismatch() {
    let pair = Pair::memory().await;
    let id = ItemId::new("101").unwrap();
    let expected = [
        ItemState::Pending,
        ItemState::Leased,
        ItemState::Leased,
        ItemState::Complete,
    ];
    for (sequence, (command, state)) in lifecycle(id).into_iter().zip(expected).enumerate() {
        pair.apply(sequence as u64, command).await;
        pair.assert_items_equal(&[id]).await;
        assert_eq!(
            AsyncProjectionStore::item_state(&pair.turso, pair.shard.clone(), id)
                .await
                .unwrap(),
            Some(state)
        );
    }
    pair.sqlite.close_and_drain().await.unwrap();
}

#[tokio::test]
async fn sqlite_and_turso_rollback_the_same_conflicting_batch_without_cursor_drift() {
    let pair = Pair::memory().await;
    let ids = [ItemId::new("111").unwrap(), ItemId::new("112").unwrap()];
    let command = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![
                item("111", "duplicate-active-key", 0),
                item("112", "duplicate-active-key", 1),
            ],
        }),
        ids.to_vec(),
    );
    let position = CommandPosition::new(pair.shard.clone(), 0, 0);
    let sqlite = AsyncProjectionStore::apply_live(
        &pair.sqlite,
        vec![position.clone()],
        vec![command.clone()],
    )
    .await
    .unwrap_err();
    let turso = AsyncProjectionStore::apply_live(&pair.turso, vec![position], vec![command])
        .await
        .unwrap_err();
    assert_eq!(
        std::mem::discriminant(&turso),
        std::mem::discriminant(&sqlite),
        "SQLite and Turso must return the same structured error class"
    );
    pair.assert_items_equal(&ids).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&pair.turso, pair.shard.clone())
            .await
            .unwrap(),
        None
    );
    pair.sqlite.close_and_drain().await.unwrap();
}
