mod support;

use pqueue_core::{ItemId, ItemState};
use pqueue_engine::AsyncProjectionStore;

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
