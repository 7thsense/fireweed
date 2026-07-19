use std::sync::Arc;

use pqueue_conformance::{envelope, item, qdef};
use pqueue_core::{ItemId, ItemState, QueueId};
use pqueue_engine::{AsyncProjectionStore, CommandPosition, PushCommand, QueueCommand, QueueKey};
use pqueue_turso::TursoRelational;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_disjoint_writers_survive_and_same_key_has_one_winner() {
    let store = Arc::new(TursoRelational::in_memory().await.unwrap());
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..16u32 {
        let mut definition = qdef();
        definition.queue_id = QueueId::new(format!("q-{writer}")).unwrap();
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
            .await
            .unwrap();
        let store = store.clone();
        tasks.spawn(async move {
            let id = ItemId::mint(1, 1, writer);
            AsyncProjectionStore::apply_live(
                store.as_ref(),
                vec![CommandPosition::new(shard.clone(), 0, 0)],
                vec![envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item(&id.to_string(), &format!("key-{writer}"), 0)],
                    }),
                    vec![id],
                )],
            )
            .await
            .unwrap();
            (shard, id)
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (shard, id) = result.unwrap();
        assert_eq!(
            AsyncProjectionStore::item_state(store.as_ref(), shard, id)
                .await
                .unwrap(),
            Some(ItemState::Pending)
        );
    }

    let mut definition = qdef();
    definition.queue_id = QueueId::new("conflict").unwrap();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    AsyncProjectionStore::ensure_shard(store.as_ref(), definition)
        .await
        .unwrap();
    let ids = [ItemId::mint(2, 1, 1), ItemId::mint(2, 1, 2)];
    let mut contenders = tokio::task::JoinSet::new();
    for (sequence, id) in ids.into_iter().enumerate() {
        let store = store.clone();
        let shard = shard.clone();
        contenders.spawn(async move {
            AsyncProjectionStore::apply_live(
                store.as_ref(),
                vec![CommandPosition::new(shard, 0, sequence as u64)],
                vec![envelope(
                    QueueCommand::Push(PushCommand {
                        items: vec![item(&id.to_string(), "same-active-key", 0)],
                    }),
                    vec![id],
                )],
            )
            .await
        });
    }
    let mut successes = 0;
    while let Some(result) = contenders.join_next().await {
        successes += usize::from(result.unwrap().is_ok());
    }
    assert_eq!(successes, 1);
    let mut present = 0;
    for id in ids {
        present += usize::from(
            AsyncProjectionStore::item_state(store.as_ref(), shard.clone(), id)
                .await
                .unwrap()
                .is_some(),
        );
    }
    assert_eq!(present, 1);
}
