mod support;

use pqueue_conformance::qdef;
use pqueue_core::{ItemId, ItemState};
use pqueue_engine::{AsyncProjectionStore, CommandPosition, QueueKey};
use pqueue_turso::{TursoConfig, TursoRelational};

use support::{assert_state, lifecycle};

#[tokio::test]
async fn reopen_and_genesis_replay_converge_to_the_same_cursor_and_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection.db");
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let id = ItemId::new("201").unwrap();
    let commands = lifecycle(id);

    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .unwrap();
    for (sequence, command) in commands.iter().cloned().enumerate() {
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, sequence as u64)],
            vec![command],
        )
        .await
        .unwrap();
    }
    drop(store);

    let reopened = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    assert_state(&reopened, shard.clone(), id, Some(ItemState::Complete)).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&reopened, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard.clone(), 0, 3))
    );

    let replayed = TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&replayed, definition)
        .await
        .unwrap();
    AsyncProjectionStore::apply_recovery(
        &replayed,
        (0..commands.len())
            .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence as u64))
            .collect(),
        commands,
    )
    .await
    .unwrap();
    assert_state(&replayed, shard.clone(), id, Some(ItemState::Complete)).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&replayed, shard.clone())
            .await
            .unwrap(),
        AsyncProjectionStore::recovery_high_water(&reopened, shard.clone())
            .await
            .unwrap()
    );

    // An overlapping replay is idempotent and cannot advance or duplicate state.
    AsyncProjectionStore::apply_recovery(
        &replayed,
        (0..4)
            .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence))
            .collect(),
        lifecycle(id),
    )
    .await
    .unwrap();
    assert_state(&replayed, shard.clone(), id, Some(ItemState::Complete)).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&replayed, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard.clone(), 0, 3))
    );

    // A gap fails closed and leaves the cursor and rows at the last contiguous command.
    let gap = lifecycle(ItemId::new("202").unwrap()).remove(0);
    assert!(
        AsyncProjectionStore::apply_recovery(
            &replayed,
            vec![CommandPosition::new(shard.clone(), 0, 5)],
            vec![gap],
        )
        .await
        .is_err()
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&replayed, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard, 0, 3))
    );
}

#[tokio::test]
async fn local_file_loss_rebuilds_exactly_from_authoritative_history() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("lost.db");
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let id = ItemId::new("211").unwrap();
    let commands = lifecycle(id);
    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition.clone())
        .await
        .unwrap();
    drop(store);
    std::fs::remove_file(&path).unwrap();

    let rebuilt = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&rebuilt, definition)
        .await
        .unwrap();
    AsyncProjectionStore::apply_recovery(
        &rebuilt,
        (0..commands.len())
            .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence as u64))
            .collect(),
        commands,
    )
    .await
    .unwrap();
    assert_state(&rebuilt, shard.clone(), id, Some(ItemState::Complete)).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&rebuilt, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard, 0, 3))
    );
}

#[tokio::test]
async fn snapshot_tail_recovery_skips_overlap_and_applies_only_the_contiguous_tail() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot-tail.db");
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let id = ItemId::new("221").unwrap();
    let commands = lifecycle(id);

    let store = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&store, definition)
        .await
        .unwrap();
    AsyncProjectionStore::apply_live(
        &store,
        vec![
            CommandPosition::new(shard.clone(), 0, 0),
            CommandPosition::new(shard.clone(), 0, 1),
        ],
        commands[..2].to_vec(),
    )
    .await
    .unwrap();
    drop(store);

    let reopened = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();
    AsyncProjectionStore::apply_recovery(
        &reopened,
        (0..commands.len())
            .map(|sequence| CommandPosition::new(shard.clone(), 0, sequence as u64))
            .collect(),
        commands,
    )
    .await
    .unwrap();
    assert_state(&reopened, shard.clone(), id, Some(ItemState::Complete)).await;
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&reopened, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard, 0, 3))
    );
}
