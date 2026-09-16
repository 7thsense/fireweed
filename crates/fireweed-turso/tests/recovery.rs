mod support;

use fireweed_conformance::qdef;
use fireweed_core::{ItemId, ItemState};
use fireweed_engine::{AsyncProjectionStore, CommandPosition, QueueKey};
use fireweed_turso::{TursoConfig, TursoRelational};

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
    let metrics = reopened.server_metrics(&shard).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 0, 1, 0]
    );
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
    let metrics = replayed.server_metrics(&shard).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 0, 1, 0]
    );
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
    let metrics = replayed.server_metrics(&shard).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 0, 1, 0]
    );
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
    let metrics = rebuilt.server_metrics(&shard).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 0, 1, 0]
    );
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
    let metrics = reopened.server_metrics(&shard).await.unwrap();
    assert_eq!(
        [
            metrics.pending,
            metrics.leased,
            metrics.complete,
            metrics.failed
        ],
        [0, 0, 1, 0]
    );
    assert_eq!(
        AsyncProjectionStore::recovery_high_water(&reopened, shard.clone())
            .await
            .unwrap(),
        Some(CommandPosition::new(shard, 0, 3))
    );
}

#[tokio::test]
async fn mixed_legacy_and_compact_priorities_reopen_update_and_order() {
    use bytes::Bytes;
    use fireweed_conformance::{envelope, item, ts};
    use fireweed_core::{DecimalValue, PriorityModelKind, PriorityValue};
    use fireweed_engine::{
        PayloadUpdate, PushCommand, QueueCommand, ScheduleUpdate, UpdateFieldsCommand,
    };

    for (kind, low, high) in [
        (
            PriorityModelKind::Int64,
            PriorityValue::Int64(-9),
            PriorityValue::Int64(20),
        ),
        (
            PriorityModelKind::Timestamp,
            PriorityValue::Timestamp(ts(9)),
            PriorityValue::Timestamp(ts(20)),
        ),
        (
            PriorityModelKind::Decimal,
            PriorityValue::Decimal(DecimalValue {
                mantissa: -900,
                scale: 2,
            }),
            PriorityValue::Decimal(DecimalValue {
                mantissa: 2000,
                scale: 2,
            }),
        ),
        (
            PriorityModelKind::Text,
            PriorityValue::Text("a\n🦊".into()),
            PriorityValue::Text("z".into()),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("projection.db");
        let mut definition = qdef();
        definition.priority_model.kind = kind;
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        let store = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        store.ensure_shard(definition).await.unwrap();
        let mut a = item("901", "a", 0);
        a.priority = Some(low.clone());
        let mut b = item("902", "b", 0);
        b.priority = Some(high.clone());
        let ids = vec![a.item_id, b.item_id];
        let keys = vec![a.client_item_key.clone(), b.client_item_key.clone()];
        store
            .apply_live(
                vec![CommandPosition::new(shard.clone(), 0, 0)],
                vec![envelope(
                    QueueCommand::Push(PushCommand { items: vec![a, b] }),
                    ids.clone(),
                )],
            )
            .await
            .unwrap();
        let raw = store
            .query(
                "SELECT priority FROM fireweed_items ORDER BY item_id",
                vec![],
            )
            .await
            .unwrap();
        assert!(raw.iter().all(
            |row| matches!(&row.values[0], turso::Value::Text(value) if value.starts_with('['))
        ));
        // Simulate a pre-upgrade row without touching its independently encoded sort key.
        store
            .execute(
                "UPDATE fireweed_items SET priority=?1 WHERE item_id=?2",
                vec![
                    serde_json::to_string(&low).unwrap().into(),
                    ids[0].to_string().into(),
                ],
            )
            .await
            .unwrap();
        drop(store);
        let store = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        let rows = store.server_live_items(&shard, &keys).await.unwrap();
        assert_eq!(rows[0].as_ref().unwrap().priority, Some(low.clone()));
        assert_eq!(rows[1].as_ref().unwrap().priority, Some(high.clone()));
        assert_eq!(
            store
                .eligible_candidates(shard.clone(), ts(100), 10)
                .await
                .unwrap(),
            ids
        );
        // Keep must decode legacy rows; changing and clearing must update the actual eligibility order.
        for (sequence, priority, expected, order) in [
            (1, ScheduleUpdate::Keep, Some(low.clone()), ids.clone()),
            (
                2,
                ScheduleUpdate::Set(Some(low.clone())),
                Some(low.clone()),
                ids.clone(),
            ),
            (3, ScheduleUpdate::Set(None), None, vec![ids[1], ids[0]]),
        ] {
            let command = envelope(
                QueueCommand::UpdateFields(UpdateFieldsCommand {
                    item_id: ids[0],
                    field_ops: Default::default(),
                    payload: PayloadUpdate::Set(Some(Bytes::from_static(b"enriched"))),
                    set_priority: priority,
                    set_not_before: ScheduleUpdate::Keep,
                    set_entity_document: None,
                    set_fields: None,
                    set_metadata: None,
                    set_gate_keys: None,
                    api001_batch: false,
                    client_item_key: None,
                    expected_item_version: None,
                }),
                vec![ids[0]],
            );
            store
                .apply_live(
                    vec![CommandPosition::new(shard.clone(), 0, sequence)],
                    vec![command],
                )
                .await
                .unwrap();
            let rows = store.server_live_items(&shard, &keys).await.unwrap();
            let row = rows[0].as_ref().unwrap();
            assert_eq!(row.priority, expected);
            assert_eq!(row.payload.as_deref(), Some(&b"enriched"[..]));
            assert_eq!(
                store
                    .eligible_candidates(shard.clone(), ts(100), 10)
                    .await
                    .unwrap(),
                order
            );
        }
        drop(store);
        let reopened = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        let rows = reopened.server_live_items(&shard, &keys).await.unwrap();
        assert_eq!(rows[0].as_ref().unwrap().priority, None);
        assert_eq!(rows[1].as_ref().unwrap().priority, Some(high));
    }
}
