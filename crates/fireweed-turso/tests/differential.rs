mod support;

use std::collections::BTreeMap;

use bytes::Bytes;
use fireweed_conformance::{envelope, item, qdef};
use fireweed_core::{ClientItemKey, GateKeyPolicy, ItemId, ItemState, LeaseToken};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, AsyncProjectionStore, ClaimCommand, CommandPosition,
    FenceLeaseCommand, LeaseExpiredCommand, PauseQueueCommand, PayloadUpdate, PushCommand,
    QueueCommand, ReassignLeaseCommand, ReplacePendingCommand, ScheduleUpdate, SetGatesCommand,
    SideRecord, UnfenceLeaseCommand, UpdateFieldsCommand, WriteSideRecordsCommand,
};

use support::{Pair, lifecycle};

async fn gated_pair() -> Pair {
    let mut definition = qdef();
    definition.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
    definition.eligibility_policy.max_gate_keys_per_item = Some(4);
    definition.eligibility_policy.max_gates_per_request = Some(4);
    let shard =
        fireweed_engine::QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let sqlite = fireweed_sqlite::AsyncSqliteProjectionStore::open(":memory:")
        .await
        .unwrap();
    let turso = fireweed_turso::TursoRelational::in_memory().await.unwrap();
    AsyncProjectionStore::ensure_shard(&sqlite, definition.clone())
        .await
        .unwrap();
    AsyncProjectionStore::ensure_shard(&turso, definition)
        .await
        .unwrap();
    Pair {
        sqlite,
        turso,
        shard,
    }
}

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
        pair.assert_projection_image_and_reads_equal(&[id]).await;
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
async fn generated_rich_history_has_exact_projection_image_and_read_parity() {
    let pair = gated_pair().await;
    let original = ItemId::new("121").unwrap();
    let leased = ItemId::new("122").unwrap();
    let replacement = ItemId::new("123").unwrap();
    let mut original_item = item("121", "replace-key", 1);
    original_item.gate_keys = vec!["capacity".to_string()];
    pair.apply(
        0,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![original_item, item("122", "lease-key", 2)],
            }),
            vec![original, leased],
        ),
    )
    .await;
    pair.apply(
        1,
        envelope(
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: vec!["capacity".to_string()],
                blocked: true,
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        2,
        envelope(
            QueueCommand::ReplacePending(ReplacePendingCommand {
                client_item_key: ClientItemKey::new("replace-key").unwrap(),
                superseded_item_id: original,
                replacement: item("123", "replace-key", 3),
            }),
            vec![original, replacement],
        ),
    )
    .await;
    pair.apply(
        3,
        envelope(
            QueueCommand::UpdateFields(UpdateFieldsCommand {
                item_id: replacement,
                field_ops: BTreeMap::from([(
                    "status".to_string(),
                    Some(Bytes::from_static(b"ready")),
                )]),
                payload: PayloadUpdate::Set(Some(Bytes::from_static(b"payload"))),
                set_priority: ScheduleUpdate::Keep,
                set_not_before: ScheduleUpdate::Keep,
                set_entity_document: None,
                set_fields: None,
                set_metadata: None,
                set_gate_keys: None,
                api001_batch: false,
            }),
            vec![replacement],
        ),
    )
    .await;
    pair.apply(
        4,
        envelope(
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![leased],
                lease_token: LeaseToken::new("generated-lease").unwrap(),
                lease_expires_at: fireweed_conformance::ts(20),
                worker_id: None,
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        5,
        envelope(
            QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: vec![leased],
                lease_token: LeaseToken::new("generated-reassign").unwrap(),
                lease_expires_at: fireweed_conformance::ts(30),
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        6,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        7,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        8,
        envelope(
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![leased],
            }),
            vec![leased],
        ),
    )
    .await;
    pair.apply(
        9,
        envelope(
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![SideRecord {
                    key: b"side".to_vec(),
                    payload: Bytes::from_static(b"value"),
                }],
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        10,
        envelope(
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance".to_vec(),
                expected: 0,
                next: 1,
            }),
            vec![],
        ),
    )
    .await;
    pair.apply(
        11,
        envelope(
            QueueCommand::PauseQueue(PauseQueueCommand { drain_intake: true }),
            vec![],
        ),
    )
    .await;
    pair.apply(12, envelope(QueueCommand::ResumeQueue, vec![]))
        .await;
    pair.assert_projection_image_and_reads_equal(&[original, leased, replacement])
        .await;
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
