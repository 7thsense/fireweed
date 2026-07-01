//! Projection-only replay/apply seam for object_log_sqlite_projection.

use bytes::Bytes;
use pqueue_conformance::{item, qdef, shard, ts};
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, LeaseToken, Metadata, MetadataValue, PriorityValue,
};
use pqueue_engine::{
    AdvanceInstanceFenceCommand, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, EngineError, FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionRead,
    PushCommand, PushItem, QueueCommand, SideRecord, WriteSideRecordsCommand,
};
use pqueue_projection::InMemoryProjection;
use pqueue_sqlite::{HybridProjectionStore, SqliteProjectionStore};

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

fn rich_item(id: &str, key: &str) -> PushItem {
    let mut fields = std::collections::BTreeMap::new();
    fields.insert("color".to_string(), Bytes::from_static(b"red"));
    let mut metadata = Metadata::new();
    metadata.insert("origin", MetadataValue::String("sqlite-image".to_string()));
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: Some(PriorityValue::Int64(10)),
        not_before: Some(ts(5)),
        group_key: Some(GroupKey::new("group-a").unwrap()),
        max_attempts: 3,
        payload: Some(Bytes::from_static(b"payload")),
        fields,
        metadata,
        cohort_size: None,
        gate_keys: vec!["gate-a".to_string()],
        entity_document: Some(serde_json::json!({"kind":"job","rank":7})),
    }
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

#[tokio::test]
async fn sqlite_projection_image_exports_hydratable_recovery_image() {
    let store = SqliteProjectionStore::in_memory().unwrap();
    let definition = qdef();
    store.create_queue_projection(definition.clone()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let lease_token = LeaseToken::new("lease-1").unwrap();

    let commands = vec![
        envelope(
            "push-1",
            QueueCommand::Push(PushCommand {
                items: vec![rich_item("1", "k1")],
            }),
            vec![item_id],
            0,
        ),
        envelope(
            "claim-1",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token,
                lease_expires_at: ts(60),
            }),
            vec![item_id],
            1,
        ),
        envelope("pause-1", QueueCommand::PauseQueue, vec![], 2),
        envelope(
            "side-1",
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![SideRecord {
                    key: b"side-key".to_vec(),
                    payload: Bytes::from_static(b"side-payload"),
                }],
            }),
            vec![],
            3,
        ),
        envelope(
            "fence-1",
            QueueCommand::AdvanceInstanceFence(AdvanceInstanceFenceCommand {
                instance_key: b"instance".to_vec(),
                expected: 0,
                next: 11,
            }),
            vec![],
            4,
        ),
    ];
    let positions: Vec<CommandPosition> = (0..commands.len() as u64).map(pos).collect();
    store.apply_committed_batch(&positions, &commands).unwrap();

    let image = store.export_projection_image(&shard()).unwrap();
    assert_eq!(image.high_water, Some(pos(4)));
    assert!(image.paused);
    assert_eq!(image.metrics.leased, 1);
    assert_eq!(
        image.side_records.get(b"side-key".as_slice()),
        Some(&Bytes::from_static(b"side-payload"))
    );
    assert_eq!(image.instance_fences.get(b"instance".as_slice()), Some(&11));
    assert_eq!(image.items[0].gate_keys, vec!["gate-a"]);
    assert_eq!(
        image.items[0].entity_document,
        Some(serde_json::json!({"kind":"job","rank":7}))
    );

    let mut memory = InMemoryProjection::new();
    memory.hydrate_shard(&definition, image).unwrap();
    assert_eq!(
        pqueue_engine::ProjectionStore::metrics(&memory, &shard())
            .unwrap()
            .leased,
        1
    );
    assert_eq!(
        pqueue_engine::ProjectionStore::side_record(&memory, &shard(), b"side-key").unwrap(),
        Some(Bytes::from_static(b"side-payload"))
    );
    assert_eq!(
        pqueue_engine::ProjectionStore::instance_fence(&memory, &shard(), b"instance").unwrap(),
        Some(11)
    );
    assert!(
        pqueue_engine::ProjectionStore::select_eligible(&memory, &shard(), ts(10), 10)
            .unwrap()
            .is_empty()
    );
    let live = pqueue_engine::ProjectionStore::live_items(
        &memory,
        &shard(),
        &[ClientItemKey::new("k1").unwrap()],
    )
    .unwrap();
    assert_eq!(
        live[0].as_ref().unwrap().payload,
        Some(Bytes::from_static(b"payload"))
    );
}

#[test]
fn hybrid_projection_applies_sqlite_first_and_serves_memory_parity() {
    let mut store = HybridProjectionStore::in_memory().unwrap();
    let definition = qdef();
    pqueue_engine::ProjectionStore::ensure_shard(&mut store, &definition).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let lease_token = LeaseToken::new("lease-1").unwrap();

    let commands = vec![
        envelope(
            "push-1",
            QueueCommand::Push(PushCommand {
                items: vec![rich_item("1", "k1")],
            }),
            vec![item_id],
            0,
        ),
        envelope(
            "claim-1",
            QueueCommand::Claim(ClaimCommand {
                item_ids: vec![item_id],
                lease_token,
                lease_expires_at: ts(60),
            }),
            vec![item_id],
            1,
        ),
    ];
    let positions = vec![pos(0), pos(1)];

    pqueue_engine::ProjectionStore::apply(&mut store, &positions, &commands).unwrap();

    let sqlite_image = store.sqlite().export_projection_image(&shard()).unwrap();
    let memory_metrics = pqueue_engine::ProjectionStore::metrics(&store, &shard()).unwrap();
    assert_eq!(sqlite_image.high_water, Some(pos(1)));
    assert_eq!(sqlite_image.metrics, memory_metrics);
    assert_eq!(memory_metrics.leased, 1);
    assert!(
        pqueue_engine::ProjectionStore::select_eligible(&store, &shard(), ts(10), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        pqueue_engine::ProjectionStore::live_items(
            &store,
            &shard(),
            &[ClientItemKey::new("k1").unwrap()]
        )
        .unwrap()[0]
            .as_ref()
            .unwrap()
            .payload,
        Some(Bytes::from_static(b"payload"))
    );
}

#[test]
fn hybrid_projection_hydrates_from_sqlite_before_returning_high_water() {
    let sqlite = SqliteProjectionStore::in_memory().unwrap();
    let definition = qdef();
    sqlite.create_queue_projection(definition.clone()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let push = envelope(
        "push-1",
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "k1", 10)],
        }),
        vec![item_id],
        0,
    );
    sqlite.apply_committed_batch(&[pos(0)], &[push]).unwrap();

    let mut store = HybridProjectionStore::new(sqlite);
    assert!(
        pqueue_engine::ProjectionStore::recovery_high_water(&store, &shard()).is_err(),
        "unhydrated hybrid must not expose sqlite high-water"
    );

    pqueue_engine::ProjectionStore::ensure_shard(&mut store, &definition).unwrap();

    assert_eq!(
        pqueue_engine::ProjectionStore::recovery_high_water(&store, &shard()).unwrap(),
        Some(pos(0))
    );
    assert_eq!(
        pqueue_engine::ProjectionStore::metrics(&store, &shard())
            .unwrap()
            .pending,
        1
    );
    assert_eq!(
        pqueue_engine::ProjectionStore::select_eligible(&store, &shard(), ts(0), 10).unwrap(),
        vec![item_id]
    );
}

#[test]
fn hybrid_projection_poisoned_after_sqlite_commit_memory_apply_failure() {
    let sqlite = SqliteProjectionStore::in_memory().unwrap();
    sqlite.create_queue_projection(qdef()).unwrap();
    let mut store = HybridProjectionStore::from_parts(sqlite, InMemoryProjection::new());
    let item_id = ItemId::new("1").unwrap();
    let push = envelope(
        "push-1",
        QueueCommand::Push(PushCommand {
            items: vec![item("1", "k1", 10)],
        }),
        vec![item_id],
        0,
    );

    let err =
        pqueue_engine::ProjectionStore::apply(&mut store, &[pos(0)], &[push.clone()]).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("hybrid projection poisoned")),
        "unexpected error: {err:?}"
    );
    assert_eq!(
        store.sqlite().recovery_high_water(&shard()).unwrap(),
        Some(1)
    );

    let read_err = pqueue_engine::ProjectionStore::metrics(&store, &shard()).unwrap_err();
    assert!(matches!(read_err, EngineError::Storage(ref msg) if msg.contains("poisoned")));

    let write_err =
        pqueue_engine::ProjectionStore::apply(&mut store, &[pos(1)], &[push]).unwrap_err();
    assert!(matches!(write_err, EngineError::Storage(ref msg) if msg.contains("poisoned")));
}
