//! Projection-only replay/apply seam for object_log_sqlite_projection.

use std::sync::Arc;

use bytes::Bytes;
use fireweed_conformance::{item, qdef, shard, ts};
use fireweed_core::{
    ClientItemKey, GroupKey, IndexSpec, ItemId, LeaseToken, Metadata, MetadataValue, PriorityValue,
    QueueDefinition, QueueId, WorkerId,
};
use fireweed_engine::{
    AdvanceInstanceFenceCommand, ClaimCommand, CommandChecksum, CommandEnvelope, CommandId,
    CommandPosition, EngineError, FinalizeCommand, FinalizeKind, FinalizeOutcome,
    LeaseExpiredCommand, PauseQueueCommand, ProjectionRead, PushCommand, PushItem, QueueCommand,
    QueueKey, SideRecord, WriteSideRecordsCommand,
};
use fireweed_projection::InMemoryProjection;
use fireweed_sqlite::{
    AsyncProjectionFaultCutPoint, AsyncProjectionFaultHook, AsyncProjectionThresholds,
    BackpressureLevel, LegacySqliteProjectionStore, SqliteProjectionStore,
};
use rusqlite::Connection;

fn envelope(
    id: &str,
    command: QueueCommand,
    item_ids: Vec<ItemId>,
    created_at: i64,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id),
        request_id: None,
        request_fingerprint: None,
        request_outcome: None,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(created_at),
    }
}

fn pos(sequence: u64) -> CommandPosition {
    CommandPosition::new(shard(), 0, sequence)
}

fn pos_for(queue: &QueueKey, sequence: u64) -> CommandPosition {
    CommandPosition::new(queue.clone(), 0, sequence)
}

fn run_bounded_queue_workers(
    store: &Arc<SqliteProjectionStore>,
    queues: &[QueueKey],
    workers: usize,
    operation: impl Fn(&SqliteProjectionStore, usize, &QueueKey) + Sync,
) {
    let chunk_size = queues.len().div_ceil(workers.max(1));
    std::thread::scope(|scope| {
        for (chunk_index, chunk) in queues.chunks(chunk_size.max(1)).enumerate() {
            let store = Arc::clone(store);
            let operation = &operation;
            scope.spawn(move || {
                for (offset, queue) in chunk.iter().enumerate() {
                    operation(&store, chunk_index * chunk_size + offset, queue);
                }
            });
        }
    });
}

fn named_qdef(name: &str) -> QueueDefinition {
    let mut definition = qdef();
    definition.queue_id = QueueId::new(name).unwrap();
    definition
}

fn named_shard(name: &str) -> QueueKey {
    let definition = named_qdef(name);
    QueueKey::new(definition.tenant_id, definition.queue_id)
}

fn named_pos(name: &str, sequence: u64) -> CommandPosition {
    CommandPosition::new(named_shard(name), 0, sequence)
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

fn indexed_qdef() -> QueueDefinition {
    let mut definition = qdef();
    definition.secondary_indexes = vec![
        IndexSpec {
            name: "by_color".to_string(),
            fields: vec!["color".to_string()],
            unique: false,
        },
        IndexSpec {
            name: "uniq_origin".to_string(),
            fields: vec!["origin_field".to_string()],
            unique: true,
        },
    ];
    definition
}

fn temp_projection_path(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "fireweed-sqlite-{tag}-{}-projection.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
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
            worker_id: None,
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

#[tokio::test]
async fn same_queue_reseed_revisit_remains_claim_visible() {
    let store = SqliteProjectionStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    for cycle in 0..2_u64 {
        let item_id = ItemId::new((cycle + 1).to_string()).unwrap();
        let sequence = cycle * 3;
        store
            .apply_committed(
                &pos(sequence),
                &envelope(
                    &format!("reseed-push-{cycle}"),
                    QueueCommand::Push(PushCommand {
                        items: vec![item(
                            &(cycle + 1).to_string(),
                            &format!("reseed-key-{cycle}"),
                            10,
                        )],
                    }),
                    vec![item_id],
                    sequence as i64,
                ),
            )
            .unwrap();
        let token = LeaseToken::new(format!("reseed-lease-{cycle}")).unwrap();
        store
            .apply_committed(
                &pos(sequence + 1),
                &envelope(
                    &format!("reseed-claim-{cycle}"),
                    QueueCommand::Claim(ClaimCommand {
                        item_ids: vec![item_id],
                        lease_token: token.clone(),
                        lease_expires_at: ts(60),
                        worker_id: Some(WorkerId::new("reseed-worker").unwrap()),
                    }),
                    vec![item_id],
                    sequence as i64 + 1,
                ),
            )
            .unwrap();
        let claimed = store.claimed_view(&shard(), &[item_id]).await.unwrap();
        assert_eq!(claimed.len(), 1, "cycle {cycle} claim disappeared");
        assert_eq!(claimed[0].lease_token.as_ref(), Some(&token));
        store
            .apply_committed(
                &pos(sequence + 2),
                &envelope(
                    &format!("reseed-finalize-{cycle}"),
                    QueueCommand::Finalize(FinalizeCommand {
                        outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
                    }),
                    vec![item_id],
                    sequence as i64 + 2,
                ),
            )
            .unwrap();
    }
}

#[tokio::test]
async fn concurrent_many_queue_reseeds_remain_visible_with_bounded_workers_and_exact_reopen() {
    const QUEUES: usize = 64;
    const WORKERS: usize = 8;
    const CYCLES: u64 = 4;

    let path = temp_projection_path("many-queue-reseed");
    let store = Arc::new(SqliteProjectionStore::open(path.to_str().unwrap()).unwrap());
    let mut queues = Vec::with_capacity(QUEUES);
    for index in 0..QUEUES {
        let definition = named_qdef(&format!("reseed-q-{index}"));
        let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        store.create_queue_projection(definition).unwrap();
        queues.push(queue);
    }

    for cycle in 0..CYCLES {
        let item_id = ItemId::new((cycle + 1).to_string()).unwrap();
        let sequence = cycle * 3;
        run_bounded_queue_workers(&store, &queues, WORKERS, |_, index, queue| {
            let push = envelope(
                &format!("many-push-{cycle}-{index}"),
                QueueCommand::Push(PushCommand {
                    items: vec![item(
                        &(cycle + 1).to_string(),
                        &format!("many-key-{cycle}-{index}"),
                        10,
                    )],
                }),
                vec![item_id],
                sequence as i64,
            );
            store
                .apply_committed(&pos_for(queue, sequence), &push)
                .unwrap();
        });
        run_bounded_queue_workers(&store, &queues, WORKERS, |_, index, queue| {
            let claim = envelope(
                &format!("many-claim-{cycle}-{index}"),
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![item_id],
                    lease_token: LeaseToken::new(format!("many-lease-{cycle}-{index}")).unwrap(),
                    lease_expires_at: ts(60),
                    worker_id: Some(WorkerId::new(format!("many-worker-{index}")).unwrap()),
                }),
                vec![item_id],
                sequence as i64 + 1,
            );
            store
                .apply_committed(&pos_for(queue, sequence + 1), &claim)
                .unwrap();
        });

        // Finalizing half the queues clears colliding queue-local item ids. Before the fix these clears
        // erased the other queues' tokens from the process-global ItemId map, producing empty claims after
        // successful reseeds. Keep the worker pool fixed while the active queue count is eight times larger.
        let even_queues: Vec<_> = queues.iter().step_by(2).cloned().collect();
        run_bounded_queue_workers(&store, &even_queues, WORKERS, |_, _, queue| {
            let finalize = envelope(
                &format!("many-finalize-even-{cycle}-{}", queue.queue_id),
                QueueCommand::Finalize(FinalizeCommand {
                    outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
                }),
                vec![item_id],
                sequence as i64 + 2,
            );
            store
                .apply_committed(&pos_for(queue, sequence + 2), &finalize)
                .unwrap();
        });
        for (index, queue) in queues
            .iter()
            .enumerate()
            .filter(|(index, _)| index % 2 == 1)
        {
            let claimed = store.claimed_view(queue, &[item_id]).await.unwrap();
            assert_eq!(
                claimed.len(),
                1,
                "cycle {cycle} queue {index} lost claim visibility under concurrent finalization"
            );
            assert_eq!(
                claimed[0].lease_token.as_ref(),
                Some(&LeaseToken::new(format!("many-lease-{cycle}-{index}")).unwrap())
            );
        }
        let odd_queues: Vec<_> = queues.iter().skip(1).step_by(2).cloned().collect();
        run_bounded_queue_workers(&store, &odd_queues, WORKERS, |_, _, queue| {
            let finalize = envelope(
                &format!("many-finalize-odd-{cycle}-{}", queue.queue_id),
                QueueCommand::Finalize(FinalizeCommand {
                    outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
                }),
                vec![item_id],
                sequence as i64 + 2,
            );
            store
                .apply_committed(&pos_for(queue, sequence + 2), &finalize)
                .unwrap();
        });
    }

    drop(store);
    let reopened = SqliteProjectionStore::open(path.to_str().unwrap()).unwrap();
    for (index, queue) in queues.iter().enumerate() {
        let metrics = reopened.metrics(queue).await.unwrap();
        assert_eq!(metrics.pending, 0, "queue {index} pending mismatch");
        assert_eq!(metrics.leased, 0, "queue {index} lease mismatch");
        assert_eq!(metrics.complete, CYCLES, "queue {index} recovery mismatch");
        assert_eq!(metrics.failed, 0, "queue {index} failed mismatch");
    }
    drop(reopened);
    let _ = std::fs::remove_file(path);
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
                worker_id: None,
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
async fn apply_committed_batch_replays_contiguous_push_run_idempotently() {
    let store = SqliteProjectionStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let first = ItemId::new("1").unwrap();
    let second = ItemId::new("2").unwrap();
    let positions = vec![pos(0), pos(1)];
    let envelopes = vec![
        envelope(
            "push-run-1",
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "run-1", 10)],
            }),
            vec![first],
            0,
        ),
        envelope(
            "push-run-2",
            QueueCommand::Push(PushCommand {
                items: vec![item("2", "run-2", 20)],
            }),
            vec![second],
            1,
        ),
    ];

    store.apply_committed_batch(&positions, &envelopes).unwrap();
    assert_eq!(store.metrics(&shard()).await.unwrap().pending, 2);

    store.apply_committed_batch(&positions, &envelopes).unwrap();
    assert_eq!(
        store.metrics(&shard()).await.unwrap().pending,
        2,
        "replaying a fully absorbed push run must not duplicate items"
    );
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
                worker_id: None,
            }),
            vec![item_id],
            1,
        ),
        envelope(
            "pause-1",
            QueueCommand::PauseQueue(PauseQueueCommand::default()),
            vec![],
            2,
        ),
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
        fireweed_engine::ProjectionStore::metrics(&memory, &shard())
            .unwrap()
            .leased,
        1
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::side_record(&memory, &shard(), b"side-key").unwrap(),
        Some(Bytes::from_static(b"side-payload"))
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::instance_fence(&memory, &shard(), b"instance").unwrap(),
        Some(11)
    );
    assert!(
        fireweed_engine::ProjectionStore::select_eligible(&memory, &shard(), ts(10), 10)
            .unwrap()
            .is_empty()
    );
    let live = fireweed_engine::ProjectionStore::live_items(
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
fn legacy_async_projection_projection_applies_sqlite_first_and_serves_memory_parity() {
    let mut store = LegacySqliteProjectionStore::in_memory().unwrap();
    let definition = qdef();
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &definition).unwrap();
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
                worker_id: None,
            }),
            vec![item_id],
            1,
        ),
    ];
    let positions = vec![pos(0), pos(1)];

    fireweed_engine::ProjectionStore::apply(&mut store, &positions, &commands).unwrap();

    let sqlite_image = store.sqlite().export_projection_image(&shard()).unwrap();
    let memory_metrics = fireweed_engine::ProjectionStore::metrics(&store, &shard()).unwrap();
    assert_eq!(sqlite_image.high_water, Some(pos(1)));
    assert_eq!(sqlite_image.metrics, memory_metrics);
    assert_eq!(memory_metrics.leased, 1);
    assert!(
        fireweed_engine::ProjectionStore::select_eligible(&store, &shard(), ts(10), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::live_items(
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
fn legacy_async_projection_projection_hydrates_from_sqlite_before_returning_high_water() {
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

    let mut store = LegacySqliteProjectionStore::new(sqlite);
    assert!(
        fireweed_engine::ProjectionStore::recovery_high_water(&store, &shard()).is_err(),
        "unhydrated projection must not expose sqlite high-water"
    );

    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &definition).unwrap();

    assert_eq!(
        fireweed_engine::ProjectionStore::recovery_high_water(&store, &shard()).unwrap(),
        Some(pos(0))
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::metrics(&store, &shard())
            .unwrap()
            .pending,
        1
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::select_eligible(&store, &shard(), ts(0), 10).unwrap(),
        vec![item_id]
    );
}

#[test]
fn legacy_async_projection_projection_poisoned_after_sqlite_commit_memory_apply_failure() {
    let sqlite = SqliteProjectionStore::in_memory().unwrap();
    sqlite.create_queue_projection(qdef()).unwrap();
    let mut store = LegacySqliteProjectionStore::from_parts(sqlite, InMemoryProjection::new());
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
        fireweed_engine::ProjectionStore::apply(&mut store, &[pos(0)], std::slice::from_ref(&push))
            .unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("async projection poisoned")),
        "unexpected error: {err:?}"
    );
    assert_eq!(
        store.sqlite().recovery_high_water(&shard()).unwrap(),
        Some(pos(0))
    );

    let read_err = fireweed_engine::ProjectionStore::metrics(&store, &shard()).unwrap_err();
    assert!(matches!(read_err, EngineError::Storage(ref msg) if msg.contains("poisoned")));

    let write_err =
        fireweed_engine::ProjectionStore::apply(&mut store, &[pos(1)], &[push]).unwrap_err();
    assert!(matches!(write_err, EngineError::Storage(ref msg) if msg.contains("poisoned")));
}

#[test]
fn legacy_async_projection_chaos_sqlite_commit_before_memory_failure_poisons_and_recovers() {
    let path = temp_projection_path("async-projection-chaos-poison");
    let sqlite = SqliteProjectionStore::open(path.to_str().unwrap()).unwrap();
    let definition = qdef();
    sqlite.create_queue_projection(definition.clone()).unwrap();
    let mut store = LegacySqliteProjectionStore::from_parts(sqlite, InMemoryProjection::new());
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
        fireweed_engine::ProjectionStore::apply(&mut store, &[pos(0)], std::slice::from_ref(&push))
            .unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref msg) if msg.contains("async projection poisoned")),
        "unexpected poison error: {err:?}"
    );
    assert_eq!(
        store.sqlite().recovery_high_water(&shard()).unwrap(),
        Some(pos(0)),
        "SQLite committed the batch before memory failed"
    );
    assert!(fireweed_engine::ProjectionStore::metrics(&store, &shard()).is_err());

    drop(store);
    let sqlite = SqliteProjectionStore::open(path.to_str().unwrap()).unwrap();
    let mut recovered = LegacySqliteProjectionStore::new(sqlite);
    fireweed_engine::ProjectionStore::ensure_shard(&mut recovered, &definition).unwrap();
    assert_eq!(
        fireweed_engine::ProjectionStore::metrics(&recovered, &shard())
            .unwrap()
            .pending,
        1,
        "restart hydrates hot memory from the durable SQLite image"
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::recovery_high_water(&recovered, &shard()).unwrap(),
        Some(pos(0))
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn legacy_async_projection_chaos_replay_overlap_skips_idempotent_prefix_and_applies_tail() {
    let mut store = LegacySqliteProjectionStore::in_memory().unwrap();
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &qdef()).unwrap();
    let first = ItemId::new("1").unwrap();
    let second = ItemId::new("2").unwrap();
    let commands = vec![
        envelope(
            "push-1",
            QueueCommand::Push(PushCommand {
                items: vec![item("1", "k1", 10)],
            }),
            vec![first],
            0,
        ),
        envelope(
            "push-2",
            QueueCommand::Push(PushCommand {
                items: vec![item("2", "k2", 20)],
            }),
            vec![second],
            1,
        ),
    ];

    fireweed_engine::ProjectionStore::apply(&mut store, &[pos(0)], &commands[..1]).unwrap();
    fireweed_engine::ProjectionStore::apply(&mut store, &[pos(0), pos(1)], &commands).unwrap();

    let metrics = fireweed_engine::ProjectionStore::metrics(&store, &shard()).unwrap();
    assert_eq!(metrics.pending, 2, "overlapped prefix was skipped once");
    assert_eq!(
        fireweed_engine::ProjectionStore::recovery_high_water(&store, &shard()).unwrap(),
        Some(pos(1))
    );
    assert_eq!(
        fireweed_engine::ProjectionStore::select_eligible(&store, &shard(), ts(0), 10).unwrap(),
        vec![first, second]
    );
}

#[test]
fn legacy_async_projection_chaos_secondary_index_and_metrics_match_sqlite_image() {
    let mut store = LegacySqliteProjectionStore::in_memory().unwrap();
    let definition = indexed_qdef();
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &definition).unwrap();
    let mut item_a = rich_item("1", "k1");
    item_a
        .fields
        .insert("origin_field".to_string(), Bytes::from_static(b"a"));
    let mut item_b = rich_item("2", "k2");
    item_b
        .fields
        .insert("origin_field".to_string(), Bytes::from_static(b"b"));
    let first = ItemId::new("1").unwrap();
    let second = ItemId::new("2").unwrap();

    fireweed_engine::ProjectionStore::apply(
        &mut store,
        &[pos(0)],
        &[envelope(
            "push-indexed",
            QueueCommand::Push(PushCommand {
                items: vec![item_a, item_b],
            }),
            vec![first, second],
            0,
        )],
    )
    .unwrap();

    let sqlite_image = store.sqlite().export_projection_image(&shard()).unwrap();
    let memory_metrics = fireweed_engine::ProjectionStore::metrics(&store, &shard()).unwrap();
    assert_eq!(sqlite_image.metrics, memory_metrics);
    assert_eq!(memory_metrics.pending, 2);

    let hits = fireweed_engine::ProjectionStore::index_lookup(
        &store,
        &shard(),
        "by_color",
        &[b"red".to_vec()],
    )
    .unwrap();
    assert_eq!(hits.len(), 2, "hot memory serves non-unique index parity");
    let hit = fireweed_engine::ProjectionStore::index_get_unique(
        &store,
        &shard(),
        "uniq_origin",
        &[b"a".to_vec()],
    )
    .unwrap()
    .expect("unique index hit");
    assert_eq!(hit.item_id, first);
}
#[test]
fn reset_projection_preserves_unrelated_shared_database_tables() {
    let path = std::env::temp_dir().join(format!(
        "fireweed-projection-shared-db-{}-{}.sqlite",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let _ = std::fs::remove_file(&path);
    let path_string = path.to_string_lossy().into_owned();
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE application_state (key TEXT PRIMARY KEY, value TEXT NOT NULL); \
                 INSERT INTO application_state VALUES ('owner', 'preserved');",
            )
            .unwrap();
    }
    let store = SqliteProjectionStore::open(&path_string).unwrap();
    store.reset_projection().unwrap();
    drop(store);

    let connection = Connection::open(&path).unwrap();
    let value: String = connection
        .query_row(
            "SELECT value FROM application_state WHERE key='owner'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, "preserved");
    let queues_table: String = connection
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='queues'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(queues_table, "queues");
    drop(connection);
    let _ = std::fs::remove_file(path);
}

struct AsyncCheckpointFailure;

impl AsyncProjectionFaultHook for AsyncCheckpointFailure {
    fn fault_point(&self, cut: AsyncProjectionFaultCutPoint) -> fireweed_engine::EngineResult<()> {
        if cut == AsyncProjectionFaultCutPoint::DuringAsyncSqliteApply {
            Err(EngineError::Storage("checkpoint worker failed".into()))
        } else {
            Ok(())
        }
    }
}

#[test]
fn async_checkpoint_worker_failure_is_visible_and_fails_closed() {
    let mut store = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(10, 10, 10, 10, 1).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &qdef()).unwrap();
    let id = ItemId::new("1").unwrap();
    fireweed_engine::ProjectionStore::apply_live(
        &mut store,
        &[pos(0)],
        &[envelope(
            "async-worker-push",
            QueueCommand::Push(PushCommand {
                items: vec![rich_item("1", "async-worker-key")],
            }),
            vec![id],
            0,
        )],
    )
    .unwrap();
    assert_eq!(store.deferred_command_count(), 1);
    store.set_fault_hook(Some(Arc::new(AsyncCheckpointFailure)));
    assert!(fireweed_engine::ProjectionStore::flush_deferred(&mut store).is_err());
    assert!(store.poison_reason().is_some());
    assert!(fireweed_engine::ProjectionStore::metrics(&store, &shard()).is_err());
}

#[test]
fn async_debt_uses_real_encoded_bytes_and_oldest_queue_age() {
    let command = envelope(
        "measured-debt-push",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("1", "measured-debt-key")],
        }),
        vec![ItemId::new("1").unwrap()],
        0,
    );

    let mut byte_limited = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, 1, 100, 10_000, 3).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut byte_limited, &qdef()).unwrap();
    byte_limited.set_async_debt_now_ms_for_test(Some(1_000));
    fireweed_engine::ProjectionStore::apply_live(
        &mut byte_limited,
        &[pos(0)],
        std::slice::from_ref(&command),
    )
    .unwrap();
    let metrics = byte_limited.async_metrics().unwrap();
    assert!(metrics.apply_debt_bytes > 1);
    assert_eq!(metrics.backpressure_level, BackpressureLevel::Hard);
    assert!(
        fireweed_engine::ProjectionStore::admit_mutation(&mut byte_limited, &shard()).is_err(),
        "the real encoded-byte bound must reject subsequent mutations"
    );

    let mut age_limited = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, u64::MAX, 100, 50, 3).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut age_limited, &qdef()).unwrap();
    age_limited.set_async_debt_now_ms_for_test(Some(2_000));
    fireweed_engine::ProjectionStore::apply_live(&mut age_limited, &[pos(0)], &[command]).unwrap();
    assert_eq!(
        age_limited.async_metrics().unwrap().oldest_unapplied_age_ms,
        0
    );
    age_limited.set_async_debt_now_ms_for_test(Some(2_050));
    assert!(
        fireweed_engine::ProjectionStore::admit_mutation(&mut age_limited, &shard()).is_err(),
        "admission must resample oldest-unapplied age"
    );
    let metrics = age_limited.async_metrics().unwrap();
    assert_eq!(metrics.oldest_unapplied_age_ms, 50);
    assert_eq!(metrics.backpressure_level, BackpressureLevel::Hard);
}

#[test]
fn async_debt_is_per_queue_and_depth_counts_sealed_batches() {
    let mut store = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, u64::MAX, 2, 10_000, 3).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &named_qdef("debt-a")).unwrap();
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &named_qdef("debt-b")).unwrap();
    store.set_async_debt_now_ms_for_test(Some(1_000));

    let first_batch = vec![
        envelope(
            "a-1",
            QueueCommand::Push(PushCommand {
                items: vec![rich_item("101", "a-key-1")],
            }),
            vec![ItemId::new("101").unwrap()],
            0,
        ),
        envelope(
            "a-2",
            QueueCommand::Push(PushCommand {
                items: vec![rich_item("102", "a-key-2")],
            }),
            vec![ItemId::new("102").unwrap()],
            0,
        ),
    ];
    fireweed_engine::ProjectionStore::apply_live(
        &mut store,
        &[named_pos("debt-a", 0), named_pos("debt-a", 1)],
        &first_batch,
    )
    .unwrap();
    assert_eq!(
        store
            .async_metrics_for(&named_shard("debt-a"))
            .unwrap()
            .apply_queue_depth,
        1,
        "one sealed two-command apply is one queued batch"
    );

    let a3 = envelope(
        "a-3",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("103", "a-key-3")],
        }),
        vec![ItemId::new("103").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(&mut store, &[named_pos("debt-a", 2)], &[a3])
        .unwrap();
    let b1 = envelope(
        "b-1",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("201", "b-key-1")],
        }),
        vec![ItemId::new("201").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(&mut store, &[named_pos("debt-b", 0)], &[b1])
        .unwrap();

    let a = named_shard("debt-a");
    let b = named_shard("debt-b");
    let a_metrics = store.async_metrics_for(&a).unwrap();
    let b_metrics = store.async_metrics_for(&b).unwrap();
    assert_eq!(a_metrics.apply_lag_commands, 3);
    assert_eq!(a_metrics.apply_queue_depth, 2);
    assert_eq!(a_metrics.backpressure_level, BackpressureLevel::Hard);
    assert_eq!(b_metrics.apply_lag_commands, 1);
    assert_eq!(b_metrics.apply_queue_depth, 1);
    assert_ne!(b_metrics.backpressure_level, BackpressureLevel::Hard);
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &a).is_err());
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &b).is_ok());
}

#[test]
fn failed_flush_resamples_quiet_oldest_age_before_returning() {
    let mut store = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, u64::MAX, 100, 50, 3).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &qdef()).unwrap();
    store.set_async_debt_now_ms_for_test(Some(5_000));
    let command = envelope(
        "quiet-age",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("301", "quiet-key")],
        }),
        vec![ItemId::new("301").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(&mut store, &[pos(0)], &[command]).unwrap();
    store.set_fault_hook(Some(Arc::new(AsyncCheckpointFailure)));
    store.set_async_debt_now_ms_for_test(Some(5_050));
    assert!(fireweed_engine::ProjectionStore::flush_deferred(&mut store).is_err());
    let metrics = store.async_metrics_for(&shard()).unwrap();
    assert_eq!(metrics.oldest_unapplied_age_ms, 50);
    assert_eq!(metrics.backpressure_level, BackpressureLevel::Hard);
}

#[test]
fn async_poison_rejects_future_admission_but_not_an_already_admitted_apply() {
    let mut store = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, u64::MAX, 100, 10_000, 1).unwrap());
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &qdef()).unwrap();
    let first = envelope(
        "poison-race-first",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("401", "race-key-1")],
        }),
        vec![ItemId::new("401").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(&mut store, &[pos(0)], &[first]).unwrap();
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &shard()).is_ok());

    store.set_fault_hook(Some(Arc::new(AsyncCheckpointFailure)));
    assert!(fireweed_engine::ProjectionStore::flush_deferred(&mut store).is_err());
    store.set_fault_hook(None);
    assert!(store.async_metrics_for(&shard()).unwrap().poisoned);

    let admitted_before_poison = envelope(
        "poison-race-admitted",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("402", "race-key-2")],
        }),
        vec![ItemId::new("402").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(&mut store, &[pos(1)], &[admitted_before_poison])
        .expect("an admitted group-commit buffer must finish its live apply after async poison");
    assert_eq!(store.deferred_command_count(), 2);
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &shard()).is_err());
}
#[test]
fn lease_clearing_transitions_remove_worker_attribution_from_export_and_recovery() {
    let worker = WorkerId::new("projection-worker").unwrap();
    let lease_token = LeaseToken::new("projection-lease").unwrap();
    for (case, command) in [
        (
            "complete",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(
                    ItemId::new("1").unwrap(),
                    FinalizeKind::Complete,
                )],
            }),
        ),
        (
            "retry",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(
                    ItemId::new("1").unwrap(),
                    FinalizeKind::Retry,
                )],
            }),
        ),
        (
            "rearm",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(
                    ItemId::new("1").unwrap(),
                    FinalizeKind::Rearm,
                )],
            }),
        ),
        (
            "expired",
            QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: vec![ItemId::new("1").unwrap()],
            }),
        ),
    ] {
        let definition = qdef();
        let store = SqliteProjectionStore::in_memory().unwrap();
        store.create_queue_projection(definition.clone()).unwrap();
        let item_id = ItemId::new("1").unwrap();
        let commands = vec![
            envelope(
                &format!("push-{case}"),
                QueueCommand::Push(PushCommand {
                    items: vec![rich_item("1", "k1")],
                }),
                vec![item_id],
                0,
            ),
            envelope(
                &format!("claim-{case}"),
                QueueCommand::Claim(ClaimCommand {
                    item_ids: vec![item_id],
                    lease_token: lease_token.clone(),
                    lease_expires_at: ts(60),
                    worker_id: Some(worker.clone()),
                }),
                vec![item_id],
                1,
            ),
        ];
        store
            .apply_committed_batch(&[pos(0), pos(1)], &commands)
            .unwrap();
        assert_eq!(
            store.export_projection_image(&shard()).unwrap().items[0].worker_id,
            Some(worker.clone()),
            "{case}: leased export must retain attribution"
        );
        store
            .apply_committed_batch(
                &[pos(2)],
                &[envelope(
                    &format!("clear-{case}"),
                    command,
                    vec![item_id],
                    2,
                )],
            )
            .unwrap();
        let image = store.export_projection_image(&shard()).unwrap();
        assert_eq!(
            image.items[0].worker_id, None,
            "{case}: lease-clearing export must drop attribution"
        );
        let mut recovered = InMemoryProjection::new();
        recovered.hydrate_shard(&definition, image.clone()).unwrap();
        assert_eq!(
            fireweed_engine::ProjectionStore::metrics(&recovered, &shard()).unwrap(),
            image.metrics,
            "{case}: recovered memory projection must match SQLite export"
        );
    }
}
#[test]
fn poisoned_head_shard_does_not_block_healthy_tail_checkpoint_progress() {
    let mut store = LegacySqliteProjectionStore::in_memory()
        .unwrap()
        .with_async_monitor(AsyncProjectionThresholds::new(100, u64::MAX, 100, 10_000, 1).unwrap());
    let poisoned = named_shard("poisoned-head");
    let healthy = named_shard("healthy-tail");
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &named_qdef("poisoned-head"))
        .unwrap();
    fireweed_engine::ProjectionStore::ensure_shard(&mut store, &named_qdef("healthy-tail"))
        .unwrap();

    let poisoned_command = envelope(
        "poisoned-head-push",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("501", "poisoned-head-key")],
        }),
        vec![ItemId::new("501").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(
        &mut store,
        &[named_pos("poisoned-head", 0)],
        &[poisoned_command],
    )
    .unwrap();
    let healthy_command = envelope(
        "healthy-tail-push",
        QueueCommand::Push(PushCommand {
            items: vec![rich_item("502", "healthy-tail-key")],
        }),
        vec![ItemId::new("502").unwrap()],
        0,
    );
    fireweed_engine::ProjectionStore::apply_live(
        &mut store,
        &[named_pos("healthy-tail", 0)],
        &[healthy_command],
    )
    .unwrap();

    store.set_fault_hook(Some(Arc::new(AsyncCheckpointFailure)));
    assert!(fireweed_engine::ProjectionStore::flush_deferred(&mut store).is_err());
    store.set_fault_hook(None);
    assert!(store.async_metrics_for(&poisoned).unwrap().poisoned);

    fireweed_engine::ProjectionStore::flush_deferred(&mut store)
        .expect("healthy tail shard must checkpoint past a poisoned FIFO head");
    assert_eq!(store.deferred_command_count(), 1);
    let healthy_metrics = store.async_metrics_for(&healthy).unwrap();
    assert_eq!(healthy_metrics.apply_lag_commands, 0);
    assert_eq!(healthy_metrics.apply_queue_depth, 0);
    assert_eq!(
        fireweed_engine::ProjectionStore::recovery_high_water(store.sqlite(), &healthy)
            .unwrap()
            .unwrap(),
        named_pos("healthy-tail", 0)
    );
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &healthy).is_ok());
    assert!(fireweed_engine::ProjectionStore::admit_mutation(&mut store, &poisoned).is_err());
}
