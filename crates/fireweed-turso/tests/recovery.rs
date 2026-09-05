mod support;

use bytes::Bytes;
use fireweed_conformance::{envelope, qdef};
use fireweed_core::{ItemId, ItemState, RequestId};
use fireweed_engine::{
    AsyncProjectionStore, CommandPosition, CommitOutcomeEntry, QueueCommand, QueueKey,
    RequestOutcome, SideRecord, WriteSideRecordsCommand,
};
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

fn side(key: &str, payload: &str) -> SideRecord {
    SideRecord {
        key: key.as_bytes().to_vec(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    }
}

/// Bead fireweed-82211ac4 (mirrors `fireweed-sqlite`'s
/// `relational_side_records_by_prefix_pages_ordered_and_survives_reopen`): side records written by a
/// committed batch read back through `side_record` (point get) and `side_records_by_prefix` (ordered,
/// cursor-paged), the retained commit outcome reads back through `read_durable_commit`, and all three
/// survive a reopen because they come from the durable `fireweed_side_records` /
/// `fireweed_request_idempotency` tables.
#[tokio::test]
async fn side_records_and_commit_recovery_read_back_and_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("side-records.db");
    let definition = qdef();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let request_id = RequestId::new("prefix-scan-1").unwrap();
    let consumed = ItemId::new("301").unwrap();

    {
        let store = TursoRelational::open(TursoConfig::local(&path))
            .await
            .unwrap();
        AsyncProjectionStore::ensure_shard(&store, definition.clone())
            .await
            .unwrap();
        // The commit path persists side records + the retained commit outcome through the same
        // apply arm this envelope exercises (WriteSideRecords + RequestOutcome::CommitTransition).
        let mut command = envelope(
            QueueCommand::WriteSideRecords(WriteSideRecordsCommand {
                records: vec![
                    side("audit:instance-1:001", "a1"),
                    side("audit:instance-1:003", "a3"),
                    side("audit:instance-1:002", "a2"),
                    side("audit:instance-2:001", "other-instance"),
                ],
            }),
            vec![],
        );
        command.request_id = Some(request_id.clone());
        command.request_fingerprint = Some(42);
        command.request_outcome = Some(RequestOutcome::CommitTransition {
            entries: vec![CommitOutcomeEntry {
                consumed_input_id: consumed,
                additional_consumed_input_ids: vec![],
                instance: None,
                side_record_keys: vec![],
                lifecycle_item_ids: vec![],
                rejection: None,
            }],
        });
        AsyncProjectionStore::apply_live(
            &store,
            vec![CommandPosition::new(shard.clone(), 0, 0)],
            vec![command],
        )
        .await
        .unwrap();
    } // drop the handle

    // Reopen the same file: the reads below come from durable tables, not an in-process cache.
    let reopened = TursoRelational::open(TursoConfig::local(&path))
        .await
        .unwrap();

    assert_eq!(
        AsyncProjectionStore::side_record(
            &reopened,
            shard.clone(),
            b"audit:instance-1:002".to_vec()
        )
        .await
        .unwrap(),
        Some(Bytes::from_static(b"a2"))
    );
    assert_eq!(
        AsyncProjectionStore::side_record(&reopened, shard.clone(), b"audit:missing".to_vec())
            .await
            .unwrap(),
        None
    );

    let first_page = AsyncProjectionStore::side_records_by_prefix(
        &reopened,
        shard.clone(),
        b"audit:instance-1:".to_vec(),
        2,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        first_page.entries,
        vec![
            (b"audit:instance-1:001".to_vec(), Bytes::from_static(b"a1")),
            (b"audit:instance-1:002".to_vec(), Bytes::from_static(b"a2")),
        ]
    );
    let cursor = first_page
        .next_cursor
        .clone()
        .expect("a third matching entry remains");
    assert_eq!(cursor, b"audit:instance-1:003".to_vec());

    let second_page = AsyncProjectionStore::side_records_by_prefix(
        &reopened,
        shard.clone(),
        b"audit:instance-1:".to_vec(),
        2,
        Some(cursor),
    )
    .await
    .unwrap();
    assert_eq!(
        second_page.entries,
        vec![(b"audit:instance-1:003".to_vec(), Bytes::from_static(b"a3"))]
    );
    assert_eq!(
        second_page.next_cursor, None,
        "the prefix's key range is exhausted"
    );

    // A sibling instance's records under a different prefix are excluded entirely.
    let other = AsyncProjectionStore::side_records_by_prefix(
        &reopened,
        shard.clone(),
        b"audit:instance-2:".to_vec(),
        10,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        other.entries,
        vec![(
            b"audit:instance-2:001".to_vec(),
            Bytes::from_static(b"other-instance")
        )]
    );
    assert_eq!(other.next_cursor, None);

    // The retained whole-body commit outcome survives the reopen; an unknown id reads as None.
    let entries =
        AsyncProjectionStore::read_durable_commit(&reopened, shard.clone(), request_id.clone())
            .await
            .unwrap()
            .expect("retained commit row");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].consumed_input_id, consumed);
    assert!(entries[0].rejection.is_none());
    assert_eq!(
        AsyncProjectionStore::read_durable_commit(
            &reopened,
            shard,
            RequestId::new("never-committed").unwrap()
        )
        .await
        .unwrap(),
        None
    );
}
