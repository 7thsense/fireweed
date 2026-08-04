//! Legacy-compatibility assertions migrated to the provider-neutral AsyncProjection test ledger.
//!
//! Async SQLite logical checkpoint store for the `objectlog/async projection` profile (bead pqueue-16b85e28).
//!
//! These tests exercise [`SqliteCheckpointStore`], the off-hot-path worker that consumes committed
//! object-log entries in order and, per batch in ONE SQLite transaction: applies the commands, persists
//! request-id idempotency/outcome rows, records the object-log lineage, and advances the LOGICAL
//! high-water LAST. They also assert the LOGICAL high-water is distinct from the PHYSICAL SQLite WAL
//! checkpoint.

use fireweed_conformance::{item, qdef, shard, ts};
use fireweed_core::{ItemId, LeaseToken, RequestId};
use fireweed_engine::{
    ClaimCommand, CommandChecksum, CommandEnvelope, CommandId, CommandPosition, EngineError,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionStore, PushCommand, QueueCommand,
    RequestOutcome,
};
use fireweed_projection::InMemoryProjection;
use fireweed_sqlite::{CheckpointLineage, SqliteCheckpointStore, SqliteProjectionStore};

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

/// A push carrying the full request-id replay metadata (fingerprint + push outcome) — the shape the async
/// checkpoint worker persists into the durable idempotency table so a committed-but-unreturned retry
/// converges after restart.
fn push_with_request(
    id: &str,
    request: &str,
    fingerprint: u64,
    item_id: ItemId,
    key: &str,
    priority: i64,
    created_at: i64,
) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(id),
        request_id: Some(RequestId::new(request).unwrap()),
        request_fingerprint: Some(fingerprint),
        request_outcome: Some(RequestOutcome::Push {
            item_ids: vec![item_id],
        }),
        item_ids: vec![item_id],
        command: QueueCommand::Push(PushCommand {
            items: vec![item(&item_id.to_string(), key, priority)],
        }),
        checksum: CommandChecksum(0),
        created_at: ts(created_at),
    }
}

/// A command position stamped with an explicit object-log assignment epoch (the lineage epoch).
fn pos_epoch(epoch: u64, sequence: u64) -> CommandPosition {
    CommandPosition::new(shard(), epoch, sequence)
}

fn pos(sequence: u64) -> CommandPosition {
    pos_epoch(0, sequence)
}

fn lineage(epoch: u64, segment: &str) -> CheckpointLineage {
    CheckpointLineage {
        source_epoch: epoch,
        source_segment: segment.to_string(),
    }
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "fireweed-async projection-checkpoint-{tag}-{}.db",
        std::process::id()
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", p.display()));
    }
    p
}

#[tokio::test]
async fn async_projection_checkpoint_applies_ordered_batches_and_advances_logical_high_water() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let lease = LeaseToken::new("lease-1").unwrap();

    // No projection commands applied yet: logical high-water starts at 0 (next_seq), no lineage.
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(0));
    assert!(store.progress(&shard()).unwrap().lineage.is_none());

    // First ordered batch: push + claim, applied in one transaction.
    let batch1 = vec![
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
                lease_token: lease.clone(),
                lease_expires_at: ts(60),
                worker_id: None,
            }),
            vec![item_id],
            1,
        ),
    ];
    let progress = store
        .checkpoint(
            &shard(),
            &[pos(0), pos(1)],
            &batch1,
            &lineage(0, "seg-0000"),
        )
        .await
        .unwrap();
    assert_eq!(progress.logical_high_water, Some(2));
    assert_eq!(progress.applied_commands, 2);
    let image = store
        .projection()
        .export_projection_image(&shard())
        .unwrap();
    assert_eq!(image.metrics.leased, 1);
    assert_eq!(image.high_water, Some(pos(1)));

    // A batch that starts past the cursor (a gap) is a hard error — nothing is applied.
    let finalize = envelope(
        "finalize-1",
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome::new(item_id, FinalizeKind::Complete)],
        }),
        vec![item_id],
        2,
    );
    let gap = store
        .checkpoint(
            &shard(),
            std::slice::from_ref(&pos(5)),
            std::slice::from_ref(&finalize),
            &lineage(0, "seg-gap"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(gap, EngineError::Storage(ref m) if m.contains("checkpoint replay gap")),
        "unexpected gap error: {gap:?}"
    );
    // The failed checkpoint left the logical high-water untouched.
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(2));

    // The next contiguous batch (finalize at seq 2) completes the item.
    let progress = store
        .checkpoint(
            &shard(),
            std::slice::from_ref(&pos(2)),
            std::slice::from_ref(&finalize),
            &lineage(0, "seg-0001"),
        )
        .await
        .unwrap();
    assert_eq!(progress.logical_high_water, Some(3));
    assert_eq!(
        progress.applied_commands, 3,
        "cumulative across both batches"
    );
    let image = store
        .projection()
        .export_projection_image(&shard())
        .unwrap();
    assert_eq!(image.metrics.complete, 1);
    assert_eq!(image.metrics.leased, 0);
}

#[tokio::test]
async fn async_projection_checkpoint_skips_already_applied_prefix_idempotently() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
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

    store
        .checkpoint(&shard(), &[pos(0)], &commands[..1], &lineage(0, "seg-a"))
        .await
        .unwrap();
    // Re-checkpoint an overlapping [0,1] batch: position 0 is skipped idempotently, only 1 applies.
    let progress = store
        .checkpoint(&shard(), &[pos(0), pos(1)], &commands, &lineage(0, "seg-b"))
        .await
        .unwrap();
    assert_eq!(progress.logical_high_water, Some(2));
    // 1 applied in the first batch + 1 net-new in the second (the overlapped prefix was not re-counted).
    assert_eq!(progress.applied_commands, 2);
    let image = store
        .projection()
        .export_projection_image(&shard())
        .unwrap();
    assert_eq!(
        image.metrics.pending, 2,
        "the overlapped push was not double-applied"
    );
}

#[tokio::test]
async fn async_projection_checkpoint_persists_idempotency_rows_through_high_water() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();
    let request = RequestId::new("req-1").unwrap();

    // No idempotency row before the push is checkpointed.
    assert_eq!(store.replay_push(&shard(), &request).unwrap(), None);

    let push = push_with_request("push-r1", "req-1", 0xABCD, item_id, "k1", 10, 0);
    let progress = store
        .checkpoint(
            &shard(),
            std::slice::from_ref(&pos(0)),
            std::slice::from_ref(&push),
            &lineage(0, "seg-0000"),
        )
        .await
        .unwrap();
    assert_eq!(progress.logical_high_water, Some(1));

    // The idempotency/outcome row was persisted through the high-water: a same-request retry replays the
    // original ids from the durable table.
    assert_eq!(
        store.replay_push(&shard(), &request).unwrap(),
        Some(vec![item_id])
    );
    // An unrelated request id has no persisted outcome.
    let other = RequestId::new("req-unknown").unwrap();
    assert_eq!(store.replay_push(&shard(), &other).unwrap(), None);
}

#[tokio::test]
async fn async_projection_checkpoint_records_object_log_lineage() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let first = ItemId::new("1").unwrap();
    let second = ItemId::new("2").unwrap();

    // First segment, committed under object-log assignment epoch 7.
    store
        .checkpoint(
            &shard(),
            &[pos_epoch(7, 0)],
            &[envelope(
                "push-1",
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "k1", 10)],
                }),
                vec![first],
                0,
            )],
            &lineage(7, "s3://log/seg-00000007-0000"),
        )
        .await
        .unwrap();
    let progress = store.progress(&shard()).unwrap();
    assert_eq!(
        progress.lineage,
        Some(lineage(7, "s3://log/seg-00000007-0000"))
    );
    assert_eq!(progress.applied_commands, 1);
    assert_eq!(progress.logical_high_water, Some(1));

    // A later segment updates the lineage to the newest object-log reference; applied count is cumulative.
    store
        .checkpoint(
            &shard(),
            &[pos_epoch(7, 1)],
            &[envelope(
                "push-2",
                QueueCommand::Push(PushCommand {
                    items: vec![item("2", "k2", 20)],
                }),
                vec![second],
                1,
            )],
            &lineage(7, "s3://log/seg-00000007-0001"),
        )
        .await
        .unwrap();
    let progress = store.progress(&shard()).unwrap();
    assert_eq!(
        progress.lineage,
        Some(lineage(7, "s3://log/seg-00000007-0001"))
    );
    assert_eq!(progress.applied_commands, 2);
}

#[tokio::test]
async fn async_projection_checkpoint_distinguishes_logical_high_water_from_wal_checkpoint() {
    // A file-backed store so the WAL is real (in-memory databases have no WAL).
    let path = temp_path("wal-distinct");
    let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let item_id = ItemId::new("1").unwrap();

    store
        .checkpoint(
            &shard(),
            &[pos(0)],
            &[envelope(
                "push-1",
                QueueCommand::Push(PushCommand {
                    items: vec![item("1", "k1", 10)],
                }),
                vec![item_id],
                0,
            )],
            &lineage(0, "seg-0000"),
        )
        .await
        .unwrap();

    let before = store.progress(&shard()).unwrap();
    assert_eq!(before.logical_high_water, Some(1));

    // A PHYSICAL WAL checkpoint reclaims WAL frames but must NOT advance the LOGICAL high-water or lineage.
    let stats = store.wal_checkpoint().await.unwrap();
    assert_eq!(
        stats.busy, 0,
        "no competing connection, so the checkpoint runs to completion"
    );

    let after = store.progress(&shard()).unwrap();
    assert_eq!(
        after, before,
        "wal_checkpoint is a storage-file concern; it does not touch the logical command cursor"
    );

    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[test]
fn async_projection_checkpoint_survives_reopen_and_rehydrates_memory() {
    let path = temp_path("reopen");
    let item_id = ItemId::new("1").unwrap();
    let request = RequestId::new("req-1").unwrap();
    let definition = qdef();

    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(definition.clone()).unwrap();
        let push = push_with_request("push-r1", "req-1", 0x1234, item_id, "k1", 10, 0);
        // Drive the async checkpoint on a local runtime, then drop the store (simulated restart).
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            store
                .checkpoint(
                    &shard(),
                    std::slice::from_ref(&pos(0)),
                    std::slice::from_ref(&push),
                    &lineage(3, "seg-durable"),
                )
                .await
                .unwrap();
        });
    }

    // Reopen: the logical high-water, lineage, and persisted idempotency row all survive the restart.
    let reopened = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    assert_eq!(reopened.logical_high_water(&shard()).unwrap(), Some(1));
    assert_eq!(
        reopened.progress(&shard()).unwrap().lineage,
        Some(lineage(3, "seg-durable"))
    );
    assert_eq!(
        reopened.replay_push(&shard(), &request).unwrap(),
        Some(vec![item_id]),
        "request-id outcome persisted through the high-water converges after restart"
    );

    // The durable image rehydrates hot memory to the same state — the accelerated-restart path.
    let image = reopened
        .projection()
        .export_projection_image(&shard())
        .unwrap();
    let mut memory = InMemoryProjection::new();
    memory.hydrate_shard(&definition, image).unwrap();
    assert_eq!(
        ProjectionStore::metrics(&memory, &shard()).unwrap().pending,
        1
    );
    assert_eq!(
        ProjectionStore::select_eligible(&memory, &shard(), ts(0), 10).unwrap(),
        vec![item_id]
    );

    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// The checkpoint store is constructible from an existing projection store, so a running projection can hand
/// its durable SQLite projection to the async worker without reopening the file.
#[test]
fn async_projection_checkpoint_wraps_an_existing_projection_store() {
    let projection = SqliteProjectionStore::in_memory().unwrap();
    projection.create_queue_projection(qdef()).unwrap();
    let store = SqliteCheckpointStore::new(projection);
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(0));
    assert!(store.progress(&shard()).unwrap().lineage.is_none());
}
