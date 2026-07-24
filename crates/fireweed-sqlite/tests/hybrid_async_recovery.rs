//! Recovery-on-open for the `objectlog/hybrid-async` profile (bead pqueue-45cbb98e).
//!
//! These tests exercise the projection-side recovery contract [`HybridProjectionStore`] enforces before a
//! reopened composition may serve: it hydrates hot memory from the durable SQLite image, cross-validates the
//! image's recorded object-log lineage against the log's identity (namespace / manifest-generation epoch /
//! segment-chain high-water), advertises its high-water as a replay-skip point ONLY after hydration, and
//! FAILS CLOSED (poisons) when the SQLite image does not descend from the log it is about to replay against.
//!
//! The object-log substrate lives in `fireweed-objectlog` (which `fireweed-sqlite` deliberately does not depend
//! on), so these tests present the log's identity through the crate-boundary [`LogLineageIdentity`] value the
//! composition builds during `recover`. End-to-end recovery over the real `ObjectLog` substrate is covered by
//! `fireweed-conformance/tests/objectlog_hybrid.rs` and `fireweed-objectlog/tests/hybrid_request_id.rs`.

use fireweed_conformance::{item, qdef, shard, ts};
use fireweed_core::{ItemId, RequestId};
use fireweed_engine::{
    CommandChecksum, CommandEnvelope, CommandId, CommandPosition, EngineError, LogLineageIdentity,
    ProjectionStore, PushCommand, QueueCommand, RequestOutcome,
};
use fireweed_sqlite::{CheckpointLineage, HybridProjectionStore, SqliteCheckpointStore};

/// A push carrying the full request-id replay metadata the async checkpoint worker persists into the durable
/// idempotency table, so a committed-but-unreturned retry converges after restart.
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

fn pos_epoch(epoch: u64, sequence: u64) -> CommandPosition {
    CommandPosition::new(shard(), epoch, sequence)
}

fn lineage(epoch: u64, segment: &str) -> CheckpointLineage {
    CheckpointLineage {
        source_epoch: epoch,
        source_segment: segment.to_string(),
    }
}

/// The object-log identity a fully-caught-up log presents for `shard`: `next_seq` commands committed under
/// `epoch`, so the durable head is at `sequence = next_seq - 1` (or `None` for an empty log).
fn identity(epoch: u64, next_seq: u64) -> LogLineageIdentity {
    LogLineageIdentity {
        shard: shard(),
        current_epoch: epoch,
        high_water: (next_seq > 0).then(|| CommandPosition::new(shard(), epoch, next_seq - 1)),
    }
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "pqueue-hybrid-async-recovery-{tag}-{}.db",
        std::process::id()
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", p.display()));
    }
    p
}

fn cleanup(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Seed a durable SQLite image at `path`: run the async checkpoint worker over `batch` (recording lineage +
/// idempotency + advancing the logical high-water), then drop the worker to simulate a restart.
fn seed_checkpoint(
    path: &std::path::Path,
    positions: &[CommandPosition],
    batch: &[CommandEnvelope],
    lineage: &CheckpointLineage,
) {
    let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        store
            .checkpoint(&shard(), positions, batch, lineage)
            .await
            .unwrap();
    });
}

/// The accelerated-restart happy path: reopen, hydrate memory from the validated SQLite image, validate the
/// recorded lineage against a matching log identity, then advertise the high-water as a replay-skip point.
/// The durably persisted request-id outcome survives so a committed-but-unreturned push converges.
#[test]
fn hybrid_async_recovery_hydrates_validates_and_advertises_high_water() {
    let path = temp_path("happy");
    let item_id = ItemId::new("1").unwrap();
    let request = RequestId::new("req-1").unwrap();

    // Checkpoint one push under object-log epoch 3, then restart.
    let push = push_with_request("push-r1", "req-1", 0x1234, item_id, "k1", 10, 0);
    seed_checkpoint(
        &path,
        std::slice::from_ref(&pos_epoch(3, 0)),
        std::slice::from_ref(&push),
        &lineage(3, "s3://log/seg-00000003-0000"),
    );

    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();

    // High-water barrier (TD-004): before the shard is hydrated, recovery_high_water fails closed rather than
    // advertise a skip point the composition would use to skip un-replayed log history.
    assert!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).is_err(),
        "recovery_high_water must fail closed before hydration"
    );

    // Hydrate memory from the durable SQLite image.
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();

    // Lineage validation against a log at epoch 3 with the same single-command head — it descends cleanly.
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(3, 1)).unwrap();
    // A log re-acquired at a HIGHER epoch (owner handoff) is also fine: the projection is merely behind.
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(5, 1)).unwrap();

    // After hydration the high-water is the last absorbed command (seq 0), the replay-skip point.
    assert_eq!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).unwrap(),
        Some(CommandPosition::new(shard(), 3, 0)),
    );
    assert_eq!(
        ProjectionStore::metrics(&hybrid, &shard()).unwrap().pending,
        1,
        "memory hydrated the pending item from the durable image"
    );

    // The request-id outcome persisted through the high-water survives the restart, so a committed-but-
    // unreturned push is rebuilt from the durable table before serving.
    let reopened = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        reopened.replay_push(&shard(), &request).unwrap(),
        Some(vec![item_id]),
        "durable request-id outcome converges after restart"
    );

    cleanup(&path);
}

/// Fail closed when the SQLite image records an object-log epoch NEWER than the log currently records: the
/// image cannot descend from this log (a rolled-back or foreign log, or an image restored over the wrong
/// namespace). The mismatch poisons the projection so subsequent reads also fail closed.
#[test]
fn hybrid_async_recovery_fails_closed_on_newer_lineage_epoch() {
    let path = temp_path("newer-epoch");
    let item_id = ItemId::new("1").unwrap();

    // The durable image was checkpointed from object-log epoch 5.
    let push = push_with_request("push-r1", "req-1", 0xAAAA, item_id, "k1", 10, 0);
    seed_checkpoint(
        &path,
        std::slice::from_ref(&pos_epoch(5, 0)),
        std::slice::from_ref(&push),
        &lineage(5, "s3://log/seg-00000005-0000"),
    );

    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();

    // The log presented at recovery is only at epoch 3 — older than the recorded lineage.
    let err = ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(3, 1)).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("newer than")),
        "expected fail-closed epoch mismatch, got {err:?}"
    );

    // Poisoned: the projection refuses to serve from an image that does not descend from the log.
    let poisoned = ProjectionStore::metrics(&hybrid, &shard()).unwrap_err();
    assert!(
        matches!(poisoned, EngineError::Storage(ref m) if m.contains("poisoned")),
        "reads must fail closed after a lineage mismatch, got {poisoned:?}"
    );

    cleanup(&path);
}

/// Fail closed when the SQLite logical high-water is AHEAD of the object-log's committed head: the projection
/// absorbed commands the durable log does not contain, so its high-water can never be a safe replay-skip
/// point.
#[test]
fn hybrid_async_recovery_fails_closed_when_sqlite_ahead_of_log() {
    let path = temp_path("sqlite-ahead");
    let first = ItemId::new("1").unwrap();
    let second = ItemId::new("2").unwrap();

    // Two commands checkpointed → SQLite logical high-water (next_seq) is 2.
    let batch = vec![
        push_with_request("push-1", "req-1", 0x1, first, "k1", 10, 0),
        push_with_request("push-2", "req-2", 0x2, second, "k2", 20, 1),
    ];
    seed_checkpoint(
        &path,
        &[pos_epoch(0, 0), pos_epoch(0, 1)],
        &batch,
        &lineage(0, "seg-0001"),
    );

    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();

    // The log only durably committed ONE command (head at seq 0, next_seq 1) — the SQLite image at next_seq 2
    // is ahead of the durable log.
    let err = ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(0, 1)).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("ahead of the object-log head")),
        "expected fail-closed high-water mismatch, got {err:?}"
    );
    assert!(
        ProjectionStore::metrics(&hybrid, &shard()).is_err(),
        "reads must fail closed after a high-water mismatch"
    );

    cleanup(&path);
}

/// A projection that recorded no lineage (never async-checkpointed, or synchronously materialized) has no
/// epoch to cross-check, but the high-water identity check still guards against a SQLite image ahead of the
/// log. A caught-up, fully-covered image validates cleanly.
#[test]
fn hybrid_async_recovery_no_lineage_still_checks_high_water() {
    let path = temp_path("no-lineage");

    // A never-checkpointed queue: only the projection row exists, no lineage, no commands.
    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        assert!(store.progress(&shard()).unwrap().lineage.is_none());
    }

    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();

    // No lineage + empty log: validates cleanly (nothing absorbed, nothing ahead).
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(0, 0)).unwrap();
    // A non-empty log with an empty projection is the normal genesis-tail case — still valid.
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(2, 8)).unwrap();
    assert_eq!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).unwrap(),
        None,
        "an empty projection advertises genesis, so the whole log tail is replayed"
    );

    cleanup(&path);
}
