//! Legacy-compatibility assertions migrated to the provider-neutral AsyncProjection test ledger.
//!
//! Crash / chaos coverage for the `objectlog/hybrid-async` converged plan (bead pqueue-fed791af,
//! parent pqueue-b207e65d; TD-004).
//!
//! The hybrid-async success barrier is: object-log manifest commit (durable) → synchronous in-memory
//! apply/render (the client-visible barrier) → ASYNCHRONOUS SQLite checkpoint that advances the LOGICAL
//! high-water off the hot path. These tests inject a simulated crash at each window along that path and
//! assert the recovery contract holds:
//!
//!   * crash after object-log commit, before the async SQLite apply — the SQLite image lags, so recovery
//!     replays the un-checkpointed log tail (nothing lost);
//!   * crash after in-memory apply, before the async SQLite apply — same lag, plus request-id convergence;
//!   * crash during the SQLite transaction, before the high-water advances — the transaction is atomic, so
//!     NOTHING is applied and NO in-flight lease is orphaned;
//!   * crash after the high-water commit — a re-delivered committed batch is skipped idempotently (no
//!     duplicate item / duplicate lease);
//!   * crash before response delivery — the durable request-id outcome converges on replay (no duplicate
//!     lease minted);
//!   * disk-loss of the SQLite image — the logical high-water resets to genesis so the whole durable log is
//!     replayed and identical state is rebuilt;
//!   * disk-full / repeated apply failure — the worker poisons and FAILS CLOSED, never advancing the
//!     advertised high-water past the poison;
//!   * async apply backlog / backpressure — the debt controller gates new mutations and withholds the
//!     recovery skip-point until the backlog drains.
//!
//! Each window asserts the load-bearing safety invariants: no lost or duplicate leases, no orphaned
//! in-flight (leased) records, and no high-water advance past a poison. The object-log substrate lives in
//! `fireweed-objectlog` (which this crate deliberately does not depend on), so the log side is presented here
//! through the crate-boundary [`LogLineageIdentity`] value the composition builds during `recover`;
//! end-to-end crash coverage over the real `ObjectLog` substrate lives in
//! `fireweed-objectlog/tests/async_projection_chaos.rs` and `fireweed-server/tests/server.rs`.

use fireweed_conformance::{item, qdef, shard, ts};
use fireweed_core::{ItemId, LeaseToken, RequestId};
use fireweed_engine::{
    ClaimCommand, CommandChecksum, CommandEnvelope, CommandId, CommandPosition, EngineError,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, LogLineageIdentity, ProjectionStore,
    PushCommand, QueueCommand, RequestOutcome,
};
use fireweed_sqlite::{
    BackpressureLevel, CheckpointLineage, HybridAsyncDebt, HybridAsyncMonitor,
    HybridAsyncThresholds, HybridProjectionStore, SqliteCheckpointStore,
};

// ---------------------------------------------------------------------------
// Fixtures — mirror `async_projection_checkpoint.rs` / `async_projection_recovery.rs`.
// ---------------------------------------------------------------------------

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

fn push_env(
    id: &str,
    item_id: ItemId,
    key: &str,
    priority: i64,
    created_at: i64,
) -> CommandEnvelope {
    envelope(
        id,
        QueueCommand::Push(PushCommand {
            items: vec![item(&item_id.to_string(), key, priority)],
        }),
        vec![item_id],
        created_at,
    )
}

fn claim_env(id: &str, item_id: ItemId, lease: &LeaseToken, created_at: i64) -> CommandEnvelope {
    envelope(
        id,
        QueueCommand::Claim(ClaimCommand {
            item_ids: vec![item_id],
            lease_token: lease.clone(),
            lease_expires_at: ts(60),
            worker_id: None,
        }),
        vec![item_id],
        created_at,
    )
}

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

fn pos(sequence: u64) -> CommandPosition {
    pos_epoch(0, sequence)
}

fn lineage(epoch: u64, segment: &str) -> CheckpointLineage {
    CheckpointLineage {
        source_epoch: epoch,
        source_segment: segment.to_string(),
    }
}

/// The object-log identity a caught-up log presents for `shard`: `next_seq` commands committed under
/// `epoch`, so the durable head is at `sequence = next_seq - 1` (or `None` for an empty log).
fn identity(epoch: u64, next_seq: u64) -> LogLineageIdentity {
    LogLineageIdentity {
        shard: shard(),
        current_epoch: epoch,
        high_water: (next_seq > 0).then(|| CommandPosition::new(shard(), epoch, next_seq - 1)),
    }
}

fn thresholds() -> HybridAsyncThresholds {
    // Lag hard-limit 100 (soft 75, clear 50); poison after 3 consecutive apply failures.
    HybridAsyncThresholds::new(100, 1_000_000, 100, 60_000, 3).expect("valid thresholds")
}

fn lag(commands: u64) -> HybridAsyncDebt {
    HybridAsyncDebt {
        apply_lag_commands: commands,
        ..Default::default()
    }
}

fn temp_path(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "fireweed-hybrid-async-chaos-{tag}-{}.db",
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

fn leased(store: &SqliteCheckpointStore) -> u64 {
    store
        .projection()
        .export_projection_image(&shard())
        .unwrap()
        .metrics
        .leased
}

fn complete(store: &SqliteCheckpointStore) -> u64 {
    store
        .projection()
        .export_projection_image(&shard())
        .unwrap()
        .metrics
        .complete
}

fn pending(store: &SqliteCheckpointStore) -> u64 {
    store
        .projection()
        .export_projection_image(&shard())
        .unwrap()
        .metrics
        .pending
}

// ---------------------------------------------------------------------------
// Window 1: crash after object-log commit, BEFORE the async SQLite apply.
// ---------------------------------------------------------------------------

/// The object log durably committed a batch but the async checkpoint worker crashed before applying ANY of
/// it. On restart the SQLite image is empty, so the whole committed log tail is un-applied lag and recovery
/// replays it from genesis — nothing is lost, nothing is duplicated.
#[test]
fn async_projection_chaos_crash_after_objectlog_commit_before_apply_replays_full_tail() {
    let path = temp_path("commit-before-apply");
    let committed_head = 2u64; // log durably committed seq 0,1,2 → next_seq 3.

    // A fresh checkpoint store that never ran a checkpoint: the crash happened before the first apply.
    let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    store.create_queue_projection(qdef()).unwrap();

    // The logical high-water is at genesis and every committed command is apply lag.
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(0));
    assert_eq!(
        store
            .apply_lag_commands(&shard(), Some(committed_head))
            .unwrap(),
        committed_head + 1,
        "all committed-but-unapplied commands are lag"
    );
    assert!(store.progress(&shard()).unwrap().lineage.is_none());
    drop(store);

    // Recovery reopens the (empty) SQLite image: it advertises genesis, so the composition replays the whole
    // durable log tail rather than skipping un-applied history. A caught-up log validates cleanly.
    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(0, committed_head + 1))
        .unwrap();
    assert_eq!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).unwrap(),
        None,
        "an un-applied image advertises genesis; the full log tail is replayed (nothing lost)"
    );
    assert_eq!(
        ProjectionStore::metrics(&hybrid, &shard()).unwrap().pending,
        0,
        "no half-applied state before replay: no orphaned in-flight records"
    );
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Window 2: crash after in-memory apply, BEFORE the async SQLite apply.
// ---------------------------------------------------------------------------

/// The success barrier (memory apply) passed and the client saw its response, but the async checkpoint had
/// only absorbed a PREFIX when the crash hit. On restart the SQLite high-water sits at the prefix, the log
/// head is ahead, recovery validates cleanly (SQLite behind log is the normal lagging case), advertises the
/// prefix as the skip-point, and the un-checkpointed tail replays. The checkpointed request-id converges
/// from the durable table.
#[tokio::test]
async fn async_projection_chaos_crash_after_memory_apply_before_sqlite_apply_resumes_at_prefix() {
    let path = temp_path("memory-before-sqlite");
    let first = ItemId::new("1").unwrap();
    let req1 = RequestId::new("req-1").unwrap();

    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        // Log committed 3 pushes (seq 0,1,2). The worker checkpointed only seq 0 before crashing.
        store
            .checkpoint(
                &shard(),
                std::slice::from_ref(&pos(0)),
                &[push_with_request("p1", "req-1", 0x11, first, "k1", 10, 0)],
                &lineage(0, "seg-0000"),
            )
            .await
            .unwrap();
        assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(1));
        // Seq 1 and 2 remain lag against the committed head at seq 2.
        assert_eq!(store.apply_lag_commands(&shard(), Some(2)).unwrap(), 2);
    }

    // Restart: recovery hydrates the prefix, validates against the full log, and advertises seq 0 as the
    // skip-point so only seq 1,2 replay.
    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();
    ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(0, 3)).unwrap();
    assert_eq!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).unwrap(),
        Some(CommandPosition::new(shard(), 0, 0)),
        "resume replay at the checkpointed prefix, not genesis and not past the log"
    );
    assert_eq!(
        ProjectionStore::metrics(&hybrid, &shard()).unwrap().pending,
        1,
        "only the checkpointed prefix is resident before tail replay"
    );

    // The checkpointed request-id outcome converged; the tail (seq 1,2) is rebuilt from the durable log on
    // replay, so nothing committed is lost.
    let reopened = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        reopened.replay_push(&shard(), &req1).unwrap(),
        Some(vec![first]),
        "committed-but-unreturned push converges from the durable idempotency table"
    );
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Window 3: crash DURING the SQLite transaction, before the high-water advances.
// ---------------------------------------------------------------------------

/// A checkpoint that fails mid-transaction (here: a batch that starts past the cursor — a replay gap) is
/// atomic: NOTHING is applied and the logical high-water is untouched. Critically, a claim inside the failed
/// batch does NOT leave an orphaned in-flight lease behind.
#[tokio::test]
async fn async_projection_chaos_crash_during_sqlite_txn_leaves_no_partial_apply_or_orphan_lease() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let one = ItemId::new("1").unwrap();
    let two = ItemId::new("2").unwrap();
    let lease = LeaseToken::new("lease-1").unwrap();

    // Establish a clean prefix: push+claim item 1 (leased), high-water at 2.
    store
        .checkpoint(
            &shard(),
            &[pos(0), pos(1)],
            &[
                push_env("p1", one, "k1", 10, 0),
                claim_env("c1", one, &lease, 1),
            ],
            &lineage(0, "seg-a"),
        )
        .await
        .unwrap();
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(2));
    assert_eq!(leased(&store), 1);

    // A checkpoint whose first position (seq 5) is past the cursor (2) is a hard replay gap: the transaction
    // aborts before applying the push+claim it carries.
    let gapped = [
        push_env("p2", two, "k2", 20, 2),
        claim_env("c2", two, &lease, 3),
    ];
    let err = store
        .checkpoint(&shard(), &[pos(5), pos(6)], &gapped, &lineage(0, "seg-gap"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("checkpoint replay gap")),
        "expected an atomic replay-gap abort, got {err:?}"
    );

    // The failed transaction advanced nothing and orphaned nothing.
    assert_eq!(
        store.logical_high_water(&shard()).unwrap(),
        Some(2),
        "the high-water did not advance across the failed transaction"
    );
    assert_eq!(
        leased(&store),
        1,
        "the failed claim left no orphaned in-flight lease (still exactly the clean prefix's lease)"
    );
    assert_eq!(
        pending(&store),
        0,
        "the gapped push was not partially applied"
    );

    // The correct contiguous batch then applies normally — the item is not lost, no duplicate lease.
    store
        .checkpoint(&shard(), &[pos(2), pos(3)], &gapped, &lineage(0, "seg-b"))
        .await
        .unwrap();
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(4));
    assert_eq!(
        leased(&store),
        2,
        "both distinct items now leased exactly once"
    );
}

// ---------------------------------------------------------------------------
// Window 4: crash AFTER the high-water commit — a re-delivered batch is idempotent.
// ---------------------------------------------------------------------------

/// After the high-water commit but before the worker recorded that the batch was consumed, a restart may
/// re-deliver the already-applied batch. The applied prefix is skipped idempotently: no item is inserted
/// twice, no lease is minted twice.
#[tokio::test]
async fn async_projection_chaos_crash_after_high_water_replays_committed_batch_idempotently() {
    let store = SqliteCheckpointStore::in_memory().unwrap();
    store.create_queue_projection(qdef()).unwrap();
    let one = ItemId::new("1").unwrap();
    let two = ItemId::new("2").unwrap();
    let lease = LeaseToken::new("lease-1").unwrap();
    let batch = vec![
        push_env("p1", one, "k1", 10, 0),
        claim_env("c1", one, &lease, 1),
        push_env("p2", two, "k2", 20, 2),
    ];

    // First delivery applies the whole batch; high-water at 3.
    let first = store
        .checkpoint(
            &shard(),
            &[pos(0), pos(1), pos(2)],
            &batch,
            &lineage(0, "seg-a"),
        )
        .await
        .unwrap();
    assert_eq!(first.logical_high_water, Some(3));
    let leased_once = leased(&store);
    let pending_once = pending(&store);
    assert_eq!((leased_once, pending_once), (1, 1));

    // Re-deliver the identical committed batch (the crash-after-high-water replay). Every position is <=
    // cursor, so the whole prefix is skipped: no double-apply.
    let replay = store
        .checkpoint(
            &shard(),
            &[pos(0), pos(1), pos(2)],
            &batch,
            &lineage(0, "seg-b"),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.logical_high_water,
        Some(3),
        "the high-water did not advance on a fully-overlapping replay"
    );
    assert_eq!(leased(&store), leased_once, "no duplicate lease on replay");
    assert_eq!(pending(&store), pending_once, "no duplicate item on replay");
}

// ---------------------------------------------------------------------------
// Window 5: crash BEFORE response delivery — request-id replay converges.
// ---------------------------------------------------------------------------

/// The command committed and applied (through the high-water) but the server crashed before the client
/// received the response. On restart the durable request-id outcome is replayed: the SAME item ids come back
/// and NO second item / second lease is minted.
#[tokio::test]
async fn async_projection_chaos_crash_before_response_delivery_replays_request_id() {
    let path = temp_path("before-response");
    let item_id = ItemId::new("1").unwrap();
    let request = RequestId::new("req-1").unwrap();

    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        let push = push_with_request("p1", "req-1", 0xABCD, item_id, "k1", 10, 0);
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
        // Crash here — before the client observed the outcome.
    }

    // Restart: the same-request retry replays the original ids from the durable table without appending.
    let reopened = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    assert_eq!(
        reopened.replay_push(&shard(), &request).unwrap(),
        Some(vec![item_id]),
        "the committed-but-unreturned outcome converges to the original ids"
    );
    assert_eq!(
        reopened.logical_high_water(&shard()).unwrap(),
        Some(1),
        "replay does not advance the high-water"
    );
    assert_eq!(pending(&reopened), 1, "replay minted no duplicate item");
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Window 6: disk-loss of the SQLite image — rebuild from the durable log.
// ---------------------------------------------------------------------------

/// The SQLite image file is lost (disk failure / volume wipe). Reopening a fresh file resets the logical
/// high-water to genesis, so recovery replays the whole durable object log. Re-applying the same committed
/// batch (the log replay) rebuilds identical state: same resident set, request-id still converges, no
/// duplicate.
#[tokio::test]
async fn async_projection_chaos_disk_loss_resets_high_water_and_rebuilds_from_log() {
    let path = temp_path("disk-loss");
    let one = ItemId::new("1").unwrap();
    let two = ItemId::new("2").unwrap();
    let lease = LeaseToken::new("lease-1").unwrap();
    let request = RequestId::new("req-1").unwrap();
    // The durable object-log batch (source of truth): push(req)+claim+push.
    let batch = vec![
        push_with_request("p1", "req-1", 0x1, one, "k1", 10, 0),
        claim_env("c1", one, &lease, 1),
        push_env("p2", two, "k2", 20, 2),
    ];

    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        store
            .checkpoint(
                &shard(),
                &[pos(0), pos(1), pos(2)],
                &batch,
                &lineage(0, "seg-a"),
            )
            .await
            .unwrap();
        assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(3));
        assert_eq!((leased(&store), pending(&store)), (1, 1));
    }

    // DISK LOSS: the SQLite image and its WAL/shm sidecars are gone.
    cleanup(&path);

    // Reopen a fresh image: the high-water is back at genesis (no skip-point survives disk loss).
    let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    store.create_queue_projection(qdef()).unwrap();
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(0));
    assert!(store.progress(&shard()).unwrap().lineage.is_none());
    assert_eq!(store.replay_push(&shard(), &request).unwrap(), None);

    // Replaying the durable log from genesis rebuilds identical state.
    store
        .checkpoint(
            &shard(),
            &[pos(0), pos(1), pos(2)],
            &batch,
            &lineage(0, "seg-a"),
        )
        .await
        .unwrap();
    assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(3));
    assert_eq!(
        (leased(&store), pending(&store)),
        (1, 1),
        "genesis replay reconstructs the exact resident set (no lost/duplicate lease)"
    );
    assert_eq!(
        store.replay_push(&shard(), &request).unwrap(),
        Some(vec![one]),
        "the request-id outcome is rebuilt from the log"
    );
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Window 7: disk-full / repeated apply failure — poison, fail closed, no advance.
// ---------------------------------------------------------------------------

/// A disk-full (or otherwise repeatedly-failing) async apply is modelled through the debt controller's
/// apply-failure latch: after the poison threshold of consecutive failures the worker poisons, fails new
/// mutations CLOSED, halts retention, and — the load-bearing invariant — WITHHOLDS the recovery high-water
/// so no reader skips past the un-applied poison.
#[test]
fn async_projection_chaos_disk_full_apply_failure_poisons_and_never_advances_past_poison() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    // A healthy skip-point is advertised while clear.
    let hw = CommandPosition::new(shard(), 0, 41);
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw.clone())),
        Some(hw.clone())
    );

    // Simulate three consecutive "disk full" apply failures — the third trips poison.
    assert!(!monitor.record_checkpoint_error("SQLITE_FULL: database or disk is full"));
    assert!(!monitor.record_checkpoint_error("SQLITE_FULL: database or disk is full"));
    assert!(monitor.record_checkpoint_error("SQLITE_FULL: database or disk is full"));
    assert!(monitor.is_poisoned());

    // Fail closed: new mutations get a Storage poison error (NOT the retryable backpressure error).
    match monitor.admit_mutation() {
        Err(EngineError::Storage(msg)) => assert!(msg.contains("poisoned"), "{msg}"),
        other => panic!("expected Storage poison, got {other:?}"),
    }
    // No high-water advance past poison, and retention is halted.
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw)),
        None,
        "a poisoned worker must not advertise a high-water past the un-applied poison"
    );
    assert!(!monitor.retention_may_advance());
}

/// The projection-side counterpart: a checkpoint store that advanced its logical high-water to N still fails
/// closed on the recovery high-water once the hybrid projection is poisoned (here by a lineage mismatch), so
/// the divergent-but-advanced image is never advertised as a replay-skip point.
#[test]
fn async_projection_chaos_projection_poison_withholds_advanced_high_water() {
    let path = temp_path("proj-poison");
    let item_id = ItemId::new("1").unwrap();
    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        // Checkpointed under object-log epoch 5 → logical high-water 1.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            store
                .checkpoint(
                    &shard(),
                    std::slice::from_ref(&pos_epoch(5, 0)),
                    &[push_with_request(
                        "p1", "req-1", 0xAAAA, item_id, "k1", 10, 0,
                    )],
                    &lineage(5, "s3://log/seg-00000005-0000"),
                )
                .await
                .unwrap();
        });
        assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(1));
    }

    let mut hybrid = HybridProjectionStore::open(path.to_str().unwrap()).unwrap();
    ProjectionStore::ensure_shard(&mut hybrid, &qdef()).unwrap();
    // The log presented at recovery is only at epoch 3 — older than the recorded lineage: the image cannot
    // descend from this log. Validation poisons.
    let err = ProjectionStore::validate_recovery_lineage(&mut hybrid, &identity(3, 1)).unwrap_err();
    assert!(
        matches!(err, EngineError::Storage(ref m) if m.contains("newer than")),
        "expected fail-closed epoch mismatch, got {err:?}"
    );
    // Even though the durable logical high-water is 1, the poisoned projection withholds it (fails closed).
    assert!(
        ProjectionStore::recovery_high_water(&hybrid, &shard()).is_err(),
        "a poisoned projection must not advertise its advanced high-water"
    );
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Window 8: async apply backlog / backpressure.
// ---------------------------------------------------------------------------

/// A growing async-apply backlog crosses the soft then hard debt bands; hard backpressure gates NEW
/// mutations with a retryable error and withholds the recovery skip-point, and the queue only clears once
/// the backlog drains below the clear band AND a clean batch has applied (hysteresis).
#[test]
fn async_projection_chaos_apply_backlog_gates_mutations_and_withholds_high_water_until_drained() {
    let mut monitor = HybridAsyncMonitor::new(thresholds());
    let hw = CommandPosition::new(shard(), 0, 41);

    // Clear: mutations admitted, skip-point advertised.
    assert_eq!(monitor.observe(lag(10), 0), BackpressureLevel::Clear);
    assert!(monitor.admit_mutation().is_ok());
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw.clone())),
        Some(hw.clone())
    );

    // Backlog grows past the soft band: warned but still admits mutations.
    assert_eq!(monitor.observe(lag(80), 1), BackpressureLevel::Soft);
    assert!(monitor.admit_mutation().is_ok(), "soft still admits");

    // Backlog hits the hard band: new mutations rejected retryably, skip-point withheld.
    assert_eq!(monitor.observe(lag(100), 2), BackpressureLevel::Hard);
    assert!(
        matches!(monitor.admit_mutation(), Err(EngineError::Unavailable)),
        "hard backpressure rejects new mutations with a retryable error"
    );
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw.clone())),
        None,
        "the lagging high-water must not be advertised under hard backpressure"
    );

    // Draining below the clear band is not enough on its own — a clean batch must also apply (hysteresis).
    assert_eq!(monitor.observe(lag(40), 3), BackpressureLevel::Hard);
    monitor.record_apply_success();
    assert_eq!(monitor.observe(lag(40), 4), BackpressureLevel::Clear);
    assert!(
        monitor.admit_mutation().is_ok(),
        "drained: mutations admitted again"
    );
    assert_eq!(
        monitor.recovery_high_water_safe(Some(hw)),
        Some(CommandPosition::new(shard(), 0, 41)),
        "the skip-point is advertised again once the backlog has drained"
    );
}

/// A finalize checkpoint that fails mid-transaction leaves a claimed item in-flight but NOT orphaned: on
/// restart the leased record is recoverable and a correct finalize checkpoint drives it terminal exactly
/// once. This is the "no orphaned in-flight record" invariant across a rolled-back finalize window.
#[tokio::test]
async fn async_projection_chaos_rolled_back_finalize_keeps_recoverable_inflight_lease() {
    let path = temp_path("rolled-back-finalize");
    let one = ItemId::new("1").unwrap();
    let lease = LeaseToken::new("lease-1").unwrap();

    {
        let store = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
        store.create_queue_projection(qdef()).unwrap();
        // push + claim → item 1 leased, high-water 2.
        store
            .checkpoint(
                &shard(),
                &[pos(0), pos(1)],
                &[
                    push_env("p1", one, "k1", 10, 0),
                    claim_env("c1", one, &lease, 1),
                ],
                &lineage(0, "seg-a"),
            )
            .await
            .unwrap();

        // A finalize checkpoint that starts past the cursor aborts atomically: the lease stays in-flight.
        let finalize = envelope(
            "f1",
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome::new(one, FinalizeKind::Complete)],
            }),
            vec![one],
            2,
        );
        let err = store
            .checkpoint(
                &shard(),
                std::slice::from_ref(&pos(9)),
                std::slice::from_ref(&finalize),
                &lineage(0, "seg-gap"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Storage(_)));
        assert_eq!(store.logical_high_water(&shard()).unwrap(), Some(2));
        assert_eq!(
            leased(&store),
            1,
            "the in-flight lease survived the failed finalize"
        );
    }

    // Restart: the leased record is recoverable from the durable image (not orphaned, not lost).
    let reopened = SqliteCheckpointStore::open(path.to_str().unwrap()).unwrap();
    assert_eq!(leased(&reopened), 1);
    assert_eq!(complete(&reopened), 0);

    // The correct contiguous finalize then drives it terminal exactly once.
    let finalize = envelope(
        "f1",
        QueueCommand::Finalize(FinalizeCommand {
            outcomes: vec![FinalizeOutcome::new(one, FinalizeKind::Complete)],
        }),
        vec![one],
        2,
    );
    reopened
        .checkpoint(
            &shard(),
            std::slice::from_ref(&pos(2)),
            std::slice::from_ref(&finalize),
            &lineage(0, "seg-b"),
        )
        .await
        .unwrap();
    assert_eq!(reopened.logical_high_water(&shard()).unwrap(), Some(3));
    assert_eq!(leased(&reopened), 0);
    assert_eq!(
        complete(&reopened),
        1,
        "finalized exactly once after recovery"
    );
    cleanup(&path);
}
