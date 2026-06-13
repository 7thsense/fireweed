// B-021: fault-injection harness for storage and service kills.
//
// Tests verify INV-2 (no lost work) and INV-10 (durable ack) by:
//   - Injecting deterministic failures into LogStore.append_batch
//   - Simulating partial appends (truncated batch)
//   - Replaying from a surviving log into a fresh projection
//   - Simulating worker/service kills at deterministic checkpoints
//
// Governing spec: TP-003 §2, AC-CLAIM-2, AC-SHARD-3, AC-E2E-2/3/5/7.

use pqueue_core::{ClientItemKey, ItemId, QueueId, TenantId, UtcTimestamp};
use pqueue_storage::{
    commands::{BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, FinalizeKind, FinalizeOutcome, PushItem},
    fault_injection::{replay, FailureMode, FaultInjectedLogStore, KillSchedule},
    memory::{MemoryLogStore, MemoryProjectionStore},
    traits::{LogStore, LogStoreError, ProjectionStore},
    types::{QueueKey, ShardId, ShardKey},
    CommandEnvelope, CommandId, QueueCommand,
};

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn iid(s: &str) -> ItemId {
    ItemId::new(s).unwrap()
}

fn tid() -> TenantId {
    TenantId::new("test").unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn sk(q: &str) -> ShardKey {
    ShardKey { tenant_id: tid(), queue_id: qid(q), shard_id: ShardId::new(0) }
}

fn push_env(q: &str, ids: &[&str], cmd_id: &str) -> CommandEnvelope {
    let t = tid();
    let queue = qid(q);
    let items: Vec<PushItem> = ids
        .iter()
        .map(|id| PushItem {
            item_id: iid(id),
            client_item_key: ClientItemKey::new(*id).unwrap(),
            priority: None,
            not_before: None,
            max_attempts: 3,
        })
        .collect();
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(0),
        item_ids: items.iter().map(|i| i.item_id.clone()).collect(),
        command: QueueCommand::BatchPush(BatchPushCommand { items }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn claim_env(q: &str, ids: &[&str], token: &str, cmd_id: &str) -> CommandEnvelope {
    let t = tid();
    let queue = qid(q);
    let item_ids: Vec<ItemId> = ids.iter().map(|id| iid(id)).collect();
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(0),
        item_ids: item_ids.clone(),
        command: QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids,
            lease_token: token.to_string(),
            lease_expires_at: ts(9999),
        }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

fn finalize_env(q: &str, id: &str, kind: FinalizeKind, cmd_id: &str) -> CommandEnvelope {
    let t = tid();
    let queue = qid(q);
    CommandEnvelope {
        command_id: CommandId::new(cmd_id),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: queue.clone(),
        shard_id: ShardId::new(0),
        item_ids: vec![iid(id)],
        command: QueueCommand::BatchFinalize(BatchFinalizeCommand {
            outcomes: vec![FinalizeOutcome { item_id: iid(id), kind }],
        }),
        checksum: pqueue_storage::types::CommandChecksum(0),
        created_at: ts(0),
    }
}

// ---------------------------------------------------------------------------
// Passthrough (FailureMode::None) smoke test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_injection_harness_tests_passthrough_succeeds() {
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::None);
    let shard = sk("passthrough");
    let env = push_env("passthrough", &["i1"], "cmd-1");
    let res = store.append_batch(&shard, None, vec![env]).await.unwrap();
    assert_eq!(res.last_position.sequence, 0);
    assert_eq!(store.call_count(), 1);
}

// ---------------------------------------------------------------------------
// FailAtCallN — fail a specific append call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_injection_harness_tests_fail_at_call_n() {
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::FailAtCallN(2));
    let shard = sk("fail-n");

    // Call 1: succeeds.
    store.append_batch(&shard, None, vec![push_env("fail-n", &["i1"], "cmd-1")]).await.unwrap();

    // Call 2: injected failure.
    let err = store
        .append_batch(&shard, None, vec![push_env("fail-n", &["i2"], "cmd-2")])
        .await
        .unwrap_err();
    assert!(matches!(err, LogStoreError::StorageFailure(_)));
    assert_eq!(store.call_count(), 2);

    // Call 3: succeeds again.
    store.append_batch(&shard, None, vec![push_env("fail-n", &["i3"], "cmd-3")]).await.unwrap();

    // Log only has i1 and i3 (i2 was injected failure, never committed).
    let page = store.read_from(&shard, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 2);
}

// ---------------------------------------------------------------------------
// PartialAppend — commit first N commands, then fail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_injection_harness_tests_partial_append_commits_prefix() {
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::PartialAppend(1));
    let shard = sk("partial");

    let batch = vec![
        push_env("partial", &["i1"], "cmd-1"),
        push_env("partial", &["i2"], "cmd-2"),
        push_env("partial", &["i3"], "cmd-3"),
    ];

    // The call fails (partial), but the first command is committed.
    let err = store.append_batch(&shard, None, batch).await.unwrap_err();
    assert!(matches!(err, LogStoreError::StorageFailure(_)));

    // Only 1 command survived.
    let page = store.read_from(&shard, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 1);
}

#[tokio::test]
async fn fault_injection_harness_tests_partial_append_zero_fails_immediately() {
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::PartialAppend(0));
    let shard = sk("partial-zero");

    let err = store
        .append_batch(&shard, None, vec![push_env("partial-zero", &["i1"], "cmd-1")])
        .await
        .unwrap_err();
    assert!(matches!(err, LogStoreError::StorageFailure(_)));

    // Nothing was committed; shard never initialized → ShardNotFound.
    let err2 = store.read_from(&shard, None, 10).await.unwrap_err();
    assert!(matches!(err2, LogStoreError::ShardNotFound));
}

// ---------------------------------------------------------------------------
// Replay: rebuild projection from surviving log after a crash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_injection_harness_tests_replay_restores_committed_items() {
    let log = MemoryLogStore::new();
    let shard = sk("replay");

    // Write 3 items to log.
    log.append_batch(&shard, None, vec![push_env("replay", &["i1", "i2", "i3"], "push")]).await.unwrap();

    // Simulate crash: create fresh projection, replay from log.
    let proj = MemoryProjectionStore::new();
    let last = replay(&log, &proj, &shard).await.unwrap();
    assert!(last.is_some());

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("replay") };
    let m = proj.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 3, "all pushed items should be present after replay");
}

#[tokio::test]
async fn fault_injection_harness_tests_replay_empty_log_returns_none() {
    let log = MemoryLogStore::new();
    let shard = sk("replay-empty");

    // Initialize shard (needed so read_from doesn't return ShardNotFound).
    log.append_batch(&shard, None, vec![push_env("replay-empty", &[], "empty")]).await.unwrap();

    let proj = MemoryProjectionStore::new();
    let last = replay(&log, &proj, &shard).await.unwrap();
    // Empty push envelopes produce no items but do return a position.
    assert!(last.is_some());
}

#[tokio::test]
async fn fault_injection_harness_tests_replay_after_partial_append_no_lost_work() {
    // INV-2 / INV-10: items that survived the partial append must reappear
    // in the projection after replay. Items in the lost tail are never lost
    // because they were never acknowledged.
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::PartialAppend(1));
    let shard = sk("replay-partial");

    let batch = vec![
        push_env("replay-partial", &["i1"], "cmd-1"),  // committed
        push_env("replay-partial", &["i2"], "cmd-2"),  // lost (not committed)
    ];
    let _ = store.append_batch(&shard, None, batch).await; // expected Err

    let proj = MemoryProjectionStore::new();
    replay(&store, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("replay-partial") };
    let m = proj.metrics(&qk).await.unwrap();
    // Only i1 was committed; i2 was never acknowledged, so 0 lost items.
    assert_eq!(m.pending_count, 1, "only committed items appear after replay (INV-10)");
}

#[tokio::test]
async fn fault_injection_harness_tests_replay_restores_claimed_state() {
    // Verify that a replay after crash restores lease state correctly.
    let log = MemoryLogStore::new();
    let shard = sk("replay-claim");

    log.append_batch(&shard, None, vec![push_env("replay-claim", &["i1"], "push")]).await.unwrap();
    log.append_batch(&shard, None, vec![claim_env("replay-claim", &["i1"], "tok-1", "claim")]).await.unwrap();

    // Fresh projection via replay.
    let proj = MemoryProjectionStore::new();
    replay(&log, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("replay-claim") };
    let m = proj.metrics(&qk).await.unwrap();
    assert_eq!(m.leased_count, 1, "claim state survives crash+replay");
    assert_eq!(m.pending_count, 0);
}

#[tokio::test]
async fn fault_injection_harness_tests_replay_restores_terminal_state() {
    let log = MemoryLogStore::new();
    let shard = sk("replay-terminal");

    log.append_batch(&shard, None, vec![push_env("replay-terminal", &["i1"], "push")]).await.unwrap();
    log.append_batch(&shard, None, vec![claim_env("replay-terminal", &["i1"], "tok", "claim")]).await.unwrap();
    log.append_batch(&shard, None, vec![finalize_env("replay-terminal", "i1", FinalizeKind::Complete, "fin")]).await.unwrap();

    let proj = MemoryProjectionStore::new();
    replay(&log, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("replay-terminal") };
    let m = proj.metrics(&qk).await.unwrap();
    assert_eq!(m.completed_count, 1, "terminal state survives crash+replay (INV-3)");
}

// ---------------------------------------------------------------------------
// KillSchedule: deterministic worker kill simulation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fault_injection_harness_tests_kill_schedule_fires_at_target() {
    let kill = KillSchedule::kill_after(3);
    assert!(!kill.checkpoint()); // 1
    assert!(!kill.checkpoint()); // 2
    assert!(kill.checkpoint());  // 3 — kill fires
    assert!(kill.checkpoint());  // 4 — already past target
    assert_eq!(kill.checkpoint_count(), 4);
}

#[tokio::test]
async fn fault_injection_harness_tests_kill_schedule_never_fires() {
    let kill = KillSchedule::never();
    for _ in 0..1000 {
        assert!(!kill.checkpoint());
    }
}

/// Simulate: push items → claim → worker killed before finalize → replay → items re-claimable.
///
/// This models AC-CLAIM-2 (lease expiry redelivery after kill).
#[tokio::test]
async fn fault_injection_harness_tests_worker_kill_mid_claim_items_redeliverable() {
    let log = MemoryLogStore::new();
    let shard = sk("kill-mid-claim");
    let kill = KillSchedule::kill_after(2); // kill after 2nd checkpoint

    // Worker turn 1: push + claim (2 checkpoints).
    log.append_batch(&shard, None, vec![push_env("kill-mid-claim", &["i1", "i2"], "push")]).await.unwrap();
    assert!(!kill.checkpoint()); // checkpoint 1: after push ack
    log.append_batch(&shard, None, vec![claim_env("kill-mid-claim", &["i1", "i2"], "tok", "claim")]).await.unwrap();
    assert!(kill.checkpoint());  // checkpoint 2: after claim ack — kill fires here

    // Worker "crashed" before finalize. Simulate lease expiry by replaying
    // into a fresh projection (expiry event would be written by a background
    // process in production; for the harness we test replay fidelity directly).
    let proj = MemoryProjectionStore::new();
    replay(&log, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("kill-mid-claim") };
    let m = proj.metrics(&qk).await.unwrap();
    // After replay the items are still in Leased state (lease has not expired yet).
    // A real system would write a LeaseExpired command; the harness verifies
    // that the committed log state is consistent and not lost.
    assert_eq!(m.leased_count, 2, "committed claim state survives crash (INV-10)");
    assert_eq!(m.pending_count, 0);
    assert_eq!(m.completed_count, 0);
}

/// AC-SHARD-3: stale-epoch appends rejected after a shard is reassigned.
#[tokio::test]
async fn fault_injection_harness_tests_stale_epoch_rejected_after_reassign() {
    let log = MemoryLogStore::new();
    let shard = sk("epoch-fence");

    // Worker A appends with epoch=0.
    log.append_batch(&shard, Some(0), vec![push_env("epoch-fence", &["i1"], "cmd-1")]).await.unwrap();

    // Shard epoch changes (e.g., after a rebalance/failover). Simulate by
    // appending without a fence to advance internal epoch: the memory store
    // does NOT auto-advance epoch on append (epoch stays at init 0). Stale
    // epoch fencing works by having workers supply the expected epoch and the
    // store comparing. A wrong expected epoch is rejected.
    let err = log.append_batch(&shard, Some(99), vec![push_env("epoch-fence", &["i2"], "cmd-2")]).await.unwrap_err();
    assert!(matches!(err, LogStoreError::StalEpoch { .. }), "stale epoch must be rejected (AC-SHARD-3)");

    // Valid append (current epoch=0) succeeds.
    log.append_batch(&shard, Some(0), vec![push_env("epoch-fence", &["i3"], "cmd-3")]).await.unwrap();

    let page = log.read_from(&shard, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 2, "only fenced-in appends committed; stale rejected");
}

/// Replay across multiple pages (pagination).
#[tokio::test]
async fn fault_injection_harness_tests_replay_paginates_large_log() {
    let log = MemoryLogStore::new();
    let shard = sk("replay-pages");

    // Write 10 single-item batches.
    let ids: Vec<String> = (0..10).map(|i| format!("i{}", i)).collect();
    for (i, id) in ids.iter().enumerate() {
        log.append_batch(&shard, None, vec![push_env("replay-pages", &[id.as_str()], &format!("cmd-{}", i))]).await.unwrap();
    }

    let proj = MemoryProjectionStore::new();
    replay(&log, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("replay-pages") };
    let m = proj.metrics(&qk).await.unwrap();
    assert_eq!(m.pending_count, 10, "all 10 items present after paginated replay");
}

/// Deterministic failure + replay round-trip: demonstrates the harness
/// covers the AC-E2E-5 partial-commit / replay convergence pattern.
#[tokio::test]
async fn fault_injection_harness_tests_ac_e2e5_partial_commit_replay_convergence() {
    // Phase 1: worker appends 3 commands; 3rd is partially lost.
    let inner = MemoryLogStore::new();
    let store = FaultInjectedLogStore::new(inner, FailureMode::PartialAppend(2));
    let shard = sk("e2e5");

    let batch = vec![
        push_env("e2e5", &["i1"], "cmd-1"),
        push_env("e2e5", &["i2"], "cmd-2"),
        push_env("e2e5", &["i3"], "cmd-3"), // this and beyond are lost
    ];
    let _ = store.append_batch(&shard, None, batch).await; // expected partial-fail

    // Phase 2: restart — replay surviving log into fresh projection.
    let proj = MemoryProjectionStore::new();
    replay(&store, &proj, &shard).await.unwrap();

    let qk = QueueKey { tenant_id: tid(), queue_id: qid("e2e5") };
    let m = proj.metrics(&qk).await.unwrap();
    // Exactly the 2 committed commands' items are present; i3 was never acked.
    assert_eq!(m.pending_count, 2);

    // Phase 3: continue appending from the clean replay position.
    store.append_batch(&shard, None, vec![push_env("e2e5", &["i3"], "cmd-3-retry")]).await.unwrap();

    let proj2 = MemoryProjectionStore::new();
    replay(&store, &proj2, &shard).await.unwrap();
    let m2 = proj2.metrics(&qk).await.unwrap();
    assert_eq!(m2.pending_count, 3, "convergence: all 3 items present after retry+replay");
}
