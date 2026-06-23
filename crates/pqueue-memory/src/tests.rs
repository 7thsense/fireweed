//! Behavioral conformance for the memory backend. Each test fails if the method under test returns
//! a default/no-op — the behavioral no-stub proof (plan §6) for this backend.

use super::*;
use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModelKind, PriorityTieBreaker,
    RecurrencePolicy, RetryPolicy, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimPort, ClaimRequest, CommandChecksum, ControlPlaneStore,
    FinalizeCommand, FinalizeKind, FinalizeOutcome, ProjectionRead, PushCommand, PushItem,
    QueueCommand, ReclaimDriver, ReplacePendingCommand, SnapshotStore, UpsertOutcome, UpsertPort,
};

fn tenant() -> TenantId {
    TenantId::new("t1").unwrap()
}
fn queue() -> QueueId {
    QueueId::new("q1").unwrap()
}
fn qkey() -> QueueKey {
    QueueKey::new(tenant(), queue())
}
fn shard() -> ShardKey {
    ShardKey::new(tenant(), queue(), ShardId::ZERO)
}
fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: tenant(),
        queue_id: queue(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        shard_count: 1,
    }
}

fn item(id: &str, key: &str, priority: i64) -> PushItem {
    PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id: ItemId::new(id).unwrap(),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: None,
        max_attempts: 3,
        payload: None,
    }
}

fn envelope(command: QueueCommand, item_ids: Vec<ItemId>) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("c"),
        request_id: None,
        shard_id: ShardId::ZERO,
        item_ids,
        command,
        checksum: CommandChecksum(0),
        created_at: ts(0),
    }
}

/// Apply a command through the atomic unit of work (append + apply together).
async fn commit(backend: &MemoryBackend, env: CommandEnvelope) {
    backend
        .write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env))?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .expect("commit");
}

#[tokio::test]
async fn push_then_select_eligible_in_priority_order() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();

    // Push out of priority order: 30, 10, 20.
    let push = QueueCommand::Push(PushCommand {
        items: vec![
            item("a", "ka", 30),
            item("b", "kb", 10),
            item("c", "kc", 20),
        ],
    });
    commit(&b, envelope(push, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<&str> = eligible.iter().map(|i| i.as_str()).collect();
    // Ascending Int64 priority => 10(b), 20(c), 30(a). Fails if select_eligible is a no-op.
    assert_eq!(
        ids,
        vec!["b", "c", "a"],
        "must be priority-ordered, not insertion order"
    );
}

#[tokio::test]
async fn claim_then_complete_lifecycle() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Claim it.
    let claim = QueueCommand::Claim(ClaimCommand {
        item_ids: vec![ItemId::new("a").unwrap()],
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(200),
    });
    commit(&b, envelope(claim, vec![ItemId::new("a").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.leased, 1, "claim must move item to leased");
    assert_eq!(m.pending, 0);
    // Claimed item is no longer eligible.
    assert!(
        b.select_eligible(&shard(), ts(300), 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Complete it.
    let fin = QueueCommand::Finalize(FinalizeCommand {
        outcomes: vec![FinalizeOutcome {
            item_id: ItemId::new("a").unwrap(),
            kind: FinalizeKind::Complete,
        }],
    });
    commit(&b, envelope(fin, vec![ItemId::new("a").unwrap()])).await;

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        m.complete, 1,
        "finalize-complete must move item to complete"
    );
    assert_eq!(m.leased, 0);
}

#[tokio::test]
async fn replace_pending_supersedes_old() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("old", "dup", 5)],
            }),
            vec![],
        ),
    )
    .await;

    // Upsert: same client_item_key replaces the pending item with a new id.
    let replace = QueueCommand::ReplacePending(ReplacePendingCommand {
        client_item_key: ClientItemKey::new("dup").unwrap(),
        superseded_item_id: ItemId::new("old").unwrap(),
        replacement: item("new", "dup", 5),
    });
    commit(&b, envelope(replace, vec![])).await;

    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    let ids: Vec<&str> = eligible.iter().map(|i| i.as_str()).collect();
    assert_eq!(
        ids,
        vec!["new"],
        "superseded old id must not be eligible; new id is"
    );

    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1, "superseded item excluded from counts");
}

#[tokio::test]
async fn high_water_is_monotonic() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let p1 = CommandPosition::new(shard(), 0, 1);
    let p2 = CommandPosition::new(shard(), 0, 2);

    b.set_high_water(&shard(), p2.clone()).await.unwrap();
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2.clone()));
    // Regression must be rejected (TD-007 §4).
    assert_eq!(
        b.set_high_water(&shard(), p1).await,
        Err(EngineError::Invalid("high-water regression"))
    );
    assert_eq!(b.high_water(&shard()).await.unwrap(), Some(p2));
}

#[tokio::test]
async fn manual_clock_and_idgen_are_real() {
    let clock = ManualClock::at(10);
    assert_eq!(clock.now(), ts(10));
    clock.set(20);
    assert_eq!(clock.now(), ts(20));

    let ids = SeqIdGen::default();
    let a = ids.next_item_id();
    let b = ids.next_item_id();
    assert_ne!(
        a.as_str(),
        b.as_str(),
        "ids must be unique, not a no-op constant"
    );
}

// ---------------------------------------------------------------------------
// Phase 1c: ClaimPort / UpsertPort / ReclaimDriver
// ---------------------------------------------------------------------------

fn claim_req(max_items: usize, lease_expires_at: i64, now: i64) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(lease_expires_at),
        now: ts(now),
    }
}

#[tokio::test]
async fn claim_returns_priority_ordered_rich_items() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![
                    item("a", "ka", 30),
                    item("b", "kb", 10),
                    item("c", "kc", 20),
                ],
            }),
            vec![],
        ),
    )
    .await;

    let claimed = b.claim(claim_req(2, 500, 100)).await.unwrap();
    let ids: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["b", "c"],
        "claim must deliver highest priority first"
    );
    // Rich shape populated (would fail if claim returned a stub).
    let first = &claimed.items[0];
    assert_eq!(first.lease_token.as_str(), "lease-1");
    assert_eq!(first.item_version, 2, "claim bumps item_version");
    assert_eq!(first.attempt_count, 1, "first delivery");
    assert_eq!(first.lease_expires_at, ts(500));

    // The unclaimed lowest-priority item remains eligible.
    let remaining = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        remaining.iter().map(|i| i.as_str()).collect::<Vec<_>>(),
        vec!["a"]
    );
}

#[tokio::test]
async fn claim_empty_when_nothing_eligible() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert!(claimed.items.is_empty());
}

#[tokio::test]
async fn upsert_inserts_then_replaces_pending() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();

    let out = b
        .replace_if_pending(
            &shard(),
            &key,
            ItemId::new("i1").unwrap(),
            Some(PriorityValue::Int64(5)),
            None,
            ts(1),
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        UpsertOutcome::Inserted {
            item_id: ItemId::new("i1").unwrap()
        }
    );

    let out = b
        .replace_if_pending(
            &shard(),
            &key,
            ItemId::new("i2").unwrap(),
            Some(PriorityValue::Int64(5)),
            None,
            ts(2),
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        UpsertOutcome::Replaced {
            new_item_id: ItemId::new("i2").unwrap(),
            superseded_item_id: ItemId::new("i1").unwrap(),
        }
    );
    // Only the replacement is eligible; the superseded id is gone.
    let elig = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        elig.iter().map(|i| i.as_str()).collect::<Vec<_>>(),
        vec!["i2"]
    );
}

#[tokio::test]
async fn upsert_rejects_claimed_and_terminal() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();
    b.replace_if_pending(
        &shard(),
        &key,
        ItemId::new("i1").unwrap(),
        Some(PriorityValue::Int64(5)),
        None,
        ts(1),
    )
    .await
    .unwrap();

    // Claim it → leased. Upsert must be rejected with Invalid (no transition on in-flight work).
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            ItemId::new("i2").unwrap(),
            None,
            None,
            ts(20),
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Invalid("collision with claimed item"));

    // Finalize-complete → terminal. Upsert must be rejected with Terminal.
    commit(
        &b,
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: ItemId::new("i1").unwrap(),
                    kind: FinalizeKind::Complete,
                }],
            }),
            vec![ItemId::new("i1").unwrap()],
        ),
    )
    .await;
    let err = b
        .replace_if_pending(
            &shard(),
            &key,
            ItemId::new("i3").unwrap(),
            None,
            None,
            ts(30),
        )
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Terminal);
}

#[tokio::test]
async fn tick_reclaims_expired_lease_with_no_client_traffic() {
    let b = MemoryBackend::new();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim with a lease expiring at t=100.
    b.claim(claim_req(10, 100, 10)).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Before expiry: tick is a no-op.
    let r = b.tick(ts(50)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // After expiry: tick reclaims — WITH ZERO intervening client commands (DoD, TD-007 §3).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 1);
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(m.pending, 1);
    assert_eq!(m.leased, 0);

    // Idempotent: re-ticking at the same time reclaims nothing (item already pending).
    let r = b.tick(ts(200)).await.unwrap();
    assert_eq!(r.leases_reclaimed, 0);

    // Reclaim charged a second attempt (claim=1, reclaim=2).
    let pending = b.select_eligible(&shard(), ts(300), 10).await.unwrap();
    assert_eq!(
        pending.iter().map(|i| i.as_str()).collect::<Vec<_>>(),
        vec!["a"]
    );
}
