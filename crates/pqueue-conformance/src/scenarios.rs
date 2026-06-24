//! The conformance scenarios. Each is generic over a [`ConformanceBackend`](crate::ConformanceBackend)
//! and takes a `make: impl Fn() -> B` factory (some build a second backend for replay reconstruction).
//! Each fails if the port under test returns a default/no-op — the behavioral no-stub proof (plan §6).

use bytes::Bytes;
use pqueue_core::{ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue};
use pqueue_engine::{
    ClaimCommand, CommandPosition, EngineError, FenceLeaseCommand, FinalizeCommand, FinalizeKind,
    FinalizeOutcome, ProjectionSnapshot, PushCommand, QueueCommand, ReplacePendingCommand,
    UnfenceLeaseCommand, UpsertOutcome,
};

// Method calls resolve through the `ConformanceBackend` bound's supertraits, so the individual port
// traits need not be imported here.
use crate::{ConformanceBackend, claim_req, commit, envelope, item, qdef, qkey, shard, ts};

/// Eventual-apply backends MUST refuse upsert (Invariant 2 / TD-007 §2.3: the atomic XDEL+XADD
/// `replace_if_pending` is offered only on the atomic durability class). The refusal is the structured
/// `Unavailable` (RESP `-ERR pqueue unavailable`). Used by the eventual-apply conformance variant in
/// place of the three atomic-class upsert scenarios.
pub async fn upsert_is_unavailable<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let err = b
        .replace_if_pending(
            &shard(),
            &ClientItemKey::new("dup").unwrap(),
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            ts(1),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        EngineError::Unavailable,
        "eventual-apply backends must refuse upsert with Unavailable (Invariant 2)"
    );
}

/// `ProjectionRead::peek` — non-destructive, priority-ordered eligible view (fails if it returns a
/// default/empty no-op).
pub async fn peek_is_priority_ordered_and_nondestructive<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
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
    let views = b.peek(&shard(), 10).await.unwrap();
    let peeked: Vec<&str> = views.iter().map(|v| v.item_id.as_str()).collect();
    assert_eq!(
        peeked,
        vec!["b", "c", "a"],
        "peek is priority-ordered (10,20,30)"
    );
    // Non-destructive: peeking again returns the same items (peek must not consume/claim).
    assert_eq!(
        b.peek(&shard(), 10).await.unwrap().len(),
        3,
        "peek does not consume"
    );
    assert_eq!(
        b.peek(&shard(), 1).await.unwrap().len(),
        1,
        "peek honors the limit"
    );
}

/// `ProjectionRead::pending` — lists in-flight (leased) items (fails on a default/empty no-op).
pub async fn pending_lists_leased_items<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "nothing leased yet"
    );
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending.len(), 1, "the leased item appears in pending");
    assert_eq!(pending[0].item_id.as_str(), "a");
    assert_eq!(pending[0].lease_token.as_str(), "lease-1");
}

/// `SnapshotStore::write_snapshot`/`read_snapshot`/`latest_snapshot` — durable round-trip (fails on a
/// no-op store: latest must reflect the most-recent write and read must return the exact payload).
pub async fn snapshots_write_read_latest<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(
        b.latest_snapshot(&sk).await.unwrap().is_none(),
        "no snapshot yet"
    );
    let pos = CommandPosition::new(sk.clone(), 0, 0);
    let r1 = b
        .write_snapshot(
            &sk,
            pos.clone(),
            ProjectionSnapshot {
                payload: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    let r2 = b
        .write_snapshot(
            &sk,
            pos,
            ProjectionSnapshot {
                payload: vec![4, 5, 6],
            },
        )
        .await
        .unwrap();
    assert_ne!(r1.ref_id, r2.ref_id, "each snapshot gets a distinct ref");
    assert_eq!(
        b.latest_snapshot(&sk)
            .await
            .unwrap()
            .expect("a snapshot exists")
            .ref_id,
        r2.ref_id,
        "latest is the most-recent write"
    );
    assert_eq!(b.read_snapshot(&r1).await.unwrap().payload, vec![1, 2, 3]);
    assert_eq!(b.read_snapshot(&r2).await.unwrap().payload, vec![4, 5, 6]);
}

pub async fn push_then_select_eligible_in_priority_order<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
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

pub async fn claim_then_complete_lifecycle<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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

pub async fn replace_pending_supersedes_old<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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

pub async fn high_water_is_monotonic<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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

pub async fn claim_returns_priority_ordered_rich_items<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
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

pub async fn claim_empty_when_nothing_eligible<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert!(claimed.items.is_empty());
}

pub async fn upsert_inserts_then_replaces_pending<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();

    // First upsert → Inserted with a BACKEND-ASSIGNED id (capture it).
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            ts(1),
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Second upsert (same key) → Replaced; the new id is backend-assigned and supersedes id1.
    let id2 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            ts(2),
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Replaced {
            new_item_id,
            superseded_item_id,
        } => {
            assert_eq!(superseded_item_id, id1, "the first id is superseded");
            assert_ne!(
                new_item_id, id1,
                "the replacement got a fresh backend-assigned id"
            );
            new_item_id
        }
        other => panic!("expected Replaced, got {other:?}"),
    };
    // Only the replacement is eligible; the superseded id is gone.
    let elig = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(elig, vec![id2], "only the replacement is eligible");
}

pub async fn upsert_rejects_claimed_and_terminal<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();
    let id1 = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            ts(1),
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    // Claim it → leased. Upsert must be rejected with Invalid (no transition on in-flight work).
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let err = b
        .replace_if_pending(&shard(), &key, None, None, None, None, ts(20))
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Invalid("collision with claimed item"));

    // Finalize-complete the leased item → terminal. Upsert must then be rejected with Terminal.
    commit(
        &b,
        envelope(
            QueueCommand::Finalize(FinalizeCommand {
                outcomes: vec![FinalizeOutcome {
                    item_id: id1.clone(),
                    kind: FinalizeKind::Complete,
                }],
            }),
            vec![id1],
        ),
    )
    .await;
    let err = b
        .replace_if_pending(&shard(), &key, None, None, None, None, ts(30))
        .await
        .unwrap_err();
    assert_eq!(err, EngineError::Terminal);
}

pub async fn upsert_preserves_group_delay_and_payload_in_claim_shape<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("grouped").unwrap();
    let group = GroupKey::new("group-a").unwrap();

    let assigned = match b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            Some(group.clone()),
            Some(ts(250)),
            Some(Bytes::from_static(b"payload")),
            ts(1),
        )
        .await
        .unwrap()
    {
        UpsertOutcome::Inserted { item_id } => item_id,
        other => panic!("expected Inserted, got {other:?}"),
    };

    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty(),
        "not_before must keep the upserted item out of early claims"
    );

    let claimed = b.claim(claim_req(10, 500, 300)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    let item = &claimed.items[0];
    assert_eq!(
        item.item_id, assigned,
        "the claimed item is the backend-assigned upsert id"
    );
    assert_eq!(item.group_key.as_ref(), Some(&group));
    assert_eq!(item.not_before, Some(ts(250)));
    assert_eq!(item.payload.as_deref(), Some(&b"payload"[..]));
}

pub async fn tick_reclaims_expired_lease_with_no_client_traffic<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
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

    // The reclaimed item is back to pending/eligible. (The reclaim itself does NOT charge an attempt —
    // attempt_count = number of deliveries; a fresh claim of this item would charge the next one.)
    let pending = b.select_eligible(&shard(), ts(300), 10).await.unwrap();
    assert_eq!(
        pending.iter().map(|i| i.as_str()).collect::<Vec<_>>(),
        vec!["a"]
    );
}

pub async fn tick_lease_boundary_is_half_open<B: ConformanceBackend>(make: impl Fn() -> B) {
    // Convention: a lease is valid THROUGH `lease_expires_at`; reclaim fires only at now > exp (B1).
    let b = make();
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
    b.claim(claim_req(10, 100, 10)).await.unwrap(); // lease_expires_at = ts(100)

    // At exactly the expiry instant: lease still held, nothing reclaimed.
    assert_eq!(b.tick(ts(100)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // One unit past expiry: reclaimed.
    assert_eq!(b.tick(ts(101)).await.unwrap().leases_reclaimed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 0);
}

pub async fn paused_queue_yields_no_claims<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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
    // Pause: nothing eligible/claimable.
    commit(&b, envelope(QueueCommand::PauseQueue, vec![])).await;
    assert!(
        b.claim(claim_req(10, 500, 10))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        b.select_eligible(&shard(), ts(10), 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Resume: claimable again.
    commit(&b, envelope(QueueCommand::ResumeQueue, vec![])).await;
    assert_eq!(
        b.claim(claim_req(10, 500, 10)).await.unwrap().items.len(),
        1
    );
}

pub async fn fenced_lease_finalize_is_stale<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
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
    b.claim(claim_req(10, 500, 10)).await.unwrap();
    let id = ItemId::new("a").unwrap();

    // Operator fences the lease.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![id.clone()],
            }),
            vec![id.clone()],
        ),
    )
    .await;
    // The holder's finalize is rejected StaleLease, and nothing is committed (still leased).
    let outcomes = vec![FinalizeOutcome {
        item_id: id.clone(),
        kind: FinalizeKind::Complete,
    }];
    assert_eq!(
        b.finalize(&shard(), outcomes.clone(), ts(20)).await,
        Err(EngineError::StaleLease)
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Operator unfences: finalize now succeeds.
    commit(
        &b,
        envelope(
            QueueCommand::UnfenceLease(UnfenceLeaseCommand {
                item_ids: vec![id.clone()],
            }),
            vec![id.clone()],
        ),
    )
    .await;
    b.finalize(&shard(), outcomes, ts(30)).await.unwrap();
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}

pub async fn renew_extends_lease_and_rejects<B: ConformanceBackend>(make: impl Fn() -> B) {
    // renew_validate MIRRORS finalize_validate: only a live, non-fenced, non-terminal, non-superseded
    // leased item may be renewed; a renew extends the lease WITHOUT charging an attempt (TD-006:129).
    let b = make();
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
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "a" leased, expires at ts(500)
    let id = ItemId::new("a").unwrap();

    // Unknown id -> NotFound, and NOTHING appended (reject before commit, B1).
    let before = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(
        b.renew(
            &shard(),
            vec![ItemId::new("nope").unwrap()],
            ts(2000),
            ts(20)
        )
        .await,
        Err(EngineError::NotFound)
    );
    let after = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(before, after, "rejected renew must NOT append a command");

    // Happy path: extend the lease to ts(2000). Ticking PAST the old expiry (500) reclaims nothing,
    // and the attempt_count is unchanged (renew does not charge a delivery).
    b.renew(&shard(), vec![id.clone()], ts(2000), ts(20))
        .await
        .unwrap();
    assert_eq!(
        b.tick(ts(600)).await.unwrap().leases_reclaimed,
        0,
        "extended lease must not be reclaimed past the OLD expiry"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("item still in-flight");
    assert_eq!(lease.attempt_count, 1, "renew does not charge a delivery");
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "renew extended the lease deadline"
    );

    // A never-leased (Pending) item -> Invalid, same as finalize_validate's catch-all.
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("p", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    assert_eq!(
        b.renew(&shard(), vec![ItemId::new("p").unwrap()], ts(2000), ts(21))
            .await,
        Err(EngineError::Invalid("item is not leased"))
    );

    // Fenced lease -> StaleLease, exactly as finalize_validate rejects it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![id.clone()],
            }),
            vec![id.clone()],
        ),
    )
    .await;
    assert_eq!(
        b.renew(&shard(), vec![id], ts(3000), ts(30)).await,
        Err(EngineError::StaleLease)
    );
}

pub async fn reassign_swaps_token_and_charges_attempt<B: ConformanceBackend>(make: impl Fn() -> B) {
    // Cross-consumer XCLAIM: ReassignLease swaps the lease token to a new consumer AND charges exactly one
    // delivery (TD-006:129). Rejection semantics mirror renew/finalize (validate_leased), appending
    // nothing on reject.
    let b = make();
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
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // "a" leased by "lease-1", attempt_count = 1
    let id = ItemId::new("a").unwrap();
    let new_token = LeaseToken::new("lease-2").unwrap();

    // Unknown id -> NotFound, and NOTHING appended.
    let before = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(
        b.reassign(
            &shard(),
            vec![ItemId::new("nope").unwrap()],
            new_token.clone(),
            ts(2000),
            ts(20)
        )
        .await,
        Err(EngineError::NotFound)
    );
    let after = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(before, after, "rejected reassign must NOT append a command");

    // Happy path: transfer the lease to "lease-2", extend to ts(2000), charge exactly one delivery.
    b.reassign(
        &shard(),
        vec![id.clone()],
        new_token.clone(),
        ts(2000),
        ts(20),
    )
    .await
    .unwrap();
    let lease = b
        .pending(&shard())
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.item_id == id)
        .expect("still in-flight under the new consumer");
    assert_eq!(
        lease.lease_token, new_token,
        "lease transferred to the new consumer"
    );
    assert_eq!(
        lease.attempt_count, 2,
        "reassign charges one delivery (claim=1 + reassign=1)"
    );
    assert_eq!(
        lease.lease_expires_at,
        ts(2000),
        "reassign extended the deadline"
    );
    // The new lease is live: ticking past the OLD expiry (500) reclaims nothing.
    assert_eq!(b.tick(ts(600)).await.unwrap().leases_reclaimed, 0);
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 1);

    // Fenced lease -> StaleLease, exactly as renew/finalize reject it.
    commit(
        &b,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![id.clone()],
            }),
            vec![id.clone()],
        ),
    )
    .await;
    assert_eq!(
        b.reassign(&shard(), vec![id], new_token, ts(3000), ts(30))
            .await,
        Err(EngineError::StaleLease)
    );
}

pub async fn claimed_view_renders_leased_items<B: ConformanceBackend>(make: impl Fn() -> B) {
    // `claimed_view` renders the rich claim shape for currently-leased ids; pending + unknown ids are
    // omitted (the RESP `XCLAIM` reply source).
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5), item("p", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    // Claim only the top-priority item "a" (5 < 9, ascending); "p" stays pending.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let a = ItemId::new("a").unwrap();
    let p = ItemId::new("p").unwrap();

    let view = b
        .claimed_view(&shard(), &[a.clone(), p, ItemId::new("nope").unwrap()])
        .await
        .unwrap();
    assert_eq!(
        view.len(),
        1,
        "only the leased item renders; the pending + unknown ids are omitted"
    );
    assert_eq!(view[0].item_id, a);
    assert_eq!(view[0].lease_token, LeaseToken::new("lease-1").unwrap());
    assert_eq!(view[0].attempt_count, 1);
}

pub async fn finalize_of_nonleased_item_is_rejected_without_appending<B: ConformanceBackend>(
    make: impl Fn() -> B,
) {
    let b = make();
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
    let id = ItemId::new("a").unwrap();
    let before = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    // Item is Pending (never claimed) -> finalize rejected, and NOTHING is appended (no divergence, B1).
    let outcomes = vec![FinalizeOutcome {
        item_id: id,
        kind: FinalizeKind::Complete,
    }];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(10)).await,
        Err(EngineError::Invalid("item is not leased"))
    );
    let after = b
        .read_from(&shard(), None, 1000)
        .await
        .unwrap()
        .entries
        .len();
    assert_eq!(before, after, "rejected finalize must NOT append a command");
}

pub async fn pause_and_fence_reconstruct_from_log<B: ConformanceBackend>(make: impl Fn() -> B) {
    // Backend A: push two items, claim+fence one, leave one pending, pause the queue.
    let a = make();
    a.create_queue(qdef()).await.unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5), item("p", "kp", 9)],
            }),
            vec![],
        ),
    )
    .await;
    a.claim(claim_req(1, 500, 10)).await.unwrap(); // claims "a" (priority 5 < 9)
    let aid = ItemId::new("a").unwrap();
    commit(
        &a,
        envelope(
            QueueCommand::FenceLease(FenceLeaseCommand {
                item_ids: vec![aid.clone()],
            }),
            vec![aid.clone()],
        ),
    )
    .await;
    commit(&a, envelope(QueueCommand::PauseQueue, vec![])).await;

    // Replay A's full log into a fresh backend B (TD-007 §4 replay reconstruction).
    let page = a.read_from(&shard(), None, 1000).await.unwrap();
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    for (_pos, env) in &page.entries {
        let env = env.clone();
        b.write(move |lw, pw| {
            let pos = lw.append(&shard(), std::slice::from_ref(&env))?;
            pw.apply(&pos, std::slice::from_ref(&env))?;
            Ok(())
        })
        .await
        .unwrap();
    }

    // B reconstructed the durable state: pause withholds the pending item, and the fence holds.
    assert!(
        b.claim(claim_req(10, 500, 50))
            .await
            .unwrap()
            .items
            .is_empty(),
        "pause reconstructed"
    );
    let outcomes = vec![FinalizeOutcome {
        item_id: aid,
        kind: FinalizeKind::Complete,
    }];
    assert_eq!(
        b.finalize(&shard(), outcomes, ts(60)).await,
        Err(EngineError::StaleLease),
        "fence reconstructed"
    );
}

pub async fn high_water_advances_on_each_commit<B: ConformanceBackend>(make: impl Fn() -> B) {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let sk = shard();
    assert!(b.high_water(&sk).await.unwrap().is_none(), "no commits yet");
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
    let h1 = b.high_water(&sk).await.unwrap().expect("after push");
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    let h2 = b.high_water(&sk).await.unwrap().expect("after claim");
    assert!(
        h1.precedes(&h2),
        "command_position high-water must advance on each commit (push -> claim)"
    );
}
