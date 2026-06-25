//! Conformance for the sqlite **relational** projection family (`pqueue_items` DB-authoritative).
//!
//! Three layers of evidence:
//!
//! 1. **Full `core_suite!(@atomic)`** (BQ-11b) — every core scenario (claim/lease/eligibility/upsert
//!    included) runs against the relational backend at parity with the in-memory reference, now that the
//!    serialized claim CTE is in place. (group/cohort/gate selection is BQ-14; dup-push idempotency +
//!    tombstone is BQ-11c; the relational-reconnect class is BQ-11d.)
//! 2. **Lifecycle round-trip** (BQ-11a) — each apply arm applied as SQL and observed back through the read
//!    ports, proving the apply-UoW round-trips item state through `pqueue_items` (incl. CohortExpired,
//!    which is not in the core class).
//! 3. **Regression guards** — id-counter restore on reopen + stable-FIFO from the BQ-11a fresh-eyes review.

use bytes::Bytes;
use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
use pqueue_core::{ClientItemKey, GroupKey, ItemId, LeaseToken, PriorityValue, UtcTimestamp};
use pqueue_engine::{
    ClaimPort, CohortExpiredCommand, ControlPlaneStore, FenceLeaseCommand, FinalizeKind,
    FinalizeOutcome, FinalizePort, ProjectionRead, PurgePort, PushCommand, PushItem, PushPort,
    PushSpec, QueueCommand, ReassignLeasePort, ReclaimDriver, RenewLeasePort, UnfenceLeaseCommand,
    UpsertOutcome, UpsertPort,
};
use pqueue_sqlite::SqliteRelationalBackend;

fn make() -> SqliteRelationalBackend {
    SqliteRelationalBackend::in_memory().expect("open in-memory relational backend")
}

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

fn iid(s: &str) -> ItemId {
    ItemId::new(s).unwrap()
}

fn spec(priority: i64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// 1. Full core conformance class — at parity with the in-memory reference (BQ-11b).
// ---------------------------------------------------------------------------

pqueue_conformance::core_suite!(@atomic make);

// ---------------------------------------------------------------------------
// 2. Lifecycle round-trip — every apply arm observed back through the read ports
// ---------------------------------------------------------------------------

/// Push (apply: INSERT pending) then Claim (apply: lease, charge one delivery, version+1).
#[tokio::test]
async fn claim_apply_leases_and_charges_delivery() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();

    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "nothing leased yet"
    );
    let claimed = b.claim(claim_req(10, 500, 10)).await.unwrap();
    assert_eq!(claimed.items.len(), 1);
    assert_eq!(claimed.items[0].lease_token.as_str(), "lease-1");
    assert_eq!(
        claimed.items[0].attempt_count, 1,
        "claim charges one delivery"
    );

    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending.len(), 1, "the leased item appears in pending");
    assert_eq!(pending[0].lease_token.as_str(), "lease-1");
    assert_eq!(pending[0].lease_expires_at, ts(500));
    // No longer eligible while leased.
    assert!(
        b.select_eligible(&shard(), ts(10), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// RenewLease (apply: extend deadline, version+1) — the leased item's expiry moves forward.
#[tokio::test]
async fn renew_extends_lease_deadline() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    let id = claimed.items[0].item_id.clone();

    b.renew(&shard(), vec![id.clone()], ts(900), ts(20))
        .await
        .unwrap();
    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending[0].lease_expires_at, ts(900), "deadline extended");
    assert_eq!(
        pending[0].attempt_count, 1,
        "renew does not charge a delivery"
    );

    // A renew of a non-leased id is rejected (NotFound), nothing changes.
    assert!(
        b.renew(&shard(), vec![iid("nope")], ts(1000), ts(30))
            .await
            .is_err()
    );
}

/// ReassignLease (apply: swap token, charge one delivery) — XCLAIM to a new consumer.
#[tokio::test]
async fn reassign_swaps_token_and_charges_delivery() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    let id = claimed.items[0].item_id.clone();

    b.reassign(
        &shard(),
        vec![id.clone()],
        LeaseToken::new("lease-2").unwrap(),
        ts(800),
        ts(20),
    )
    .await
    .unwrap();
    let pending = b.pending(&shard()).await.unwrap();
    assert_eq!(pending[0].lease_token.as_str(), "lease-2", "token swapped");
    assert_eq!(
        pending[0].attempt_count, 2,
        "reassign charges a re-delivery"
    );
}

/// Finalize Complete / Fail land in the matching terminal metric.
#[tokio::test]
async fn finalize_complete_and_fail_are_terminal() {
    for (kind, expect_complete) in [(FinalizeKind::Complete, true), (FinalizeKind::Fail, false)] {
        let b = make();
        b.create_queue(qdef()).await.unwrap();
        b.push(&shard(), vec![PushSpec::default()], ts(0))
            .await
            .unwrap();
        let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
        let id = claimed.items[0].item_id.clone();
        b.finalize(
            &shard(),
            vec![FinalizeOutcome { item_id: id, kind }],
            ts(20),
        )
        .await
        .unwrap();

        let m = b.metrics(&qkey()).await.unwrap();
        if expect_complete {
            assert_eq!((m.complete, m.failed, m.leased), (1, 0, 0));
        } else {
            assert_eq!((m.complete, m.failed, m.leased), (0, 1, 0));
        }
    }
}

/// Finalize Retry returns to pending under the bound and goes terminal once deliveries are exhausted.
#[tokio::test]
async fn finalize_retry_respects_attempt_bound() {
    let b = make();
    b.create_queue(qdef()).await.unwrap(); // qdef max_attempts = 3
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();

    // Deliveries 1 and 2: retry returns the item to pending (claimable again).
    for attempt in 1..=2 {
        let claimed = b.claim(claim_req(1, 500, 10 * attempt)).await.unwrap();
        let id = claimed.items[0].item_id.clone();
        b.finalize(
            &shard(),
            vec![FinalizeOutcome {
                item_id: id,
                kind: FinalizeKind::Retry,
            }],
            ts(10 * attempt),
        )
        .await
        .unwrap();
        assert_eq!(
            b.metrics(&qkey()).await.unwrap().pending,
            1,
            "back to pending under the bound"
        );
    }
    // Delivery 3 exhausts the bound: retry goes terminal (Failed).
    let claimed = b.claim(claim_req(1, 500, 100)).await.unwrap();
    let id = claimed.items[0].item_id.clone();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: id,
            kind: FinalizeKind::Retry,
        }],
        ts(100),
    )
    .await
    .unwrap();
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.pending, m.failed),
        (0, 1),
        "retry beyond max_attempts is terminal"
    );
}

/// Finalize Release returns to pending (no fault, no terminal); Rearm returns to pending and resets the
/// delivery count (recurrence).
#[tokio::test]
async fn finalize_release_and_rearm_return_to_pending() {
    // Release -> pending, delivery count preserved.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    let id = b.claim(claim_req(1, 500, 10)).await.unwrap().items[0]
        .item_id
        .clone();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: id,
            kind: FinalizeKind::Release,
        }],
        ts(20),
    )
    .await
    .unwrap();
    let next = b.claim(claim_req(1, 500, 30)).await.unwrap();
    assert_eq!(
        next.items[0].attempt_count, 2,
        "release preserves the delivery count"
    );

    // Rearm -> pending, delivery count reset to 0.
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    let id = b.claim(claim_req(1, 500, 10)).await.unwrap().items[0]
        .item_id
        .clone();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: id,
            kind: FinalizeKind::Rearm,
        }],
        ts(20),
    )
    .await
    .unwrap();
    let next = b.claim(claim_req(1, 500, 30)).await.unwrap();
    assert_eq!(
        next.items[0].attempt_count, 1,
        "rearm resets the delivery count"
    );
}

/// LeaseExpired (fired by the reclaim tick) returns an expired lease to pending with no delivery charge.
#[tokio::test]
async fn tick_reclaims_expired_lease() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // leased through ts(500)

    // Before expiry: nothing reclaimed (half-open — valid through the deadline).
    assert!(b.tick(ts(500)).await.unwrap().is_empty());
    // After expiry: reclaimed back to pending.
    let report = b.tick(ts(501)).await.unwrap();
    assert_eq!(report.leases_reclaimed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    assert!(
        b.pending(&shard()).await.unwrap().is_empty(),
        "no longer leased"
    );
}

/// FenceLease blocks finalize (StaleLease); UnfenceLease restores it.
#[tokio::test]
async fn fence_then_unfence_gate_finalize() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0))
        .await
        .unwrap();
    let id = b.claim(claim_req(1, 500, 10)).await.unwrap().items[0]
        .item_id
        .clone();

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
    assert!(
        b.finalize(
            &shard(),
            vec![FinalizeOutcome {
                item_id: id.clone(),
                kind: FinalizeKind::Complete
            }],
            ts(20)
        )
        .await
        .is_err(),
        "fenced lease cannot be finalized"
    );

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
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: id,
            kind: FinalizeKind::Complete,
        }],
        ts(30),
    )
    .await
    .expect("unfenced lease finalizes");
    assert_eq!(b.metrics(&qkey()).await.unwrap().complete, 1);
}

/// CohortExpired forces every non-terminal member of a group to terminal (cohort-incomplete).
#[tokio::test]
async fn cohort_expired_fails_group_members() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let group = GroupKey::new("g1").unwrap();
    let grouped = |id: &str, key: &str, p: i64| PushItem {
        client_item_key: ClientItemKey::new(key).unwrap(),
        item_id: iid(id),
        priority: Some(PriorityValue::Int64(p)),
        not_before: None,
        group_key: Some(group.clone()),
        max_attempts: 3,
        payload: None,
    };
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![grouped("a", "ka", 5), grouped("b", "kb", 6)],
            }),
            vec![],
        ),
    )
    .await;
    b.claim(claim_req(1, 500, 10)).await.unwrap(); // lease one member

    commit(
        &b,
        envelope(
            QueueCommand::CohortExpired(CohortExpiredCommand { group_key: group }),
            vec![],
        ),
    )
    .await;
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!(
        (m.failed, m.pending, m.leased),
        (2, 0, 0),
        "all members forced terminal"
    );
}

/// PurgeItems hard-deletes present items (XDEL), de-dups, and gates a leased item behind `force`.
#[tokio::test]
async fn purge_removes_items_and_gates_leased() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    commit(
        &b,
        envelope(
            QueueCommand::Push(PushCommand {
                items: vec![item("a", "ka", 5), item("b", "kb", 6)],
            }),
            vec![],
        ),
    )
    .await;
    // Purge a pending item (no force needed); repeated id counts once.
    let removed = b
        .purge(&shard(), vec![iid("a"), iid("a")], false, ts(20))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);

    // Lease "b", then a non-forced purge is gated; forced purge removes it.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert!(
        b.purge(&shard(), vec![iid("b")], false, ts(30))
            .await
            .is_err(),
        "leased purge needs force"
    );
    assert_eq!(
        b.purge(&shard(), vec![iid("b")], true, ts(31))
            .await
            .unwrap(),
        1
    );
    let m = b.metrics(&qkey()).await.unwrap();
    assert_eq!((m.pending, m.leased), (0, 0));
}

/// UpsertPort: insert on a fresh key, replace a pending item under the same key, reject collisions with
/// claimed/terminal items (the dup-push idempotency hardening is BQ-11c).
#[tokio::test]
async fn upsert_inserts_then_replaces_then_rejects() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let key = ClientItemKey::new("dup").unwrap();

    let UpsertOutcome::Inserted { item_id: first } = b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            Some(Bytes::from_static(b"v1")),
            ts(0),
        )
        .await
        .unwrap()
    else {
        panic!("first upsert inserts");
    };

    // Same key, still pending -> replace.
    let UpsertOutcome::Replaced {
        superseded_item_id, ..
    } = b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            Some(Bytes::from_static(b"v2")),
            ts(1),
        )
        .await
        .unwrap()
    else {
        panic!("second upsert replaces the pending item");
    };
    assert_eq!(superseded_item_id, first);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "superseded predecessor excluded"
    );

    // Claim the active item -> a further upsert collides with a claimed item.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert!(
        b.replace_if_pending(&shard(), &key, None, None, None, None, ts(2))
            .await
            .is_err(),
        "upsert collides with a claimed item"
    );
}

/// BQ-11c: duplicate-push convergence across a purge (TD-002 `pqueue_item_key_retention`). After a
/// TERMINAL item under a key is purged, a re-push of the same key is still rejected as a duplicate
/// (`Terminal`) until `client_item_key_retention_ms` elapses — it cannot resurrect the completed work.
#[tokio::test]
async fn purged_terminal_key_is_retained_against_repush() {
    let b = make();
    b.create_queue(qdef()).await.unwrap(); // client_item_key_retention_ms = 60_000
    let key = ClientItemKey::new("rk").unwrap();

    // Insert, claim, complete -> terminal; then purge the terminal item (no force: it is not leased).
    let UpsertOutcome::Inserted { item_id } = b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(5)),
            None,
            None,
            None,
            ts(0),
        )
        .await
        .unwrap()
    else {
        panic!("insert");
    };
    b.claim(claim_req(1, 500, 1)).await.unwrap();
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: item_id.clone(),
            kind: FinalizeKind::Complete,
        }],
        ts(2),
    )
    .await
    .unwrap();
    assert_eq!(
        b.purge(&shard(), vec![item_id], false, ts(3))
            .await
            .unwrap(),
        1
    );

    // Re-push the SAME key within retention (now=10 << expiry 3s + 60s): still a duplicate, not a new item.
    assert!(
        b.replace_if_pending(&shard(), &key, None, None, None, None, ts(10))
            .await
            .is_err(),
        "purged terminal key is retained -> re-push rejected as a duplicate"
    );

    // After retention expires (now beyond 3s + 60s), the key is reusable: a fresh insert succeeds.
    let outcome = b
        .replace_if_pending(
            &shard(),
            &key,
            Some(PriorityValue::Int64(7)),
            None,
            None,
            None,
            ts(70),
        )
        .await
        .unwrap();
    assert!(
        matches!(outcome, UpsertOutcome::Inserted { .. }),
        "after retention elapses the key is freely reusable"
    );
}

// ---------------------------------------------------------------------------
// 3. Regression guards from the BQ-11a fresh-eyes review
// ---------------------------------------------------------------------------

/// F1: the server id counter is restored on reopen, so a push after reconnect does not re-mint an item
/// id that already exists in the durable projection (PushPort restart-safety). DB-authoritative: the
/// committed items are present after reopen with no log replay.
#[tokio::test]
async fn reopen_restores_id_counter_and_state() {
    let path = std::env::temp_dir()
        .join(format!("pqueue-rel-reopen-{}.db", std::process::id()))
        .to_str()
        .unwrap()
        .to_string();
    let _ = std::fs::remove_file(&path);

    {
        let a = SqliteRelationalBackend::open(&path).unwrap();
        a.create_queue(qdef()).await.unwrap();
        a.push(&shard(), vec![spec(10), spec(30)], ts(0))
            .await
            .unwrap();
    } // drop = simulated crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    // A fresh push must NOT collide on the primary key with the reopened rows.
    let new_ids = b.push(&shard(), vec![spec(20)], ts(1)).await.unwrap();
    assert_eq!(new_ids.len(), 1);

    // All three committed items present, in ascending priority order (10, 20, 30) — no log replay.
    let eligible = b.select_eligible(&shard(), ts(100), 10).await.unwrap();
    assert_eq!(
        eligible.len(),
        3,
        "two reopened + one new, none lost or collided"
    );
    let prios: Vec<Option<PriorityValue>> = b
        .peek(&shard(), 10)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.priority)
        .collect();
    assert_eq!(
        prios,
        vec![
            Some(PriorityValue::Int64(10)),
            Some(PriorityValue::Int64(20)),
            Some(PriorityValue::Int64(30)),
        ],
        "priority order preserved across reopen + new push"
    );
    let _ = std::fs::remove_file(&path);
}

/// F3: the FIFO tiebreak is a STABLE per-item insertion sequence, not a mutation-advanced one. A
/// released equal-priority item keeps its original eligibility slot (ahead of a later-inserted peer),
/// matching the in-memory `created_seq` — unlike `last_command_sequence`, which the release would bump.
#[tokio::test]
async fn released_item_keeps_its_fifo_slot() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    // Two equal-priority items: first-inserted is "rel-0-0", second is "rel-0-1".
    b.push(&shard(), vec![spec(5), spec(5)], ts(0))
        .await
        .unwrap();
    let order: Vec<String> = b
        .select_eligible(&shard(), ts(100), 10)
        .await
        .unwrap()
        .iter()
        .map(|i| i.as_str().to_string())
        .collect();
    let first = order[0].clone();

    // Claim + release the FIRST item (its last_command_sequence advances well past the second item's).
    let id = b.claim(claim_req(1, 500, 10)).await.unwrap().items[0]
        .item_id
        .clone();
    assert_eq!(id.as_str(), first, "claim takes the FIFO head");
    b.finalize(
        &shard(),
        vec![FinalizeOutcome {
            item_id: id,
            kind: FinalizeKind::Release,
        }],
        ts(20),
    )
    .await
    .unwrap();

    // It must return to the HEAD of the equal-priority FIFO, not behind the second item.
    let after: Vec<String> = b
        .select_eligible(&shard(), ts(100), 10)
        .await
        .unwrap()
        .iter()
        .map(|i| i.as_str().to_string())
        .collect();
    assert_eq!(
        after, order,
        "released item keeps its original FIFO position"
    );
}
