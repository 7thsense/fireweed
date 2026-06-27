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

use std::collections::BTreeMap;

use bytes::Bytes;
use pqueue_conformance::{claim_req, commit, envelope, item, qdef, qkey, shard};
use pqueue_core::{
    ClientItemKey, CohortPolicy, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition,
    UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    ActiveScope, ClaimCompatibility, ClaimPort, ClaimRequest, CohortExpiredCommand,
    ControlPlaneStore, DiscoveryGranularity, DiscoveryPort, FenceLeaseCommand, FinalizeKind,
    FinalizeOutcome, FinalizePort, GroupBatching, ProjectionRead, PurgePort, PushCommand, PushItem,
    PushPort, PushSpec, QueueCommand, ReassignLeasePort, ReclaimDriver, RenewLeasePort,
    SetGatesCommand, UnfenceLeaseCommand, UpsertOutcome, UpsertPort,
};
use pqueue_sqlite::SqliteRelationalBackend;

/// A queue definition with a group-size bound (so `group_batching` validates) — clones the conformance
/// default and sets `max_eligible_group_size`.
fn qdef_groups(max_group_size: u64) -> QueueDefinition {
    QueueDefinition {
        max_eligible_group_size: Some(max_group_size),
        ..qdef()
    }
}

/// A grouped push spec (priority + group_key).
fn gspec(priority: i64, group: &str) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        ..Default::default()
    }
}

/// A cohort-member push spec (group_key + declared cohort_size).
fn cspec(priority: i64, group: &str, cohort_size: u64) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        cohort_size: Some(cohort_size),
        ..Default::default()
    }
}

/// A queue definition with cohorts enabled (so `whole_cohort` validates).
fn qdef_cohort() -> QueueDefinition {
    QueueDefinition {
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: None,
            max_cohort_size: Some(10),
        }),
        ..qdef()
    }
}

/// A claim request carrying compatibility options (group_batching / same_group_key).
fn claim_req_compat(
    max: usize,
    exp: i64,
    now: i64,
    compatibility: ClaimCompatibility,
) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("w1").unwrap(),
        max_items: max,
        lease_token: LeaseToken::new("lease-1").unwrap(),
        lease_expires_at: ts(exp),
        now: ts(now),
        compatibility,
        expected_epoch: None,
    }
}

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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    let id = claimed.items[0].item_id.clone();

    b.renew(&shard(), vec![id.clone()], ts(900), ts(20), None)
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
        b.renew(&shard(), vec![iid("nope")], ts(1000), ts(30), None)
            .await
            .is_err()
    );
}

/// ReassignLease (apply: swap token, charge one delivery) — XCLAIM to a new consumer.
#[tokio::test]
async fn reassign_swaps_token_and_charges_delivery() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
        .await
        .unwrap();
    let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
    let id = claimed.items[0].item_id.clone();

    b.reassign(
        &shard(),
        vec![id.clone()],
        LeaseToken::new("lease-2").unwrap(),
        ts(800),
        ts(20), None
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
        b.push(&shard(), vec![PushSpec::default()], ts(0), None)
            .await
            .unwrap();
        let claimed = b.claim(claim_req(1, 500, 10)).await.unwrap();
        let id = claimed.items[0].item_id.clone();
        b.finalize(
            &shard(),
            vec![FinalizeOutcome { item_id: id, kind }],
            ts(20), None
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
            ts(10 * attempt), None
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
        ts(100), None
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
        ts(20), None
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
        ts(20), None
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
    b.push(&shard(), vec![PushSpec::default()], ts(0), None)
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
            ts(20),
        None)
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
        ts(30), None
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
        fields: BTreeMap::new(),
        cohort_size: None,
        gate_keys: Vec::new(),
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
        .purge(&shard(), vec![iid("a"), iid("a")], false, ts(20), None)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);

    // Lease "b", then a non-forced purge is gated; forced purge removes it.
    b.claim(claim_req(1, 500, 10)).await.unwrap();
    assert!(
        b.purge(&shard(), vec![iid("b")], false, ts(30), None)
            .await
            .is_err(),
        "leased purge needs force"
    );
    assert_eq!(
        b.purge(&shard(), vec![iid("b")], true, ts(31), None)
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
            BTreeMap::new(),
            ts(0), None
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
            BTreeMap::new(),
            ts(1), None
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
        b.replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            ts(2),
        None)
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
            BTreeMap::new(),
            ts(0), None
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
        ts(2), None
    )
    .await
    .unwrap();
    assert_eq!(
        b.purge(&shard(), vec![item_id], false, ts(3), None)
            .await
            .unwrap(),
        1
    );

    // Re-push the SAME key within retention (now=10 << expiry 3s + 60s): still a duplicate, not a new item.
    assert!(
        b.replace_if_pending(
            &shard(),
            &key,
            None,
            None,
            None,
            None,
            BTreeMap::new(),
            ts(10),
        None)
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
            BTreeMap::new(),
            ts(70), None
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
        a.push(&shard(), vec![spec(10), spec(30)], ts(0), None)
            .await
            .unwrap();
    } // drop = simulated crash

    let b = SqliteRelationalBackend::open(&path).unwrap();
    // A fresh push must NOT collide on the primary key with the reopened rows.
    let new_ids = b.push(&shard(), vec![spec(20)], ts(1), None).await.unwrap();
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
    b.push(&shard(), vec![spec(5), spec(5)], ts(0), None)
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
        ts(20), None
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

// ---------------------------------------------------------------------------
// 4. BQ-14b group-aware claim selection (RELATIONAL-class — the in-memory family lacks group_summary,
//    so these are NOT in the shared core suite).
// ---------------------------------------------------------------------------

/// `group_batching` leases the oldest-N candidate groups' WHOLE eligible sets, ordered by each group's
/// representative claim key, and skips the rest.
#[tokio::test]
async fn group_batching_leases_whole_groups_oldest_first() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    // g1 rep=10, g2 rep=20, g3 rep=30 (rep = each group's first-claimable item).
    let ids = b
        .push(
            &shard(),
            vec![
                gspec(11, "g1"),
                gspec(10, "g1"),
                gspec(21, "g2"),
                gspec(20, "g2"),
                gspec(30, "g3"),
            ],
            ts(0),
        None)
        .await
        .unwrap();

    // group_batching, max_groups=2 → the two oldest groups (g1, g2) leased WHOLE; g3 untouched.
    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, compat))
        .await
        .unwrap();
    let mut leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    leased.sort();
    // g1 = ids[0],ids[1]; g2 = ids[2],ids[3]; g3 = ids[4].
    let mut expect: Vec<&str> = vec![
        ids[0].as_str(),
        ids[1].as_str(),
        ids[2].as_str(),
        ids[3].as_str(),
    ];
    expect.sort();
    assert_eq!(leased, expect, "g1 + g2 leased whole; g3 not leased");
    // g3's item remains pending + claimable item-level.
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 1);
    let rest = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert_eq!(rest.items.len(), 1);
    assert_eq!(rest.items[0].item_id.as_str(), ids[4].as_str());
}

/// `same_group_key` leases ONLY the single oldest eligible group (capped at `max_items`, partial allowed).
#[tokio::test]
async fn same_group_key_leases_one_server_selected_group() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![gspec(10, "g1"), gspec(11, "g1"), gspec(20, "g2")],
            ts(0),
        None)
        .await
        .unwrap();

    let compat = ClaimCompatibility {
        same_group_key: true,
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, compat))
        .await
        .unwrap();
    let mut leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    leased.sort();
    let mut expect = vec![ids[0].as_str(), ids[1].as_str()]; // g1 only (the oldest group)
    expect.sort();
    assert_eq!(leased, expect, "only the oldest group g1 is leased");
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 2);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "g2 still pending"
    );
}

/// `group_batching` stops accumulating before exceeding `max_items` (whole-group granularity).
#[tokio::test]
async fn group_batching_respects_the_max_items_ceiling() {
    let b = make();
    b.create_queue(qdef_groups(3)).await.unwrap(); // group size <= 3 <= max_items, so validation passes
    let ids = b
        .push(
            &shard(),
            vec![
                gspec(10, "g1"),
                gspec(11, "g1"),
                gspec(20, "g2"),
                gspec(21, "g2"),
            ],
            ts(0),
        None)
        .await
        .unwrap();

    // max_groups large, but max_items=3: g1 (2) fits; adding g2 (2 more → 4 > 3) would exceed → stop.
    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 5 }),
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(3, 500, 100, compat))
        .await
        .unwrap();
    let mut leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    leased.sort();
    let mut expect = vec![ids[0].as_str(), ids[1].as_str()]; // only g1 (whole), g2 would overflow
    expect.sort();
    assert_eq!(
        leased, expect,
        "only the whole group that fits within max_items is leased"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 2, "g2 untouched");
}

/// A `group_batching` request with no eligible groups leases nothing (and an invalid combo still rejects).
#[tokio::test]
async fn group_batching_empty_and_invalid() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    // No grouped items pushed → no candidate groups → empty claim.
    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, compat))
        .await
        .unwrap();
    assert!(
        claimed.items.is_empty(),
        "no eligible groups → nothing leased"
    );

    // Invalid combo is still rejected with the structured error.
    let bad = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        whole_cohort: true,
        ..Default::default()
    };
    assert!(
        b.claim(claim_req_compat(10, 500, 100, bad)).await.is_err(),
        "group_batching + whole_cohort is an invalid combination"
    );
}

/// B1 (fresh-eyes): a paused queue yields nothing for a GROUP claim too (parity with item-level + postgres).
#[tokio::test]
async fn group_claim_yields_nothing_while_paused() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1"), gspec(11, "g1")], ts(0), None)
        .await
        .unwrap();
    commit(&b, envelope(QueueCommand::PauseQueue, vec![])).await;

    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    assert!(
        b.claim(claim_req_compat(10, 500, 100, compat))
            .await
            .unwrap()
            .items
            .is_empty(),
        "paused queue → group_batching leases nothing"
    );
    let same = ClaimCompatibility {
        same_group_key: true,
        ..Default::default()
    };
    assert!(
        b.claim(claim_req_compat(10, 500, 100, same))
            .await
            .unwrap()
            .items
            .is_empty(),
        "paused queue → same_group_key leases nothing"
    );
    // Resume → the group is claimable again.
    commit(&b, envelope(QueueCommand::ResumeQueue, vec![])).await;
    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    assert_eq!(
        b.claim(claim_req_compat(10, 500, 100, compat))
            .await
            .unwrap()
            .items
            .len(),
        2,
        "resumed → the whole group leases"
    );
}

/// I1 (fresh-eyes): a single group whose live-eligible set alone exceeds `max_items` cannot be delivered
/// whole → `BatchTooLarge`, leasing nothing (`max_eligible_group_size` is not a hard size cap, so a group
/// can grow past it). Item state is unchanged.
#[tokio::test]
async fn group_batching_oversized_group_is_batch_too_large() {
    let b = make();
    // max_eligible_group_size=5 passes validation against max_items=4 (5 > 4 is the CLAIM bound, but here
    // we claim with max_items=2 so the live group of 3 exceeds it). Push a group of 3 eligible items.
    b.create_queue(qdef_groups(5)).await.unwrap();
    b.push(
        &shard(),
        vec![gspec(10, "g1"), gspec(11, "g1"), gspec(12, "g1")],
        ts(0),
    None)
    .await
    .unwrap();

    // group_batching with max_items=2: the single group g1 (3 eligible) alone exceeds → BatchTooLarge.
    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    assert!(
        matches!(
            b.claim(claim_req_compat(2, 500, 100, compat)).await,
            Err(pqueue_engine::EngineError::BatchTooLarge)
        ),
        "an oversized whole group → BatchTooLarge"
    );
    // Nothing leased — all 3 still pending.
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 3);
}

// ---------------------------------------------------------------------------
// 5. BQ-14c whole_cohort claim (RELATIONAL-class). All-or-nothing: a COMPLETE cohort whose members are
//    all eligible leases together; otherwise it is skipped.
// ---------------------------------------------------------------------------

fn whole_cohort_compat() -> ClaimCompatibility {
    ClaimCompatibility {
        whole_cohort: true,
        ..Default::default()
    }
}

/// A complete cohort (member_count == cohort_size) with all members eligible leases WHOLE.
#[tokio::test]
async fn whole_cohort_leases_a_complete_cohort() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    // Cohort "c1" of size 3 — all three members present + eligible.
    let ids = b
        .push(
            &shard(),
            vec![cspec(10, "c1", 3), cspec(11, "c1", 3), cspec(12, "c1", 3)],
            ts(0),
        None)
        .await
        .unwrap();

    let claimed = b
        .claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
        .await
        .unwrap();
    let mut leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    leased.sort();
    let mut expect: Vec<&str> = ids.iter().map(|i| i.as_str()).collect();
    expect.sort();
    assert_eq!(leased, expect, "the whole complete cohort leases together");
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 3);
}

/// An INCOMPLETE cohort (fewer members present than the declared size) is NOT claimable → empty.
#[tokio::test]
async fn whole_cohort_skips_an_incomplete_cohort() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    // Declared size 3 but only 2 members pushed → incomplete.
    b.push(
        &shard(),
        vec![cspec(10, "c1", 3), cspec(11, "c1", 3)],
        ts(0),
    None)
    .await
    .unwrap();
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
        .await
        .unwrap();
    assert!(
        claimed.items.is_empty(),
        "an incomplete cohort is not claimable"
    );
    // The members stay pending.
    assert_eq!(b.metrics(&qkey()).await.unwrap().pending, 2);
}

/// A complete cohort with a member NOT currently eligible (already leased) is skipped (all-or-nothing).
#[tokio::test]
async fn whole_cohort_skips_when_a_member_is_not_eligible() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![cspec(10, "c1", 3), cspec(11, "c1", 3), cspec(12, "c1", 3)],
            ts(0),
        None)
        .await
        .unwrap();
    // Item-level claim one member → it is leased, so the cohort is no longer all-eligible.
    b.claim(claim_req(1, 500, 50)).await.unwrap();
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
        .await
        .unwrap();
    assert!(
        claimed.items.is_empty(),
        "a cohort with a non-eligible member is not claimable whole"
    );
    let _ = ids;
}

/// A complete cohort larger than `max_items` → `BatchTooLarge`, leasing nothing.
#[tokio::test]
async fn whole_cohort_oversized_is_batch_too_large() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    b.push(
        &shard(),
        vec![cspec(10, "c1", 3), cspec(11, "c1", 3), cspec(12, "c1", 3)],
        ts(0),
    None)
    .await
    .unwrap();
    // max_items=2 < cohort size 3.
    assert!(
        matches!(
            b.claim(claim_req_compat(2, 500, 100, whole_cohort_compat()))
                .await,
            Err(pqueue_engine::EngineError::BatchTooLarge)
        ),
        "an oversized cohort → BatchTooLarge"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        3,
        "nothing leased"
    );
}

/// F1 (fresh-eyes): a plain (non-cohort) push to a cohort's group_key does NOT strand the cohort — only
/// cohort-declared members (cohort_size set) count toward completeness.
#[tokio::test]
async fn whole_cohort_ignores_non_cohort_group_members() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    // 3 cohort members of "c1" (size 3) + one PLAIN group item sharing group_key "c1" (no cohort_size).
    let ids = b
        .push(
            &shard(),
            vec![
                cspec(10, "c1", 3),
                cspec(11, "c1", 3),
                cspec(12, "c1", 3),
                gspec(13, "c1"), // plain group member, not a cohort member
            ],
            ts(0),
        None)
        .await
        .unwrap();
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
        .await
        .unwrap();
    let mut leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    leased.sort();
    let mut expect = vec![ids[0].as_str(), ids[1].as_str(), ids[2].as_str()]; // the 3 cohort members
    expect.sort();
    assert_eq!(
        leased, expect,
        "only cohort-declared members lease; the plain group item is untouched"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "the plain group item stays pending"
    );
}

// ---------------------------------------------------------------------------
// 6. GATES (BQ-14d) — a blocked gate key makes every item carrying it INELIGIBLE via the eligibility
//    anti-join; clearing the gate restores eligibility with no per-item rewrite (exact-on-read). Gates are
//    a RELATIONAL-class feature (kept out of the shared core suite); item-level claim is unchanged when no
//    gate is set (parity preserved by the anti-join being a no-op against an empty pqueue_gate_state).
// ---------------------------------------------------------------------------

/// A push spec carrying gate-key membership (priority + gate_keys).
fn gatespec(priority: i64, gate_keys: &[&str]) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        gate_keys: gate_keys.iter().map(|g| g.to_string()).collect(),
        ..Default::default()
    }
}

/// Apply a SetGates command (block or clear the given gate keys) through the log/apply UoW.
async fn set_gates(b: &SqliteRelationalBackend, gate_keys: &[&str], blocked: bool) {
    commit(
        b,
        envelope(
            QueueCommand::SetGates(SetGatesCommand {
                gate_keys: gate_keys.iter().map(|g| g.to_string()).collect(),
                blocked,
            }),
            vec![],
        ),
    )
    .await;
}

/// A blocked gate hides every item carrying its key; clearing the gate restores them, with no per-item
/// rewrite (the same items become claimable again on the next claim).
#[tokio::test]
async fn blocked_gate_hides_items_then_clear_restores() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(&shard(), vec![gatespec(10, &["region-eu"])], ts(0), None)
        .await
        .unwrap();

    // Block the gate → the item is ineligible (claim leases nothing), but stays pending (not consumed).
    set_gates(&b, &["region-eu"], true).await;
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert!(claimed.items.is_empty(), "a blocked gate hides its item");
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "the gated item is still pending, just not eligible"
    );

    // Clear the gate → the SAME item is claimable again (exact-on-read; no rewrite happened).
    set_gates(&b, &["region-eu"], false).await;
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "clearing the gate restores the item"
    );
    assert_eq!(claimed.items[0].item_id.as_str(), ids[0].as_str());
}

/// CORE PARITY: an item with NO gate keys is unaffected by a blocked gate — and a blocked gate on one item
/// does not stall claim of an ungated sibling (the anti-join only excludes the gated item).
#[tokio::test]
async fn ungated_items_are_unaffected_by_a_blocked_gate() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![gatespec(10, &["g"]), spec(20)], // one gated (older), one ungated
            ts(0),
        None)
        .await
        .unwrap();

    set_gates(&b, &["g"], true).await;
    // The gated item (priority 10, older) is hidden; claim skips straight to the ungated item.
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    let leased: Vec<&str> = claimed.items.iter().map(|i| i.item_id.as_str()).collect();
    assert_eq!(
        leased,
        vec![ids[1].as_str()],
        "only the ungated item leases; the gated one is hidden but does not block it"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "gated item pending"
    );
}

/// A gate blocks an item even when several of its keys are clear — ANY blocked key on the item hides it.
#[tokio::test]
async fn any_blocked_key_hides_a_multi_gate_item() {
    let b = make();
    b.create_queue(qdef()).await.unwrap();
    b.push(&shard(), vec![gatespec(10, &["a", "b"])], ts(0), None)
        .await
        .unwrap();

    // Block only "b" (one of the item's two keys) → the item is hidden.
    set_gates(&b, &["b"], true).await;
    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty(),
        "blocking any one of the item's keys hides it"
    );
    // Clearing "b" restores it even though "a" was never blocked.
    set_gates(&b, &["b"], false).await;
    assert_eq!(
        b.claim(claim_req(10, 500, 100)).await.unwrap().items.len(),
        1
    );
}

/// A blocked gate on a single cohort member makes the WHOLE cohort un-claimable (all-or-nothing); clearing
/// it restores the whole-cohort claim.
#[tokio::test]
async fn blocked_gate_on_a_cohort_member_blocks_the_whole_cohort() {
    let b = make();
    b.create_queue(qdef_cohort()).await.unwrap();
    // Cohort "c1" size 3; the middle member also carries gate key "hold".
    let gated_member = PushSpec {
        priority: Some(PriorityValue::Int64(11)),
        group_key: Some(GroupKey::new("c1").unwrap()),
        cohort_size: Some(3),
        gate_keys: vec!["hold".to_string()],
        ..Default::default()
    };
    b.push(
        &shard(),
        vec![cspec(10, "c1", 3), gated_member, cspec(12, "c1", 3)],
        ts(0),
    None)
    .await
    .unwrap();

    set_gates(&b, &["hold"], true).await;
    assert!(
        b.claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
            .await
            .unwrap()
            .items
            .is_empty(),
        "a gated cohort member makes the whole cohort un-claimable"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        3,
        "nothing leased"
    );

    set_gates(&b, &["hold"], false).await;
    assert_eq!(
        b.claim(claim_req_compat(10, 500, 100, whole_cohort_compat()))
            .await
            .unwrap()
            .items
            .len(),
        3,
        "clearing the gate restores the whole-cohort claim"
    );
}

// ---------------------------------------------------------------------------
// 7. ACTIVE-SCOPE DISCOVERY (BQ-14e) — DiscoveryPort rolls up pqueue_group_summary into ranked
//    ActiveScopes (owner-local oldest-first; Queue granularity collapses to one queue rollup). A
//    relational-class read (the in-memory family has no group summary), so it lives in this suite.
// ---------------------------------------------------------------------------

/// At Group granularity, discovery returns one scope per active group, ranked owner-local oldest-first
/// (the most-aged group leads), with per-group eligible counts from the summary.
#[tokio::test]
async fn discover_group_granularity_ranks_oldest_first() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    // g1 (2 items) eligible since t=10, g2 since t=20, g3 since t=30 (later push = younger age).
    b.push(&shard(), vec![gspec(10, "g1"), gspec(11, "g1")], ts(10), None)
        .await
        .unwrap();
    b.push(&shard(), vec![gspec(20, "g2")], ts(20), None)
        .await
        .unwrap();
    b.push(&shard(), vec![gspec(30, "g3")], ts(30), None)
        .await
        .unwrap();

    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(1000))
        .await
        .unwrap();
    // Oldest-first: g1 (age 990s) → g2 (980s) → g3 (970s).
    let order: Vec<&str> = scopes
        .iter()
        .map(|s| s.group_key.as_deref().unwrap())
        .collect();
    assert_eq!(order, vec!["g1", "g2", "g3"], "ranked most-aged first");
    assert_eq!(scopes[0].oldest_eligible_age_ms, 990_000);
    assert_eq!(scopes[1].oldest_eligible_age_ms, 980_000);
    assert_eq!(scopes[2].oldest_eligible_age_ms, 970_000);
    // Counts come from the per-group summary (g1 has 2 eligible items, g2/g3 one each).
    assert_eq!(scopes[0].eligible_count, Some(2));
    assert_eq!(scopes[1].eligible_count, Some(1));
    // at-risk derivation is deferred → reported as None ("no signal"), NOT Some(0) (a measured zero).
    assert!(
        scopes.iter().all(|s| s.progress_bound_risk_count.is_none()),
        "deferred at-risk is None, not a fabricated zero"
    );
    // Every scope carries this queue's id and (group granularity) a group key.
    assert!(
        scopes
            .iter()
            .all(|s| s.queue_id == shard().queue_id.as_str())
    );
}

/// PAUSE divergence (intentional): discovery reports INTRINSIC eligibility — a paused queue still surfaces
/// its active scopes (so an operator sees pause-induced buildup), even though a claim on it leases nothing.
#[tokio::test]
async fn discover_reports_scopes_on_a_paused_queue() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1")], ts(10), None)
        .await
        .unwrap();
    commit(&b, envelope(QueueCommand::PauseQueue, vec![])).await;

    // A claim on the paused queue leases nothing...
    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    // ...but discovery still reports the built-up scope.
    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(1000))
        .await
        .unwrap();
    assert_eq!(
        scopes.len(),
        1,
        "paused queue still surfaces its active scope"
    );
    assert_eq!(scopes[0].group_key.as_deref(), Some("g1"));
    assert_eq!(scopes[0].eligible_count, Some(1));
}

/// Discovery of a queue that does not exist is an empty list, not an error (a read of an unknown queue
/// simply has no active scopes — unlike the claim path, which would reject).
#[tokio::test]
async fn discover_unknown_queue_is_empty_not_error() {
    let b = make();
    // No create_queue.
    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000))
        .await
        .unwrap();
    assert!(scopes.is_empty());
}

/// At Queue granularity, discovery collapses the queue's groups to ONE rollup scope: age = MAX across
/// groups (worst-aged drives the queue), eligible_count = SUM, group_key cleared.
#[tokio::test]
async fn discover_queue_granularity_rolls_up_to_one_scope() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1"), gspec(11, "g1")], ts(10), None)
        .await
        .unwrap();
    b.push(&shard(), vec![gspec(20, "g2")], ts(20), None)
        .await
        .unwrap();

    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000))
        .await
        .unwrap();
    assert_eq!(scopes.len(), 1, "one rolled-up scope for the queue");
    let q = &scopes[0];
    assert_eq!(q.group_key, None, "queue rollup clears group_key");
    assert_eq!(q.oldest_eligible_age_ms, 990_000, "max age across groups");
    assert_eq!(q.eligible_count, Some(3), "summed eligible counts (2 + 1)");
}

/// A group with no currently-eligible work (its only item is leased) drops out of discovery — the summary
/// sets `oldest_eligible_at = NULL`, so it is not an active scope.
#[tokio::test]
async fn discover_excludes_groups_with_no_eligible_work() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1")], ts(10), None)
        .await
        .unwrap();
    b.push(&shard(), vec![gspec(20, "g2")], ts(20), None)
        .await
        .unwrap();

    // Item-level claim leases g1's only item → g1 has no eligible work left.
    b.claim(claim_req(1, 500, 100)).await.unwrap();

    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(1000))
        .await
        .unwrap();
    let order: Vec<&str> = scopes
        .iter()
        .map(|s| s.group_key.as_deref().unwrap())
        .collect();
    assert_eq!(
        order,
        vec!["g2"],
        "the fully-leased group g1 is not an active scope"
    );
}

/// An empty queue (no groups) discovers no active scopes.
#[tokio::test]
async fn discover_empty_queue_is_empty() {
    let b = make();
    b.create_queue(qdef_groups(5)).await.unwrap();
    let scopes: Vec<ActiveScope> = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000))
        .await
        .unwrap();
    assert!(scopes.is_empty());
}
