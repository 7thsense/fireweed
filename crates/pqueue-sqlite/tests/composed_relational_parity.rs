//! ComposedBackend feature-parity conformance (DDx B0.2 / B0.3 / B0.4): the rich (non-item) claim units,
//! per-group active-scope discovery, and operator gate state are RELATIONAL-class capabilities the
//! composition delegates to its projection axis. These scenarios exercise them on the **composed
//! relational** backend (`ComposedBackend<SqliteRelational, SqliteRelational, _>`), which now reaches
//! parity with the monolithic `SqliteRelationalBackend`, and confirm the **composed log-replay** backend
//! (`ComposedBackend<SqliteLog, InMemoryProjection, _>`) still refuses them with the structured
//! `Unavailable` (capability parity with the in-memory family), rather than silently downgrading.

use pqueue_conformance::{claim_req, qdef, qkey, shard};
use pqueue_core::{
    CohortOnIncomplete, CohortPolicy, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition,
    UtcTimestamp, WorkerId,
};
use pqueue_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, DiscoveryGranularity,
    DiscoveryPort, EngineError, GroupBatching, ProjectionRead, PushPort, PushSpec, SetGatesCommand,
    SetGatesPort,
};
use pqueue_sqlite::{composed_sqlite_backend_in_memory, composed_sqlite_relational_in_memory};

fn ts(s: i64) -> UtcTimestamp {
    UtcTimestamp::new(s, 0).unwrap()
}

/// A queue def with a group-size bound (so `group_batching` / `same_group_key` validate) and cohorts
/// enabled (so `whole_cohort` validates) — one def usable by every rich-claim unit.
fn qdef_rich() -> QueueDefinition {
    QueueDefinition {
        max_eligible_group_size: Some(5),
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        }),
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

/// A gate-bearing push spec (priority + gate keys).
fn gatespec(priority: i64, gate_keys: &[&str]) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        gate_keys: gate_keys.iter().map(|g| g.to_string()).collect(),
        ..Default::default()
    }
}

/// A claim request carrying compatibility options.
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

// ---------------------------------------------------------------------------
// B0.2: rich (non-item) claim selection on the composed relational backend.
// ---------------------------------------------------------------------------

/// `whole_cohort` on the composed relational backend selects the complete cohort, leases it via a
/// `CohortClaim` (flipping `pqueue_cohorts` to leased), and returns the API-001 cohort response shape.
#[tokio::test]
async fn composed_relational_whole_cohort_claim_selects_and_leases() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![cspec(10, "c1", 3), cspec(11, "c1", 3), cspec(12, "c1", 3)],
            ts(0),
            None,
        )
        .await
        .unwrap();

    let compat = ClaimCompatibility {
        whole_cohort: true,
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, compat))
        .await
        .unwrap();

    assert_eq!(
        claimed.cohort_lease_token,
        Some(LeaseToken::new("lease-1").unwrap()),
        "whole-cohort claim carries the shared lease token at the response top level"
    );
    assert_eq!(
        claimed.cohort_id.as_ref().map(|id| id.as_str()),
        Some("coh:c1:0"),
        "whole-cohort claim identifies the leased cohort generation"
    );
    assert!(
        claimed.items.iter().all(|item| item.lease_token.is_none()),
        "whole-cohort item rows omit the per-item lease token"
    );
    let mut leased: Vec<ItemId> = claimed.items.iter().map(|i| i.item_id).collect();
    leased.sort();
    let mut expect: Vec<ItemId> = ids.to_vec();
    expect.sort();
    assert_eq!(leased, expect, "the whole complete cohort leases together");
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 3);
}

/// `group_batching` (whole_group) on the composed relational backend leases the oldest-N candidate groups'
/// whole eligible sets and leaves the rest pending.
#[tokio::test]
async fn composed_relational_whole_group_claim_selects() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
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
            None,
        )
        .await
        .unwrap();

    let compat = ClaimCompatibility {
        group_batching: Some(GroupBatching { max_groups: 2 }),
        ..Default::default()
    };
    let claimed = b
        .claim(claim_req_compat(10, 500, 100, compat))
        .await
        .unwrap();
    let mut leased: Vec<ItemId> = claimed.items.iter().map(|i| i.item_id).collect();
    leased.sort();
    let mut expect: Vec<ItemId> = vec![ids[0], ids[1], ids[2], ids[3]]; // g1 + g2 whole; g3 untouched
    expect.sort();
    assert_eq!(
        leased, expect,
        "the two oldest groups lease whole; g3 stays pending"
    );
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 4);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "g3 still pending"
    );
}

/// `same_group_key` on the composed relational backend leases ONLY the single oldest eligible group.
#[tokio::test]
async fn composed_relational_same_group_key_claim_selects() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    let ids = b
        .push(
            &shard(),
            vec![gspec(10, "g1"), gspec(11, "g1"), gspec(20, "g2")],
            ts(0),
            None,
        )
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
    let mut leased: Vec<ItemId> = claimed.items.iter().map(|i| i.item_id).collect();
    leased.sort();
    let mut expect = vec![ids[0], ids[1]]; // g1 only (the oldest group)
    expect.sort();
    assert_eq!(leased, expect, "only the oldest group g1 is leased");
    assert_eq!(b.metrics(&qkey()).await.unwrap().leased, 2);
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "g2 still pending"
    );
}

/// The composed LOG-REPLAY backend has no group/cohort projection, so every VALID non-item claim unit is
/// refused with the structured `Unavailable` (not silently downgraded to an item claim).
#[tokio::test]
async fn composed_log_replay_claim_stays_unavailable_for_non_item_units() {
    let b = composed_sqlite_backend_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    b.push(
        &shard(),
        vec![cspec(10, "c1", 2), cspec(11, "c1", 2)],
        ts(0),
        None,
    )
    .await
    .unwrap();

    for compat in [
        ClaimCompatibility {
            whole_cohort: true,
            ..Default::default()
        },
        ClaimCompatibility {
            same_group_key: true,
            ..Default::default()
        },
        ClaimCompatibility {
            group_batching: Some(GroupBatching { max_groups: 2 }),
            ..Default::default()
        },
    ] {
        assert!(
            matches!(
                b.claim(claim_req_compat(10, 500, 100, compat)).await,
                Err(EngineError::Unavailable)
            ),
            "the log-replay family refuses a valid non-item claim unit with Unavailable"
        );
    }
    // The item-level (default) claim still works unchanged.
    assert_eq!(
        b.claim(claim_req(10, 500, 100)).await.unwrap().items.len(),
        2
    );
}

/// REGRESSION (fencing/rollback discipline): a rich claim that gets FENCED (stale epoch) must leave no
/// durable side effect — in particular the in-transaction `pqueue_group_summary` refresh performed during
/// candidate selection must be ROLLED BACK, not committed. We observe this through read-only discovery
/// (which never refreshes the summary itself): a group made eligible only by TIME passing (so its summary
/// row is still stale) must stay INVISIBLE to discovery after the fenced claim.
#[tokio::test]
async fn composed_relational_fenced_rich_claim_leaves_group_summary_unchanged() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    // A grouped item that is NOT due until ts(200): at push time (ts 0) the group summary's
    // oldest_eligible_at stays NULL (no eligible member yet) and no later mutation refreshes it.
    let delayed = PushSpec {
        not_before: Some(ts(200)),
        ..gspec(10, "g1")
    };
    b.push(&shard(), vec![delayed], ts(0), None).await.unwrap();

    // Baseline: at ts(300) the item is due by time, but discovery (read-only) still under-reports it
    // because no mutation has refreshed the stale summary row yet.
    let before = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(300))
        .await
        .unwrap();
    assert!(
        before.is_empty(),
        "precondition: the time-due group's summary is still stale (discovery under-reports)"
    );

    // A same_group_key claim under a STALE epoch: selection refreshes the summary IN-TRANSACTION and finds
    // the now-due item, but `commit_locked` fences it (nothing is leased).
    let mut req = claim_req_compat(
        10,
        500,
        300,
        ClaimCompatibility {
            same_group_key: true,
            ..Default::default()
        },
    );
    req.expected_epoch = Some(9_999); // not the queue's current epoch → fenced at commit
    assert!(
        matches!(b.claim(req).await, Err(EngineError::EpochFenced)),
        "a stale-epoch rich claim is fenced"
    );

    // The fenced claim must have left NO durable side effect: discovery still reports nothing, proving the
    // in-transaction group-summary refresh was rolled back (not committed).
    let after = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Group, ts(300))
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "a fenced rich claim must not durably refresh pqueue_group_summary"
    );
}

// ---------------------------------------------------------------------------
// B0.3: active-scope discovery on the composed relational backend.
// ---------------------------------------------------------------------------

/// Discovery rolls the composed relational backend's per-group summary up to one Queue-granularity scope
/// (max age across groups, summed eligible counts).
#[tokio::test]
async fn composed_relational_discover_active_scopes_rolls_up() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    b.push(
        &shard(),
        vec![gspec(10, "g1"), gspec(11, "g1")],
        ts(10),
        None,
    )
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

/// The composed LOG-REPLAY backend maintains no per-group summary, so discovery is refused with the
/// structured `Unavailable` (parity with the in-memory family).
#[tokio::test]
async fn composed_log_replay_discovery_stays_unavailable() {
    let b = composed_sqlite_backend_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1")], ts(10), None)
        .await
        .unwrap();

    assert!(
        matches!(
            b.discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000))
                .await,
            Err(EngineError::Unavailable)
        ),
        "the log-replay family refuses discovery with Unavailable"
    );
}

// ---------------------------------------------------------------------------
// B0.4: gate support on the composed relational backend.
// ---------------------------------------------------------------------------

/// The composed relational backend accepts a gate-bearing push and honors `SetGates`: a blocked gate hides
/// its item from claim selection; clearing it restores the SAME item (exact-on-read, no per-item rewrite).
#[tokio::test]
async fn composed_relational_set_gates_and_gate_bearing_push_accepted() {
    let b = composed_sqlite_relational_in_memory().unwrap();
    assert!(
        b.supports_gates(),
        "the composed relational backend supports gates"
    );
    b.create_queue(qdef()).await.unwrap();

    // A gate-bearing push is ACCEPTED (rejected on a non-gate backend).
    let ids = b
        .push(&shard(), vec![gatespec(10, &["region-eu"])], ts(0), None)
        .await
        .unwrap();

    // Block the gate → the item is hidden from claim but stays pending.
    b.set_gates(
        &shard(),
        SetGatesCommand {
            gate_keys: vec!["region-eu".to_string()],
            blocked: true,
        },
        ts(1),
        None,
    )
    .await
    .unwrap();
    assert!(
        b.claim(claim_req(10, 500, 100))
            .await
            .unwrap()
            .items
            .is_empty(),
        "a blocked gate hides its item"
    );
    assert_eq!(
        b.metrics(&qkey()).await.unwrap().pending,
        1,
        "gated item still pending"
    );

    // Clear the gate → the SAME item is claimable again.
    b.set_gates(
        &shard(),
        SetGatesCommand {
            gate_keys: vec!["region-eu".to_string()],
            blocked: false,
        },
        ts(2),
        None,
    )
    .await
    .unwrap();
    let claimed = b.claim(claim_req(10, 500, 100)).await.unwrap();
    assert_eq!(
        claimed.items.len(),
        1,
        "clearing the gate restores the item"
    );
    assert_eq!(claimed.items[0].item_id, ids[0]);
}
