//! ComposedBackend feature-parity conformance (DDx B0.2 / B0.3 / B0.4): the rich (non-item) claim units,
//! per-group active-scope discovery, and operator gate state are delegated to the projection axis. Rich
//! claims and discovery remain relational-only. Gate state is shared by relational and in-memory/log-replay
//! projections; focused coverage for the latter lives with memory and reconnect tests.

use fireweed_conformance::{claim_req, qdef, qkey, shard};
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, GroupKey, ItemId, LeaseToken, PriorityValue, QueueDefinition,
    UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    Backend, ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, DiscoveryGranularity,
    DiscoveryPort, EngineError, GroupBatching, ProjectionRead, PushPort, PushSpec, SetGatesCommand,
    SetGatesPort,
};
use fireweed_sqlite::{
    composed_sqlite_backend_in_memory, composed_sqlite_relational,
    composed_sqlite_relational_in_memory,
};

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
        eligibility_time: None,
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
/// `CohortClaim` (flipping `fireweed_cohorts` to leased), and returns the API-001 cohort response shape.
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

/// The composed log-replay backend uses the shared projection for every valid non-item claim unit.
#[tokio::test]
async fn composed_log_replay_accepts_non_item_claim_units() {
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

    let cohort = b
        .claim(claim_req_compat(
            10,
            500,
            100,
            ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(cohort.items.len(), 2);
    assert!(cohort.cohort_id.is_some());

    for compat in [
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
            b.claim(claim_req_compat(10, 500, 100, compat))
                .await
                .unwrap()
                .items
                .is_empty()
        );
    }
}

/// REGRESSION (fencing/rollback discipline): a rich claim that gets FENCED (stale epoch) must leave no
/// durable side effect — in particular the in-transaction `fireweed_group_summary` refresh performed during
/// candidate selection must be ROLLED BACK, not committed. Discovery now derives exact eligibility from
/// live item rows, so this storage-invariant regression uses a private file-backed database and inspects the
/// summary row directly before and after the fenced claim.
#[tokio::test]
async fn composed_relational_fenced_rich_claim_leaves_group_summary_unchanged() {
    let path = std::env::temp_dir().join(format!(
        "fireweed-composed-relational-fenced-summary-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let b = composed_sqlite_relational(path.to_str().unwrap()).unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    // A grouped item that is NOT due until ts(200): at push time (ts 0) the group summary's
    // oldest_eligible_at stays NULL (no eligible member yet) and no later mutation refreshes it.
    let delayed = PushSpec {
        not_before: Some(ts(200)),
        ..gspec(10, "g1")
    };
    b.push(&shard(), vec![delayed], ts(0), None).await.unwrap();

    let summary = || {
        rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT oldest_eligible_at, eligible_item_count, rep_item_id \
                 FROM fireweed_group_summary \
                 WHERE tenant_id=?1 AND queue_id=?2 AND group_key='g1'",
                rusqlite::params![shard().tenant_id.as_str(), shard().queue_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap()
    };
    let stale = (None, 0, None);
    assert_eq!(
        summary(),
        stale,
        "precondition: the time-due group's mutation-time summary is still stale"
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

    // The fenced claim must have left NO durable side effect: the transient selection refresh was rolled
    // back, so the durable mutation-time summary remains byte-for-byte equivalent at its semantic fields.
    assert_eq!(
        summary(),
        stale,
        "a fenced rich claim must not durably refresh fireweed_group_summary"
    );
    drop(b);
    std::fs::remove_file(path).unwrap();
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

/// The composed LOG-REPLAY backend derives exact active scopes from its shared in-memory projection.
#[tokio::test]
async fn composed_log_replay_discovers_active_scopes() {
    let b = composed_sqlite_backend_in_memory().unwrap();
    b.create_queue(qdef_rich()).await.unwrap();
    b.push(&shard(), vec![gspec(10, "g1")], ts(10), None)
        .await
        .unwrap();

    let scopes = b
        .discover_active_scopes(&shard(), DiscoveryGranularity::Queue, ts(1000))
        .await
        .unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].oldest_eligible_age_ms, 990_000);
    assert_eq!(scopes[0].eligible_count, Some(1));
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
