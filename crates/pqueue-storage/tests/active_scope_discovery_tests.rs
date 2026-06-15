#![forbid(unsafe_code)]

use pqueue_core::{GroupKey, QueueId, TenantId};
use pqueue_storage::multi_shard::{
    ShardActiveScopeRead, ShardActiveScopeSummary, aggregate_cross_shard_active_scopes,
};
use pqueue_storage::types::{ShardId, ShardKey};

fn tenant() -> TenantId {
    TenantId::new("test-tenant").unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn group(s: &str) -> GroupKey {
    GroupKey::new(s).unwrap()
}

fn shard(tenant: TenantId, queue: QueueId, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: tenant,
        queue_id: queue,
        shard_id: ShardId::new(shard_id),
    }
}

fn scope(
    group_key: Option<GroupKey>,
    oldest_eligible_age_ms: Option<u64>,
    eligible_count: Option<u64>,
    progress_bound_risk_count: Option<u64>,
) -> ShardActiveScopeSummary {
    ShardActiveScopeSummary {
        group_key,
        oldest_eligible_age_ms,
        eligible_count,
        progress_bound_risk_count,
    }
}

fn read(
    tenant_id: &TenantId,
    queue_id: &QueueId,
    shard_id: u32,
    observed_at_ms: u64,
    active_scopes: Vec<ShardActiveScopeSummary>,
) -> ShardActiveScopeRead {
    ShardActiveScopeRead {
        shard_key: shard(tenant_id.clone(), queue_id.clone(), shard_id),
        observed_at_ms,
        active_scopes,
    }
}

#[test]
fn storage_conformance_discovery_tests_cross_shard_group_merge_happens_before_top_n() {
    let t = tenant();
    let q = qid("discovery");
    let result = aggregate_cross_shard_active_scopes(
        &[
            read(
                &t,
                &q,
                0,
                10_000,
                vec![
                    scope(Some(group("small-a")), Some(40_000), Some(1), Some(0)),
                    scope(Some(group("shared")), Some(70_000), Some(2), Some(1)),
                ],
            ),
            read(
                &t,
                &q,
                1,
                9_950,
                vec![
                    scope(Some(group("shared")), Some(120_000), Some(3), Some(2)),
                    scope(Some(group("small-b")), Some(60_000), Some(1), Some(0)),
                ],
            ),
            read(&t, &q, 2, 9_900, Vec::new()),
        ],
        1,
    );

    assert_eq!(result.as_of_ms, 9_900);
    assert_eq!(result.active_scopes.len(), 1);
    let shared = &result.active_scopes[0];
    assert_eq!(shared.queue_id.as_str(), "discovery");
    assert_eq!(
        shared.group_key.as_ref().map(GroupKey::as_str),
        Some("shared")
    );
    assert_eq!(shared.oldest_eligible_age_ms, 120_000);
    assert_eq!(shared.eligible_count, Some(5));
    assert_eq!(shared.progress_bound_risk_count, Some(3));
}

#[test]
fn storage_conformance_discovery_tests_null_group_is_ungrouped_scope_not_queue_rollup() {
    let t = tenant();
    let q = qid("ungrouped");
    let result = aggregate_cross_shard_active_scopes(
        &[
            read(
                &t,
                &q,
                0,
                20_000,
                vec![
                    scope(None, Some(35_000), Some(2), Some(0)),
                    scope(Some(group("gated-out")), None, Some(10), Some(8)),
                ],
            ),
            read(
                &t,
                &q,
                1,
                20_010,
                vec![
                    scope(None, Some(55_000), Some(1), Some(1)),
                    scope(Some(group("grouped")), Some(45_000), Some(3), Some(0)),
                ],
            ),
        ],
        10,
    );

    assert_eq!(
        result
            .active_scopes
            .iter()
            .map(|scope| (
                scope.group_key.as_ref().map(GroupKey::as_str),
                scope.oldest_eligible_age_ms,
                scope.eligible_count,
            ))
            .collect::<Vec<_>>(),
        vec![(None, 55_000, Some(3)), (Some("grouped"), 45_000, Some(3))]
    );
    assert!(
        result
            .active_scopes
            .iter()
            .all(|scope| scope.group_key.as_ref().map(GroupKey::as_str) != Some("gated-out"))
    );
}

#[test]
fn storage_conformance_group_batching_tests_group_co_residency_is_placement_invariance() {
    let t = tenant();
    let q = qid("placement");
    let co_resident = aggregate_cross_shard_active_scopes(
        &[
            read(
                &t,
                &q,
                0,
                30_000,
                vec![scope(Some(group("alpha")), Some(30_000), Some(1), Some(0))],
            ),
            read(
                &t,
                &q,
                1,
                30_000,
                vec![scope(Some(group("beta")), Some(50_000), Some(2), Some(1))],
            ),
        ],
        10,
    );
    let non_co_resident = aggregate_cross_shard_active_scopes(
        &[
            read(
                &t,
                &q,
                0,
                30_000,
                vec![scope(Some(group("alpha")), Some(20_000), Some(1), Some(0))],
            ),
            read(
                &t,
                &q,
                1,
                30_000,
                vec![scope(Some(group("alpha")), Some(70_000), Some(4), Some(2))],
            ),
        ],
        10,
    );

    assert_eq!(
        co_resident
            .active_scopes
            .iter()
            .map(|scope| (
                scope.group_key.as_ref().unwrap().as_str(),
                scope.oldest_eligible_age_ms,
                scope.eligible_count,
            ))
            .collect::<Vec<_>>(),
        vec![("beta", 50_000, Some(2)), ("alpha", 30_000, Some(1))]
    );
    assert_eq!(non_co_resident.active_scopes.len(), 1);
    assert_eq!(
        non_co_resident.active_scopes[0]
            .group_key
            .as_ref()
            .map(GroupKey::as_str),
        Some("alpha")
    );
    assert_eq!(
        non_co_resident.active_scopes[0].oldest_eligible_age_ms,
        70_000
    );
    assert_eq!(non_co_resident.active_scopes[0].eligible_count, Some(5));
    assert_eq!(
        non_co_resident.active_scopes[0].progress_bound_risk_count,
        Some(2)
    );
}
