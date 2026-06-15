#![forbid(unsafe_code)]

use pqueue_core::{QueueId, TenantId};
use pqueue_storage::multi_shard::{ShardProgress, aggregate_cross_shard_progress};
use pqueue_storage::types::{ShardId, ShardKey};

fn tenant() -> TenantId {
    TenantId::new("test-tenant").unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn shard(tenant: TenantId, queue: QueueId, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: tenant,
        queue_id: queue,
        shard_id: ShardId::new(shard_id),
    }
}

fn progress(
    tenant_id: &TenantId,
    queue_id: &QueueId,
    shard_id: u32,
    oldest_eligible_age_ms: Option<u64>,
    progress_bound_risk_count: u64,
    observed_at_ms: u64,
    owned: bool,
) -> ShardProgress {
    ShardProgress {
        shard_key: shard(tenant_id.clone(), queue_id.clone(), shard_id),
        oldest_eligible_age_ms,
        progress_bound_risk_count,
        observed_at_ms,
        owned,
    }
}

#[test]
fn cross_shard_progress_tests_aggregates_queue_global_oldest_age_and_risk() {
    let t = tenant();
    let q = qid("progress");
    let progress_bound_ms = 30_000;
    let aggregate = aggregate_cross_shard_progress(
        &[
            progress(&t, &q, 0, Some(10_000), 0, 1_000, true),
            progress(&t, &q, 1, Some(35_000), 2, 1_010, true),
            progress(&t, &q, 2, None, 0, 1_005, true),
        ],
        progress_bound_ms,
        10_000,
        1_020,
    );

    assert_eq!(aggregate.oldest_eligible_age_ms, Some(35_000));
    assert_eq!(aggregate.progress_bound_risk_count, 3);
    assert_eq!(aggregate.as_of_ms, 1_000);
    assert!(aggregate.stalled_shards.is_empty());
}

#[test]
fn cross_shard_progress_tests_cold_shards_preserve_visibility_without_age() {
    let t = tenant();
    let q = qid("cold-shard");
    let aggregate = aggregate_cross_shard_progress(
        &[
            progress(&t, &q, 0, None, 0, 2_000, true),
            progress(&t, &q, 1, Some(5_000), 0, 2_010, true),
        ],
        30_000,
        10_000,
        2_020,
    );

    assert_eq!(aggregate.oldest_eligible_age_ms, Some(5_000));
    assert_eq!(aggregate.progress_bound_risk_count, 0);
    assert_eq!(aggregate.as_of_ms, 2_000);
}

#[test]
fn cross_shard_progress_tests_surfaces_stalled_and_unowned_shards() {
    let t = tenant();
    let q = qid("stalled");
    let aggregate = aggregate_cross_shard_progress(
        &[
            progress(&t, &q, 2, Some(1_000), 0, 10_000, true),
            progress(&t, &q, 1, Some(2_000), 0, 1_000, true),
            progress(&t, &q, 0, None, 0, 9_990, false),
        ],
        30_000,
        5_000,
        10_001,
    );

    assert_eq!(
        aggregate
            .stalled_shards
            .iter()
            .map(|shard| shard.shard_id.as_u32())
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(aggregate.oldest_eligible_age_ms, Some(2_000));
}
