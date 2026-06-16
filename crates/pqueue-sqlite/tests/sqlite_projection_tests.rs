#![forbid(unsafe_code)]

use pqueue_core::{QueueId, TenantId};
use pqueue_sqlite::{ProjectionHandleCache, SqliteProjection};
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

#[test]
fn sqlite_projection_tests_materializes_shard_scoped_group_summary_and_gates() {
    let t = tenant();
    let q = qid("sqlite-summary");
    let projection = SqliteProjection::new_in_memory(shard(t, q, 2)).unwrap();

    projection
        .insert_item("item-a", Some("group-a"), Some("gate-open"), 100)
        .unwrap();
    projection
        .insert_item("item-b", Some("group-a"), Some("gate-blocked"), 50)
        .unwrap();
    projection.set_gate("gate-blocked", true).unwrap();
    projection.recompute_group_summary().unwrap();

    let summary = projection.group_summary(Some("group-a")).unwrap().unwrap();
    assert_eq!(summary.group_key.as_deref(), Some("group-a"));
    assert_eq!(summary.oldest_eligible_at_ms, 100);
    assert_eq!(summary.eligible_count, 1);
    assert_eq!(projection.shard_key().shard_id.as_u32(), 2);
}

#[test]
fn sqlite_projection_tests_snapshot_restores_group_summary_and_cohorts() {
    let t = tenant();
    let q = qid("sqlite-snapshot");
    let shard_key = shard(t, q, 0);
    let projection = SqliteProjection::new_in_memory(shard_key.clone()).unwrap();

    projection
        .insert_item("item-a", Some("cohort-a"), None, 10)
        .unwrap();
    projection.insert_cohort("cohort-a", 3, "forming").unwrap();
    projection.recompute_group_summary().unwrap();
    projection.set_applied_sequence(42).unwrap();

    let snapshot = projection.snapshot_bytes().unwrap();
    let restored = SqliteProjection::restore_from_snapshot(shard_key, &snapshot).unwrap();
    assert_eq!(restored.applied_sequence().unwrap(), 42);
    assert_eq!(
        restored
            .group_summary(Some("cohort-a"))
            .unwrap()
            .unwrap()
            .eligible_count,
        1
    );
    let cohort = restored.cohort("cohort-a").unwrap().unwrap();
    assert_eq!(cohort.member_count, 3);
    assert_eq!(cohort.state, "forming");
}

#[test]
fn sqlite_projection_tests_snapshot_plus_bounded_replay_applies_only_tail() {
    let t = tenant();
    let q = qid("sqlite-replay");
    let projection = SqliteProjection::new_in_memory(shard(t, q, 0)).unwrap();
    projection.set_applied_sequence(10).unwrap();

    let applied = projection.apply_tail_sequences(&[8, 10, 11, 12]).unwrap();
    assert_eq!(applied, vec![11, 12]);
    assert_eq!(projection.applied_sequence().unwrap(), 12);
}

#[test]
fn sqlite_projection_tests_apply_before_return_advances_own_committed_sequence() {
    let t = tenant();
    let q = qid("sqlite-apply-before-return");
    let projection = SqliteProjection::new_in_memory(shard(t, q, 0)).unwrap();

    let applied = projection.apply_before_return(7).unwrap();
    assert_eq!(applied, 7);
    assert_eq!(projection.applied_sequence().unwrap(), 7);
    assert_eq!(projection.apply_before_return(5).unwrap(), 7);
}

#[test]
fn sqlite_projection_tests_bounded_apply_lag_reports_unrelated_reader_budget() {
    let t = tenant();
    let q = qid("sqlite-lag");
    let projection = SqliteProjection::new_in_memory(shard(t, q, 0)).unwrap();
    projection.set_applied_sequence(20).unwrap();

    let within = projection.apply_lag_status(23, 3).unwrap();
    assert_eq!(within.lag_sequences, 3);
    assert!(within.within_bound);

    let outside = projection.apply_lag_status(24, 3).unwrap();
    assert_eq!(outside.lag_sequences, 4);
    assert!(!outside.within_bound);
}

#[test]
fn sqlite_projection_tests_lru_handle_cache_evicts_idle_shards() {
    let t = tenant();
    let q = qid("sqlite-lru");
    let s0 = shard(t.clone(), q.clone(), 0);
    let s1 = shard(t.clone(), q.clone(), 1);
    let s2 = shard(t, q, 2);
    let mut cache = ProjectionHandleCache::new(2);

    cache.touch(s0.clone());
    cache.touch(s1.clone());
    cache.touch(s0.clone());
    cache.touch(s2.clone());

    assert_eq!(cache.len(), 2);
    assert!(cache.contains(&s0));
    assert!(cache.contains(&s2));
    assert!(!cache.contains(&s1));
}
