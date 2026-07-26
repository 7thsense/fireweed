use fireweed_conformance::{qdef, shard};
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, GroupKey, LeaseToken, PriorityValue, QueueDefinition,
    UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, GroupBatching, PushPort,
    PushSpec,
};
use fireweed_memory::composed_memory_backend;

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn group_definition() -> QueueDefinition {
    QueueDefinition {
        max_eligible_group_size: Some(4),
        ..qdef()
    }
}

fn cohort_definition() -> QueueDefinition {
    QueueDefinition {
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        }),
        ..qdef()
    }
}

fn grouped(priority: i64, group: &str) -> PushSpec {
    PushSpec {
        priority: Some(PriorityValue::Int64(priority)),
        group_key: Some(GroupKey::new(group).unwrap()),
        ..Default::default()
    }
}

fn cohort(priority: i64, group: &str, size: u64) -> PushSpec {
    PushSpec {
        cohort_size: Some(size),
        ..grouped(priority, group)
    }
}

fn claim(compatibility: ClaimCompatibility, max_items: usize) -> ClaimRequest {
    ClaimRequest {
        shard: shard(),
        worker_id: WorkerId::new("worker").unwrap(),
        max_items,
        lease_token: LeaseToken::new("rich-lease").unwrap(),
        lease_expires_at: ts(1_000),
        eligibility_time: None,
        now: ts(100),
        compatibility,
        expected_epoch: None,
    }
}

#[tokio::test]
async fn composed_memory_whole_group_claim_keeps_groups_atomic_and_ordered() {
    let backend = composed_memory_backend();
    backend.create_queue(group_definition()).await.unwrap();
    let first = backend
        .push(
            &shard(),
            vec![grouped(10, "g1"), grouped(12, "g1")],
            ts(0),
            None,
        )
        .await
        .unwrap();
    let second = backend
        .push(
            &shard(),
            vec![grouped(20, "g2"), grouped(21, "g2")],
            ts(1),
            None,
        )
        .await
        .unwrap();
    backend
        .push(&shard(), vec![grouped(30, "g3")], ts(2), None)
        .await
        .unwrap();

    let claimed = backend
        .claim(claim(
            ClaimCompatibility {
                group_batching: Some(GroupBatching { max_groups: 2 }),
                ..Default::default()
            },
            4,
        ))
        .await
        .unwrap();
    let actual = claimed
        .items
        .iter()
        .map(|item| item.item_id)
        .collect::<Vec<_>>();
    assert_eq!(actual, [first, second].concat());
}

#[tokio::test]
async fn composed_memory_same_group_key_claim_selects_one_group_and_allows_a_partial_group() {
    let backend = composed_memory_backend();
    backend.create_queue(group_definition()).await.unwrap();
    let oldest = backend
        .push(
            &shard(),
            vec![
                grouped(10, "oldest"),
                grouped(11, "oldest"),
                grouped(12, "oldest"),
            ],
            ts(0),
            None,
        )
        .await
        .unwrap();
    backend
        .push(&shard(), vec![grouped(20, "later")], ts(1), None)
        .await
        .unwrap();

    let claimed = backend
        .claim(claim(
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            2,
        ))
        .await
        .unwrap();
    let actual = claimed
        .items
        .iter()
        .map(|item| item.item_id)
        .collect::<Vec<_>>();
    assert_eq!(actual, oldest[..2]);
}

#[tokio::test]
async fn composed_memory_whole_cohort_claim_is_all_or_nothing_with_shared_lease() {
    let backend = composed_memory_backend();
    backend.create_queue(cohort_definition()).await.unwrap();
    backend
        .push(&shard(), vec![cohort(1, "incomplete", 2)], ts(0), None)
        .await
        .unwrap();
    let complete = backend
        .push(
            &shard(),
            vec![
                cohort(10, "complete", 3),
                cohort(11, "complete", 3),
                cohort(12, "complete", 3),
            ],
            ts(1),
            None,
        )
        .await
        .unwrap();

    let claimed = backend
        .claim(claim(
            ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
            10,
        ))
        .await
        .unwrap();
    assert_eq!(
        claimed
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        complete
    );
    assert!(claimed.cohort_id.is_some());
    assert_eq!(
        claimed.cohort_lease_token,
        Some(LeaseToken::new("rich-lease").unwrap())
    );
    assert!(claimed.items.iter().all(|item| item.lease_token.is_none()));
}
