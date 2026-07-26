use fireweed_conformance::qdef;
use fireweed_core::{
    CohortOnIncomplete, CohortPolicy, GroupKey, LeaseToken, PriorityValue, QueueDefinition,
    QueueId, UtcTimestamp, WorkerId,
};
use fireweed_engine::{
    ClaimCompatibility, ClaimPort, ClaimRequest, ControlPlaneStore, GroupBatching, PushPort,
    PushSpec, QueueKey,
};
use fireweed_sqlite::composed_sqlite_backend;

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn grouped_definition(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        queue_id: QueueId::new(queue_id).unwrap(),
        max_eligible_group_size: Some(4),
        ..qdef()
    }
}

fn cohort_definition(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        queue_id: QueueId::new(queue_id).unwrap(),
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

fn claim(
    shard: QueueKey,
    lease: &str,
    compatibility: ClaimCompatibility,
    max_items: usize,
) -> ClaimRequest {
    ClaimRequest {
        shard,
        worker_id: WorkerId::new("worker").unwrap(),
        max_items,
        lease_token: LeaseToken::new(lease).unwrap(),
        lease_expires_at: ts(1_000),
        eligibility_time: None,
        now: ts(100),
        compatibility,
        expected_epoch: None,
    }
}

#[tokio::test]
async fn durable_log_replay_restores_every_rich_claim_unit() {
    let path = std::env::temp_dir().join(format!(
        "fireweed-rich-claim-replay-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path_text = path.to_str().unwrap();
    let whole_group = grouped_definition("whole-group");
    let same_group = grouped_definition("same-group");
    let whole_cohort = cohort_definition("whole-cohort");
    let whole_group_key =
        QueueKey::new(whole_group.tenant_id.clone(), whole_group.queue_id.clone());
    let same_group_key = QueueKey::new(same_group.tenant_id.clone(), same_group.queue_id.clone());
    let whole_cohort_key = QueueKey::new(
        whole_cohort.tenant_id.clone(),
        whole_cohort.queue_id.clone(),
    );

    let backend = composed_sqlite_backend(path_text).unwrap();
    for definition in [whole_group, same_group, whole_cohort] {
        backend.create_queue(definition).await.unwrap();
    }
    let expected_whole_groups = backend
        .push(
            &whole_group_key,
            vec![grouped(1, "g1"), grouped(2, "g1"), grouped(3, "g2")],
            ts(0),
            None,
        )
        .await
        .unwrap();
    let expected_same_group = backend
        .push(
            &same_group_key,
            vec![grouped(1, "g1"), grouped(2, "g1"), grouped(20, "g2")],
            ts(0),
            None,
        )
        .await
        .unwrap();
    let expected_cohort = backend
        .push(
            &whole_cohort_key,
            vec![cohort(1, "c1", 2), cohort(2, "c1", 2)],
            ts(0),
            None,
        )
        .await
        .unwrap();
    drop(backend);

    let reopened = composed_sqlite_backend(path_text).unwrap();
    let grouped_claim = reopened
        .claim(claim(
            whole_group_key,
            "group-lease",
            ClaimCompatibility {
                group_batching: Some(GroupBatching { max_groups: 2 }),
                ..Default::default()
            },
            4,
        ))
        .await
        .unwrap();
    assert_eq!(
        grouped_claim
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        expected_whole_groups
    );

    let same_claim = reopened
        .claim(claim(
            same_group_key,
            "same-lease",
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
            2,
        ))
        .await
        .unwrap();
    assert_eq!(
        same_claim
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        expected_same_group[..2]
    );

    let cohort_claim = reopened
        .claim(claim(
            whole_cohort_key,
            "cohort-lease",
            ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
            10,
        ))
        .await
        .unwrap();
    assert_eq!(
        cohort_claim
            .items
            .iter()
            .map(|item| item.item_id)
            .collect::<Vec<_>>(),
        expected_cohort
    );
    assert!(cohort_claim.cohort_id.is_some());
    assert_eq!(
        cohort_claim.cohort_lease_token,
        Some(LeaseToken::new("cohort-lease").unwrap())
    );
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}
