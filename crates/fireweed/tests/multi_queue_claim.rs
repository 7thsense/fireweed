#![allow(dead_code, unused_imports)]

use std::sync::{Arc, Mutex};

use fireweed::*;
use fireweed_memory::ManualClock;

fn definition(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("multi-tenant").unwrap(),
        queue_id: QueueId::new(queue_id).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn queue(queue_id: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("multi-tenant").unwrap(),
        QueueId::new(queue_id).unwrap(),
    )
}

fn target(queue: QueueKey, max: usize) -> MultiQueueClaimTarget {
    MultiQueueClaimTarget {
        queue,
        claim: ClaimAt::new(max, 30_000),
    }
}

#[tokio::test]
async fn memory_claims_share_time_and_preserve_input_order() {
    let clock = Arc::new(ManualClock::at(17));
    let fireweed = fireweed::open_memory(clock);
    let a = queue("a");
    let b = queue("b");
    for (key, id) in [(&a, "a"), (&b, "b")] {
        fireweed.create_queue(definition(id)).await.unwrap();
        fireweed.push(key, NewItem::default()).await.unwrap();
    }

    let results = fireweed
        .claim_across_queues(
            vec![target(b.clone(), 1), target(a.clone(), 1)],
            MultiQueueClaimLimits::default(),
        )
        .await
        .unwrap();

    assert_eq!(
        results.iter().map(|entry| &entry.queue).collect::<Vec<_>>(),
        vec![&b, &a]
    );
    for entry in results {
        let claimed = entry.result.unwrap();
        assert_eq!(claimed.items.len(), 1);
        assert_eq!(claimed.items[0].lease_expires_at.seconds, 47);
    }
}

#[tokio::test]
async fn structural_and_definition_preflight_have_no_claim_effects() {
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(0)));
    let a = queue("a");
    let b = queue("b");
    fireweed.create_queue(definition("a")).await.unwrap();
    fireweed.create_queue(definition("b")).await.unwrap();
    fireweed.push(&a, NewItem::default()).await.unwrap();
    fireweed.push(&b, NewItem::default()).await.unwrap();

    let invalid_calls = [
        vec![target(a.clone(), 0)],
        vec![target(a.clone(), 1), target(a.clone(), 1)],
        vec![MultiQueueClaimTarget {
            queue: a.clone(),
            claim: ClaimAt::new(1, 1).lease_time(fireweed::UtcTimestamp::new(1, 0).unwrap()),
        }],
    ];
    for targets in invalid_calls {
        assert!(matches!(
            fireweed
                .claim_across_queues(targets, MultiQueueClaimLimits::default())
                .await,
            Err(EngineError::Invalid(_))
        ));
    }
    assert!(matches!(
        fireweed
            .claim_across_queues(
                vec![target(a.clone(), 1)],
                MultiQueueClaimLimits {
                    max_targets: 17,
                    max_total_items: 1024
                },
            )
            .await,
        Err(EngineError::Invalid(
            "multi-queue claim max_targets exceeds fixed ceiling"
        ))
    ));
    assert!(matches!(
        fireweed
            .claim_across_queues(
                vec![target(a.clone(), 1), target(b.clone(), 101)],
                MultiQueueClaimLimits::default(),
            )
            .await,
        Err(EngineError::BatchTooLarge)
    ));

    assert_eq!(fireweed.metrics(&a).await.unwrap().pending, 1);
    assert_eq!(fireweed.metrics(&b).await.unwrap().pending, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn durable_relational_backend_claims_each_target() {
    let fireweed =
        fireweed::open_sqlite_relational(":memory:", Arc::new(ManualClock::at(5))).unwrap();
    let a = queue("durable-a");
    let b = queue("durable-b");
    for (key, id) in [(&a, "durable-a"), (&b, "durable-b")] {
        fireweed.create_queue(definition(id)).await.unwrap();
        fireweed.push(key, NewItem::default()).await.unwrap();
    }
    let results = fireweed
        .claim_across_queues(
            vec![target(a, 1), target(b, 1)],
            MultiQueueClaimLimits::default(),
        )
        .await
        .unwrap();
    assert!(
        results
            .into_iter()
            .all(|entry| entry.result.unwrap().items.len() == 1)
    );
}
