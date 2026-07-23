use std::sync::Arc;

use pqueue::{
    Bytes, ClientItemKey, Clock, EligibilityPolicy, NewItem, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    QueueKey, RecurrencePolicy, RetryPolicy, SystemClock, TenantId,
};

struct FakeCapacity {
    granted: usize,
}

impl FakeCapacity {
    fn acquire_for_callee(requested: usize) -> Self {
        Self {
            granted: requested.min(2),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> pqueue::EngineResult<()> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let pq = pqueue::open_memory(clock);
    let queue = QueueKey::new(
        TenantId::new("tenant-a").expect("valid tenant"),
        QueueId::new("deliveries").expect("valid queue"),
    );

    pq.create_queue(queue_definition()).await?;

    for n in 0..4 {
        pq.push(
            &queue,
            NewItem {
                client_item_key: Some(
                    ClientItemKey::new(format!("delivery-{n}")).expect("valid key"),
                ),
                priority: Some(PriorityValue::Int64(n)),
                payload: Some(Bytes::from(format!("delivery payload {n}"))),
                ..Default::default()
            },
        )
        .await?;
    }

    let capacity = FakeCapacity::acquire_for_callee(3);
    if capacity.granted == 0 {
        return Ok(());
    }

    let application_batch_ceiling = 3;
    let max_items = capacity.granted.min(application_batch_ceiling);
    let claimed = pq.claim(&queue, max_items, 30_000).await?;
    for item in &claimed {
        println!("processing {}", item.client_item_key.as_str());
    }

    pq.ack(&queue, claimed.into_iter().map(|item| item.item_id))
        .await?;

    Ok(())
}

fn queue_definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("tenant-a").expect("valid tenant"),
        queue_id: QueueId::new("deliveries").expect("valid queue"),
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
        request_id_retention_ms: 600_000,
        client_item_key_retention_ms: 600_000,
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
