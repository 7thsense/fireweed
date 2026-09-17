#![cfg(feature = "memory")]

use std::sync::Arc;

use fireweed::*;
use fireweed_memory::ManualClock;

#[cfg(all(feature = "turso", feature = "objectlog"))]
#[path = "support/storage.rs"]
mod storage;

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("transition-validation").unwrap(),
        queue_id: QueueId::new("work").unwrap(),
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
        emit_change_records: false,
    }
}

async fn rejected_entries_leave_claims_keys_and_fences_available(fireweed: Fireweed) {
    let definition = definition();
    let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();
    fireweed
        .push_batch(&shard, vec![NewItem::default(); 3])
        .await
        .unwrap();
    let claimed = fireweed.claim(&shard, 3, 50_000).await.unwrap();
    assert_eq!(claimed.len(), 3);
    let push = |key: &str| NewItem {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        ..Default::default()
    };
    let entry = |input: usize, lifecycle_items: Vec<NewItem>, fence: Option<(u64, u64)>| {
        let item = &claimed[input];
        CommitEntry {
            claim_ref: ClaimRef {
                item_id: item.item_id,
                lease_token: item.lease_token.clone().unwrap(),
                lease_expires_at: item.lease_expires_at,
                item_version: item.item_version,
            },
            finalize: FinalizeKind::Complete,
            side_records: vec![],
            lifecycle_items,
            instance_fence: fence.map(|(expected, next)| InstanceFence {
                instance_key: b"instance".to_vec(),
                expected,
                next,
            }),
        }
    };
    let mut malformed = push("malformed");
    malformed.priority = Some(PriorityValue::Text("wrong-model".into()));
    let mut gated = push("gated");
    gated.gate_keys = vec!["disabled-gate".into()];
    let request_id = RequestId::new("entry-validation").unwrap();
    let results = fireweed
        .commit(
            &shard,
            CommitRequest {
                request_id: Some(request_id.clone()),
                entries: vec![
                    entry(0, vec![malformed], None),
                    entry(0, vec![push("shared")], Some((0, 1))),
                    entry(1, vec![push("fresh"), push("shared")], Some((1, 2))),
                    entry(1, vec![push("fresh")], Some((1, 2))),
                    entry(2, vec![gated], None),
                    entry(2, vec![push("other"), push("other")], None),
                    entry(2, vec![push("other")], None),
                ],
            },
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 7);
    assert!(matches!(
        results[0],
        EntryOutcome::Rejected(EngineError::Invalid(_))
    ));
    assert!(matches!(results[1], EntryOutcome::Committed { .. }));
    assert_eq!(results[2], EntryOutcome::Rejected(EngineError::Conflict));
    assert!(matches!(results[3], EntryOutcome::Committed { .. }));
    assert!(matches!(
        results[4],
        EntryOutcome::Rejected(EngineError::Invalid(_))
    ));
    assert_eq!(results[5], EntryOutcome::Rejected(EngineError::Conflict));
    assert!(matches!(results[6], EntryOutcome::Committed { .. }));
    for key in ["shared", "fresh", "other"] {
        assert!(
            fireweed
                .live_item(&shard, ClientItemKey::new(key).unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }
    for key in ["malformed", "gated"] {
        assert!(
            fireweed
                .live_item(&shard, ClientItemKey::new(key).unwrap())
                .await
                .unwrap()
                .is_none()
        );
    }
    let recovery = fireweed
        .explain_commit(&shard, request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovery.entries[3].instance,
        Some((b"instance".to_vec(), 2))
    );
    let metrics = fireweed.metrics(&shard).await.unwrap();
    assert_eq!(
        (metrics.pending, metrics.leased, metrics.complete),
        (3, 0, 3)
    );
}

#[tokio::test]
async fn memory_rejected_entries_do_not_reserve_claims_client_keys_or_instance_fences() {
    rejected_entries_leave_claims_keys_and_fences_available(open_memory(Arc::new(
        ManualClock::at(0),
    )))
    .await;
}

#[cfg(all(feature = "turso", feature = "objectlog"))]
#[tokio::test]
async fn turso_rejected_entries_do_not_reserve_claims_client_keys_or_instance_fences() {
    let fixture = storage::Fixture::new();
    let fireweed = storage::open_log_turso(fixture.path(), Arc::new(ManualClock::at(0))).unwrap();
    rejected_entries_leave_claims_keys_and_fences_available(fireweed).await;
}
