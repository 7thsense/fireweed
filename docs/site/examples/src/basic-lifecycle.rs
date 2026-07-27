// Provenance: crates/fireweed/tests/concrete_fireweed.rs::exercise_operation_families
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
async fn exercise_operation_families(fireweed: &Fireweed, queue_name: &str) {
    let mut definition = queue_definition();
    definition.queue_id = QueueId::new(queue_name).unwrap();
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();

    let client_key = ClientItemKey::new(format!("{queue_name}-item")).unwrap();
    let item_id = fireweed
        .push(
            &key,
            NewItem {
                client_item_key: Some(client_key.clone()),
                priority: Some(PriorityValue::Int64(10)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fireweed
            .live_item(&key, client_key)
            .await
            .unwrap()
            .unwrap()
            .item_id,
        item_id
    );
    fireweed
        .update(
            &key,
            item_id,
            ScheduleUpdate::Set(Some(PriorityValue::Int64(5))),
            ScheduleUpdate::Keep,
            None,
        )
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&key).await.unwrap().pending, 1);
    let _ = fireweed.hot_projection_capabilities(&key);

    let claimed = fireweed.claim(&key, 1, 30_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    fireweed
        .complete(&key, claimed.iter().map(|item| item.item_id))
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&key).await.unwrap().complete, 1);
}

// Provenance: crates/fireweed/tests/concrete_fireweed.rs::queue_definition
// Do not edit by hand — regenerate with scripts/site/extract_examples.py
fn queue_definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("downstream").unwrap(),
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
        emit_change_records: true,
    }
}
