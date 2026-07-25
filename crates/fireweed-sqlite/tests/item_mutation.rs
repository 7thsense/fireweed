use std::collections::BTreeMap;

use bytes::Bytes;
use fireweed_core::{
    ClientItemKey, EligibilityPolicy, GateKeyPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueDefinition, QueueId,
    RecurrencePolicy, RequestId, RetryPolicy, TenantId, UtcTimestamp,
};
use fireweed_engine::{
    AddressedMutation, ControlPlaneStore, GateChange, ItemMutationOperation, ItemMutationPort,
    ItemMutationRequest, ItemMutationReturning, ItemPatch, ProjectionRead, PushPort, PushSpec,
    QueueKey,
};
use fireweed_sqlite::SqliteRelationalBackend;

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("mutation-tests").unwrap(),
        queue_id: QueueId::new("direct-sqlite").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(8),
            ..EligibilityPolicy::default()
        },
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

fn request(
    request_id: &str,
    item_id: fireweed_core::ItemId,
    expected_item_version: u64,
) -> ItemMutationRequest {
    ItemMutationRequest {
        request_id: RequestId::new(request_id).unwrap(),
        evaluated_at: ts(10),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::Addressed {
            entries: vec![AddressedMutation {
                item_id,
                expected_item_version: Some(expected_item_version),
                predicates: vec![],
                lease_guard: Default::default(),
                patch: ItemPatch {
                    field_edits: BTreeMap::from([(
                        "owner".into(),
                        Some(Bytes::from_static(b"snorri")),
                    )]),
                    ..Default::default()
                },
            }],
        },
    }
}

#[tokio::test]
async fn reopen_exact_replay_and_invalid_request_rollback() {
    let path = std::env::temp_dir().join(format!(
        "fireweed-direct-mutation-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path = path.to_string_lossy().into_owned();
    let definition = definition();
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let key = ClientItemKey::new("direct-item").unwrap();
    let item_id;
    let replay_request;
    let committed;
    {
        let backend = SqliteRelationalBackend::open(&path).unwrap();
        backend.create_queue(definition).await.unwrap();
        item_id = backend
            .push(
                &queue,
                vec![PushSpec {
                    client_item_key: Some(key.clone()),
                    ..Default::default()
                }],
                ts(1),
                None,
            )
            .await
            .unwrap()[0];

        let mut invalid = request("rollback", item_id, 1);
        invalid.gate_changes = vec![GateChange {
            gate_keys: vec!["invalid key".into()],
            blocked: true,
        }];
        assert!(backend.mutate_items(&queue, invalid, None).await.is_err());
        assert_eq!(
            backend
                .live_items(&queue, std::slice::from_ref(&key))
                .await
                .unwrap()[0]
                .as_ref()
                .unwrap()
                .item_version,
            1
        );

        assert_eq!(
            backend
                .mutate_items(&queue, request("rollback", item_id, 1), None)
                .await
                .unwrap()
                .summary
                .changed,
            1,
            "a rolled-back request must not reserve its request id"
        );
        replay_request = request("replay", item_id, 2);
        committed = backend
            .mutate_items(&queue, replay_request.clone(), None)
            .await
            .unwrap();
    }

    let reopened = SqliteRelationalBackend::open(&path).unwrap();
    assert_eq!(
        reopened
            .mutate_items(&queue, replay_request, None)
            .await
            .unwrap(),
        committed,
        "replay after reopen returns the stored response without re-evaluating the stale version"
    );
    drop(reopened);
    let _ = std::fs::remove_file(path);
}
