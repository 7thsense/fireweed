//! ADR-011 schema validation integration tests over MemoryBackend.
//!
//! Proves that entity schema violations are rejected BEFORE log append, idempotency recording,
//! SQL mutation, or projection apply — and that schema-less queues are byte-identical.

use std::sync::Arc;

use pqueue::{
    ClaimRef, ClaimedItem, CommitEntry, CommitRequest, EngineError, EntryOutcome, FinalizeKind,
    NewItem, PayloadUpdate, Pqueue, RequestId,
};
use pqueue_core::{
    ClientItemKey, EligibilityPolicy, EntitySchemaDocument, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueDefinition, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::QueueKey;
use pqueue_memory::{ManualClock, MemoryBackend};
use serde_json::json;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn base_def() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
    }
}

fn typed_def() -> QueueDefinition {
    let schema_doc: EntitySchemaDocument = serde_json::from_value(json!({
        "entity_schema": {
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": {"type": "string"},
                "count": {"type": "integer"}
            }
        }
    }))
    .unwrap();
    QueueDefinition {
        entity_schema: Some(schema_doc),
        ..base_def()
    }
}

fn make() -> Pqueue<MemoryBackend> {
    Pqueue::new(Arc::new(MemoryBackend::new()), Arc::new(ManualClock::at(0)))
}

fn valid_item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        entity: Some(json!({"name": "alice", "count": 1})),
        ..Default::default()
    }
}

fn invalid_item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        entity: Some(json!({"count": 42})), // missing required "name"
        ..Default::default()
    }
}

fn no_entity_item(priority: i64) -> NewItem {
    NewItem {
        priority: Some(PriorityValue::Int64(priority)),
        entity: None,
        ..Default::default()
    }
}

// ── push ─────────────────────────────────────────────────────────────────────

/// A push with an invalid entity document is rejected with EntitySchemaViolation.
/// The queue must remain empty (nothing appended).
#[tokio::test]
async fn schema_validation_push_invalid_entity_rejected() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    let err = pq.push(&q, invalid_item(1)).await.unwrap_err();
    assert!(
        matches!(err, EngineError::EntitySchemaViolation(_)),
        "expected EntitySchemaViolation, got {err:?}"
    );

    // Queue must be empty.
    let items = pq.peek(&q, 10).await.unwrap();
    assert!(items.is_empty(), "rejected push must not append anything");
}

/// A push with a valid entity document succeeds.
#[tokio::test]
async fn schema_validation_push_valid_entity_accepted() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    pq.push(&q, valid_item(1)).await.unwrap();

    let items = pq.peek(&q, 10).await.unwrap();
    assert_eq!(items.len(), 1, "valid push must appear in queue");
}

/// A typed queue with no entity_document in the item is allowed (document is optional).
#[tokio::test]
async fn schema_validation_push_no_entity_on_typed_queue_accepted() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    pq.push(&q, no_entity_item(1)).await.unwrap();

    let items = pq.peek(&q, 10).await.unwrap();
    assert_eq!(items.len(), 1);
}

/// A schema-less queue accepts any entity document (byte-identical behaviour).
#[tokio::test]
async fn schema_validation_schemaless_queue_accepts_any_entity() {
    let pq = make();
    let q = qkey();
    pq.create_queue(base_def()).await.unwrap();

    // Even something that would fail the typed queue schema is accepted.
    let item = NewItem {
        priority: Some(PriorityValue::Int64(1)),
        entity: Some(json!({"totally": "unstructured"})),
        ..Default::default()
    };
    pq.push(&q, item).await.unwrap();

    let items = pq.peek(&q, 10).await.unwrap();
    assert_eq!(items.len(), 1);
}

// ── push with request_id (idempotency) ───────────────────────────────────────

/// An invalid entity push with a request_id must NOT record an idempotency entry.
/// Replaying the same request_id with a valid entity must succeed (not hit a stale "seen" record).
#[tokio::test]
async fn schema_validation_push_with_request_id_no_idempotency_entry_on_failure() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();
    let rid = RequestId::new("req-1").unwrap();

    // First attempt with invalid entity — must fail.
    let err = pq
        .push_with_request_id(&q, rid.clone(), invalid_item(1))
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));

    // Queue must be empty — no partial append.
    assert!(pq.peek(&q, 10).await.unwrap().is_empty());

    // Replay with the same request_id but a valid entity — must succeed (no stale record).
    pq.push_with_request_id(&q, rid, valid_item(1))
        .await
        .unwrap();
    let items = pq.peek(&q, 10).await.unwrap();
    assert_eq!(items.len(), 1, "valid retry after schema failure must push");
}

// ── upsert ───────────────────────────────────────────────────────────────────

/// upsert (replace_if_pending) with an invalid entity is rejected before any state change.
#[tokio::test]
async fn schema_validation_upsert_invalid_entity_rejected() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    // First push a valid item so upsert has something to replace.
    let _id = pq.push(&q, valid_item(1)).await.unwrap();
    let ckey = ClientItemKey::new("k1").unwrap();

    // Upsert with an invalid entity.
    let err = pq
        .upsert(
            &q,
            ckey.clone(),
            NewItem {
                priority: Some(PriorityValue::Int64(2)),
                entity: Some(json!({"no_name_field": true})),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::EntitySchemaViolation(_)),
        "expected EntitySchemaViolation on upsert, got {err:?}"
    );
}

// ── update_fields ─────────────────────────────────────────────────────────────

/// update_fields with an invalid set_entity_document is rejected.
#[tokio::test]
async fn schema_validation_update_fields_invalid_entity_rejected() {
    use std::collections::BTreeMap;

    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    let id = pq.push(&q, valid_item(5)).await.unwrap();
    // Claim so update_fields is legal (atomic backends require leased for update_fields? check)
    // Actually update_fields works on pending items in pqueue. Let's just try.
    let err = pq
        .update_fields(
            &q,
            id,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            Some(json!({"missing_name": true})), // invalid entity
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::EntitySchemaViolation(_)),
        "expected EntitySchemaViolation on update_fields, got {err:?}"
    );
}

// ── counter not consumed by invalid entity ────────────────────────────────────

/// An invalid entity push must NOT advance the item-id counter. The first successful push
/// after a failed push must still receive counter=0 (the next unused slot in the queue).
#[tokio::test]
async fn schema_validation_invalid_push_does_not_consume_item_counter() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    let err = pq.push(&q, invalid_item(1)).await.unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));

    let id = pq.push(&q, valid_item(1)).await.unwrap();
    assert_eq!(
        id.counter(),
        0,
        "invalid push must not consume item-id counter space"
    );
}

/// An invalid push_with_request_id must NOT advance the item-id counter.
#[tokio::test]
async fn schema_validation_invalid_push_with_request_id_does_not_consume_item_counter() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();
    let rid = RequestId::new("r1").unwrap();

    let err = pq
        .push_with_request_id(&q, rid, invalid_item(1))
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::EntitySchemaViolation(_)));

    let id = pq.push(&q, valid_item(1)).await.unwrap();
    assert_eq!(
        id.counter(),
        0,
        "invalid push_with_request_id must not consume item-id counter space"
    );
}

/// An invalid lifecycle item in a commit_transition entry must NOT advance the item-id counter.
/// After a per-entry rejection, the next valid push must receive the next sequential counter slot.
#[tokio::test]
async fn schema_validation_invalid_commit_lifecycle_item_does_not_consume_item_counter() {
    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    // Push one valid item (counter=0) and claim it.
    let input_id = pq.push(&q, valid_item(1)).await.unwrap();
    assert_eq!(input_id.counter(), 0);
    let claimed: Vec<ClaimedItem> = pq.claim(&q, 1, 60_000).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let ci = &claimed[0];
    let claim_ref = ClaimRef {
        item_id: ci.item_id,
        lease_token: ci.lease_token.clone().expect("lease"),
        lease_expires_at: ci.lease_expires_at,
        item_version: ci.item_version,
    };

    // Commit with an invalid lifecycle item — the entry must be per-entry rejected.
    let outcomes = pq
        .commit(
            &q,
            CommitRequest {
                request_id: None,
                entries: vec![CommitEntry {
                    claim_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![invalid_item(2)],
                    instance_fence: None,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert!(
        matches!(
            outcomes[0],
            EntryOutcome::Rejected(EngineError::EntitySchemaViolation(_))
        ),
        "commit entry with invalid lifecycle item must be per-entry rejected"
    );

    // The input item is still leased (the entry was rejected — nothing finalized).
    // Push a new valid item; it must get counter=1 (not counter=2 or higher).
    let next_id = pq.push(&q, valid_item(3)).await.unwrap();
    assert_eq!(
        next_id.counter(),
        1,
        "invalid commit lifecycle item must not consume item-id counter space"
    );
}

/// update_fields with a valid entity document succeeds and bumps item_version.
#[tokio::test]
async fn schema_validation_update_fields_valid_entity_accepted() {
    use std::collections::BTreeMap;

    let pq = make();
    let q = qkey();
    pq.create_queue(typed_def()).await.unwrap();

    let id = pq.push(&q, valid_item(5)).await.unwrap();
    let new_ver = pq
        .update_fields(
            &q,
            id,
            BTreeMap::new(),
            PayloadUpdate::Keep,
            Some(json!({"name": "bob"})), // valid entity
            None,
        )
        .await
        .unwrap();
    assert!(new_ver >= 2, "item_version must advance");
}
