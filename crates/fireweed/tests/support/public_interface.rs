use std::collections::BTreeMap;
use std::future::Future;

use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateOutcome, BatchUpdateRequest, BatchUpdateValue,
    BoundedMutationRequest, BucketRule, ClaimAt, ClaimByQueryAt, ClaimByQueryRequest,
    ClaimCompatibility, ClaimRef, ClientItemKey, CohortOnIncomplete, CohortPolicy, CommitEntry,
    CommitEntryStatus, CommitRequest, CompoundIndexDef, CompoundIndexField, CreateQueue,
    DeclaredBucketSegmentRequest, DiscoveryGranularity, EligibilityPolicy, EngineError,
    FinalizeKind, Fireweed, GateKeyPolicy, GroupBatching, GroupByField, GroupKey,
    GroupedAggregateRequest, IndexDeclaration, IndexDef, IndexType, ItemMutationOperation,
    ItemMutationOutcome, ItemMutationRequest, ItemMutationReturning, ItemPatch, ItemPredicate,
    ItemSelector, ItemSelectorScope, LeaseGuard, MetricsByQueryRequest, MultiClaimCommitEntry,
    MultiClaimCommitRequest, MultiQueueClaimLimits, MultiQueueClaimTarget, MutationOutcome, Nack,
    NewItem, OrderField, OrderingMode, PayloadUpdate, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueryFilter, QueueCreationPolicy,
    QueueDefinition, QueueId, QueueIndex, QueueKey, QueueTemplate, RangeScanRequest,
    RecurrencePolicy, RequestId, RetryPolicy, ScheduleUpdate, SelectedMutation, SideRecord,
    SortDirection, TenantId, TypedValue, UtcTimestamp, WorkerId,
};
use serde_json::json;

pub async fn run(cell: &str, fireweed: &Fireweed, expect_projection_control: bool) {
    let mut failures = Vec::new();
    exercise_control(cell, fireweed, &mut failures).await;
    exercise_push_read_and_index(cell, fireweed, &mut failures).await;
    exercise_claim_and_finalize(cell, fireweed, &mut failures).await;
    exercise_rich_claims(cell, fireweed, &mut failures).await;
    exercise_mutation(cell, fireweed, &mut failures).await;
    exercise_commit(cell, fireweed, &mut failures).await;
    exercise_queries(cell, fireweed, &mut failures).await;
    exercise_projection(cell, fireweed, expect_projection_control, &mut failures).await;
    if !failures.is_empty() {
        panic!(
            "public Fireweed interface conformance failed for {cell}:\n{}",
            failures.join("\n")
        );
    }
}

async fn call<T, F>(cell: &str, method: &str, failures: &mut Vec<String>, future: F) -> Option<T>
where
    F: Future<Output = Result<T, EngineError>>,
{
    match future.await {
        Ok(value) => Some(value),
        Err(error) => {
            failures.push(format!("{cell}.{method}: {error}"));
            None
        }
    }
}

fn key(name: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new("public-interface").unwrap(),
        QueueId::new(name).unwrap(),
    )
}

fn check(cell: &str, method: &str, failures: &mut Vec<String>, condition: bool, detail: &str) {
    if !condition {
        failures.push(format!("{cell}.{method}: {detail}"));
    }
}

fn definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("public-interface").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
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
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: typed_indexes(),
        emit_change_records: true,
    }
}

fn rich_definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        max_eligible_group_size: Some(4),
        ..definition(name)
    }
}

fn cohort_definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        cohort_policy: Some(CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(30_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(10),
        }),
        ..definition(name)
    }
}

fn gated_definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        eligibility_policy: EligibilityPolicy {
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(8),
            ..EligibilityPolicy::default()
        },
        ..definition(name)
    }
}

fn template_definition(name: &str) -> CreateQueue {
    let definition = definition(name);
    CreateQueue {
        tenant_id: definition.tenant_id,
        queue_id: definition.queue_id,
        priority_model: definition.priority_model,
        ordering_mode: definition.ordering_mode,
        max_rank_error: definition.max_rank_error,
        progress_bound_ms: definition.progress_bound_ms,
        eligibility_policy: definition.eligibility_policy,
        cohort_policy: CohortPolicy::disabled(),
        recurrence: definition.recurrence,
        request_id_retention_ms: definition.request_id_retention_ms,
        client_item_key_retention_ms: definition.client_item_key_retention_ms,
        terminal_retention_ms: definition.terminal_retention_ms,
        max_lease_duration_ms: definition.max_lease_duration_ms,
        retry_policy: definition.retry_policy,
        max_push_batch_size: definition.max_push_batch_size,
        max_claim_batch_size: definition.max_claim_batch_size,
        max_eligible_group_size: definition.max_eligible_group_size,
        secondary_indexes: definition.secondary_indexes,
        entity_schema: definition.entity_schema,
        typed_indexes: definition.typed_indexes,
        emit_change_records: definition.emit_change_records,
    }
}

fn typed_indexes() -> Vec<QueueIndex> {
    vec![
        QueueIndex {
            name: "by_kind_due".into(),
            declaration: IndexDeclaration::Compound(CompoundIndexDef {
                fields: vec![
                    CompoundIndexField {
                        field: "kind".into(),
                        index_type: IndexType::String,
                    },
                    CompoundIndexField {
                        field: "due_at".into(),
                        index_type: IndexType::Datetime,
                    },
                ],
                unique: false,
            }),
        },
        QueueIndex {
            name: "by_score".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "score".into(),
                index_type: IndexType::Float,
                unique: false,
            }),
        },
        QueueIndex {
            name: "by_external_id".into(),
            declaration: IndexDeclaration::Single(IndexDef {
                field: "external_id".into(),
                index_type: IndexType::String,
                unique: true,
            }),
        },
    ]
}

fn item(label: &str, priority: i64) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(label).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(format!("payload-{label}").into()),
        entity: Some(json!({
            "kind": "work",
            "due_at": "2026-07-25T12:00:00Z",
            "score": priority as f64,
            "external_id": label,
            "mutated": false
        })),
        fields: BTreeMap::from([("label".into(), label.as_bytes().to_vec().into())]),
        ..Default::default()
    }
}

fn grouped_item(label: &str, priority: i64, group: &str) -> NewItem {
    NewItem {
        group_key: Some(GroupKey::new(group).unwrap()),
        ..item(label, priority)
    }
}

fn cohort_item(label: &str, priority: i64, group: &str, size: u64) -> NewItem {
    NewItem {
        cohort_size: Some(size),
        ..grouped_item(label, priority, group)
    }
}

async fn create(
    cell: &str,
    fireweed: &Fireweed,
    failures: &mut Vec<String>,
    name: &str,
) -> QueueKey {
    let queue = key(name);
    let outcome = call(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        fireweed.create_queue(definition(name)),
    )
    .await;
    check(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        outcome
            .as_ref()
            .is_some_and(|value| value.created && value.definition.queue_id == queue.queue_id),
        "did not report creation of the requested queue",
    );
    queue
}

async fn create_rich(
    cell: &str,
    fireweed: &Fireweed,
    failures: &mut Vec<String>,
    name: &str,
) -> QueueKey {
    let queue = key(name);
    let outcome = call(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        fireweed.create_queue(rich_definition(name)),
    )
    .await;
    check(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        outcome
            .as_ref()
            .is_some_and(|value| value.created && value.definition.queue_id == queue.queue_id),
        "did not create the rich-claim queue",
    );
    queue
}

async fn create_cohort(
    cell: &str,
    fireweed: &Fireweed,
    failures: &mut Vec<String>,
    name: &str,
) -> QueueKey {
    let queue = key(name);
    let outcome = call(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        fireweed.create_queue(cohort_definition(name)),
    )
    .await;
    check(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        outcome
            .as_ref()
            .is_some_and(|value| value.created && value.definition.queue_id == queue.queue_id),
        "did not create the cohort queue",
    );
    queue
}

async fn create_gated(
    cell: &str,
    fireweed: &Fireweed,
    failures: &mut Vec<String>,
    name: &str,
) -> QueueKey {
    let queue = key(name);
    let outcome = call(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        fireweed.create_queue(gated_definition(name)),
    )
    .await;
    check(
        cell,
        &format!("create_queue[{name}]"),
        failures,
        outcome
            .as_ref()
            .is_some_and(|value| value.created && value.definition.queue_id == queue.queue_id),
        "did not create the gated queue",
    );
    queue
}

fn claim_ref(item: &fireweed::ClaimedItem) -> Option<ClaimRef> {
    Some(ClaimRef {
        item_id: item.item_id,
        lease_token: item.lease_token.clone()?,
        lease_expires_at: item.lease_expires_at,
        item_version: item.item_version,
    })
}

fn valid_claim(item: &fireweed::ClaimedItem) -> bool {
    item.lease_token.is_some() && item.item_version > 0 && item.lease_expires_at.seconds > 0
}

async fn exercise_control(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "control").await;
    let stored = call(
        cell,
        "queue_definition",
        failures,
        fw.queue_definition(&queue),
    )
    .await;
    check(
        cell,
        "queue_definition",
        failures,
        stored
            .as_ref()
            .is_some_and(|value| value.queue_id == queue.queue_id),
        "did not return the created queue definition",
    );
    let template = QueueTemplate::new(
        template_definition("template"),
        QueueCreationPolicy::default(),
    );
    match fw.ensure_queue(&key("ensured"), &template).await {
        Ok(outcome) => check(
            cell,
            "ensure_queue",
            failures,
            outcome.definition.queue_id == QueueId::new("ensured").unwrap(),
            "returned a definition for the wrong queue",
        ),
        Err(error) => failures.push(format!("{cell}.ensure_queue: {error}")),
    }
    let _ = call(
        cell,
        "push[ownership]",
        failures,
        fw.push(&queue, item("ownership-probe", 0)),
    )
    .await;
    let ownership = call(cell, "ownership", failures, fw.ownership(&queue)).await;
    check(
        cell,
        "ownership",
        failures,
        matches!(ownership, Some(fireweed::Ownership::Mine { .. })),
        "active handle did not report ownership as mine after its first queue operation",
    );
    if let Err(error) = fw.renew_owned() {
        failures.push(format!("{cell}.renew_owned: {error}"));
    }
}

async fn exercise_push_read_and_index(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "push-read").await;
    let first = call(cell, "push", failures, fw.push(&queue, item("push", 1))).await;
    let requested = call(
        cell,
        "push_with_request_id",
        failures,
        fw.push_with_request_id(
            &queue,
            RequestId::new("push-request").unwrap(),
            item("push-request", 2),
        ),
    )
    .await;
    let requested_again = call(
        cell,
        "push_with_request_id[idempotent]",
        failures,
        fw.push_with_request_id(
            &queue,
            RequestId::new("push-request").unwrap(),
            item("push-request", 2),
        ),
    )
    .await;
    check(
        cell,
        "push_with_request_id",
        failures,
        requested.is_some() && requested == requested_again,
        "same request id did not converge on the same item id",
    );
    let batch = call(
        cell,
        "push_batch",
        failures,
        fw.push_batch(&queue, vec![item("batch-a", 3), item("batch-b", 4)]),
    )
    .await;
    check(
        cell,
        "push_batch",
        failures,
        batch.as_ref().is_some_and(|ids| ids.len() == 2),
        "did not return one id per input item",
    );
    let requested_batch = call(
        cell,
        "push_batch_with_request_id",
        failures,
        fw.push_batch_with_request_id(
            &queue,
            RequestId::new("batch-request").unwrap(),
            vec![item("batch-request-a", 5), item("batch-request-b", 6)],
        ),
    )
    .await;
    let requested_batch_again = call(
        cell,
        "push_batch_with_request_id[idempotent]",
        failures,
        fw.push_batch_with_request_id(
            &queue,
            RequestId::new("batch-request").unwrap(),
            vec![item("batch-request-a", 5), item("batch-request-b", 6)],
        ),
    )
    .await;
    check(
        cell,
        "push_batch_with_request_id",
        failures,
        requested_batch.as_ref().is_some_and(|ids| ids.len() == 2)
            && requested_batch == requested_batch_again,
        "same request id did not return the same two item ids",
    );
    let upsert = call(
        cell,
        "upsert",
        failures,
        fw.upsert(
            &queue,
            ClientItemKey::new("upsert").unwrap(),
            item("upsert", 7),
        ),
    )
    .await;
    let replaced = call(
        cell,
        "upsert[replace]",
        failures,
        fw.upsert(
            &queue,
            ClientItemKey::new("upsert").unwrap(),
            item("upsert", 8),
        ),
    )
    .await;
    check(
        cell,
        "upsert",
        failures,
        matches!(upsert, Some(fireweed::UpsertOutcome::Inserted { .. }))
            && matches!(replaced, Some(fireweed::UpsertOutcome::Replaced { .. })),
        "did not insert and then atomically replace the pending key",
    );
    let peeked = call(cell, "peek", failures, fw.peek(&queue, 10)).await;
    check(
        cell,
        "peek",
        failures,
        peeked.as_ref().is_some_and(|items| items.len() == 7),
        "did not expose the pushed pending items",
    );
    let position = call(
        cell,
        "current_position",
        failures,
        fw.current_position(&queue),
    )
    .await;
    check(
        cell,
        "current_position",
        failures,
        position.is_some_and(|value| value.sequence > 0 && value.queue == queue),
        "position did not advance after writes",
    );
    let scopes = call(
        cell,
        "discover_active_scopes",
        failures,
        fw.discover_active_scopes(&queue, DiscoveryGranularity::Queue),
    )
    .await;
    check(
        cell,
        "discover_active_scopes",
        failures,
        scopes.as_ref().is_some_and(|values| !values.is_empty()),
        "did not discover the active queue",
    );
    let stamped = call(
        cell,
        "discover_active_scopes_stamped",
        failures,
        fw.discover_active_scopes_stamped(&queue, DiscoveryGranularity::Queue),
    )
    .await;
    check(
        cell,
        "discover_active_scopes_stamped",
        failures,
        stamped.as_ref().is_some_and(|value| {
            value.queue == queue
                && value.granularity == DiscoveryGranularity::Queue
                && !value.scopes.is_empty()
        }),
        "did not return a correctly stamped active scope",
    );
    let discovered = call(
        cell,
        "discover",
        failures,
        fw.discover(&queue, DiscoveryGranularity::Queue),
    )
    .await;
    check(
        cell,
        "discover",
        failures,
        discovered.as_ref().is_some_and(|values| !values.is_empty()),
        "alias did not discover the active queue",
    );
    let live = call(
        cell,
        "live_item",
        failures,
        fw.live_item(&queue, ClientItemKey::new("push").unwrap()),
    )
    .await;
    check(
        cell,
        "live_item",
        failures,
        live.as_ref().is_some_and(|value| {
            value.as_ref().is_some_and(|item| {
                first == Some(item.item_id)
                    && item.payload.as_deref() == Some(b"payload-push".as_slice())
            })
        }),
        "did not return the pushed item's id and payload",
    );
    let lives = call(
        cell,
        "live_items",
        failures,
        fw.live_items(
            &queue,
            vec![
                ClientItemKey::new("push").unwrap(),
                ClientItemKey::new("batch-a").unwrap(),
            ],
        ),
    )
    .await;
    check(
        cell,
        "live_items",
        failures,
        lives
            .as_ref()
            .is_some_and(|values| values.len() == 2 && values.iter().all(Option::is_some)),
        "did not preserve input cardinality and resolve both keys",
    );
    let external_key = vec![b"push".to_vec()];
    let unique = call(
        cell,
        "query_index_unique",
        failures,
        fw.query_index_unique(&queue, "by_external_id", external_key.clone()),
    )
    .await;
    check(
        cell,
        "query_index_unique",
        failures,
        unique.as_ref().is_some_and(|hit| {
            hit.as_ref()
                .is_some_and(|hit| hit.client_item_key.as_str() == "push")
        }),
        "unique index did not resolve the pushed item",
    );
    let indexed = call(
        cell,
        "query_index",
        failures,
        fw.query_index(&queue, "by_external_id", external_key),
    )
    .await;
    check(
        cell,
        "query_index",
        failures,
        indexed
            .as_ref()
            .is_some_and(|hits| hits.len() == 1 && hits[0].client_item_key.as_str() == "push"),
        "index query did not return exactly the pushed item",
    );
    let typed = [json!("push")];
    let typed_unique = call(
        cell,
        "query_index_unique_typed",
        failures,
        fw.query_index_unique_typed(&queue, "by_external_id", &typed),
    )
    .await;
    check(
        cell,
        "query_index_unique_typed",
        failures,
        typed_unique.as_ref().is_some_and(|hit| hit.is_some()),
        "typed unique query did not resolve the pushed item",
    );
    let typed_hits = call(
        cell,
        "query_index_typed",
        failures,
        fw.query_index_typed(&queue, "by_external_id", &typed),
    )
    .await;
    check(
        cell,
        "query_index_typed",
        failures,
        typed_hits.as_ref().is_some_and(|hits| hits.len() == 1),
        "typed index query did not return exactly one item",
    );
    if first.is_none() {
        failures.push(format!(
            "{cell}.push prerequisite did not return an item id"
        ));
    }
}

async fn seed_claim(
    cell: &str,
    fw: &Fireweed,
    failures: &mut Vec<String>,
    name: &str,
) -> (QueueKey, Option<fireweed::ClaimedItem>) {
    let queue = create(cell, fw, failures, name).await;
    let _ = call(
        cell,
        &format!("push[{name}]"),
        failures,
        fw.push(&queue, item(&format!("{name}-item"), 1)),
    )
    .await;
    let claimed = call(
        cell,
        &format!("claim[{name}]"),
        failures,
        fw.claim(&queue, 1, 60_000),
    )
    .await
    .and_then(|mut values| values.pop());
    if claimed.is_none() {
        failures.push(format!(
            "{cell}.claim[{name}]: successful claim returned no item"
        ));
    }
    (queue, claimed)
}

async fn exercise_claim_and_finalize(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "claim-variants").await;
    let seeded = call(
        cell,
        "push[claim-variants]",
        failures,
        fw.push_batch(
            &queue,
            (0..8)
                .map(|index| item(&format!("claim-{index}"), index))
                .collect(),
        ),
    )
    .await;
    check(
        cell,
        "push[claim-variants]",
        failures,
        seeded.as_ref().is_some_and(|ids| ids.len() == 8),
        "did not seed all claim variants",
    );
    let claim_with = call(
        cell,
        "claim_with",
        failures,
        fw.claim_with(&queue, 1, 60_000, ClaimCompatibility::default()),
    )
    .await;
    check(
        cell,
        "claim_with",
        failures,
        claim_with
            .as_ref()
            .is_some_and(|items| items.len() == 1 && valid_claim(&items[0])),
        "did not lease exactly one eligible item",
    );
    let claim_response_with = call(
        cell,
        "claim_response_with",
        failures,
        fw.claim_response_with(&queue, 1, 60_000, ClaimCompatibility::default()),
    )
    .await;
    check(
        cell,
        "claim_response_with",
        failures,
        claim_response_with
            .as_ref()
            .is_some_and(|value| value.items.len() == 1 && valid_claim(&value.items[0])),
        "did not return exactly one leased item",
    );
    let claim_at = call(
        cell,
        "claim_at",
        failures,
        fw.claim_at(&queue, ClaimAt::new(1, 60_000)),
    )
    .await;
    check(
        cell,
        "claim_at",
        failures,
        claim_at
            .as_ref()
            .is_some_and(|items| items.len() == 1 && valid_claim(&items[0])),
        "did not lease exactly one eligible item",
    );
    let claim_response_at = call(
        cell,
        "claim_response_at",
        failures,
        fw.claim_response_at(&queue, ClaimAt::new(1, 60_000)),
    )
    .await;
    check(
        cell,
        "claim_response_at",
        failures,
        claim_response_at
            .as_ref()
            .is_some_and(|value| value.items.len() == 1 && valid_claim(&value.items[0])),
        "did not return exactly one leased item",
    );

    let query_request = ClaimByQueryRequest {
        index: Some("by_kind_due".into()),
        filters: vec![QueryFilter {
            field: "kind".into(),
            op: fireweed::FilterOp::Eq,
            value: TypedValue::String("work".into()),
        }],
        order_by: OrderField {
            field: "due_at".into(),
            direction: SortDirection::Ascending,
        },
        max_items: 1,
        lease_duration_ms: 60_000,
        worker_id: WorkerId::new("public-query-worker").unwrap(),
        request_id: Some(RequestId::new("public-query-claim").unwrap()),
    };
    let query_claim = call(
        cell,
        "claim_by_query",
        failures,
        fw.claim_by_query(&queue, query_request.clone()),
    )
    .await;
    check(
        cell,
        "claim_by_query",
        failures,
        query_claim
            .as_ref()
            .is_some_and(|value| value.items.len() == 1 && valid_claim(&value.items[0])),
        "query matched work but returned no leased item",
    );
    let query_claim_at = call(
        cell,
        "claim_by_query_at",
        failures,
        fw.claim_by_query_at(
            &queue,
            ClaimByQueryRequest {
                request_id: Some(RequestId::new("public-query-claim-at").unwrap()),
                ..query_request
            },
            ClaimByQueryAt::new().eligibility_time(UtcTimestamp::new(1_800_000_000, 0).unwrap()),
        ),
    )
    .await;
    check(
        cell,
        "claim_by_query_at",
        failures,
        query_claim_at
            .as_ref()
            .is_some_and(|value| value.items.len() == 1 && valid_claim(&value.items[0])),
        "query matched work but returned no leased item",
    );

    let a = create(cell, fw, failures, "multi-a").await;
    let b = create(cell, fw, failures, "multi-b").await;
    let _ = call(
        cell,
        "push[multi-a]",
        failures,
        fw.push(&a, item("multi-a", 1)),
    )
    .await;
    let _ = call(
        cell,
        "push[multi-b]",
        failures,
        fw.push(&b, item("multi-b", 1)),
    )
    .await;
    let across = call(
        cell,
        "claim_across_queues",
        failures,
        fw.claim_across_queues(
            vec![
                MultiQueueClaimTarget {
                    queue: a,
                    claim: ClaimAt::new(1, 60_000),
                },
                MultiQueueClaimTarget {
                    queue: b,
                    claim: ClaimAt::new(1, 60_000),
                },
            ],
            MultiQueueClaimLimits::default(),
        ),
    )
    .await;
    check(
        cell,
        "claim_across_queues",
        failures,
        across.as_ref().is_some_and(|results| {
            results.len() == 2
                && results.iter().all(|result| {
                    result.result.as_ref().is_ok_and(|claimed| {
                        claimed.items.len() == 1 && valid_claim(&claimed.items[0])
                    })
                })
        }),
        "did not return one claimed item from each target queue",
    );

    for method in ["ack", "complete", "release", "fail", "rearm", "purge"] {
        let scenario = format!("finalize-{method}");
        let (queue, claimed) = seed_claim(cell, fw, failures, &scenario).await;
        let Some(claimed) = claimed else { continue };
        match method {
            "ack" => {
                let _ = call(cell, method, failures, fw.ack(&queue, [claimed.item_id])).await;
            }
            "complete" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.complete(&queue, [claimed.item_id]),
                )
                .await;
            }
            "release" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.release(&queue, [claimed.item_id]),
                )
                .await;
            }
            "fail" => {
                let _ = call(cell, method, failures, fw.fail(&queue, [claimed.item_id])).await;
            }
            "rearm" => {
                let _ = call(cell, method, failures, fw.rearm(&queue, [claimed.item_id])).await;
            }
            _ => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.purge(&queue, [claimed.item_id], true),
                )
                .await;
            }
        }
        let expected_live = matches!(method, "release" | "rearm");
        let post_state = if expected_live {
            call(
                cell,
                &format!("{method}.post_claim"),
                failures,
                fw.claim(&queue, 1, 1_000),
            )
            .await
            .map(|items| items.len() == 1 && items[0].item_id == claimed.item_id)
        } else {
            call(
                cell,
                &format!("{method}.post_state"),
                failures,
                fw.live_item(
                    &queue,
                    ClientItemKey::new(format!("{scenario}-item")).unwrap(),
                ),
            )
            .await
            .map(|item| item.is_none())
        };
        check(
            cell,
            method,
            failures,
            post_state == Some(true),
            "successful lifecycle operation did not produce its expected terminal/pending state",
        );
    }

    for (method, delay) in [
        ("nack", None),
        ("retry", None),
        ("nack_retry_after", Some(1)),
        ("retry_after", Some(1)),
        ("rearm_at", None),
        ("rearm_after", Some(1)),
    ] {
        let scenario = format!("alias-{method}");
        let (queue, claimed) = seed_claim(cell, fw, failures, &scenario).await;
        let Some(claimed) = claimed else { continue };
        match method {
            "nack" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.nack(&queue, [claimed.item_id], Nack::Release),
                )
                .await;
            }
            "retry" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.retry(&queue, [claimed.item_id], None),
                )
                .await;
            }
            "nack_retry_after" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.nack_retry_after(&queue, [claimed.item_id], delay.unwrap()),
                )
                .await;
            }
            "retry_after" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.retry_after(&queue, [claimed.item_id], delay.unwrap()),
                )
                .await;
            }
            "rearm_at" => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.rearm_at(&queue, [claimed.item_id], UtcTimestamp::new(1, 0).unwrap()),
                )
                .await;
            }
            _ => {
                let _ = call(
                    cell,
                    method,
                    failures,
                    fw.rearm_after(&queue, [claimed.item_id], delay.unwrap()),
                )
                .await;
            }
        }
        let immediate = matches!(method, "nack" | "retry");
        let post_state = if immediate {
            call(
                cell,
                &format!("{method}.post_claim"),
                failures,
                fw.claim(&queue, 1, 1_000),
            )
            .await
            .map(|items| items.len() == 1 && items[0].item_id == claimed.item_id)
        } else {
            call(
                cell,
                &format!("{method}.post_state"),
                failures,
                fw.live_item(
                    &queue,
                    ClientItemKey::new(format!("{scenario}-item")).unwrap(),
                ),
            )
            .await
            .map(|item| item.is_some_and(|item| item.not_before.is_some()))
        };
        check(
            cell,
            method,
            failures,
            post_state == Some(true),
            "successful retry/release/rearm did not return the item to pending",
        );
    }

    let (queue, claimed) = seed_claim(cell, fw, failures, "lease-ops").await;
    if let Some(claimed) = claimed {
        let renewed = call(
            cell,
            "renew",
            failures,
            fw.renew(&queue, [claimed.item_id], 60_000),
        )
        .await;
        check(
            cell,
            "renew",
            failures,
            renewed.is_some(),
            "did not renew the lease",
        );
        let reassigned = call(
            cell,
            "reassign",
            failures,
            fw.reassign(&queue, [claimed.item_id], 60_000),
        )
        .await;
        check(
            cell,
            "reassign",
            failures,
            reassigned.is_some(),
            "did not reassign the lease",
        );
        let leased = call(
            cell,
            "claimed",
            failures,
            fw.claimed(&queue, &[claimed.item_id]),
        )
        .await;
        check(
            cell,
            "claimed/renew/reassign",
            failures,
            leased.as_ref().is_some_and(|items| {
                items.len() == 1
                    && items[0].item_id == claimed.item_id
                    && items[0].lease_token.is_some()
            }),
            "renewed/reassigned lease was not observable through claimed",
        );
    }
    let reclaimed = call(
        cell,
        "reclaim_expired",
        failures,
        fw.reclaim_expired(&queue, Some(100)),
    )
    .await;
    check(
        cell,
        "reclaim_expired",
        failures,
        reclaimed.as_ref().is_some_and(Vec::is_empty),
        "fresh lease was unexpectedly reclaimed",
    );
    let reclaimed_at = call(
        cell,
        "reclaim_expired_at",
        failures,
        fw.reclaim_expired_at(
            &queue,
            Some(100),
            UtcTimestamp::new(4_000_000_000, 0).unwrap(),
        ),
    )
    .await;
    check(
        cell,
        "reclaim_expired_at",
        failures,
        reclaimed_at.as_ref().is_some_and(|ids| !ids.is_empty()),
        "expired lease was not reclaimed",
    );
    let metrics = call(cell, "metrics", failures, fw.metrics(&queue)).await;
    check(
        cell,
        "metrics",
        failures,
        metrics
            .as_ref()
            .is_some_and(|value| value.pending >= 1 && value.leased == 0),
        "metrics did not reflect the reclaimed pending item",
    );
}

async fn exercise_rich_claims(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let whole_group = create_rich(cell, fw, failures, "rich-whole-group").await;
    let whole_group_ids = call(
        cell,
        "push[whole_group]",
        failures,
        fw.push_batch(
            &whole_group,
            vec![
                grouped_item("whole-g1-a", 1, "g1"),
                grouped_item("whole-g1-b", 2, "g1"),
                grouped_item("whole-g2-a", 3, "g2"),
            ],
        ),
    )
    .await;
    let whole_group_claim = call(
        cell,
        "claim_with[whole_group]",
        failures,
        fw.claim_with(
            &whole_group,
            4,
            60_000,
            ClaimCompatibility {
                group_batching: Some(GroupBatching { max_groups: 2 }),
                ..Default::default()
            },
        ),
    )
    .await;
    check(
        cell,
        "claim_with[whole_group]",
        failures,
        whole_group_claim.as_ref().is_some_and(|items| {
            whole_group_ids.as_ref().is_some_and(|expected| {
                items.iter().map(|item| item.item_id).collect::<Vec<_>>() == *expected
                    && items
                        .iter()
                        .filter(|item| {
                            item.group_key
                                .as_ref()
                                .is_some_and(|group| group.as_str() == "g1")
                        })
                        .count()
                        == 2
                    && items
                        .iter()
                        .filter(|item| {
                            item.group_key
                                .as_ref()
                                .is_some_and(|group| group.as_str() == "g2")
                        })
                        .count()
                        == 1
            })
        }),
        "did not return both complete groups with correct membership",
    );

    let same_group = create_rich(cell, fw, failures, "rich-same-group").await;
    let same_group_ids = call(
        cell,
        "push[same_group]",
        failures,
        fw.push_batch(
            &same_group,
            vec![
                grouped_item("same-g1-a", 1, "g1"),
                grouped_item("same-g1-b", 2, "g1"),
                grouped_item("same-g1-c", 3, "g1"),
                grouped_item("same-g2-a", 20, "g2"),
            ],
        ),
    )
    .await;
    let same_group_claim = call(
        cell,
        "claim_with[same_group_key]",
        failures,
        fw.claim_with(
            &same_group,
            2,
            60_000,
            ClaimCompatibility {
                same_group_key: true,
                ..Default::default()
            },
        ),
    )
    .await;
    check(
        cell,
        "claim_with[same_group_key]",
        failures,
        same_group_claim.as_ref().is_some_and(|items| {
            same_group_ids.as_ref().is_some_and(|expected| {
                items.len() == 2
                    && items
                        .iter()
                        .map(|item| item.item_id)
                        .eq(expected[..2].iter().copied())
                    && items.iter().all(|item| {
                        item.group_key
                            .as_ref()
                            .is_some_and(|group| group.as_str() == "g1")
                    })
            })
        }),
        "did not select two members from exactly the oldest group",
    );

    let cohort = create_cohort(cell, fw, failures, "rich-whole-cohort").await;
    let _ = call(
        cell,
        "push[incomplete_cohort]",
        failures,
        fw.push(&cohort, cohort_item("incomplete-a", 1, "incomplete", 2)),
    )
    .await;
    let cohort_ids = call(
        cell,
        "push[complete_cohort]",
        failures,
        fw.push_batch(
            &cohort,
            vec![
                cohort_item("complete-a", 10, "complete", 2),
                cohort_item("complete-b", 11, "complete", 2),
            ],
        ),
    )
    .await;
    let cohort_claim = call(
        cell,
        "claim_response_with[whole_cohort]",
        failures,
        fw.claim_response_with(
            &cohort,
            10,
            60_000,
            ClaimCompatibility {
                whole_cohort: true,
                ..Default::default()
            },
        ),
    )
    .await;
    check(
        cell,
        "claim_response_with[whole_cohort]",
        failures,
        cohort_claim.as_ref().is_some_and(|claimed| {
            cohort_ids.as_ref().is_some_and(|expected| {
                claimed
                    .items
                    .iter()
                    .map(|item| item.item_id)
                    .collect::<Vec<_>>()
                    == *expected
                    && claimed.cohort_id.is_some()
                    && claimed.cohort_lease_token.is_some()
                    && claimed.items.iter().all(|item| {
                        item.lease_token.is_none()
                            && item
                                .group_key
                                .as_ref()
                                .is_some_and(|group| group.as_str() == "complete")
                    })
            })
        }),
        "did not skip the incomplete cohort and return the complete cohort atomically",
    );
}

async fn exercise_mutation(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "mutation").await;
    let Some(id) = call(
        cell,
        "push[mutation]",
        failures,
        fw.push(&queue, item("mutation-item", 1)),
    )
    .await
    else {
        return;
    };
    let version = call(
        cell,
        "update",
        failures,
        fw.update(
            &queue,
            id,
            ScheduleUpdate::Set(Some(PriorityValue::Int64(2))),
            ScheduleUpdate::Keep,
            Some(1),
        ),
    )
    .await;
    check(
        cell,
        "update",
        failures,
        version.is_some_and(|value| value > 1),
        "did not advance item_version",
    );
    let field_version = call(
        cell,
        "update_fields",
        failures,
        fw.update_fields(
            &queue,
            id,
            BTreeMap::from([("updated".into(), Some(b"yes".to_vec().into()))]),
            PayloadUpdate::Keep,
            None,
            version,
        ),
    )
    .await;
    check(
        cell,
        "update_fields",
        failures,
        field_version.is_some_and(|value| version.is_some_and(|prior| value > prior)),
        "did not advance item_version after field mutation",
    );
    let live = call(
        cell,
        "update/update_fields.post_state",
        failures,
        fw.live_item(&queue, ClientItemKey::new("mutation-item").unwrap()),
    )
    .await;
    check(
        cell,
        "update/update_fields",
        failures,
        live.as_ref().is_some_and(|value| {
            value.as_ref().is_some_and(|item| {
                item.priority == Some(PriorityValue::Int64(2))
                    && item
                        .fields
                        .get("updated")
                        .is_some_and(|value| value.as_ref() == b"yes")
            })
        }),
        "priority/field mutations were not observable",
    );

    let gated_queue = create_gated(cell, fw, failures, "gates").await;
    let mut gated = item("gated-item", 1);
    gated.gate_keys = vec!["gate-a".into()];
    let _ = call(cell, "push[gates]", failures, fw.push(&gated_queue, gated)).await;
    let _ = call(
        cell,
        "set_gates[block]",
        failures,
        fw.set_gates(&gated_queue, vec!["gate-a".into()], true),
    )
    .await;
    let blocked = call(
        cell,
        "set_gates.blocked_claim",
        failures,
        fw.claim(&gated_queue, 1, 1_000),
    )
    .await;
    check(
        cell,
        "set_gates[block]",
        failures,
        blocked.as_ref().is_some_and(Vec::is_empty),
        "blocked gate did not prevent claim",
    );
    let _ = call(
        cell,
        "set_gates[unblock]",
        failures,
        fw.set_gates(&gated_queue, vec!["gate-a".into()], false),
    )
    .await;
    let unblocked = call(
        cell,
        "set_gates.unblocked_claim",
        failures,
        fw.claim(&gated_queue, 1, 1_000),
    )
    .await;
    check(
        cell,
        "set_gates[unblock]",
        failures,
        unblocked
            .as_ref()
            .is_some_and(|items| items.len() == 1 && items[0].gate_keys == ["gate-a".to_owned()]),
        "unblocked gate did not make its member claimable",
    );

    let request = BatchUpdateRequest {
        request_id: RequestId::new("batch-update-request").unwrap(),
        updates: vec![BatchUpdateEntry {
            item_ref: BatchUpdateItemRef::ItemId(id),
            expected_item_version: field_version,
            priority: BatchUpdateValue::Keep,
            not_before: BatchUpdateValue::Keep,
            payload: BatchUpdateValue::Replace(Some(b"batch-updated".to_vec().into())),
            metadata: BatchUpdateValue::Keep,
            gate_keys: BatchUpdateValue::Keep,
            fields: BatchUpdateValue::Keep,
        }],
    };
    let batch = call(
        cell,
        "batch_update",
        failures,
        fw.batch_update(&queue, request.clone()),
    )
    .await;
    let replay = call(
        cell,
        "batch_update[idempotent]",
        failures,
        fw.batch_update(&queue, request),
    )
    .await;
    check(
        cell,
        "batch_update",
        failures,
        batch.as_ref().is_some_and(|value| {
            value.results.len() == 1
                && matches!(value.results[0], BatchUpdateOutcome::Updated { item_id, .. } if item_id == id)
        }) && batch == replay,
        "did not update exactly once and replay the same response",
    );
    let batch_live = call(
        cell,
        "batch_update.post_state",
        failures,
        fw.live_item(&queue, ClientItemKey::new("mutation-item").unwrap()),
    )
    .await;
    check(
        cell,
        "batch_update",
        failures,
        batch_live.as_ref().is_some_and(|value| {
            value
                .as_ref()
                .is_some_and(|item| item.payload.as_deref() == Some(b"batch-updated".as_slice()))
        }),
        "payload replacement was not observable",
    );

    let item_version = batch_live
        .as_ref()
        .and_then(|value| value.as_ref())
        .map(|item| item.item_version);
    let mutation_request = ItemMutationRequest {
        request_id: RequestId::new("selector-mutation-request").unwrap(),
        evaluated_at: UtcTimestamp::new(1_800_000_000, 0).unwrap(),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![
                SelectedMutation {
                    selector_id: "matching-clause".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![
                            ItemPredicate::ClientItemKeyEq(
                                ClientItemKey::new("mutation-item").unwrap(),
                            ),
                            ItemPredicate::FieldEq {
                                name: "selector-mutated".into(),
                                value: None,
                            },
                        ],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(4))),
                        field_edits: BTreeMap::from([(
                            "selector-mutated".into(),
                            Some(bytes::Bytes::from_static(b"yes")),
                        )]),
                        ..ItemPatch::default()
                    },
                },
                SelectedMutation {
                    selector_id: "must-not-run".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![ItemPredicate::ClientItemKeyEq(
                            ClientItemKey::new("mutation-item").unwrap(),
                        )],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(99))),
                        ..ItemPatch::default()
                    },
                },
            ],
        },
    };
    let mut dry_run = mutation_request.clone();
    dry_run.dry_run = true;
    let preview = call(
        cell,
        "mutate_items[dry_run]",
        failures,
        fw.mutate_items(&queue, dry_run),
    )
    .await;
    check(
        cell,
        "mutate_items[dry_run]",
        failures,
        preview.as_ref().is_some_and(|response| {
            response.position.is_none()
                && response.summary.changed == 1
                && response.results.len() == 1
                && response.results[0].selector_id.as_deref() == Some("matching-clause")
                && matches!(
                    response.results[0].outcome,
                    ItemMutationOutcome::WouldUpdate { .. }
                )
        }),
        "dry-run did not resolve the first selector without committing",
    );
    let after_preview = call(
        cell,
        "mutate_items[dry_run].post_state",
        failures,
        fw.live_item(&queue, ClientItemKey::new("mutation-item").unwrap()),
    )
    .await;
    check(
        cell,
        "mutate_items[dry_run]",
        failures,
        after_preview.as_ref().is_some_and(|value| {
            value.as_ref().is_some_and(|item| {
                Some(item.item_version) == item_version
                    && !item.fields.contains_key("selector-mutated")
            })
        }),
        "dry-run changed the item or its version",
    );

    let mutation = call(
        cell,
        "mutate_items",
        failures,
        fw.mutate_items(&queue, mutation_request.clone()),
    )
    .await;
    let mutation_replay = call(
        cell,
        "mutate_items[idempotent]",
        failures,
        fw.mutate_items(&queue, mutation_request.clone()),
    )
    .await;
    check(
        cell,
        "mutate_items",
        failures,
        mutation.as_ref().is_some_and(|response| {
            response.position.is_some()
                && response.summary.changed == 1
                && response.results.len() == 1
                && response.results[0].selector_id.as_deref() == Some("matching-clause")
                && matches!(
                    response.results[0].outcome,
                    ItemMutationOutcome::Updated { .. }
                )
        }) && mutation == mutation_replay,
        "selector mutation did not commit once and replay exactly",
    );
    let mut changed_body = mutation_request;
    let ItemMutationOperation::SelectFirst { clauses } = &mut changed_body.operation else {
        unreachable!("shared mutation request uses SelectFirst")
    };
    clauses[0].patch.priority = BatchUpdateValue::Replace(Some(PriorityValue::Int64(5)));
    check(
        cell,
        "mutate_items[request_id_conflict]",
        failures,
        matches!(
            fw.mutate_items(&queue, changed_body).await,
            Err(EngineError::RequestIdConflict)
        ),
        "changed mutation body did not conflict with the retained request id",
    );
    let mutated_live = call(
        cell,
        "mutate_items.post_state",
        failures,
        fw.live_item(&queue, ClientItemKey::new("mutation-item").unwrap()),
    )
    .await;
    check(
        cell,
        "mutate_items",
        failures,
        mutated_live.as_ref().is_some_and(|value| {
            value.as_ref().is_some_and(|item| {
                item.priority == Some(PriorityValue::Int64(4))
                    && item
                        .fields
                        .get("selector-mutated")
                        .is_some_and(|value| value.as_ref() == b"yes")
                    && item_version.is_some_and(|before| item.item_version == before + 1)
            })
        }),
        "selector mutation state, first-match ownership, or version increment was incorrect",
    );

    // Lease invalidation must remove the old claimed-row selection and token/version reference before the
    // item can be selected for a replacement claim. The first selector deliberately stops matching after
    // the mutation; a replay that reevaluates would fall through to the priority-99 clause.
    let lease_queue = create(cell, fw, failures, "mutation-lease").await;
    let lease_key = ClientItemKey::new("mutation-leased-item").unwrap();
    let leased_id = call(
        cell,
        "mutate_items[lease].push",
        failures,
        fw.push(
            &lease_queue,
            NewItem {
                client_item_key: Some(lease_key.clone()),
                priority: Some(PriorityValue::Int64(10)),
                ..NewItem::default()
            },
        ),
    )
    .await;
    let claimed = call(
        cell,
        "mutate_items[lease].claim",
        failures,
        fw.claim(&lease_queue, 1, 60_000),
    )
    .await
    .unwrap_or_default();
    let Some(old_claimed) = claimed.first().cloned() else {
        failures.push(format!(
            "{cell}.mutate_items[lease]: no claimed item prerequisite"
        ));
        return;
    };
    check(
        cell,
        "mutate_items[lease].claim",
        failures,
        leased_id == Some(old_claimed.item_id),
        "claim did not return the seeded lease-invalidation item",
    );
    let Some(old_ref) = claim_ref(&old_claimed) else {
        failures.push(format!(
            "{cell}.mutate_items[lease]: claimed item omitted its claim reference"
        ));
        return;
    };
    let old_token = old_ref.lease_token.clone();
    let lease_request = ItemMutationRequest {
        request_id: RequestId::new("selector-lease-invalidation").unwrap(),
        evaluated_at: UtcTimestamp::new(
            old_claimed.lease_expires_at.seconds.saturating_sub(1),
            old_claimed.lease_expires_at.nanoseconds,
        )
        .unwrap(),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![
                SelectedMutation {
                    selector_id: "active-old-claim".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![
                            ItemPredicate::ClientItemKeyEq(lease_key.clone()),
                            ItemPredicate::LeaseActive(true),
                        ],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::InvalidateActive,
                    patch: ItemPatch {
                        lifecycle: fireweed::LifecyclePatch::SetPending,
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(5))),
                        field_edits: BTreeMap::from([(
                            "lease-invalidated".into(),
                            Some(bytes::Bytes::from_static(b"yes")),
                        )]),
                        ..ItemPatch::default()
                    },
                },
                SelectedMutation {
                    selector_id: "must-not-reevaluate".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![ItemPredicate::ClientItemKeyEq(lease_key.clone())],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::InvalidateActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(99))),
                        ..ItemPatch::default()
                    },
                },
            ],
        },
    };
    let invalidated = call(
        cell,
        "mutate_items[lease]",
        failures,
        fw.mutate_items(&lease_queue, lease_request.clone()),
    )
    .await;
    let invalidated_replay = call(
        cell,
        "mutate_items[lease][idempotent]",
        failures,
        fw.mutate_items(&lease_queue, lease_request),
    )
    .await;
    check(
        cell,
        "mutate_items[lease]",
        failures,
        invalidated.as_ref().is_some_and(|response| {
            response.summary.changed == 1
                && response.results[0].selector_id.as_deref() == Some("active-old-claim")
                && matches!(
                    response.results[0].outcome,
                    ItemMutationOutcome::Updated {
                        state: fireweed::ItemState::Pending,
                        ..
                    }
                )
        }) && invalidated == invalidated_replay,
        "active lease was not invalidated exactly once or replay reevaluated its selector",
    );
    let old_claim_selection = call(
        cell,
        "mutate_items[lease].claimed_after_invalidation",
        failures,
        fw.claimed(&lease_queue, &[old_claimed.item_id]),
    )
    .await;
    let lease_metrics = call(
        cell,
        "mutate_items[lease].metrics_after_invalidation",
        failures,
        fw.metrics(&lease_queue),
    )
    .await;
    check(
        cell,
        "mutate_items[lease].selection_invalidation",
        failures,
        old_claim_selection.as_ref().is_some_and(Vec::is_empty)
            && lease_metrics
                .as_ref()
                .is_some_and(|metrics| metrics.pending == 1 && metrics.leased == 0),
        "old claimed-row selection remained visible after lease invalidation",
    );
    let replacement = call(
        cell,
        "mutate_items[lease].replacement_claim",
        failures,
        fw.claim(&lease_queue, 1, 60_000),
    )
    .await
    .unwrap_or_default();
    check(
        cell,
        "mutate_items[lease].replacement_claim",
        failures,
        matches!(replacement.as_slice(), [fresh]
            if fresh.item_id == old_claimed.item_id
                && fresh.lease_token.as_ref().is_some_and(|token| token != &old_token)
                && fresh.item_version > old_claimed.item_version),
        "replacement claim did not receive a fresh token/version after every old reference was invalidated",
    );
    let stale_commit = fw
        .commit(
            &lease_queue,
            CommitRequest {
                request_id: Some(RequestId::new("stale-invalidated-claim").unwrap()),
                entries: vec![CommitEntry {
                    claim_ref: old_ref,
                    finalize: FinalizeKind::Complete,
                    side_records: vec![],
                    lifecycle_items: vec![],
                    instance_fence: None,
                }],
            },
        )
        .await;
    check(
        cell,
        "mutate_items[lease].stale_claim_ref",
        failures,
        matches!(
            stale_commit,
            Ok(ref outcomes)
                if matches!(
                    outcomes.as_slice(),
                    [fireweed::EntryOutcome::Rejected(EngineError::StaleLease)]
                )
        ),
        "old lease token/version claim reference remained usable after replacement claiming",
    );
}

async fn exercise_commit(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "commit").await;
    let _ = call(
        cell,
        "push[commit]",
        failures,
        fw.push_batch(
            &queue,
            vec![
                item("commit-a", 1),
                item("commit-b", 2),
                item("commit-c", 3),
            ],
        ),
    )
    .await;
    let claimed = call(cell, "claim[commit]", failures, fw.claim(&queue, 3, 60_000))
        .await
        .unwrap_or_default();
    let refs = claimed.iter().filter_map(claim_ref).collect::<Vec<_>>();
    if let Some(first) = refs.first().cloned() {
        let request_id = RequestId::new("commit-request").unwrap();
        let committed = call(
            cell,
            "commit",
            failures,
            fw.commit(
                &queue,
                CommitRequest {
                    request_id: Some(request_id.clone()),
                    entries: vec![CommitEntry {
                        claim_ref: first,
                        finalize: FinalizeKind::Complete,
                        side_records: vec![SideRecord {
                            key: b"public-side-record".to_vec(),
                            payload: b"value".to_vec().into(),
                        }],
                        lifecycle_items: vec![item("commit-continuation", 4)],
                        instance_fence: None,
                    }],
                },
            ),
        )
        .await;
        check(
            cell,
            "commit",
            failures,
            committed.as_ref().is_some_and(|outcomes| {
                matches!(outcomes.as_slice(), [fireweed::EntryOutcome::Committed { lifecycle_item_ids }] if lifecycle_item_ids.len() == 1)
            }),
            "did not commit the input and create exactly one lifecycle item",
        );
        let recovery = call(
            cell,
            "explain_commit",
            failures,
            fw.explain_commit(&queue, request_id.clone()),
        )
        .await;
        check(
            cell,
            "explain_commit",
            failures,
            recovery.as_ref().is_some_and(|value| value.as_ref().is_some_and(|record| {
                record.request_id == request_id
                    && matches!(record.entries.as_slice(), [entry] if entry.status == CommitEntryStatus::Committed)
            })),
            "did not reconstruct the committed transition",
        );
        let side_record = call(
            cell,
            "side_record",
            failures,
            fw.side_record(&queue, b"public-side-record"),
        )
        .await;
        check(
            cell,
            "side_record",
            failures,
            side_record
                .as_ref()
                .is_some_and(|value| value.as_deref() == Some(b"value".as_slice())),
            "did not return the committed side-record payload",
        );
    } else {
        failures.push(format!("{cell}.commit: no valid claimed item prerequisite"));
    }
    if refs.len() >= 3 {
        let multi = call(
            cell,
            "commit_multi_claim",
            failures,
            fw.commit_multi_claim(
                &queue,
                MultiClaimCommitRequest {
                    request_id: Some(RequestId::new("multi-commit-request").unwrap()),
                    entries: vec![MultiClaimCommitEntry {
                        claim_ref: refs[1].clone(),
                        additional_claim_refs: vec![refs[2].clone()],
                        finalize: FinalizeKind::Complete,
                        side_records: vec![],
                        lifecycle_items: vec![],
                        instance_fence: None,
                    }],
                },
            ),
        )
        .await;
        check(
            cell,
            "commit_multi_claim",
            failures,
            multi.as_ref().is_some_and(|outcomes| {
                matches!(
                    outcomes.as_slice(),
                    [fireweed::EntryOutcome::Committed { .. }]
                )
            }),
            "did not atomically commit the multi-claim entry",
        );
    } else {
        failures.push(format!(
            "{cell}.commit_multi_claim: fewer than three claimed prerequisites"
        ));
    }
    match fw.commit_capabilities(&queue) {
        Ok(capabilities) => check(
            cell,
            "commit_capabilities",
            failures,
            capabilities.atomic_transition_commit
                && capabilities.vectorized_commit
                && capabilities.lease_validation
                && capabilities.retained_commit_idempotency
                && capabilities.non_work_side_records
                && capabilities.authoritative_recovery_reads,
            "did not advertise the guarantees exercised by the public commit surface",
        ),
        Err(error) => failures.push(format!("{cell}.commit_capabilities: {error}")),
    }
}

async fn exercise_queries(cell: &str, fw: &Fireweed, failures: &mut Vec<String>) {
    let queue = create(cell, fw, failures, "queries").await;
    let _ = call(
        cell,
        "push[queries]",
        failures,
        fw.push_batch(&queue, vec![item("query-a", 1), item("query-b", 2)]),
    )
    .await;
    let filters = vec![QueryFilter {
        field: "kind".into(),
        op: fireweed::FilterOp::Eq,
        value: TypedValue::String("work".into()),
    }];
    let metrics = call(
        cell,
        "metrics_by_query",
        failures,
        fw.metrics_by_query(
            &queue,
            MetricsByQueryRequest {
                index: Some("by_kind_due".into()),
                filters: filters.clone(),
            },
        ),
    )
    .await;
    check(
        cell,
        "metrics_by_query",
        failures,
        metrics.as_ref().is_some_and(|value| value.pending == 2),
        "query metrics did not count both matching pending items",
    );
    let capabilities = fw.hot_projection_capabilities(&queue);
    check(
        cell,
        "hot_projection_capabilities",
        failures,
        capabilities.range_scan
            && capabilities.grouped_aggregate
            && capabilities.declared_bucket_segment
            && capabilities.bounded_mutation
            && capabilities.claim_by_query
            && !capabilities.side_record_query,
        "did not advertise the complete supported query surface",
    );
    let range = call(
        cell,
        "range_scan",
        failures,
        fw.range_scan(
            &queue,
            RangeScanRequest {
                index: Some("by_kind_due".into()),
                filters: filters.clone(),
                order_by: vec![OrderField {
                    field: "due_at".into(),
                    direction: SortDirection::Ascending,
                }],
                page_size: 100,
                cursor: None,
            },
        ),
    )
    .await;
    check(
        cell,
        "range_scan",
        failures,
        range.as_ref().is_some_and(|value| value.rows.len() == 2),
        "did not return both matching rows",
    );
    let grouped = call(
        cell,
        "grouped_aggregate",
        failures,
        fw.grouped_aggregate(
            &queue,
            GroupedAggregateRequest {
                index: Some("by_kind_due".into()),
                filters: vec![],
                group_by: vec![GroupByField {
                    field: "kind".into(),
                    time_bucket: None,
                }],
                max_groups: 10,
            },
        ),
    )
    .await;
    check(
        cell,
        "grouped_aggregate",
        failures,
        grouped
            .as_ref()
            .is_some_and(|value| value.groups.len() == 1 && value.groups[0].count == 2),
        "did not aggregate the two work items into one group",
    );
    let buckets = call(
        cell,
        "declared_bucket_segment",
        failures,
        fw.declared_bucket_segment(
            &queue,
            DeclaredBucketSegmentRequest {
                index: Some("by_score".into()),
                filters: vec![],
                field: "score".into(),
                buckets: vec![BucketRule {
                    label: "all".into(),
                    exact: None,
                    gt: None,
                    gte: Some(0.0),
                    lt: Some(10.0),
                    lte: None,
                }],
                null_bucket_label: "missing".into(),
            },
        ),
    )
    .await;
    check(
        cell,
        "declared_bucket_segment",
        failures,
        buckets.as_ref().is_some_and(|value| {
            value
                .buckets
                .iter()
                .any(|bucket| bucket.label == "all" && bucket.count == 2)
        }),
        "did not count both numeric values in the declared bucket",
    );
    let mutated = call(
        cell,
        "bounded_mutation",
        failures,
        fw.bounded_mutation(
            &queue,
            BoundedMutationRequest {
                index: Some("by_kind_due".into()),
                filters,
                set_fields: BTreeMap::from([("score".into(), TypedValue::Float(9.0))]),
                max_scan_rows: 100,
            },
        ),
    )
    .await;
    check(
        cell,
        "bounded_mutation",
        failures,
        mutated.as_ref().is_some_and(|value| {
            value.results.len() == 2
                && value
                    .results
                    .iter()
                    .all(|result| result.outcome == MutationOutcome::Updated)
        }),
        "did not report both matching records as updated",
    );
    let post_mutation = call(
        cell,
        "bounded_mutation.post_state",
        failures,
        fw.declared_bucket_segment(
            &queue,
            DeclaredBucketSegmentRequest {
                index: Some("by_score".into()),
                filters: vec![],
                field: "score".into(),
                buckets: vec![BucketRule {
                    label: "updated".into(),
                    exact: Some(9.0),
                    gt: None,
                    gte: None,
                    lt: None,
                    lte: None,
                }],
                null_bucket_label: "missing".into(),
            },
        ),
    )
    .await;
    check(
        cell,
        "bounded_mutation",
        failures,
        post_mutation.as_ref().is_some_and(|value| {
            value
                .buckets
                .iter()
                .any(|bucket| bucket.label == "updated" && bucket.count == 2)
        }),
        "updated indexed values were not observable",
    );
}

async fn exercise_projection(
    cell: &str,
    fw: &Fireweed,
    expect_projection_control: bool,
    failures: &mut Vec<String>,
) {
    let control = fw.projection_control();
    check(
        cell,
        "projection_control",
        failures,
        control.is_some() == expect_projection_control,
        "projection lifecycle availability did not match the configured composition",
    );
    let Some(control) = control else {
        return;
    };
    let capabilities = control.capabilities();
    check(
        cell,
        "projection.capabilities",
        failures,
        capabilities.verify && capabilities.delete && capabilities.rebuild,
        "projection control did not advertise all lifecycle operations",
    );
    let verified = call(cell, "projection.verify", failures, control.verify()).await;
    check(
        cell,
        "projection.verify",
        failures,
        verified.as_ref().is_some_and(|value| {
            value.compatible && value.projection_sequence == value.authoritative_sequence
        }),
        "projection was not compatible and caught up before deletion",
    );
    let _ = call(cell, "projection.delete", failures, control.delete()).await;
    let rebuilt = call(cell, "projection.rebuild", failures, control.rebuild()).await;
    check(
        cell,
        "projection.rebuild",
        failures,
        rebuilt
            .as_ref()
            .is_some_and(|value| value.projection_sequence > 0),
        "rebuild did not replay projection state",
    );
    let reverified = call(
        cell,
        "projection.verify[rebuilt]",
        failures,
        control.verify(),
    )
    .await;
    check(
        cell,
        "projection.rebuild",
        failures,
        reverified.as_ref().is_some_and(|value| {
            value.compatible && value.projection_sequence == value.authoritative_sequence
        }),
        "rebuilt projection did not converge with its authoritative log",
    );
}
