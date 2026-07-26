use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};

use fireweed::{
    DiscoveryGranularity, EligibilityPolicy, EngineError, EntitySchemaDocument, GateKeyPolicy,
    GroupKey, IndexDeclaration, IndexDef, IndexSpec, IndexType, MetadataValue, NewItem,
    OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker,
    QueueDefinition, QueueId, QueueIndex, QueueKey, RecurrenceMode, RecurrencePolicy, RetryPolicy,
    TenantId, UtcTimestamp,
};
use fireweed_memory::ManualClock;

fn sqlite_path(tag: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "fireweed-facade-{tag}-{}-{nonce}.sqlite",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

fn rich_definition() -> QueueDefinition {
    let tenant_id = TenantId::new("tenant").unwrap();
    let queue_id = QueueId::new("queue").unwrap();
    let mut definition = QueueDefinition {
        tenant_id,
        queue_id,
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
        secondary_indexes: Vec::new(),
        entity_schema: None,
        typed_indexes: Vec::new(),
        emit_change_records: true,
    };
    let mut blockers = BTreeMap::new();
    blockers.insert(
        "blocked".to_string(),
        vec![MetadataValue::String("yes".to_string())],
    );
    definition.priority_model = PriorityModel {
        kind: PriorityModelKind::Text,
        direction: PriorityDirection::Descending,
        tie_breaker: PriorityTieBreaker::ClientItemKey,
    };
    definition.ordering_mode = OrderingMode::BoundedRelaxed;
    definition.max_rank_error = 7;
    definition.progress_bound_ms = 12_345;
    definition.eligibility_policy = EligibilityPolicy {
        metadata_blockers: blockers,
        gate_keys: GateKeyPolicy::Dynamic,
        max_gate_keys_per_item: Some(3),
        max_gates_per_request: Some(5),
    };
    // Group batching and cohort mode are mutually exclusive queue policies. This reopen fixture exercises
    // the group-batching lane; cohort-definition persistence is covered by the dedicated cohort suites.
    definition.cohort_policy = None;
    definition.recurrence = RecurrencePolicy {
        mode: RecurrenceMode::Recurring,
        until: Some(UtcTimestamp::new(4_242, 0).unwrap()),
    };
    definition.request_id_retention_ms = 11_000;
    definition.client_item_key_retention_ms = 12_000;
    definition.terminal_retention_ms = 13_000;
    definition.max_lease_duration_ms = 14_000;
    definition.retry_policy = RetryPolicy { max_attempts: 9 };
    definition.max_push_batch_size = 17;
    definition.max_claim_batch_size = 19;
    definition.max_eligible_group_size = Some(23);
    definition.secondary_indexes = vec![IndexSpec {
        name: "by_customer".to_string(),
        fields: vec!["customer".to_string(), "region".to_string()],
        unique: true,
    }];
    definition.entity_schema = Some(
        serde_json::from_value::<EntitySchemaDocument>(serde_json::json!({
            "entity_schema": {
                "type": "object",
                "required": ["status"],
                "properties": {
                    "status": {"type": "string"},
                    "attempt": {"type": "integer"}
                }
            }
        }))
        .unwrap(),
    );
    definition.typed_indexes = vec![QueueIndex {
        name: "by_status".to_string(),
        declaration: IndexDeclaration::Single(IndexDef {
            field: "status".to_string(),
            index_type: IndexType::String,
            unique: false,
        }),
    }];
    definition.emit_change_records = false;
    definition
}

fn queue_key() -> QueueKey {
    QueueKey::new(
        TenantId::new("tenant").unwrap(),
        QueueId::new("queue").unwrap(),
    )
}

macro_rules! race_create {
    ($handles:expr, $definition:expr) => {{
        let barrier = Arc::new(Barrier::new(2));
        let handles = $handles.each_ref().map(|handle| {
            let handle = Arc::clone(handle);
            let barrier = barrier.clone();
            let definition = $definition.clone();
            std::thread::spawn(move || {
                barrier.wait();
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(handle.create_queue(definition))
            })
        });
        handles.map(|handle| handle.join().unwrap().unwrap())
    }};
}

#[tokio::test]
async fn open_sqlite_atomic_create_rich_reopen_and_capability() {
    let path = sqlite_path("log-replay-create-read");
    let definition = rich_definition();
    let handles = [
        Arc::new(fireweed::open_sqlite(&path, Arc::new(ManualClock::at(10))).unwrap()),
        Arc::new(fireweed::open_sqlite(&path, Arc::new(ManualClock::at(10))).unwrap()),
    ];
    let outcomes = race_create!(handles, definition);
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == definition)
    );
    let loser = &handles[outcomes
        .iter()
        .position(|outcome| !outcome.created)
        .unwrap()];
    loser
        .push(
            &queue_key(),
            NewItem {
                group_key: Some(GroupKey::new("workers").unwrap()),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let scopes = loser
        .discover_active_scopes(&queue_key(), DiscoveryGranularity::Queue)
        .await
        .unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(loser.claim(&queue_key(), 1, 1_000).await.unwrap().len(), 1);
    drop(handles);

    let reopened = fireweed::open_sqlite(&path, Arc::new(ManualClock::at(20))).unwrap();
    let reopened_outcome = reopened.create_queue(definition.clone()).await.unwrap();
    assert!(!reopened_outcome.created);
    assert_eq!(reopened_outcome.definition, definition);
    let mut incompatible = definition.clone();
    incompatible.ordering_mode = OrderingMode::Strict;
    let incompatible_handle = fireweed::open_sqlite(&path, Arc::new(ManualClock::at(20))).unwrap();
    assert!(matches!(
        incompatible_handle.create_queue(incompatible).await,
        Err(EngineError::QueueDefinitionConflict)
    ));
    drop(incompatible_handle);
    drop(reopened);
    let final_reopen = fireweed::open_sqlite(&path, Arc::new(ManualClock::at(30))).unwrap();
    assert_eq!(
        final_reopen
            .create_queue(definition.clone())
            .await
            .unwrap()
            .definition,
        definition
    );
    drop(final_reopen);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn open_sqlite_relational_atomic_create_rich_reopen_and_discovery() {
    let path = sqlite_path("relational-create-read");
    let definition = rich_definition();
    let handles = [
        Arc::new(fireweed::open_sqlite_relational(&path, Arc::new(ManualClock::at(10))).unwrap()),
        Arc::new(fireweed::open_sqlite_relational(&path, Arc::new(ManualClock::at(10))).unwrap()),
    ];
    let outcomes = race_create!(handles, definition);
    assert_eq!(outcomes.iter().filter(|outcome| outcome.created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.definition == definition)
    );
    let loser = &handles[outcomes
        .iter()
        .position(|outcome| !outcome.created)
        .unwrap()];
    loser
        .push(
            &queue_key(),
            NewItem {
                group_key: Some(GroupKey::new("workers").unwrap()),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    let scopes = loser
        .discover_active_scopes(&queue_key(), DiscoveryGranularity::Queue)
        .await
        .unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(loser.claim(&queue_key(), 1, 1_000).await.unwrap().len(), 1);
    drop(handles);

    let reopened = fireweed::open_sqlite_relational(&path, Arc::new(ManualClock::at(20))).unwrap();
    let reopened_outcome = reopened.create_queue(definition.clone()).await.unwrap();
    assert!(!reopened_outcome.created);
    assert_eq!(reopened_outcome.definition, definition);
    let mut incompatible = definition.clone();
    incompatible.ordering_mode = OrderingMode::Strict;
    let incompatible_handle =
        fireweed::open_sqlite_relational(&path, Arc::new(ManualClock::at(20))).unwrap();
    assert!(matches!(
        incompatible_handle.create_queue(incompatible).await,
        Err(EngineError::QueueDefinitionConflict)
    ));
    drop(incompatible_handle);
    drop(reopened);
    let final_reopen =
        fireweed::open_sqlite_relational(&path, Arc::new(ManualClock::at(30))).unwrap();
    assert_eq!(
        final_reopen
            .create_queue(definition.clone())
            .await
            .unwrap()
            .definition,
        definition
    );
    drop(final_reopen);
    std::fs::remove_file(path).unwrap();
}
