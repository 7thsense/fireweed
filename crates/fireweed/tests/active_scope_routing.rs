use std::collections::BTreeSet;
use std::sync::Arc;

use fireweed::{
    ActiveScope, ActiveScopeDiscovery, DiscoveryGranularity, EngineError, GroupKey, NewItem,
    OldestFirstScopePrefix, QueueDefinition, QueueId, QueueKey, RuntimeCore, TenantId,
    UtcTimestamp, select_active_scope_from_prefix,
};
use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, RecurrencePolicy, RetryPolicy,
};
use fireweed_memory::{ManualClock, composed_memory_backend};

fn queue(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(TenantId::new(tenant).unwrap(), QueueId::new(queue).unwrap())
}

fn definition(queue: &QueueKey, progress_bound_ms: u64) -> QueueDefinition {
    QueueDefinition {
        tenant_id: queue.tenant_id.clone(),
        queue_id: queue.queue_id.clone(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms,
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

fn scope(queue_id: &str, group: Option<&str>, age: u64) -> ActiveScope {
    ActiveScope {
        queue_id: queue_id.to_string(),
        group_key: group.map(str::to_string),
        oldest_eligible_age_ms: age,
        eligible_count: Some(1),
        progress_bound_risk_count: Some(0),
    }
}

fn stamped(queue: &QueueKey, scopes: Vec<ActiveScope>) -> ActiveScopeDiscovery {
    ActiveScopeDiscovery {
        queue: queue.clone(),
        granularity: DiscoveryGranularity::Group,
        scopes,
    }
}

#[test]
fn attestation_rejects_invalid_granularity_identity_empty_and_order() {
    let q = queue("tenant", "queue");
    let mut wrong_granularity = stamped(&q, vec![scope("queue", Some("g"), 10)]);
    wrong_granularity.granularity = DiscoveryGranularity::Queue;
    assert!(matches!(
        OldestFirstScopePrefix::attest(wrong_granularity),
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        OldestFirstScopePrefix::attest(stamped(&q, vec![])),
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        OldestFirstScopePrefix::attest(stamped(&q, vec![scope("another-queue", Some("g"), 10)])),
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        OldestFirstScopePrefix::attest(stamped(
            &q,
            vec![
                scope("queue", Some("newer"), 9),
                scope("queue", Some("older"), 10),
            ]
        )),
        Err(EngineError::Invalid(_))
    ));
}

#[test]
fn selector_rejects_mismatched_full_queue_and_invalid_policy() {
    let q = queue("tenant-a", "queue");
    let prefix =
        OldestFirstScopePrefix::attest(stamped(&q, vec![scope("queue", Some("g"), 10)])).unwrap();
    let other_tenant = queue("tenant-b", "queue");
    assert!(matches!(
        select_active_scope_from_prefix(&prefix, &other_tenant, b"worker", 1, 100, 0, 0),
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        select_active_scope_from_prefix(&prefix, &q, b"worker", 0, 100, 0, 0),
        Err(EngineError::Invalid(_))
    ));
    assert!(matches!(
        select_active_scope_from_prefix(&prefix, &q, b"worker", 1, 0, 0, 0),
        Err(EngineError::Invalid(_))
    ));
}

#[test]
fn bounded_selection_is_stable_dispersed_and_preserves_source_order() {
    let q = queue("tenant", "queue");
    let source = vec![
        scope("queue", Some("g0"), 10),
        scope("queue", Some("g1"), 9),
        scope("queue", Some("g2"), 8),
        scope("queue", Some("outside"), 7),
    ];
    let prefix = OldestFirstScopePrefix::attest(stamped(&q, source.clone())).unwrap();
    let mut selected = BTreeSet::new();
    for worker in 0..256 {
        let routing_key = format!("worker-{worker}");
        let first =
            select_active_scope_from_prefix(&prefix, &q, routing_key.as_bytes(), 3, 10_000, 0, 0)
                .unwrap();
        let repeat =
            select_active_scope_from_prefix(&prefix, &q, routing_key.as_bytes(), 3, 10_000, 0, 0)
                .unwrap();
        assert_eq!(first, repeat);
        assert!(first.index < 3, "selection escaped its leading window");
        selected.insert(first.index);
    }
    assert!(
        selected.len() > 1,
        "routing keys should disperse ordinary work"
    );
    assert_eq!(prefix.scopes(), source.as_slice());

    let oversized =
        select_active_scope_from_prefix(&prefix, &q, b"oversized-window", usize::MAX, 10_000, 0, 0)
            .unwrap();
    assert!(oversized.index < source.len());
}

#[test]
fn urgency_uses_saturating_age_skew_and_guard_boundary() {
    let q = queue("tenant", "queue");
    let prefix = OldestFirstScopePrefix::attest(stamped(
        &q,
        vec![
            scope("queue", Some("oldest"), 89),
            scope("queue", Some("other"), 80),
        ],
    ))
    .unwrap();

    let below = select_active_scope_from_prefix(&prefix, &q, b"worker", 2, 100, 5, 5).unwrap();
    assert!(!below.urgency_forced);
    let at_guard = select_active_scope_from_prefix(&prefix, &q, b"worker", 2, 100, 6, 5).unwrap();
    assert_eq!(at_guard.index, 0);
    assert!(at_guard.urgency_forced);

    let saturated = OldestFirstScopePrefix::attest(stamped(
        &q,
        vec![scope("queue", Some("oldest"), u64::MAX - 1)],
    ))
    .unwrap();
    let selected =
        select_active_scope_from_prefix(&saturated, &q, b"worker", 1, u64::MAX, 10, 10).unwrap();
    assert!(selected.urgency_forced);
}

#[test]
fn ungrouped_scope_is_selectable_with_explicit_filter_diagnostic() {
    let q = queue("tenant", "queue");
    let prefix = OldestFirstScopePrefix::attest(stamped(
        &q,
        vec![scope("queue", None, 10), scope("queue", Some("grouped"), 9)],
    ))
    .unwrap();
    let selected = select_active_scope_from_prefix(&prefix, &q, b"worker", 1, 100, 0, 0).unwrap();
    assert_eq!(selected.index, 0);
    assert!(selected.scope.group_key.is_none());
    assert!(!selected.group_filter_available);
}

#[tokio::test]
async fn queue_definition_accessor_reads_memory_and_durable_policy() {
    let q = queue("tenant", "queue");
    let memory = RuntimeCore::new(
        Arc::new(composed_memory_backend()),
        Arc::new(ManualClock::at(0)),
    );
    memory.create_queue(definition(&q, 12_345)).await.unwrap();
    assert_eq!(
        memory.queue_definition(&q).await.unwrap().progress_bound_ms,
        12_345
    );

    let path = std::env::temp_dir().join(format!(
        "fireweed-active-scope-policy-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let durable =
        fireweed::open_sqlite_relational(path.to_str().unwrap(), Arc::new(ManualClock::at(0)))
            .unwrap();
    durable.create_queue(definition(&q, 54_321)).await.unwrap();
    assert_eq!(
        durable
            .queue_definition(&q)
            .await
            .unwrap()
            .progress_bound_ms,
        54_321
    );
    drop(durable);
    std::fs::remove_file(path).unwrap();
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

#[tokio::test]
async fn time_only_crossed_group_triggers_progress_guard_without_reordering() {
    let q = queue("tenant", "queue");
    let clock = Arc::new(ManualClock::at(0));
    let path = std::env::temp_dir().join(format!(
        "fireweed-active-scope-time-crossing-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let durable = fireweed::open_sqlite_relational(path.to_str().unwrap(), clock.clone()).unwrap();
    durable.create_queue(definition(&q, 60_000)).await.unwrap();
    durable
        .push(
            &q,
            NewItem {
                group_key: Some(GroupKey::new("crossed").unwrap()),
                not_before: Some(ts(10)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    clock.set(9);
    assert!(
        durable
            .discover_active_scopes(&q, DiscoveryGranularity::Group)
            .await
            .unwrap()
            .is_empty()
    );
    clock.set(69);
    let discovery = durable
        .discover_active_scopes_stamped(&q, DiscoveryGranularity::Group)
        .await
        .unwrap();
    assert_eq!(discovery.scopes[0].oldest_eligible_age_ms, 59_000);
    let policy = durable.queue_definition(&q).await.unwrap();
    let source = discovery.scopes.clone();
    let prefix = OldestFirstScopePrefix::attest(discovery).unwrap();
    let selected = select_active_scope_from_prefix(
        &prefix,
        &q,
        b"worker",
        8,
        policy.progress_bound_ms,
        0,
        1_000,
    )
    .unwrap();
    assert_eq!(selected.index, 0);
    assert!(selected.urgency_forced);
    assert_eq!(prefix.scopes(), source.as_slice());

    drop(durable);
    std::fs::remove_file(path).unwrap();
}
