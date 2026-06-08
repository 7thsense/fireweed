use pqueue_core::{
    CohortOnIncomplete, CohortPolicy, CreateQueue, GateKeyPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueCreationPolicy, QueueId,
    RecurrenceMode, RecurrencePolicy, RetryPolicy, TenantId,
};

fn valid_create_queue() -> CreateQueue {
    CreateQueue {
        tenant_id: TenantId::new("tenant_acme").unwrap(),
        queue_id: QueueId::new("scheduled_actions").unwrap(),
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: true,
        progress_bound_ms: 10_000,
        eligibility_policy: pqueue_core::EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: Some(25),
        shard_count: Some(4),
    }
}

fn policy() -> QueueCreationPolicy {
    QueueCreationPolicy {
        deployment_max_shard_count: 8,
        default_max_gate_keys_per_item: 12,
        default_max_gates_per_request: 6,
    }
}

#[test]
fn core_domain_tests_rejects_mutually_exclusive_recurrence_and_cohort_fields() {
    let mut request = valid_create_queue();
    request.cohort_policy = CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(9_000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(10),
    };
    request.recurrence = RecurrencePolicy {
        mode: RecurrenceMode::Recurring,
        until: Some(pqueue_core::UtcTimestamp::new(1_700_000_000, 0).unwrap()),
    };

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::InvalidRequest
    );
    assert!(
        error.message.contains("mutually exclusive"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn core_domain_tests_rejects_completion_bound_above_progress_bound() {
    let mut request = valid_create_queue();
    request.cohort_policy = CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(11_000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(10),
    };

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::QueueDefinitionConflict
    );
    assert!(
        error
            .message
            .contains("less than or equal to progress_bound_ms"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn core_domain_tests_rejects_cohort_without_group_co_residency() {
    let mut request = valid_create_queue();
    request.group_co_residency = false;
    request.cohort_policy = CohortPolicy {
        enabled: true,
        completion_bound_ms: Some(9_000),
        on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
        max_cohort_size: Some(10),
    };

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::QueueDefinitionConflict
    );
    assert!(
        error.message.contains("group_co_residency=true"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn core_domain_tests_applies_shard_count_policy_fields_and_defaults() {
    let mut request = valid_create_queue();
    request.shard_count = None;
    request.eligibility_policy = pqueue_core::EligibilityPolicy {
        metadata_blockers: Default::default(),
        gate_keys: GateKeyPolicy::Dynamic,
        max_gate_keys_per_item: None,
        max_gates_per_request: None,
    };

    let queue = request.validate(&policy()).unwrap();
    assert_eq!(queue.shard_count, 1);
    assert_eq!(queue.eligibility_policy.max_gate_keys_per_item, Some(12));
    assert_eq!(queue.eligibility_policy.max_gates_per_request, Some(6));
}

#[test]
fn core_domain_tests_rejects_shard_count_above_deployment_cap() {
    let mut request = valid_create_queue();
    request.shard_count = Some(9);

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::InvalidRequest
    );
    assert!(
        error.message.contains("deployment_max_shard_count"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn core_domain_tests_rejects_zero_shard_count() {
    let mut request = valid_create_queue();
    request.shard_count = Some(0);

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::InvalidRequest
    );
    assert!(
        error.message.contains("greater than or equal to 1"),
        "unexpected error message: {}",
        error.message
    );
}

#[test]
fn core_domain_tests_rejects_timestamp_priority_with_non_created_sequence_tie_breaker() {
    let mut request = valid_create_queue();
    request.priority_model = PriorityModel {
        kind: PriorityModelKind::Timestamp,
        direction: PriorityDirection::Ascending,
        tie_breaker: PriorityTieBreaker::ClientItemKey,
    };

    let error = request.validate(&policy()).unwrap_err();
    assert_eq!(
        error.kind,
        pqueue_core::CreateQueueErrorKind::InvalidRequest
    );
    assert!(
        error.message.contains("created_sequence"),
        "unexpected error message: {}",
        error.message
    );
}
