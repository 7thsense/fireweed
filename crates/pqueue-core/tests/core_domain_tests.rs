use pqueue_core::{
    ApiError, ApiErrorCode, ClientItemKey, CohortOnIncomplete, CohortPolicy, CreateQueue,
    CreateQueueErrorKind, GateKeyPolicy, ItemId, Metadata, MetadataValue, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, QueueCreationPolicy,
    QueueId, RecurrenceMode, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};

type CreateQueueMutation = fn(&mut CreateQueue);
type InvalidRequestCase = (&'static str, CreateQueueErrorKind, CreateQueueMutation);

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
    }
}

fn policy() -> QueueCreationPolicy {
    QueueCreationPolicy {
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
fn core_domain_tests_applies_policy_defaults() {
    let mut request = valid_create_queue();
    request.eligibility_policy = pqueue_core::EligibilityPolicy {
        metadata_blockers: Default::default(),
        gate_keys: GateKeyPolicy::Dynamic,
        max_gate_keys_per_item: None,
        max_gates_per_request: None,
    };

    let queue = request.validate(&policy()).unwrap();
    assert_eq!(queue.eligibility_policy.max_gate_keys_per_item, Some(12));
    assert_eq!(queue.eligibility_policy.max_gates_per_request, Some(6));
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

#[test]
fn core_domain_tests_identifier_timestamp_metadata_and_error_helpers() {
    let tenant = TenantId::new("tenant_acme").unwrap();
    assert_eq!(tenant.as_str(), "tenant_acme");
    assert_eq!(tenant.to_string(), "tenant_acme");
    assert_eq!(tenant.clone().into_inner(), "tenant_acme");
    let tenant_string: String = tenant.into();
    assert_eq!(tenant_string, "tenant_acme");

    let empty = QueueId::new("   ").unwrap_err();
    assert!(empty.to_string().contains("QueueId must not be empty"));

    let timestamp = UtcTimestamp::new(42, 123).unwrap();
    assert_eq!(timestamp.seconds, 42);
    let invalid_timestamp = UtcTimestamp::new(42, 1_000_000_000).unwrap_err();
    assert!(
        invalid_timestamp
            .to_string()
            .contains("nanoseconds must be less")
    );

    let mut metadata = Metadata::new();
    assert!(metadata.is_empty());
    assert_eq!(metadata.insert("flag", MetadataValue::Bool(true)), None);
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata.get("flag"), Some(&MetadataValue::Bool(true)));
    let entries = metadata.into_inner();
    assert!(matches!(
        entries.get("flag"),
        Some(MetadataValue::Bool(true))
    ));

    let error = ApiError::new(ApiErrorCode::QueueNotFound, "missing queue");
    assert_eq!(error.code, ApiErrorCode::QueueNotFound);
    assert!(error.to_string().contains("QueueNotFound"));
}

#[test]
fn core_domain_tests_create_queue_success_response_preserves_fields() {
    let queue = valid_create_queue().validate(&policy()).unwrap();
    assert_eq!(queue.tenant_id.as_str(), "tenant_acme");
    assert_eq!(queue.queue_id.as_str(), "scheduled_actions");
    assert_eq!(queue.cohort_policy, None);

    let response = queue.create_response(true);
    assert!(response.created);
    assert_eq!(response.queue.max_eligible_group_size, Some(25));
}

#[test]
fn core_domain_tests_rejects_scalar_zero_limits() {
    let cases: Vec<(&str, CreateQueueMutation)> = vec![
        ("progress_bound_ms", |request| request.progress_bound_ms = 0),
        ("request_id_retention_ms", |request| {
            request.request_id_retention_ms = 0
        }),
        ("client_item_key_retention_ms", |request| {
            request.client_item_key_retention_ms = 0
        }),
        ("max_lease_duration_ms", |request| {
            request.max_lease_duration_ms = 0
        }),
        ("retry_policy.max_attempts", |request| {
            request.retry_policy.max_attempts = 0
        }),
        ("max_push_batch_size", |request| {
            request.max_push_batch_size = 0
        }),
        ("max_claim_batch_size", |request| {
            request.max_claim_batch_size = 0
        }),
    ];

    for (message, mutate) in cases {
        let mut request = valid_create_queue();
        mutate(&mut request);
        let error = request.validate(&policy()).unwrap_err();
        assert_eq!(error.kind, CreateQueueErrorKind::InvalidRequest);
        assert!(
            error.message.contains(message),
            "expected {message}, got {}",
            error.message
        );
    }
}

#[test]
fn core_domain_tests_rejects_all_invalid_cohort_shapes() {
    let cases: Vec<InvalidRequestCase> = vec![
        (
            "completion_bound_ms is required",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: None,
                    on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
                    max_cohort_size: Some(10),
                };
            },
        ),
        (
            "completion_bound_ms must be greater than 0",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: Some(0),
                    on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
                    max_cohort_size: Some(10),
                };
            },
        ),
        (
            "on_incomplete is required",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: Some(1_000),
                    on_incomplete: None,
                    max_cohort_size: Some(10),
                };
            },
        ),
        (
            "max_cohort_size is required",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: Some(1_000),
                    on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
                    max_cohort_size: None,
                };
            },
        ),
        (
            "max_cohort_size must be greater than 0",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: Some(1_000),
                    on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
                    max_cohort_size: Some(0),
                };
            },
        ),
        (
            "max_cohort_size must be less than or equal to max_claim_batch_size",
            CreateQueueErrorKind::QueueDefinitionConflict,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: true,
                    completion_bound_ms: Some(1_000),
                    on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
                    max_cohort_size: Some(51),
                };
            },
        ),
        (
            "fields other than enabled must be omitted",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.cohort_policy = CohortPolicy {
                    enabled: false,
                    completion_bound_ms: Some(1_000),
                    on_incomplete: None,
                    max_cohort_size: None,
                };
            },
        ),
    ];

    for (message, kind, mutate) in cases {
        let mut request = valid_create_queue();
        mutate(&mut request);
        let error = request.validate(&policy()).unwrap_err();
        assert_eq!(error.kind, kind);
        assert!(
            error.message.contains(message),
            "expected {message}, got {}",
            error.message
        );
    }
}

#[test]
fn core_domain_tests_rejects_invalid_recurrence_group_and_gate_shapes() {
    let cases: Vec<InvalidRequestCase> = vec![
        (
            "recurrence.until is valid only",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.recurrence = RecurrencePolicy {
                    mode: RecurrenceMode::Oneshot,
                    until: Some(UtcTimestamp::new(10, 0).unwrap()),
                };
            },
        ),
        (
            "recurrence.until is required",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.recurrence = RecurrencePolicy {
                    mode: RecurrenceMode::Recurring,
                    until: None,
                };
            },
        ),
        (
            "max_eligible_group_size must be greater than 0",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.max_eligible_group_size = Some(0);
            },
        ),
        (
            "max_eligible_group_size requires group_co_residency=true",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.group_co_residency = false;
                request.max_eligible_group_size = Some(1);
            },
        ),
        (
            "max_eligible_group_size must be less than or equal to max_claim_batch_size",
            CreateQueueErrorKind::QueueDefinitionConflict,
            |request| {
                request.max_eligible_group_size = Some(51);
            },
        ),
        (
            "gate-key caps must be omitted",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.eligibility_policy.max_gate_keys_per_item = Some(1);
            },
        ),
        (
            "max_gate_keys_per_item must be greater than 0",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
                request.eligibility_policy.max_gate_keys_per_item = Some(0);
            },
        ),
        (
            "max_gates_per_request must be greater than 0",
            CreateQueueErrorKind::InvalidRequest,
            |request| {
                request.eligibility_policy.gate_keys = GateKeyPolicy::Dynamic;
                request.eligibility_policy.max_gates_per_request = Some(0);
            },
        ),
    ];

    for (message, kind, mutate) in cases {
        let mut request = valid_create_queue();
        mutate(&mut request);
        let error = request.validate(&policy()).unwrap_err();
        assert_eq!(error.kind, kind);
        assert!(
            error.message.contains(message),
            "expected {message}, got {}",
            error.message
        );
    }
}

#[test]
fn core_domain_tests_exercises_result_identifier_variants() {
    let client_key = ClientItemKey::new("client-key").unwrap();
    let item_id = ItemId::new("item-1").unwrap();
    assert_eq!(client_key.to_string(), "client-key");
    assert_eq!(item_id.to_string(), "item-1");
}
