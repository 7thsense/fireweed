#![allow(dead_code, unused_imports)]

//! B3 (ADR-009 §4a / L6): the published library is the only blessed surface to the engine.
//!
//! Two guarantees:
//! 1. `open_*` builds a usable `RuntimeCore` WITHOUT the client ever naming a concrete backend or a port.
//! 2. A local source-scan guard catches accidental port re-exports and backend accessors quickly.
//!    `scripts/verify-public-crate-boundary.sh` separately compiles a downstream crate and proves that
//!    ports, internal crates, and the wrapped backend are unreachable through the supported dependency.

use std::sync::Arc;

use fireweed::{
    CohortPolicy, CommitResponseBarrier, ComposedProjectionConfig, ComposedStorageConfig,
    CreateQueue, EligibilityPolicy, ObjectLogAuthorityConfig, ObjectLogConfig, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    ProjectionRecoveryAction, ProjectionRecoveryPolicy, QueueCreationPolicy, QueueDefinition,
    QueueId, QueueKey, RecurrencePolicy, RetryPolicy, SecretValue, SegmentSettings, TenantId,
};
use fireweed_memory::ManualClock;

fn qkey() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn qdef() -> QueueDefinition {
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
        emit_change_records: true,
    }
}

fn sqlite_test_path(test_name: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!(
            "fireweed-{test_name}-{}-{nonce}.sqlite",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned()
}

fn retain_static<T: 'static>(value: T) -> Arc<T> {
    Arc::new(value)
}

fn composed_storage_config(inputs: &mut [String]) -> ComposedStorageConfig {
    ComposedStorageConfig {
        object_log: ObjectLogConfig::S3Compatible {
            endpoint: std::mem::take(&mut inputs[0]),
            bucket: std::mem::take(&mut inputs[1]),
            region: std::mem::take(&mut inputs[2]),
            access_key_id: SecretValue::new(std::mem::take(&mut inputs[3])),
            secret_access_key: SecretValue::new(std::mem::take(&mut inputs[4])),
            allow_insecure_http: true,
        },
        object_log_authority: ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Postgres {
            url: SecretValue::new(std::mem::take(&mut inputs[5])),
        },
        response_barrier: CommitResponseBarrier::Strict,
        segments: SegmentSettings::new(8 * 1024 * 1024, 20).unwrap(),
        namespace: std::mem::take(&mut inputs[6]),
        recovery: ProjectionRecoveryPolicy {
            incompatible_projection: ProjectionRecoveryAction::FailClosed,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

#[test]
fn composed_storage_config_is_owned_and_secret_safe() {
    let mut inputs = vec![
        "http://127.0.0.1:9000".to_owned(),
        "queue-log".to_owned(),
        "us-east-1".to_owned(),
        "visible-access-key".to_owned(),
        "visible-secret-key".to_owned(),
        "postgres://user:password@localhost/projection".to_owned(),
        "integration-test".to_owned(),
    ];
    let config = composed_storage_config(&mut inputs);
    drop(inputs);

    let debug = format!("{config:?}");
    assert!(!debug.contains("visible-access-key"));
    assert!(!debug.contains("visible-secret-key"));
    assert!(!debug.contains("password"));
    assert!(debug.contains("queue-log"));
    config.validate().unwrap();

    let local = ComposedStorageConfig {
        object_log: ObjectLogConfig::Local {
            root: "local-log".into(),
        },
        object_log_authority: ObjectLogAuthorityConfig::NativeConditionalWrite,
        projection: ComposedProjectionConfig::Sqlite {
            path: "projection.sqlite".into(),
        },
        response_barrier: CommitResponseBarrier::AsyncProjection,
        segments: SegmentSettings::new(1024, 5).unwrap(),
        namespace: "local-test".to_owned(),
        recovery: ProjectionRecoveryPolicy {
            incompatible_projection: ProjectionRecoveryAction::RebuildProjection,
            ..Default::default()
        },
    };
    local.validate().unwrap();
}
