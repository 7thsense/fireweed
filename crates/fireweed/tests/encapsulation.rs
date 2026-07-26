//! B3 (ADR-009 §4a / L6): the published library is the only blessed surface to the engine.
//!
//! Two guarantees:
//! 1. `open_*` builds a usable `RuntimeCore` WITHOUT the client ever naming a concrete backend or a port.
//! 2. A local source-scan guard catches accidental port re-exports and backend accessors quickly.
//!    `scripts/verify-public-crate-boundary.sh` separately compiles a downstream crate and proves that
//!    ports, internal crates, and the wrapped backend are unreachable through the supported dependency.

use std::sync::Arc;

use fireweed::{
    CohortPolicy, CommitResponseBarrier, ComposedStorageConfig, CreateQueue, EligibilityPolicy,
    ObjectLogAuthorityConfig, ObjectLogConfig, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionRecoveryAction,
    ProjectionRecoveryPolicy, ProjectionStoreConfig, QueueCreationPolicy, QueueDefinition, QueueId,
    QueueKey, RecurrencePolicy, RetryPolicy, SecretValue, SegmentSettings, TenantId,
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
        projection: ProjectionStoreConfig::Postgres {
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
        projection: ProjectionStoreConfig::Sqlite {
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

#[test]
fn facade_exports_queue_definition_construction_surface() {
    let key = qkey();
    assert_eq!(key.tenant_id.as_str(), "t1");
    assert_eq!(key.queue_id.as_str(), "q1");

    let definition = qdef();
    assert_eq!(definition.tenant_id.as_str(), "t1");
    assert_eq!(definition.queue_id.as_str(), "q1");

    let create = CreateQueue {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q2").unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
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
    };

    let validated = create.validate(&QueueCreationPolicy::default()).unwrap();
    assert_eq!(validated.queue_id.as_str(), "q2");
}

/// The blessed construction path: `fireweed::open_memory` yields a usable handle with no concrete backend
/// type named by the caller (the returned type is `RuntimeCore<impl LibBackend>`).
#[tokio::test]
async fn open_memory_builds_a_usable_fireweed() {
    let fireweed = fireweed::open_memory(Arc::new(ManualClock::at(0)));
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed
        .push(
            &qkey(),
            fireweed::NewItem {
                priority: Some(PriorityValue::Int64(5)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = fireweed.claim(&qkey(), 10, 1_000).await.unwrap();
    assert_eq!(claimed.len(), 1, "open_memory handle claims normally");
}

/// The blessed sqlite path builds and round-trips too.
#[tokio::test]
async fn open_sqlite_builds_a_usable_fireweed() {
    let fireweed = fireweed::open_sqlite(":memory:", Arc::new(ManualClock::at(0))).unwrap();
    fireweed.create_queue(qdef()).await.unwrap();
    fireweed
        .push(&qkey(), fireweed::NewItem::default())
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&qkey()).await.unwrap().pending, 1);
}

/// Rust 2024 return-position `impl Trait` must not capture the borrowed path: the backend owns all
/// state needed by the returned handle.
#[tokio::test]
async fn open_sqlite_retained_handle_owns_path() {
    let path = sqlite_test_path("retained-handle-owns-path");
    let cleanup_path = std::path::PathBuf::from(&path);
    let fireweed =
        retain_static(fireweed::open_sqlite(path.as_str(), Arc::new(ManualClock::at(0))).unwrap());
    drop(path);

    fireweed.create_queue(qdef()).await.unwrap();
    fireweed
        .push(&qkey(), fireweed::NewItem::default())
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&qkey()).await.unwrap().pending, 1);
    drop(fireweed);
    std::fs::remove_file(cleanup_path).unwrap();
}

/// Dropping every handle closes SQLite cleanly; the same caller-owned path can then reopen the durable
/// queue without leaking the path to manufacture a `'static` borrow.
#[tokio::test]
async fn open_sqlite_owned_path_reopens_after_all_handles_drop() {
    let path = sqlite_test_path("owned-path-reopens");
    {
        let fireweed = retain_static(
            fireweed::open_sqlite(path.as_str(), Arc::new(ManualClock::at(0))).unwrap(),
        );
        fireweed.create_queue(qdef()).await.unwrap();
        fireweed
            .push(&qkey(), fireweed::NewItem::default())
            .await
            .unwrap();
        let second_handle = Arc::clone(&fireweed);
        drop(fireweed);
        assert_eq!(second_handle.metrics(&qkey()).await.unwrap().pending, 1);
    }

    let reopened =
        retain_static(fireweed::open_sqlite(path.as_str(), Arc::new(ManualClock::at(1))).unwrap());
    assert_eq!(reopened.metrics(&qkey()).await.unwrap().pending, 1);
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

/// FAST LOCAL GUARD (ADR-009 L6): `fireweed` must not re-export a port trait on its public surface, or expose
/// a backend accessor — either would let a client reach a raw port and bypass coordination/fencing. This
/// scans the library source so an accidental `pub use ...Port` regression fails the crate test. The
/// downstream compile-fail gate is authoritative for reachability from a consumer crate.
#[test]
fn public_surface_exposes_no_port_or_backend() {
    let src = include_str!("../src/lib.rs");

    // No `pub use` may re-export a port trait (a name ending in `Port`). Private `use ... Port` (how the
    // facade consumes the ports internally) is fine — only the PUBLIC re-export surface is constrained.
    let mut in_pub_use = false;
    for line in src.lines() {
        let t = line.trim_start();
        if t.starts_with("pub use") {
            in_pub_use = true;
        }
        if in_pub_use {
            for tok in line.split(|c: char| !c.is_alphanumeric() && c != '_') {
                assert!(
                    !tok.ends_with("Port"),
                    "fireweed must not re-export the port trait `{tok}` on its public surface — clients must \
                     use `RuntimeCore`, never the raw ports (ADR-009 L6 / B3)"
                );
            }
            if line.contains(';') {
                in_pub_use = false;
            }
        }
    }

    // No public backend accessor — `RuntimeCore` must never hand back the port-bearing backend it wraps.
    assert!(
        !src.contains("pub fn backend"),
        "RuntimeCore must not expose a backend accessor (it would leak a port-bearing handle)"
    );

    // The backend-injection constructor `RuntimeCore::new(Arc<B>, …)` must stay `#[doc(hidden)]` so the
    // documented construction surface is only the `open_*` builders (which never expose a backend type).
    let lines: Vec<&str> = src.lines().collect();
    let new_is_hidden = lines
        .windows(2)
        .any(|w| w[0].trim() == "#[doc(hidden)]" && w[1].trim_start().starts_with("pub fn new("));
    assert!(
        new_is_hidden,
        "RuntimeCore::new must be #[doc(hidden)] — open_* is the only documented construction path (ADR-009 L6)"
    );
}
