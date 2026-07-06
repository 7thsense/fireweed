#![allow(non_snake_case)]

use std::path::PathBuf;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::{AuthContext, EngineError, QueueKey};
use pqueue_server::{
    BackendSpec, Config, ControlPlaneSpec, EmbeddedFjordConfig, LogSpec, ProjectionSpec,
    authorize_fjord_topic_read, build_embedded_fjord_surface, fjord_topic_name,
    register_embedded_fjord_topics, start,
};

fn queue_definition(tenant: &str, queue: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new(tenant).expect("valid tenant"),
        queue_id: QueueId::new(queue).expect("valid queue"),
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

fn queue_key(tenant: &str, queue: &str) -> QueueKey {
    QueueKey::new(
        TenantId::new(tenant).expect("valid tenant"),
        QueueId::new(queue).expect("valid queue"),
    )
}

#[test]
fn TestFjordDependencyIsGitPinnedNoPathDeps() {
    let cargo_toml =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .expect("read pqueue-server Cargo.toml");

    assert!(
        cargo_toml.contains(r#"fjord = { git = "https://github.com/telepathdata/fjord.git""#),
        "fjord must be sourced from the git repository"
    );
    assert!(
        cargo_toml.contains(r#"package = "fjord-broker""#),
        "pqueue-server must depend on the fjord broker package"
    );
    assert!(
        !cargo_toml.contains(r#"path = "../fjord""#),
        "fjord must not be a path dependency"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestFjordBootstrapConfigWiresEmbeddedSurface() {
    let mut config = Config::new(
        BackendSpec {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        },
        7,
        "127.0.0.1:0".to_string(),
        Duration::from_millis(100),
        vec![queue_definition("t1", "q1")],
    );
    config.embedded_fjord = EmbeddedFjordConfig {
        namespace_root: PathBuf::from("/var/lib/pqueue/fjord-test"),
        cluster_id: "fjord-test-cluster".to_string(),
    };

    let surface = build_embedded_fjord_surface(config.node_id as i32, &config.embedded_fjord);

    assert_eq!(
        surface.namespace_root(),
        &PathBuf::from("/var/lib/pqueue/fjord-test/node-7")
    );
    assert_eq!(surface.cluster_id(), "fjord-test-cluster");

    let server = start(config)
        .await
        .expect("start pqueue-server with embedded fjord");
    assert!(
        server.is_running(),
        "server must boot with embedded fjord config"
    );
    server.shutdown();

    surface.topic_registry.register_topic("t1:q1", 1);
    assert_eq!(
        surface.topic_registry.topic_list(),
        vec![("t1:q1".to_string(), 1)]
    );
}

#[test]
fn TestKafkaTenantAclRejectsCrossTenantRead() {
    let allowed = queue_key("tenant-a", "queue-a");
    let denied = queue_key("tenant-b", "queue-b");
    let auth = AuthContext::new("fjord-reader", [allowed.tenant_id.as_str()]);
    let denied_topic = fjord_topic_name(&denied);

    assert_eq!(fjord_topic_name(&allowed), "tenant-a:queue-a");

    let surface = build_embedded_fjord_surface(
        7,
        &EmbeddedFjordConfig {
            namespace_root: PathBuf::from("/var/lib/pqueue/fjord-test"),
            cluster_id: "fjord-test-cluster".to_string(),
        },
    );
    register_embedded_fjord_topics(
        &surface.topic_registry,
        &[
            queue_definition("tenant-a", "queue-a"),
            queue_definition("tenant-b", "queue-b"),
        ],
    );

    let mut topics = surface.topic_registry.topic_list();
    topics.sort();
    assert_eq!(
        topics,
        vec![
            ("tenant-a:queue-a".to_string(), 1),
            ("tenant-b:queue-b".to_string(), 1),
        ]
    );
    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, "tenant-a:queue-a"),
        Ok(())
    );
    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, &denied_topic),
        Err(EngineError::Forbidden(
            "principal is not authorized for the requested queue namespace"
        ))
    );
}
