#![allow(non_snake_case)]

use std::path::PathBuf;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{
    BackendSpec, Config, ControlPlaneSpec, EmbeddedFjordConfig, LogSpec, ProjectionSpec,
    build_embedded_fjord_surface,
};

fn queue_definition() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").expect("valid tenant"),
        queue_id: QueueId::new("q1").expect("valid queue"),
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

#[test]
fn TestFjordDependencyIsGitPinnedAndNoPathDeps() {
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

#[test]
fn TestPqueueServerBootsEmbeddedFjordFromConfig() {
    let mut config = Config::new(
        BackendSpec {
            log: LogSpec::Memory,
            projection: ProjectionSpec::InMemory,
            control_plane: ControlPlaneSpec::InProcess,
        },
        7,
        "127.0.0.1:0".to_string(),
        std::time::Duration::from_millis(100),
        vec![queue_definition()],
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

    surface.topic_registry.register_topic("t1:q1", 1);
    assert_eq!(
        surface.topic_registry.topic_list(),
        vec![("t1:q1".to_string(), 1)]
    );
}
