#![allow(non_snake_case)]

use std::path::PathBuf;
use std::time::Duration;

use heimq_broker::consumer_group::{GroupCoordinatorBackend as _, JoinRequest};
use heimq_broker::storage::OffsetStore as _;
use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_engine::{
    AuthContext, ChangeRecord, ChangeRecordKind, ChangeRecordPosition, ChangeRecordSink as _,
    ChangeRecordState, EngineError, QueueKey,
};
use pqueue_server::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, EmbeddedFjordConfig,
    FjordChangeRecordSink, LogSpec, ProjectionSpec, authorize_fjord_topic_read,
    build_embedded_fjord_surface, fjord_topic_name, register_embedded_fjord_topics,
    spawn_embedded_fjord_broker,
};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message as _};

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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct EmbeddedBroker {
    handle: tokio::task::JoinHandle<()>,
    bootstrap: String,
}

impl Drop for EmbeddedBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn change_record(
    tenant: &str,
    queue: &str,
    item_id: Option<u64>,
    backend_epoch: u64,
    sequence: u64,
    kind: ChangeRecordKind,
) -> ChangeRecord {
    ChangeRecord {
        tenant_id: TenantId::new(tenant).unwrap(),
        queue_id: QueueId::new(queue).unwrap(),
        item_id: item_id.map(pqueue_core::ItemId::from_u64),
        position: ChangeRecordPosition {
            backend_epoch,
            sequence,
        },
        command_kind: kind,
        new_state: Some(ChangeRecordState::Pending),
        item_version: Some(1),
        terminal_at: None,
        emitted_at: Some(pqueue_core::UtcTimestamp::new(1, 0).unwrap()),
        source_owner_id: None,
        source_epoch: backend_epoch,
    }
}

async fn start_embedded_broker(queue: &QueueDefinition) -> EmbeddedBroker {
    let port = free_port();
    let bootstrap = format!("127.0.0.1:{port}");
    let topic = fjord_topic_name(&queue_key(
        queue.tenant_id.as_str(),
        queue.queue_id.as_str(),
    ))
    .expect("valid fjord topic");
    let handle = spawn_embedded_fjord_broker(
        7,
        &EmbeddedFjordConfig {
            namespace_root: std::env::temp_dir()
                .join(format!("pqueue-fjord-test-{}", std::process::id())),
            cluster_id: "fjord-test-cluster".to_string(),
        },
        &format!("kafka://{bootstrap}"),
        std::slice::from_ref(queue),
    )
    .await
    .expect("spawn embedded fjord broker");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let ready = tokio::task::block_in_place(|| {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap)
                .create()
                .expect("metadata consumer");
            consumer
                .fetch_metadata(Some(&topic), Duration::from_millis(500))
                .map(|metadata| {
                    metadata.topics().iter().any(|topic_meta| {
                        topic_meta.name() == topic && topic_meta.partitions().len() == 1
                    })
                })
                .unwrap_or(false)
        });
        if ready {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "embedded fjord broker did not publish metadata for {topic} within 10s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    EmbeddedBroker { handle, bootstrap }
}

fn make_sink(bootstrap: &str) -> FjordChangeRecordSink {
    let config = ChangeRecordSinkConfig {
        enabled: true,
        endpoint: Some(format!("kafka://{bootstrap}")),
        ..Default::default()
    };
    FjordChangeRecordSink::new(&config).expect("fjord sink")
}

fn consume_records(
    bootstrap: &str,
    topic: &str,
    expected: usize,
) -> Vec<rdkafka::message::OwnedMessage> {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    tokio::task::block_in_place(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", format!("fjord-test-{}", std::process::id()))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        consumer.subscribe(&[&topic]).expect("subscribe");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let mut out = Vec::new();
        while out.len() < expected && std::time::Instant::now() < deadline {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(200)) {
                out.push(message.detach());
            }
        }
        out
    })
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
async fn TestEmbeddedFjordSurfaceUsesDedicatedNamespaceRoot() {
    let base = std::env::temp_dir().join(format!("pqueue-fjord-namespace-{}", std::process::id()));
    let queue_storage_root = base.join("queue-storage");
    let projection_path = base.join("queue-projection.db");
    let mut config = Config::new(
        BackendSpec {
            log: LogSpec::ObjectLog {
                root: queue_storage_root.clone(),
            },
            projection: ProjectionSpec::Hybrid {
                path: projection_path.clone(),
            },
            control_plane: ControlPlaneSpec::InProcess,
        },
        7,
        "127.0.0.1:0".to_string(),
        Duration::from_millis(100),
        vec![queue_definition("t1", "q1")],
    );
    config.embedded_fjord = EmbeddedFjordConfig {
        namespace_root: base.join("fjord-state"),
        cluster_id: "fjord-test-cluster".to_string(),
    };

    let surface = build_embedded_fjord_surface(config.node_id as i32, &config.embedded_fjord);
    let expected_root = config.embedded_fjord.namespace_root.join("node-7");

    assert_eq!(surface.namespace_root(), &expected_root);
    assert!(
        surface
            .namespace_root()
            .starts_with(&config.embedded_fjord.namespace_root)
    );
    assert!(!surface.namespace_root().starts_with(&queue_storage_root));
    assert_ne!(surface.namespace_root(), &queue_storage_root.join("node-7"));
    assert_eq!(surface.cluster_id(), "fjord-test-cluster");
}

#[test]
fn TestKafkaTenantAclRejectsCrossTenantRead() {
    let allowed = queue_key("tenant-a", "queue-a");
    let denied = queue_key("tenant-b", "queue-b");
    let auth = AuthContext::new("fjord-reader", [allowed.tenant_id.as_str()]);
    let denied_topic = fjord_topic_name(&denied).expect("valid fjord topic");

    assert_eq!(
        fjord_topic_name(&allowed).expect("valid fjord topic"),
        "tenant-a.queue-a"
    );

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
    )
    .expect("register embedded fjord topics");

    let mut topics = surface.topic_registry.topic_list();
    topics.sort();
    assert_eq!(
        topics,
        vec![
            ("tenant-a.queue-a".to_string(), 1),
            ("tenant-b.queue-b".to_string(), 1),
        ]
    );
    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, "tenant-a.queue-a"),
        Ok(())
    );
    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, &denied_topic),
        Err(EngineError::Forbidden(
            "principal is not authorized for the requested queue namespace"
        ))
    );
}

#[test]
fn TestKafkaTenantAclRejectsCrossQueueRead() {
    let allowed = queue_key("tenant-a", "queue-a");
    let denied = queue_key("tenant-a", "queue-b");
    let auth = AuthContext::new("fjord-reader", [allowed.tenant_id.as_str()]);
    let denied_topic = fjord_topic_name(&denied).expect("valid fjord topic");

    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, "tenant-a.queue-a"),
        Ok(())
    );
    assert_eq!(
        authorize_fjord_topic_read(&auth, &allowed, &denied_topic),
        Err(EngineError::Forbidden(
            "principal is not authorized for the requested queue namespace"
        ))
    );
}

#[test]
fn TestRejectAmbiguousTenantQueueMappings() {
    let left = queue_key("a.b", "c");
    let right = queue_key("a", "b.c");

    assert!(fjord_topic_name(&left).is_err());
    assert!(fjord_topic_name(&right).is_err());

    let surface = build_embedded_fjord_surface(
        7,
        &EmbeddedFjordConfig {
            namespace_root: PathBuf::from("/var/lib/pqueue/fjord-test"),
            cluster_id: "fjord-test-cluster".to_string(),
        },
    );

    let result = register_embedded_fjord_topics(
        &surface.topic_registry,
        &[
            QueueDefinition {
                tenant_id: left.tenant_id.clone(),
                queue_id: left.queue_id.clone(),
                ..queue_definition("tenant-a", "queue-a")
            },
            QueueDefinition {
                tenant_id: right.tenant_id.clone(),
                queue_id: right.queue_id.clone(),
                ..queue_definition("tenant-a", "queue-a")
            },
        ],
    );

    assert!(result.is_err());
}

#[test]
fn TestRejectKafkaIllegalTenantAndQueueIds() {
    let illegal = queue_key("tenant with space", "queue/1");

    assert!(fjord_topic_name(&illegal).is_err());

    let surface = build_embedded_fjord_surface(
        7,
        &EmbeddedFjordConfig {
            namespace_root: PathBuf::from("/var/lib/pqueue/fjord-test"),
            cluster_id: "fjord-test-cluster".to_string(),
        },
    );

    let result = register_embedded_fjord_topics(
        &surface.topic_registry,
        &[QueueDefinition {
            tenant_id: illegal.tenant_id.clone(),
            queue_id: illegal.queue_id.clone(),
            ..queue_definition("tenant-a", "queue-a")
        }],
    );

    assert!(result.is_err());
}

#[test]
fn TestKafkaSurfaceKeepsConsumerGroupStateTenantScoped() {
    let surface = build_embedded_fjord_surface(
        7,
        &EmbeddedFjordConfig {
            namespace_root: std::env::temp_dir()
                .join(format!("pqueue-fjord-test-{}", std::process::id())),
            cluster_id: "fjord-test-cluster".to_string(),
        },
    );
    let tenant_a = queue_definition("tenant-a", "queue-a");
    let tenant_b = queue_definition("tenant-b", "queue-b");

    register_embedded_fjord_topics(
        &surface.topic_registry,
        &[tenant_a.clone(), tenant_b.clone()],
    )
    .expect("register embedded fjord topics");

    let mut topics = surface.topic_registry.topic_list();
    topics.sort();
    assert_eq!(
        topics,
        vec![
            ("tenant-a.queue-a".to_string(), 1),
            ("tenant-b.queue-b".to_string(), 1),
        ]
    );

    let topic_a = fjord_topic_name(&queue_key("tenant-a", "queue-a")).expect("valid fjord topic");
    let topic_b = fjord_topic_name(&queue_key("tenant-b", "queue-b")).expect("valid fjord topic");

    surface
        .offset_store
        .commit("tenant-a.reader", &topic_a, 0, 11, 0, None)
        .expect("commit tenant-a offset");
    surface
        .offset_store
        .commit("tenant-b.reader", &topic_b, 0, 22, 0, None)
        .expect("commit tenant-b offset");

    assert_eq!(
        surface
            .offset_store
            .fetch("tenant-a.reader", &topic_a, 0)
            .map(|offset| offset.offset),
        Some(11)
    );
    assert_eq!(
        surface
            .offset_store
            .fetch("tenant-a.reader", &topic_b, 0)
            .map(|offset| offset.offset),
        None
    );
    assert_eq!(
        surface
            .offset_store
            .fetch_all_for_group("tenant-a.reader")
            .get(&(topic_a.clone(), 0))
            .map(|offset| offset.offset),
        Some(11)
    );
    assert_eq!(
        surface
            .offset_store
            .fetch_all_for_group("tenant-b.reader")
            .get(&(topic_b.clone(), 0))
            .map(|offset| offset.offset),
        Some(22)
    );

    let join_a = surface.group_coordinator.join_group(JoinRequest {
        group_id: "tenant-a.reader".to_string(),
        member_id: "tenant-a-member".to_string(),
        client_id: "client-a".to_string(),
        client_host: "127.0.0.1".to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
    });
    let join_b = surface.group_coordinator.join_group(JoinRequest {
        group_id: "tenant-b.reader".to_string(),
        member_id: "tenant-b-member".to_string(),
        client_id: "client-b".to_string(),
        client_host: "127.0.0.1".to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
    });

    assert_eq!(join_a.error_code, 0);
    assert_eq!(join_b.error_code, 0);

    let desc_a = surface
        .group_coordinator
        .describe_group("tenant-a.reader")
        .expect("tenant-a group exists");
    let desc_b = surface
        .group_coordinator
        .describe_group("tenant-b.reader")
        .expect("tenant-b group exists");

    assert_eq!(desc_a.group_id, "tenant-a.reader");
    assert_eq!(desc_a.group_state, "Stable");
    assert_eq!(desc_a.members.len(), 1);
    assert_eq!(desc_a.members[0].member_id, "tenant-a-member");
    assert_eq!(desc_a.members[0].member_assignment, Vec::<u8>::new());
    assert_eq!(desc_b.group_id, "tenant-b.reader");
    assert_eq!(desc_b.group_state, "Stable");
    assert_eq!(desc_b.members.len(), 1);
    assert_eq!(desc_b.members[0].member_id, "tenant-b-member");
    assert_eq!(desc_b.members[0].member_assignment, Vec::<u8>::new());

    let mut groups = surface.group_coordinator.list_groups();
    groups.sort();
    assert_eq!(
        groups,
        vec!["tenant-a.reader".to_string(), "tenant-b.reader".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestKafkaChangeLogConsumesInCommandPositionOrder() {
    let queue = queue_definition("tenant-a", "queue-a");
    let broker = start_embedded_broker(&queue).await;
    let sink = make_sink(&broker.bootstrap);
    let shard = queue_key("tenant-a", "queue-a");
    let records = vec![
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push),
        change_record(
            "tenant-a",
            "queue-a",
            Some(2),
            7,
            2,
            ChangeRecordKind::Claim,
        ),
        change_record(
            "tenant-a",
            "queue-a",
            Some(3),
            7,
            3,
            ChangeRecordKind::Finalize,
        ),
    ];

    sink.emit(&shard, &records).expect("emit records");

    let consumed = consume_records(
        &broker.bootstrap,
        &fjord_topic_name(&shard).expect("valid fjord topic"),
        3,
    );
    assert_eq!(consumed.len(), 3);
    for (idx, message) in consumed.iter().enumerate() {
        let payload = message.payload().expect("payload");
        let decoded: ChangeRecord = serde_json::from_slice(payload).expect("decode record");
        assert_eq!(decoded.position.sequence, (idx + 1) as u64);
        let headers = message.headers().expect("headers");
        assert_eq!(headers.get(0).key, "pq-tenant-id");
        assert_eq!(headers.get(1).key, "pq-queue-id");
        assert_eq!(headers.get(2).key, "pq-backend-epoch");
        assert_eq!(headers.get(3).key, "pq-sequence");
        assert_eq!(headers.get(4).key, "pq-command-kind");
        assert_eq!(message.partition(), 0);
        assert_eq!(message.offset(), idx as i64);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestKafkaTopicMapsOneQueueToOnePartition() {
    let queue_a = queue_definition("tenant-a", "queue-a");
    let queue_b = queue_definition("tenant-b", "queue-b");
    let port = free_port();
    let bootstrap = format!("127.0.0.1:{port}");
    let broker = spawn_embedded_fjord_broker(
        7,
        &EmbeddedFjordConfig {
            namespace_root: std::env::temp_dir()
                .join(format!("pqueue-fjord-test-{}", std::process::id())),
            cluster_id: "fjord-test-cluster".to_string(),
        },
        &format!("kafka://{bootstrap}"),
        &[queue_a.clone(), queue_b.clone()],
    )
    .await
    .expect("spawn embedded fjord broker");

    let topic_a = fjord_topic_name(&queue_key(
        queue_a.tenant_id.as_str(),
        queue_a.queue_id.as_str(),
    ))
    .expect("valid fjord topic");
    let topic_b = fjord_topic_name(&queue_key(
        queue_b.tenant_id.as_str(),
        queue_b.queue_id.as_str(),
    ))
    .expect("valid fjord topic");

    tokio::task::block_in_place(|| {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", format!("fjord-test-{}", std::process::id()))
            .create()
            .expect("metadata consumer");
        let metadata = consumer
            .fetch_metadata(None, Duration::from_secs(5))
            .expect("fetch metadata");
        let partition_count = |topic_name: &str| {
            metadata
                .topics()
                .iter()
                .find(|topic| topic.name() == topic_name)
                .map(|topic| topic.partitions().len())
                .expect("topic metadata")
        };

        assert_eq!(partition_count(&topic_a), 1);
        assert_eq!(partition_count(&topic_b), 1);
    });

    drop(broker);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestKafkaOffsetNeverRegressesAcrossFailover() {
    let queue = queue_definition("tenant-a", "queue-a");
    let broker = start_embedded_broker(&queue).await;
    let shard = queue_key("tenant-a", "queue-a");
    let logical_record =
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("initial emit");
    drop(sink);

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("re-emit");

    let consumed = consume_records(
        &broker.bootstrap,
        &fjord_topic_name(&shard).expect("valid fjord topic"),
        2,
    );
    assert_eq!(consumed.len(), 2);
    let first = consumed[0].offset();
    let second = consumed[1].offset();
    assert!(second > first, "broker offsets must move forward");
    assert_eq!(consumed[0].key(), consumed[1].key());
    assert_eq!(consumed[0].partition(), consumed[1].partition());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestKafkaIdempotencyKeyIsStableAcrossReemit() {
    let queue = queue_definition("tenant-a", "queue-a");
    let broker = start_embedded_broker(&queue).await;
    let shard = queue_key("tenant-a", "queue-a");
    let logical_record =
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("initial emit");
    drop(sink);

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("re-emit");

    let consumed = consume_records(
        &broker.bootstrap,
        &fjord_topic_name(&shard).expect("valid fjord topic"),
        2,
    );
    assert_eq!(consumed.len(), 2);
    assert_eq!(consumed[0].key(), consumed[1].key());
}
