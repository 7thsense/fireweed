#![allow(non_snake_case)]

use std::path::PathBuf;
use std::time::Duration;

use pqueue_engine::{
    AuthContext, ChangeRecord, ChangeRecordKind, ChangeRecordPosition, ChangeRecordSink as _,
    ChangeRecordState, EngineError, QueueKey,
};
use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, EmbeddedFjordConfig,
    FjordChangeRecordSink, LogSpec, ProjectionSpec, authorize_fjord_topic_read,
    build_embedded_fjord_surface, fjord_topic_name, register_embedded_fjord_topics,
    spawn_embedded_fjord_broker, start,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message as _};
use rdkafka::ClientConfig;

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
    ));
    let handle = spawn_embedded_fjord_broker(
        7,
        &EmbeddedFjordConfig {
            namespace_root: PathBuf::from(std::env::temp_dir().join(format!(
                "pqueue-fjord-test-{}",
                std::process::id()
            ))),
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
                    metadata
                        .topics()
                        .iter()
                        .any(|topic_meta| topic_meta.name() == topic && topic_meta.partitions().len() == 1)
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
    let mut config = ChangeRecordSinkConfig::default();
    config.enabled = true;
    config.endpoint = Some(format!("kafka://{bootstrap}"));
    FjordChangeRecordSink::new(&config).expect("fjord sink")
}

fn consume_records(bootstrap: &str, topic: &str, expected: usize) -> Vec<rdkafka::message::OwnedMessage> {
    let bootstrap = bootstrap.to_string();
    let topic = topic.to_string();
    tokio::task::block_in_place(move || {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .set("group.id", &format!("fjord-test-{}", std::process::id()))
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

    surface.topic_registry.register_topic("t1.q1", 1);
    assert_eq!(
        surface.topic_registry.topic_list(),
        vec![("t1.q1".to_string(), 1)]
    );
}

#[test]
fn TestKafkaTenantAclRejectsCrossTenantRead() {
    let allowed = queue_key("tenant-a", "queue-a");
    let denied = queue_key("tenant-b", "queue-b");
    let auth = AuthContext::new("fjord-reader", [allowed.tenant_id.as_str()]);
    let denied_topic = fjord_topic_name(&denied);

    assert_eq!(fjord_topic_name(&allowed), "tenant-a.queue-a");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn TestKafkaChangeLogConsumesInCommandPositionOrder() {
    let queue = queue_definition("tenant-a", "queue-a");
    let broker = start_embedded_broker(&queue).await;
    let sink = make_sink(&broker.bootstrap);
    let shard = queue_key("tenant-a", "queue-a");
    let records = vec![
        change_record(
            "tenant-a",
            "queue-a",
            Some(1),
            7,
            1,
            ChangeRecordKind::Push,
        ),
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

    let consumed = consume_records(&broker.bootstrap, &fjord_topic_name(&shard), 3);
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
async fn TestKafkaOffsetNeverRegressesAcrossFailover() {
    let queue = queue_definition("tenant-a", "queue-a");
    let broker = start_embedded_broker(&queue).await;
    let shard = queue_key("tenant-a", "queue-a");
    let logical_record = change_record(
        "tenant-a",
        "queue-a",
        Some(1),
        7,
        1,
        ChangeRecordKind::Push,
    );

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("initial emit");
    drop(sink);

    let sink = make_sink(&broker.bootstrap);
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("re-emit");

    let consumed = consume_records(&broker.bootstrap, &fjord_topic_name(&shard), 2);
    assert_eq!(consumed.len(), 2);
    let first = consumed[0].offset();
    let second = consumed[1].offset();
    assert!(second > first, "broker offsets must move forward");
    assert_eq!(consumed[0].key(), consumed[1].key());
    assert_eq!(consumed[0].partition(), consumed[1].partition());
}
