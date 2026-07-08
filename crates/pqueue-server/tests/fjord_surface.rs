
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
    EmbeddedChangeRecord, EmbeddedFjordConfig, EmbeddedFjordSurface, FjordChangeRecordSink,
    authorize_fjord_topic_read, build_embedded_fjord_surface, fjord_topic_name,
    read_embedded_change_records, register_embedded_fjord_topics, spawn_embedded_fjord_broker,
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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn test_fjord_config() -> EmbeddedFjordConfig {
    EmbeddedFjordConfig {
        namespace_root: std::env::temp_dir().join(format!(
            "pqueue-fjord-test-{}-{}",
            std::process::id(),
            free_port()
        )),
        cluster_id: "fjord-test-cluster".to_string(),
        broker_listen: None,
    }
}

/// Build a standalone embedded surface plus an in-process sink over the SAME shared log. The sink's
/// appends land in `surface.log`, exactly as they would inside `start()`, so a fetch through the same log
/// observes the records an external Kafka consumer would receive.
fn surface_with_sink(queue: &QueueDefinition) -> (EmbeddedFjordSurface, FjordChangeRecordSink) {
    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
    register_embedded_fjord_topics(&surface.topic_registry, std::slice::from_ref(queue))
        .expect("register embedded fjord topics");
    let sink = FjordChangeRecordSink::new(surface.log_backend());
    (surface, sink)
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

fn header_value<'a>(record: &'a EmbeddedChangeRecord, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, value)| value.as_deref())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_does_not_depend_on_fixed_sleep() {
    // The embedded broker's external-consumer TCP surface still boots deterministically over the SHARED
    // surface log — no fixed sleep, and readiness is an in-process property (topics created in the shared
    // log/registry before serving), verified without any Kafka client.
    let queue = queue_definition("tenant-a", "queue-a");
    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
    register_embedded_fjord_topics(&surface.topic_registry, std::slice::from_ref(&queue))
        .expect("register topics");
    let port = free_port();
    let broker = spawn_embedded_fjord_broker(
        &surface,
        &format!("kafka://127.0.0.1:{port}"),
        std::slice::from_ref(&queue),
    )
    .await
    .expect("spawn embedded fjord broker");

    let topic = fjord_topic_name(&queue_key("tenant-a", "queue-a")).expect("valid fjord topic");
    let topics = surface.topic_registry.topic_list();
    assert!(
        topics
            .iter()
            .any(|(name, partitions)| name == &topic && *partitions == 1),
        "shared surface must publish the change-log topic with one partition: {topics:?}"
    );

    broker.abort();
}

#[test]
fn fjord_dependency_is_git_pinned_no_path_deps() {
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
fn no_c_kafka_client_dependency() {
    // The C Kafka client is fully removed: no C build, no loopback socket on the write path. The pure-Rust
    // kafka-protocol codec encodes the in-process change-record batches instead.
    let cargo_toml =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .expect("read pqueue-server Cargo.toml");
    let banned = ["rd", "kafka"].concat(); // avoid the literal token in this source file
    assert!(
        !cargo_toml.contains(&banned),
        "pqueue-server must not depend on the C Kafka client"
    );
    assert!(
        cargo_toml.contains("kafka-protocol"),
        "the pure-Rust kafka-protocol codec encodes the in-process change-record batches"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_fjord_surface_uses_dedicated_namespace_root() {
    let base = std::env::temp_dir().join(format!("pqueue-fjord-namespace-{}", std::process::id()));
    let config = EmbeddedFjordConfig {
        namespace_root: base.join("fjord-state"),
        cluster_id: "fjord-test-cluster".to_string(),
        broker_listen: None,
    };
    let queue_storage_root = base.join("queue-storage");

    let surface = build_embedded_fjord_surface(7, &config);
    let expected_root = config.namespace_root.join("node-7");

    assert_eq!(surface.namespace_root(), &expected_root);
    assert!(surface.namespace_root().starts_with(&config.namespace_root));
    assert!(!surface.namespace_root().starts_with(&queue_storage_root));
    assert_ne!(surface.namespace_root(), &queue_storage_root.join("node-7"));
    assert_eq!(surface.cluster_id(), "fjord-test-cluster");
}

#[test]
fn kafka_tenant_acl_rejects_cross_tenant_read() {
    let allowed = queue_key("tenant-a", "queue-a");
    let denied = queue_key("tenant-b", "queue-b");
    let auth = AuthContext::new("fjord-reader", [allowed.tenant_id.as_str()]);
    let denied_topic = fjord_topic_name(&denied).expect("valid fjord topic");

    assert_eq!(
        fjord_topic_name(&allowed).expect("valid fjord topic"),
        "tenant-a.queue-a"
    );

    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
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
fn kafka_tenant_acl_rejects_cross_queue_read() {
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
fn reject_ambiguous_tenant_queue_mappings() {
    let left = queue_key("a.b", "c");
    let right = queue_key("a", "b.c");

    assert!(fjord_topic_name(&left).is_err());
    assert!(fjord_topic_name(&right).is_err());

    let surface = build_embedded_fjord_surface(7, &test_fjord_config());

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
fn reject_kafka_illegal_tenant_and_queue_ids() {
    let illegal = queue_key("tenant with space", "queue/1");

    assert!(fjord_topic_name(&illegal).is_err());

    let surface = build_embedded_fjord_surface(7, &test_fjord_config());

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
fn kafka_surface_keeps_consumer_group_state_tenant_scoped() {
    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
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

/// The ADR-014 "Normative consumer contract", verified WITHOUT any Kafka client: change records are appended
/// in-process to the embedded broker's shared Rust log and read back through the same log's fetch path (the
/// exact Kafka v2 record batches an external consumer would receive). Asserts partition 0, monotonic
/// offsets, stable idempotency keys, the JSON payload, and the pinned `pq-*` headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjord_surface_contract_verified_without_rdkafka() {
    let queue = queue_definition("tenant-a", "queue-a");
    let (surface, sink) = surface_with_sink(&queue);
    let shard = queue_key("tenant-a", "queue-a");
    let topic = fjord_topic_name(&shard).expect("valid fjord topic");
    let records = vec![
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push),
        change_record("tenant-a", "queue-a", Some(2), 7, 2, ChangeRecordKind::Claim),
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

    let consumed = read_embedded_change_records(&surface, &topic).expect("read change records");
    assert_eq!(consumed.len(), 3);
    let mut last_offset: Option<i64> = None;
    for (idx, message) in consumed.iter().enumerate() {
        // Payload: the TD-008 ChangeRecord JSON.
        let payload = message.value.as_deref().expect("payload");
        let decoded: ChangeRecord = serde_json::from_slice(payload).expect("decode record");
        assert_eq!(decoded.position.sequence, (idx + 1) as u64);
        assert_eq!(decoded, records[idx]);

        // Headers: the ADR-014:116 pinned wire order (item-scoped records carry pq-item-id third).
        let header_keys: Vec<&str> = message.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            header_keys,
            vec![
                "pq-tenant-id",
                "pq-queue-id",
                "pq-item-id",
                "pq-backend-epoch",
                "pq-sequence",
                "pq-command-kind",
            ]
        );
        assert_eq!(header_value(message, "pq-tenant-id"), Some(b"tenant-a".as_ref()));
        assert_eq!(header_value(message, "pq-queue-id"), Some(b"queue-a".as_ref()));
        assert_eq!(
            header_value(message, "pq-item-id"),
            Some(format!("{}", idx + 1).as_bytes())
        );
        assert_eq!(header_value(message, "pq-backend-epoch"), Some(b"7".as_ref()));
        assert_eq!(
            header_value(message, "pq-sequence"),
            Some(format!("{}", idx + 1).as_bytes())
        );

        // Idempotency key: "{item_id}:{backend_epoch}:{sequence}".
        assert_eq!(
            message.key.as_deref(),
            Some(format!("{}:7:{}", idx + 1, idx + 1).as_bytes())
        );

        // Single partition 0, monotonically increasing offsets.
        assert_eq!(message.partition, 0);
        assert_eq!(message.offset, idx as i64);
        if let Some(previous) = last_offset {
            assert!(message.offset > previous, "offsets must be monotonic");
        }
        last_offset = Some(message.offset);
    }
}

/// End-to-end verification of the REAL consumer surface: append a change record in-process to the shared
/// embedded log, then consume it back over the embedded `HeimqServer`'s TCP Kafka fetch surface using the
/// pure-Rust `rskafka` CONSUMER. This proves the broker actually serves the in-process appends over Kafka
/// (not just that the bytes are stored), and asserts the same ADR-014 contract: partition 0, offset 0
/// (monotonic), idempotency key `{item_id}:{backend_epoch}:{sequence}`, the pq-* headers with correct
/// values, and the JSON payload. (The owner's "no socket on the write path" rule is upheld: the WRITE is the
/// in-process append above; this TCP socket is a test CONSUMER only. rskafka returns headers as a `BTreeMap`,
/// so header WIRE ORDER is verified by `fjord_surface_contract_verified_without_rdkafka`, not here.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjord_surface_contract_verified_over_rskafka_consumer() {
    let queue = queue_definition("tenant-a", "queue-a");
    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
    register_embedded_fjord_topics(&surface.topic_registry, std::slice::from_ref(&queue))
        .expect("register topics");
    let port = free_port();
    let bootstrap = format!("127.0.0.1:{port}");
    let broker = spawn_embedded_fjord_broker(
        &surface,
        &format!("kafka://{bootstrap}"),
        std::slice::from_ref(&queue),
    )
    .await
    .expect("spawn embedded fjord broker with TCP surface");

    let shard = queue_key("tenant-a", "queue-a");
    let topic = fjord_topic_name(&shard).expect("valid fjord topic");

    // WRITE path: in-process append to the shared log (no socket).
    let sink = FjordChangeRecordSink::new(surface.log_backend());
    let record = change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);
    sink.emit(&shard, std::slice::from_ref(&record))
        .expect("in-process emit");

    // READ path: pure-Rust rskafka consumer over the broker's TCP Kafka surface.
    let client = rskafka::client::ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .expect("build rskafka client");
    let partition_client = client
        .partition_client(
            topic.clone(),
            0,
            rskafka::client::partition::UnknownTopicHandling::Retry,
        )
        .await
        .expect("open rskafka partition client");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let consumed = loop {
        let (records, _hwm) = partition_client
            .fetch_records(0, 1..10_000_000, 500)
            .await
            .expect("rskafka fetch_records");
        if !records.is_empty() {
            break records;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rskafka consumer fetched no records over the broker TCP surface within 20s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(consumed.len(), 1);
    let record_and_offset = &consumed[0];
    // Single partition 0 (we fetched partition 0), monotonic offset starting at 0.
    assert_eq!(record_and_offset.offset, 0);
    let consumed_record = &record_and_offset.record;

    // Idempotency key.
    assert_eq!(
        consumed_record.key.as_deref(),
        Some(b"1:7:1".as_ref()),
        "idempotency key {{item_id}}:{{backend_epoch}}:{{sequence}}"
    );
    // JSON payload round-trips to the same ChangeRecord.
    let payload = consumed_record.value.as_deref().expect("payload present");
    let decoded: ChangeRecord = serde_json::from_slice(payload).expect("payload is ChangeRecord json");
    assert_eq!(decoded, record);
    // pq-* headers present with correct values (order not asserted here: rskafka returns a BTreeMap).
    let header = |k: &str| consumed_record.headers.get(k).map(|v| v.as_slice());
    assert_eq!(consumed_record.headers.len(), 6);
    assert_eq!(header("pq-tenant-id"), Some(b"tenant-a".as_ref()));
    assert_eq!(header("pq-queue-id"), Some(b"queue-a".as_ref()));
    assert_eq!(header("pq-item-id"), Some(b"1".as_ref()));
    assert_eq!(header("pq-backend-epoch"), Some(b"7".as_ref()));
    assert_eq!(header("pq-sequence"), Some(b"1".as_ref()));
    assert_eq!(header("pq-command-kind"), Some(b"push".as_ref()));

    broker.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_topic_maps_one_queue_to_one_partition() {
    let queue_a = queue_definition("tenant-a", "queue-a");
    let queue_b = queue_definition("tenant-b", "queue-b");
    let surface = build_embedded_fjord_surface(7, &test_fjord_config());
    register_embedded_fjord_topics(&surface.topic_registry, &[queue_a.clone(), queue_b.clone()])
        .expect("register topics");
    let port = free_port();
    let broker = spawn_embedded_fjord_broker(
        &surface,
        &format!("kafka://127.0.0.1:{port}"),
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

    let topics = surface.topic_registry.topic_list();
    let partition_count = |topic_name: &str| {
        topics
            .iter()
            .find(|(name, _)| name == topic_name)
            .map(|(_, partitions)| *partitions)
            .expect("topic metadata")
    };
    assert_eq!(partition_count(&topic_a), 1);
    assert_eq!(partition_count(&topic_b), 1);

    broker.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_offset_never_regresses_across_failover() {
    let queue = queue_definition("tenant-a", "queue-a");
    let (surface, _sink) = surface_with_sink(&queue);
    let shard = queue_key("tenant-a", "queue-a");
    let topic = fjord_topic_name(&shard).expect("valid fjord topic");
    let logical_record =
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);

    // Two sink lifecycles (a failover re-emit) over the SAME shared log: offsets must move forward.
    let sink = FjordChangeRecordSink::new(surface.log_backend());
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("initial emit");
    drop(sink);

    let sink = FjordChangeRecordSink::new(surface.log_backend());
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("re-emit");

    let consumed = read_embedded_change_records(&surface, &topic).expect("read change records");
    assert_eq!(consumed.len(), 2);
    let first = consumed[0].offset;
    let second = consumed[1].offset;
    assert!(second > first, "broker offsets must move forward");
    assert_eq!(consumed[0].key, consumed[1].key);
    assert_eq!(consumed[0].partition, consumed[1].partition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_idempotency_key_is_stable_across_reemit() {
    let queue = queue_definition("tenant-a", "queue-a");
    let (surface, _sink) = surface_with_sink(&queue);
    let shard = queue_key("tenant-a", "queue-a");
    let topic = fjord_topic_name(&shard).expect("valid fjord topic");
    let logical_record =
        change_record("tenant-a", "queue-a", Some(1), 7, 1, ChangeRecordKind::Push);

    let sink = FjordChangeRecordSink::new(surface.log_backend());
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("initial emit");
    drop(sink);

    let sink = FjordChangeRecordSink::new(surface.log_backend());
    sink.emit(&shard, std::slice::from_ref(&logical_record))
        .expect("re-emit");

    let consumed = read_embedded_change_records(&surface, &topic).expect("read change records");
    assert_eq!(consumed.len(), 2);
    assert_eq!(consumed[0].key, consumed[1].key);
    assert_eq!(consumed[0].key.as_deref(), Some(b"1:7:1".as_ref()));
}
