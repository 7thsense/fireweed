use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use fireweed_engine::{ControlPlaneConfig, QueueControlPlane, QueueKey, resolve_target};
use fireweed_memory::{ManualClock, composed_memory_backend};
use fireweed_postgres::PostgresControlPlane;
use fireweed_server::start_with_ownership;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn fresh_schema() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    format!(
        "pq_server_cp_{}_{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::SeqCst)
    )
}

fn owner(value: &str) -> OwnerId {
    OwnerId::new(value).unwrap()
}

fn queue_key() -> QueueKey {
    QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new("q1").unwrap())
}

fn queue_definition() -> QueueDefinition {
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
        emit_change_records: false,
    }
}

fn queue_definition_targeting(owner_id: &OwnerId, peers: &[OwnerId]) -> QueueDefinition {
    for index in 0..10_000 {
        let mut definition = queue_definition();
        definition.queue_id = QueueId::new(format!("route-{index}")).unwrap();
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        if resolve_target(&key, peers.iter()).as_ref() == Some(owner_id) {
            return definition;
        }
    }
    panic!("failed to find deterministic queue target");
}

async fn raw_resp(address: std::net::SocketAddr, parts: &[&str]) -> String {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("connect RESP owner");
    let mut request = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        request.extend_from_slice(part.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    stream
        .write_all(&request)
        .await
        .expect("write RESP request");
    let mut response = vec![0; 512];
    let read = stream
        .read(&mut response)
        .await
        .expect("read RESP response");
    String::from_utf8_lossy(&response[..read]).into_owned()
}

#[test]
fn two_service_runtimes_share_owner_membership_and_monotonic_epochs() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES CONTROL-PLANE SERVER WIRING SKIPPED — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    let schema = fresh_schema();
    let control_config = ControlPlaneConfig {
        heartbeat_ttl_ms: 50,
        lease_ttl_ms: 100,
    };
    // Independent connections model separate service processes. Shared observations prove authority lives
    // in Postgres rather than a process-local `InMemoryControlPlane`.
    let cp_a = Arc::new(
        PostgresControlPlane::connect_in_schema(&url, &schema, control_config)
            .expect("connect owner-a control plane"),
    );
    let cp_b = Arc::new(
        PostgresControlPlane::connect_in_schema(&url, &schema, control_config)
            .expect("connect owner-b control plane"),
    );
    let observer = PostgresControlPlane::connect_in_schema(&url, &schema, control_config)
        .expect("connect observer control plane");
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(100));
    let queue = queue_key();
    let queues = [queue_definition()];
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime");

    let server_a = runtime
        .block_on(start_with_ownership(
            backend.clone(),
            cp_a.clone(),
            owner("node-a"),
            clock.clone(),
            "127.0.0.1:0",
            Duration::from_millis(10),
            &queues,
        ))
        .expect("start first service runtime");
    let seen_from_b = cp_b
        .resolve_queue_owner(&queue, UtcTimestamp::new(100, 0).unwrap())
        .expect("second connection resolves first owner");
    assert_eq!(seen_from_b.active_owner.as_ref(), Some(&owner("node-a")));
    assert_eq!(seen_from_b.assignment_epoch, Some(1));

    // Model an owner process kill: stop all owner-a background work, advance beyond both heartbeat and
    // queue-lease TTL, then start a new service runtime through a different Postgres connection.
    server_a.shutdown();
    drop(server_a);
    clock.set(101);
    let server_b = runtime
        .block_on(start_with_ownership(
            backend,
            cp_b.clone(),
            owner("node-b"),
            clock,
            "127.0.0.1:0",
            Duration::from_millis(10),
            &queues,
        ))
        .expect("start replacement service runtime");

    let reassigned = observer
        .resolve_queue_owner(&queue, UtcTimestamp::new(101, 0).unwrap())
        .expect("observer resolves replacement owner");
    assert_eq!(reassigned.active_owner.as_ref(), Some(&owner("node-b")));
    assert_eq!(
        reassigned.assignment_epoch,
        Some(2),
        "a different service runtime must acquire a strictly greater durable epoch"
    );
    server_b.shutdown();
}

#[test]
fn peer_endpoint_discovery_returns_one_hop_moved() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES ENDPOINT-DISCOVERY WIRING SKIPPED — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };

    let schema = fresh_schema();
    let control_config = ControlPlaneConfig {
        heartbeat_ttl_ms: 5_000,
        lease_ttl_ms: 15_000,
    };
    let cp_a = Arc::new(
        PostgresControlPlane::connect_in_schema(&url, &schema, control_config)
            .expect("connect owner-a control plane"),
    );
    let cp_b = Arc::new(
        PostgresControlPlane::connect_in_schema(&url, &schema, control_config)
            .expect("connect owner-b control plane"),
    );
    let owner_a = owner("node-a");
    let owner_b = owner("node-b");
    let definition = queue_definition_targeting(&owner_a, &[owner_a.clone(), owner_b.clone()]);
    let routing_key = format!(
        "{}:{}",
        definition.tenant_id.as_str(),
        definition.queue_id.as_str()
    );
    let backend = Arc::new(composed_memory_backend());
    let clock = Arc::new(ManualClock::at(100));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build test runtime");

    // Pre-register B so deterministic placement is stable before A acquires. B has no endpoint until its
    // own runtime starts, proving endpoint publication is part of service startup rather than test setup.
    cp_b.register_owner(&owner_b, UtcTimestamp::new(100, 0).unwrap())
        .expect("pre-register peer membership");
    let server_a = runtime
        .block_on(start_with_ownership(
            backend.clone(),
            cp_a.clone(),
            owner_a,
            clock.clone(),
            "127.0.0.1:0",
            Duration::from_secs(60),
            std::slice::from_ref(&definition),
        ))
        .expect("start active owner runtime");
    let server_b = runtime
        .block_on(start_with_ownership(
            backend,
            cp_b.clone(),
            owner_b,
            clock,
            "127.0.0.1:0",
            Duration::from_secs(60),
            std::slice::from_ref(&definition),
        ))
        .expect("start wrong-owner runtime");
    assert_ne!(server_a.addr(), server_b.addr());

    let first = runtime.block_on(raw_resp(
        server_b.addr(),
        &["XADD", &routing_key, "*", "priority", "1"],
    ));
    assert!(
        first.starts_with("-MOVED "),
        "expected one MOVED, got {first:?}"
    );
    let advertised = first
        .split_whitespace()
        .last()
        .expect("MOVED includes endpoint")
        .parse::<std::net::SocketAddr>()
        .expect("MOVED endpoint is dialable");
    assert_eq!(advertised, server_a.addr());

    let second = runtime.block_on(raw_resp(
        advertised,
        &["XADD", &routing_key, "*", "priority", "1"],
    ));
    assert!(
        !second.starts_with("-MOVED ") && !second.starts_with("-ERR"),
        "one retry at the advertised active owner must succeed, got {second:?}"
    );
    server_b.shutdown();
    server_a.shutdown();
}
