use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    UtcTimestamp,
};
use pqueue_engine::{ControlPlaneConfig, QueueControlPlane, QueueKey};
use pqueue_memory::{ManualClock, composed_memory_backend};
use pqueue_postgres::PostgresControlPlane;
use pqueue_server::start_with_ownership;

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
            cp_a,
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
            cp_b,
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
