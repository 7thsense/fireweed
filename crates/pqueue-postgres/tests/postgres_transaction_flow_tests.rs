// Integration tests: ControlPlaneStore transaction flows.
// Verifies: tenant-scoped create/read, static single-shard assignment/epoch,
// and INV-8 (cross-tenant isolation).
//
// In OrbStack Linux, port forwarding doesn't expose mapped ports on 127.0.0.1,
// so we connect directly to the container's bridge IP on port 5432.

use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityModel,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_postgres::PostgresControlPlaneStore;
use pqueue_storage::{
    traits::{ControlPlaneError, ControlPlaneStore},
    types::QueueKey,
};

async fn start_pg() -> (Arc<Mutex<tokio_postgres::Client>>, impl std::fmt::Debug) {
    let pg = Postgres::default().start().await.unwrap();

    let container_ip = {
        let id = pg.id();
        let out = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                id,
            ])
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };

    let url =
        format!("host={container_ip} port=5432 user=postgres password=postgres dbname=postgres");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    (Arc::new(Mutex::new(client)), pg)
}

async fn store_from(client_arc: Arc<Mutex<tokio_postgres::Client>>) -> PostgresControlPlaneStore {
    PostgresControlPlaneStore::new(client_arc).await.unwrap()
}

fn tid(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn simple_def(tenant: TenantId, queue: QueueId) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tenant,
        queue_id: queue,
        priority_model: PriorityModel::timestamp_ascending(),
        ordering_mode: OrderingMode::Strict,
        group_co_residency: false,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy::disabled(),
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: None,
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

// ---------------------------------------------------------------------------
// Create and read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_queue_and_read_back() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let def = simple_def(tid("tenant-a"), qid("orders"));
    let result = s.create_queue(def.clone()).await.unwrap();

    assert!(result.created);
    assert_eq!(result.definition.queue_id, qid("orders"));
    assert_eq!(result.definition.tenant_id, tid("tenant-a"));

    let key = QueueKey { tenant_id: tid("tenant-a"), queue_id: qid("orders") };
    let fetched = s.queue_definition(&key).await.unwrap();
    assert_eq!(fetched.queue_id, qid("orders"));
    assert_eq!(fetched.tenant_id, tid("tenant-a"));
    assert_eq!(fetched.progress_bound_ms, 30_000);
    assert_eq!(fetched.shard_count, 1);
    assert_eq!(fetched.retry_policy.max_attempts, 3);
}

#[tokio::test]
async fn create_queue_roundtrips_priority_model() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let def = simple_def(tid("t"), qid("q-pm"));
    let pm = def.priority_model;
    s.create_queue(def).await.unwrap();

    let key = QueueKey { tenant_id: tid("t"), queue_id: qid("q-pm") };
    let fetched = s.queue_definition(&key).await.unwrap();
    assert_eq!(fetched.priority_model, pm);
}

#[tokio::test]
async fn duplicate_create_returns_queue_already_exists() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let def = simple_def(tid("t"), qid("dup"));
    s.create_queue(def.clone()).await.unwrap();
    let err = s.create_queue(def).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueAlreadyExists);
}

#[tokio::test]
async fn read_missing_queue_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let key = QueueKey { tenant_id: tid("t"), queue_id: qid("ghost") };
    let err = s.queue_definition(&key).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueNotFound);
}

// ---------------------------------------------------------------------------
// Shard assignment and epoch (AC: static single-shard assignment/epoch)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shard_assignments_single_shard_epoch_one() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let def = simple_def(tid("t"), qid("sharded"));
    s.create_queue(def).await.unwrap();

    let key = QueueKey { tenant_id: tid("t"), queue_id: qid("sharded") };
    let shards = s.shard_assignments(&key).await.unwrap();

    assert_eq!(shards.len(), 1, "single-shard queue must have exactly one assignment");
    assert_eq!(shards[0].epoch, 1, "initial epoch must be 1");
    assert!(shards[0].worker_id.is_none(), "initial shard has no owner");
    assert_eq!(shards[0].shard_key.shard_id.as_u32(), 0);
}

#[tokio::test]
async fn shard_assignments_missing_queue_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let key = QueueKey { tenant_id: tid("t"), queue_id: qid("ghost") };
    let err = s.shard_assignments(&key).await.unwrap_err();
    assert_eq!(err, ControlPlaneError::QueueNotFound);
}

// ---------------------------------------------------------------------------
// List queues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_queues_returns_own_queues() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    s.create_queue(simple_def(tid("t"), qid("q1"))).await.unwrap();
    s.create_queue(simple_def(tid("t"), qid("q2"))).await.unwrap();

    let mut listed = s.list_queues(&tid("t")).await.unwrap();
    listed.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let names: Vec<&str> = listed.iter().map(|q| q.as_str()).collect();
    assert_eq!(names, vec!["q1", "q2"]);
}

#[tokio::test]
async fn list_queues_empty_tenant_returns_empty() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    let listed = s.list_queues(&tid("nobody")).await.unwrap();
    assert!(listed.is_empty());
}

// ---------------------------------------------------------------------------
// INV-8: cross-tenant isolation
// Tenant B must not see queues belonging to Tenant A.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inv8_cross_tenant_read_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("secret-queue")))
        .await
        .unwrap();

    let key = QueueKey { tenant_id: tid("tenant-b"), queue_id: qid("secret-queue") };
    let err = s.queue_definition(&key).await.unwrap_err();
    assert_eq!(
        err,
        ControlPlaneError::QueueNotFound,
        "INV-8: cross-tenant queue_definition must return QueueNotFound"
    );
}

#[tokio::test]
async fn inv8_cross_tenant_shard_read_returns_not_found() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("q")))
        .await
        .unwrap();

    let key = QueueKey { tenant_id: tid("tenant-b"), queue_id: qid("q") };
    let err = s.shard_assignments(&key).await.unwrap_err();
    assert_eq!(
        err,
        ControlPlaneError::QueueNotFound,
        "INV-8: cross-tenant shard_assignments must return QueueNotFound"
    );
}

#[tokio::test]
async fn inv8_list_queues_does_not_leak_across_tenants() {
    let (c, _pg) = start_pg().await;
    let s = store_from(c).await;

    s.create_queue(simple_def(tid("tenant-a"), qid("a-queue")))
        .await
        .unwrap();
    s.create_queue(simple_def(tid("tenant-b"), qid("b-queue")))
        .await
        .unwrap();

    let a_list = s.list_queues(&tid("tenant-a")).await.unwrap();
    let b_list = s.list_queues(&tid("tenant-b")).await.unwrap();

    assert_eq!(a_list.len(), 1);
    assert_eq!(a_list[0], qid("a-queue"));
    assert_eq!(b_list.len(), 1);
    assert_eq!(b_list[0], qid("b-queue"), "INV-8: list_queues must not leak across tenants");
}
