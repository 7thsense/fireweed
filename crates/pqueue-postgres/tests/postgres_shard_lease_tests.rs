// Integration tests for B-070 Postgres shard assignment leases and epoch fencing.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{AppendError, PgBatchPushRequest, PgPushItem},
    control_plane::{PgRegisterOwnerRequest, PgShardLeaseRequest},
};
use pqueue_storage::traits::ControlPlaneStore;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

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

fn tid(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn queue_def(queue: &str) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tid("tenant"),
        queue_id: qid(queue),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
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

fn push_request(queue: &str, expected_epoch: u64, item_id: &str) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch,
        command_id: format!("push-{queue}-{item_id}"),
        request_id: None,
        items: vec![PgPushItem {
            item_id: item_id.to_string(),
            client_item_key: format!("key-{item_id}"),
            priority: Some(PriorityValue::Int64(10)),
            not_before: None,
            group_key: None,
            cohort_size: None,
            recurrence_until: None,
            gate_keys: vec![],
            payload: None,
        }],
        now: ts(1_718_000_000),
    }
}

async fn create_queue(
    queue: &str,
) -> (
    Arc<Mutex<tokio_postgres::Client>>,
    PostgresControlPlaneStore,
    PostgresAppendStore,
    impl std::fmt::Debug,
) {
    let (client, pg) = start_pg().await;
    let control = PostgresControlPlaneStore::new(client.clone())
        .await
        .unwrap();
    let append = PostgresAppendStore::new(client.clone()).await.unwrap();
    control.create_queue(queue_def(queue)).await.unwrap();
    (client, control, append, pg)
}

#[tokio::test]
async fn postgres_shard_lease_tests_acquire_reclaim_and_epoch_fence_appends() {
    let (_client, control, append, _pg) = create_queue("lease").await;
    control
        .register_owner(PgRegisterOwnerRequest {
            owner_id: "owner-a".to_string(),
            heartbeat_ttl_ms: 10_000,
            now: ts(1_718_000_000),
        })
        .await
        .unwrap();
    control
        .register_owner(PgRegisterOwnerRequest {
            owner_id: "owner-b".to_string(),
            heartbeat_ttl_ms: 10_000,
            now: ts(1_718_000_000),
        })
        .await
        .unwrap();

    let first = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "lease".to_string(),
            shard_id: 0,
            owner_id: "owner-a".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_000),
        })
        .await
        .unwrap();
    assert!(first.acquired);
    assert_eq!(first.assignment_epoch, 2);

    let blocked = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "lease".to_string(),
            shard_id: 0,
            owner_id: "owner-b".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_001),
        })
        .await
        .unwrap();
    assert!(!blocked.acquired);
    assert_eq!(blocked.assignment_epoch, first.assignment_epoch);
    assert_eq!(blocked.active_owner_id.as_deref(), Some("owner-a"));

    let current = append
        .batch_push(push_request("lease", first.assignment_epoch, "current"))
        .await
        .unwrap();
    assert_eq!(current.command_sequence, 0);

    let second = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "lease".to_string(),
            shard_id: 0,
            owner_id: "owner-b".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_020),
        })
        .await
        .unwrap();
    assert!(second.acquired);
    assert_eq!(second.assignment_epoch, first.assignment_epoch + 1);

    let stale = append
        .batch_push(push_request("lease", first.assignment_epoch, "stale"))
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        AppendError::EpochMismatch {
            expected: 2,
            current: 3
        }
    ));

    append
        .batch_push(push_request("lease", second.assignment_epoch, "fresh"))
        .await
        .unwrap();
}
