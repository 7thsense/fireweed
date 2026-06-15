// Integration tests for B-070 Postgres shard assignment leases and epoch fencing.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{AppendError, PgBatchClaimRequest, PgBatchPushRequest, PgPushItem},
    control_plane::{
        PgBeginDrainRequest, PgEpochShardLeaseRequest, PgRegisterOwnerRequest, PgShardLeaseRequest,
    },
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

fn claim_request(queue: &str, expected_epoch: u64) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch,
        command_id: format!("claim-{queue}-{expected_epoch}"),
        request_id: None,
        max_items: 10,
        now: ts(1_718_000_002),
        lease_token: "lease-drain".to_string(),
        lease_expires_at: ts(1_718_000_100),
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

#[tokio::test]
async fn postgres_shard_lease_tests_graceful_drain_blocks_claim_until_reacquire() {
    let (client, control, append, _pg) = create_queue("drain").await;
    for owner in ["owner-a", "owner-b"] {
        control
            .register_owner(PgRegisterOwnerRequest {
                owner_id: owner.to_string(),
                heartbeat_ttl_ms: 10_000,
                now: ts(1_718_000_000),
            })
            .await
            .unwrap();
    }

    let first = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "drain".to_string(),
            shard_id: 0,
            owner_id: "owner-a".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_000),
        })
        .await
        .unwrap();
    append
        .batch_push(push_request("drain", first.assignment_epoch, "pending"))
        .await
        .unwrap();

    let draining = control
        .begin_drain(PgBeginDrainRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "drain".to_string(),
            shard_id: 0,
            owner_id: "owner-a".to_string(),
            expected_epoch: first.assignment_epoch,
            target_owner_id: "owner-b".to_string(),
            now: ts(1_718_000_001),
        })
        .await
        .unwrap();
    assert_eq!(draining.state, "draining");
    assert_eq!(draining.active_owner_id.as_deref(), Some("owner-a"));
    assert_eq!(draining.target_owner_id.as_deref(), Some("owner-b"));

    let blocked_claim = append
        .batch_claim(claim_request("drain", first.assignment_epoch))
        .await
        .unwrap_err();
    assert!(matches!(blocked_claim, AppendError::InvalidRequest(_)));

    let target_blocked = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "drain".to_string(),
            shard_id: 0,
            owner_id: "owner-b".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_002),
        })
        .await
        .unwrap();
    assert!(!target_blocked.acquired);

    control
        .release_shard_lease(PgEpochShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "drain".to_string(),
            shard_id: 0,
            owner_id: "owner-a".to_string(),
            expected_epoch: first.assignment_epoch,
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_003),
        })
        .await
        .unwrap();

    let second = control
        .acquire_shard_lease(PgShardLeaseRequest {
            tenant_id: "tenant".to_string(),
            queue_id: "drain".to_string(),
            shard_id: 0,
            owner_id: "owner-b".to_string(),
            lease_ttl_ms: 10_000,
            now: ts(1_718_000_004),
        })
        .await
        .unwrap();
    assert!(second.acquired);
    assert_eq!(second.assignment_epoch, first.assignment_epoch + 1);

    let claimed = append
        .batch_claim(claim_request("drain", second.assignment_epoch))
        .await
        .unwrap();
    assert_eq!(claimed.claimed_item_ids, vec!["pending"]);

    let c = client.lock().await;
    let state = c
        .query_one(
            "SELECT state, active_owner_id
             FROM pqueue_shards
             WHERE tenant_id='tenant' AND queue_id='drain' AND shard_id=0",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(state.get::<_, String>("state"), "assigned");
    assert_eq!(
        state.get::<_, Option<String>>("active_owner_id").as_deref(),
        Some("owner-b")
    );
}
