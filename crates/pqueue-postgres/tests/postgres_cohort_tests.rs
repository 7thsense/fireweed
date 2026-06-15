// Integration tests for B-052 Postgres cohort storage behavior.

use std::sync::Arc;

use pqueue_core::{
    CohortOnIncomplete, CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        AppendError, PgBatchClaimRequest, PgBatchPushRequest, PgCohortClaimRequest,
        PgCohortExpiredRequest, PgPushItem,
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

fn cohort_queue(queue: &str) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tid("tenant"),
        queue_id: qid(queue),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        group_co_residency: true,
        progress_bound_ms: 30_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: CohortPolicy {
            enabled: true,
            completion_bound_ms: Some(1_000),
            on_incomplete: Some(CohortOnIncomplete::ExpireCohort),
            max_cohort_size: Some(4),
        },
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 86_400_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 50,
        max_eligible_group_size: Some(50),
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn push_item(id: &str, group: &str, priority: i64, cohort_size: Option<u32>) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: Some(group.to_string()),
        cohort_size,
        gate_keys: vec![],
        payload: None,
    }
}

fn push_request(
    queue: &str,
    command: &str,
    items: Vec<PgPushItem>,
    now: i64,
) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: command.to_string(),
        request_id: None,
        items,
        now: ts(now),
    }
}

fn item_claim_request(queue: &str) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("item-claim-{queue}"),
        request_id: None,
        max_items: 10,
        now: ts(1_718_000_010),
        lease_token: format!("item-lease-{queue}"),
        lease_expires_at: ts(1_718_000_100),
    }
}

fn cohort_claim_request(queue: &str, max_items: usize) -> PgCohortClaimRequest {
    PgCohortClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cohort-claim-{queue}"),
        request_id: None,
        max_items,
        now: ts(1_718_000_010),
        cohort_lease_token: format!("cohort-lease-{queue}"),
        lease_expires_at: ts(1_718_000_100),
    }
}

fn expire_request(queue: &str, now: i64) -> PgCohortExpiredRequest {
    PgCohortExpiredRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("cohort-expire-{queue}"),
        request_id: None,
        max_cohorts: 10,
        now: ts(now),
    }
}

async fn create_queue(
    queue: &str,
) -> (
    Arc<Mutex<tokio_postgres::Client>>,
    PostgresAppendStore,
    impl std::fmt::Debug,
) {
    let (client, pg) = start_pg().await;
    let control = PostgresControlPlaneStore::new(client.clone())
        .await
        .unwrap();
    let append = PostgresAppendStore::new(client.clone()).await.unwrap();
    control.create_queue(cohort_queue(queue)).await.unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_cohort_tests_complete_cohort_leases_atomically_without_member_leakage() {
    let (client, append, _pg) = create_queue("complete-cohort").await;
    append
        .batch_push(push_request(
            "complete-cohort",
            "push-complete",
            vec![
                push_item("member-a", "callback-1", 100, Some(2)),
                push_item("member-b", "callback-1", 10, Some(2)),
            ],
            1_718_000_000,
        ))
        .await
        .unwrap();

    let item_claim = append
        .batch_claim(item_claim_request("complete-cohort"))
        .await
        .unwrap();
    assert_eq!(item_claim.claimed_item_ids, Vec::<String>::new());

    let cohort_claim = append
        .cohort_claim(cohort_claim_request("complete-cohort", 10))
        .await
        .unwrap();
    assert_eq!(cohort_claim.group_key.as_deref(), Some("callback-1"));
    assert_eq!(cohort_claim.claimed_item_ids, vec!["member-a", "member-b"]);

    let c = client.lock().await;
    let row = c
        .query_one(
            "SELECT state, cohort_lease_token_hash IS NOT NULL AS leased
             FROM pqueue_cohorts
             WHERE tenant_id='tenant' AND queue_id='complete-cohort' AND group_key='callback-1'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("state"), "leased");
    assert!(row.get::<_, bool>("leased"));

    let leaked = c
        .query_one(
            "SELECT COUNT(*)::bigint
             FROM pqueue_items
             WHERE tenant_id='tenant'
               AND queue_id='complete-cohort'
               AND group_key='callback-1'
               AND lifecycle_state <> 'leased'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(leaked, 0);
}

#[tokio::test]
async fn postgres_cohort_tests_incomplete_cohort_expires_before_member_leakage() {
    let (client, append, _pg) = create_queue("incomplete-cohort").await;
    append
        .batch_push(push_request(
            "incomplete-cohort",
            "push-incomplete",
            vec![push_item("member-a", "callback-2", 100, Some(2))],
            1_718_000_000,
        ))
        .await
        .unwrap();

    let item_claim = append
        .batch_claim(item_claim_request("incomplete-cohort"))
        .await
        .unwrap();
    assert_eq!(item_claim.claimed_item_ids, Vec::<String>::new());

    let expired = append
        .materialize_expired_cohorts(expire_request("incomplete-cohort", 1_718_000_002))
        .await
        .unwrap();
    assert_eq!(expired.expired_group_keys, vec!["callback-2"]);
    assert_eq!(expired.expired_item_ids, vec!["member-a"]);

    let cohort_claim = append
        .cohort_claim(cohort_claim_request("incomplete-cohort", 10))
        .await
        .unwrap();
    assert_eq!(cohort_claim.claimed_item_ids, Vec::<String>::new());

    let c = client.lock().await;
    let row = c
        .query_one(
            "SELECT c.state, i.lifecycle_state, i.failure_code, c.expire_command_pos IS NOT NULL AS has_expire_pos
             FROM pqueue_cohorts c
             JOIN pqueue_items i
               ON i.tenant_id = c.tenant_id
              AND i.queue_id = c.queue_id
              AND i.shard_id = c.shard_id
              AND i.group_key = c.group_key
             WHERE c.tenant_id='tenant'
               AND c.queue_id='incomplete-cohort'
               AND c.group_key='callback-2'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("state"), "terminal");
    assert_eq!(row.get::<_, String>("lifecycle_state"), "failed");
    assert_eq!(row.get::<_, String>("failure_code"), "cohort-incomplete");
    assert!(row.get::<_, bool>("has_expire_pos"));
}

#[tokio::test]
async fn postgres_cohort_tests_rejects_missing_and_conflicting_cohort_shape() {
    let (_client, append, _pg) = create_queue("cohort-shape").await;
    let missing = append
        .batch_push(push_request(
            "cohort-shape",
            "push-missing",
            vec![push_item("member-a", "callback-3", 100, None)],
            1_718_000_000,
        ))
        .await
        .unwrap_err();
    assert!(matches!(missing, AppendError::InvalidRequest(_)));

    append
        .batch_push(push_request(
            "cohort-shape",
            "push-first",
            vec![push_item("member-a", "callback-3", 100, Some(2))],
            1_718_000_000,
        ))
        .await
        .unwrap();
    let conflict = append
        .batch_push(push_request(
            "cohort-shape",
            "push-conflict",
            vec![push_item("member-b", "callback-3", 90, Some(3))],
            1_718_000_001,
        ))
        .await
        .unwrap_err();
    assert!(matches!(conflict, AppendError::RequestConflict));
}
