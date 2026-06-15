// Integration tests for B-043 canonical pqueue_group_summary projection.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchClaimRequest, PgBatchPushRequest, PgBatchUpdateRequest, PgPushItem, PgUpdateItem,
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
        group_co_residency: true,
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
        max_eligible_group_size: Some(50),
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn push_item(id: &str, group: &str, priority: i64, not_before: Option<UtcTimestamp>) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before,
        group_key: Some(group.to_string()),
        payload: None,
    }
}

fn push_request(queue: &str, items: Vec<PgPushItem>, now: UtcTimestamp) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue}-{}", now.seconds),
        request_id: None,
        items,
        now,
    }
}

fn update_request(queue: &str, item_id: &str, now: UtcTimestamp) -> PgBatchUpdateRequest {
    PgBatchUpdateRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("update-{queue}-{item_id}"),
        request_id: None,
        items: vec![PgUpdateItem {
            item_id: item_id.to_string(),
            expected_item_version: Some(1),
            priority: Some(PriorityValue::Int64(75)),
            not_before: None,
        }],
        now,
    }
}

fn claim_request(queue: &str, max_items: usize, now: UtcTimestamp) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("claim-{queue}-{max_items}"),
        request_id: None,
        max_items,
        now,
        lease_token: format!("lease-{queue}"),
        lease_expires_at: ts(now.seconds + 60),
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
    control.create_queue(queue_def(queue)).await.unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_group_summary_tests_schema_is_canonical() {
    let (client, _append, _pg) = create_queue("schema").await;
    let c = client.lock().await;

    let group_summary = c
        .query_one(
            "SELECT COUNT(*)::int
             FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'pqueue_group_summary'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(group_summary.get::<_, i32>(0), 1);

    let active_scope = c
        .query_one(
            "SELECT COUNT(*)::int
             FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'pqueue_active_scope_summary'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(active_scope.get::<_, i32>(0), 0);
}

#[tokio::test]
async fn postgres_group_summary_tests_push_records_exact_oldest_and_counts() {
    let (client, append, _pg) = create_queue("push-summary").await;
    append
        .batch_push(push_request(
            "push-summary",
            vec![
                push_item("old", "g-old", 1, Some(ts(1_718_000_100))),
                push_item("new", "g-new", 1, Some(ts(1_718_000_150))),
                push_item("future", "g-old", 10, Some(ts(1_718_010_000))),
            ],
            ts(1_718_000_200),
        ))
        .await
        .unwrap();

    let c = client.lock().await;
    let rows = c
        .query(
            "SELECT group_key, oldest_eligible_at, eligible_count, pending_count, leased_count,
                    terminal_count, rep_item_id
             FROM pqueue_group_summary
             WHERE tenant_id='tenant' AND queue_id='push-summary' AND shard_id=0
             ORDER BY oldest_eligible_at ASC NULLS LAST, group_key ASC",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("group_key"), "g-old");
    assert_eq!(
        rows[0]
            .get::<_, Option<time::OffsetDateTime>>("oldest_eligible_at")
            .unwrap()
            .unix_timestamp(),
        1_718_000_100
    );
    assert_eq!(rows[0].get::<_, i64>("eligible_count"), 1);
    assert_eq!(rows[0].get::<_, i64>("pending_count"), 2);
    assert_eq!(rows[0].get::<_, i64>("leased_count"), 0);
    assert_eq!(rows[0].get::<_, i64>("terminal_count"), 0);
    assert_eq!(rows[0].get::<_, String>("rep_item_id"), "old");

    assert_eq!(rows[1].get::<_, String>("group_key"), "g-new");
    assert_eq!(
        rows[1]
            .get::<_, Option<time::OffsetDateTime>>("oldest_eligible_at")
            .unwrap()
            .unix_timestamp(),
        1_718_000_150
    );
}

#[tokio::test]
async fn postgres_group_summary_tests_update_and_claim_refresh_group_rows() {
    let (client, append, _pg) = create_queue("mutate-summary").await;
    append
        .batch_push(push_request(
            "mutate-summary",
            vec![push_item("delayed", "g-update", 5, Some(ts(1_718_010_000)))],
            ts(1_718_000_000),
        ))
        .await
        .unwrap();

    {
        let c = client.lock().await;
        let row = c
            .query_one(
                "SELECT oldest_eligible_at, eligible_count, pending_count
                 FROM pqueue_group_summary
                 WHERE tenant_id='tenant' AND queue_id='mutate-summary' AND group_key='g-update'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            row.get::<_, Option<time::OffsetDateTime>>("oldest_eligible_at")
                .is_none()
        );
        assert_eq!(row.get::<_, i64>("eligible_count"), 0);
        assert_eq!(row.get::<_, i64>("pending_count"), 1);
    }

    append
        .batch_update(update_request(
            "mutate-summary",
            "delayed",
            ts(1_718_000_050),
        ))
        .await
        .unwrap();

    {
        let c = client.lock().await;
        let row = c
            .query_one(
                "SELECT oldest_eligible_at, eligible_count, pending_count, rep_item_id
                 FROM pqueue_group_summary
                 WHERE tenant_id='tenant' AND queue_id='mutate-summary' AND group_key='g-update'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, time::OffsetDateTime>("oldest_eligible_at")
                .unix_timestamp(),
            1_718_000_050
        );
        assert_eq!(row.get::<_, i64>("eligible_count"), 1);
        assert_eq!(row.get::<_, i64>("pending_count"), 1);
        assert_eq!(row.get::<_, String>("rep_item_id"), "delayed");
    }

    append
        .batch_claim(claim_request("mutate-summary", 1, ts(1_718_000_060)))
        .await
        .unwrap();

    let c = client.lock().await;
    let row = c
        .query_one(
            "SELECT oldest_eligible_at, eligible_count, pending_count, leased_count, rep_item_id
             FROM pqueue_group_summary
             WHERE tenant_id='tenant' AND queue_id='mutate-summary' AND group_key='g-update'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, Option<time::OffsetDateTime>>("oldest_eligible_at")
            .is_none()
    );
    assert_eq!(row.get::<_, i64>("eligible_count"), 0);
    assert_eq!(row.get::<_, i64>("pending_count"), 0);
    assert_eq!(row.get::<_, i64>("leased_count"), 1);
    assert!(row.get::<_, Option<String>>("rep_item_id").is_none());
}
