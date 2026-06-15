// Integration tests for B-051 Postgres whole-eligible-group batching.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{AppendError, PgBatchPushRequest, PgGroupBatchClaimRequest, PgPushItem},
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

fn queue_def(queue: &str, co_resident: bool) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tid("tenant"),
        queue_id: qid(queue),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Descending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        group_co_residency: co_resident,
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
        max_eligible_group_size: co_resident.then_some(50),
        shard_count: Some(1),
    }
    .validate(&QueueCreationPolicy::default())
    .unwrap()
}

fn push_item(id: &str, group: Option<&str>, priority: i64) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: group.map(str::to_string),
        cohort_size: None,
        gate_keys: vec![],
        payload: None,
    }
}

fn push_request(queue: &str, items: Vec<PgPushItem>) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue}-{}", items.len()),
        request_id: None,
        items,
        now: ts(1_718_000_000),
    }
}

fn group_claim_request(
    queue: &str,
    max_groups: usize,
    max_items: usize,
) -> PgGroupBatchClaimRequest {
    PgGroupBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("group-claim-{queue}-{max_groups}-{max_items}"),
        request_id: None,
        max_groups,
        max_items,
        now: ts(1_718_000_010),
        lease_token: format!("lease-{queue}"),
        lease_expires_at: ts(1_718_000_100),
    }
}

async fn create_queue(
    queue: &str,
    co_resident: bool,
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
    control
        .create_queue(queue_def(queue, co_resident))
        .await
        .unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_group_batching_tests_claims_whole_eligible_group_without_split() {
    let (_client, append, _pg) = create_queue("whole-group", true).await;
    append
        .batch_push(push_request(
            "whole-group",
            vec![
                push_item("a-high", Some("group-a"), 100),
                push_item("a-low", Some("group-a"), 10),
                push_item("b-mid", Some("group-b"), 50),
            ],
        ))
        .await
        .unwrap();

    let first = append
        .group_batch_claim(group_claim_request("whole-group", 1, 10))
        .await
        .unwrap();
    assert_eq!(first.claimed_group_keys, vec!["group-a"]);
    assert_eq!(first.claimed_item_ids, vec!["a-high", "a-low"]);

    let mut second_req = group_claim_request("whole-group", 1, 10);
    second_req.command_id = "group-claim-whole-group-second".to_string();
    second_req.lease_token = "lease-whole-group-second".to_string();
    let second = append.group_batch_claim(second_req).await.unwrap();
    assert_eq!(second.claimed_group_keys, vec!["group-b"]);
    assert_eq!(second.claimed_item_ids, vec!["b-mid"]);
}

#[tokio::test]
async fn postgres_group_batching_tests_next_group_too_large_leases_nothing() {
    let (client, append, _pg) = create_queue("too-large", true).await;
    append
        .batch_push(push_request(
            "too-large",
            vec![
                push_item("a1", Some("group-a"), 100),
                push_item("a2", Some("group-a"), 90),
            ],
        ))
        .await
        .unwrap();

    let err = append
        .group_batch_claim(group_claim_request("too-large", 1, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, AppendError::BatchTooLarge));

    let c = client.lock().await;
    let leased = c
        .query_one(
            "SELECT COUNT(*)::bigint
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='too-large' AND lifecycle_state='leased'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(leased, 0);
}

#[tokio::test]
async fn postgres_group_batching_tests_rejects_non_co_resident_queue_and_missing_group_key() {
    let (_client, append, _pg) = create_queue("non-co", false).await;
    append
        .batch_push(push_request("non-co", vec![push_item("i1", None, 1)]))
        .await
        .unwrap();

    let err = append
        .group_batch_claim(group_claim_request("non-co", 1, 10))
        .await
        .unwrap_err();
    assert!(matches!(err, AppendError::InvalidRequest(_)));

    let (_client, append, _pg) = create_queue("missing-group", true).await;
    let err = append
        .batch_push(push_request(
            "missing-group",
            vec![push_item("i1", None, 1)],
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, AppendError::InvalidRequest(_)));
}
