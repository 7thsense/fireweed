// Integration tests for B-041 Postgres claim path:
// single active lease, strict ordering, bounded-relaxed zero-rank-error
// baseline, and not_before/progress guard basics.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{PgBatchClaimRequest, PgBatchPushRequest, PgPushItem},
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

async fn control_store(client: Arc<Mutex<tokio_postgres::Client>>) -> PostgresControlPlaneStore {
    PostgresControlPlaneStore::new(client).await.unwrap()
}

async fn append_store(client: Arc<Mutex<tokio_postgres::Client>>) -> PostgresAppendStore {
    PostgresAppendStore::new(client).await.unwrap()
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

fn int64_descending_model() -> PriorityModel {
    PriorityModel {
        kind: PriorityModelKind::Int64,
        direction: PriorityDirection::Descending,
        tie_breaker: PriorityTieBreaker::CreatedSequence,
    }
}

fn queue_def(
    tenant: TenantId,
    queue: QueueId,
    ordering_mode: OrderingMode,
) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tenant,
        queue_id: queue,
        priority_model: int64_descending_model(),
        ordering_mode,
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

fn push_item(id: &str, key: &str, priority: i64, not_before: Option<UtcTimestamp>) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: key.to_string(),
        priority: Some(PriorityValue::Int64(priority)),
        not_before,
        group_key: None,
        payload: None,
    }
}

fn push_request(queue_id: &str, items: Vec<PgPushItem>) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue_id.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue_id}-{}", items.len()),
        request_id: None,
        items,
        now: ts(1_718_000_000),
    }
}

fn claim_request(queue_id: &str, token: &str, max_items: usize) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue_id.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("claim-{queue_id}-{token}"),
        request_id: None,
        max_items,
        now: ts(1_718_000_010),
        lease_token: token.to_string(),
        lease_expires_at: ts(1_718_000_070),
    }
}

async fn create_queue_and_store(
    queue_id: &str,
    ordering_mode: OrderingMode,
) -> (
    Arc<Mutex<tokio_postgres::Client>>,
    PostgresAppendStore,
    impl std::fmt::Debug,
) {
    let (client, pg) = start_pg().await;
    let control = control_store(client.clone()).await;
    let append = append_store(client.clone()).await;
    control
        .create_queue(queue_def(tid("tenant"), qid(queue_id), ordering_mode))
        .await
        .unwrap();
    (client, append, pg)
}

#[tokio::test]
async fn postgres_concurrency_claim_tests_strict_order_and_single_active_lease() {
    let (client, append, _pg) = create_queue_and_store("strict", OrderingMode::Strict).await;
    append
        .batch_push(push_request(
            "strict",
            vec![
                push_item("low", "k-low", 10, None),
                push_item("high", "k-high", 100, None),
                push_item("mid", "k-mid", 50, None),
            ],
        ))
        .await
        .unwrap();

    let first = append
        .batch_claim(claim_request("strict", "tok-1", 2))
        .await
        .unwrap();
    assert_eq!(first.claimed_item_ids, vec!["high", "mid"]);

    let second = append
        .batch_claim(claim_request("strict", "tok-2", 3))
        .await
        .unwrap();
    assert_eq!(
        second.claimed_item_ids,
        vec!["low"],
        "already leased rows must not be claimed a second time"
    );

    let c = client.lock().await;
    let active = c
        .query_one(
            "SELECT COUNT(*)::bigint
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='strict' AND lifecycle_state='leased'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(active.get::<_, i64>(0), 3);
}

#[tokio::test]
async fn postgres_concurrency_claim_tests_not_before_items_are_not_claimed() {
    let (_client, append, _pg) = create_queue_and_store("future", OrderingMode::Strict).await;
    append
        .batch_push(push_request(
            "future",
            vec![
                push_item("ready", "k-ready", 10, None),
                push_item("future", "k-future", 100, Some(ts(1_718_010_000))),
            ],
        ))
        .await
        .unwrap();

    let claimed = append
        .batch_claim(claim_request("future", "tok-ready", 10))
        .await
        .unwrap();
    assert_eq!(claimed.claimed_item_ids, vec!["ready"]);
}

#[tokio::test]
async fn postgres_concurrency_claim_tests_bounded_relaxed_stays_within_zero_rank_error() {
    let (_client, append, _pg) =
        create_queue_and_store("relaxed", OrderingMode::BoundedRelaxed).await;
    append
        .batch_push(push_request(
            "relaxed",
            vec![
                push_item("p1", "k-p1", 1, None),
                push_item("p3", "k-p3", 3, None),
                push_item("p2", "k-p2", 2, None),
            ],
        ))
        .await
        .unwrap();

    let claimed = append
        .batch_claim(claim_request("relaxed", "tok-relaxed", 3))
        .await
        .unwrap();
    assert_eq!(
        claimed.claimed_item_ids,
        vec!["p3", "p2", "p1"],
        "exact priority order is valid bounded-relaxed behavior with zero rank error"
    );
}

#[tokio::test]
async fn postgres_concurrency_claim_tests_claim_writes_command_and_lease_metadata() {
    let (client, append, _pg) = create_queue_and_store("command", OrderingMode::Strict).await;
    append
        .batch_push(push_request(
            "command",
            vec![push_item("i1", "k-i1", 5, None)],
        ))
        .await
        .unwrap();

    let result = append
        .batch_claim(claim_request("command", "tok-command", 1))
        .await
        .unwrap();
    assert_eq!(result.command_sequence, 1);
    assert_eq!(result.claimed_item_ids, vec!["i1"]);

    let c = client.lock().await;
    let item = c
        .query_one(
            "SELECT lifecycle_state, lease_token_hash IS NOT NULL AS has_hash,
                    lease_expires_at IS NOT NULL AS has_expiry, item_version
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='command' AND item_id='i1'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(item.get::<_, String>("lifecycle_state"), "leased");
    assert!(item.get::<_, bool>("has_hash"));
    assert!(item.get::<_, bool>("has_expiry"));
    assert_eq!(item.get::<_, i64>("item_version"), 2);

    let command = c
        .query_one(
            "SELECT command_type, item_ids
             FROM pqueue_commands
             WHERE tenant_id='tenant' AND queue_id='command' AND sequence=1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(command.get::<_, String>("command_type"), "batch_claim");
    assert_eq!(command.get::<_, Vec<String>>("item_ids"), vec!["i1"]);
}
