// Integration tests for B-042 Postgres durability, idempotency, and replay.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        AppendError, PgBatchClaimRequest, PgBatchFinalizeRequest, PgBatchPushRequest,
        PgFinalizeItem, PgFinalizeKind, PgFinalizeOutcome, PgPushItem, PgPushOutcome,
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

fn push_item(id: &str, key: &str) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: key.to_string(),
        priority: Some(PriorityValue::Int64(10)),
        not_before: None,
        group_key: None,
        gate_keys: vec![],
        payload: None,
    }
}

fn push_request(
    queue: &str,
    command_id: &str,
    request_id: Option<&str>,
    items: Vec<PgPushItem>,
) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: command_id.to_string(),
        request_id: request_id.map(str::to_string),
        items,
        now: ts(1_718_000_000),
    }
}

fn claim_request(queue: &str) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("claim-{queue}"),
        request_id: None,
        max_items: 10,
        now: ts(1_718_000_010),
        lease_token: "tok".to_string(),
        lease_expires_at: ts(1_718_000_100),
    }
}

fn finalize_request(queue: &str) -> PgBatchFinalizeRequest {
    PgBatchFinalizeRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("finalize-{queue}"),
        request_id: None,
        items: vec![PgFinalizeItem {
            item_id: "item".to_string(),
            lease_token: "tok".to_string(),
            kind: PgFinalizeKind::Complete,
            retry_not_before: None,
        }],
        now: ts(1_718_000_020),
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
async fn postgres_durability_idempotency_tests_request_replay_returns_stored_response() {
    let (client, append, _pg) = create_queue("idem-replay").await;
    let first = append
        .batch_push(push_request(
            "idem-replay",
            "push-1",
            Some("req-1"),
            vec![push_item("item", "key")],
        ))
        .await
        .unwrap();
    assert_eq!(first.command_sequence, 0);
    assert!(matches!(
        first.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));

    let replay = append
        .batch_push(push_request(
            "idem-replay",
            "push-1-replay-command-id-ignored",
            Some("req-1"),
            vec![push_item("item", "key")],
        ))
        .await
        .unwrap();
    assert_eq!(replay.command_sequence, first.command_sequence);
    assert!(matches!(
        replay.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));

    let c = client.lock().await;
    let counts = c
        .query_one(
            "SELECT
               (SELECT COUNT(*)::bigint FROM pqueue_commands
                WHERE tenant_id='tenant' AND queue_id='idem-replay') AS command_count,
               (SELECT COUNT(*)::bigint FROM pqueue_items
                WHERE tenant_id='tenant' AND queue_id='idem-replay') AS item_count,
               (SELECT COUNT(*)::bigint FROM pqueue_request_idempotency
                WHERE tenant_id='tenant' AND queue_id='idem-replay' AND request_id='req-1')
                AS idem_count",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(counts.get::<_, i64>("command_count"), 1);
    assert_eq!(counts.get::<_, i64>("item_count"), 1);
    assert_eq!(counts.get::<_, i64>("idem_count"), 1);
}

#[tokio::test]
async fn postgres_durability_idempotency_tests_request_replay_conflict_is_rejected() {
    let (_client, append, _pg) = create_queue("idem-conflict").await;
    append
        .batch_push(push_request(
            "idem-conflict",
            "push-1",
            Some("req-conflict"),
            vec![push_item("item-a", "key-a")],
        ))
        .await
        .unwrap();

    let err = append
        .batch_push(push_request(
            "idem-conflict",
            "push-2",
            Some("req-conflict"),
            vec![push_item("item-b", "key-b")],
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, AppendError::RequestConflict));
}

#[tokio::test]
async fn postgres_durability_idempotency_tests_command_log_replays_mutation_order() {
    let (client, append, _pg) = create_queue("command-replay").await;
    append
        .batch_push(push_request(
            "command-replay",
            "push",
            Some("req-push"),
            vec![push_item("item", "key")],
        ))
        .await
        .unwrap();
    append
        .batch_claim(claim_request("command-replay"))
        .await
        .unwrap();
    let finalized = append
        .batch_finalize(finalize_request("command-replay"))
        .await
        .unwrap();
    assert_eq!(
        finalized.items[0].outcome,
        PgFinalizeOutcome::Completed { item_version: 3 }
    );

    let c = client.lock().await;
    let rows = c
        .query(
            "SELECT sequence, command_type, item_ids
             FROM pqueue_commands
             WHERE tenant_id='tenant' AND queue_id='command-replay' AND shard_id=0
             ORDER BY sequence ASC",
            &[],
        )
        .await
        .unwrap();
    let command_types: Vec<String> = rows.iter().map(|row| row.get("command_type")).collect();
    assert_eq!(
        command_types,
        vec!["batch_push", "batch_claim", "batch_finalize"]
    );
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i64>("sequence"), idx as i64);
        assert_eq!(row.get::<_, Vec<String>>("item_ids"), vec!["item"]);
    }
}
