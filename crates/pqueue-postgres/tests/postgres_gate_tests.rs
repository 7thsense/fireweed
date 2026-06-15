// Integration tests for B-050 Postgres dynamic gate storage.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, GateKeyPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy,
    QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchClaimRequest, PgBatchPushRequest, PgGateState, PgPushItem, PgSetGate,
        PgSetGatesRequest,
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
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: Default::default(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(8),
            max_gates_per_request: Some(8),
        },
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

fn push_item(id: &str, priority: i64, gate_keys: Vec<&str>) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: Some("group".to_string()),
        gate_keys: gate_keys.into_iter().map(str::to_string).collect(),
        payload: None,
    }
}

fn push_request(queue: &str) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("push-{queue}"),
        request_id: None,
        items: vec![
            push_item("blocked-high", 100, vec!["acct-blocked"]),
            push_item("open-low", 10, vec!["acct-open"]),
        ],
        now: ts(1_718_000_000),
    }
}

fn claim_request(queue: &str, token: &str) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("claim-{queue}-{token}"),
        request_id: None,
        max_items: 1,
        now: ts(1_718_000_010),
        lease_token: token.to_string(),
        lease_expires_at: ts(1_718_000_100),
    }
}

fn set_gates_request(
    queue: &str,
    gate_key: &str,
    state: PgGateState,
    seq: u32,
) -> PgSetGatesRequest {
    PgSetGatesRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("set-gates-{queue}-{seq}"),
        request_id: None,
        gates: vec![PgSetGate {
            gate_key: gate_key.to_string(),
            state,
        }],
        now: ts(1_718_000_005 + seq as i64),
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
async fn postgres_gate_tests_blocked_gate_is_not_claimed_and_flip_touches_no_items() {
    let (client, append, _pg) = create_queue("gates").await;
    append.batch_push(push_request("gates")).await.unwrap();

    let c = client.lock().await;
    let before_versions = c
        .query(
            "SELECT item_id, item_version, updated_at
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='gates'
             ORDER BY item_id",
            &[],
        )
        .await
        .unwrap();
    drop(c);

    let set = append
        .set_gates(set_gates_request(
            "gates",
            "acct-blocked",
            PgGateState::Blocked,
            1,
        ))
        .await
        .unwrap();
    assert_eq!(set.gates.len(), 1);

    let claimed = append
        .batch_claim(claim_request("gates", "tok-open"))
        .await
        .unwrap();
    assert_eq!(
        claimed.claimed_item_ids,
        vec!["open-low"],
        "blocked high-priority item must be skipped by gate anti-join"
    );

    let c = client.lock().await;
    let after_versions = c
        .query(
            "SELECT item_id, item_version, updated_at
             FROM pqueue_items
             WHERE tenant_id='tenant' AND queue_id='gates'
             ORDER BY item_id",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(before_versions.len(), after_versions.len());
    for (before, after) in before_versions.iter().zip(after_versions.iter()) {
        assert_eq!(
            before.get::<_, String>("item_id"),
            after.get::<_, String>("item_id")
        );
        if before.get::<_, String>("item_id") == "blocked-high" {
            assert_eq!(
                before.get::<_, i64>("item_version"),
                after.get::<_, i64>("item_version"),
                "SetGates must not rewrite blocked item rows"
            );
            assert_eq!(
                before.get::<_, time::OffsetDateTime>("updated_at"),
                after.get::<_, time::OffsetDateTime>("updated_at"),
                "SetGates must not update item timestamps"
            );
        }
    }
}

#[tokio::test]
async fn postgres_gate_tests_reopening_gate_makes_pending_item_claimable() {
    let (_client, append, _pg) = create_queue("gates-open").await;
    append.batch_push(push_request("gates-open")).await.unwrap();
    append
        .set_gates(set_gates_request(
            "gates-open",
            "acct-blocked",
            PgGateState::Blocked,
            1,
        ))
        .await
        .unwrap();

    let first = append
        .batch_claim(claim_request("gates-open", "tok-open"))
        .await
        .unwrap();
    assert_eq!(first.claimed_item_ids, vec!["open-low"]);

    append
        .set_gates(set_gates_request(
            "gates-open",
            "acct-blocked",
            PgGateState::Open,
            2,
        ))
        .await
        .unwrap();

    let second = append
        .batch_claim(claim_request("gates-open", "tok-reopened"))
        .await
        .unwrap();
    assert_eq!(second.claimed_item_ids, vec!["blocked-high"]);
}
