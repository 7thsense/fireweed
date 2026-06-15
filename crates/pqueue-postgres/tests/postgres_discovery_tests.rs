// Integration tests for B-054 Postgres single-shard active-scope discovery.

use std::sync::Arc;

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, GateKeyPolicy, OrderingMode, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy,
    QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchPushRequest, PgDiscoverActiveScopesRequest, PgGateState, PgPushItem, PgSetGate,
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
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(8),
            ..EligibilityPolicy::default()
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

fn push_item(id: &str, group: &str, priority: i64, gates: Vec<&str>) -> PgPushItem {
    PgPushItem {
        item_id: id.to_string(),
        client_item_key: format!("key-{id}"),
        priority: Some(PriorityValue::Int64(priority)),
        not_before: None,
        group_key: Some(group.to_string()),
        cohort_size: None,
        recurrence_until: None,
        gate_keys: gates.into_iter().map(str::to_string).collect(),
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

fn discover_request(queue: &str, now: i64, max_results: usize) -> PgDiscoverActiveScopesRequest {
    PgDiscoverActiveScopesRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        max_results,
        now: ts(now),
    }
}

fn set_gate_request(queue: &str, gate: &str, state: PgGateState) -> PgSetGatesRequest {
    PgSetGatesRequest {
        tenant_id: "tenant".to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: format!("gate-{queue}-{gate}"),
        request_id: None,
        gates: vec![PgSetGate {
            gate_key: gate.to_string(),
            state,
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
async fn postgres_discovery_tests_ranks_single_shard_groups_by_exact_oldest_age() {
    let (_client, append, _pg) = create_queue("discover-rank").await;
    append
        .batch_push(push_request(
            "discover-rank",
            "push-old",
            vec![push_item("old", "group-old", 10, vec![])],
            1_718_000_000,
        ))
        .await
        .unwrap();
    append
        .batch_push(push_request(
            "discover-rank",
            "push-new",
            vec![push_item("new", "group-new", 100, vec![])],
            1_718_000_010,
        ))
        .await
        .unwrap();

    let result = append
        .discover_active_scopes(discover_request("discover-rank", 1_718_000_030, 10))
        .await
        .unwrap();
    assert_eq!(result.scopes.len(), 2);
    assert_eq!(result.scopes[0].group_key, "group-old");
    assert_eq!(result.scopes[0].oldest_eligible_age_ms, 30_000);
    assert_eq!(result.scopes[0].eligible_count, 1);
    assert_eq!(result.scopes[1].group_key, "group-new");
    assert_eq!(result.scopes[1].oldest_eligible_age_ms, 20_000);
}

#[tokio::test]
async fn postgres_discovery_tests_gate_current_advances_to_next_eligible_item() {
    let (_client, append, _pg) = create_queue("discover-gate").await;
    append
        .batch_push(push_request(
            "discover-gate",
            "push-blocked",
            vec![push_item("blocked", "group-a", 100, vec!["gate-a"])],
            1_718_000_000,
        ))
        .await
        .unwrap();
    append
        .batch_push(push_request(
            "discover-gate",
            "push-open",
            vec![push_item("open", "group-a", 90, vec![])],
            1_718_000_010,
        ))
        .await
        .unwrap();

    append
        .set_gates(set_gate_request(
            "discover-gate",
            "gate-a",
            PgGateState::Blocked,
        ))
        .await
        .unwrap();

    let result = append
        .discover_active_scopes(discover_request("discover-gate", 1_718_000_030, 10))
        .await
        .unwrap();
    assert_eq!(result.scopes.len(), 1);
    assert_eq!(result.scopes[0].group_key, "group-a");
    assert_eq!(result.scopes[0].oldest_eligible_age_ms, 20_000);
    assert_eq!(result.scopes[0].eligible_count, 1);
}

#[tokio::test]
async fn postgres_discovery_tests_query_plan_uses_group_summary_not_items_scan() {
    let (client, append, _pg) = create_queue("discover-plan").await;
    append
        .batch_push(push_request(
            "discover-plan",
            "push-plan",
            vec![push_item("plan", "group-plan", 100, vec![])],
            1_718_000_000,
        ))
        .await
        .unwrap();

    let c = client.lock().await;
    let rows = c
        .query(
            "EXPLAIN
             SELECT queue_id, group_key, oldest_eligible_at, eligible_count, updated_at
             FROM pqueue_group_summary
             WHERE tenant_id = 'tenant'
               AND queue_id = 'discover-plan'
               AND shard_id = 0
               AND oldest_eligible_at IS NOT NULL
             ORDER BY oldest_eligible_at ASC,
                      rep_progress_guard_sort ASC,
                      rep_priority_sort ASC,
                      rep_created_at ASC,
                      rep_item_id ASC
             LIMIT 10",
            &[],
        )
        .await
        .unwrap();
    let plan = rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("pqueue_group_summary"));
    assert!(!plan.contains("pqueue_items"));
}
