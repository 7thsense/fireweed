#![forbid(unsafe_code)]

mod support;

use std::time::{SystemTime, UNIX_EPOCH};

use pqueue_core::{
    CohortPolicy, CreateQueue, EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel,
    PriorityModelKind, PriorityTieBreaker, PriorityValue, QueueCreationPolicy, QueueId,
    RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_postgres::{
    PostgresAppendStore, PostgresControlPlaneStore,
    append::{
        PgBatchClaimRequest, PgBatchFinalizeRequest, PgBatchPushRequest, PgFinalizeItem,
        PgFinalizeKind, PgFinalizeOutcome, PgPushItem, PgPushOutcome,
    },
};
use pqueue_storage::{traits::ControlPlaneStore, types::QueueKey};
use support::local_deployment::LocalPostgresProfile;

const TENANT_ID: &str = "local-smoke-tenant";
const ITEM_ID: &str = "local-smoke-item";
const CLIENT_ITEM_KEY: &str = "local-smoke-key";
const LEASE_TOKEN: &str = "local-smoke-lease-token";

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/postgres_native_local.toml")
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

fn unique_queue_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("local-smoke-{}-{millis}", std::process::id())
}

fn queue_def(queue: &str) -> pqueue_core::QueueDefinition {
    CreateQueue {
        tenant_id: tid(TENANT_ID),
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

fn push_request(queue: &str, command_id: &str) -> PgBatchPushRequest {
    PgBatchPushRequest {
        tenant_id: TENANT_ID.to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: command_id.to_string(),
        request_id: Some("local-smoke-push-request".to_string()),
        items: vec![PgPushItem {
            item_id: ITEM_ID.to_string(),
            client_item_key: CLIENT_ITEM_KEY.to_string(),
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

fn claim_request(queue: &str) -> PgBatchClaimRequest {
    PgBatchClaimRequest {
        tenant_id: TENANT_ID.to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: "local-smoke-claim".to_string(),
        request_id: None,
        max_items: 10,
        now: ts(1_718_000_010),
        lease_token: LEASE_TOKEN.to_string(),
        lease_expires_at: ts(1_718_000_100),
    }
}

fn finalize_request(queue: &str) -> PgBatchFinalizeRequest {
    PgBatchFinalizeRequest {
        tenant_id: TENANT_ID.to_string(),
        queue_id: queue.to_string(),
        shard_id: 0,
        expected_epoch: 1,
        command_id: "local-smoke-finalize".to_string(),
        request_id: Some("local-smoke-finalize-request".to_string()),
        items: vec![PgFinalizeItem {
            item_id: ITEM_ID.to_string(),
            lease_token: LEASE_TOKEN.to_string(),
            kind: PgFinalizeKind::Complete,
            retry_not_before: None,
        }],
        now: ts(1_718_000_020),
    }
}

#[tokio::test]
#[ignore = "requires `docker compose up -d postgres`"]
async fn local_postgres_deployment_smoke_tests() {
    let profile = LocalPostgresProfile::from_fixture(fixture_path());
    let queue = unique_queue_id();

    let first_connection = profile.connect().await;
    let control = PostgresControlPlaneStore::new(first_connection.client.clone())
        .await
        .unwrap();
    let append = PostgresAppendStore::new(first_connection.client.clone())
        .await
        .unwrap();

    control.create_queue(queue_def(&queue)).await.unwrap();
    let key = QueueKey {
        tenant_id: tid(TENANT_ID),
        queue_id: qid(&queue),
    };
    assert_eq!(
        control.queue_definition(&key).await.unwrap().queue_id,
        qid(&queue)
    );

    let pushed = append
        .batch_push(push_request(&queue, "local-smoke-push"))
        .await
        .unwrap();
    assert!(matches!(
        pushed.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));

    let claimed = append.batch_claim(claim_request(&queue)).await.unwrap();
    assert_eq!(claimed.claimed_item_ids, vec![ITEM_ID]);

    let finalized = append
        .batch_finalize(finalize_request(&queue))
        .await
        .unwrap();
    assert_eq!(
        finalized.items[0].outcome,
        PgFinalizeOutcome::Completed { item_version: 3 }
    );

    drop(append);
    drop(control);
    drop(first_connection);

    let restarted_connection = profile.connect().await;
    let restarted_control = PostgresControlPlaneStore::new(restarted_connection.client.clone())
        .await
        .unwrap();
    let restarted_append = PostgresAppendStore::new(restarted_connection.client.clone())
        .await
        .unwrap();
    assert_eq!(
        restarted_control
            .queue_definition(&key)
            .await
            .unwrap()
            .queue_id,
        qid(&queue)
    );

    let client = restarted_connection.client.lock().await;
    let row = client
        .query_one(
            "SELECT lifecycle_state, terminal_at IS NOT NULL AS terminal_set
             FROM pqueue_items
             WHERE tenant_id = $1 AND queue_id = $2 AND item_id = $3",
            &[&TENANT_ID, &queue, &ITEM_ID],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("lifecycle_state"), "complete");
    assert!(row.get::<_, bool>("terminal_set"));
    drop(client);

    let replay = restarted_append
        .batch_push(push_request(&queue, "local-smoke-push-replay"))
        .await
        .unwrap();
    assert_eq!(replay.command_sequence, pushed.command_sequence);
    assert_eq!(replay.items[0].item_id, ITEM_ID);
    assert!(matches!(
        replay.items[0].outcome,
        PgPushOutcome::New { item_version: 1 }
    ));
}
