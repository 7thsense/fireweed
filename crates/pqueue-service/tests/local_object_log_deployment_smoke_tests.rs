#![forbid(unsafe_code)]

mod support;

use std::time::{SystemTime, UNIX_EPOCH};

use pqueue_core::{
    ClientItemKey, CohortPolicy, CreateQueue, EligibilityPolicy, ItemId, OrderingMode,
    PriorityDirection, PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue,
    QueueCreationPolicy, QueueId, RecurrencePolicy, RetryPolicy, TenantId, UtcTimestamp,
};
use pqueue_sqlite::SqliteProjection;
use pqueue_storage::commands::{
    BatchClaimCommand, BatchFinalizeCommand, BatchPushCommand, CommandEnvelope, CommandId,
    FinalizeKind, FinalizeOutcome, PushItem, QueueCommand,
};
use pqueue_storage::traits::LogStore;
use pqueue_storage::types::{CommandChecksum, ShardId, ShardKey};
use support::local_object_log::LocalObjectLogProfile;

const TENANT_ID: &str = "local-object-log-tenant";
const ITEM_ID: &str = "local-object-log-item";
const CLIENT_ITEM_KEY: &str = "local-object-log-key";
const LEASE_TOKEN: &str = "local-object-log-lease";

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/object_log_sqlite_projection_local.toml")
}

fn tid(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn qid(s: &str) -> QueueId {
    QueueId::new(s).unwrap()
}

fn iid(s: &str) -> ItemId {
    ItemId::new(s).unwrap()
}

fn cik(s: &str) -> ClientItemKey {
    ClientItemKey::new(s).unwrap()
}

fn ts(seconds: i64) -> UtcTimestamp {
    UtcTimestamp::new(seconds, 0).unwrap()
}

fn unique_queue_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("local-object-log-{}-{millis}", std::process::id())
}

fn shard_key(queue: &str) -> ShardKey {
    ShardKey {
        tenant_id: tid(TENANT_ID),
        queue_id: qid(queue),
        shard_id: ShardId::new(0),
    }
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

fn push_envelope(queue: &str) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("local-object-log-push"),
        request_id: Some(pqueue_core::RequestId::new("local-object-log-push-request").unwrap()),
        tenant_id: tid(TENANT_ID),
        queue_id: qid(queue),
        shard_id: ShardId::new(0),
        item_ids: vec![iid(ITEM_ID)],
        command: QueueCommand::BatchPush(BatchPushCommand {
            items: vec![PushItem {
                item_id: iid(ITEM_ID),
                client_item_key: cik(CLIENT_ITEM_KEY),
                priority: Some(PriorityValue::Int64(10)),
                not_before: None,
                max_attempts: 3,
                payload: None,
            }],
        }),
        checksum: CommandChecksum(1),
        created_at: ts(1_718_000_000),
    }
}

fn claim_envelope(queue: &str) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("local-object-log-claim"),
        request_id: None,
        tenant_id: tid(TENANT_ID),
        queue_id: qid(queue),
        shard_id: ShardId::new(0),
        item_ids: vec![iid(ITEM_ID)],
        command: QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids: vec![iid(ITEM_ID)],
            lease_token: LEASE_TOKEN.to_string(),
            lease_expires_at: ts(1_718_000_100),
        }),
        checksum: CommandChecksum(2),
        created_at: ts(1_718_000_010),
    }
}

fn finalize_envelope(queue: &str) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new("local-object-log-finalize"),
        request_id: Some(pqueue_core::RequestId::new("local-object-log-finalize-request").unwrap()),
        tenant_id: tid(TENANT_ID),
        queue_id: qid(queue),
        shard_id: ShardId::new(0),
        item_ids: vec![iid(ITEM_ID)],
        command: QueueCommand::BatchFinalize(BatchFinalizeCommand {
            outcomes: vec![FinalizeOutcome {
                item_id: iid(ITEM_ID),
                kind: FinalizeKind::Complete,
            }],
        }),
        checksum: CommandChecksum(3),
        created_at: ts(1_718_000_020),
    }
}

#[tokio::test]
#[ignore = "local object-log deployment smoke is opt-in"]
async fn local_object_log_deployment_smoke_tests() {
    let profile = LocalObjectLogProfile::from_fixture(fixture_path());
    let queue = unique_queue_id();
    let definition = queue_def(&queue);
    let queue_manifest = profile.persist_queue_manifest(&definition);
    assert!(
        queue_manifest.exists(),
        "queue manifest should be persisted"
    );

    let first_connection = profile.connect();
    assert!(first_connection.object_log_root.exists());
    assert!(first_connection.sqlite_projection_root.exists());
    let shard = shard_key(&queue);
    let projection = first_connection.projection(shard.clone());

    let pushed = first_connection
        .store
        .append_batch(&shard, Some(0), vec![push_envelope(&queue)])
        .await
        .unwrap();
    assert_eq!(pushed.last_position.sequence, 0);
    projection
        .insert_item(ITEM_ID, Some("local-object-log-group"), None, 1_718_000_000)
        .unwrap();
    projection.recompute_group_summary().unwrap();
    projection
        .apply_before_return(pushed.last_position.sequence)
        .unwrap();

    let claimed = first_connection
        .store
        .append_batch(&shard, Some(0), vec![claim_envelope(&queue)])
        .await
        .unwrap();
    assert_eq!(claimed.last_position.sequence, 1);

    let finalized = first_connection
        .store
        .append_batch(&shard, Some(0), vec![finalize_envelope(&queue)])
        .await
        .unwrap();
    assert_eq!(finalized.last_position.sequence, 2);
    projection
        .apply_before_return(finalized.last_position.sequence)
        .unwrap();

    let snapshot_path = first_connection.snapshot_path("local-object-log-smoke");
    std::fs::write(&snapshot_path, projection.snapshot_bytes().unwrap())
        .expect("projection snapshot should be writable");
    assert!(snapshot_path.exists(), "snapshot file should be persisted");
    drop(first_connection);

    let restarted_connection = profile.connect();
    let recovered = restarted_connection
        .store
        .read_from(&shard, None, 10)
        .await
        .unwrap();
    assert_eq!(recovered.commands.len(), 3);
    assert!(matches!(
        recovered.commands[0].1.command,
        QueueCommand::BatchPush(_)
    ));
    assert!(matches!(
        recovered.commands[1].1.command,
        QueueCommand::BatchClaim(_)
    ));
    assert!(matches!(
        recovered.commands[2].1.command,
        QueueCommand::BatchFinalize(_)
    ));

    let restored_snapshot = std::fs::read(snapshot_path).expect("snapshot should be readable");
    let restored_projection = SqliteProjection::restore_from_snapshot(shard, &restored_snapshot)
        .expect("projection should restore from snapshot");
    assert_eq!(restored_projection.applied_sequence().unwrap(), 2);
    let summary = restored_projection
        .group_summary(Some("local-object-log-group"))
        .unwrap()
        .unwrap();
    assert_eq!(summary.eligible_count, 1);
    assert_eq!(summary.oldest_eligible_at_ms, 1_718_000_000);
}
