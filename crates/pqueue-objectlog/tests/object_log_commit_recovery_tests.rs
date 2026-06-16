#![forbid(unsafe_code)]

use pqueue_core::{ClientItemKey, ItemId, QueueId, RequestId, TenantId, UtcTimestamp};
use pqueue_objectlog::{
    ConfigError, DeploymentProfile, FjordObjectLogStore, ManifestMode, MemoryBlobStore,
    MemoryCoordinator, PqueueObjectLogConfig,
};
use pqueue_storage::commands::{
    BatchClaimCommand, BatchPushCommand, CommandEnvelope, CommandId, PushItem, QueueCommand,
};
use pqueue_storage::traits::{LogStore, LogStoreError};
use pqueue_storage::types::{CommandChecksum, ShardId, ShardKey};
use std::sync::Arc;

fn tenant() -> TenantId {
    TenantId::new("test-tenant").unwrap()
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

fn shard(tenant: TenantId, queue: QueueId, shard_id: u32) -> ShardKey {
    ShardKey {
        tenant_id: tenant,
        queue_id: queue,
        shard_id: ShardId::new(shard_id),
    }
}

fn push_envelope(t: &TenantId, q: &QueueId, shard_id: u32, seq: u32) -> CommandEnvelope {
    let item_id = iid(&format!("item-{seq}"));
    CommandEnvelope {
        command_id: CommandId::new(format!("cmd-push-{seq}")),
        request_id: None,
        tenant_id: t.clone(),
        queue_id: q.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: vec![item_id.clone()],
        command: QueueCommand::BatchPush(BatchPushCommand {
            items: vec![PushItem {
                item_id,
                client_item_key: cik(&format!("key-{seq}")),
                priority: None,
                not_before: None,
                max_attempts: 3,
                payload: None,
            }],
        }),
        checksum: CommandChecksum(seq),
        created_at: ts(seq as i64),
    }
}

fn claim_envelope(t: &TenantId, q: &QueueId, shard_id: u32, seq: u32) -> CommandEnvelope {
    CommandEnvelope {
        command_id: CommandId::new(format!("cmd-claim-{seq}")),
        request_id: Some(RequestId::new(format!("req-claim-{seq}")).unwrap()),
        tenant_id: t.clone(),
        queue_id: q.clone(),
        shard_id: ShardId::new(shard_id),
        item_ids: vec![iid(&format!("item-{seq}"))],
        command: QueueCommand::BatchClaim(BatchClaimCommand {
            item_ids: vec![iid(&format!("item-{seq}"))],
            lease_token: format!("lease-{seq}"),
            lease_expires_at: ts(100 + seq as i64),
        }),
        checksum: CommandChecksum(10 + seq),
        created_at: ts(seq as i64),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_group_commit_uses_fjord_blob_once() {
    let (store, blob) = FjordObjectLogStore::new_memory();
    let t = tenant();
    let q = qid("object-log-group");
    let shard = shard(t.clone(), q.clone(), 0);

    let result = store
        .append_batch(
            &shard,
            Some(0),
            vec![
                push_envelope(&t, &q, 0, 0),
                push_envelope(&t, &q, 0, 1),
                claim_envelope(&t, &q, 0, 1),
            ],
        )
        .await
        .unwrap();

    assert_eq!(result.last_position.sequence, 2);
    assert_eq!(
        blob.object_count(),
        1,
        "fjord-log groups one flush into one object"
    );
    let page = store.read_from(&shard, None, 10).await.unwrap();
    assert_eq!(page.commands.len(), 3);
    assert_eq!(page.commands[0].0.sequence, 0);
    assert_eq!(page.commands[2].0.sequence, 2);
    assert!(matches!(
        page.commands[2].1.command,
        QueueCommand::BatchClaim(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_reopens_from_fjord_coordinator_and_blob() {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let first = FjordObjectLogStore::new(Arc::clone(&coordinator), Arc::clone(&blob_dyn));
    let t = tenant();
    let q = qid("object-log-recovery");
    let shard = shard(t.clone(), q.clone(), 0);

    first
        .append_batch(
            &shard,
            Some(0),
            vec![push_envelope(&t, &q, 0, 0), push_envelope(&t, &q, 0, 1)],
        )
        .await
        .unwrap();
    drop(first);

    let reopened = FjordObjectLogStore::new(coordinator, blob_dyn);
    let page = reopened.read_from(&shard, None, 10).await.unwrap();
    assert_eq!(
        page.commands
            .iter()
            .map(|(_, envelope)| envelope.command_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["cmd-push-0", "cmd-push-1"]
    );
    assert_eq!(blob.object_count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_current_epoch_fences_stale_writers() {
    let (store, _blob) = FjordObjectLogStore::new_memory();
    let t = tenant();
    let q = qid("object-log-epoch");
    let shard = shard(t.clone(), q.clone(), 0);

    store
        .append_batch(&shard, Some(0), vec![push_envelope(&t, &q, 0, 0)])
        .await
        .unwrap();
    store.advance_epoch(&shard, 1);

    let stale = store
        .append_batch(&shard, Some(0), vec![push_envelope(&t, &q, 0, 1)])
        .await
        .unwrap_err();
    assert_eq!(
        stale,
        LogStoreError::StalEpoch {
            expected: 0,
            current: 1
        }
    );

    let current = store
        .append_batch(&shard, Some(1), vec![push_envelope(&t, &q, 0, 2)])
        .await
        .unwrap();
    assert_eq!(current.last_position.backend_epoch, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_epoch_fence_survives_reopen_before_data_commit() {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let first = FjordObjectLogStore::new(Arc::clone(&coordinator), Arc::clone(&blob_dyn));
    let t = tenant();
    let q = qid("object-log-reopen-fence");
    let shard = shard(t.clone(), q.clone(), 0);

    first.commit_epoch_fence(&shard, 1).unwrap();
    drop(first);
    let reopened = FjordObjectLogStore::new(coordinator, blob_dyn);
    let object_count_after_fence = blob.object_count();

    let stale = reopened
        .append_batch(&shard, Some(0), vec![push_envelope(&t, &q, 0, 0)])
        .await
        .unwrap_err();
    assert_eq!(
        stale,
        LogStoreError::StalEpoch {
            expected: 0,
            current: 1
        }
    );
    assert_eq!(
        blob.object_count(),
        object_count_after_fence,
        "stale writer must not append a data object after epoch handoff"
    );

    let current = reopened
        .append_batch(&shard, Some(1), vec![push_envelope(&t, &q, 0, 1)])
        .await
        .unwrap();
    assert_eq!(current.last_position.backend_epoch, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_request_id_replay_finds_committed_command_after_reopen() {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let first = FjordObjectLogStore::new(Arc::clone(&coordinator), Arc::clone(&blob_dyn));
    let t = tenant();
    let q = qid("object-log-request-replay");
    let shard = shard(t.clone(), q.clone(), 0);

    first
        .append_batch(&shard, Some(0), vec![claim_envelope(&t, &q, 0, 7)])
        .await
        .unwrap();
    drop(first);

    let reopened = FjordObjectLogStore::new(coordinator, blob_dyn);
    let request_id = RequestId::new("req-claim-7").unwrap();
    let replayed = reopened
        .find_by_request_id(&shard, &request_id)
        .unwrap()
        .expect("committed request_id should be replayable");

    assert_eq!(replayed.0.sequence, 0);
    assert_eq!(replayed.1.request_id.as_ref(), Some(&request_id));
    assert!(matches!(replayed.1.command, QueueCommand::BatchClaim(_)));
}

#[test]
fn object_log_commit_recovery_tests_rejects_production_one_object_per_command_config() {
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::ObjectStoreCas,
        max_commands_per_segment: 1,
        dev_unsafe_one_command_segments: false,
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::OneCommandSegmentInProduction)
    );
}

#[test]
fn object_log_commit_recovery_tests_rejects_dev_unsafe_segment_flag_in_production() {
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::ObjectStoreCas,
        max_commands_per_segment: 16,
        dev_unsafe_one_command_segments: true,
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::DevUnsafeFlagInProduction)
    );
}

#[test]
fn object_log_commit_recovery_tests_rejects_missing_cas_without_fallback() {
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::NoConditionalWrite,
        max_commands_per_segment: 16,
        dev_unsafe_one_command_segments: false,
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::MissingConditionalWriteWithoutFallback)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_log_commit_recovery_tests_postgres_manifest_pointer_fallback_keeps_epoch_fence() {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::PostgresManifestPointerFallback,
        max_commands_per_segment: 16,
        dev_unsafe_one_command_segments: false,
    };
    let store = FjordObjectLogStore::new_with_config(coordinator, blob_dyn, config).unwrap();
    let t = tenant();
    let q = qid("object-log-fallback");
    let shard = shard(t.clone(), q.clone(), 0);

    assert_eq!(
        store.config().manifest_mode,
        ManifestMode::PostgresManifestPointerFallback
    );
    store.commit_epoch_fence(&shard, 2).unwrap();
    let stale = store
        .append_batch(&shard, Some(1), vec![push_envelope(&t, &q, 0, 0)])
        .await
        .unwrap_err();
    assert_eq!(
        stale,
        LogStoreError::StalEpoch {
            expected: 1,
            current: 2
        }
    );
    store
        .append_batch(&shard, Some(2), vec![push_envelope(&t, &q, 0, 1)])
        .await
        .unwrap();
}
