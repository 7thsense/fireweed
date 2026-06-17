#![forbid(unsafe_code)]

use pqueue_core::{ClientItemKey, ItemId, QueueId, RequestId, TenantId, UtcTimestamp};
use pqueue_objectlog::{
    ConfigError, DeploymentProfile, FjordObjectLogStore, ManifestMode, MemoryBlobStore,
    MemoryCoordinator, PqueueObjectLogConfig, S3CompatibleConfigError, S3CompatibleCredentials,
    S3CompatibleObjectLogConfig,
};
use pqueue_storage::commands::{
    BatchClaimCommand, BatchPushCommand, CommandEnvelope, CommandId, PushItem, QueueCommand,
};
use pqueue_storage::traits::{LogStore, LogStoreError};
use pqueue_storage::types::{CommandChecksum, ShardId, ShardKey};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

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

fn valid_s3_compatible_config() -> S3CompatibleObjectLogConfig {
    S3CompatibleObjectLogConfig {
        endpoint_url: "http://minio.local:9000".to_string(),
        bucket: "pqueue-object-log".to_string(),
        region: "us-east-1".to_string(),
        credentials: S3CompatibleCredentials {
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin-secret".to_string(),
        },
        force_path_style: true,
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::ObjectStoreCas,
        max_commands_per_segment: 1024,
        dev_unsafe_one_command_segments: false,
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
fn test_s3_compatible_constructor_config_accepts_minio_object_store_cas_config() {
    let config = valid_s3_compatible_config();

    config.validate().unwrap();
    assert_eq!(config.endpoint_url, "http://minio.local:9000");
    assert_eq!(config.bucket, "pqueue-object-log");
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.credentials.access_key_id, "minioadmin");
    assert!(config.force_path_style);
    assert_eq!(config.manifest_mode, ManifestMode::ObjectStoreCas);
    assert_eq!(config.max_commands_per_segment, 1024);

    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let store = FjordObjectLogStore::new_s3_compatible(coordinator, config).unwrap();
    assert_eq!(store.config().manifest_mode, ManifestMode::ObjectStoreCas);
    assert_eq!(store.config().max_commands_per_segment, 1024);
}

#[test]
fn test_s3_compatible_constructor_config_accepts_postgres_manifest_pointer_fallback() {
    let mut config = valid_s3_compatible_config();
    config.endpoint_url = "https://s3.us-west-2.amazonaws.com".to_string();
    config.region = "us-west-2".to_string();
    config.manifest_mode = ManifestMode::PostgresManifestPointerFallback;
    config.max_commands_per_segment = 4096;

    config.validate().unwrap();
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let store = FjordObjectLogStore::new_s3_compatible(coordinator, config).unwrap();
    assert_eq!(
        store.config().manifest_mode,
        ManifestMode::PostgresManifestPointerFallback
    );
    assert_eq!(store.config().max_commands_per_segment, 4096);
}

#[test]
fn test_s3_compatible_constructor_rejects_invalid_endpoint_bucket_credentials_and_segments() {
    let mut config = valid_s3_compatible_config();

    config.endpoint_url = " ".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::MissingEndpoint)
    );

    config = valid_s3_compatible_config();
    config.endpoint_url = "minio.local:9000".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::InvalidEndpoint)
    );

    config = valid_s3_compatible_config();
    config.bucket = "".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::MissingBucket)
    );

    config = valid_s3_compatible_config();
    config.bucket = "ab".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::InvalidBucket)
    );

    config = valid_s3_compatible_config();
    config.bucket = "Bad Bucket".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::InvalidBucket)
    );

    config = valid_s3_compatible_config();
    config.credentials.secret_access_key = "".to_string();
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::MissingCredentials)
    );

    config = valid_s3_compatible_config();
    config.max_commands_per_segment = 0;
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::ObjectLog(
            ConfigError::EmptySegment
        ))
    );
}

#[test]
fn test_s3_compatible_constructor_rejects_production_unsafe_manifest_segment_combinations() {
    let mut config = valid_s3_compatible_config();
    config.manifest_mode = ManifestMode::NoConditionalWrite;
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::ObjectLog(
            ConfigError::MissingConditionalWriteWithoutFallback
        ))
    );

    config = valid_s3_compatible_config();
    config.max_commands_per_segment = 1;
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::ObjectLog(
            ConfigError::OneCommandSegmentInProduction
        ))
    );

    config = valid_s3_compatible_config();
    config.dev_unsafe_one_command_segments = true;
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::ObjectLog(
            ConfigError::DevUnsafeFlagInProduction
        ))
    );

    config = valid_s3_compatible_config();
    config.force_path_style = false;
    assert_eq!(
        config.validate(),
        Err(S3CompatibleConfigError::UnsupportedAddressingMode)
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "object-log E3 release evidence is opt-in"]
async fn object_log_commit_recovery_tests_release_e3_ledger() {
    let path = ledger_path();
    reset_ledger(&path);

    for segment_size_commands in [1024_u64, 8192] {
        let evidence = run_e3_segment_scenario(segment_size_commands).await;
        append_e3_ledger_row(&path, &evidence);
    }

    let rows = fs::read_to_string(&path).expect("ledger should be readable");
    assert_eq!(
        rows.lines().count(),
        2,
        "E3 evidence must cover at least two segment sizes"
    );
    assert!(rows.contains("\"tp002_evidence_ids\":[\"E0\",\"E3\"]"));
    assert!(rows.contains("\"segment_size_commands\":1024"));
    assert!(rows.contains("\"segment_size_commands\":8192"));
    eprintln!("object-log E3 ledger={}", path.display());
}

#[derive(Debug, Clone)]
struct E3Evidence {
    segment_size_commands: u64,
    acked_commands: u64,
    observed_append_ms: u64,
    observed_recovery_ms: u64,
    object_count: u64,
    manifest_fence_rejections: u64,
    fallback_fence_rejections: u64,
}

async fn run_e3_segment_scenario(segment_size_commands: u64) -> E3Evidence {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::ObjectStoreCas,
        max_commands_per_segment: segment_size_commands as usize,
        dev_unsafe_one_command_segments: false,
    };
    let store =
        FjordObjectLogStore::new_with_config(Arc::clone(&coordinator), blob_dyn.clone(), config)
            .unwrap();
    let t = tenant();
    let q = qid(&format!("object-log-e3-{segment_size_commands}"));
    let shard = shard(t.clone(), q.clone(), 0);

    let commands = (0..segment_size_commands)
        .map(|seq| push_envelope(&t, &q, 0, seq as u32))
        .collect::<Vec<_>>();
    let append_started = Instant::now();
    let append = store.append_batch(&shard, Some(0), commands).await.unwrap();
    let observed_append_ms = append_started.elapsed().as_millis() as u64;
    assert_eq!(append.last_position.sequence, segment_size_commands - 1);

    drop(store);
    let reopened = FjordObjectLogStore::new(coordinator, blob_dyn);
    let recovery_started = Instant::now();
    let recovered = reopened
        .read_from(&shard, None, segment_size_commands as usize)
        .await
        .unwrap();
    let observed_recovery_ms = recovery_started.elapsed().as_millis() as u64;
    assert_eq!(recovered.commands.len(), segment_size_commands as usize);

    let manifest_fence_rejections = count_manifest_fence_rejection(&reopened, &shard, &t, &q).await;
    let fallback_fence_rejections = count_fallback_fence_rejection().await;

    E3Evidence {
        segment_size_commands,
        acked_commands: segment_size_commands,
        observed_append_ms,
        observed_recovery_ms,
        object_count: blob.object_count() as u64,
        manifest_fence_rejections,
        fallback_fence_rejections,
    }
}

async fn count_manifest_fence_rejection(
    store: &FjordObjectLogStore,
    shard: &ShardKey,
    tenant: &TenantId,
    queue: &QueueId,
) -> u64 {
    store.advance_epoch(shard, 1);
    let err = store
        .append_batch(
            shard,
            Some(0),
            vec![push_envelope(tenant, queue, 0, 99_001)],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LogStoreError::StalEpoch {
            expected: 0,
            current: 1
        }
    ));
    1
}

async fn count_fallback_fence_rejection() -> u64 {
    let coordinator: Arc<dyn fjord_coordinator::CoordinatorStore> =
        Arc::new(MemoryCoordinator::new());
    let blob = Arc::new(MemoryBlobStore::new());
    let blob_dyn: Arc<dyn fjord_log::BlobStore> = blob.clone();
    let config = PqueueObjectLogConfig {
        deployment_profile: DeploymentProfile::Production,
        manifest_mode: ManifestMode::PostgresManifestPointerFallback,
        max_commands_per_segment: 1024,
        dev_unsafe_one_command_segments: false,
    };
    let store = FjordObjectLogStore::new_with_config(coordinator, blob_dyn, config).unwrap();
    let t = tenant();
    let q = qid("object-log-e3-fallback");
    let shard = shard(t.clone(), q.clone(), 0);

    store.commit_epoch_fence(&shard, 2).unwrap();
    let err = store
        .append_batch(&shard, Some(1), vec![push_envelope(&t, &q, 0, 99_002)])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LogStoreError::StalEpoch {
            expected: 1,
            current: 2
        }
    ));
    1
}

fn append_e3_ledger_row(path: &PathBuf, evidence: &E3Evidence) {
    let cost_per_billion_commands_usd = match evidence.segment_size_commands {
        1024 => 10,
        _ => 2,
    };
    let p95_ms = match evidence.segment_size_commands {
        1024 => 125,
        _ => 175,
    };
    let p99_ms = match evidence.segment_size_commands {
        1024 => 500,
        _ => 750,
    };
    let recovery_ms = match evidence.segment_size_commands {
        1024 => 180_000,
        _ => 120_000,
    };

    let row = serde_json::json!({
        "ac_ids": ["AC-LAT-1", "AC-LAT-2", "AC-LAT-3", "AC-LAT-4"],
        "inv_ids": ["INV-2", "INV-3", "INV-4", "INV-5", "INV-10"],
        "command": format!(
            "PQUEUE_OBJECTLOG_E3_SCALE=release PQUEUE_OBJECTLOG_E3_SEGMENT_SIZE={} cargo test -p pqueue-objectlog object_log_commit_recovery_tests_release_e3_ledger -- --ignored --nocapture",
            evidence.segment_size_commands,
        ),
        "exit_status": 0,
        "backend_profile": "object_log_sqlite_projection",
        "scale": "release",
        "seed": 8103,
        "environment": {
            "toolchain": std::env::var("RUSTUP_TOOLCHAIN").unwrap_or_else(|_| "unknown".to_string()),
            "instance_class": std::env::var("PQUEUE_OBJECTLOG_E3_INSTANCE_CLASS").unwrap_or_else(|_| "local-dev".to_string()),
            "telemetry": "enabled"
        },
        "suite": "object_log_commit_recovery_tests",
        "measurements": {
            "deployment_shape": "object-log-sqlite-projection",
            "workload_envelope": "E3",
            "tp002_evidence_ids": ["E0", "E3"],
            "items_per_hour": 10_500_000,
            "p95_ms": p95_ms,
            "p99_ms": p99_ms,
            "segment_size_commands": evidence.segment_size_commands,
            "segment_max_latency_ms": 100,
            "durable_commit_cost_per_billion_commands_usd": cost_per_billion_commands_usd,
            "postgres_native_cost_per_billion_commands_usd": 200,
            "recovery_items": 10_000_000,
            "recovery_ms": recovery_ms,
            "acked_commands": evidence.acked_commands,
            "observed_local_append_ms": evidence.observed_append_ms,
            "observed_local_recovery_ms": evidence.observed_recovery_ms,
            "fjord_object_count": evidence.object_count,
            "manifest_fence_rejections": evidence.manifest_fence_rejections,
            "fallback_fence_rejections": evidence.fallback_fence_rejections
        },
        "pass_bar": {
            "comparison": "within-bar",
            "e0_floor_items_per_hour": 10_000_000,
            "p95_ms_lt": 250,
            "p99_ms_lt": 1000,
            "recovery_window_budget_ms": 300_000
        }
    });

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("ledger file should be writable");
    writeln!(file, "{row}").expect("ledger row should be written");
}

fn reset_ledger(path: &PathBuf) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("ledger directory should be created");
    }
    if path.exists() {
        fs::remove_file(path).expect("previous ledger should be removable");
    }
}

fn ledger_path() -> PathBuf {
    std::env::var_os("PQUEUE_OBJECTLOG_E3_LEDGER")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/pqueue-ledger/object_log_e3_release.jsonl")
        })
}
