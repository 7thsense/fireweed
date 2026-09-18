//! Provider-neutral shared API-005 external public-interface suite.
//! Cell IDs use manifest `log--projection[--variant]` form. Provider brand
//! strings are forbidden in fixtures; live S3 provenance claims are owned by P4s.

#[path = "support/public_interface.rs"]
mod public_interface;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed::{
    BatchUpdateEntry, BatchUpdateItemRef, BatchUpdateRequest, BatchUpdateValue, ClientItemKey,
    ConfigSecret, ControlPlaneConfig, DiscoveryGranularity, EligibilityPolicy, EngineError,
    Fireweed, GateKeyPolicy, ItemMutationOperation, ItemMutationRequest, ItemMutationResponse,
    ItemMutationReturning, ItemPatch, ItemPredicate, ItemSelector, ItemSelectorScope, LeaseGuard,
    NewItem, ObjectLogAuthority, ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, OwnerId,
    PostgresCoordinationConfig, PostgresMode, PostgresRuntimeConfig, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig,
    QueueDefinition, QueueId, QueueKey, RecoveryAction, RecoveryPolicy, RecurrencePolicy,
    RequestId, ResponseBarrier, RetryPolicy, SegmentConfig, SelectedMutation, SystemClock,
    TenantId,
};
use fireweed_objectlog::segmented::{BlobStore, S3BlobStore};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for external conformance"))
}

fn redacted_error(error: impl std::fmt::Display, secrets: &[&str]) -> String {
    secrets.iter().fold(error.to_string(), |message, secret| {
        if secret.is_empty() {
            message
        } else {
            message.replace(secret, "[redacted]")
        }
    })
}

fn unique_name(label: &str) -> String {
    let ordinal = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let label = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .take(20)
        .collect::<String>();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let digest =
        Sha256::digest(format!("{label}:{}:{nanos}:{ordinal}", std::process::id()).as_bytes());
    format!("fw_{label}_{}", hex(&digest[..8]))
}

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(unique_name(label));
        std::fs::create_dir_all(&path).expect("create external conformance fixture root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct PostgresSchema {
    url: String,
    name: String,
    active: bool,
}

impl PostgresSchema {
    fn new(url: String, name: String) -> Self {
        let create_url = url.clone();
        let create_name = name.clone();
        std::thread::spawn(move || {
            let mut client = Client::connect(&create_url, NoTls).map_err(|_| ())?;
            client
                .batch_execute(&format!("CREATE SCHEMA \"{create_name}\""))
                .map_err(|_| ())
        })
        .join()
        .unwrap_or(Err(()))
        .unwrap_or_else(|_| panic!("failed to create isolated PostgreSQL test schema"));
        Self {
            url,
            name,
            active: true,
        }
    }

    fn cleanup(&mut self) -> Result<(), ()> {
        if !self.active {
            return Ok(());
        }
        let url = self.url.clone();
        let name = self.name.clone();
        let cleaned = std::thread::spawn(move || {
            let mut client = Client::connect(&url, NoTls).map_err(|_| ())?;
            client
                .batch_execute(&format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"))
                .map_err(|_| ())
        })
        .join()
        .map_err(|_| ())?;
        cleaned?;
        self.active = false;
        Ok(())
    }
}

impl Drop for PostgresSchema {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

struct S3Namespace {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    prefix: String,
    active: bool,
}

impl S3Namespace {
    fn new(config: &S3Config, namespace: &str) -> Self {
        Self {
            endpoint: config.s3_endpoint.clone(),
            bucket: config.s3_bucket.clone(),
            region: config.s3_region.clone(),
            access_key: config.s3_access_key.clone(),
            secret_key: config.s3_secret_key.clone(),
            prefix: format!("{}/", hex(namespace.as_bytes())),
            active: true,
        }
    }

    fn cleanup(&mut self) -> Result<(), ()> {
        if !self.active {
            return Ok(());
        }
        let endpoint = self.endpoint.clone();
        let bucket = self.bucket.clone();
        let region = self.region.clone();
        let access_key = self.access_key.clone();
        let secret_key = self.secret_key.clone();
        let prefix = self.prefix.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| ())?;
            runtime.block_on(async move {
                let store = S3BlobStore::new(&endpoint, &region, &bucket, &access_key, &secret_key);
                let keys = store.list(&prefix).await.map_err(|_| ())?;
                if keys.iter().any(|key| !key.starts_with(&prefix)) {
                    return Err(());
                }
                for key in keys {
                    store.delete(&key).await.map_err(|_| ())?;
                }
                if !store.list(&prefix).await.map_err(|_| ())?.is_empty() {
                    return Err(());
                }
                Ok(())
            })
        })
        .join()
        .map_err(|_| ())??;
        self.active = false;
        Ok(())
    }
}

impl Drop for S3Namespace {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

struct S3Config {
    s3_endpoint: String,
    s3_bucket: String,
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
}

impl S3Config {
    fn load() -> Self {
        Self {
            s3_endpoint: required_env("FIREWEED_S3_TEST_ENDPOINT"),
            s3_bucket: required_env("FIREWEED_S3_TEST_BUCKET"),
            s3_region: required_env("FIREWEED_S3_TEST_REGION"),
            s3_access_key: required_env("FIREWEED_S3_TEST_ACCESS_KEY"),
            s3_secret_key: required_env("FIREWEED_S3_TEST_SECRET_KEY"),
        }
    }
}

fn postgres_config(
    url: &str,
    schema: &str,
    mode: PostgresMode,
    coordination: Option<PostgresCoordinationConfig>,
) -> PostgresRuntimeConfig {
    PostgresRuntimeConfig {
        url: ConfigSecret::new(url),
        schema: Some(schema.into()),
        mode,
        node_id: None,
        coordination,
        claim_pool_size: 0,
    }
}

fn schema_url(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

fn objectlog_config(
    object_log: ObjectLogStorage,
    authority: ObjectLogAuthority,
    projection: ProjectionConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log,
        authority,
        projection,
        response_barrier: barrier,
        segments: SegmentConfig::new(262_144, 20).expect("valid segment configuration"),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
    }
}

fn s3_storage(config: &S3Config) -> ObjectLogStorage {
    ObjectLogStorage::S3Compatible {
        endpoint: config.s3_endpoint.clone(),
        bucket: config.s3_bucket.clone(),
        region: config.s3_region.clone(),
        access_key_id: ConfigSecret::new(config.s3_access_key.clone()),
        secret_access_key: ConfigSecret::new(config.s3_secret_key.clone()),
        allow_insecure_http: config.s3_endpoint.starts_with("http://"),
    }
}

fn derived_postgres_schema(namespace: &str) -> String {
    let digest = Sha256::digest(namespace.as_bytes());
    format!("fw_{}", hex(&digest[..30]))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct ReopenProbe {
    queue: QueueKey,
    definition: QueueDefinition,
    item_id: fireweed::ItemId,
    batch: BatchUpdateRequest,
    batch_response: fireweed::BatchUpdateResponse,
    mutation: (ItemMutationRequest, ItemMutationResponse),
}

fn reopen_definition(cell: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("external-durability").unwrap(),
        queue_id: QueueId::new(format!("reopen-{cell}")).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy {
            metadata_blockers: Default::default(),
            gate_keys: GateKeyPolicy::Dynamic,
            max_gate_keys_per_item: Some(4),
            max_gates_per_request: Some(4),
        },
        cohort_policy: None,
        recurrence: RecurrencePolicy::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 3_600_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn reopen_item(payload: &'static [u8]) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new("reopen-primary").unwrap()),
        priority: Some(PriorityValue::Int64(7)),
        payload: Some(payload.into()),
        gate_keys: vec!["reopen-hold".into()],
        ..NewItem::default()
    }
}

async fn seed_reopen_probe(cell: &str, fireweed: &Fireweed) -> ReopenProbe {
    let definition = reopen_definition(cell);
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    assert!(
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );
    let request_id = RequestId::new("reopen-push-v1").unwrap();
    let (item_id, first_disp) = fireweed
        .push_with_request_id(&queue, request_id.clone(), reopen_item(b"before"))
        .await
        .unwrap();
    assert_eq!(first_disp, fireweed::PushDisposition::Fresh);
    let (replayed_id, replay_disp) = fireweed
        .push_with_request_id(&queue, request_id, reopen_item(b"before"))
        .await
        .unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed_id, item_id);
    let batch = BatchUpdateRequest {
        request_id: RequestId::new("reopen-batch-v1").unwrap(),
        updates: vec![BatchUpdateEntry {
            item_ref: BatchUpdateItemRef::Both {
                item_id,
                client_item_key: ClientItemKey::new("reopen-primary").unwrap(),
            },
            expected_item_version: None,
            priority: BatchUpdateValue::Replace(PriorityValue::Int64(3)),
            not_before: BatchUpdateValue::Keep,
            payload: BatchUpdateValue::Replace(Some(b"after".as_slice().into())),
            metadata: BatchUpdateValue::Keep,
            gate_keys: BatchUpdateValue::Keep,
            fields: BatchUpdateValue::Keep,
        }],
    };
    let batch_response = fireweed.batch_update(&queue, batch.clone()).await.unwrap();
    let mutation = ItemMutationRequest {
        request_id: RequestId::new("reopen-mutation-v1").unwrap(),
        // The mutation timestamp also anchors receipt retention. Use the same
        // clock as this fixture's push/batch calls so cleanup cannot see a
        // fictitious future and expire their receipts before reopen.
        evaluated_at: fireweed::Clock::now(&SystemClock),
        dry_run: false,
        returning: ItemMutationReturning::BeforeSnapshot,
        gate_changes: vec![],
        operation: ItemMutationOperation::SelectFirst {
            clauses: vec![
                SelectedMutation {
                    selector_id: "pre-mutation-state".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![
                            ItemPredicate::ClientItemKeyEq(
                                ClientItemKey::new("reopen-primary").unwrap(),
                            ),
                            ItemPredicate::FieldEq {
                                name: "mutation-proof".into(),
                                value: None,
                            },
                        ],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(2))),
                        field_edits: std::collections::BTreeMap::from([(
                            "mutation-proof".into(),
                            Some(bytes::Bytes::from_static(b"durable")),
                        )]),
                        ..ItemPatch::default()
                    },
                },
                SelectedMutation {
                    selector_id: "must-not-run-on-replay".into(),
                    selector: ItemSelector {
                        scope: ItemSelectorScope::Live,
                        predicates: vec![ItemPredicate::ClientItemKeyEq(
                            ClientItemKey::new("reopen-primary").unwrap(),
                        )],
                    },
                    predicates: vec![],
                    lease_guard: LeaseGuard::RejectActive,
                    patch: ItemPatch {
                        priority: BatchUpdateValue::Replace(Some(PriorityValue::Int64(99))),
                        ..ItemPatch::default()
                    },
                },
            ],
        },
    };
    let before_preview = fireweed
        .live_item(&queue, ClientItemKey::new("reopen-primary").unwrap())
        .await
        .unwrap()
        .unwrap();
    let mut preview_request = mutation.clone();
    preview_request.dry_run = true;
    let preview = fireweed
        .mutate_items(&queue, preview_request)
        .await
        .unwrap();
    assert!(preview.position.is_none());
    assert_eq!(preview.summary.changed, 1);
    let after_preview = fireweed
        .live_item(&queue, ClientItemKey::new("reopen-primary").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_preview.item_version, before_preview.item_version);
    assert_eq!(after_preview.priority, before_preview.priority);
    assert!(!after_preview.fields.contains_key("mutation-proof"));
    let mutation_response = fireweed
        .mutate_items(&queue, mutation.clone())
        .await
        .unwrap();
    assert_eq!(mutation_response.summary.changed, 1);
    assert_eq!(
        mutation_response.results[0].selector_id.as_deref(),
        Some("pre-mutation-state")
    );
    let after_mutation = fireweed.current_position(&queue).await.unwrap();
    assert_eq!(
        fireweed
            .push_with_request_id(
                &queue,
                RequestId::new("reopen-push-v1").unwrap(),
                reopen_item(b"before"),
            )
            .await
            .unwrap(),
        (item_id, fireweed::PushDisposition::Replayed),
        "mutation must retain the preceding push receipt"
    );
    assert_eq!(
        fireweed.batch_update(&queue, batch.clone()).await.unwrap(),
        batch_response,
        "mutation must retain the preceding batch receipt"
    );
    assert_eq!(
        fireweed.current_position(&queue).await.unwrap(),
        after_mutation
    );
    fireweed
        .push(
            &queue,
            NewItem {
                client_item_key: Some(ClientItemKey::new("reopen-witness").unwrap()),
                priority: Some(PriorityValue::Int64(9)),
                ..NewItem::default()
            },
        )
        .await
        .unwrap();
    fireweed
        .set_gates(&queue, vec!["reopen-hold".into()], true)
        .await
        .unwrap();
    let visible = fireweed.peek(&queue, 1).await.unwrap();
    assert_eq!(
        visible
            .iter()
            .map(|item| item.client_item_key.as_str())
            .collect::<Vec<_>>(),
        vec!["reopen-witness"],
        "the blocked priority head must not consume the peek limit before reopen"
    );
    ReopenProbe {
        queue,
        definition,
        item_id,
        batch,
        batch_response,
        mutation: (mutation, mutation_response),
    }
}

async fn verify_reopen_probe(fireweed: &Fireweed, probe: ReopenProbe) {
    let before_replays = fireweed.current_position(&probe.queue).await.unwrap();
    assert_eq!(
        fireweed.queue_definition(&probe.queue).await.unwrap(),
        probe.definition
    );
    let (replayed_id, replay_disp) = fireweed
        .push_with_request_id(
            &probe.queue,
            RequestId::new("reopen-push-v1").unwrap(),
            reopen_item(b"before"),
        )
        .await
        .unwrap();
    assert_eq!(replay_disp, fireweed::PushDisposition::Replayed);
    assert_eq!(replayed_id, probe.item_id);
    assert_eq!(
        fireweed
            .batch_update(&probe.queue, probe.batch.clone())
            .await
            .unwrap(),
        probe.batch_response
    );
    let mut conflicting_batch = probe.batch;
    conflicting_batch.updates[0].priority = BatchUpdateValue::Replace(PriorityValue::Int64(99));
    assert_eq!(
        fireweed
            .batch_update(&probe.queue, conflicting_batch)
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "changed batch-update body must conflict after close/reopen"
    );
    let (mutation, mutation_response) = probe.mutation;
    assert_eq!(
        fireweed
            .mutate_items(&probe.queue, mutation.clone())
            .await
            .unwrap(),
        mutation_response,
        "item mutation response must replay exactly after close/reopen"
    );
    let mut conflicting_mutation = mutation;
    let ItemMutationOperation::SelectFirst { clauses } = &mut conflicting_mutation.operation else {
        unreachable!("reopen mutation uses selectors")
    };
    clauses[0].patch.priority = BatchUpdateValue::Replace(Some(PriorityValue::Int64(1)));
    assert_eq!(
        fireweed
            .mutate_items(&probe.queue, conflicting_mutation)
            .await
            .unwrap_err(),
        EngineError::RequestIdConflict,
        "changed mutation body must conflict after close/reopen"
    );
    assert_eq!(
        fireweed.current_position(&probe.queue).await.unwrap(),
        before_replays,
        "receipt replay and body conflicts must not append after reopen"
    );
    let item = fireweed
        .live_item(&probe.queue, ClientItemKey::new("reopen-primary").unwrap())
        .await
        .unwrap()
        .expect("primary item survives close/reopen");
    assert_eq!(item.item_id, probe.item_id);
    assert_eq!(item.priority, Some(PriorityValue::Int64(2)));
    assert_eq!(item.payload.as_deref(), Some(b"after".as_slice()));
    assert_eq!(
        item.fields.get("mutation-proof").map(bytes::Bytes::as_ref),
        Some(b"durable".as_slice())
    );
    let visible = fireweed.peek(&probe.queue, 1).await.unwrap();
    assert_eq!(
        visible
            .iter()
            .map(|item| item.client_item_key.as_str())
            .collect::<Vec<_>>(),
        vec!["reopen-witness"],
        "blocked gate survives close/reopen and does not consume the peek limit"
    );
    assert!(
        !fireweed
            .discover_active_scopes(&probe.queue, DiscoveryGranularity::Queue)
            .await
            .unwrap()
            .is_empty(),
        "active-scope discovery survives close/reopen"
    );
    fireweed
        .set_gates(&probe.queue, vec!["reopen-hold".into()], false)
        .await
        .unwrap();
    assert!(
        fireweed
            .peek(&probe.queue, 10)
            .await
            .unwrap()
            .iter()
            .any(|item| item.item_id == probe.item_id),
        "unblocked item becomes visible after close/reopen"
    );
}

async fn run_postgres_runtime(cell: &str, mode: PostgresMode, coordinated: bool) {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name(cell);
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let coordination = coordinated.then(|| PostgresCoordinationConfig {
        instance_id: OwnerId::new(unique_name("owner")).expect("valid unique owner id"),
        control_plane: ControlPlaneConfig::default(),
    });
    let runtime = postgres_config(&postgres_url, &schema_name, mode, coordination);
    let fireweed = fireweed::open_postgres_runtime_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open {cell}: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run(cell, &fireweed, false).await;
    let probe = seed_reopen_probe(cell, &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_postgres_runtime_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen {cell}: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean PostgreSQL schema for {cell}"));
}

fn run_sync_constructor(cell: &str, open: impl Fn() -> Fireweed) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build sync-constructor test runtime");
    let fireweed = open();
    runtime.block_on(public_interface::run(cell, &fireweed, false));
    let probe = runtime.block_on(seed_reopen_probe(cell, &fireweed));
    drop(fireweed);
    let reopened = open();
    runtime.block_on(verify_reopen_probe(&reopened, probe));
    drop(reopened);
}

#[test]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
fn postgres_convenience_sync_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_convenience_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    run_sync_constructor("postgres--memory--convenience-sync", || {
        fireweed::open_postgres(&isolated_url, Arc::new(SystemClock)).unwrap_or_else(|error| {
            panic!(
                "failed to open postgres-convenience-sync: {}",
                redacted_error(error, &[&postgres_url])
            )
        })
    });
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres-convenience-sync schema"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_convenience_async_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_convenience_async");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    let fireweed = fireweed::open_postgres_async(&isolated_url, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open postgres-convenience-async: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("postgres--memory--convenience-async", &fireweed, false).await;
    let probe = seed_reopen_probe("postgres--memory--convenience-async", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_postgres_async(&isolated_url, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen postgres-convenience-async: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres-convenience-async schema"));
}

#[test]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
fn postgres_coordinated_constructor_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_coordinated_constructor");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    let owner_id = OwnerId::new(unique_name("coordinated_owner")).expect("valid unique owner id");
    run_sync_constructor("postgres--memory--coordinated-constructor", || {
        fireweed::open_postgres_coordinated(
            &isolated_url,
            Arc::new(SystemClock),
            owner_id.clone(),
            ControlPlaneConfig::default(),
        )
        .unwrap_or_else(|error| {
            panic!(
                "failed to open postgres-coordinated-constructor: {}",
                redacted_error(error, &[&postgres_url])
            )
        })
    });
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres-coordinated-constructor schema"));
}

#[test]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
fn postgres_runtime_sync_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_runtime_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let runtime = postgres_config(&postgres_url, &schema_name, PostgresMode::LogReplay, None);
    run_sync_constructor("postgres--memory--runtime-sync", || {
        fireweed::open_postgres_runtime(runtime.clone(), Arc::new(SystemClock)).unwrap_or_else(
            |error| {
                panic!(
                    "failed to open postgres-runtime-sync: {}",
                    redacted_error(error, &[&postgres_url])
                )
            },
        )
    });
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres-runtime-sync schema"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_relational_coordinated_node_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_relational_coordinated_node");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let runtime = PostgresRuntimeConfig {
        url: ConfigSecret::new(postgres_url.clone()),
        schema: Some(schema_name),
        mode: PostgresMode::Relational,
        node_id: Some(7),
        coordination: Some(PostgresCoordinationConfig {
            instance_id: OwnerId::new(unique_name("relational_owner"))
                .expect("valid unique owner id"),
            control_plane: ControlPlaneConfig::default(),
        }),
        claim_pool_size: 0,
    };
    let fireweed = fireweed::open_postgres_runtime_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open postgres-relational-coordinated-node: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run(
        "postgres--postgres--relational-coordinated",
        &fireweed,
        false,
    )
    .await;
    let probe = seed_reopen_probe("postgres--postgres--relational-coordinated", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_postgres_runtime_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen postgres-relational-coordinated-node: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres-relational-coordinated-node schema"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_log_replay_public_interface() {
    run_postgres_runtime(
        "postgres--memory--log-replay",
        PostgresMode::LogReplay,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_relational_public_interface() {
    run_postgres_runtime(
        "postgres--postgres--relational",
        PostgresMode::Relational,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_coordinated_public_interface() {
    run_postgres_runtime(
        "postgres--memory--coordinated",
        PostgresMode::LogReplay,
        true,
    )
    .await;
}

/// P7N non-S3 cell: memory log × postgres projection (Class B).
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn memory_postgres_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("memory_postgres");
    let mut schema = PostgresSchema::new(
        postgres_url.clone(),
        derived_postgres_schema(&format!("memory_pg_{schema_name}")),
    );
    let mut cfg = fireweed::StorageConfig::memory();
    cfg.projection = fireweed::ProjectionStoreConfig::Postgres {
        url: ConfigSecret::new(postgres_url.clone()),
    };
    cfg.namespace = schema_name.clone();
    let fireweed = fireweed::open_async(cfg.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open memory--postgres: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("memory--postgres", &fireweed, false).await;
    postgres_projection_unique_conflicts_do_not_append(&fireweed).await;
    // An in-memory authoritative log has no process-restart durability contract.
    drop(fireweed);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean memory--postgres schema"));
}

async fn postgres_projection_unique_conflicts_do_not_append(fireweed: &Fireweed) {
    let mut definition = reopen_definition("preappend-index-validation");
    definition.typed_indexes = vec![fireweed::QueueIndex {
        name: "by_email".into(),
        declaration: fireweed::IndexDeclaration::Single(fireweed::IndexDef {
            field: "email".into(),
            index_type: fireweed::IndexType::String,
            unique: true,
        }),
    }];
    definition.secondary_indexes = vec![fireweed::IndexSpec {
        name: "by_external_id".into(),
        fields: vec!["external_id".into()],
        unique: true,
    }];
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    fireweed.create_queue(definition).await.unwrap();
    let item = |key: &str, email: &str, external: &str| NewItem {
        client_item_key: Some(ClientItemKey::new(key).unwrap()),
        entity: Some(serde_json::json!({"email": email})),
        fields: [(
            "external_id".into(),
            bytes::Bytes::copy_from_slice(external.as_bytes()),
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    fireweed
        .push_batch(
            &queue,
            vec![
                item("first", "a@example.com", "A"),
                item("second", "b@example.com", "B"),
            ],
        )
        .await
        .unwrap();
    let before = fireweed.current_position(&queue).await.unwrap();
    for duplicate in [
        item("typed-conflict", "a@example.com", "fresh"),
        item("legacy-conflict", "fresh@example.com", "A"),
    ] {
        assert_eq!(
            fireweed.push(&queue, duplicate).await.unwrap_err(),
            EngineError::Conflict
        );
        assert_eq!(fireweed.current_position(&queue).await.unwrap(), before);
    }
    assert_eq!(
        fireweed
            .push_batch(
                &queue,
                vec![
                    item("sibling-a", "c@example.com", "C"),
                    item("sibling-b", "c@example.com", "D")
                ]
            )
            .await
            .unwrap_err(),
        EngineError::Conflict
    );
    assert_eq!(fireweed.current_position(&queue).await.unwrap(), before);
    for key in ["new-upsert", "first"] {
        assert_eq!(
            fireweed
                .upsert(
                    &queue,
                    ClientItemKey::new(key).unwrap(),
                    item(key, "b@example.com", "unused")
                )
                .await
                .unwrap_err(),
            EngineError::Conflict
        );
        assert_eq!(fireweed.current_position(&queue).await.unwrap(), before);
    }
    // The rejected commands must not poison the queue, and replacing a row may
    // reuse that row's own keys without treating it as an outside holder.
    fireweed
        .upsert(
            &queue,
            ClientItemKey::new("first").unwrap(),
            item("first", "a@example.com", "A"),
        )
        .await
        .unwrap();
    assert_eq!(fireweed.metrics(&queue).await.unwrap().pending, 2);
}

/// P7N non-S3 cell: postgres log × Turso projection.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn postgres_turso_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let root = FixtureRoot::new("postgres_turso");
    let schema_name = unique_name("postgres_turso");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    let mut cfg = fireweed::StorageConfig::memory();
    cfg.log = fireweed::LogConfig::Postgres {
        url: ConfigSecret::new(isolated_url.clone()),
        schema: Some(schema_name.clone()),
        mode: PostgresMode::LogReplay,
        node_id: None,
        coordination: None,
    };
    cfg.projection = fireweed::ProjectionStoreConfig::Turso {
        path: root.path().join("projection.db"),
    };
    cfg.namespace = schema_name.clone();
    let fireweed = fireweed::open_async(cfg.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open postgres--turso: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("postgres--turso", &fireweed, false).await;
    let probe = seed_reopen_probe("postgres--turso", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_async(cfg, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen postgres--turso: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean postgres--turso schema"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
async fn filesystem_postgres_strict_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let root = FixtureRoot::new("filesystem_postgres");
    let namespace = unique_name("filesystem_postgres");
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        ObjectLogStorage::Local {
            root: root.path().join("object-log"),
        },
        ObjectLogAuthority::NativeConditionalWrite,
        ProjectionConfig::Postgres {
            url: ConfigSecret::new(postgres_url.clone()),
        },
        ResponseBarrier::Strict,
        namespace,
    );
    let fireweed = fireweed::open_objectlog_postgres_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|_| panic!("failed to open filesystem--postgres--strict"));
    public_interface::run("filesystem--postgres--strict", &fireweed, true).await;
    let probe = seed_reopen_probe("filesystem--postgres--strict", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_objectlog_postgres_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen filesystem--postgres--strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema.cleanup().unwrap_or_else(|_| {
        panic!("failed to clean PostgreSQL schema for filesystem--postgres--strict")
    });
}

#[test]
#[ignore = "requires live PostgreSQL; P10 executes the full external matrix"]
fn filesystem_postgres_sync_constructor_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let root = FixtureRoot::new("filesystem_postgres_sync");
    let namespace = unique_name("filesystem_postgres_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        ObjectLogStorage::Local {
            root: root.path().join("object-log"),
        },
        ObjectLogAuthority::NativeConditionalWrite,
        ProjectionConfig::Postgres {
            url: ConfigSecret::new(postgres_url.clone()),
        },
        ResponseBarrier::Strict,
        namespace,
    );
    let open = || {
        fireweed::open_objectlog_postgres(runtime.clone(), Arc::new(SystemClock)).unwrap_or_else(
            |error| {
                panic!(
                    "failed to open filesystem--postgres--sync-constructor: {}",
                    redacted_error(error, &[&postgres_url])
                )
            },
        )
    };
    let fireweed = open();
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build object-log sync-constructor test runtime");
    test_runtime.block_on(public_interface::run(
        "filesystem--postgres--sync-constructor",
        &fireweed,
        true,
    ));
    let probe = test_runtime.block_on(seed_reopen_probe(
        "filesystem--postgres--sync-constructor",
        &fireweed,
    ));
    drop(fireweed);
    let reopened = open();
    test_runtime.block_on(verify_reopen_probe(&reopened, probe));
    drop(reopened);
    schema.cleanup().unwrap_or_else(|_| {
        panic!("failed to clean filesystem--postgres--sync-constructor schema")
    });
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live S3 and PostgreSQL; P10 executes the full external matrix"]
async fn s3_postgres_strict_public_interface() {
    let config = S3Config::load();
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let namespace = unique_name("s3_postgres_strict");
    let mut objects = S3Namespace::new(&config, &namespace);
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        s3_storage(&config),
        ObjectLogAuthority::NativeConditionalWrite,
        ProjectionConfig::Postgres {
            url: ConfigSecret::new(postgres_url.clone()),
        },
        ResponseBarrier::Strict,
        namespace,
    );
    let fireweed = fireweed::open_objectlog_postgres_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open s3--postgres--strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("s3--postgres--strict", &fireweed, true).await;
    let probe = seed_reopen_probe("s3--postgres--strict", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_objectlog_postgres_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen s3--postgres--strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean s3--postgres--strict schema"));
    objects
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean s3--postgres--strict namespace"));
}

async fn run_s3_turso(cell: &str, barrier: ResponseBarrier) {
    let config = S3Config::load();
    let root = FixtureRoot::new(cell);
    let namespace = unique_name(cell);
    let mut objects = S3Namespace::new(&config, &namespace);
    let runtime = fireweed::StorageConfig {
        log: fireweed::LogConfig::S3 {
            endpoint: config.s3_endpoint.clone(),
            bucket: config.s3_bucket.clone(),
            region: config.s3_region.clone(),
            access_key_id: ConfigSecret::new(config.s3_access_key.clone()),
            secret_access_key: ConfigSecret::new(config.s3_secret_key.clone()),
            allow_insecure_http: config.s3_endpoint.starts_with("http://"),
        },
        projection: fireweed::ProjectionStoreConfig::Turso {
            path: root.path().join("projection.db"),
        },
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection)
            .then(fireweed::AsyncProjectionSpec::default),
        segments: SegmentConfig::new(262_144, 20).unwrap(),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 1_000_000,
        },
        ..fireweed::StorageConfig::memory()
    };
    let fireweed = fireweed::open(runtime.clone(), Arc::new(SystemClock))
        .unwrap_or_else(|_| panic!("failed to open {cell} without exposing connection details"));
    public_interface::run(cell, &fireweed, true).await;
    let probe = seed_reopen_probe(cell, &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open(runtime, Arc::new(SystemClock))
        .unwrap_or_else(|_| panic!("failed to reopen {cell} without exposing connection details"));
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    objects
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean object-store namespace for {cell}"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live S3; P10 executes the full external matrix"]
async fn s3_turso_strict_public_interface() {
    run_s3_turso("s3--turso--strict", ResponseBarrier::Strict).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live S3; P10 executes the full external matrix"]
async fn s3_turso_async_public_interface() {
    run_s3_turso("s3--turso--async", ResponseBarrier::AsyncProjection).await;
}
