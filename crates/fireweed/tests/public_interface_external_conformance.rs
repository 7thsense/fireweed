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
    NewItem, ObjectLogRuntimeConfig, ObjectLogStorage, OrderingMode, OwnerId,
    PostgresCoordinationConfig, PostgresMode, PostgresRuntimeConfig, PriorityDirection,
    PriorityModel, PriorityModelKind, PriorityTieBreaker, PriorityValue, ProjectionConfig,
    QueueDefinition, QueueId, QueueKey, RecoveryAction, RecoveryPolicy, RecurrencePolicy,
    RequestId, ResponseBarrier, RetryPolicy, SegmentConfig, SelectedMutation, SystemClock,
    TenantId, UtcTimestamp,
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
        let store = S3BlobStore::new(
            &self.endpoint,
            &self.bucket,
            &self.access_key,
            &self.secret_key,
            &self.region,
        )
        .map_err(|_| ())?;
        let keys = store.list(&self.prefix).map_err(|_| ())?;
        if keys.iter().any(|key| !key.starts_with(&self.prefix)) {
            return Err(());
        }
        for key in keys {
            store.delete(&key).map_err(|_| ())?;
        }
        if !store.list(&self.prefix).map_err(|_| ())?.is_empty() {
            return Err(());
        }
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
    }
}

fn schema_url(url: &str, schema: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}options=-c%20search_path%3D{schema}")
}

fn objectlog_config(
    object_log: ObjectLogStorage,
    projection: ProjectionConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> ObjectLogRuntimeConfig {
    ObjectLogRuntimeConfig {
        object_log,
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
    mutation: ItemMutationRequest,
    mutation_response: ItemMutationResponse,
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
    let item_id = fireweed
        .push_with_request_id(&queue, request_id.clone(), reopen_item(b"before"))
        .await
        .unwrap();
    assert_eq!(
        fireweed
            .push_with_request_id(&queue, request_id, reopen_item(b"before"))
            .await
            .unwrap(),
        item_id
    );
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
        evaluated_at: UtcTimestamp::new(1_800_000_000, 0).unwrap(),
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
    ReopenProbe {
        queue,
        definition,
        item_id,
        batch,
        batch_response,
        mutation,
        mutation_response,
    }
}

async fn verify_reopen_probe(fireweed: &Fireweed, probe: ReopenProbe) {
    assert_eq!(
        fireweed.queue_definition(&probe.queue).await.unwrap(),
        probe.definition
    );
    assert_eq!(
        fireweed
            .push_with_request_id(
                &probe.queue,
                RequestId::new("reopen-push-v1").unwrap(),
                reopen_item(b"before"),
            )
            .await
            .unwrap(),
        probe.item_id
    );
    assert_eq!(
        fireweed
            .batch_update(&probe.queue, probe.batch.clone())
            .await
            .unwrap(),
        probe.batch_response
    );
    assert_eq!(
        fireweed
            .mutate_items(&probe.queue, probe.mutation.clone())
            .await
            .unwrap(),
        probe.mutation_response,
        "item mutation response must replay exactly after close/reopen"
    );
    let mut conflicting_mutation = probe.mutation;
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
    let item = fireweed
        .live_item(&probe.queue, ClientItemKey::new("reopen-primary").unwrap())
        .await
        .unwrap()
        .expect("primary item survives close/reopen");
    assert_eq!(item.priority, Some(PriorityValue::Int64(2)));
    assert_eq!(item.payload.as_deref(), Some(b"after".as_slice()));
    assert_eq!(
        item.fields.get("mutation-proof").map(bytes::Bytes::as_ref),
        Some(b"durable".as_slice())
    );
    assert!(
        fireweed
            .peek(&probe.queue, 10)
            .await
            .unwrap()
            .iter()
            .all(|item| item.item_id != probe.item_id),
        "blocked gate survives close/reopen"
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
fn postgres_convenience_sync_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_convenience_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    run_sync_constructor("postgres-convenience-sync", || {
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
    public_interface::run("postgres-convenience-async", &fireweed, false).await;
    let probe = seed_reopen_probe("postgres-convenience-async", &fireweed).await;
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
fn postgres_coordinated_constructor_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_coordinated_constructor");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let isolated_url = schema_url(&postgres_url, &schema_name);
    let owner_id = OwnerId::new(unique_name("coordinated_owner")).expect("valid unique owner id");
    run_sync_constructor("postgres-coordinated-constructor", || {
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
fn postgres_runtime_sync_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let schema_name = unique_name("postgres_runtime_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), schema_name.clone());
    let runtime = postgres_config(&postgres_url, &schema_name, PostgresMode::LogReplay, None);
    run_sync_constructor("postgres-runtime-sync", || {
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
    };
    let fireweed = fireweed::open_postgres_runtime_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to open postgres-relational-coordinated-node: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("postgres-relational-coordinated-node", &fireweed, false).await;
    let probe = seed_reopen_probe("postgres-relational-coordinated-node", &fireweed).await;
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
async fn postgres_log_replay_public_interface() {
    run_postgres_runtime("postgres-log-replay", PostgresMode::LogReplay, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_relational_public_interface() {
    run_postgres_runtime("postgres-relational", PostgresMode::Relational, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_coordinated_public_interface() {
    run_postgres_runtime("postgres-coordinated", PostgresMode::LogReplay, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn objectlog_local_postgres_strict_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let root = FixtureRoot::new("objectlog_local_postgres");
    let namespace = unique_name("objectlog_local_postgres");
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        ObjectLogStorage::Local {
            root: root.path().join("object-log"),
        },
        ProjectionConfig::Postgres {
            url: ConfigSecret::new(postgres_url.clone()),
        },
        ResponseBarrier::Strict,
        namespace,
    );
    let fireweed = fireweed::open_objectlog_postgres_async(runtime.clone(), Arc::new(SystemClock))
        .await
        .unwrap_or_else(|_| panic!("failed to open objectlog-local-postgres-strict"));
    public_interface::run("objectlog-local-postgres-strict", &fireweed, true).await;
    let probe = seed_reopen_probe("objectlog-local-postgres-strict", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_objectlog_postgres_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen objectlog-local-postgres-strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema.cleanup().unwrap_or_else(|_| {
        panic!("failed to clean PostgreSQL schema for objectlog-local-postgres-strict")
    });
}

#[test]
fn objectlog_local_postgres_sync_constructor_public_interface() {
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let root = FixtureRoot::new("objectlog_local_postgres_sync");
    let namespace = unique_name("objectlog_local_postgres_sync");
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        ObjectLogStorage::Local {
            root: root.path().join("object-log"),
        },
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
                    "failed to open objectlog-local-postgres-sync: {}",
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
        "objectlog-local-postgres-sync",
        &fireweed,
        true,
    ));
    let probe = test_runtime.block_on(seed_reopen_probe(
        "objectlog-local-postgres-sync",
        &fireweed,
    ));
    drop(fireweed);
    let reopened = open();
    test_runtime.block_on(verify_reopen_probe(&reopened, probe));
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean objectlog-local-postgres-sync schema"));
}

#[tokio::test(flavor = "current_thread")]
async fn garage_s3_postgres_strict_public_interface() {
    let config = S3Config::load();
    let postgres_url = required_env("FIREWEED_PG_TEST_URL");
    let namespace = unique_name("garage_s3_postgres_strict");
    let mut objects = S3Namespace::new(&config, &namespace);
    let mut schema = PostgresSchema::new(postgres_url.clone(), derived_postgres_schema(&namespace));
    let runtime = objectlog_config(
        s3_storage(&config),
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
                "failed to open garage-s3-postgres-strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    public_interface::run("garage-s3-postgres-strict", &fireweed, true).await;
    let probe = seed_reopen_probe("garage-s3-postgres-strict", &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_objectlog_postgres_async(runtime, Arc::new(SystemClock))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to reopen garage-s3-postgres-strict: {}",
                redacted_error(error, &[&postgres_url])
            )
        });
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    schema
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean garage-s3-postgres-strict schema"));
    objects
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean garage-s3-postgres-strict namespace"));
}

async fn run_s3_sqlite(cell: &str, barrier: ResponseBarrier) {
    let config = S3Config::load();
    let root = FixtureRoot::new(cell);
    let namespace = unique_name(cell);
    let mut objects = S3Namespace::new(&config, &namespace);
    let runtime = objectlog_config(
        s3_storage(&config),
        ProjectionConfig::Sqlite {
            path: root.path().join("projection.sqlite"),
        },
        barrier,
        namespace,
    );
    let fireweed = fireweed::open_objectlog_sqlite(runtime.clone(), Arc::new(SystemClock))
        .unwrap_or_else(|_| panic!("failed to open {cell} without exposing connection details"));
    public_interface::run(cell, &fireweed, true).await;
    let probe = seed_reopen_probe(cell, &fireweed).await;
    drop(fireweed);
    let reopened = fireweed::open_objectlog_sqlite(runtime, Arc::new(SystemClock))
        .unwrap_or_else(|_| panic!("failed to reopen {cell} without exposing connection details"));
    verify_reopen_probe(&reopened, probe).await;
    drop(reopened);
    objects
        .cleanup()
        .unwrap_or_else(|_| panic!("failed to clean object-store namespace for {cell}"));
}

#[tokio::test(flavor = "current_thread")]
async fn garage_s3_sqlite_strict_public_interface() {
    run_s3_sqlite("garage-s3-sqlite-strict", ResponseBarrier::Strict).await;
}

#[tokio::test(flavor = "current_thread")]
async fn garage_s3_sqlite_async_public_interface() {
    run_s3_sqlite("garage-s3-sqlite-async", ResponseBarrier::AsyncProjection).await;
}
