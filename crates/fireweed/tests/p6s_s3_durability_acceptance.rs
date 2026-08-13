//! P6s — provider-neutral Snorri S3 durability acceptance (fireweed-side harness).
//!
//! Executable boundary (fireweed-2886078a / P6s):
//! After P8S3 (projection maintenance/delete-rebuild) and P8cs (durable emission
//! cursor), prove the TP-004 live S3 semantic IDs against P1s-attested
//! provider-neutral values with **zero skips**:
//!
//! | Semantic ID | Proof |
//! | --- | --- |
//! | `SNORRI-REOPEN` | Class A round-trip reopen on `s3×memory|sqlite|postgres` |
//! | `SNORRI-PROJECTION-REBUILD` | Disposable projection verify/delete/rebuild on
//! |  | `s3×sqlite` and Postgres-control-plane `s3×postgres` (same item image) |
//! | `SNORRI-RETRY-ONCE` | `push_with_request_id` Fresh → Replayed; changed body
//! |  | `RequestIdConflict` on every live S3 projection row |
//!
//! Garage/`eldir` are not accepted. Provider brand strings must not appear in
//! cell IDs. Unsupported endpoint/field negatives remain owned by P3s.
//!
//! Focused run:
//! ```text
//! export LD_LIBRARY_PATH="/home/linuxbrew/.linuxbrew/opt/openssl@3/lib:${LD_LIBRARY_PATH:-}"
//! export FIREWEED_PG_TEST_URL='postgres://fireweed:fireweed@127.0.0.1:55432/fireweed_snorri_p6p'
//! set -a; source /tmp/fireweed-s3-secrets/credentials.env; set +a
//! rustup run 1.97.1 cargo test -p fireweed --features objectlog,sqlite,postgres \
//!   --test p6s_s3_durability_acceptance -- --nocapture
//! ```
//!
//! Orchestrator: `bash scripts/ci/snorri-s3-durability-acceptance.sh`

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    ClientItemKey, ConfigSecret, EligibilityPolicy, EngineError, Fireweed, LogConfig, NewItem,
    ObjectLogAuthority, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, PriorityValue, ProjectionStoreConfig, PushDisposition, QueueDefinition,
    QueueId, QueueKey, RecoveryAction, RecoveryPolicy, RequestId, ResponseBarrier, RetryPolicy,
    SegmentConfig, StorageConfig, SystemClock, TenantId,
};
use serde_json::Value;

static ORDINAL: AtomicU64 = AtomicU64::new(0);

const ATTESTATION_PATH: &str = "/tmp/fireweed-s3-secrets/s3-native-cas-capability-attestation.json";

fn require_s3_env() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P6s (P1s provenance; zero skips)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET")
        .expect("FIREWEED_S3_TEST_BUCKET required for P6s (P1s provenance; zero skips)");
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
        .expect("FIREWEED_S3_TEST_ACCESS_KEY required for P6s (P1s provenance; zero skips)");
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
        .expect("FIREWEED_S3_TEST_SECRET_KEY required for P6s (P1s provenance; zero skips)");
    (endpoint, bucket, region, access, secret)
}

fn require_pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .or_else(|_| std::env::var("SNORRI_FIREWEED_POSTGRES_URL"))
        .expect(
            "FIREWEED_PG_TEST_URL or SNORRI_FIREWEED_POSTGRES_URL required for P6s s3×postgres (zero skips)",
        )
}

fn load_attestation() -> Value {
    let text = std::fs::read_to_string(ATTESTATION_PATH).unwrap_or_else(|error| {
        panic!("P6s requires P1s attestation at {ATTESTATION_PATH}: {error}")
    });
    serde_json::from_str(&text).unwrap_or_else(|error| {
        panic!("P1s attestation must be valid JSON at {ATTESTATION_PATH}: {error}")
    })
}

/// Fail closed unless P1s native-CAS attestation is present, selected, and matches env.
/// Garage is explicitly rejected as implicit provisioning.
fn require_p1s_native_cas_provenance() {
    let (endpoint, bucket, _region, _access, _secret) = require_s3_env();
    let doc = load_attestation();
    assert_eq!(
        doc.get("bead_id").and_then(Value::as_str),
        Some("fireweed-f5fa7380"),
        "attestation must bind to P1s bead fireweed-f5fa7380"
    );
    assert_eq!(
        doc.get("capability_id").and_then(Value::as_str),
        Some("S3-NATIVE-CAS-CAPABILITY-ATTESTATION"),
        "attestation must carry S3-NATIVE-CAS-CAPABILITY-ATTESTATION"
    );
    let results = doc.get("results").expect("attestation.results required");
    assert_eq!(
        results.get("selected").and_then(Value::as_bool),
        Some(true),
        "attestation results.selected must be true"
    );
    let provider = results
        .get("selected_provider")
        .and_then(Value::as_str)
        .or_else(|| {
            doc.get("s3")
                .and_then(|s| s.get("provider"))
                .and_then(Value::as_str)
        })
        .unwrap_or("")
        .to_ascii_lowercase();
    assert_ne!(
        provider.as_str(),
        "garage",
        "P6s rejects garage as selected provider; use P1s native-CAS endpoint"
    );
    let s3 = doc
        .get("s3")
        .expect("attestation must expose top-level s3 topology fields");
    let preflight = doc
        .get("preflight")
        .expect("attestation.preflight required");
    let attested_endpoint = s3
        .get("endpoint")
        .and_then(Value::as_str)
        .or_else(|| preflight.get("endpoint").and_then(Value::as_str))
        .expect("attestation must record endpoint");
    assert_eq!(
        endpoint.trim_end_matches('/'),
        attested_endpoint.trim_end_matches('/'),
        "FIREWEED_S3_TEST_ENDPOINT must match P1s attestation endpoint"
    );
    let attested_bucket = s3
        .get("bucket")
        .and_then(Value::as_str)
        .or_else(|| preflight.get("bucket").and_then(Value::as_str))
        .expect("attestation must record bucket");
    assert_eq!(
        bucket, attested_bucket,
        "FIREWEED_S3_TEST_BUCKET must match P1s attestation bucket"
    );
    let native_create = s3
        .get("native_atomic_conditional_create")
        .and_then(Value::as_bool)
        .or_else(|| {
            preflight
                .get("native_atomic_conditional_create")
                .and_then(Value::as_bool)
        })
        .unwrap_or(false);
    let native_update = s3
        .get("native_atomic_conditional_update")
        .and_then(Value::as_bool)
        .or_else(|| {
            preflight
                .get("native_atomic_conditional_update")
                .and_then(Value::as_bool)
        })
        .unwrap_or(false);
    assert!(
        native_create && native_update,
        "P6s requires attested native CAS create+update; create={native_create} update={native_update}"
    );
    let preflight_ok = doc.pointer("/preflight/status").and_then(Value::as_str) == Some("passed");
    assert!(
        preflight_ok,
        "P1s preflight.status must be passed for native-CAS provenance"
    );
    eprintln!(
        "P6s provenance: endpoint={endpoint} bucket={bucket} provider={provider} native_cas=create+update"
    );
}

fn unique_ns(label: &str) -> String {
    let n = ORDINAL.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("p6s-{label}-{}-{n}-{nanos}", std::process::id())
}

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(unique_ns(label));
        std::fs::create_dir_all(&path).expect("fixture root");
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

fn s3_log_config(namespace: String, projection: ProjectionStoreConfig) -> StorageConfig {
    let (endpoint, bucket, region, access, secret) = require_s3_env();
    StorageConfig {
        log: LogConfig::S3 {
            endpoint,
            bucket,
            region,
            access_key_id: ConfigSecret::new(access),
            secret_access_key: ConfigSecret::new(secret),
            allow_insecure_http: true,
        },
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

async fn open_cell(cell_id: &str, config: StorageConfig) -> Fireweed {
    config
        .validate()
        .unwrap_or_else(|e| panic!("{cell_id} validate: {e:?}"));
    fireweed::open_async(config, Arc::new(SystemClock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} open: {e:?}"))
}

fn qdef(name: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("p6s").unwrap(),
        queue_id: QueueId::new(name).unwrap(),
        priority_model: PriorityModel {
            kind: PriorityModelKind::Int64,
            direction: PriorityDirection::Ascending,
            tie_breaker: PriorityTieBreaker::CreatedSequence,
        },
        ordering_mode: OrderingMode::Strict,
        max_rank_error: 0,
        progress_bound_ms: 60_000,
        eligibility_policy: EligibilityPolicy::default(),
        cohort_policy: None,
        recurrence: Default::default(),
        request_id_retention_ms: 3_600_000,
        client_item_key_retention_ms: 3_600_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 5 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

fn item(label: &str, priority: i64, payload: &'static [u8]) -> NewItem {
    NewItem {
        client_item_key: Some(ClientItemKey::new(label).unwrap()),
        priority: Some(PriorityValue::Int64(priority)),
        payload: Some(payload.into()),
        ..Default::default()
    }
}

/// SNORRI-REOPEN: Class A round-trip reopen recovers definition, pending, and request_id.
async fn run_snorri_reopen(cell_id: &str, config: StorageConfig) {
    let definition = qdef(&format!("reopen-{}", cell_id.replace("--", "-")));
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let rid = RequestId::new(format!("p6s-reopen-{}", cell_id.replace("--", "-"))).unwrap();
    let body = item("reopen-primary", 7, b"p6s-reopen-body");

    let fireweed = open_cell(cell_id, config.clone()).await;
    assert!(
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap_or_else(|e| panic!("{cell_id} create_queue: {e:?}"))
            .created,
        "{cell_id} first create must create"
    );
    let (item_id, disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} push: {e:?}"));
    assert_eq!(disp, PushDisposition::Fresh, "{cell_id} first push Fresh");
    assert_eq!(
        fireweed.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} pending before reopen"
    );
    drop(fireweed);

    let reopened = open_cell(cell_id, config).await;
    assert_eq!(
        reopened.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} SNORRI-REOPEN: definition survives reopen"
    );
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} SNORRI-REOPEN: pending recovered from durable s3 log"
    );
    let (after_id, after_disp) = reopened
        .push_with_request_id(&queue, rid, body)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} post-reopen request_id: {e:?}"));
    assert_eq!(
        after_disp,
        PushDisposition::Replayed,
        "{cell_id} SNORRI-REOPEN: request_id survives process death"
    );
    assert_eq!(after_id, item_id, "{cell_id} same item id after reopen");
    drop(reopened);
    eprintln!("P6s PASS SNORRI-REOPEN {cell_id}");
}

/// SNORRI-RETRY-ONCE: response-loss retry converges to one transition; conflict fails.
async fn run_snorri_retry_once(cell_id: &str, config: StorageConfig) {
    let definition = qdef(&format!("retry-{}", cell_id.replace("--", "-")));
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let rid = RequestId::new(format!("p6s-retry-{}", cell_id.replace("--", "-"))).unwrap();
    let body = item("retry-primary", 3, b"p6s-retry-body");

    let fireweed = open_cell(cell_id, config.clone()).await;
    assert!(
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );

    let (first_id, first_disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap();
    assert_eq!(first_disp, PushDisposition::Fresh);

    // Simulate response loss: same request_id + body must replay exactly once.
    let (replay_id, replay_disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap();
    assert_eq!(
        replay_disp,
        PushDisposition::Replayed,
        "{cell_id} SNORRI-RETRY-ONCE: same-body retry is Replayed"
    );
    assert_eq!(
        replay_id, first_id,
        "{cell_id} SNORRI-RETRY-ONCE: same item id"
    );
    assert_eq!(
        fireweed.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} SNORRI-RETRY-ONCE: exactly one pending item"
    );

    let mut conflicting = body.clone();
    conflicting.payload = Some(b"p6s-retry-conflict".as_slice().into());
    let conflict = fireweed
        .push_with_request_id(&queue, rid.clone(), conflicting.clone())
        .await
        .unwrap_err();
    assert_eq!(
        conflict,
        EngineError::RequestIdConflict,
        "{cell_id} SNORRI-RETRY-ONCE: conflicting body fails"
    );

    // Survive reopen (Class A request_id ledger on durable s3 log).
    drop(fireweed);
    let reopened = open_cell(cell_id, config).await;
    let (after_id, after_disp) = reopened
        .push_with_request_id(&queue, rid.clone(), body)
        .await
        .unwrap();
    assert_eq!(after_disp, PushDisposition::Replayed);
    assert_eq!(after_id, first_id);
    let post_conflict = reopened
        .push_with_request_id(&queue, rid, conflicting)
        .await
        .unwrap_err();
    assert_eq!(post_conflict, EngineError::RequestIdConflict);
    assert_eq!(reopened.metrics(&queue).await.unwrap().pending, 1);
    drop(reopened);
    eprintln!("P6s PASS SNORRI-RETRY-ONCE {cell_id}");
}

/// SNORRI-PROJECTION-REBUILD via public projection_control (capability-bearing cells).
async fn run_snorri_projection_rebuild(cell_id: &str, config: StorageConfig) {
    let definition = qdef(&format!("rebuild-{}", cell_id.replace("--", "-")));
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let rid = RequestId::new(format!("p6s-rebuild-{}", cell_id.replace("--", "-"))).unwrap();
    let body = item("rebuild-primary", 5, b"p6s-rebuild-body");

    let fireweed = open_cell(cell_id, config).await;
    assert!(
        fireweed
            .create_queue(definition.clone())
            .await
            .unwrap()
            .created
    );
    let (item_id, disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap();
    assert_eq!(disp, PushDisposition::Fresh);
    assert_eq!(fireweed.metrics(&queue).await.unwrap().pending, 1);

    let control = fireweed
        .projection_control()
        .unwrap_or_else(|| panic!("{cell_id} must expose projection_control for rebuild proof"));
    let caps = control.capabilities();
    assert!(
        caps.verify && caps.delete && caps.rebuild,
        "{cell_id} projection_control capabilities: verify/delete/rebuild required, got {caps:?}"
    );

    let before = control
        .verify()
        .await
        .unwrap_or_else(|e| panic!("{cell_id} verify before delete: {e:?}"));
    assert!(
        before.compatible,
        "{cell_id} projection must be compatible before delete"
    );
    let seq_before = before.projection_sequence;

    control
        .delete()
        .await
        .unwrap_or_else(|e| panic!("{cell_id} delete_projection: {e:?}"));

    let rebuilt = control
        .rebuild()
        .await
        .unwrap_or_else(|e| panic!("{cell_id} rebuild_projection: {e:?}"));
    assert!(
        rebuilt.tail_commands_replayed > 0 || rebuilt.projection_sequence > 0,
        "{cell_id} rebuild must replay or materialize sequence (got {rebuilt:?})"
    );
    assert!(
        rebuilt.projection_sequence >= seq_before || rebuilt.tail_commands_replayed > 0,
        "{cell_id} rebuild sequence/tail must advance; before={seq_before} rebuilt={rebuilt:?}"
    );

    let after = control
        .verify()
        .await
        .unwrap_or_else(|e| panic!("{cell_id} verify after rebuild: {e:?}"));
    assert!(
        after.compatible,
        "{cell_id} projection must be compatible after rebuild"
    );

    assert_eq!(
        fireweed.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} definition image after rebuild"
    );
    assert_eq!(
        fireweed.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} pending image after rebuild"
    );
    let (replay_id, replay_disp) = fireweed
        .push_with_request_id(&queue, rid, body)
        .await
        .unwrap();
    assert_eq!(replay_disp, PushDisposition::Replayed);
    assert_eq!(
        replay_id, item_id,
        "{cell_id} request_id/item image after rebuild"
    );

    drop(fireweed);
    eprintln!(
        "P6s PASS SNORRI-PROJECTION-REBUILD {cell_id} tail={} seq={}",
        rebuilt.tail_commands_replayed, rebuilt.projection_sequence
    );
}

// --- SNORRI-REOPEN: all three S3 cells ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_reopen_s3_memory() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory-reopen");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_snorri_reopen("s3--memory", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_reopen_s3_sqlite() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-reopen");
    let ns = unique_ns("s3-sqlite-reopen");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_snorri_reopen("s3--sqlite", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_reopen_s3_postgres() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres-reopen");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_snorri_reopen("s3--postgres", config).await;
}

// --- SNORRI-RETRY-ONCE: every live S3 projection row ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_retry_once_s3_memory() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory-retry");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_snorri_retry_once("s3--memory", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_retry_once_s3_sqlite() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-retry");
    let ns = unique_ns("s3-sqlite-retry");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_snorri_retry_once("s3--sqlite", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_retry_once_s3_postgres() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres-retry");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_snorri_retry_once("s3--postgres", config).await;
}

// --- SNORRI-PROJECTION-REBUILD: disposable projections (sqlite + postgres control plane) ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_projection_rebuild_s3_sqlite() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-rebuild");
    let ns = unique_ns("s3-sqlite-rebuild");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_snorri_projection_rebuild("s3--sqlite", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_projection_rebuild_s3_postgres_control_plane() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres-rebuild");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_snorri_projection_rebuild("s3--postgres", config).await;
}

/// Memory projection is not disposable: projection_control must be None (unsupported negative retained).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snorri_projection_rebuild_s3_memory_has_no_control() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory-no-control");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    let fireweed = open_cell("s3--memory", config).await;
    assert!(
        fireweed.projection_control().is_none(),
        "s3×memory must not expose disposable projection_control (unsupported negative retained)"
    );
    drop(fireweed);
    eprintln!("P6s PASS SNORRI-PROJECTION-REBUILD unsupported-negative s3--memory control=None");
}
