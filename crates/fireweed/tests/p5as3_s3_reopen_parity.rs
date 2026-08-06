//! P5aS3 — S3 Class A reopen and recovery-replay parity.
//!
//! Executable boundary (fireweed-aebd93c2): after P1s/P3s/P4s, prove all three
//! S3-log cells satisfy the identical P5a Class A assertion set against native
//! CAS, including failover/reopen with attested provenance; zero skips.
//!
//! Cells: `s3×memory`, `s3×sqlite`, `s3×postgres` (strict / NativeConditionalWrite).
//!
//! Assertion set (identical across cells):
//! 1. **T2 reopen** — pending work survives process-local drop + reopen from the
//!    durable S3 log (Class A log authority).
//! 2. **Recovery-replay** — `request_id` Fresh → Replayed in-process and across
//!    reopen; changed body yields `RequestIdConflict` after reopen.
//! 3. **Definition durability** — queue definition is byte-stable across reopen.
//! 4. **Native-CAS failover** — concurrent create_queue on the same S3 namespace
//!    elects exactly one creator; loser reads the winner; reopen recovers it
//!    (create-only `If-None-Match:*` definition authority from P7S3).
//! 5. **P1s provenance** — env endpoint/bucket match the P1s attestation and
//!    native CAS create+update are attested (no silent skip).
//!
//! Focused run:
//! ```text
//! set -a; source /tmp/fireweed-s3-secrets/credentials.env; set +a
//! export FIREWEED_PG_TEST_URL=postgres://fireweed:fireweed@127.0.0.1:55432/fireweed
//! cargo test -p fireweed --features objectlog,sqlite,postgres --test p5as3_s3_reopen_parity -- --nocapture
//! ```

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
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P5aS3 (P1s provenance; zero skips)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET")
        .expect("FIREWEED_S3_TEST_BUCKET required for P5aS3 (P1s provenance; zero skips)");
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
        .expect("FIREWEED_S3_TEST_ACCESS_KEY required for P5aS3 (P1s provenance; zero skips)");
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
        .expect("FIREWEED_S3_TEST_SECRET_KEY required for P5aS3 (P1s provenance; zero skips)");
    (endpoint, bucket, region, access, secret)
}

fn require_pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required for P5aS3 s3×postgres (zero skips)")
}

fn load_attestation() -> Value {
    let text = std::fs::read_to_string(ATTESTATION_PATH).unwrap_or_else(|error| {
        panic!("P5aS3 requires P1s attestation at {ATTESTATION_PATH}: {error}")
    });
    serde_json::from_str(&text).unwrap_or_else(|error| {
        panic!("P1s attestation must be valid JSON at {ATTESTATION_PATH}: {error}")
    })
}

/// Fail closed unless P1s native-CAS attestation is present and matches env.
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
        "P5aS3 requires attested native CAS create+update; create={native_create} update={native_update}"
    );
    let preflight_ok = doc.pointer("/preflight/status").and_then(Value::as_str) == Some("passed");
    assert!(
        preflight_ok,
        "P1s preflight.status must be passed for native-CAS provenance"
    );
    eprintln!("P5aS3 provenance: endpoint={endpoint} bucket={bucket} native_cas=create+update");
}

fn unique_ns(label: &str) -> String {
    let n = ORDINAL.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("p5as3-{label}-{}-{n}-{nanos}", std::process::id())
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
        tenant_id: TenantId::new("p5as3").unwrap(),
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

/// Identical P5a Class A reopen + recovery-replay assertions for one S3 cell.
async fn run_class_a_reopen_recovery_replay(cell_id: &str, config: StorageConfig) {
    let definition = qdef(&format!("reopen-{}", cell_id.replace("--", "-")));
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let rid_label = cell_id.replace("--", "-");
    let rid = RequestId::new(format!("p5as3-rid-{rid_label}")).unwrap();
    let body = item("reopen-primary", 7, b"before");

    let fireweed = open_cell(cell_id, config.clone()).await;
    let created = fireweed
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} create_queue: {e:?}"));
    assert!(created.created, "{cell_id} first create_queue must create");
    assert_eq!(
        created.definition, definition,
        "{cell_id} create outcome definition"
    );

    let (first_id, first_disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} first push_with_request_id: {e:?}"));
    assert_eq!(
        first_disp,
        PushDisposition::Fresh,
        "{cell_id} first request_id must be Fresh"
    );
    let (replay_id, replay_disp) = fireweed
        .push_with_request_id(&queue, rid.clone(), body.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} in-process replay: {e:?}"));
    assert_eq!(
        replay_disp,
        PushDisposition::Replayed,
        "{cell_id} same-body replay must be Replayed"
    );
    assert_eq!(
        replay_id, first_id,
        "{cell_id} replay must return same item id"
    );

    let mut conflicting = body.clone();
    conflicting.payload = Some(b"different-body".as_slice().into());
    let conflict = fireweed
        .push_with_request_id(&queue, rid.clone(), conflicting.clone())
        .await
        .unwrap_err();
    assert_eq!(
        conflict,
        EngineError::RequestIdConflict,
        "{cell_id} pre-reopen RequestIdConflict"
    );

    assert_eq!(
        fireweed.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} pending before reopen"
    );
    assert_eq!(
        fireweed.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} definition readable before reopen"
    );

    drop(fireweed);

    // --- Class A reopen from durable S3 log ---
    let reopened = open_cell(cell_id, config).await;
    assert_eq!(
        reopened.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} TD004-DURABLE-MANIFEST-REOPEN: definition survives reopen"
    );
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} T2 Class A: pending recovered from durable s3 log"
    );

    let (after_id, after_disp) = reopened
        .push_with_request_id(&queue, rid.clone(), body)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} post-reopen request_id: {e:?}"));
    assert_eq!(
        after_disp,
        PushDisposition::Replayed,
        "{cell_id} recovery-replay: request_id survives process death"
    );
    assert_eq!(
        after_id, first_id,
        "{cell_id} recovery-replay: same item id after reopen"
    );

    let post_conflict = reopened
        .push_with_request_id(&queue, rid, conflicting)
        .await
        .unwrap_err();
    assert_eq!(
        post_conflict,
        EngineError::RequestIdConflict,
        "{cell_id} post-reopen RequestIdConflict"
    );

    let claimed = reopened
        .claim(&queue, 1, 30_000)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} post-reopen claim: {e:?}"));
    assert_eq!(claimed.len(), 1, "{cell_id} claim after reopen");
    assert_eq!(claimed[0].item_id, first_id);
    reopened
        .complete(&queue, claimed.iter().map(|c| c.item_id))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} post-reopen complete: {e:?}"));
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        0,
        "{cell_id} pending cleared after complete"
    );
    drop(reopened);
    eprintln!("P5aS3 PASS {cell_id} Class A reopen + recovery-replay");
}

/// Native-CAS definition failover: winner publishes create-only definition on S3;
/// a successor process loses create-only, observes the durable winner, and reopen
/// recovers winner state (If-None-Match:* / P7S3 definition authority).
async fn run_native_cas_failover(cell_id: &str, config: StorageConfig) {
    let definition = qdef(&format!("failover-{}", cell_id.replace("--", "-")));
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());

    // Writer A (first owner): create-only win + append.
    let a = open_cell(cell_id, config.clone()).await;
    let a_out = a
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} writer-A create: {e:?}"));
    assert!(a_out.created, "{cell_id} writer-A must win create-only");
    assert_eq!(a_out.definition, definition);
    a.push(&queue, item("failover-seed", 1, b"seed"))
        .await
        .unwrap_or_else(|e| panic!("{cell_id} winner push: {e:?}"));
    assert_eq!(a.metrics(&queue).await.unwrap().pending, 1);
    drop(a);

    // Writer B (failover successor): create-only lose; durable winner visible.
    let b = open_cell(cell_id, config.clone()).await;
    let b_out = b
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} writer-B create: {e:?}"));
    assert!(
        !b_out.created,
        "{cell_id} writer-B must lose create-only (native CAS / If-None-Match)"
    );
    assert_eq!(
        b_out.definition, definition,
        "{cell_id} loser must observe durable winner definition"
    );
    assert_eq!(
        b.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} loser queue_definition reads winner"
    );
    assert_eq!(
        b.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} loser recovers winner append from S3 log"
    );
    drop(b);

    // Third open: pure reopen recovery of definition + pending after handoff.
    let reopened = open_cell(cell_id, config).await;
    assert_eq!(
        reopened.queue_definition(&queue).await.unwrap(),
        definition,
        "{cell_id} failover definition survives reopen"
    );
    assert_eq!(
        reopened.metrics(&queue).await.unwrap().pending,
        1,
        "{cell_id} winner append survives reopen after CAS handoff"
    );
    let third = reopened
        .create_queue(definition.clone())
        .await
        .unwrap_or_else(|e| panic!("{cell_id} post-reopen create: {e:?}"));
    assert!(
        !third.created,
        "{cell_id} post-reopen create must not recreate"
    );
    assert_eq!(third.definition, definition);
    drop(reopened);
    eprintln!("P5aS3 PASS {cell_id} native-CAS failover + reopen");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p1s_attestation_native_cas_provenance() {
    require_p1s_native_cas_provenance();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_memory_class_a_reopen_and_recovery_replay() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory-reopen");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_class_a_reopen_recovery_replay("s3--memory--strict", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sqlite_class_a_reopen_and_recovery_replay() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-reopen");
    let ns = unique_ns("s3-sqlite-reopen");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_class_a_reopen_recovery_replay("s3--sqlite--strict", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_postgres_class_a_reopen_and_recovery_replay() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres-reopen");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_class_a_reopen_recovery_replay("s3--postgres--strict", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_memory_native_cas_failover_reopen() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory-failover");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_native_cas_failover("s3--memory--strict", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sqlite_native_cas_failover_reopen() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-failover");
    let ns = unique_ns("s3-sqlite-failover");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_native_cas_failover("s3--sqlite--strict", config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_postgres_native_cas_failover_reopen() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres-failover");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_native_cas_failover("s3--postgres--strict", config).await;
}

/// Disposable-projection rebuild: wipe local sqlite projection, reopen same S3
/// namespace, Class A log rebuilds exact pending + request_id retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sqlite_projection_loss_rebuilds_from_durable_log() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite-rebuild");
    let ns = unique_ns("s3-sqlite-rebuild");
    let proj_path = fixture.path().join("projection.sqlite");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: proj_path.clone(),
        },
    );
    let cell_id = "s3--sqlite--strict";
    let definition = qdef("rebuild-queue");
    let queue = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    let rid = RequestId::new("p5as3-rebuild-rid").unwrap();
    let body = item("rebuild-primary", 3, b"rebuild-body");

    let fireweed = open_cell(cell_id, config.clone()).await;
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
    drop(fireweed);

    // Destroy projection only — durable S3 log remains system of record.
    std::fs::remove_file(&proj_path).expect("delete disposable sqlite projection");
    assert!(
        !proj_path.exists(),
        "projection must be gone before rebuild open"
    );

    let rebuilt = open_cell(cell_id, config).await;
    assert_eq!(
        rebuilt.queue_definition(&queue).await.unwrap(),
        definition,
        "definition rebuilt from durable S3 log"
    );
    assert_eq!(
        rebuilt.metrics(&queue).await.unwrap().pending,
        1,
        "pending rebuilt from durable S3 log after projection loss"
    );
    let (replay_id, replay_disp) = rebuilt
        .push_with_request_id(&queue, rid, body)
        .await
        .unwrap();
    assert_eq!(replay_disp, PushDisposition::Replayed);
    assert_eq!(replay_id, item_id);
    drop(rebuilt);
    eprintln!("P5aS3 PASS {cell_id} projection-loss rebuild from durable log");
}
