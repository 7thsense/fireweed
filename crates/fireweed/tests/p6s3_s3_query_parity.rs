//! P6S3 — S3 queue, ownership, read, discovery, and query parity.
//!
//! Executable boundary (fireweed-f3cb9ad9): on the three S3 cells
//! (`s3×memory`, `s3×sqlite`, `s3×postgres`) run exactly P6N's applicable
//! assertion/method set (`public_interface::run_p6_surface`) using P5aS3 open
//! helpers and live P1s provenance; no unexpected `Unavailable`, skip, or
//! provider-specific substitute.
//!
//! Authority: ObjectLogAuthority::NativeConditionalWrite (create-only after P7S3).
//!
//! Focused run:
//! ```text
//! export LD_LIBRARY_PATH="/home/linuxbrew/.linuxbrew/opt/openssl@3/lib:${LD_LIBRARY_PATH:-}"
//! export FIREWEED_PG_TEST_URL='postgres://fireweed:fireweed@127.0.0.1:55432/fireweed'
//! export CARGO_TARGET_DIR=/home/erik/Projects/fireweed-shared-target
//! set -a; source /tmp/fireweed-s3-secrets/credentials.env; set +a
//! rustup run 1.92.0 cargo test -p fireweed --features objectlog,sqlite,postgres \
//!   --test p6s3_s3_query_parity -- --nocapture
//! ```

#[path = "support/public_interface.rs"]
mod public_interface;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    ConfigSecret, Fireweed, LogConfig, ObjectLogAuthority, ProjectionStoreConfig, RecoveryAction,
    RecoveryPolicy, ResponseBarrier, SegmentConfig, StorageConfig, SystemClock,
};
use serde_json::Value;

static ORDINAL: AtomicU64 = AtomicU64::new(0);

const ATTESTATION_PATH: &str = "/tmp/fireweed-s3-secrets/s3-native-cas-capability-attestation.json";

fn require_s3_env() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P6S3 (P1s provenance; zero skips)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET")
        .expect("FIREWEED_S3_TEST_BUCKET required for P6S3 (P1s provenance; zero skips)");
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY")
        .expect("FIREWEED_S3_TEST_ACCESS_KEY required for P6S3 (P1s provenance; zero skips)");
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
        .expect("FIREWEED_S3_TEST_SECRET_KEY required for P6S3 (P1s provenance; zero skips)");
    (endpoint, bucket, region, access, secret)
}

fn require_pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required for P6S3 s3×postgres (zero skips)")
}

fn load_attestation() -> Value {
    let text = std::fs::read_to_string(ATTESTATION_PATH).unwrap_or_else(|error| {
        panic!("P6S3 requires P1s attestation at {ATTESTATION_PATH}: {error}")
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
        "P6S3 requires attested native CAS create+update; create={native_create} update={native_update}"
    );
    let preflight_ok = doc.pointer("/preflight/status").and_then(Value::as_str) == Some("passed");
    assert!(
        preflight_ok,
        "P1s preflight.status must be passed for native-CAS provenance"
    );
    eprintln!("P6S3 provenance: endpoint={endpoint} bucket={bucket} native_cas=create+update");
}

fn unique_ns(label: &str) -> String {
    let n = ORDINAL.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("p6s3-{label}-{}-{n}-{nanos}", std::process::id())
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

async fn run_p6(cell_id: &str, config: StorageConfig, expect_projection_control: bool) {
    let fireweed = open_cell(cell_id, config).await;
    public_interface::run_p6_surface(cell_id, &fireweed, expect_projection_control).await;
    eprintln!(
        "P6S3 PASS {cell_id} run_p6_surface (projection_control={expect_projection_control})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_memory_strict_p6_query_parity() {
    require_p1s_native_cas_provenance();
    let ns = unique_ns("s3-memory");
    let config = s3_log_config(ns, ProjectionStoreConfig::Memory);
    run_p6("s3--memory--strict", config, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sqlite_strict_p6_query_parity() {
    require_p1s_native_cas_provenance();
    let fixture = FixtureRoot::new("s3-sqlite");
    let ns = unique_ns("s3-sqlite");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_p6("s3--sqlite--strict", config, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_postgres_strict_p6_query_parity() {
    require_p1s_native_cas_provenance();
    let pg = require_pg_url();
    let ns = unique_ns("s3-postgres");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(pg),
        },
    );
    run_p6("s3--postgres--strict", config, true).await;
}
