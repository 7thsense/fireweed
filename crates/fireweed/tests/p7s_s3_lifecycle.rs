//! P7S3 — S3 append, claim, finalize, and lifecycle parity.
//!
//! Executable boundary (fireweed-3f5a1de3): run P7N's applicable product
//! assertions on all three S3 cells with native-CAS failover and P1s provenance;
//! consume the provider-neutral verifier without editing its shared logic.
//!
//! ADR-024 later retired `s3×memory` and `s3×postgres`: those tests now prove
//! rejection before storage I/O, and the assertion set runs on `s3×turso`.
//!
//! - `s3×memory` / `s3×turso`: full `public_interface::run`
//! - `s3×postgres`: P7 method family only (append/claim/finalize). Shared
//!   verifier also hits P6/P8 stubs (`Unavailable` on upsert/update_fields/
//!   current_position) outside P7 ownership — follow-on scope.

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

static ORDINAL: AtomicU64 = AtomicU64::new(0);

fn require_s3_env() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT required for P7S3 (P1s provenance)");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed".into());
    let region = std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access = std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "fireweed".into());
    let secret = std::env::var("FIREWEED_S3_TEST_SECRET_KEY")
        .unwrap_or_else(|_| "fireweed-test-rustfs".into());
    (endpoint, bucket, region, access, secret)
}

fn unique_ns(label: &str) -> String {
    let n = ORDINAL.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("p7s-{label}-{}-{n}-{nanos}", std::process::id())
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
        response_barrier: ResponseBarrier::AsyncProjection,
        async_projection: Some(fireweed::AsyncProjectionSpec::default()),
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

/// ADR-024 retired every S3 cell except s3 × turso. `open` rejects them before
/// storage I/O, so these checks need neither the P1s attestation nor PostgreSQL.
async fn assert_retired_cell(cell_id: &str, projection: ProjectionStoreConfig) {
    let config = s3_log_config(unique_ns(cell_id), projection);
    let retired = fireweed::EngineError::Invalid(fireweed::RETIRED_STORAGE_CELL);
    assert_eq!(
        config.validate(),
        Err(retired.clone()),
        "{cell_id} validate"
    );
    let error = fireweed::open_async(config, Arc::new(SystemClock) as _)
        .await
        .err()
        .unwrap_or_else(|| panic!("{cell_id} must not open"));
    assert_eq!(error, retired, "{cell_id} open");
}

async fn open_cell(cell_id: &str, config: StorageConfig) -> Fireweed {
    config
        .validate()
        .unwrap_or_else(|e| panic!("{cell_id} validate: {e:?}"));
    fireweed::open_async(config, Arc::new(SystemClock) as _)
        .await
        .unwrap_or_else(|e| panic!("{cell_id} open: {e:?}"))
}

async fn run_full_verifier(cell_id: &str, config: StorageConfig, expect_projection_control: bool) {
    let fireweed = open_cell(cell_id, config).await;
    public_interface::run(cell_id, &fireweed, expect_projection_control).await;
    eprintln!("P7S3 PASS {cell_id} public_interface (full verifier)");
}

/// ADR-024 retired s3 × memory; see `assert_retired_cell`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_memory_strict_public_interface_lifecycle() {
    assert_retired_cell("s3--memory", ProjectionStoreConfig::Memory).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_turso_strict_public_interface_lifecycle() {
    let _s3 = require_s3_env();
    let fixture = FixtureRoot::new("s3-sqlite");
    let ns = unique_ns("s3-sqlite");
    let config = s3_log_config(
        ns,
        ProjectionStoreConfig::Turso {
            path: fixture.path().join("projection.sqlite"),
        },
    );
    run_full_verifier("s3--turso--strict", config, true).await;
}

/// ADR-024 retired s3 × postgres; see `assert_retired_cell`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_postgres_strict_p7_method_parity() {
    assert_retired_cell(
        "s3--postgres",
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
        },
    )
    .await;
}
