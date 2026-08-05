//! P3s — S3 configuration and barrier semantics.
//!
//! Proves every clause of the fireweed-2fdce55d executable boundary that is
//! owned at the facade composition seam:
//! - retire S3 `objectlog-memory-async-pending` and both S3×Postgres strict pins
//! - thread caller `AsyncProjectionSpec` + SQLite deferred-flush through the
//!   three split S3 concrete helpers (without editing filesystem helpers or
//!   inventing an S3-specific config type)
//! - open all three S3 cells under Strict and AsyncProjection against a live
//!   native-CAS endpoint, including deferred-flush tuning on S3×SQLite
//! - keep unsupported endpoint/field negatives and exact deferred-flush
//!   rejections on S3×memory / S3×Postgres
//!
//! Provider-neutral async apply (lag/catch-up/restart/poison/transactional SQL
//! checkpoints) is reused from P3b; this suite only proves the S3 composition
//! wiring reaches those pipelines with caller-selected bounds.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    AsyncProjectionSpec, ConfigSecret, EngineError, LogConfig, ObjectLogAuthority,
    ProjectionStoreConfig, RecoveryAction, RecoveryPolicy, ResponseBarrier, SegmentConfig,
    StorageConfig, SystemClock,
};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fireweed-p3s-s3-barriers-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create p3s fixture root");
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

fn non_default_spec() -> AsyncProjectionSpec {
    AsyncProjectionSpec::new(13, 65_537, 7, 12_345, 4).expect("valid non-default bounds")
}

fn require_s3_env() -> (String, String, String, String, String) {
    let endpoint = std::env::var("FIREWEED_S3_TEST_ENDPOINT")
        .expect("FIREWEED_S3_TEST_ENDPOINT is required for P3s live S3 cells");
    let bucket = std::env::var("FIREWEED_S3_TEST_BUCKET").unwrap_or_else(|_| "fireweed".to_owned());
    let region =
        std::env::var("FIREWEED_S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let access =
        std::env::var("FIREWEED_S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_owned());
    let secret =
        std::env::var("FIREWEED_S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_owned());
    (endpoint, bucket, region, access, secret)
}

fn require_pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL is required for P3s S3×Postgres cells")
}

fn s3_config(
    projection: ProjectionStoreConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> StorageConfig {
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
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection).then(non_default_spec),
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

fn fixture_postgres_schema(namespace: &str) -> String {
    let digest = Sha256::digest(namespace.as_bytes());
    let mut schema = String::from("fireweed_");
    for byte in digest.iter().take(27) {
        schema.push_str(&format!("{byte:02x}"));
    }
    schema
}

fn drop_test_schema(url: &str, namespace: &str) {
    let schema = fixture_postgres_schema(namespace);
    let mut client = Client::connect(url, NoTls).expect("connect for fixture cleanup");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop isolated projection schema");
}

fn structural_s3_config(
    projection: ProjectionStoreConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> StorageConfig {
    StorageConfig {
        log: LogConfig::S3 {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "fireweed".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: ConfigSecret::new("access"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection).then(non_default_spec),
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace,
        recovery: RecoveryPolicy::default(),
    }
}

#[test]
fn s3_validate_time_pins_are_retired_for_all_three_projections() {
    for projection in [
        ProjectionStoreConfig::Memory,
        ProjectionStoreConfig::Sqlite {
            path: PathBuf::from("/tmp/p3s-never-opened.sqlite"),
        },
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
        },
    ] {
        for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
            let config = structural_s3_config(
                projection.clone(),
                barrier,
                format!("p3s-validate-{}-{:?}", projection.axis_name(), barrier),
            );
            assert_eq!(
                config.validate(),
                Ok(()),
                "S3×{} under {barrier:?} must validate",
                projection.axis_name()
            );
        }
    }
}

#[test]
fn s3_deferred_flush_accepts_sqlite_and_rejects_memory_and_postgres() {
    let mut sqlite = StorageConfig {
        log: LogConfig::S3 {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "fireweed".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: ConfigSecret::new("access"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        projection: ProjectionStoreConfig::Sqlite {
            path: PathBuf::from("/tmp/p3s-deferred.sqlite"),
        },
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: Some(7),
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: "p3s-deferred-sqlite".to_owned(),
        recovery: RecoveryPolicy::default(),
    };
    assert_eq!(sqlite.validate(), Ok(()));

    sqlite.response_barrier = ResponseBarrier::AsyncProjection;
    sqlite.async_projection = Some(non_default_spec());
    assert_eq!(sqlite.validate(), Ok(()));

    let mut memory = sqlite.clone();
    memory.projection = ProjectionStoreConfig::Memory;
    assert_eq!(
        memory.validate(),
        Err(EngineError::Invalid(
            "sqlite-projection-deferred-flush-requires-sqlite-projection"
        ))
    );

    let mut postgres = sqlite;
    postgres.projection = ProjectionStoreConfig::Postgres {
        url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
    };
    assert_eq!(
        postgres.validate(),
        Err(EngineError::Invalid(
            "sqlite-projection-deferred-flush-requires-sqlite-projection"
        ))
    );
}

#[test]
fn unsupported_s3_field_and_endpoint_negatives_are_retained() {
    let projection = ProjectionStoreConfig::Memory;
    let empty_fields = StorageConfig {
        log: LogConfig::S3 {
            endpoint: String::new(),
            bucket: "fireweed".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: ConfigSecret::new("access"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        projection: projection.clone(),
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: "p3s-empty-fields".to_owned(),
        recovery: RecoveryPolicy::default(),
    };
    assert_eq!(
        empty_fields.validate(),
        Err(EngineError::Invalid(
            "S3 object-log configuration fields must not be empty"
        ))
    );

    // Unreachable endpoint must fail with a real storage/native-CAS style error,
    // not a retired validate-time barrier pin.
    let unreachable = StorageConfig {
        log: LogConfig::S3 {
            endpoint: "http://127.0.0.1:1".to_owned(),
            bucket: "fireweed".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id: ConfigSecret::new("akid"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        projection,
        control_plane: None,
        authority: Some(ObjectLogAuthority::NativeConditionalWrite),
        response_barrier: ResponseBarrier::AsyncProjection,
        async_projection: Some(non_default_spec()),
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).unwrap(),
        namespace: "p3s-unreachable".to_owned(),
        recovery: RecoveryPolicy::default(),
    };
    assert_eq!(unreachable.validate(), Ok(()));
    let err = fireweed::open(unreachable, Arc::new(SystemClock))
        .expect_err("unreachable S3 endpoint must fail at open");
    assert!(
        !matches!(err, EngineError::Invalid("objectlog-memory-async-pending")),
        "retired pending pin must not return: {err:?}"
    );
    assert!(
        !matches!(err, EngineError::Unavailable),
        "retired Unavailable pin must not return: {err:?}"
    );
}

#[test]
fn s3_facade_routes_caller_async_spec_and_deferred_flush_fields() {
    // Source guard: freeze the private conversion boundary so a future default
    // cannot erase caller-owned values without changing validation behavior.
    let source = include_str!("../src/lib.rs");
    assert!(source.contains("fn open_s3_log_cell("));
    assert!(source.contains("fn open_s3_objectlog_memory_projection("));
    assert!(source.contains("fn open_s3_composed_sqlite("));
    assert!(source.contains("fn open_s3_objectlog_postgres_blocking("));
    assert!(
        source.contains("from_log_store_with_async_projection"),
        "S3×memory must reuse the provider-neutral async memory pipeline"
    );
    assert!(
        source.contains("from_log_and_projection_with_async_projection"),
        "S3 SQL cells must reuse provider-neutral async apply pipelines"
    );
    // Caller fields must be threaded, not rewritten to AsyncProjectionSpec::default()
    // or hard-coded None at the S3 cell boundary.
    let s3_cell = between(
        source,
        "fn open_s3_log_cell(",
        "fn composed_storage_config(",
    );
    assert!(
        s3_cell.contains("async_projection,"),
        "open_s3_log_cell must forward async_projection"
    );
    assert!(
        s3_cell.contains("sqlite_projection_deferred_flush_chunk,"),
        "open_s3_log_cell must forward deferred-flush tuning"
    );
    assert!(
        !s3_cell.contains("AsyncProjectionSpec::default"),
        "S3 cell must not re-default the caller's AsyncProjectionSpec"
    );
    assert!(
        !s3_cell.contains("CommitResponseBarrier::Strict"),
        "S3 helpers must not hard-pin Strict"
    );
    assert!(
        !source.contains("objectlog-memory-async-pending"),
        "S3 memory-async pending rejection must be fully retired"
    );
}

fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    let start = source
        .find(start)
        .unwrap_or_else(|| panic!("missing {start}"));
    let tail = &source[start..];
    let end = tail.find(end).unwrap_or_else(|| panic!("missing {end}"));
    &tail[..end]
}

#[test]
fn all_six_s3_barrier_cells_open_with_caller_tuning() {
    let fixture = FixtureRoot::new();
    let _s3 = require_s3_env();
    let mut ordinal = 0_u8;

    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        ordinal += 1;
        let config = s3_config(
            ProjectionStoreConfig::Memory,
            barrier,
            format!("p3s-memory-{}-{}", std::process::id(), ordinal),
        );
        let handle =
            fireweed::open(config, Arc::new(SystemClock)).expect("s3×memory barrier must open");
        assert!(handle.projection_control().is_none());
        drop(handle);
        eprintln!("P3s PASS s3×memory barrier={barrier:?}");

        ordinal += 1;
        let mut config = s3_config(
            ProjectionStoreConfig::Sqlite {
                path: fixture.path().join(format!("projection-{ordinal}.sqlite")),
            },
            barrier,
            format!("p3s-sqlite-{}-{}", std::process::id(), ordinal),
        );
        config.sqlite_projection_deferred_flush_chunk = Some(7);
        let handle =
            fireweed::open(config, Arc::new(SystemClock)).expect("s3×SQLite barrier must open");
        let control = handle
            .projection_control()
            .expect("durable SQLite projection control");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build verification runtime")
            .block_on(control.verify())
            .expect("empty SQLite projection verifies");
        drop(handle);
        eprintln!("P3s PASS s3×sqlite barrier={barrier:?} deferred_flush_chunk=7");
    }

    let url = require_pg_url();
    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        ordinal += 1;
        let namespace = format!("p3s-postgres-{}-{}", std::process::id(), ordinal);
        let config = s3_config(
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(&url),
            },
            barrier,
            namespace.clone(),
        );
        let handle =
            fireweed::open(config, Arc::new(SystemClock)).expect("s3×PostgreSQL barrier must open");
        let control = handle
            .projection_control()
            .expect("durable PostgreSQL projection control");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build verification runtime")
            .block_on(control.verify())
            .expect("empty PostgreSQL projection verifies");
        drop(handle);
        drop_test_schema(&url, &namespace);
        eprintln!("P3s PASS s3×postgres barrier={barrier:?}");
    }

    assert_eq!(ordinal, 6, "exactly six S3 barrier cells");
}

#[test]
fn s3_create_queue_retains_native_cas_fail_closed_until_adapter_upgrade() {
    // docs/operator/object-log-authority-compatibility.md: the crates.io BlobStore
    // port is overwrite-only, so S3 queue definition authority fails closed even
    // when the qualification endpoint itself enforces If-None-Match:*. P3s must
    // not paper over that with a silent process-local put.
    use fireweed::{
        EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
        PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
    };

    let _s3 = require_s3_env();
    let config = s3_config(
        ProjectionStoreConfig::Memory,
        ResponseBarrier::Strict,
        format!("p3s-cas-negative-{}", std::process::id()),
    );
    let fireweed = fireweed::open(config, Arc::new(SystemClock)).expect("s3×memory opens");
    let definition = QueueDefinition {
        tenant_id: TenantId::new("p3s").unwrap(),
        queue_id: QueueId::new("cas-negative").unwrap(),
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
    };
    let err = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(fireweed.create_queue(definition))
        .expect_err("S3 create_queue must fail closed without put-if-absent");
    let text = format!("{err:?}");
    assert!(
        text.contains("create-only") || text.contains("NativeConditionalWrite"),
        "expected native create-only fail-closed text, got {text}"
    );
    eprintln!("P3s PASS s3 create_queue retains native-CAS fail-closed negative");
}

#[test]
fn s3_cells_reopen_with_namespace_segments_and_recovery_fields() {
    let fixture = FixtureRoot::new();
    let _s3 = require_s3_env();
    let namespace = format!("p3s-reopen-{}", std::process::id());

    let mut config = s3_config(
        ProjectionStoreConfig::Sqlite {
            path: fixture.path().join("reopen-projection.sqlite"),
        },
        ResponseBarrier::AsyncProjection,
        namespace.clone(),
    );
    config.segments = SegmentConfig::new(128 * 1024, 11).expect("production-safe segments");
    config.recovery = RecoveryPolicy {
        incompatible_projection: RecoveryAction::RebuildProjection,
        verify_checksums: true,
        max_tail_commands: 4_096,
    };
    config.sqlite_projection_deferred_flush_chunk = Some(3);

    let first = fireweed::open(config.clone(), Arc::new(SystemClock))
        .expect("first s3×sqlite open with nested fields");
    drop(first);

    let reopened = fireweed::open(config, Arc::new(SystemClock))
        .expect("reopen must preserve namespace/segments/recovery wiring");
    let control = reopened
        .projection_control()
        .expect("reopened SQLite control");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build verification runtime")
        .block_on(control.verify())
        .expect("reopened projection verifies");
    drop(reopened);
    eprintln!("P3s PASS s3×sqlite reopen namespace={namespace} segments+recovery+deferred-flush");
}
