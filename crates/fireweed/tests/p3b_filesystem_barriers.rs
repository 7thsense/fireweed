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
            "fireweed-p3b-filesystem-barriers-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
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

fn filesystem_config(
    root: PathBuf,
    projection: ProjectionStoreConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> StorageConfig {
    StorageConfig {
        log: LogConfig::Filesystem { root },
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

#[test]
fn all_six_filesystem_barrier_cells_open_with_caller_tuning() {
    let fixture = FixtureRoot::new();
    let mut ordinal = 0_u8;

    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        ordinal += 1;
        let config = filesystem_config(
            fixture.path().join(format!("memory-log-{ordinal}")),
            ProjectionStoreConfig::Memory,
            barrier,
            format!("p3b-memory-{ordinal}"),
        );
        let handle = fireweed::open(config, Arc::new(SystemClock))
            .expect("filesystem×memory barrier must open");
        assert!(handle.projection_control().is_none());
        drop(handle);

        ordinal += 1;
        let mut config = filesystem_config(
            fixture.path().join(format!("sqlite-log-{ordinal}")),
            ProjectionStoreConfig::Sqlite {
                path: fixture.path().join(format!("projection-{ordinal}.sqlite")),
            },
            barrier,
            format!("p3b-sqlite-{ordinal}"),
        );
        config.sqlite_projection_deferred_flush_chunk = Some(7);
        let handle = fireweed::open(config, Arc::new(SystemClock))
            .expect("filesystem×SQLite barrier must open");
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
    }

    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL is required for all six filesystem barrier cells");
    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        ordinal += 1;
        let namespace = format!("p3b-postgres-{}-{}", std::process::id(), ordinal);
        let config = filesystem_config(
            fixture.path().join(format!("postgres-log-{ordinal}")),
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(&url),
            },
            barrier,
            namespace.clone(),
        );
        let handle = fireweed::open(config, Arc::new(SystemClock))
            .expect("filesystem×PostgreSQL barrier must open");
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
    }
}

#[test]
fn filesystem_pins_are_retired_but_s3_transitions_are_unchanged() {
    let fixture = FixtureRoot::new();
    for projection in [
        ProjectionStoreConfig::Memory,
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
        },
    ] {
        let config = filesystem_config(
            fixture.path().join(projection.axis_name()),
            projection,
            ResponseBarrier::AsyncProjection,
            "p3b-retired-pin".to_owned(),
        );
        assert_eq!(config.validate(), Ok(()));
    }

    let mut s3_memory = filesystem_config(
        fixture.path().join("unused"),
        ProjectionStoreConfig::Memory,
        ResponseBarrier::AsyncProjection,
        "p3b-s3-memory".to_owned(),
    );
    s3_memory.log = LogConfig::S3 {
        endpoint: "http://127.0.0.1:1".to_owned(),
        bucket: "fireweed".to_owned(),
        region: "us-east-1".to_owned(),
        access_key_id: ConfigSecret::new("access"),
        secret_access_key: ConfigSecret::new("secret"),
        allow_insecure_http: true,
    };
    assert_eq!(
        s3_memory.validate(),
        Err(EngineError::Invalid("objectlog-memory-async-pending"))
    );

    let mut s3_postgres = s3_memory;
    s3_postgres.projection = ProjectionStoreConfig::Postgres {
        url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
    };
    assert_eq!(s3_postgres.validate(), Err(EngineError::Unavailable));
}

#[test]
fn exact_invalid_neighbors_and_facade_routing_are_guarded() {
    let fixture = FixtureRoot::new();
    let mut missing_spec = filesystem_config(
        fixture.path().join("missing-spec"),
        ProjectionStoreConfig::Memory,
        ResponseBarrier::AsyncProjection,
        "p3b-missing-spec".to_owned(),
    );
    missing_spec.async_projection = None;
    assert_eq!(
        missing_spec.validate(),
        Err(EngineError::Invalid("async-projection-spec-required"))
    );

    let mut strict_with_spec = missing_spec;
    strict_with_spec.response_barrier = ResponseBarrier::Strict;
    strict_with_spec.async_projection = Some(non_default_spec());
    assert_eq!(
        strict_with_spec.validate(),
        Err(EngineError::Invalid(
            "async-projection-spec-requires-async-projection-barrier"
        ))
    );

    let mut wrong_chunk = filesystem_config(
        fixture.path().join("wrong-chunk"),
        ProjectionStoreConfig::Memory,
        ResponseBarrier::Strict,
        "p3b-wrong-chunk".to_owned(),
    );
    wrong_chunk.sqlite_projection_deferred_flush_chunk = Some(7);
    assert_eq!(
        wrong_chunk.validate(),
        Err(EngineError::Invalid(
            "sqlite-projection-deferred-flush-requires-sqlite-projection"
        ))
    );

    // This source guard complements the public construction proof: it freezes the
    // caller-owned values at every private conversion boundary where a future default
    // could otherwise erase them without changing validation behavior.
    let source = include_str!("../src/lib.rs");
    assert!(source.contains("config.async_projection,"));
    assert!(source.contains("config.sqlite_projection_deferred_flush_chunk,"));
    assert!(source.contains("from_log_store_with_async_projection"));
    assert!(source.contains("from_log_and_projection_with_async_projection"));
    assert!(source.contains("sqlite_projection_deferred_flush_chunk\n        .unwrap_or"));
}
