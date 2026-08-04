use std::path::{Path, PathBuf};

use fireweed::{
    AsyncProjectionSpec, ConfigSecret, EngineError, LogConfig, ObjectLogAuthority, PostgresMode,
    ProjectionStoreConfig, RecoveryPolicy, ResponseBarrier, SegmentConfig, StorageConfig,
};

const ASYNC_REQUIRES_OBJECT_LOG: EngineError =
    EngineError::Invalid("async-projection-requires-object-log");

fn base(log: LogConfig, projection: ProjectionStoreConfig) -> StorageConfig {
    StorageConfig {
        log,
        projection,
        control_plane: None,
        authority: None,
        response_barrier: ResponseBarrier::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(1024, 5).unwrap(),
        namespace: "p3b-validation".to_owned(),
        recovery: RecoveryPolicy::default(),
    }
}

fn async_config(mut config: StorageConfig) -> StorageConfig {
    config.response_barrier = ResponseBarrier::AsyncProjection;
    config.async_projection = Some(AsyncProjectionSpec::default());
    config
}

fn filesystem(projection: ProjectionStoreConfig, root: &Path) -> StorageConfig {
    let mut config = base(
        LogConfig::Filesystem {
            root: root.to_owned(),
        },
        projection,
    );
    config.authority = Some(ObjectLogAuthority::NativeConditionalWrite);
    config
}

fn sqlite_projection(path: impl Into<PathBuf>) -> ProjectionStoreConfig {
    ProjectionStoreConfig::Sqlite { path: path.into() }
}

fn postgres_log() -> LogConfig {
    LogConfig::Postgres {
        url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
        schema: Some("p3b".to_owned()),
        mode: PostgresMode::LogReplay,
        node_id: Some(1),
        coordination: None,
    }
}

fn postgres_projection() -> ProjectionStoreConfig {
    ProjectionStoreConfig::Postgres {
        url: ConfigSecret::new("postgres://127.0.0.1:1/fireweed"),
    }
}

#[test]
fn response_barrier_shape_is_explicit_and_provider_neutral() {
    let root = PathBuf::from("/p3b-validation-never-opened");
    let strict = filesystem(ProjectionStoreConfig::Memory, &root);
    assert_eq!(strict.validate(), Ok(()));

    let mut strict_with_spec = strict.clone();
    strict_with_spec.async_projection = Some(AsyncProjectionSpec::default());
    strict_with_spec.segments.target_bytes = 0;
    assert_eq!(
        strict_with_spec.validate(),
        Err(EngineError::Invalid(
            "async-projection-spec-requires-async-projection-barrier"
        ))
    );

    let mut asynchronous_without_spec = strict;
    asynchronous_without_spec.response_barrier = ResponseBarrier::AsyncProjection;
    assert_eq!(
        asynchronous_without_spec.validate(),
        Err(EngineError::Invalid("async-projection-spec-required"))
    );
}

#[test]
fn each_async_projection_bound_is_validated_before_tuple_coherence() {
    let root = PathBuf::new();
    let baseline = AsyncProjectionSpec::default();
    let cases = [
        (
            AsyncProjectionSpec {
                apply_lag_max_commands: 0,
                ..baseline
            },
            "async projection bound apply_lag_max_commands must be > 0",
        ),
        (
            AsyncProjectionSpec {
                apply_debt_max_bytes: 0,
                ..baseline
            },
            "async projection bound apply_debt_max_bytes must be > 0",
        ),
        (
            AsyncProjectionSpec {
                apply_queue_depth_max: 0,
                ..baseline
            },
            "async projection bound apply_queue_depth_max must be > 0",
        ),
        (
            AsyncProjectionSpec {
                oldest_unapplied_max_ms: 0,
                ..baseline
            },
            "async projection bound oldest_unapplied_max_ms must be > 0",
        ),
        (
            AsyncProjectionSpec {
                apply_poison_retry_threshold: 0,
                ..baseline
            },
            "async projection bound apply_poison_retry_threshold must be > 0",
        ),
    ];

    for (spec, reason) in cases {
        let mut config = base(LogConfig::Memory, ProjectionStoreConfig::Memory);
        config.response_barrier = ResponseBarrier::AsyncProjection;
        config.async_projection = Some(spec);
        assert_eq!(config.validate(), Err(EngineError::Invalid(reason)));
    }
    assert!(!root.exists());
}

#[test]
fn all_nine_non_object_log_async_selections_fail_before_io() {
    let root = std::env::temp_dir().join(format!(
        "fireweed-p3b-validation-never-created-{}",
        std::process::id()
    ));
    assert!(!root.exists());

    for log in [
        LogConfig::Memory,
        LogConfig::Sqlite {
            path: root.join("log.db"),
        },
        postgres_log(),
    ] {
        for projection in [
            ProjectionStoreConfig::Memory,
            sqlite_projection(root.join("projection.db")),
            postgres_projection(),
        ] {
            assert_eq!(
                async_config(base(log.clone(), projection)).validate(),
                Err(ASYNC_REQUIRES_OBJECT_LOG)
            );
        }
    }
    assert!(!root.exists(), "validation performed filesystem I/O");
}

#[test]
fn sqlite_deferred_flush_tuning_is_independent_and_cell_scoped() {
    let root = PathBuf::from("/p3b-sqlite-tuning-never-opened");
    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        let mut config = filesystem(sqlite_projection("projection.db"), &root);
        config.response_barrier = barrier;
        config.async_projection =
            (barrier == ResponseBarrier::AsyncProjection).then(AsyncProjectionSpec::default);
        config.sqlite_projection_deferred_flush_chunk = Some(17);
        assert_eq!(config.validate(), Ok(()));
    }

    let mut zero = filesystem(sqlite_projection("projection.db"), &root);
    zero.sqlite_projection_deferred_flush_chunk = Some(0);
    assert_eq!(
        zero.validate(),
        Err(EngineError::Invalid(
            "sqlite projection deferred flush chunk must be > 0"
        ))
    );

    let mut wrong_projection = filesystem(ProjectionStoreConfig::Memory, &root);
    wrong_projection.sqlite_projection_deferred_flush_chunk = Some(17);
    assert_eq!(
        wrong_projection.validate(),
        Err(EngineError::Invalid(
            "sqlite-projection-deferred-flush-requires-sqlite-projection"
        ))
    );

    let mut wrong_log = base(LogConfig::Memory, sqlite_projection("projection.db"));
    wrong_log.sqlite_projection_deferred_flush_chunk = Some(17);
    assert_eq!(
        wrong_log.validate(),
        Err(EngineError::Invalid(
            "sqlite-projection-deferred-flush-requires-object-log"
        ))
    );
}

#[test]
fn endpoint_syntax_precedes_barrier_shape_without_io() {
    let missing = PathBuf::from("/p3b-endpoint-precedence-never-created");
    let mut filesystem = filesystem(ProjectionStoreConfig::Memory, &missing);
    filesystem.log = LogConfig::Filesystem {
        root: PathBuf::new(),
    };
    filesystem.response_barrier = ResponseBarrier::AsyncProjection;
    filesystem.segments.target_bytes = 0;
    assert_eq!(
        filesystem.validate(),
        Err(EngineError::Invalid(
            "filesystem object-log root must not be empty"
        ))
    );

    let mut s3 = base(
        LogConfig::S3 {
            endpoint: String::new(),
            bucket: "bucket".to_owned(),
            region: "region".to_owned(),
            access_key_id: ConfigSecret::new("access"),
            secret_access_key: ConfigSecret::new("secret"),
            allow_insecure_http: true,
        },
        ProjectionStoreConfig::Memory,
    );
    s3.response_barrier = ResponseBarrier::AsyncProjection;
    assert_eq!(
        s3.validate(),
        Err(EngineError::Invalid(
            "S3 object-log configuration fields must not be empty"
        ))
    );
    assert!(!missing.exists());
}
