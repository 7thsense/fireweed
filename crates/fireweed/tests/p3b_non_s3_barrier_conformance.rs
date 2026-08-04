use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fireweed::{
    AsyncProjectionSpec, ConfigSecret, EngineError, LogConfig, ObjectLogAuthority, PostgresMode,
    ProjectionStoreConfig, RecoveryAction, RecoveryPolicy, ResponseBarrier, SegmentConfig,
    StorageConfig, SystemClock,
};
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};

const ASYNC_REQUIRES_OBJECT_LOG: EngineError =
    EngineError::Invalid("async-projection-requires-object-log");

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fireweed-p3b-non-s3-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create fixture root");
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

fn pg_url() -> String {
    std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL is required; AC-TXN-5/5A never skip live PostgreSQL cells")
}

fn schema_for(isolation_key: &str) -> String {
    let digest = Sha256::digest(isolation_key.as_bytes());
    let mut schema = String::from("fireweed_");
    for byte in digest.iter().take(27) {
        schema.push_str(&format!("{byte:02x}"));
    }
    schema
}

fn drop_schema(url: &str, schema: &str) {
    assert!(
        schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "fixture schema must be safe to interpolate"
    );
    let mut client = Client::connect(url, NoTls).expect("connect for PostgreSQL fixture cleanup");
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .expect("drop isolated PostgreSQL fixture schema");
}

fn config(
    log: LogConfig,
    projection: ProjectionStoreConfig,
    barrier: ResponseBarrier,
    namespace: String,
) -> StorageConfig {
    StorageConfig {
        authority: matches!(log, LogConfig::Filesystem { .. })
            .then_some(ObjectLogAuthority::NativeConditionalWrite),
        log,
        projection,
        control_plane: None,
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection).then(|| {
            AsyncProjectionSpec::new(13, 65_537, 7, 12_345, 4)
                .expect("all five non-default async bounds are valid")
        }),
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(64 * 1024, 5).expect("production-safe segment bounds"),
        namespace,
        recovery: RecoveryPolicy {
            incompatible_projection: RecoveryAction::RebuildProjection,
            verify_checksums: true,
            max_tail_commands: 10_000,
        },
    }
}

fn postgres_log(url: &str, schema: &str) -> LogConfig {
    LogConfig::Postgres {
        url: ConfigSecret::new(url),
        schema: Some(schema.to_owned()),
        mode: PostgresMode::LogReplay,
        node_id: Some(1),
        coordination: None,
    }
}

fn open_and_report(cell: &str, config: StorageConfig) {
    config
        .validate()
        .unwrap_or_else(|error| panic!("AC-TXN-5 {cell} validation failed: {error}"));
    let handle = fireweed::open(config, Arc::new(SystemClock))
        .unwrap_or_else(|error| panic!("AC-TXN-5 {cell} open failed: {error}"));
    eprintln!("AC-TXN-5 PASS {cell} barrier=Strict");
    drop(handle);
}

#[test]
fn ac_txn_5_strict_opens_all_twelve_non_s3_cells() {
    let root = FixtureRoot::new("strict");
    let url = pg_url();
    let nonce = root
        .path()
        .file_name()
        .expect("fixture name")
        .to_string_lossy();
    let mut seen = BTreeSet::<String>::new();

    let memory_namespace = format!("p3b-memory-pg-{nonce}");
    let memory_cells = [
        ("memory×memory", ProjectionStoreConfig::Memory, None),
        (
            "memory×sqlite",
            ProjectionStoreConfig::Sqlite {
                path: root.path().join("memory-sqlite-projection.db"),
            },
            None,
        ),
        (
            "memory×postgres",
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(&url),
            },
            Some(schema_for(&format!("memory_pg_{memory_namespace}"))),
        ),
    ];
    for (cell, projection, cleanup) in memory_cells {
        seen.insert(cell.to_owned());
        open_and_report(
            cell,
            config(
                LogConfig::Memory,
                projection,
                ResponseBarrier::Strict,
                memory_namespace.clone(),
            ),
        );
        if let Some(schema) = cleanup {
            drop_schema(&url, &schema);
        }
    }

    for (ordinal, projection) in [
        ProjectionStoreConfig::Memory,
        ProjectionStoreConfig::Sqlite {
            path: root.path().join("sqlite-sqlite-projection.db"),
        },
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(&url),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let projection_name = projection.axis_name();
        let cell = format!("sqlite×{projection_name}");
        let log_path = root.path().join(format!("sqlite-log-{ordinal}.db"));
        let cleanup = (projection_name == "postgres")
            .then(|| schema_for(&format!("sqlite_pg_{}", log_path.to_string_lossy())));
        seen.insert(cell.clone());
        open_and_report(
            &cell,
            config(
                LogConfig::Sqlite { path: log_path },
                projection,
                ResponseBarrier::Strict,
                format!("p3b-sqlite-{ordinal}-{nonce}"),
            ),
        );
        if let Some(schema) = cleanup {
            drop_schema(&url, &schema);
        }
    }

    for (ordinal, projection) in [
        ProjectionStoreConfig::Memory,
        ProjectionStoreConfig::Sqlite {
            path: root.path().join("filesystem-sqlite-projection.db"),
        },
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(&url),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let projection_name = projection.axis_name();
        let cell = format!("filesystem×{projection_name}");
        let namespace = format!("p3b-filesystem-{ordinal}-{nonce}");
        let cleanup = (projection_name == "postgres").then(|| schema_for(namespace.as_str()));
        seen.insert(cell.clone());
        open_and_report(
            &cell,
            config(
                LogConfig::Filesystem {
                    root: root.path().join(format!("filesystem-log-{ordinal}")),
                },
                projection,
                ResponseBarrier::Strict,
                namespace,
            ),
        );
        if let Some(schema) = cleanup {
            drop_schema(&url, &schema);
        }
    }

    for (ordinal, projection) in [
        ProjectionStoreConfig::Memory,
        ProjectionStoreConfig::Sqlite {
            path: root.path().join("postgres-sqlite-projection.db"),
        },
        ProjectionStoreConfig::Postgres {
            url: ConfigSecret::new(&url),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let projection_name = projection.axis_name();
        let cell = format!("postgres×{projection_name}");
        let schema = format!("p3b_strict_{ordinal}_{}", std::process::id());
        seen.insert(cell.clone());
        open_and_report(
            &cell,
            config(
                postgres_log(&url, &schema),
                projection,
                ResponseBarrier::Strict,
                format!("p3b-postgres-{ordinal}-{nonce}"),
            ),
        );
        drop_schema(&url, &schema);
    }

    assert_eq!(seen.len(), 12, "AC-TXN-5 must enumerate twelve cells");
}

#[test]
fn ac_txn_5a_rejects_nine_non_object_log_async_cells_before_io() {
    let root = FixtureRoot::new("async-rejection");
    let never_created = root.path().join("never-created");
    let dummy_url = "postgres://127.0.0.1:1/fireweed";
    let logs = [
        ("memory", LogConfig::Memory),
        (
            "sqlite",
            LogConfig::Sqlite {
                path: never_created.join("log.db"),
            },
        ),
        ("postgres", postgres_log(dummy_url, "p3b_async_rejection")),
    ];
    let mut rejected = 0;
    for (log_name, log) in logs {
        for projection in [
            ProjectionStoreConfig::Memory,
            ProjectionStoreConfig::Sqlite {
                path: never_created.join("projection.db"),
            },
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(dummy_url),
            },
        ] {
            let cell = format!("{log_name}×{}", projection.axis_name());
            assert_eq!(
                config(
                    log.clone(),
                    projection,
                    ResponseBarrier::AsyncProjection,
                    format!("p3b-reject-{rejected}"),
                )
                .validate(),
                Err(ASYNC_REQUIRES_OBJECT_LOG),
                "AC-TXN-5A exact rejection for {cell}"
            );
            eprintln!("AC-TXN-5A PASS {cell} barrier=AsyncProjection disposition=rejected");
            rejected += 1;
        }
    }
    assert_eq!(rejected, 9);
    assert!(
        !never_created.exists(),
        "AC-TXN-5A rejection must precede storage I/O"
    );
}

#[test]
fn ac_txn_5a_filesystem_supports_both_barriers_and_all_five_bounds() {
    let root = FixtureRoot::new("filesystem-barriers");
    let url = pg_url();
    let mut opened = 0;
    for barrier in [ResponseBarrier::Strict, ResponseBarrier::AsyncProjection] {
        for projection in [
            ProjectionStoreConfig::Memory,
            ProjectionStoreConfig::Sqlite {
                path: root.path().join(format!("projection-{opened}.db")),
            },
            ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(&url),
            },
        ] {
            let projection_name = projection.axis_name();
            let namespace = format!("p3b-fs-barrier-{opened}-{}", std::process::id());
            let cleanup = (projection_name == "postgres").then(|| schema_for(namespace.as_str()));
            let config = config(
                LogConfig::Filesystem {
                    root: root.path().join(format!("log-{opened}")),
                },
                projection,
                barrier,
                namespace,
            );
            config.validate().unwrap_or_else(|error| {
                panic!("AC-TXN-5A filesystem×{projection_name} {barrier:?}: {error}")
            });
            let handle = fireweed::open(config, Arc::new(SystemClock)).unwrap_or_else(|error| {
                panic!("AC-TXN-5A filesystem×{projection_name} {barrier:?} open: {error}")
            });
            eprintln!(
                "AC-TXN-5A PASS filesystem×{projection_name} barrier={barrier:?} bounds=lag,bytes,depth,age,poison"
            );
            drop(handle);
            if let Some(schema) = cleanup {
                drop_schema(&url, &schema);
            }
            opened += 1;
        }
    }
    assert_eq!(opened, 6);
}

fn collect_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read source directory") {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn backend_semantic_hybrid_names_are_confined_to_explicit_legacy_boundaries() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let crates_root = workspace.join("crates");
    let compatibility_files = BTreeSet::from([
        "crates/fireweed-objectlog/src/async_product_hybrid.rs",
        "crates/fireweed-sqlite/src/relational/hybrid.rs",
        "crates/fireweed-sqlite/src/relational/monitor.rs",
        "crates/fireweed-sqlite/tests/async_projection_assertion_map.rs",
    ]);
    let implementation_boundaries = BTreeSet::from([
        "crates/fireweed-objectlog/src/async_product_hybrid.rs",
        "crates/fireweed-sqlite/src/relational/hybrid.rs",
        "crates/fireweed-sqlite/src/relational/monitor.rs",
    ]);
    let allowed_mixed_lines = BTreeSet::from([
        "mod async_product_hybrid;",
        "pub use async_product_hybrid::{AsyncObjectLogHybridBackend, HybridProductConfig};",
        "pub type LegacyObjectLogSqliteBackend = AsyncObjectLogHybridBackend;",
        "pub type LegacyObjectLogSqliteConfig = HybridProductConfig;",
        "HybridAsyncDebt, HybridAsyncMetrics, HybridAsyncMonitor, HybridAsyncThresholds,",
        "HybridFaultCutPoint, HybridFaultHook, HybridProjectionStore, SqliteCheckpointStore,",
        "pub type AsyncProjectionDebt = HybridAsyncDebt;",
        "pub type AsyncProjectionMetrics = HybridAsyncMetrics;",
        "pub type AsyncProjectionMonitor = HybridAsyncMonitor;",
        "pub type AsyncProjectionThresholds = HybridAsyncThresholds;",
        "pub type AsyncProjectionFaultCutPoint = HybridFaultCutPoint;",
        "pub use HybridFaultHook as AsyncProjectionFaultHook;",
        "pub type LegacySqliteProjectionStore = HybridProjectionStore;",
        "mod hybrid;",
        "pub use hybrid::*;",
        "assert_eq!(product_class_for_log_name(\"hybrid\"), None);",
    ]);
    let guard_path = "crates/fireweed/tests/p3b_non_s3_barrier_conformance.rs";

    let mut files = Vec::new();
    collect_rust_files(&crates_root, &mut files);
    files.sort();
    let mut mixed_hits = BTreeMap::new();
    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(workspace)
            .expect("workspace-relative source path")
            .to_string_lossy()
            .replace('\\', "/");
        if relative.starts_with("crates/fireweed-server/")
            || relative.starts_with("crates/fireweed-bench/")
            || relative == guard_path
        {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("read Rust source");
        if relative.to_ascii_lowercase().contains("hybrid")
            && !compatibility_files.contains(relative.as_str())
        {
            violations.push(format!("{relative}: path contains retired backend name"));
        }
        if compatibility_files.contains(relative.as_str()) {
            if implementation_boundaries.contains(relative.as_str())
                && !source.contains("LEGACY COMPATIBILITY BOUNDARY")
            {
                violations.push(format!("{relative}: missing compatibility-boundary marker"));
            }
            continue;
        }
        for (index, line) in source.lines().enumerate() {
            if !line.to_ascii_lowercase().contains("hybrid") {
                continue;
            }
            let trimmed = line.trim();
            if allowed_mixed_lines.contains(trimmed) {
                *mixed_hits.entry(trimmed.to_owned()).or_insert(0_usize) += 1;
            } else {
                violations.push(format!("{relative}:{}:{trimmed}", index + 1));
            }
        }
    }
    for allowed in &allowed_mixed_lines {
        if mixed_hits.get(*allowed) != Some(&1) {
            violations.push(format!(
                "stale or duplicated mixed-source allowance ({:?} hits): {allowed}",
                mixed_hits.get(*allowed)
            ));
        }
    }
    violations.sort();
    assert!(
        violations.is_empty(),
        "retired backend-semantic names escaped explicit compatibility boundaries:\n{}",
        violations.join("\n")
    );

    let ledger = std::fs::read_to_string(
        workspace.join("crates/fireweed-sqlite/tests/async_projection_assertion_map.rs"),
    )
    .expect("read old-to-new assertion ledger");
    assert!(ledger.contains("const ASSERTION_MAP"));
    assert!(
        ledger
            .contains("async_projection_assertion_map_binds_every_migrated_assertion_exactly_once")
    );
}
