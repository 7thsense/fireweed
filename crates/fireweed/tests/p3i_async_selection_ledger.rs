use std::path::Path;
use std::sync::Arc;

use fireweed::{
    AsyncProjectionSpec, ConfigSecret, EngineError, LogConfig, PostgresMode, ProjectionStoreConfig,
    QueueId, QueueKey, RecoveryPolicy, ResponseBarrier, SegmentConfig, StorageConfig, SystemClock,
    TenantId, open,
};
use fireweed_engine::DurabilityClass;

fn constructor_route(config: &StorageConfig) -> &'static str {
    match (&config.log, &config.projection) {
        (LogConfig::Memory, ProjectionStoreConfig::Memory) => "open_memory_log_cell::memory",
        (LogConfig::Memory, ProjectionStoreConfig::Sqlite { .. }) => "open_memory_log_cell::sqlite",
        (LogConfig::Memory, ProjectionStoreConfig::Postgres { .. }) => {
            "open_memory_log_cell::postgres"
        }
        (LogConfig::Sqlite { .. }, ProjectionStoreConfig::Memory) => "open_sqlite_log_cell::memory",
        (LogConfig::Sqlite { .. }, ProjectionStoreConfig::Sqlite { .. }) => {
            "open_sqlite_log_cell::sqlite"
        }
        (LogConfig::Sqlite { .. }, ProjectionStoreConfig::Postgres { .. }) => {
            "open_sqlite_log_cell::postgres"
        }
        (LogConfig::Postgres { .. }, ProjectionStoreConfig::Memory) => {
            "open_postgres_log_cell::memory"
        }
        (LogConfig::Postgres { .. }, ProjectionStoreConfig::Sqlite { .. }) => {
            "open_postgres_log_cell::sqlite"
        }
        (LogConfig::Postgres { .. }, ProjectionStoreConfig::Postgres { .. }) => {
            "open_postgres_log_cell::postgres"
        }
        (LogConfig::Filesystem { .. } | LogConfig::S3 { .. }, _) => {
            panic!("P3i ledger is limited to non-object-log selections")
        }
    }
}

fn config(log: &str, projection: &str, root: &Path, barrier: ResponseBarrier) -> StorageConfig {
    let postgres_url = "postgres://127.0.0.1:1/fireweed";
    StorageConfig {
        log: match log {
            "memory" => LogConfig::Memory,
            "sqlite" => LogConfig::Sqlite {
                path: root.join(format!("{barrier:?}-{projection}-log.db")),
            },
            "postgres" => LogConfig::Postgres {
                url: ConfigSecret::new(postgres_url),
                schema: Some("p3i".to_owned()),
                mode: PostgresMode::LogReplay,
                node_id: Some(1),
                coordination: None,
            },
            other => panic!("unknown P3i log axis {other}"),
        },
        projection: match projection {
            "memory" => ProjectionStoreConfig::Memory,
            "sqlite" => ProjectionStoreConfig::Sqlite {
                path: root.join(format!("{barrier:?}-{log}-projection.db")),
            },
            "postgres" => ProjectionStoreConfig::Postgres {
                url: ConfigSecret::new(postgres_url),
            },
            other => panic!("unknown P3i projection axis {other}"),
        },
        control_plane: None,
        authority: None,
        response_barrier: barrier,
        async_projection: (barrier == ResponseBarrier::AsyncProjection)
            .then(AsyncProjectionSpec::default),
        sqlite_projection_deferred_flush_chunk: None,
        segments: SegmentConfig::new(1024, 5).unwrap(),
        namespace: format!("p3i-{log}-{projection}"),
        recovery: RecoveryPolicy::default(),
    }
}

#[test]
fn p3i_non_object_async_selection_ledger() {
    let ledger: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/p3i-non-object-async-selection-ledger.json"
    ))
    .expect("P3i ledger JSON");
    let rows = ledger.as_array().expect("P3i ledger array");
    assert_eq!(rows.len(), 9);
    let root = std::env::temp_dir().join(format!(
        "fireweed-p3i-ledger-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut old_assertions = std::collections::BTreeSet::new();
    let mut successor_assertions = std::collections::BTreeSet::new();

    for row in rows {
        let log = row["log"].as_str().unwrap();
        let projection = row["projection"].as_str().unwrap();
        assert!(old_assertions.insert(row["assertion_id"].as_str().unwrap()));
        assert!(successor_assertions.insert(row["successor_assertion_id"].as_str().unwrap()));
        assert_eq!(row["current_validate"], "Ok(())");
        assert_eq!(
            row["successor_validate"],
            "Err(Invalid(async-projection-requires-object-log))"
        );
        assert_eq!(row["current_open_result"], "accepted_then_dispatch");
        assert_eq!(row["effective_barrier"], "Strict");
        assert_eq!(row["response_timing"], "after_projection_apply");
        assert_eq!(row["engine_durability_class"], "Atomic");
        assert_eq!(row["strict_equivalent"], true);
        assert_eq!(
            row["product_durability_class"],
            if log == "memory" {
                "Class B"
            } else {
                "Class A"
            }
        );

        let strict = config(log, projection, &root, ResponseBarrier::Strict);
        let asynchronous = config(log, projection, &root, ResponseBarrier::AsyncProjection);
        assert_eq!(strict.validate(), Ok(()));
        assert_eq!(
            asynchronous.validate(),
            Err(EngineError::Invalid("async-projection-requires-object-log"))
        );
        assert_eq!(
            constructor_route(&strict),
            row["constructor_route"].as_str().unwrap()
        );
        assert_eq!(
            constructor_route(&asynchronous),
            row["constructor_route"].as_str().unwrap()
        );
    }
    assert_eq!(old_assertions.len(), 9);
    assert_eq!(successor_assertions.len(), 9);

    // The four deterministic local cells retain their Strict route while the old ignored async
    // selector now fails before construction. PostgreSQL routes above are validated without I/O.
    for log in ["memory", "sqlite"] {
        for projection in ["memory", "sqlite"] {
            let strict = open(
                config(log, projection, &root, ResponseBarrier::Strict),
                Arc::new(SystemClock),
            )
            .unwrap();
            let queue = QueueKey::new(
                TenantId::new("p3i").unwrap(),
                QueueId::new("ledger").unwrap(),
            );
            let strict_fingerprint = strict.commit_capabilities(&queue).unwrap();
            assert_eq!(strict_fingerprint.durability_class, DurabilityClass::Atomic);
            let error = match open(
                config(log, projection, &root, ResponseBarrier::AsyncProjection),
                Arc::new(SystemClock),
            ) {
                Ok(_) => panic!("async non-object-log selection must fail before construction"),
                Err(error) => error,
            };
            assert_eq!(
                error,
                EngineError::Invalid("async-projection-requires-object-log")
            );
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
