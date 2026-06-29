//! The single OPTIONAL environment-variable populator for [`Config`] (feature `env-config`).
//!
//! This is the ONE place that knows the documented `PQUEUE_*` / `DATABRICKS_*` env NAMES and maps them onto
//! the typed [`Config`]. It is a PURE function over a caller-supplied `BTreeMap<String, String>` — it never
//! touches the process environment. The bin (`pqueue-service`) is the only caller that reads the live process
//! env (`std::env::vars().collect()`); a pure-library embedder builds [`Config`] directly and, by compiling
//! with `default-features = false`, drops this module (and all env-name knowledge) entirely.
//!
//! Behaviour (names + defaults) is preserved EXACTLY from the previous in-bin parsing so the Helm chart and
//! deployments keep working unchanged.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};

use crate::{Backend, Config, DEFAULT_RECOVERY_MAX_TAIL, SegmentConfig, resolve_node_id};

/// A rejected runtime configuration: the populator could not build a valid [`Config`] from the supplied env
/// map (unknown/unsupported backend combination, malformed `PQUEUE_BOOTSTRAP_QUEUES`, invalid segment
/// configuration, a TLS-requiring DSN met by a non-TLS build, …). The bin prints this and exits non-zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        ConfigError(message.into())
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Value of `key` if present (set-but-empty maps to `""`, mirroring `std::env::var(..).ok()`), else `default`.
fn env_or(env: &BTreeMap<String, String>, key: &str, default: &str) -> String {
    env.get(key).cloned().unwrap_or_else(|| default.to_string())
}

fn parse_usize(env: &BTreeMap<String, String>, key: &str, default: usize) -> usize {
    env.get(key)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn parse_u64(env: &BTreeMap<String, String>, key: &str, default: u64) -> u64 {
    env.get(key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_duration_ms(env: &BTreeMap<String, String>, key: &str, default_ms: u64) -> Duration {
    Duration::from_millis(parse_u64(env, key, default_ms))
}

fn unsupported_storage(log: &str, projection: &str, reason: &str) -> ConfigError {
    ConfigError::new(format!(
        "unsupported storage configuration PQUEUE_LOG_BACKEND={log} PQUEUE_PROJECTION_BACKEND={projection}: {reason}"
    ))
}

fn segment_config(env: &BTreeMap<String, String>) -> Result<SegmentConfig, ConfigError> {
    let target_bytes = parse_usize(env, "PQUEUE_SEGMENT_TARGET_BYTES", 262_144);
    let max_latency_ms = parse_u64(env, "PQUEUE_SEGMENT_MAX_LATENCY_MS", 20);
    SegmentConfig::new(target_bytes, max_latency_ms)
        .map_err(|e| ConfigError::new(format!("invalid segment configuration: {e}")))
}

fn parse_backend(env: &BTreeMap<String, String>) -> Result<Backend, ConfigError> {
    let log = env_or(env, "PQUEUE_LOG_BACKEND", "objectlog");
    let projection = env_or(env, "PQUEUE_PROJECTION_BACKEND", "inmemory");

    match (log.as_str(), projection.as_str()) {
        ("memory", "inmemory") => Ok(Backend::Memory),
        ("sqlite", "inmemory") => Ok(Backend::Sqlite(PathBuf::from(env_or(
            env,
            "PQUEUE_SQLITE_LOG_PATH",
            "/var/lib/pqueue/pqueue-log.db",
        )))),
        ("objectlog", "inmemory") => {
            let object_root = PathBuf::from(env_or(
                env,
                "PQUEUE_OBJECT_LOG_ROOT",
                "/var/lib/pqueue/object-log",
            ));
            // `file` (default) = the per-command file `ObjectLogBackend`; `segmented` = the group-commit
            // substrate over an IN-MEMORY projection (Fix B): durable via the sealed log, fast apply.
            match env_or(env, "PQUEUE_OBJECT_LOG_MODE", "file").as_str() {
                "file" => Ok(Backend::ObjectLog(object_root)),
                "segmented" => Ok(Backend::SegmentedObjectLogInMemory {
                    object_root,
                    config: segment_config(env)?,
                }),
                other => Err(unsupported_storage(
                    &log,
                    &projection,
                    &format!("unknown PQUEUE_OBJECT_LOG_MODE={other:?}; expected file|segmented"),
                )),
            }
        }
        ("objectlog", "sqlite") => {
            let object_root = PathBuf::from(env_or(
                env,
                "PQUEUE_OBJECT_LOG_ROOT",
                "/var/lib/pqueue/object-log",
            ));
            let projection_path = PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            ));
            // `file` (default) preserves the per-command object-log path; `segmented` selects the
            // group-commit substrate (one sealed segment object + one batched SQLite apply per batch).
            match env_or(env, "PQUEUE_OBJECT_LOG_MODE", "file").as_str() {
                "file" => Ok(Backend::ObjectLogSqlite {
                    object_root,
                    projection_path,
                }),
                "segmented" => Ok(Backend::SegmentedObjectLogSqlite {
                    object_root,
                    projection_path,
                    config: segment_config(env)?,
                }),
                other => Err(unsupported_storage(
                    &log,
                    &projection,
                    &format!("unknown PQUEUE_OBJECT_LOG_MODE={other:?}; expected file|segmented"),
                )),
            }
        }
        #[cfg(feature = "postgres")]
        ("postgres", "inmemory") => {
            // Resolve the DSN + optional Databricks credentials from the env names the Helm Lakebase
            // profile renders (the DSN secret is `PQUEUE_POSTGRES_LOG_DATABASE_URL`; `PQUEUE_PG_URL` is the
            // local/dev fallback). Fails closed if an sslmode=require DSN meets a non-tls build.
            crate::resolve_postgres_backend(env)
                .map_err(|reason| unsupported_storage(&log, &projection, &reason))
        }
        #[cfg(not(feature = "postgres"))]
        ("postgres", "inmemory") => Err(unsupported_storage(
            &log,
            &projection,
            "postgres adapter is wired through the blocking-safe PostgresNativeBackend, but this binary \
             was built without the `postgres` cargo feature; rebuild with `--features postgres` (or \
             `--features postgres,tls` for native-tls)",
        )),
        (_, "sqlite" | "postgres") => Err(unsupported_storage(
            &log,
            &projection,
            "the requested projection backend is not wired by pqueue-server yet",
        )),
        _ => Err(unsupported_storage(
            &log,
            &projection,
            "supported wired combinations are memory/inmemory, sqlite/inmemory, and objectlog/inmemory",
        )),
    }
}

fn queue_definition(tenant: &str, queue: &str) -> Result<QueueDefinition, ConfigError> {
    Ok(QueueDefinition {
        tenant_id: TenantId::new(tenant).map_err(|e| {
            ConfigError::new(format!("invalid tenant id in PQUEUE_BOOTSTRAP_QUEUES: {e}"))
        })?,
        queue_id: QueueId::new(queue).map_err(|e| {
            ConfigError::new(format!("invalid queue id in PQUEUE_BOOTSTRAP_QUEUES: {e}"))
        })?,
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
        request_id_retention_ms: 60_000,
        client_item_key_retention_ms: 60_000,
        terminal_retention_ms: 60_000,
        max_lease_duration_ms: 60_000,
        retry_policy: RetryPolicy { max_attempts: 3 },
        max_push_batch_size: 100,
        max_claim_batch_size: 100,
        max_eligible_group_size: None,
        secondary_indexes: vec![],
    })
}

fn parse_bootstrap_queues(
    env: &BTreeMap<String, String>,
) -> Result<Vec<QueueDefinition>, ConfigError> {
    env_or(env, "PQUEUE_BOOTSTRAP_QUEUES", "t1:q1")
        .split(',')
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            Some(match trimmed.split_once(':') {
                Some((tenant, queue)) => queue_definition(tenant, queue),
                None => Err(ConfigError::new(format!(
                    "invalid PQUEUE_BOOTSTRAP_QUEUES entry {trimmed:?}; expected tenant:queue"
                ))),
            })
        })
        .collect()
}

impl Config {
    /// The SINGLE optional env-var populator: map the documented `PQUEUE_*` / `DATABRICKS_*` env names in
    /// `env` onto a typed [`Config`]. PURE over the supplied map — it does NOT read the process environment
    /// (the bin's `main` collects `std::env::vars()` and passes the map in). Available only with the
    /// `env-config` feature (default-on for the bin); a library embedder can drop it via `default-features = false`.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Config, ConfigError> {
        Ok(Config {
            backend: parse_backend(env)?,
            node_id: resolve_node_id(&env_or(env, "PQUEUE_NODE_ID", "0")),
            listen: env_or(env, "PQUEUE_LISTEN_ADDR", "0.0.0.0:8080"),
            reclaim_interval: parse_duration_ms(env, "PQUEUE_RECLAIM_INTERVAL_MS", 1_000),
            queues: parse_bootstrap_queues(env)?,
            recovery_max_tail: parse_u64(
                env,
                "PQUEUE_RECOVERY_MAX_TAIL_COMMANDS",
                DEFAULT_RECOVERY_MAX_TAIL,
            ),
            // Matches the prior `std::env::var(..).is_ok()` semantics: present (even empty) enables telemetry.
            debug_segments: env.contains_key("PQUEUE_DEBUG_SEGMENTS"),
            // `>0` caps the tokio worker pool; absent/0 = one worker per core (the bin builds the runtime).
            worker_threads: env
                .get("PQUEUE_WORKER_THREADS")
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| *n > 0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_when_env_is_empty() {
        let config = Config::from_env(&BTreeMap::new()).expect("empty env yields defaults");
        assert!(matches!(config.backend, Backend::ObjectLog(_)));
        assert_eq!(config.node_id, 0);
        assert_eq!(config.listen, "0.0.0.0:8080");
        assert_eq!(config.reclaim_interval, Duration::from_millis(1_000));
        assert_eq!(config.recovery_max_tail, DEFAULT_RECOVERY_MAX_TAIL);
        assert!(!config.debug_segments);
        assert_eq!(config.worker_threads, None);
        assert_eq!(config.queues.len(), 1, "default bootstrap is t1:q1");
        assert_eq!(config.queues[0].tenant_id.as_str(), "t1");
        assert_eq!(config.queues[0].queue_id.as_str(), "q1");
    }

    #[test]
    fn memory_backend_and_scalar_knobs_are_honored() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "memory"),
            ("PQUEUE_PROJECTION_BACKEND", "inmemory"),
            ("PQUEUE_NODE_ID", "7"),
            ("PQUEUE_LISTEN_ADDR", "127.0.0.1:6390"),
            ("PQUEUE_WORKER_THREADS", "4"),
            ("PQUEUE_RECLAIM_INTERVAL_MS", "250"),
            ("PQUEUE_RECOVERY_MAX_TAIL_COMMANDS", "42"),
            ("PQUEUE_DEBUG_SEGMENTS", "1"),
            ("PQUEUE_BOOTSTRAP_QUEUES", "ta:qa,tb:qb"),
        ]))
        .expect("valid env");
        assert!(matches!(config.backend, Backend::Memory));
        assert_eq!(config.node_id, 7);
        assert_eq!(config.listen, "127.0.0.1:6390");
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(config.reclaim_interval, Duration::from_millis(250));
        assert_eq!(config.recovery_max_tail, 42);
        assert!(config.debug_segments);
        assert_eq!(config.queues.len(), 2);
        assert_eq!(config.queues[1].queue_id.as_str(), "qb");
    }

    #[test]
    fn sqlite_log_path_is_threaded() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "sqlite"),
            ("PQUEUE_SQLITE_LOG_PATH", "/data/log.db"),
        ]))
        .expect("valid env");
        match config.backend {
            Backend::Sqlite(path) => assert_eq!(path, PathBuf::from("/data/log.db")),
            _ => panic!("expected Backend::Sqlite"),
        }
    }

    #[test]
    fn segmented_objectlog_sqlite_carries_segment_config_and_paths() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_PROJECTION_BACKEND", "sqlite"),
            ("PQUEUE_OBJECT_LOG_MODE", "segmented"),
            ("PQUEUE_OBJECT_LOG_ROOT", "/data/olog"),
            ("PQUEUE_SQLITE_PROJECTION_PATH", "/data/proj.db"),
            ("PQUEUE_SEGMENT_TARGET_BYTES", "131072"),
            ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "5"),
        ]))
        .expect("valid env");
        match config.backend {
            Backend::SegmentedObjectLogSqlite {
                object_root,
                projection_path,
                config: segment,
            } => {
                assert_eq!(object_root, PathBuf::from("/data/olog"));
                assert_eq!(projection_path, PathBuf::from("/data/proj.db"));
                assert_eq!(segment.target_bytes, 131_072);
                assert_eq!(segment.max_latency_ms, 5);
            }
            _ => panic!("expected SegmentedObjectLogSqlite"),
        }
    }

    #[test]
    fn unknown_object_log_mode_is_rejected() {
        let result = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_OBJECT_LOG_MODE", "bogus"),
        ]));
        let Err(err) = result else {
            panic!("unknown mode must fail");
        };
        assert!(err.0.contains("PQUEUE_OBJECT_LOG_MODE"), "{}", err.0);
    }

    #[test]
    fn malformed_bootstrap_queue_is_rejected() {
        let result = Config::from_env(&map(&[("PQUEUE_BOOTSTRAP_QUEUES", "no-colon-here")]));
        let Err(err) = result else {
            panic!("missing colon must fail");
        };
        assert!(err.0.contains("PQUEUE_BOOTSTRAP_QUEUES"), "{}", err.0);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn lakebase_postgres_env_resolves_with_tls_and_databricks_credentials() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "postgres"),
            ("PQUEUE_PROJECTION_BACKEND", "inmemory"),
            (
                "PQUEUE_POSTGRES_LOG_DATABASE_URL",
                "postgres://app:native-password@instance.lakebase.cloud:5432/db?sslmode=require",
            ),
            ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
            ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
            ("DATABRICKS_CLIENT_ID", "sp-client"),
            ("DATABRICKS_CLIENT_SECRET", "sp-secret"),
        ]))
        .expect("Lakebase env resolves without a live DB");
        match config.backend {
            Backend::PostgresNative { url, credentials } => {
                assert!(url.contains("sslmode=require"));
                assert!(
                    credentials.is_some(),
                    "Databricks service-principal env must inject a credential provider"
                );
            }
            _ => panic!("postgres env must select Backend::PostgresNative"),
        }
    }
}
