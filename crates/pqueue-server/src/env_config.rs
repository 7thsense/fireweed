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
use pqueue_sqlite::{DEFAULT_DEFERRED_FLUSH_CHUNK, HybridAsyncThresholds};

use crate::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, DEFAULT_RECOVERY_MAX_TAIL,
    EmbeddedFjordConfig, LogSpec, ObjectLogSpec, ProjectionSpec, S3CredentialSource, SegmentConfig,
    resolve_node_id,
};

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

/// The group-commit segment configuration for the segmented object-log families (byte-size + latency seal
/// triggers), from `PQUEUE_SEGMENT_TARGET_BYTES` / `PQUEUE_SEGMENT_MAX_LATENCY_MS`.
fn segment_config(env: &BTreeMap<String, String>) -> Result<SegmentConfig, ConfigError> {
    let target_bytes = parse_usize(env, "PQUEUE_SEGMENT_TARGET_BYTES", 262_144);
    let max_latency_ms = parse_u64(env, "PQUEUE_SEGMENT_MAX_LATENCY_MS", 20);
    SegmentConfig::new(target_bytes, max_latency_ms)
        .map_err(|e| ConfigError::new(format!("invalid segment configuration: {e}")))
}

fn parse_u32(env: &BTreeMap<String, String>, key: &str, default: u32) -> u32 {
    env.get(key)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

/// The `objectlog/hybrid-async` async-apply debt bounds (bead pqueue-6da52695): hard lag/bytes/depth/age
/// limits and the apply-retry poison threshold that drive backpressure and fail-closed poison, from the
/// `PQUEUE_HYBRID_ASYNC_*` env names. A zero bound is rejected by [`HybridAsyncThresholds::new`] (it would
/// leave the queue instantly backpressured), surfaced here as a [`ConfigError`].
fn hybrid_async_thresholds(
    env: &BTreeMap<String, String>,
) -> Result<HybridAsyncThresholds, ConfigError> {
    let d = HybridAsyncThresholds::default();
    HybridAsyncThresholds::new(
        parse_u64(
            env,
            "PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS",
            d.apply_lag_max_commands,
        ),
        parse_u64(
            env,
            "PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES",
            d.apply_debt_max_bytes,
        ),
        parse_u64(
            env,
            "PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX",
            d.apply_queue_depth_max,
        ),
        parse_u64(
            env,
            "PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS",
            d.oldest_unapplied_max_ms,
        ),
        parse_u32(
            env,
            "PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD",
            d.apply_poison_retry_threshold,
        ),
    )
    .map_err(|e| ConfigError::new(format!("invalid hybrid-async threshold configuration: {e}")))
}

fn parse_bool(env: &BTreeMap<String, String>, key: &str, default: bool) -> bool {
    env.get(key)
        .and_then(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn change_record_sink_config(
    env: &BTreeMap<String, String>,
) -> Result<ChangeRecordSinkConfig, ConfigError> {
    let mut config = ChangeRecordSinkConfig::default();
    config.endpoint = env
        .get("PQUEUE_CHANGE_RECORD_SINK_ENDPOINT")
        .cloned()
        .filter(|v| !v.trim().is_empty());
    config.enabled = parse_bool(env, "PQUEUE_CHANGE_RECORD_SINK_ENABLED", false);
    config.tick_interval = parse_duration_ms(
        env,
        "PQUEUE_CHANGE_RECORD_SINK_TICK_INTERVAL_MS",
        config.tick_interval.as_millis() as u64,
    );
    config.batch_size = parse_usize(
        env,
        "PQUEUE_CHANGE_RECORD_SINK_BATCH_SIZE",
        config.batch_size,
    );
    if let Some(value) = env
        .get("PQUEUE_CHANGE_RECORD_SINK_AUTHORIZATION")
        .cloned()
        .filter(|v| !v.trim().is_empty())
    {
        config.headers.insert("authorization".to_string(), value);
    }
    for (key, value) in env {
        if let Some(name) = key.strip_prefix("PQUEUE_CHANGE_RECORD_SINK_HEADER_")
            && !value.trim().is_empty()
        {
            let header = name.replace('_', "-").to_ascii_lowercase();
            config.headers.insert(header, value.clone());
        }
    }
    // An enabled sink with no endpoint selects the in-process Embedded mode (the default, ADR-014); no
    // endpoint is required. A present endpoint still selects Http (`http://`) or ExternalKafka (`kafka://`).
    if config.enabled && config.batch_size == 0 {
        return Err(ConfigError::new(
            "PQUEUE_CHANGE_RECORD_SINK_BATCH_SIZE must be greater than 0",
        ));
    }
    if config.enabled && config.tick_interval.is_zero() {
        return Err(ConfigError::new(
            "PQUEUE_CHANGE_RECORD_SINK_TICK_INTERVAL_MS must be greater than 0",
        ));
    }
    config
        .validate()
        .map_err(|e| ConfigError::new(e.to_string()))?;
    Ok(config)
}

fn unsupported_storage(log: &str, projection: &str, reason: &str) -> ConfigError {
    ConfigError::new(format!(
        "unsupported storage configuration PQUEUE_LOG_BACKEND={log} PQUEUE_PROJECTION_BACKEND={projection}: {reason}"
    ))
}

/// Map the documented `PQUEUE_LOG_BACKEND` × `PQUEUE_PROJECTION_BACKEND` env names onto the typed two-axis
/// [`BackendSpec`] (ADR-012). Each axis is parsed independently, then the pairing is validated against the
/// set pqueue-server actually wires. The `PQUEUE_OBJECT_LOG_MODE` pseudo-axis is retired — the object log's
/// only production form is the segmented group-commit substrate, which the composed `ObjectLog` axis is.
fn required_nonempty(env: &BTreeMap<String, String>, key: &str) -> Result<String, ConfigError> {
    env.get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| ConfigError::new(format!("{key} is required and must not be empty")))
}

fn object_log_spec(
    env: &BTreeMap<String, String>,
    segments: SegmentConfig,
) -> Result<ObjectLogSpec, ConfigError> {
    const S3_PREFIX: &str = "PQUEUE_OBJECT_LOG_S3_";
    let profile = env_or(env, "PQUEUE_OBJECT_LOG_STORE", "local");
    match profile.as_str() {
        "local" => {
            if let Some((key, _)) = env.iter().find(|(key, _)| key.starts_with(S3_PREFIX)) {
                return Err(ConfigError::new(format!(
                    "{key} is set while PQUEUE_OBJECT_LOG_STORE=local; refusing to ignore shared S3 configuration and fall back to node-local storage"
                )));
            }
            Ok(ObjectLogSpec::local(
                PathBuf::from(env_or(
                    env,
                    "PQUEUE_OBJECT_LOG_ROOT",
                    "/var/lib/pqueue/object-log",
                )),
                segments,
            ))
        }
        "s3" => {
            if env.contains_key("PQUEUE_OBJECT_LOG_ROOT") {
                return Err(ConfigError::new(
                    "PQUEUE_OBJECT_LOG_ROOT is local-only and must not be set when PQUEUE_OBJECT_LOG_STORE=s3",
                ));
            }
            let endpoint = required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_ENDPOINT")?;
            let bucket = required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_BUCKET")?;
            let region = required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_REGION")?;
            let credential_source =
                required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE")?;
            if credential_source != "static" {
                return Err(ConfigError::new(format!(
                    "unsupported PQUEUE_OBJECT_LOG_S3_CREDENTIAL_SOURCE={credential_source:?}; expected static"
                )));
            }
            let access_key_id = required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_ACCESS_KEY_ID")?;
            let secret_access_key =
                required_nonempty(env, "PQUEUE_OBJECT_LOG_S3_SECRET_ACCESS_KEY")?;
            let allow_insecure_http = match env.get("PQUEUE_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP") {
                None => false,
                Some(value) if matches!(value.as_str(), "1" | "true") => true,
                Some(value) if matches!(value.as_str(), "0" | "false") => false,
                Some(value) => {
                    return Err(ConfigError::new(format!(
                        "PQUEUE_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP must be true|false|1|0, got {value:?}"
                    )));
                }
            };
            let spec = ObjectLogSpec::S3 {
                endpoint,
                bucket,
                region,
                credentials: S3CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                },
                segment_config: segments,
                allow_insecure_http,
            };
            spec.validate().map_err(|error| {
                ConfigError::new(format!("invalid S3 object-log configuration: {error}"))
            })?;
            Ok(spec)
        }
        other => Err(ConfigError::new(format!(
            "unknown PQUEUE_OBJECT_LOG_STORE={other:?}; expected local|s3"
        ))),
    }
}

fn parse_backend(
    env: &BTreeMap<String, String>,
    segments: SegmentConfig,
) -> Result<BackendSpec, ConfigError> {
    let log = env_or(env, "PQUEUE_LOG_BACKEND", "objectlog");
    let projection = env_or(env, "PQUEUE_PROJECTION_BACKEND", "inmemory");

    let log_spec = match log.as_str() {
        "memory" => LogSpec::Memory,
        "sqlite" => LogSpec::Sqlite {
            path: PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_LOG_PATH",
                "/var/lib/pqueue/pqueue-log.db",
            )),
        },
        "objectlog" => LogSpec::ObjectLog(object_log_spec(env, segments)?),
        #[cfg(feature = "postgres")]
        "postgres" => {
            // Resolve the DSN + optional Databricks credentials from the env names the Helm Lakebase
            // profile renders (DSN secret `PQUEUE_POSTGRES_LOG_DATABASE_URL`; `PQUEUE_PG_URL` is the
            // local/dev fallback). Fails closed if an sslmode=require DSN meets a non-tls build.
            crate::resolve_postgres_log(env)
                .map_err(|reason| unsupported_storage(&log, &projection, &reason))?
        }
        #[cfg(not(feature = "postgres"))]
        "postgres" => {
            return Err(unsupported_storage(
                &log,
                &projection,
                "postgres adapter is wired through the blocking-safe PostgresNativeBackend, but this \
                 binary was built without the `postgres` cargo feature; rebuild with `--features \
                 postgres` (or `--features postgres,tls` for native-tls)",
            ));
        }
        other => {
            return Err(unsupported_storage(
                &log,
                &projection,
                &format!(
                    "unknown PQUEUE_LOG_BACKEND={other:?}; expected memory|sqlite|objectlog|postgres"
                ),
            ));
        }
    };

    let projection_spec = match projection.as_str() {
        "inmemory" => ProjectionSpec::InMemory,
        "sqlite" => ProjectionSpec::Sqlite {
            path: PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            )),
        },
        "hybrid" => ProjectionSpec::Hybrid {
            path: PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            )),
        },
        // The `objectlog/hybrid-strict` profile (TD-004): same hot-memory-over-durable-SQLite substrate as
        // `hybrid`, but the group-commit write path commits the sealed batch DURABLY to SQLite BEFORE applying
        // it to hot memory (`apply_durable_then_memory`). Selected under its canonical name so the SQLite
        // durable-before-visible barrier and the SQLite-commit-then-memory-fail poison cut land on the real
        // server write pipeline.
        "hybrid-strict" => ProjectionSpec::HybridStrict {
            path: PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            )),
        },
        // The `objectlog/hybrid-async` profile (TD-004): same hot-memory-over-durable-SQLite substrate as
        // `hybrid`, selected under its canonical name so the deployment carries the async-apply threshold
        // config (`PQUEUE_HYBRID_ASYNC_*`, already parsed into `Config::hybrid_async`).
        "hybrid-async" => ProjectionSpec::HybridAsync {
            path: PathBuf::from(env_or(
                env,
                "PQUEUE_SQLITE_PROJECTION_PATH",
                "/var/lib/pqueue/pqueue-projection.db",
            )),
        },
        #[cfg(feature = "postgres")]
        "postgres" => {
            // Resolve the DSN from the env names the Helm chart's `storage.projection.postgres` axis
            // renders (DSN secret `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL`; `PQUEUE_PG_PROJECTION_URL` is
            // the local/dev fallback). Fails closed if an sslmode=require DSN meets a non-tls build.
            crate::resolve_postgres_projection(env)
                .map_err(|reason| unsupported_storage(&log, &projection, &reason))?
        }
        #[cfg(not(feature = "postgres"))]
        "postgres" => {
            return Err(unsupported_storage(
                &log,
                &projection,
                "postgres projection adapter is wired through the blocking-safe PostgresRelational store, \
                 but this binary was built without the `postgres` cargo feature; rebuild with `--features \
                 postgres` (or `--features postgres,tls` for native-tls)",
            ));
        }
        other => {
            return Err(unsupported_storage(
                &log,
                &projection,
                &format!(
                    "unknown PQUEUE_PROJECTION_BACKEND={other:?}; expected inmemory|sqlite|hybrid|hybrid-strict|hybrid-async|postgres"
                ),
            ));
        }
    };

    // Only specific log×projection pairings are wired (preserve the prior behavior): memory/inmemory,
    // sqlite/inmemory, objectlog/inmemory, objectlog/sqlite, and (with the feature) postgres/inmemory,
    // postgres/sqlite, postgres/postgres.
    let wired = match (&log_spec, &projection_spec) {
        (LogSpec::Memory, ProjectionSpec::InMemory) => true,
        (LogSpec::Sqlite { .. }, ProjectionSpec::InMemory) => true,
        (LogSpec::ObjectLog(_), ProjectionSpec::InMemory) => true,
        (LogSpec::ObjectLog(_), ProjectionSpec::Sqlite { .. }) => true,
        (LogSpec::ObjectLog(_), ProjectionSpec::Hybrid { .. }) => true,
        (LogSpec::ObjectLog(_), ProjectionSpec::HybridStrict { .. }) => true,
        (LogSpec::ObjectLog(_), ProjectionSpec::HybridAsync { .. }) => true,
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { .. }, ProjectionSpec::InMemory) => true,
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { .. }, ProjectionSpec::Sqlite { .. }) => true,
        #[cfg(feature = "postgres")]
        (LogSpec::Postgres { .. }, ProjectionSpec::Postgres { .. }) => true,
        _ => false,
    };
    if !wired {
        return Err(unsupported_storage(
            &log,
            &projection,
            "this PQUEUE_LOG_BACKEND × PQUEUE_PROJECTION_BACKEND pairing is not wired by pqueue-server",
        ));
    }

    Ok(BackendSpec {
        log: log_spec,
        projection: projection_spec,
        control_plane: ControlPlaneSpec::InProcess,
    })
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
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
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

fn embedded_fjord_config(env: &BTreeMap<String, String>) -> EmbeddedFjordConfig {
    EmbeddedFjordConfig {
        namespace_root: PathBuf::from(env_or(
            env,
            "PQUEUE_FJORD_STATE_ROOT",
            "/var/lib/pqueue/fjord",
        )),
        cluster_id: env_or(env, "PQUEUE_FJORD_CLUSTER_ID", "pqueue-fjord"),
        // Optional external-consumer TCP surface for the in-process embedded change log. Unset keeps the
        // change log purely in-process (no socket on the write path).
        broker_listen: env
            .get("PQUEUE_FJORD_BROKER_LISTEN")
            .cloned()
            .filter(|v| !v.trim().is_empty()),
    }
}

impl Config {
    /// The SINGLE optional env-var populator: map the documented `PQUEUE_*` / `DATABRICKS_*` env names in
    /// `env` onto a typed [`Config`]. PURE over the supplied map — it does NOT read the process environment
    /// (the bin's `main` collects `std::env::vars()` and passes the map in). Available only with the
    /// `env-config` feature (default-on for the bin); a library embedder can drop it via `default-features = false`.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Config, ConfigError> {
        let segments = segment_config(env)?;
        Ok(Config {
            backend: parse_backend(env, segments)?,
            embedded_fjord: embedded_fjord_config(env),
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
            hybrid_async: hybrid_async_thresholds(env)?,
            deferred_flush_chunk: parse_usize(
                env,
                "PQUEUE_HYBRID_DEFERRED_FLUSH_CHUNK",
                DEFAULT_DEFERRED_FLUSH_CHUNK,
            ),
            change_record_sink: change_record_sink_config(env)?,
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
        assert!(matches!(config.backend.log, LogSpec::ObjectLog(_)));
        assert!(matches!(
            config.backend.projection,
            ProjectionSpec::InMemory
        ));
        assert_eq!(
            config.embedded_fjord.namespace_root,
            PathBuf::from("/var/lib/pqueue/fjord")
        );
        assert_eq!(config.embedded_fjord.cluster_id, "pqueue-fjord");
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
        assert!(matches!(config.backend.log, LogSpec::Memory));
        assert!(matches!(
            config.backend.projection,
            ProjectionSpec::InMemory
        ));
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
        match config.backend.log {
            LogSpec::Sqlite { path } => assert_eq!(path, PathBuf::from("/data/log.db")),
            _ => panic!("expected LogSpec::Sqlite"),
        }
        assert!(matches!(
            config.backend.projection,
            ProjectionSpec::InMemory
        ));
    }

    #[test]
    fn objectlog_sqlite_projection_carries_paths_and_segment_config() {
        // The object log's only production form is the segmented group-commit substrate; the retired
        // `PQUEUE_OBJECT_LOG_MODE` knob is ignored, and the projection axis is the derived sqlite store.
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_PROJECTION_BACKEND", "sqlite"),
            ("PQUEUE_OBJECT_LOG_ROOT", "/data/olog"),
            ("PQUEUE_SQLITE_PROJECTION_PATH", "/data/proj.db"),
            ("PQUEUE_SEGMENT_TARGET_BYTES", "131072"),
            ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "5"),
        ]))
        .expect("valid env");
        match (config.backend.log, config.backend.projection) {
            (
                LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem {
                    root,
                    segment_config,
                }),
                ProjectionSpec::Sqlite { path },
            ) => {
                assert_eq!(root, PathBuf::from("/data/olog"));
                assert_eq!(path, PathBuf::from("/data/proj.db"));
                assert_eq!(segment_config.target_bytes, 131_072);
                assert_eq!(segment_config.max_latency_ms, 5);
            }
            _ => panic!("expected objectlog log × sqlite projection"),
        }
    }

    #[test]
    fn objectlog_hybrid_projection_carries_sqlite_path_and_segment_config() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_PROJECTION_BACKEND", "hybrid"),
            ("PQUEUE_OBJECT_LOG_ROOT", "/data/olog"),
            ("PQUEUE_SQLITE_PROJECTION_PATH", "/data/hybrid.db"),
            ("PQUEUE_SEGMENT_TARGET_BYTES", "65536"),
            ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "7"),
        ]))
        .expect("valid hybrid env");
        match (config.backend.log, config.backend.projection) {
            (
                LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem {
                    root,
                    segment_config,
                }),
                ProjectionSpec::Hybrid { path },
            ) => {
                assert_eq!(root, PathBuf::from("/data/olog"));
                assert_eq!(path, PathBuf::from("/data/hybrid.db"));
                assert_eq!(segment_config.target_bytes, 65_536);
                assert_eq!(segment_config.max_latency_ms, 7);
            }
            _ => panic!("expected objectlog log × hybrid projection"),
        }
    }

    #[test]
    fn fjord_namespace_config_is_parsed_from_env() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_FJORD_STATE_ROOT", "/data/fjord"),
            ("PQUEUE_FJORD_CLUSTER_ID", "fjord-test"),
        ]))
        .expect("valid fjord namespace env");
        assert_eq!(
            config.embedded_fjord.namespace_root,
            PathBuf::from("/data/fjord")
        );
        assert_eq!(config.embedded_fjord.cluster_id, "fjord-test");
    }

    #[test]
    fn objectlog_hybrid_strict_projection_selects_profile_and_carries_sqlite_path() {
        // The canonical `objectlog/hybrid-strict` runtime profile (TD-004): the object-log log axis paired
        // with the hybrid-strict projection, carrying the sqlite durable-projection path. This is the profile
        // whose group-commit write path commits SQLite durably BEFORE hot memory.
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_PROJECTION_BACKEND", "hybrid-strict"),
            ("PQUEUE_OBJECT_LOG_ROOT", "/data/olog"),
            ("PQUEUE_SQLITE_PROJECTION_PATH", "/data/hybrid-strict.db"),
            ("PQUEUE_SEGMENT_TARGET_BYTES", "65536"),
            ("PQUEUE_SEGMENT_MAX_LATENCY_MS", "9"),
        ]))
        .expect("valid hybrid-strict env");
        match (config.backend.log, config.backend.projection) {
            (
                LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem {
                    root,
                    segment_config,
                }),
                ProjectionSpec::HybridStrict { path },
            ) => {
                assert_eq!(root, PathBuf::from("/data/olog"));
                assert_eq!(path, PathBuf::from("/data/hybrid-strict.db"));
                assert_eq!(segment_config.target_bytes, 65_536);
                assert_eq!(segment_config.max_latency_ms, 9);
            }
            _ => panic!("expected objectlog log × hybrid-strict projection"),
        }
    }

    #[test]
    fn non_objectlog_hybrid_strict_pairing_is_rejected() {
        // Only the object-log log axis pairs with hybrid-strict; any other log backend fails closed.
        for log in ["memory", "sqlite"] {
            let result = Config::from_env(&map(&[
                ("PQUEUE_LOG_BACKEND", log),
                ("PQUEUE_PROJECTION_BACKEND", "hybrid-strict"),
            ]));
            let Err(err) = result else {
                panic!("{log}/hybrid-strict must not be wired");
            };
            assert!(
                err.0.contains("PQUEUE_PROJECTION_BACKEND=hybrid-strict"),
                "{}",
                err.0
            );
        }
    }

    #[test]
    fn objectlog_hybrid_async_projection_selects_profile_and_carries_paths_and_thresholds() {
        // The canonical `objectlog/hybrid-async` runtime profile: the object-log log axis paired with the
        // hybrid-async projection, carrying both the sqlite checkpoint path and the async-apply thresholds.
        let config = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "objectlog"),
            ("PQUEUE_PROJECTION_BACKEND", "hybrid-async"),
            ("PQUEUE_OBJECT_LOG_ROOT", "/data/olog"),
            ("PQUEUE_SQLITE_PROJECTION_PATH", "/data/hybrid-async.db"),
            ("PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS", "4096"),
            ("PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD", "9"),
        ]))
        .expect("valid hybrid-async env");
        assert_eq!(config.hybrid_async.apply_lag_max_commands, 4096);
        assert_eq!(config.hybrid_async.apply_poison_retry_threshold, 9);
        match (config.backend.log, config.backend.projection) {
            (
                LogSpec::ObjectLog(ObjectLogSpec::LocalFilesystem { root, .. }),
                ProjectionSpec::HybridAsync { path },
            ) => {
                assert_eq!(root, PathBuf::from("/data/olog"));
                assert_eq!(path, PathBuf::from("/data/hybrid-async.db"));
            }
            _ => panic!("expected objectlog log × hybrid-async projection"),
        }
    }

    #[test]
    fn non_objectlog_hybrid_async_pairing_is_rejected() {
        // Only the object-log log axis pairs with hybrid-async; any other log backend fails closed.
        for log in ["memory", "sqlite"] {
            let result = Config::from_env(&map(&[
                ("PQUEUE_LOG_BACKEND", log),
                ("PQUEUE_PROJECTION_BACKEND", "hybrid-async"),
            ]));
            let Err(err) = result else {
                panic!("{log}/hybrid-async must not be wired");
            };
            assert!(
                err.0.contains("PQUEUE_PROJECTION_BACKEND=hybrid-async"),
                "{}",
                err.0
            );
        }
    }

    #[test]
    fn hybrid_async_thresholds_default_when_env_absent() {
        let config = Config::from_env(&BTreeMap::new()).expect("empty env yields defaults");
        assert_eq!(config.hybrid_async, HybridAsyncThresholds::default());
    }

    #[test]
    fn hybrid_async_thresholds_parsed_from_env() {
        let config = Config::from_env(&map(&[
            ("PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS", "5000"),
            ("PQUEUE_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES", "1048576"),
            ("PQUEUE_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX", "64"),
            ("PQUEUE_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS", "30000"),
            ("PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD", "5"),
        ]))
        .expect("valid hybrid-async env");
        assert_eq!(config.hybrid_async.apply_lag_max_commands, 5000);
        assert_eq!(config.hybrid_async.apply_debt_max_bytes, 1_048_576);
        assert_eq!(config.hybrid_async.apply_queue_depth_max, 64);
        assert_eq!(config.hybrid_async.oldest_unapplied_max_ms, 30_000);
        assert_eq!(config.hybrid_async.apply_poison_retry_threshold, 5);
    }

    #[test]
    fn hybrid_async_zero_threshold_is_rejected() {
        // A zero debt bound would leave the queue instantly and permanently backpressured — reject it at
        // configuration time rather than silently accepting a queue that can never admit a mutation.
        let result = Config::from_env(&map(&[("PQUEUE_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS", "0")]));
        let Err(err) = result else {
            panic!("zero hybrid-async lag bound must fail");
        };
        assert!(
            err.0
                .contains("hybrid-async threshold apply_lag_max_commands"),
            "{}",
            err.0
        );
    }

    #[test]
    fn hybrid_async_zero_poison_retry_threshold_is_rejected() {
        let result = Config::from_env(&map(&[(
            "PQUEUE_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD",
            "0",
        )]));
        let Err(err) = result else {
            panic!("zero poison retry threshold must fail");
        };
        assert!(err.0.contains("apply_poison_retry_threshold"), "{}", err.0);
    }

    #[test]
    fn unknown_log_backend_is_rejected() {
        let result = Config::from_env(&map(&[("PQUEUE_LOG_BACKEND", "bogus")]));
        let Err(err) = result else {
            panic!("unknown log backend must fail");
        };
        assert!(err.0.contains("PQUEUE_LOG_BACKEND"), "{}", err.0);
    }

    #[test]
    fn unwired_pairing_is_rejected() {
        // sqlite log + sqlite projection is not a wired pairing (only objectlog pairs with sqlite proj).
        let result = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "sqlite"),
            ("PQUEUE_PROJECTION_BACKEND", "sqlite"),
        ]));
        assert!(result.is_err(), "sqlite/sqlite is not wired");
    }

    #[test]
    fn non_objectlog_hybrid_pairing_is_rejected() {
        let result = Config::from_env(&map(&[
            ("PQUEUE_LOG_BACKEND", "sqlite"),
            ("PQUEUE_PROJECTION_BACKEND", "hybrid"),
        ]));
        let Err(err) = result else {
            panic!("sqlite/hybrid is not wired");
        };
        assert!(
            err.0
                .contains("PQUEUE_LOG_BACKEND=sqlite PQUEUE_PROJECTION_BACKEND=hybrid"),
            "{}",
            err.0
        );
    }

    #[test]
    fn malformed_bootstrap_queue_is_rejected() {
        let result = Config::from_env(&map(&[("PQUEUE_BOOTSTRAP_QUEUES", "no-colon-here")]));
        let Err(err) = result else {
            panic!("missing colon must fail");
        };
        assert!(err.0.contains("PQUEUE_BOOTSTRAP_QUEUES"), "{}", err.0);
    }

    // The Lakebase DSN carries `sslmode=require`, which only resolves on a `tls` build (a `postgres`-only
    // build correctly fails closed — see `require_dsn_fails_closed_without_tls_feature`). Gate on `tls` so
    // the `--features postgres` (no-tls) matrix leg stays green.
    #[cfg(feature = "tls")]
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
        assert!(matches!(
            config.backend.projection,
            ProjectionSpec::InMemory
        ));
        match config.backend.log {
            LogSpec::Postgres { url, credentials } => {
                assert!(url.contains("sslmode=require"));
                assert!(
                    credentials.is_some(),
                    "Databricks service-principal env must inject a credential provider"
                );
            }
            _ => panic!("postgres env must select LogSpec::Postgres"),
        }
    }
}
