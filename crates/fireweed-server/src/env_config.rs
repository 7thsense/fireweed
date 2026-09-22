//! The single OPTIONAL environment-variable **adapter** for [`Config`] (feature `env-config`).
//!
//! Env vars are **not** the product storage vocabulary. The normative model is the orthogonal
//! StorageConfig axes (log × projection); this module is the container injection map that
//! deserializes documented `FIREWEED_*` / retained `DATABRICKS_*` names into typed [`Config`] /
//! [`BackendSpec`]. It is a PURE function over a caller-supplied `BTreeMap<String, String>` — it never
//! touches the process environment. The bin (`fireweed-service`) is the only caller that reads the live process
//! env (`std::env::vars().collect()`); a pure-library embedder builds [`Config`] directly and, by compiling
//! with `default-features = false`, drops this module (and all env-name knowledge) entirely.
//!
//! Public product names (injection values):
//! - log: `s3`
//! - projection: `turso`
//!
//! Legacy / non-public names are **hard-rejected** on this surface (no long-lived aliases):
//! `memory`, `postgres`, `filesystem`, `sqlite`, `objectlog`, `inmemory`,
//! `hybrid`, `hybrid-strict`, `hybrid-async`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, OwnerId, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_engine::AsyncProjectionSpec;

use crate::{
    BackendSpec, ChangeRecordSinkConfig, Config, ControlPlaneSpec, DEFAULT_RECOVERY_MAX_TAIL,
    EmbeddedFjordConfig, LogSpec, ObjectLogByteLimits, ObjectLogSpec, ProjectionSpec,
    ResponseBarrierSpec, S3CredentialSource, SegmentConfig, resolve_node_id,
    validated_owner_endpoint,
};

/// A rejected runtime configuration: the populator could not build a valid [`Config`] from the supplied env
/// map (unknown/unsupported backend combination, malformed `FIREWEED_BOOTSTRAP_QUEUES`, invalid segment
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

fn postgres_pool_size(env: &BTreeMap<String, String>) -> Result<usize, ConfigError> {
    let key = "FIREWEED_POSTGRES_POOL_SIZE";
    let raw = env.get(key).map(String::as_str).unwrap_or("8");
    let size = raw.parse::<usize>().map_err(|_| {
        ConfigError::new(format!(
            "{key} must be an integer between 1 and {}, got {raw:?}",
            crate::MAX_POSTGRES_POOL_SIZE
        ))
    })?;
    if !(1..=crate::MAX_POSTGRES_POOL_SIZE).contains(&size) {
        return Err(ConfigError::new(format!(
            "{key} must be between 1 and {}, got {size}",
            crate::MAX_POSTGRES_POOL_SIZE
        )));
    }
    Ok(size)
}

fn validated_usize(
    env: &BTreeMap<String, String>,
    key: &str,
    default: usize,
) -> Result<usize, ConfigError> {
    let raw = env.get(key).map(String::as_str).unwrap_or("");
    if raw.is_empty() && !env.contains_key(key) {
        return Ok(default);
    }
    raw.parse::<usize>().map_err(|_| {
        ConfigError::new(format!(
            "{key} must be a positive integer number of bytes, got {raw:?}"
        ))
    })
}

fn objectlog_byte_limits(
    env: &BTreeMap<String, String>,
    segment_target_bytes: usize,
) -> Result<ObjectLogByteLimits, ConfigError> {
    let defaults = ObjectLogByteLimits::default();
    let global = validated_usize(
        env,
        "FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL",
        defaults.global,
    )?;
    let queue_waiting = validated_usize(
        env,
        "FIREWEED_OBJECTLOG_QUEUE_WAITING_BYTES",
        defaults.queue_waiting,
    )?;
    let tenant = env
        .get("FIREWEED_OBJECTLOG_BUFFERED_BYTES_TENANT")
        .map(|_| {
            validated_usize(
                env,
                "FIREWEED_OBJECTLOG_BUFFERED_BYTES_TENANT",
                defaults.global,
            )
        })
        .transpose()?;
    ObjectLogByteLimits {
        global,
        tenant,
        queue_waiting,
    }
    .validate(segment_target_bytes)
    .map_err(|reason| ConfigError::new(format!("invalid object-log byte admission: {reason}")))
}

fn parse_u64(env: &BTreeMap<String, String>, key: &str, default: u64) -> u64 {
    env.get(key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_duration_ms(env: &BTreeMap<String, String>, key: &str, default_ms: u64) -> Duration {
    Duration::from_millis(parse_u64(env, key, default_ms))
}

fn replica_count(env: &BTreeMap<String, String>) -> Result<usize, ConfigError> {
    let raw = env_or(env, "FIREWEED_REPLICA_COUNT", "1");
    let count = raw.parse::<usize>().map_err(|_| {
        ConfigError::new(format!(
            "FIREWEED_REPLICA_COUNT must be a positive integer, got {raw:?}"
        ))
    })?;
    if count == 0 {
        return Err(ConfigError::new(
            "FIREWEED_REPLICA_COUNT must be greater than 0",
        ));
    }
    Ok(count)
}

fn control_plane_ttl_ms(
    env: &BTreeMap<String, String>,
    key: &str,
    default_ms: u64,
) -> Result<u64, ConfigError> {
    let raw = env_or(env, key, &default_ms.to_string());
    raw.parse::<u64>().map_err(|_| {
        ConfigError::new(format!(
            "{key} must be a positive integer number of milliseconds, got {raw:?}"
        ))
    })
}

fn parse_control_plane(
    env: &BTreeMap<String, String>,
    replicas: usize,
) -> Result<ControlPlaneSpec, ConfigError> {
    let profile = env_or(env, "FIREWEED_CONTROL_PLANE", "inprocess");
    match profile.as_str() {
        "inprocess" => {
            if replicas > 1 {
                return Err(ConfigError::new(format!(
                    "FIREWEED_CONTROL_PLANE=inprocess is a development-only single-process profile and cannot be used with FIREWEED_REPLICA_COUNT={replicas}; select postgres and configure FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL"
                )));
            }
            Ok(ControlPlaneSpec::InProcess)
        }
        "postgres" => {
            let url = env
                .get("FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL")
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ConfigError::new(
                        "FIREWEED_CONTROL_PLANE=postgres requires non-empty FIREWEED_POSTGRES_CONTROL_PLANE_DATABASE_URL",
                    )
                })?
                .to_string();
            let heartbeat_ttl_ms =
                control_plane_ttl_ms(env, "FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS", 5_000)?;
            let lease_ttl_ms =
                control_plane_ttl_ms(env, "FIREWEED_CONTROL_PLANE_LEASE_TTL_MS", 15_000)?;
            if heartbeat_ttl_ms == 0 || lease_ttl_ms == 0 {
                return Err(ConfigError::new(
                    "FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS and FIREWEED_CONTROL_PLANE_LEASE_TTL_MS must be greater than 0",
                ));
            }
            if lease_ttl_ms < heartbeat_ttl_ms {
                return Err(ConfigError::new(
                    "FIREWEED_CONTROL_PLANE_LEASE_TTL_MS must be greater than or equal to FIREWEED_CONTROL_PLANE_HEARTBEAT_TTL_MS",
                ));
            }
            Ok(ControlPlaneSpec::Postgres {
                url,
                config: fireweed_engine::ControlPlaneConfig {
                    heartbeat_ttl_ms,
                    lease_ttl_ms,
                },
            })
        }
        other => Err(ConfigError::new(format!(
            "unknown FIREWEED_CONTROL_PLANE={other:?}; expected inprocess|postgres"
        ))),
    }
}

/// The group-commit segment configuration for the segmented object-log families.
fn segment_config(env: &BTreeMap<String, String>) -> Result<SegmentConfig, ConfigError> {
    // Match the RESP coalescing window (`PIPELINE_XADD_BYTE_LIMIT` = 1 MiB) so one client pipeline of
    // up to 1000 XADDs can force-seal as a single segment rather than mid-batch size seals at 256 KiB.
    let target_bytes = parse_usize(env, "FIREWEED_SEGMENT_TARGET_BYTES", 1024 * 1024);
    let max_latency_ms = parse_u64(env, "FIREWEED_SEGMENT_MAX_LATENCY_MS", 20);
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
/// `FIREWEED_HYBRID_ASYNC_*` env names. These names are transitional debt; P12a replaces them after the
/// response-barrier selector lands. A zero bound retains the legacy [`ConfigError`] fingerprint.
fn hybrid_async_thresholds(
    env: &BTreeMap<String, String>,
) -> Result<AsyncProjectionSpec, ConfigError> {
    let d = AsyncProjectionSpec::default();
    let spec = AsyncProjectionSpec {
        apply_lag_max_commands: parse_u64(
            env,
            "FIREWEED_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS",
            d.apply_lag_max_commands,
        ),
        apply_debt_max_bytes: parse_u64(
            env,
            "FIREWEED_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES",
            d.apply_debt_max_bytes,
        ),
        apply_queue_depth_max: parse_usize(
            env,
            "FIREWEED_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX",
            d.apply_queue_depth_max,
        ),
        oldest_unapplied_max_ms: parse_u64(
            env,
            "FIREWEED_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS",
            d.oldest_unapplied_max_ms,
        ),
        apply_poison_retry_threshold: parse_u32(
            env,
            "FIREWEED_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD",
            d.apply_poison_retry_threshold,
        ),
        apply_start_delay_ms: 0,
    };
    let zero_name = [
        (spec.apply_lag_max_commands == 0, "apply_lag_max_commands"),
        (spec.apply_debt_max_bytes == 0, "apply_debt_max_bytes"),
        (spec.apply_queue_depth_max == 0, "apply_queue_depth_max"),
        (spec.oldest_unapplied_max_ms == 0, "oldest_unapplied_max_ms"),
        (
            spec.apply_poison_retry_threshold == 0,
            "apply_poison_retry_threshold",
        ),
    ]
    .into_iter()
    .find_map(|(zero, name)| zero.then_some(name));
    if let Some(name) = zero_name {
        return Err(ConfigError::new(format!(
            "invalid hybrid-async threshold configuration: hybrid-async threshold {name} must be > 0 (a zero bound is instantly backpressured)"
        )));
    }
    Ok(spec)
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
        .get("FIREWEED_CHANGE_RECORD_SINK_ENDPOINT")
        .cloned()
        .filter(|v| !v.trim().is_empty());
    config.enabled = parse_bool(env, "FIREWEED_CHANGE_RECORD_SINK_ENABLED", false);
    config.tick_interval = parse_duration_ms(
        env,
        "FIREWEED_CHANGE_RECORD_SINK_TICK_INTERVAL_MS",
        config.tick_interval.as_millis() as u64,
    );
    config.batch_size = parse_usize(
        env,
        "FIREWEED_CHANGE_RECORD_SINK_BATCH_SIZE",
        config.batch_size,
    );
    if let Some(value) = env
        .get("FIREWEED_CHANGE_RECORD_SINK_AUTHORIZATION")
        .cloned()
        .filter(|v| !v.trim().is_empty())
    {
        config.headers.insert("authorization".to_string(), value);
    }
    for (key, value) in env {
        if let Some(name) = key.strip_prefix("FIREWEED_CHANGE_RECORD_SINK_HEADER_")
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
            "FIREWEED_CHANGE_RECORD_SINK_BATCH_SIZE must be greater than 0",
        ));
    }
    if config.enabled && config.tick_interval.is_zero() {
        return Err(ConfigError::new(
            "FIREWEED_CHANGE_RECORD_SINK_TICK_INTERVAL_MS must be greater than 0",
        ));
    }
    Ok(config)
}

fn unsupported_storage(log: &str, projection: &str, reason: &str) -> ConfigError {
    ConfigError::new(format!(
        "unsupported storage configuration FIREWEED_LOG_BACKEND={log} FIREWEED_PROJECTION_BACKEND={projection}: {reason}"
    ))
}

/// Map the documented `FIREWEED_LOG_BACKEND` × `FIREWEED_PROJECTION_BACKEND` env names onto the typed two-axis
/// [`BackendSpec`] (ADR-012). Each axis is parsed independently, then the pairing is validated against the
/// set fireweed-server actually wires. The `FIREWEED_OBJECT_LOG_MODE` pseudo-axis is retired — the object log's
/// only production form is the segmented group-commit substrate, which the composed `ObjectLog` axis is.
fn required_nonempty(env: &BTreeMap<String, String>, key: &str) -> Result<String, ConfigError> {
    env.get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| ConfigError::new(format!("{key} is required and must not be empty")))
}

/// S3-compatible object-log fields for `FIREWEED_LOG_BACKEND=s3`.
fn object_log_spec_s3(
    env: &BTreeMap<String, String>,
    segments: SegmentConfig,
) -> Result<ObjectLogSpec, ConfigError> {
    if env.contains_key("FIREWEED_OBJECT_LOG_ROOT") {
        return Err(ConfigError::new(
            "FIREWEED_OBJECT_LOG_ROOT is filesystem/local-only and must not be set for an S3 log backend",
        ));
    }
    let endpoint = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_ENDPOINT")?;
    let bucket = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_BUCKET")?;
    let region = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_REGION")?;
    let credential_source = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE")?;
    if credential_source != "static" {
        return Err(ConfigError::new(format!(
            "unsupported FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE={credential_source:?}; expected static"
        )));
    }
    let access_key_id = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID")?;
    let secret_access_key = required_nonempty(env, "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY")?;
    let allow_insecure_http = match env.get("FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP") {
        None => false,
        Some(value) if matches!(value.as_str(), "1" | "true") => true,
        Some(value) if matches!(value.as_str(), "0" | "false") => false,
        Some(value) => {
            return Err(ConfigError::new(format!(
                "FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP must be true|false|1|0, got {value:?}"
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

fn parse_backend(
    env: &BTreeMap<String, String>,
    segments: SegmentConfig,
) -> Result<BackendSpec, ConfigError> {
    // Product default: s3 log × turso projection.
    let log = env_or(env, "FIREWEED_LOG_BACKEND", "s3");
    let projection = env_or(env, "FIREWEED_PROJECTION_BACKEND", "turso");

    let log_spec = match log.as_str() {
        "s3" => LogSpec::ObjectLog(object_log_spec_s3(env, segments)?),
        other => {
            return Err(unsupported_storage(
                &log,
                &projection,
                &format!(
                    "unknown FIREWEED_LOG_BACKEND={other:?}; expected s3 (storage is s3 log × turso projection only)"
                ),
            ));
        }
    };

    let projection_spec = match projection.as_str() {
        "turso" => {
            #[cfg(feature = "turso-projection")]
            {
                ProjectionSpec::Turso {
                    path: PathBuf::from(env_or(
                        env,
                        "FIREWEED_TURSO_PROJECTION_PATH",
                        "/var/lib/fireweed/fireweed-projection.turso",
                    )),
                }
            }
            #[cfg(not(feature = "turso-projection"))]
            {
                return Err(unsupported_storage(
                    &log,
                    &projection,
                    "turso projection requires the `turso-projection` cargo feature; rebuild with \
                     default features (or `--features turso-projection`). Feature-disabled builds \
                     reject turso before storage I/O and never fall back",
                ));
            }
        }
        other => {
            return Err(unsupported_storage(
                &log,
                &projection,
                &format!(
                    "unknown FIREWEED_PROJECTION_BACKEND={other:?}; expected turso (storage is s3 log × turso projection only)"
                ),
            ));
        }
    };

    let wired = matches!(
        (&log_spec, &projection_spec),
        (
            LogSpec::ObjectLog(ObjectLogSpec::S3 { .. }),
            ProjectionSpec::Turso { .. }
        )
    );
    if !wired {
        return Err(unsupported_storage(
            &log,
            &projection,
            "storage is s3 log × turso projection only",
        ));
    }

    let replicas = replica_count(env)?;
    let response_barrier = ResponseBarrierSpec::AsyncProjection;
    let thresholds = hybrid_async_thresholds(env)?;
    let async_projection = Some(thresholds);
    Ok(BackendSpec {
        log: log_spec,
        projection: projection_spec,
        control_plane: parse_control_plane(env, replicas)?,
        response_barrier,
        async_projection,
    })
}

fn queue_definition(tenant: &str, queue: &str) -> Result<QueueDefinition, ConfigError> {
    Ok(QueueDefinition {
        tenant_id: TenantId::new(tenant).map_err(|e| {
            ConfigError::new(format!(
                "invalid tenant id in FIREWEED_BOOTSTRAP_QUEUES: {e}"
            ))
        })?,
        queue_id: QueueId::new(queue).map_err(|e| {
            ConfigError::new(format!(
                "invalid queue id in FIREWEED_BOOTSTRAP_QUEUES: {e}"
            ))
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
        emit_change_records: false,
    })
}

/// Hard cap for generated bootstrap inventories. This bounds startup memory,
/// queue-definition writes, and the rendered deployment contract independently
/// of the caller or chart.
pub const MAX_GENERATED_BOOTSTRAP_QUEUES: usize = 10_000;

fn parse_explicit_bootstrap_queues(value: &str) -> Result<Vec<QueueDefinition>, ConfigError> {
    value
        .split(',')
        .filter_map(|entry| {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                return None;
            }
            Some(match trimmed.split_once(':') {
                Some((tenant, queue)) => queue_definition(tenant, queue),
                None => Err(ConfigError::new(format!(
                    "invalid FIREWEED_BOOTSTRAP_QUEUES entry {trimmed:?}; expected tenant:queue"
                ))),
            })
        })
        .collect()
}

fn parse_bootstrap_queues(
    env: &BTreeMap<String, String>,
) -> Result<Vec<QueueDefinition>, ConfigError> {
    // An explicitly configured non-empty list always wins. This preserves the
    // original contract and gives operators an unambiguous override when both
    // explicit and generated settings are present.
    if let Some(explicit) = env
        .get("FIREWEED_BOOTSTRAP_QUEUES")
        .filter(|value| !value.trim().is_empty())
    {
        return parse_explicit_bootstrap_queues(explicit);
    }

    if let Some(raw_count) = env.get("FIREWEED_BOOTSTRAP_GENERATED_COUNT") {
        let count = raw_count.parse::<usize>().map_err(|_| {
            ConfigError::new(format!(
                "FIREWEED_BOOTSTRAP_GENERATED_COUNT must be an integer from 1 to {MAX_GENERATED_BOOTSTRAP_QUEUES}, got {raw_count:?}"
            ))
        })?;
        if !(1..=MAX_GENERATED_BOOTSTRAP_QUEUES).contains(&count) {
            return Err(ConfigError::new(format!(
                "FIREWEED_BOOTSTRAP_GENERATED_COUNT must be from 1 to {MAX_GENERATED_BOOTSTRAP_QUEUES}, got {count}"
            )));
        }

        let tenant = env_or(env, "FIREWEED_BOOTSTRAP_GENERATED_TENANT", "t1");
        let prefix = env_or(env, "FIREWEED_BOOTSTRAP_GENERATED_PREFIX", "q");
        if prefix.trim().is_empty() {
            return Err(ConfigError::new(
                "FIREWEED_BOOTSTRAP_GENERATED_PREFIX must not be empty",
            ));
        }

        return (0..count)
            .map(|index| queue_definition(&tenant, &format!("{prefix}{index}")))
            .collect();
    }

    parse_explicit_bootstrap_queues("t1:q1")
}

fn embedded_fjord_config(env: &BTreeMap<String, String>) -> EmbeddedFjordConfig {
    EmbeddedFjordConfig {
        namespace_root: PathBuf::from(env_or(
            env,
            "FIREWEED_FJORD_STATE_ROOT",
            "/var/lib/fireweed/fjord",
        )),
        cluster_id: env_or(env, "FIREWEED_FJORD_CLUSTER_ID", "fireweed-fjord"),
        // Optional external-consumer TCP surface for the in-process embedded change log. Unset keeps the
        // change log purely in-process (no socket on the write path).
        broker_listen: env
            .get("FIREWEED_FJORD_BROKER_LISTEN")
            .cloned()
            .filter(|v| !v.trim().is_empty()),
    }
}

impl Config {
    /// The SINGLE optional env-var populator: map authoritative `FIREWEED_*` and retained `DATABRICKS_*`
    /// env names in `env` onto a typed [`Config`]. PURE over the supplied map — it
    /// does NOT read the process environment
    /// (the bin's `main` collects `std::env::vars()` and passes the map in). Available only with the
    /// `env-config` feature (default-on for the bin); a library embedder can drop it via `default-features = false`.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Config, ConfigError> {
        let segments = segment_config(env)?;
        let replicas = replica_count(env)?;
        let node_id = resolve_node_id(&env_or(env, "FIREWEED_NODE_ID", "0"));
        let owner_id = match env
            .get("FIREWEED_OWNER_ID")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(value) => OwnerId::new(value)
                .map_err(|error| ConfigError::new(format!("invalid FIREWEED_OWNER_ID: {error}")))?,
            None if replicas > 1 => {
                return Err(ConfigError::new(
                    "FIREWEED_REPLICA_COUNT>1 requires a non-empty, full-width FIREWEED_OWNER_ID",
                ));
            }
            None => OwnerId::new(format!("node-{node_id}"))
                .expect("a numeric node id always forms a valid owner id"),
        };
        let advertise_addr = match env
            .get("FIREWEED_ADVERTISE_ADDR")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            Some(endpoint) => Some(validated_owner_endpoint(endpoint).ok_or_else(|| {
                ConfigError::new(
                    "FIREWEED_ADVERTISE_ADDR must be a dialable IP socket address with a nonzero port",
                )
            })?),
            None if replicas > 1 => {
                return Err(ConfigError::new(
                    "FIREWEED_REPLICA_COUNT>1 requires FIREWEED_ADVERTISE_ADDR with a pod-reachable IP:port",
                ));
            }
            None => None,
        };
        Ok(Config {
            backend: parse_backend(env, segments)?,
            embedded_fjord: embedded_fjord_config(env),
            node_id,
            owner_id,
            listen: env_or(env, "FIREWEED_LISTEN_ADDR", "0.0.0.0:8080"),
            advertise_addr,
            reclaim_interval: parse_duration_ms(env, "FIREWEED_RECLAIM_INTERVAL_MS", 1_000),
            queues: parse_bootstrap_queues(env)?,
            recovery_max_tail: parse_u64(
                env,
                "FIREWEED_RECOVERY_MAX_TAIL_COMMANDS",
                DEFAULT_RECOVERY_MAX_TAIL,
            ),
            // Matches the prior `std::env::var(..).is_ok()` semantics: present (even empty) enables telemetry.
            debug_segments: env.contains_key("FIREWEED_DEBUG_SEGMENTS"),
            objectlog_byte_limits: objectlog_byte_limits(env, segments.target_bytes)?,
            // `>0` caps the tokio worker pool; absent/0 = one worker per core (the bin builds the runtime).
            worker_threads: env
                .get("FIREWEED_WORKER_THREADS")
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| *n > 0),
            postgres_pool_size: postgres_pool_size(env)?,
            runtime_resource_metrics_path: match env.get("FIREWEED_RUNTIME_RESOURCE_METRICS_PATH") {
                None => None,
                Some(value) => {
                    let value = value.trim();
                    if value.is_empty() {
                        return Err(ConfigError::new(
                            "FIREWEED_RUNTIME_RESOURCE_METRICS_PATH must not be empty",
                        ));
                    }
                    let path = PathBuf::from(value);
                    if !path.is_absolute() {
                        return Err(ConfigError::new(
                            "FIREWEED_RUNTIME_RESOURCE_METRICS_PATH must be absolute",
                        ));
                    }
                    Some(path)
                }
            },
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

    fn s3_env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut env = map(&[
            ("FIREWEED_LOG_BACKEND", "s3"),
            ("FIREWEED_PROJECTION_BACKEND", "turso"),
            ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "http://127.0.0.1:19000"),
            ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fireweed-test"),
            ("FIREWEED_OBJECT_LOG_S3_REGION", "us-east-1"),
            ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
            ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "fireweed"),
            (
                "FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY",
                "fireweed-test-minio",
            ),
            ("FIREWEED_OBJECT_LOG_S3_ALLOW_INSECURE_HTTP", "true"),
        ]);
        for (key, value) in pairs {
            env.insert((*key).to_owned(), (*value).to_owned());
        }
        env
    }

    #[test]
    fn empty_env_requires_s3_fields() {
        let Err(err) = Config::from_env(&BTreeMap::new()) else {
            panic!("s3 fields are required");
        };
        assert!(
            err.0.contains("FIREWEED_OBJECT_LOG_S3_ENDPOINT") || err.0.contains("s3"),
            "{err:?}"
        );
    }

    #[test]
    fn defaults_when_env_is_empty() {
        let config = Config::from_env(&s3_env(&[])).expect("s3 env yields product defaults");
        assert!(matches!(config.backend.log, LogSpec::ObjectLog(_)));
        // TD-010: Turso is the public default projection when FIREWEED_PROJECTION_BACKEND is absent.
        assert!(
            matches!(
                config.backend.projection,
                ProjectionSpec::Turso { ref path }
                    if path == &PathBuf::from("/var/lib/fireweed/fireweed-projection.turso")
            ),
            "default projection must be Turso at FIREWEED_TURSO_PROJECTION_PATH default"
        );
        assert_eq!(config.backend.projection.label(), "turso");
        assert_eq!(
            config.embedded_fjord.namespace_root,
            PathBuf::from("/var/lib/fireweed/fjord")
        );
        assert_eq!(config.embedded_fjord.cluster_id, "fireweed-fjord");
        assert_eq!(config.node_id, 0);
        assert_eq!(config.listen, "0.0.0.0:8080");
        assert_eq!(config.reclaim_interval, Duration::from_millis(1_000));
        assert_eq!(config.postgres_pool_size, crate::DEFAULT_POSTGRES_POOL_SIZE);
        assert_eq!(config.recovery_max_tail, DEFAULT_RECOVERY_MAX_TAIL);
        assert!(!config.debug_segments);
        assert_eq!(config.worker_threads, None);
        assert_eq!(config.runtime_resource_metrics_path, None);
        assert_eq!(config.queues.len(), 1, "default bootstrap is t1:q1");
        assert_eq!(config.queues[0].tenant_id.as_str(), "t1");
        assert_eq!(config.queues[0].queue_id.as_str(), "q1");
    }

    #[test]
    fn turso_projection_is_the_public_env_default() {
        let config = Config::from_env(&s3_env(&[])).expect("s3 × default turso");
        match config.backend.projection {
            ProjectionSpec::Turso { path } => {
                assert_eq!(
                    path,
                    PathBuf::from("/var/lib/fireweed/fireweed-projection.turso")
                );
            }
            other => panic!("expected Turso default, got label={}", other.label()),
        }
        let config = Config::from_env(&s3_env(&[(
            "FIREWEED_TURSO_PROJECTION_PATH",
            "/data/custom-projection.turso",
        )]))
        .expect("s3 × turso with custom path");
        match config.backend.projection {
            ProjectionSpec::Turso { path } => {
                assert_eq!(path, PathBuf::from("/data/custom-projection.turso"));
            }
            other => panic!("expected Turso, got {}", other.label()),
        }
    }

    #[test]
    fn public_projection_help_and_parser_are_bijective() {
        let config = Config::from_env(&s3_env(&[("FIREWEED_PROJECTION_BACKEND", "turso")]))
            .expect("turso remains the public projection");
        assert_eq!(config.backend.projection.label(), "turso");
        for bad in [
            "memory",
            "postgres",
            "sqlite",
            "inmemory",
            "hybrid",
            "hybrid-strict",
            "hybrid-async",
            "objectlog",
        ] {
            let result = Config::from_env(&s3_env(&[("FIREWEED_PROJECTION_BACKEND", bad)]));
            assert!(
                result.is_err(),
                "{bad} must be rejected on public env surface"
            );
        }
    }

    #[test]
    fn retired_log_backends_are_rejected() {
        for log in ["memory", "postgres", "filesystem", "sqlite", "objectlog"] {
            let result = Config::from_env(&s3_env(&[("FIREWEED_LOG_BACKEND", log)]));
            assert!(result.is_err(), "{log} log must be rejected");
        }
    }

    #[test]
    fn s3_turso_is_the_public_env_cell() {
        let config = Config::from_env(&s3_env(&[(
            "FIREWEED_TURSO_PROJECTION_PATH",
            "/tmp/s3.turso",
        )]))
        .expect("s3 × turso");
        assert_eq!(config.backend.projection.label(), "turso");
        assert_eq!(config.backend.log.label(), "s3");
        assert_eq!(config.validate_for_start(), Ok(()));
    }

    #[test]
    fn postgres_pool_size_is_positive_and_bounded() {
        let configured = Config::from_env(&s3_env(&[("FIREWEED_POSTGRES_POOL_SIZE", "3")]))
            .expect("valid postgres pool size");
        assert_eq!(configured.postgres_pool_size, 3);

        for invalid in ["0", "65", "not-a-number"] {
            let error = match Config::from_env(&s3_env(&[("FIREWEED_POSTGRES_POOL_SIZE", invalid)]))
            {
                Err(error) => error,
                Ok(_) => panic!("invalid postgres pool size must fail closed"),
            };
            assert!(error.to_string().contains("FIREWEED_POSTGRES_POOL_SIZE"));
        }
    }

    #[test]
    fn scalar_knobs_are_honored_on_the_product_cell() {
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_NODE_ID", "7"),
            ("FIREWEED_LISTEN_ADDR", "127.0.0.1:6390"),
            ("FIREWEED_WORKER_THREADS", "4"),
            (
                "FIREWEED_RUNTIME_RESOURCE_METRICS_PATH",
                "/tmp/fireweed-runtime-resources.json",
            ),
            ("FIREWEED_RECLAIM_INTERVAL_MS", "250"),
            ("FIREWEED_RECOVERY_MAX_TAIL_COMMANDS", "42"),
            ("FIREWEED_DEBUG_SEGMENTS", "1"),
            ("FIREWEED_BOOTSTRAP_QUEUES", "ta:qa,tb:qb"),
        ]))
        .expect("valid env");
        assert_eq!(config.backend.log.label(), "s3");
        assert_eq!(config.backend.projection.label(), "turso");
        assert_eq!(config.node_id, 7);
        assert_eq!(config.listen, "127.0.0.1:6390");
        assert_eq!(config.worker_threads, Some(4));
        assert_eq!(
            config.runtime_resource_metrics_path,
            Some(PathBuf::from("/tmp/fireweed-runtime-resources.json"))
        );
        assert_eq!(config.reclaim_interval, Duration::from_millis(250));
        assert_eq!(config.recovery_max_tail, 42);
        assert!(config.debug_segments);
        assert_eq!(config.queues.len(), 2);
        assert_eq!(config.queues[1].queue_id.as_str(), "qb");
    }

    #[test]
    fn generated_bootstrap_queue_inventory() {
        let generated = s3_env(&[
            ("FIREWEED_BOOTSTRAP_GENERATED_COUNT", "1001"),
            ("FIREWEED_BOOTSTRAP_GENERATED_TENANT", "density"),
            ("FIREWEED_BOOTSTRAP_GENERATED_PREFIX", "q"),
        ]);
        let first = Config::from_env(&generated).expect("valid generated inventory");
        let second = Config::from_env(&generated).expect("generated inventory is reproducible");

        assert_eq!(first.queues.len(), 1001);
        assert_eq!(first.queues[0].tenant_id.as_str(), "density");
        assert_eq!(first.queues[0].queue_id.as_str(), "q0");
        assert_eq!(first.queues[1000].queue_id.as_str(), "q1000");
        let keys: std::collections::BTreeSet<_> = first
            .queues
            .iter()
            .map(|queue| {
                (
                    queue.tenant_id.as_str().to_string(),
                    queue.queue_id.as_str().to_string(),
                )
            })
            .collect();
        assert_eq!(keys.len(), 1001, "generated queue keys must be unique");
        assert_eq!(
            first
                .queues
                .iter()
                .map(|queue| queue.queue_id.as_str())
                .collect::<Vec<_>>(),
            second
                .queues
                .iter()
                .map(|queue| queue.queue_id.as_str())
                .collect::<Vec<_>>(),
            "the same generated contract must produce the same ordered inventory"
        );

        let explicit = Config::from_env(&s3_env(&[
            ("FIREWEED_BOOTSTRAP_QUEUES", "override:only"),
            ("FIREWEED_BOOTSTRAP_GENERATED_COUNT", "1001"),
            ("FIREWEED_BOOTSTRAP_GENERATED_TENANT", "density"),
            ("FIREWEED_BOOTSTRAP_GENERATED_PREFIX", "q"),
        ]))
        .expect("explicit inventory takes precedence");
        assert_eq!(explicit.queues.len(), 1);
        assert_eq!(explicit.queues[0].tenant_id.as_str(), "override");
        assert_eq!(explicit.queues[0].queue_id.as_str(), "only");
    }

    #[test]
    fn runtime_resource_metrics_path_rejects_empty_and_relative_values() {
        for value in ["", "   ", "relative/resources.json"] {
            let error = match Config::from_env(&s3_env(&[(
                "FIREWEED_RUNTIME_RESOURCE_METRICS_PATH",
                value,
            )])) {
                Ok(_) => panic!("invalid runtime resource metrics path was accepted"),
                Err(error) => error,
            };
            assert!(
                error
                    .to_string()
                    .contains("FIREWEED_RUNTIME_RESOURCE_METRICS_PATH"),
                "{error}"
            );
        }
    }

    #[test]
    fn generated_bootstrap_queue_inventory_rejects_invalid_or_unbounded_counts() {
        for count in ["", "zero", "0", "10001", "18446744073709551616"] {
            let result =
                Config::from_env(&s3_env(&[("FIREWEED_BOOTSTRAP_GENERATED_COUNT", count)]));
            assert!(
                result.is_err(),
                "generated count {count:?} must be rejected"
            );
        }

        let empty_prefix = Config::from_env(&s3_env(&[
            ("FIREWEED_BOOTSTRAP_GENERATED_COUNT", "1"),
            ("FIREWEED_BOOTSTRAP_GENERATED_PREFIX", ""),
        ]));
        assert!(
            empty_prefix.is_err(),
            "empty generated prefix must be rejected"
        );

        let invalid_tenant = Config::from_env(&s3_env(&[
            ("FIREWEED_BOOTSTRAP_GENERATED_COUNT", "1"),
            ("FIREWEED_BOOTSTRAP_GENERATED_TENANT", ""),
        ]));
        assert!(
            invalid_tenant.is_err(),
            "generated tenant must satisfy TenantId validation"
        );
    }

    #[test]
    fn objectlog_byte_limits_are_typed_and_validated_against_segment_target() {
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_SEGMENT_TARGET_BYTES", "1024"),
            ("FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL", "8192"),
            ("FIREWEED_OBJECTLOG_BUFFERED_BYTES_TENANT", "4096"),
            ("FIREWEED_OBJECTLOG_QUEUE_WAITING_BYTES", "2048"),
        ]))
        .expect("valid byte limits");
        assert_eq!(
            config.objectlog_byte_limits,
            ObjectLogByteLimits {
                global: 8192,
                tenant: Some(4096),
                queue_waiting: 2048,
            }
        );

        for pairs in [
            vec![("FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL", "0")],
            vec![("FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL", "wat")],
            vec![("FIREWEED_OBJECTLOG_QUEUE_WAITING_BYTES", "0")],
            vec![
                ("FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL", "1024"),
                ("FIREWEED_OBJECTLOG_QUEUE_WAITING_BYTES", "2048"),
            ],
            vec![
                ("FIREWEED_SEGMENT_TARGET_BYTES", "4096"),
                ("FIREWEED_OBJECTLOG_BUFFERED_BYTES_GLOBAL", "2048"),
                ("FIREWEED_OBJECTLOG_QUEUE_WAITING_BYTES", "1024"),
            ],
        ] {
            assert!(
                Config::from_env(&s3_env(&pairs)).is_err(),
                "accepted {pairs:?}"
            );
        }
    }

    #[test]
    fn segment_format_is_fixed_and_has_no_runtime_selector() {
        let baseline = segment_config(&map(&[])).unwrap();
        let with_retired_selector =
            segment_config(&map(&[("FIREWEED_SEGMENT_WRITER_FORMAT", "retired")])).unwrap();
        assert_eq!(baseline, with_retired_selector);
    }

    #[test]
    fn s3_turso_projection_carries_paths_and_segment_config() {
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_TURSO_PROJECTION_PATH", "/data/proj.turso"),
            ("FIREWEED_SEGMENT_TARGET_BYTES", "131072"),
            ("FIREWEED_SEGMENT_MAX_LATENCY_MS", "5"),
        ]))
        .expect("valid env");
        match (config.backend.log, config.backend.projection) {
            (
                LogSpec::ObjectLog(ObjectLogSpec::S3 { segment_config, .. }),
                ProjectionSpec::Turso { path },
            ) => {
                assert_eq!(path, PathBuf::from("/data/proj.turso"));
                assert_eq!(segment_config.target_bytes, 131_072);
                assert_eq!(segment_config.max_latency_ms, 5);
            }
            _ => panic!("expected s3 log × turso projection"),
        }
    }

    #[test]
    fn turso_projection_is_accepted_on_public_env_surface() {
        // Turso is the public default projection (TD-010); explicit selection parses with the
        // single documented FIREWEED_TURSO_PROJECTION_PATH setting.
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_PROJECTION_BACKEND", "turso"),
            ("FIREWEED_TURSO_PROJECTION_PATH", "/data/projection.turso"),
        ]))
        .expect("public env surface accepts turso");
        match config.backend.projection {
            ProjectionSpec::Turso { path } => {
                assert_eq!(path, PathBuf::from("/data/projection.turso"));
            }
            other => panic!("expected Turso, got {}", other.label()),
        }
    }

    #[test]
    fn demoted_hybrid_projection_is_rejected_on_public_env_surface() {
        // hybrid|hybrid-strict|hybrid-async are demoted from the public FIREWEED_PROJECTION_BACKEND
        // axis. Internal Config construction may still name them; the env adapter fails closed.
        for projection in ["hybrid", "hybrid-strict", "hybrid-async"] {
            let result = Config::from_env(&s3_env(&[("FIREWEED_PROJECTION_BACKEND", projection)]));
            let Err(err) = result else {
                panic!("{projection} must be rejected on the public env surface");
            };
            assert!(
                err.0.contains("FIREWEED_PROJECTION_BACKEND"),
                "unexpected error for {projection}: {}",
                err.0
            );
        }
    }

    #[test]
    fn fjord_namespace_config_is_parsed_from_env() {
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_FJORD_STATE_ROOT", "/data/fjord"),
            ("FIREWEED_FJORD_CLUSTER_ID", "fjord-test"),
        ]))
        .expect("valid fjord namespace env");
        assert_eq!(
            config.embedded_fjord.namespace_root,
            PathBuf::from("/data/fjord")
        );
        assert_eq!(config.embedded_fjord.cluster_id, "fjord-test");
    }

    #[test]
    fn demoted_hybrid_strict_and_async_are_rejected_for_any_log_backend() {
        // Demotion is at the projection name, not only the pairing matrix — any log × hybrid*
        // env selection fails closed on the public surface.
        for log in ["memory", "sqlite", "filesystem", "s3"] {
            for projection in ["hybrid-strict", "hybrid-async"] {
                let mut pairs = vec![
                    ("FIREWEED_LOG_BACKEND", log),
                    ("FIREWEED_PROJECTION_BACKEND", projection),
                ];
                if log == "s3" {
                    // Provide minimal S3 fields so rejection is on the projection name, not S3 parse.
                    pairs.extend_from_slice(&[
                        ("FIREWEED_OBJECT_LOG_S3_ENDPOINT", "https://s3.example.com"),
                        ("FIREWEED_OBJECT_LOG_S3_BUCKET", "fw"),
                        ("FIREWEED_OBJECT_LOG_S3_REGION", "us-east-1"),
                        ("FIREWEED_OBJECT_LOG_S3_CREDENTIAL_SOURCE", "static"),
                        ("FIREWEED_OBJECT_LOG_S3_ACCESS_KEY_ID", "ak"),
                        ("FIREWEED_OBJECT_LOG_S3_SECRET_ACCESS_KEY", "sk"),
                    ]);
                }
                let result = Config::from_env(&s3_env(&pairs));
                let Err(err) = result else {
                    panic!("{log}/{projection} must be rejected on the public env surface");
                };
                assert!(
                    err.0.contains("FIREWEED_LOG_BACKEND")
                        || err.0.contains("FIREWEED_PROJECTION_BACKEND"),
                    "{}",
                    err.0
                );
            }
        }
    }

    #[test]
    fn hybrid_async_thresholds_default_when_env_absent() {
        // Default Strict composition leaves async unset (P3v Option/barrier coherence). P12a later
        // maps explicit barrier syntax; public env cannot select HybridAsync today.
        let config = Config::from_env(&s3_env(&[])).expect("s3 env yields defaults");
        assert_eq!(
            config.backend.response_barrier,
            ResponseBarrierSpec::AsyncProjection
        );
        assert!(config.backend.async_projection.is_some());
        assert_eq!(config.validate_for_start(), Ok(()));
    }

    #[test]
    fn hybrid_async_thresholds_parsed_from_env() {
        // Public env is Strict-only until P12a; legacy FIREWEED_HYBRID_ASYNC_* bounds are still
        // parsed for zero-bound ConfigError fingerprints but are not attached under Strict
        // (Strict+Some is a typed EngineError at start). Deferred flush attaches on
        // filesystem×sqlite only.
        let config = Config::from_env(&s3_env(&[
            ("FIREWEED_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS", "5000"),
            ("FIREWEED_HYBRID_ASYNC_APPLY_DEBT_MAX_BYTES", "1048576"),
            ("FIREWEED_HYBRID_ASYNC_APPLY_QUEUE_DEPTH_MAX", "64"),
            ("FIREWEED_HYBRID_ASYNC_OLDEST_UNAPPLIED_MAX_MS", "30000"),
            ("FIREWEED_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD", "5"),
        ]))
        .expect("valid s3×turso env");
        assert_eq!(
            config.backend.response_barrier,
            ResponseBarrierSpec::AsyncProjection
        );
        assert!(
            config.backend.async_projection.is_some(),
            "object-log env attaches async projection bounds"
        );
        assert_eq!(config.validate_for_start(), Ok(()));
    }

    #[test]
    fn hybrid_async_zero_threshold_is_rejected() {
        // A zero debt bound would leave the queue instantly and permanently backpressured — reject it at
        // configuration time rather than silently accepting a queue that can never admit a mutation.
        let result = Config::from_env(&s3_env(&[(
            "FIREWEED_HYBRID_ASYNC_APPLY_LAG_MAX_COMMANDS",
            "0",
        )]));
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
        let result = Config::from_env(&s3_env(&[(
            "FIREWEED_HYBRID_ASYNC_APPLY_POISON_RETRY_THRESHOLD",
            "0",
        )]));
        let Err(err) = result else {
            panic!("zero poison retry threshold must fail");
        };
        assert!(err.0.contains("apply_poison_retry_threshold"), "{}", err.0);
    }

    #[test]
    fn unknown_log_backend_is_rejected() {
        let result = Config::from_env(&s3_env(&[("FIREWEED_LOG_BACKEND", "bogus")]));
        let Err(err) = result else {
            panic!("unknown log backend must fail");
        };
        assert!(err.0.contains("FIREWEED_LOG_BACKEND"), "{}", err.0);
    }

    #[test]
    fn unwired_pairing_is_rejected() {
        // Public surface never accepts hybrid; demoted names fail before pairing checks.
        let result = Config::from_env(&s3_env(&[
            ("FIREWEED_LOG_BACKEND", "memory"),
            ("FIREWEED_PROJECTION_BACKEND", "hybrid"),
        ]));
        assert!(
            result.is_err(),
            "memory/hybrid is demoted from public select"
        );
    }

    #[test]
    fn hybrid_pairing_is_rejected_for_any_public_log() {
        let result = Config::from_env(&s3_env(&[
            ("FIREWEED_LOG_BACKEND", "sqlite"),
            ("FIREWEED_PROJECTION_BACKEND", "hybrid"),
        ]));
        let Err(err) = result else {
            panic!("sqlite/hybrid must be rejected");
        };
        assert!(
            err.0.contains("FIREWEED_LOG_BACKEND") || err.0.contains("FIREWEED_PROJECTION_BACKEND"),
            "{}",
            err.0
        );
    }

    #[test]
    fn malformed_bootstrap_queue_is_rejected() {
        let result = Config::from_env(&s3_env(&[("FIREWEED_BOOTSTRAP_QUEUES", "no-colon-here")]));
        let Err(err) = result else {
            panic!("missing colon must fail");
        };
        assert!(err.0.contains("FIREWEED_BOOTSTRAP_QUEUES"), "{}", err.0);
    }

    #[test]
    fn sink_endpoint_validation_is_deferred_for_enabled_and_disabled_env_configs() {
        for enabled in ["false", "true"] {
            let config = Config::from_env(&s3_env(&[
                ("FIREWEED_CHANGE_RECORD_SINK_ENABLED", enabled),
                ("FIREWEED_CHANGE_RECORD_SINK_ENDPOINT", "not-a-url"),
            ]))
            .expect("the env adapter must not validate endpoint syntax");
            assert_eq!(
                config.change_record_sink.endpoint.as_deref(),
                Some("not-a-url")
            );
        }
    }

    #[test]
    fn enabled_sink_scalar_zero_bounds_remain_env_config_errors() {
        for (key, expected) in [
            (
                "FIREWEED_CHANGE_RECORD_SINK_BATCH_SIZE",
                "FIREWEED_CHANGE_RECORD_SINK_BATCH_SIZE must be greater than 0",
            ),
            (
                "FIREWEED_CHANGE_RECORD_SINK_TICK_INTERVAL_MS",
                "FIREWEED_CHANGE_RECORD_SINK_TICK_INTERVAL_MS must be greater than 0",
            ),
        ] {
            let error = Config::from_env(&s3_env(&[
                ("FIREWEED_CHANGE_RECORD_SINK_ENABLED", "true"),
                (key, "0"),
            ]))
            .err()
            .expect("zero scalar bound must remain an env syntax error");
            assert_eq!(error.0, expected);
        }
    }

    // The Lakebase DSN carries `sslmode=require`, which only resolves on a `tls` build (a `postgres`-only
    // build correctly fails closed — see `require_dsn_fails_closed_without_tls_feature`). Gate on `tls` so
    // the `--features postgres` (no-tls) matrix leg stays green.
    #[cfg(feature = "tls")]
    #[test]
    fn lakebase_postgres_log_is_retired_on_the_public_env_surface() {
        let result = Config::from_env(&s3_env(&[
            ("FIREWEED_LOG_BACKEND", "postgres"),
            (
                "FIREWEED_POSTGRES_LOG_DATABASE_URL",
                "postgres://app:native-password@instance.lakebase.cloud:5432/db?sslmode=require",
            ),
        ]));
        let Err(err) = result else {
            panic!("postgres log is retired on the public env surface");
        };
        assert!(err.0.contains("FIREWEED_LOG_BACKEND"), "{}", err.0);
    }
}
