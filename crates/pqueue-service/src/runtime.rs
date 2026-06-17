//! Runtime configuration and health surface for the container entrypoint.
//!
//! This module hosts the narrow runtime wiring the `pqueue-service` binary needs
//! to run inside the production container image: environment-driven
//! configuration, the liveness/readiness health router, and the `--help` text
//! that documents the configuration contract consumed by the Helm deployment.
//!
//! The configuration keys parsed here are the contract surface that
//! `docs/deployment/container-runtime-contract.md` documents and that the Helm
//! chart populates.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use pqueue_objectlog::{
    DeploymentProfile, ManifestMode, S3CompatibleCredentials, S3CompatibleObjectLogConfig,
    probe_s3_compatible_object_path,
};
use tokio_postgres::NoTls;

use crate::{AppState, AuthContext, app_with_state};

/// Default socket address the service binds when `PQUEUE_LISTEN_ADDR` is unset.
pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8080";
/// Default bootstrap principal when `PQUEUE_PRINCIPAL_ID` is unset.
pub const DEFAULT_PRINCIPAL_ID: &str = "pqueue-service";

/// Liveness endpoint path served by [`health_router`].
pub const LIVENESS_PATH: &str = "/healthz";
/// Readiness endpoint path served by [`health_router`].
pub const READINESS_PATH: &str = "/readyz";

/// Backend storage profile selected at deploy time.
///
/// Only the two profiles in the BUILD-001 production-readiness scope are
/// accepted; see `docs/helix/04-build/DEPLOYMENT-READINESS.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendProfile {
    /// Postgres-native reference correctness backend.
    PostgresNative,
    /// fjord object-log plus SQLite projection backend.
    ObjectLogSqliteProjection,
}

impl BackendProfile {
    /// Canonical string form used in configuration and ledger evidence.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PostgresNative => "postgres_native",
            Self::ObjectLogSqliteProjection => "object_log_sqlite_projection",
        }
    }

    /// Parses a profile name, rejecting any profile outside production scope.
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        match value {
            "postgres_native" => Ok(Self::PostgresNative),
            "object_log_sqlite_projection" => Ok(Self::ObjectLogSqliteProjection),
            other => Err(ConfigError::UnsupportedBackendProfile(other.to_string())),
        }
    }
}

/// Error returned when the runtime configuration is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `PQUEUE_LISTEN_ADDR` is not a parseable `host:port` socket address.
    InvalidListenAddr { value: String, source: String },
    /// `PQUEUE_BACKEND_PROFILE` names a profile outside production scope.
    UnsupportedBackendProfile(String),
    /// Required object-log runtime configuration is missing.
    MissingObjectLogRuntimeEnv(&'static str),
    /// `PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS` is not a positive integer.
    InvalidObjectLogSegmentMaxCommands { value: String, source: String },
    /// S3-compatible object-log configuration failed validation.
    InvalidObjectLogConfig(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidListenAddr { value, source } => write!(
                f,
                "PQUEUE_LISTEN_ADDR `{value}` is not a valid socket address: {source}"
            ),
            Self::UnsupportedBackendProfile(value) => write!(
                f,
                "PQUEUE_BACKEND_PROFILE `{value}` is not supported; expected \
                 `postgres_native` or `object_log_sqlite_projection`"
            ),
            Self::MissingObjectLogRuntimeEnv(key) => {
                write!(f, "`{key}` is required for `object_log_sqlite_projection`")
            }
            Self::InvalidObjectLogSegmentMaxCommands { value, source } => write!(
                f,
                "PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS `{value}` is not a valid positive integer: {source}"
            ),
            Self::InvalidObjectLogConfig(source) => {
                write!(f, "object-log runtime configuration is invalid: {source}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Runtime configuration consumed by the container entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// Socket address the HTTP server binds.
    pub listen_addr: SocketAddr,
    /// Selected backend storage profile.
    pub backend_profile: BackendProfile,
    /// Bootstrap principal id for the service auth context.
    pub principal_id: String,
    /// Tenants the bootstrap principal is authorized for.
    pub tenants: Vec<String>,
    /// PostgreSQL connection URL used by postgres-native readiness checks.
    pub postgres_database_url: Option<String>,
    /// Object-log runtime dependencies used by object-log readiness checks.
    pub object_log: Option<ObjectLogRuntimeConfig>,
}

/// Runtime dependencies required by the `object_log_sqlite_projection` profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLogRuntimeConfig {
    /// PostgreSQL control-plane URL used by object-log manifest/control-plane wiring.
    pub postgres_control_plane_url: String,
    /// S3-compatible object-log configuration.
    pub s3: S3CompatibleObjectLogConfig,
    /// Local SQLite projection directory.
    pub sqlite_projection_dir: PathBuf,
}

impl RuntimeConfig {
    /// Builds configuration from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_getter(|key| std::env::var(key).ok())
    }

    /// Builds configuration from an arbitrary key lookup.
    ///
    /// Factored out from [`RuntimeConfig::from_env`] so the parsing/validation
    /// contract can be unit tested without mutating process-global environment.
    pub fn from_getter(getter: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let listen_raw = non_empty(getter("PQUEUE_LISTEN_ADDR"))
            .unwrap_or_else(|| DEFAULT_LISTEN_ADDR.to_string());
        let listen_addr =
            listen_raw
                .parse::<SocketAddr>()
                .map_err(|err| ConfigError::InvalidListenAddr {
                    value: listen_raw.clone(),
                    source: err.to_string(),
                })?;

        let backend_profile = match non_empty(getter("PQUEUE_BACKEND_PROFILE")) {
            Some(value) => BackendProfile::parse(&value)?,
            None => BackendProfile::PostgresNative,
        };

        let principal_id = non_empty(getter("PQUEUE_PRINCIPAL_ID"))
            .unwrap_or_else(|| DEFAULT_PRINCIPAL_ID.to_string());

        let tenants = getter("PQUEUE_TENANTS")
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|tenant| !tenant.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let postgres_database_url = non_empty(getter("PQUEUE_POSTGRES_DATABASE_URL"));
        let object_log = match backend_profile {
            BackendProfile::PostgresNative => None,
            BackendProfile::ObjectLogSqliteProjection => Some(ObjectLogRuntimeConfig::from_getter(
                &getter,
                postgres_database_url.clone(),
            )?),
        };

        Ok(Self {
            listen_addr,
            backend_profile,
            principal_id,
            tenants,
            postgres_database_url,
            object_log,
        })
    }

    /// Builds the bootstrap auth context for this configuration.
    pub fn auth_context(&self) -> AuthContext {
        AuthContext::new(self.principal_id.clone(), self.tenants.clone())
    }

    /// Builds the full HTTP router (health surface plus the API-001 app).
    pub fn router(&self) -> Router {
        service_router_with_readiness(AppState::new(self.auth_context()), self.readiness_check())
    }

    fn readiness_check(&self) -> ReadinessCheck {
        match self.backend_profile {
            BackendProfile::PostgresNative => self
                .postgres_database_url
                .clone()
                .map(ReadinessCheck::Postgres)
                .unwrap_or(ReadinessCheck::MissingPostgresDatabaseUrl),
            BackendProfile::ObjectLogSqliteProjection => self
                .object_log
                .clone()
                .map(ReadinessCheck::ObjectLogSqliteProjection)
                .unwrap_or_else(|| {
                    ReadinessCheck::ObjectLogConfigurationError(
                        "object-log runtime configuration is missing".to_string(),
                    )
                }),
        }
    }
}

impl ObjectLogRuntimeConfig {
    fn from_getter(
        getter: &impl Fn(&str) -> Option<String>,
        postgres_database_url: Option<String>,
    ) -> Result<Self, ConfigError> {
        let postgres_control_plane_url = postgres_database_url.ok_or(
            ConfigError::MissingObjectLogRuntimeEnv("PQUEUE_POSTGRES_DATABASE_URL"),
        )?;
        let endpoint_url = required_env(getter, "PQUEUE_OBJECT_LOG_ENDPOINT")?;
        let bucket = required_env(getter, "PQUEUE_OBJECT_LOG_BUCKET")?;
        let region = required_env(getter, "PQUEUE_OBJECT_LOG_REGION")?;
        let access_key_id = required_env(getter, "PQUEUE_OBJECT_LOG_ACCESS_KEY_ID")?;
        let secret_access_key = required_env(getter, "PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY")?;
        let segment_max_commands_raw =
            required_env(getter, "PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS")?;
        let max_commands_per_segment =
            segment_max_commands_raw.parse::<usize>().map_err(|err| {
                ConfigError::InvalidObjectLogSegmentMaxCommands {
                    value: segment_max_commands_raw.clone(),
                    source: err.to_string(),
                }
            })?;
        let sqlite_projection_dir =
            PathBuf::from(required_env(getter, "PQUEUE_SQLITE_PROJECTION_DIR")?);

        let s3 = S3CompatibleObjectLogConfig {
            endpoint_url,
            bucket,
            region,
            credentials: S3CompatibleCredentials {
                access_key_id,
                secret_access_key,
            },
            force_path_style: true,
            deployment_profile: DeploymentProfile::Production,
            manifest_mode: ManifestMode::ObjectStoreCas,
            max_commands_per_segment,
            dev_unsafe_one_command_segments: false,
        };
        s3.validate()
            .map_err(|err| ConfigError::InvalidObjectLogConfig(err.to_string()))?;

        Ok(Self {
            postgres_control_plane_url,
            s3,
            sqlite_projection_dir,
        })
    }
}

fn required_env(
    getter: &impl Fn(&str) -> Option<String>,
    key: &'static str,
) -> Result<String, ConfigError> {
    non_empty(getter(key)).ok_or(ConfigError::MissingObjectLogRuntimeEnv(key))
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Builds the router served by the container entrypoint.
///
/// Merges the [`health_router`] liveness/readiness probes with the API-001
/// application router so health probes and the API share one listener.
pub fn service_router(state: AppState) -> Router {
    health_router().merge(app_with_state(state))
}

/// Builds the production service router with backend-specific readiness checks.
pub fn service_router_with_readiness(state: AppState, readiness: ReadinessCheck) -> Router {
    health_router_with_readiness(readiness).merge(app_with_state(state))
}

/// Builds the standalone liveness/readiness health router.
pub fn health_router() -> Router {
    Router::new()
        .route(LIVENESS_PATH, get(liveness))
        .route(READINESS_PATH, get(readiness))
}

fn health_router_with_readiness(readiness: ReadinessCheck) -> Router {
    Router::new()
        .route(LIVENESS_PATH, get(liveness))
        .route(READINESS_PATH, get(checked_readiness))
        .with_state(HealthState { readiness })
}

/// Backend dependency checked by the production readiness endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadinessCheck {
    /// No backend dependency is currently wired into the health path.
    Ready,
    /// `postgres_native` requires a configured PostgreSQL URL.
    MissingPostgresDatabaseUrl,
    /// `postgres_native` is ready only when PostgreSQL accepts a trivial query.
    Postgres(String),
    /// `object_log_sqlite_projection` has invalid or incomplete configuration.
    ObjectLogConfigurationError(String),
    /// `object_log_sqlite_projection` dependencies must be usable.
    ObjectLogSqliteProjection(ObjectLogRuntimeConfig),
}

#[derive(Debug, Clone)]
struct HealthState {
    readiness: ReadinessCheck,
}

async fn liveness() -> &'static str {
    "ok"
}

async fn readiness() -> &'static str {
    "ready"
}

async fn checked_readiness(State(state): State<HealthState>) -> Response {
    match state.readiness {
        ReadinessCheck::Ready => (StatusCode::OK, "ready").into_response(),
        ReadinessCheck::MissingPostgresDatabaseUrl => (
            StatusCode::SERVICE_UNAVAILABLE,
            "postgres database url is not configured",
        )
            .into_response(),
        ReadinessCheck::Postgres(database_url) => postgres_readiness(&database_url).await,
        ReadinessCheck::ObjectLogConfigurationError(reason) => {
            (StatusCode::SERVICE_UNAVAILABLE, reason).into_response()
        }
        ReadinessCheck::ObjectLogSqliteProjection(config) => {
            object_log_sqlite_readiness(config).await
        }
    }
}

async fn object_log_sqlite_readiness(config: ObjectLogRuntimeConfig) -> Response {
    if config.postgres_control_plane_url.trim().is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "postgres control-plane url is not configured",
        )
            .into_response();
    }
    if ensure_sqlite_projection_dir(&config.sqlite_projection_dir).is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "sqlite projection directory is not usable",
        )
            .into_response();
    }

    let s3 = config.s3;
    let key = object_log_readiness_key();
    let probe = tokio::task::spawn_blocking(move || {
        probe_s3_compatible_object_path(&s3, &key, b"pqueue-object-log-readiness-v1")
    });
    match tokio::time::timeout(Duration::from_secs(2), probe).await {
        Ok(Ok(Ok(()))) => (StatusCode::OK, "ready").into_response(),
        Ok(Ok(Err(_))) | Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "object-log storage probe failed",
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "object-log storage probe timed out",
        )
            .into_response(),
    }
}

fn ensure_sqlite_projection_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let probe_path = path.join(format!(".pqueue-readiness-{}", std::process::id()));
    std::fs::write(&probe_path, b"ok")?;
    std::fs::remove_file(probe_path)?;
    Ok(())
}

fn object_log_readiness_key() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("pqueue/readiness/{}/{nanos}.probe", std::process::id())
}

async fn postgres_readiness(database_url: &str) -> Response {
    let connect = tokio_postgres::connect(database_url, NoTls);
    let Ok(connect_result) = tokio::time::timeout(Duration::from_secs(2), connect).await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "postgres readiness timed out",
        )
            .into_response();
    };
    let Ok((client, connection)) = connect_result else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "postgres connection failed",
        )
            .into_response();
    };

    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = tokio::time::timeout(Duration::from_secs(2), client.simple_query("SELECT 1")).await;
    drop(client);
    connection_task.abort();

    match query {
        Ok(Ok(_)) => (StatusCode::OK, "ready").into_response(),
        Ok(Err(_)) => (StatusCode::SERVICE_UNAVAILABLE, "postgres query failed").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "postgres query timed out").into_response(),
    }
}

/// Returns the `--help` text documenting the runtime configuration contract.
pub fn help_text() -> String {
    format!(
        "pqueue-service {version}\n\
         The pqueue API-001 service binary (container entrypoint).\n\
         \n\
         USAGE:\n\
         \x20\x20pqueue-service [FLAGS]\n\
         \n\
         FLAGS:\n\
         \x20\x20-h, --help       Print this configuration contract and exit\n\
         \x20\x20-V, --version    Print the service version and exit\n\
         \n\
         With no flags the service reads its configuration from the environment\n\
         and serves HTTP on the configured listen address until terminated.\n\
         \n\
         ENVIRONMENT:\n\
         \x20\x20PQUEUE_LISTEN_ADDR       host:port to bind (default {default_addr})\n\
         \x20\x20PQUEUE_BACKEND_PROFILE   postgres_native (default) |\n\
         \x20\x20                         object_log_sqlite_projection\n\
         \x20\x20PQUEUE_PRINCIPAL_ID      bootstrap principal id (default {default_principal})\n\
         \x20\x20PQUEUE_TENANTS           comma-separated tenant allowlist (default empty)\n\
         \x20\x20PQUEUE_POSTGRES_DATABASE_URL  required by postgres_native readiness\n\
         \x20\x20PQUEUE_OBJECT_LOG_ENDPOINT     required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_OBJECT_LOG_BUCKET       required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_OBJECT_LOG_REGION       required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_OBJECT_LOG_ACCESS_KEY_ID      required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY  required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS  required by object_log_sqlite_projection\n\
         \x20\x20PQUEUE_SQLITE_PROJECTION_DIR   required by object_log_sqlite_projection\n\
         \n\
         HEALTH:\n\
         \x20\x20GET {liveness}   liveness probe (200 ok)\n\
         \x20\x20GET {readiness}    readiness probe (200 ready)\n",
        version = env!("CARGO_PKG_VERSION"),
        default_addr = DEFAULT_LISTEN_ADDR,
        default_principal = DEFAULT_PRINCIPAL_ID,
        liveness = LIVENESS_PATH,
        readiness = READINESS_PATH,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply_when_env_absent() {
        let config = RuntimeConfig::from_getter(|_| None).expect("defaults are valid");
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:8080");
        assert_eq!(config.backend_profile, BackendProfile::PostgresNative);
        assert_eq!(config.principal_id, DEFAULT_PRINCIPAL_ID);
        assert!(config.tenants.is_empty());
        assert_eq!(config.postgres_database_url, None);
    }

    #[test]
    fn overrides_are_parsed() {
        let config = RuntimeConfig::from_getter(|key| match key {
            "PQUEUE_LISTEN_ADDR" => Some("127.0.0.1:9090".to_string()),
            "PQUEUE_BACKEND_PROFILE" => Some("object_log_sqlite_projection".to_string()),
            "PQUEUE_PRINCIPAL_ID" => Some("operator-deploy".to_string()),
            "PQUEUE_TENANTS" => Some(" tenant-a , tenant-b ,".to_string()),
            "PQUEUE_POSTGRES_DATABASE_URL" => {
                Some("postgres://pqueue:pqueue@postgres:5432/pqueue".to_string())
            }
            "PQUEUE_OBJECT_LOG_ENDPOINT" => Some("http://minio.local:9000".to_string()),
            "PQUEUE_OBJECT_LOG_BUCKET" => Some("pqueue-object-log".to_string()),
            "PQUEUE_OBJECT_LOG_REGION" => Some("us-east-1".to_string()),
            "PQUEUE_OBJECT_LOG_ACCESS_KEY_ID" => Some("minioadmin".to_string()),
            "PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY" => Some("minioadmin-secret".to_string()),
            "PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS" => Some("1024".to_string()),
            "PQUEUE_SQLITE_PROJECTION_DIR" => Some("/var/lib/pqueue/sqlite".to_string()),
            _ => None,
        })
        .expect("overrides are valid");
        assert_eq!(config.listen_addr.to_string(), "127.0.0.1:9090");
        assert_eq!(
            config.backend_profile,
            BackendProfile::ObjectLogSqliteProjection
        );
        assert_eq!(config.principal_id, "operator-deploy");
        assert_eq!(config.tenants, vec!["tenant-a", "tenant-b"]);
        assert_eq!(
            config.postgres_database_url,
            Some("postgres://pqueue:pqueue@postgres:5432/pqueue".to_string())
        );
        let object_log = config.object_log.expect("object-log config should parse");
        assert_eq!(object_log.s3.endpoint_url, "http://minio.local:9000");
        assert_eq!(object_log.s3.bucket, "pqueue-object-log");
        assert_eq!(object_log.s3.region, "us-east-1");
        assert_eq!(object_log.s3.max_commands_per_segment, 1024);
        assert_eq!(
            object_log.sqlite_projection_dir,
            PathBuf::from("/var/lib/pqueue/sqlite")
        );
    }

    #[test]
    fn blank_values_fall_back_to_defaults() {
        let config = RuntimeConfig::from_getter(|key| match key {
            "PQUEUE_LISTEN_ADDR" => Some("   ".to_string()),
            "PQUEUE_PRINCIPAL_ID" => Some("".to_string()),
            _ => None,
        })
        .expect("blank values fall back");
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:8080");
        assert_eq!(config.principal_id, DEFAULT_PRINCIPAL_ID);
    }

    #[test]
    fn unsupported_backend_profile_is_rejected() {
        let error = RuntimeConfig::from_getter(|key| {
            (key == "PQUEUE_BACKEND_PROFILE").then(|| "kafka_redpanda".to_string())
        })
        .expect_err("unsupported profile must be rejected");
        assert_eq!(
            error,
            ConfigError::UnsupportedBackendProfile("kafka_redpanda".to_string())
        );
    }

    #[test]
    fn invalid_listen_addr_is_rejected() {
        let error = RuntimeConfig::from_getter(|key| {
            (key == "PQUEUE_LISTEN_ADDR").then(|| "not-an-address".to_string())
        })
        .expect_err("invalid listen address must be rejected");
        assert!(matches!(error, ConfigError::InvalidListenAddr { .. }));
    }

    #[test]
    fn help_text_documents_the_contract() {
        let help = help_text();
        assert!(help.contains("PQUEUE_LISTEN_ADDR"));
        assert!(help.contains("PQUEUE_BACKEND_PROFILE"));
        assert!(help.contains("PQUEUE_POSTGRES_DATABASE_URL"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_ENDPOINT"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_BUCKET"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_REGION"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_ACCESS_KEY_ID"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_SECRET_ACCESS_KEY"));
        assert!(help.contains("PQUEUE_OBJECT_LOG_SEGMENT_MAX_COMMANDS"));
        assert!(help.contains("PQUEUE_SQLITE_PROJECTION_DIR"));
        assert!(help.contains("postgres_native"));
        assert!(help.contains("object_log_sqlite_projection"));
        assert!(help.contains(LIVENESS_PATH));
        assert!(help.contains(READINESS_PATH));
    }
}
