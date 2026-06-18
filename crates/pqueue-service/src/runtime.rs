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
    io::{Read, Write},
    net::SocketAddr,
    net::TcpStream,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentProfile {
    Production,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestMode {
    ObjectStoreCas,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleObjectLogConfig {
    pub endpoint_url: String,
    pub bucket: String,
    pub region: String,
    pub credentials: S3CompatibleCredentials,
    pub force_path_style: bool,
    pub deployment_profile: DeploymentProfile,
    pub manifest_mode: ManifestMode,
    pub max_commands_per_segment: usize,
    pub dev_unsafe_one_command_segments: bool,
}

impl S3CompatibleObjectLogConfig {
    fn validate(&self) -> Result<(), S3CompatibleConfigError> {
        validate_endpoint_url(&self.endpoint_url)?;
        validate_bucket(&self.bucket)?;
        validate_region(&self.region)?;
        validate_credentials(&self.credentials)?;
        if !self.force_path_style {
            return Err(S3CompatibleConfigError::UnsupportedAddressingMode);
        }
        if self.max_commands_per_segment == 0 {
            return Err(S3CompatibleConfigError::EmptySegment);
        }
        if self.deployment_profile == DeploymentProfile::Production
            && self.dev_unsafe_one_command_segments
        {
            return Err(S3CompatibleConfigError::DevUnsafeFlagInProduction);
        }
        if self.deployment_profile == DeploymentProfile::Production
            && self.max_commands_per_segment == 1
        {
            return Err(S3CompatibleConfigError::OneCommandSegmentInProduction);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum S3CompatibleConfigError {
    MissingEndpoint,
    InvalidEndpoint,
    MissingBucket,
    InvalidBucket,
    MissingRegion,
    MissingCredentials,
    UnsupportedAddressingMode,
    EmptySegment,
    OneCommandSegmentInProduction,
    DevUnsafeFlagInProduction,
}

impl std::fmt::Display for S3CompatibleConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEndpoint => write!(f, "S3-compatible endpoint URL is required"),
            Self::InvalidEndpoint => write!(
                f,
                "S3-compatible endpoint URL must be http(s) with a non-empty host"
            ),
            Self::MissingBucket => write!(f, "S3-compatible bucket is required"),
            Self::InvalidBucket => write!(f, "S3-compatible bucket name is invalid"),
            Self::MissingRegion => write!(f, "S3-compatible region is required"),
            Self::MissingCredentials => {
                write!(f, "S3-compatible access key and secret key are required")
            }
            Self::UnsupportedAddressingMode => write!(
                f,
                "object-log S3-compatible runtime currently requires path-style addressing"
            ),
            Self::EmptySegment => write!(f, "max_commands_per_segment must be greater than zero"),
            Self::OneCommandSegmentInProduction => {
                write!(f, "one-command object segments are rejected in production")
            }
            Self::DevUnsafeFlagInProduction => {
                write!(
                    f,
                    "dev_unsafe_one_command_segments cannot be set in production"
                )
            }
        }
    }
}

impl std::error::Error for S3CompatibleConfigError {}

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

fn validate_endpoint_url(endpoint_url: &str) -> Result<(), S3CompatibleConfigError> {
    let endpoint_url = endpoint_url.trim();
    if endpoint_url.is_empty() {
        return Err(S3CompatibleConfigError::MissingEndpoint);
    }
    let without_scheme = endpoint_url
        .strip_prefix("http://")
        .or_else(|| endpoint_url.strip_prefix("https://"))
        .ok_or(S3CompatibleConfigError::InvalidEndpoint)?;
    let host = without_scheme.split('/').next().unwrap_or_default();
    if host.is_empty() {
        return Err(S3CompatibleConfigError::InvalidEndpoint);
    }
    Ok(())
}

fn validate_bucket(bucket: &str) -> Result<(), S3CompatibleConfigError> {
    let bucket = bucket.trim();
    if bucket.is_empty() {
        return Err(S3CompatibleConfigError::MissingBucket);
    }
    let valid = bucket.len() >= 3
        && bucket.len() <= 63
        && bucket
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(S3CompatibleConfigError::InvalidBucket)
    }
}

fn validate_region(region: &str) -> Result<(), S3CompatibleConfigError> {
    if region.trim().is_empty() {
        Err(S3CompatibleConfigError::MissingRegion)
    } else {
        Ok(())
    }
}

fn validate_credentials(
    credentials: &S3CompatibleCredentials,
) -> Result<(), S3CompatibleConfigError> {
    if credentials.access_key_id.trim().is_empty()
        || credentials.secret_access_key.trim().is_empty()
    {
        Err(S3CompatibleConfigError::MissingCredentials)
    } else {
        Ok(())
    }
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
        .route(
            "/__pqueue/deployment/object-log-smoke/{proof_id}",
            post(object_log_deployment_smoke_write).get(object_log_deployment_smoke_verify),
        )
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

#[derive(Debug, Serialize)]
struct ObjectLogDeploymentSmokeResponse {
    proof_id: String,
    object_key: String,
    sqlite_projection_marker: String,
    recovered: bool,
}

async fn object_log_deployment_smoke_write(
    State(state): State<HealthState>,
    AxumPath(proof_id): AxumPath<String>,
) -> Response {
    object_log_deployment_smoke(state, proof_id, SmokeMode::Write).await
}

async fn object_log_deployment_smoke_verify(
    State(state): State<HealthState>,
    AxumPath(proof_id): AxumPath<String>,
) -> Response {
    object_log_deployment_smoke(state, proof_id, SmokeMode::Verify).await
}

#[derive(Debug, Clone, Copy)]
enum SmokeMode {
    Write,
    Verify,
}

async fn object_log_deployment_smoke(
    state: HealthState,
    proof_id: String,
    mode: SmokeMode,
) -> Response {
    if !valid_deployment_smoke_proof_id(&proof_id) {
        return (
            StatusCode::BAD_REQUEST,
            "proof_id must be 1-80 ASCII letters, digits, hyphens, or underscores",
        )
            .into_response();
    }
    let ReadinessCheck::ObjectLogSqliteProjection(config) = state.readiness else {
        return (
            StatusCode::NOT_FOUND,
            "object-log deployment smoke is available only for object_log_sqlite_projection",
        )
            .into_response();
    };

    let proof = proof_id.clone();
    let task = tokio::task::spawn_blocking(move || match mode {
        SmokeMode::Write => write_object_log_deployment_smoke(&config, &proof),
        SmokeMode::Verify => verify_object_log_deployment_smoke(&config, &proof),
    });
    match tokio::time::timeout(Duration::from_secs(4), task).await {
        Ok(Ok(Ok(response))) => (StatusCode::OK, Json(response)).into_response(),
        Ok(Ok(Err(message))) => (StatusCode::SERVICE_UNAVAILABLE, message).into_response(),
        Ok(Err(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "object-log deployment smoke task failed",
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "object-log deployment smoke timed out",
        )
            .into_response(),
    }
}

fn write_object_log_deployment_smoke(
    config: &ObjectLogRuntimeConfig,
    proof_id: &str,
) -> Result<ObjectLogDeploymentSmokeResponse, String> {
    ensure_sqlite_projection_dir(&config.sqlite_projection_dir)
        .map_err(|_| "sqlite projection directory is not usable".to_string())?;
    let object_key = object_log_deployment_smoke_key(proof_id);
    let marker_path = sqlite_projection_marker_path(&config.sqlite_projection_dir, proof_id);
    let payload = object_log_deployment_smoke_payload(proof_id);
    probe_s3_compatible_object_path(&config.s3, &object_key, payload.as_bytes())
        .map_err(|_| "object-log deployment smoke object write/read failed".to_string())?;
    if let Some(parent) = marker_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| "sqlite projection smoke marker directory create failed".to_string())?;
    }
    std::fs::write(&marker_path, payload.as_bytes())
        .map_err(|_| "sqlite projection smoke marker write failed".to_string())?;
    Ok(ObjectLogDeploymentSmokeResponse {
        proof_id: proof_id.to_string(),
        object_key,
        sqlite_projection_marker: marker_path.display().to_string(),
        recovered: false,
    })
}

fn verify_object_log_deployment_smoke(
    config: &ObjectLogRuntimeConfig,
    proof_id: &str,
) -> Result<ObjectLogDeploymentSmokeResponse, String> {
    ensure_sqlite_projection_dir(&config.sqlite_projection_dir)
        .map_err(|_| "sqlite projection directory is not usable".to_string())?;
    let object_key = object_log_deployment_smoke_key(proof_id);
    let marker_path = sqlite_projection_marker_path(&config.sqlite_projection_dir, proof_id);
    let expected = object_log_deployment_smoke_payload(proof_id).into_bytes();
    let object = get_s3_compatible_object_path(&config.s3, &object_key)
        .map_err(|_| "object-log deployment smoke object recovery failed".to_string())?;
    let marker = std::fs::read(&marker_path)
        .map_err(|_| "sqlite projection smoke marker recovery failed".to_string())?;
    if object != expected || marker != expected {
        return Err("object-log deployment smoke recovery payload mismatch".to_string());
    }
    Ok(ObjectLogDeploymentSmokeResponse {
        proof_id: proof_id.to_string(),
        object_key,
        sqlite_projection_marker: marker_path.display().to_string(),
        recovered: true,
    })
}

fn valid_deployment_smoke_proof_id(proof_id: &str) -> bool {
    !proof_id.is_empty()
        && proof_id.len() <= 80
        && proof_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn object_log_deployment_smoke_key(proof_id: &str) -> String {
    format!("pqueue/deployment-smoke/{proof_id}.json")
}

fn sqlite_projection_marker_path(root: &Path, proof_id: &str) -> PathBuf {
    root.join("deployment-smoke")
        .join(format!("{proof_id}.json"))
}

fn object_log_deployment_smoke_payload(proof_id: &str) -> String {
    format!("{{\"kind\":\"pqueue-object-log-deployment-smoke-v1\",\"proof_id\":\"{proof_id}\"}}")
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

fn probe_s3_compatible_object_path(
    config: &S3CompatibleObjectLogConfig,
    key: &str,
    payload: &[u8],
) -> Result<(), S3CompatibleProbeError> {
    put_s3_compatible_object_path(config, key, payload)?;
    let body = get_s3_compatible_object_path(config, key)?;
    if body == payload {
        Ok(())
    } else {
        Err(S3CompatibleProbeError::ProbePayloadMismatch)
    }
}

fn put_s3_compatible_object_path(
    config: &S3CompatibleObjectLogConfig,
    key: &str,
    payload: &[u8],
) -> Result<(), S3CompatibleProbeError> {
    config.validate().map_err(S3CompatibleProbeError::Config)?;
    let endpoint = HttpEndpoint::parse(&config.endpoint_url)?;
    let path = endpoint.path_for(&config.bucket, key);
    let put_response = http_request(&endpoint, "PUT", &path, payload)?;
    if !put_response.status_success {
        return Err(S3CompatibleProbeError::Put);
    }
    Ok(())
}

fn get_s3_compatible_object_path(
    config: &S3CompatibleObjectLogConfig,
    key: &str,
) -> Result<Vec<u8>, S3CompatibleProbeError> {
    config.validate().map_err(S3CompatibleProbeError::Config)?;
    let endpoint = HttpEndpoint::parse(&config.endpoint_url)?;
    let path = endpoint.path_for(&config.bucket, key);
    let get_response = http_request(&endpoint, "GET", &path, &[])?;
    if !get_response.status_success {
        return Err(S3CompatibleProbeError::Get);
    }
    Ok(get_response.body)
}

#[derive(Debug, Clone)]
struct HttpEndpoint {
    host_header: String,
    address: String,
    base_path: String,
}

impl HttpEndpoint {
    fn parse(endpoint_url: &str) -> Result<Self, S3CompatibleProbeError> {
        let endpoint_url = endpoint_url.trim().trim_end_matches('/');
        let without_scheme = endpoint_url
            .strip_prefix("http://")
            .ok_or(S3CompatibleProbeError::UnsupportedEndpointScheme)?;
        let (host_port, base_path) = without_scheme
            .split_once('/')
            .map(|(host, path)| (host, format!("/{path}")))
            .unwrap_or((without_scheme, String::new()));
        if host_port.is_empty() {
            return Err(S3CompatibleProbeError::UnsupportedEndpointScheme);
        }
        let address = if host_port.contains(':') {
            host_port.to_string()
        } else {
            format!("{host_port}:80")
        };
        Ok(Self {
            host_header: host_port.to_string(),
            address,
            base_path,
        })
    }

    fn path_for(&self, bucket: &str, key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_path.trim_end_matches('/'),
            bucket.trim_matches('/'),
            key.trim_start_matches('/')
        )
    }
}

#[derive(Debug, Clone)]
struct HttpResponse {
    status_success: bool,
    body: Vec<u8>,
}

fn http_request(
    endpoint: &HttpEndpoint,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<HttpResponse, S3CompatibleProbeError> {
    let mut stream =
        TcpStream::connect(&endpoint.address).map_err(|_| S3CompatibleProbeError::Connect)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| S3CompatibleProbeError::Io)?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| S3CompatibleProbeError::Io)?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        endpoint.host_header,
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|_| S3CompatibleProbeError::Io)?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|_| S3CompatibleProbeError::Io)?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(S3CompatibleProbeError::InvalidResponse)?;
    let header = String::from_utf8_lossy(&response[..header_end]);
    let status_line = header.lines().next().unwrap_or_default();
    Ok(HttpResponse {
        status_success: status_line.contains(" 200 ")
            || status_line.contains(" 201 ")
            || status_line.contains(" 204 "),
        body: response[header_end + 4..].to_vec(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum S3CompatibleProbeError {
    Config(S3CompatibleConfigError),
    UnsupportedEndpointScheme,
    Connect,
    Io,
    InvalidResponse,
    Put,
    Get,
    ProbePayloadMismatch,
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
