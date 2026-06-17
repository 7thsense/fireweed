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

use std::net::SocketAddr;

use axum::{Router, routing::get};

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

        Ok(Self {
            listen_addr,
            backend_profile,
            principal_id,
            tenants,
        })
    }

    /// Builds the bootstrap auth context for this configuration.
    pub fn auth_context(&self) -> AuthContext {
        AuthContext::new(self.principal_id.clone(), self.tenants.clone())
    }

    /// Builds the full HTTP router (health surface plus the API-001 app).
    pub fn router(&self) -> Router {
        service_router(AppState::new(self.auth_context()))
    }
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

/// Builds the standalone liveness/readiness health router.
pub fn health_router() -> Router {
    Router::new()
        .route(LIVENESS_PATH, get(liveness))
        .route(READINESS_PATH, get(readiness))
}

async fn liveness() -> &'static str {
    "ok"
}

async fn readiness() -> &'static str {
    "ready"
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
    }

    #[test]
    fn overrides_are_parsed() {
        let config = RuntimeConfig::from_getter(|key| match key {
            "PQUEUE_LISTEN_ADDR" => Some("127.0.0.1:9090".to_string()),
            "PQUEUE_BACKEND_PROFILE" => Some("object_log_sqlite_projection".to_string()),
            "PQUEUE_PRINCIPAL_ID" => Some("operator-deploy".to_string()),
            "PQUEUE_TENANTS" => Some(" tenant-a , tenant-b ,".to_string()),
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
        assert!(help.contains("postgres_native"));
        assert!(help.contains("object_log_sqlite_projection"));
        assert!(help.contains(LIVENESS_PATH));
        assert!(help.contains(READINESS_PATH));
    }
}
