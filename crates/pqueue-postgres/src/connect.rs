use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use postgres::config::SslMode as PgSslMode;
use postgres::{Client, Config, NoTls};
use pqueue_core::UtcTimestamp;
use pqueue_engine::{EngineError, EngineResult};

use crate::{Credential, DatabricksCredentialProvider, RefreshingCredentialProvider};

/// Keep future pooled connections comfortably below Databricks' one-hour OAuth token TTL.
pub const DEFAULT_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);

pub fn default_max_connection_lifetime() -> Duration {
    DEFAULT_MAX_CONNECTION_LIFETIME
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSslMode {
    Disable,
    Prefer,
    Require,
}

impl PostgresSslMode {
    fn from_pg(mode: PgSslMode) -> Self {
        match mode {
            PgSslMode::Disable => Self::Disable,
            PgSslMode::Prefer => Self::Prefer,
            PgSslMode::Require => Self::Require,
            _ => Self::Require,
        }
    }

    fn to_pg(self) -> PgSslMode {
        match self {
            Self::Disable => PgSslMode::Disable,
            Self::Prefer => PgSslMode::Prefer,
            Self::Require => PgSslMode::Require,
        }
    }
}

#[derive(Clone)]
pub enum CredentialProvider {
    StaticPassword(String),
    StaticCredential(Credential),
    Refreshing(RefreshingCredentialProvider),
    Databricks(DatabricksCredentialProvider),
}

impl CredentialProvider {
    fn credential(&self, now: UtcTimestamp) -> EngineResult<Credential> {
        match self {
            Self::StaticPassword(token) => Ok(Credential {
                token: token.clone(),
                expires_at: UtcTimestamp::new(i64::MAX, 0)
                    .expect("static credential timestamp is valid"),
            }),
            Self::StaticCredential(credential) => Ok(credential.clone()),
            Self::Refreshing(provider) => provider.credential(now),
            Self::Databricks(provider) => provider.credential(now),
        }
    }

    fn postgres_user(&self) -> Option<&str> {
        match self {
            Self::Databricks(provider) => Some(provider.postgres_user()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct PostgresConnectConfig {
    url: String,
    credential_provider: Option<CredentialProvider>,
    max_connection_lifetime: Duration,
}

impl PostgresConnectConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            credential_provider: None,
            max_connection_lifetime: DEFAULT_MAX_CONNECTION_LIFETIME,
        }
    }

    pub fn with_static_password(mut self, password: impl Into<String>) -> Self {
        self.credential_provider = Some(CredentialProvider::StaticPassword(password.into()));
        self
    }

    pub fn with_credential_provider(mut self, provider: CredentialProvider) -> Self {
        self.credential_provider = Some(provider);
        self
    }

    pub fn with_max_connection_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_connection_lifetime = lifetime;
        self
    }

    pub fn max_connection_lifetime(&self) -> Duration {
        self.max_connection_lifetime
    }

    pub fn parsed_ssl_mode(&self) -> EngineResult<PostgresSslMode> {
        Ok(PostgresSslMode::from_pg(
            parse_config(&self.url)?.get_ssl_mode(),
        ))
    }

    fn postgres_config(&self, now: UtcTimestamp) -> EngineResult<Config> {
        let mut config = parse_config(&self.url)?;
        if let Some(provider) = &self.credential_provider {
            let credential = provider.credential(now)?;
            config.password(credential.token);
            if let Some(user) = provider.postgres_user() {
                config.user(user);
            }
        }
        config.ssl_mode(self.parsed_ssl_mode()?.to_pg());
        Ok(config)
    }
}

/// Build one sync postgres client through the centralized pqueue-postgres connection helper.
///
/// TLS-capable connectors are intentionally not created here yet: the current crate depends only on
/// `postgres` and drives `NoTls`, while the server-runtime/pool boundary is tracked separately. Requiring
/// TLS fails explicitly instead of falling back to plaintext.
pub fn connect(config: PostgresConnectConfig) -> EngineResult<Client> {
    let ssl_mode = config.parsed_ssl_mode()?;
    if matches!(ssl_mode, PostgresSslMode::Require) {
        return Err(EngineError::Unavailable);
    }
    st(config.postgres_config(now())?.connect(NoTls))
}

fn parse_config(url: &str) -> EngineResult<Config> {
    Config::from_str(url).map_err(|e| EngineError::Storage(e.to_string()))
}

fn now() -> UtcTimestamp {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    UtcTimestamp::new(d.as_secs() as i64, d.subsec_nanos()).expect("system nanos are normalized")
}

fn st<T>(r: Result<T, postgres::Error>) -> EngineResult<T> {
    r.map_err(|e| EngineError::Storage(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parses_libpq_url_sslmode_and_default_lifetime() {
        let config = PostgresConnectConfig::new(
            "postgres://postgres:pq@localhost:5432/postgres?sslmode=disable",
        );

        assert_eq!(config.parsed_ssl_mode().unwrap(), PostgresSslMode::Disable);
        assert_eq!(
            config.max_connection_lifetime(),
            Duration::from_secs(45 * 60)
        );
    }

    #[test]
    fn static_password_overrides_url_password_without_string_splicing() {
        let config = PostgresConnectConfig::new("host=localhost user=postgres password=old")
            .with_static_password("new-secret");

        let parsed = config
            .postgres_config(UtcTimestamp::new(0, 0).unwrap())
            .unwrap();

        assert_eq!(parsed.get_password(), Some("new-secret".as_bytes()));
    }

    #[test]
    fn refreshing_provider_injects_a_fresh_password() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_fetch = calls.clone();
        let refreshing = RefreshingCredentialProvider::new(move || {
            let n = calls_for_fetch.fetch_add(1, Ordering::SeqCst);
            Ok(Credential {
                token: format!("token-{n}"),
                expires_at: UtcTimestamp::new(10_000, 0).unwrap(),
            })
        });
        let config = PostgresConnectConfig::new("host=localhost user=postgres")
            .with_credential_provider(CredentialProvider::Refreshing(refreshing));

        let parsed = config
            .postgres_config(UtcTimestamp::new(0, 0).unwrap())
            .unwrap();

        assert_eq!(parsed.get_password(), Some("token-0".as_bytes()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn databricks_provider_exposes_postgres_user_without_live_fetch() {
        let databricks = DatabricksCredentialProvider::from_config(
            crate::DatabricksCredentialConfig::from_env_map([
                ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
                ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
                ("DATABRICKS_CLIENT_ID", "sp-client"),
                ("DATABRICKS_CLIENT_SECRET", "secret"),
            ])
            .unwrap(),
        )
        .unwrap();
        let provider = CredentialProvider::Databricks(databricks);

        assert_eq!(provider.postgres_user(), Some("sp-client"));
    }

    #[test]
    fn required_ssl_fails_before_no_tls_connection_attempt() {
        let result = connect(PostgresConnectConfig::new(
            "postgres://postgres:pq@localhost/postgres?sslmode=require",
        ));

        assert!(matches!(result, Err(EngineError::Unavailable)));
    }
}
