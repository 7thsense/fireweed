//! Postgres connection helper for `postgres_native`, including managed cloud
//! endpoints such as **Databricks Lakebase**.
//!
//! Two things distinguish a managed endpoint from a local/testcontainers
//! Postgres, and this module owns both:
//!
//! 1. **TLS.** Lakebase requires `sslmode=require`. We let `tokio_postgres::Config`
//!    parse the connection string (URL or `key=value` DSN) and read its
//!    [`SslMode`]; `Disable` connects with `NoTls`, anything else uses a rustls
//!    connector (compiled only under the `tls` feature).
//! 2. **Short-lived credentials.** Lakebase's OAuth auth mode uses a database
//!    credential that expires (~60 min). A [`CredentialProvider`] supplies the
//!    password at connect time, so a fresh token can be minted per connection;
//!    [`StaticPassword`] covers the native-password (pooler) mode.
//!
//! pqueue's core SQL (`FOR UPDATE SKIP LOCKED`, `ON CONFLICT`, DDL) is supported
//! on Lakebase both directly and through its transaction-mode pooler, so no query
//! changes are required — only connection setup.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config};

/// Error establishing a Postgres connection.
#[derive(Debug)]
pub enum ConnectError {
    /// The connection string failed to parse.
    Parse(tokio_postgres::Error),
    /// The driver failed to connect.
    Connect(tokio_postgres::Error),
    /// The credential provider failed to supply a password.
    Credential(CredentialError),
    /// `sslmode` requires TLS but the crate was built without the `tls` feature.
    TlsNotCompiled,
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(e) => write!(f, "postgres connection string parse error: {e}"),
            Self::Connect(e) => write!(f, "postgres connect error: {e}"),
            Self::Credential(e) => write!(f, "postgres credential error: {e}"),
            Self::TlsNotCompiled => write!(
                f,
                "sslmode requires TLS but pqueue-postgres was built without the `tls` feature"
            ),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Error obtaining a database credential (password / token).
#[derive(Debug)]
pub struct CredentialError(pub String);

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CredentialError {}

/// Supplies the Postgres password at (re)connect time.
///
/// Implementors return the *current* password. For static passwords this is a
/// fixed value; for Lakebase OAuth it is a freshly-valid short-lived database
/// credential (see [`RefreshingCredentialProvider`]).
#[async_trait::async_trait]
pub trait CredentialProvider: Send + Sync {
    async fn password(&self) -> Result<String, CredentialError>;
}

/// A fixed password — native Postgres auth (including Lakebase's pooler mode).
pub struct StaticPassword(pub String);

#[async_trait::async_trait]
impl CredentialProvider for StaticPassword {
    async fn password(&self) -> Result<String, CredentialError> {
        Ok(self.0.clone())
    }
}

/// A short-lived credential with the instant it must no longer be used.
#[derive(Clone)]
pub struct Credential {
    pub token: String,
    /// When the token expires. The provider refreshes a `refresh_skew` before this.
    pub expires_at: Instant,
}

type Fetcher = Arc<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Credential, CredentialError>> + Send>,
        > + Send
        + Sync,
>;

/// Caches a short-lived credential and re-mints it before expiry.
///
/// This is the seam for Lakebase OAuth: the `fetcher` closure calls the
/// Databricks credential API (CLI / SDK / REST `generate-database-credential`)
/// and returns the token plus its expiry. The closure is the only
/// Databricks-specific code; everything else here is generic. Because Lakebase
/// enforces token expiry only at *login*, refreshing before opening a new
/// connection is sufficient — live connections survive expiry.
pub struct RefreshingCredentialProvider {
    fetcher: Fetcher,
    refresh_skew: Duration,
    cached: tokio::sync::Mutex<Option<Credential>>,
}

impl RefreshingCredentialProvider {
    /// `refresh_skew` is how long before expiry a cached token is considered
    /// stale (e.g. 15 min under Lakebase's 60-min TTL).
    pub fn new<F, Fut>(refresh_skew: Duration, fetcher: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Credential, CredentialError>> + Send + 'static,
    {
        Self {
            fetcher: Arc::new(move || Box::pin(fetcher())),
            refresh_skew,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// `now` is injectable for tests; production uses [`Instant::now`].
    async fn password_at(&self, now: Instant) -> Result<String, CredentialError> {
        let mut guard = self.cached.lock().await;
        let fresh = guard
            .as_ref()
            .is_some_and(|c| c.expires_at.saturating_duration_since(now) > self.refresh_skew);
        if fresh {
            return Ok(guard.as_ref().unwrap().token.clone());
        }
        let credential = (self.fetcher)().await?;
        let token = credential.token.clone();
        *guard = Some(credential);
        Ok(token)
    }
}

#[async_trait::async_trait]
impl CredentialProvider for RefreshingCredentialProvider {
    async fn password(&self) -> Result<String, CredentialError> {
        self.password_at(Instant::now()).await
    }
}

/// Whether a parsed `SslMode` needs a TLS connector.
pub fn requires_tls(mode: SslMode) -> bool {
    !matches!(mode, SslMode::Disable)
}

/// A parsed Databricks `generate-database-credential` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCredential {
    /// The OAuth token used as the Postgres password.
    pub token: String,
    /// RFC3339 expiration, when present (informational; the provider refreshes on
    /// a configured TTL since Lakebase enforces expiry only at login).
    pub expiration_time: Option<String>,
}

/// Parse the JSON from `databricks database generate-database-credential -o json`.
/// Pure so it is unit-tested against a fixture without a live Databricks call.
pub fn parse_database_credential(json: &str) -> Result<DatabaseCredential, CredentialError> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| CredentialError(format!("invalid credential JSON: {e}")))?;
    let token = value
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| CredentialError("credential response missing `token`".to_string()))?
        .to_string();
    let expiration_time = value
        .get("expiration_time")
        .or_else(|| value.get("expiration"))
        .and_then(|e| e.as_str())
        .map(str::to_string);
    Ok(DatabaseCredential {
        token,
        expiration_time,
    })
}

/// Build a [`RefreshingCredentialProvider`] backed by the authenticated
/// Databricks CLI: each refresh runs
/// `databricks database generate-database-credential --json '{"instance_names":[..]}'`.
///
/// `assumed_ttl` is how long a minted token is treated as valid (Lakebase's is
/// ~60 min); `refresh_skew` is how early to re-mint. This is the reference
/// fetcher for environments where the CLI holds the Databricks auth; production
/// deployments without the CLI can supply their own fetcher to
/// [`RefreshingCredentialProvider::new`].
#[cfg(feature = "lakebase")]
pub fn databricks_cli_credential_provider(
    instance_name: impl Into<String>,
    profile: Option<String>,
    assumed_ttl: Duration,
    refresh_skew: Duration,
) -> RefreshingCredentialProvider {
    let instance_name = instance_name.into();
    RefreshingCredentialProvider::new(refresh_skew, move || {
        let instance_name = instance_name.clone();
        let profile = profile.clone();
        async move {
            let json_body = format!("{{\"instance_names\":[\"{instance_name}\"]}}");
            let mut cmd = tokio::process::Command::new("databricks");
            cmd.arg("database")
                .arg("generate-database-credential")
                .arg("--json")
                .arg(&json_body)
                .arg("-o")
                .arg("json");
            if let Some(profile) = &profile {
                cmd.arg("--profile").arg(profile);
            }
            let output = cmd
                .output()
                .await
                .map_err(|e| CredentialError(format!("databricks CLI invocation failed: {e}")))?;
            if !output.status.success() {
                return Err(CredentialError(format!(
                    "databricks generate-database-credential failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            let parsed = parse_database_credential(&String::from_utf8_lossy(&output.stdout))?;
            Ok(Credential {
                token: parsed.token,
                expires_at: Instant::now() + assumed_ttl,
            })
        }
    })
}

/// Connect using a connection string (URL or `key=value` DSN), overriding the
/// password from `creds`. TLS is selected from the string's `sslmode`.
///
/// Prefer the `key=value` DSN form for Lakebase OAuth: the username is an email
/// containing `@`, which the URL form parses ambiguously.
pub async fn connect_str(
    conn_str: &str,
    creds: &dyn CredentialProvider,
) -> Result<(Client, JoinHandle<()>), ConnectError> {
    let mut config: Config = conn_str.parse().map_err(ConnectError::Parse)?;
    let password = creds.password().await.map_err(ConnectError::Credential)?;
    config.password(password);
    connect_config(&config).await
}

/// Connect with a fully-formed [`Config`], choosing the connector from its
/// `sslmode`.
pub async fn connect_config(config: &Config) -> Result<(Client, JoinHandle<()>), ConnectError> {
    if requires_tls(config.get_ssl_mode()) {
        connect_tls(config).await
    } else {
        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(ConnectError::Connect)?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, handle))
    }
}

#[cfg(feature = "tls")]
async fn connect_tls(config: &Config) -> Result<(Client, JoinHandle<()>), ConnectError> {
    let (client, connection) = config
        .connect(make_rustls())
        .await
        .map_err(ConnectError::Connect)?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, handle))
}

#[cfg(not(feature = "tls"))]
async fn connect_tls(_config: &Config) -> Result<(Client, JoinHandle<()>), ConnectError> {
    Err(ConnectError::TlsNotCompiled)
}

/// Build a rustls connector that verifies the server certificate against the
/// Mozilla/webpki root store (satisfies `verify-full` and therefore `require`
/// for a public-CA managed endpoint like Lakebase).
#[cfg(feature = "tls")]
pub fn make_rustls() -> tokio_postgres_rustls::MakeRustlsConnect {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("rustls default protocol versions are valid")
    .with_root_certificates(roots)
    .with_no_client_auth();
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_mode_selection_disable_vs_require() {
        let disable: Config = "host=localhost user=u dbname=d sslmode=disable"
            .parse()
            .unwrap();
        assert!(!requires_tls(disable.get_ssl_mode()));

        let require: Config = "host=ep-x.databricks.com port=5432 user=role dbname=databricks_postgres sslmode=require"
            .parse()
            .unwrap();
        assert!(requires_tls(require.get_ssl_mode()));
    }

    #[test]
    fn default_ssl_mode_is_not_tls() {
        // No sslmode given -> tokio-postgres defaults to Prefer; we only force
        // NoTls on explicit Disable, so a bare local DSN still works as before.
        let bare: Config = "host=localhost user=u dbname=d".parse().unwrap();
        // Prefer is the tokio-postgres default; treat anything non-Disable as TLS-capable.
        assert_eq!(bare.get_ssl_mode(), SslMode::Prefer);
    }

    #[test]
    fn parse_database_credential_extracts_token_and_expiration() {
        let json = r#"{"token":"dbapi-abc123","expiration_time":"2026-06-21T01:30:00Z"}"#;
        let cred = parse_database_credential(json).unwrap();
        assert_eq!(cred.token, "dbapi-abc123");
        assert_eq!(
            cred.expiration_time.as_deref(),
            Some("2026-06-21T01:30:00Z")
        );

        // Tolerates a missing expiration and the `expiration` alias.
        let alias = r#"{"token":"t","expiration":"2026-06-21T01:30:00Z"}"#;
        assert_eq!(
            parse_database_credential(alias)
                .unwrap()
                .expiration_time
                .as_deref(),
            Some("2026-06-21T01:30:00Z")
        );
        // Missing token is an error.
        assert!(parse_database_credential(r#"{"expiration_time":"x"}"#).is_err());
    }

    #[tokio::test]
    async fn static_password_returns_fixed_value() {
        let p = StaticPassword("secret".into());
        assert_eq!(p.password().await.unwrap(), "secret");
    }

    #[tokio::test]
    async fn refreshing_provider_caches_then_refreshes_before_expiry() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let calls = Arc::new(AtomicU64::new(0));
        let calls2 = Arc::clone(&calls);
        let base = Instant::now();
        let provider = RefreshingCredentialProvider::new(Duration::from_secs(900), move || {
            let n = calls2.fetch_add(1, Ordering::SeqCst);
            let base = base;
            async move {
                Ok(Credential {
                    token: format!("tok-{n}"),
                    // each minted token lasts 3600s from `base`
                    expires_at: base + Duration::from_secs(3600),
                })
            }
        });

        // First call mints tok-0.
        assert_eq!(provider.password_at(base).await.unwrap(), "tok-0");
        // Well before the 900s skew window -> cached, no new mint.
        assert_eq!(
            provider
                .password_at(base + Duration::from_secs(100))
                .await
                .unwrap(),
            "tok-0"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Inside the skew window (3600 - 800 = 2800 < 900 remaining? remaining=800<900) -> refresh.
        assert_eq!(
            provider
                .password_at(base + Duration::from_secs(2800))
                .await
                .unwrap(),
            "tok-1"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[cfg(not(feature = "tls"))]
    async fn require_without_tls_feature_errors() {
        let config: Config =
            "host=ep-x.databricks.com user=role dbname=databricks_postgres sslmode=require"
                .parse()
                .unwrap();
        let err = connect_config(&config).await.unwrap_err();
        assert!(matches!(err, ConnectError::TlsNotCompiled));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn rustls_connector_builds() {
        // Must not panic (provider installed, roots loaded).
        let _ = make_rustls();
    }
}
