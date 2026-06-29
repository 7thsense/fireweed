use std::collections::BTreeMap;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pqueue_core::UtcTimestamp;
use pqueue_engine::{EngineError, EngineResult};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A short-lived database credential usable as the postgres password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub token: String,
    pub expires_at: UtcTimestamp,
}

type CredentialFetcher = dyn Fn() -> EngineResult<Credential> + Send + Sync + 'static;

/// Caches credentials from a fetcher and refreshes them before expiry.
#[derive(Clone)]
pub struct RefreshingCredentialProvider {
    fetcher: Arc<CredentialFetcher>,
    refresh_margin_seconds: i64,
    cached: Arc<Mutex<Option<Credential>>>,
}

impl RefreshingCredentialProvider {
    pub fn new<F>(fetcher: F) -> Self
    where
        F: Fn() -> EngineResult<Credential> + Send + Sync + 'static,
    {
        Self {
            fetcher: Arc::new(fetcher),
            // Databricks Lakebase OAuth credentials are one-hour tokens. Refresh with slack.
            refresh_margin_seconds: 300,
            cached: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_refresh_margin_seconds(mut self, seconds: i64) -> Self {
        self.refresh_margin_seconds = seconds.max(0);
        self
    }

    pub fn credential(&self, now: UtcTimestamp) -> EngineResult<Credential> {
        let mut cached = self.cached.lock().expect("credential cache poisoned");
        if let Some(existing) = cached.as_ref()
            && !expires_within(existing, now, self.refresh_margin_seconds)
        {
            return Ok(existing.clone());
        }
        let next = (self.fetcher)()?;
        *cached = Some(next.clone());
        Ok(next)
    }
}

fn expires_within(credential: &Credential, now: UtcTimestamp, margin_seconds: i64) -> bool {
    credential.expires_at.seconds <= now.seconds.saturating_add(margin_seconds)
}

/// Databricks authentication mode used by the CLI-backed credential fetcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabricksAuth {
    ServicePrincipal {
        client_id: String,
        client_secret: String,
    },
    Pat {
        token: String,
    },
}

/// Environment-derived Databricks database-credential settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabricksCredentialConfig {
    pub workspace_host: String,
    pub instance_name: String,
    pub postgres_user: String,
    pub auth: DatabricksAuth,
    pub cli_program: String,
}

impl DatabricksCredentialConfig {
    /// Build the config from a caller-provided iterator of `(key, value)` env-style pairs. This is a PURE
    /// mapping over the supplied vars — it does NOT touch the process environment, so it is library-safe.
    /// The composition root (the bin) collects `std::env::vars()` and passes them in; tests pass a fixture.
    pub fn from_env_map<I, K, V>(vars: I) -> EngineResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let env: BTreeMap<String, String> = vars
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let get = |key: &str| env.get(key).filter(|s| !s.is_empty()).cloned();
        let workspace_host =
            get("DATABRICKS_HOST").ok_or(EngineError::Invalid("DATABRICKS_HOST is required"))?;
        let instance_name = get("PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME")
            .or_else(|| get("DATABRICKS_DATABASE_INSTANCE_NAME"))
            .ok_or(EngineError::Invalid(
                "DATABRICKS_DATABASE_INSTANCE_NAME is required",
            ))?;
        let (auth, postgres_user) = match (
            get("DATABRICKS_CLIENT_ID"),
            get("DATABRICKS_CLIENT_SECRET"),
        ) {
            (Some(client_id), Some(client_secret)) => (
                DatabricksAuth::ServicePrincipal {
                    client_id: client_id.clone(),
                    client_secret,
                },
                client_id,
            ),
            _ => {
                let token = get("DATABRICKS_TOKEN").or_else(|| get("DATABRICKS_PAT")).ok_or(
                        EngineError::Invalid(
                            "DATABRICKS_CLIENT_ID+DATABRICKS_CLIENT_SECRET or DATABRICKS_TOKEN is required",
                        ),
                    )?;
                let postgres_user = get("PQUEUE_DATABRICKS_POSTGRES_USER")
                    .or_else(|| get("DATABRICKS_POSTGRES_USER"))
                    .ok_or(EngineError::Invalid(
                        "PQUEUE_DATABRICKS_POSTGRES_USER is required with DATABRICKS_TOKEN",
                    ))?;
                (DatabricksAuth::Pat { token }, postgres_user)
            }
        };
        Ok(Self {
            workspace_host,
            instance_name,
            postgres_user,
            auth,
            cli_program: get("DATABRICKS_CLI").unwrap_or_else(|| "databricks".to_string()),
        })
    }

    /// The postgres role/user for OAuth direct mode.
    pub fn postgres_user(&self) -> &str {
        self.postgres_user.as_str()
    }

    pub fn command(&self, request_id: &str) -> DatabricksCliCommand {
        let json = format!(
            "{{\"instance_names\":[\"{}\"]}}",
            escape_json_string(&self.instance_name)
        );
        let mut env = vec![("DATABRICKS_HOST".to_string(), self.workspace_host.clone())];
        match &self.auth {
            DatabricksAuth::ServicePrincipal {
                client_id,
                client_secret,
            } => {
                env.push(("DATABRICKS_CLIENT_ID".to_string(), client_id.clone()));
                env.push((
                    "DATABRICKS_CLIENT_SECRET".to_string(),
                    client_secret.clone(),
                ));
            }
            DatabricksAuth::Pat { token } => {
                env.push(("DATABRICKS_TOKEN".to_string(), token.clone()));
            }
        }
        DatabricksCliCommand {
            program: self.cli_program.clone(),
            args: vec![
                "database".to_string(),
                "generate-database-credential".to_string(),
                "--request-id".to_string(),
                request_id.to_string(),
                "--json".to_string(),
                json,
                "--output".to_string(),
                "json".to_string(),
            ],
            env,
        }
    }
}

/// A side-effect-free representation of the Databricks CLI command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabricksCliCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

/// Provider wrapper for Databricks OAuth-direct database credentials.
#[derive(Clone)]
pub struct DatabricksCredentialProvider {
    config: DatabricksCredentialConfig,
    inner: RefreshingCredentialProvider,
}

impl DatabricksCredentialProvider {
    pub fn from_config(config: DatabricksCredentialConfig) -> EngineResult<Self> {
        let fetcher = databricks_fetcher_with_runner(config.clone(), run_databricks_cli);
        Ok(Self {
            config,
            inner: RefreshingCredentialProvider::new(fetcher),
        })
    }

    pub fn credential(&self, now: UtcTimestamp) -> EngineResult<Credential> {
        self.inner.credential(now)
    }

    pub fn postgres_user(&self) -> &str {
        self.config.postgres_user()
    }
}

pub fn databricks_fetcher_with_runner<R>(
    config: DatabricksCredentialConfig,
    runner: R,
) -> impl Fn() -> EngineResult<Credential> + Send + Sync + 'static
where
    R: Fn(DatabricksCliCommand) -> EngineResult<String> + Send + Sync + 'static,
{
    move || {
        let request_id = next_request_id();
        let stdout = runner(config.command(&request_id))?;
        parse_databricks_credential_response(&stdout)
    }
}

fn run_databricks_cli(command: DatabricksCliCommand) -> EngineResult<String> {
    let output = Command::new(&command.program)
        .args(&command.args)
        .envs(command.env)
        .output()
        .map_err(|e| EngineError::Storage(format!("databricks CLI failed to start: {e}")))?;
    if !output.status.success() {
        return Err(EngineError::Storage(format!(
            "databricks CLI failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|e| EngineError::Storage(format!("databricks CLI output was not UTF-8: {e}")))
}

fn next_request_id() -> String {
    let n = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("pqueue-{pid}-{now}-{n}", pid = std::process::id())
}

pub fn parse_databricks_credential_response(raw: &str) -> EngineResult<Credential> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| EngineError::Storage(e.to_string()))?;
    let token = value
        .get("token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or(EngineError::Invalid("databricks response missing token"))?;
    let expires_at = ["expiration_time", "expire_time", "expires_at"]
        .iter()
        .find_map(|field| value.get(*field).and_then(parse_expiry_value))
        .ok_or(EngineError::Invalid(
            "databricks response missing expiration_time",
        ))?;
    Ok(Credential {
        token: token.to_string(),
        expires_at,
    })
}

fn parse_expiry_value(value: &serde_json::Value) -> Option<UtcTimestamp> {
    if let Some(seconds) = value.as_i64() {
        return UtcTimestamp::new(seconds, 0).ok();
    }
    if let Some(s) = value.as_str() {
        return parse_rfc3339_utc(s).ok();
    }
    None
}

fn parse_rfc3339_utc(s: &str) -> EngineResult<UtcTimestamp> {
    let s = s.strip_suffix('Z').ok_or(EngineError::Invalid(
        "expiration_time must be UTC RFC3339 ending in Z",
    ))?;
    let (date, time) = s
        .split_once('T')
        .ok_or(EngineError::Invalid("expiration_time must contain T"))?;
    let mut date_parts = date.split('-');
    let year: i32 = parse_part(date_parts.next(), "year")?;
    let month: u32 = parse_part(date_parts.next(), "month")?;
    let day: u32 = parse_part(date_parts.next(), "day")?;
    if date_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "expiration_time has too many date parts",
        ));
    }
    let mut time_and_fraction = time.splitn(2, '.');
    let hms = time_and_fraction.next().unwrap_or("");
    let nanos = match time_and_fraction.next() {
        Some(frac) => parse_nanos(frac)?,
        None => 0,
    };
    let mut time_parts = hms.split(':');
    let hour: u32 = parse_part(time_parts.next(), "hour")?;
    let minute: u32 = parse_part(time_parts.next(), "minute")?;
    let second: u32 = parse_part(time_parts.next(), "second")?;
    if time_parts.next().is_some() {
        return Err(EngineError::Invalid(
            "expiration_time has too many time parts",
        ));
    }
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return Err(EngineError::Invalid(
            "expiration_time component out of range",
        ));
    }
    let days = days_from_civil(year, month, day);
    let seconds = days
        .saturating_mul(86_400)
        .saturating_add((hour as i64) * 3_600)
        .saturating_add((minute as i64) * 60)
        .saturating_add(second as i64);
    UtcTimestamp::new(seconds, nanos).map_err(|_| EngineError::Invalid("bad expiration_time nanos"))
}

fn parse_part<T>(part: Option<&str>, name: &'static str) -> EngineResult<T>
where
    T: std::str::FromStr,
{
    part.ok_or(EngineError::Invalid(name))?
        .parse()
        .map_err(|_| EngineError::Invalid(name))
}

fn parse_nanos(frac: &str) -> EngineResult<u32> {
    if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(EngineError::Invalid("bad expiration_time fraction"));
    }
    let mut nanos = 0u32;
    for (idx, b) in frac.bytes().take(9).enumerate() {
        nanos += ((b - b'0') as u32) * 10u32.pow(8 - idx as u32);
    }
    Ok(nanos)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = year - (month <= 2) as i32;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}

fn escape_json_string(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn ts(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(seconds, 0).unwrap()
    }

    #[test]
    fn parses_recorded_databricks_credential_fixture() {
        let raw = r#"{
            "expiration_time": "2025-08-24T14:15:22Z",
            "token": "oauth-token"
        }"#;

        let credential = parse_databricks_credential_response(raw).unwrap();

        assert_eq!(credential.token, "oauth-token");
        assert_eq!(
            credential.expires_at,
            UtcTimestamp::new(1_756_044_922, 0).unwrap()
        );
    }

    #[test]
    fn parses_expiration_aliases_and_fractional_seconds() {
        let raw = r#"{"expires_at":"2025-08-24T14:15:22.123456789Z","token":"t"}"#;

        let credential = parse_databricks_credential_response(raw).unwrap();

        assert_eq!(credential.expires_at.nanoseconds, 123_456_789);
    }

    #[test]
    fn parses_postgres_endpoint_expire_time_alias() {
        let raw = r#"{"expire_time":"2025-08-24T14:15:22Z","token":"t"}"#;

        let credential = parse_databricks_credential_response(raw).unwrap();

        assert_eq!(
            credential.expires_at,
            UtcTimestamp::new(1_756_044_922, 0).unwrap()
        );
    }

    #[test]
    fn rejects_response_without_token_or_expiry() {
        assert!(parse_databricks_credential_response(r#"{"token":"t"}"#).is_err());
        assert!(
            parse_databricks_credential_response(r#"{"expiration_time":"2025-08-24T14:15:22Z"}"#)
                .is_err()
        );
    }

    #[test]
    fn builds_config_from_service_principal_env() {
        let config = DatabricksCredentialConfig::from_env_map([
            ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
            ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
            ("DATABRICKS_CLIENT_ID", "sp-client"),
            ("DATABRICKS_CLIENT_SECRET", "secret"),
        ])
        .unwrap();

        assert_eq!(config.postgres_user(), "sp-client");
        let command = config.command("req-1");
        assert_eq!(command.program, "databricks");
        assert_eq!(
            command.args,
            vec![
                "database",
                "generate-database-credential",
                "--request-id",
                "req-1",
                "--json",
                "{\"instance_names\":[\"lakebase-prod\"]}",
                "--output",
                "json"
            ]
        );
        assert!(
            command
                .env
                .contains(&("DATABRICKS_CLIENT_ID".to_string(), "sp-client".to_string()))
        );
    }

    #[test]
    fn builds_config_from_pat_env_and_project_instance_override() {
        let config = DatabricksCredentialConfig::from_env_map([
            ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
            ("DATABRICKS_DATABASE_INSTANCE_NAME", "ignored"),
            (
                "PQUEUE_DATABRICKS_DATABASE_INSTANCE_NAME",
                "lakebase-override",
            ),
            ("DATABRICKS_TOKEN", "pat"),
            ("PQUEUE_DATABRICKS_POSTGRES_USER", "user@example.com"),
            ("DATABRICKS_CLI", "/opt/bin/databricks"),
        ])
        .unwrap();

        assert_eq!(config.postgres_user(), "user@example.com");
        let command = config.command("req-2");
        assert_eq!(command.program, "/opt/bin/databricks");
        assert!(
            command
                .args
                .contains(&"{\"instance_names\":[\"lakebase-override\"]}".to_string())
        );
        assert!(
            command
                .env
                .contains(&("DATABRICKS_TOKEN".to_string(), "pat".to_string()))
        );
    }

    #[test]
    fn pat_env_requires_explicit_postgres_user() {
        let err = DatabricksCredentialConfig::from_env_map([
            ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
            ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
            ("DATABRICKS_TOKEN", "pat"),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("PQUEUE_DATABRICKS_POSTGRES_USER"));
    }

    #[test]
    fn databricks_fetcher_uses_injected_runner_without_live_call() {
        let config = DatabricksCredentialConfig::from_env_map([
            ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
            ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
            ("DATABRICKS_TOKEN", "pat"),
            ("PQUEUE_DATABRICKS_POSTGRES_USER", "user@example.com"),
        ])
        .unwrap();
        let fetcher = databricks_fetcher_with_runner(config, |cmd| {
            assert_eq!(cmd.program, "databricks");
            assert_eq!(
                &cmd.args[0..2],
                ["database", "generate-database-credential"]
            );
            Ok(r#"{"expiration_time":"2025-08-24T14:15:22Z","token":"fresh"}"#.to_string())
        });

        let credential = fetcher().unwrap();

        assert_eq!(credential.token, "fresh");
    }

    #[test]
    fn refreshing_provider_caches_until_refresh_margin() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_fetch = calls.clone();
        let provider = RefreshingCredentialProvider::new(move || {
            let n = calls_for_fetch.fetch_add(1, Ordering::SeqCst);
            Ok(Credential {
                token: format!("token-{n}"),
                expires_at: ts(1_000),
            })
        })
        .with_refresh_margin_seconds(100);

        assert_eq!(provider.credential(ts(800)).unwrap().token, "token-0");
        assert_eq!(provider.credential(ts(850)).unwrap().token, "token-0");
        assert_eq!(provider.credential(ts(901)).unwrap().token, "token-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
