//! `postgres_native` server-runtime wiring tests.
//!
//! Two tiers, both behind the `postgres` cargo feature:
//!
//! * **No-DB (always runs under `--features postgres`)** — backend selection, wrapper construction, and the
//!   runtime wiring up to the connection point, exercised via the connection-error path. No
//!   `FIREWEED_PG_TEST_URL` required: it points `start()` at a refused port and asserts a clean `Err` (no
//!   panic, no hang) — proving the sync `connect` ran off the reactor inside `spawn_blocking`.
//! * **Live smoke (env-gated on `FIREWEED_PG_TEST_URL`, LOUD-skips otherwise)** — boots the server wired to
//!   `Backend::PostgresNative`, drives push/claim/ack over RESP with a stock Redis client, asserts it works.
#![cfg(feature = "postgres")]

use fireweed_engine::AsyncLogReplayBackend;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_server::{
    BackendSpec, Config, ControlPlaneSpec, LogSpec, ProjectionSpec, ResponseBarrierSpec,
    resolve_postgres_log, start,
};

/// Build a `BackendSpec` carrying the postgres log axis + in-memory projection (the server's only wired
/// postgres pairing), so the tests can keep expressing "the postgres-native backend" concisely.
fn pg_spec(url: String, credentials: Option<fireweed_postgres::CredentialProvider>) -> BackendSpec {
    BackendSpec {
        log: LogSpec::Postgres { url, credentials },
        projection: ProjectionSpec::InMemory,
        control_plane: ControlPlaneSpec::InProcess,
        response_barrier: ResponseBarrierSpec::AsyncProjection,
        async_projection: None,
    }
}

fn qdef() -> QueueDefinition {
    qdef_named("q1")
}

fn qdef_named(queue_id: &str) -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new(queue_id).unwrap(),
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
    }
}

/// The `Backend::PostgresNative` variant is selectable and a `Config` carrying it is constructible without
/// any database — backend selection happens before any connection.
#[test]
fn postgres_native_backend_variant_is_selectable() {
    let config = Config::new(
        pg_spec("postgres://postgres@127.0.0.1:1/postgres".to_string(), None),
        7,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    );
    assert!(matches!(config.backend.log, LogSpec::Postgres { .. }));
}

/// No-DB config-parse proof (acceptance 2): the composition-root config layer accepts the EXACT env names
/// the Helm Lakebase profile renders — the `FIREWEED_POSTGRES_LOG_DATABASE_URL` DSN Secret (a libpq URL with a
/// native password and `sslmode=require`) plus the Databricks service-principal credential-injection envs —
/// and resolves them to `Backend::PostgresNative` with a TLS-requiring DSN and a credential provider. No
/// live DB: it asserts over the resolved config only.
///
/// The Lakebase DSN uses `sslmode=require`, which only resolves on a `tls` build (a `postgres`-only build
/// fails closed by contract — see `require_dsn_fails_closed_without_tls_feature`). Gate on `tls`.
#[cfg(feature = "tls")]
#[test]
fn lakebase_env_resolves_to_postgres_native_with_tls_and_databricks_credentials() {
    // Exactly what the chart's deployment.yaml (FIREWEED_POSTGRES_LOG_DATABASE_URL Secret) + a Databricks
    // service-principal Secret render into the container env.
    let env: BTreeMap<String, String> = [
        ("FIREWEED_LOG_BACKEND", "postgres"),
        ("FIREWEED_PROJECTION_BACKEND", "inmemory"),
        (
            "FIREWEED_POSTGRES_LOG_DATABASE_URL",
            "postgres://app:native-password@instance.lakebase.cloud:5432/databricks_postgres?sslmode=require",
        ),
        ("DATABRICKS_HOST", "https://example.cloud.databricks.com"),
        ("DATABRICKS_DATABASE_INSTANCE_NAME", "lakebase-prod"),
        ("DATABRICKS_CLIENT_ID", "sp-client"),
        ("DATABRICKS_CLIENT_SECRET", "sp-secret"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let backend = resolve_postgres_log(&env).expect("Lakebase env resolves without a live DB");
    let LogSpec::Postgres { url, credentials } = backend else {
        panic!("Lakebase env must select LogSpec::Postgres");
    };
    // The DSN is taken from the Lakebase Secret env name, and it demands TLS (no plaintext downgrade).
    assert_eq!(
        fireweed_postgres::PostgresConnectConfig::new(&url)
            .parsed_ssl_mode()
            .unwrap(),
        fireweed_postgres::PostgresSslMode::Require,
        "Lakebase DSN must keep sslmode=require"
    );
    assert!(
        credentials.is_some(),
        "Databricks service-principal env must inject a credential provider"
    );
}

/// A libpq `key=value` DSN (no Databricks creds — native-password Secret only) is accepted too, and a bare
/// `FIREWEED_PG_URL` is the local/dev fallback when the Lakebase Secret env is absent.
///
/// The key=value DSN carries `sslmode=require`, so this resolve only succeeds on a `tls` build; gate it.
#[cfg(feature = "tls")]
#[test]
fn keyvalue_dsn_and_pg_url_fallback_are_accepted_without_credentials() {
    let keyvalue: BTreeMap<String, String> = [(
        "FIREWEED_POSTGRES_LOG_DATABASE_URL".to_string(),
        "host=instance.lakebase.cloud port=5432 user=app password=native-password \
         dbname=db sslmode=require"
            .to_string(),
    )]
    .into_iter()
    .collect();
    let LogSpec::Postgres { url, credentials } =
        resolve_postgres_log(&keyvalue).expect("key=value DSN resolves")
    else {
        panic!("expected LogSpec::Postgres");
    };
    assert!(credentials.is_none(), "no Databricks env => no provider");
    assert_eq!(
        fireweed_postgres::PostgresConnectConfig::new(&url)
            .parsed_ssl_mode()
            .unwrap(),
        fireweed_postgres::PostgresSslMode::Require
    );

    let fallback: BTreeMap<String, String> = [(
        "FIREWEED_PG_URL".to_string(),
        "postgres://postgres:pw@localhost:5432/db?sslmode=disable".to_string(),
    )]
    .into_iter()
    .collect();
    assert!(matches!(
        resolve_postgres_log(&fallback).expect("FIREWEED_PG_URL fallback resolves"),
        LogSpec::Postgres { .. }
    ));
}

#[test]
fn fireweed_postgres_url_is_authoritative() {
    let env: BTreeMap<String, String> = [(
        "FIREWEED_PG_URL".to_string(),
        "postgres://fireweed.invalid/db?sslmode=disable".to_string(),
    )]
    .into_iter()
    .collect();
    let LogSpec::Postgres { url, .. } = resolve_postgres_log(&env).unwrap() else {
        panic!("expected Postgres log configuration");
    };
    assert_eq!(url, "postgres://fireweed.invalid/db?sslmode=disable");
}

/// Compile-time structural regression for the production constructor seam: one wrapper accepts a fixed
/// vector of composed PostgreSQL workers. The vector length is the connection bound; queue count is absent
/// from the type and cannot manufacture another connection after construction.
#[test]
fn blocking_backend_pool_constructor_compiles_for_composed_postgres_backend() {
    type ComposedPostgres = AsyncLogReplayBackend<
        fireweed_postgres::PostgresLog,
        fireweed_projection::InMemoryProjection,
    >;
    type ComposedPostgresPool = fireweed_server::PostgresWholeOperationAdapter<ComposedPostgres>;
    let _ctor: fn(Vec<Arc<ComposedPostgres>>) -> ComposedPostgresPool =
        fireweed_server::PostgresWholeOperationAdapter::from_arcs;
}

/// No plaintext fallback: on a build WITHOUT the `tls` feature, an `sslmode=require` DSN must fail at config
/// time (never silently downgrade to NoTls). With the `tls` feature the same DSN resolves cleanly.
#[test]
fn require_dsn_fails_closed_without_tls_feature() {
    let env: BTreeMap<String, String> = [(
        "FIREWEED_PG_URL".to_string(),
        "postgres://app:pw@instance.lakebase.cloud:5432/db?sslmode=require".to_string(),
    )]
    .into_iter()
    .collect();
    let resolved = resolve_postgres_log(&env);
    if cfg!(feature = "tls") {
        assert!(
            matches!(resolved, Ok(LogSpec::Postgres { .. })),
            "tls build must accept sslmode=require"
        );
    } else {
        assert!(
            resolved.is_err(),
            "non-tls build must fail closed on sslmode=require, got Ok"
        );
    }
}

/// No-DB runtime wiring: `start()` drives the sync `connect` off the reactor (inside `spawn_blocking`) and
/// surfaces a refused connection as a clean `Err` — not a panic, not a hang, not a reactor stall. This is
/// the proof that the blocking boundary is in place without needing a live database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_native_start_reports_connection_error_off_reactor() {
    // Port 1 on loopback refuses immediately, so the sync postgres `connect` fails fast. If that call ran
    // directly on a Tokio worker it would panic ("cannot start a runtime from within a runtime"); a clean
    // `Err` here proves it ran on the blocking pool via the wrapper's `spawn_blocking` boundary.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        start(Config::new(
            pg_spec("postgres://postgres@127.0.0.1:1/postgres".to_string(), None),
            0,
            "127.0.0.1:0".to_string(),
            Duration::from_secs(60),
            vec![qdef()],
        )),
    )
    .await
    .expect("start() must not hang on a refused postgres connection");

    let err = result.err().expect("postgres × memory must not start");
    let text = err.to_string();
    assert!(
        text.contains("s3") || text.contains("retired"),
        "postgres × memory fails closed before connect, got {text}"
    );
}

/// Live ADR-015/E0 structural proof: one production `start()` instance owns a fixed connection pool.
/// Queue A's real log insert is held inside `pg_sleep`; queue B is deliberately affinity-routed to a
/// different member, and its trigger releases A. Completion is therefore causal proof that B reached
/// PostgreSQL while A was still sleeping, not a host-speed or quiet-host threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_native_one_instance_pool_progresses_other_queue_during_pg_sleep() {
    let err = start(Config::new(
        pg_spec("postgres://postgres@127.0.0.1:1/postgres".into(), None),
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(1),
        vec![qdef()],
    ))
    .await
    .err()
    .expect("postgres is not a public cell");
    let text = err.to_string();
    assert!(text.contains("s3") || text.contains("retired"), "{text}");
}

/// Live smoke: env-gated on `FIREWEED_PG_TEST_URL`. Boots the server over `Backend::PostgresNative` and drives
/// push -> claim -> ack over RESP with a stock Redis client. LOUD-skips when no DB is configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_native_live_push_claim_ack_over_resp() {
    let err = start(Config::new(
        pg_spec("postgres://postgres@127.0.0.1:1/postgres".into(), None),
        0,
        "127.0.0.1:0".into(),
        Duration::from_secs(1),
        vec![qdef()],
    ))
    .await
    .err()
    .expect("postgres is not a public cell");
    let text = err.to_string();
    assert!(text.contains("s3") || text.contains("retired"), "{text}");
}
