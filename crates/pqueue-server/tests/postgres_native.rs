//! `postgres_native` server-runtime wiring tests.
//!
//! Two tiers, both behind the `postgres` cargo feature:
//!
//! * **No-DB (always runs under `--features postgres`)** — backend selection, wrapper construction, and the
//!   runtime wiring up to the connection point, exercised via the connection-error path. No
//!   `PQUEUE_PG_TEST_URL` required: it points `start()` at a refused port and asserts a clean `Err` (no
//!   panic, no hang) — proving the sync `connect` ran off the reactor inside `spawn_blocking`.
//! * **Live smoke (env-gated on `PQUEUE_PG_TEST_URL`, LOUD-skips otherwise)** — boots the server wired to
//!   `Backend::PostgresNative`, drives push/claim/ack over RESP with a stock Redis client, asserts it works.
#![cfg(feature = "postgres")]

use std::collections::BTreeMap;
use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{Backend, Config, resolve_postgres_backend, start};

fn qdef() -> QueueDefinition {
    QueueDefinition {
        tenant_id: TenantId::new("t1").unwrap(),
        queue_id: QueueId::new("q1").unwrap(),
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
    }
}

/// The `Backend::PostgresNative` variant is selectable and a `Config` carrying it is constructible without
/// any database — backend selection happens before any connection.
#[test]
fn postgres_native_backend_variant_is_selectable() {
    let config = Config {
        backend: Backend::PostgresNative {
            url: "postgres://postgres@127.0.0.1:1/postgres".to_string(),
            credentials: None,
        },
        node_id: 7,
        listen: "127.0.0.1:0".to_string(),
        reclaim_interval: Duration::from_secs(60),
        queues: vec![qdef()],
    };
    assert!(matches!(config.backend, Backend::PostgresNative { .. }));
}

/// No-DB config-parse proof (acceptance 2): the composition-root config layer accepts the EXACT env names
/// the Helm Lakebase profile renders — the `PQUEUE_POSTGRES_LOG_DATABASE_URL` DSN Secret (a libpq URL with a
/// native password and `sslmode=require`) plus the Databricks service-principal credential-injection envs —
/// and resolves them to `Backend::PostgresNative` with a TLS-requiring DSN and a credential provider. No
/// live DB: it asserts over the resolved config only.
#[test]
fn lakebase_env_resolves_to_postgres_native_with_tls_and_databricks_credentials() {
    // Exactly what the chart's deployment.yaml (PQUEUE_POSTGRES_LOG_DATABASE_URL Secret) + a Databricks
    // service-principal Secret render into the container env.
    let env: BTreeMap<String, String> = [
        ("PQUEUE_LOG_BACKEND", "postgres"),
        ("PQUEUE_PROJECTION_BACKEND", "inmemory"),
        (
            "PQUEUE_POSTGRES_LOG_DATABASE_URL",
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

    let backend = resolve_postgres_backend(&env).expect("Lakebase env resolves without a live DB");
    let Backend::PostgresNative { url, credentials } = backend else {
        panic!("Lakebase env must select Backend::PostgresNative");
    };
    // The DSN is taken from the Lakebase Secret env name, and it demands TLS (no plaintext downgrade).
    assert_eq!(
        pqueue_postgres::PostgresConnectConfig::new(&url)
            .parsed_ssl_mode()
            .unwrap(),
        pqueue_postgres::PostgresSslMode::Require,
        "Lakebase DSN must keep sslmode=require"
    );
    assert!(
        credentials.is_some(),
        "Databricks service-principal env must inject a credential provider"
    );
}

/// A libpq `key=value` DSN (no Databricks creds — native-password Secret only) is accepted too, and a bare
/// `PQUEUE_PG_URL` is the local/dev fallback when the Lakebase Secret env is absent.
#[test]
fn keyvalue_dsn_and_pg_url_fallback_are_accepted_without_credentials() {
    let keyvalue: BTreeMap<String, String> = [(
        "PQUEUE_POSTGRES_LOG_DATABASE_URL".to_string(),
        "host=instance.lakebase.cloud port=5432 user=app password=native-password \
         dbname=db sslmode=require"
            .to_string(),
    )]
    .into_iter()
    .collect();
    let Backend::PostgresNative { url, credentials } =
        resolve_postgres_backend(&keyvalue).expect("key=value DSN resolves")
    else {
        panic!("expected PostgresNative");
    };
    assert!(credentials.is_none(), "no Databricks env => no provider");
    assert_eq!(
        pqueue_postgres::PostgresConnectConfig::new(&url)
            .parsed_ssl_mode()
            .unwrap(),
        pqueue_postgres::PostgresSslMode::Require
    );

    let fallback: BTreeMap<String, String> = [(
        "PQUEUE_PG_URL".to_string(),
        "postgres://postgres:pw@localhost:5432/db?sslmode=disable".to_string(),
    )]
    .into_iter()
    .collect();
    assert!(matches!(
        resolve_postgres_backend(&fallback).expect("PQUEUE_PG_URL fallback resolves"),
        Backend::PostgresNative { .. }
    ));
}

/// No plaintext fallback: on a build WITHOUT the `tls` feature, an `sslmode=require` DSN must fail at config
/// time (never silently downgrade to NoTls). With the `tls` feature the same DSN resolves cleanly.
#[test]
fn require_dsn_fails_closed_without_tls_feature() {
    let env: BTreeMap<String, String> = [(
        "PQUEUE_PG_URL".to_string(),
        "postgres://app:pw@instance.lakebase.cloud:5432/db?sslmode=require".to_string(),
    )]
    .into_iter()
    .collect();
    let resolved = resolve_postgres_backend(&env);
    if cfg!(feature = "tls") {
        assert!(
            matches!(resolved, Ok(Backend::PostgresNative { .. })),
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
        start(Config {
            backend: Backend::PostgresNative {
                url: "postgres://postgres@127.0.0.1:1/postgres".to_string(),
                credentials: None,
            },
            node_id: 0,
            listen: "127.0.0.1:0".to_string(),
            reclaim_interval: Duration::from_secs(60),
            queues: vec![qdef()],
        }),
    )
    .await
    .expect("start() must not hang on a refused postgres connection");

    assert!(
        result.is_err(),
        "a refused postgres connection must surface as a structured Err, got Ok"
    );
}

/// Live smoke: env-gated on `PQUEUE_PG_TEST_URL`. Boots the server over `Backend::PostgresNative` and drives
/// push -> claim -> ack over RESP with a stock Redis client. LOUD-skips when no DB is configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_native_live_push_claim_ack_over_resp() {
    let Ok(url) = std::env::var("PQUEUE_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES NATIVE SERVER SMOKE SKIPPED (push/claim/ack) — set PQUEUE_PG_TEST_URL to a live DB"
        );
        return;
    };
    // A unique search_path so reruns and parallel suites never collide on the shared queue tables.
    let schema = format!("pq_native_{}", std::process::id());
    let url = if url.contains("?options=") || url.contains("&options=") {
        url
    } else if url.contains('?') {
        format!("{url}&options=-csearch_path%3D{schema}")
    } else {
        format!("{url}?options=-csearch_path%3D{schema}")
    };
    // Pre-create the schema so the connection's `SET search_path` target exists.
    {
        let create = url.clone();
        let schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let mut client =
                pqueue_postgres::connect(pqueue_postgres::PostgresConnectConfig::new(create))
                    .expect("connect to create schema");
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                .expect("create schema");
        })
        .await
        .unwrap();
    }

    let server = start(Config {
        backend: Backend::PostgresNative {
            url: url.clone(),
            credentials: None,
        },
        node_id: 0,
        listen: "127.0.0.1:0".to_string(),
        reclaim_interval: Duration::from_secs(60),
        queues: vec![qdef()],
    })
    .await
    .expect("postgres_native server starts against a live DB");

    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let produced: String = redis::cmd("XADD")
        .arg("t1:q1")
        .arg("*")
        .arg("priority")
        .arg(7)
        .query_async(&mut con)
        .await
        .unwrap();

    let reply: redis::streams::StreamReadReply = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg("g")
        .arg("c")
        .arg("STREAMS")
        .arg("t1:q1")
        .arg(">")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 1, "claim returns the pushed item");
    assert_eq!(reply.keys[0].ids[0].id, produced);

    let acked: i64 = redis::cmd("XACK")
        .arg("t1:q1")
        .arg("g")
        .arg(&produced)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(acked, 1, "ack finalizes the claimed item");

    server.shutdown_and_drain(Duration::from_secs(5)).await;

    // Best-effort cleanup of the test schema.
    let drop_url = url.clone();
    let drop_schema = schema.clone();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut client) =
            pqueue_postgres::connect(pqueue_postgres::PostgresConnectConfig::new(drop_url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {drop_schema} CASCADE;"));
        }
    })
    .await;
}
