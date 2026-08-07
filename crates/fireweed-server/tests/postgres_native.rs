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
use fireweed_engine::QueueKey;
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
        response_barrier: ResponseBarrierSpec::Strict,
        async_projection: None,
        sqlite_projection_deferred_flush_chunk: None,
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

fn append_query_parameter(url: &str, parameter: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{parameter}")
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

    assert!(
        result.is_err(),
        "a refused postgres connection must surface as a structured Err, got Ok"
    );
}

/// Live ADR-015/E0 structural proof: one production `start()` instance owns a fixed connection pool.
/// Queue A's real log insert is held inside `pg_sleep`; queue B is deliberately affinity-routed to a
/// different member, and its trigger releases A. Completion is therefore causal proof that B reached
/// PostgreSQL while A was still sleeping, not a host-speed or quiet-host threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_native_one_instance_pool_progresses_other_queue_during_pg_sleep() {
    let Ok(base_url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        panic!("POSTGRES NATIVE POOL E0 SKIPPED — set FIREWEED_PG_TEST_URL to a live DB");
    };
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("fireweed_pool_0b249abb_{}_{}", std::process::id(), unique);
    let application_name = format!("fireweed_pool_0b249abb_{}", std::process::id());
    let pool_size = 2usize;
    let queue_a = "pool_a";
    let queue_a_key = QueueKey::new(TenantId::new("t1").unwrap(), QueueId::new(queue_a).unwrap());
    let queue_b = (0..100)
        .map(|index| format!("pool_b_{index}"))
        .find(|candidate| {
            let key = QueueKey::new(
                TenantId::new("t1").unwrap(),
                QueueId::new(candidate).unwrap(),
            );
            fireweed_engine::queue_worker_partition(&key, pool_size)
                != fireweed_engine::queue_worker_partition(&queue_a_key, pool_size)
        })
        .expect("two queue keys must cover both pool members");

    let observer_url = base_url.clone();
    let create_schema = schema.clone();
    tokio::task::spawn_blocking(move || {
        let mut observer =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(observer_url))
                .expect("connect postgres observer");
        observer
            .batch_execute(&format!("CREATE SCHEMA {create_schema}"))
            .expect("create isolated pool schema");
    })
    .await
    .unwrap();

    let pool_url = append_query_parameter(
        &append_query_parameter(&base_url, &format!("options=-csearch_path%3D{schema}")),
        &format!("application_name={application_name}"),
    );
    let mut queues = vec![qdef_named(queue_a), qdef_named(&queue_b)];
    queues.extend((0..62).map(|index| qdef_named(&format!("density_{index}"))));
    let mut config = Config::new(
        pg_spec(pool_url, None),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        queues,
    );
    config.postgres_pool_size = pool_size;
    let server = start(config)
        .await
        .expect("one pooled postgres production server starts");

    let setup_url = base_url.clone();
    let setup_schema = schema.clone();
    let setup_application = application_name.clone();
    let setup_a = queue_a.to_string();
    let setup_b = queue_b.clone();
    tokio::task::spawn_blocking(move || {
        let mut observer =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(setup_url))
                .expect("connect postgres observer");
        let connections: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                &[&setup_application],
            )
            .expect("count production pool connections")
            .get(0);
        assert_eq!(connections as usize, pool_size);
        observer
            .batch_execute(&format!(
                "SET search_path TO {setup_schema};
                 CREATE TABLE pool_hold(queue_id TEXT PRIMARY KEY);
                 INSERT INTO pool_hold(queue_id) VALUES('{setup_a}');
                 CREATE FUNCTION pool_sleep_gate() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN
                   IF NEW.queue = '{setup_a}' THEN
                     WHILE EXISTS (SELECT 1 FROM pool_hold WHERE queue_id = '{setup_a}') LOOP
                       PERFORM pg_sleep(0.01);
                     END LOOP;
                   ELSIF NEW.queue = '{setup_b}' THEN
                     DELETE FROM pool_hold WHERE queue_id = '{setup_a}';
                   END IF;
                   RETURN NEW;
                 END $$;
                 CREATE TRIGGER pool_sleep_gate BEFORE INSERT ON log_entries
                   FOR EACH ROW EXECUTE FUNCTION pool_sleep_gate();"
            ))
            .expect("install causal pg_sleep gate");
    })
    .await
    .unwrap();

    let client = redis::Client::open(format!("redis://{}", server.addr())).unwrap();
    let mut a_connection = client.get_multiplexed_async_connection().await.unwrap();
    let a_stream = format!("t1:{queue_a}");
    let a_push = tokio::spawn(async move {
        redis::cmd("XADD")
            .arg(a_stream)
            .arg("*")
            .arg("priority")
            .arg(1)
            .query_async::<String>(&mut a_connection)
            .await
    });

    let wait_url = base_url.clone();
    let wait_application = application_name.clone();
    let a_reached_sleep = tokio::task::spawn_blocking(move || {
        let mut observer =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(wait_url))
                .expect("connect postgres observer");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            let sleeping: bool = observer
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                     WHERE application_name=$1 AND wait_event='PgSleep')",
                    &[&wait_application],
                )
                .expect("observe pg_sleep")
                .get(0);
            if sleeping {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    })
    .await
    .unwrap();
    if !a_reached_sleep {
        let release_url = base_url.clone();
        let release_schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let mut observer = fireweed_postgres::connect(
                fireweed_postgres::PostgresConnectConfig::new(release_url),
            )
            .expect("connect precondition cleanup observer");
            observer
                .batch_execute(&format!(
                    "DELETE FROM {release_schema}.pool_hold WHERE queue_id='{queue_a}'"
                ))
                .expect("release failed precondition gate");
        })
        .await
        .unwrap();
        server.shutdown_and_drain(Duration::from_secs(10)).await;
        panic!("queue A never reached the production connection's pg_sleep gate");
    }

    let mut b_connection = client.get_multiplexed_async_connection().await.unwrap();
    let b_stream = format!("t1:{queue_b}");
    let b_push = tokio::spawn(async move {
        redis::cmd("XADD")
            .arg(b_stream)
            .arg("*")
            .arg("priority")
            .arg(2)
            .query_async::<String>(&mut b_connection)
            .await
    });
    // B's trigger deletes the row that keeps A sleeping. Neither request can finish unless the one
    // production wrapper actually drives both fixed pool connections concurrently.
    let causal_result = tokio::time::timeout(Duration::from_secs(30), async {
        b_push.await.unwrap().expect("queue B push");
        a_push.await.unwrap().expect("queue A push after B release");
    })
    .await;
    if causal_result.is_err() {
        // Release the database-side gate before failing so an implementation regression cannot strand a
        // sleeping sync driver or make later tests inherit an orphaned accepted mutation.
        let release_url = base_url.clone();
        let release_schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let mut observer = fireweed_postgres::connect(
                fireweed_postgres::PostgresConnectConfig::new(release_url),
            )
            .expect("connect cleanup observer");
            observer
                .batch_execute(&format!(
                    "DELETE FROM {release_schema}.pool_hold WHERE queue_id='{queue_a}'"
                ))
                .expect("release failed causal gate");
        })
        .await
        .unwrap();
        server.shutdown_and_drain(Duration::from_secs(10)).await;
        panic!("causal pool proof deadlocked");
    }

    let count_url = base_url.clone();
    let count_application = application_name.clone();
    tokio::task::spawn_blocking(move || {
        let mut observer =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(count_url))
                .expect("connect postgres observer");
        let connections: i64 = observer
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                &[&count_application],
            )
            .expect("recount production pool connections")
            .get(0);
        assert_eq!(connections as usize, pool_size);
    })
    .await
    .unwrap();

    server.shutdown_and_drain(Duration::from_secs(10)).await;
}

/// Live smoke: env-gated on `FIREWEED_PG_TEST_URL`. Boots the server over `Backend::PostgresNative` and drives
/// push -> claim -> ack over RESP with a stock Redis client. LOUD-skips when no DB is configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_native_live_push_claim_ack_over_resp() {
    let url = std::env::var("FIREWEED_PG_TEST_URL")
        .expect("FIREWEED_PG_TEST_URL required (fail-closed live postgres; no LOUD skip)");
    // A unique search_path so reruns and parallel suites never collide on the shared queue tables.
    let schema = format!("fireweed_native_{}", std::process::id());
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
                fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(create))
                    .expect("connect to create schema");
            client
                .batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema};"))
                .expect("create schema");
        })
        .await
        .unwrap();
    }

    let server = start(Config::new(
        pg_spec(url.clone(), None),
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
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
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(drop_url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {drop_schema} CASCADE;"));
        }
    })
    .await;
}
