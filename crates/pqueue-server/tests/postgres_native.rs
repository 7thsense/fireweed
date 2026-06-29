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

use std::time::Duration;

use pqueue_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use pqueue_server::{Backend, Config, start};

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
        },
        node_id: 7,
        listen: "127.0.0.1:0".to_string(),
        reclaim_interval: Duration::from_secs(60),
        queues: vec![qdef()],
    };
    assert!(matches!(config.backend, Backend::PostgresNative { .. }));
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
        backend: Backend::PostgresNative { url: url.clone() },
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
