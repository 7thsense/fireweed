//! Runtime wiring for the postgres-log × {sqlite, postgres}-projection combos (ADR-012 P2 gap-closure).
//!
//! Both combos assemble their composed backend the SAME off-reactor way as the already-wired
//! postgres/inmemory combo (`crates/fireweed-server/tests/postgres_native.rs`): connect + recover inside
//! `spawn_blocking`, then drive every port through the bounded whole-operation adapter so no sync
//! postgres client call ever runs on a Tokio reactor worker (it would panic — "cannot start a runtime from
//! within a runtime"). These tests boot the full server over each combo and drive push → claim → finalize
//! over RESP, proving the composition survives a real `#[tokio::test]` runtime end to end.
//!
//! Env-gated on `FIREWEED_PG_TEST_URL`; LOUD-skips (not silently) when no live database is configured.
#![cfg(feature = "postgres")]

use fireweed_core::{
    EligibilityPolicy, OrderingMode, PriorityDirection, PriorityModel, PriorityModelKind,
    PriorityTieBreaker, QueueDefinition, QueueId, RecurrencePolicy, RetryPolicy, TenantId,
};
use fireweed_server::{BackendSpec, Config, ControlPlaneSpec, LogSpec, ProjectionSpec, start};
use std::time::Duration;

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
        entity_schema: None,
        typed_indexes: vec![],
        emit_change_records: true,
    }
}

/// A unique `?options=-csearch_path=<schema>` DSN, so parallel/rerun test runs never collide on the shared
/// queue tables — same trick `postgres_native.rs`'s live smoke uses for the log axis.
fn url_with_schema(url: &str, schema: &str) -> String {
    if url.contains("?options=") || url.contains("&options=") {
        url.to_string()
    } else if url.contains('?') {
        format!("{url}&options=-csearch_path%3D{schema}")
    } else {
        format!("{url}?options=-csearch_path%3D{schema}")
    }
}

async fn create_schema(url: &str, schema: &str) {
    let create = url.to_string();
    let schema = schema.to_string();
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

async fn drop_schema(url: &str, schema: &str) {
    let drop_url = url.to_string();
    let drop_schema = schema.to_string();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut client) =
            fireweed_postgres::connect(fireweed_postgres::PostgresConnectConfig::new(drop_url))
        {
            let _ = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {drop_schema} CASCADE;"));
        }
    })
    .await;
}

async fn push_claim_finalize_over_resp(addr: std::net::SocketAddr) {
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
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
    assert_eq!(acked, 1, "finalize (ack) commits the claimed item");
}

/// Composed postgres-log + sqlite-projection backend, driven end to end under a real Tokio runtime: proves
/// the `spawn_blocking` + whole-operation boundary covers this combo the same way it covers postgres/inmemory
/// (no reactor-thread panic on the sync postgres `connect`/`recover`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_sqlite_combo_runs_under_tokio() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES/SQLITE COMBO SMOKE SKIPPED (push/claim/finalize) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = format!("fireweed_pgsqlite_{}", std::process::id());
    let scoped_url = url_with_schema(&url, &schema);
    create_schema(&url, &schema).await;

    let sqlite_path = std::env::temp_dir().join(format!(
        "fireweed-server-postgres-sqlite-combo-{}-projection.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&sqlite_path);

    let backend = BackendSpec {
        log: LogSpec::Postgres {
            url: scoped_url,
            credentials: None,
        },
        projection: ProjectionSpec::Sqlite {
            path: sqlite_path.clone(),
        },
        control_plane: ControlPlaneSpec::InProcess,
    };
    let server = start(Config::new(
        backend,
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await
    .expect("postgres/sqlite combo server starts under tokio against a live DB");

    push_claim_finalize_over_resp(server.addr()).await;

    server.shutdown_and_drain(Duration::from_secs(5)).await;
    let _ = std::fs::remove_file(&sqlite_path);
    drop_schema(&url, &schema).await;
}

/// Unified atomic postgres/postgres backend through the production fixed-pool selector.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_postgres_combo_runs_under_tokio() {
    let Ok(url) = std::env::var("FIREWEED_PG_TEST_URL") else {
        eprintln!(
            "POSTGRES/POSTGRES COMBO SMOKE SKIPPED (push/claim/finalize) — set FIREWEED_PG_TEST_URL to a live DB"
        );
        return;
    };
    let schema = format!("fireweed_pgpg_atomic_{}", std::process::id());
    let atomic_url = url_with_schema(&url, &schema);
    create_schema(&url, &schema).await;

    let backend = BackendSpec {
        log: LogSpec::Postgres {
            url: atomic_url.clone(),
            credentials: None,
        },
        projection: ProjectionSpec::Postgres { url: atomic_url },
        control_plane: ControlPlaneSpec::InProcess,
    };
    let server = start(Config::new(
        backend,
        0,
        "127.0.0.1:0".to_string(),
        Duration::from_secs(60),
        vec![qdef()],
    ))
    .await
    .expect("postgres/postgres combo server starts under tokio against a live DB");

    push_claim_finalize_over_resp(server.addr()).await;

    server.shutdown_and_drain(Duration::from_secs(5)).await;
    drop_schema(&url, &schema).await;
}
