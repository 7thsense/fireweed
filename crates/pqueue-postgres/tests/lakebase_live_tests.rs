//! Live acceptance tests against a real Databricks Lakebase instance.
//!
//! These are `#[ignore]` and opt-in: they require a provisioned Lakebase
//! instance and the `tls` feature. Provision with `scripts/lakebase/provision.sh`
//! (needs an authenticated Databricks CLI), then:
//!
//! ```sh
//! export PQUEUE_LAKEBASE_DSN="host=ep-xxx.databricks.com port=5432 \
//!   user=ROLE password=TOKEN dbname=databricks_postgres sslmode=require"
//! cargo test -p pqueue-postgres --features tls --test lakebase_live_tests -- --ignored --nocapture
//! ```
//!
//! They prove the Lakebase-critical guarantees: a TLS handshake to the managed
//! endpoint, that pqueue's claim primitive (`FOR UPDATE SKIP LOCKED`) gives
//! single-active-lease there, and DDL apply.

use std::time::Instant;

use pqueue_postgres::connect::{StaticPassword, connect_str};
use tokio_postgres::Client;

fn dsn() -> Option<String> {
    std::env::var("PQUEUE_LAKEBASE_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Connect using the DSN's own password (StaticPassword no-op override keeps it).
async fn connect(dsn: &str) -> Client {
    // The DSN already carries the password; StaticPassword re-supplies it so the
    // helper's credential-override path is exercised end to end.
    let password = dsn
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("password="))
        .expect("DSN must contain password=")
        .to_string();
    let (client, _conn) = connect_str(dsn, &StaticPassword(password))
        .await
        .expect("connect to Lakebase over TLS");
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a live Lakebase instance + PQUEUE_LAKEBASE_DSN"]
async fn lakebase_tls_connect_and_select() {
    let Some(dsn) = dsn() else {
        eprintln!("PQUEUE_LAKEBASE_DSN unset; skipping");
        return;
    };
    let started = Instant::now();
    let client = connect(&dsn).await;
    let row = client.query_one("SELECT 1::int AS one", &[]).await.unwrap();
    let one: i32 = row.get("one");
    assert_eq!(one, 1);
    eprintln!(
        "lakebase TLS connect + SELECT 1 ok in {} ms",
        started.elapsed().as_millis()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a live Lakebase instance + PQUEUE_LAKEBASE_DSN"]
async fn lakebase_skip_locked_single_active_lease() {
    let Some(dsn) = dsn() else {
        eprintln!("PQUEUE_LAKEBASE_DSN unset; skipping");
        return;
    };
    let setup = connect(&dsn).await;
    // Disposable table; pgcrypto/uuid not required.
    setup
        .batch_execute(
            "DROP TABLE IF EXISTS pqueue_lakebase_probe;
             CREATE TABLE pqueue_lakebase_probe (id int primary key, leased bool not null default false);
             INSERT INTO pqueue_lakebase_probe (id) SELECT g FROM generate_series(1, 100) g;",
        )
        .await
        .expect("DDL apply on Lakebase");

    // Two independent connections claim concurrently with FOR UPDATE SKIP LOCKED.
    let claim = |mut client: Client, batch: i64| async move {
        let tx = client.transaction().await.unwrap();
        let rows = tx
            .query(
                "SELECT id FROM pqueue_lakebase_probe WHERE NOT leased \
                 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $1",
                &[&batch],
            )
            .await
            .unwrap();
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("id")).collect();
        for id in &ids {
            tx.execute(
                "UPDATE pqueue_lakebase_probe SET leased = true WHERE id = $1",
                &[id],
            )
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        ids
    };

    let a = connect(&dsn).await;
    let b = connect(&dsn).await;
    let (ids_a, ids_b) = tokio::join!(claim(a, 30), claim(b, 30));

    // SKIP LOCKED must hand disjoint rows to the two claimers (single active lease).
    let overlap: Vec<_> = ids_a.iter().filter(|id| ids_b.contains(id)).collect();
    assert!(
        overlap.is_empty(),
        "SKIP LOCKED leaked rows to two claimers on Lakebase: {overlap:?}"
    );
    assert!(
        !ids_a.is_empty() && !ids_b.is_empty(),
        "both claimers got work"
    );

    setup
        .batch_execute("DROP TABLE IF EXISTS pqueue_lakebase_probe;")
        .await
        .ok();
    eprintln!(
        "lakebase SKIP LOCKED single-active-lease ok: A={} B={} rows, no overlap",
        ids_a.len(),
        ids_b.len()
    );
}
