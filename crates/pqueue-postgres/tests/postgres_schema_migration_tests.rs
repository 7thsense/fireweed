// Integration tests: schema creation and idempotent re-migration.
// Requires Docker (uses testcontainers to spin up a Postgres instance).
//
// In OrbStack Linux, port forwarding doesn't expose mapped ports on 127.0.0.1,
// so we connect directly to the container's bridge IP on port 5432.

use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use pqueue_postgres::{PostgresControlPlaneStore, schema::DDL};

/// Start a Postgres container and return a connected client.
/// Connects via container bridge IP rather than a forwarded host port.
async fn start_pg() -> (Arc<Mutex<tokio_postgres::Client>>, impl std::fmt::Debug) {
    let pg = Postgres::default().start().await.unwrap();

    let container_ip = {
        let id = pg.id();
        let out = std::process::Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                id,
            ])
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };

    let url =
        format!("host={container_ip} port=5432 user=postgres password=postgres dbname=postgres");
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(conn);
    (Arc::new(Mutex::new(client)), pg)
}

#[tokio::test]
async fn schema_creates_pqueue_queues_table() {
    let (client_arc, _pg) = start_pg().await;

    PostgresControlPlaneStore::new(client_arc.clone())
        .await
        .unwrap();

    let client = client_arc.lock().await;
    let row = client
        .query_one(
            "SELECT COUNT(*)::int FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'pqueue_queues'",
            &[],
        )
        .await
        .unwrap();
    let count: i32 = row.get(0);
    assert_eq!(count, 1, "pqueue_queues table must exist after migration");
}

#[tokio::test]
async fn schema_creates_pqueue_shards_table() {
    let (client_arc, _pg) = start_pg().await;

    PostgresControlPlaneStore::new(client_arc.clone())
        .await
        .unwrap();

    let client = client_arc.lock().await;
    let row = client
        .query_one(
            "SELECT COUNT(*)::int FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'pqueue_shards'",
            &[],
        )
        .await
        .unwrap();
    let count: i32 = row.get(0);
    assert_eq!(count, 1, "pqueue_shards table must exist after migration");
}

#[tokio::test]
async fn schema_migration_is_idempotent() {
    let (client_arc, _pg) = start_pg().await;

    // Run migration twice; CREATE TABLE IF NOT EXISTS must not error.
    PostgresControlPlaneStore::new(client_arc.clone())
        .await
        .unwrap();
    PostgresControlPlaneStore::new(client_arc.clone())
        .await
        .unwrap();

    let client = client_arc.lock().await;
    let row = client
        .query_one(
            "SELECT COUNT(*)::int FROM information_schema.tables
             WHERE table_schema = 'public'
               AND table_name IN ('pqueue_queues', 'pqueue_shards')",
            &[],
        )
        .await
        .unwrap();
    let count: i32 = row.get(0);
    assert_eq!(count, 2, "both tables exist after double migration");
}

#[tokio::test]
async fn schema_ddl_constant_is_non_empty() {
    assert!(!DDL.trim().is_empty(), "DDL constant must not be empty");
    assert!(
        DDL.contains("pqueue_queues"),
        "DDL must define pqueue_queues"
    );
    assert!(
        DDL.contains("pqueue_shards"),
        "DDL must define pqueue_shards"
    );
}

#[tokio::test]
async fn schema_shards_fk_references_queues() {
    let (client_arc, _pg) = start_pg().await;

    PostgresControlPlaneStore::new(client_arc.clone())
        .await
        .unwrap();

    let client = client_arc.lock().await;
    let rows = client
        .query(
            "SELECT tc.constraint_type
             FROM information_schema.table_constraints tc
             WHERE tc.table_name = 'pqueue_shards'
               AND tc.constraint_type = 'FOREIGN KEY'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !rows.is_empty(),
        "pqueue_shards must have a FK constraint referencing pqueue_queues"
    );
}
