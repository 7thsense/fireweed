#![cfg(feature = "local")]

use std::error::Error;
use turso::{Builder, Connection};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

async fn integer(connection: &Connection, sql: &str) -> Result<i64> {
    let mut rows = connection.query(sql, ()).await?;
    Ok(rows.next().await?.expect("one result row").get::<i64>(0)?)
}

async fn insert_blobs(connection: &Connection, count: usize) -> Result {
    let values = (1..=count)
        .map(|id| format!("({id},zeroblob(8192))"))
        .collect::<Vec<_>>()
        .join(",");
    connection
        .execute(format!("INSERT INTO items VALUES {values}"), ())
        .await?;
    Ok(())
}

async fn cached_count(connection: &Connection) -> Result<i64> {
    let mut statement = connection
        .prepare_cached("SELECT count(*) FROM items")
        .await?;
    let mut rows = statement.query(()).await?;
    Ok(rows.next().await?.expect("count row").get::<i64>(0)?)
}

#[tokio::test]
async fn explicit_checkpoints_preserve_cached_readback_across_connections() -> Result {
    let root = tempfile::tempdir()?;
    let path = root.path().join("policy.turso");
    let database = Builder::new_local(path.to_str().unwrap()).build().await?;
    let writer = database.connect()?;
    let reader = database.connect()?;
    writer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB)", ())
        .await?;
    assert_eq!(cached_count(&reader).await?, 0);
    insert_blobs(&writer, 64).await?;
    assert_eq!(cached_count(&reader).await?, 64);
    writer.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    assert_eq!(cached_count(&reader).await?, 64);
    writer
        .execute("INSERT INTO items VALUES(65,zeroblob(8192))", ())
        .await?;
    reader.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    assert_eq!(cached_count(&writer).await?, 65);
    assert_eq!(cached_count(&reader).await?, 65);
    drop(reader);
    drop(writer);
    drop(database);
    let reopened = Builder::new_local(path.to_str().unwrap()).build().await?;
    let connection = reopened.connect()?;
    assert_eq!(cached_count(&connection).await?, 65);
    Ok(())
}

#[tokio::test]
async fn explicit_checkpoint_materializes_backfill_and_preserves_reopen() -> Result {
    let root = tempfile::tempdir()?;
    let path = root.path().join("backfill.turso");
    let database = Builder::new_local(path.to_str().unwrap()).build().await?;
    let writer = database.connect()?;
    writer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB)", ())
        .await?;
    writer.pragma_update("wal_checkpoint", "PASSIVE").await?;
    let initial_main_bytes = std::fs::metadata(&path)?.len();
    insert_blobs(&writer, 600).await?;
    // Native automatic checkpointing may also backfill. The app's explicit
    // checkpoint must still complete and preserve all rows and their bodies.
    writer.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    assert!(std::fs::metadata(&path)?.len() > initial_main_bytes + 4 * 1024 * 1024);
    let wal_bytes = std::fs::metadata(path.with_file_name("backfill.turso-wal"))
        .map_or(0, |metadata| metadata.len());
    assert!(wal_bytes <= 32, "truncation must leave no WAL data frames");
    writer
        .execute("INSERT INTO items VALUES(601, zeroblob(8192))", ())
        .await?;
    writer
        .execute("INSERT INTO items VALUES(602, zeroblob(8192))", ())
        .await?;
    writer.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    assert_eq!(integer(&writer, "SELECT count(*) FROM items").await?, 602);
    drop(writer);
    drop(database);
    let reopened = Builder::new_local(path.to_str().unwrap()).build().await?;
    let connection = reopened.connect()?;
    assert_eq!(
        integer(&connection, "SELECT count(*) FROM items").await?,
        602
    );
    assert_eq!(
        integer(&connection, "SELECT sum(length(body)) FROM items").await?,
        602 * 8192
    );
    assert_eq!(
        integer(&connection, "SELECT sum(id) FROM items").await?,
        602 * 603 / 2
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_under_pinned_reader_preserves_snapshot_and_writer_progress() -> Result {
    let root = tempfile::tempdir()?;
    let database = Builder::new_local(root.path().join("snapshot.db").to_str().unwrap())
        .build()
        .await?;
    let writer = database.connect()?;
    let reader = database.connect()?;
    writer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB)", ())
        .await?;
    reader.execute("BEGIN", ()).await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 0);
    insert_blobs(&writer, 64).await?;
    writer.pragma_update("wal_checkpoint", "PASSIVE").await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 0);
    assert_eq!(integer(&writer, "SELECT count(*) FROM items").await?, 64);
    reader.execute("COMMIT", ()).await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 64);
    writer.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    Ok(())
}
