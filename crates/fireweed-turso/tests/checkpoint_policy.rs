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

async fn cached_limit(connection: &Connection) -> Result<i64> {
    let mut statement = connection
        .prepare_cached("PRAGMA wal_autocheckpoint")
        .await?;
    let mut rows = statement.query(()).await?;
    Ok(rows
        .next()
        .await?
        .expect("checkpoint setting row")
        .get::<i64>(0)?)
}

#[tokio::test]
async fn checkpoint_limit_is_connection_local_and_cached_readback_tracks_changes() -> Result {
    let root = tempfile::tempdir()?;
    let database = Builder::new_local(root.path().join("policy.db").to_str().unwrap())
        .build()
        .await?;
    let writer = database.connect()?;
    let reader = database.connect()?;
    assert_eq!(cached_limit(&writer).await?, 1000);
    writer.pragma_update("wal_autocheckpoint", 32_000).await?;
    assert_eq!(cached_limit(&writer).await?, 32_000);
    assert_eq!(cached_limit(&reader).await?, 1000);
    reader.pragma_update("wal_autocheckpoint", 0).await?;
    assert_eq!(cached_limit(&reader).await?, 0);
    assert_eq!(cached_limit(&writer).await?, 32_000);
    writer.pragma_update("wal_autocheckpoint", -1).await?;
    assert_eq!(cached_limit(&writer).await?, 0);
    assert!(
        writer
            .pragma_update("wal_autocheckpoint", "1.5")
            .await
            .is_err()
    );
    assert_eq!(cached_limit(&writer).await?, 0);
    Ok(())
}

#[tokio::test]
async fn checkpoint_limit_controls_backfill_and_preserves_reopen() -> Result {
    let root = tempfile::tempdir()?;
    let path = root.path().join("backfill.db");
    let database = Builder::new_local(path.to_str().unwrap()).build().await?;
    let writer = database.connect()?;
    writer
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body BLOB)", ())
        .await?;
    writer.pragma_update("wal_checkpoint", "PASSIVE").await?;
    let initial_main_bytes = std::fs::metadata(&path)?.len();
    writer.pragma_update("wal_autocheckpoint", 0).await?;
    insert_blobs(&writer, 600).await?;
    assert!(std::fs::metadata(path.with_file_name("backfill.db-wal"))?.len() > 4096 * 1000);
    assert_eq!(
        std::fs::metadata(&path)?.len(),
        initial_main_bytes,
        "disabled automatic checkpoints must not backfill on commit"
    );
    writer.pragma_update("wal_autocheckpoint", 32).await?;
    writer
        .execute("INSERT INTO items VALUES(601, zeroblob(8192))", ())
        .await?;
    assert!(
        std::fs::metadata(&path)?.len() > initial_main_bytes + 4 * 1024 * 1024,
        "lowering the threshold must actually trigger checkpoint backfill"
    );
    writer.pragma_update("wal_autocheckpoint", 0).await?;
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
        cached_limit(&connection).await?,
        1000,
        "policy is not persisted as database state"
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
    writer.pragma_update("wal_autocheckpoint", 8).await?;
    reader.execute("BEGIN", ()).await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 0);
    insert_blobs(&writer, 64).await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 0);
    assert_eq!(integer(&writer, "SELECT count(*) FROM items").await?, 64);
    reader.execute("COMMIT", ()).await?;
    assert_eq!(integer(&reader, "SELECT count(*) FROM items").await?, 64);
    writer.pragma_update("wal_checkpoint", "TRUNCATE").await?;
    Ok(())
}
